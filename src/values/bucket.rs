use crate::codec::Codec;
use crate::error::Result;
use crate::object::{millis, HasKey, Key};
use bytes::Bytes;
use fred::interfaces::KeysInterface;
use fred::types::scripts::Script;
use fred::types::{Expiration, SetOptions, Value};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;
use std::sync::LazyLock;
use std::time::Duration;

static COMPARE_AND_SET: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('GET', KEYS[1]) == ARGV[1] then
            redis.call('SET', KEYS[1], ARGV[2])
            return 1
        end
        return 0",
    )
});

static COMPARE_AND_DELETE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('GET', KEYS[1]) == ARGV[1] then
            redis.call('DEL', KEYS[1])
            return 1
        end
        return 0",
    )
});

static GET_AND_DELETE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local value = redis.call('GET', KEYS[1])
        redis.call('DEL', KEYS[1])
        return value",
    )
});

static GET_AND_SET_EX: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local value = redis.call('GET', KEYS[1])
        redis.call('PSETEX', KEYS[1], ARGV[2], ARGV[1])
        return value",
    )
});

/// A single value stored in a Redis string.
pub struct Bucket<V, C: Codec> {
    key: Key,
    codec: C,
    _marker: PhantomData<fn() -> V>,
}

impl<V, C: Codec> Clone for Bucket<V, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
            _marker: PhantomData,
        }
    }
}

impl<V, C: Codec> fmt::Debug for Bucket<V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "Bucket")
    }
}

impl<V, C: Codec> HasKey for Bucket<V, C> {
    fn key(&self) -> &Key {
        &self.key
    }
}

impl<V, C: Codec> Bucket<V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self {
            key,
            codec,
            _marker: PhantomData,
        }
    }
}

impl<V, C> Bucket<V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    fn decode(&self, raw: Option<Bytes>) -> Result<Option<V>> {
        raw.map(|bytes| self.codec.decode(&bytes)).transpose()
    }

    async fn set_with(
        &self,
        bytes: Bytes,
        expire: Option<Expiration>,
        options: Option<SetOptions>,
    ) -> Result<bool> {
        let stored: Option<String> = self
            .key
            .core
            .redis_no_retry()
            .set(self.key.redis_key(), bytes, expire, options, false)
            .await?;
        Ok(stored.is_some())
    }

    async fn get_ex(&self, args: Vec<Value>) -> Result<Option<V>> {
        let args = std::iter::once(Value::from(self.key.redis_key()))
            .chain(args)
            .collect();
        let raw: Option<Bytes> = self.key.command("GETEX", args, 0).await?.convert()?;
        self.decode(raw)
    }

    /// Stores the value, replacing any existing one and clearing its time to live.
    pub async fn set<Q>(&self, value: &Q) -> Result<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let bytes = self.codec.encode(value)?;
        self.key
            .core
            .redis()
            .set::<(), _, _>(self.key.redis_key(), bytes, None, None, false)
            .await?;
        Ok(())
    }

    /// Returns the value, or `None` when the key is missing.
    pub async fn get(&self) -> Result<Option<V>> {
        let raw: Option<Bytes> = self.key.core.redis().get(self.key.redis_key()).await?;
        self.decode(raw)
    }

    /// Stores the value with a time to live (`PSETEX`).
    pub async fn set_ex<Q>(&self, value: &Q, ttl: Duration) -> Result<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let bytes = self.codec.encode(value)?;
        self.key
            .core
            .redis()
            .set::<(), _, _>(
                self.key.redis_key(),
                bytes,
                Some(Expiration::PX(millis(ttl)?)),
                None,
                false,
            )
            .await?;
        Ok(())
    }

    /// Stores the value and keeps the current time to live (`SET KEEPTTL`).
    pub async fn set_keep_ttl<Q>(&self, value: &Q) -> Result<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let bytes = self.codec.encode(value)?;
        self.key
            .core
            .redis()
            .set::<(), _, _>(
                self.key.redis_key(),
                bytes,
                Some(Expiration::KEEPTTL),
                None,
                false,
            )
            .await?;
        Ok(())
    }

    /// Stores the value only when the key is missing; returns whether it was stored. Like Redisson `setIfAbsent`.
    pub async fn set_nx<Q>(&self, value: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let bytes = self.codec.encode(value)?;
        let stored: bool = self
            .key
            .core
            .redis_no_retry()
            .setnx(self.key.redis_key(), bytes)
            .await?;
        Ok(stored)
    }

    /// Stores the value with a time to live only when the key is missing; returns whether it was stored.
    pub async fn set_nx_ex<Q>(&self, value: &Q, ttl: Duration) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let bytes = self.codec.encode(value)?;
        self.set_with(
            bytes,
            Some(Expiration::PX(millis(ttl)?)),
            Some(SetOptions::NX),
        )
        .await
    }

    /// Replaces the value only when the key exists; returns whether it was stored. Like Redisson `setIfExists`.
    pub async fn set_xx<Q>(&self, value: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let bytes = self.codec.encode(value)?;
        self.set_with(bytes, None, Some(SetOptions::XX)).await
    }

    /// Replaces the value and sets a time to live only when the key exists; returns whether it was stored.
    pub async fn set_xx_ex<Q>(&self, value: &Q, ttl: Duration) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let bytes = self.codec.encode(value)?;
        self.set_with(
            bytes,
            Some(Expiration::PX(millis(ttl)?)),
            Some(SetOptions::XX),
        )
        .await
    }

    /// Stores the value and returns the previous one (`GETSET`). The time to live is cleared.
    pub async fn get_set<Q>(&self, value: &Q) -> Result<Option<V>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let bytes = self.codec.encode(value)?;
        let previous: Option<Bytes> = self
            .key
            .core
            .redis()
            .getset(self.key.redis_key(), bytes)
            .await?;
        self.decode(previous)
    }

    /// Stores the value with a time to live and returns the previous one.
    pub async fn get_set_ex<Q>(&self, value: &Q, ttl: Duration) -> Result<Option<V>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let previous: Option<Bytes> = self
            .key
            .core
            .eval(
                &GET_AND_SET_EX,
                vec![self.key.redis_key()],
                vec![
                    self.codec.encode(value)?,
                    Bytes::from(millis(ttl)?.to_string()),
                ],
            )
            .await?;
        self.decode(previous)
    }

    /// Returns the value and deletes the key.
    pub async fn get_del(&self) -> Result<Option<V>> {
        let previous: Option<Bytes> = self
            .key
            .core
            .eval(&GET_AND_DELETE, vec![self.key.redis_key()], Vec::new())
            .await?;
        self.decode(previous)
    }

    /// Returns the value and sets a time to live on the key (`GETEX PX`). Needs Redis 6.2 or newer.
    pub async fn get_and_expire(&self, ttl: Duration) -> Result<Option<V>> {
        self.get_ex(vec![Value::from("PX"), Value::from(millis(ttl)?)])
            .await
    }

    /// Returns the value and clears the time to live of the key (`GETEX PERSIST`). Needs Redis 6.2 or newer.
    pub async fn get_and_clear_expire(&self) -> Result<Option<V>> {
        self.get_ex(vec![Value::from("PERSIST")]).await
    }

    /// Length of the encoded value in bytes (`STRLEN`); 0 when the key is missing.
    pub async fn size(&self) -> Result<usize> {
        let size: usize = self.key.core.redis().strlen(self.key.redis_key()).await?;
        Ok(size)
    }

    /// Atomically replaces the value when it currently equals `expected`; returns whether it did.
    ///
    /// Like Redisson `compareAndSet`: `expected = None` stores `new` only when the key is missing,
    /// `new = None` deletes the key when it holds `expected`, and both `None` returns whether the key is missing.
    pub async fn compare_and_set<Q>(&self, expected: Option<&Q>, new: Option<&Q>) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        match (expected, new) {
            (None, None) => Ok(!self.key.exists().await?),
            (None, Some(new)) => self.set_nx(new).await,
            (Some(expected), None) => self.compare_and_delete(expected).await,
            (Some(expected), Some(new)) => {
                let swapped: i64 = self
                    .key
                    .core
                    .eval(
                        &COMPARE_AND_SET,
                        vec![self.key.redis_key()],
                        vec![self.codec.encode(expected)?, self.codec.encode(new)?],
                    )
                    .await?;
                Ok(swapped == 1)
            }
        }
    }

    /// Atomically deletes the key when it holds `expected`; returns whether it did.
    pub async fn compare_and_delete<Q>(&self, expected: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let deleted: i64 = self
            .key
            .core
            .eval(
                &COMPARE_AND_DELETE,
                vec![self.key.redis_key()],
                vec![self.codec.encode(expected)?],
            )
            .await?;
        Ok(deleted == 1)
    }
}
