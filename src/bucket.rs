use crate::codec::Codec;
use crate::error::Result;
use crate::object::{millis, HasKey, Key};
use bytes::Bytes;
use fred::interfaces::KeysInterface;
use fred::types::scripts::Script;
use fred::types::{Expiration, SetOptions};
use serde::de::DeserializeOwned;
use serde::Serialize;
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
    /// Stores the value, replacing any existing one.
    pub async fn set(&self, value: &V) -> Result<()> {
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
        raw.map(|bytes| self.codec.decode(&bytes)).transpose()
    }

    /// Stores the value with a time to live.
    pub async fn set_ex(&self, value: &V, ttl: Duration) -> Result<()> {
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

    /// Stores the value only when the key is missing; returns whether it was stored.
    pub async fn set_nx(&self, value: &V) -> Result<bool> {
        let bytes = self.codec.encode(value)?;
        let stored: Option<String> = self
            .key
            .core
            .redis()
            .set(
                self.key.redis_key(),
                bytes,
                None,
                Some(SetOptions::NX),
                false,
            )
            .await?;
        Ok(stored.is_some())
    }

    /// Stores the value and returns the previous one. Needs Redis 6.2 or newer.
    pub async fn get_set(&self, value: &V) -> Result<Option<V>> {
        let bytes = self.codec.encode(value)?;
        let previous: Option<Bytes> = self
            .key
            .core
            .redis()
            .set(self.key.redis_key(), bytes, None, None, true)
            .await?;
        previous.map(|bytes| self.codec.decode(&bytes)).transpose()
    }

    /// Returns the value and deletes the key. Needs Redis 6.2 or newer.
    pub async fn get_del(&self) -> Result<Option<V>> {
        let previous: Option<Bytes> = self.key.core.redis().getdel(self.key.redis_key()).await?;
        previous.map(|bytes| self.codec.decode(&bytes)).transpose()
    }

    /// Replaces the value with `new` only when it currently equals `expected`; returns whether it did.
    pub async fn compare_and_set(&self, expected: &V, new: &V) -> Result<bool> {
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
