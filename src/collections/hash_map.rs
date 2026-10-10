use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::object::{HasKey, Key};
use bytes::Bytes;
use fred::interfaces::{HashesInterface, KeysInterface};
use fred::types::scripts::Script;
use fred::types::Value;
use futures::{stream, Stream, StreamExt, TryStreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;
use std::sync::LazyLock;

pub(crate) static INSERT: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local previous = redis.call('HGET', KEYS[1], ARGV[1])
        redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
        return previous",
    )
});

pub(crate) static REMOVE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local previous = redis.call('HGET', KEYS[1], ARGV[1])
        redis.call('HDEL', KEYS[1], ARGV[1])
        return previous",
    )
});

static PUT_IF_ABSENT: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2]) == 1 then
            return nil
        end
        return redis.call('HGET', KEYS[1], ARGV[1])",
    )
});

static PUT_IF_EXISTS: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local value = redis.call('HGET', KEYS[1], ARGV[1])
        if value ~= false then
            redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
            return value
        end
        return nil",
    )
});

static FAST_PUT_IF_EXISTS: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local value = redis.call('HGET', KEYS[1], ARGV[1])
        if value ~= false then
            redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
            return 1
        end
        return 0",
    )
});

static REPLACE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then
            local value = redis.call('HGET', KEYS[1], ARGV[1])
            redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
            return value
        end
        return nil",
    )
});

static FAST_REPLACE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then
            redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
            return 1
        end
        return 0",
    )
});

static REPLACE_IF: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then
            redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
            return 1
        end
        return 0",
    )
});

static REMOVE_IF: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then
            return redis.call('HDEL', KEYS[1], ARGV[1])
        end
        return 0",
    )
});

static CONTAINS_VALUE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local values = redis.call('HVALS', KEYS[1])
        for i = 1, #values, 1 do
            if ARGV[1] == values[i] then
                return 1
            end
        end
        return 0",
    )
});

const SCAN_PAGE: i64 = 100;

/// A distributed map stored in a Redis hash.
pub struct HashMap<K, V, C: Codec> {
    key: Key,
    codec: C,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V, C: Codec> Clone for HashMap<K, V, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
            _marker: PhantomData,
        }
    }
}

impl<K, V, C: Codec> fmt::Debug for HashMap<K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "HashMap")
    }
}

impl<K, V, C: Codec> HasKey for HashMap<K, V, C> {
    fn key(&self) -> &Key {
        &self.key
    }
}

impl<K, V, C: Codec> HashMap<K, V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self {
            key,
            codec,
            _marker: PhantomData,
        }
    }
}

impl<K, V, C> HashMap<K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    fn decode_value(&self, raw: Option<Bytes>) -> Result<Option<V>> {
        raw.map(|bytes| self.codec.decode(&bytes)).transpose()
    }

    /// Inserts an entry and returns the value it replaced. The key and value are borrowed: `map.insert("a", &value)`.
    pub async fn insert<Q, W>(&self, k: &Q, v: &W) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        let previous: Option<Bytes> = self
            .key
            .core
            .eval(
                &INSERT,
                vec![self.key.redis_key()],
                vec![self.codec.encode(k)?, self.codec.encode(v)?],
            )
            .await?;
        self.decode_value(previous)
    }

    /// Returns the value for the key, or `None` when it is missing.
    pub async fn get<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let raw: Option<Bytes> = self
            .key
            .core
            .redis()
            .hget(self.key.redis_key(), self.codec.encode(k)?)
            .await?;
        self.decode_value(raw)
    }

    /// Removes the entry and returns its value.
    pub async fn remove<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let previous: Option<Bytes> = self
            .key
            .core
            .eval(
                &REMOVE,
                vec![self.key.redis_key()],
                vec![self.codec.encode(k)?],
            )
            .await?;
        self.decode_value(previous)
    }

    /// Returns whether the key is present.
    pub async fn contains_key<Q>(&self, k: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let found: bool = self
            .key
            .core
            .redis()
            .hexists(self.key.redis_key(), self.codec.encode(k)?)
            .await?;
        Ok(found)
    }

    /// Number of entries.
    pub async fn len(&self) -> Result<usize> {
        let len: usize = self.key.core.redis().hlen(self.key.redis_key()).await?;
        Ok(len)
    }

    /// Returns whether the map has no entries.
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// Deletes every entry.
    pub async fn clear(&self) -> Result<()> {
        self.key
            .core
            .redis()
            .del::<(), _>(self.key.redis_key())
            .await?;
        Ok(())
    }

    /// Inserts all entries in one round trip. Entries are `(&key, &value)` pairs, for example `map.extend(other.iter())`.
    pub async fn extend<'a, Q, W>(
        &self,
        entries: impl IntoIterator<Item = (&'a Q, &'a W)>,
    ) -> Result<()>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync + 'a,
        W: Serialize + ?Sized + Sync + 'a,
    {
        let encoded = entries
            .into_iter()
            .map(|(k, v)| Ok((self.codec.encode(k)?, self.codec.encode(v)?)))
            .collect::<Result<Vec<(Bytes, Bytes)>>>()?;
        if encoded.is_empty() {
            return Ok(());
        }
        self.key
            .core
            .redis()
            .hset::<(), _, _>(self.key.redis_key(), encoded)
            .await?;
        Ok(())
    }

    /// Returns the values for several keys, in the order of the keys.
    pub async fn get_many<Q>(&self, ks: &[&Q]) -> Result<Vec<Option<V>>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if ks.is_empty() {
            return Ok(Vec::new());
        }
        let fields = ks
            .iter()
            .map(|k| self.codec.encode(*k))
            .collect::<Result<Vec<Bytes>>>()?;
        let raw: Vec<Option<Bytes>> = self
            .key
            .core
            .redis()
            .hmget(self.key.redis_key(), fields)
            .await?;
        raw.into_iter().map(|r| self.decode_value(r)).collect()
    }

    /// Inserts the entry only when the key is missing (`HSETNX`); returns whether it was inserted. Like Redisson `fastPutIfAbsent`.
    pub async fn insert_nx<Q, W>(&self, k: &Q, v: &W) -> Result<bool>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        let inserted: bool = self
            .key
            .core
            .redis_no_retry()
            .hsetnx(
                self.key.redis_key(),
                self.codec.encode(k)?,
                self.codec.encode(v)?,
            )
            .await?;
        Ok(inserted)
    }

    /// Adds `delta` to the integer stored under the key, creating it at zero, and returns the result.
    pub async fn incr_by<Q>(&self, k: &Q, delta: i64) -> Result<i64>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let value: i64 = self
            .key
            .core
            .redis_no_retry()
            .hincrby(self.key.redis_key(), self.codec.encode(k)?, delta)
            .await?;
        Ok(value)
    }

    /// Adds `delta` to the float stored under the key, creating it at zero, and returns the result.
    pub async fn incr_by_float<Q>(&self, k: &Q, delta: f64) -> Result<f64>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let value: f64 = self
            .key
            .core
            .redis_no_retry()
            .hincrbyfloat(self.key.redis_key(), self.codec.encode(k)?, delta)
            .await?;
        Ok(value)
    }

    async fn eval_value(&self, script: &Script, args: Vec<Bytes>) -> Result<Option<V>> {
        let raw: Option<Bytes> = self
            .key
            .core
            .eval(script, vec![self.key.redis_key()], args)
            .await?;
        self.decode_value(raw)
    }

    async fn eval_flag(&self, script: &Script, args: Vec<Bytes>) -> Result<bool> {
        let flag: i64 = self
            .key
            .core
            .eval(script, vec![self.key.redis_key()], args)
            .await?;
        Ok(flag == 1)
    }

    /// Inserts an entry and returns whether the key was new (`HSET`). Like Redisson `fastPut`.
    pub async fn fast_insert<Q, W>(&self, k: &Q, v: &W) -> Result<bool>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        let added: i64 = self
            .key
            .core
            .redis()
            .hset(
                self.key.redis_key(),
                (self.codec.encode(k)?, self.codec.encode(v)?),
            )
            .await?;
        Ok(added == 1)
    }

    /// Inserts the entry only when the key is missing and returns the current value otherwise. Like Redisson `putIfAbsent`.
    pub async fn insert_if_absent<Q, W>(&self, k: &Q, v: &W) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        let raw: Option<Bytes> = self
            .key
            .core
            .eval_no_retry(
                &PUT_IF_ABSENT,
                vec![self.key.redis_key()],
                vec![self.codec.encode(k)?, self.codec.encode(v)?],
            )
            .await?;
        self.decode_value(raw)
    }

    /// Replaces the value only when the key is present and returns the value it replaced. Like Redisson `putIfExists`.
    pub async fn insert_if_exists<Q, W>(&self, k: &Q, v: &W) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        self.eval_value(
            &PUT_IF_EXISTS,
            vec![self.codec.encode(k)?, self.codec.encode(v)?],
        )
        .await
    }

    /// Replaces the value only when the key is present; returns whether it did. Like Redisson `fastPutIfExists`.
    pub async fn fast_insert_if_exists<Q, W>(&self, k: &Q, v: &W) -> Result<bool>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        self.eval_flag(
            &FAST_PUT_IF_EXISTS,
            vec![self.codec.encode(k)?, self.codec.encode(v)?],
        )
        .await
    }

    /// Replaces the value of a present key and returns the value it replaced. Like Redisson `replace(key, value)`.
    pub async fn replace<Q, W>(&self, k: &Q, v: &W) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        self.eval_value(&REPLACE, vec![self.codec.encode(k)?, self.codec.encode(v)?])
            .await
    }

    /// Replaces the value of a present key; returns whether it did. Like Redisson `fastReplace`.
    pub async fn fast_replace<Q, W>(&self, k: &Q, v: &W) -> Result<bool>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        self.eval_flag(
            &FAST_REPLACE,
            vec![self.codec.encode(k)?, self.codec.encode(v)?],
        )
        .await
    }

    /// Replaces the value only when it currently equals `old`; returns whether it did. Like Redisson `replace(key, old, new)`.
    pub async fn replace_if<Q, W>(&self, k: &Q, old: &W, new: &W) -> Result<bool>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        self.eval_flag(
            &REPLACE_IF,
            vec![
                self.codec.encode(k)?,
                self.codec.encode(old)?,
                self.codec.encode(new)?,
            ],
        )
        .await
    }

    /// Removes the entry only when its value equals `v`; returns whether it did. Like Redisson `remove(key, value)`.
    pub async fn remove_if<Q, W>(&self, k: &Q, v: &W) -> Result<bool>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        self.eval_flag(
            &REMOVE_IF,
            vec![self.codec.encode(k)?, self.codec.encode(v)?],
        )
        .await
    }

    /// Removes the keys and returns how many were present (`HDEL`). Like Redisson `fastRemove`.
    pub async fn fast_remove<Q>(&self, ks: &[&Q]) -> Result<usize>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if ks.is_empty() {
            return Ok(0);
        }
        let fields = ks
            .iter()
            .map(|k| self.codec.encode(*k))
            .collect::<Result<Vec<Bytes>>>()?;
        let removed: usize = self
            .key
            .core
            .redis()
            .hdel(self.key.redis_key(), fields)
            .await?;
        Ok(removed)
    }

    /// Returns whether any entry has a value equal to `v`. It reads every value on the server.
    pub async fn contains_value<W>(&self, v: &W) -> Result<bool>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        self.eval_flag(&CONTAINS_VALUE, vec![self.codec.encode(v)?])
            .await
    }

    /// Length in bytes of the encoded value stored under the key; 0 when missing (`HSTRLEN`).
    pub async fn value_size<Q>(&self, k: &Q) -> Result<usize>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let size: usize = self
            .key
            .core
            .redis()
            .hstrlen(self.key.redis_key(), self.codec.encode(k)?)
            .await?;
        Ok(size)
    }

    /// Returns up to `count` distinct random keys (`HRANDFIELD`).
    pub async fn random_keys(&self, count: usize) -> Result<Vec<K>> {
        let count = i64::try_from(count).unwrap_or(i64::MAX);
        let raw: Vec<Bytes> = self
            .key
            .core
            .redis()
            .hrandfield(self.key.redis_key(), Some((count, false)))
            .await?;
        raw.iter().map(|bytes| self.codec.decode(bytes)).collect()
    }

    /// Returns every key in one call (`HKEYS`).
    pub async fn read_all_keys(&self) -> Result<Vec<K>> {
        let raw: Vec<Bytes> = self.key.core.redis().hkeys(self.key.redis_key()).await?;
        raw.iter().map(|bytes| self.codec.decode(bytes)).collect()
    }

    /// Returns every value in one call (`HVALS`).
    pub async fn read_all_values(&self) -> Result<Vec<V>> {
        let raw: Vec<Bytes> = self.key.core.redis().hvals(self.key.redis_key()).await?;
        raw.iter().map(|bytes| self.codec.decode(bytes)).collect()
    }

    /// Returns every entry in one call (`HGETALL`).
    pub async fn read_all_entries(&self) -> Result<Vec<(K, V)>> {
        let raw: Value = self.key.core.redis().hgetall(self.key.redis_key()).await?;
        self.decode_pairs(raw)
    }

    fn decode_pairs(&self, raw: Value) -> Result<Vec<(K, V)>> {
        let flat: Vec<Bytes> = match raw {
            Value::Map(map) => map
                .inner()
                .into_iter()
                .flat_map(|(field, value)| [Value::from(field), value])
                .map(|value| value.convert())
                .collect::<std::result::Result<_, _>>()?,
            other => other.convert()?,
        };
        flat.as_chunks::<2>()
            .0
            .iter()
            .map(|[field, value]| Ok((self.codec.decode(field)?, self.codec.decode(value)?)))
            .collect()
    }

    /// Streams all entries, reading the hash in pages of 100 with `HSCAN`.
    pub fn iter(&self) -> impl Stream<Item = Result<(K, V)>> + '_ {
        stream::try_unfold(Some(Bytes::from_static(b"0")), move |cursor| async move {
            let Some(cursor) = cursor else {
                return Ok::<_, Error>(None);
            };
            let reply = self
                .key
                .command(
                    "HSCAN",
                    vec![
                        Value::from(self.key.redis_key()),
                        Value::from(cursor),
                        Value::from("COUNT"),
                        Value::from(SCAN_PAGE),
                    ],
                    0,
                )
                .await?;
            let (next, page): (Bytes, Value) = reply.convert()?;
            let next = (next.as_ref() != b"0").then_some(next);
            let items = self.decode_pairs(page)?;
            Ok(Some((stream::iter(items.into_iter().map(Ok)), next)))
        })
        .try_flatten()
    }

    /// Streams all keys.
    pub fn keys(&self) -> impl Stream<Item = Result<K>> + '_ {
        self.iter().map(|entry| entry.map(|(k, _)| k))
    }

    /// Streams all values.
    pub fn values(&self) -> impl Stream<Item = Result<V>> + '_ {
        self.iter().map(|entry| entry.map(|(_, v)| v))
    }
}
