use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::list;
use crate::object::{HasKey, Key};
use bytes::Bytes;
use fred::interfaces::{KeysInterface, ListInterface};
use fred::types::scripts::Script;
use futures::Stream;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::LazyLock;
use uuid::Uuid;

static INSERT: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local length = redis.call('LLEN', KEYS[1])
        local index = tonumber(ARGV[1])
        if index > length then
            return redis.error_reply('index out of range')
        end
        if index == length then
            redis.call('RPUSH', KEYS[1], ARGV[2])
            return 1
        end
        local displaced = redis.call('LINDEX', KEYS[1], index)
        redis.call('LSET', KEYS[1], index, ARGV[3])
        redis.call('LINSERT', KEYS[1], 'BEFORE', ARGV[3], ARGV[2])
        redis.call('LSET', KEYS[1], index + 1, displaced)
        return 1",
    )
});

static REMOVE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if tonumber(ARGV[1]) >= redis.call('LLEN', KEYS[1]) then
            return false
        end
        local removed = redis.call('LINDEX', KEYS[1], ARGV[1])
        if not removed then
            return false
        end
        redis.call('LSET', KEYS[1], ARGV[1], ARGV[2])
        redis.call('LREM', KEYS[1], 1, ARGV[2])
        return removed",
    )
});

fn redis_index(index: usize) -> Option<i64> {
    i64::try_from(index).ok()
}

fn redis_end(end: usize) -> i64 {
    i64::try_from(end).unwrap_or(i64::MAX) - 1
}

fn out_of_range(error: Error) -> Error {
    match error {
        Error::Redis(message)
            if message.contains("index out of range") || message.contains("no such key") =>
        {
            Error::OutOfRange
        }
        other => other,
    }
}

/// A distributed vector stored in a Redis list. It is indexed from the front.
pub struct Vec<V, C: Codec> {
    key: Key,
    codec: C,
    _marker: PhantomData<fn() -> V>,
}

impl<V, C: Codec> Clone for Vec<V, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
            _marker: PhantomData,
        }
    }
}

impl<V, C: Codec> fmt::Debug for Vec<V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "Vec")
    }
}

impl<V, C: Codec> HasKey for Vec<V, C> {
    fn key(&self) -> &Key {
        &self.key
    }
}

impl<V, C: Codec> Vec<V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self {
            key,
            codec,
            _marker: PhantomData,
        }
    }
}

impl<V, C> Vec<V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    fn decode(&self, raw: Option<Bytes>) -> Result<Option<V>> {
        raw.map(|bytes| self.codec.decode(&bytes)).transpose()
    }

    /// Appends a value. The value is borrowed: `vec.push("a")`.
    pub async fn push<Q>(&self, v: &Q) -> Result<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.key
            .core
            .redis()
            .rpush::<(), _, _>(self.key.redis_key(), self.codec.encode(v)?)
            .await?;
        Ok(())
    }

    /// Appends many values in one round trip.
    pub async fn extend<'a, Q>(&self, values: impl IntoIterator<Item = &'a Q>) -> Result<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync + 'a,
    {
        let encoded = values
            .into_iter()
            .map(|v| self.codec.encode(v))
            .collect::<Result<std::vec::Vec<Bytes>>>()?;
        if encoded.is_empty() {
            return Ok(());
        }
        self.key
            .core
            .redis()
            .rpush::<(), _, _>(self.key.redis_key(), encoded)
            .await?;
        Ok(())
    }

    /// Removes and returns the last value.
    pub async fn pop(&self) -> Result<Option<V>> {
        let raw: Option<Bytes> = self
            .key
            .core
            .redis()
            .rpop(self.key.redis_key(), None)
            .await?;
        self.decode(raw)
    }

    /// Returns the value at the index, or `None` when the index is past the end.
    pub async fn get(&self, index: usize) -> Result<Option<V>> {
        let Some(index) = redis_index(index) else {
            return Ok(None);
        };
        let raw: Option<Bytes> = self
            .key
            .core
            .redis()
            .lindex(self.key.redis_key(), index)
            .await?;
        self.decode(raw)
    }

    /// Replaces the value at the index. Returns [`Error::OutOfRange`] when the index is past the end.
    pub async fn set<Q>(&self, index: usize, v: &Q) -> Result<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let index = redis_index(index).ok_or(Error::OutOfRange)?;
        self.key
            .core
            .redis()
            .lset::<(), _, _>(self.key.redis_key(), index, self.codec.encode(v)?)
            .await
            .map_err(|error| out_of_range(error.into()))
    }

    /// Inserts a value before the index and moves the rest back. It is atomic and costs O(n).
    /// Returns [`Error::OutOfRange`] when the index is past the length.
    pub async fn insert<Q>(&self, index: usize, v: &Q) -> Result<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.key
            .core
            .eval::<i64>(
                &INSERT,
                vec![self.key.redis_key()],
                vec![
                    Bytes::from(index.to_string()),
                    self.codec.encode(v)?,
                    Bytes::from(Uuid::new_v4().to_string()),
                ],
            )
            .await
            .map(|_| ())
            .map_err(out_of_range)
    }

    /// Removes and returns the value at the index. It is atomic and costs O(n).
    pub async fn remove(&self, index: usize) -> Result<Option<V>> {
        let raw: Option<Bytes> = self
            .key
            .core
            .eval(
                &REMOVE,
                vec![self.key.redis_key()],
                vec![
                    Bytes::from(index.to_string()),
                    Bytes::from(Uuid::new_v4().to_string()),
                ],
            )
            .await?;
        self.decode(raw)
    }

    /// Returns the values in the range. The end is exclusive, like a slice range.
    pub async fn range(&self, range: Range<usize>) -> Result<std::vec::Vec<V>> {
        if range.start >= range.end {
            return Ok(std::vec::Vec::new());
        }
        let raw: std::vec::Vec<Bytes> = self
            .key
            .core
            .redis()
            .lrange(
                self.key.redis_key(),
                redis_index(range.start).unwrap_or(i64::MAX),
                redis_end(range.end),
            )
            .await?;
        raw.iter().map(|bytes| self.codec.decode(bytes)).collect()
    }

    /// Keeps only the values in the range and drops the rest. The end is exclusive.
    pub async fn trim(&self, range: Range<usize>) -> Result<()> {
        if range.start >= range.end {
            return self.clear().await;
        }
        self.key
            .core
            .redis()
            .ltrim::<(), _>(
                self.key.redis_key(),
                redis_index(range.start).unwrap_or(i64::MAX),
                redis_end(range.end),
            )
            .await?;
        Ok(())
    }

    /// Returns the index of the first equal value.
    pub async fn position<Q>(&self, v: &Q) -> Result<Option<usize>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let index: Option<i64> = self
            .key
            .core
            .redis()
            .lpos(
                self.key.redis_key(),
                self.codec.encode(v)?,
                None,
                None,
                None,
            )
            .await?;
        Ok(index.map(|index| index as usize))
    }

    /// Returns whether an equal value is in the vector.
    pub async fn contains<Q>(&self, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Ok(self.position(v).await?.is_some())
    }

    /// Removes the first equal value; returns whether one was removed.
    pub async fn remove_value<Q>(&self, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Ok(self.remove_matching(v, 1).await? > 0)
    }

    /// Removes every equal value and returns how many were removed.
    pub async fn remove_all<Q>(&self, v: &Q) -> Result<usize>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.remove_matching(v, 0).await
    }

    async fn remove_matching<Q>(&self, v: &Q, count: i64) -> Result<usize>
    where
        Q: Serialize + ?Sized + Sync,
    {
        let removed: usize = self
            .key
            .core
            .redis()
            .lrem(self.key.redis_key(), count, self.codec.encode(v)?)
            .await?;
        Ok(removed)
    }

    /// Number of values.
    pub async fn len(&self) -> Result<usize> {
        let len: usize = self.key.core.redis().llen(self.key.redis_key()).await?;
        Ok(len)
    }

    /// Returns whether the vector has no values.
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// Deletes every value.
    pub async fn clear(&self) -> Result<()> {
        self.key
            .core
            .redis()
            .del::<(), _>(self.key.redis_key())
            .await?;
        Ok(())
    }

    /// Streams all values in order, reading the list in pages of 100 with `LRANGE`.
    /// Changes made while the stream runs can make it skip or repeat a value.
    pub fn iter(&self) -> impl Stream<Item = Result<V>> + '_ {
        list::pages(&self.key, &self.codec)
    }
}
