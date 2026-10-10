use crate::codec::Codec;
use crate::collections::list;
use crate::error::{Error, Result};
use crate::object::{HasKey, Key};
use bytes::Bytes;
use fred::interfaces::ListInterface;
use fred::types::lists::ListLocation;
use fred::types::scripts::Script;
use futures::Stream;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};
use std::sync::LazyLock;
use uuid::Uuid;

static INSERT: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local ind = table.remove(ARGV, 1)
        local size = redis.call('llen', KEYS[1])
        assert(tonumber(ind) <= size, 'index: ' .. ind .. ' but current size: ' .. size)
        local tail = redis.call('lrange', KEYS[1], ind, -1)
        redis.call('ltrim', KEYS[1], 0, ind - 1)
        for i = 1, #ARGV, 5000 do
            redis.call('rpush', KEYS[1], unpack(ARGV, i, math.min(i + 4999, #ARGV)))
        end
        if #tail > 0 then
            for i = 1, #tail, 5000 do
                redis.call('rpush', KEYS[1], unpack(tail, i, math.min(i + 4999, #tail)))
            end
        end
        return 1",
    )
});

static SET: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local v = redis.call('lindex', KEYS[1], ARGV[1])
        redis.call('lset', KEYS[1], ARGV[1], ARGV[2])
        return v",
    )
});

static REMOVE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local v = redis.call('lindex', KEYS[1], ARGV[1])
        redis.call('lset', KEYS[1], ARGV[1], ARGV[2])
        redis.call('lrem', KEYS[1], 1, ARGV[2])
        return v",
    )
});

static FAST_REMOVE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "redis.call('lset', KEYS[1], ARGV[1], ARGV[2])
        redis.call('lrem', KEYS[1], 1, ARGV[2])
        return 1",
    )
});

static CONTAINS_ALL: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local items = redis.call('lrange', KEYS[1], 0, -1)
        for i = 1, #items, 1 do
            for j = #ARGV, 1, -1 do
                if items[i] == ARGV[j] then
                    table.remove(ARGV, j)
                end
            end
        end
        return #ARGV == 0 and 1 or 0",
    )
});

static REMOVE_VALUES: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local v = 0
        for i = 1, #ARGV, 1 do
            if redis.call('lrem', KEYS[1], 0, ARGV[i]) > 0 then
                v = 1
            end
        end
        return v",
    )
});

static RETAIN_VALUES: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local changed = 0
        local items = redis.call('lrange', KEYS[1], 0, -1)
        for i = 1, #items, 1 do
            local element = items[i]
            local keep = false
            for j = 1, #ARGV, 1 do
                if ARGV[j] == element then
                    keep = true
                    break
                end
            end
            if keep == false then
                redis.call('lrem', KEYS[1], 0, element)
                changed = 1
            end
        end
        return changed",
    )
});

fn redis_index(index: usize) -> Result<i64> {
    i64::try_from(index).map_err(|_| Error::OutOfRange)
}

fn bounds(range: impl RangeBounds<usize>) -> Option<(i64, i64)> {
    let start = match range.start_bound() {
        Bound::Included(&start) => start,
        Bound::Excluded(&start) => start.checked_add(1)?,
        Bound::Unbounded => 0,
    };
    let stop = match range.end_bound() {
        Bound::Included(&end) => Some(end),
        Bound::Excluded(&end) => Some(end.checked_sub(1)?),
        Bound::Unbounded => None,
    };
    if stop.is_some_and(|stop| start > stop) {
        return None;
    }
    let clamp = |index: usize| i64::try_from(index).unwrap_or(i64::MAX);
    Some((clamp(start), stop.map_or(-1, clamp)))
}

fn out_of_range(error: Error) -> Error {
    match error {
        Error::Redis(message)
            if message.contains("index out of range")
                || message.contains("no such key")
                || message.contains("but current size") =>
        {
            Error::OutOfRange
        }
        other => other,
    }
}

fn marker() -> Bytes {
    Bytes::from(format!("redissun__deleted:{}", Uuid::new_v4()))
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

    /// Appends a value (`RPUSH`, sent once without retry). The value is borrowed: `vec.push("a")`.
    pub async fn push<Q>(&self, v: &Q) -> Result<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.key
            .core
            .redis_no_retry()
            .rpush::<(), _, _>(self.key.redis_key(), self.codec.encode(v)?)
            .await?;
        Ok(())
    }

    /// Appends many values in one round trip (`RPUSH`, sent once without retry).
    pub async fn extend<'a, Q>(&self, values: impl IntoIterator<Item = &'a Q>) -> Result<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync + 'a,
    {
        let encoded = list::encode_all(&self.codec, values)?;
        if encoded.is_empty() {
            return Ok(());
        }
        self.key
            .core
            .redis_no_retry()
            .rpush::<(), _, _>(self.key.redis_key(), encoded)
            .await?;
        Ok(())
    }

    /// Removes and returns the last value (`RPOP`, sent once without retry).
    pub async fn pop(&self) -> Result<Option<V>> {
        let raw: Option<Bytes> = self
            .key
            .core
            .redis_no_retry()
            .rpop(self.key.redis_key(), None)
            .await?;
        self.decode(raw)
    }

    /// Returns the value at the index, or `None` when the index is past the end.
    pub async fn get(&self, index: usize) -> Result<Option<V>> {
        let Ok(index) = redis_index(index) else {
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

    /// Replaces the value at the index and returns the old one.
    /// Returns [`Error::OutOfRange`] when the index is past the end.
    pub async fn set<Q>(&self, index: usize, v: &Q) -> Result<V>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let index = redis_index(index)?;
        let previous: Option<Bytes> = self
            .key
            .core
            .eval(
                &SET,
                vec![self.key.redis_key()],
                vec![Bytes::from(index.to_string()), self.codec.encode(v)?],
            )
            .await
            .map_err(out_of_range)?;
        self.decode(previous)?.ok_or(Error::OutOfRange)
    }

    /// Replaces the value at the index without reading the old one (`LSET`).
    /// Returns [`Error::OutOfRange`] when the index is past the end.
    pub async fn fast_set<Q>(&self, index: usize, v: &Q) -> Result<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let index = redis_index(index)?;
        self.key
            .core
            .redis()
            .lset::<(), _, _>(self.key.redis_key(), index, self.codec.encode(v)?)
            .await
            .map_err(|error| out_of_range(error.into()))
    }

    /// Inserts a value before the index and moves the rest back. It is atomic, costs O(n) and is sent once without retry.
    /// Returns [`Error::OutOfRange`] when the index is past the length.
    pub async fn insert<Q>(&self, index: usize, v: &Q) -> Result<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.insert_all(index, std::iter::once(v)).await
    }

    /// Inserts values, in order, before the index and moves the rest back. It is atomic, costs O(n) and is sent once without retry.
    /// Returns [`Error::OutOfRange`] when the index is past the length.
    pub async fn insert_all<'a, Q>(
        &self,
        index: usize,
        values: impl IntoIterator<Item = &'a Q>,
    ) -> Result<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync + 'a,
    {
        let index = redis_index(index)?;
        let mut encoded = list::encode_all(&self.codec, values)?;
        if encoded.is_empty() {
            return Ok(());
        }
        if index == 0 {
            encoded.reverse();
            self.key
                .core
                .redis_no_retry()
                .lpush::<(), _, _>(self.key.redis_key(), encoded)
                .await?;
            return Ok(());
        }
        let args = std::iter::once(Bytes::from(index.to_string()))
            .chain(encoded)
            .collect();
        self.key
            .core
            .eval_no_retry::<i64>(&INSERT, vec![self.key.redis_key()], args)
            .await
            .map(|_| ())
            .map_err(out_of_range)
    }

    /// Inserts a value before the first value equal to `pivot` (`LINSERT`).
    /// Returns the new length, or `None` when no value equals `pivot`.
    pub async fn insert_before<Q>(&self, pivot: &Q, v: &Q) -> Result<Option<usize>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.linsert(ListLocation::Before, pivot, v).await
    }

    /// Inserts a value after the first value equal to `pivot` (`LINSERT`).
    /// Returns the new length, or `None` when no value equals `pivot`.
    pub async fn insert_after<Q>(&self, pivot: &Q, v: &Q) -> Result<Option<usize>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.linsert(ListLocation::After, pivot, v).await
    }

    async fn linsert<Q>(&self, location: ListLocation, pivot: &Q, v: &Q) -> Result<Option<usize>>
    where
        Q: Serialize + ?Sized + Sync,
    {
        let length: i64 = self
            .key
            .core
            .redis()
            .linsert(
                self.key.redis_key(),
                location,
                self.codec.encode(pivot)?,
                self.codec.encode(v)?,
            )
            .await?;
        Ok(usize::try_from(length).ok().filter(|length| *length > 0))
    }

    /// Removes and returns the value at the index. It is atomic and costs O(n).
    /// Returns [`Error::OutOfRange`] when the index is past the end.
    pub async fn remove(&self, index: usize) -> Result<V> {
        let index = redis_index(index)?;
        let raw: Option<Bytes> = if index == 0 {
            self.key
                .core
                .redis_no_retry()
                .lpop(self.key.redis_key(), None)
                .await?
        } else {
            self.key
                .core
                .eval(
                    &REMOVE,
                    vec![self.key.redis_key()],
                    vec![Bytes::from(index.to_string()), marker()],
                )
                .await
                .map_err(out_of_range)?
        };
        self.decode(raw)?.ok_or(Error::OutOfRange)
    }

    /// Removes the value at the index without returning it. It is atomic and costs O(n).
    /// Returns [`Error::OutOfRange`] when the index is past the end.
    pub async fn fast_remove(&self, index: usize) -> Result<()> {
        let index = redis_index(index)?;
        self.key
            .core
            .eval::<i64>(
                &FAST_REMOVE,
                vec![self.key.redis_key()],
                vec![Bytes::from(index.to_string()), marker()],
            )
            .await
            .map(|_| ())
            .map_err(out_of_range)
    }

    /// Returns the values in the range, like a slice range: `vec.range(1..3)` or, for Redisson's inclusive `range(1, 3)`, `vec.range(1..=3)`.
    pub async fn range(&self, range: impl RangeBounds<usize>) -> Result<std::vec::Vec<V>> {
        let Some((start, stop)) = bounds(range) else {
            return Ok(std::vec::Vec::new());
        };
        let raw: std::vec::Vec<Bytes> = self
            .key
            .core
            .redis()
            .lrange(self.key.redis_key(), start, stop)
            .await?;
        list::decode_all(&self.codec, &raw)
    }

    /// Keeps only the values in the range and drops the rest (`LTRIM`). Redisson's inclusive `trim(1, 3)` is `vec.trim(1..=3)`.
    pub async fn trim(&self, range: impl RangeBounds<usize>) -> Result<()> {
        let Some((start, stop)) = bounds(range) else {
            return self.clear().await;
        };
        self.key
            .core
            .redis()
            .ltrim::<(), _>(self.key.redis_key(), start, stop)
            .await?;
        Ok(())
    }

    /// Returns all values in order (`LRANGE 0 -1`).
    pub async fn read_all(&self) -> Result<std::vec::Vec<V>> {
        list::read_all(&self.key, &self.codec).await
    }

    /// Returns the index of the first equal value.
    pub async fn position<Q>(&self, v: &Q) -> Result<Option<usize>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        list::position(&self.key, &self.codec, v, false).await
    }

    /// Returns the index of the last equal value.
    pub async fn rposition<Q>(&self, v: &Q) -> Result<Option<usize>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        list::position(&self.key, &self.codec, v, true).await
    }

    /// Returns whether an equal value is in the vector.
    pub async fn contains<Q>(&self, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Ok(self.position(v).await?.is_some())
    }

    /// Returns whether every one of the values is in the vector. It is `true` for no values.
    pub async fn contains_all<'a, Q>(&self, values: impl IntoIterator<Item = &'a Q>) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync + 'a,
    {
        let encoded = list::encode_all(&self.codec, values)?;
        if encoded.is_empty() {
            return Ok(true);
        }
        let found: i64 = self
            .key
            .core
            .eval(&CONTAINS_ALL, vec![self.key.redis_key()], encoded)
            .await?;
        Ok(found == 1)
    }

    /// Removes the first equal value; returns whether one was removed.
    pub async fn remove_value<Q>(&self, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Ok(list::remove_matching(&self.key, &self.codec, v, 1).await? > 0)
    }

    /// Removes up to `count` equal values from the front and returns how many were removed.
    pub async fn remove_value_n<Q>(&self, v: &Q, count: usize) -> Result<usize>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if count == 0 {
            return Ok(0);
        }
        let count = i64::try_from(count).unwrap_or(i64::MAX);
        list::remove_matching(&self.key, &self.codec, v, count).await
    }

    /// Removes every equal value and returns how many were removed.
    pub async fn remove_all<Q>(&self, v: &Q) -> Result<usize>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        list::remove_matching(&self.key, &self.codec, v, 0).await
    }

    /// Removes every value equal to one of `values` atomically; returns whether any was removed.
    pub async fn remove_values<'a, Q>(
        &self,
        values: impl IntoIterator<Item = &'a Q>,
    ) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync + 'a,
    {
        let encoded = list::encode_all(&self.codec, values)?;
        if encoded.is_empty() {
            return Ok(false);
        }
        let changed: i64 = self
            .key
            .core
            .eval(&REMOVE_VALUES, vec![self.key.redis_key()], encoded)
            .await?;
        Ok(changed == 1)
    }

    /// Keeps only the values equal to one of `values` atomically; returns whether any was removed.
    /// With no values it deletes the vector.
    pub async fn retain_values<'a, Q>(
        &self,
        values: impl IntoIterator<Item = &'a Q>,
    ) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync + 'a,
    {
        let encoded = list::encode_all(&self.codec, values)?;
        if encoded.is_empty() {
            return self.key.del_all(std::vec::Vec::new()).await;
        }
        let changed: i64 = self
            .key
            .core
            .eval(&RETAIN_VALUES, vec![self.key.redis_key()], encoded)
            .await?;
        Ok(changed == 1)
    }

    /// Number of values.
    pub async fn len(&self) -> Result<usize> {
        list::len(&self.key).await
    }

    /// Returns whether the vector has no values.
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// Deletes every value.
    pub async fn clear(&self) -> Result<()> {
        list::clear(&self.key).await
    }

    /// Streams all values in order, reading the list in pages of 100 with `LRANGE`.
    /// Changes made while the stream runs can make it skip or repeat a value.
    pub fn iter(&self) -> impl Stream<Item = Result<V>> + '_ {
        list::pages(&self.key, &self.codec)
    }
}
