use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::object::{HasKey, Key};
use crate::pending::Pending;
use bytes::Bytes;
use fred::interfaces::SortedSetsInterface;
use futures::{stream, Stream, TryStreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;
use std::ops::Range;
use std::time::Duration;

const PAGE: i64 = 100;

fn redis_index(index: usize) -> i64 {
    i64::try_from(index).unwrap_or(i64::MAX)
}

fn redis_end(end: usize) -> i64 {
    i64::try_from(end).unwrap_or(i64::MAX) - 1
}

/// A distributed set whose values are ordered by a score, stored in a Redis sorted set. Values with the same score are ordered by their encoded bytes. Values are equal when their encoded bytes are equal.
pub struct SortedSet<V, C: Codec> {
    key: Key,
    codec: C,
    _marker: PhantomData<fn() -> V>,
}

impl<V, C: Codec> Clone for SortedSet<V, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
            _marker: PhantomData,
        }
    }
}

impl<V, C: Codec> fmt::Debug for SortedSet<V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "SortedSet")
    }
}

impl<V, C: Codec> HasKey for SortedSet<V, C> {
    fn key(&self) -> &Key {
        &self.key
    }
}

impl<V, C: Codec> SortedSet<V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self {
            key,
            codec,
            _marker: PhantomData,
        }
    }
}

impl<V, C> SortedSet<V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    fn decode_all(&self, raw: Vec<(Bytes, f64)>) -> Result<Vec<(V, f64)>> {
        raw.iter()
            .map(|(bytes, score)| Ok((self.codec.decode(bytes)?, *score)))
            .collect()
    }

    fn decode_one(&self, mut raw: Vec<(Bytes, f64)>) -> Result<Option<(V, f64)>> {
        Ok(self
            .decode_all(raw.drain(..1.min(raw.len())).collect())?
            .pop())
    }

    /// Adds a value with a score, or changes its score; returns whether the value was new. The value is borrowed: `set.insert("ann", 12.0)`.
    pub async fn insert<Q>(&self, v: &Q, score: f64) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let added: i64 = self
            .key
            .core
            .redis()
            .zadd(
                self.key.redis_key(),
                None,
                None,
                false,
                false,
                (score, self.codec.encode(v)?),
            )
            .await?;
        Ok(added > 0)
    }

    /// Adds `delta` to the score of a value and returns the new score. A missing value starts from zero.
    pub async fn add_score<Q>(&self, v: &Q, delta: f64) -> Result<f64>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let score: f64 = self
            .key
            .core
            .redis()
            .zincrby(self.key.redis_key(), delta, self.codec.encode(v)?)
            .await?;
        Ok(score)
    }

    /// Returns the score of a value.
    pub async fn score<Q>(&self, v: &Q) -> Result<Option<f64>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let score: Option<f64> = self
            .key
            .core
            .redis()
            .zscore(self.key.redis_key(), self.codec.encode(v)?)
            .await?;
        Ok(score)
    }

    /// Removes a value; returns whether it was present.
    pub async fn remove<Q>(&self, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let removed: i64 = self
            .key
            .core
            .redis()
            .zrem(self.key.redis_key(), self.codec.encode(v)?)
            .await?;
        Ok(removed > 0)
    }

    /// Returns whether the value is in the set.
    pub async fn contains<Q>(&self, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Ok(self.score(v).await?.is_some())
    }

    /// Position of a value when sorted from the lowest score; the lowest has rank 0.
    pub async fn rank<Q>(&self, v: &Q) -> Result<Option<usize>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let rank: Option<usize> = self
            .key
            .core
            .redis()
            .zrank(self.key.redis_key(), self.codec.encode(v)?, false)
            .await?;
        Ok(rank)
    }

    /// Position of a value when sorted from the highest score; the highest has rank 0.
    pub async fn rev_rank<Q>(&self, v: &Q) -> Result<Option<usize>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let rank: Option<usize> = self
            .key
            .core
            .redis()
            .zrevrank(self.key.redis_key(), self.codec.encode(v)?, false)
            .await?;
        Ok(rank)
    }

    /// Number of values.
    pub async fn len(&self) -> Result<usize> {
        let length: usize = self.key.core.redis().zcard(self.key.redis_key()).await?;
        Ok(length)
    }

    /// Returns whether the set has no values.
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// Removes every value.
    pub async fn clear(&self) -> Result<()> {
        self.key.del_all(Vec::new()).await?;
        Ok(())
    }

    async fn at(&self, index: i64) -> Result<Option<(V, f64)>> {
        let raw: Vec<(Bytes, f64)> = self
            .key
            .core
            .redis()
            .zrange(self.key.redis_key(), index, index, None, false, None, true)
            .await?;
        self.decode_one(raw)
    }

    /// Returns the value with the lowest score without removing it.
    pub async fn first(&self) -> Result<Option<(V, f64)>> {
        self.at(0).await
    }

    /// Returns the value with the highest score without removing it.
    pub async fn last(&self) -> Result<Option<(V, f64)>> {
        self.at(-1).await
    }

    /// Removes and returns the value with the lowest score.
    pub async fn pop_first(&self) -> Result<Option<(V, f64)>> {
        let raw: Vec<(Bytes, f64)> = self
            .key
            .core
            .redis()
            .zpopmin(self.key.redis_key(), None)
            .await?;
        self.decode_one(raw)
    }

    /// Removes and returns the value with the highest score.
    pub async fn pop_last(&self) -> Result<Option<(V, f64)>> {
        let raw: Vec<(Bytes, f64)> = self
            .key
            .core
            .redis()
            .zpopmax(self.key.redis_key(), None)
            .await?;
        self.decode_one(raw)
    }

    /// Removes and returns the value with the lowest score, waiting for one to arrive (`BZPOPMIN`). Add `.timeout(duration)` to wait at most that long; it then resolves to `None` when the time runs out.
    ///
    /// Use `.timeout` for a time limit and not `tokio::time::timeout`: dropping a pending pop can lose a value that Redis has just handed over.
    pub fn pop_first_wait(&self) -> Pending<'_, (V, f64)> {
        Pending::new(move |wait| self.pop_blocking(true, wait))
    }

    /// Removes and returns the value with the highest score, waiting for one to arrive (`BZPOPMAX`). Add `.timeout(duration)` to wait at most that long.
    pub fn pop_last_wait(&self) -> Pending<'_, (V, f64)> {
        Pending::new(move |wait| self.pop_blocking(false, wait))
    }

    async fn pop_blocking(
        &self,
        first: bool,
        timeout: Option<Duration>,
    ) -> Result<Option<(V, f64)>> {
        let immediate = if first {
            self.pop_first().await?
        } else {
            self.pop_last().await?
        };
        if immediate.is_some() || timeout.is_some_and(|timeout| timeout.is_zero()) {
            return Ok(immediate);
        }
        let seconds = timeout.map_or(0.0, |timeout| timeout.as_secs_f64());
        let connection = self.key.core.blocking_client().await?;
        let reply: std::result::Result<Option<(String, Bytes, f64)>, fred::error::Error> = if first
        {
            connection.bzpopmin(self.key.redis_key(), seconds).await
        } else {
            connection.bzpopmax(self.key.redis_key(), seconds).await
        };
        match reply {
            Ok(reply) => reply
                .map(|(_, bytes, score)| Ok((self.codec.decode(&bytes)?, score)))
                .transpose(),
            Err(error) if *error.kind() == fred::error::ErrorKind::Timeout => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn ranked(&self, range: Range<usize>, rev: bool) -> Result<Vec<(V, f64)>> {
        if range.start >= range.end {
            return Ok(Vec::new());
        }
        let raw: Vec<(Bytes, f64)> = self
            .key
            .core
            .redis()
            .zrange(
                self.key.redis_key(),
                redis_index(range.start),
                redis_end(range.end),
                None,
                rev,
                None,
                true,
            )
            .await?;
        self.decode_all(raw)
    }

    /// Returns the values at these positions, sorted from the lowest score. The end is exclusive, like a slice range.
    pub async fn range(&self, range: Range<usize>) -> Result<Vec<(V, f64)>> {
        self.ranked(range, false).await
    }

    /// Returns the values at these positions, sorted from the highest score. The end is exclusive.
    pub async fn rev_range(&self, range: Range<usize>) -> Result<Vec<(V, f64)>> {
        self.ranked(range, true).await
    }

    /// Returns the values with a score from `min` to `max`, both included, sorted from the lowest score. Use `f64::NEG_INFINITY` and `f64::INFINITY` for open ends.
    pub async fn range_by_score(&self, min: f64, max: f64) -> Result<Vec<(V, f64)>> {
        let raw: Vec<(Bytes, f64)> = self
            .key
            .core
            .redis()
            .zrangebyscore(self.key.redis_key(), min, max, true, None)
            .await?;
        self.decode_all(raw)
    }

    /// Number of values with a score from `min` to `max`, both included.
    pub async fn count_by_score(&self, min: f64, max: f64) -> Result<usize> {
        let count: usize = self
            .key
            .core
            .redis()
            .zcount(self.key.redis_key(), min, max)
            .await?;
        Ok(count)
    }

    /// Removes the values with a score from `min` to `max`, both included; returns how many.
    pub async fn remove_by_score(&self, min: f64, max: f64) -> Result<usize> {
        let removed: usize = self
            .key
            .core
            .redis()
            .zremrangebyscore(self.key.redis_key(), min, max)
            .await?;
        Ok(removed)
    }

    /// Streams every value with its score, from the lowest score, one page at a time. Changes made while the stream runs can make it skip or repeat a value.
    pub fn iter(&self) -> impl Stream<Item = Result<(V, f64)>> + '_ {
        stream::try_unfold(Some(0i64), move |state| async move {
            let Some(offset) = state else {
                return Ok::<_, Error>(None);
            };
            let raw: Vec<(Bytes, f64)> = self
                .key
                .core
                .redis()
                .zrange(
                    self.key.redis_key(),
                    offset,
                    offset + PAGE - 1,
                    None,
                    false,
                    None,
                    true,
                )
                .await?;
            if raw.is_empty() {
                return Ok(None);
            }
            let next = (raw.len() as i64 == PAGE).then_some(offset + PAGE);
            let items = self.decode_all(raw)?;
            Ok(Some((stream::iter(items.into_iter().map(Ok)), next)))
        })
        .try_flatten()
    }
}
