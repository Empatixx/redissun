use crate::codec::Codec;
use crate::core::no_retry;
use crate::error::Result;
use crate::object::{block_seconds, HasKey, Key};
use crate::pending::Pending;
use crate::reply::{self, malformed};
use bytes::Bytes;
use fred::interfaces::SortedSetsInterface;
use fred::types::scan::Scanner;
use fred::types::scripts::Script;
use fred::types::sorted_sets::{Ordering, ZCmp};
use fred::types::{SetOptions, Value};
use futures::{stream, Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;
use std::ops::{Bound, Range, RangeBounds};
use std::sync::LazyLock;
use std::time::Duration;

const SCAN_PAGE: u32 = 100;

static POLL: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local v = redis.call('ZRANGE', KEYS[1], ARGV[1], ARGV[2], 'WITHSCORES')
        if #v > 0 then
            redis.call('ZREMRANGEBYRANK', KEYS[1], ARGV[1], ARGV[2])
        end
        return v",
    )
});

static ADD_AND_RANK: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
        return redis.call(ARGV[3], KEYS[1], ARGV[2])",
    )
});

static ADD_SCORE_AND_RANK: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "redis.call('ZINCRBY', KEYS[1], ARGV[1], ARGV[2])
        return redis.call(ARGV[3], KEYS[1], ARGV[2])",
    )
});

static REPLACE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
        if score ~= false then
            redis.call('ZREM', KEYS[1], ARGV[1])
            redis.call('ZADD', KEYS[1], score, ARGV[2])
            return 1
        end
        return 0",
    )
});

static REV_RANKS: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local r = {}
        for i = 1, #ARGV, 1 do
            local rank = redis.call('ZREVRANK', KEYS[1], ARGV[i])
            if rank == false then
                r[#r + 1] = -1
            else
                r[#r + 1] = rank
            end
        end
        return r",
    )
});

static RETAIN: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local keep = {}
        for i = 1, #ARGV, 1 do
            keep[ARGV[i]] = true
        end
        local changed = 0
        for _, member in ipairs(redis.call('ZRANGE', KEYS[1], 0, -1)) do
            if not keep[member] then
                redis.call('ZREM', KEYS[1], member)
                changed = 1
            end
        end
        return changed",
    )
});

fn redis_index(index: usize) -> i64 {
    i64::try_from(index).unwrap_or(i64::MAX)
}

fn redis_end(end: usize) -> i64 {
    i64::try_from(end).unwrap_or(i64::MAX) - 1
}

fn score_text(score: f64) -> String {
    if score.is_infinite() {
        if score > 0.0 { "+inf" } else { "-inf" }.to_string()
    } else {
        score.to_string()
    }
}

fn score_bound(bound: Bound<&f64>, low: bool) -> String {
    match bound {
        Bound::Included(score) => score_text(*score),
        Bound::Excluded(score) => format!("({}", score_text(*score)),
        Bound::Unbounded if low => "-inf".to_string(),
        Bound::Unbounded => "+inf".to_string(),
    }
}

fn score_bounds(range: &impl RangeBounds<f64>) -> (Value, Value) {
    (
        Value::from(score_bound(range.start_bound(), true)),
        Value::from(score_bound(range.end_bound(), false)),
    )
}

fn parse_score(value: &Value) -> Result<f64> {
    match value {
        Value::Double(score) => Ok(*score),
        Value::Integer(score) => Ok(*score as f64),
        other => {
            let text = reply::text(other).ok_or_else(malformed)?;
            match text.as_str() {
                "inf" | "+inf" => Ok(f64::INFINITY),
                "-inf" => Ok(f64::NEG_INFINITY),
                _ => text.parse().map_err(|_| malformed()),
            }
        }
    }
}

fn scored(value: &Value) -> Result<Vec<(Bytes, f64)>> {
    let items = reply::array(value);
    if items.iter().all(|item| matches!(item, Value::Array(_))) {
        return items
            .iter()
            .map(|pair| match reply::array(pair) {
                [member, score] => Ok((
                    reply::bytes(member).ok_or_else(malformed)?,
                    parse_score(score)?,
                )),
                _ => Err(malformed()),
            })
            .collect();
    }
    items
        .chunks(2)
        .map(|pair| match pair {
            [member, score] => Ok((
                reply::bytes(member).ok_or_else(malformed)?,
                parse_score(score)?,
            )),
            _ => Err(malformed()),
        })
        .collect()
}

/// How the scores of a value found in several sets are combined.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Aggregate {
    /// Adds the weighted scores together.
    #[default]
    Sum,
    /// Keeps the lowest weighted score.
    Min,
    /// Keeps the highest weighted score.
    Max,
}

impl Aggregate {
    fn as_str(self) -> &'static str {
        match self {
            Aggregate::Sum => "SUM",
            Aggregate::Min => "MIN",
            Aggregate::Max => "MAX",
        }
    }
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

    fn encode_many<Q>(&self, values: &[&Q]) -> Result<Vec<Bytes>>
    where
        Q: Serialize + ?Sized + Sync,
    {
        values.iter().map(|v| self.codec.encode(*v)).collect()
    }

    async fn zadd<Q>(
        &self,
        entries: Vec<(&Q, f64)>,
        condition: Option<SetOptions>,
        ordering: Option<Ordering>,
        changed: bool,
    ) -> Result<usize>
    where
        Q: Serialize + ?Sized + Sync,
    {
        if entries.is_empty() {
            return Ok(0);
        }
        let encoded = entries
            .into_iter()
            .map(|(v, score)| Ok((score, self.codec.encode(v)?)))
            .collect::<Result<Vec<(f64, Bytes)>>>()?;
        let count: usize = self
            .key
            .core
            .redis()
            .zadd(
                self.key.redis_key(),
                condition,
                ordering,
                changed,
                false,
                encoded,
            )
            .await?;
        Ok(count)
    }

    /// Adds a value with a score, or changes its score; returns whether the value was new. The value is borrowed: `set.insert("ann", 12.0)`.
    pub async fn insert<Q>(&self, v: &Q, score: f64) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Ok(self.zadd(vec![(v, score)], None, None, false).await? > 0)
    }

    /// Adds the value only when it is not in the set yet (`ZADD NX`); returns whether it was added.
    pub async fn try_insert<Q>(&self, v: &Q, score: f64) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Ok(self
            .zadd(vec![(v, score)], Some(SetOptions::NX), None, false)
            .await?
            > 0)
    }

    /// Changes the score only when the value is already in the set (`ZADD XX CH`); returns whether the score changed.
    pub async fn insert_if_exists<Q>(&self, v: &Q, score: f64) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Ok(self
            .zadd(vec![(v, score)], Some(SetOptions::XX), None, true)
            .await?
            > 0)
    }

    /// Adds the value, or raises its score when `score` is greater (`ZADD GT CH`); returns whether anything changed.
    pub async fn insert_if_greater<Q>(&self, v: &Q, score: f64) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Ok(self
            .zadd(vec![(v, score)], None, Some(Ordering::GreaterThan), true)
            .await?
            > 0)
    }

    /// Adds the value, or lowers its score when `score` is less (`ZADD LT CH`); returns whether anything changed.
    pub async fn insert_if_less<Q>(&self, v: &Q, score: f64) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Ok(self
            .zadd(vec![(v, score)], None, Some(Ordering::LessThan), true)
            .await?
            > 0)
    }

    /// Adds many values with scores in one round trip; returns how many were new.
    pub async fn extend<'a, Q>(
        &self,
        entries: impl IntoIterator<Item = (&'a Q, f64)>,
    ) -> Result<usize>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync + 'a,
    {
        self.zadd(entries.into_iter().collect(), None, None, false)
            .await
    }

    /// Adds only the values not in the set yet (`ZADD NX`); returns how many were added.
    pub async fn extend_if_absent<'a, Q>(
        &self,
        entries: impl IntoIterator<Item = (&'a Q, f64)>,
    ) -> Result<usize>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync + 'a,
    {
        self.zadd(
            entries.into_iter().collect(),
            Some(SetOptions::NX),
            None,
            false,
        )
        .await
    }

    /// Changes the scores of the values already in the set (`ZADD XX CH`); returns how many changed.
    pub async fn extend_if_exists<'a, Q>(
        &self,
        entries: impl IntoIterator<Item = (&'a Q, f64)>,
    ) -> Result<usize>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync + 'a,
    {
        self.zadd(
            entries.into_iter().collect(),
            Some(SetOptions::XX),
            None,
            true,
        )
        .await
    }

    /// Adds new values and raises scores that are lower (`ZADD GT CH`); returns how many were added or changed.
    pub async fn extend_if_greater<'a, Q>(
        &self,
        entries: impl IntoIterator<Item = (&'a Q, f64)>,
    ) -> Result<usize>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync + 'a,
    {
        self.zadd(
            entries.into_iter().collect(),
            None,
            Some(Ordering::GreaterThan),
            true,
        )
        .await
    }

    /// Adds new values and lowers scores that are higher (`ZADD LT CH`); returns how many were added or changed.
    pub async fn extend_if_less<'a, Q>(
        &self,
        entries: impl IntoIterator<Item = (&'a Q, f64)>,
    ) -> Result<usize>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync + 'a,
    {
        self.zadd(
            entries.into_iter().collect(),
            None,
            Some(Ordering::LessThan),
            true,
        )
        .await
    }

    async fn ranked_script<Q>(
        &self,
        script: &Script,
        v: &Q,
        score: f64,
        rank: &str,
    ) -> Result<usize>
    where
        Q: Serialize + ?Sized + Sync,
    {
        let rank: i64 = self
            .key
            .core
            .eval(
                script,
                vec![self.key.redis_key()],
                vec![
                    Bytes::from(score_text(score)),
                    self.codec.encode(v)?,
                    Bytes::from(rank.to_string()),
                ],
            )
            .await?;
        usize::try_from(rank).map_err(|_| malformed())
    }

    /// Adds the value or changes its score, then returns its rank from the lowest score.
    pub async fn insert_and_rank<Q>(&self, v: &Q, score: f64) -> Result<usize>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.ranked_script(&ADD_AND_RANK, v, score, "ZRANK").await
    }

    /// Adds the value or changes its score, then returns its rank from the highest score.
    pub async fn insert_and_rev_rank<Q>(&self, v: &Q, score: f64) -> Result<usize>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.ranked_script(&ADD_AND_RANK, v, score, "ZREVRANK")
            .await
    }

    /// Adds `delta` to the score of a value and returns the new score. A missing value starts from zero. Sent once, without retries.
    pub async fn add_score<Q>(&self, v: &Q, delta: f64) -> Result<f64>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let score: f64 = self
            .key
            .core
            .redis_no_retry()
            .zincrby(self.key.redis_key(), delta, self.codec.encode(v)?)
            .await?;
        Ok(score)
    }

    /// Adds `delta` to the score of a value, then returns its rank from the lowest score.
    pub async fn add_score_and_rank<Q>(&self, v: &Q, delta: f64) -> Result<usize>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.ranked_script(&ADD_SCORE_AND_RANK, v, delta, "ZRANK")
            .await
    }

    /// Adds `delta` to the score of a value, then returns its rank from the highest score.
    pub async fn add_score_and_rev_rank<Q>(&self, v: &Q, delta: f64) -> Result<usize>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.ranked_script(&ADD_SCORE_AND_RANK, v, delta, "ZREVRANK")
            .await
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

    /// Returns the scores of several values, in the order given.
    pub async fn scores<Q>(&self, values: &[&Q]) -> Result<Vec<Option<f64>>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if values.is_empty() {
            return Ok(Vec::new());
        }
        let reply: Value = self
            .key
            .core
            .redis()
            .zmscore(self.key.redis_key(), self.encode_many(values)?)
            .await?;
        let items = match reply {
            Value::Array(items) => items,
            other => vec![other],
        };
        items
            .iter()
            .map(|item| match item {
                Value::Null => Ok(None),
                other => parse_score(other).map(Some),
            })
            .collect()
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

    /// Removes several values; returns how many were present.
    pub async fn remove_many<Q>(&self, values: &[&Q]) -> Result<usize>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if values.is_empty() {
            return Ok(0);
        }
        let removed: usize = self
            .key
            .core
            .redis()
            .zrem(self.key.redis_key(), self.encode_many(values)?)
            .await?;
        Ok(removed)
    }

    /// Keeps only the given values, with their scores, and removes the rest; returns whether the set changed.
    pub async fn retain<Q>(&self, values: &[&Q]) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let changed: i64 = self
            .key
            .core
            .eval(
                &RETAIN,
                vec![self.key.redis_key()],
                self.encode_many(values)?,
            )
            .await?;
        Ok(changed == 1)
    }

    /// Replaces `old` with `new`, keeping its score; returns whether `old` was present.
    pub async fn replace<Q>(&self, old: &Q, new: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let replaced: i64 = self
            .key
            .core
            .eval(
                &REPLACE,
                vec![self.key.redis_key()],
                vec![self.codec.encode(old)?, self.codec.encode(new)?],
            )
            .await?;
        Ok(replaced == 1)
    }

    /// Returns whether the value is in the set.
    pub async fn contains<Q>(&self, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Ok(self.score(v).await?.is_some())
    }

    /// Returns whether every given value is in the set. An empty slice gives `true`.
    pub async fn contains_all<Q>(&self, values: &[&Q]) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Ok(self.scores(values).await?.iter().all(Option::is_some))
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

    /// Rank from the lowest score together with the score of a value (`ZRANK WITHSCORE`, Redis 7.2 or newer).
    pub async fn rank_with_score<Q>(&self, v: &Q) -> Result<Option<(usize, f64)>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let reply: Value = self
            .key
            .core
            .redis()
            .zrank(self.key.redis_key(), self.codec.encode(v)?, true)
            .await?;
        match reply {
            Value::Null => Ok(None),
            Value::Array(items) => match items.as_slice() {
                [rank, score] => {
                    let rank = reply::number(rank).ok_or_else(malformed)?;
                    Ok(Some((
                        usize::try_from(rank).map_err(|_| malformed())?,
                        parse_score(score)?,
                    )))
                }
                _ => Err(malformed()),
            },
            _ => Err(malformed()),
        }
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

    /// Ranks from the highest score of several values, in the order given.
    pub async fn rev_rank_many<Q>(&self, values: &[&Q]) -> Result<Vec<Option<usize>>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if values.is_empty() {
            return Ok(Vec::new());
        }
        let ranks: Vec<i64> = self
            .key
            .core
            .eval(
                &REV_RANKS,
                vec![self.key.redis_key()],
                self.encode_many(values)?,
            )
            .await?;
        Ok(ranks
            .into_iter()
            .map(|rank| usize::try_from(rank).ok())
            .collect())
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

    /// Returns the lowest score.
    pub async fn first_score(&self) -> Result<Option<f64>> {
        Ok(self.first().await?.map(|(_, score)| score))
    }

    /// Returns the highest score.
    pub async fn last_score(&self) -> Result<Option<f64>> {
        Ok(self.last().await?.map(|(_, score)| score))
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

    async fn poll(&self, from: i64, to: i64) -> Result<Vec<(V, f64)>> {
        let raw: Vec<Bytes> = self
            .key
            .core
            .eval(
                &POLL,
                vec![self.key.redis_key()],
                vec![Bytes::from(from.to_string()), Bytes::from(to.to_string())],
            )
            .await?;
        let values: Vec<Value> = raw.into_iter().map(Value::Bytes).collect();
        self.decode_all(scored(&Value::Array(values))?)
    }

    /// Removes and returns up to `count` values with the lowest scores, from the lowest score.
    pub async fn pop_first_many(&self, count: usize) -> Result<Vec<(V, f64)>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        self.poll(0, redis_index(count) - 1).await
    }

    /// Removes and returns up to `count` values with the highest scores, from the lowest score.
    pub async fn pop_last_many(&self, count: usize) -> Result<Vec<(V, f64)>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        self.poll(-redis_index(count), -1).await
    }

    /// Removes and returns the value with the lowest score, waiting for one to arrive (`BZPOPMIN`). Add `.timeout(duration)` to wait at most that long; it then resolves to `None` when the time runs out.
    ///
    /// Like Redisson, the timeout is rounded down to whole seconds, with at least one second. A zero timeout pops once without waiting.
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
        if timeout.is_some_and(|timeout| timeout.is_zero()) {
            return if first {
                self.pop_first().await
            } else {
                self.pop_last().await
            };
        }
        let seconds = timeout.map_or(0.0, block_seconds);
        let connection = self.key.core.blocking_client().await?;
        let once = no_retry(&*connection);
        let reply: std::result::Result<Option<(String, Bytes, f64)>, fred::error::Error> = if first
        {
            once.bzpopmin(self.key.redis_key(), seconds).await
        } else {
            once.bzpopmax(self.key.redis_key(), seconds).await
        };
        match reply {
            Ok(reply) => reply
                .map(|(_, bytes, score)| Ok((self.codec.decode(&bytes)?, score)))
                .transpose(),
            Err(error) if *error.kind() == fred::error::ErrorKind::Timeout => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Removes and returns up to `count` values with the lowest scores, waiting until there is at least one (`BZMPOP`, Redis 7.0 or newer). Add `.timeout(duration)` to wait at most that long.
    pub fn pop_first_many_wait(&self, count: usize) -> Pending<'_, Vec<(V, f64)>> {
        Pending::new(move |wait| self.pop_many_blocking(ZCmp::Min, count, wait))
    }

    /// Removes and returns up to `count` values with the highest scores, highest first, waiting until there is at least one (`BZMPOP`). Add `.timeout(duration)` to wait at most that long.
    pub fn pop_last_many_wait(&self, count: usize) -> Pending<'_, Vec<(V, f64)>> {
        Pending::new(move |wait| self.pop_many_blocking(ZCmp::Max, count, wait))
    }

    async fn pop_many_blocking(
        &self,
        side: ZCmp,
        count: usize,
        timeout: Option<Duration>,
    ) -> Result<Option<Vec<(V, f64)>>> {
        let count = redis_index(count.max(1));
        let reply: std::result::Result<Value, fred::error::Error> =
            if timeout.is_some_and(|timeout| timeout.is_zero()) {
                self.key
                    .core
                    .redis()
                    .zmpop(self.key.redis_key(), side, Some(count))
                    .await
            } else {
                let seconds = timeout.map_or(0.0, block_seconds);
                let connection = self.key.core.blocking_client().await?;
                no_retry(&*connection)
                    .bzmpop(seconds, self.key.redis_key(), side, Some(count))
                    .await
            };
        match reply {
            Ok(Value::Null) => Ok(None),
            Ok(Value::Array(items)) => match items.as_slice() {
                [_, entries] => Ok(Some(self.decode_all(scored(entries)?)?)),
                _ => Err(malformed()),
            },
            Ok(_) => Err(malformed()),
            Err(error) if *error.kind() == fred::error::ErrorKind::Timeout => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Returns a random value without removing it.
    pub async fn random(&self) -> Result<Option<V>> {
        let raw: Option<Bytes> = self
            .key
            .core
            .redis()
            .zrandmember(self.key.redis_key(), None)
            .await?;
        raw.map(|bytes| self.codec.decode(&bytes)).transpose()
    }

    /// Returns up to `count` distinct random values without removing them.
    pub async fn random_many(&self, count: usize) -> Result<Vec<V>> {
        let raw: Vec<Bytes> = self
            .key
            .core
            .redis()
            .zrandmember(self.key.redis_key(), Some((redis_index(count), false)))
            .await?;
        raw.iter().map(|bytes| self.codec.decode(bytes)).collect()
    }

    /// Returns up to `count` distinct random values with their scores without removing them.
    pub async fn random_entries(&self, count: usize) -> Result<Vec<(V, f64)>> {
        let reply: Value = self
            .key
            .core
            .redis()
            .zrandmember(self.key.redis_key(), Some((redis_index(count), true)))
            .await?;
        self.decode_all(scored(&reply)?)
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

    /// Removes the values at these positions, counted from the lowest score; returns how many. The end is exclusive.
    pub async fn remove_range(&self, range: Range<usize>) -> Result<usize> {
        if range.start >= range.end {
            return Ok(0);
        }
        let removed: usize = self
            .key
            .core
            .redis()
            .zremrangebyrank(
                self.key.redis_key(),
                redis_index(range.start),
                redis_end(range.end),
            )
            .await?;
        Ok(removed)
    }

    async fn by_score(
        &self,
        range: &impl RangeBounds<f64>,
        rev: bool,
        limit: Option<(usize, usize)>,
    ) -> Result<Vec<(V, f64)>> {
        let (min, max) = score_bounds(range);
        let mut args = vec![Value::from(self.key.redis_key())];
        if rev {
            args.extend([max, min, Value::from("BYSCORE"), Value::from("REV")]);
        } else {
            args.extend([min, max, Value::from("BYSCORE")]);
        }
        if let Some((offset, count)) = limit {
            args.extend([Value::from("LIMIT"), reply::int(offset), reply::int(count)]);
        }
        args.push(Value::from("WITHSCORES"));
        let reply = self.key.command("ZRANGE", args, 0).await?;
        self.decode_all(scored(&reply)?)
    }

    /// Returns the values with a score in the range, sorted from the lowest score.
    /// Any range works: `2.0..=3.0`, `1.0..4.0`, `..`, or `(Bound::Excluded(1.0), Bound::Included(3.0))`.
    pub async fn range_by_score(&self, range: impl RangeBounds<f64>) -> Result<Vec<(V, f64)>> {
        self.by_score(&range, false, None).await
    }

    /// Like [`range_by_score`](Self::range_by_score), but skips `offset` values and returns at most `count`.
    pub async fn range_by_score_limit(
        &self,
        range: impl RangeBounds<f64>,
        offset: usize,
        count: usize,
    ) -> Result<Vec<(V, f64)>> {
        self.by_score(&range, false, Some((offset, count))).await
    }

    /// Returns the values with a score in the range, sorted from the highest score.
    pub async fn rev_range_by_score(&self, range: impl RangeBounds<f64>) -> Result<Vec<(V, f64)>> {
        self.by_score(&range, true, None).await
    }

    /// Like [`rev_range_by_score`](Self::rev_range_by_score), but skips `offset` values and returns at most `count`.
    pub async fn rev_range_by_score_limit(
        &self,
        range: impl RangeBounds<f64>,
        offset: usize,
        count: usize,
    ) -> Result<Vec<(V, f64)>> {
        self.by_score(&range, true, Some((offset, count))).await
    }

    /// Number of values with a score in the range.
    pub async fn count_by_score(&self, range: impl RangeBounds<f64>) -> Result<usize> {
        let (min, max) = score_bounds(&range);
        let reply = self
            .key
            .command(
                "ZCOUNT",
                vec![Value::from(self.key.redis_key()), min, max],
                0,
            )
            .await?;
        reply::number(&reply)
            .and_then(|count| usize::try_from(count).ok())
            .ok_or_else(malformed)
    }

    /// Removes the values with a score in the range; returns how many.
    pub async fn remove_by_score(&self, range: impl RangeBounds<f64>) -> Result<usize> {
        let (min, max) = score_bounds(&range);
        let reply = self
            .key
            .command(
                "ZREMRANGEBYSCORE",
                vec![Value::from(self.key.redis_key()), min, max],
                0,
            )
            .await?;
        reply::number(&reply)
            .and_then(|count| usize::try_from(count).ok())
            .ok_or_else(malformed)
    }

    fn keys_with(&self, others: &[&SortedSet<V, C>]) -> Vec<Value> {
        std::iter::once(self.key.redis_key())
            .chain(others.iter().map(|other| other.key.redis_key()))
            .map(Value::from)
            .collect()
    }

    fn combine_args(keys: Vec<Value>, weights: &[f64], aggregate: Aggregate) -> Vec<Value> {
        let mut args = vec![reply::int(keys.len())];
        args.extend(keys);
        if !weights.is_empty() {
            args.push(Value::from("WEIGHTS"));
            args.extend(
                weights
                    .iter()
                    .map(|weight| Value::from(score_text(*weight))),
            );
        }
        args.push(Value::from("AGGREGATE"));
        args.push(Value::from(aggregate.as_str()));
        args
    }

    async fn read_combined(
        &self,
        command: &'static str,
        args: Vec<Value>,
    ) -> Result<Vec<(V, f64)>> {
        let mut args = args;
        args.push(Value::from("WITHSCORES"));
        let reply = self.key.command(command, args, 1).await?;
        self.decode_all(scored(&reply)?)
    }

    /// Returns the values in this set or any of the others, with their scores summed (`ZUNION`).
    /// In Redis Cluster all the sets must live in the same slot.
    pub async fn union(&self, others: &[&SortedSet<V, C>]) -> Result<Vec<(V, f64)>> {
        self.union_with(others, &[], Aggregate::Sum).await
    }

    /// Like [`union`](Self::union), with a weight per set (this one first; empty for all 1) and an [`Aggregate`].
    pub async fn union_with(
        &self,
        others: &[&SortedSet<V, C>],
        weights: &[f64],
        aggregate: Aggregate,
    ) -> Result<Vec<(V, f64)>> {
        let args = Self::combine_args(self.keys_with(others), weights, aggregate);
        self.read_combined("ZUNION", args).await
    }

    /// Returns the values in this set and all the others, with their scores summed (`ZINTER`).
    /// In Redis Cluster all the sets must live in the same slot.
    pub async fn intersection(&self, others: &[&SortedSet<V, C>]) -> Result<Vec<(V, f64)>> {
        self.intersection_with(others, &[], Aggregate::Sum).await
    }

    /// Like [`intersection`](Self::intersection), with a weight per set (this one first; empty for all 1) and an [`Aggregate`].
    pub async fn intersection_with(
        &self,
        others: &[&SortedSet<V, C>],
        weights: &[f64],
        aggregate: Aggregate,
    ) -> Result<Vec<(V, f64)>> {
        let args = Self::combine_args(self.keys_with(others), weights, aggregate);
        self.read_combined("ZINTER", args).await
    }

    /// Returns the values in this set and in none of the others, with their scores (`ZDIFF`).
    /// In Redis Cluster all the sets must live in the same slot.
    pub async fn difference(&self, others: &[&SortedSet<V, C>]) -> Result<Vec<(V, f64)>> {
        let keys = self.keys_with(others);
        let mut args = vec![reply::int(keys.len())];
        args.extend(keys);
        self.read_combined("ZDIFF", args).await
    }

    async fn store(
        &self,
        command: &'static str,
        sources: &[&SortedSet<V, C>],
        weights: Option<(&[f64], Aggregate)>,
    ) -> Result<usize> {
        let keys: Vec<Value> = sources
            .iter()
            .map(|source| Value::from(source.key.redis_key()))
            .collect();
        let mut args = vec![Value::from(self.key.redis_key())];
        match weights {
            Some((weights, aggregate)) => args.extend(Self::combine_args(keys, weights, aggregate)),
            None => {
                args.push(reply::int(keys.len()));
                args.extend(keys);
            }
        }
        let reply = self.key.command(command, args, 0).await?;
        reply::number(&reply)
            .and_then(|count| usize::try_from(count).ok())
            .ok_or_else(malformed)
    }

    /// Replaces this set with the union of `sources`, scores summed (`ZUNIONSTORE`); returns its new size.
    pub async fn store_union(&self, sources: &[&SortedSet<V, C>]) -> Result<usize> {
        self.store("ZUNIONSTORE", sources, None).await
    }

    /// Like [`store_union`](Self::store_union), with a weight per source (empty for all 1) and an [`Aggregate`].
    pub async fn store_union_with(
        &self,
        sources: &[&SortedSet<V, C>],
        weights: &[f64],
        aggregate: Aggregate,
    ) -> Result<usize> {
        self.store("ZUNIONSTORE", sources, Some((weights, aggregate)))
            .await
    }

    /// Replaces this set with the intersection of `sources`, scores summed (`ZINTERSTORE`); returns its new size.
    pub async fn store_intersection(&self, sources: &[&SortedSet<V, C>]) -> Result<usize> {
        self.store("ZINTERSTORE", sources, None).await
    }

    /// Like [`store_intersection`](Self::store_intersection), with a weight per source (empty for all 1) and an [`Aggregate`].
    pub async fn store_intersection_with(
        &self,
        sources: &[&SortedSet<V, C>],
        weights: &[f64],
        aggregate: Aggregate,
    ) -> Result<usize> {
        self.store("ZINTERSTORE", sources, Some((weights, aggregate)))
            .await
    }

    /// Replaces this set with the values of the first source that are in none of the others (`ZDIFFSTORE`); returns its new size.
    pub async fn store_difference(&self, sources: &[&SortedSet<V, C>]) -> Result<usize> {
        self.store("ZDIFFSTORE", sources, None).await
    }

    /// Streams every value with its score, reading the set in pages with `ZSCAN` like Redisson's iterator.
    /// The order is not guaranteed. A value that was in the set the whole time is returned at least once, and may be returned twice.
    pub fn iter(&self) -> impl Stream<Item = Result<(V, f64)>> + '_ {
        let pages = Box::pin(self.key.core.redis().zscan(
            self.key.redis_key(),
            "*",
            Some(SCAN_PAGE),
        ));
        pages.flat_map(move |page| {
            let items: Vec<Result<(V, f64)>> = match page {
                Ok(mut page) => {
                    let results = page.take_results();
                    page.next();
                    results
                        .unwrap_or_default()
                        .into_iter()
                        .map(|(value, score)| {
                            let bytes: Bytes = value.convert()?;
                            Ok((self.codec.decode(&bytes)?, score))
                        })
                        .collect()
                }
                Err(error) => vec![Err(error.into())],
            };
            stream::iter(items)
        })
    }
}
