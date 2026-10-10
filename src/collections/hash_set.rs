use crate::codec::Codec;
use crate::error::Result;
use crate::object::{HasKey, Key};
use crate::reply::{self, malformed};
use bytes::Bytes;
use fred::interfaces::{KeysInterface, SetsInterface};
use fred::types::scan::Scanner;
use fred::types::scripts::Script;
use fred::types::Value;
use futures::{stream, Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;
use std::sync::LazyLock;

const SCAN_PAGE: u32 = 100;

static TRY_ADD: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "for i = 1, #ARGV, 1 do
            if redis.call('SISMEMBER', KEYS[1], ARGV[i]) == 1 then
                return 0
            end
        end
        for i = 1, #ARGV, 5000 do
            redis.call('SADD', KEYS[1], unpack(ARGV, i, math.min(i + 4999, #ARGV)))
        end
        return 1",
    )
});

static RETAIN: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local keep = {}
        for i = 1, #ARGV, 1 do
            keep[ARGV[i]] = true
        end
        local changed = 0
        for _, member in ipairs(redis.call('SMEMBERS', KEYS[1])) do
            if not keep[member] then
                redis.call('SREM', KEYS[1], member)
                changed = 1
            end
        end
        return changed",
    )
});

/// A distributed set stored in a Redis set. Values are equal when their encoded bytes are equal.
pub struct HashSet<V, C: Codec> {
    key: Key,
    codec: C,
    _marker: PhantomData<fn() -> V>,
}

impl<V, C: Codec> Clone for HashSet<V, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
            _marker: PhantomData,
        }
    }
}

impl<V, C: Codec> fmt::Debug for HashSet<V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "HashSet")
    }
}

impl<V, C: Codec> HasKey for HashSet<V, C> {
    fn key(&self) -> &Key {
        &self.key
    }
}

impl<V, C: Codec> HashSet<V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self {
            key,
            codec,
            _marker: PhantomData,
        }
    }
}

impl<V, C> HashSet<V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    fn decode_all(&self, raw: Vec<Bytes>) -> Result<Vec<V>> {
        raw.iter().map(|bytes| self.codec.decode(bytes)).collect()
    }

    /// Adds a value; returns whether it was new. The value is borrowed: `set.insert("a")`.
    pub async fn insert<Q>(&self, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let added: i64 = self
            .key
            .core
            .redis()
            .sadd(self.key.redis_key(), self.codec.encode(v)?)
            .await?;
        Ok(added > 0)
    }

    /// Adds many values in one round trip; returns how many were new.
    pub async fn extend<'a, Q>(&self, values: impl IntoIterator<Item = &'a Q>) -> Result<usize>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync + 'a,
    {
        let encoded = self.encode_iter(values)?;
        if encoded.is_empty() {
            return Ok(0);
        }
        let added: usize = self
            .key
            .core
            .redis()
            .sadd(self.key.redis_key(), encoded)
            .await?;
        Ok(added)
    }

    /// Adds all the values only when none of them is in the set yet; returns whether they were added.
    pub async fn try_extend<'a, Q>(&self, values: impl IntoIterator<Item = &'a Q>) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync + 'a,
    {
        let encoded = self.encode_iter(values)?;
        if encoded.is_empty() {
            return Ok(true);
        }
        let added: i64 = self
            .key
            .core
            .eval(&TRY_ADD, vec![self.key.redis_key()], encoded)
            .await?;
        Ok(added == 1)
    }

    fn encode_iter<'a, Q>(&self, values: impl IntoIterator<Item = &'a Q>) -> Result<Vec<Bytes>>
    where
        Q: Serialize + ?Sized + Sync + 'a,
    {
        values.into_iter().map(|v| self.codec.encode(v)).collect()
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
            .srem(self.key.redis_key(), self.codec.encode(v)?)
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
            .srem(
                self.key.redis_key(),
                self.encode_iter(values.iter().copied())?,
            )
            .await?;
        Ok(removed)
    }

    /// Keeps only the given values and removes the rest; returns whether the set changed.
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
                self.encode_iter(values.iter().copied())?,
            )
            .await?;
        Ok(changed == 1)
    }

    /// Returns whether the value is in the set.
    pub async fn contains<Q>(&self, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let found: bool = self
            .key
            .core
            .redis()
            .sismember(self.key.redis_key(), self.codec.encode(v)?)
            .await?;
        Ok(found)
    }

    /// Checks several values in one round trip, in the order given.
    pub async fn contains_many<Q>(&self, values: &[&Q]) -> Result<Vec<bool>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if values.is_empty() {
            return Ok(Vec::new());
        }
        let encoded = values
            .iter()
            .map(|v| self.codec.encode(*v))
            .collect::<Result<Vec<Bytes>>>()?;
        let found: Vec<bool> = self
            .key
            .core
            .redis()
            .smismember(self.key.redis_key(), encoded)
            .await?;
        Ok(found)
    }

    /// Returns whether every given value is in the set. An empty slice gives `true`.
    pub async fn contains_all<Q>(&self, values: &[&Q]) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Ok(self
            .contains_many(values)
            .await?
            .into_iter()
            .all(|found| found))
    }

    /// Number of values.
    pub async fn len(&self) -> Result<usize> {
        let len: usize = self.key.core.redis().scard(self.key.redis_key()).await?;
        Ok(len)
    }

    /// Returns whether the set has no values.
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

    /// Removes and returns a random value.
    pub async fn pop(&self) -> Result<Option<V>> {
        let raw: Option<Bytes> = self
            .key
            .core
            .redis()
            .spop(self.key.redis_key(), None)
            .await?;
        raw.map(|bytes| self.codec.decode(&bytes)).transpose()
    }

    /// Removes and returns up to `count` random values.
    pub async fn pop_many(&self, count: usize) -> Result<Vec<V>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let raw: Vec<Bytes> = self
            .key
            .core
            .redis()
            .spop(self.key.redis_key(), Some(count))
            .await?;
        self.decode_all(raw)
    }

    /// Returns up to `count` distinct random values without removing them.
    pub async fn random_many(&self, count: usize) -> Result<Vec<V>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let raw: Vec<Bytes> = self
            .key
            .core
            .redis()
            .srandmember(self.key.redis_key(), Some(count))
            .await?;
        self.decode_all(raw)
    }

    /// Returns a random value without removing it.
    pub async fn random(&self) -> Result<Option<V>> {
        let raw: Option<Bytes> = self
            .key
            .core
            .redis()
            .srandmember(self.key.redis_key(), None)
            .await?;
        raw.map(|bytes| self.codec.decode(&bytes)).transpose()
    }

    /// Moves a value to another set; returns whether it was present.
    pub async fn move_to<Q>(&self, other: &HashSet<V, C>, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let moved: bool = self
            .key
            .core
            .redis()
            .smove(
                self.key.redis_key(),
                other.key.redis_key(),
                self.codec.encode(v)?,
            )
            .await?;
        Ok(moved)
    }

    fn keys_with(&self, others: &[&HashSet<V, C>]) -> Vec<String> {
        std::iter::once(self.key.redis_key())
            .chain(others.iter().map(|other| other.key.redis_key()))
            .collect()
    }

    /// Returns the values that are in this set or in any of the others.
    /// In Redis Cluster all the sets must live in the same slot.
    pub async fn union(&self, others: &[&HashSet<V, C>]) -> Result<Vec<V>> {
        let raw: Vec<Bytes> = self.key.core.redis().sunion(self.keys_with(others)).await?;
        self.decode_all(raw)
    }

    /// Returns the values that are in this set and in all the others.
    /// In Redis Cluster all the sets must live in the same slot.
    pub async fn intersection(&self, others: &[&HashSet<V, C>]) -> Result<Vec<V>> {
        let raw: Vec<Bytes> = self.key.core.redis().sinter(self.keys_with(others)).await?;
        self.decode_all(raw)
    }

    /// Returns the values that are in this set and in none of the others.
    /// In Redis Cluster all the sets must live in the same slot.
    pub async fn difference(&self, others: &[&HashSet<V, C>]) -> Result<Vec<V>> {
        let raw: Vec<Bytes> = self.key.core.redis().sdiff(self.keys_with(others)).await?;
        self.decode_all(raw)
    }

    /// Number of values in this set and all the others (`SINTERCARD`, Redis 7.0 or newer). Counting stops at `limit` when it is not zero.
    /// In Redis Cluster all the sets must live in the same slot.
    pub async fn intersection_len(&self, others: &[&HashSet<V, C>], limit: usize) -> Result<usize> {
        let keys = self.keys_with(others);
        let mut args = vec![reply::int(keys.len())];
        args.extend(keys.into_iter().map(Value::from));
        if limit > 0 {
            args.push(Value::from("LIMIT"));
            args.push(reply::int(limit));
        }
        let count = self.key.command("SINTERCARD", args, 1).await?;
        reply::number(&count)
            .and_then(|count| usize::try_from(count).ok())
            .ok_or_else(malformed)
    }

    async fn store(&self, command: &'static str, sources: &[&HashSet<V, C>]) -> Result<usize> {
        let args = std::iter::once(self.key.redis_key())
            .chain(sources.iter().map(|source| source.key.redis_key()))
            .map(Value::from)
            .collect();
        let count = self.key.command(command, args, 0).await?;
        reply::number(&count)
            .and_then(|count| usize::try_from(count).ok())
            .ok_or_else(malformed)
    }

    /// Replaces this set with the union of `sources` (`SUNIONSTORE`); returns its new size.
    /// In Redis Cluster all the sets must live in the same slot.
    pub async fn store_union(&self, sources: &[&HashSet<V, C>]) -> Result<usize> {
        self.store("SUNIONSTORE", sources).await
    }

    /// Replaces this set with the intersection of `sources` (`SINTERSTORE`); returns its new size.
    /// In Redis Cluster all the sets must live in the same slot.
    pub async fn store_intersection(&self, sources: &[&HashSet<V, C>]) -> Result<usize> {
        self.store("SINTERSTORE", sources).await
    }

    /// Replaces this set with the values of the first source that are in none of the others (`SDIFFSTORE`); returns its new size.
    /// In Redis Cluster all the sets must live in the same slot.
    pub async fn store_difference(&self, sources: &[&HashSet<V, C>]) -> Result<usize> {
        self.store("SDIFFSTORE", sources).await
    }

    /// Streams all values, reading the set in pages of 100 with `SSCAN`.
    /// A value that was in the set the whole time is returned at least once, and may be returned twice.
    pub fn iter(&self) -> impl Stream<Item = Result<V>> + '_ {
        let pages = Box::pin(self.key.core.redis().sscan(
            self.key.redis_key(),
            "*",
            Some(SCAN_PAGE),
        ));
        pages.flat_map(move |page| {
            let items: Vec<Result<V>> = match page {
                Ok(mut page) => {
                    let results = page.take_results();
                    page.next();
                    results
                        .unwrap_or_default()
                        .into_iter()
                        .map(|value| {
                            let bytes: Bytes = value.convert()?;
                            self.codec.decode(&bytes)
                        })
                        .collect()
                }
                Err(error) => vec![Err(error.into())],
            };
            stream::iter(items)
        })
    }
}
