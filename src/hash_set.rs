use crate::codec::Codec;
use crate::error::Result;
use crate::object::{HasKey, Key};
use bytes::Bytes;
use fred::interfaces::{KeysInterface, SetsInterface};
use fred::types::scan::Scanner;
use futures::{stream, Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;

const SCAN_PAGE: u32 = 100;

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

    /// Adds many values in one round trip.
    pub async fn extend<'a, Q>(&self, values: impl IntoIterator<Item = &'a Q>) -> Result<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync + 'a,
    {
        let encoded = values
            .into_iter()
            .map(|v| self.codec.encode(v))
            .collect::<Result<Vec<Bytes>>>()?;
        if encoded.is_empty() {
            return Ok(());
        }
        self.key
            .core
            .redis()
            .sadd::<(), _, _>(self.key.redis_key(), encoded)
            .await?;
        Ok(())
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
