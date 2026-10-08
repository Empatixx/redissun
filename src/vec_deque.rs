use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::list;
use crate::object::{HasKey, Key};
use crate::pending::Pending;
use bytes::Bytes;
use fred::interfaces::{KeysInterface, ListInterface};
use futures::Stream;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;
use std::time::Duration;

/// A distributed double-ended queue stored in a Redis list.
pub struct VecDeque<V, C: Codec> {
    key: Key,
    codec: C,
    _marker: PhantomData<fn() -> V>,
}

impl<V, C: Codec> Clone for VecDeque<V, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
            _marker: PhantomData,
        }
    }
}

impl<V, C: Codec> fmt::Debug for VecDeque<V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "VecDeque")
    }
}

impl<V, C: Codec> HasKey for VecDeque<V, C> {
    fn key(&self) -> &Key {
        &self.key
    }
}

impl<V, C: Codec> VecDeque<V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self {
            key,
            codec,
            _marker: PhantomData,
        }
    }
}

impl<V, C> VecDeque<V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    fn decode(&self, raw: Option<Bytes>) -> Result<Option<V>> {
        raw.map(|bytes| self.codec.decode(&bytes)).transpose()
    }

    /// Adds a value at the back. The value is borrowed: `deque.push_back("a")`.
    pub async fn push_back<Q>(&self, v: &Q) -> Result<()>
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

    /// Adds a value at the front.
    pub async fn push_front<Q>(&self, v: &Q) -> Result<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.key
            .core
            .redis()
            .lpush::<(), _, _>(self.key.redis_key(), self.codec.encode(v)?)
            .await?;
        Ok(())
    }

    /// Removes and returns the value at the back.
    pub async fn pop_back(&self) -> Result<Option<V>> {
        let raw: Option<Bytes> = self
            .key
            .core
            .redis()
            .rpop(self.key.redis_key(), None)
            .await?;
        self.decode(raw)
    }

    /// Removes and returns the value at the front.
    pub async fn pop_front(&self) -> Result<Option<V>> {
        let raw: Option<Bytes> = self
            .key
            .core
            .redis()
            .lpop(self.key.redis_key(), None)
            .await?;
        self.decode(raw)
    }

    /// Removes and returns the front value, waiting for one to arrive (`BLPOP`). Add `.timeout(duration)` to wait at most that long; it then resolves to `None` when the time runs out.
    ///
    /// Use `.timeout` for a time limit and not `tokio::time::timeout`: dropping a pending pop can lose an element that Redis has just handed over.
    pub fn pop_front_wait(&self) -> Pending<'_, V> {
        Pending::new(move |wait| self.pop_blocking(true, wait))
    }

    /// Removes and returns the back value, waiting for one to arrive (`BRPOP`). Add `.timeout(duration)` to wait at most that long.
    pub fn pop_back_wait(&self) -> Pending<'_, V> {
        Pending::new(move |wait| self.pop_blocking(false, wait))
    }

    async fn pop_blocking(&self, front: bool, timeout: Option<Duration>) -> Result<Option<V>> {
        let seconds = match timeout {
            Some(timeout) if timeout.is_zero() => {
                return Err(Error::Config("timeout must be positive".into()));
            }
            Some(timeout) => timeout.as_secs_f64(),
            None => 0.0,
        };
        let immediate = if front {
            self.pop_front().await?
        } else {
            self.pop_back().await?
        };
        if immediate.is_some() {
            return Ok(immediate);
        }
        let connection = self.key.core.blocking_client().await?;
        let reply: std::result::Result<Option<(String, Bytes)>, fred::error::Error> = if front {
            connection.blpop(self.key.redis_key(), seconds).await
        } else {
            connection.brpop(self.key.redis_key(), seconds).await
        };
        match reply {
            Ok(reply) => reply
                .map(|(_, bytes)| self.codec.decode(&bytes))
                .transpose(),
            Err(error) if *error.kind() == fred::error::ErrorKind::Timeout => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Returns the value at the front without removing it.
    pub async fn front(&self) -> Result<Option<V>> {
        self.peek(0).await
    }

    /// Returns the value at the back without removing it.
    pub async fn back(&self) -> Result<Option<V>> {
        self.peek(-1).await
    }

    async fn peek(&self, index: i64) -> Result<Option<V>> {
        let raw: Option<Bytes> = self
            .key
            .core
            .redis()
            .lindex(self.key.redis_key(), index)
            .await?;
        self.decode(raw)
    }

    /// Number of values.
    pub async fn len(&self) -> Result<usize> {
        let len: usize = self.key.core.redis().llen(self.key.redis_key()).await?;
        Ok(len)
    }

    /// Returns whether the deque has no values.
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

    /// Streams all values from front to back, reading the list in pages of 100 with `LRANGE`.
    /// Changes made while the stream runs can make it skip or repeat a value.
    pub fn iter(&self) -> impl Stream<Item = Result<V>> + '_ {
        list::pages(&self.key, &self.codec)
    }
}
