use crate::client::request::Request;
use crate::codec::Codec;
use crate::collections::list;
use crate::error::Result;
use crate::object::block_seconds;
use crate::object::{HasKey, Key};
use crate::pending::Pending;
use bytes::Bytes;
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

    /// Adds a value at the back (`RPUSH`, sent once without retry). The value is borrowed: `deque.push_back("a")`.
    pub async fn push_back<Q>(&self, v: &Q) -> Result<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.extend_back(std::iter::once(v)).await.map(|_| ())
    }

    /// Adds a value at the front (`LPUSH`, sent once without retry).
    pub async fn push_front<Q>(&self, v: &Q) -> Result<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.extend_front(std::iter::once(v)).await.map(|_| ())
    }

    /// Adds values at the back, in order, and returns the new length (`RPUSH`, sent once without retry).
    pub async fn extend_back<'a, Q>(&self, values: impl IntoIterator<Item = &'a Q>) -> Result<usize>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync + 'a,
    {
        self.push_command(values, false, false).await
    }

    /// Adds values at the front one after another, so the last one ends up first, and returns the new length (`LPUSH`, sent once without retry).
    pub async fn extend_front<'a, Q>(
        &self,
        values: impl IntoIterator<Item = &'a Q>,
    ) -> Result<usize>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync + 'a,
    {
        self.push_command(values, true, false).await
    }

    /// Like [`extend_back`](Self::extend_back), but only when the deque exists (`RPUSHX`). Returns the new length, 0 when it does not exist.
    pub async fn extend_back_if_exists<'a, Q>(
        &self,
        values: impl IntoIterator<Item = &'a Q>,
    ) -> Result<usize>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync + 'a,
    {
        self.push_command(values, false, true).await
    }

    /// Like [`extend_front`](Self::extend_front), but only when the deque exists (`LPUSHX`). Returns the new length, 0 when it does not exist.
    pub async fn extend_front_if_exists<'a, Q>(
        &self,
        values: impl IntoIterator<Item = &'a Q>,
    ) -> Result<usize>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync + 'a,
    {
        self.push_command(values, true, true).await
    }

    async fn push_command<'a, Q>(
        &self,
        values: impl IntoIterator<Item = &'a Q>,
        front: bool,
        if_exists: bool,
    ) -> Result<usize>
    where
        Q: Serialize + ?Sized + Sync + 'a,
    {
        let encoded = list::encode_all(&self.codec, values)?;
        if encoded.is_empty() {
            // The length a push returns comes from the master, like the push's own reply.
            return self.key.query(self.key.key_cmd("LLEN")).await;
        }
        let name = match (front, if_exists) {
            (true, false) => "LPUSH",
            (false, false) => "RPUSH",
            (true, true) => "LPUSHX",
            (false, true) => "RPUSHX",
        };
        let mut cmd = self.key.key_cmd(name);
        cmd.arg(crate::client::request::binary(&encoded));
        let length: usize = self.key.query_once(cmd).await?;
        Ok(length)
    }

    /// Removes and returns the value at the back (`RPOP`, sent once without retry).
    pub async fn pop_back(&self) -> Result<Option<V>> {
        let raw: Option<Bytes> = self
            .key
            .query_once({
                let mut cmd = redis::cmd("RPOP");
                cmd.arg(self.key.redis_key());
                cmd
            })
            .await?;
        self.decode(raw)
    }

    /// Removes and returns the value at the front (`LPOP`, sent once without retry).
    pub async fn pop_front(&self) -> Result<Option<V>> {
        let raw: Option<Bytes> = self
            .key
            .query_once({
                let mut cmd = redis::cmd("LPOP");
                cmd.arg(self.key.redis_key());
                cmd
            })
            .await?;
        self.decode(raw)
    }

    /// Removes and returns up to `limit` values from the front, atomically and sent once without retry.
    pub async fn pop_front_many(&self, limit: usize) -> Result<std::vec::Vec<V>> {
        list::pop_many(&self.key, &self.codec, true, limit).await
    }

    /// Removes and returns up to `limit` values from the back, last first, atomically and sent once without retry.
    pub async fn pop_back_many(&self, limit: usize) -> Result<std::vec::Vec<V>> {
        list::pop_many(&self.key, &self.codec, false, limit).await
    }

    /// Removes every value and returns them in order, atomically.
    pub async fn drain(&self) -> Result<std::vec::Vec<V>> {
        list::drain(&self.key, &self.codec, None).await
    }

    /// Removes up to `max` values from the front and returns them in order, atomically.
    pub async fn drain_up_to(&self, max: usize) -> Result<std::vec::Vec<V>> {
        list::drain(&self.key, &self.codec, Some(max)).await
    }

    /// Moves the back value to the front of `to` and returns it (`RPOPLPUSH`, sent once without retry). `to` may be this deque.
    pub async fn pop_back_push_front<D: Codec>(&self, to: &VecDeque<V, D>) -> Result<Option<V>> {
        let raw: Option<Bytes> = self
            .key
            .query_once({
                let mut cmd = redis::cmd("RPOPLPUSH");
                cmd.arg(self.key.redis_key());
                cmd.arg(to.key.redis_key());
                cmd
            })
            .await?;
        self.decode(raw)
    }

    /// Moves the front value to the back of `to` and returns it (`LMOVE LEFT RIGHT`). `to` may be this deque.
    pub async fn pop_front_push_back<D: Codec>(&self, to: &VecDeque<V, D>) -> Result<Option<V>> {
        let mut cmd = self.key.key_cmd("LMOVE");
        cmd.arg(to.key.redis_key()).arg("LEFT").arg("RIGHT");
        let raw: Option<Bytes> = self.key.query(cmd).await?;
        self.decode(raw)
    }

    /// Removes and returns the front value, waiting for one to arrive (`BLPOP`). Add `.timeout(duration)` to wait at most that long; it then resolves to `None` when the time runs out.
    ///
    /// Like Redisson, the timeout is rounded down to whole seconds, and a timeout under one second waits one second. A zero timeout pops once without waiting.
    /// Use `.timeout` for a time limit and not `tokio::time::timeout`: dropping a pending pop can lose an element that Redis has just handed over.
    pub fn pop_front_wait(&self) -> Pending<'_, V> {
        Pending::new(move |wait| async move {
            Ok(self
                .pop_any(Vec::new(), true, wait)
                .await?
                .map(|(_, value)| value))
        })
    }

    /// Removes and returns the back value, waiting for one to arrive (`BRPOP`). Add `.timeout(duration)` to wait at most that long; the timeout is rounded like [`pop_front_wait`](Self::pop_front_wait).
    pub fn pop_back_wait(&self) -> Pending<'_, V> {
        Pending::new(move |wait| async move {
            Ok(self
                .pop_any(Vec::new(), false, wait)
                .await?
                .map(|(_, value)| value))
        })
    }

    /// Removes and returns the front value of the first non-empty deque among this one and `others`, with that deque's name, waiting for one (`BLPOP`).
    /// In a cluster all names must share a hash slot. The timeout is rounded like [`pop_front_wait`](Self::pop_front_wait).
    pub fn pop_front_wait_any<D: Codec>(
        &self,
        others: &[&VecDeque<V, D>],
    ) -> Pending<'_, (String, V)> {
        let names = others.iter().map(|other| other.key.redis_key()).collect();
        Pending::new(move |wait| self.pop_any(names, true, wait))
    }

    /// Removes and returns the back value of the first non-empty deque among this one and `others`, with that deque's name, waiting for one (`BRPOP`).
    /// In a cluster all names must share a hash slot. The timeout is rounded like [`pop_front_wait`](Self::pop_front_wait).
    pub fn pop_back_wait_any<D: Codec>(
        &self,
        others: &[&VecDeque<V, D>],
    ) -> Pending<'_, (String, V)> {
        let names = others.iter().map(|other| other.key.redis_key()).collect();
        Pending::new(move |wait| self.pop_any(names, false, wait))
    }

    /// Moves the back value to the front of `to` and returns it, waiting for one to arrive (`BRPOPLPUSH`). The timeout is rounded like [`pop_front_wait`](Self::pop_front_wait).
    pub fn pop_back_push_front_wait<D: Codec>(&self, to: &VecDeque<V, D>) -> Pending<'_, V> {
        let to = to.key.redis_key();
        Pending::new(move |wait| self.move_blocking(to, false, wait))
    }

    /// Moves the front value to the back of `to` and returns it, waiting for one to arrive (`BLMOVE LEFT RIGHT`). The timeout is rounded like [`pop_front_wait`](Self::pop_front_wait).
    pub fn pop_front_push_back_wait<D: Codec>(&self, to: &VecDeque<V, D>) -> Pending<'_, V> {
        let to = to.key.redis_key();
        Pending::new(move |wait| self.move_blocking(to, true, wait))
    }

    async fn pop_any(
        &self,
        others: std::vec::Vec<String>,
        front: bool,
        timeout: Option<Duration>,
    ) -> Result<Option<(String, V)>> {
        let names: std::vec::Vec<String> = std::iter::once(self.key.redis_key())
            .chain(others)
            .collect();
        if timeout.is_some_and(|timeout| timeout.is_zero()) {
            for name in names {
                let mut cmd = redis::cmd(if front { "LPOP" } else { "RPOP" });
                cmd.arg(&name);
                // Each pop is routed by the list it takes from, which may be another deque.
                let request = Request::new(cmd).key(name.as_bytes()).once();
                let raw: Option<Bytes> = self.key.core.query(request).await?;
                if let Some(value) = self.decode(raw)? {
                    return Ok(Some((name, value)));
                }
            }
            return Ok(None);
        }
        let seconds = timeout.map_or(0.0, block_seconds);
        let key = self.key.redis_key();
        let mut cmd = redis::cmd(if front { "BLPOP" } else { "BRPOP" });
        cmd.arg(&names).arg(seconds);
        let reply: Option<(String, Bytes)> = self.key.core.blocking(key.as_bytes(), cmd).await?;
        reply
            .map(|(name, bytes)| Ok((name, self.codec.decode(&bytes)?)))
            .transpose()
    }

    async fn move_blocking(
        &self,
        to: String,
        front: bool,
        timeout: Option<Duration>,
    ) -> Result<Option<V>> {
        let from = self.key.redis_key();
        if timeout.is_some_and(|timeout| timeout.is_zero()) {
            let raw: Option<Bytes> = if front {
                self.key
                    .query({
                        let mut cmd = redis::cmd("LMOVE");
                        cmd.arg(from).arg(to).arg("LEFT").arg("RIGHT");
                        cmd
                    })
                    .await?
            } else {
                self.key
                    .query_once({
                        let mut cmd = redis::cmd("RPOPLPUSH");
                        cmd.arg(from);
                        cmd.arg(to);
                        cmd
                    })
                    .await?
            };
            return self.decode(raw);
        }
        let seconds = timeout.map_or(0.0, block_seconds);
        let mut cmd = if front {
            let mut cmd = redis::cmd("BLMOVE");
            cmd.arg(&from).arg(&to).arg("LEFT").arg("RIGHT");
            cmd
        } else {
            let mut cmd = redis::cmd("BRPOPLPUSH");
            cmd.arg(&from).arg(&to);
            cmd
        };
        cmd.arg(seconds);
        let reply: Option<Bytes> = self.key.core.blocking(from.as_bytes(), cmd).await?;
        self.decode(reply)
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
            .read({
                let mut cmd = redis::cmd("LINDEX");
                cmd.arg(self.key.redis_key());
                cmd.arg(index);
                cmd
            })
            .await?;
        self.decode(raw)
    }

    /// Returns whether an equal value is in the deque.
    pub async fn contains<Q>(&self, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Ok(self.position(v).await?.is_some())
    }

    /// Returns the index of the first equal value, counted from the front.
    pub async fn position<Q>(&self, v: &Q) -> Result<Option<usize>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        list::position(&self.key, &self.codec, v, false).await
    }

    /// Removes the equal value nearest the front; returns whether one was removed.
    pub async fn remove_first_occurrence<Q>(&self, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Ok(list::remove_matching(&self.key, &self.codec, v, 1).await? > 0)
    }

    /// Removes the equal value nearest the back; returns whether one was removed.
    pub async fn remove_last_occurrence<Q>(&self, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Ok(list::remove_matching(&self.key, &self.codec, v, -1).await? > 0)
    }

    /// Returns all values from front to back (`LRANGE 0 -1`).
    pub async fn read_all(&self) -> Result<std::vec::Vec<V>> {
        list::read_all(&self.key, &self.codec).await
    }

    /// Number of values.
    pub async fn len(&self) -> Result<usize> {
        list::len(&self.key).await
    }

    /// Returns whether the deque has no values.
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// Deletes every value.
    pub async fn clear(&self) -> Result<()> {
        list::clear(&self.key).await
    }

    /// Streams all values from front to back, reading the list in pages of 100 with `LRANGE`.
    /// Changes made while the stream runs can make it skip or repeat a value.
    pub fn iter(&self) -> impl Stream<Item = Result<V>> + '_ {
        list::pages(&self.key, &self.codec)
    }

    /// Streams all values from back to front, reading the list in pages of 100 with `LRANGE`.
    /// Changes made while the stream runs can make it skip or repeat a value.
    pub fn iter_rev(&self) -> impl Stream<Item = Result<V>> + '_ {
        list::pages_rev(&self.key, &self.codec)
    }
}
