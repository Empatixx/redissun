//! The builders [`HashMapCache`] write methods return: await them, optionally after setting a time limit.

use super::scripts::{
    FAST_PUT_WITH_LIMITS, PUT, PUT_ALL_TTL_IDLE, PUT_IF_ABSENT_WITH_LIMITS, PUT_WITH_LIMITS,
};
use super::{limits, HashMapCache, Name, PUBLISH};
use crate::codec::Codec;
use crate::error::Result;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::time::Duration;

/// A pending [`HashMapCache::insert`]. Await it, optionally after setting [`Insert::ttl`] or [`Insert::max_idle`].
#[must_use = "an insert does nothing until it is awaited"]
pub struct Insert<'a, K, V, C: Codec, Q: ?Sized, W: ?Sized> {
    cache: &'a HashMapCache<K, V, C>,
    k: &'a Q,
    v: &'a W,
    ttl: Option<Duration>,
    max_idle: Option<Duration>,
}

impl<'a, K, V, C: Codec, Q: ?Sized, W: ?Sized> Insert<'a, K, V, C, Q, W> {
    pub(super) fn new(cache: &'a HashMapCache<K, V, C>, k: &'a Q, v: &'a W) -> Self {
        Self {
            cache,
            k,
            v,
            ttl: None,
            max_idle: None,
        }
    }

    /// Makes the entry expire after `ttl`, whether it is read or not. Zero means no time to live.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Makes the entry expire when nobody has read it for `max_idle`. Every read starts the time again. Zero means no idle limit.
    pub fn max_idle(mut self, max_idle: Duration) -> Self {
        self.max_idle = Some(max_idle);
        self
    }
}

impl<'a, K, V, C, Q, W> IntoFuture for Insert<'a, K, V, C, Q, W>
where
    K: Serialize + DeserializeOwned + Send + Sync + Borrow<Q> + 'a,
    V: Serialize + DeserializeOwned + Send + Sync + Borrow<W> + 'a,
    C: Codec,
    Q: Serialize + ?Sized + Sync + 'a,
    W: Serialize + ?Sized + Sync + 'a,
{
    type Output = Result<Option<V>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let cache = self.cache;
            let limits = limits(self.ttl, self.max_idle)?;
            let key = cache.codec.encode(self.k)?;
            let value = cache.codec.encode(self.v)?;
            let previous: Option<Bytes> = if limits.any() {
                cache
                    .key
                    .core
                    .eval(
                        &PUT_WITH_LIMITS,
                        cache.redis_keys(&[
                            Name::Main,
                            Name::Timeout,
                            Name::Idle,
                            Name::Created,
                            Name::Updated,
                            Name::LastAccess,
                            Name::Removed,
                            Name::Options,
                        ]),
                        limits.arguments(vec![key, value, Bytes::from(PUBLISH)]),
                    )
                    .await?
            } else {
                cache
                    .key
                    .core
                    .eval(
                        &PUT,
                        cache.redis_keys(&[
                            Name::Main,
                            Name::Timeout,
                            Name::Idle,
                            Name::Created,
                            Name::Updated,
                            Name::LastAccess,
                            Name::Removed,
                            Name::Options,
                            Name::Lease,
                        ]),
                        vec![limits.now(), key, value, Bytes::from(PUBLISH)],
                    )
                    .await?
            };
            cache.decode_value(previous)
        })
    }
}

/// A pending [`HashMapCache::try_insert`]. Await it, optionally after setting [`TryInsert::ttl`] or [`TryInsert::max_idle`].
#[must_use = "an insert does nothing until it is awaited"]
pub struct TryInsert<'a, K, V, C: Codec, Q: ?Sized, W: ?Sized> {
    cache: &'a HashMapCache<K, V, C>,
    k: &'a Q,
    v: &'a W,
    ttl: Option<Duration>,
    max_idle: Option<Duration>,
}

impl<'a, K, V, C: Codec, Q: ?Sized, W: ?Sized> TryInsert<'a, K, V, C, Q, W> {
    pub(super) fn new(cache: &'a HashMapCache<K, V, C>, k: &'a Q, v: &'a W) -> Self {
        Self {
            cache,
            k,
            v,
            ttl: None,
            max_idle: None,
        }
    }

    /// Makes the entry expire after `ttl`, whether it is read or not. Zero means no time to live.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Makes the entry expire when nobody has read it for `max_idle`. Every read starts the time again. Zero means no idle limit.
    pub fn max_idle(mut self, max_idle: Duration) -> Self {
        self.max_idle = Some(max_idle);
        self
    }
}

impl<'a, K, V, C, Q, W> IntoFuture for TryInsert<'a, K, V, C, Q, W>
where
    K: Serialize + DeserializeOwned + Send + Sync + Borrow<Q> + 'a,
    V: Serialize + DeserializeOwned + Send + Sync + Borrow<W> + 'a,
    C: Codec,
    Q: Serialize + ?Sized + Sync + 'a,
    W: Serialize + ?Sized + Sync + 'a,
{
    type Output = Result<Option<V>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let cache = self.cache;
            let limits = limits(self.ttl, self.max_idle)?;
            let key = cache.codec.encode(self.k)?;
            let value = cache.codec.encode(self.v)?;
            // Always the script with limits, with zeros when there are none: Redisson's script without limits leaves the old deadline of an expired entry in place.
            let live: Option<Bytes> = cache
                .key
                .core
                .eval(
                    &PUT_IF_ABSENT_WITH_LIMITS,
                    cache.redis_keys(&[
                        Name::Main,
                        Name::Timeout,
                        Name::Idle,
                        Name::Created,
                        Name::LastAccess,
                        Name::Removed,
                        Name::Options,
                        Name::Lease,
                    ]),
                    limits.arguments(vec![key, value, Bytes::from(PUBLISH)]),
                )
                .await?;
            cache.decode_value(live)
        })
    }
}

/// A pending [`HashMapCache::set`]. Await it, optionally after setting [`Set::ttl`] or [`Set::max_idle`].
#[must_use = "a set does nothing until it is awaited"]
pub struct Set<'a, K, V, C: Codec, Q: ?Sized, W: ?Sized> {
    cache: &'a HashMapCache<K, V, C>,
    k: &'a Q,
    v: &'a W,
    ttl: Option<Duration>,
    max_idle: Option<Duration>,
}

impl<'a, K, V, C: Codec, Q: ?Sized, W: ?Sized> Set<'a, K, V, C, Q, W> {
    pub(super) fn new(cache: &'a HashMapCache<K, V, C>, k: &'a Q, v: &'a W) -> Self {
        Self {
            cache,
            k,
            v,
            ttl: None,
            max_idle: None,
        }
    }

    /// Makes the entry expire after `ttl`, whether it is read or not. Zero means no time to live.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Makes the entry expire when nobody has read it for `max_idle`. Every read starts the time again. Zero means no idle limit.
    pub fn max_idle(mut self, max_idle: Duration) -> Self {
        self.max_idle = Some(max_idle);
        self
    }
}

impl<'a, K, V, C, Q, W> IntoFuture for Set<'a, K, V, C, Q, W>
where
    K: Serialize + DeserializeOwned + Send + Sync + Borrow<Q> + 'a,
    V: Serialize + DeserializeOwned + Send + Sync + Borrow<W> + 'a,
    C: Codec,
    Q: Serialize + ?Sized + Sync + 'a,
    W: Serialize + ?Sized + Sync + 'a,
{
    type Output = Result<bool>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let cache = self.cache;
            let limits = limits(self.ttl, self.max_idle)?;
            let key = cache.codec.encode(self.k)?;
            let value = cache.codec.encode(self.v)?;
            if limits.any() {
                // An empty lease token: the script then behaves exactly as `PUT_WITH_LIMITS`.
                let inserted: i64 = cache
                    .key
                    .core
                    .eval(
                        &FAST_PUT_WITH_LIMITS,
                        cache.redis_keys(&[
                            Name::Main,
                            Name::Timeout,
                            Name::Idle,
                            Name::Created,
                            Name::Updated,
                            Name::LastAccess,
                            Name::Removed,
                            Name::Options,
                            Name::Lease,
                        ]),
                        limits.arguments(vec![key, value, Bytes::from(PUBLISH), Bytes::new()]),
                    )
                    .await?;
                Ok(inserted == 1)
            } else {
                // The script of `insert`; the previous value comes back but is not decoded.
                let previous: Option<Bytes> = cache
                    .key
                    .core
                    .eval(
                        &PUT,
                        cache.redis_keys(&[
                            Name::Main,
                            Name::Timeout,
                            Name::Idle,
                            Name::Created,
                            Name::Updated,
                            Name::LastAccess,
                            Name::Removed,
                            Name::Options,
                            Name::Lease,
                        ]),
                        vec![limits.now(), key, value, Bytes::from(PUBLISH)],
                    )
                    .await?;
                Ok(previous.is_none())
            }
        })
    }
}

/// A pending [`HashMapCache::extend`]. Await it, optionally after setting [`Extend::batch_size`], [`Extend::ttl`] or [`Extend::max_idle`].
#[must_use = "an extend does nothing until it is awaited"]
pub struct Extend<'m, K, V, C: Codec> {
    cache: &'m HashMapCache<K, V, C>,
    encoded: Result<std::vec::Vec<(Bytes, Bytes)>>,
    batch_size: usize,
    ttl: Option<Duration>,
    max_idle: Option<Duration>,
}

impl<K, V, C: Codec> fmt::Debug for Extend<'_, K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Extend")
            .field("batch_size", &self.batch_size)
            .field("ttl", &self.ttl)
            .field("max_idle", &self.max_idle)
            .finish_non_exhaustive()
    }
}

impl<'m, K, V, C: Codec> Extend<'m, K, V, C> {
    pub(super) fn new(
        cache: &'m HashMapCache<K, V, C>,
        encoded: Result<std::vec::Vec<(Bytes, Bytes)>>,
    ) -> Self {
        Self {
            cache,
            encoded,
            batch_size: 0,
            ttl: None,
            max_idle: None,
        }
    }

    /// Sends one script per `batch_size` entries, in order, Redisson's `putAll(map, batchSize)`. Each batch is atomic, the whole call is not. Zero, the default, sends everything at once.
    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Makes every entry expire after `ttl`, whether it is read or not. Zero means no time to live.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Makes every entry expire when nobody has read it for `max_idle`. Zero means no idle limit.
    pub fn max_idle(mut self, max_idle: Duration) -> Self {
        self.max_idle = Some(max_idle);
        self
    }
}

impl<'m, K, V, C> IntoFuture for Extend<'m, K, V, C>
where
    K: Send + Sync + 'm,
    V: Send + Sync + 'm,
    C: Codec,
{
    type Output = Result<()>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'm>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let encoded = self.encoded?;
            // Checked before anything is sent, so a bad duration fails even with no entries.
            limits(self.ttl, self.max_idle)?;
            let size = if self.batch_size == 0 {
                encoded.len().max(1)
            } else {
                self.batch_size
            };
            for batch in encoded.chunks(size) {
                // A fresh `now` per batch, as each batch is its own `putAll`.
                let limits = limits(self.ttl, self.max_idle)?;
                let mut rest = std::vec::Vec::with_capacity(1 + batch.len() * 2);
                rest.push(Bytes::from(PUBLISH));
                for (k, v) in batch {
                    rest.push(k.clone());
                    rest.push(v.clone());
                }
                let _: redis::Value = self
                    .cache
                    .key
                    .core
                    .eval(
                        &PUT_ALL_TTL_IDLE,
                        self.cache.redis_keys(&[
                            Name::Main,
                            Name::Timeout,
                            Name::Idle,
                            Name::Created,
                            Name::Updated,
                            Name::LastAccess,
                            Name::Removed,
                            Name::Options,
                        ]),
                        limits.arguments(rest),
                    )
                    .await?;
            }
            Ok(())
        })
    }
}
