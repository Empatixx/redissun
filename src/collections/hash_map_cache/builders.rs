//! The builders [`HashMapCache`] write methods return: await them, optionally after setting a time limit.

use super::scripts::{
    EXPIRE_ENTRIES, EXPIRE_ENTRIES_IF_NOT_SET, EXPIRE_ENTRY, EXPIRE_ENTRY_IF_NOT_SET,
    MIGRATE_FROM_0_23, PUT_ALL_TTL_IDLE, PUT_IF_ABSENT_WITH_LIMITS, PUT_WITH_LIMITS,
};
use super::{announced, limits, names, HashMapCache, Name, Op, PUBLISH};
use crate::codec::Codec;
use crate::collections::map::entry::{Entry, Target};
use crate::error::{Error, Result};
use crate::object::now_millis;
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
            let entry = [(key.clone(), value.clone())];
            let previous: Option<Bytes> = if limits.any() {
                cache
                    .eval_write(
                        &PUT_WITH_LIMITS,
                        &announced::PUT_WITH_LIMITS,
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
                        Op::Put {
                            entries: &entry,
                            ttl_timeout: limits.ttl_timeout,
                            idle_delta: limits.idle_delta,
                        },
                        false,
                    )
                    .await?
            } else {
                cache.put(&limits, key, value).await?
            };
            cache.written(&entry).await?;
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
            // Sent at most once: a second run would find this call's own value and return it as the existing one.
            let entry = [(key.clone(), value.clone())];
            let live: Option<Bytes> = cache
                .key
                .core
                .eval_once(
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
            if live.is_none() {
                cache.written(&entry).await?;
            }
            cache.decode_value(live)
        })
    }
}

/// A pending [`HashMapCache::set`]. Await it, optionally after setting [`Set::ttl`], [`Set::max_idle`] or [`Set::lease`].
#[must_use = "a set does nothing until it is awaited"]
pub struct Set<'a, K, V, C: Codec, Q: ?Sized, W: ?Sized> {
    cache: &'a HashMapCache<K, V, C>,
    k: &'a Q,
    v: &'a W,
    ttl: Option<Duration>,
    max_idle: Option<Duration>,
    token: Option<&'a str>,
}

impl<'a, K, V, C: Codec, Q: ?Sized, W: ?Sized> Set<'a, K, V, C, Q, W> {
    pub(super) fn new(cache: &'a HashMapCache<K, V, C>, k: &'a Q, v: &'a W) -> Self {
        Self {
            cache,
            k,
            v,
            ttl: None,
            max_idle: None,
            token: None,
        }
    }

    /// Stores the value only while the lease `token` holds, and frees the lease, Redisson's `putWithLease`: the way to finish a lease that [`HashMapCache::lease`] took in another process, which handed over [`LeaseGuard::token`](super::LeaseGuard::token). The result means what [`LeaseGuard::insert`](super::LeaseGuard::insert) returns: `false` when the lease is gone or belongs to someone else (nothing stored), and also when it matched but a live entry was there (stored over it). An empty token is [`Error::Config`](crate::Error::Config).
    #[doc(alias = "putWithLease")]
    pub fn lease(mut self, token: &'a str) -> Self {
        self.token = Some(token);
        self
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
            if let Some(token) = self.token {
                // An empty token would make the script store without any lease check.
                if token.is_empty() {
                    return Err(Error::Config("lease token is empty".into()));
                }
                return cache
                    .fast_put_with_limits(
                        &limits,
                        key,
                        value,
                        Bytes::copy_from_slice(token.as_bytes()),
                    )
                    .await;
            }
            // A write with a lease token above bypasses the writer, as Redisson's `putWithLease` does.
            let entry = [(key.clone(), value.clone())];
            let inserted = if limits.any() {
                // An empty lease token: the script then behaves exactly as `PUT_WITH_LIMITS`.
                cache
                    .fast_put_with_limits(&limits, key, value, Bytes::new())
                    .await?
            } else {
                // The script of `insert`; the previous value comes back but is not decoded.
                cache.put(&limits, key, value).await?.is_none()
            };
            cache.written(&entry).await?;
            Ok(inserted)
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
    K: Serialize + DeserializeOwned + Send + Sync + 'm,
    V: Serialize + DeserializeOwned + Send + Sync + 'm,
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
                self.cache.written(batch).await?;
            }
            Ok(())
        })
    }
}

/// A pending [`HashMapCache::expire_entry`]. Await it, optionally after setting [`ExpireEntry::ttl`], [`ExpireEntry::max_idle`] or [`ExpireEntry::if_not_set`].
#[must_use = "an expire_entry does nothing until it is awaited"]
pub struct ExpireEntry<'a, K, V, C: Codec, Q: ?Sized> {
    cache: &'a HashMapCache<K, V, C>,
    k: &'a Q,
    ttl: Option<Duration>,
    max_idle: Option<Duration>,
    if_not_set: bool,
}

impl<'a, K, V, C: Codec, Q: ?Sized> ExpireEntry<'a, K, V, C, Q> {
    pub(super) fn new(cache: &'a HashMapCache<K, V, C>, k: &'a Q) -> Self {
        Self {
            cache,
            k,
            ttl: None,
            max_idle: None,
            if_not_set: false,
        }
    }

    /// Makes the entry expire after `ttl` from now, whether it is read or not. Zero, like leaving it out, removes the time to live.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Makes the entry expire when nobody has read it for `max_idle`, starting now. Every read starts the time again. Zero, like leaving it out, removes the idle limit.
    pub fn max_idle(mut self, max_idle: Duration) -> Self {
        self.max_idle = Some(max_idle);
        self
    }

    /// Changes the entry only when it has neither a time to live nor an idle limit, Redisson's `expireEntryIfNotSet`.
    #[doc(alias = "expireEntryIfNotSet")]
    pub fn if_not_set(mut self) -> Self {
        self.if_not_set = true;
        self
    }
}

impl<'a, K, V, C, Q> IntoFuture for ExpireEntry<'a, K, V, C, Q>
where
    K: Serialize + DeserializeOwned + Send + Sync + Borrow<Q> + 'a,
    V: Send + Sync + 'a,
    C: Codec,
    Q: Serialize + ?Sized + Sync + 'a,
{
    type Output = Result<bool>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let cache = self.cache;
            let limits = limits(self.ttl, self.max_idle)?;
            let key = cache.codec.encode(self.k)?;
            let script = if self.if_not_set {
                &*EXPIRE_ENTRY_IF_NOT_SET
            } else {
                &*EXPIRE_ENTRY
            };
            let changed: i64 = cache
                .key
                .core
                .eval(
                    script,
                    cache.redis_keys(&[
                        Name::Main,
                        Name::Timeout,
                        Name::Idle,
                        Name::LastAccess,
                        Name::Options,
                    ]),
                    limits.arguments(vec![key]),
                )
                .await?;
            Ok(changed == 1)
        })
    }
}

/// A pending [`HashMapCache::expire_entries`]. Await it, optionally after setting [`ExpireEntries::ttl`], [`ExpireEntries::max_idle`] or [`ExpireEntries::if_not_set`].
#[must_use = "an expire_entries does nothing until it is awaited"]
pub struct ExpireEntries<'a, K, V, C: Codec, Q: ?Sized> {
    cache: &'a HashMapCache<K, V, C>,
    ks: &'a [&'a Q],
    ttl: Option<Duration>,
    max_idle: Option<Duration>,
    if_not_set: bool,
}

impl<'a, K, V, C: Codec, Q: ?Sized> ExpireEntries<'a, K, V, C, Q> {
    pub(super) fn new(cache: &'a HashMapCache<K, V, C>, ks: &'a [&'a Q]) -> Self {
        Self {
            cache,
            ks,
            ttl: None,
            max_idle: None,
            if_not_set: false,
        }
    }

    /// Makes each entry expire after `ttl` from now, whether it is read or not. Zero, like leaving it out, removes the time to live.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Makes each entry expire when nobody has read it for `max_idle`, starting now. Every read starts the time again. Zero, like leaving it out, removes the idle limit.
    pub fn max_idle(mut self, max_idle: Duration) -> Self {
        self.max_idle = Some(max_idle);
        self
    }

    /// Changes only the entries that have neither a time to live nor an idle limit, Redisson's `expireEntriesIfNotSet`.
    #[doc(alias = "expireEntriesIfNotSet")]
    pub fn if_not_set(mut self) -> Self {
        self.if_not_set = true;
        self
    }
}

impl<'a, K, V, C, Q> IntoFuture for ExpireEntries<'a, K, V, C, Q>
where
    K: Serialize + DeserializeOwned + Send + Sync + Borrow<Q> + 'a,
    V: Send + Sync + 'a,
    C: Codec,
    Q: Serialize + ?Sized + Sync + 'a,
{
    type Output = Result<usize>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let cache = self.cache;
            // Checked before anything else, so a bad duration fails even with no keys.
            let limits = limits(self.ttl, self.max_idle)?;
            if self.ks.is_empty() {
                return Ok(0);
            }
            let keys = self
                .ks
                .iter()
                .map(|k| cache.codec.encode(*k))
                .collect::<Result<std::vec::Vec<_>>>()?;
            let script = if self.if_not_set {
                &*EXPIRE_ENTRIES_IF_NOT_SET
            } else {
                &*EXPIRE_ENTRIES
            };
            let changed: usize = cache
                .key
                .core
                .eval(
                    script,
                    cache.redis_keys(&[
                        Name::Main,
                        Name::Timeout,
                        Name::Idle,
                        Name::LastAccess,
                        Name::Options,
                    ]),
                    limits.arguments(keys),
                )
                .await?;
            Ok(changed)
        })
    }
}

/// A pending [`HashMapCache::entry`]. Await it, optionally after setting [`EntryRequest::ttl`], to take the key's lock and read its value.
#[must_use = "an entry request does nothing until it is awaited"]
pub struct EntryRequest<'a, K, V, C: Codec> {
    cache: &'a HashMapCache<K, V, C>,
    key: Result<Bytes>,
    ttl: Option<Duration>,
}

impl<K, V, C: Codec> fmt::Debug for EntryRequest<'_, K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EntryRequest")
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl<'a, K, V, C: Codec> EntryRequest<'a, K, V, C> {
    pub(super) fn new(cache: &'a HashMapCache<K, V, C>, key: Result<Bytes>) -> Self {
        Self {
            cache,
            key,
            ttl: None,
        }
    }

    /// Makes a write through the entry expire after `ttl`, Redisson's `compute(key, ttl, f)`. Zero means no time to live, and the write clears the entry's limits as without this call.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }
}

impl<'a, K, V, C> IntoFuture for EntryRequest<'a, K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync + 'a,
    V: Serialize + DeserializeOwned + Send + Sync + 'a,
    C: Codec,
{
    type Output = Result<Entry<'a, K, V, C>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let key = self.key?;
            // Checked before the lock is taken, so a bad duration fails at once.
            limits(self.ttl, None)?;
            Entry::acquire(Target::Cache(self.cache, self.ttl), key).await
        })
    }
}

/// A pending [`HashMapCache::migrate_from_0_23`]. Await it, optionally after setting [`Migrate::batch_size`].
#[must_use = "a migration does nothing until it is awaited"]
pub struct Migrate<'a, K, V, C: Codec> {
    cache: &'a HashMapCache<K, V, C>,
    batch_size: usize,
}

impl<K, V, C: Codec> fmt::Debug for Migrate<'_, K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Migrate")
            .field("batch_size", &self.batch_size)
            .finish_non_exhaustive()
    }
}

impl<'a, K, V, C: Codec> Migrate<'a, K, V, C> {
    pub(super) fn new(cache: &'a HashMapCache<K, V, C>) -> Self {
        Self {
            cache,
            batch_size: 1000,
        }
    }

    /// Handles at most `batch_size` old entries in this call, 1000 by default. A smaller batch blocks Redis for less time per call and takes more calls. Zero is an [`Error::Config`].
    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

impl<'a, K, V, C> IntoFuture for Migrate<'a, K, V, C>
where
    K: Send + Sync + 'a,
    V: Send + Sync + 'a,
    C: Codec,
{
    type Output = Result<usize>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            if self.batch_size == 0 {
                return Err(Error::Config("batch_size must be at least 1".into()));
            }
            let name = self.cache.key.name();
            let mut keys = vec![names::old_main(name)];
            keys.extend(
                self.cache
                    .redis_keys(&[Name::Main, Name::Timeout, Name::Idle]),
            );
            keys.push(names::old_last_access(name));
            keys.push(names::last_access(name));
            keys.push(names::old_options(name));
            keys.push(names::options(name));
            keys.push(names::latch(name));
            let cap = migration_scan_cap(self.batch_size);
            let mut total = 0;
            // One script call scans at most `cap` fields; keep calling until the batch is full or the scan is complete, so that `0` still means nothing old is left.
            loop {
                let (handled, finished): (i64, i64) = self
                    .cache
                    .key
                    .core
                    .eval(
                        &MIGRATE_FROM_0_23,
                        keys.clone(),
                        vec![
                            Bytes::from(now_millis().to_string()),
                            Bytes::from((self.batch_size - total).to_string()),
                            Bytes::from(cap.to_string()),
                        ],
                    )
                    .await?;
                total += usize::try_from(handled)
                    .map_err(|_| Error::Codec(format!("migration reported {handled} entries")))?;
                if finished == 1 || total >= self.batch_size {
                    return Ok(total);
                }
            }
        })
    }
}

/// How many fields one migration script call scans at most: ten times the batch, so a cache that is mostly in the new layout does not block Redis in one call.
fn migration_scan_cap(batch_size: usize) -> usize {
    batch_size.saturating_mul(10)
}
