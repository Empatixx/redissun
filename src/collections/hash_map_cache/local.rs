//! The local cache of a [`HashMapCache`] built for a [`LocalCachedMapCache`](crate::LocalCachedMapCache): the announced write path and the cached reads.

use super::announced::{GET_ALL_EXPIRY, GET_EXPIRY};
use super::scripts::{CONTAINS_KEY, GET_ALL_WITH_TTL_ONLY, SCAN_KEYS};
use super::{now, HashMapCache, Name};
use crate::codec::Codec;
use crate::collections::local_cache::local::ReadGuard;
use crate::collections::local_cache::messages::{
    expiring_update, log_arguments, message, now_millis, INVALIDATE,
};
use crate::collections::local_cache::store::{copy_lifetime, Lifetime};
use crate::collections::local_cache::{LocalCache, SyncStrategy};
use crate::collections::map::entry::Target;
use crate::collections::map::load::load_value;
use crate::error::Result;
use crate::lua::Lua;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;

/// What a write changes, which decides what it announces and what it leaves in the local cache.
pub(crate) enum Op<'a> {
    /// Unconditional writes of these entries with these limits (`0` for none): cached and announced.
    Put {
        entries: &'a [(Bytes, Bytes)],
        ttl_timeout: i64,
        idle_delta: i64,
    },
    /// A write when no live entry exists, which replies nil when it stored.
    TryPut {
        field: &'a Bytes,
        value: &'a Bytes,
        ttl_timeout: i64,
        idle_delta: i64,
    },
    /// A change of a live entry, which replies with something when there was one.
    IfSome(&'a Bytes),
    /// A conditional change, which replies `1` when it happened.
    IfOne(&'a Bytes),
    /// A change of several keys, which replies `1` per key it changed.
    PerKey(&'a [Bytes]),
    /// A change whose result the client does not know: always announced as an invalidation.
    Touch(&'a [Bytes]),
}

impl Op<'_> {
    /// The wrapper's `when` code.
    fn when(&self) -> &'static str {
        match self {
            Op::Put { .. } | Op::Touch(_) => "a",
            Op::TryPut { .. } => "n",
            Op::IfSome(_) => "s",
            Op::IfOne(_) => "1",
            Op::PerKey(_) => "k",
        }
    }

    /// The keys the write may change.
    fn keys(&self) -> std::vec::Vec<&Bytes> {
        match self {
            Op::Put { entries, .. } => entries.iter().map(|(k, _)| k).collect(),
            Op::TryPut { field, .. } => vec![*field],
            Op::IfSome(field) | Op::IfOne(field) => vec![*field],
            Op::PerKey(fields) | Op::Touch(fields) => fields.iter().collect(),
        }
    }

    /// One `(message, updates log entry)` per key: `E` for a value with known limits under [`SyncStrategy::Update`], else `I`.
    fn notices(&self, local: &LocalCache) -> std::vec::Vec<(Bytes, Bytes)> {
        let id = &local.local.id;
        let update = local.sync == SyncStrategy::Update;
        let notice = |field: &Bytes, value: Option<(&Bytes, i64, i64)>| {
            let [_, entry, _] = log_arguments(field);
            let message = match value {
                Some((value, ttl_timeout, idle_delta)) if update => {
                    expiring_update(id, ttl_timeout, idle_delta, field, value)
                }
                _ => message(id, INVALIDATE, &[field]),
            };
            (message, entry)
        };
        match self {
            Op::Put {
                entries,
                ttl_timeout,
                idle_delta,
            } => entries
                .iter()
                .map(|(k, v)| notice(k, Some((v, *ttl_timeout, *idle_delta))))
                .collect(),
            Op::TryPut {
                field,
                value,
                ttl_timeout,
                idle_delta,
            } => vec![notice(field, Some((value, *ttl_timeout, *idle_delta)))],
            _ => self.keys().into_iter().map(|k| notice(k, None)).collect(),
        }
    }
}

impl<K, V, C: Codec> HashMapCache<K, V, C> {
    /// Runs a write: `plain` without a local cache, exactly as before; else `announced`, its twin, after forgetting the keys `op` changes, and then caches what `op` stored. `once` sends the script at most once. A failed script caches nothing.
    pub(crate) async fn eval_write<R: redis::FromRedisValue>(
        &self,
        plain: &'static Lua,
        announced: &'static Lua,
        mut keys: std::vec::Vec<String>,
        mut args: std::vec::Vec<Bytes>,
        op: Op<'_>,
        once: bool,
    ) -> Result<R> {
        let core = &self.key.core;
        let Some(local) = &self.local else {
            return if once {
                core.eval_once(plain, keys, args).await
            } else {
                core.eval(plain, keys, args).await
            };
        };
        op.keys()
            .into_iter()
            .for_each(|field| local.local.forget(field));
        let [now, _, keep] = log_arguments(&Bytes::new());
        let notices = op.notices(local);
        keys.push(local.updates_log.clone());
        let start = args.len();
        args.extend([
            Bytes::from(local.channel.clone()),
            local.mode(),
            Bytes::from_static(op.when().as_bytes()),
            now,
            keep,
        ]);
        for (notice, entry) in notices {
            args.push(notice);
            args.push(entry);
        }
        let count = args.len() - start;
        args.push(Bytes::from(count.to_string()));
        let reply: redis::Value = if once {
            core.eval_once(announced, keys, args).await?
        } else {
            core.eval(announced, keys, args).await?
        };
        match op {
            Op::Put {
                entries,
                ttl_timeout,
                idle_delta,
            } => {
                if let Some(lifetime) =
                    copy_lifetime(ttl_timeout, idle_delta, now_millis()).lifetime()
                {
                    for (k, v) in entries {
                        local.local.write(k.clone(), Some(v.clone()), lifetime);
                    }
                }
            }
            Op::TryPut {
                field,
                value,
                ttl_timeout,
                idle_delta,
            } if reply == redis::Value::Nil => {
                if let Some(lifetime) =
                    copy_lifetime(ttl_timeout, idle_delta, now_millis()).lifetime()
                {
                    local
                        .local
                        .write(field.clone(), Some(value.clone()), lifetime);
                }
            }
            _ => {}
        }
        Ok(redis::from_redis_value(reply)?)
    }
}

impl<K, V, C> HashMapCache<K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    /// [`get`](HashMapCache::get) with a local cache: a local hit answers; a miss reads [`GET_EXPIRY`] and caches the value for its [`copy_lifetime`], a missing key only with `store_cache_miss`. With a loader, a miss asks the loader, whose write caches the value.
    pub(crate) async fn read_cached(&self, local: &LocalCache, field: &Bytes) -> Result<Option<V>> {
        match local.local.lookup(field) {
            Some(Some(value)) => return Ok(Some(self.codec.decode(&value)?)),
            Some(None) if local.store_cache_miss => return Ok(None),
            _ => {}
        }
        let read = local.local.begin_read(field);
        // An error or a cancelled call drops `read`, which caches nothing.
        let raw: Option<(Bytes, i64, i64)> = self
            .key
            .core
            .eval(
                &GET_EXPIRY,
                self.redis_keys(&[
                    Name::Main,
                    Name::Timeout,
                    Name::Idle,
                    Name::LastAccess,
                    Name::Options,
                ]),
                vec![now(), field.clone()],
            )
            .await?;
        match raw {
            Some((value, ttl_timeout, idle_delta)) => {
                if let Some(lifetime) =
                    copy_lifetime(ttl_timeout, idle_delta, now_millis()).lifetime()
                {
                    read.finish(Some((Some(value.clone()), lifetime)));
                }
                Ok(Some(self.codec.decode(&value)?))
            }
            None if self.loads() => self.load_missing_cached(local, field, read).await,
            None => {
                read.finish(
                    local
                        .store_cache_miss
                        .then_some((None, Lifetime::default())),
                );
                Ok(None)
            }
        }
    }

    /// The local answers for `fields`, in order, and the indices of the keys the local cache does not answer. A cached miss answers only with `store_cache_miss`.
    pub(crate) fn local_hits(
        &self,
        local: &LocalCache,
        fields: &[Bytes],
    ) -> Result<(std::vec::Vec<Option<V>>, std::vec::Vec<usize>)> {
        let mut values = std::vec::Vec::with_capacity(fields.len());
        let mut misses = std::vec::Vec::new();
        for (i, field) in fields.iter().enumerate() {
            match local.local.lookup(field) {
                Some(Some(value)) => values.push(Some(self.codec.decode(&value)?)),
                Some(None) if local.store_cache_miss => values.push(None),
                _ => {
                    values.push(None);
                    misses.push(i);
                }
            }
        }
        Ok((values, misses))
    }

    /// Reads `fields` with [`GET_ALL_EXPIRY`] under one read guard each; returns, per field, the value and whether it is now cached.
    async fn read_expiring<'l>(
        &self,
        local: &'l LocalCache,
        fields: &[&Bytes],
    ) -> Result<std::vec::Vec<(Option<Bytes>, Option<ReadGuard<'l>>)>> {
        let guards: std::vec::Vec<ReadGuard<'l>> = fields
            .iter()
            .map(|field| local.local.begin_read(field))
            .collect();
        let mut args = std::vec::Vec::with_capacity(fields.len() + 1);
        args.push(now());
        args.extend(fields.iter().map(|field| (*field).clone()));
        // An error or a cancelled call drops the guards, which cache nothing.
        let raw: std::vec::Vec<Option<(Bytes, i64, i64)>> = self
            .key
            .core
            .eval(
                &GET_ALL_EXPIRY,
                self.redis_keys(&[
                    Name::Main,
                    Name::Timeout,
                    Name::Idle,
                    Name::LastAccess,
                    Name::Options,
                ]),
                args,
            )
            .await?;
        let now = now_millis();
        Ok(raw
            .into_iter()
            .zip(guards)
            .map(|(raw, guard)| match raw {
                Some((value, ttl_timeout, idle_delta)) => {
                    match copy_lifetime(ttl_timeout, idle_delta, now).lifetime() {
                        Some(lifetime) => {
                            guard.finish(Some((Some(value.clone()), lifetime)));
                        }
                        None => drop(guard),
                    }
                    (Some(value), None)
                }
                None => (None, Some(guard)),
            })
            .collect())
    }

    /// [`get_many`](HashMapCache::get_many) with a local cache: local hits, then one [`GET_ALL_EXPIRY`] for the misses, each cached for its lifetime; the loader runs for the keys still missing, in order.
    pub(crate) async fn get_many_cached(
        &self,
        local: &LocalCache,
        fields: &[Bytes],
    ) -> Result<std::vec::Vec<Option<V>>> {
        let (mut values, misses) = self.local_hits(local, fields)?;
        if misses.is_empty() {
            return Ok(values);
        }
        let missing: std::vec::Vec<&Bytes> = misses.iter().filter_map(|i| fields.get(*i)).collect();
        let read = self.read_expiring(local, &missing).await?;
        for ((i, field), (value, guard)) in misses.iter().zip(&missing).zip(read) {
            let value = match (value, guard) {
                (Some(value), _) => Some(self.codec.decode(&value)?),
                (None, Some(guard)) if self.loads() => {
                    self.load_missing_cached(local, field, guard).await?
                }
                (None, Some(guard)) => {
                    guard.finish(
                        local
                            .store_cache_miss
                            .then_some((None, Lifetime::default())),
                    );
                    None
                }
                (None, None) => None,
            };
            if let Some(slot) = values.get_mut(*i) {
                *slot = value;
            }
        }
        Ok(values)
    }

    /// [`peek_many`](HashMapCache::peek_many) with a local cache: local hits, then `GET_ALL_WITH_TTL_ONLY` for the misses, which are not cached (the reply has no idle deadline); `args` are `now, keys…`. The loader runs for the keys still missing, in order.
    pub(crate) async fn peek_many_cached(
        &self,
        local: &LocalCache,
        args: std::vec::Vec<Bytes>,
    ) -> Result<std::vec::Vec<Option<V>>> {
        let fields = args.get(1..).unwrap_or_default();
        let (mut values, misses) = self.local_hits(local, fields)?;
        if misses.is_empty() {
            return Ok(values);
        }
        let missing: std::vec::Vec<Bytes> = misses
            .iter()
            .filter_map(|i| fields.get(*i).cloned())
            .collect();
        let raw: std::vec::Vec<Option<Bytes>> = self
            .key
            .core
            .eval(
                &GET_ALL_WITH_TTL_ONLY,
                self.redis_keys(&[Name::Main, Name::Timeout]),
                std::iter::once(now())
                    .chain(missing.iter().cloned())
                    .collect(),
            )
            .await?;
        for ((i, field), raw) in misses.iter().zip(&missing).zip(raw) {
            let mut value = self.decode_value(raw)?;
            if value.is_none() && self.loads() {
                value = load_value(&Target::Cache(self, None), field, false).await?;
            }
            if let Some(slot) = values.get_mut(*i) {
                *slot = value;
            }
        }
        Ok(values)
    }

    /// Redisson's `preloadCache`: walks the keys with `SCAN_KEYS`, `page` at a time, and caches each page read with [`GET_ALL_EXPIRY`]. Returns how many entries it cached; a key that vanished between the two is skipped.
    pub(crate) async fn preload(&self, local: &LocalCache, page: usize) -> Result<usize> {
        let mut cursor = "0".to_string();
        let mut cached = 0;
        loop {
            let (next, keys) = self.scan_page(&SCAN_KEYS, cursor, None, page).await?;
            if !keys.is_empty() {
                let fields: std::vec::Vec<&Bytes> = keys.iter().collect();
                cached += self
                    .read_expiring(local, &fields)
                    .await?
                    .iter()
                    .filter(|(value, _)| value.is_some())
                    .count();
            }
            if next == "0" {
                return Ok(cached);
            }
            cursor = next;
        }
    }

    /// [`contains_key`](HashMapCache::contains_key) with a local cache: a cached key or cached miss answers; else `CONTAINS_KEY`, and a miss is cached only with `store_cache_miss`, as `LocalCachedMap` does.
    pub(crate) async fn contains_cached(&self, local: &LocalCache, field: &Bytes) -> Result<bool> {
        if let Some(cached) = local.local.lookup(field) {
            return Ok(cached.is_some());
        }
        let read = local.local.begin_read(field);
        let found: i64 = self
            .key
            .core
            .eval(
                &CONTAINS_KEY,
                self.redis_keys(&[
                    Name::Main,
                    Name::Timeout,
                    Name::Idle,
                    Name::LastAccess,
                    Name::Options,
                ]),
                vec![now(), field.clone()],
            )
            .await?;
        if found != 1 && self.loads() {
            return Ok(self
                .load_missing_cached(local, field, read)
                .await?
                .is_some());
        }
        let miss = found != 1 && local.store_cache_miss;
        read.finish(miss.then_some((None, Lifetime::default())));
        Ok(found == 1)
    }

    /// Asks the loader for a key that missed, for a read that [`begin_read`](crate::collections::local_cache::local::Local::begin_read) opened. A loaded value is cached by its write; a key the loader lacks is cached as missing only with `store_cache_miss`, as Redisson's `getAsync`.
    pub(crate) async fn load_missing_cached(
        &self,
        local: &LocalCache,
        field: &Bytes,
        read: ReadGuard<'_>,
    ) -> Result<Option<V>> {
        let loaded = load_value(&Target::Cache(self, None), field, false).await;
        let miss = matches!(loaded, Ok(None)) && local.store_cache_miss;
        read.finish(miss.then_some((None, Lifetime::default())));
        loaded
    }
}
