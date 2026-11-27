//! [`StoreMode::Local`](crate::StoreMode::Local) for a [`LocalCachedMapCache`](crate::LocalCachedMapCache): the entries live only in the local caches, each with its own time to live and max idle, and Redis carries only the sync messages (Redisson's `LOCALCACHE`). Every write here is the in-memory version of the `HashMapCache` script it replaces, on encoded bytes, and publishes the messages `LocalCachedMap` publishes in this mode.

use super::{HashMapCache, Limits};
use crate::codec::Codec;
use crate::collections::local_cache::messages::{expiring_update, message, CLEAR, INVALIDATE};
use crate::collections::local_cache::store::Lifetime;
use crate::collections::local_cache::{LocalCache, SyncStrategy};
use crate::error::{Error, Result};
use bytes::Bytes;
use std::time::Duration;
use tokio::time::Instant;

/// The error of a method that needs the entries in Redis.
pub(crate) fn needs_redis(method: &str) -> Error {
    Error::Config(format!(
        "LocalCachedMapCache::{method} needs StoreMode::LocalRedis"
    ))
}

/// The local [`Lifetime`] of an entry written now with `limits`: the time-to-live deadline and the max idle, which local reads restart.
fn lifetime(limits: &Limits) -> Lifetime {
    let deadline = (limits.ttl_timeout > 0)
        .then(|| {
            let left = limits.ttl_timeout.saturating_sub(limits.now).max(0);
            Instant::now().checked_add(Duration::from_millis(left.unsigned_abs()))
        })
        .flatten();
    let idle =
        (limits.idle_delta > 0).then(|| Duration::from_millis(limits.idle_delta.unsigned_abs()));
    Lifetime { deadline, idle }
}

impl<K, V, C: Codec> HashMapCache<K, V, C> {
    /// The local cache when the entries live only there.
    pub(crate) fn local_only(&self) -> Option<&LocalCache> {
        self.local.as_deref().filter(|local| local.local_only())
    }

    /// Fails with [`needs_redis`] in [`StoreMode::Local`](crate::StoreMode::Local).
    pub(crate) fn redis_store(&self, method: &str) -> Result<()> {
        match self.local_only() {
            Some(_) => Err(needs_redis(method)),
            None => Ok(()),
        }
    }

    /// The live local value of `field`; reading it restarts its idle time.
    pub(super) fn local_value(local: &LocalCache, field: &Bytes) -> Option<Bytes> {
        local.local.lookup(field).flatten()
    }

    /// Stores `entries` locally with `limits` and announces them: the value and its limits with [`SyncStrategy::Update`], else an invalidation.
    pub(super) async fn local_store(
        &self,
        local: &LocalCache,
        entries: &[(Bytes, Bytes)],
        limits: &Limits,
    ) -> Result<()> {
        let lifetime = lifetime(limits);
        for (k, v) in entries {
            local.local.write(k.clone(), Some(v.clone()), lifetime);
        }
        if !local.announces() {
            return Ok(());
        }
        for (k, v) in entries {
            let notice = if local.sync == SyncStrategy::Update {
                expiring_update(&local.local.id, limits.ttl_timeout, limits.idle_delta, k, v)
            } else {
                message(&local.local.id, INVALIDATE, &[k])
            };
            local.publish(&self.key, notice).await?;
        }
        Ok(())
    }

    /// Forgets `fields` locally and announces an invalidation for each.
    pub(super) async fn local_forget(&self, local: &LocalCache, fields: &[Bytes]) -> Result<()> {
        for field in fields {
            local.local.forget(field);
        }
        if local.announces() {
            for field in fields {
                local
                    .publish(&self.key, message(&local.local.id, INVALIDATE, &[field]))
                    .await?;
            }
        }
        Ok(())
    }

    /// Removes the live `fields`; returns the removed ones.
    pub(super) async fn local_remove(
        &self,
        local: &LocalCache,
        fields: &[Bytes],
    ) -> Result<std::vec::Vec<(Bytes, Bytes)>> {
        let removed: std::vec::Vec<(Bytes, Bytes)> = fields
            .iter()
            .filter_map(|f| Some((f.clone(), Self::local_value(local, f)?)))
            .collect();
        let keys: std::vec::Vec<Bytes> = removed.iter().map(|(k, _)| k.clone()).collect();
        self.local_forget(local, &keys).await?;
        Ok(removed)
    }

    /// The time left to the time-to-live deadline of a live local entry: `None` for a missing entry, `Some(None)` for one without a deadline.
    pub(super) fn local_ttl(local: &LocalCache, field: &Bytes) -> Option<Option<Duration>> {
        let (value, lifetime) = local.local.lookup_with_lifetime(field)?;
        value?;
        Some(
            lifetime
                .deadline
                .map(|deadline| deadline.saturating_duration_since(Instant::now())),
        )
    }

    /// Gives the live `field` the new `limits`, `if_not_set` only when it has no time to live yet (no max idle for `max_idle` alone); returns whether it changed.
    pub(super) async fn local_expire(
        &self,
        local: &LocalCache,
        field: &Bytes,
        limits: &Limits,
        if_not_set: bool,
    ) -> Result<bool> {
        let Some((Some(value), old)) = local.local.lookup_with_lifetime(field) else {
            return Ok(false);
        };
        if if_not_set
            && ((limits.ttl_timeout > 0 && old.deadline.is_some())
                || (limits.ttl_timeout == 0 && limits.idle_delta > 0 && old.idle.is_some()))
        {
            return Ok(false);
        }
        self.local_store(local, &[(field.clone(), value)], limits)
            .await?;
        Ok(true)
    }

    /// The live local entries.
    pub(crate) fn local_snapshot(local: &LocalCache) -> std::vec::Vec<(Bytes, Bytes)> {
        local.local.entries()
    }

    /// Empties the local cache and tells the other instances; returns whether it held a value.
    pub(super) async fn local_clear(&self, local: &LocalCache) -> Result<bool> {
        let held = !local.local.entries().is_empty();
        local.local.forget_all();
        if local.announces() {
            local
                .publish(&self.key, message(&local.local.id, CLEAR, &[]))
                .await?;
        }
        Ok(held)
    }
}
