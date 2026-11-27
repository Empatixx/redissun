//! Leases on a [`HashMapCache`] key, Redisson's `RLeasedMap`: one caller loads a missing value while the others wait or read through.

use super::scripts::{LEASE_GET, LEASE_REMOVE};
use super::{limits, HashMapCache, Name, PUBLISH};
use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::object::{millis, now_millis};
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::time::Duration;
use uuid::Uuid;

/// What [`HashMapCache::lease`] found.
#[non_exhaustive]
pub enum Lease<'m, K, V, C: Codec> {
    /// The key had a live value.
    Hit(V),
    /// This call took the lease. Load the value and call [`LeaseGuard::insert`].
    Acquired(LeaseGuard<'m, K, V, C>),
    /// Someone else holds the lease. Retry later or read through.
    Held,
}

impl<K, V: fmt::Debug, C: Codec> fmt::Debug for Lease<'_, K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Lease::Hit(value) => f.debug_tuple("Hit").field(value).finish(),
            Lease::Acquired(guard) => f.debug_tuple("Acquired").field(guard).finish(),
            Lease::Held => f.write_str("Held"),
        }
    }
}

/// The lease this caller took on a missing key: finish it with [`LeaseGuard::insert`] or [`LeaseGuard::remove`].
///
/// Dropping the guard does nothing: the lease ends by itself when its time is up, and until then [`HashMapCache::lease`] reports [`Lease::Held`] to everybody else. To finish the lease in another process, hand it [`LeaseGuard::token`] and call [`Set::lease`](super::Set::lease) there.
#[must_use = "a lease guard holds the lease until its time is up; finish it with insert or remove"]
pub struct LeaseGuard<'m, K, V, C: Codec> {
    cache: &'m HashMapCache<K, V, C>,
    key: Bytes,
    token: String,
}

impl<K, V, C: Codec> fmt::Debug for LeaseGuard<'_, K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LeaseGuard")
            .field("cache", self.cache)
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}

impl<'m, K, V, C: Codec> LeaseGuard<'m, K, V, C> {
    /// The lease token, for finishing the lease in another process with [`Set::lease`](super::Set::lease).
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Stores the loaded value and frees the lease, when this guard still holds it. Await it, optionally after setting [`LeaseInsert::ttl`] or [`LeaseInsert::max_idle`].
    ///
    /// Resolves to `true` when the value was stored and the lease freed. `false` means the lease's time was up or someone else took or freed it (by a plain [`HashMapCache::insert`] or [`HashMapCache::set`], [`HashMapCache::invalidate`] or a newer lease), and nothing was stored. As in Redisson, `false` also comes back when the lease still matched but a live entry had been written meanwhile: the value was then stored over it. The script is sent at most once: when the connection fails after it was sent, the call returns that error instead of sending it again, so a stored value is never reported as `false` by a second run.
    #[doc(alias = "putWithLease")]
    pub fn insert<W>(self, value: &'m W) -> LeaseInsert<'m, K, V, C, W>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        LeaseInsert {
            guard: self,
            value,
            ttl: None,
            max_idle: None,
        }
    }

    /// Deletes the entry and the lease, Redisson's `removeWithLease`. Returns whether an entry was there. See [`HashMapCache::invalidate`].
    #[doc(alias = "removeWithLease")]
    pub async fn remove(self) -> Result<bool> {
        self.cache.lease_remove(self.key).await
    }
}

/// A pending [`LeaseGuard::insert`]. Await it, optionally after setting [`LeaseInsert::ttl`] or [`LeaseInsert::max_idle`].
#[must_use = "an insert does nothing until it is awaited"]
pub struct LeaseInsert<'m, K, V, C: Codec, W: ?Sized> {
    guard: LeaseGuard<'m, K, V, C>,
    value: &'m W,
    ttl: Option<Duration>,
    max_idle: Option<Duration>,
}

impl<K, V, C: Codec, W: ?Sized> fmt::Debug for LeaseInsert<'_, K, V, C, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LeaseInsert")
            .field("guard", &self.guard)
            .field("ttl", &self.ttl)
            .field("max_idle", &self.max_idle)
            .finish_non_exhaustive()
    }
}

impl<K, V, C: Codec, W: ?Sized> LeaseInsert<'_, K, V, C, W> {
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

impl<'m, K, V, C, W> IntoFuture for LeaseInsert<'m, K, V, C, W>
where
    K: Send + Sync + 'm,
    V: Borrow<W> + Send + Sync + 'm,
    C: Codec,
    W: Serialize + ?Sized + Sync + 'm,
{
    type Output = Result<bool>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'm>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let cache = self.guard.cache;
            let limits = limits(self.ttl, self.max_idle)?;
            let value = cache.codec.encode(self.value)?;
            cache
                .fast_put_with_limits(
                    &limits,
                    self.guard.key,
                    value,
                    Bytes::from(self.guard.token),
                )
                .await
        })
    }
}

impl<K, V, C> HashMapCache<K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    /// Reads the value, or takes a lease on the missing key for `time` so that only this caller loads it, Redisson's `getWithLease`.
    ///
    /// Returns [`Lease::Hit`] with a live value (a read like [`HashMapCache::get`]), [`Lease::Acquired`] with a guard when this call took the lease, or [`Lease::Held`] when someone else holds it. A zero `time` is [`Error::Config`].
    ///
    /// ```ignore
    /// match cache.lease("a", Duration::from_secs(5)).await? {
    ///     Lease::Hit(value) => use_it(value),
    ///     Lease::Acquired(guard) => {
    ///         let value = load("a").await?;
    ///         guard.insert(&value).ttl(Duration::from_secs(60)).await?;
    ///     }
    ///     _ => { /* Lease::Held: someone else is loading it */ }
    /// }
    /// ```
    #[doc(alias = "getWithLease")]
    pub async fn lease<Q>(&self, k: &Q, time: Duration) -> Result<Lease<'_, K, V, C>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if time.is_zero() {
            return Err(Error::Config("lease time should be positive".into()));
        }
        let lease_ms = millis(time)?;
        let key = self.codec.encode(k)?;
        let token = Uuid::new_v4().to_string();
        let (status, value, holder, acquired): (i64, Option<Bytes>, Option<String>, i64) = self
            .key
            .core
            .eval_once(
                &LEASE_GET,
                self.redis_keys(&[
                    Name::Main,
                    Name::Timeout,
                    Name::Idle,
                    Name::LastAccess,
                    Name::Options,
                    Name::Lease,
                ]),
                vec![
                    Bytes::from(now_millis().to_string()),
                    key.clone(),
                    Bytes::from(token.clone()),
                    Bytes::from(lease_ms.to_string()),
                ],
            )
            .await?;
        let malformed = || Error::Redis("malformed lease reply".into());
        if status == 1 {
            let value = value.ok_or_else(malformed)?;
            return Ok(Lease::Hit(self.codec.decode(&value)?));
        }
        match holder {
            Some(holder) if !holder.is_empty() => {
                if took_lease(acquired, &holder, &token) {
                    Ok(Lease::Acquired(LeaseGuard {
                        cache: self,
                        key,
                        token,
                    }))
                } else {
                    Ok(Lease::Held)
                }
            }
            _ => Err(malformed()),
        }
    }

    /// Deletes the entry and any lease on the key, Redisson's `removeWithLease`: the call for "the source changed", which drops the cached value and makes a pending [`LeaseGuard::insert`] store nothing. Returns whether an entry was there.
    #[doc(alias = "removeWithLease")]
    pub async fn invalidate<Q>(&self, k: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.lease_remove(self.codec.encode(k)?).await
    }
}

impl<K, V, C: Codec> HashMapCache<K, V, C> {
    /// Redisson's `removeWithLease` script for one encoded key.
    async fn lease_remove(&self, key: Bytes) -> Result<bool> {
        let removed: std::vec::Vec<i64> = self
            .key
            .core
            .eval(
                &LEASE_REMOVE,
                self.redis_keys(&[
                    Name::Main,
                    Name::Timeout,
                    Name::Idle,
                    Name::Removed,
                    Name::LastAccess,
                    Name::Options,
                    Name::Lease,
                ]),
                vec![Bytes::from(PUBLISH), key],
            )
            .await?;
        Ok(removed.iter().any(|n| *n != 0))
    }
}

/// Whether a `LEASE_GET` reply means this call holds the lease: it took it now, or the holder is this call's token (a run whose reply was lost took it).
fn took_lease(acquired: i64, holder: &str, token: &str) -> bool {
    acquired == 1 || holder == token
}

#[cfg(test)]
mod tests {
    use super::took_lease;

    #[test]
    fn the_lease_is_ours_when_the_holder_is_our_token() {
        assert!(took_lease(1, "t", "t"));
        assert!(took_lease(0, "t", "t"));
        assert!(!took_lease(0, "other", "t"));
    }
}
