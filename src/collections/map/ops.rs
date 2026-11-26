//! Redisson's `compute`, `computeIfAbsent`, `computeIfPresent` and `merge`, written once for every map that can do the few atomic operations of [`AtomicMap`].
//!
//! As in `RedissonMap`, each runs under the per-key lock (`getLock(key)`), so Redisson clients and this crate exclude each other.

use crate::collections::map::key_hash::map_key_name;
use crate::error::Result;
use crate::locks::{Lock, LockGuard};
use crate::object::Key;
use bytes::Bytes;
use std::future::Future;

/// The atomic operations on an encoded key that the compute family needs.
pub(crate) trait AtomicMap<V>: Sync {
    /// The map's own key.
    fn map_key(&self) -> &Key;
    fn get_encoded(&self, k: &Bytes) -> impl Future<Output = Result<Option<V>>> + Send;
    fn fast_insert_encoded(&self, k: &Bytes, v: &V) -> impl Future<Output = Result<()>> + Send;
    fn insert_if_absent_encoded(
        &self,
        k: &Bytes,
        v: &V,
    ) -> impl Future<Output = Result<Option<V>>> + Send;
    fn fast_insert_if_exists_encoded(
        &self,
        k: &Bytes,
        v: &V,
    ) -> impl Future<Output = Result<bool>> + Send;
    fn encode_value(&self, v: &V) -> Result<Bytes>;
    /// Removes the entry only while its encoded value is still `v`.
    fn remove_if_encoded(&self, k: &Bytes, v: &Bytes) -> impl Future<Output = Result<bool>> + Send;
    fn fast_remove_encoded(&self, k: &Bytes) -> impl Future<Output = Result<()>> + Send;
}

fn key_lock<V>(map: &impl AtomicMap<V>, k: &Bytes) -> Lock {
    let key = map.map_key();
    Lock::new(Key::new(
        key.core.clone(),
        map_key_name(key.name(), k, "lock"),
    ))
}

/// Releases the lock after `body`, and returns the body's error before the unlock's.
async fn release<T>(guard: LockGuard, body: Result<T>) -> Result<T> {
    let unlocked = guard.unlock().await;
    let value = body?;
    unlocked?;
    Ok(value)
}

/// `compute`: the closure gets the current value; `Some` stores the result, `None` removes the entry.
pub(crate) async fn compute<V, F, Fut>(map: &impl AtomicMap<V>, k: Bytes, f: F) -> Result<Option<V>>
where
    V: Send + Sync,
    F: FnOnce(Option<V>) -> Fut + Send,
    Fut: Future<Output = Option<V>> + Send,
{
    let lock = key_lock(map, &k);
    let guard = lock.lock().await?;
    let body = async {
        let old = map.get_encoded(&k).await?;
        let existed = old.is_some();
        match f(old).await {
            Some(new) => {
                map.fast_insert_encoded(&k, &new).await?;
                Ok(Some(new))
            }
            None => {
                if existed {
                    map.fast_remove_encoded(&k).await?;
                }
                Ok(None)
            }
        }
    }
    .await;
    release(guard, body).await
}

/// `computeIfAbsent`: returns the present value, or stores and returns what the closure gives for a missing key.
pub(crate) async fn compute_if_absent<V, F, Fut>(
    map: &impl AtomicMap<V>,
    k: Bytes,
    f: F,
) -> Result<Option<V>>
where
    V: Send + Sync,
    F: FnOnce() -> Fut + Send,
    Fut: Future<Output = Option<V>> + Send,
{
    // Redisson's fast path: a present value needs no lock.
    if let Some(value) = map.get_encoded(&k).await? {
        return Ok(Some(value));
    }
    let lock = key_lock(map, &k);
    let guard = lock.lock().await?;
    let body = async {
        if let Some(value) = map.get_encoded(&k).await? {
            return Ok(Some(value));
        }
        let Some(new) = f().await else {
            return Ok(None);
        };
        match map.insert_if_absent_encoded(&k, &new).await? {
            Some(winner) => Ok(Some(winner)),
            None => Ok(Some(new)),
        }
    }
    .await;
    release(guard, body).await
}

/// `computeIfPresent`: the closure gets the present value; `Some` replaces it, `None` removes it. A missing key stays missing.
pub(crate) async fn compute_if_present<V, F, Fut>(
    map: &impl AtomicMap<V>,
    k: Bytes,
    f: F,
) -> Result<Option<V>>
where
    V: Send + Sync,
    F: FnOnce(V) -> Fut + Send,
    Fut: Future<Output = Option<V>> + Send,
{
    let lock = key_lock(map, &k);
    let guard = lock.lock().await?;
    let body = async {
        let Some(old) = map.get_encoded(&k).await? else {
            return Ok(None);
        };
        // The closure consumes the old value; keep its encoded form for Redisson's `remove(key, oldValue)`.
        let old_encoded = map.encode_value(&old)?;
        match f(old).await {
            Some(new) => {
                if map.fast_insert_if_exists_encoded(&k, &new).await? {
                    Ok(Some(new))
                } else {
                    Ok(None)
                }
            }
            None => {
                map.remove_if_encoded(&k, &old_encoded).await?;
                Ok(None)
            }
        }
    }
    .await;
    release(guard, body).await
}

/// `merge`: stores `value` for a missing key, otherwise what the closure makes of the old value and `value`; `None` removes the entry.
pub(crate) async fn merge<V, F, Fut>(
    map: &impl AtomicMap<V>,
    k: Bytes,
    value: V,
    f: F,
) -> Result<Option<V>>
where
    V: Send + Sync,
    F: FnOnce(V, V) -> Fut + Send,
    Fut: Future<Output = Option<V>> + Send,
{
    let lock = key_lock(map, &k);
    let guard = lock.lock().await?;
    let body = async {
        let new = match map.get_encoded(&k).await? {
            Some(old) => f(old, value).await,
            None => Some(value),
        };
        match new {
            Some(new) => {
                map.fast_insert_encoded(&k, &new).await?;
                Ok(Some(new))
            }
            None => {
                map.fast_remove_encoded(&k).await?;
                Ok(None)
            }
        }
    }
    .await;
    release(guard, body).await
}
