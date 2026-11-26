//! Redisson's `compute`, `computeIfAbsent`, `computeIfPresent` and `merge`, written once for every map that can do the few atomic operations of [`AtomicMap`].
//!
//! As in `RedissonMap`, each runs under the per-key lock (`getLock(key)`), so Redisson clients and this crate exclude each other.

use crate::collections::map::key_hash::map_key_name;
use crate::error::Result;
use crate::locks::{Lock, LockGuard};
use crate::object::Key;
use bytes::Bytes;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, LazyLock, Mutex, Weak};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

/// One async mutex per client and per-key lock, taken before the distributed lock.
///
/// The distributed lock is reentrant for its owner, the client and the current tokio task, so futures joined inside one task (`join_all`, `join!`) would all hold it at once and lose updates. This mutex lets one of them in at a time, in this process; the distributed lock still excludes every other process and client.
static LOCAL: LazyLock<Mutex<HashMap<String, Weak<AsyncMutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

async fn local_turn(client_id: &str, lock_name: &str) -> OwnedMutexGuard<()> {
    let id = format!("{client_id}\0{lock_name}");
    let mutex = {
        let mut all = LOCAL.lock().unwrap_or_else(|e| e.into_inner());
        match all.get(&id).and_then(Weak::upgrade) {
            Some(mutex) => mutex,
            None => {
                all.retain(|_, mutex| mutex.strong_count() > 0);
                let mutex = Arc::new(AsyncMutex::new(()));
                all.insert(id, Arc::downgrade(&mutex));
                mutex
            }
        }
    };
    mutex.lock_owned().await
}

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

/// Holds the key's turn in this process and its distributed lock, Redisson's `getLock(key)`.
struct KeyLock {
    turn: OwnedMutexGuard<()>,
    guard: LockGuard,
}

async fn lock_key<V>(map: &impl AtomicMap<V>, k: &Bytes) -> Result<KeyLock> {
    let key = map.map_key();
    let name = map_key_name(key.name(), k, "lock");
    let turn = local_turn(key.core.client_id(), &name).await;
    let lock = Lock::new(Key::new(key.core.clone(), name));
    let guard = lock.lock().await?;
    Ok(KeyLock { turn, guard })
}

/// Releases the lock after `body`, and returns the body's error before the unlock's. The turn in this process ends after the unlock.
async fn release<T>(lock: KeyLock, body: Result<T>) -> Result<T> {
    let KeyLock { turn, guard } = lock;
    let unlocked = guard.unlock().await;
    drop(turn);
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
    let lock = lock_key(map, &k).await?;
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
    release(lock, body).await
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
    let lock = lock_key(map, &k).await?;
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
    release(lock, body).await
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
    let lock = lock_key(map, &k).await?;
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
    release(lock, body).await
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
    let lock = lock_key(map, &k).await?;
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
    release(lock, body).await
}
