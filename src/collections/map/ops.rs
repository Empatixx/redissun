//! The per-key lock behind [`HashMap::entry`](crate::HashMap::entry): Redisson's `getLock(key)`, the lock `compute` and `merge` take, so Redisson clients and this crate exclude each other.

use crate::collections::map::key_hash::map_key_name;
use crate::error::Result;
use crate::locks::{Lock, LockGuard};
use crate::object::Key;
use std::collections::HashMap;
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

/// Holds the key's turn in this process and its distributed lock. Dropping it releases the lock in the background.
pub(crate) struct KeyLock {
    turn: OwnedMutexGuard<()>,
    guard: LockGuard,
}

impl KeyLock {
    /// Takes the lock of the encoded key `k` of the map `key`.
    pub(crate) async fn acquire(key: &Key, k: &[u8]) -> Result<Self> {
        let name = map_key_name(key.name(), k, "lock");
        let turn = local_turn(key.core.client_id(), &name).await;
        let guard = Lock::new(Key::new(key.core.clone(), name)).lock().await?;
        Ok(Self { turn, guard })
    }

    /// Releases the lock, and returns `body`'s error before the unlock's. The turn in this process ends after the unlock.
    pub(crate) async fn release<T>(self, body: Result<T>) -> Result<T> {
        let Self { turn, guard } = self;
        let unlocked = guard.unlock().await;
        drop(turn);
        let value = body?;
        unlocked?;
        Ok(value)
    }
}
