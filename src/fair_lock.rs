use crate::error::Result;
use crate::lock::{acquire, fair, Lock, LockGuard, LockRequest, Mode};
use crate::object::{HasKey, Key};
use bytes::Bytes;
use fred::interfaces::ListInterface;
use std::fmt;
use std::time::Duration;

/// A reentrant distributed lock that hands itself to waiters in the order they arrived.
///
/// Owners work as in [`Lock`]: the client together with the current tokio task. A caller that asks while others already wait goes to the back of the queue, so a busy lock cannot starve anybody and [`try_lock`](FairLock::try_lock) never jumps the queue.
///
/// The scheme is taken from Redisson's `RedissonFairLock`. The lock is the hash `{name}`. Waiters sit in the list `redissun__lock_queue:{name}` and in the sorted set `redissun__lock_timeout:{name}`, which stores when each waiter gives up its place. A waiter that crashed loses its place after that time, so it cannot block the others. Each waiter listens on its own channel, and a release wakes only the next one.
#[derive(Clone)]
pub struct FairLock {
    lock: Lock,
    key: Key,
}

impl fmt::Debug for FairLock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "FairLock")
    }
}

impl From<FairLock> for crate::multi_lock::LockTarget {
    fn from(lock: FairLock) -> Self {
        Self::new(lock.key, Mode::Fair)
    }
}

impl HasKey for FairLock {
    fn key(&self) -> &Key {
        &self.key
    }

    fn companions(&self) -> Vec<String> {
        vec![fair::queue_key(&self.key), fair::timeout_key(&self.key)]
    }
}

impl FairLock {
    pub(crate) fn new(key: Key) -> Self {
        Self {
            lock: Lock::new(key.clone()),
            key,
        }
    }

    /// Waits until the lock is acquired, after every owner that asked earlier. Add `.timeout(duration)` to wait at most that long, or `.lease(duration)` to set a lease instead of using the watchdog.
    pub fn lock(&self) -> LockRequest<'_> {
        LockRequest::new(&self.key, Mode::Fair)
    }

    /// Acquires the lock when it is free and nobody waits; returns `None` immediately otherwise.
    pub async fn try_lock(&self) -> Result<Option<LockGuard>> {
        acquire(&self.key, None, Some(Duration::ZERO), Mode::Fair).await
    }

    /// Returns whether any owner holds the lock.
    pub async fn is_locked(&self) -> Result<bool> {
        self.lock.is_locked().await
    }

    /// Returns whether the calling task holds the lock.
    pub async fn is_held_by_current(&self) -> Result<bool> {
        self.lock.is_held_by_current().await
    }

    /// Number of reentrant holds by the calling task.
    pub async fn hold_count(&self) -> Result<u64> {
        self.lock.hold_count().await
    }

    /// Number of owners that wait for the lock.
    pub async fn queue_len(&self) -> Result<usize> {
        let length: usize = self
            .key
            .core
            .redis()
            .llen(fair::queue_key(&self.key))
            .await?;
        Ok(length)
    }

    /// Releases the lock regardless of its owner and wakes the first waiter; returns whether it was held.
    pub async fn force_unlock(&self) -> Result<bool> {
        let removed: i64 = self
            .key
            .core
            .eval(
                &fair::FORCE_UNLOCK,
                fair::keys(&self.key),
                vec![Bytes::from(fair::channel_prefix(self.key.name()))],
            )
            .await?;
        Ok(removed == 1)
    }
}
