use crate::error::Result;
use crate::locks::{acquire, token_key, Lock, LockGuard, LockRequest, Mode, Wait};
use crate::object::{HasKey, Key};
use fred::interfaces::KeysInterface;
use std::fmt;

/// A [`Lock`] whose every acquisition gets a higher number, the fencing token, like Redisson's `RFencedLock`.
///
/// A resource that remembers the highest token it has seen can refuse a write that carries an older one. This protects it from a holder that was paused for so long that its lease ran out and another owner took over. Read the token from [`LockGuard::fencing_token`].
///
/// The acquire script increments the token atomically with every acquisition, reentrant ones included, so each guard has its own token. The counter lives in the key `redissun__lock_token:{name}`. Unlocking keeps it; [`Object::del`](crate::Object::del) deletes it together with the lock.
#[derive(Clone)]
pub struct FencedLock {
    lock: Lock,
    key: Key,
}

impl fmt::Debug for FencedLock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "FencedLock")
    }
}

impl From<FencedLock> for crate::locks::multi_lock::LockTarget {
    fn from(lock: FencedLock) -> Self {
        Self::new(lock.key, Mode::Fenced)
    }
}

impl HasKey for FencedLock {
    fn key(&self) -> &Key {
        &self.key
    }

    fn companions(&self) -> Vec<String> {
        vec![token_key(&self.key)]
    }
}

impl FencedLock {
    pub(crate) fn new(key: Key) -> Self {
        Self {
            lock: Lock::new(key.clone()),
            key,
        }
    }

    /// Waits until the lock is acquired. Add `.timeout(duration)` to wait at most that long, or `.lease(duration)` to set a lease instead of using the watchdog.
    pub fn lock(&self) -> LockRequest<'_> {
        LockRequest::new(&self.key, Mode::Fenced)
    }

    /// Acquires the lock when it is free; returns `None` immediately otherwise.
    pub async fn try_lock(&self) -> Result<Option<LockGuard>> {
        acquire(&self.key, None, Wait::Once, Mode::Fenced).await
    }

    /// The token of the latest acquisition, or 0 when the lock was never taken.
    pub async fn current_token(&self) -> Result<u64> {
        let token: Option<u64> = self.key.core.redis().get(token_key(&self.key)).await?;
        Ok(token.unwrap_or(0))
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

    /// Releases the lock regardless of its owner; returns whether it was held.
    pub async fn force_unlock(&self) -> Result<bool> {
        self.lock.force_unlock().await
    }
}
