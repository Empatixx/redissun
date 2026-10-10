use super::scripts::FORCE_UNLOCK;
use super::{
    acquire, channel, force_unlock, hold_count, is_held_by_current, is_locked, unlock_message,
    LockGuard, Mode, Wait,
};
use crate::error::{Error, Result};
use crate::object::{HasKey, Key};
use crate::pending::{Pending, PendingTimeout};
use bytes::Bytes;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::time::Duration;

/// A pending lock call of a [`Lock`], a [`FairLock`](crate::FairLock), a [`FencedLock`](crate::FencedLock) or an [`RwLock`](crate::RwLock). Await it to wait for the lock without a limit. Set `.lease(duration)` or `.timeout(duration)` first to change that.
#[must_use = "a pending lock does nothing until it is awaited"]
pub struct LockRequest<'a> {
    key: &'a Key,
    mode: Mode,
    lease: Option<Duration>,
}

impl<'a> LockRequest<'a> {
    pub(crate) fn new(key: &'a Key, mode: Mode) -> Self {
        Self {
            key,
            mode,
            lease: None,
        }
    }

    /// Sets an explicit lease, which disables the watchdog. Without it the watchdog renews the lock.
    pub fn lease(mut self, lease: Duration) -> Self {
        self.lease = Some(lease);
        self
    }

    /// Waits at most `timeout` for the lock. The call then resolves to `None` when the time runs out.
    pub fn timeout(self, timeout: Duration) -> PendingTimeout<'a, LockGuard> {
        let (key, mode, lease) = (self.key, self.mode, self.lease);
        Pending::new(move |wait| acquire(key, lease, wait.map_or(Wait::Forever, Wait::For), mode))
            .timeout(timeout)
    }
}

impl<'a> IntoFuture for LockRequest<'a> {
    type Output = Result<LockGuard>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        let (key, mode, lease) = (self.key, self.mode, self.lease);
        Box::pin(async move {
            acquire(key, lease, Wait::Forever, mode)
                .await?
                .ok_or(Error::Timeout)
        })
    }
}

/// A reentrant distributed lock, the counterpart of Redisson's `RLock`. The owner is the client together with the current tokio task.
///
/// Code that runs outside any spawned task (the body of `#[tokio::main]`, `block_on`, `spawn_blocking`) shares one owner per client, and futures joined inside a single task share that task's owner; neither case excludes the others.
///
/// Acquiring is cancellation safe: dropping a pending `lock` future, for example through `tokio::time::timeout`, never leaves the lock held.
#[derive(Clone)]
pub struct Lock {
    key: Key,
}

impl fmt::Debug for Lock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "Lock")
    }
}

impl From<Lock> for crate::locks::multi_lock::LockTarget {
    fn from(lock: Lock) -> Self {
        Self::new(lock.key, Mode::Exclusive)
    }
}

impl HasKey for Lock {
    fn key(&self) -> &Key {
        &self.key
    }
}

impl Lock {
    pub(crate) fn new(key: Key) -> Self {
        Self { key }
    }

    /// Waits until the lock is acquired. Add `.timeout(duration)` to wait at most that long, or `.lease(duration)` to set a lease instead of using the watchdog.
    pub fn lock(&self) -> LockRequest<'_> {
        LockRequest::new(&self.key, Mode::Exclusive)
    }

    /// Acquires the lock when it is free; returns `None` immediately otherwise.
    pub async fn try_lock(&self) -> Result<Option<LockGuard>> {
        acquire(&self.key, None, Wait::Once, Mode::Exclusive).await
    }

    /// Returns whether any owner holds the lock.
    pub async fn is_locked(&self) -> Result<bool> {
        is_locked(&self.key).await
    }

    /// Returns whether the calling task holds the lock.
    pub async fn is_held_by_current(&self) -> Result<bool> {
        is_held_by_current(&self.key, Mode::Exclusive).await
    }

    /// Number of reentrant holds by the calling task.
    pub async fn hold_count(&self) -> Result<u64> {
        hold_count(&self.key, Mode::Exclusive).await
    }

    /// Releases the lock regardless of its owner; returns whether it was held.
    pub async fn force_unlock(&self) -> Result<bool> {
        force_unlock(
            &self.key,
            &FORCE_UNLOCK,
            vec![self.key.redis_key()],
            vec![Bytes::from(channel(self.key.name())), unlock_message()],
        )
        .await
    }
}
