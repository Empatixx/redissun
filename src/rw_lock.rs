use crate::error::Result;
use crate::lock::{acquire, channel, LockGuard, Mode, FORCE_UNLOCK};
use crate::object::Key;
use crate::pending::Pending;
use bytes::Bytes;
use fred::interfaces::HashesInterface;
use std::fmt;
use std::time::Duration;

/// Proof that a read lock is held. It is the same type as a [`LockGuard`].
pub type RwLockReadGuard = LockGuard;

/// Proof that the write lock is held. It is the same type as a [`LockGuard`].
pub type RwLockWriteGuard = LockGuard;

/// A reentrant distributed lock that admits many readers or one writer.
///
/// Owners work as in [`Lock`](crate::Lock): the client together with the current tokio task. A task that holds the write lock may also take the read lock. A task that holds a read lock cannot take the write lock; it waits until it times out.
///
/// Readers are not blocked by waiting writers, so a steady stream of readers can keep a writer waiting.
///
/// The lock is one Redis hash `{name}` with the field `mode` (`read` or `write`) and a hold count for each owner. Every read hold also has a key `{name}:<owner>:rwlock_timeout:<n>` with its own lease, so a crashed reader stops blocking writers when its lease ends. In Redis Cluster all these keys share the hash tag `{name}`.
#[derive(Clone)]
pub struct RwLock {
    key: Key,
}

impl fmt::Debug for RwLock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "RwLock")
    }
}

impl RwLock {
    pub(crate) fn new(key: Key) -> Self {
        Self { key }
    }

    /// Waits until a read lock is acquired. Add `.timeout(duration)` to wait at most that long.
    pub fn read(&self) -> Pending<'_, RwLockReadGuard> {
        Pending::new(move |wait| acquire(&self.key, None, wait, Mode::Read))
    }

    /// Takes a read lock when no other owner holds the write lock; returns `None` at once otherwise.
    pub async fn try_read(&self) -> Result<Option<RwLockReadGuard>> {
        acquire(&self.key, None, Some(Duration::ZERO), Mode::Read).await
    }

    /// Waits until the write lock is acquired. Add `.timeout(duration)` to wait at most that long.
    pub fn write(&self) -> Pending<'_, RwLockWriteGuard> {
        Pending::new(move |wait| acquire(&self.key, None, wait, Mode::Write))
    }

    /// Takes the write lock when nobody else holds any lock; returns `None` at once otherwise.
    pub async fn try_write(&self) -> Result<Option<RwLockWriteGuard>> {
        acquire(&self.key, None, Some(Duration::ZERO), Mode::Write).await
    }

    /// Returns whether any owner holds the write lock.
    pub async fn is_write_locked(&self) -> Result<bool> {
        let mode: Option<String> = self
            .key
            .core
            .redis()
            .hget(self.key.redis_key(), "mode")
            .await?;
        Ok(mode.as_deref() == Some("write"))
    }

    /// Releases every read and write hold regardless of owner; returns whether anything was held.
    pub async fn force_unlock(&self) -> Result<bool> {
        let released: i64 = self
            .key
            .core
            .eval(
                &FORCE_UNLOCK,
                vec![self.key.redis_key()],
                vec![Bytes::from(channel(self.key.name()))],
            )
            .await?;
        Ok(released == 1)
    }
}
