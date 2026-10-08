use crate::error::{Error, Result};
use crate::lock::{acquire, channel, LockGuard, LockOptions, Mode, FORCE_UNLOCK};
use crate::object::Key;
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

    async fn acquire(&self, wait: Option<Duration>, mode: Mode) -> Result<Option<LockGuard>> {
        let options = match wait {
            Some(wait) => LockOptions::new().wait(wait),
            None => LockOptions::new(),
        };
        acquire(&self.key, options, mode).await
    }

    /// Waits until a read lock is acquired.
    pub async fn read(&self) -> Result<RwLockReadGuard> {
        self.acquire(None, Mode::Read).await?.ok_or(Error::Timeout)
    }

    /// Takes a read lock when no other owner holds the write lock; returns `None` at once otherwise.
    pub async fn try_read(&self) -> Result<Option<RwLockReadGuard>> {
        self.acquire(Some(Duration::ZERO), Mode::Read).await
    }

    /// Waits up to `wait` for a read lock.
    pub async fn read_for(&self, wait: Duration) -> Result<Option<RwLockReadGuard>> {
        self.acquire(Some(wait), Mode::Read).await
    }

    /// Waits until the write lock is acquired.
    pub async fn write(&self) -> Result<RwLockWriteGuard> {
        self.acquire(None, Mode::Write).await?.ok_or(Error::Timeout)
    }

    /// Takes the write lock when nobody else holds any lock; returns `None` at once otherwise.
    pub async fn try_write(&self) -> Result<Option<RwLockWriteGuard>> {
        self.acquire(Some(Duration::ZERO), Mode::Write).await
    }

    /// Waits up to `wait` for the write lock.
    pub async fn write_for(&self, wait: Duration) -> Result<Option<RwLockWriteGuard>> {
        self.acquire(Some(wait), Mode::Write).await
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
