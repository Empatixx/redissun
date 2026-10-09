use crate::error::Result;
use crate::lock::{
    acquire, channel, force_unlock, hold_count, is_held_by_current, read_unlock_message,
    unlock_message, LockGuard, LockRequest, Mode, Wait, READ_FORCE_UNLOCK, READ_IS_LOCKED,
    WRITE_FORCE_UNLOCK,
};
use crate::object::{HasKey, Key};
use fred::interfaces::HashesInterface;
use std::fmt;

/// Proof that a read lock is held. It is the same type as a [`LockGuard`].
pub type RwLockReadGuard = LockGuard;

/// Proof that the write lock is held. It is the same type as a [`LockGuard`].
pub type RwLockWriteGuard = LockGuard;

/// A reentrant distributed lock that admits many readers or one writer, the counterpart of Redisson's `RReadWriteLock`.
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

impl HasKey for RwLock {
    fn key(&self) -> &Key {
        &self.key
    }
}

impl RwLock {
    pub(crate) fn new(key: Key) -> Self {
        Self { key }
    }

    /// Waits until a read lock is acquired. Add `.timeout(duration)` to wait at most that long, or `.lease(duration)` to set a lease instead of using the watchdog.
    pub fn read(&self) -> LockRequest<'_> {
        LockRequest::new(&self.key, Mode::Read)
    }

    /// Takes a read lock when no other owner holds the write lock; returns `None` at once otherwise.
    pub async fn try_read(&self) -> Result<Option<RwLockReadGuard>> {
        acquire(&self.key, None, Wait::Once, Mode::Read).await
    }

    /// Waits until the write lock is acquired. Add `.timeout(duration)` to wait at most that long, or `.lease(duration)` to set a lease instead of using the watchdog.
    pub fn write(&self) -> LockRequest<'_> {
        LockRequest::new(&self.key, Mode::Write)
    }

    /// Takes the write lock when nobody else holds any lock; returns `None` at once otherwise.
    pub async fn try_write(&self) -> Result<Option<RwLockWriteGuard>> {
        acquire(&self.key, None, Wait::Once, Mode::Write).await
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

    /// Returns whether any owner holds a read lock.
    pub async fn is_read_locked(&self) -> Result<bool> {
        let locked: i64 = self
            .key
            .core
            .eval(&READ_IS_LOCKED, vec![self.key.redis_key()], vec![])
            .await?;
        Ok(locked == 1)
    }

    /// Returns whether the calling task holds a read lock.
    pub async fn is_read_held_by_current(&self) -> Result<bool> {
        is_held_by_current(&self.key, Mode::Read).await
    }

    /// Returns whether the calling task holds the write lock.
    pub async fn is_write_held_by_current(&self) -> Result<bool> {
        is_held_by_current(&self.key, Mode::Write).await
    }

    /// Number of reentrant read holds by the calling task.
    pub async fn read_hold_count(&self) -> Result<u64> {
        hold_count(&self.key, Mode::Read).await
    }

    /// Number of reentrant write holds by the calling task.
    pub async fn write_hold_count(&self) -> Result<u64> {
        hold_count(&self.key, Mode::Write).await
    }

    /// Releases every read hold regardless of owner when the lock is in read mode; returns whether it was.
    pub async fn force_unlock_read(&self) -> Result<bool> {
        force_unlock(
            &self.key,
            &READ_FORCE_UNLOCK,
            vec![self.key.redis_key(), channel(self.key.name())],
            vec![unlock_message()],
        )
        .await
    }

    /// Releases the write lock and the read holds of its owner regardless of owner when the lock is in write mode; returns whether it was.
    pub async fn force_unlock_write(&self) -> Result<bool> {
        force_unlock(
            &self.key,
            &WRITE_FORCE_UNLOCK,
            vec![self.key.redis_key(), channel(self.key.name())],
            vec![read_unlock_message()],
        )
        .await
    }

    /// Releases every read and write hold regardless of owner; returns whether anything was held.
    pub async fn force_unlock(&self) -> Result<bool> {
        Ok(self.force_unlock_write().await? || self.force_unlock_read().await?)
    }
}
