use crate::error::{Error, Result};
use crate::lock::{acquire, LockGuard, Mode};
use crate::object::Key;
use crate::pending::Pending;
use std::fmt;
use std::time::Duration;
use tokio::time::Instant;
use uuid::Uuid;

const WINDOW_PER_LOCK: Duration = Duration::from_millis(1500);

/// A lock that can be part of a [`MultiLock`]: a [`Lock`](crate::Lock), a [`FairLock`](crate::FairLock) or a [`FencedLock`](crate::FencedLock).
#[derive(Clone, Debug)]
pub struct LockTarget {
    key: Key,
    mode: Mode,
}

impl LockTarget {
    pub(crate) fn new(key: Key, mode: Mode) -> Self {
        Self { key, mode }
    }
}

/// Several locks that are taken and released together.
///
/// The locks are tried in the order given. When one of them cannot be taken in time, every lock taken so far is released and the whole round starts again. [`lock`](MultiLock::lock) uses a round of 1.5 seconds for each lock and a short random pause between rounds, so owners that take the same locks in opposite order do not wait for each other forever. This is the scheme of Redisson's `RedissonMultiLock`.
///
/// The watchdog keeps every lock alive while the guard exists. In Redis Cluster the locks may live on different nodes.
#[derive(Clone, Debug)]
pub struct MultiLock {
    targets: Vec<LockTarget>,
}

impl MultiLock {
    pub(crate) fn new(targets: Vec<LockTarget>) -> Result<Self> {
        if targets.is_empty() {
            return Err(Error::Config("a multi lock needs at least one lock".into()));
        }
        Ok(Self { targets })
    }

    /// Waits until every lock is taken. Add `.timeout(duration)` to wait at most that long; the call then resolves to `None` and holds nothing.
    pub fn lock(&self) -> Pending<'_, MultiLockGuard> {
        Pending::new(move |wait| async move {
            match wait {
                Some(wait) => self.round(wait).await,
                None => loop {
                    let window = WINDOW_PER_LOCK * self.targets.len() as u32;
                    if let Some(guard) = self.round(window).await? {
                        return Ok(Some(guard));
                    }
                    let jitter = Uuid::new_v4().as_u128() % 100;
                    tokio::time::sleep(Duration::from_millis(jitter as u64)).await;
                },
            }
        })
    }

    /// Takes every lock when all are free; returns `None` at once otherwise and holds nothing.
    pub async fn try_lock(&self) -> Result<Option<MultiLockGuard>> {
        self.round(Duration::ZERO).await
    }

    async fn round(&self, wait: Duration) -> Result<Option<MultiLockGuard>> {
        let started = Instant::now();
        let mut taken = Vec::with_capacity(self.targets.len());
        for target in &self.targets {
            let remaining = wait.saturating_sub(started.elapsed());
            match acquire(&target.key, None, Some(remaining), target.mode).await {
                Ok(Some(guard)) => taken.push(guard),
                Ok(None) => {
                    let _ = release_all(taken).await;
                    return Ok(None);
                }
                Err(error) => {
                    let _ = release_all(taken).await;
                    return Err(error);
                }
            }
        }
        Ok(Some(MultiLockGuard { guards: taken }))
    }
}

async fn release_all(guards: Vec<LockGuard>) -> Result<()> {
    let mut first_error = None;
    for guard in guards {
        if let Err(error) = guard.unlock().await {
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

/// Proof that every lock of a [`MultiLock`] is held. Dropping it releases all of them in the background.
#[must_use = "dropping the guard releases the locks in the background; call unlock().await to release them deterministically"]
pub struct MultiLockGuard {
    guards: Vec<LockGuard>,
}

impl MultiLockGuard {
    /// Releases every lock and stops the watchdogs. Every lock is tried even when one fails; the first error is returned.
    pub async fn unlock(self) -> Result<()> {
        release_all(self.guards).await
    }

    /// The guards of the single locks, in the order of the locks.
    pub fn guards(&self) -> &[LockGuard] {
        &self.guards
    }
}

impl fmt::Debug for MultiLockGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiLockGuard")
            .field("locks", &self.guards.len())
            .finish()
    }
}
