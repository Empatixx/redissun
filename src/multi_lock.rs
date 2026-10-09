use crate::error::{Error, Result};
use crate::lock::{acquire, is_held_by_current, LockGuard, Mode, Wait};
use crate::object::{millis, Key};
use crate::pending::{Pending, PendingTimeout};
use fred::interfaces::KeysInterface;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::time::Duration;
use tokio::time::Instant;
use uuid::Uuid;

const WAIT_PER_LOCK: Duration = Duration::from_millis(1500);

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

/// Several locks that are taken and released together, the counterpart of Redisson's `RedissonMultiLock`.
///
/// The locks are taken one after another in the order you give. When one of them cannot be taken, every lock taken so far is released and, while time is left, the round starts again from the first lock. Owners that take overlapping locks in different orders can therefore block each other until their wait runs out, so give the locks in the same order everywhere.
///
/// [`lock`](MultiLock::lock) tries rounds that wait 1.5 seconds per lock, or a random part of the lease when one is set, until one succeeds. With a lease and a timeout every lock is taken with a lease of twice the timeout, which is cut to the requested lease once all are held.
///
/// The watchdog keeps every lock alive while the guard exists, unless a lease is set. In Redis Cluster the locks may live on different nodes.
#[derive(Clone, Debug)]
pub struct MultiLock {
    targets: Vec<LockTarget>,
}

/// A pending [`MultiLock::lock`]. Await it to wait for every lock without a limit. Set `.lease(duration)` or `.timeout(duration)` first to change that.
#[must_use = "a pending lock does nothing until it is awaited"]
pub struct MultiLockRequest<'a> {
    multi: &'a MultiLock,
    lease: Option<Duration>,
}

impl<'a> MultiLockRequest<'a> {
    /// Sets an explicit lease for every lock, which disables the watchdog.
    pub fn lease(mut self, lease: Duration) -> Self {
        self.lease = Some(lease);
        self
    }

    /// Waits at most `timeout` for all locks. The call then resolves to `None` and holds nothing when the time runs out.
    pub fn timeout(self, timeout: Duration) -> PendingTimeout<'a, MultiLockGuard> {
        let (multi, lease) = (self.multi, self.lease);
        Pending::new(move |wait| async move {
            match wait {
                Some(wait) => multi.try_lock_with(Some(wait), lease).await,
                None => multi.lock_with(lease).await.map(Some),
            }
        })
        .timeout(timeout)
    }
}

impl<'a> IntoFuture for MultiLockRequest<'a> {
    type Output = Result<MultiLockGuard>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.multi.lock_with(self.lease))
    }
}

fn random_between(low: u128, high: u128) -> u128 {
    if high <= low {
        return low;
    }
    low + Uuid::new_v4().as_u128() % (high - low)
}

impl MultiLock {
    pub(crate) fn new(targets: Vec<LockTarget>) -> Result<Self> {
        if targets.is_empty() {
            return Err(Error::Config("a multi lock needs at least one lock".into()));
        }
        Ok(Self { targets })
    }

    /// Waits until every lock is taken. Add `.timeout(duration)` to wait at most that long, or `.lease(duration)` to set a lease instead of using the watchdog.
    pub fn lock(&self) -> MultiLockRequest<'_> {
        MultiLockRequest {
            multi: self,
            lease: None,
        }
    }

    /// Takes every lock when all are free; returns `None` at once otherwise and holds nothing.
    pub async fn try_lock(&self) -> Result<Option<MultiLockGuard>> {
        self.try_lock_with(None, None).await
    }

    /// Returns whether the calling task holds every lock.
    pub async fn is_held_by_current(&self) -> Result<bool> {
        for target in &self.targets {
            if !is_held_by_current(&target.key, target.mode).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn lock_with(&self, lease: Option<Duration>) -> Result<MultiLockGuard> {
        let base = WAIT_PER_LOCK * self.targets.len() as u32;
        loop {
            let wait = match lease {
                None => base,
                Some(lease) => {
                    let lease_ms = lease.as_millis();
                    let base_ms = base.as_millis();
                    let wait_ms = if lease_ms <= base_ms {
                        random_between(lease_ms / 2, lease_ms)
                    } else {
                        random_between(base_ms, lease_ms)
                    };
                    Duration::from_millis(wait_ms as u64)
                }
            };
            if let Some(guard) = self.try_lock_with(Some(wait), lease).await? {
                return Ok(guard);
            }
        }
    }

    async fn try_lock_with(
        &self,
        wait: Option<Duration>,
        lease: Option<Duration>,
    ) -> Result<Option<MultiLockGuard>> {
        if let Some(lease) = lease {
            millis(lease)?;
        }
        let wait = wait.filter(|wait| !wait.is_zero());
        let new_lease = lease.map(|lease| wait.map_or(lease, |wait| wait * 2));
        let mut time = Instant::now();
        let mut remain = wait;
        let lock_wait = remain;

        let mut acquired: Vec<LockGuard> = Vec::with_capacity(self.targets.len());
        let mut index = 0;
        while index < self.targets.len() {
            let target = &self.targets[index];
            let attempt = if wait.is_none() && lease.is_none() {
                acquire(&target.key, None, Wait::Once, target.mode).await
            } else {
                let await_time = match (lock_wait, remain) {
                    (Some(lock_wait), Some(remain)) => lock_wait.min(remain),
                    _ => Duration::ZERO,
                };
                acquire(&target.key, new_lease, Wait::For(await_time), target.mode).await
            };

            match attempt {
                Ok(Some(guard)) => {
                    acquired.push(guard);
                    index += 1;
                }
                _ => {
                    release_all(std::mem::take(&mut acquired)).await.ok();
                    if wait.is_none() {
                        return Ok(None);
                    }
                    index = 0;
                }
            }

            if let Some(left) = remain {
                let left = left.saturating_sub(time.elapsed());
                time = Instant::now();
                if left.is_zero() {
                    release_all(std::mem::take(&mut acquired)).await.ok();
                    return Ok(None);
                }
                remain = Some(left);
            }
        }

        if let Some(lease) = lease {
            for guard in &acquired {
                if let Some(key) = guard.key() {
                    let _: bool = key
                        .core
                        .redis()
                        .pexpire(key.redis_key(), millis(lease)?, None)
                        .await?;
                }
            }
        }
        Ok(Some(MultiLockGuard { guards: acquired }))
    }
}

async fn release_all(guards: impl IntoIterator<Item = LockGuard>) -> Result<()> {
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
