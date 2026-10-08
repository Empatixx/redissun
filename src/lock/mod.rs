mod guard;
mod watchdog;

pub use guard::LockGuard;

use crate::error::{Error, Result};
use crate::object::{HasKey, Key};
use bytes::Bytes;
use fred::interfaces::{HashesInterface, KeysInterface};
use fred::types::scripts::Script;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const NO_EXPIRY_POLL: Duration = Duration::from_secs(1);

static ACQUIRE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('EXISTS', KEYS[1]) == 0 then
            redis.call('HSET', KEYS[1], ARGV[2], 1)
            redis.call('PEXPIRE', KEYS[1], ARGV[1])
            return nil
        end
        if redis.call('HEXISTS', KEYS[1], ARGV[2]) == 1 then
            redis.call('HINCRBY', KEYS[1], ARGV[2], 1)
            redis.call('PEXPIRE', KEYS[1], ARGV[1])
            return nil
        end
        return redis.call('PTTL', KEYS[1])",
    )
});

static RELEASE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('HEXISTS', KEYS[1], ARGV[2]) == 0 then
            return nil
        end
        local count = redis.call('HINCRBY', KEYS[1], ARGV[2], -1)
        if count > 0 then
            redis.call('PEXPIRE', KEYS[1], ARGV[1])
            return 0
        end
        redis.call('DEL', KEYS[1])
        redis.call('PUBLISH', ARGV[3], 'unlocked')
        return 1",
    )
});

static RENEW: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('HEXISTS', KEYS[1], ARGV[2]) == 1 then
            redis.call('PEXPIRE', KEYS[1], ARGV[1])
            return 1
        end
        return 0",
    )
});

static FORCE_UNLOCK: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('DEL', KEYS[1]) == 1 then
            redis.call('PUBLISH', ARGV[1], 'unlocked')
            return 1
        end
        return 0",
    )
});

fn channel(name: &str) -> String {
    format!("redgrid__unlock__{name}")
}

fn millis(duration: Duration) -> Bytes {
    Bytes::from((duration.as_millis() as i64).to_string())
}

pub(crate) async fn release(key: &Key, owner: &str, lease: Duration) -> Result<()> {
    let outcome: Option<i64> = key
        .core
        .eval(
            &RELEASE,
            vec![key.redis_key()],
            vec![
                millis(lease),
                Bytes::from(owner.to_string()),
                Bytes::from(channel(key.name())),
            ],
        )
        .await?;
    outcome.map(|_| ()).ok_or(Error::LockNotHeld)
}

pub(crate) async fn renew(key: &Key, owner: &str, lease: Duration) -> Result<bool> {
    let renewed: i64 = key
        .core
        .eval(
            &RENEW,
            vec![key.redis_key()],
            vec![millis(lease), Bytes::from(owner.to_string())],
        )
        .await?;
    Ok(renewed == 1)
}

#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct LockOptions {
    pub lease: Option<Duration>,
    pub wait: Option<Duration>,
}

impl LockOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn lease(mut self, lease: Duration) -> Self {
        self.lease = Some(lease);
        self
    }

    pub fn wait(mut self, wait: Duration) -> Self {
        self.wait = Some(wait);
        self
    }
}

#[derive(Clone)]
pub struct Lock {
    key: Key,
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

    pub async fn lock(&self) -> Result<LockGuard> {
        self.lock_with(LockOptions::new())
            .await?
            .ok_or(Error::Timeout)
    }

    pub async fn try_lock(&self) -> Result<Option<LockGuard>> {
        self.lock_with(LockOptions::new().wait(Duration::ZERO))
            .await
    }

    pub async fn lock_for(&self, wait: Duration) -> Result<Option<LockGuard>> {
        self.lock_with(LockOptions::new().wait(wait)).await
    }

    pub async fn lock_with(&self, options: LockOptions) -> Result<Option<LockGuard>> {
        let core = &self.key.core;
        let owner = core.owner();
        let watchdog = options.lease.is_none();
        let lease = options.lease.unwrap_or(core.lock_lease);
        let deadline = options.wait.map(|wait| Instant::now() + wait);
        let notify = match options.wait {
            Some(wait) if wait.is_zero() => Arc::new(Notify::new()),
            _ => core.pubsub.subscribe(&channel(self.key.name())).await?,
        };

        loop {
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let pttl: Option<i64> = core
                .eval(
                    &ACQUIRE,
                    vec![self.key.redis_key()],
                    vec![millis(lease), Bytes::from(owner.clone())],
                )
                .await?;

            let Some(pttl) = pttl else {
                let cancel = CancellationToken::new();
                if watchdog {
                    watchdog::spawn(self.key.clone(), owner.clone(), lease, cancel.clone());
                }
                return Ok(Some(LockGuard::new(self.key.clone(), owner, lease, cancel)));
            };

            let remaining = match deadline {
                Some(deadline) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Ok(None);
                    }
                    Some(remaining)
                }
                None => None,
            };
            let until_expiry = if pttl > 0 {
                Duration::from_millis(pttl as u64)
            } else {
                NO_EXPIRY_POLL
            };
            let pause = remaining.map_or(until_expiry, |r| r.min(until_expiry));
            let _ = tokio::time::timeout(pause, notified).await;
        }
    }

    pub async fn is_locked(&self) -> Result<bool> {
        let found: i64 = self.key.core.redis().exists(self.key.redis_key()).await?;
        Ok(found > 0)
    }

    pub async fn is_held_by_current(&self) -> Result<bool> {
        let held: bool = self
            .key
            .core
            .redis()
            .hexists(self.key.redis_key(), self.key.core.owner())
            .await?;
        Ok(held)
    }

    pub async fn hold_count(&self) -> Result<u64> {
        let count: Option<u64> = self
            .key
            .core
            .redis()
            .hget(self.key.redis_key(), self.key.core.owner())
            .await?;
        Ok(count.unwrap_or(0))
    }

    pub async fn force_unlock(&self) -> Result<bool> {
        let removed: i64 = self
            .key
            .core
            .eval(
                &FORCE_UNLOCK,
                vec![self.key.redis_key()],
                vec![Bytes::from(channel(self.key.name()))],
            )
            .await?;
        Ok(removed == 1)
    }
}
