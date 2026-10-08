mod guard;
mod watchdog;

pub use guard::LockGuard;

use crate::error::{Error, Result};
use crate::object::{millis as to_millis, HasKey, Key};
use crate::shield::shielded;
use bytes::Bytes;
use fred::interfaces::{HashesInterface, KeysInterface};
use fred::types::scripts::Script;
use std::fmt;
use std::sync::LazyLock;
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

static READ_ACQUIRE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('EXISTS', KEYS[1]) == 1 and redis.call('HEXISTS', KEYS[1], ARGV[2]) == 0 then
            return redis.call('PTTL', KEYS[1])
        end
        redis.call('INCR', KEYS[3])
        redis.call('PEXPIRE', KEYS[3], ARGV[1])
        redis.call('SADD', KEYS[2], ARGV[2])
        if redis.call('PTTL', KEYS[2]) < tonumber(ARGV[1]) then
            redis.call('PEXPIRE', KEYS[2], ARGV[1])
        end
        return nil",
    )
});

static WRITE_ACQUIRE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('EXISTS', KEYS[1]) == 1 then
            if redis.call('HEXISTS', KEYS[1], ARGV[2]) == 1 then
                redis.call('HINCRBY', KEYS[1], ARGV[2], 1)
                redis.call('PEXPIRE', KEYS[1], ARGV[1])
                return nil
            end
            return redis.call('PTTL', KEYS[1])
        end
        local wait = 0
        for _, member in ipairs(redis.call('SMEMBERS', KEYS[2])) do
            local ttl = redis.call('PTTL', ARGV[3] .. member)
            if ttl == -2 then
                redis.call('SREM', KEYS[2], member)
            elseif ttl == -1 then
                wait = math.max(wait, 1000)
            else
                wait = math.max(wait, ttl, 1)
            end
        end
        if wait > 0 then
            return wait
        end
        redis.call('HSET', KEYS[1], ARGV[2], 1)
        redis.call('PEXPIRE', KEYS[1], ARGV[1])
        return nil",
    )
});

static READ_RELEASE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if not redis.call('GET', KEYS[3]) then
            return nil
        end
        local count = redis.call('DECR', KEYS[3])
        if count > 0 then
            redis.call('PEXPIRE', KEYS[3], ARGV[1])
            return 0
        end
        redis.call('DEL', KEYS[3])
        redis.call('SREM', KEYS[2], ARGV[2])
        redis.call('PUBLISH', ARGV[3], 'unlocked')
        return 1",
    )
});

static READ_RENEW: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('EXISTS', KEYS[1]) == 1 then
            redis.call('PEXPIRE', KEYS[1], ARGV[1])
            return 1
        end
        return 0",
    )
});

pub(crate) static RW_FORCE_UNLOCK: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local released = redis.call('DEL', KEYS[1])
        for _, member in ipairs(redis.call('SMEMBERS', KEYS[2])) do
            released = released + redis.call('DEL', ARGV[2] .. member)
        end
        released = released + redis.call('DEL', KEYS[2])
        if released > 0 then
            redis.call('PUBLISH', ARGV[1], 'unlocked')
            return 1
        end
        return 0",
    )
});

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    Exclusive,
    Read,
    Write,
}

pub(crate) fn readers_key(key: &Key) -> String {
    format!("{}:readers", key.redis_key())
}

pub(crate) fn read_prefix(key: &Key) -> String {
    format!("{}:r:", key.redis_key())
}

fn read_key(key: &Key, owner: &str) -> String {
    format!("{}{}", read_prefix(key), owner)
}

pub(crate) fn channel(name: &str) -> String {
    format!("redissun__unlock__{name}")
}

fn lease_arg(duration: Duration) -> Result<Bytes> {
    Ok(Bytes::from(to_millis(duration)?.to_string()))
}

pub(crate) async fn release(key: &Key, owner: &str, lease: Duration, mode: Mode) -> Result<()> {
    let outcome: Option<i64> = match mode {
        Mode::Read => {
            key.core
                .eval(
                    &READ_RELEASE,
                    vec![key.redis_key(), readers_key(key), read_key(key, owner)],
                    vec![
                        lease_arg(lease)?,
                        Bytes::from(owner.to_string()),
                        Bytes::from(channel(key.name())),
                    ],
                )
                .await?
        }
        _ => {
            key.core
                .eval(
                    &RELEASE,
                    vec![key.redis_key()],
                    vec![
                        lease_arg(lease)?,
                        Bytes::from(owner.to_string()),
                        Bytes::from(channel(key.name())),
                    ],
                )
                .await?
        }
    };
    outcome.map(|_| ()).ok_or(Error::LockNotHeld)
}

pub(crate) async fn renew(key: &Key, owner: &str, lease: Duration, mode: Mode) -> Result<bool> {
    let renewed: i64 = match mode {
        Mode::Read => {
            key.core
                .eval(
                    &READ_RENEW,
                    vec![read_key(key, owner)],
                    vec![lease_arg(lease)?],
                )
                .await?
        }
        _ => {
            key.core
                .eval(
                    &RENEW,
                    vec![key.redis_key()],
                    vec![lease_arg(lease)?, Bytes::from(owner.to_string())],
                )
                .await?
        }
    };
    Ok(renewed == 1)
}

enum Attempt {
    Acquired(LockGuard),
    Busy(i64),
}

/// Options for [`Lock::lock_with`].
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct LockOptions {
    /// Time after which the lock frees itself. When unset the lock is renewed by a watchdog instead.
    pub lease: Option<Duration>,
    /// How long to wait for the lock. When unset the call waits indefinitely.
    pub wait: Option<Duration>,
}

impl LockOptions {
    /// Options that wait indefinitely and use the watchdog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets an explicit lease, which disables the watchdog.
    pub fn lease(mut self, lease: Duration) -> Self {
        self.lease = Some(lease);
        self
    }

    /// Sets the maximum time to wait for the lock.
    pub fn wait(mut self, wait: Duration) -> Self {
        self.wait = Some(wait);
        self
    }
}

/// A reentrant distributed lock. The owner is the client together with the current tokio task.
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

impl HasKey for Lock {
    fn key(&self) -> &Key {
        &self.key
    }
}

impl Lock {
    pub(crate) fn new(key: Key) -> Self {
        Self { key }
    }

    /// Waits until the lock is acquired.
    pub async fn lock(&self) -> Result<LockGuard> {
        self.lock_with(LockOptions::new())
            .await?
            .ok_or(Error::Timeout)
    }

    /// Acquires the lock when it is free; returns `None` immediately otherwise.
    pub async fn try_lock(&self) -> Result<Option<LockGuard>> {
        self.lock_with(LockOptions::new().wait(Duration::ZERO))
            .await
    }

    /// Waits up to `wait` for the lock; returns `None` when the time runs out.
    pub async fn lock_for(&self, wait: Duration) -> Result<Option<LockGuard>> {
        self.lock_with(LockOptions::new().wait(wait)).await
    }

    /// Acquires the lock according to `options`; returns `None` when the wait runs out.
    pub async fn lock_with(&self, options: LockOptions) -> Result<Option<LockGuard>> {
        acquire(&self.key, options, Mode::Exclusive).await
    }

    /// Returns whether any owner holds the lock.
    pub async fn is_locked(&self) -> Result<bool> {
        let found: i64 = self.key.core.redis().exists(self.key.redis_key()).await?;
        Ok(found > 0)
    }

    /// Returns whether the calling task holds the lock.
    pub async fn is_held_by_current(&self) -> Result<bool> {
        let held: bool = self
            .key
            .core
            .redis()
            .hexists(self.key.redis_key(), self.key.core.owner())
            .await?;
        Ok(held)
    }

    /// Number of reentrant holds by the calling task.
    pub async fn hold_count(&self) -> Result<u64> {
        let count: Option<u64> = self
            .key
            .core
            .redis()
            .hget(self.key.redis_key(), self.key.core.owner())
            .await?;
        Ok(count.unwrap_or(0))
    }

    /// Releases the lock regardless of its owner; returns whether it was held.
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

pub(crate) async fn acquire(
    key: &Key,
    options: LockOptions,
    mode: Mode,
) -> Result<Option<LockGuard>> {
    let core = &key.core;
    let owner = core.owner();
    let watchdog = options.lease.is_none();
    let lease = options.lease.unwrap_or(core.lock_lease);
    let deadline = options.wait.map(|wait| Instant::now() + wait);
    let subscription = match options.wait {
        Some(wait) if wait.is_zero() => None,
        _ => Some(core.pubsub.subscribe(&channel(key.name())).await?),
    };
    let local = Notify::new();
    let notify: &Notify = subscription.as_ref().map_or(&local, |s| s.notify());

    loop {
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let attempt = {
            let key = key.clone();
            let owner = owner.clone();
            let argument = lease_arg(lease)?;
            shielded(async move {
                let (script, keys, args): (&Script, Vec<String>, Vec<Bytes>) = match mode {
                    Mode::Exclusive => (
                        &ACQUIRE,
                        vec![key.redis_key()],
                        vec![argument, Bytes::from(owner.clone())],
                    ),
                    Mode::Read => (
                        &READ_ACQUIRE,
                        vec![key.redis_key(), readers_key(&key), read_key(&key, &owner)],
                        vec![argument, Bytes::from(owner.clone())],
                    ),
                    Mode::Write => (
                        &WRITE_ACQUIRE,
                        vec![key.redis_key(), readers_key(&key)],
                        vec![
                            argument,
                            Bytes::from(owner.clone()),
                            Bytes::from(read_prefix(&key)),
                        ],
                    ),
                };
                let pttl: Option<i64> = key.core.eval(script, keys, args).await?;
                Ok(match pttl {
                    Some(pttl) => Attempt::Busy(pttl),
                    None => {
                        let cancel = CancellationToken::new();
                        if watchdog {
                            watchdog::spawn(
                                key.clone(),
                                owner.clone(),
                                lease,
                                mode,
                                cancel.clone(),
                            );
                        }
                        Attempt::Acquired(LockGuard::new(key, owner, lease, mode, cancel))
                    }
                })
            })
            .await?
        };
        let pttl = match attempt {
            Attempt::Acquired(guard) => return Ok(Some(guard)),
            Attempt::Busy(pttl) => pttl,
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
