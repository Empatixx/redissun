pub(crate) mod fair;
mod guard;
mod watchdog;

pub use guard::LockGuard;

use crate::error::{Error, Result};
use crate::object::{millis as to_millis, HasKey, Key};
use crate::pending::{Pending, PendingTimeout};
use crate::shield::shielded;
use bytes::Bytes;
use fred::interfaces::{HashesInterface, KeysInterface};
use fred::types::scripts::Script;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
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

pub(crate) static FORCE_UNLOCK: LazyLock<Script> = LazyLock::new(|| {
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
        "local mode = redis.call('HGET', KEYS[1], 'mode')
        if mode == false then
            redis.call('HSET', KEYS[1], 'mode', 'read')
            redis.call('HSET', KEYS[1], ARGV[2], 1)
            redis.call('SET', KEYS[2] .. ':1', 1)
            redis.call('PEXPIRE', KEYS[2] .. ':1', ARGV[1])
            redis.call('PEXPIRE', KEYS[1], ARGV[1])
            return nil
        end
        if mode == 'read' or (mode == 'write' and redis.call('HEXISTS', KEYS[1], ARGV[3]) == 1) then
            local index = redis.call('HINCRBY', KEYS[1], ARGV[2], 1)
            local hold = KEYS[2] .. ':' .. index
            redis.call('SET', hold, 1)
            redis.call('PEXPIRE', hold, ARGV[1])
            local remain = redis.call('PTTL', KEYS[1])
            redis.call('PEXPIRE', KEYS[1], math.max(remain, ARGV[1]))
            return nil
        end
        return redis.call('PTTL', KEYS[1])",
    )
});

static WRITE_ACQUIRE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local mode = redis.call('HGET', KEYS[1], 'mode')
        if mode == false then
            redis.call('HSET', KEYS[1], 'mode', 'write')
            redis.call('HSET', KEYS[1], ARGV[2], 1)
            redis.call('PEXPIRE', KEYS[1], ARGV[1])
            return nil
        end
        if mode == 'write' and redis.call('HEXISTS', KEYS[1], ARGV[2]) == 1 then
            redis.call('HINCRBY', KEYS[1], ARGV[2], 1)
            redis.call('PEXPIRE', KEYS[1], ARGV[1])
            return nil
        end
        return redis.call('PTTL', KEYS[1])",
    )
});

static READ_RELEASE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local mode = redis.call('HGET', KEYS[1], 'mode')
        if mode == false then
            redis.call('PUBLISH', KEYS[2], ARGV[1])
            return nil
        end
        if redis.call('HEXISTS', KEYS[1], ARGV[2]) == 0 then
            return nil
        end
        local counter = redis.call('HINCRBY', KEYS[1], ARGV[2], -1)
        if counter == 0 then
            redis.call('HDEL', KEYS[1], ARGV[2])
        end
        redis.call('DEL', KEYS[3] .. ':' .. (counter + 1))
        if redis.call('HLEN', KEYS[1]) > 1 then
            local maxRemain = -3
            for _, field in ipairs(redis.call('HKEYS', KEYS[1])) do
                local held = tonumber(redis.call('HGET', KEYS[1], field))
                if held then
                    for i = held, 1, -1 do
                        local remain = redis.call('PTTL', KEYS[4] .. ':' .. field .. ':rwlock_timeout:' .. i)
                        maxRemain = math.max(remain, maxRemain)
                    end
                end
            end
            if maxRemain > 0 then
                redis.call('PEXPIRE', KEYS[1], maxRemain)
                return 0
            end
            if mode == 'write' then
                return 0
            end
        end
        redis.call('DEL', KEYS[1])
        redis.call('PUBLISH', KEYS[2], ARGV[1])
        return 1",
    )
});

static WRITE_RELEASE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local mode = redis.call('HGET', KEYS[1], 'mode')
        if mode == false then
            redis.call('PUBLISH', KEYS[2], ARGV[1])
            return nil
        end
        if mode ~= 'write' or redis.call('HEXISTS', KEYS[1], ARGV[3]) == 0 then
            return nil
        end
        local counter = redis.call('HINCRBY', KEYS[1], ARGV[3], -1)
        if counter > 0 then
            redis.call('PEXPIRE', KEYS[1], ARGV[2])
            return 0
        end
        redis.call('HDEL', KEYS[1], ARGV[3])
        if redis.call('HLEN', KEYS[1]) == 1 then
            redis.call('DEL', KEYS[1])
        else
            redis.call('HSET', KEYS[1], 'mode', 'read')
        end
        redis.call('PUBLISH', KEYS[2], ARGV[1])
        return 1",
    )
});

static READ_RENEW: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local held = redis.call('HGET', KEYS[1], ARGV[2])
        if held == false then
            return 0
        end
        redis.call('PEXPIRE', KEYS[1], math.max(redis.call('PTTL', KEYS[1]), ARGV[1]))
        for i = tonumber(held), 1, -1 do
            redis.call('PEXPIRE', KEYS[2] .. ':' .. i, ARGV[1])
        end
        return 1",
    )
});

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    Exclusive,
    Read,
    Write,
    Fair,
}

fn write_field(owner: &str) -> String {
    format!("{owner}:write")
}

fn timeout_prefix(key: &Key, owner: &str) -> String {
    format!("{}:{}:rwlock_timeout", key.redis_key(), owner)
}

pub(crate) fn channel(name: &str) -> String {
    format!("redissun__unlock__{name}")
}

fn lease_arg(duration: Duration) -> Result<Bytes> {
    Ok(Bytes::from(to_millis(duration)?.to_string()))
}

pub(crate) async fn release(key: &Key, owner: &str, lease: Duration, mode: Mode) -> Result<()> {
    let message = Bytes::from_static(b"unlocked");
    let outcome: Option<i64> = match mode {
        Mode::Read => {
            key.core
                .eval(
                    &READ_RELEASE,
                    vec![
                        key.redis_key(),
                        channel(key.name()),
                        timeout_prefix(key, owner),
                        key.redis_key(),
                    ],
                    vec![message, Bytes::from(owner.to_string())],
                )
                .await?
        }
        Mode::Write => {
            key.core
                .eval(
                    &WRITE_RELEASE,
                    vec![key.redis_key(), channel(key.name())],
                    vec![message, lease_arg(lease)?, Bytes::from(write_field(owner))],
                )
                .await?
        }
        Mode::Fair => {
            key.core
                .eval(
                    &fair::RELEASE,
                    fair::keys(key),
                    vec![
                        lease_arg(lease)?,
                        Bytes::from(owner.to_string()),
                        Bytes::from(fair::channel_prefix(key.name())),
                    ],
                )
                .await?
        }
        Mode::Exclusive => {
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
                    vec![key.redis_key(), timeout_prefix(key, owner)],
                    vec![lease_arg(lease)?, Bytes::from(owner.to_string())],
                )
                .await?
        }
        Mode::Write => {
            key.core
                .eval(
                    &RENEW,
                    vec![key.redis_key()],
                    vec![lease_arg(lease)?, Bytes::from(write_field(owner))],
                )
                .await?
        }
        Mode::Exclusive | Mode::Fair => {
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

/// A pending [`Lock::lock`]. Await it to wait for the lock without a limit. Set `.lease(duration)` or `.timeout(duration)` first to change that.
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
        Pending::new(move |wait| acquire(key, lease, wait, mode)).timeout(timeout)
    }
}

impl<'a> IntoFuture for LockRequest<'a> {
    type Output = Result<LockGuard>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        let (key, mode, lease) = (self.key, self.mode, self.lease);
        Box::pin(async move { acquire(key, lease, None, mode).await?.ok_or(Error::Timeout) })
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

    /// Waits until the lock is acquired. Add `.timeout(duration)` to wait at most that long, or `.lease(duration)` to set a lease instead of using the watchdog.
    pub fn lock(&self) -> LockRequest<'_> {
        LockRequest::new(&self.key, Mode::Exclusive)
    }

    /// Acquires the lock when it is free; returns `None` immediately otherwise.
    pub async fn try_lock(&self) -> Result<Option<LockGuard>> {
        acquire(&self.key, None, Some(Duration::ZERO), Mode::Exclusive).await
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
    lease: Option<Duration>,
    wait: Option<Duration>,
    mode: Mode,
) -> Result<Option<LockGuard>> {
    let core = &key.core;
    let owner = core.owner();
    let watchdog = lease.is_none();
    let lease = lease.unwrap_or(core.lock_lease);
    let deadline = wait.map(|wait| Instant::now() + wait);
    let wake_channel = match mode {
        Mode::Fair => fair::channel(key.name(), &owner),
        _ => channel(key.name()),
    };
    let subscription = match wait {
        Some(wait) if wait.is_zero() => None,
        _ => Some(core.pubsub.subscribe(&wake_channel).await?),
    };
    let mut queued = (mode == Mode::Fair).then(|| fair::Queued::new(key.clone(), owner.clone()));
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
                        vec![key.redis_key(), timeout_prefix(&key, &owner)],
                        vec![
                            argument,
                            Bytes::from(owner.clone()),
                            Bytes::from(write_field(&owner)),
                        ],
                    ),
                    Mode::Write => (
                        &WRITE_ACQUIRE,
                        vec![key.redis_key()],
                        vec![argument, Bytes::from(write_field(&owner))],
                    ),
                    Mode::Fair => (
                        &fair::ACQUIRE,
                        fair::keys(&key),
                        vec![
                            argument,
                            Bytes::from(owner.clone()),
                            Bytes::from(fair::SLOT_MILLIS.to_string()),
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
            Attempt::Acquired(guard) => {
                if let Some(queued) = queued.as_mut() {
                    queued.disarm();
                }
                return Ok(Some(guard));
            }
            Attempt::Busy(pttl) => pttl,
        };

        let remaining = match deadline {
            Some(deadline) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    if let Some(queued) = queued.as_mut() {
                        queued.cancel().await;
                    }
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
