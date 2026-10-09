pub(crate) mod fair;
mod guard;
pub(crate) mod sync;
mod watchdog;

pub use guard::LockGuard;
pub(crate) use sync::LockSettings;

use crate::error::{Error, Result};
use crate::object::{millis as to_millis, tagged, HasKey, Key};
use crate::pending::{Pending, PendingTimeout};
use crate::shield::shielded;
use bytes::Bytes;
use fred::interfaces::{HashesInterface, KeysInterface};
use fred::types::scripts::Script;
use fred::types::Value;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::LazyLock;
use std::time::Duration;
use sync::{synced_eval, with_sync_retry, UNLOCK_LATCH_TTL};
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub(crate) const REQUEST_LATCH_TTL: Duration = Duration::from_secs(30);

const UNLOCK_MESSAGE: &str = "0";
const READ_UNLOCK_MESSAGE: &str = "1";

static ACQUIRE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if ((redis.call('exists', KEYS[1]) == 0)
                or (redis.call('hexists', KEYS[1], ARGV[2]) == 1)) then
            redis.call('hincrby', KEYS[1], ARGV[2], 1)
            redis.call('pexpire', KEYS[1], ARGV[1])
            return nil
        end
        return redis.call('pttl', KEYS[1])",
    )
});

static FENCED_ACQUIRE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if (redis.call('exists', KEYS[1]) == 0
                or (redis.call('hexists', KEYS[1], ARGV[2]) == 1)) then
            local token = redis.call('incr', KEYS[2])
            redis.call('hincrby', KEYS[1], ARGV[2], 1)
            redis.call('pexpire', KEYS[1], ARGV[1])
            return {-1, token}
        end
        return {redis.call('pttl', KEYS[1]), -1}",
    )
});

static RELEASE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local val = redis.call('get', KEYS[2])
        if val ~= false then
            return tonumber(val)
        end
        if (redis.call('hexists', KEYS[1], ARGV[2]) == 0) then
            return nil
        end
        local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1)
        if (counter > 0) then
            redis.call('pexpire', KEYS[1], ARGV[1])
            redis.call('set', KEYS[2], 0, 'px', ARGV[5])
            return 0
        else
            redis.call('del', KEYS[1])
            redis.call('publish', ARGV[3], ARGV[4])
            redis.call('set', KEYS[2], 1, 'px', ARGV[5])
            return 1
        end",
    )
});

static RENEW: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then
            redis.call('pexpire', KEYS[1], ARGV[1])
            return 1
        end
        return 0",
    )
});

static FORCE_UNLOCK: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if (redis.call('del', KEYS[1]) == 1) then
            redis.call('publish', ARGV[1], ARGV[2])
            return 1
        else
            return 0
        end",
    )
});

static READ_ACQUIRE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local mode = redis.call('hget', KEYS[1], 'mode')
        if (mode == false) then
            redis.call('hset', KEYS[1], 'mode', 'read')
            redis.call('hset', KEYS[1], ARGV[2], 1)
            redis.call('set', KEYS[2] .. ':1', 1)
            redis.call('pexpire', KEYS[2] .. ':1', ARGV[1])
            redis.call('pexpire', KEYS[1], ARGV[1])
            return nil
        end
        if (mode == 'read') or (mode == 'write' and redis.call('hexists', KEYS[1], ARGV[3]) == 1) then
            local ind = redis.call('hincrby', KEYS[1], ARGV[2], 1)
            local key = KEYS[2] .. ':' .. ind
            redis.call('set', key, 1)
            redis.call('pexpire', key, ARGV[1])
            local remainTime = redis.call('pttl', KEYS[1])
            redis.call('pexpire', KEYS[1], math.max(remainTime, ARGV[1]))
            return nil
        end
        return redis.call('pttl', KEYS[1])",
    )
});

static WRITE_ACQUIRE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local mode = redis.call('hget', KEYS[1], 'mode')
        if (mode == false) then
            redis.call('hset', KEYS[1], 'mode', 'write')
            redis.call('hset', KEYS[1], ARGV[2], 1)
            redis.call('pexpire', KEYS[1], ARGV[1])
            return nil
        end
        if (mode == 'write') then
            if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then
                redis.call('hincrby', KEYS[1], ARGV[2], 1)
                local currentExpire = redis.call('pttl', KEYS[1])
                redis.call('pexpire', KEYS[1], currentExpire + ARGV[1])
                return nil
            end
        end
        return redis.call('pttl', KEYS[1])",
    )
});

static READ_RELEASE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local val = redis.call('get', KEYS[5])
        if val ~= false then
            return tonumber(val)
        end
        local mode = redis.call('hget', KEYS[1], 'mode')
        if (mode == false) then
            redis.call('publish', KEYS[2], ARGV[1])
            redis.call('set', KEYS[5], 1, 'px', ARGV[3])
            return nil
        end
        local lockExists = redis.call('hexists', KEYS[1], ARGV[2])
        if (lockExists == 0) then
            return nil
        end
        local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1)
        if (counter == 0) then
            redis.call('hdel', KEYS[1], ARGV[2])
        end
        redis.call('del', KEYS[3] .. ':' .. (counter + 1))
        if (redis.call('hlen', KEYS[1]) > 1) then
            local maxRemainTime = -3
            local keys = redis.call('hkeys', KEYS[1])
            for n, key in ipairs(keys) do
                counter = tonumber(redis.call('hget', KEYS[1], key))
                if type(counter) == 'number' then
                    for i = counter, 1, -1 do
                        local remainTime = redis.call('pttl', KEYS[4] .. ':' .. key .. ':rwlock_timeout:' .. i)
                        maxRemainTime = math.max(remainTime, maxRemainTime)
                    end
                end
            end
            if maxRemainTime > 0 then
                redis.call('pexpire', KEYS[1], maxRemainTime)
                redis.call('set', KEYS[5], 0, 'px', ARGV[3])
                return 0
            end
            if mode == 'write' then
                redis.call('set', KEYS[5], 0, 'px', ARGV[3])
                return 0
            end
        end
        redis.call('del', KEYS[1])
        redis.call('publish', KEYS[2], ARGV[1])
        redis.call('set', KEYS[5], 1, 'px', ARGV[3])
        return 1",
    )
});

static WRITE_RELEASE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local val = redis.call('get', KEYS[3])
        if val ~= false then
            return tonumber(val)
        end
        local mode = redis.call('hget', KEYS[1], 'mode')
        if (mode == false) then
            redis.call('publish', KEYS[2], ARGV[1])
            redis.call('set', KEYS[3], 1, 'px', ARGV[4])
            return nil
        end
        if (mode == 'write') then
            local lockExists = redis.call('hexists', KEYS[1], ARGV[3])
            if (lockExists == 0) then
                return nil
            else
                local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1)
                if (counter > 0) then
                    redis.call('pexpire', KEYS[1], ARGV[2])
                    redis.call('set', KEYS[3], 0, 'px', ARGV[4])
                    return 0
                else
                    redis.call('hdel', KEYS[1], ARGV[3])
                    if (redis.call('hlen', KEYS[1]) == 1) then
                        redis.call('del', KEYS[1])
                        redis.call('publish', KEYS[2], ARGV[1])
                    else
                        redis.call('hset', KEYS[1], 'mode', 'read')
                    end
                    redis.call('set', KEYS[3], 1, 'px', ARGV[4])
                    return 1
                end
            end
        end
        return nil",
    )
});

static READ_RENEW: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local counter = redis.call('hget', KEYS[1], ARGV[2])
        if (counter ~= false) then
            for c = counter, 1, -1 do
                redis.call('pexpire', KEYS[2] .. ':' .. ARGV[2] .. ':rwlock_timeout:' .. c, ARGV[1])
            end
            redis.call('pexpire', KEYS[1], ARGV[1])
            return 1
        end
        return 0",
    )
});

pub(crate) static READ_FORCE_UNLOCK: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if (redis.call('hget', KEYS[1], 'mode') == 'read') then
            redis.call('del', KEYS[1])
            redis.call('publish', KEYS[2], ARGV[1])
            return 1
        end
        return 0",
    )
});

pub(crate) static WRITE_FORCE_UNLOCK: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if (redis.call('hget', KEYS[1], 'mode') == 'write') then
            redis.call('del', KEYS[1])
            redis.call('publish', KEYS[2], ARGV[1])
            return 1
        end
        return 0",
    )
});

pub(crate) static READ_IS_LOCKED: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local mode = redis.call('hget', KEYS[1], 'mode')
        if (mode == 'read') or (mode == 'write' and redis.call('hlen', KEYS[1]) > 2) then
            return 1
        end
        return 0",
    )
});

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    Exclusive,
    Read,
    Write,
    Fair,
    Fenced,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Wait {
    Once,
    For(Duration),
    Forever,
}

pub(crate) fn write_field(owner: &str) -> String {
    format!("{owner}:write")
}

fn timeout_prefix(key: &Key, owner: &str) -> String {
    format!("{}:{}:rwlock_timeout", key.redis_key(), owner)
}

pub(crate) fn token_key(key: &Key) -> String {
    format!("redissun__lock_token:{}", key.redis_key())
}

pub(crate) fn channel(name: &str) -> String {
    format!("redissun__unlock__{name}")
}

fn lease_arg(duration: Duration) -> Result<Bytes> {
    Ok(Bytes::from(to_millis(duration)?.to_string()))
}

pub(crate) fn request_latch(key: &Key, request: &str) -> String {
    format!("redissun__request_latch:{}:{request}", tagged(key.name()))
}

fn unlock_latch(key: &Key, request: &str) -> String {
    format!("redissun__unlock_latch:{}:{request}", tagged(key.name()))
}

pub(crate) fn held_field(owner: &str, mode: Mode) -> String {
    match mode {
        Mode::Write => write_field(owner),
        _ => owner.to_string(),
    }
}

pub(crate) async fn release(key: &Key, owner: &str, lease: Duration, mode: Mode) -> Result<()> {
    let request = Uuid::new_v4().to_string();
    with_sync_retry(|| release_inner(key, owner, lease, mode, &request)).await
}

async fn release_inner(
    key: &Key,
    owner: &str,
    lease: Duration,
    mode: Mode,
    request: &str,
) -> Result<()> {
    let latch = unlock_latch(key, request);
    let latch_ttl = Bytes::from(UNLOCK_LATCH_TTL.as_millis().to_string());
    let (script, keys, args): (&Script, Vec<String>, Vec<Bytes>) = match mode {
        Mode::Read => (
            &READ_RELEASE,
            vec![
                key.redis_key(),
                channel(key.name()),
                timeout_prefix(key, owner),
                key.redis_key(),
                latch.clone(),
            ],
            vec![
                Bytes::from_static(UNLOCK_MESSAGE.as_bytes()),
                Bytes::from(owner.to_string()),
                latch_ttl,
            ],
        ),
        Mode::Write => (
            &WRITE_RELEASE,
            vec![key.redis_key(), channel(key.name()), latch.clone()],
            vec![
                Bytes::from_static(READ_UNLOCK_MESSAGE.as_bytes()),
                lease_arg(lease)?,
                Bytes::from(write_field(owner)),
                latch_ttl,
            ],
        ),
        Mode::Fair => (
            &fair::RELEASE,
            [fair::keys(key), vec![latch.clone()]].concat(),
            vec![
                Bytes::from_static(UNLOCK_MESSAGE.as_bytes()),
                lease_arg(lease)?,
                Bytes::from(owner.to_string()),
                fair::now_millis(),
                latch_ttl,
                Bytes::from(fair::channel_prefix(key.name())),
            ],
        ),
        Mode::Exclusive | Mode::Fenced => (
            &RELEASE,
            vec![key.redis_key(), latch.clone()],
            vec![
                lease_arg(lease)?,
                Bytes::from(owner.to_string()),
                Bytes::from(channel(key.name())),
                Bytes::from_static(UNLOCK_MESSAGE.as_bytes()),
                latch_ttl,
            ],
        ),
    };
    let outcome: Option<i64> = synced_eval(key, script, keys, args, false).await?;
    let client = key.core.redis().clone();
    tokio::spawn(async move {
        let _: std::result::Result<i64, _> = client.del(latch).await;
    });
    outcome.map(|_| ()).ok_or(Error::LockNotHeld)
}

pub(crate) async fn renew(key: &Key, owner: &str, lease: Duration, mode: Mode) -> Result<bool> {
    let (script, keys, args): (&Script, Vec<String>, Vec<Bytes>) = match mode {
        Mode::Read => (
            &READ_RENEW,
            vec![key.redis_key(), key.redis_key()],
            vec![lease_arg(lease)?, Bytes::from(owner.to_string())],
        ),
        _ => (
            &RENEW,
            vec![key.redis_key()],
            vec![lease_arg(lease)?, Bytes::from(held_field(owner, mode))],
        ),
    };
    let renewed: i64 = synced_eval(key, script, keys, args, true).await?;
    Ok(renewed == 1)
}

pub(crate) async fn force_unlock(
    key: &Key,
    script: &Script,
    keys: Vec<String>,
    args: Vec<Bytes>,
) -> Result<bool> {
    with_sync_retry(|| async {
        let removed: i64 = synced_eval(key, script, keys.clone(), args.clone(), true).await?;
        Ok(removed == 1)
    })
    .await
}

pub(crate) fn unlock_message() -> Bytes {
    Bytes::from_static(UNLOCK_MESSAGE.as_bytes())
}

pub(crate) fn read_unlock_message() -> Bytes {
    Bytes::from_static(READ_UNLOCK_MESSAGE.as_bytes())
}

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

impl From<Lock> for crate::multi_lock::LockTarget {
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

pub(crate) async fn is_locked(key: &Key) -> Result<bool> {
    let found: i64 = key.core.redis().exists(key.redis_key()).await?;
    Ok(found > 0)
}

pub(crate) async fn is_held_by_current(key: &Key, mode: Mode) -> Result<bool> {
    let field = held_field(&key.core.owner(), mode);
    let held: bool = key.core.redis().hexists(key.redis_key(), field).await?;
    Ok(held)
}

pub(crate) async fn hold_count(key: &Key, mode: Mode) -> Result<u64> {
    let field = held_field(&key.core.owner(), mode);
    let count: Option<u64> = key.core.redis().hget(key.redis_key(), field).await?;
    Ok(count.unwrap_or(0))
}

enum Outcome {
    Acquired(Option<u64>),
    Busy(i64),
}

fn outcome(mode: Mode, value: Value) -> Result<Outcome> {
    if mode == Mode::Fenced {
        let reply: Vec<i64> = value.convert()?;
        return match reply.as_slice() {
            [-1, token] => Ok(Outcome::Acquired(Some(*token as u64))),
            [ttl, ..] => Ok(Outcome::Busy(*ttl)),
            [] => Err(Error::Redis(
                "empty reply from the fenced lock script".into(),
            )),
        };
    }
    let ttl: Option<i64> = value.convert()?;
    Ok(ttl.map_or(Outcome::Acquired(None), Outcome::Busy))
}

fn acquire_call(
    key: &Key,
    owner: &str,
    lease: Duration,
    mode: Mode,
    wait: Wait,
    fair_wait: Duration,
) -> Result<(&'static Script, Vec<String>, Vec<Bytes>)> {
    let argument = lease_arg(lease)?;
    Ok(match mode {
        Mode::Exclusive => (
            &ACQUIRE,
            vec![key.redis_key()],
            vec![argument, Bytes::from(owner.to_string())],
        ),
        Mode::Read => (
            &READ_ACQUIRE,
            vec![key.redis_key(), timeout_prefix(key, owner)],
            vec![
                argument,
                Bytes::from(owner.to_string()),
                Bytes::from(write_field(owner)),
            ],
        ),
        Mode::Write => (
            &WRITE_ACQUIRE,
            vec![key.redis_key()],
            vec![argument, Bytes::from(write_field(owner))],
        ),
        Mode::Fenced => (
            &FENCED_ACQUIRE,
            vec![key.redis_key(), token_key(key)],
            vec![argument, Bytes::from(owner.to_string())],
        ),
        Mode::Fair if wait == Wait::Once => (
            &fair::TRY_ACQUIRE,
            fair::keys(key),
            vec![
                argument,
                Bytes::from(owner.to_string()),
                fair::now_millis(),
                fair::wait_arg(fair_wait),
            ],
        ),
        Mode::Fair => (
            &fair::ACQUIRE,
            fair::keys(key),
            vec![
                argument,
                Bytes::from(owner.to_string()),
                fair::wait_arg(fair_wait),
                fair::now_millis(),
            ],
        ),
    })
}

async fn try_acquire(
    key: &Key,
    owner: &str,
    lease: Duration,
    mode: Mode,
    wait: Wait,
    fair_wait: Duration,
) -> Result<Outcome> {
    with_sync_retry(|| async {
        let (script, keys, args) = acquire_call(key, owner, lease, mode, wait, fair_wait)?;
        match synced_eval::<Value>(key, script, keys, args, false).await {
            Err(Error::NoSyncedReplicas) => {
                let request = Uuid::new_v4().to_string();
                let _ = release_inner(key, owner, lease, mode, &request).await;
                Err(Error::NoSyncedReplicas)
            }
            Err(error) => Err(error),
            Ok(value) => outcome(mode, value),
        }
    })
    .await
}

enum Attempt {
    Acquired(LockGuard),
    Busy(i64),
}

#[allow(clippy::too_many_arguments)]
async fn attempt(
    key: &Key,
    owner: &str,
    lease: Duration,
    watchdog: bool,
    mode: Mode,
    wait: Wait,
    fair_wait: Duration,
) -> Result<Attempt> {
    let key = key.clone();
    let owner = owner.to_string();
    shielded(async move {
        Ok(
            match try_acquire(&key, &owner, lease, mode, wait, fair_wait).await? {
                Outcome::Busy(ttl) => Attempt::Busy(ttl),
                Outcome::Acquired(token) => {
                    let cancel = CancellationToken::new();
                    if watchdog {
                        watchdog::spawn(key.clone(), owner.clone(), lease, mode, cancel.clone());
                    }
                    Attempt::Acquired(
                        LockGuard::new(key, owner, lease, mode, cancel).with_token(token),
                    )
                }
            },
        )
    })
    .await
}

fn fair_wait_of(key: &Key, wait: Wait) -> Duration {
    match wait {
        Wait::For(wait) if wait >= Duration::from_millis(1) => wait,
        _ => key.core.lock_settings.fair_wait_timeout,
    }
}

pub(crate) async fn acquire(
    key: &Key,
    lease: Option<Duration>,
    wait: Wait,
    mode: Mode,
) -> Result<Option<LockGuard>> {
    let core = &key.core;
    let owner = core.owner();
    let watchdog = lease.is_none();
    let lease = lease.unwrap_or(core.lock_lease);
    lease_arg(lease)?;
    let fair_wait = fair_wait_of(key, wait);
    let started = Instant::now();

    if wait == Wait::Once {
        return Ok(
            match attempt(key, &owner, lease, watchdog, mode, wait, fair_wait).await? {
                Attempt::Acquired(guard) => Some(guard),
                Attempt::Busy(_) => None,
            },
        );
    }

    let deadline = match wait {
        Wait::For(wait) => Some(started + wait),
        _ => None,
    };
    let mut queued =
        (mode == Mode::Fair).then(|| fair::Queued::new(key.clone(), owner.clone(), fair_wait));
    let expired = |deadline: Option<Instant>| deadline.is_some_and(|d| Instant::now() >= d);

    if let Attempt::Acquired(guard) =
        attempt(key, &owner, lease, watchdog, mode, wait, fair_wait).await?
    {
        if let Some(queued) = queued.as_mut() {
            queued.disarm();
        }
        return Ok(Some(guard));
    }

    let failed = |mut queued: Option<fair::Queued>| async move {
        if let Some(queued) = queued.as_mut() {
            let _ = queued.acquire_failed().await;
        }
        Ok::<Option<LockGuard>, Error>(None)
    };
    if expired(deadline) {
        return failed(queued).await;
    }

    let wake_channel = match mode {
        Mode::Fair => fair::channel(key.name(), &owner),
        _ => channel(key.name()),
    };
    let subscription = match deadline {
        Some(deadline) => {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining, core.pubsub.subscribe(&wake_channel)).await {
                Ok(subscription) => subscription?,
                Err(_) => return failed(queued).await,
            }
        }
        None => core.pubsub.subscribe(&wake_channel).await?,
    };
    if expired(deadline) {
        return failed(queued).await;
    }
    let notify: &Notify = subscription.notify();

    loop {
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let ttl = match attempt(key, &owner, lease, watchdog, mode, wait, fair_wait).await? {
            Attempt::Acquired(guard) => {
                if let Some(queued) = queued.as_mut() {
                    queued.disarm();
                }
                return Ok(Some(guard));
            }
            Attempt::Busy(ttl) => ttl,
        };

        match deadline {
            None => {
                if ttl >= 0 {
                    let _ = tokio::time::timeout(Duration::from_millis(ttl as u64), notified).await;
                } else {
                    notified.await;
                }
            }
            Some(deadline) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return failed(queued).await;
                }
                let pause = if ttl >= 0 && Duration::from_millis(ttl as u64) < remaining {
                    Duration::from_millis(ttl as u64)
                } else {
                    remaining
                };
                let _ = tokio::time::timeout(pause, notified).await;
                if expired(Some(deadline)) {
                    return failed(queued).await;
                }
            }
        }
    }
}
