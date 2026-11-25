use crate::error::{Error, Result};
use crate::locks::sync::synced_eval;
use crate::lua::Lua;
use crate::object::{millis, HasKey, Key};
use crate::pending::{Pending, PendingTimeout};
use crate::shield::shielded;
use bytes::Bytes;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::LazyLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::runtime::Handle;
use tokio::time::Instant;

/// The expiry Redisson gives a permit without a lease, far enough to never come.
const NON_EXPIRABLE: i64 = 922_337_203_685_477;

// The scripts are Redisson's. KEYS: the permits, the permit ids by expiry, the channel; ARGV starts with the count or id.
static TRY_ACQUIRE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local expiredIds = redis.call('zrangebyscore', KEYS[2], 0, ARGV[3], 'limit', 0, ARGV[1])
        if #expiredIds > 0 then
            redis.call('zrem', KEYS[2], unpack(expiredIds))
            local value = redis.call('incrby', KEYS[1], #expiredIds)
            if tonumber(value) > 0 then
                redis.call(ARGV[5], KEYS[3], value)
            end
        end
        local value = redis.call('get', KEYS[1])
        if (value ~= false and tonumber(value) >= tonumber(ARGV[1])) then
            redis.call('decrby', KEYS[1], ARGV[1])
            for i = 6, #ARGV, 1 do
                redis.call('zadd', KEYS[2], ARGV[2], ARGV[i])
            end
            local ttl = redis.call('pttl', KEYS[1])
            if ttl > 0 then
                redis.call('pexpire', KEYS[2], ttl)
            end
            return 'OK'
        end
        local v = redis.call('zrange', KEYS[2], 0, 0, 'WITHSCORES')
        if v[1] ~= nil and v[2] ~= ARGV[4] then
            return ':' .. tostring(v[2])
        end
        return nil",
    )
});

// KEYS: the permits, the channel, the permit ids by expiry.
static TRY_RELEASE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local expire = redis.call('zscore', KEYS[3], ARGV[1])
        local removed = redis.call('zrem', KEYS[3], ARGV[1])
        if tonumber(removed) ~= 1 then
            return 0
        end
        local value = redis.call('incrby', KEYS[1], ARGV[2])
        redis.call(ARGV[4], KEYS[2], value)
        if tonumber(expire) <= tonumber(ARGV[3]) then
            return 0
        end
        return 1",
    )
});

// KEYS: the permits, the channel, the permit ids by expiry.
static TRY_RELEASE_MANY: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "for i = 4, #ARGV, 1 do
            local expire = redis.call('zscore', KEYS[3], ARGV[i])
            if expire == false or tonumber(expire) <= tonumber(ARGV[2]) then
                return 0
            end
        end
        local expiredIds = redis.call('zrangebyscore', KEYS[3], 0, ARGV[2], 'limit', 0, -1)
        if #expiredIds > 0 then
            redis.call('zrem', KEYS[3], unpack(expiredIds))
            local value = redis.call('incrby', KEYS[1], #expiredIds)
            if tonumber(value) > 0 then
                redis.call(ARGV[3], KEYS[2], value)
            end
        end
        local keys = {}
        for i = 4, #ARGV, 1 do
            table.insert(keys, ARGV[i])
        end
        local removed = redis.call('zrem', KEYS[3], unpack(keys))
        redis.call('incrby', KEYS[1], removed)
        redis.call(ARGV[3], KEYS[2], removed)
        return removed",
    )
});

/// Gives back the permits whose lease ran out; the scripts below start with it. KEYS: the permits, the permit ids by expiry, the channel. ARGV[1] is now, ARGV[2] the publish command.
const RECLAIM: &str =
    "local expiredIds = redis.call('zrangebyscore', KEYS[2], 0, ARGV[1], 'limit', 0, -1)
        if #expiredIds > 0 then
            redis.call('zrem', KEYS[2], unpack(expiredIds))
            local value = redis.call('incrby', KEYS[1], #expiredIds)
            if tonumber(value) > 0 then
                redis.call(ARGV[2], KEYS[3], value)
            end
            ";

static AVAILABLE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(format!(
        "{RECLAIM}return value
        end
        local ret = redis.call('get', KEYS[1])
        return ret == false and 0 or ret"
    ))
});

static PERMITS: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(format!(
        "{RECLAIM}end
        local available = redis.call('get', KEYS[1])
        if available == false then
            return 0
        end
        local acquired = redis.call('zcount', KEYS[2], 0, '+inf')
        if acquired == false then
            return tonumber(available)
        end
        return tonumber(available) + acquired"
    ))
});

static ACQUIRED: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(format!(
        "{RECLAIM}end
        local acquired = redis.call('zcount', KEYS[2], 0, '+inf')
        return acquired == false and 0 or acquired"
    ))
});

// KEYS: the permits, the channel, the permit ids by expiry.
static SET_PERMITS: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local available = redis.call('get', KEYS[1])
        if (available == false) then
            redis.call('set', KEYS[1], ARGV[1])
            redis.call(ARGV[2], KEYS[2], ARGV[1])
            return
        end
        local acquired = redis.call('zcount', KEYS[3], 0, '+inf')
        local maximum = (acquired == false and 0 or acquired) + tonumber(available)
        if (maximum == tonumber(ARGV[1])) then
            return
        end
        redis.call('incrby', KEYS[1], tonumber(ARGV[1]) - maximum)
        redis.call(ARGV[2], KEYS[2], ARGV[1])",
    )
});

// KEYS: the permits, the channel.
static TRY_SET_PERMITS: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local value = redis.call('get', KEYS[1])
        if (value == false) then
            redis.call('set', KEYS[1], ARGV[1])
            redis.call(ARGV[2], KEYS[2], ARGV[1])
            return 1
        end
        return 0",
    )
});

// KEYS: the permits, the channel.
static ADD_PERMITS: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local value = redis.call('get', KEYS[1])
        if (value == false) then
            value = 0
        end
        redis.call('set', KEYS[1], tonumber(value) + tonumber(ARGV[1]))
        if tonumber(ARGV[1]) > 0 then
            redis.call(ARGV[2], KEYS[2], ARGV[1])
        end",
    )
});

// KEYS: the permits, the permit ids by expiry, the channel. ARGV: the id, the new expiry, now, the publish command.
static UPDATE_LEASE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local expiredIds = redis.call('zrangebyscore', KEYS[2], 0, ARGV[3], 'limit', 0, -1)
        if #expiredIds > 0 then
            redis.call('zrem', KEYS[2], unpack(expiredIds))
            local value = redis.call('incrby', KEYS[1], #expiredIds)
            if tonumber(value) > 0 then
                redis.call(ARGV[4], KEYS[3], value)
            end
        end
        local value = redis.call('zscore', KEYS[2], ARGV[1])
        if (value ~= false) then
            redis.call('zadd', KEYS[2], ARGV[2], ARGV[1])
            return 1
        end
        return 0",
    )
});

// KEYS: the permits, the permit ids by expiry, the channel. ARGV: the id, the expiry of a permit without lease, now, the publish command.
static LEASE_TIME: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local expiredIds = redis.call('zrangebyscore', KEYS[2], 0, ARGV[3], 'limit', 0, -1)
        if #expiredIds > 0 then
            redis.call('zrem', KEYS[2], unpack(expiredIds))
            local value = redis.call('incrby', KEYS[1], #expiredIds)
            if tonumber(value) > 0 then
                redis.call(ARGV[4], KEYS[3], value)
            end
        end
        local value = redis.call('zscore', KEYS[2], ARGV[1])
        if (value ~= false) then
            return tonumber(value) == tonumber(ARGV[2]) and -1 or tonumber(value) - tonumber(ARGV[3])
        end
        return 0",
    )
});

const PUBLISH: &str = "PUBLISH";

/// The channel waiters listen on, the one of [`Semaphore`](crate::Semaphore), as in Redisson.
fn channel(name: &str) -> String {
    if name.contains('{') {
        format!("redissun_sc:{name}")
    } else {
        format!("redissun_sc:{{{name}}}")
    }
}

/// The sorted set of the permit ids by expiry, Redisson's `suffixName(name, "timeout")`, in the slot of the permits.
fn timeout_name(name: &str) -> String {
    if name.contains('{') {
        format!("{name}:timeout")
    } else {
        format!("{{{name}}}:timeout")
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
        })
}

/// When a permit taken now with `lease` expires, Redisson's `calcTimeout`.
fn expiry(lease: Option<Duration>) -> Result<i64> {
    match lease {
        Some(lease) => Ok(now_millis().saturating_add(millis(lease)?)),
        None => Ok(NON_EXPIRABLE),
    }
}

/// A new permit id: 16 random bytes, written as 32 hex digits, Redisson's `generateId`.
fn new_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// The bytes of a permit id, Redisson's `decodeHexDump`.
fn id_bytes(id: &str) -> Result<Bytes> {
    let invalid = || Error::Config(format!("{id} is not a permit id"));
    if !id.len().is_multiple_of(2) {
        return Err(invalid());
    }
    (0..id.len())
        .step_by(2)
        .map(|at| {
            id.get(at..at + 2)
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                .ok_or_else(invalid)
        })
        .collect::<Result<Vec<u8>>>()
        .map(Bytes::from)
}

/// What one try to take permits came to.
enum Attempt {
    Taken(Vec<String>),
    /// None free, and the first held permit expires at this time (milliseconds since the epoch).
    NextExpiry(i64),
    /// None free, and no held permit ever expires.
    Busy,
}

/// Permits taken while the caller may stop waiting: dropped before they are handed over, they are released, as Redisson releases permits whose acquire was cancelled.
struct Taken {
    key: Key,
    ids: Option<Vec<String>>,
}

impl Taken {
    fn hand_over(mut self) -> Vec<String> {
        self.ids.take().unwrap_or_default()
    }
}

impl Drop for Taken {
    fn drop(&mut self) {
        let Some(ids) = self.ids.take().filter(|ids| !ids.is_empty()) else {
            return;
        };
        if let Ok(runtime) = Handle::try_current() {
            let key = self.key.clone();
            runtime.spawn(async move {
                let _ = try_release_many(&key, &ids).await;
            });
        }
    }
}

async fn try_release_many(key: &Key, ids: &[String]) -> Result<usize> {
    if ids.is_empty() {
        return Err(Error::Config("permit ids can't be empty".into()));
    }
    let mut args = vec![
        Bytes::from(ids.len().to_string()),
        Bytes::from(now_millis().to_string()),
        Bytes::from_static(PUBLISH.as_bytes()),
    ];
    for id in ids {
        args.push(id_bytes(id)?);
    }
    let released: i64 = synced_eval(
        key,
        &TRY_RELEASE_MANY,
        vec![
            key.redis_key(),
            channel(key.name()),
            timeout_name(key.name()),
        ],
        args,
        true,
    )
    .await?;
    Ok(usize::try_from(released).unwrap_or(0))
}

/// A semaphore whose permits each have an id and may have a lease, Redisson's `RPermitExpirableSemaphore`.
///
/// Acquiring returns permit ids; release a permit with its id. A permit taken with a lease returns to the semaphore by itself when the lease ends, so permits held by a process that crashed come back. The free permits live in a string named like the semaphore, the held ids in the sorted set `{name}:timeout` by their expiry, and waiters are woken through the channel of [`Semaphore`](crate::Semaphore), `redissun_sc:{name}`. Expiry times come from the clock of the client, as in Redisson.
///
/// ```no_run
/// # async fn run(client: redissun::Client) -> redissun::Result<()> {
/// use std::time::Duration;
///
/// let slots = client.permit_expirable_semaphore("render-slots");
/// slots.try_set_permits(4).await?;
/// let id = slots.acquire().lease(Duration::from_secs(30)).await?;
/// // … render …
/// slots.release(&id).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct PermitExpirableSemaphore {
    key: Key,
}

impl fmt::Debug for PermitExpirableSemaphore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "PermitExpirableSemaphore")
    }
}

impl HasKey for PermitExpirableSemaphore {
    fn key(&self) -> &Key {
        &self.key
    }

    fn companions(&self) -> Vec<String> {
        vec![timeout_name(self.key.name())]
    }

    fn renamed_companions(&self, new_name: &str) -> Option<Vec<String>> {
        // Redisson's `rename` moves only the permits and leaves the held ids behind; here they move with them, so held permits stay held.
        Some(vec![timeout_name(new_name)])
    }
}

/// A pending acquire of a [`PermitExpirableSemaphore`]: await it to wait without a limit for free permits. Set `.lease(duration)` or `.timeout(duration)` first to change that.
#[must_use = "a pending acquire does nothing until it is awaited"]
pub struct PermitRequest<'a, T> {
    semaphore: &'a PermitExpirableSemaphore,
    permits: usize,
    lease: Option<Duration>,
    pick: fn(Vec<String>) -> Option<T>,
}

impl<'a, T: Send + 'a> PermitRequest<'a, T> {
    /// The permits return to the semaphore by themselves after `lease`. Without it they are held until released.
    pub fn lease(mut self, lease: Duration) -> Self {
        self.lease = Some(lease);
        self
    }

    /// Waits at most `timeout` for free permits. The call then resolves to `None` when the time runs out; `Duration::ZERO` tries once.
    pub fn timeout(self, timeout: Duration) -> PendingTimeout<'a, T> {
        let (semaphore, permits, lease, pick) =
            (self.semaphore, self.permits, self.lease, self.pick);
        Pending::new(move |wait| async move {
            Ok(semaphore
                .acquire_inner(permits, lease, wait)
                .await?
                .and_then(pick))
        })
        .timeout(timeout)
    }
}

impl<'a, T: Send + 'a> IntoFuture for PermitRequest<'a, T> {
    type Output = Result<T>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        let (semaphore, permits, lease, pick) =
            (self.semaphore, self.permits, self.lease, self.pick);
        Box::pin(async move {
            semaphore
                .acquire_inner(permits, lease, None)
                .await?
                .and_then(pick)
                .ok_or(Error::Timeout)
        })
    }
}

fn first(ids: Vec<String>) -> Option<String> {
    ids.into_iter().next()
}

fn all(ids: Vec<String>) -> Option<Vec<String>> {
    Some(ids)
}

impl PermitExpirableSemaphore {
    pub(crate) fn new(key: Key) -> Self {
        Self { key }
    }

    fn keys(&self) -> Vec<String> {
        vec![
            self.key.redis_key(),
            timeout_name(self.key.name()),
            channel(self.key.name()),
        ]
    }

    fn key_and_channel(&self) -> Vec<String> {
        vec![self.key.redis_key(), channel(self.key.name())]
    }

    fn release_keys(&self) -> Vec<String> {
        vec![
            self.key.redis_key(),
            channel(self.key.name()),
            timeout_name(self.key.name()),
        ]
    }

    /// Waits for one free permit and returns its id, Redisson's `acquire`. Add `.lease(duration)` to have it return by itself, `.timeout(duration)` to wait at most that long.
    pub fn acquire(&self) -> PermitRequest<'_, String> {
        PermitRequest {
            semaphore: self,
            permits: 1,
            lease: None,
            pick: first,
        }
    }

    /// Waits for `permits` free permits and returns their ids, Redisson's `acquire(int)`. Taking 0 permits succeeds at once with no ids.
    pub fn acquire_many(&self, permits: usize) -> PermitRequest<'_, Vec<String>> {
        PermitRequest {
            semaphore: self,
            permits,
            lease: None,
            pick: all,
        }
    }

    /// Takes one permit if one is free now and returns its id, Redisson's `tryAcquire`.
    pub async fn try_acquire(&self) -> Result<Option<String>> {
        self.acquire().timeout(Duration::ZERO).await
    }

    /// Takes `permits` permits if they are free now and returns their ids, empty otherwise, Redisson's `tryAcquire(int)`.
    pub async fn try_acquire_many(&self, permits: usize) -> Result<Vec<String>> {
        Ok(self
            .acquire_many(permits)
            .timeout(Duration::ZERO)
            .await?
            .unwrap_or_default())
    }

    async fn attempt(&self, permits: usize, lease: Option<Duration>) -> Result<Attempt> {
        let ids: Vec<String> = (0..permits).map(|_| new_id()).collect();
        let mut args = vec![
            Bytes::from(permits.to_string()),
            Bytes::from(expiry(lease)?.to_string()),
            Bytes::from(now_millis().to_string()),
            Bytes::from(NON_EXPIRABLE.to_string()),
            Bytes::from_static(PUBLISH.as_bytes()),
        ];
        for id in &ids {
            args.push(id_bytes(id)?);
        }
        let key = self.key.clone();
        let keys = self.keys();
        // The script runs to its end when the caller stops waiting, and permits it took then are released.
        let taken = shielded(async move {
            let reply: Option<String> = synced_eval(&key, &TRY_ACQUIRE, keys, args, false).await?;
            Ok(match reply {
                Some(reply) if reply == "OK" => (
                    Some(Taken {
                        key,
                        ids: Some(ids),
                    }),
                    None,
                ),
                Some(reply) => (None, reply.strip_prefix(':').and_then(parse_score)),
                None => (None, None),
            })
        })
        .await?;
        Ok(match taken {
            (Some(taken), _) => Attempt::Taken(taken.hand_over()),
            (None, Some(next)) => Attempt::NextExpiry(next),
            (None, None) => Attempt::Busy,
        })
    }

    async fn acquire_inner(
        &self,
        permits: usize,
        lease: Option<Duration>,
        wait: Option<Duration>,
    ) -> Result<Option<Vec<String>>> {
        let deadline = wait.map(|wait| Instant::now() + wait);
        let mut attempt = self.attempt(permits, lease).await?;
        if let Attempt::Taken(ids) = attempt {
            return Ok(Some(ids));
        }
        if wait.is_some_and(|wait| wait.is_zero()) {
            return Ok(None);
        }
        let subscription = self
            .key
            .core
            .pubsub()
            .await?
            .subscribe(&channel(self.key.name()))
            .await?;
        let notify = subscription.notify();
        loop {
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !matches!(attempt, Attempt::Taken(_)) {
                attempt = self.attempt(permits, lease).await?;
            }
            let next_expiry = match attempt {
                Attempt::Taken(ids) => return Ok(Some(ids)),
                Attempt::NextExpiry(at) => Some(at),
                Attempt::Busy => None,
            };
            let Some(mut pause) = crate::wait::next_pause(deadline) else {
                return Ok(None);
            };
            // A lease that ends frees a permit without a message, so the waiter tries again then, as in Redisson.
            if let Some(at) = next_expiry {
                let until = u64::try_from(at.saturating_sub(now_millis())).unwrap_or(0);
                pause = pause.min(Duration::from_millis(until.max(1)));
            }
            let _ = tokio::time::timeout(pause, notified).await;
        }
    }

    /// Returns the permit `id` to the semaphore, Redisson's `release`. Fails with [`Error::PermitNotHeld`] when it was already released, its lease ended, or it never existed.
    pub async fn release(&self, id: &str) -> Result<()> {
        if self.try_release(id).await? {
            Ok(())
        } else {
            Err(Error::PermitNotHeld(id.to_string()))
        }
    }

    /// Returns the permits `ids`, Redisson's `release(List)`. Fails with [`Error::PermitNotHeld`] unless every one was held.
    pub async fn release_many<S: AsRef<str>>(&self, ids: &[S]) -> Result<()> {
        if self.try_release_many(ids).await? == ids.len() {
            Ok(())
        } else {
            let ids: Vec<&str> = ids.iter().map(AsRef::as_ref).collect();
            Err(Error::PermitNotHeld(ids.join(", ")))
        }
    }

    /// Returns the permit `id`; returns whether it was held, Redisson's `tryRelease`. A permit whose lease already ended gives `false`.
    pub async fn try_release(&self, id: &str) -> Result<bool> {
        let args = vec![
            id_bytes(id)?,
            Bytes::from_static(b"1"),
            Bytes::from(now_millis().to_string()),
            Bytes::from_static(PUBLISH.as_bytes()),
        ];
        let released: i64 =
            synced_eval(&self.key, &TRY_RELEASE, self.release_keys(), args, true).await?;
        Ok(released == 1)
    }

    /// Returns the permits `ids`; returns how many were released, Redisson's `tryRelease(List)`. When any of them is not held or its lease ended, nothing is released and it gives 0. An empty list is an [`Error::Config`], as in Redisson.
    pub async fn try_release_many<S: AsRef<str>>(&self, ids: &[S]) -> Result<usize> {
        let ids: Vec<String> = ids.iter().map(|id| id.as_ref().to_string()).collect();
        try_release_many(&self.key, &ids).await
    }

    async fn count(&self, script: &Lua) -> Result<i64> {
        let args = vec![
            Bytes::from(now_millis().to_string()),
            Bytes::from_static(PUBLISH.as_bytes()),
        ];
        self.key.core.eval(script, self.keys(), args).await
    }

    /// How many permits are free now, Redisson's `availablePermits`. Permits whose lease ended are given back first.
    pub async fn available_permits(&self) -> Result<i64> {
        self.count(&AVAILABLE).await
    }

    /// How many permits there are, free and held, Redisson's `getPermits`. 0 when the semaphore does not exist.
    pub async fn permits(&self) -> Result<i64> {
        self.count(&PERMITS).await
    }

    /// How many permits are held, Redisson's `acquiredPermits`.
    pub async fn acquired_permits(&self) -> Result<i64> {
        self.count(&ACQUIRED).await
    }

    /// Sets the number of permits, free and held together, and wakes waiters, Redisson's `setPermits`. With more held than that, the free count goes below zero.
    pub async fn set_permits(&self, permits: i64) -> Result<()> {
        let args = vec![
            Bytes::from(permits.to_string()),
            Bytes::from_static(PUBLISH.as_bytes()),
        ];
        synced_eval::<()>(&self.key, &SET_PERMITS, self.release_keys(), args, true).await
    }

    /// Sets the number of permits only when the semaphore does not exist yet; returns whether it did, Redisson's `trySetPermits`.
    pub async fn try_set_permits(&self, permits: i64) -> Result<bool> {
        let args = vec![
            Bytes::from(permits.to_string()),
            Bytes::from_static(PUBLISH.as_bytes()),
        ];
        let set: i64 = synced_eval(
            &self.key,
            &TRY_SET_PERMITS,
            self.key_and_channel(),
            args,
            true,
        )
        .await?;
        Ok(set == 1)
    }

    /// Adds free permits, creating the semaphore when needed, and wakes waiters, Redisson's `addPermits`. A negative number takes permits away.
    pub async fn add_permits(&self, permits: i64) -> Result<()> {
        let args = vec![
            Bytes::from(permits.to_string()),
            Bytes::from_static(PUBLISH.as_bytes()),
        ];
        synced_eval::<()>(&self.key, &ADD_PERMITS, self.key_and_channel(), args, true).await
    }

    /// Gives the held permit `id` a new lease from now, or none; returns whether it was held, Redisson's `updateLeaseTime`.
    pub async fn update_lease(&self, id: &str, lease: Option<Duration>) -> Result<bool> {
        let args = vec![
            id_bytes(id)?,
            Bytes::from(expiry(lease)?.to_string()),
            Bytes::from(now_millis().to_string()),
            Bytes::from_static(PUBLISH.as_bytes()),
        ];
        let updated: i64 = synced_eval(&self.key, &UPDATE_LEASE, self.keys(), args, true).await?;
        Ok(updated == 1)
    }

    /// The lease the held permit `id` has left, `None` when it has none, Redisson's `getLeaseTime`. Fails with [`Error::PermitNotHeld`] when it is not held.
    pub async fn lease_time(&self, id: &str) -> Result<Option<Duration>> {
        let args = vec![
            id_bytes(id)?,
            Bytes::from(NON_EXPIRABLE.to_string()),
            Bytes::from(now_millis().to_string()),
            Bytes::from_static(PUBLISH.as_bytes()),
        ];
        let left: i64 = self.key.core.eval(&LEASE_TIME, self.keys(), args).await?;
        match left {
            -1 => Ok(None),
            0 => Err(Error::PermitNotHeld(id.to_string())),
            left => Ok(Some(Duration::from_millis(
                u64::try_from(left).unwrap_or(0),
            ))),
        }
    }
}

/// A score as Lua's `tostring` writes it: `1700000000000` or `1.7e+12`.
fn parse_score(text: &str) -> Option<i64> {
    text.parse::<i64>()
        .ok()
        .or_else(|| text.parse::<f64>().ok().map(|score| score as i64))
}

#[cfg(test)]
mod tests {
    use super::{channel, id_bytes, new_id, parse_score, timeout_name};

    #[test]
    fn ids_are_32_hex_digits_and_go_out_as_their_16_bytes() {
        let id = new_id();
        assert_eq!(id.len(), 32);
        assert_eq!(id_bytes(&id).unwrap().len(), 16);
        assert_eq!(&id_bytes("00ff10").unwrap()[..], &[0x00, 0xff, 0x10]);
        assert!(id_bytes("abc").is_err());
        assert!(id_bytes("zz").is_err());
    }

    #[test]
    fn the_companion_keys_share_the_slot_of_the_semaphore() {
        assert_eq!(timeout_name("slots"), "{slots}:timeout");
        assert_eq!(timeout_name("{a}slots"), "{a}slots:timeout");
        assert_eq!(channel("slots"), "redissun_sc:{slots}");
    }

    #[test]
    fn scores_read_back_from_lua_text() {
        assert_eq!(parse_score("1700000000000"), Some(1_700_000_000_000));
        assert_eq!(parse_score("1.7e+12"), Some(1_700_000_000_000));
        assert_eq!(parse_score("x"), None);
    }
}
