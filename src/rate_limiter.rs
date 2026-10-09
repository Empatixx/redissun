use crate::error::{Error, Result};
use crate::object::{millis, Key, Object};
use crate::pending::Pending;
use bytes::Bytes;
use fred::interfaces::{HashesInterface, KeysInterface};
use fred::types::scripts::Script;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::LazyLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::Instant;

const SETTINGS: &str = "
local rate = redis.call('HGET', KEYS[1], 'rate')
local interval = redis.call('HGET', KEYS[1], 'interval')
local kind = redis.call('HGET', KEYS[1], 'type')
if rate == false or interval == false or kind == false then
    return redis.error_reply('RateLimiter is not initialized')
end
local valueName = KEYS[2]
local permitsName = KEYS[4]
if kind == '1' then
    valueName = KEYS[3]
    permitsName = KEYS[5]
end
";

const PERMITS_OF: &str = "
local function permitsOf(member)
    return tonumber(string.match(member, ':(%d+)$'))
end
";

const KEEP_ALIVE: &str = "
local keepAliveTime = redis.call('HGET', KEYS[1], 'keepAliveTime')
if keepAliveTime ~= false and tonumber(keepAliveTime) > 0 then
    redis.call('PEXPIRE', KEYS[1], keepAliveTime)
    redis.call('PEXPIRE', valueName, keepAliveTime)
    redis.call('PEXPIRE', permitsName, keepAliveTime)
else
    local ttl = redis.call('PTTL', KEYS[1])
    if ttl > 0 then
        redis.call('PEXPIRE', valueName, ttl)
        redis.call('PEXPIRE', permitsName, ttl)
    end
end
";

static TRY_ACQUIRE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(format!(
        "{SETTINGS}{PERMITS_OF}
        if tonumber(rate) < tonumber(ARGV[1]) then
            return redis.error_reply('Requested permits amount cannot exceed defined rate')
        end
        local now = tonumber(ARGV[2])
        local member = ARGV[3] .. ':' .. ARGV[1]
        local currentValue = redis.call('GET', valueName)
        local result = nil
        if currentValue ~= false then
            local expired = redis.call('ZRANGEBYSCORE', permitsName, 0, now - tonumber(interval))
            local released = 0
            for i, v in ipairs(expired) do
                released = released + permitsOf(v)
            end
            if released > 0 then
                redis.call('ZREMRANGEBYSCORE', permitsName, 0, now - tonumber(interval))
                if tonumber(currentValue) + released > tonumber(rate) then
                    local used = 0
                    for i, v in ipairs(redis.call('ZRANGE', permitsName, 0, -1)) do
                        used = used + permitsOf(v)
                    end
                    currentValue = tonumber(rate) - used
                else
                    currentValue = tonumber(currentValue) + released
                end
                redis.call('SET', valueName, currentValue)
            end
            if tonumber(currentValue) < tonumber(ARGV[1]) then
                local first = redis.call('ZRANGE', permitsName, 0, 0, 'WITHSCORES')
                local waited = 0
                if first[2] ~= nil then
                    waited = now - tonumber(first[2])
                end
                result = 3 + tonumber(interval) - waited
            else
                redis.call('ZADD', permitsName, now, member)
                redis.call('DECRBY', valueName, ARGV[1])
            end
        else
            redis.call('SET', valueName, rate)
            redis.call('ZADD', permitsName, now, member)
            redis.call('DECRBY', valueName, ARGV[1])
        end
        {KEEP_ALIVE}
        return result"
    ))
});

static AVAILABLE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(format!(
        "{SETTINGS}{PERMITS_OF}
        local currentValue = redis.call('GET', valueName)
        if currentValue == false then
            redis.call('SET', valueName, rate)
            return tonumber(rate)
        end
        local now = tonumber(ARGV[1])
        local expired = redis.call('ZRANGEBYSCORE', permitsName, 0, now - tonumber(interval))
        local released = 0
        for i, v in ipairs(expired) do
            released = released + permitsOf(v)
        end
        if released > 0 then
            redis.call('ZREMRANGEBYSCORE', permitsName, 0, now - tonumber(interval))
            currentValue = tonumber(currentValue) + released
            redis.call('SET', valueName, currentValue, 'KEEPTTL')
        end
        return tonumber(currentValue)"
    ))
});

static RELEASE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(format!(
        "{SETTINGS}
        local toRelease = tonumber(ARGV[1])
        local values = redis.call('ZRANGE', permitsName, 0, -1, 'WITHSCORES')
        for i = 1, #values - 1, 2 do
            if toRelease <= 0 then
                break
            end
            local v = values[i]
            local score = values[i + 1]
            local random, permits = string.match(v, '^(.*):(%d+)$')
            permits = tonumber(permits)
            redis.call('ZREM', permitsName, v)
            if permits <= toRelease then
                toRelease = toRelease - permits
            else
                redis.call('ZADD', permitsName, score, random .. ':' .. (permits - toRelease))
                toRelease = 0
            end
        end
        local currentValue = redis.call('GET', valueName)
        if currentValue == false then
            currentValue = tonumber(rate)
        else
            currentValue = tonumber(currentValue)
        end
        local newValue = currentValue + tonumber(ARGV[1])
        if newValue > tonumber(rate) then
            newValue = tonumber(rate)
        end
        redis.call('SET', valueName, newValue)
        {KEEP_ALIVE}"
    ))
});

static TRY_SET_RATE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "redis.call('HSETNX', KEYS[1], 'rate', ARGV[1])
        redis.call('HSETNX', KEYS[1], 'interval', ARGV[2])
        redis.call('HSETNX', KEYS[1], 'keepAliveTime', ARGV[4])
        local res = redis.call('HSETNX', KEYS[1], 'type', ARGV[3])
        if res == 1 and tonumber(ARGV[4]) > 0 then
            redis.call('PEXPIRE', KEYS[1], ARGV[4])
        end
        return res",
    )
});

static SET_RATE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local valueName = KEYS[2]
        local permitsName = KEYS[4]
        if ARGV[3] == '1' then
            valueName = KEYS[3]
            permitsName = KEYS[5]
        end
        redis.call('HSET', KEYS[1], 'rate', ARGV[1], 'interval', ARGV[2], 'type', ARGV[3], 'keepAliveTime', ARGV[4])
        if tonumber(ARGV[4]) > 0 then
            redis.call('PEXPIRE', KEYS[1], ARGV[4])
        end
        redis.call('DEL', valueName, permitsName)
        return 1",
    )
});

static SET_OR_UPDATE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(format!(
        "if ARGV[7] == '1' and redis.call('EXISTS', KEYS[1]) == 0 then
            return 0
        end
        {PERMITS_OF}
        local valueName = KEYS[2]
        local permitsName = KEYS[4]
        if ARGV[3] == '1' then
            valueName = KEYS[3]
            permitsName = KEYS[5]
        end
        local oldType = redis.call('HGET', KEYS[1], 'type')
        redis.call('HSET', KEYS[1], 'rate', ARGV[1], 'interval', ARGV[2], 'type', ARGV[3], 'keepAliveTime', ARGV[4])
        if tonumber(ARGV[4]) > 0 then
            redis.call('PEXPIRE', KEYS[1], ARGV[4])
        end
        if oldType ~= false and oldType ~= ARGV[3] then
            redis.call('DEL', KEYS[2], KEYS[3], KEYS[4], KEYS[5])
            return 1
        end
        if ARGV[6] == '0' then
            redis.call('DEL', valueName, permitsName)
            return 1
        end
        local rate = tonumber(ARGV[1])
        local interval = tonumber(ARGV[2])
        local now = tonumber(ARGV[5])
        redis.call('ZREMRANGEBYSCORE', permitsName, 0, now - interval)
        local used = 0
        for i, v in ipairs(redis.call('ZRANGE', permitsName, 0, -1)) do
            used = used + permitsOf(v)
        end
        local newValue = rate - used
        if newValue < 0 then
            newValue = 0
        end
        redis.call('SET', valueName, newValue)
        {KEEP_ALIVE}
        return 1"
    ))
});

pub(crate) static EXPIRE_ANY: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local result = 0
        for i = 1, #KEYS do
            if redis.call('PEXPIRE', KEYS[i], ARGV[1]) == 1 then
                result = 1
            end
        end
        return result",
    )
});

pub(crate) static PERSIST_ANY: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local result = 0
        for i = 1, #KEYS do
            if redis.call('PERSIST', KEYS[i]) == 1 then
                result = 1
            end
        end
        return result",
    )
});

pub(crate) fn suffix_name(name: &str, suffix: &str) -> String {
    if name.contains('{') {
        format!("{name}:{suffix}")
    } else {
        format!("{{{name}}}:{suffix}")
    }
}

fn now_millis() -> Bytes {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    Bytes::from(now.to_string())
}

/// How a [`RateLimiter`] counts permits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RateType {
    /// One limit shared by every client that uses the limiter.
    Overall,
    /// Every client gets its own limit.
    PerClient,
}

impl RateType {
    fn code(self) -> &'static str {
        match self {
            RateType::Overall => "0",
            RateType::PerClient => "1",
        }
    }
}

/// Settings for [`RateLimiter::set_rate_with`], [`RateLimiter::update_rate`] and [`RateLimiter::try_set_rate_with`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RateLimiterArgs {
    rate_type: RateType,
    rate: u64,
    interval: Duration,
    keep_alive: Duration,
    keep_state: bool,
}

impl RateLimiterArgs {
    /// At most `rate` permits in any window of length `interval`, counted as `rate_type` says.
    pub fn new(rate_type: RateType, rate: u64, interval: Duration) -> Self {
        Self {
            rate_type,
            rate,
            interval,
            keep_alive: Duration::ZERO,
            keep_state: false,
        }
    }

    /// Removes the limiter when it is not used for `keep_alive`; zero (the default) keeps it forever. Must be at least the interval.
    pub fn keep_alive(mut self, keep_alive: Duration) -> Self {
        self.keep_alive = keep_alive;
        self
    }

    /// Keeps the permits used within the current window when the rate changes, instead of starting over.
    pub fn keep_state(mut self, keep_state: bool) -> Self {
        self.keep_state = keep_state;
        self
    }

    fn settings(&self) -> Result<Vec<Bytes>> {
        if self.rate == 0 {
            return Err(Error::Config("rate must be positive".into()));
        }
        let interval = millis(self.interval)?;
        let keep_alive = if self.keep_alive.is_zero() {
            0
        } else {
            millis(self.keep_alive)?
        };
        if keep_alive != 0 && keep_alive < interval {
            return Err(Error::Config(
                "keep alive time must be at least the rate interval".into(),
            ));
        }
        Ok(vec![
            Bytes::from(self.rate.to_string()),
            Bytes::from(interval.to_string()),
            Bytes::from(self.rate_type.code()),
            Bytes::from(keep_alive.to_string()),
        ])
    }
}

/// The settings a [`RateLimiter`] was configured with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct RateLimiterConfig {
    /// How permits are counted.
    pub rate_type: RateType,
    /// Permits per interval.
    pub rate: u64,
    /// Length of the window.
    pub interval: Duration,
}

/// A shared rate limiter with a sliding window, modelled on Redisson's `RRateLimiter` and using the same scripts and key layout.
///
/// Configure it once with [`RateLimiter::try_set_rate`], then call [`RateLimiter::try_acquire`] or [`RateLimiter::acquire`] before each rate-limited action. The settings live in the hash `name`, the remaining permits in `{name}:value` and the handed-out permits in the sorted set `{name}:permits` (with a `:<client id>` suffix for [`RateType::PerClient`]). Times come from the clock of the calling client, so clients should keep their clocks in sync.
#[derive(Clone)]
pub struct RateLimiter {
    key: Key,
}

impl fmt::Debug for RateLimiter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "RateLimiter")
    }
}

impl RateLimiter {
    pub(crate) fn new(key: Key) -> Self {
        Self { key }
    }

    fn keys(&self) -> Vec<String> {
        let name = self.key.name();
        let client = self.key.core.client_id();
        let value = suffix_name(name, "value");
        let permits = suffix_name(name, "permits");
        vec![
            self.key.redis_key(),
            value.clone(),
            suffix_name(&value, client),
            permits.clone(),
            suffix_name(&permits, client),
        ]
    }

    /// Sets the rate, but only when the limiter is not configured yet. Returns whether it was set.
    ///
    /// At most `rate` permits are handed out in any window of length `interval`.
    pub async fn try_set_rate(
        &self,
        rate_type: RateType,
        rate: u64,
        interval: Duration,
    ) -> Result<bool> {
        self.try_set_rate_with(RateLimiterArgs::new(rate_type, rate, interval))
            .await
    }

    /// Like [`RateLimiter::try_set_rate`], and also applies the keep-alive time of `args`. `keep_state` is ignored.
    pub async fn try_set_rate_with(&self, args: RateLimiterArgs) -> Result<bool> {
        let set: i64 = self
            .key
            .core
            .eval_no_retry(&TRY_SET_RATE, vec![self.key.redis_key()], args.settings()?)
            .await?;
        Ok(set == 1)
    }

    /// Sets the rate, replacing any earlier configuration and forgetting the permits used under the new rate type.
    pub async fn set_rate(&self, rate_type: RateType, rate: u64, interval: Duration) -> Result<()> {
        let _: i64 = self
            .key
            .core
            .eval(
                &SET_RATE,
                self.keys(),
                RateLimiterArgs::new(rate_type, rate, interval).settings()?,
            )
            .await?;
        Ok(())
    }

    /// Sets the rate from `args`. With `keep_state`, permits used in the current window still count against the new rate; a change of rate type always starts over.
    pub async fn set_rate_with(&self, args: RateLimiterArgs) -> Result<()> {
        self.set_or_update(args, false).await?;
        Ok(())
    }

    /// Like [`RateLimiter::set_rate_with`], but only when the limiter is configured already. Returns whether it was.
    pub async fn update_rate(&self, args: RateLimiterArgs) -> Result<bool> {
        self.set_or_update(args, true).await
    }

    async fn set_or_update(&self, args: RateLimiterArgs, require_exist: bool) -> Result<bool> {
        let mut arguments = args.settings()?;
        arguments.push(now_millis());
        arguments.push(Bytes::from(if args.keep_state { "1" } else { "0" }));
        arguments.push(Bytes::from(if require_exist { "1" } else { "0" }));
        let done: i64 = self
            .key
            .core
            .eval(&SET_OR_UPDATE, self.keys(), arguments)
            .await?;
        Ok(done == 1)
    }

    /// The configured settings, or `None` when the limiter is not configured.
    pub async fn config(&self) -> Result<Option<RateLimiterConfig>> {
        let fields: HashMap<String, String> =
            self.key.core.redis().hgetall(self.key.redis_key()).await?;
        if fields.is_empty() {
            return Ok(None);
        }
        let invalid = || Error::Config("the rate limiter settings are not valid".into());
        let number = |field: &str| -> Result<u64> {
            fields
                .get(field)
                .and_then(|value| value.parse().ok())
                .ok_or_else(invalid)
        };
        let rate_type = match fields.get("type").map(String::as_str) {
            Some("0") => RateType::Overall,
            Some("1") => RateType::PerClient,
            _ => return Err(invalid()),
        };
        Ok(Some(RateLimiterConfig {
            rate_type,
            rate: number("rate")?,
            interval: Duration::from_millis(number("interval")?),
        }))
    }

    /// Number of permits that can be taken right now.
    pub async fn available_permits(&self) -> Result<u64> {
        let available: i64 = self
            .key
            .core
            .eval(&AVAILABLE, self.keys(), vec![now_millis()])
            .await?;
        Ok(available.max(0) as u64)
    }

    /// Hands `permits` back before their window ends, up to the configured rate. Releasing 0 permits does nothing.
    pub async fn release(&self, permits: u64) -> Result<()> {
        if permits == 0 {
            return Ok(());
        }
        self.key
            .core
            .eval::<()>(
                &RELEASE,
                self.keys(),
                vec![Bytes::from(permits.to_string())],
            )
            .await
    }

    /// Takes `permits` if the limit allows it right now; returns whether it did.
    pub async fn try_acquire(&self, permits: u64) -> Result<bool> {
        self.acquire_inner(permits, Some(Duration::ZERO)).await
    }

    /// Waits until the limit allows `permits`. Add `.timeout(duration)` to wait at most that long; it then resolves to `None` when the time runs out.
    pub fn acquire(&self, permits: u64) -> Pending<'_, ()> {
        Pending::new(move |wait| async move {
            Ok(self.acquire_inner(permits, wait).await?.then_some(()))
        })
    }

    async fn take(&self, keys: &[String], permits: &Bytes) -> Result<Option<i64>> {
        self.key
            .core
            .eval(
                &TRY_ACQUIRE,
                keys.to_vec(),
                vec![
                    permits.clone(),
                    now_millis(),
                    Bytes::from(uuid::Uuid::new_v4().to_string()),
                ],
            )
            .await
    }

    async fn acquire_inner(&self, permits: u64, wait: Option<Duration>) -> Result<bool> {
        let deadline = wait.map(|wait| Instant::now() + wait);
        let keys = self.keys();
        let permits = Bytes::from(permits.to_string());
        loop {
            let Some(delay) = self.take(&keys, &permits).await? else {
                return Ok(true);
            };
            let delay = Duration::from_millis(delay.max(1) as u64);
            let Some(deadline) = deadline else {
                tokio::time::sleep(delay).await;
                continue;
            };
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(false);
            }
            if remaining < delay {
                tokio::time::sleep(remaining).await;
                return Ok(false);
            }
            tokio::time::sleep(delay).await;
            if Instant::now() >= deadline {
                return Ok(false);
            }
        }
    }

    async fn delete_all(&self) -> Result<bool> {
        let removed: i64 = self.key.core.redis().del(self.keys()).await?;
        Ok(removed > 0)
    }

    async fn expire_all(&self, ttl: Duration) -> Result<bool> {
        let applied: i64 = self
            .key
            .core
            .eval(
                &EXPIRE_ANY,
                self.keys(),
                vec![Bytes::from(millis(ttl)?.to_string())],
            )
            .await?;
        Ok(applied == 1)
    }

    async fn persist_all(&self) -> Result<bool> {
        let applied: i64 = self
            .key
            .core
            .eval(&PERSIST_ANY, self.keys(), Vec::new())
            .await?;
        Ok(applied == 1)
    }
}

impl Object for RateLimiter {
    fn name(&self) -> &str {
        self.key.name()
    }

    fn del(&self) -> impl Future<Output = Result<bool>> + Send {
        self.delete_all()
    }

    fn exists(&self) -> impl Future<Output = Result<bool>> + Send {
        self.key.exists()
    }

    async fn rename(&self, _new_name: &str) -> Result<()> {
        Err(Error::Unsupported("RateLimiter cannot be renamed".into()))
    }

    fn expire(&self, ttl: Duration) -> impl Future<Output = Result<bool>> + Send {
        self.expire_all(ttl)
    }

    fn ttl(&self) -> impl Future<Output = Result<Option<Duration>>> + Send {
        self.key.ttl()
    }

    fn persist(&self) -> impl Future<Output = Result<bool>> + Send {
        self.persist_all()
    }
}
