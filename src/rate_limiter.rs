use crate::error::{Error, Result};
use crate::object::{millis, tagged, Key, Object};
use bytes::Bytes;
use fred::interfaces::KeysInterface;
use fred::types::scripts::Script;
use std::fmt;
use std::future::Future;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::time::Instant;
use uuid::Uuid;

const REFRESH_EXPIRED: &str = "
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
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local currentValue = redis.call('GET', valueName)
if currentValue ~= false then
    local expired = redis.call('ZRANGEBYSCORE', permitsName, 0, now - tonumber(interval))
    local released = 0
    for i, member in ipairs(expired) do
        released = released + tonumber(string.match(member, ':(%d+)$'))
    end
    if released > 0 then
        redis.call('ZREMRANGEBYSCORE', permitsName, 0, now - tonumber(interval))
        if tonumber(currentValue) + released > tonumber(rate) then
            local used = 0
            for i, member in ipairs(redis.call('ZRANGE', permitsName, 0, -1)) do
                used = used + tonumber(string.match(member, ':(%d+)$'))
            end
            currentValue = tonumber(rate) - used
        else
            currentValue = tonumber(currentValue) + released
        end
        redis.call('SET', valueName, currentValue, 'KEEPTTL')
    end
else
    currentValue = false
end
";

static TRY_ACQUIRE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(format!(
        "{REFRESH_EXPIRED}
        if tonumber(rate) < tonumber(ARGV[1]) then
            return redis.error_reply('Requested permits amount cannot exceed defined rate')
        end
        local result = nil
        if currentValue ~= false then
            if tonumber(currentValue) < tonumber(ARGV[1]) then
                local first = redis.call('ZRANGE', permitsName, 0, 0, 'WITHSCORES')
                local waited = 0
                if first[2] ~= nil then
                    waited = now - tonumber(first[2])
                end
                result = 3 + tonumber(interval) - waited
            else
                redis.call('ZADD', permitsName, now, ARGV[2] .. ':' .. ARGV[1])
                redis.call('DECRBY', valueName, ARGV[1])
            end
        else
            redis.call('SET', valueName, rate)
            redis.call('ZADD', permitsName, now, ARGV[2] .. ':' .. ARGV[1])
            redis.call('DECRBY', valueName, ARGV[1])
        end
        local ttl = redis.call('PTTL', KEYS[1])
        if ttl > 0 then
            redis.call('PEXPIRE', valueName, ttl)
            redis.call('PEXPIRE', permitsName, ttl)
        end
        return result"
    ))
});

static AVAILABLE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(format!(
        "{REFRESH_EXPIRED}
        if currentValue == false then
            return tonumber(rate)
        end
        return tonumber(currentValue)"
    ))
});

static TRY_SET_RATE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('EXISTS', KEYS[1]) == 0 then
            redis.call('HSET', KEYS[1], 'rate', ARGV[1], 'interval', ARGV[2], 'type', ARGV[3])
            return 1
        end
        return 0",
    )
});

static SET_RATE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "redis.call('HSET', KEYS[1], 'rate', ARGV[1], 'interval', ARGV[2], 'type', ARGV[3])
        redis.call('DEL', KEYS[2], KEYS[3], KEYS[4], KEYS[5])
        return 1",
    )
});

static EXPIRE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local applied = redis.call('PEXPIRE', KEYS[1], ARGV[1])
        for i = 2, #KEYS do
            redis.call('PEXPIRE', KEYS[i], ARGV[1])
        end
        return applied",
    )
});

static PERSIST: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local applied = redis.call('PERSIST', KEYS[1])
        for i = 2, #KEYS do
            redis.call('PERSIST', KEYS[i])
        end
        return applied",
    )
});

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

/// A shared rate limiter with a sliding window, as in Redisson's `RRateLimiter`.
///
/// Configure it once with [`RateLimiter::try_set_rate`], then call [`RateLimiter::try_acquire`] or [`RateLimiter::acquire`] before each rate-limited action. Time is taken from the Redis server, so clients with different clocks agree.
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

    fn main_key(&self) -> String {
        tagged(self.key.name())
    }

    fn keys(&self) -> Vec<String> {
        let base = self.main_key();
        let client = self.key.core.client_id();
        vec![
            base.clone(),
            format!("{base}:value"),
            format!("{base}:value:{client}"),
            format!("{base}:permits"),
            format!("{base}:permits:{client}"),
        ]
    }

    fn settings(rate_type: RateType, rate: u64, interval: Duration) -> Result<Vec<Bytes>> {
        if rate == 0 {
            return Err(Error::Config("rate must be positive".into()));
        }
        Ok(vec![
            Bytes::from(rate.to_string()),
            Bytes::from(millis(interval)?.to_string()),
            Bytes::from(rate_type.code()),
        ])
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
        let set: i64 = self
            .key
            .core
            .eval(
                &TRY_SET_RATE,
                vec![self.main_key()],
                Self::settings(rate_type, rate, interval)?,
            )
            .await?;
        Ok(set == 1)
    }

    /// Sets the rate, replacing any earlier configuration and forgetting permits already used.
    pub async fn set_rate(&self, rate_type: RateType, rate: u64, interval: Duration) -> Result<()> {
        let _: i64 = self
            .key
            .core
            .eval(
                &SET_RATE,
                self.keys(),
                Self::settings(rate_type, rate, interval)?,
            )
            .await?;
        Ok(())
    }

    /// Number of permits that can be taken right now.
    pub async fn available_permits(&self) -> Result<u64> {
        let available: i64 = self
            .key
            .core
            .eval(&AVAILABLE, self.keys(), Vec::new())
            .await?;
        Ok(available.max(0) as u64)
    }

    /// Takes `permits` if the limit allows it right now; returns whether it did.
    pub async fn try_acquire(&self, permits: u64) -> Result<bool> {
        self.acquire_inner(permits, Some(Duration::ZERO)).await
    }

    /// Waits up to `wait` for the limit to allow `permits`; returns whether it did.
    pub async fn try_acquire_for(&self, permits: u64, wait: Duration) -> Result<bool> {
        self.acquire_inner(permits, Some(wait)).await
    }

    /// Waits until the limit allows `permits`.
    pub async fn acquire(&self, permits: u64) -> Result<()> {
        self.acquire_inner(permits, None).await?;
        Ok(())
    }

    async fn take(&self, keys: &[String], arguments: &[Bytes]) -> Result<Option<i64>> {
        self.key
            .core
            .eval(&TRY_ACQUIRE, keys.to_vec(), arguments.to_vec())
            .await
    }

    async fn acquire_inner(&self, permits: u64, wait: Option<Duration>) -> Result<bool> {
        if permits == 0 {
            return Err(Error::Config("permits must be positive".into()));
        }
        let deadline = wait.map(|wait| Instant::now() + wait);
        let keys = self.keys();
        let arguments = [
            Bytes::from(permits.to_string()),
            Bytes::from(Uuid::new_v4().to_string()),
        ];
        loop {
            let Some(delay) = self.take(&keys, &arguments).await? else {
                return Ok(true);
            };
            let delay = Duration::from_millis(delay.max(1) as u64);
            let pause = match deadline {
                Some(deadline) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Ok(false);
                    }
                    if remaining < delay {
                        tokio::time::sleep(remaining).await;
                        return Ok(false);
                    }
                    delay
                }
                None => delay,
            };
            tokio::time::sleep(pause).await;
        }
    }

    async fn delete_all(&self) -> Result<bool> {
        let removed: i64 = self.key.core.redis().del(self.keys()).await?;
        Ok(removed > 0)
    }

    fn main(&self) -> Key {
        Key::new(self.key.core.clone(), self.main_key())
    }

    async fn exists_main(&self) -> Result<bool> {
        self.main().exists().await
    }

    async fn expire_all(&self, ttl: Duration) -> Result<bool> {
        let applied: i64 = self
            .key
            .core
            .eval(
                &EXPIRE,
                self.keys(),
                vec![Bytes::from(millis(ttl)?.to_string())],
            )
            .await?;
        Ok(applied == 1)
    }

    async fn ttl_main(&self) -> Result<Option<Duration>> {
        self.main().ttl().await
    }

    async fn persist_all(&self) -> Result<bool> {
        let applied: i64 = self
            .key
            .core
            .eval(&PERSIST, self.keys(), Vec::new())
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
        self.exists_main()
    }

    async fn rename(&self, _new_name: &str) -> Result<()> {
        Err(Error::Unsupported("RateLimiter cannot be renamed".into()))
    }

    fn expire(&self, ttl: Duration) -> impl Future<Output = Result<bool>> + Send {
        self.expire_all(ttl)
    }

    fn ttl(&self) -> impl Future<Output = Result<Option<Duration>>> + Send {
        self.ttl_main()
    }

    fn persist(&self) -> impl Future<Output = Result<bool>> + Send {
        self.persist_all()
    }
}
