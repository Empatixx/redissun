//! [`Gcra`], Redisson's `RGcra`: a rate limiter on the Generic Cell Rate Algorithm.

use crate::error::{Error, Result};
use crate::lua::Lua;
use crate::object::{suffix_name, Key, Object};
use bytes::Bytes;
use std::collections::HashMap;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::LazyLock;
use std::time::Duration;

/// RedissonGcra.trySetRateAsync
static TRY_SET_RATE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "redis.call('hsetnx', KEYS[1], 'maxBurst', ARGV[1]);",
        "redis.call('hsetnx', KEYS[1], 'rate', ARGV[2]);",
        "return redis.call('hsetnx', KEYS[1], 'period', ARGV[3]);",
    ))
});

/// RedissonGcra.setRateAsync
static SET_RATE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "redis.call('hset', KEYS[1], 'maxBurst', ARGV[1], 'rate', ARGV[2], 'period', ARGV[3]);",
        "redis.call('del', KEYS[2]);",
    ))
});

/// RedissonGcra.tryAcquireAsync
static TRY_ACQUIRE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local maxBurst = redis.call('hget', KEYS[1], 'maxBurst');",
        "local rate = redis.call('hget', KEYS[1], 'rate');",
        "local period = redis.call('hget', KEYS[1], 'period');",
        "assert(maxBurst ~= false and rate ~= false and period ~= false, 'GCRA rate is not set');",
        "if tonumber(ARGV[1]) == 1 then ",
        "return redis.call('GCRA', KEYS[2], maxBurst, rate, period);",
        "end;",
        "return redis.call('GCRA', KEYS[2], maxBurst, rate, period, 'TOKENS', ARGV[1]);",
    ))
});

/// Port of gcraCommand (redis/src/gcra.c) for servers without `GCRA`, after Redisson's config prefix from [`TRY_ACQUIRE`].
///
/// It gives the native reply. The state is the theoretical arrival time in microseconds of the server clock, a string at `KEYS[2]` that expires at that time (`PXAT`); the native command keeps the same number in a `gcra` object, so a key used by both gives `WRONGTYPE`.
static TRY_ACQUIRE_LUA: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        r#"local maxBurst = redis.call('hget', KEYS[1], 'maxBurst');
local rate = redis.call('hget', KEYS[1], 'rate');
local period = redis.call('hget', KEYS[1], 'period');
assert(maxBurst ~= false and rate ~= false and period ~= false, 'GCRA rate is not set');
-- Port of gcraCommand, redis/src/gcra.c (unstable, 2026-11-02). Lua numbers are doubles: 2^53 replaces LLONG_MAX.
local LIMIT = 9007199254740992
local overflowTokens = 'GCRA limiting uses microsecond accuracy. Combination of period, tokens_per_period and TOKENS would cause an overflow'
local burst = tonumber(maxBurst) + 1
local perPeriod = tonumber(rate)
local periodS = tonumber(period)
local tokens = tonumber(ARGV[1])
if periodS <= 0 or periodS >= 1e12 then
    return redis.error_reply('period must be > 0 and < 1e12')
end
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000000 + tonumber(time[2])
local stored = redis.call('GET', KEYS[2])
local tat = now
if stored ~= false then tat = tonumber(stored) end
local emission = math.floor(periodS * 1000000 / perPeriod + 0.5)
if emission == 0 then emission = 1 end
if emission > LIMIT / tokens then return redis.error_reply(overflowTokens) end
if emission > LIMIT / burst then
    return redis.error_reply('GCRA limiting uses microsecond accuracy. Combination of period, tokens_per_period and max_burst would cause an overflow')
end
local variance = emission * burst
local increment = emission * tokens
local base = math.max(now, tat)
if LIMIT - base < increment then return redis.error_reply(overflowTokens) end
local newTat = base + increment
local diff = now - (newTat - variance)
local limited, retry, ttl = 0, -1, 0
if diff < 0 then
    limited = 1
    if increment <= variance then retry = math.ceil(-diff / 1000000) end
    ttl = tat - now
else
    ttl = newTat - now
    redis.call('SET', KEYS[2], string.format('%.0f', newTat), 'PXAT', string.format('%.0f', math.floor(newTat / 1000)))
end
local nextUs = variance - ttl
local remaining = 0
if nextUs >= 0 then remaining = math.floor(nextUs / emission) end -- C truncates toward zero; (-emission, 0) gives 0
return {limited, burst, remaining, retry, math.ceil(ttl / 1000000)}"#,
    )
});

/// Reads the reply to the probe `GCRA <name> 0 1 1 TOKENS`, which a server with the command refuses for its missing `TOKENS` value before it looks at the key.
///
/// "unknown command" means the server has no `GCRA`. Any other error reply, such as `NOPERM`, counts as no too, since Redisson's script would fail the same way; a failure that is not a server reply is passed on, and nothing is kept.
fn probe_answer(reply: Result<redis::Value>) -> Result<bool> {
    match reply {
        Ok(_) => Ok(true),
        Err(Error::Redis(message)) => {
            let message = message.to_lowercase();
            Ok(!message.contains("unknown command") && message.contains("missing tokens"))
        }
        Err(error) => Err(error),
    }
}

/// Reads the five integers of a `GCRA` reply, Redisson's `GcraResultDecoder`.
fn decode_result(value: redis::Value) -> Result<GcraResult> {
    let unexpected =
        |value: &redis::Value| Error::Redis(format!("unexpected GCRA reply: {value:?}"));
    let numbers: Option<Vec<i64>> = match &value {
        redis::Value::Array(items) if items.len() == 5 => items
            .iter()
            .map(|item| match item {
                redis::Value::Int(n) => Some(*n),
                _ => None,
            })
            .collect(),
        _ => None,
    };
    let Some([limited, max_tokens, available, retry, full_burst]) =
        numbers.and_then(|numbers| <[i64; 5]>::try_from(numbers).ok())
    else {
        return Err(unexpected(&value));
    };
    let count = |n: i64| u64::try_from(n).unwrap_or(0);
    Ok(GcraResult {
        limited: limited != 0,
        max_tokens: count(max_tokens),
        available_tokens: count(available),
        retry_after: u64::try_from(retry).ok().map(Duration::from_secs),
        full_burst_after: Duration::from_secs(count(full_burst)),
    })
}

/// Redisson's `validateTokens`.
fn tokens_arg(tokens: u64) -> Result<Bytes> {
    if tokens == 0 {
        return Err(Error::Config("tokens must be positive".into()));
    }
    if i64::try_from(tokens).is_err() {
        return Err(Error::Config("tokens must be at most i64::MAX".into()));
    }
    Ok(Bytes::from(tokens.to_string()))
}

/// Redisson's `toSeconds`: the period in decimal seconds, trailing zeros stripped (`250ms` → `"0.25"`).
fn period_arg(period: Duration) -> String {
    let seconds = period.as_secs();
    let nanos = period.subsec_nanos();
    if nanos == 0 {
        return seconds.to_string();
    }
    let text = format!("{seconds}.{nanos:09}");
    text.trim_end_matches('0').to_string()
}

/// Redisson's `fromSeconds` for the strings [`period_arg`] writes: whole seconds with at most 9 fraction digits.
fn parse_period(text: &str) -> Option<Duration> {
    let (whole, fraction) = match text.split_once('.') {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (text, None),
    };
    let digits = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
    if !digits(whole) {
        return None;
    }
    let seconds: u64 = whole.parse().ok()?;
    let nanos = match fraction {
        None => 0,
        Some(fraction) if digits(fraction) && fraction.len() <= 9 => {
            let scale = 10u32.pow(9 - fraction.len() as u32);
            fraction.parse::<u32>().ok()? * scale
        }
        Some(_) => return None,
    };
    Some(Duration::new(seconds, nanos))
}

/// Redisson's `validate`, and the arguments of [`TRY_SET_RATE`] and [`SET_RATE`]: `maxBurst`, `rate`, `period`.
fn rate_args(max_burst: u64, tokens_per_period: u64, period: Duration) -> Result<Vec<Bytes>> {
    if i64::try_from(max_burst).is_err() {
        return Err(Error::Config("max_burst must be at most i64::MAX".into()));
    }
    if tokens_per_period == 0 {
        return Err(Error::Config("tokens_per_period must be positive".into()));
    }
    if i64::try_from(tokens_per_period).is_err() {
        return Err(Error::Config(
            "tokens_per_period must be at most i64::MAX".into(),
        ));
    }
    if period.is_zero() {
        return Err(Error::Config("period must be positive".into()));
    }
    Ok(vec![
        Bytes::from(max_burst.to_string()),
        Bytes::from(tokens_per_period.to_string()),
        Bytes::from(period_arg(period)),
    ])
}

/// The settings a [`Gcra`] was configured with, Redisson's `GcraConfig`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct GcraConfig {
    /// Tokens that can be taken at once on top of the steady rate; the bucket holds `max_burst + 1`.
    #[doc(alias = "getMaxBurst")]
    pub max_burst: u64,
    /// Tokens per `period` at the steady rate.
    #[doc(alias = "getTokensPerPeriod")]
    pub tokens_per_period: u64,
    /// The period of the steady rate.
    #[doc(alias = "getPeriod")]
    pub period: Duration,
}

/// What one [`Gcra::try_acquire`] got, Redisson's `GcraResult`. The times are whole seconds, rounded up by the server.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct GcraResult {
    /// Whether the request was refused; a refused request takes no tokens.
    #[doc(alias = "isLimited")]
    pub limited: bool,
    /// The bucket size, `max_burst + 1`.
    #[doc(alias = "getMaxTokens")]
    pub max_tokens: u64,
    /// Tokens that can be taken right now after this request.
    #[doc(alias = "getAvailableTokens")]
    pub available_tokens: u64,
    /// When to try again: `None` when the request was allowed, and also when it was refused because it asks for more than `max_tokens`, which never passes. Redisson's `-1`.
    #[doc(alias = "getRetryAfterSeconds")]
    pub retry_after: Option<Duration>,
    /// How long until the bucket is full again.
    #[doc(alias = "getFullBurstAfterSeconds")]
    pub full_burst_after: Duration,
}

/// **Experimental.** A shared rate limiter on the Generic Cell Rate Algorithm (GCRA), Redisson's `RGcra`, with the same scripts and keys.
///
/// It hands out `tokens_per_period` tokens per `period` at a steady rate, plus up to `max_burst` at once. There is no background task: the state is one timestamp, the theoretical arrival time, at the key `name`, and the settings live in the hash `{name}:config` (fields `maxBurst`, `rate`, `period`), shared with Redisson.
///
/// On a Redis build with the `GCRA` command (today only the `redis:8.8-m03` preview) [`Gcra::try_acquire`] runs Redisson's script, which calls the command. On every other Redis 6.2+ or Valkey server it runs a Lua port of the command with the same replies. A client asks once, at its first acquire, and keeps the answer. The two paths store the state differently, so one key must not be used by both: that gives `WRONGTYPE` until the state expires or [`Gcra::set_rate`] resets it.
///
/// ```no_run
/// # async fn run(client: redissun::Client) -> redissun::Result<()> {
/// use std::time::Duration;
/// let gcra = client.gcra("api");
/// gcra.init_rate(5, 10, Duration::from_secs(1)).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Gcra {
    key: Key,
}

impl fmt::Debug for Gcra {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "Gcra")
    }
}

impl Gcra {
    pub(crate) fn new(key: Key) -> Self {
        Self { key }
    }

    /// The settings hash, Redisson's `getConfigName`.
    fn config_key(&self) -> String {
        suffix_name(self.key.name(), "config")
    }

    fn config_object(&self) -> Key {
        Key::new(self.key.core.clone(), self.config_key())
    }

    /// Sets the rate, but only when none is set yet; resolves to whether it was set. Redisson's `trySetRate`.
    ///
    /// `tokens_per_period` tokens per `period` at a steady rate, plus up to `max_burst` at once. A `max_burst` of 0 is valid; a `tokens_per_period` or `period` of zero is [`Error::Config`], and nothing is sent.
    #[doc(alias = "trySetRate")]
    pub async fn init_rate(
        &self,
        max_burst: u64,
        tokens_per_period: u64,
        period: Duration,
    ) -> Result<bool> {
        let args = rate_args(max_burst, tokens_per_period, period)?;
        let set: bool = self
            .key
            .core
            .eval_once(&TRY_SET_RATE, vec![self.config_key()], args)
            .await?;
        Ok(set)
    }

    /// Sets the rate, replacing any earlier one, and forgets the tokens used. Redisson's `setRate`.
    #[doc(alias = "setRate")]
    pub async fn set_rate(
        &self,
        max_burst: u64,
        tokens_per_period: u64,
        period: Duration,
    ) -> Result<()> {
        let args = rate_args(max_burst, tokens_per_period, period)?;
        self.key
            .core
            .eval::<()>(
                &SET_RATE,
                vec![self.config_key(), self.key.redis_key()],
                args,
            )
            .await
    }

    /// The configured settings, or `None` when no rate is set. Redisson's `getConfig`.
    #[doc(alias = "getConfig")]
    pub async fn config(&self) -> Result<Option<GcraConfig>> {
        let config = self.config_object();
        let fields: HashMap<String, String> = config.read(config.key_cmd("HGETALL")).await?;
        if fields.is_empty() {
            return Ok(None);
        }
        let invalid = || Error::Config("the GCRA settings are not valid".into());
        let number = |field: &str| -> Result<u64> {
            fields
                .get(field)
                .and_then(|value| value.parse().ok())
                .ok_or_else(invalid)
        };
        let period = fields
            .get("period")
            .and_then(|value| parse_period(value))
            .ok_or_else(invalid)?;
        Ok(Some(GcraConfig {
            max_burst: number("maxBurst")?,
            tokens_per_period: number("rate")?,
            period,
        }))
    }

    /// Takes one token if the limit allows it right now, Redisson's `tryAcquire`. Add `.tokens(n)` to take `n` at once.
    ///
    /// A refused request takes nothing; [`GcraResult::retry_after`] says when to try again. Times come from the server's clock. A limiter without a rate is an [`Error::Redis`] containing "GCRA rate is not set".
    ///
    /// ```no_run
    /// # async fn run(client: redissun::Client) -> redissun::Result<()> {
    /// let gcra = client.gcra("api");
    /// let result = gcra.try_acquire().tokens(2).await?;
    /// if result.limited {
    ///     println!("try again in {:?}", result.retry_after);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[doc(alias = "tryAcquire")]
    pub fn try_acquire(&self) -> TryAcquire<'_> {
        TryAcquire {
            gcra: self,
            tokens: 1,
        }
    }

    /// Whether the server has the `GCRA` command, asked once per client with a probe that the server refuses for its arguments, so it never writes.
    async fn has_gcra(&self) -> Result<bool> {
        let cell = &self.key.core.gcra_command;
        let answer = cell
            .get_or_try_init(|| async {
                let mut cmd = self.key.key_cmd("GCRA");
                cmd.arg(0).arg(1).arg(1).arg("TOKENS");
                probe_answer(self.key.query::<redis::Value>(cmd).await)
            })
            .await?;
        Ok(*answer)
    }

    async fn acquire(&self, tokens: u64) -> Result<GcraResult> {
        let tokens = tokens_arg(tokens)?;
        let script: &Lua = if self.has_gcra().await? {
            &TRY_ACQUIRE
        } else {
            &TRY_ACQUIRE_LUA
        };
        // Sent at most once: a script whose reply was lost has taken its tokens.
        let reply: redis::Value = self
            .key
            .core
            .eval_once(
                script,
                vec![self.config_key(), self.key.redis_key()],
                vec![tokens],
            )
            .await?;
        decode_result(reply)
    }

    async fn exists_any(&self) -> Result<bool> {
        let mut cmd = self.key.key_cmd("EXISTS");
        cmd.arg(self.config_key());
        let found: i64 = self.key.read(cmd).await?;
        Ok(found > 0)
    }
}

/// The `Object` methods act on the state key `name` and the settings hash `{name}:config` together. [`Object::exists`] is true when either exists, and [`Object::ttl`] is the settings hash's time to live; Redisson's `isExists`, `remainTimeToLive` and `rename` look at the state key only.
impl Object for Gcra {
    fn name(&self) -> &str {
        self.key.name()
    }

    fn del(&self) -> impl Future<Output = Result<bool>> + Send {
        self.key.del_all(vec![self.config_key()])
    }

    fn exists(&self) -> impl Future<Output = Result<bool>> + Send {
        self.exists_any()
    }

    fn rename(&self, new_name: &str) -> impl Future<Output = Result<()>> + Send {
        let companions = vec![self.config_key()];
        let renamed = vec![suffix_name(new_name, "config")];
        let new_name = new_name.to_string();
        async move { self.key.rename_all(&new_name, companions, renamed).await }
    }

    fn expire(&self, ttl: Duration) -> impl Future<Output = Result<bool>> + Send {
        self.key.expire_all(ttl, vec![self.config_key()])
    }

    fn ttl(&self) -> impl Future<Output = Result<Option<Duration>>> + Send {
        let config = self.config_object();
        async move { config.ttl().await }
    }

    fn persist(&self) -> impl Future<Output = Result<bool>> + Send {
        self.key.persist_all(vec![self.config_key()])
    }
}

/// A pending [`Gcra::try_acquire`]. Await it, optionally after [`TryAcquire::tokens`].
#[must_use = "try_acquire does nothing until it is awaited"]
#[derive(Debug)]
pub struct TryAcquire<'a> {
    gcra: &'a Gcra,
    tokens: u64,
}

impl TryAcquire<'_> {
    /// Takes `tokens` at once instead of 1, Redisson's `tryAcquire(tokens)`; the last call wins. Zero is [`Error::Config`], and nothing is sent.
    #[doc(alias = "tryAcquire")]
    pub fn tokens(mut self, tokens: u64) -> Self {
        self.tokens = tokens;
        self
    }
}

impl<'a> IntoFuture for TryAcquire<'a> {
    type Output = Result<GcraResult>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.gcra.acquire(self.tokens))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use std::time::Duration;

    #[test]
    fn period_arg_is_redissons_decimal_seconds() {
        assert_eq!(period_arg(Duration::from_secs(1)), "1");
        assert_eq!(period_arg(Duration::from_millis(250)), "0.25");
        assert_eq!(period_arg(Duration::from_millis(1500)), "1.5");
        assert_eq!(period_arg(Duration::from_secs(100)), "100");
        assert_eq!(period_arg(Duration::from_nanos(1)), "0.000000001");
        assert_eq!(period_arg(Duration::from_millis(60_010)), "60.01");
    }

    #[test]
    fn parse_period_reads_the_strings_back() {
        for period in [
            Duration::from_secs(1),
            Duration::from_millis(250),
            Duration::from_millis(1500),
            Duration::from_secs(100),
            Duration::from_nanos(1),
            Duration::from_millis(60_010),
        ] {
            assert_eq!(parse_period(&period_arg(period)), Some(period));
        }
    }

    #[test]
    fn parse_period_rejects_other_strings() {
        for text in ["0.1234567891", "-1", "", "1e3", "abc", "1.", ".5", "1.2.3"] {
            assert_eq!(parse_period(text), None, "{text:?}");
        }
    }

    fn config_error(result: crate::error::Result<Vec<bytes::Bytes>>) -> String {
        match result {
            Err(Error::Config(message)) => message,
            other => panic!("expected a config error, got {other:?}"),
        }
    }

    fn server(message: &str) -> crate::error::Result<redis::Value> {
        Err(Error::Redis(message.into()))
    }

    #[test]
    fn probe_answer_reads_the_server() {
        assert!(probe_answer(Ok(redis::Value::Okay)).unwrap());
        assert!(probe_answer(server("ERR Missing TOKENS value")).unwrap());
        assert!(!probe_answer(server(
            "ERR unknown command 'GCRA', with args beginning with: 'k' '0' '1' '1' 'TOKENS' "
        ))
        .unwrap());
        assert!(!probe_answer(server(
            "NOPERM User app has no permissions to run the 'gcra' command"
        ))
        .unwrap());
        assert!(matches!(
            probe_answer(Err(Error::Timeout)),
            Err(Error::Timeout)
        ));
    }

    fn reply(items: &[i64]) -> redis::Value {
        redis::Value::Array(items.iter().map(|&n| redis::Value::Int(n)).collect())
    }

    fn unexpected(value: redis::Value) {
        match decode_result(value) {
            Err(Error::Redis(message)) => {
                assert!(message.contains("unexpected GCRA reply"), "{message}")
            }
            other => panic!("expected an unexpected reply error, got {other:?}"),
        }
    }

    #[test]
    fn decode_result_reads_the_five_integers() {
        let allowed = decode_result(reply(&[0, 3, 2, -1, 1])).unwrap();
        assert!(!allowed.limited);
        assert_eq!(allowed.max_tokens, 3);
        assert_eq!(allowed.available_tokens, 2);
        assert_eq!(allowed.retry_after, None);
        assert_eq!(allowed.full_burst_after, Duration::from_secs(1));
        let limited = decode_result(reply(&[1, 3, 0, 2, 3])).unwrap();
        assert!(limited.limited);
        assert_eq!(limited.retry_after, Some(Duration::from_secs(2)));
        assert_eq!(limited.full_burst_after, Duration::from_secs(3));
        let never = decode_result(reply(&[1, 3, 0, -1, 0])).unwrap();
        assert!(never.limited);
        assert_eq!(never.retry_after, None);
        assert_eq!(never.full_burst_after, Duration::ZERO);
    }

    #[test]
    fn decode_result_rejects_other_replies() {
        unexpected(reply(&[0, 3, 2, -1]));
        unexpected(reply(&[0, 3, 2, -1, 1, 0]));
        unexpected(redis::Value::Int(1));
        unexpected(redis::Value::Array(vec![
            redis::Value::Int(0),
            redis::Value::Int(3),
            redis::Value::BulkString(b"2".to_vec()),
            redis::Value::Int(-1),
            redis::Value::Int(1),
        ]));
    }

    #[test]
    fn rate_args_validate_like_redisson() {
        let second = Duration::from_secs(1);
        assert!(config_error(rate_args(2, 0, second)).contains("tokens_per_period"));
        assert!(config_error(rate_args(2, 1, Duration::ZERO)).contains("period"));
        assert!(config_error(rate_args(u64::MAX, 1, second)).contains("max_burst"));
        assert!(config_error(rate_args(1, u64::MAX, second)).contains("tokens_per_period"));
        let args = rate_args(0, 1, Duration::from_millis(250)).unwrap();
        assert_eq!(args, vec!["0", "1", "0.25"]);
    }
}
