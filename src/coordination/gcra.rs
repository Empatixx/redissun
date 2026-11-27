//! [`Gcra`], Redisson's `RGcra`: a rate limiter on the Generic Cell Rate Algorithm.

use crate::error::{Error, Result};
use crate::lua::Lua;
use crate::object::{suffix_name, Key, Object};
use bytes::Bytes;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
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

/// **Experimental.** A shared rate limiter on the Generic Cell Rate Algorithm (GCRA), Redisson's `RGcra`, with the same scripts and keys.
///
/// It hands out `tokens_per_period` tokens per `period` at a steady rate, plus up to `max_burst` at once. There is no background task: the state is one timestamp, the theoretical arrival time, at the key `name`, and the settings live in the hash `{name}:config` (fields `maxBurst`, `rate`, `period`), shared with Redisson.
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
