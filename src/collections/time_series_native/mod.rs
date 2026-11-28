//! [`TimeSeriesNative`] and [`TimeSeriesNatives`], Redisson's `RTimeSeriesNative` and `RTimeSeriesNatives`: the time series of Redis 8 (or the RedisTimeSeries module), through the `TS.*` commands exactly as Redisson sends them.

mod natives;
mod options;
mod reply;

use crate::error::{Error, Result};
use crate::lua::Lua;
use crate::object::{HasKey, Key};
use bytes::Bytes;
use futures::{stream, Stream, TryStreamExt};
use options::{creation_options, range_ends, range_options, CreationOptions, RangeOptions};
use redis::Value;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::ops::RangeBounds;
use std::pin::Pin;
use std::sync::LazyLock;
use std::time::Duration;

pub use natives::{
    Filter, GroupedRange, LabelNames, LabelValues, MultiGet, MultiRange, SeriesSample,
    SeriesSamples, TimeSeriesNatives,
};
pub use options::{
    Aggregator, Align, BucketTimestamp, DuplicatePolicy, Encoding, ReadFrom, Reducer,
};
pub use reply::{ChunkInfo, Rule, Sample, TsInfo};

/// The page size of [`TimeSeriesNative::iter`], Redisson's `iterator()`.
const ITER_PAGE: u64 = 10;

/// Redisson's `createIfAbsentAsync` script.
static TRY_CREATE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "if redis.call('exists', KEYS[1]) == 0 then redis.call('TS.CREATE', KEYS[1], unpack(ARGV)); return 1; end; return 0;",
    )
});

/// An "unknown command" error means the server has no time series: [`Error::Unsupported`]. Nothing is cached, so a server that gains the module is used at once.
pub(crate) fn unsupported(error: Error) -> Error {
    match &error {
        Error::Redis(message) => {
            let lower = message.to_ascii_lowercase();
            if lower.contains("unknown command") || lower.contains("unknown redis command") {
                Error::Unsupported(
                    "TimeSeriesNative needs Redis 8.0 or newer, or the RedisTimeSeries module"
                        .into(),
                )
            } else {
                error
            }
        }
        _ => error,
    }
}

/// The arguments a `redis::Cmd` holds, for a script's ARGV.
fn cmd_args(cmd: &redis::Cmd) -> Vec<Bytes> {
    cmd.args_iter()
        .filter_map(|arg| match arg {
            redis::Arg::Simple(bytes) => Some(Bytes::copy_from_slice(bytes)),
            _ => None,
        })
        .collect()
}

/// **Experimental.** One time series of Redis 8 (or the RedisTimeSeries module), Redisson's `RTimeSeriesNative`. Values are `f64`, so no codec is involved.
///
/// It needs Redis 8.0 or newer, or the module; on another server every method returns [`Error::Unsupported`]. A range with several aggregators, `countNaN` and `countAll` need Redis 8.6. [`Object`](crate::Object) covers the key.
///
/// ```no_run
/// # async fn run(client: redissun::Client) -> redissun::Result<()> {
/// use redissun::time_series_native::Aggregator;
/// use std::time::Duration;
///
/// let cpu = client.time_series_native("cpu:1");
/// cpu.create().retention(Duration::from_secs(3600)).label("host", "a").await?;
/// cpu.add(1_000, 0.5).await?;
/// cpu.add(2_000, 0.7).await?;
/// let avg = cpu.range(..).aggregate(Duration::from_secs(60), &[Aggregator::Avg]).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct TimeSeriesNative {
    key: Key,
}

impl fmt::Debug for TimeSeriesNative {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "TimeSeriesNative")
    }
}

impl HasKey for TimeSeriesNative {
    fn key(&self) -> &Key {
        &self.key
    }
}

impl TimeSeriesNative {
    pub(crate) fn new(key: Key) -> Self {
        Self { key }
    }

    fn cmd(&self, name: &str) -> redis::Cmd {
        self.key.key_cmd(name)
    }

    async fn write<T: redis::FromRedisValue>(&self, cmd: redis::Cmd) -> Result<T> {
        self.key.query(cmd).await.map_err(unsupported)
    }

    /// A write that is not safe to repeat (`TS.ADD` under `SUM`, `TS.INCRBY`, `TS.MADD`): sent at most once.
    async fn write_once<T: redis::FromRedisValue>(&self, cmd: redis::Cmd) -> Result<T> {
        self.key.query_once(cmd).await.map_err(unsupported)
    }

    async fn read_cmd<T: redis::FromRedisValue>(&self, cmd: redis::Cmd) -> Result<T> {
        self.key.read(cmd).await.map_err(unsupported)
    }

    /// Creates the series (`TS.CREATE`), Redisson's `create`. An existing key is an error from the server.
    pub fn create(&self) -> Create<'_> {
        Create {
            series: self,
            creation: CreationOptions::default(),
        }
    }

    /// Creates the series unless the key exists, Redisson's `createIfAbsent`. Resolves to whether it created it.
    #[doc(alias = "createIfAbsent")]
    pub fn try_create(&self) -> TryCreate<'_> {
        TryCreate {
            series: self,
            creation: CreationOptions::default(),
        }
    }

    /// Changes the options of the series (`TS.ALTER`), Redisson's `alter`. Options left out stay; `labels` replaces every label.
    pub fn alter(&self) -> Alter<'_> {
        Alter {
            series: self,
            creation: CreationOptions::default(),
        }
    }

    /// Adds a sample (`TS.ADD`), creating the series with the given options when it is missing, Redisson's `add`. Resolves to the timestamp stored.
    pub fn add(&self, timestamp: i64, value: f64) -> Add<'_> {
        Add {
            series: self,
            timestamp: Some(timestamp),
            value,
            on_duplicate: None,
            creation: CreationOptions::default(),
        }
    }

    /// Adds a sample at the server's clock (`TS.ADD key * value`), Redisson's `addCurrent`.
    #[doc(alias = "addCurrent")]
    pub fn add_now(&self, value: f64) -> Add<'_> {
        Add {
            series: self,
            timestamp: None,
            value,
            on_duplicate: None,
            creation: CreationOptions::default(),
        }
    }

    /// Adds samples in one `TS.MADD`, Redisson's `addAll`. Resolves to the timestamps stored, in input order. An empty input sends nothing. It does not create the series; a sample the server refuses makes the call fail, while the others may be stored.
    #[doc(alias = "addAll")]
    pub async fn add_many(
        &self,
        samples: impl IntoIterator<Item = (i64, f64)>,
    ) -> Result<Vec<i64>> {
        let mut cmd = redis::cmd("TS.MADD");
        let mut any = false;
        for (ts, value) in samples {
            cmd.arg(self.key.redis_key()).arg(ts).arg(value.to_string());
            any = true;
        }
        if !any {
            return Ok(Vec::new());
        }
        let reply: Value = self.write_once(cmd).await?;
        madd_reply(&reply)
    }

    /// Adds `value` to the newest sample (`TS.INCRBY`), Redisson's `incrementBy`. Resolves to the timestamp it wrote.
    #[doc(alias = "incrementBy")]
    pub fn incr_by(&self, value: f64) -> IncrBy<'_> {
        IncrBy {
            series: self,
            command: "TS.INCRBY",
            value,
            timestamp: None,
            creation: CreationOptions::default(),
        }
    }

    /// Subtracts `value` from the newest sample (`TS.DECRBY`), Redisson's `decrementBy`.
    #[doc(alias = "decrementBy")]
    pub fn decr_by(&self, value: f64) -> IncrBy<'_> {
        IncrBy {
            series: self,
            command: "TS.DECRBY",
            value,
            timestamp: None,
            creation: CreationOptions::default(),
        }
    }

    /// Removes the samples in `range` (`TS.DEL`), Redisson's `removeRange`. Resolves to how many it removed. Unbounded ends are `0` and `i64::MAX`, as the module's timestamps are not negative.
    #[doc(alias = "removeRange")]
    pub async fn remove_range(&self, range: impl RangeBounds<i64>) -> Result<u64> {
        let Some((from, to)) = range_ends(&range) else {
            return Ok(0);
        };
        let end = |e: options::RangeEnd, open: i64| match e {
            options::RangeEnd::At(t) => t,
            _ => open,
        };
        let mut cmd = self.cmd("TS.DEL");
        cmd.arg(end(from, 0)).arg(end(to, i64::MAX));
        let removed: i64 = self.write(cmd).await?;
        Ok(u64::try_from(removed).unwrap_or(0))
    }

    /// The oldest sample (`TS.RANGE key - + COUNT 1`), Redisson's `first`.
    pub async fn first(&self) -> Result<Option<Sample>> {
        let mut cmd = self.cmd("TS.RANGE");
        cmd.arg("-").arg("+").arg("COUNT").arg(1);
        let reply: Value = self.read_cmd(cmd).await?;
        Ok(reply::samples(&reply)?.into_iter().next())
    }

    /// The number of samples (`TS.INFO`), Redisson's `size`.
    #[doc(alias = "size")]
    pub async fn len(&self) -> Result<u64> {
        Ok(self.info().await?.total_samples)
    }

    /// Whether the series holds no sample.
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// The oldest timestamp, `None` for an empty series, Redisson's `firstTimestamp`.
    #[doc(alias = "firstTimestamp")]
    pub async fn first_timestamp(&self) -> Result<Option<i64>> {
        Ok(self.info().await?.first_timestamp)
    }

    /// The newest timestamp, `None` for an empty series, Redisson's `lastTimestamp`.
    #[doc(alias = "lastTimestamp")]
    pub async fn last_timestamp(&self) -> Result<Option<i64>> {
        Ok(self.info().await?.last_timestamp)
    }

    /// The newest sample (`TS.GET`), Redisson's `get` and, with [`Get::latest`], `getLatest`.
    #[doc(alias = "getLatest")]
    pub fn get(&self) -> Get<'_> {
        Get {
            series: self,
            latest: false,
        }
    }

    /// The samples in `range`, oldest first (`TS.RANGE`; newest first with [`Range::rev`], `TS.REVRANGE`), Redisson's `range` and `rangeReversed`. `range(..)` is `- +`.
    #[doc(alias = "rangeReversed")]
    pub fn range(&self, range: impl RangeBounds<i64>) -> Range<'_> {
        Range {
            series: self,
            ends: range_ends(&range),
            rev: false,
            range: RangeOptions::default(),
        }
    }

    /// The samples after a position (`TS.READ`), Redisson's `read`; with [`Read::block`] it waits for them.
    pub fn read(&self, from: ReadFrom) -> Read<'_> {
        Read {
            series: self,
            from,
            block: None,
            max_count: None,
        }
    }

    /// Writes compactions of this series into `destination` (`TS.CREATERULE`), Redisson's `createRule`.
    #[doc(alias = "createRule")]
    pub fn create_rule<'a>(
        &'a self,
        destination: &'a str,
        aggregator: Aggregator,
        bucket: Duration,
    ) -> CreateRule<'a> {
        CreateRule {
            series: self,
            destination,
            aggregator,
            bucket,
            align: None,
        }
    }

    /// Removes the rule to `destination` (`TS.DELETERULE`), Redisson's `deleteRule`.
    #[doc(alias = "deleteRule")]
    pub async fn delete_rule(&self, destination: &str) -> Result<()> {
        let mut cmd = self.cmd("TS.DELETERULE");
        cmd.arg(destination);
        let _: Value = self.write(cmd).await?;
        Ok(())
    }

    /// The labels of the series, in the server's order, Redisson's `getLabels`.
    #[doc(alias = "getLabels")]
    pub async fn labels(&self) -> Result<Vec<(String, String)>> {
        Ok(self.info().await?.labels)
    }

    /// `TS.INFO`, Redisson's `getInfo`; with [`Info::debug`] it adds the chunks, Redisson's `getDebugInfo`.
    #[doc(alias = "getInfo")]
    #[doc(alias = "getDebugInfo")]
    pub fn info(&self) -> Info<'_> {
        Info {
            series: self,
            debug: false,
        }
    }

    /// Streams every sample, oldest first, 10 per `TS.RANGE`, Redisson's `iterator()` and `stream()`. A series that disappears midway yields an error, not a silent end.
    #[doc(alias = "iterator")]
    #[doc(alias = "stream")]
    pub fn iter(&self) -> impl Stream<Item = Result<Sample>> + '_ {
        self.iter_with(ITER_PAGE)
    }

    /// Like [`iter`](TimeSeriesNative::iter) with `page_size` samples per call (at least 1), Redisson's `iterator(count)`.
    pub fn iter_with(&self, page_size: u64) -> impl Stream<Item = Result<Sample>> + '_ {
        let page = page_size.max(1);
        stream::try_unfold(Some(0i64), move |from| async move {
            let Some(from) = from else {
                return Ok::<_, Error>(None);
            };
            let mut cmd = self.cmd("TS.RANGE");
            cmd.arg(from).arg("+").arg("COUNT").arg(page);
            let reply: Value = self.read_cmd(cmd).await?;
            let samples = reply::samples(&reply)?;
            let next = match samples.last() {
                Some(last) if samples.len() as u64 == page => last.timestamp.checked_add(1),
                _ => None,
            };
            Ok(Some((stream::iter(samples.into_iter().map(Ok)), next)))
        })
        .try_flatten()
    }
}

/// The reply of `TS.MADD`: a timestamp per sample, or an error for a sample the server refused.
fn madd_reply(reply: &Value) -> Result<Vec<i64>> {
    reply::items(reply)
        .iter()
        .map(|item| match item {
            Value::ServerError(e) => Err(Error::Redis(format!("{e:?}"))),
            other => reply::long(other),
        })
        .collect()
}

/// A pending [`TimeSeriesNative::create`]. Await it, optionally after the creation options.
#[must_use = "a create does nothing until it is awaited"]
pub struct Create<'a> {
    series: &'a TimeSeriesNative,
    creation: CreationOptions,
}

impl Create<'_> {
    creation_options!();
}

impl<'a> IntoFuture for Create<'a> {
    type Output = Result<()>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let mut cmd = self.series.cmd("TS.CREATE");
            self.creation.append(&mut cmd)?;
            let _: Value = self.series.write_once(cmd).await?;
            Ok(())
        })
    }
}

/// A pending [`TimeSeriesNative::try_create`]. Await it, optionally after the creation options.
#[must_use = "a create does nothing until it is awaited"]
pub struct TryCreate<'a> {
    series: &'a TimeSeriesNative,
    creation: CreationOptions,
}

impl TryCreate<'_> {
    creation_options!();
}

impl<'a> IntoFuture for TryCreate<'a> {
    type Output = Result<bool>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let mut cmd = redis::cmd("X");
            self.creation.append(&mut cmd)?;
            let mut args = cmd_args(&cmd);
            args.remove(0);
            let created: i64 = self
                .series
                .key
                .core
                .eval(&TRY_CREATE, vec![self.series.key.redis_key()], args)
                .await
                .map_err(unsupported)?;
            Ok(created == 1)
        })
    }
}

/// A pending [`TimeSeriesNative::alter`]. Await it after setting the options to change.
#[must_use = "an alter does nothing until it is awaited"]
pub struct Alter<'a> {
    series: &'a TimeSeriesNative,
    creation: CreationOptions,
}

impl Alter<'_> {
    creation_options!();
}

impl<'a> IntoFuture for Alter<'a> {
    type Output = Result<()>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let mut cmd = self.series.cmd("TS.ALTER");
            self.creation.append(&mut cmd)?;
            let _: Value = self.series.write(cmd).await?;
            Ok(())
        })
    }
}

/// A pending [`TimeSeriesNative::add`] or [`add_now`](TimeSeriesNative::add_now). Await it, optionally after [`Add::on_duplicate`] and the creation options used when the series is missing.
#[must_use = "an add does nothing until it is awaited"]
pub struct Add<'a> {
    series: &'a TimeSeriesNative,
    timestamp: Option<i64>,
    value: f64,
    on_duplicate: Option<DuplicatePolicy>,
    creation: CreationOptions,
}

impl Add<'_> {
    /// What this sample does at a taken timestamp, overriding the series' policy (`ON_DUPLICATE`).
    pub fn on_duplicate(mut self, policy: DuplicatePolicy) -> Self {
        self.on_duplicate = Some(policy);
        self
    }

    creation_options!();
}

impl<'a> IntoFuture for Add<'a> {
    type Output = Result<i64>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let mut cmd = self.series.cmd("TS.ADD");
            match self.timestamp {
                Some(ts) => cmd.arg(ts),
                None => cmd.arg("*"),
            };
            cmd.arg(self.value.to_string());
            if let Some(policy) = self.on_duplicate {
                cmd.arg("ON_DUPLICATE").arg(policy.keyword());
            }
            self.creation.append(&mut cmd)?;
            self.series.write_once(cmd).await
        })
    }
}

/// A pending [`TimeSeriesNative::incr_by`] or [`decr_by`](TimeSeriesNative::decr_by). Await it, optionally after [`IncrBy::timestamp`] and the creation options.
#[must_use = "an increment does nothing until it is awaited"]
pub struct IncrBy<'a> {
    series: &'a TimeSeriesNative,
    command: &'static str,
    value: f64,
    timestamp: Option<i64>,
    creation: CreationOptions,
}

impl IncrBy<'_> {
    /// Writes at `timestamp` instead of the server's clock (`TIMESTAMP`).
    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    creation_options!();
}

impl<'a> IntoFuture for IncrBy<'a> {
    type Output = Result<i64>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let mut cmd = self.series.cmd(self.command);
            cmd.arg(self.value.to_string());
            if let Some(ts) = self.timestamp {
                cmd.arg("TIMESTAMP").arg(ts);
            }
            self.creation.append(&mut cmd)?;
            self.series.write_once(cmd).await
        })
    }
}

/// A pending [`TimeSeriesNative::get`]. Await it, optionally after [`Get::latest`].
#[must_use = "a get does nothing until it is awaited"]
pub struct Get<'a> {
    series: &'a TimeSeriesNative,
    latest: bool,
}

impl Get<'_> {
    /// Reports the latest, possibly partial, bucket of a compaction (`LATEST`), Redisson's `getLatest`.
    pub fn latest(mut self) -> Self {
        self.latest = true;
        self
    }
}

impl<'a> IntoFuture for Get<'a> {
    type Output = Result<Option<Sample>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let mut cmd = self.series.cmd("TS.GET");
            if self.latest {
                cmd.arg("LATEST");
            }
            let reply: Value = self.series.read_cmd(cmd).await?;
            reply::sample(&reply)
        })
    }
}

/// A pending [`TimeSeriesNative::range`]. Await it, optionally after [`Range::rev`] and the range options.
#[must_use = "a range does nothing until it is awaited"]
pub struct Range<'a> {
    series: &'a TimeSeriesNative,
    ends: Option<(options::RangeEnd, options::RangeEnd)>,
    rev: bool,
    range: RangeOptions,
}

impl Range<'_> {
    /// Newest first (`TS.REVRANGE`), Redisson's `rangeReversed`.
    pub fn rev(mut self) -> Self {
        self.rev = true;
        self
    }

    range_options!();
}

impl<'a> IntoFuture for Range<'a> {
    type Output = Result<Vec<Sample>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let Some((from, to)) = self.ends else {
                return Ok(Vec::new());
            };
            let mut cmd = self
                .series
                .cmd(if self.rev { "TS.REVRANGE" } else { "TS.RANGE" });
            self.range.append(from, to, &[], &mut cmd)?;
            let reply: Value = self.series.read_cmd(cmd).await?;
            reply::samples(&reply)
        })
    }
}

/// A pending [`TimeSeriesNative::read`]. Await it, optionally after [`Read::block`] and [`Read::max_count`].
#[must_use = "a read does nothing until it is awaited"]
pub struct Read<'a> {
    series: &'a TimeSeriesNative,
    from: ReadFrom,
    block: Option<(Duration, u64)>,
    max_count: Option<u64>,
}

impl Read<'_> {
    /// Waits up to `timeout` until at least `min_count` samples are there (`BLOCK`), on a connection of its own without the command timeout. A timeout under 1 ms is an [`Error::Config`].
    pub fn block(mut self, timeout: Duration, min_count: u64) -> Self {
        self.block = Some((timeout, min_count));
        self
    }

    /// At most `n` samples (`MAX_COUNT`).
    pub fn max_count(mut self, n: u64) -> Self {
        self.max_count = Some(n);
        self
    }
}

impl<'a> IntoFuture for Read<'a> {
    type Output = Result<Vec<Sample>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let mut cmd = self.series.cmd("TS.READ");
            cmd.arg(self.from.arg());
            if let Some((timeout, min_count)) = self.block {
                let ms = options::millis(timeout);
                if ms < 1 {
                    return Err(Error::Config("block timeout must be at least 1ms".into()));
                }
                cmd.arg("BLOCK").arg(ms).arg(min_count);
            }
            if let Some(n) = self.max_count {
                cmd.arg("MAX_COUNT").arg(n);
            }
            let reply: Value = if self.block.is_some() {
                let key = self.series.key.redis_key();
                self.series
                    .key
                    .core
                    .blocking(key.as_bytes(), cmd)
                    .await
                    .map_err(unsupported)?
            } else {
                self.series.read_cmd(cmd).await?
            };
            reply::samples(&reply)
        })
    }
}

/// A pending [`TimeSeriesNative::create_rule`]. Await it, optionally after [`CreateRule::align_timestamp`].
#[must_use = "a rule does nothing until it is awaited"]
pub struct CreateRule<'a> {
    series: &'a TimeSeriesNative,
    destination: &'a str,
    aggregator: Aggregator,
    bucket: Duration,
    align: Option<i64>,
}

impl CreateRule<'_> {
    /// Aligns the buckets on `timestamp`.
    pub fn align_timestamp(mut self, timestamp: i64) -> Self {
        self.align = Some(timestamp);
        self
    }
}

impl<'a> IntoFuture for CreateRule<'a> {
    type Output = Result<()>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let mut cmd = self.series.cmd("TS.CREATERULE");
            cmd.arg(self.destination)
                .arg("AGGREGATION")
                .arg(self.aggregator.keyword())
                .arg(options::millis(self.bucket));
            if let Some(ts) = self.align {
                cmd.arg(ts);
            }
            let _: Value = self.series.write(cmd).await?;
            Ok(())
        })
    }
}

/// A pending [`TimeSeriesNative::info`]. Await it, optionally after [`Info::debug`].
#[must_use = "an info does nothing until it is awaited"]
pub struct Info<'a> {
    series: &'a TimeSeriesNative,
    debug: bool,
}

impl Info<'_> {
    /// Adds the chunks (`DEBUG`), Redisson's `getDebugInfo`.
    pub fn debug(mut self) -> Self {
        self.debug = true;
        self
    }
}

impl<'a> IntoFuture for Info<'a> {
    type Output = Result<TsInfo>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let mut cmd = self.series.cmd("TS.INFO");
            if self.debug {
                cmd.arg("DEBUG");
            }
            let reply: Value = self.series.read_cmd(cmd).await?;
            reply::info(&reply)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_commands_mean_unsupported() {
        for message in [
            "ERR unknown command 'TS.ADD', with args beginning with: ",
            "ERR Error running script: Unknown Redis command called from script",
        ] {
            assert!(matches!(
                unsupported(Error::Redis(message.into())),
                Error::Unsupported(m) if m.contains("8.0")
            ));
        }
        assert!(matches!(
            unsupported(Error::Redis("ERR TSDB: key already exists".into())),
            Error::Redis(_)
        ));
        assert!(matches!(unsupported(Error::OutOfRange), Error::OutOfRange));
    }
}
