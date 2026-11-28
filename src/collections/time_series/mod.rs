//! [`TimeSeries`], Redisson's `RTimeSeries`: values by timestamp with labels, per-entry TTL and retention, on Redisson's keys and Lua scripts.

mod aggregate;
mod builders;
mod ranges;
mod reads;
mod scripts;
mod timestamp;

use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::eviction;
use crate::object::{now_millis, prefix_name, HasKey, Key};
use crate::reply;
use bytes::Bytes;
use scripts::{
    ADD_COMPARING, ADD_ENTRY, ADD_IF_ABSENT, ADD_OR_REPLACE, EVICT, EXTEND, EXTEND_TTL, GET,
    GET_ENTRY, INCREMENT, SIZE,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;
use std::sync::{Arc, OnceLock};

pub use aggregate::{Aggregate, Aggregation, TimeSeriesBucket, TimeSeriesInfo};
pub use builders::{Add, Extend, Insert, Set, SetIfGreater, SetIfLess, SetSum, TryInsert};
pub use ranges::{Range, RangeEntries, ReadAfter, RemoveRange};
pub use timestamp::{TimeSeriesEntry, Timestamp};

/// Redisson's `getTimeoutSetName`: the sorted set of expiry times.
fn ttl_name(name: &str) -> String {
    prefix_name("redissun__ts_ttl", name)
}

/// Redisson's `getSequenceName`: the hash of the id counter and `add`'s running total.
fn seq_name(name: &str) -> String {
    prefix_name("redissun__ts_seq", name)
}

/// **Experimental.** Values ordered by timestamp, each with an optional label and time to live, Redisson's `RTimeSeries`, sharing its data with Redisson.
///
/// A timestamp is signed milliseconds ([`Timestamp`]). A timestamp can hold several entries: [`insert`](TimeSeries::insert) keeps what is there and adds one, in insertion order. Expired entries are invisible to every read and are deleted by a background task, as Redisson's eviction task does: it first runs [`ClientBuilder::eviction_interval`](crate::ClientBuilder::eviction_interval) after the handle is created and removes up to 100 entries per run. Expiry uses each client's clock.
///
/// The data lives in three keys, as in Redisson 4.8.0: the sorted set `name` scored by timestamp, the sorted set `redissun__ts_ttl:{name}` scored by expiry (Unix milliseconds) and the hash `redissun__ts_seq:{name}` (fields `id`, `total`, `incremented`). Each member is `struct.pack('BBc0Lc0Lc0', 4, #id, id, #value, value, #field, field)`, where `id` is a 20-digit counter and `field` a flag byte (`2` no label, `3` label) followed by the encoded label. [`Object`](crate::Object) covers all three keys.
///
/// ```no_run
/// # async fn run(client: redissun::Client) -> redissun::Result<()> {
/// use std::time::Duration;
///
/// let temperatures = client.time_series::<f64, String>("temperatures");
/// temperatures.insert(1_000, &21.5).label("kitchen").await?;
/// temperatures
///     .insert(2_000, &22.0)
///     .ttl(Duration::from_secs(3600))
///     .await?;
/// assert_eq!(temperatures.get(1_000).await?, Some(21.5));
/// # Ok(())
/// # }
/// ```
pub struct TimeSeries<V, L, C: Codec> {
    key: Key,
    codec: C,
    /// Whether the codec encodes numbers as text, Redisson's `numericCodec`, probed once per handle.
    numeric: Arc<OnceLock<bool>>,
    _marker: PhantomData<fn() -> (V, L)>,
}

impl<V, L, C: Codec> Clone for TimeSeries<V, L, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
            numeric: self.numeric.clone(),
            _marker: PhantomData,
        }
    }
}

impl<V, L, C: Codec> fmt::Debug for TimeSeries<V, L, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "TimeSeries")
    }
}

impl<V, L, C: Codec> HasKey for TimeSeries<V, L, C> {
    fn key(&self) -> &Key {
        &self.key
    }

    fn companions(&self) -> Vec<String> {
        vec![ttl_name(self.key.name()), seq_name(self.key.name())]
    }

    fn renamed_companions(&self, new_name: &str) -> Option<Vec<String>> {
        Some(vec![ttl_name(new_name), seq_name(new_name)])
    }
}

impl<V, L, C: Codec> TimeSeries<V, L, C> {
    /// The handle of the series at `key`; it schedules the clean-up task, as Redisson's constructor does.
    pub(crate) fn new(key: Key, codec: C) -> Self {
        let series = Self {
            key,
            codec,
            numeric: Arc::new(OnceLock::new()),
            _marker: PhantomData,
        };
        eviction::schedule(
            &series.key.core,
            format!("redissun__time_series_eviction:{}", series.key.name()),
            &EVICT,
            series.all_keys(),
            eviction::time_series_arguments,
            Vec::new(),
        );
        series
    }

    /// `[name, redissun__ts_ttl:{name}, redissun__ts_seq:{name}]`.
    fn all_keys(&self) -> Vec<String> {
        let name = self.key.name();
        vec![name.to_string(), ttl_name(name), seq_name(name)]
    }

    /// `[name, redissun__ts_ttl:{name}]`, the keys of the scripts that leave the counter alone.
    fn data_keys(&self) -> Vec<String> {
        let name = self.key.name();
        vec![name.to_string(), ttl_name(name)]
    }

    /// `[now, timestamp]`, the ARGV of the scripts that read one timestamp.
    fn at(timestamp: impl Into<Timestamp>) -> Vec<Bytes> {
        vec![
            Bytes::from(now_millis().to_string()),
            Bytes::from(timestamp.into().as_millis().to_string()),
        ]
    }

    async fn write_flag(&self, script: &crate::lua::Lua, args: Vec<Bytes>) -> Result<bool> {
        let written: i64 = self
            .key
            .core
            .eval_once(script, self.all_keys(), args)
            .await?;
        Ok(written == 1)
    }

    async fn run_insert(&self, args: Vec<Bytes>) -> Result<bool> {
        self.write_flag(&ADD_ENTRY, args).await
    }

    async fn run_try_insert(&self, args: Vec<Bytes>) -> Result<bool> {
        self.write_flag(&ADD_IF_ABSENT, args).await
    }

    async fn run_set(&self, args: Vec<Bytes>) -> Result<bool> {
        self.write_flag(&ADD_OR_REPLACE, args).await
    }

    /// Redisson's `checkNumericCodec`: the comparing policies and `add` read stored values with `tonumber`, so the codec must encode numbers as text. A codec that cannot encode a number at all is left to the script.
    fn check_numeric(&self, method: &str) -> Result<()> {
        let numeric = *self.numeric.get_or_init(|| {
            [1.5f64, -2.25f64]
                .iter()
                .all(|probe| match self.codec.encode(probe) {
                    Err(_) => true,
                    Ok(bytes) => std::str::from_utf8(&bytes)
                        .ok()
                        .filter(|text| text.is_ascii())
                        .and_then(|text| text.trim().parse::<f64>().ok())
                        .is_some_and(|parsed| parsed == *probe),
                })
        });
        if numeric {
            Ok(())
        } else {
            Err(Error::Config(format!(
                "{method} on '{}' requires a codec that encodes numbers as text; use JsonCodec or StringCodec",
                self.key.name()
            )))
        }
    }

    /// `ADD_COMPARING` with Redisson's policy `1` (less), `2` (greater) or `3` (sum).
    async fn run_comparing(
        &self,
        method: &str,
        policy: &'static str,
        mut args: Vec<Bytes>,
    ) -> Result<bool> {
        self.check_numeric(method)?;
        args.push(Bytes::from_static(policy.as_bytes()));
        self.write_flag(&ADD_COMPARING, args).await
    }

    async fn run_set_if_less(&self, args: Vec<Bytes>) -> Result<bool> {
        self.run_comparing("set_if_less", "1", args).await
    }

    async fn run_set_if_greater(&self, args: Vec<Bytes>) -> Result<bool> {
        self.run_comparing("set_if_greater", "2", args).await
    }

    async fn run_set_sum(&self, args: Vec<Bytes>) -> Result<bool> {
        self.run_comparing("set_sum", "3", args).await
    }

    /// `INCREMENT`, Redisson's `addAndGet`: the reply is the running total as text.
    async fn run_add(&self, args: Vec<Bytes>) -> Result<f64> {
        self.check_numeric("add")?;
        let total: String = self
            .key
            .core
            .eval_once(&INCREMENT, self.all_keys(), args)
            .await?;
        total
            .parse()
            .map_err(|_| Error::Codec(format!("not a number: {total}")))
    }

    /// Adds `value` at `timestamp`, keeping the entries already there, Redisson's `add`. Resolves to `false` only when the entry is behind its [`retention`](Insert::retention) window and was not stored.
    #[doc(alias = "add")]
    pub fn insert<W>(&self, timestamp: impl Into<Timestamp>, value: &W) -> Insert<'_, V, L, C>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized,
    {
        Insert::new(self, timestamp.into().as_millis(), self.codec.encode(value))
    }

    /// Adds `value` at `timestamp` only when no live entry is there, Redisson's `addIfAbsent`; expired entries there are deleted and count as absent. Resolves to whether it stored the entry.
    #[doc(alias = "addIfAbsent")]
    pub fn try_insert<W>(
        &self,
        timestamp: impl Into<Timestamp>,
        value: &W,
    ) -> TryInsert<'_, V, L, C>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized,
    {
        TryInsert::new(self, timestamp.into().as_millis(), self.codec.encode(value))
    }

    /// Replaces every entry at `timestamp` with this one, Redisson's `addOrReplace`. Resolves to `true` when no live entry was there (or nothing was stored because of the retention window: then `false`).
    #[doc(alias = "addOrReplace")]
    pub fn set<W>(&self, timestamp: impl Into<Timestamp>, value: &W) -> Set<'_, V, L, C>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized,
    {
        Set::new(self, timestamp.into().as_millis(), self.codec.encode(value))
    }

    /// Stores `value` at `timestamp` when it is less than every live value there (or none is there), replacing them with this one entry, Redisson's `addIfLess`. Needs a codec that encodes numbers as text. Resolves to whether it stored the value.
    #[doc(alias = "addIfLess")]
    pub fn set_if_less<W>(
        &self,
        timestamp: impl Into<Timestamp>,
        value: &W,
    ) -> SetIfLess<'_, V, L, C>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized,
    {
        SetIfLess::new(self, timestamp.into().as_millis(), self.codec.encode(value))
    }

    /// Like [`set_if_less`](TimeSeries::set_if_less) for a greater value, Redisson's `addIfGreater`.
    #[doc(alias = "addIfGreater")]
    pub fn set_if_greater<W>(
        &self,
        timestamp: impl Into<Timestamp>,
        value: &W,
    ) -> SetIfGreater<'_, V, L, C>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized,
    {
        SetIfGreater::new(self, timestamp.into().as_millis(), self.codec.encode(value))
    }

    /// Replaces the live entries at `timestamp` with one holding their sum plus `value`, Redisson's `addAndSum`. Needs a codec that encodes numbers as text.
    #[doc(alias = "addAndSum")]
    pub fn set_sum<W>(&self, timestamp: impl Into<Timestamp>, value: &W) -> SetSum<'_, V, L, C>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized,
    {
        SetSum::new(self, timestamp.into().as_millis(), self.codec.encode(value))
    }

    /// Adds `delta` to the series' running total and records the new total at `timestamp`, Redisson's `addAndGet`. Resolves to the total. Needs a codec that encodes numbers as text.
    #[doc(alias = "addAndGet")]
    pub fn add<W>(&self, timestamp: impl Into<Timestamp>, delta: &W) -> Add<'_, V, L, C>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized,
    {
        Add::new(self, timestamp.into().as_millis(), self.codec.encode(delta))
    }

    /// Adds every entry, keeping what is at their timestamps, Redisson's `addAll`. Takes `(timestamp, value)`, `(timestamp, value, label)` or [`TimeSeriesEntry`] items; optionally with [`Extend::ttl`]. An empty input sends nothing.
    #[doc(alias = "addAll")]
    pub fn extend<I, E>(&self, entries: I) -> Extend<'_, V, L, C>
    where
        I: IntoIterator<Item = E>,
        E: Into<TimeSeriesEntry<V, L>>,
        V: Serialize,
        L: Serialize,
    {
        let encoded = entries
            .into_iter()
            .map(|entry| -> Result<(i64, Bytes, Option<Bytes>)> {
                let entry = entry.into();
                let label = entry
                    .label
                    .as_ref()
                    .map(|l| self.codec.encode(l))
                    .transpose()?;
                Ok((entry.timestamp, self.codec.encode(&entry.value)?, label))
            })
            .collect();
        Extend::new(self, encoded)
    }

    /// Runs Redisson's `addAll(Collection, ttl)` scripts.
    async fn run_extend(&self, args: Vec<Bytes>, ttl: bool) -> Result<()> {
        let script = if ttl { &EXTEND_TTL } else { &EXTEND };
        let _: redis::Value = self
            .key
            .core
            .eval_once(script, self.all_keys(), args)
            .await?;
        Ok(())
    }

    /// The number of live entries, Redisson's `size`.
    #[doc(alias = "size")]
    pub async fn len(&self) -> Result<usize> {
        let size: i64 = self
            .key
            .core
            .eval(
                &SIZE,
                self.data_keys(),
                vec![Bytes::from(now_millis().to_string())],
            )
            .await?;
        Ok(usize::try_from(size).unwrap_or(0))
    }

    /// Whether the series holds no live entry.
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }
}

impl<V, L, C> TimeSeries<V, L, C>
where
    V: DeserializeOwned,
    L: DeserializeOwned,
    C: Codec,
{
    fn decode_value(&self, value: &redis::Value) -> Result<V> {
        let bytes = reply::bytes(value).ok_or_else(reply::malformed)?;
        self.codec.decode(&bytes)
    }

    fn decode_optional(&self, value: &redis::Value) -> Result<Option<V>> {
        match value {
            redis::Value::Nil => Ok(None),
            value => self.decode_value(value).map(Some),
        }
    }

    /// The label of an entry whose flag is `flag`: present only for `3`, as Redisson's decoders read it.
    fn decode_label(&self, flag: i64, label: Option<&redis::Value>) -> Result<Option<L>> {
        if flag != 3 {
            return Ok(None);
        }
        let label = label.and_then(reply::bytes).ok_or_else(reply::malformed)?;
        self.codec.decode(&label).map(Some)
    }

    /// One entry as `[flag, timestamp, value[, label]]`, Redisson's `TimeSeriesSingleEntryReplayDecoder`; `None` for a nil reply.
    fn decode_entry(&self, reply: &redis::Value) -> Result<Option<TimeSeriesEntry<V, L>>> {
        if *reply == redis::Value::Nil {
            return Ok(None);
        }
        let items = reply::array(reply);
        let (Some(flag), Some(timestamp), Some(value)) = (
            items.first().and_then(reply::number),
            items.get(1).and_then(reply::number),
            items.get(2),
        ) else {
            return Err(reply::malformed());
        };
        Ok(Some(TimeSeriesEntry {
            timestamp,
            value: self.decode_value(value)?,
            label: self.decode_label(flag, items.get(3))?,
        }))
    }

    /// The value of the first live entry at `timestamp`, Redisson's `get`.
    pub async fn get(&self, timestamp: impl Into<Timestamp>) -> Result<Option<V>> {
        let value: redis::Value = self
            .key
            .core
            .eval(&GET, self.data_keys(), Self::at(timestamp))
            .await?;
        self.decode_optional(&value)
    }

    /// The first live entry at `timestamp` with its label, Redisson's `getEntry`.
    #[doc(alias = "getEntry")]
    pub async fn entry(
        &self,
        timestamp: impl Into<Timestamp>,
    ) -> Result<Option<TimeSeriesEntry<V, L>>> {
        let reply: redis::Value = self
            .key
            .core
            .eval(&GET_ENTRY, self.data_keys(), Self::at(timestamp))
            .await?;
        self.decode_entry(&reply)
    }
}
