//! [`TimeSeries::aggregate`] and [`TimeSeries::info`]: buckets of statistics over a range, and a summary of the series.

use super::reads::LabelFilter;
use super::scripts::{AGGREGATE_RANGE, INFO};
use super::{TimeSeries, Timestamp};
use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::object::now_millis;
use bytes::Bytes;
use serde::Serialize;
use std::borrow::Borrow;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::time::Duration;

/// A statistic [`TimeSeries::aggregate`] computes per bucket, Redisson's `TimeSeriesAggregation`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Aggregation {
    /// The number of values.
    Count,
    /// Their sum.
    Sum,
    /// Their mean.
    Avg,
    /// The smallest value.
    Min,
    /// The largest value.
    Max,
    /// The largest minus the smallest value.
    ValueRange,
    /// The value of the oldest entry.
    First,
    /// The value of the newest entry.
    Last,
    /// The population standard deviation.
    StdDevPopulation,
    /// The sample standard deviation; undefined for one value.
    StdDevSample,
    /// The population variance.
    VariancePopulation,
    /// The sample variance; undefined for one value.
    VarianceSample,
}

impl Aggregation {
    /// Redisson's code for the script: `ordinal() + 1`.
    fn code(self) -> u8 {
        match self {
            Aggregation::Count => 1,
            Aggregation::Sum => 2,
            Aggregation::Avg => 3,
            Aggregation::Min => 4,
            Aggregation::Max => 5,
            Aggregation::ValueRange => 6,
            Aggregation::First => 7,
            Aggregation::Last => 8,
            Aggregation::StdDevPopulation => 9,
            Aggregation::StdDevSample => 10,
            Aggregation::VariancePopulation => 11,
            Aggregation::VarianceSample => 12,
        }
    }
}

/// One bucket of [`TimeSeries::aggregate`]: its start and the statistics that are defined for it, Redisson's `TimeSeriesBucket`.
#[derive(Clone, Debug, PartialEq)]
pub struct TimeSeriesBucket {
    /// The start of the bucket, in milliseconds.
    pub timestamp: i64,
    values: Vec<(Aggregation, f64)>,
}

impl TimeSeriesBucket {
    /// The value of `aggregation`, `None` when it was not asked for or is undefined (a sample variance of one value).
    pub fn get(&self, aggregation: Aggregation) -> Option<f64> {
        self.values
            .iter()
            .find(|(a, _)| *a == aggregation)
            .map(|(_, v)| *v)
    }

    /// The defined values, in the order they were asked for.
    pub fn values(&self) -> &[(Aggregation, f64)] {
        &self.values
    }
}

/// A summary of a [`TimeSeries`], Redisson's `TimeSeriesInfo`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct TimeSeriesInfo {
    /// The live entries.
    pub len: usize,
    /// Every stored entry, the expired ones the clean-up has not removed included.
    pub total_entries: usize,
    /// The oldest live timestamp.
    pub first_timestamp: Option<i64>,
    /// The newest live timestamp.
    pub last_timestamp: Option<i64>,
    /// The memory the three keys use, in bytes (`MEMORY USAGE`).
    pub memory_usage: u64,
    /// The time to live of the series itself, `None` without one.
    pub ttl: Option<Duration>,
    /// How many entry ids were handed out.
    pub entries_issued: u64,
}

/// Decodes the rows of `AGGREGATE_RANGE`: per bucket, its timestamp and one text value per aggregation, `''` for an undefined one, Redisson's `decodeBuckets`.
pub(super) fn decode_buckets(
    rows: &[String],
    aggregations: &[Aggregation],
) -> Result<Vec<TimeSeriesBucket>> {
    let stride = aggregations.len() + 1;
    rows.chunks(stride)
        .filter(|row| row.len() == stride)
        .map(|row| {
            let timestamp = row[0].parse().map_err(|_| crate::reply::malformed())?;
            let values = aggregations
                .iter()
                .zip(&row[1..])
                .filter(|(_, value)| !value.is_empty())
                .map(|(a, value)| {
                    value
                        .parse::<f64>()
                        .map(|v| (*a, v))
                        .map_err(|_| crate::reply::malformed())
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(TimeSeriesBucket { timestamp, values })
        })
        .collect()
}

/// Decodes the seven rows of `INFO`, Redisson's `infoAsync`.
pub(super) fn decode_info(rows: &[String]) -> Result<TimeSeriesInfo> {
    let [len, total, first, last, memory, pttl, issued] = rows else {
        return Err(crate::reply::malformed());
    };
    let number = |text: &str| text.parse::<i64>().map_err(|_| crate::reply::malformed());
    let timestamp = |text: &str| {
        if text.is_empty() {
            Ok(None)
        } else {
            number(text).map(Some)
        }
    };
    let pttl = number(pttl)?;
    Ok(TimeSeriesInfo {
        len: usize::try_from(number(len)?).unwrap_or(0),
        total_entries: usize::try_from(number(total)?).unwrap_or(0),
        first_timestamp: timestamp(first)?,
        last_timestamp: timestamp(last)?,
        memory_usage: u64::try_from(number(memory)?).unwrap_or(0),
        ttl: u64::try_from(pttl).ok().map(Duration::from_millis),
        // The counter is text and can outgrow an i64: it saturates, as Redisson's `parseEntriesIssued`.
        entries_issued: issued.parse::<u64>().unwrap_or(u64::MAX),
    })
}

/// A pending [`TimeSeries::aggregate`]. Await it, optionally after [`Aggregate::align_to`], [`Aggregate::filter_by_value`], [`Aggregate::label`] or [`Aggregate::unlabelled`].
#[must_use = "an aggregation does nothing until it is awaited"]
pub struct Aggregate<'a, V, L, C: Codec> {
    series: &'a TimeSeries<V, L, C>,
    bounds: Option<(i64, i64)>,
    bucket: Duration,
    aggregations: &'a [Aggregation],
    alignment: i64,
    value_filter: Option<(f64, f64)>,
    label: Option<Result<Bytes>>,
    unlabelled: bool,
}

impl<'a, V, L, C: Codec> Aggregate<'a, V, L, C> {
    pub(super) fn new(
        series: &'a TimeSeries<V, L, C>,
        bounds: Option<(i64, i64)>,
        bucket: Duration,
        aggregations: &'a [Aggregation],
    ) -> Self {
        Self {
            series,
            bounds,
            bucket,
            aggregations,
            alignment: 0,
            value_filter: None,
            label: None,
            unlabelled: false,
        }
    }

    /// Starts the buckets at `timestamp` plus whole multiples of the bucket, Redisson's `alignTo`. The default is `0`.
    #[doc(alias = "alignTo")]
    pub fn align_to(mut self, timestamp: impl Into<Timestamp>) -> Self {
        self.alignment = timestamp.into().as_millis();
        self
    }

    /// Counts only the values between `min` and `max`, both included, Redisson's `filterByValue`.
    #[doc(alias = "filterByValue")]
    pub fn filter_by_value(mut self, min: f64, max: f64) -> Self {
        self.value_filter = Some((min, max));
        self
    }

    /// Counts only the entries labelled `label`. The last of [`label`](Self::label) and [`unlabelled`](Self::unlabelled) wins.
    #[doc(alias = "filterByLabel")]
    pub fn label<Q: Serialize + ?Sized>(mut self, label: &Q) -> Self
    where
        L: Borrow<Q>,
    {
        self.label = Some(self.series.codec.encode(label));
        self.unlabelled = false;
        self
    }

    /// Counts only the entries stored without a label.
    pub fn unlabelled(mut self) -> Self {
        self.label = None;
        self.unlabelled = true;
        self
    }
}

impl<'a, V, L, C> IntoFuture for Aggregate<'a, V, L, C>
where
    V: Send + Sync + 'a,
    L: Send + Sync + 'a,
    C: Codec,
{
    type Output = Result<Vec<TimeSeriesBucket>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            self.series.check_numeric("aggregate")?;
            let bucket = i64::try_from(self.bucket.as_millis()).unwrap_or(i64::MAX);
            if bucket <= 0 {
                return Err(Error::Config(
                    "bucket interval should be defined and at least one millisecond".into(),
                ));
            }
            // Duplicates are dropped, keeping the first order, as Redisson's `LinkedHashSet`.
            let mut aggregations: Vec<Aggregation> = Vec::with_capacity(self.aggregations.len());
            for a in self.aggregations {
                if !aggregations.contains(a) {
                    aggregations.push(*a);
                }
            }
            if aggregations.is_empty() {
                return Err(Error::Config(
                    "at least one aggregation should be defined".into(),
                ));
            }
            let label = match (self.label, self.unlabelled) {
                (Some(label), _) => LabelFilter::Match(label?),
                (None, true) => LabelFilter::Absent,
                (None, false) => LabelFilter::None,
            };
            let Some((start, end)) = self.bounds else {
                return Ok(Vec::new());
            };
            let (mode, label) = label.args();
            let (filtered, min, max) = match self.value_filter {
                Some((min, max)) => ("1", min, max),
                None => ("0", 0.0, 0.0),
            };
            let mut args = vec![
                Bytes::from(now_millis().to_string()),
                Bytes::from(start.to_string()),
                Bytes::from(end.to_string()),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"0"),
                mode,
                label,
                Bytes::from(bucket.to_string()),
                Bytes::from(self.alignment.to_string()),
                Bytes::from_static(filtered.as_bytes()),
                Bytes::from(min.to_string()),
                Bytes::from(max.to_string()),
            ];
            args.extend(
                aggregations
                    .iter()
                    .map(|a| Bytes::from(a.code().to_string())),
            );
            let rows: Vec<String> = self
                .series
                .key
                .core
                .eval(&AGGREGATE_RANGE, self.series.data_keys(), args)
                .await?;
            decode_buckets(&rows, &aggregations)
        })
    }
}

impl<V, L, C: Codec> TimeSeries<V, L, C> {
    /// Splits the live entries in `range` into buckets of `bucket` and computes `aggregations` for each non-empty one, oldest first, Redisson's `aggregate`. Needs a codec that encodes numbers as text. A bucket under a millisecond or an empty list is an [`Error::Config`].
    pub fn aggregate<'a>(
        &'a self,
        range: impl std::ops::RangeBounds<i64>,
        bucket: Duration,
        aggregations: &'a [Aggregation],
    ) -> Aggregate<'a, V, L, C> {
        Aggregate::new(self, super::timestamp::bounds(&range), bucket, aggregations)
    }

    /// A summary of the series: live and stored entries, first and last timestamp, memory, time to live and issued ids, Redisson's `info`.
    pub async fn info(&self) -> Result<TimeSeriesInfo> {
        let rows: Vec<String> = self
            .key
            .core
            .eval(
                &INFO,
                self.all_keys(),
                vec![Bytes::from(now_millis().to_string())],
            )
            .await?;
        decode_info(&rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(values: &[&str]) -> Vec<String> {
        values.iter().map(|v| v.to_string()).collect()
    }

    #[test]
    fn undefined_values_are_left_out() {
        let buckets = decode_buckets(
            &rows(&["0", "2", "", "10", "1", "0.5"]),
            &[Aggregation::Count, Aggregation::VarianceSample],
        )
        .unwrap();
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].timestamp, 0);
        assert_eq!(buckets[0].get(Aggregation::Count), Some(2.0));
        assert_eq!(buckets[0].get(Aggregation::VarianceSample), None);
        assert_eq!(buckets[1].get(Aggregation::VarianceSample), Some(0.5));
        assert_eq!(buckets[1].get(Aggregation::Sum), None);
    }

    #[test]
    fn info_rows_decode() {
        let info = decode_info(&rows(&[
            "2",
            "3",
            "-5",
            "9",
            "100",
            "-1",
            "99999999999999999999999",
        ]))
        .unwrap();
        assert_eq!(info.len, 2);
        assert_eq!(info.total_entries, 3);
        assert_eq!(info.first_timestamp, Some(-5));
        assert_eq!(info.last_timestamp, Some(9));
        assert_eq!(info.ttl, None);
        assert_eq!(info.entries_issued, u64::MAX);
        let empty = decode_info(&rows(&["0", "0", "", "", "0", "1500", "0"])).unwrap();
        assert_eq!(empty.first_timestamp, None);
        assert_eq!(empty.ttl, Some(Duration::from_millis(1500)));
    }
}
