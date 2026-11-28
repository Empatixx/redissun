//! The options of the `TS.*` commands, written exactly as Redisson's `appendCreationOptions` and `appendRangeOptions` write them.

use crate::error::{Error, Result};
use std::ops::{Bound, RangeBounds};
use std::time::Duration;

/// What `TS.ADD` does with a sample at a timestamp that already holds one, Redisson's `TSDuplicatePolicy`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DuplicatePolicy {
    /// Fail the write.
    Block,
    /// Keep the old sample.
    First,
    /// Keep the new sample.
    Last,
    /// Keep the smaller value.
    Min,
    /// Keep the larger value.
    Max,
    /// Store the sum of both.
    Sum,
}

impl DuplicatePolicy {
    pub(crate) fn keyword(self) -> &'static str {
        match self {
            DuplicatePolicy::Block => "BLOCK",
            DuplicatePolicy::First => "FIRST",
            DuplicatePolicy::Last => "LAST",
            DuplicatePolicy::Min => "MIN",
            DuplicatePolicy::Max => "MAX",
            DuplicatePolicy::Sum => "SUM",
        }
    }

    pub(crate) fn parse(text: &str) -> Option<Self> {
        Some(match text.to_ascii_uppercase().as_str() {
            "BLOCK" => DuplicatePolicy::Block,
            "FIRST" => DuplicatePolicy::First,
            "LAST" => DuplicatePolicy::Last,
            "MIN" => DuplicatePolicy::Min,
            "MAX" => DuplicatePolicy::Max,
            "SUM" => DuplicatePolicy::Sum,
            _ => return None,
        })
    }
}

/// How the server stores the chunks, Redisson's `TSEncoding`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Encoding {
    /// Gorilla compression, the server's default.
    Compressed,
    /// Raw samples.
    Uncompressed,
}

impl Encoding {
    pub(crate) fn keyword(self) -> &'static str {
        match self {
            Encoding::Compressed => "COMPRESSED",
            Encoding::Uncompressed => "UNCOMPRESSED",
        }
    }

    pub(crate) fn parse(text: &str) -> Option<Self> {
        match text.to_ascii_lowercase().as_str() {
            "compressed" => Some(Encoding::Compressed),
            "uncompressed" => Some(Encoding::Uncompressed),
            _ => None,
        }
    }
}

/// An aggregator of `AGGREGATION` and `TS.CREATERULE`, Redisson's `TSAggregation`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Aggregator {
    /// `avg`.
    Avg,
    /// `sum`.
    Sum,
    /// `min`.
    Min,
    /// `max`.
    Max,
    /// `range`: max minus min.
    Range,
    /// `count`.
    Count,
    /// `countNaN` (Redis 8.6).
    CountNan,
    /// `countAll` (Redis 8.6).
    CountAll,
    /// `first`.
    First,
    /// `last`.
    Last,
    /// `std.p`.
    StdP,
    /// `std.s`.
    StdS,
    /// `var.p`.
    VarP,
    /// `var.s`.
    VarS,
    /// `twa`: time-weighted average.
    Twa,
}

impl Aggregator {
    pub(crate) fn keyword(self) -> &'static str {
        match self {
            Aggregator::Avg => "avg",
            Aggregator::Sum => "sum",
            Aggregator::Min => "min",
            Aggregator::Max => "max",
            Aggregator::Range => "range",
            Aggregator::Count => "count",
            Aggregator::CountNan => "countNaN",
            Aggregator::CountAll => "countAll",
            Aggregator::First => "first",
            Aggregator::Last => "last",
            Aggregator::StdP => "std.p",
            Aggregator::StdS => "std.s",
            Aggregator::VarP => "var.p",
            Aggregator::VarS => "var.s",
            Aggregator::Twa => "twa",
        }
    }

    pub(crate) fn parse(text: &str) -> Option<Self> {
        let all = [
            Aggregator::Avg,
            Aggregator::Sum,
            Aggregator::Min,
            Aggregator::Max,
            Aggregator::Range,
            Aggregator::Count,
            Aggregator::CountNan,
            Aggregator::CountAll,
            Aggregator::First,
            Aggregator::Last,
            Aggregator::StdP,
            Aggregator::StdS,
            Aggregator::VarP,
            Aggregator::VarS,
            Aggregator::Twa,
        ];
        all.into_iter()
            .find(|a| a.keyword().eq_ignore_ascii_case(text))
    }
}

/// A reducer of `GROUPBY … REDUCE`, Redisson's `TSReducer`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Reducer {
    /// `avg`.
    Avg,
    /// `sum`.
    Sum,
    /// `min`.
    Min,
    /// `max`.
    Max,
    /// `range`.
    Range,
    /// `count`.
    Count,
    /// `countNaN` (Redis 8.6).
    CountNan,
    /// `countAll` (Redis 8.6).
    CountAll,
    /// `std.p`.
    StdP,
    /// `std.s`.
    StdS,
    /// `var.p`.
    VarP,
    /// `var.s`.
    VarS,
}

impl Reducer {
    pub(crate) fn keyword(self) -> &'static str {
        match self {
            Reducer::Avg => "avg",
            Reducer::Sum => "sum",
            Reducer::Min => "min",
            Reducer::Max => "max",
            Reducer::Range => "range",
            Reducer::Count => "count",
            Reducer::CountNan => "countNaN",
            Reducer::CountAll => "countAll",
            Reducer::StdP => "std.p",
            Reducer::StdS => "std.s",
            Reducer::VarP => "var.p",
            Reducer::VarS => "var.s",
        }
    }
}

/// Which timestamp a bucket reports, Redisson's `TSBucketTimestamp`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BucketTimestamp {
    /// The start of the bucket, the server's default.
    Start,
    /// Its end.
    End,
    /// Its middle.
    Mid,
}

impl BucketTimestamp {
    fn keyword(self) -> &'static str {
        match self {
            BucketTimestamp::Start => "start",
            BucketTimestamp::End => "end",
            BucketTimestamp::Mid => "mid",
        }
    }
}

/// Where the buckets of an aggregation are aligned, Redisson's `align(..)`, `alignStart()` and `alignEnd()`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Align {
    /// The start of the range (`-`); the range needs an explicit start.
    Start,
    /// The end of the range (`+`); the range needs an explicit end.
    End,
    /// This timestamp.
    At(i64),
}

/// Where a [`read`](super::TimeSeriesNative::read) starts, Redisson's `TSReadArgs` positions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ReadFrom {
    /// The samples after this timestamp.
    At(i64),
    /// From the oldest sample (`-`).
    Earliest,
    /// The newest sample and after (`+`).
    Last,
    /// Only samples added from now on (`$`).
    Next,
}

impl ReadFrom {
    pub(crate) fn arg(self) -> String {
        match self {
            ReadFrom::At(ts) => ts.to_string(),
            ReadFrom::Earliest => "-".into(),
            ReadFrom::Last => "+".into(),
            ReadFrom::Next => "$".into(),
        }
    }
}

/// Whole milliseconds of `d`, saturating at `i64::MAX`.
pub(crate) fn millis(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

/// The creation options of `TS.CREATE`, `TS.ALTER`, `TS.ADD` and `TS.INCRBY`, Redisson's `TSCreationParams`.
#[derive(Clone, Debug, Default)]
pub(crate) struct CreationOptions {
    pub(crate) retention: Option<Duration>,
    pub(crate) encoding: Option<Encoding>,
    pub(crate) chunk_size: Option<u64>,
    pub(crate) duplicate_policy: Option<DuplicatePolicy>,
    pub(crate) ignore: Option<(Duration, f64)>,
    pub(crate) labels: Option<Vec<(String, String)>>,
}

impl CreationOptions {
    /// Redisson's `appendCreationOptions`: `RETENTION`, `ENCODING`, `CHUNK_SIZE`, `DUPLICATE_POLICY`, `IGNORE`, `LABELS`, in that order.
    pub(crate) fn append(&self, cmd: &mut redis::Cmd) -> Result<()> {
        if let Some(retention) = self.retention {
            let ms = millis(retention);
            if ms == 0 && !retention.is_zero() {
                return Err(Error::Config(format!(
                    "retention below 1ms is not representable: {retention:?} would be sent as RETENTION 0, which never expires"
                )));
            }
            cmd.arg("RETENTION").arg(ms);
        }
        if let Some(encoding) = self.encoding {
            cmd.arg("ENCODING").arg(encoding.keyword());
        }
        if let Some(size) = self.chunk_size {
            cmd.arg("CHUNK_SIZE").arg(size);
        }
        if let Some(policy) = self.duplicate_policy {
            cmd.arg("DUPLICATE_POLICY").arg(policy.keyword());
        }
        if let Some((time, value)) = self.ignore {
            cmd.arg("IGNORE").arg(millis(time)).arg(value.to_string());
        }
        if let Some(labels) = &self.labels {
            cmd.arg("LABELS");
            for (name, value) in labels {
                cmd.arg(name).arg(value);
            }
        }
        Ok(())
    }
}

/// The setters of [`CreationOptions`] on a builder with a field `creation`.
macro_rules! creation_options {
    () => {
        /// Drops samples older than `retention` relative to the newest one (`RETENTION`). Zero keeps everything; a non-zero time under 1 ms is an [`Error::Config`](crate::Error::Config).
        pub fn retention(mut self, retention: std::time::Duration) -> Self {
            self.creation.retention = Some(retention);
            self
        }

        /// The chunk encoding (`ENCODING`).
        pub fn encoding(
            mut self,
            encoding: $crate::collections::time_series_native::Encoding,
        ) -> Self {
            self.creation.encoding = Some(encoding);
            self
        }

        /// The chunk size in bytes (`CHUNK_SIZE`).
        pub fn chunk_size(mut self, bytes: u64) -> Self {
            self.creation.chunk_size = Some(bytes);
            self
        }

        /// What a sample at a taken timestamp does (`DUPLICATE_POLICY`).
        pub fn duplicate_policy(
            mut self,
            policy: $crate::collections::time_series_native::DuplicatePolicy,
        ) -> Self {
            self.creation.duplicate_policy = Some(policy);
            self
        }

        /// Ignores a sample within `max_time_diff` and `max_value_diff` of the last one (`IGNORE`).
        pub fn ignore(mut self, max_time_diff: std::time::Duration, max_value_diff: f64) -> Self {
            self.creation.ignore = Some((max_time_diff, max_value_diff));
            self
        }

        /// Adds a label (`LABELS`).
        pub fn label(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
            self.creation
                .labels
                .get_or_insert_with(Vec::new)
                .push((name.into(), value.into()));
            self
        }

        /// Adds labels (`LABELS`).
        pub fn labels<N: Into<String>, W: Into<String>>(
            mut self,
            labels: impl IntoIterator<Item = (N, W)>,
        ) -> Self {
            let list = self.creation.labels.get_or_insert_with(Vec::new);
            list.extend(labels.into_iter().map(|(n, v)| (n.into(), v.into())));
            self
        }
    };
}
pub(crate) use creation_options;

/// One end of a `TS.RANGE` range: `-`, `+` or a timestamp.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RangeEnd {
    Earliest,
    Latest,
    At(i64),
}

impl RangeEnd {
    fn arg(self) -> String {
        match self {
            RangeEnd::Earliest => "-".into(),
            RangeEnd::Latest => "+".into(),
            RangeEnd::At(ts) => ts.to_string(),
        }
    }
}

/// `(from, to)` of a range of timestamps; `None` when it holds none. Unbounded ends are `-` and `+`.
pub(crate) fn range_ends(range: &impl RangeBounds<i64>) -> Option<(RangeEnd, RangeEnd)> {
    let from = match range.start_bound() {
        Bound::Included(t) => RangeEnd::At(*t),
        Bound::Excluded(t) => RangeEnd::At(t.checked_add(1)?),
        Bound::Unbounded => RangeEnd::Earliest,
    };
    let to = match range.end_bound() {
        Bound::Included(t) => RangeEnd::At(*t),
        Bound::Excluded(t) => RangeEnd::At(t.checked_sub(1)?),
        Bound::Unbounded => RangeEnd::Latest,
    };
    if let (RangeEnd::At(a), RangeEnd::At(b)) = (from, to) {
        if a > b {
            return None;
        }
    }
    Some((from, to))
}

/// The options of `TS.RANGE`, `TS.MRANGE` and the others, Redisson's `TSBaseRangeParams`.
#[derive(Clone, Debug, Default)]
pub(crate) struct RangeOptions {
    pub(crate) latest: bool,
    pub(crate) filter_by_ts: Vec<i64>,
    pub(crate) filter_by_value: Option<(f64, f64)>,
    pub(crate) count: Option<u64>,
    pub(crate) align: Option<Align>,
    pub(crate) aggregation: Option<(Duration, Vec<Vec<Aggregator>>)>,
    pub(crate) bucket_timestamp: Option<BucketTimestamp>,
    pub(crate) empty: bool,
}

impl RangeOptions {
    /// Redisson's `appendRangeOptions`: `from to [LATEST] [FILTER_BY_TS ..] [FILTER_BY_VALUE min max] selection.. [COUNT n] [ALIGN a] [AGGREGATION aggs bucket [BUCKETTIMESTAMP b] [EMPTY]]`.
    pub(crate) fn append(
        &self,
        from: RangeEnd,
        to: RangeEnd,
        selection: &[String],
        cmd: &mut redis::Cmd,
    ) -> Result<()> {
        cmd.arg(from.arg()).arg(to.arg());
        if self.latest {
            cmd.arg("LATEST");
        }
        if !self.filter_by_ts.is_empty() {
            cmd.arg("FILTER_BY_TS");
            for ts in &self.filter_by_ts {
                cmd.arg(*ts);
            }
        }
        if let Some((min, max)) = self.filter_by_value {
            cmd.arg("FILTER_BY_VALUE")
                .arg(min.to_string())
                .arg(max.to_string());
        }
        for item in selection {
            cmd.arg(item);
        }
        if let Some(count) = self.count {
            cmd.arg("COUNT").arg(count);
        }
        if let Some(align) = self.align {
            if self.aggregation.is_none() {
                return Err(Error::Config("align() requires aggregation(): the module rejects ALIGN without AGGREGATION, and dropping it would silently run an unaligned query".into()));
            }
            if align == Align::Start && from == RangeEnd::Earliest {
                return Err(Error::Config(
                    "alignStart() requires a range with an explicit start".into(),
                ));
            }
            if align == Align::End && to == RangeEnd::Latest {
                return Err(Error::Config(
                    "alignEnd() requires a range with an explicit end".into(),
                ));
            }
        }
        if let Some((bucket, aggregations)) = &self.aggregation {
            if let Some(align) = self.align {
                cmd.arg("ALIGN").arg(match align {
                    Align::Start => "-".to_string(),
                    Align::End => "+".to_string(),
                    Align::At(ts) => ts.to_string(),
                });
            }
            cmd.arg("AGGREGATION");
            for per_key in aggregations {
                let joined: Vec<&str> = per_key.iter().map(|a| a.keyword()).collect();
                cmd.arg(joined.join(","));
            }
            cmd.arg(millis(*bucket));
            if let Some(b) = self.bucket_timestamp {
                cmd.arg("BUCKETTIMESTAMP").arg(b.keyword());
            }
            if self.empty {
                cmd.arg("EMPTY");
            }
        }
        Ok(())
    }
}

/// The setters of [`RangeOptions`] on a builder with a field `range`.
macro_rules! range_options {
    () => {
        /// Reports the latest, possibly partial, bucket of a compaction (`LATEST`).
        pub fn latest(mut self) -> Self {
            self.range.latest = true;
            self
        }

        /// Only the samples at these timestamps (`FILTER_BY_TS`).
        pub fn filter_by_ts(mut self, timestamps: &[i64]) -> Self {
            self.range.filter_by_ts = timestamps.to_vec();
            self
        }

        /// Only the samples with a value between `min` and `max`, both included (`FILTER_BY_VALUE`).
        pub fn filter_by_value(mut self, min: f64, max: f64) -> Self {
            self.range.filter_by_value = Some((min, max));
            self
        }

        /// At most `count` samples (`COUNT`).
        pub fn count(mut self, count: u64) -> Self {
            self.range.count = Some(count);
            self
        }

        /// Aligns the buckets (`ALIGN`); needs [`aggregate`](Self::aggregate).
        pub fn align(mut self, align: $crate::collections::time_series_native::Align) -> Self {
            self.range.align = Some(align);
            self
        }

        /// Aggregates into buckets of `bucket` with `aggregators` (`AGGREGATION`; several need Redis 8.6).
        pub fn aggregate(
            mut self,
            bucket: std::time::Duration,
            aggregators: &[$crate::collections::time_series_native::Aggregator],
        ) -> Self {
            self.range.aggregation = Some((bucket, vec![aggregators.to_vec()]));
            self
        }

        /// Which timestamp a bucket reports (`BUCKETTIMESTAMP`).
        pub fn bucket_timestamp(
            mut self,
            b: $crate::collections::time_series_native::BucketTimestamp,
        ) -> Self {
            self.range.bucket_timestamp = Some(b);
            self
        }

        /// Reports empty buckets too (`EMPTY`).
        pub fn empty(mut self) -> Self {
            self.range.empty = true;
            self
        }
    };
}
pub(crate) use range_options;

#[cfg(test)]
mod tests {
    use super::*;

    fn args(cmd: &redis::Cmd) -> Vec<String> {
        cmd.args_iter()
            .filter_map(|a| match a {
                redis::Arg::Simple(b) => Some(String::from_utf8_lossy(b).into_owned()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn creation_options_follow_redissons_order() {
        let options = CreationOptions {
            retention: Some(Duration::from_secs(1)),
            encoding: Some(Encoding::Compressed),
            chunk_size: Some(128),
            duplicate_policy: Some(DuplicatePolicy::Max),
            ignore: Some((Duration::from_millis(10), 0.5)),
            labels: Some(vec![("a".into(), "1".into()), ("b".into(), "2".into())]),
        };
        let mut cmd = redis::cmd("TS.CREATE");
        options.append(&mut cmd).unwrap();
        assert_eq!(
            args(&cmd).join(" "),
            "TS.CREATE RETENTION 1000 ENCODING COMPRESSED CHUNK_SIZE 128 DUPLICATE_POLICY MAX IGNORE 10 0.5 LABELS a 1 b 2"
        );
        let sub = CreationOptions {
            retention: Some(Duration::from_nanos(999_999)),
            ..Default::default()
        };
        assert!(matches!(
            sub.append(&mut redis::cmd("X")),
            Err(Error::Config(_))
        ));
        let zero = CreationOptions {
            retention: Some(Duration::ZERO),
            ..Default::default()
        };
        let mut cmd = redis::cmd("X");
        zero.append(&mut cmd).unwrap();
        assert_eq!(args(&cmd).join(" "), "X RETENTION 0");
    }

    #[test]
    fn range_options_follow_redissons_order_and_checks() {
        let options = RangeOptions {
            latest: true,
            filter_by_ts: vec![1, 2],
            filter_by_value: Some((0.0, 5.5)),
            count: Some(3),
            align: Some(Align::At(7)),
            aggregation: Some((
                Duration::from_millis(10),
                vec![vec![Aggregator::Avg, Aggregator::Max]],
            )),
            bucket_timestamp: Some(BucketTimestamp::Mid),
            empty: true,
        };
        let mut cmd = redis::cmd("TS.RANGE");
        options
            .append(RangeEnd::At(1), RangeEnd::Latest, &[], &mut cmd)
            .unwrap();
        assert_eq!(
            args(&cmd).join(" "),
            "TS.RANGE 1 + LATEST FILTER_BY_TS 1 2 FILTER_BY_VALUE 0 5.5 COUNT 3 ALIGN 7 AGGREGATION avg,max 10 BUCKETTIMESTAMP mid EMPTY"
        );
        let no_agg = RangeOptions {
            align: Some(Align::At(1)),
            ..Default::default()
        };
        assert!(no_agg
            .append(
                RangeEnd::Earliest,
                RangeEnd::Latest,
                &[],
                &mut redis::cmd("X")
            )
            .is_err());
        let start = RangeOptions {
            align: Some(Align::Start),
            aggregation: Some((Duration::from_millis(1), vec![vec![Aggregator::Sum]])),
            ..Default::default()
        };
        assert!(start
            .append(
                RangeEnd::Earliest,
                RangeEnd::At(5),
                &[],
                &mut redis::cmd("X")
            )
            .is_err());
        assert!(start
            .append(RangeEnd::At(0), RangeEnd::Latest, &[], &mut redis::cmd("X"))
            .is_ok());
    }

    #[test]
    fn range_ends_map_unbounded_to_dash_and_plus() {
        assert_eq!(
            range_ends(&(..)),
            Some((RangeEnd::Earliest, RangeEnd::Latest))
        );
        assert_eq!(
            range_ends(&(1000..3000)),
            Some((RangeEnd::At(1000), RangeEnd::At(2999)))
        );
        assert_eq!(range_ends(&(5..5)), None);
    }
}
