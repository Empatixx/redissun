//! The range builders of [`TimeSeries`]: [`Range`], [`RangeEntries`], [`RemoveRange`] and [`ReadAfter`].

use super::reads::{limit, LabelFilter};
use super::scripts::{COLLECT_ENTRIES, COLLECT_VALUES, REMOVE_RANGE};
use super::{TimeSeries, TimeSeriesEntry};
use crate::codec::Codec;
use crate::error::Result;
use crate::object::now_millis;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::future::{Future, IntoFuture};
use std::pin::Pin;

/// The label filter a builder was given, encoded when it is awaited.
enum LabelArg {
    None,
    Match(Result<Bytes>),
    Absent,
}

impl LabelArg {
    fn filter(self) -> Result<LabelFilter> {
        Ok(match self {
            LabelArg::None => LabelFilter::None,
            LabelArg::Match(label) => LabelFilter::Match(label?),
            LabelArg::Absent => LabelFilter::Absent,
        })
    }
}

/// The label setters every range builder has.
macro_rules! label_setters {
    () => {
        /// Takes only the entries labelled `label`, Redisson's `…ByLabel` variants. The last of [`label`](Self::label) and [`unlabelled`](Self::unlabelled) wins.
        pub fn label<Q: Serialize + ?Sized>(mut self, label: &Q) -> Self
        where
            L: Borrow<Q>,
        {
            self.label = LabelArg::Match(self.series.codec.encode(label));
            self
        }

        /// Takes only the entries stored without a label, Redisson's `…ByLabel(.., null)`.
        pub fn unlabelled(mut self) -> Self {
            self.label = LabelArg::Absent;
            self
        }
    };
}

/// The range options shared by [`Range`] and [`RangeEntries`].
macro_rules! range_builder {
    ($(#[$doc:meta])* $name:ident, $item:ty, $script:ident, $decode:ident) => {
        $(#[$doc])*
        #[must_use = "a range does nothing until it is awaited"]
        pub struct $name<'a, V, L, C: Codec> {
            series: &'a TimeSeries<V, L, C>,
            bounds: Option<(i64, i64)>,
            rev: bool,
            limit: Option<usize>,
            label: LabelArg,
        }

        impl<'a, V, L, C: Codec> $name<'a, V, L, C> {
            pub(super) fn new(series: &'a TimeSeries<V, L, C>, bounds: Option<(i64, i64)>) -> Self {
                Self {
                    series,
                    bounds,
                    rev: false,
                    limit: None,
                    label: LabelArg::None,
                }
            }

            /// Newest first, Redisson's `…Reversed` variants.
            pub fn rev(mut self) -> Self {
                self.rev = true;
                self
            }

            /// At most `limit` entries, taken from the start of the order (the newest with [`rev`](Self::rev)). `0` returns nothing, without a call.
            pub fn limit(mut self, limit: usize) -> Self {
                self.limit = Some(limit);
                self
            }

            label_setters!();
        }

        impl<'a, V, L, C> IntoFuture for $name<'a, V, L, C>
        where
            V: DeserializeOwned + Send + Sync + 'a,
            L: DeserializeOwned + Send + Sync + 'a,
            C: Codec,
        {
            type Output = Result<Vec<$item>>;
            type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

            fn into_future(self) -> Self::IntoFuture {
                Box::pin(async move {
                    let label = self.label.filter()?;
                    let Some((start, end)) = self.bounds else {
                        return Ok(Vec::new());
                    };
                    // The scripts read 0 as "no limit".
                    let limit = match self.limit {
                        Some(0) => return Ok(Vec::new()),
                        Some(n) => limit(n),
                        None => 0,
                    };
                    let reply = self
                        .series
                        .collect(
                            &$script,
                            start.to_string(),
                            end.to_string(),
                            limit,
                            self.rev,
                            &label,
                            false,
                        )
                        .await?;
                    self.series.$decode(&reply)
                })
            }
        }
    };
}

range_builder!(
    /// A pending [`TimeSeries::range`]. Await it, optionally after [`Range::rev`], [`Range::limit`], [`Range::label`] or [`Range::unlabelled`].
    Range,
    V,
    COLLECT_VALUES,
    decode_values
);

range_builder!(
    /// A pending [`TimeSeries::range_entries`]. Await it, optionally after [`RangeEntries::rev`], [`RangeEntries::limit`], [`RangeEntries::label`] or [`RangeEntries::unlabelled`].
    RangeEntries,
    TimeSeriesEntry<V, L>,
    COLLECT_ENTRIES,
    decode_entries
);

/// A pending [`TimeSeries::remove_range`]. Await it, optionally after [`RemoveRange::label`] or [`RemoveRange::unlabelled`]. Resolves to how many live entries it removed.
#[must_use = "a removal does nothing until it is awaited"]
pub struct RemoveRange<'a, V, L, C: Codec> {
    series: &'a TimeSeries<V, L, C>,
    bounds: Option<(i64, i64)>,
    label: LabelArg,
}

impl<'a, V, L, C: Codec> RemoveRange<'a, V, L, C> {
    pub(super) fn new(series: &'a TimeSeries<V, L, C>, bounds: Option<(i64, i64)>) -> Self {
        Self {
            series,
            bounds,
            label: LabelArg::None,
        }
    }

    label_setters!();
}

impl<'a, V, L, C> IntoFuture for RemoveRange<'a, V, L, C>
where
    V: Send + Sync + 'a,
    L: Send + Sync + 'a,
    C: Codec,
{
    type Output = Result<usize>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let (mode, label) = self.label.filter()?.args();
            let Some((start, end)) = self.bounds else {
                return Ok(0);
            };
            let args = vec![
                Bytes::from(now_millis().to_string()),
                Bytes::from(start.to_string()),
                Bytes::from(end.to_string()),
                mode,
                label,
            ];
            let removed: i64 = self
                .series
                .key
                .core
                .eval_once(&REMOVE_RANGE, self.series.data_keys(), args)
                .await?;
            Ok(usize::try_from(removed).unwrap_or(0))
        })
    }
}

/// A pending [`TimeSeries::read_after`]. Await it, optionally after [`ReadAfter::count`], [`ReadAfter::label`] or [`ReadAfter::unlabelled`].
#[must_use = "a read does nothing until it is awaited"]
pub struct ReadAfter<'a, V, L, C: Codec> {
    series: &'a TimeSeries<V, L, C>,
    timestamp: i64,
    count: usize,
    label: LabelArg,
}

impl<'a, V, L, C: Codec> ReadAfter<'a, V, L, C> {
    pub(super) fn new(series: &'a TimeSeries<V, L, C>, timestamp: i64) -> Self {
        Self {
            series,
            timestamp,
            count: 0,
            label: LabelArg::None,
        }
    }

    /// About `count` entries: the read stops at the first timestamp boundary after `count` entries, so it can return more, never splitting a timestamp. `0` (the default) means no limit, as Redisson's `readTail` without a count.
    pub fn count(mut self, count: usize) -> Self {
        self.count = count;
        self
    }

    label_setters!();
}

/// The exclusive lower score of [`TimeSeries::read_after`], Redisson's `readTailAsync`: the smallest double no timestamp at or below `timestamp` reaches.
pub(super) fn after(timestamp: i64) -> f64 {
    let bound = timestamp as f64;
    if bound as i64 <= timestamp {
        bound.next_up()
    } else {
        bound
    }
}

impl<'a, V, L, C> IntoFuture for ReadAfter<'a, V, L, C>
where
    V: DeserializeOwned + Send + Sync + 'a,
    L: DeserializeOwned + Send + Sync + 'a,
    C: Codec,
{
    type Output = Result<Vec<TimeSeriesEntry<V, L>>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let label = self.label.filter()?;
            let reply = self
                .series
                .collect(
                    &COLLECT_ENTRIES,
                    after(self.timestamp).to_string(),
                    i64::MAX.to_string(),
                    limit(self.count),
                    false,
                    &label,
                    true,
                )
                .await?;
            self.series.decode_entries(&reply)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::after;

    #[test]
    fn after_is_the_next_double_above_the_timestamp() {
        assert_eq!(after(5), 5f64.next_up());
        assert!(after(5) > 5.0);
        assert!(after(-1) > -1.0);
        // 2^53 + 1 rounds down to 2^53, so the bound is the next double up.
        let big = (1i64 << 53) + 1;
        assert!(after(big) > big as f64);
        // i64::MAX converts to 2^63, which casts back to i64::MAX, so Redisson takes the next double up as well.
        assert_eq!(after(i64::MAX), (i64::MAX as f64).next_up());
    }
}
