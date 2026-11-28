//! The builders [`TimeSeries`] methods return: await them, optionally after setting their options.

use super::TimeSeries;
use crate::codec::Codec;
use crate::error::Result;
use crate::object::now_millis;
use bytes::Bytes;
use serde::Serialize;
use std::borrow::Borrow;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::time::Duration;

/// Redisson's `SEQUENCE_WIDTH`: entry ids are 20 zero-padded digits.
const SEQUENCE_WIDTH: &str = "20";
/// Redisson's `TimeUnit.DAYS.toMillis(365 * 100)`, the expiry of an entry without a time to live.
const CENTURY_MILLIS: i64 = 3_153_600_000_000;
/// Redisson's `LABEL_FIELD_EMPTY`: the entry carries no label.
const LABEL_FIELD_EMPTY: &str = "2";
/// Redisson's `LABEL_FIELD_SET`: a label follows the flag.
const LABEL_FIELD_SET: &str = "3";

/// The options of one write, Redisson's `TimeSeriesAddArgs`.
#[derive(Default)]
pub(crate) struct AddArgs {
    pub(crate) label: Option<Result<Bytes>>,
    pub(crate) ttl: Option<Duration>,
    pub(crate) retention: Option<Duration>,
}

/// Milliseconds of `duration` when it is at least one and fits an `i64`.
fn whole_millis(duration: Duration) -> Option<i64> {
    i64::try_from(duration.as_millis())
        .ok()
        .filter(|millis| *millis > 0)
}

/// Redisson's `expirationTime`: `now + ttl`, or a century away without a usable time to live.
pub(crate) fn expiration(ttl: Option<Duration>, now: i64) -> i64 {
    ttl.and_then(whole_millis)
        .and_then(|millis| now.checked_add(millis))
        .unwrap_or_else(|| now.saturating_add(CENTURY_MILLIS))
}

/// Redisson's `retentionWindow`: the window in milliseconds, `0` for none, saturating at `i64::MAX`.
fn retention_window(retention: Option<Duration>) -> i64 {
    retention.map_or(0, |retention| {
        i64::try_from(retention.as_millis()).unwrap_or(i64::MAX)
    })
}

impl AddArgs {
    /// The ARGV of every add script, Redisson's `encodeArgs`: the id width, the retention, the clock, then the entry as timestamp, label flag, value, label and expiration.
    pub(crate) fn encode(self, timestamp: i64, value: Bytes, now: i64) -> Result<Vec<Bytes>> {
        let expiration = expiration(self.ttl, now);
        let (flag, label) = match self.label {
            Some(label) => (LABEL_FIELD_SET, label?),
            None => (LABEL_FIELD_EMPTY, Bytes::new()),
        };
        Ok(vec![
            Bytes::from_static(SEQUENCE_WIDTH.as_bytes()),
            Bytes::from(retention_window(self.retention).to_string()),
            Bytes::from(now.to_string()),
            Bytes::from(timestamp.to_string()),
            Bytes::from_static(flag.as_bytes()),
            value,
            label,
            Bytes::from(expiration.to_string()),
        ])
    }
}

/// Defines a write builder with the options of Redisson's `TimeSeriesAddArgs`, resolving through the private method `$run` of [`TimeSeries`].
macro_rules! add_builder {
    ($(#[$doc:meta])* $name:ident, $output:ty, $run:ident) => {
        $(#[$doc])*
        #[must_use = "a write does nothing until it is awaited"]
        pub struct $name<'a, V, L, C: Codec> {
            series: &'a TimeSeries<V, L, C>,
            timestamp: i64,
            value: Result<Bytes>,
            args: AddArgs,
        }

        impl<'a, V, L, C: Codec> $name<'a, V, L, C> {
            pub(super) fn new(series: &'a TimeSeries<V, L, C>, timestamp: i64, value: Result<Bytes>) -> Self {
                Self {
                    series,
                    timestamp,
                    value,
                    args: AddArgs::default(),
                }
            }

            /// Stores the entry with `label`. Without it the entry has no label.
            pub fn label<Q: Serialize + ?Sized>(mut self, label: &Q) -> Self
            where
                L: Borrow<Q>,
            {
                self.args.label = Some(self.series.codec.encode(label));
                self
            }

            /// Makes the entry expire after `ttl`, measured on this client's clock. Zero, less than a millisecond, or more than `i64::MAX` milliseconds means no time to live, as in Redisson.
            #[doc(alias = "timeToLive")]
            pub fn ttl(mut self, ttl: Duration) -> Self {
                self.args.ttl = Some(ttl);
                self
            }

            /// Keeps only the entries whose timestamp is within `retention` of the highest live timestamp, this one included: an entry behind that window is not stored, and up to 500 older entries are removed. Zero (or less than a millisecond) means no retention.
            pub fn retention(mut self, retention: Duration) -> Self {
                self.args.retention = Some(retention);
                self
            }
        }

        impl<'a, V, L, C: Codec> IntoFuture for $name<'a, V, L, C>
        where
            V: Send + Sync + 'a,
            L: Send + Sync + 'a,
        {
            type Output = Result<$output>;
            type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

            fn into_future(self) -> Self::IntoFuture {
                Box::pin(async move {
                    let value = self.value?;
                    let args = self.args.encode(self.timestamp, value, now_millis())?;
                    self.series.$run(args).await
                })
            }
        }
    };
}

add_builder!(
    /// A pending [`TimeSeries::insert`]. Await it, optionally after [`Insert::label`], [`Insert::ttl`] or [`Insert::retention`]. Resolves to `false` when the entry is behind its retention window and was not stored.
    Insert,
    bool,
    run_insert
);

add_builder!(
    /// A pending [`TimeSeries::try_insert`]. Await it, optionally after [`TryInsert::label`], [`TryInsert::ttl`] or [`TryInsert::retention`]. Resolves to whether it stored the entry.
    TryInsert,
    bool,
    run_try_insert
);

add_builder!(
    /// A pending [`TimeSeries::set`]. Await it, optionally after [`Set::label`], [`Set::ttl`] or [`Set::retention`]. Resolves to `true` when no live entry was at the timestamp.
    Set,
    bool,
    run_set
);

#[cfg(test)]
mod tests {
    use super::*;

    fn text(args: &[Bytes]) -> Vec<String> {
        args.iter()
            .map(|arg| String::from_utf8_lossy(arg).into_owned())
            .collect()
    }

    #[test]
    fn add_args_follow_redissons_encode_args() {
        let args = AddArgs {
            label: Some(Ok(Bytes::from_static(b"l"))),
            ttl: Some(Duration::from_millis(250)),
            retention: Some(Duration::from_secs(1)),
        }
        .encode(7, Bytes::from_static(b"v"), 1000)
        .unwrap();
        assert_eq!(
            text(&args),
            ["20", "1000", "1000", "7", "3", "v", "l", "1250"]
        );
        let never = (1000 + CENTURY_MILLIS).to_string();
        let plain = AddArgs::default()
            .encode(-1, Bytes::from_static(b"v"), 1000)
            .unwrap();
        assert_eq!(
            text(&plain),
            ["20", "0", "1000", "-1", "2", "v", "", never.as_str()]
        );
    }

    #[test]
    fn a_ttl_that_cannot_be_honoured_means_none() {
        let never = 5 + CENTURY_MILLIS;
        assert_eq!(expiration(Some(Duration::ZERO), 5), never);
        assert_eq!(expiration(Some(Duration::from_micros(500)), 5), never);
        assert_eq!(expiration(Some(Duration::MAX), 5), never);
        assert_eq!(expiration(None, 5), never);
        assert_eq!(expiration(Some(Duration::from_millis(3)), 5), 8);
    }

    #[test]
    fn retention_saturates_and_rounds_down() {
        assert_eq!(retention_window(None), 0);
        assert_eq!(retention_window(Some(Duration::from_nanos(999_999))), 0);
        assert_eq!(retention_window(Some(Duration::MAX)), i64::MAX);
        assert_eq!(retention_window(Some(Duration::from_millis(100))), 100);
    }
}
