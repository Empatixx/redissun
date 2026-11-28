//! [`Timestamp`], [`TimeSeriesEntry`] and the conversion of timestamp ranges.

use crate::error::Error;
use std::ops::{Bound, RangeBounds};
use std::time::{SystemTime, UNIX_EPOCH};

/// A point in time of a [`TimeSeries`](crate::TimeSeries): signed milliseconds since the Unix epoch, Redisson's `long` timestamp.
///
/// Integer literals, `i64`, `i32`, `u32` and [`SystemTime`] convert into it. A `SystemTime` before 1970 gives a negative timestamp, and one past `i64::MAX` milliseconds saturates. A `u64` converts with [`TryFrom`], which fails with [`Error::OutOfRange`] above `i64::MAX`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(i64);

impl Timestamp {
    /// The timestamp `millis` milliseconds after the Unix epoch (before it when negative).
    pub const fn from_millis(millis: i64) -> Self {
        Self(millis)
    }

    /// The milliseconds since the Unix epoch.
    pub const fn as_millis(self) -> i64 {
        self.0
    }
}

impl From<i64> for Timestamp {
    fn from(millis: i64) -> Self {
        Self(millis)
    }
}

impl From<i32> for Timestamp {
    fn from(millis: i32) -> Self {
        Self(i64::from(millis))
    }
}

impl From<u32> for Timestamp {
    fn from(millis: u32) -> Self {
        Self(i64::from(millis))
    }
}

impl From<SystemTime> for Timestamp {
    fn from(time: SystemTime) -> Self {
        match time.duration_since(UNIX_EPOCH) {
            Ok(after) => Self(i64::try_from(after.as_millis()).unwrap_or(i64::MAX)),
            Err(before) => {
                // Rounded down, as Java's `Instant.toEpochMilli`.
                let millis = before.duration().as_nanos().div_ceil(1_000_000);
                Self(i64::try_from(millis).map_or(i64::MIN, |millis| -millis))
            }
        }
    }
}

impl TryFrom<u64> for Timestamp {
    type Error = Error;

    fn try_from(millis: u64) -> Result<Self, Error> {
        i64::try_from(millis)
            .map(Self)
            .map_err(|_| Error::OutOfRange)
    }
}

/// One entry of a [`TimeSeries`](crate::TimeSeries): its timestamp, its value and its label, Redisson's `TimeSeriesEntry`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimeSeriesEntry<V, L> {
    /// Milliseconds since the Unix epoch.
    pub timestamp: i64,
    /// The value.
    pub value: V,
    /// The label, `None` for an entry stored without one.
    pub label: Option<L>,
}

impl<T: Into<Timestamp>, V, L> From<(T, V)> for TimeSeriesEntry<V, L> {
    fn from((timestamp, value): (T, V)) -> Self {
        Self {
            timestamp: timestamp.into().as_millis(),
            value,
            label: None,
        }
    }
}

impl<T: Into<Timestamp>, V, L> From<(T, V, L)> for TimeSeriesEntry<V, L> {
    fn from((timestamp, value, label): (T, V, L)) -> Self {
        Self {
            timestamp: timestamp.into().as_millis(),
            value,
            label: Some(label),
        }
    }
}

/// The inclusive `(start, end)` of a range of timestamps, Redisson's `(startTimestamp, endTimestamp)`; `None` when it holds no timestamp.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn bounds(range: &impl RangeBounds<i64>) -> Option<(i64, i64)> {
    let start = match range.start_bound() {
        Bound::Included(start) => *start,
        Bound::Excluded(start) => start.checked_add(1)?,
        Bound::Unbounded => i64::MIN,
    };
    let end = match range.end_bound() {
        Bound::Included(end) => *end,
        Bound::Excluded(end) => end.checked_sub(1)?,
        Bound::Unbounded => i64::MAX,
    };
    (start <= end).then_some((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn timestamp_conversions() {
        assert_eq!(Timestamp::from(5i32).as_millis(), 5);
        assert_eq!(Timestamp::from(-1i64).as_millis(), -1);
        assert_eq!(Timestamp::from(7u32).as_millis(), 7);
        assert_eq!(
            Timestamp::from(UNIX_EPOCH - Duration::from_secs(1)).as_millis(),
            -1000
        );
        assert_eq!(
            Timestamp::from(UNIX_EPOCH + Duration::from_millis(1500)).as_millis(),
            1500
        );
        assert_eq!(
            Timestamp::from(UNIX_EPOCH - Duration::from_micros(1500)).as_millis(),
            -2
        );
        assert!(matches!(
            Timestamp::try_from(u64::MAX),
            Err(Error::OutOfRange)
        ));
        assert_eq!(
            Timestamp::try_from(i64::MAX as u64).unwrap().as_millis(),
            i64::MAX
        );
        assert_eq!(Timestamp::from_millis(3).as_millis(), 3);
    }

    #[test]
    fn bounds_cover_every_shape() {
        assert_eq!(bounds(&(..)), Some((i64::MIN, i64::MAX)));
        assert_eq!(bounds(&(3..7)), Some((3, 6)));
        assert_eq!(bounds(&(3..=7)), Some((3, 7)));
        assert_eq!(bounds(&(..=i64::MAX)), Some((i64::MIN, i64::MAX)));
        assert_eq!(bounds(&(i64::MIN..)), Some((i64::MIN, i64::MAX)));
        assert_eq!(
            bounds(&(Bound::Excluded(i64::MAX), Bound::<i64>::Unbounded)),
            None
        );
        assert_eq!(bounds(&(..i64::MIN)), None);
        assert_eq!(bounds(&(5..5)), None);
        assert_eq!(bounds(&(5..=5)), Some((5, 5)));
        assert_eq!(bounds(&(Bound::Included(7), Bound::Included(3))), None);
    }

    #[test]
    fn entries_come_from_tuples() {
        let plain: TimeSeriesEntry<&str, &str> = (5, "v").into();
        assert_eq!(plain.timestamp, 5);
        assert_eq!(plain.label, None);
        let labelled: TimeSeriesEntry<&str, &str> = (6i64, "v", "l").into();
        assert_eq!(labelled.label, Some("l"));
    }
}
