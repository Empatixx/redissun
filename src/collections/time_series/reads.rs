//! The reads and removals of [`TimeSeries`]: by timestamp, at the head and the tail, labels and iteration.

use super::ranges::{Range, RangeEntries, ReadAfter, RemoveRange};
use super::scripts::{
    COLLECT_ENTRIES, COLLECT_LABELS, COLLECT_TIMESTAMPS, COLLECT_VALUES, GET_ALL, GET_ALL_ENTRIES,
    GET_AND_REMOVE, GET_AND_REMOVE_ALL, GET_AND_REMOVE_ALL_ENTRIES, GET_AND_REMOVE_ENTRY,
    POLL_ENTRIES, POLL_VALUES, SCAN,
};
use super::timestamp::bounds;
use super::{TimeSeries, TimeSeriesEntry, Timestamp};
use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::lua::Lua;
use crate::object::now_millis;
use crate::reply;
use bytes::Bytes;
use futures::{stream, Stream, TryStreamExt};
use serde::de::DeserializeOwned;
use std::ops::RangeBounds;

/// The page size of [`TimeSeries::iter`], Redisson's `iterator()`.
const ITER_PAGE: usize = 10;

/// Which entries a read takes by label, Redisson's `LABEL_NONE`, `LABEL_MATCH` and `LABEL_ABSENT`.
pub(crate) enum LabelFilter {
    /// Every entry.
    None,
    /// The entries whose encoded label is these bytes.
    Match(Bytes),
    /// The entries without a label.
    Absent,
}

impl LabelFilter {
    /// The label mode and the label argument, Redisson's `labelMode` and `encodeLabelArg`.
    pub(crate) fn args(&self) -> (Bytes, Bytes) {
        match self {
            LabelFilter::None => (Bytes::from_static(b"0"), Bytes::from_static(b"0")),
            LabelFilter::Match(label) => (Bytes::from_static(b"1"), label.clone()),
            LabelFilter::Absent => (Bytes::from_static(b"2"), Bytes::from_static(b"0")),
        }
    }
}

/// A count as the scripts read it.
pub(crate) fn limit(count: usize) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}

fn flag(set: bool) -> Bytes {
    Bytes::from_static(if set { b"1" } else { b"0" })
}

impl<V, L, C> TimeSeries<V, L, C>
where
    V: DeserializeOwned,
    L: DeserializeOwned,
    C: Codec,
{
    /// Runs one of the `COLLECT_*` scripts, Redisson's `collectAsync`: ARGV `[now, start, end, limit, rev, label mode, label, whole timestamps]`.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn collect(
        &self,
        script: &Lua,
        start: String,
        end: String,
        limit: i64,
        rev: bool,
        label: &LabelFilter,
        whole: bool,
    ) -> Result<redis::Value> {
        let (mode, label) = label.args();
        let args = vec![
            Bytes::from(now_millis().to_string()),
            Bytes::from(start),
            Bytes::from(end),
            Bytes::from(limit.to_string()),
            flag(rev),
            mode,
            label,
            flag(whole),
        ];
        self.key.core.eval(script, self.data_keys(), args).await
    }

    /// Runs one of the `POLL_*` scripts over the whole series, Redisson's `collectAndRemoveAsync`.
    async fn poll(&self, script: &Lua, count: usize, rev: bool) -> Result<redis::Value> {
        let args = vec![
            Bytes::from(now_millis().to_string()),
            Bytes::from(i64::MIN.to_string()),
            Bytes::from(i64::MAX.to_string()),
            Bytes::from(limit(count).to_string()),
            flag(rev),
            Bytes::from_static(b"0"),
            Bytes::from_static(b"0"),
            Bytes::from_static(b"0"),
        ];
        self.key
            .core
            .eval_once(script, self.data_keys(), args)
            .await
    }

    /// The values of a flat reply.
    pub(super) fn decode_values(&self, reply: &redis::Value) -> Result<Vec<V>> {
        reply::array(reply)
            .iter()
            .map(|value| self.decode_value(value))
            .collect()
    }

    /// Entries as groups of `[value, label, flag, timestamp]`, Redisson's `TimeSeriesEntryReplayDecoder`: the label counts only when the flag is `3`.
    pub(super) fn decode_entries(
        &self,
        reply: &redis::Value,
    ) -> Result<Vec<TimeSeriesEntry<V, L>>> {
        let items = reply::array(reply);
        if !items.len().is_multiple_of(4) {
            return Err(reply::malformed());
        }
        let (groups, _) = items.as_chunks::<4>();
        groups
            .iter()
            .map(|[value, label, flag, timestamp]| {
                let (Some(flag), Some(timestamp)) = (reply::number(flag), reply::number(timestamp))
                else {
                    return Err(reply::malformed());
                };
                Ok(TimeSeriesEntry {
                    timestamp,
                    value: self.decode_value(value)?,
                    label: self.decode_label(flag, Some(label))?,
                })
            })
            .collect()
    }

    async fn head_values(&self, rev: bool, count: usize) -> Result<Vec<V>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let reply = self
            .collect(
                &COLLECT_VALUES,
                i64::MIN.to_string(),
                i64::MAX.to_string(),
                limit(count),
                rev,
                &LabelFilter::None,
                false,
            )
            .await?;
        let mut values = self.decode_values(&reply)?;
        if rev {
            values.reverse();
        }
        Ok(values)
    }

    async fn head_entries(&self, rev: bool, count: usize) -> Result<Vec<TimeSeriesEntry<V, L>>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let reply = self
            .collect(
                &COLLECT_ENTRIES,
                i64::MIN.to_string(),
                i64::MAX.to_string(),
                limit(count),
                rev,
                &LabelFilter::None,
                false,
            )
            .await?;
        let mut entries = self.decode_entries(&reply)?;
        if rev {
            entries.reverse();
        }
        Ok(entries)
    }

    async fn head_timestamp(&self, rev: bool) -> Result<Option<i64>> {
        let reply = self
            .collect(
                &COLLECT_TIMESTAMPS,
                i64::MIN.to_string(),
                i64::MAX.to_string(),
                1,
                rev,
                &LabelFilter::None,
                false,
            )
            .await?;
        reply::array(&reply)
            .first()
            .map(|timestamp| reply::number(timestamp).ok_or_else(reply::malformed))
            .transpose()
    }

    async fn pop_values(&self, rev: bool, count: usize) -> Result<Vec<V>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let reply = self.poll(&POLL_VALUES, count, rev).await?;
        let mut values = self.decode_values(&reply)?;
        if rev {
            values.reverse();
        }
        Ok(values)
    }

    async fn pop_entries(&self, rev: bool, count: usize) -> Result<Vec<TimeSeriesEntry<V, L>>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let reply = self.poll(&POLL_ENTRIES, count, rev).await?;
        let mut entries = self.decode_entries(&reply)?;
        if rev {
            entries.reverse();
        }
        Ok(entries)
    }

    /// Every live value at `timestamp`, in insertion order, Redisson's `getAll`.
    #[doc(alias = "getAll")]
    pub async fn get_all(&self, timestamp: impl Into<Timestamp>) -> Result<Vec<V>> {
        let reply: redis::Value = self
            .key
            .core
            .eval(&GET_ALL, self.data_keys(), Self::at(timestamp))
            .await?;
        self.decode_values(&reply)
    }

    /// Every live entry at `timestamp`, in insertion order, Redisson's `getAllEntries`.
    #[doc(alias = "getAllEntries")]
    pub async fn get_all_entries(
        &self,
        timestamp: impl Into<Timestamp>,
    ) -> Result<Vec<TimeSeriesEntry<V, L>>> {
        let reply: redis::Value = self
            .key
            .core
            .eval(&GET_ALL_ENTRIES, self.data_keys(), Self::at(timestamp))
            .await?;
        self.decode_entries(&reply)
    }

    /// Removes the first live entry at `timestamp` and returns its value, Redisson's `remove` and `getAndRemove`.
    #[doc(alias = "getAndRemove")]
    pub async fn remove(&self, timestamp: impl Into<Timestamp>) -> Result<Option<V>> {
        let reply: redis::Value = self
            .key
            .core
            .eval_once(&GET_AND_REMOVE, self.data_keys(), Self::at(timestamp))
            .await?;
        self.decode_optional(&reply)
    }

    /// Removes the first live entry at `timestamp` and returns it, Redisson's `getAndRemoveEntry`.
    #[doc(alias = "getAndRemoveEntry")]
    pub async fn remove_entry(
        &self,
        timestamp: impl Into<Timestamp>,
    ) -> Result<Option<TimeSeriesEntry<V, L>>> {
        let reply: redis::Value = self
            .key
            .core
            .eval_once(&GET_AND_REMOVE_ENTRY, self.data_keys(), Self::at(timestamp))
            .await?;
        self.decode_entry(&reply)
    }

    /// Removes every live entry at `timestamp` and returns their values in insertion order, Redisson's `removeAll` and `getAndRemoveAll`; `.len()` is the count. Expired entries there are left for the clean-up.
    #[doc(alias = "removeAll")]
    #[doc(alias = "getAndRemoveAll")]
    pub async fn remove_all(&self, timestamp: impl Into<Timestamp>) -> Result<Vec<V>> {
        let reply: redis::Value = self
            .key
            .core
            .eval_once(&GET_AND_REMOVE_ALL, self.data_keys(), Self::at(timestamp))
            .await?;
        self.decode_values(&reply)
    }

    /// Removes every live entry at `timestamp` and returns them, Redisson's `getAndRemoveAllEntries`.
    #[doc(alias = "getAndRemoveAllEntries")]
    pub async fn remove_all_entries(
        &self,
        timestamp: impl Into<Timestamp>,
    ) -> Result<Vec<TimeSeriesEntry<V, L>>> {
        let reply: redis::Value = self
            .key
            .core
            .eval_once(
                &GET_AND_REMOVE_ALL_ENTRIES,
                self.data_keys(),
                Self::at(timestamp),
            )
            .await?;
        self.decode_entries(&reply)
    }

    /// The value of the oldest live entry, Redisson's `first()`.
    pub async fn first(&self) -> Result<Option<V>> {
        Ok(self.head_values(false, 1).await?.into_iter().next())
    }

    /// The values of the `count` oldest live entries, oldest first, Redisson's `first(count)`. A count of `0` gives nothing.
    pub async fn first_many(&self, count: usize) -> Result<Vec<V>> {
        self.head_values(false, count).await
    }

    /// The oldest live entry, Redisson's `firstEntry`.
    #[doc(alias = "firstEntry")]
    pub async fn first_entry(&self) -> Result<Option<TimeSeriesEntry<V, L>>> {
        Ok(self.head_entries(false, 1).await?.into_iter().next())
    }

    /// The `count` oldest live entries, oldest first, Redisson's `firstEntries`.
    #[doc(alias = "firstEntries")]
    pub async fn first_entries(&self, count: usize) -> Result<Vec<TimeSeriesEntry<V, L>>> {
        self.head_entries(false, count).await
    }

    /// The value of the newest live entry, Redisson's `last()`.
    pub async fn last(&self) -> Result<Option<V>> {
        Ok(self.head_values(true, 1).await?.into_iter().next())
    }

    /// The values of the `count` newest live entries, **oldest first** as in Redisson's `last(count)`.
    pub async fn last_many(&self, count: usize) -> Result<Vec<V>> {
        self.head_values(true, count).await
    }

    /// The newest live entry, Redisson's `lastEntry`.
    #[doc(alias = "lastEntry")]
    pub async fn last_entry(&self) -> Result<Option<TimeSeriesEntry<V, L>>> {
        Ok(self.head_entries(true, 1).await?.into_iter().next())
    }

    /// The `count` newest live entries, oldest first, Redisson's `lastEntries`.
    #[doc(alias = "lastEntries")]
    pub async fn last_entries(&self, count: usize) -> Result<Vec<TimeSeriesEntry<V, L>>> {
        self.head_entries(true, count).await
    }

    /// The timestamp of the oldest live entry, Redisson's `firstTimestamp`.
    #[doc(alias = "firstTimestamp")]
    pub async fn first_timestamp(&self) -> Result<Option<i64>> {
        self.head_timestamp(false).await
    }

    /// The timestamp of the newest live entry, Redisson's `lastTimestamp`.
    #[doc(alias = "lastTimestamp")]
    pub async fn last_timestamp(&self) -> Result<Option<i64>> {
        self.head_timestamp(true).await
    }

    /// Removes the oldest live entry and returns its value, Redisson's `pollFirst()`. Expired entries are skipped and left for the clean-up.
    #[doc(alias = "pollFirst")]
    pub async fn pop_first(&self) -> Result<Option<V>> {
        Ok(self.pop_values(false, 1).await?.into_iter().next())
    }

    /// Removes the `count` oldest live entries and returns their values, oldest first, Redisson's `pollFirst(count)`.
    #[doc(alias = "pollFirst")]
    pub async fn pop_first_many(&self, count: usize) -> Result<Vec<V>> {
        self.pop_values(false, count).await
    }

    /// Removes the oldest live entry and returns it, Redisson's `pollFirstEntry`.
    #[doc(alias = "pollFirstEntry")]
    pub async fn pop_first_entry(&self) -> Result<Option<TimeSeriesEntry<V, L>>> {
        Ok(self.pop_entries(false, 1).await?.into_iter().next())
    }

    /// Removes the `count` oldest live entries and returns them, oldest first, Redisson's `pollFirstEntries`.
    #[doc(alias = "pollFirstEntries")]
    pub async fn pop_first_entries(&self, count: usize) -> Result<Vec<TimeSeriesEntry<V, L>>> {
        self.pop_entries(false, count).await
    }

    /// Removes the newest live entry and returns its value, Redisson's `pollLast()`.
    #[doc(alias = "pollLast")]
    pub async fn pop_last(&self) -> Result<Option<V>> {
        Ok(self.pop_values(true, 1).await?.into_iter().next())
    }

    /// Removes the `count` newest live entries and returns their values, oldest first, Redisson's `pollLast(count)`.
    #[doc(alias = "pollLast")]
    pub async fn pop_last_many(&self, count: usize) -> Result<Vec<V>> {
        self.pop_values(true, count).await
    }

    /// Removes the newest live entry and returns it, Redisson's `pollLastEntry`.
    #[doc(alias = "pollLastEntry")]
    pub async fn pop_last_entry(&self) -> Result<Option<TimeSeriesEntry<V, L>>> {
        Ok(self.pop_entries(true, 1).await?.into_iter().next())
    }

    /// Removes the `count` newest live entries and returns them, oldest first, Redisson's `pollLastEntries`.
    #[doc(alias = "pollLastEntries")]
    pub async fn pop_last_entries(&self, count: usize) -> Result<Vec<TimeSeriesEntry<V, L>>> {
        self.pop_entries(true, count).await
    }

    /// The distinct labels of the live entries in `range`, in order of first appearance, Redisson's `labels()` and `labels(start, end)`. Use `labels(..)` for all of them.
    pub async fn labels(&self, range: impl RangeBounds<i64>) -> Result<Vec<L>> {
        let Some((start, end)) = bounds(&range) else {
            return Ok(Vec::new());
        };
        let args = vec![
            Bytes::from(now_millis().to_string()),
            Bytes::from(start.to_string()),
            Bytes::from(end.to_string()),
        ];
        let reply: redis::Value = self
            .key
            .core
            .eval(&COLLECT_LABELS, self.data_keys(), args)
            .await?;
        reply::array(&reply)
            .iter()
            .map(|label| {
                let bytes = reply::bytes(label).ok_or_else(reply::malformed)?;
                self.codec.decode(&bytes)
            })
            .collect()
    }

    /// Streams the live values in timestamp order, 10 per call, Redisson's `iterator()` and `stream()`. It pages by rank, so entries added or removed meanwhile can shift what it returns.
    #[doc(alias = "iterator")]
    #[doc(alias = "stream")]
    pub fn iter(&self) -> impl Stream<Item = Result<V>> + '_ {
        self.iter_with(ITER_PAGE)
    }

    /// Like [`iter`](TimeSeries::iter) with `page_size` values per call (at least 1), Redisson's `iterator(count)`.
    pub fn iter_with(&self, page_size: usize) -> impl Stream<Item = Result<V>> + '_ {
        let page = page_size.max(1);
        stream::try_unfold(Some("0".to_string()), move |position| async move {
            let Some(position) = position else {
                return Ok::<_, Error>(None);
            };
            let args = vec![
                Bytes::from(position),
                Bytes::from(now_millis().to_string()),
                Bytes::from(page.to_string()),
            ];
            let (next, raw): (String, Vec<Bytes>) =
                self.key.core.eval(&SCAN, self.data_keys(), args).await?;
            let values = raw
                .iter()
                .map(|bytes| self.codec.decode(bytes))
                .collect::<Result<Vec<V>>>()?;
            let next = (next != "0").then_some(next);
            Ok(Some((stream::iter(values.into_iter().map(Ok)), next)))
        })
        .try_flatten()
    }

    /// The values of the live entries in `range`, oldest first, Redisson's `range`, `rangeReversed`, `rangeByLabel` and `rangeReversedByLabel`. Options: [`Range::rev`], [`Range::limit`], [`Range::label`], [`Range::unlabelled`]. `range(..)` is every entry.
    #[doc(alias = "rangeReversed")]
    #[doc(alias = "rangeByLabel")]
    #[doc(alias = "rangeReversedByLabel")]
    pub fn range(&self, range: impl RangeBounds<i64>) -> Range<'_, V, L, C> {
        Range::new(self, bounds(&range))
    }

    /// The live entries in `range` with their labels, oldest first, Redisson's `entryRange`, `entryRangeReversed`, `entryRangeByLabel` and `entryRangeReversedByLabel`. The same options as [`range`](TimeSeries::range).
    #[doc(alias = "entryRange")]
    #[doc(alias = "entryRangeReversed")]
    #[doc(alias = "entryRangeByLabel")]
    #[doc(alias = "entryRangeReversedByLabel")]
    pub fn range_entries(&self, range: impl RangeBounds<i64>) -> RangeEntries<'_, V, L, C> {
        RangeEntries::new(self, bounds(&range))
    }

    /// Removes the live entries in `range` and resolves to how many there were, Redisson's `removeRange` and `removeRangeByLabel`. Options: [`RemoveRange::label`], [`RemoveRange::unlabelled`].
    #[doc(alias = "removeRange")]
    #[doc(alias = "removeRangeByLabel")]
    pub fn remove_range(&self, range: impl RangeBounds<i64>) -> RemoveRange<'_, V, L, C> {
        RemoveRange::new(self, bounds(&range))
    }

    /// The live entries after `timestamp` (exclusive), oldest first, Redisson's `readTail`. Options: [`ReadAfter::count`], [`ReadAfter::label`], [`ReadAfter::unlabelled`]. A read never ends inside a timestamp, so feeding the last timestamp back in reads every entry once.
    #[doc(alias = "readTail")]
    pub fn read_after(&self, timestamp: impl Into<Timestamp>) -> ReadAfter<'_, V, L, C> {
        ReadAfter::new(self, timestamp.into().as_millis())
    }
}
