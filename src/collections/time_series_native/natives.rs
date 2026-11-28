//! [`TimeSeriesNatives`], Redisson's `RTimeSeriesNatives`: the `TS.*` commands over many series, chosen by key or by label filter.

use super::options::{range_ends, range_options, Aggregator, RangeEnd, RangeOptions, Reducer};
use super::reply::{self, Sample};
use super::unsupported;
use crate::client::core::Core;
use crate::client::request::Request;
use crate::error::{Error, Result};
use redis::Value;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::ops::RangeBounds;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

/// A label filter of `TS.MGET`, `TS.MRANGE` and `TS.QUERYINDEX`, the `FILTER` expressions. Redis needs at least one `=` expression in it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Filter {
    expressions: Vec<String>,
}

impl Filter {
    /// An empty filter.
    pub fn new() -> Self {
        Self::default()
    }

    /// `label=value`.
    pub fn eq(mut self, label: &str, value: &str) -> Self {
        self.expressions.push(format!("{label}={value}"));
        self
    }

    /// `label!=value`.
    pub fn ne(mut self, label: &str, value: &str) -> Self {
        self.expressions.push(format!("{label}!={value}"));
        self
    }

    /// `label=(a,b,…)`: one of the values.
    pub fn eq_any<'v>(mut self, label: &str, values: impl IntoIterator<Item = &'v str>) -> Self {
        let values: Vec<&str> = values.into_iter().collect();
        self.expressions
            .push(format!("{label}=({})", values.join(",")));
        self
    }

    /// `label!=(a,b,…)`: none of the values.
    pub fn ne_any<'v>(mut self, label: &str, values: impl IntoIterator<Item = &'v str>) -> Self {
        let values: Vec<&str> = values.into_iter().collect();
        self.expressions
            .push(format!("{label}!=({})", values.join(",")));
        self
    }

    /// `label!=`: the series has the label.
    pub fn has(mut self, label: &str) -> Self {
        self.expressions.push(format!("{label}!="));
        self
    }

    /// `label=`: the series lacks the label.
    pub fn missing(mut self, label: &str) -> Self {
        self.expressions.push(format!("{label}="));
        self
    }

    /// An expression as written.
    pub fn raw(mut self, expression: impl Into<String>) -> Self {
        self.expressions.push(expression.into());
        self
    }

    /// The expressions, in order.
    pub fn expressions(&self) -> &[String] {
        &self.expressions
    }

    fn required(&self) -> Result<&[String]> {
        if self.expressions.is_empty() {
            return Err(Error::Config(
                "At least one filter expression is required".into(),
            ));
        }
        Ok(&self.expressions)
    }
}

/// The newest sample of one series of [`TimeSeriesNatives::get`], Redisson's `TSSeriesSample`.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct SeriesSample {
    /// The series.
    pub key: String,
    /// Its labels, when asked for; a selected label the series lacks is `None`.
    pub labels: Vec<(String, Option<String>)>,
    /// Its newest sample, `None` for an empty series.
    pub sample: Option<Sample>,
}

/// The samples of one series (or group) of [`TimeSeriesNatives::range`], Redisson's `TSSeriesSamples`.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct SeriesSamples {
    /// The series, or `label=value` with [`MultiRange::group_by`].
    pub key: String,
    /// Its labels, when asked for.
    pub labels: Vec<(String, Option<String>)>,
    /// Its samples.
    pub samples: Vec<Sample>,
}

/// **Experimental.** The `TS.*` commands over many series, Redisson's `RTimeSeriesNatives`. Needs Redis 8.0 or the RedisTimeSeries module; filter commands go to one master, whose module answers for the cluster.
#[derive(Clone)]
pub struct TimeSeriesNatives {
    core: Arc<Core>,
}

impl fmt::Debug for TimeSeriesNatives {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimeSeriesNatives").finish_non_exhaustive()
    }
}

/// `WITHLABELS` or `SELECTED_LABELS l…`, Redisson's `labelSelection`.
fn label_selection(with_labels: bool, selected: &Option<Vec<String>>) -> Result<Vec<String>> {
    match (with_labels, selected) {
        (true, Some(_)) => Err(Error::Config(
            "withLabels() and selectedLabels() are mutually exclusive".into(),
        )),
        (_, Some(labels)) if labels.is_empty() => Err(Error::Config(
            "selectedLabels() requires at least one label".into(),
        )),
        (_, Some(labels)) => Ok(std::iter::once("SELECTED_LABELS".to_string())
            .chain(labels.iter().cloned())
            .collect()),
        (true, None) => Ok(vec!["WITHLABELS".into()]),
        (false, None) => Ok(Vec::new()),
    }
}

fn text(value: &Value) -> Result<String> {
    crate::reply::text(value).ok_or_else(crate::reply::malformed)
}

impl TimeSeriesNatives {
    pub(crate) fn new(core: Arc<Core>) -> Self {
        Self { core }
    }

    /// Sends a keyless command to one master, as Redisson's `readAsync((String) null, …)`.
    async fn by_filter(&self, cmd: redis::Cmd) -> Result<Value> {
        let masters = self.core.masters().await?;
        let master = masters.first().ok_or_else(crate::reply::malformed)?;
        self.core
            .query_master(master, &cmd)
            .await
            .map_err(unsupported)
    }

    /// Adds samples to many series, one `TS.MADD` per hash slot, Redisson's `addAll(Map)`. Resolves to the timestamps stored per series, in input order. Series are not created; a refused sample fails the call while others may be stored.
    #[doc(alias = "addAll")]
    pub async fn add_many<N, S>(
        &self,
        series: impl IntoIterator<Item = (N, S)>,
    ) -> Result<Vec<(String, Vec<i64>)>>
    where
        N: Into<String>,
        S: IntoIterator<Item = (i64, f64)>,
    {
        let series: Vec<(String, Vec<(i64, f64)>)> = series
            .into_iter()
            .map(|(n, s)| (n.into(), s.into_iter().collect()))
            .collect();
        if series.is_empty() {
            return Ok(Vec::new());
        }
        let keys: Vec<&str> = series.iter().map(|(k, _)| k.as_str()).collect();
        let replies = self
            .core
            .query_by_slot(&keys, false, |group| {
                let mut cmd = redis::cmd("TS.MADD");
                for i in group {
                    if let Some((key, samples)) = series.get(*i) {
                        for (ts, value) in samples {
                            cmd.arg(key).arg(*ts).arg(value.to_string());
                        }
                    }
                }
                cmd
            })
            .await
            .map_err(unsupported)?;
        let mut result: Vec<Option<Vec<i64>>> = vec![None; series.len()];
        for (group, reply) in replies {
            let mut items = reply::items(&reply).iter();
            for i in group {
                let count = series.get(i).map_or(0, |(_, s)| s.len());
                let stamps = (&mut items)
                    .take(count)
                    .map(|item| match item {
                        Value::ServerError(e) => Err(Error::Redis(format!("{e:?}"))),
                        other => reply::long(other),
                    })
                    .collect::<Result<Vec<i64>>>()?;
                if let Some(slot) = result.get_mut(i) {
                    *slot = Some(stamps);
                }
            }
        }
        Ok(series
            .into_iter()
            .zip(result)
            .map(|((key, _), stamps)| (key, stamps.unwrap_or_default()))
            .collect())
    }

    /// The newest sample of every series `filter` selects (`TS.MGET`), Redisson's `getAll`.
    #[doc(alias = "getAll")]
    pub fn get<'a>(&'a self, filter: &'a Filter) -> MultiGet<'a> {
        MultiGet {
            natives: self,
            filter,
            latest: false,
            with_labels: false,
            selected: None,
        }
    }

    /// The samples of every series `filter` selects (`TS.MRANGE`; `TS.MREVRANGE` with [`MultiRange::rev`]), Redisson's `range` and `rangeReversed`.
    #[doc(alias = "rangeReversed")]
    pub fn range<'a>(&'a self, filter: &'a Filter, range: impl RangeBounds<i64>) -> MultiRange<'a> {
        MultiRange {
            natives: self,
            filter,
            ends: range_ends(&range),
            rev: false,
            range: RangeOptions::default(),
            with_labels: false,
            selected: None,
            group_by: None,
            exclude_empty: false,
        }
    }

    /// The samples of `keys` merged into one series (`TS.NRANGE`; `TS.NREVRANGE` with [`GroupedRange::rev`]), Redisson's `groupedRange`. Routed by the first key.
    #[doc(alias = "groupedRange")]
    #[doc(alias = "groupedRangeReversed")]
    pub fn grouped_range<'a>(
        &'a self,
        keys: &'a [&'a str],
        range: impl RangeBounds<i64>,
    ) -> GroupedRange<'a> {
        GroupedRange {
            natives: self,
            keys,
            ends: range_ends(&range),
            rev: false,
            range: RangeOptions::default(),
        }
    }

    /// The keys of the series `filter` selects (`TS.QUERYINDEX`), Redisson's `queryIndex`.
    #[doc(alias = "queryIndex")]
    pub async fn query_index(&self, filter: &Filter) -> Result<Vec<String>> {
        let mut cmd = redis::cmd("TS.QUERYINDEX");
        for e in filter.required()? {
            cmd.arg(e);
        }
        let reply = self.by_filter(cmd).await?;
        reply::items(&reply).iter().map(text).collect()
    }

    /// The label names in use (`TS.QUERYLABELS LABELS`), Redisson's `labelNames`.
    #[doc(alias = "labelNames")]
    pub fn label_names(&self) -> LabelNames<'_> {
        LabelNames {
            natives: self,
            filter: None,
        }
    }

    /// The values of `label` in use (`TS.QUERYLABELS VALUES`), Redisson's `labelValues`.
    #[doc(alias = "labelValues")]
    pub fn label_values<'a>(&'a self, label: &'a str) -> LabelValues<'a> {
        LabelValues {
            natives: self,
            label,
            filter: None,
        }
    }
}

/// A pending [`TimeSeriesNatives::get`].
#[must_use = "a get does nothing until it is awaited"]
pub struct MultiGet<'a> {
    natives: &'a TimeSeriesNatives,
    filter: &'a Filter,
    latest: bool,
    with_labels: bool,
    selected: Option<Vec<String>>,
}

impl MultiGet<'_> {
    /// `LATEST`.
    pub fn latest(mut self) -> Self {
        self.latest = true;
        self
    }

    /// Reports every label (`WITHLABELS`).
    pub fn with_labels(mut self) -> Self {
        self.with_labels = true;
        self
    }

    /// Reports these labels (`SELECTED_LABELS`).
    pub fn selected_labels(mut self, labels: &[&str]) -> Self {
        self.selected = Some(labels.iter().map(|l| l.to_string()).collect());
        self
    }
}

impl<'a> IntoFuture for MultiGet<'a> {
    type Output = Result<Vec<SeriesSample>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let filters = self.filter.required()?;
            let mut cmd = redis::cmd("TS.MGET");
            if self.latest {
                cmd.arg("LATEST");
            }
            for s in label_selection(self.with_labels, &self.selected)? {
                cmd.arg(s);
            }
            cmd.arg("FILTER");
            for f in filters {
                cmd.arg(f);
            }
            let reply = self.natives.by_filter(cmd).await?;
            reply::items(&reply)
                .iter()
                .map(|series| {
                    let parts = reply::items(series);
                    Ok(SeriesSample {
                        key: text(parts.first().ok_or_else(crate::reply::malformed)?)?,
                        labels: parts.get(1).map(reply::labels).unwrap_or_default(),
                        sample: parts.get(2).map(reply::sample).transpose()?.flatten(),
                    })
                })
                .collect()
        })
    }
}

/// A pending [`TimeSeriesNatives::range`].
#[must_use = "a range does nothing until it is awaited"]
pub struct MultiRange<'a> {
    natives: &'a TimeSeriesNatives,
    filter: &'a Filter,
    ends: Option<(RangeEnd, RangeEnd)>,
    rev: bool,
    range: RangeOptions,
    with_labels: bool,
    selected: Option<Vec<String>>,
    group_by: Option<(String, Reducer)>,
    exclude_empty: bool,
}

impl MultiRange<'_> {
    /// Newest first (`TS.MREVRANGE`).
    pub fn rev(mut self) -> Self {
        self.rev = true;
        self
    }

    /// Reports every label (`WITHLABELS`).
    pub fn with_labels(mut self) -> Self {
        self.with_labels = true;
        self
    }

    /// Reports these labels (`SELECTED_LABELS`).
    pub fn selected_labels(mut self, labels: &[&str]) -> Self {
        self.selected = Some(labels.iter().map(|l| l.to_string()).collect());
        self
    }

    /// Merges the series by the value of `label` with `reducer` (`GROUPBY … REDUCE`).
    pub fn group_by(mut self, label: &str, reducer: Reducer) -> Self {
        self.group_by = Some((label.to_string(), reducer));
        self
    }

    /// Leaves out the series with no sample in the range (`EXCLUDEEMPTY`, Redis 8.10).
    pub fn exclude_empty(mut self) -> Self {
        self.exclude_empty = true;
        self
    }

    range_options!();
}

impl<'a> IntoFuture for MultiRange<'a> {
    type Output = Result<Vec<SeriesSamples>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let filters = self.filter.required()?;
            if self.group_by.is_some() {
                if self.exclude_empty {
                    return Err(Error::Config(
                        "groupBy() and excludeEmpty() are mutually exclusive".into(),
                    ));
                }
                if self
                    .range
                    .aggregation
                    .as_ref()
                    .is_some_and(|(_, a)| a.first().is_some_and(|a| a.len() > 1))
                {
                    return Err(Error::Config("groupBy() is mutually exclusive with an aggregation() of more than one aggregator".into()));
                }
            }
            let selection = label_selection(self.with_labels, &self.selected)?;
            let Some((from, to)) = self.ends else {
                return Ok(Vec::new());
            };
            let mut cmd = redis::cmd(if self.rev {
                "TS.MREVRANGE"
            } else {
                "TS.MRANGE"
            });
            self.range.append(from, to, &selection, &mut cmd)?;
            if self.exclude_empty {
                cmd.arg("EXCLUDEEMPTY");
            }
            cmd.arg("FILTER");
            for f in filters {
                cmd.arg(f);
            }
            if let Some((label, reducer)) = &self.group_by {
                cmd.arg("GROUPBY")
                    .arg(label)
                    .arg("REDUCE")
                    .arg(reducer.keyword());
            }
            let reply = self.natives.by_filter(cmd).await?;
            reply::items(&reply)
                .iter()
                .map(|series| {
                    let parts = reply::items(series);
                    Ok(SeriesSamples {
                        key: text(parts.first().ok_or_else(crate::reply::malformed)?)?,
                        labels: parts.get(1).map(reply::labels).unwrap_or_default(),
                        samples: parts
                            .get(2)
                            .map(reply::samples)
                            .transpose()?
                            .unwrap_or_default(),
                    })
                })
                .collect()
        })
    }
}

/// A pending [`TimeSeriesNatives::grouped_range`].
#[must_use = "a range does nothing until it is awaited"]
pub struct GroupedRange<'a> {
    natives: &'a TimeSeriesNatives,
    keys: &'a [&'a str],
    ends: Option<(RangeEnd, RangeEnd)>,
    rev: bool,
    range: RangeOptions,
}

impl GroupedRange<'_> {
    /// Newest first (`TS.NREVRANGE`).
    pub fn rev(mut self) -> Self {
        self.rev = true;
        self
    }

    /// One list of aggregators per key, in key order (`AGGREGATION a,b c …`).
    pub fn aggregate_each(mut self, bucket: Duration, aggregators: &[&[Aggregator]]) -> Self {
        self.range.aggregation = Some((bucket, aggregators.iter().map(|a| a.to_vec()).collect()));
        self
    }

    range_options!();
}

impl<'a> IntoFuture for GroupedRange<'a> {
    type Output = Result<Vec<Sample>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let Some(first) = self.keys.first() else {
                return Err(Error::Config("At least one key is required".into()));
            };
            if let Some((_, aggregations)) = &self.range.aggregation {
                // One aggregation for every key is written once per key, as Redisson's `aggregation(..)` does.
                if aggregations.len() != self.keys.len() && aggregations.len() != 1 {
                    return Err(Error::Config(format!(
                        "Aggregations must hold one entry per key: {} expected, {} given",
                        self.keys.len(),
                        aggregations.len()
                    )));
                }
            }
            let mut range = self.range.clone();
            if let Some((bucket, aggregations)) = &range.aggregation {
                if aggregations.len() == 1 && self.keys.len() > 1 {
                    let each = aggregations[0].clone();
                    range.aggregation = Some((*bucket, vec![each; self.keys.len()]));
                }
            }
            let Some((from, to)) = self.ends else {
                return Ok(Vec::new());
            };
            let mut cmd = redis::cmd(if self.rev {
                "TS.NREVRANGE"
            } else {
                "TS.NRANGE"
            });
            cmd.arg(self.keys.len());
            for key in self.keys {
                cmd.arg(*key);
            }
            range.append(from, to, &[], &mut cmd)?;
            let reply: Value = self
                .natives
                .core
                .query(Request::new(cmd).key(first.as_bytes()).read())
                .await
                .map_err(unsupported)?;
            reply::grouped_samples(&reply)
        })
    }
}

/// A pending [`TimeSeriesNatives::label_names`]. Await it, optionally after [`LabelNames::filter`].
#[must_use = "a query does nothing until it is awaited"]
pub struct LabelNames<'a> {
    natives: &'a TimeSeriesNatives,
    filter: Option<&'a Filter>,
}

impl<'a> LabelNames<'a> {
    /// Only the series `filter` selects.
    pub fn filter(mut self, filter: &'a Filter) -> Self {
        self.filter = Some(filter);
        self
    }
}

impl<'a> IntoFuture for LabelNames<'a> {
    type Output = Result<Vec<String>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let mut cmd = redis::cmd("TS.QUERYLABELS");
            cmd.arg("LABELS");
            if let Some(filter) = self.filter.filter(|f| !f.expressions.is_empty()) {
                cmd.arg("FILTER");
                for e in &filter.expressions {
                    cmd.arg(e);
                }
            }
            let reply = self.natives.by_filter(cmd).await?;
            reply::items(&reply).iter().map(text).collect()
        })
    }
}

/// A pending [`TimeSeriesNatives::label_values`]. Await it, optionally after [`LabelValues::filter`].
#[must_use = "a query does nothing until it is awaited"]
pub struct LabelValues<'a> {
    natives: &'a TimeSeriesNatives,
    label: &'a str,
    filter: Option<&'a Filter>,
}

impl<'a> LabelValues<'a> {
    /// Only the series `filter` selects.
    pub fn filter(mut self, filter: &'a Filter) -> Self {
        self.filter = Some(filter);
        self
    }
}

impl<'a> IntoFuture for LabelValues<'a> {
    type Output = Result<Vec<String>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let mut cmd = redis::cmd("TS.QUERYLABELS");
            cmd.arg("VALUES").arg(self.label);
            if let Some(filter) = self.filter.filter(|f| !f.expressions.is_empty()) {
                cmd.arg("FILTER");
                for e in &filter.expressions {
                    cmd.arg(e);
                }
            }
            let reply = self.natives.by_filter(cmd).await?;
            reply::items(&reply).iter().map(text).collect()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_expressions() {
        let f = Filter::new()
            .eq("a", "1")
            .ne("b", "2")
            .eq_any("c", ["x", "y"])
            .ne_any("d", ["z"])
            .has("e")
            .missing("f");
        assert_eq!(
            f.expressions(),
            ["a=1", "b!=2", "c=(x,y)", "d!=(z)", "e!=", "f="]
        );
        assert!(Filter::new().required().is_err());
    }

    #[test]
    fn label_selection_rules() {
        assert!(label_selection(true, &Some(vec!["a".into()])).is_err());
        assert!(label_selection(false, &Some(vec![])).is_err());
        assert_eq!(label_selection(true, &None).unwrap(), ["WITHLABELS"]);
        assert_eq!(
            label_selection(false, &Some(vec!["a".into()])).unwrap(),
            ["SELECTED_LABELS", "a"]
        );
    }
}
