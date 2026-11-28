//! The replies of the `TS.*` commands (RESP2), read as Redisson's `TimeSeries*Decoder`s read them.

use super::options::{Aggregator, DuplicatePolicy, Encoding};
use crate::error::Result;
use crate::reply::malformed;
use redis::Value;
use std::time::Duration;

/// One sample: its timestamp and its values (one, or one per aggregator of a multi-aggregation range), Redisson's `TSSample`.
#[derive(Clone, Debug, PartialEq)]
pub struct Sample {
    /// Milliseconds since the Unix epoch.
    pub timestamp: i64,
    /// The values.
    pub values: Vec<f64>,
}

impl Sample {
    /// The first value, the value of a plain sample. `NaN` when there is none.
    pub fn value(&self) -> f64 {
        self.values.first().copied().unwrap_or(f64::NAN)
    }
}

/// A compaction rule of a series, Redisson's `TSRule`.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct Rule {
    /// The series the rule writes to.
    pub destination: String,
    /// The bucket duration.
    pub bucket: Duration,
    /// The aggregator, `None` for one this crate does not know.
    pub aggregator: Option<Aggregator>,
    /// The alignment timestamp.
    pub align_timestamp: i64,
}

/// One chunk of [`TsInfo::chunks`] (`TS.INFO .. DEBUG`), Redisson's `TSChunkInfo`.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct ChunkInfo {
    /// Its first timestamp.
    pub start_timestamp: i64,
    /// Its last timestamp.
    pub end_timestamp: i64,
    /// Its samples.
    pub samples: u64,
    /// Its size in bytes.
    pub size: u64,
    /// Bytes per sample.
    pub bytes_per_sample: f64,
}

/// The `TS.INFO` of a series, Redisson's `TSInfo`. Unknown fields are ignored.
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct TsInfo {
    /// The samples.
    pub total_samples: u64,
    /// The memory the series uses, in bytes.
    pub memory_usage: u64,
    /// The oldest timestamp, `None` for an empty series.
    pub first_timestamp: Option<i64>,
    /// The newest timestamp, `None` for an empty series.
    pub last_timestamp: Option<i64>,
    /// The retention, zero for none.
    pub retention: Duration,
    /// The chunks.
    pub chunk_count: u64,
    /// The chunk size in bytes.
    pub chunk_size: u64,
    /// The chunk encoding.
    pub chunk_type: Option<Encoding>,
    /// The duplicate policy, `None` for the server's default.
    pub duplicate_policy: Option<DuplicatePolicy>,
    /// The labels, in the server's order.
    pub labels: Vec<(String, String)>,
    /// The series this one is a compaction of.
    pub source_key: Option<String>,
    /// The compaction rules.
    pub rules: Vec<Rule>,
    /// The `IGNORE` time difference.
    pub ignore_max_time_diff: Duration,
    /// The `IGNORE` value difference.
    pub ignore_max_value_diff: f64,
    /// The key name (`DEBUG` only).
    pub key_self_name: Option<String>,
    /// The chunks (`DEBUG` only).
    pub chunks: Vec<ChunkInfo>,
}

fn text(value: &Value) -> Option<String> {
    match value {
        Value::BulkString(b) => Some(String::from_utf8_lossy(b).into_owned()),
        Value::SimpleString(s) => Some(s.clone()),
        Value::Int(n) => Some(n.to_string()),
        Value::Double(d) => Some(d.to_string()),
        Value::Okay => Some("OK".into()),
        _ => None,
    }
}

pub(crate) fn long(value: &Value) -> Result<i64> {
    match value {
        Value::Int(n) => Ok(*n),
        other => text(other)
            .and_then(|t| t.parse().ok())
            .ok_or_else(malformed),
    }
}

/// A number reply; `f64::from_str` reads `inf`, `-inf` and `nan`.
pub(crate) fn double(value: &Value) -> Result<f64> {
    match value {
        Value::Double(d) => Ok(*d),
        Value::Int(n) => Ok(*n as f64),
        other => text(other)
            .and_then(|t| t.parse().ok())
            .ok_or_else(malformed),
    }
}

pub(crate) fn items(value: &Value) -> &[Value] {
    match value {
        Value::Array(items) | Value::Set(items) => items,
        _ => &[],
    }
}

/// `[timestamp, value…]`, Redisson's `toSample`; `None` for an empty reply (`TS.GET` on an empty series).
pub(crate) fn sample(value: &Value) -> Result<Option<Sample>> {
    let parts = items(value);
    let Some((first, rest)) = parts.split_first() else {
        return Ok(None);
    };
    Ok(Some(Sample {
        timestamp: long(first)?,
        values: rest.iter().map(double).collect::<Result<_>>()?,
    }))
}

/// A list of samples.
pub(crate) fn samples(value: &Value) -> Result<Vec<Sample>> {
    items(value)
        .iter()
        .filter_map(|v| sample(v).transpose())
        .collect()
}

/// Labels as flat pairs or as pairs of pairs, Redisson's `toLabels`. A value may be nil (a selected label the series lacks).
pub(crate) fn labels(value: &Value) -> Vec<(String, Option<String>)> {
    let parts = items(value);
    if parts.first().is_some_and(|p| matches!(p, Value::Array(_))) {
        return parts
            .iter()
            .filter_map(|pair| {
                let pair = items(pair);
                Some((text(pair.first()?)?, pair.get(1).and_then(text)))
            })
            .collect();
    }
    parts
        .chunks(2)
        .filter_map(|pair| Some((text(pair.first()?)?, pair.get(1).and_then(text))))
        .collect()
}

fn rules(value: &Value) -> Result<Vec<Rule>> {
    let parts = items(value);
    let rule = |dest: &Value, rest: &[Value]| -> Result<Rule> {
        Ok(Rule {
            destination: text(dest).ok_or_else(malformed)?,
            bucket: Duration::from_millis(
                u64::try_from(long(rest.first().ok_or_else(malformed)?)?).unwrap_or(0),
            ),
            aggregator: rest
                .get(1)
                .and_then(text)
                .and_then(|t| Aggregator::parse(&t)),
            align_timestamp: rest.get(2).map(long).transpose()?.unwrap_or(0),
        })
    };
    if parts.first().is_some_and(|p| matches!(p, Value::Array(_))) {
        return parts
            .iter()
            .map(|r| {
                let r = items(r);
                let (dest, rest) = r.split_first().ok_or_else(malformed)?;
                rule(dest, rest)
            })
            .collect();
    }
    parts
        .chunks(2)
        .map(|pair| match pair {
            [dest, rest] => rule(dest, items(rest)),
            _ => Err(malformed()),
        })
        .collect()
}

fn chunks(value: &Value) -> Result<Vec<ChunkInfo>> {
    items(value)
        .iter()
        .map(|chunk| {
            let mut info = ChunkInfo {
                start_timestamp: 0,
                end_timestamp: 0,
                samples: 0,
                size: 0,
                bytes_per_sample: 0.0,
            };
            for pair in items(chunk).chunks(2) {
                let [name, value] = pair else { continue };
                match text(name).as_deref() {
                    Some("startTimestamp") => info.start_timestamp = long(value)?,
                    Some("endTimestamp") => info.end_timestamp = long(value)?,
                    Some("samples") => info.samples = u64::try_from(long(value)?).unwrap_or(0),
                    Some("size") => info.size = u64::try_from(long(value)?).unwrap_or(0),
                    Some("bytesPerSample") => info.bytes_per_sample = double(value)?,
                    _ => {}
                }
            }
            Ok(info)
        })
        .collect()
}

/// `TS.INFO` as flat pairs, Redisson's `TimeSeriesInfoDecoder`.
pub(crate) fn info(value: &Value) -> Result<TsInfo> {
    let mut info = TsInfo::default();
    let mut first = None;
    let mut last = None;
    for pair in items(value).chunks(2) {
        let [name, value] = pair else { continue };
        if *value == Value::Nil {
            continue;
        }
        let unsigned = |v: &Value| long(v).map(|n| u64::try_from(n).unwrap_or(0));
        match text(name).as_deref() {
            Some("totalSamples") => info.total_samples = unsigned(value)?,
            Some("memoryUsage") => info.memory_usage = unsigned(value)?,
            Some("firstTimestamp") => first = Some(long(value)?),
            Some("lastTimestamp") => last = Some(long(value)?),
            Some("retentionTime") => info.retention = Duration::from_millis(unsigned(value)?),
            Some("chunkCount") => info.chunk_count = unsigned(value)?,
            Some("chunkSize") => info.chunk_size = unsigned(value)?,
            Some("chunkType") => info.chunk_type = text(value).and_then(|t| Encoding::parse(&t)),
            Some("duplicatePolicy") => {
                info.duplicate_policy = text(value).and_then(|t| DuplicatePolicy::parse(&t))
            }
            Some("labels") => {
                info.labels = labels(value)
                    .into_iter()
                    .map(|(n, v)| (n, v.unwrap_or_default()))
                    .collect()
            }
            Some("sourceKey") => info.source_key = text(value),
            Some("rules") => info.rules = rules(value)?,
            Some("ignoreMaxTimeDiff") => {
                info.ignore_max_time_diff = Duration::from_millis(unsigned(value)?)
            }
            Some("ignoreMaxValDiff") => info.ignore_max_value_diff = double(value)?,
            Some("keySelfName") => info.key_self_name = text(value),
            Some("Chunks") => info.chunks = chunks(value)?,
            _ => {}
        }
    }
    if info.total_samples > 0 {
        info.first_timestamp = first;
        info.last_timestamp = last;
    }
    Ok(info)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(s: &str) -> Value {
        Value::BulkString(s.as_bytes().to_vec())
    }

    #[test]
    fn sample_parses_inf_and_nan() {
        let s = sample(&Value::Array(vec![Value::Int(5), bulk("inf")]))
            .unwrap()
            .unwrap();
        assert_eq!(s.timestamp, 5);
        assert_eq!(s.value(), f64::INFINITY);
        let n = sample(&Value::Array(vec![
            Value::Int(5),
            bulk("-inf"),
            bulk("nan"),
        ]))
        .unwrap()
        .unwrap();
        assert_eq!(n.values[0], f64::NEG_INFINITY);
        assert!(n.values[1].is_nan());
        assert_eq!(sample(&Value::Array(vec![])).unwrap(), None);
    }

    #[test]
    fn info_reads_flat_pairs() {
        let reply = Value::Array(vec![
            bulk("totalSamples"),
            Value::Int(0),
            bulk("firstTimestamp"),
            Value::Int(0),
            bulk("unknown"),
            bulk("x"),
            bulk("retentionTime"),
            Value::Int(1000),
            bulk("duplicatePolicy"),
            Value::Nil,
            bulk("labels"),
            Value::Array(vec![Value::Array(vec![bulk("a"), bulk("1")])]),
            bulk("rules"),
            Value::Array(vec![Value::Array(vec![
                bulk("dst"),
                Value::Int(60000),
                bulk("AVG"),
                Value::Int(0),
            ])]),
        ]);
        let info = info(&reply).unwrap();
        assert_eq!(info.first_timestamp, None);
        assert_eq!(info.retention, Duration::from_secs(1));
        assert_eq!(info.duplicate_policy, None);
        assert_eq!(info.labels, [("a".to_string(), "1".to_string())]);
        assert_eq!(info.rules[0].aggregator, Some(Aggregator::Avg));
        assert_eq!(info.rules[0].bucket, Duration::from_secs(60));
    }
}
