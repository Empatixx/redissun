//! Tests of `TimeSeriesNative` and `TimeSeriesNatives`, Redisson's `RTimeSeriesNative` and `RTimeSeriesNatives`, on `redis:8.10-alpine`.
//!
//! Ported from Redisson 4.8.0 `RedissonTimeSeriesNativeTest` and `RedissonTimeSeriesNativesTest` (named `redisson_<snake_case>`), plus own tests.

mod common;

use common::{redis810_client, unique};
use futures::TryStreamExt;
use redissun::time_series_native::{Aggregator, DuplicatePolicy, Filter, ReadFrom, Reducer};
use redissun::{Error, Object, TimeSeriesNative};
use std::time::{Duration, Instant};

async fn native(name: &str) -> TimeSeriesNative {
    redis810_client().await.time_series_native(name.to_string())
}

fn values(samples: &[redissun::time_series_native::Sample]) -> Vec<f64> {
    samples.iter().map(|s| s.value()).collect()
}

fn stamps(samples: &[redissun::time_series_native::Sample]) -> Vec<i64> {
    samples.iter().map(|s| s.timestamp).collect()
}

#[tokio::test]
async fn ts_commands_need_redis_8() {
    let t = common::client().await.time_series_native(unique("tsn"));
    let unsupported = |r: Result<(), Error>| match r {
        Err(Error::Unsupported(m)) => assert!(m.contains("8.0"), "{m}"),
        other => panic!("{other:?}"),
    };
    unsupported(t.create().await);
    unsupported(t.try_create().await.map(drop));
    unsupported(t.add(1, 1.0).await.map(drop));
    unsupported(t.add(1, 1.0).await.map(drop));
    unsupported(t.add_many([(1, 1.0)]).await.map(drop));
    unsupported(t.get().await.map(drop));
    unsupported(t.range(..).await.map(drop));
    unsupported(t.info().await.map(drop));
    unsupported(t.labels().await.map(drop));
}

// Redisson: testCreate, testCreateWithArgs, testGetLabels
#[tokio::test]
async fn redisson_create_with_args() {
    let t = native(&unique("tsn")).await;
    t.create()
        .retention(Duration::from_secs(60))
        .duplicate_policy(DuplicatePolicy::Max)
        .label("sensor", "a")
        .label("area", "x")
        .await
        .unwrap();
    let info = t.info().await.unwrap();
    assert_eq!(info.retention, Duration::from_secs(60));
    assert_eq!(info.duplicate_policy, Some(DuplicatePolicy::Max));
    assert_eq!(
        t.labels().await.unwrap(),
        [
            ("sensor".to_string(), "a".to_string()),
            ("area".to_string(), "x".to_string())
        ]
    );
    // Redisson: testCreateTwiceFails
    assert!(matches!(t.create().await, Err(Error::Redis(_))));
}

// Redisson: testCreateIfAbsent
#[tokio::test]
async fn redisson_create_if_absent() {
    let t = native(&unique("tsn")).await;
    assert!(t.try_create().label("a", "1").await.unwrap());
    assert!(!t.try_create().await.unwrap());
    assert_eq!(t.labels().await.unwrap(), [("a".into(), "1".into())]);
}

// Redisson: testAlter, testAlterKeepsLabelsWhenNotGiven
#[tokio::test]
async fn redisson_alter() {
    let t = native(&unique("tsn")).await;
    t.create().label("a", "1").await.unwrap();
    t.alter().retention(Duration::from_secs(5)).await.unwrap();
    let info = t.info().await.unwrap();
    assert_eq!(info.retention, Duration::from_secs(5));
    assert_eq!(info.labels, [("a".to_string(), "1".to_string())]);
}

// Redisson: testAdd, testGet, testGetOnEmptySeries, testSizeAndTimestamps
#[tokio::test]
async fn redisson_add_and_get() {
    let t = native(&unique("tsn")).await;
    t.create().await.unwrap();
    assert_eq!(t.get().await.unwrap(), None);
    assert_eq!(t.len().await.unwrap(), 0);
    assert_eq!(t.first_timestamp().await.unwrap(), None);
    assert_eq!(t.add(1000, 1.5).await.unwrap(), 1000);
    assert_eq!(t.add(2000, 2.5).await.unwrap(), 2000);
    let last = t.get().await.unwrap().unwrap();
    assert_eq!((last.timestamp, last.value()), (2000, 2.5));
    assert_eq!(t.len().await.unwrap(), 2);
    assert_eq!(t.first_timestamp().await.unwrap(), Some(1000));
    assert_eq!(t.last_timestamp().await.unwrap(), Some(2000));
    let first = t.first().await.unwrap().unwrap();
    assert_eq!(first.timestamp, 1000);
}

// Redisson: testAddOnDuplicate, testAddWithCreationArgs
#[tokio::test]
async fn redisson_add_on_duplicate_and_creation_args() {
    let t = native(&unique("tsn")).await;
    t.add(1, 1.0).label("k", "v").await.unwrap();
    assert_eq!(t.labels().await.unwrap(), [("k".into(), "v".into())]);
    t.add(1, 2.0)
        .on_duplicate(DuplicatePolicy::Sum)
        .await
        .unwrap();
    assert_eq!(t.get().await.unwrap().unwrap().value(), 3.0);
}

// Redisson: testAddCurrent
#[tokio::test]
async fn redisson_add_current() {
    let t = native(&unique("tsn")).await;
    let ts = t.add_now(4.0).await.unwrap();
    assert!(ts > 1_600_000_000_000);
}

// Redisson: testIncrementBy, testIncrementByCreatesSeries, testDecrementBy
#[tokio::test]
async fn redisson_increment_and_decrement() {
    let t = native(&unique("tsn")).await;
    assert_eq!(t.incr_by(5.0).timestamp(10).await.unwrap(), 10);
    assert_eq!(t.incr_by(2.0).timestamp(10).await.unwrap(), 10);
    assert_eq!(t.get().await.unwrap().unwrap().value(), 7.0);
    t.decr_by(3.0).timestamp(20).await.unwrap();
    assert_eq!(t.get().await.unwrap().unwrap().value(), 4.0);
}

// Redisson: testAddAll, testAddAllEmpty
#[tokio::test]
async fn redisson_add_all() {
    let t = native(&unique("tsn")).await;
    t.create().await.unwrap();
    assert!(t.add_many(Vec::new()).await.unwrap().is_empty());
    assert_eq!(
        t.add_many([(1, 1.0), (2, 2.0), (3, 3.0)]).await.unwrap(),
        [1, 2, 3]
    );
    assert_eq!(t.len().await.unwrap(), 3);
}

// Redisson: testAddAllDoesNotCreateTheSeries
#[tokio::test]
async fn redisson_add_all_does_not_create_the_series() {
    let t = native(&unique("tsn")).await;
    assert!(t.add_many([(1, 1.0)]).await.is_err());
    assert!(!t.exists().await.unwrap());
}

// Redisson: testSubMillisecondRetentionIsRejected
#[tokio::test]
async fn redisson_sub_millisecond_retention_is_rejected() {
    let t = native(&unique("tsn")).await;
    assert!(matches!(
        t.create().retention(Duration::from_micros(500)).await,
        Err(Error::Config(_))
    ));
    assert!(!t.exists().await.unwrap());
}

// Redisson: testRange, testRangeReversed, testRangeCount, testRemoveRange
#[tokio::test]
async fn redisson_range() {
    let t = native(&unique("tsn")).await;
    for i in 1..=5 {
        t.add(i * 1000, i as f64).await.unwrap();
    }
    assert_eq!(
        stamps(&t.range(..).await.unwrap()),
        [1000, 2000, 3000, 4000, 5000]
    );
    assert_eq!(stamps(&t.range(1000..3000).await.unwrap()), [1000, 2000]);
    assert_eq!(
        values(&t.range(..).rev().count(2).await.unwrap()),
        [5.0, 4.0]
    );
    assert!(t.range(5..5).await.unwrap().is_empty());
    assert_eq!(
        values(&t.range(..).filter_by_value(2.0, 3.0).await.unwrap()),
        [2.0, 3.0]
    );
    assert_eq!(
        stamps(&t.range(..).filter_by_ts(&[1000, 5000]).await.unwrap()),
        [1000, 5000]
    );
    assert_eq!(t.remove_range(2000..=3000).await.unwrap(), 2);
    assert_eq!(t.len().await.unwrap(), 3);
}

// Redisson: testRangeSingleAggregation, testRangeEmptyBuckets, testAlignWithoutAggregationIsRejected
#[tokio::test]
async fn redisson_range_aggregation() {
    use redissun::time_series_native::Align;
    let t = native(&unique("tsn")).await;
    for (ts, v) in [(0, 1.0), (10, 3.0), (100, 5.0)] {
        t.add(ts, v).await.unwrap();
    }
    let avg = t
        .range(..)
        .aggregate(Duration::from_millis(50), &[Aggregator::Avg])
        .await
        .unwrap();
    assert_eq!(stamps(&avg), [0, 100]);
    assert_eq!(values(&avg), [2.0, 5.0]);
    let with_empty = t
        .range(0..=100)
        .aggregate(Duration::from_millis(50), &[Aggregator::Count])
        .empty()
        .await
        .unwrap();
    assert_eq!(stamps(&with_empty), [0, 50, 100]);
    assert!(matches!(
        t.range(..).align(Align::At(5)).await,
        Err(Error::Config(_))
    ));
}

// Redisson: testRead, testReadSentinelsAndMaxCount
#[tokio::test]
async fn redisson_read() {
    let t = native(&unique("tsn")).await;
    for i in 1..=3 {
        t.add(i, i as f64).await.unwrap();
    }
    assert_eq!(stamps(&t.read(ReadFrom::At(2)).await.unwrap()), [2, 3]);
    assert_eq!(
        stamps(&t.read(ReadFrom::Earliest).max_count(2).await.unwrap()),
        [1, 2]
    );
}

#[tokio::test]
async fn blocking_read_does_not_hit_the_command_timeout() {
    let client = redissun::Client::builder()
        .url(common::redis810_url().await)
        .timeout(Duration::from_secs(1))
        .build()
        .await
        .unwrap();
    let t = client.time_series_native(unique("tsn"));
    t.create().await.unwrap();
    let started = Instant::now();
    let samples = t
        .read(ReadFrom::Next)
        .block(Duration::from_millis(2500), 1)
        .await
        .unwrap();
    assert!(samples.is_empty());
    assert!(started.elapsed() >= Duration::from_millis(2400));
    assert!(matches!(
        t.read(ReadFrom::Next).block(Duration::ZERO, 1).await,
        Err(Error::Config(_))
    ));
}

// Redisson: testCreateRule, testDeleteRule
#[tokio::test]
async fn redisson_create_and_delete_rule() {
    let tag = unique("r");
    let src = native(&format!("{{{tag}}}:src")).await;
    let dst_name = format!("{{{tag}}}:dst");
    let dst = native(&dst_name).await;
    src.create().await.unwrap();
    dst.create().await.unwrap();
    src.create_rule(&dst_name, Aggregator::Avg, Duration::from_secs(60))
        .await
        .unwrap();
    let rules = src.info().await.unwrap().rules;
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].destination, dst_name);
    assert_eq!(rules[0].aggregator, Some(Aggregator::Avg));
    assert_eq!(
        dst.info().await.unwrap().source_key.as_deref(),
        Some(src_name(&tag).as_str())
    );
    src.delete_rule(&dst_name).await.unwrap();
    assert!(src.info().await.unwrap().rules.is_empty());
}

fn src_name(tag: &str) -> String {
    format!("{{{tag}}}:src")
}

// Redisson: testDebugInfo
#[tokio::test]
async fn redisson_debug_info() {
    let t = native(&unique("tsn")).await;
    t.add(1, 1.0).await.unwrap();
    let info = t.info().debug().await.unwrap();
    assert!(!info.chunks.is_empty());
}

// Redisson: testIteratorPagesAcrossWindows, testIteratorWhenTheLastWindowIsExactlyFull
#[tokio::test]
async fn redisson_iterator_pages_across_windows() {
    let t = native(&unique("tsn")).await;
    for i in 1..=25 {
        t.add(i, i as f64).await.unwrap();
    }
    let all: Vec<_> = t.iter_with(5).try_collect().await.unwrap();
    assert_eq!(all.len(), 25);
    assert_eq!(all.last().unwrap().timestamp, 25);
    let default: Vec<_> = t.iter().try_collect().await.unwrap();
    assert_eq!(default.len(), 25);
}

// Redisson: testIteratorOnEmptyAndAbsentSeries
#[tokio::test]
async fn redisson_iterator_on_an_absent_series_fails() {
    let t = native(&unique("tsn")).await;
    let result: Result<Vec<_>, _> = t.iter().try_collect().await;
    assert!(result.is_err());
}

// Redisson: testExpirable
#[tokio::test]
async fn redisson_expirable() {
    let t = native(&unique("tsn")).await;
    t.add(1, 1.0).await.unwrap();
    assert!(t.expire(Duration::from_secs(60)).await.unwrap());
    assert!(t.ttl().await.unwrap().is_some());
    assert!(t.del().await.unwrap());
}

// ---- TimeSeriesNatives ----

/// Two series labelled `area=<unique>`, as Redisson's `setUp`.
async fn two_series() -> (redissun::TimeSeriesNatives, String, [String; 2]) {
    let client = redis810_client().await;
    let area = unique("area");
    let names = [unique("cpu"), unique("cpu")];
    for (i, name) in names.iter().enumerate() {
        let t = client.time_series_native(name.clone());
        t.create()
            .label("area", &area)
            .label("host", format!("h{i}"))
            .await
            .unwrap();
        for ts in 1..=3 {
            t.add(ts * 10, (i * 10 + ts as usize) as f64).await.unwrap();
        }
    }
    (client.time_series_natives(), area, names)
}

// Redisson (Natives): testGetAll, testGetAllWithLabels, testGetAllRequiresFilter
#[tokio::test]
async fn redisson_natives_get_all() {
    let (natives, area, names) = two_series().await;
    let filter = Filter::new().eq("area", &area);
    let mut got = natives.get(&filter).with_labels().await.unwrap();
    got.sort_by(|a, b| a.key.cmp(&b.key));
    let mut expected = names.to_vec();
    expected.sort();
    assert_eq!(
        got.iter().map(|s| s.key.clone()).collect::<Vec<_>>(),
        expected
    );
    assert!(got
        .iter()
        .all(|s| s.sample.as_ref().unwrap().timestamp == 30));
    assert!(got[0].labels.iter().any(|(n, _)| n == "host"));
    assert!(matches!(
        natives.get(&Filter::new()).await,
        Err(Error::Config(_))
    ));
    let selected = natives
        .get(&filter)
        .selected_labels(&["host", "nope"])
        .await
        .unwrap();
    assert!(selected[0]
        .labels
        .iter()
        .any(|(n, v)| n == "nope" && v.is_none()));
}

// Redisson (Natives): testRange, testRangeGroupBy, testQueryIndex, testLabelValues
#[tokio::test]
async fn redisson_natives_range_query_and_labels() {
    let (natives, area, names) = two_series().await;
    let filter = Filter::new().eq("area", &area);
    let ranges = natives.range(&filter, ..).await.unwrap();
    assert_eq!(ranges.len(), 2);
    assert!(ranges.iter().all(|r| r.samples.len() == 3));
    let grouped = natives
        .range(&filter, ..)
        .group_by("area", Reducer::Sum)
        .await
        .unwrap();
    assert_eq!(grouped.len(), 1);
    assert_eq!(grouped[0].samples.len(), 3);
    let mut index = natives.query_index(&filter).await.unwrap();
    index.sort();
    let mut expected = names.to_vec();
    expected.sort();
    assert_eq!(index, expected);
    let mut hosts = natives.label_values("host").filter(&filter).await.unwrap();
    hosts.sort();
    assert_eq!(hosts, ["h0", "h1"]);
    assert!(natives
        .label_names()
        .filter(&filter)
        .await
        .unwrap()
        .contains(&"area".to_string()));
    assert!(matches!(
        natives
            .range(&filter, ..)
            .group_by("area", Reducer::Sum)
            .exclude_empty()
            .await,
        Err(Error::Config(_))
    ));
}

// Redisson (Natives): testAddAll, testGroupedRange
#[tokio::test]
async fn redisson_natives_add_all_and_grouped_range() {
    let client = redis810_client().await;
    let (a, b) = (unique("s"), unique("s"));
    for n in [&a, &b] {
        client.time_series_native(n.clone()).create().await.unwrap();
    }
    let natives = client.time_series_natives();
    let added = natives
        .add_many([
            (a.clone(), vec![(1, 1.0), (2, 2.0)]),
            (b.clone(), vec![(1, 10.0)]),
        ])
        .await
        .unwrap();
    assert_eq!(added, [(a.clone(), vec![1, 2]), (b.clone(), vec![1])]);
    assert!(natives
        .add_many(Vec::<(String, Vec<(i64, f64)>)>::new())
        .await
        .unwrap()
        .is_empty());
    let keys = [a.as_str(), b.as_str()];
    let grouped = natives.grouped_range(&keys, ..).await.unwrap();
    assert_eq!(grouped.len(), 2);
    assert_eq!(grouped[0].timestamp, 1);
    assert_eq!(grouped[0].values, [1.0, 10.0]);
    assert!(grouped[1].values[1].is_nan());
    assert!(matches!(
        natives.grouped_range(&[], ..).await,
        Err(Error::Config(_))
    ));
}

#[tokio::test]
async fn natives_need_redis_8() {
    let natives = common::client().await.time_series_natives();
    assert!(matches!(
        natives.query_index(&Filter::new().eq("a", "b")).await,
        Err(Error::Unsupported(_))
    ));
}
