//! Tests of `TimeSeries`, Redisson's `RTimeSeries`.
//!
//! Ported from Redisson 4.8.0 `RedissonTimeSeriesTest` (named `redisson_<snake_case>`, `test()` is `redisson_basic`). Redisson's
//! `numeric(name)` (a `StringCodec` series) is [`numeric`] here; its default codec is `JsonCodec` here ([`ts`]).

mod common;

use common::{client, connect_with, raw_command, unique};
use redissun::{JsonCodec, Object, StringCodec, TimeSeries, TimeSeriesEntry};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

type Ts = TimeSeries<String, String, JsonCodec>;

async fn ts(name: &str) -> Ts {
    client().await.time_series(name.to_string())
}

async fn numeric(name: &str) -> TimeSeries<String, String, StringCodec> {
    client()
        .await
        .with_codec(StringCodec)
        .time_series(name.to_string())
}

fn entry(timestamp: i64, value: &str) -> TimeSeriesEntry<String, String> {
    TimeSeriesEntry::from((timestamp, value.to_string()))
}

fn labelled(timestamp: i64, value: &str, label: &str) -> TimeSeriesEntry<String, String> {
    TimeSeriesEntry::from((timestamp, value.to_string(), label.to_string()))
}

fn ttl_key(name: &str) -> String {
    format!("redissun__ts_ttl:{{{name}}}")
}

fn seq_key(name: &str) -> String {
    format!("redissun__ts_seq:{{{name}}}")
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// The integer of a RESP integer reply such as `:12\r\n`.
fn int_reply(reply: &str) -> i64 {
    reply
        .trim_start_matches(':')
        .trim()
        .parse()
        .unwrap_or_else(|_| panic!("not an integer reply: {reply:?}"))
}

async fn exists(keys: &[&str]) -> i64 {
    let mut parts = vec!["EXISTS"];
    parts.extend_from_slice(keys);
    int_reply(&raw_command(&parts).await)
}

// Review Focus 4: the keys, the member packing and the counter are Redisson's.
#[tokio::test]
async fn the_layout_is_redissons() {
    let n = unique("ts-layout");
    let t = ts(&n).await;
    assert!(t.insert(7, "v").label("l").await.unwrap());

    let members = raw_command(&["ZRANGE", &n, "0", "-1"]).await;
    assert!(members.starts_with("*1\r\n"), "{members:?}");
    assert!(
        members.contains("\u{4}\u{14}00000000000000000001"),
        "{members:?}"
    );
    assert_eq!(raw_command(&["ZCARD", &ttl_key(&n)]).await, ":1\r\n");
    assert_eq!(
        raw_command(&["HGET", &seq_key(&n), "id"]).await,
        "$1\r\n1\r\n"
    );
    assert_eq!(raw_command(&["ZRANGEBYSCORE", &n, "7", "7"]).await, members);
    t.del().await.unwrap();
}

#[tokio::test]
async fn insert_returns_true_and_get_reads_it() {
    let n = unique("ts-insert");
    let t = ts(&n).await;
    assert!(t.is_empty().await.unwrap());
    assert!(t.insert(1, "a").await.unwrap());
    assert_eq!(t.get(1).await.unwrap(), Some("a".to_string()));
    assert_eq!(t.get(2).await.unwrap(), None);
    assert_eq!(t.entry(1).await.unwrap(), Some(entry(1, "a")));
    assert_eq!(t.entry(2).await.unwrap(), None);
    assert_eq!(t.len().await.unwrap(), 1);
    assert!(!t.is_empty().await.unwrap());
    t.del().await.unwrap();
}

#[tokio::test]
async fn try_insert_keeps_the_live_entry() {
    let n = unique("ts-try");
    let t = ts(&n).await;
    assert!(t.try_insert(5, "a").await.unwrap());
    assert!(!t.try_insert(5, "b").label("l").await.unwrap());
    assert_eq!(t.entry(5).await.unwrap(), Some(entry(5, "a")));
    assert_eq!(t.len().await.unwrap(), 1);
    t.del().await.unwrap();
}

#[tokio::test]
async fn set_reports_new_and_replaces() {
    let n = unique("ts-set");
    let t = ts(&n).await;
    assert!(t.set(5, "a").await.unwrap());
    assert!(!t.set(5, "b").label("l").await.unwrap());
    assert_eq!(t.entry(5).await.unwrap(), Some(labelled(5, "b", "l")));
    assert_eq!(t.len().await.unwrap(), 1);
    t.del().await.unwrap();
}

#[tokio::test]
async fn a_zero_or_sub_millisecond_ttl_means_none() {
    let n = unique("ts-zero-ttl");
    let t = ts(&n).await;
    t.insert(1, "a").ttl(Duration::ZERO).await.unwrap();
    t.insert(2, "b")
        .ttl(Duration::from_micros(500))
        .await
        .unwrap();
    t.insert(3, "c").ttl(Duration::MAX).await.unwrap();
    let floor = now() + 99 * 365 * 86_400_000;
    assert_eq!(
        raw_command(&["ZCOUNT", &ttl_key(&n), &floor.to_string(), "+inf"]).await,
        ":3\r\n"
    );
    sleep(Duration::from_millis(50)).await;
    assert_eq!(t.len().await.unwrap(), 3);
    t.del().await.unwrap();
}

#[tokio::test]
async fn object_methods_cover_the_three_keys() {
    let n = unique("ts-object");
    let t = ts(&n).await;
    t.insert(1, "a").label("l").await.unwrap();
    assert_eq!(t.name(), n);
    assert!(t.exists().await.unwrap());

    assert!(t.expire(Duration::from_secs(10)).await.unwrap());
    for key in [n.clone(), ttl_key(&n), seq_key(&n)] {
        let ttl = int_reply(&raw_command(&["PTTL", &key]).await);
        assert!(ttl > 0, "{key} has no TTL: {ttl}");
    }
    assert!(t.ttl().await.unwrap().is_some());
    assert!(t.persist().await.unwrap());
    assert_eq!(int_reply(&raw_command(&["PTTL", &seq_key(&n)]).await), -1);

    let n2 = format!("{n}-renamed");
    t.rename(&n2).await.unwrap();
    assert_eq!(exists(&[&n, &ttl_key(&n), &seq_key(&n)]).await, 0);
    assert_eq!(exists(&[&n2, &ttl_key(&n2), &seq_key(&n2)]).await, 3);
    let renamed = ts(&n2).await;
    assert_eq!(renamed.entry(1).await.unwrap(), Some(labelled(1, "a", "l")));

    assert!(renamed.del().await.unwrap());
    assert_eq!(exists(&[&n2, &ttl_key(&n2), &seq_key(&n2)]).await, 0);
}

#[tokio::test]
async fn the_clean_up_removes_expired_entries_and_the_counter() {
    let n = unique("ts-clean-up");
    let client = connect_with(|builder| builder.eviction_interval(Duration::from_secs(1))).await;
    let t: Ts = client.time_series(n.clone());
    t.insert(1, "a")
        .ttl(Duration::from_millis(100))
        .await
        .unwrap();
    assert_eq!(exists(&[&n, &ttl_key(&n), &seq_key(&n)]).await, 3);
    for _ in 0..80 {
        if exists(&[&n, &ttl_key(&n), &seq_key(&n)]).await == 0 {
            return;
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("the clean-up left the keys of {n}");
}

#[tokio::test]
async fn redisson_multiple_values() {
    let n = unique("ts-multiple");
    let t = ts(&n).await;
    for _ in 0..10_000 {
        t.insert(SystemTime::now(), "my-value")
            .ttl(Duration::from_secs(60 * 86_400))
            .await
            .unwrap();
    }
    assert_eq!(t.len().await.unwrap(), 10_000);
    t.del().await.unwrap();
}

#[tokio::test]
async fn redisson_cleanup() {
    let n = unique("ts-cleanup");
    let t = ts(&n).await;
    t.insert(1, "10").ttl(Duration::from_secs(1)).await.unwrap();
    sleep(Duration::from_millis(6_500)).await;
    assert_eq!(exists(&[&n, &ttl_key(&n), &seq_key(&n)]).await, 0);
}

#[tokio::test]
async fn redisson_get_entry() {
    let n = unique("ts-get-entry");
    let t = ts(&n).await;
    t.insert(1, "10").await.unwrap();
    t.insert(2, "10").await.unwrap();
    t.insert(3, "30").await.unwrap();
    t.insert(4, "40").await.unwrap();
    assert_eq!(t.len().await.unwrap(), 4);
    assert_eq!(t.get(3).await.unwrap(), Some("30".to_string()));
    assert_eq!(t.entry(3).await.unwrap().unwrap().value, "30");
    t.del().await.unwrap();
}

#[tokio::test]
async fn redisson_label() {
    let n = unique("ts-label");
    let t = ts(&n).await;
    t.insert(1, "10").await.unwrap();
    t.insert(2, "20").label("label2").await.unwrap();
    t.insert(3, "30").label("label3").await.unwrap();
    let e = t.entry(2).await.unwrap().unwrap();
    assert_eq!(e.timestamp, 2);
    assert_eq!(e.value, "20");
    assert_eq!(e.label.as_deref(), Some("label2"));
    t.del().await.unwrap();
}

#[tokio::test]
async fn redisson_add_if_absent() {
    let n = unique("ts-add-if-absent");
    let t = ts(&n).await;
    assert!(t.try_insert(5, "a").await.unwrap());
    assert!(!t.try_insert(5, "b").await.unwrap());
    assert_eq!(t.get(5).await.unwrap(), Some("a".to_string()));
    assert_eq!(t.len().await.unwrap(), 1);
    t.del().await.unwrap();
}

#[tokio::test]
async fn redisson_add_if_absent_treats_expired_as_absent() {
    let n = unique("ts-absent-expired");
    let t = ts(&n).await;
    assert!(t
        .try_insert(5, "old")
        .ttl(Duration::from_millis(300))
        .await
        .unwrap());
    sleep(Duration::from_millis(500)).await;
    assert_eq!(t.get(5).await.unwrap(), None);
    assert!(t.try_insert(5, "new").await.unwrap());
    assert_eq!(t.get(5).await.unwrap(), Some("new".to_string()));
    assert_eq!(t.len().await.unwrap(), 1);
    t.del().await.unwrap();
}

#[tokio::test]
async fn redisson_add_or_replace() {
    let n = unique("ts-add-or-replace");
    let t = ts(&n).await;
    assert!(t.set(5, "a").await.unwrap());
    assert!(!t.set(5, "b").await.unwrap());
    assert_eq!(t.get(5).await.unwrap(), Some("b".to_string()));
    assert_eq!(t.len().await.unwrap(), 1);
    t.del().await.unwrap();
}

#[tokio::test]
async fn redisson_empty_label_distinct_from_no_label() {
    let n = unique("ts-empty-label");
    let t = ts(&n).await;
    t.insert(1, "v1").label("lab").await.unwrap();
    t.insert(2, "v2").await.unwrap();
    t.insert(3, "v3").label("").await.unwrap();
    assert_eq!(
        t.entry(1).await.unwrap().unwrap().label.as_deref(),
        Some("lab")
    );
    assert_eq!(t.entry(2).await.unwrap().unwrap().label, None);
    assert_eq!(
        t.entry(3).await.unwrap().unwrap().label.as_deref(),
        Some("")
    );
    t.del().await.unwrap();
}

#[tokio::test]
async fn redisson_zero_time_to_live_means_no_time_to_live() {
    let n = unique("ts-zero-ttl-port");
    let t = ts(&n).await;
    assert!(t.set(5, "v").ttl(Duration::ZERO).await.unwrap());
    assert_eq!(t.get(5).await.unwrap(), Some("v".to_string()));
    assert!(!t.set(5, "replacement").ttl(Duration::ZERO).await.unwrap());
    sleep(Duration::from_millis(200)).await;
    assert_eq!(t.get(5).await.unwrap(), Some("replacement".to_string()));
    assert_eq!(t.len().await.unwrap(), 1);
    t.del().await.unwrap();
}

// A Rust `Duration` cannot be negative; the unconvertible one is `Duration::MAX`.
#[tokio::test]
async fn redisson_a_time_to_live_that_cannot_be_honoured_means_no_time_to_live() {
    let n = unique("ts-unhonoured-ttl");
    let t = ts(&n).await;
    assert!(t
        .insert(2, "unconvertible")
        .ttl(Duration::from_secs(i64::MAX as u64))
        .await
        .unwrap());
    assert!(t.insert(1, "max").ttl(Duration::MAX).await.unwrap());
    assert_eq!(t.get(2).await.unwrap(), Some("unconvertible".to_string()));
    assert_eq!(t.get(1).await.unwrap(), Some("max".to_string()));
    assert_eq!(t.len().await.unwrap(), 2);
    t.del().await.unwrap();
}

// `expireAt` and `expireIfSet` are not ported; `expire` and `persist` cover every key the same way.
#[tokio::test]
async fn redisson_expire_at_covers_every_key() {
    let n = unique("ts-expire");
    let t = ts(&n).await;
    t.insert(1, "a").label("cpu").await.unwrap();
    assert!(t.expire(Duration::from_secs(600)).await.unwrap());
    for key in [n.clone(), ttl_key(&n), seq_key(&n)] {
        assert!(int_reply(&raw_command(&["PTTL", &key]).await) > 0);
    }
    assert!(t.persist().await.unwrap());
    assert_eq!(int_reply(&raw_command(&["PTTL", &seq_key(&n)]).await), -1);
    assert_eq!(t.get(1).await.unwrap(), Some("a".to_string()));
    t.del().await.unwrap();
}

#[tokio::test]
async fn numeric_series_read_back_their_text() {
    let n = unique("ts-text");
    let t = numeric(&n).await;
    t.insert(1, "1.5").await.unwrap();
    assert_eq!(t.get(1).await.unwrap(), Some("1.5".to_string()));
    t.del().await.unwrap();
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|v| v.to_string()).collect()
}

// ---- Task 2: reads and removals by timestamp, head and tail ----

#[tokio::test]
async fn one_timestamp_holds_many_until_set_collapses_it() {
    let t = ts(&unique("ts")).await;
    for v in ["a", "b", "c"] {
        assert!(t.insert(5, v).await.unwrap());
    }
    assert_eq!(t.get_all(5).await.unwrap(), strings(&["a", "b", "c"]));
    assert_eq!(t.get(5).await.unwrap().as_deref(), Some("a"));
    assert!(!t.set(5, "z").await.unwrap());
    assert_eq!(t.get_all(5).await.unwrap(), strings(&["z"]));
}

#[tokio::test]
async fn expired_entries_are_invisible_everywhere() {
    let n = unique("ts");
    let t = ts(&n).await;
    t.insert(1, "x")
        .label("l")
        .ttl(Duration::from_millis(50))
        .await
        .unwrap();
    t.insert(2, "y").await.unwrap();
    sleep(Duration::from_millis(150)).await;
    assert_eq!(t.len().await.unwrap(), 1);
    assert_eq!(t.get(1).await.unwrap(), None);
    assert!(t.get_all(1).await.unwrap().is_empty());
    assert_eq!(t.first().await.unwrap().as_deref(), Some("y"));
    assert_eq!(t.first_timestamp().await.unwrap(), Some(2));
    assert_eq!(t.last_many(5).await.unwrap(), strings(&["y"]));
    assert_eq!(t.range(..).await.unwrap(), strings(&["y"]));
    assert!(t.labels(..).await.unwrap().is_empty());
    let after: Vec<i64> = t
        .read_after(0)
        .await
        .unwrap()
        .iter()
        .map(|e| e.timestamp)
        .collect();
    assert_eq!(after, [2]);
    assert_eq!(t.pop_first().await.unwrap().as_deref(), Some("y"));
    assert_eq!(int_reply(&raw_command(&["ZCARD", &n]).await), 1);
}

#[tokio::test]
async fn head_and_tail_of_nothing() {
    let t = ts(&unique("ts")).await;
    assert_eq!(t.first().await.unwrap(), None);
    assert_eq!(t.last().await.unwrap(), None);
    assert_eq!(t.first_entry().await.unwrap(), None);
    assert_eq!(t.last_entry().await.unwrap(), None);
    assert!(t.first_many(3).await.unwrap().is_empty());
    assert!(t.last_entries(3).await.unwrap().is_empty());
    assert_eq!(t.first_timestamp().await.unwrap(), None);
    assert_eq!(t.last_timestamp().await.unwrap(), None);
    assert_eq!(t.pop_first().await.unwrap(), None);
    assert_eq!(t.pop_last_entry().await.unwrap(), None);
    assert!(t.first_many(0).await.unwrap().is_empty());
}

#[tokio::test]
async fn first_last_and_pop_keep_ascending_order() {
    let t = ts(&unique("ts")).await;
    for i in 1..=5 {
        t.insert(i, &format!("v{i}")).await.unwrap();
    }
    assert_eq!(t.first_many(2).await.unwrap(), strings(&["v1", "v2"]));
    assert_eq!(t.last_many(2).await.unwrap(), strings(&["v4", "v5"]));
    assert_eq!(t.last_timestamp().await.unwrap(), Some(5));
    assert_eq!(t.last_entries(1).await.unwrap(), [entry(5, "v5")]);
    assert_eq!(t.pop_last_many(2).await.unwrap(), strings(&["v4", "v5"]));
    assert_eq!(t.pop_first_entries(1).await.unwrap(), [entry(1, "v1")]);
    assert_eq!(t.len().await.unwrap(), 2);
    assert_eq!(t.remove(2).await.unwrap().as_deref(), Some("v2"));
    assert_eq!(t.remove(2).await.unwrap(), None);
    assert_eq!(t.remove_entry(3).await.unwrap(), Some(entry(3, "v3")));
    assert!(t.is_empty().await.unwrap());
}

// Redisson: testDuplicatesKeepInsertionOrder
#[tokio::test]
async fn redisson_duplicates_keep_insertion_order() {
    let t = ts(&unique("ts")).await;
    for v in ["a", "b", "c"] {
        t.insert(5, v).await.unwrap();
    }
    assert_eq!(t.len().await.unwrap(), 3);
    assert_eq!(t.range(5..=5).await.unwrap(), strings(&["a", "b", "c"]));
    assert_eq!(t.get(5).await.unwrap().as_deref(), Some("a"));
    assert!(t.remove(5).await.unwrap().is_some());
    assert_eq!(t.get(5).await.unwrap().as_deref(), Some("b"));
    assert_eq!(t.range(5..=5).await.unwrap(), strings(&["b", "c"]));
}

// Redisson: testGetAllAndRemoveAll
#[tokio::test]
async fn redisson_get_all_and_remove_all() {
    let t = ts(&unique("ts")).await;
    t.insert(1, "a").label("x").await.unwrap();
    t.insert(1, "b").await.unwrap();
    t.insert(2, "c").await.unwrap();
    assert_eq!(
        t.get_all_entries(1).await.unwrap(),
        [labelled(1, "a", "x"), entry(1, "b")]
    );
    assert_eq!(t.remove_all(1).await.unwrap().len(), 2);
    assert!(t.get_all(1).await.unwrap().is_empty());
    assert_eq!(t.remove_all_entries(2).await.unwrap(), [entry(2, "c")]);
    assert!(t.is_empty().await.unwrap());
}

// Redisson: testPollSkipsExpiredWithoutConsumingThem
#[tokio::test]
async fn redisson_poll_skips_expired_without_consuming_them() {
    let t = ts(&unique("ts")).await;
    for i in 0..5 {
        t.insert(i, &format!("old{i}"))
            .ttl(Duration::from_millis(500))
            .await
            .unwrap();
    }
    sleep(Duration::from_millis(700)).await;
    t.insert(100, "a").await.unwrap();
    t.insert(101, "b").await.unwrap();
    assert_eq!(t.len().await.unwrap(), 2);
    assert_eq!(t.pop_first().await.unwrap().as_deref(), Some("a"));
    assert_eq!(t.pop_first_many(10).await.unwrap(), strings(&["b"]));
    assert_eq!(t.len().await.unwrap(), 0);
}

// ---- Task 3: ranges, label filters, remove_range, labels, read_after, iter ----

#[tokio::test]
async fn range_bounds_at_the_edges() {
    use std::ops::Bound::{Excluded, Unbounded};
    let t = ts(&unique("ts")).await;
    let big = (1i64 << 53) + 1;
    for (i, ts) in [i64::MIN, -1, 0, big, i64::MAX].into_iter().enumerate() {
        t.insert(ts, &format!("v{i}")).await.unwrap();
    }
    assert_eq!(
        t.range(..).await.unwrap(),
        strings(&["v0", "v1", "v2", "v3", "v4"])
    );
    assert_eq!(t.range(..0).await.unwrap(), strings(&["v0", "v1"]));
    assert!(t
        .range((Excluded(i64::MAX), Unbounded))
        .await
        .unwrap()
        .is_empty());
    assert!(t.range(5..5).await.unwrap().is_empty());
    let last = t.range_entries(..=i64::MAX).rev().limit(1).await.unwrap();
    assert_eq!(last.len(), 1);
    assert_eq!(last[0].timestamp, i64::MAX);
}

#[tokio::test]
async fn limit_zero_is_empty() {
    let t = ts(&unique("ts")).await;
    t.insert(1, "a").await.unwrap();
    assert!(t.range(..).limit(0).await.unwrap().is_empty());
    assert!(t.range_entries(..).limit(0).await.unwrap().is_empty());
    assert_eq!(t.range(..).limit(1).await.unwrap(), strings(&["a"]));
}

#[tokio::test]
async fn label_filters() {
    let t = ts(&unique("ts")).await;
    t.insert(1, "a").label("cpu").await.unwrap();
    t.insert(2, "b").label("mem").await.unwrap();
    t.insert(3, "c").await.unwrap();
    assert_eq!(t.range(..).label("cpu").await.unwrap(), strings(&["a"]));
    assert_eq!(t.range(..).unlabelled().await.unwrap(), strings(&["c"]));
    assert_eq!(
        t.range(..).unlabelled().label("mem").await.unwrap(),
        strings(&["b"])
    );
    assert_eq!(
        t.range(..).label("mem").unlabelled().await.unwrap(),
        strings(&["c"])
    );
}

#[tokio::test]
async fn iter_pages_through_everything() {
    use futures::TryStreamExt;
    let t = ts(&unique("ts")).await;
    let expected: Vec<String> = (0..25).map(|i| format!("v{i:02}")).collect();
    for (i, v) in expected.iter().enumerate() {
        t.insert(i as i64, v).await.unwrap();
    }
    let all: Vec<String> = t.iter_with(3).try_collect().await.unwrap();
    assert_eq!(all, expected);
    let default: Vec<String> = t.iter().try_collect().await.unwrap();
    assert_eq!(default, expected);
}

// Redisson: testRangeReversed
#[tokio::test]
async fn redisson_range_reversed() {
    let t = ts(&unique("ts")).await;
    for i in 1..=4 {
        t.insert(i, &format!("{}", i * 10)).await.unwrap();
    }
    assert_eq!(
        t.range(1..=4).rev().limit(2).await.unwrap(),
        strings(&["40", "30"])
    );
    assert_eq!(
        t.range(1..=4).rev().await.unwrap(),
        strings(&["40", "30", "20", "10"])
    );
    let t2 = ts(&unique("ts")).await;
    t2.insert(1, "10").await.unwrap();
    t2.insert(2, "20").await.unwrap();
    t2.insert(3, "30")
        .ttl(Duration::from_secs(1))
        .await
        .unwrap();
    t2.insert(4, "40").await.unwrap();
    sleep(Duration::from_millis(1200)).await;
    assert_eq!(
        t2.range(1..=4).rev().limit(2).await.unwrap(),
        strings(&["40", "20"])
    );
}

// Redisson: testRemoveRangeByLabel
#[tokio::test]
async fn redisson_remove_range_by_label() {
    let t = ts(&unique("ts")).await;
    t.insert(1, "a1").label("cpu").await.unwrap();
    t.insert(2, "b1").label("mem").await.unwrap();
    t.insert(3, "a2").label("cpu").await.unwrap();
    t.insert(4, "plain").await.unwrap();
    assert_eq!(t.remove_range(0..=10).label("cpu").await.unwrap(), 2);
    assert_eq!(t.range(0..=10).await.unwrap(), strings(&["b1", "plain"]));
    assert_eq!(t.remove_range(0..=10).label("nope").await.unwrap(), 0);
    assert_eq!(t.remove_range(0..=10).unlabelled().await.unwrap(), 1);
    assert_eq!(t.range(0..=10).await.unwrap(), strings(&["b1"]));
    assert_eq!(t.remove_range(..).await.unwrap(), 1);
}

// Redisson: testLabels
#[tokio::test]
async fn redisson_labels() {
    let t = ts(&unique("ts")).await;
    t.insert(1, "a1").label("cpu").await.unwrap();
    t.insert(2, "b1").label("mem").await.unwrap();
    t.insert(3, "a2").label("cpu").await.unwrap();
    t.insert(4, "plain").await.unwrap();
    assert_eq!(t.labels(..).await.unwrap(), strings(&["cpu", "mem"]));
    assert_eq!(t.labels(2..=2).await.unwrap(), strings(&["mem"]));
    assert!(t.labels(4..=4).await.unwrap().is_empty());
    assert!(ts(&unique("ts")).await.labels(..).await.unwrap().is_empty());
}

// Redisson: testReadTailAdvancingACursor
#[tokio::test]
async fn redisson_read_tail_advancing_a_cursor() {
    let t = ts(&unique("ts")).await;
    let mut cursor = i64::MIN;
    let mut seen = Vec::new();
    for round in 0..5i64 {
        t.insert(round * 10, &format!("v{round}")).await.unwrap();
        for e in t.read_after(cursor).await.unwrap() {
            seen.push(e.value);
            cursor = e.timestamp;
        }
        assert!(t.read_after(cursor).await.unwrap().is_empty());
    }
    assert_eq!(seen, strings(&["v0", "v1", "v2", "v3", "v4"]));
}

// ---- Task 4: extend, comparing policies, add ----

/// Encodes every value as 8 big-endian bytes of an `f64`: not text, so the policies refuse it.
#[derive(Clone)]
struct F64BeCodec;

impl redissun::Codec for F64BeCodec {
    fn encode<T: serde::Serialize + ?Sized>(&self, value: &T) -> redissun::Result<bytes::Bytes> {
        let text =
            serde_json::to_string(value).map_err(|e| redissun::Error::Codec(e.to_string()))?;
        let number: f64 = text.parse().unwrap_or(0.0);
        Ok(bytes::Bytes::copy_from_slice(&number.to_be_bytes()))
    }

    fn decode<T: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> redissun::Result<T> {
        let raw: [u8; 8] = bytes
            .try_into()
            .map_err(|_| redissun::Error::Codec("not 8 bytes".into()))?;
        serde_json::from_str(&f64::from_be_bytes(raw).to_string())
            .map_err(|e| redissun::Error::Codec(e.to_string()))
    }
}

#[tokio::test]
async fn extend_takes_tuples_and_entries() {
    let t = ts(&unique("ts")).await;
    t.extend(vec![(1, "a".to_string()), (2, "b".to_string())])
        .await
        .unwrap();
    t.extend([labelled(3, "c", "l")]).await.unwrap();
    t.extend(Vec::<(i64, String)>::new()).await.unwrap();
    assert_eq!(t.len().await.unwrap(), 3);
    assert_eq!(t.entry(3).await.unwrap(), Some(labelled(3, "c", "l")));
    let n = unique("ts");
    let short = ts(&n).await;
    short
        .extend([(1, "x".to_string())])
        .ttl(Duration::from_millis(100))
        .await
        .unwrap();
    assert_eq!(short.len().await.unwrap(), 1);
    sleep(Duration::from_millis(200)).await;
    assert_eq!(short.len().await.unwrap(), 0);
}

#[tokio::test]
async fn try_insert_over_an_expired_entry_is_true() {
    let t = ts(&unique("ts")).await;
    t.insert(1, "old")
        .ttl(Duration::from_millis(50))
        .await
        .unwrap();
    sleep(Duration::from_millis(150)).await;
    assert!(t.try_insert(1, "new").await.unwrap());
    assert_eq!(t.get(1).await.unwrap().as_deref(), Some("new"));
}

#[tokio::test]
async fn policies_refuse_a_binary_codec_before_sending() {
    let n = unique("ts");
    let t = client()
        .await
        .with_codec(F64BeCodec)
        .time_series::<f64, String>(n.clone());
    match t.set_sum(1, &1.0).await {
        Err(redissun::Error::Config(m)) => assert!(m.contains("encodes numbers as text"), "{m}"),
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        t.add(1, &1.0).await,
        Err(redissun::Error::Config(_))
    ));
    assert_eq!(exists(&[&n]).await, 0);
}

// Redisson: testPutAll
#[tokio::test]
async fn redisson_put_all() {
    let t = ts(&unique("ts")).await;
    let map: std::collections::BTreeMap<i64, String> =
        (1..=4).map(|i| (i, i.to_string())).collect();
    t.extend(map).await.unwrap();
    assert_eq!(t.len().await.unwrap(), 4);
}

// Redisson: testAddIfLess
#[tokio::test]
async fn redisson_add_if_less() {
    let t = numeric(&unique("ts")).await;
    assert!(t.set_if_less(5, "10").await.unwrap());
    assert!(t.set_if_less(5, "4").await.unwrap());
    assert_eq!(t.get(5).await.unwrap().as_deref(), Some("4"));
    assert!(!t.set_if_less(5, "9").await.unwrap());
    assert!(!t.set_if_less(5, "4").await.unwrap());
    assert_eq!(t.get(5).await.unwrap().as_deref(), Some("4"));
    assert_eq!(t.len().await.unwrap(), 1);
}

// Redisson: testAddIfGreater
#[tokio::test]
async fn redisson_add_if_greater() {
    let t = numeric(&unique("ts")).await;
    assert!(t.set_if_greater(5, "4").await.unwrap());
    assert!(t.set_if_greater(5, "10").await.unwrap());
    assert!(!t.set_if_greater(5, "9").await.unwrap());
    assert_eq!(t.get(5).await.unwrap().as_deref(), Some("10"));
    assert_eq!(t.len().await.unwrap(), 1);
}

// Redisson: testAddAndSum
#[tokio::test]
async fn redisson_add_and_sum() {
    let t = numeric(&unique("ts")).await;
    assert!(t.set_sum(5, "10").await.unwrap());
    assert!(!t.set_sum(5, "5").await.unwrap());
    assert_eq!(t.get(5).await.unwrap().as_deref(), Some("15"));
    assert!(!t.set_sum(5, "-15").await.unwrap());
    assert_eq!(t.get(5).await.unwrap().as_deref(), Some("0"));
    assert_eq!(t.len().await.unwrap(), 1);
}

// Redisson: testAddAndGet
#[tokio::test]
async fn redisson_add_and_get() {
    let t = numeric(&unique("ts")).await;
    assert_eq!(t.add(1, "5").await.unwrap(), 5.0);
    assert_eq!(t.add(2, "3").await.unwrap(), 8.0);
    assert_eq!(t.add(3, "-4").await.unwrap(), 4.0);
    assert_eq!(t.range(0..=10).await.unwrap(), strings(&["5", "8", "4"]));
    assert_eq!(t.len().await.unwrap(), 3);
    assert_eq!(t.add(3, "1").await.unwrap(), 5.0);
    assert_eq!(t.get_all(3).await.unwrap(), strings(&["5"]));
}

#[tokio::test]
async fn add_under_json_codec_with_numbers() {
    let t = client().await.time_series::<f64, String>(unique("ts"));
    assert_eq!(t.add(1, &1.5).await.unwrap(), 1.5);
    assert_eq!(t.add(2, &2.0).await.unwrap(), 3.5);
    assert_eq!(t.get(2).await.unwrap(), Some(3.5));
}

// ---- Task 5: aggregate and info ----

// Redisson: testAggregate
#[tokio::test]
async fn redisson_aggregate() {
    use redissun::time_series::Aggregation::*;
    let t = numeric(&unique("ts")).await;
    for (ts, v) in [(0, "1"), (10, "3"), (20, "5"), (100, "10"), (150, "20")] {
        t.insert(ts, v).await.unwrap();
    }
    let buckets = t
        .aggregate(
            0..=200,
            Duration::from_millis(100),
            &[Count, Sum, Avg, Min, Max, ValueRange, First, Last],
        )
        .await
        .unwrap();
    assert_eq!(buckets.len(), 2);
    let first = &buckets[0];
    assert_eq!(first.timestamp, 0);
    assert_eq!(first.get(Count), Some(3.0));
    assert_eq!(first.get(Sum), Some(9.0));
    assert_eq!(first.get(Avg), Some(3.0));
    assert_eq!(first.get(Min), Some(1.0));
    assert_eq!(first.get(Max), Some(5.0));
    assert_eq!(first.get(ValueRange), Some(4.0));
    assert_eq!(first.get(First), Some(1.0));
    assert_eq!(first.get(Last), Some(5.0));
    assert_eq!(buckets[1].timestamp, 100);
    assert_eq!(buckets[1].get(Count), Some(2.0));
    assert_eq!(buckets[1].get(Avg), Some(15.0));
}

// Redisson: testAggregateAlignment
#[tokio::test]
async fn redisson_aggregate_alignment() {
    use redissun::time_series::Aggregation::Count;
    let t = numeric(&unique("ts")).await;
    for ts in (0..100).step_by(10) {
        t.insert(ts as i64, &ts.to_string()).await.unwrap();
    }
    let starts = |b: Vec<redissun::time_series::TimeSeriesBucket>| {
        b.iter().map(|b| b.timestamp).collect::<Vec<_>>()
    };
    assert_eq!(
        starts(
            t.aggregate(0..=100, Duration::from_millis(50), &[Count])
                .await
                .unwrap()
        ),
        [0, 50]
    );
    assert_eq!(
        starts(
            t.aggregate(0..=100, Duration::from_millis(50), &[Count])
                .align_to(25)
                .await
                .unwrap()
        ),
        [-25, 25, 75]
    );
}

#[tokio::test]
async fn aggregate_argument_validation() {
    use redissun::time_series::Aggregation::Count;
    let t = numeric(&unique("ts")).await;
    assert!(matches!(
        t.aggregate(.., Duration::from_micros(500), &[Count]).await,
        Err(redissun::Error::Config(_))
    ));
    assert!(matches!(
        t.aggregate(.., Duration::from_millis(1), &[]).await,
        Err(redissun::Error::Config(_))
    ));
    assert!(t
        .aggregate(.., Duration::from_millis(1), &[Count])
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn aggregate_and_info_ignore_expired_entries() {
    use redissun::time_series::Aggregation::Count;
    let t = numeric(&unique("ts")).await;
    t.insert(1, "1")
        .ttl(Duration::from_millis(50))
        .await
        .unwrap();
    t.insert(2, "2").await.unwrap();
    sleep(Duration::from_millis(150)).await;
    let buckets = t
        .aggregate(.., Duration::from_millis(1000), &[Count])
        .await
        .unwrap();
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0].get(Count), Some(1.0));
    let info = t.info().await.unwrap();
    assert_eq!(info.len, 1);
    assert_eq!(info.first_timestamp, Some(2));
}

// Redisson: testInfo
#[tokio::test]
async fn redisson_info() {
    let t = ts(&unique("ts")).await;
    let empty = t.info().await.unwrap();
    assert_eq!(empty.len, 0);
    assert_eq!(empty.total_entries, 0);
    assert_eq!(empty.first_timestamp, None);
    assert_eq!(empty.last_timestamp, None);
    assert_eq!(empty.entries_issued, 0);
    assert_eq!(empty.ttl, None);
    for (ts, v) in [(30, "c"), (10, "a"), (20, "b")] {
        t.insert(ts, v).await.unwrap();
    }
    let info = t.info().await.unwrap();
    assert_eq!(info.len, 3);
    assert_eq!(info.total_entries, 3);
    assert_eq!(info.first_timestamp, Some(10));
    assert_eq!(info.last_timestamp, Some(30));
    assert!(info.memory_usage > 0);
    assert_eq!(info.ttl, None);
    assert_eq!(info.entries_issued, 3);
    assert_eq!(info.len, t.len().await.unwrap());
    assert_eq!(info.first_timestamp, t.first_timestamp().await.unwrap());
}

// ---- Task 6: events and cluster ----

async fn next_event(
    events: &mut redissun::time_series::TimeSeriesEvents,
) -> redissun::TimeSeriesEvent {
    tokio::time::timeout(Duration::from_secs(3), events.recv())
        .await
        .expect("no event within 3 s")
        .unwrap()
}

#[tokio::test]
async fn events_report_add_remove_delete_expire() {
    use redissun::TimeSeriesEvent::*;
    let (_container, url) = common::own_redis().await;
    common::raw_command_at(&url, &["CONFIG", "SET", "notify-keyspace-events", "Egzx"]).await;
    let client = redissun::Client::builder()
        .url(url.clone())
        .build()
        .await
        .unwrap();
    let n = unique("ts");
    let t: Ts = client.time_series(n.clone());
    let other: Ts = client.time_series(unique("ts"));
    let mut events = t.events().await.unwrap();
    other.insert(1, "x").await.unwrap();
    t.insert(1, "a").await.unwrap();
    assert_eq!(next_event(&mut events).await, Added);
    t.remove(1).await.unwrap();
    assert_eq!(next_event(&mut events).await, Removed);
    t.insert(2, "b").await.unwrap();
    assert_eq!(next_event(&mut events).await, Added);
    assert!(t.expire(Duration::from_millis(100)).await.unwrap());
    assert_eq!(next_event(&mut events).await, Expired);
    t.insert(3, "c").await.unwrap();
    assert_eq!(next_event(&mut events).await, Added);
    assert!(t.del().await.unwrap());
    assert_eq!(next_event(&mut events).await, Deleted);
}

#[tokio::test]
async fn no_kinds_is_a_config_error() {
    let t = ts(&unique("ts")).await;
    assert!(matches!(
        t.events_of(&[]).await,
        Err(redissun::Error::Config(_))
    ));
}

#[tokio::test]
async fn every_operation_runs_on_a_cluster() {
    use futures::TryStreamExt;
    use redissun::time_series::Aggregation::Count;
    let topology = common::topology::Topology::cluster().await;
    let client = redissun::Client::builder()
        .url(topology.cluster_url())
        .eviction_interval(Duration::from_secs(1))
        .build()
        .await
        .expect("could not connect to the cluster");
    for name in [format!("{{{}}}:ts", unique("c")), unique("plain")] {
        let t: TimeSeries<String, String, StringCodec> =
            client.with_codec(StringCodec).time_series(name.clone());
        t.insert(1, "1")
            .label("l")
            .ttl(Duration::from_secs(60))
            .await
            .unwrap();
        assert!(t.try_insert(2, "2").await.unwrap());
        t.set(3, "3").await.unwrap();
        t.set_sum(3, "4").await.unwrap();
        t.add(4, "5").await.unwrap();
        t.extend([(5, "6".to_string())]).await.unwrap();
        assert_eq!(t.get_all(3).await.unwrap(), strings(&["7"]), "{name}");
        assert_eq!(t.range_entries(..).rev().await.unwrap().len(), 5, "{name}");
        assert_eq!(t.labels(..).await.unwrap(), strings(&["l"]), "{name}");
        assert_eq!(
            t.aggregate(.., Duration::from_secs(1), &[Count])
                .await
                .unwrap()
                .len(),
            1,
            "{name}"
        );
        assert_eq!(t.info().await.unwrap().len, 5, "{name}");
        assert_eq!(t.read_after(3).await.unwrap().len(), 2, "{name}");
        assert_eq!(t.pop_first().await.unwrap().as_deref(), Some("1"), "{name}");
        assert_eq!(t.remove_range(2..=2).await.unwrap(), 1, "{name}");
        let all: Vec<String> = t.iter().try_collect().await.unwrap();
        assert_eq!(all.len(), 3, "{name}");
        t.insert(10, "short")
            .ttl(Duration::from_millis(100))
            .await
            .unwrap();
        let ttl_key = if name.contains('{') {
            format!("redissun__ts_ttl:{name}")
        } else {
            ttl_key(&name)
        };
        let port = topology.cluster_master_of(&name).await;
        let mut gone = false;
        for _ in 0..50 {
            sleep(Duration::from_millis(200)).await;
            if int_reply(&topology.cli(port, &["ZCARD", &ttl_key]).await) == 3 {
                gone = true;
                break;
            }
        }
        assert!(
            gone,
            "{name}: the clean-up did not remove the expired entry"
        );
        assert!(t.expire(Duration::from_secs(60)).await.unwrap());
        let renamed = if name.starts_with('{') {
            format!("{}:renamed", &name[..name.find('}').unwrap() + 1])
        } else {
            format!("{{{name}}}")
        };
        if name.starts_with('{') {
            t.rename(&renamed).await.unwrap();
            let moved: TimeSeries<String, String, StringCodec> =
                client.with_codec(StringCodec).time_series(renamed.clone());
            assert_eq!(moved.len().await.unwrap(), 3, "{name}");
            assert!(moved.del().await.unwrap());
        } else {
            assert!(t.del().await.unwrap());
        }
    }
}
