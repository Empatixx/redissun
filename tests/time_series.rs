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
