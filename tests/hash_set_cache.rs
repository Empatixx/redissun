mod common;

use common::{client, connect_with, raw_command, unique};
use futures::{StreamExt, TryStreamExt};
use redissun::{HashSetCache, JsonCodec, Object};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::time::{sleep, Instant};

async fn all<V>(set: &HashSetCache<V, JsonCodec>) -> HashSet<V>
where
    V: Serialize + serde::de::DeserializeOwned + Send + Sync + Eq + std::hash::Hash,
{
    set.iter().try_collect().await.unwrap()
}

async fn eventually<F, Fut>(what: &str, within: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + within;
    while !check().await {
        assert!(Instant::now() < deadline, "{what}");
        sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn insert_reports_whether_the_value_was_new() {
    let set = client().await.hash_set_cache::<String>(unique("sc"));
    assert!(set.insert("a").await.unwrap());
    assert!(!set.insert("a").await.unwrap());
    assert!(set.contains("a").await.unwrap());
    assert_eq!(set.len().await.unwrap(), 1);
}

#[tokio::test]
async fn a_value_with_a_ttl_disappears_when_it_runs_out() {
    let set = client().await.hash_set_cache::<String>(unique("sc"));
    assert!(set
        .insert("a")
        .ttl(Duration::from_millis(300))
        .await
        .unwrap());
    set.insert("b").await.unwrap();
    assert!(set.contains("a").await.unwrap());
    sleep(Duration::from_millis(500)).await;
    assert!(!set.contains("a").await.unwrap());
    assert!(set.contains("b").await.unwrap());
    assert_eq!(set.len().await.unwrap(), 1);
    assert!(set.insert("a").await.unwrap());
}

#[tokio::test]
async fn inserting_again_replaces_the_ttl() {
    let set = client().await.hash_set_cache::<String>(unique("sc"));
    set.insert("a")
        .ttl(Duration::from_millis(300))
        .await
        .unwrap();
    assert!(!set.insert("a").await.unwrap());
    sleep(Duration::from_millis(500)).await;
    assert!(set.contains("a").await.unwrap());
    assert_eq!(set.entry_ttl("a").await.unwrap(), None);
}

#[tokio::test]
async fn insert_nx_keeps_a_live_value_and_its_ttl() {
    let set = client().await.hash_set_cache::<String>(unique("sc"));
    assert!(set
        .insert_nx("a")
        .ttl(Duration::from_secs(60))
        .await
        .unwrap());
    assert!(!set.insert_nx("a").await.unwrap());
    let left = set.entry_ttl("a").await.unwrap().unwrap();
    assert!(left > Duration::from_secs(50));
}

#[tokio::test]
async fn insert_nx_replaces_an_expired_value() {
    let set = client().await.hash_set_cache::<String>(unique("sc"));
    set.insert("a")
        .ttl(Duration::from_millis(200))
        .await
        .unwrap();
    sleep(Duration::from_millis(400)).await;
    assert!(set.insert_nx("a").await.unwrap());
    assert!(set.contains("a").await.unwrap());
}

#[tokio::test]
async fn remove_also_reports_a_stored_expired_value() {
    let set = client().await.hash_set_cache::<String>(unique("sc"));
    set.insert("a").await.unwrap();
    set.insert("b")
        .ttl(Duration::from_millis(200))
        .await
        .unwrap();
    assert!(set.remove("a").await.unwrap());
    assert!(!set.remove("a").await.unwrap());
    sleep(Duration::from_millis(400)).await;
    assert!(set.remove("b").await.unwrap());
    assert!(!set.remove("b").await.unwrap());
}

#[tokio::test]
async fn a_value_without_a_limit_is_scored_like_in_redisson() {
    let name = unique("sc");
    let set = client().await.hash_set_cache::<String>(name.clone());
    set.insert("a").await.unwrap();
    set.insert("b").ttl(Duration::ZERO).await.unwrap();
    for member in ["\"a\"", "\"b\""] {
        let reply = raw_command(&["ZSCORE", &format!("{{{name}}}"), member]).await;
        let score: f64 = reply.lines().nth(1).unwrap().parse().unwrap();
        assert!(score > 92_230_000_000_000_000.0 && score < 92_233_720_368_547_758.0);
    }
    assert_eq!(set.entry_ttl("b").await.unwrap(), None);
}

#[tokio::test]
async fn entry_ttl_reports_the_time_left() {
    let set = client().await.hash_set_cache::<String>(unique("sc"));
    assert_eq!(set.entry_ttl("missing").await.unwrap(), None);
    set.insert("a").ttl(Duration::from_secs(10)).await.unwrap();
    let left = set.entry_ttl("a").await.unwrap().unwrap();
    assert!(left > Duration::from_secs(8) && left <= Duration::from_secs(10));
}

#[tokio::test]
async fn evict_expired_deletes_and_counts() {
    let name = unique("sc");
    let set = client().await.hash_set_cache::<String>(name.clone());
    for value in ["a", "b", "c"] {
        set.insert(value)
            .ttl(Duration::from_millis(200))
            .await
            .unwrap();
    }
    set.insert("keep").await.unwrap();
    sleep(Duration::from_millis(400)).await;
    assert_eq!(set.evict_expired().await.unwrap(), 3);
    assert_eq!(set.evict_expired().await.unwrap(), 0);
    let raw = raw_command(&["ZCARD", &format!("{{{name}}}")]).await;
    assert_eq!(raw.trim(), ":1");
}

#[tokio::test]
async fn the_background_task_removes_expired_values() {
    let client =
        connect_with(|builder| builder.eviction_interval(Duration::from_millis(100))).await;
    let name = unique("sc");
    let set = client.hash_set_cache::<String>(name.clone());
    set.insert("a")
        .ttl(Duration::from_millis(200))
        .await
        .unwrap();
    sleep(Duration::from_millis(1200)).await;
    let raw = raw_command(&["ZCARD", &format!("{{{name}}}")]).await;
    assert_eq!(raw.trim(), ":0");
}

#[tokio::test]
async fn iter_skips_expired_values_and_walks_pages() {
    let set = client().await.hash_set_cache::<u32>(unique("sc"));
    for value in 0..250u32 {
        set.insert(&value).await.unwrap();
    }
    set.insert(&1000)
        .ttl(Duration::from_millis(100))
        .await
        .unwrap();
    sleep(Duration::from_millis(300)).await;
    let mut seen: Vec<u32> = set
        .iter()
        .map(|item| item.unwrap())
        .collect::<Vec<_>>()
        .await;
    seen.sort_unstable();
    assert_eq!(seen, (0..250).collect::<Vec<_>>());
}

#[tokio::test]
async fn clear_and_object_methods() {
    let set = client().await.hash_set_cache::<String>(unique("sc"));
    set.insert("a").await.unwrap();
    assert!(set.exists().await.unwrap());
    set.clear().await.unwrap();
    assert!(set.is_empty().await.unwrap());
    set.insert("b").await.unwrap();
    assert!(set.del().await.unwrap());
}

#[tokio::test]
async fn steady_inserts_do_not_postpone_the_cleanup() {
    let client =
        connect_with(|builder| builder.eviction_interval(Duration::from_millis(300))).await;
    let name = unique("sc");
    let set = client.hash_set_cache::<u32>(name.clone());
    for value in 0..30u32 {
        set.insert(&value)
            .ttl(Duration::from_millis(50))
            .await
            .unwrap();
        sleep(Duration::from_millis(100)).await;
    }
    let raw = raw_command(&["ZCARD", &format!("{{{name}}}")]).await;
    let left: i64 = raw.trim().trim_start_matches(':').parse().unwrap();
    assert!(left < 10, "{left} expired values were never deleted");
}

#[tokio::test]
async fn the_clean_up_publishes_the_expired_values() {
    let set = client().await.hash_set_cache::<u32>(unique("sc"));
    let mut expired = set.expired().await.unwrap();
    set.insert(&7)
        .ttl(Duration::from_millis(100))
        .await
        .unwrap();
    sleep(Duration::from_millis(250)).await;
    assert_eq!(set.evict_expired().await.unwrap(), 1);
    let value = tokio::time::timeout(Duration::from_secs(3), expired.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, 7);
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
struct TestObject {
    name: String,
    value: String,
}

fn object(name: &str, value: &str) -> TestObject {
    TestObject {
        name: name.into(),
        value: value.into(),
    }
}

// Redisson: testAddIfAbsent
#[tokio::test]
async fn insert_nx_with_a_ttl_accepts_the_value_after_expiry() {
    let set = client().await.hash_set_cache::<String>(unique("test"));
    let ttl = Duration::from_millis(500);
    assert!(set.insert_nx("200").ttl(ttl).await.unwrap());
    assert!(!set.insert_nx("200").ttl(ttl).await.unwrap());
    assert!(set.contains("200").await.unwrap());
    assert!(set.insert_nx("100").ttl(ttl).await.unwrap());
    assert!(!set.insert_nx("100").ttl(ttl).await.unwrap());
    assert!(set.contains("100").await.unwrap());
    sleep(ttl + Duration::from_millis(50)).await;
    assert!(!set.contains("100").await.unwrap());
    assert!(!set.contains("200").await.unwrap());
    assert!(set.insert_nx("100").ttl(ttl).await.unwrap());
    assert!(set.insert_nx("200").ttl(ttl).await.unwrap());
}

// Redisson: testTryAdd
#[tokio::test]
async fn insert_nx_refuses_a_value_already_in_the_set() {
    let set = client().await.hash_set_cache::<String>(unique("list"));
    for i in 0..200 {
        assert!(set.insert_nx(&format!("name{i}")).await.unwrap());
    }
    assert_eq!(set.len().await.unwrap(), 200);
    assert!(!set.insert_nx("name10").await.unwrap());
    assert_eq!(set.len().await.unwrap(), 200);
}

// Redisson: testDelete
#[tokio::test]
async fn del_reports_whether_the_set_existed() {
    let set = client().await.hash_set_cache::<i32>(unique("set"));
    assert!(!set.del().await.unwrap());
    set.insert(&1).ttl(Duration::from_secs(1)).await.unwrap();
    assert!(set.del().await.unwrap());
    assert!(!set.del().await.unwrap());
}

// Redisson: testEmptyReadAll
#[tokio::test]
async fn iterating_an_empty_set_yields_nothing() {
    let set = client().await.hash_set_cache::<i32>(unique("set"));
    assert!(all(&set).await.is_empty());
}

// Redisson: testAddBigBean
#[tokio::test]
async fn a_large_map_can_be_stored_as_a_value() {
    let set = client()
        .await
        .hash_set_cache::<HashMap<i32, i32>>(unique("simple"));
    let mut map: HashMap<i32, i32> = (0..150).map(|i| (i, i)).collect();
    set.insert(&map).await.unwrap();
    map.remove(&0);
    set.insert(&map).await.unwrap();
    let first = Box::pin(set.iter()).next().await.unwrap().unwrap();
    assert!(first.len() >= 149);
}

// Redisson: testAddBean
#[tokio::test]
async fn a_struct_value_round_trips() {
    let set = client()
        .await
        .hash_set_cache::<TestObject>(unique("simple"));
    assert!(set.insert(&object("1", "2")).await.unwrap());
    let first = Box::pin(set.iter()).next().await.unwrap().unwrap();
    assert_eq!(first, object("1", "2"));
}

// Redisson: testAddExpire
#[tokio::test]
async fn an_expired_value_can_be_inserted_again() {
    let set = client().await.hash_set_cache::<String>(unique("simple3"));
    assert!(set
        .insert("123")
        .ttl(Duration::from_millis(500))
        .await
        .unwrap());
    assert!(all(&set).await.contains("123"));
    sleep(Duration::from_millis(550)).await;
    assert!(!set.contains("123").await.unwrap());
    assert!(set.insert("123").ttl(Duration::from_secs(1)).await.unwrap());
}

// Redisson: testAddOverrideExpiration
#[tokio::test]
async fn inserting_a_live_value_with_a_longer_ttl_extends_it() {
    let set = client().await.hash_set_cache::<String>(unique("simple31"));
    assert!(set
        .insert("123")
        .ttl(Duration::from_millis(500))
        .await
        .unwrap());
    sleep(Duration::from_millis(400)).await;
    assert!(!set.insert("123").ttl(Duration::from_secs(3)).await.unwrap());
    sleep(Duration::from_millis(800)).await;
    assert!(set.contains("123").await.unwrap());
}

// Redisson: testAddExpireTwise
#[tokio::test]
async fn two_values_with_a_ttl_each_expire_on_time() {
    let set = client().await.hash_set_cache::<String>(unique("simple31"));
    let ttl = Duration::from_millis(400);
    assert!(set.insert("123").ttl(ttl).await.unwrap());
    sleep(ttl + Duration::from_millis(50)).await;
    assert!(!set.contains("123").await.unwrap());
    assert!(set.insert("4341").ttl(ttl).await.unwrap());
    sleep(ttl + Duration::from_millis(50)).await;
    assert!(!set.contains("4341").await.unwrap());
}

// Redisson: testAddExpireThenAdd
#[tokio::test]
async fn a_value_inserted_again_without_a_ttl_after_expiry_stays() {
    let set = client().await.hash_set_cache::<String>(unique("simple31"));
    assert!(set
        .insert("123")
        .ttl(Duration::from_millis(500))
        .await
        .unwrap());
    assert_eq!(set.len().await.unwrap(), 1);
    sleep(Duration::from_millis(550)).await;
    assert_eq!(set.len().await.unwrap(), 0);
    assert!(!set.contains("123").await.unwrap());
    assert!(set.insert("123").await.unwrap());
    sleep(Duration::from_millis(600)).await;
    assert!(set.contains("123").await.unwrap());
}

// Redisson: testExpireOverwrite
#[tokio::test]
async fn inserting_a_live_value_again_restarts_its_ttl() {
    let set = client().await.hash_set_cache::<String>(unique("simple"));
    let ttl = Duration::from_millis(500);
    assert!(set.insert("123").ttl(ttl).await.unwrap());
    sleep(Duration::from_millis(400)).await;
    assert!(!set.insert("123").ttl(ttl).await.unwrap());
    sleep(Duration::from_millis(50)).await;
    assert!(set.contains("123").await.unwrap());
    sleep(Duration::from_millis(150)).await;
    assert!(set.contains("123").await.unwrap());
}

// Redisson: testRemove
#[tokio::test]
async fn remove_takes_out_only_the_given_value_and_reports_it() {
    let set = client().await.hash_set_cache::<i32>(unique("simple"));
    set.insert(&1).ttl(Duration::from_secs(1)).await.unwrap();
    set.insert(&3).ttl(Duration::from_secs(2)).await.unwrap();
    set.insert(&7).ttl(Duration::from_secs(3)).await.unwrap();
    assert!(set.remove(&1).await.unwrap());
    assert!(!set.contains(&1).await.unwrap());
    assert_eq!(all(&set).await, HashSet::from([3, 7]));
    assert!(!set.remove(&1).await.unwrap());
    assert_eq!(all(&set).await, HashSet::from([3, 7]));
    assert!(set.remove(&3).await.unwrap());
    assert!(!set.contains(&3).await.unwrap());
    assert_eq!(all(&set).await, HashSet::from([7]));
    assert_eq!(set.len().await.unwrap(), 1);
}

// Redisson: testIteratorSequence
#[tokio::test]
async fn iter_yields_every_one_of_a_thousand_values() {
    let set = client().await.hash_set_cache::<i64>(unique("set"));
    for i in 0..1000i64 {
        set.insert(&i).await.unwrap();
    }
    assert_eq!(all(&set).await, (0..1000).collect::<HashSet<i64>>());
}

// Redisson: testContains
#[tokio::test]
async fn contains_is_false_for_expired_and_missing_values() {
    let set = client().await.hash_set_cache::<TestObject>(unique("set"));
    set.insert(&object("1", "2")).await.unwrap();
    set.insert(&object("1", "2")).await.unwrap();
    set.insert(&object("2", "3"))
        .ttl(Duration::from_millis(300))
        .await
        .unwrap();
    set.insert(&object("3", "4")).await.unwrap();
    set.insert(&object("5", "6")).await.unwrap();
    sleep(Duration::from_millis(350)).await;
    assert!(!set.contains(&object("2", "3")).await.unwrap());
    assert!(set.contains(&object("1", "2")).await.unwrap());
    assert!(!set.contains(&object("1", "9")).await.unwrap());
}

// Redisson: testDuplicates
#[tokio::test]
async fn equal_values_are_stored_once() {
    let set = client().await.hash_set_cache::<TestObject>(unique("set"));
    for (name, value) in [("1", "2"), ("1", "2"), ("2", "3"), ("3", "4"), ("5", "6")] {
        set.insert(&object(name, value)).await.unwrap();
    }
    assert_eq!(set.len().await.unwrap(), 4);
}

// Redisson: testSize
#[tokio::test]
async fn len_counts_distinct_values() {
    let set = client().await.hash_set_cache::<i32>(unique("set"));
    for value in [1, 2, 3, 3, 4, 5, 5] {
        set.insert(&value).await.unwrap();
    }
    assert_eq!(set.len().await.unwrap(), 5);
}

// Redisson: testReadAllExpired
#[tokio::test]
async fn iter_yields_nothing_when_every_value_expired() {
    let set = client().await.hash_set_cache::<i32>(unique("set"));
    set.insert(&1)
        .ttl(Duration::from_millis(300))
        .await
        .unwrap();
    sleep(Duration::from_millis(350)).await;
    assert!(all(&set).await.is_empty());
}

// Redisson: testReadAll
#[tokio::test]
async fn iter_yields_values_with_and_without_a_ttl() {
    let set = client().await.hash_set_cache::<i32>(unique("set"));
    set.insert(&1).ttl(Duration::from_secs(120)).await.unwrap();
    for value in 2..=5 {
        set.insert(&value).await.unwrap();
    }
    assert_eq!(all(&set).await, HashSet::from([1, 2, 3, 4, 5]));
}

// Redisson: testExpiredIterator
#[tokio::test]
async fn iter_skips_only_the_expired_values() {
    let set = client().await.hash_set_cache::<String>(unique("simple"));
    set.insert("0").await.unwrap();
    set.insert("1")
        .ttl(Duration::from_millis(300))
        .await
        .unwrap();
    set.insert("2").ttl(Duration::from_secs(3)).await.unwrap();
    set.insert("3").ttl(Duration::from_secs(4)).await.unwrap();
    set.insert("4")
        .ttl(Duration::from_millis(300))
        .await
        .unwrap();
    sleep(Duration::from_millis(350)).await;
    assert_eq!(
        all(&set).await,
        HashSet::from(["0".to_string(), "2".to_string(), "3".to_string()])
    );
}

// Redisson: testExpire
#[tokio::test]
async fn expire_on_the_set_removes_every_value() {
    let set = client().await.hash_set_cache::<String>(unique("simple"));
    set.insert("8").ttl(Duration::from_secs(1)).await.unwrap();
    set.expire(Duration::from_millis(100)).await.unwrap();
    sleep(Duration::from_millis(500)).await;
    assert_eq!(set.len().await.unwrap(), 0);
}

// Redisson: testClearExpire
#[tokio::test]
async fn persist_cancels_the_expire_of_the_set() {
    let set = client().await.hash_set_cache::<String>(unique("simple"));
    set.insert("8").ttl(Duration::from_secs(1)).await.unwrap();
    set.expire(Duration::from_millis(100)).await.unwrap();
    set.persist().await.unwrap();
    sleep(Duration::from_millis(500)).await;
    assert_eq!(set.len().await.unwrap(), 1);
}

// Redisson: testScheduler
#[tokio::test]
async fn the_eviction_task_deletes_an_expired_value_from_redis() {
    let client =
        connect_with(|builder| builder.eviction_interval(Duration::from_millis(300))).await;
    let name = unique("simple33");
    let set = client.hash_set_cache::<String>(name.clone());
    assert!(!set.contains("33").await.unwrap());
    assert!(set
        .insert("33")
        .ttl(Duration::from_millis(500))
        .await
        .unwrap());
    eventually(
        "the value was not cleaned",
        Duration::from_secs(6),
        || async { raw_command(&["ZCARD", &format!("{{{name}}}")]).await.trim() == ":0" },
    )
    .await;
    assert_eq!(set.len().await.unwrap(), 0);
}

// Redisson: testExpiredListener
#[tokio::test]
async fn the_expired_stream_receives_every_evicted_value() {
    let client =
        connect_with(|builder| builder.eviction_interval(Duration::from_millis(200))).await;
    let set = client.hash_set_cache::<i32>(unique("test"));
    let mut expired = set.expired().await.unwrap();
    set.insert(&1)
        .ttl(Duration::from_millis(300))
        .await
        .unwrap();
    set.insert(&2)
        .ttl(Duration::from_millis(300))
        .await
        .unwrap();
    set.insert(&3).await.unwrap();
    let mut seen = HashSet::new();
    for _ in 0..2 {
        let value = tokio::time::timeout(Duration::from_secs(10), expired.recv())
            .await
            .unwrap()
            .unwrap();
        seen.insert(value);
    }
    assert_eq!(seen, HashSet::from([1, 2]));
    assert_eq!(all(&set).await, HashSet::from([3]));
    drop(expired);
    set.insert(&4)
        .ttl(Duration::from_millis(300))
        .await
        .unwrap();
    eventually("4 was not cleaned", Duration::from_secs(6), || async {
        !set.contains(&4).await.unwrap() && set.len().await.unwrap() == 1
    })
    .await;
}

// Redisson: testAddSlowEncodingValue
#[tokio::test]
async fn a_value_that_is_slow_to_encode_round_trips() {
    let map: HashMap<i32, String> = (0..80000).map(|i| (i, format!("value{i}"))).collect();
    let set = client()
        .await
        .hash_set_cache::<HashMap<i32, String>>(unique("simple"));
    assert!(set.insert(&map).await.unwrap());
    assert!(set.contains(&map).await.unwrap());
    assert_eq!(set.len().await.unwrap(), 1);
    let read: Vec<HashMap<i32, String>> = set.iter().try_collect().await.unwrap();
    assert_eq!(read, vec![map]);
}

// Redisson: testIteratorRemove
#[tokio::test]
async fn removing_values_while_iterating_skips_expired_ones() {
    let set = client().await.hash_set_cache::<String>(unique("list"));
    let ttl = Duration::from_millis(300);
    set.insert("1").await.unwrap();
    set.insert("4").ttl(ttl).await.unwrap();
    set.insert("2").await.unwrap();
    set.insert("5").ttl(ttl).await.unwrap();
    set.insert("3").await.unwrap();
    sleep(ttl + Duration::from_millis(50)).await;

    let mut values = Box::pin(set.iter());
    while let Some(value) = values.next().await {
        let value = value.unwrap();
        if value == "2" {
            assert!(set.remove(&value).await.unwrap());
        }
    }
    drop(values);
    assert_eq!(
        all(&set).await,
        HashSet::from(["1".to_string(), "3".to_string()])
    );

    let mut iteration = 0;
    let mut values = Box::pin(set.iter());
    while let Some(value) = values.next().await {
        set.remove(&value.unwrap()).await.unwrap();
        iteration += 1;
    }
    drop(values);
    assert_eq!(iteration, 2);
    assert!(!set.contains("4").await.unwrap());
    assert!(!set.contains("5").await.unwrap());
}

// Redisson: testIteratorRemoveHighVolume
#[tokio::test]
async fn removing_ten_thousand_values_while_iterating_empties_the_set() {
    let set = client().await.hash_set_cache::<i32>(unique("set"));
    let numbers: Vec<i32> = (1..=5000).collect();
    for chunk in numbers.chunks(100) {
        let set = &set;
        futures::future::try_join_all(chunk.iter().map(|i| async move {
            let scaled = i * 100_000;
            set.insert(i).await?;
            set.insert(&scaled).ttl(Duration::from_secs(20)).await
        }))
        .await
        .unwrap();
    }
    let mut count = 0;
    let mut values = Box::pin(set.iter());
    while let Some(value) = values.next().await {
        set.remove(&value.unwrap()).await.unwrap();
        count += 1;
    }
    drop(values);
    assert_eq!(count, 10000);
    assert_eq!(set.len().await.unwrap(), 0);
}

// Redisson: testToArray
#[tokio::test]
async fn collecting_iter_returns_only_the_live_values() {
    let set = client().await.hash_set_cache::<String>(unique("set"));
    set.insert("1").await.unwrap();
    set.insert("4").await.unwrap();
    set.insert("2")
        .ttl(Duration::from_millis(300))
        .await
        .unwrap();
    set.insert("5").await.unwrap();
    set.insert("3").await.unwrap();
    sleep(Duration::from_millis(450)).await;
    let expected: HashSet<String> = ["1", "4", "5", "3"].map(String::from).into();
    assert_eq!(all(&set).await, expected);
    let array: Vec<String> = set.iter().try_collect().await.unwrap();
    assert_eq!(array.len(), 4);
    assert_eq!(array.into_iter().collect::<HashSet<_>>(), expected);
}

#[tokio::test]
async fn rename_moves_the_values_to_the_set_with_the_new_name() {
    let client = client().await;
    let new_name = unique("renamed");
    let set = client.hash_set_cache::<String>(unique("set"));
    set.insert("a").await.unwrap();
    set.insert("b").ttl(Duration::from_secs(60)).await.unwrap();
    set.rename(&new_name).await.unwrap();
    assert!(!set.exists().await.unwrap());
    let renamed = client.hash_set_cache::<String>(new_name);
    assert_eq!(renamed.len().await.unwrap(), 2);
    assert!(renamed.contains("a").await.unwrap());
    assert!(renamed.contains("b").await.unwrap());
}
