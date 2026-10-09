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

#[tokio::test]
async fn test_add_if_absent() {
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

#[tokio::test]
async fn test_try_add() {
    let set = client().await.hash_set_cache::<String>(unique("list"));
    for i in 0..200 {
        assert!(set.insert_nx(&format!("name{i}")).await.unwrap());
    }
    assert_eq!(set.len().await.unwrap(), 200);
    assert!(!set.insert_nx("name10").await.unwrap());
    assert_eq!(set.len().await.unwrap(), 200);
}

#[tokio::test]
async fn test_delete() {
    let set = client().await.hash_set_cache::<i32>(unique("set"));
    assert!(!set.del().await.unwrap());
    set.insert(&1).ttl(Duration::from_secs(1)).await.unwrap();
    assert!(set.del().await.unwrap());
    assert!(!set.del().await.unwrap());
}

#[tokio::test]
async fn test_empty_read_all() {
    let set = client().await.hash_set_cache::<i32>(unique("set"));
    assert!(all(&set).await.is_empty());
}

#[tokio::test]
async fn test_add_big_bean() {
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

#[tokio::test]
async fn test_add_bean() {
    let set = client()
        .await
        .hash_set_cache::<TestObject>(unique("simple"));
    assert!(set.insert(&object("1", "2")).await.unwrap());
    let first = Box::pin(set.iter()).next().await.unwrap().unwrap();
    assert_eq!(first, object("1", "2"));
}

#[tokio::test]
async fn test_add_expire() {
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

#[tokio::test]
async fn test_add_override_expiration() {
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

#[tokio::test]
async fn test_add_expire_twise() {
    let set = client().await.hash_set_cache::<String>(unique("simple31"));
    let ttl = Duration::from_millis(400);
    assert!(set.insert("123").ttl(ttl).await.unwrap());
    sleep(ttl + Duration::from_millis(50)).await;
    assert!(!set.contains("123").await.unwrap());
    assert!(set.insert("4341").ttl(ttl).await.unwrap());
    sleep(ttl + Duration::from_millis(50)).await;
    assert!(!set.contains("4341").await.unwrap());
}

#[tokio::test]
async fn test_add_expire_then_add() {
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

#[tokio::test]
async fn test_expire_overwrite() {
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

#[tokio::test]
async fn test_remove() {
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

#[tokio::test]
async fn test_iterator_sequence() {
    let set = client().await.hash_set_cache::<i64>(unique("set"));
    for i in 0..1000i64 {
        set.insert(&i).await.unwrap();
    }
    assert_eq!(all(&set).await, (0..1000).collect::<HashSet<i64>>());
}

#[tokio::test]
async fn test_contains() {
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

#[tokio::test]
async fn test_duplicates() {
    let set = client().await.hash_set_cache::<TestObject>(unique("set"));
    for (name, value) in [("1", "2"), ("1", "2"), ("2", "3"), ("3", "4"), ("5", "6")] {
        set.insert(&object(name, value)).await.unwrap();
    }
    assert_eq!(set.len().await.unwrap(), 4);
}

#[tokio::test]
async fn test_size() {
    let set = client().await.hash_set_cache::<i32>(unique("set"));
    for value in [1, 2, 3, 3, 4, 5, 5] {
        set.insert(&value).await.unwrap();
    }
    assert_eq!(set.len().await.unwrap(), 5);
}

#[tokio::test]
async fn test_read_all_expired() {
    let set = client().await.hash_set_cache::<i32>(unique("set"));
    set.insert(&1)
        .ttl(Duration::from_millis(300))
        .await
        .unwrap();
    sleep(Duration::from_millis(350)).await;
    assert!(all(&set).await.is_empty());
}

#[tokio::test]
async fn test_read_all() {
    let set = client().await.hash_set_cache::<i32>(unique("set"));
    set.insert(&1).ttl(Duration::from_secs(120)).await.unwrap();
    for value in 2..=5 {
        set.insert(&value).await.unwrap();
    }
    assert_eq!(all(&set).await, HashSet::from([1, 2, 3, 4, 5]));
}

#[tokio::test]
async fn test_expired_iterator() {
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

#[tokio::test]
async fn test_expire() {
    let set = client().await.hash_set_cache::<String>(unique("simple"));
    set.insert("8").ttl(Duration::from_secs(1)).await.unwrap();
    set.expire(Duration::from_millis(100)).await.unwrap();
    sleep(Duration::from_millis(500)).await;
    assert_eq!(set.len().await.unwrap(), 0);
}

#[tokio::test]
async fn test_clear_expire() {
    let set = client().await.hash_set_cache::<String>(unique("simple"));
    set.insert("8").ttl(Duration::from_secs(1)).await.unwrap();
    set.expire(Duration::from_millis(100)).await.unwrap();
    set.persist().await.unwrap();
    sleep(Duration::from_millis(500)).await;
    assert_eq!(set.len().await.unwrap(), 1);
}

#[tokio::test]
async fn test_scheduler() {
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

#[tokio::test]
async fn test_expired_listener() {
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
