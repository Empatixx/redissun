mod common;

use common::topology::Topology;
use common::{client, connect_with, raw_command, unique};
use futures::TryStreamExt;
use redissun::{
    EntryEvent, EntryEventKind, EntryEvents, Error, EvictionMode, HashMapCache, JsonCodec, Object,
    StringCodec,
};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::time::{sleep, Instant};

type Cache<K, V> = HashMapCache<K, V, JsonCodec>;

async fn pause() {
    sleep(Duration::from_millis(15)).await;
}

async fn next<K, V>(events: &mut EntryEvents<K, V, JsonCodec>) -> EntryEvent<K, V>
where
    K: serde::de::DeserializeOwned + Send,
    V: serde::de::DeserializeOwned + Send,
{
    tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("no event arrived")
        .unwrap()
}

async fn nothing_arrives<K, V>(events: &mut EntryEvents<K, V, JsonCodec>)
where
    K: serde::de::DeserializeOwned + Send + std::fmt::Debug,
    V: serde::de::DeserializeOwned + Send + std::fmt::Debug,
{
    let outcome = tokio::time::timeout(Duration::from_millis(300), events.recv()).await;
    assert!(outcome.is_err(), "unexpected event {outcome:?}");
}

async fn keys<K, V>(cache: &Cache<K, V>) -> Vec<K>
where
    K: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    V: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
{
    cache.keys().try_collect().await.unwrap()
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

/// The main hash: the raw name, as in Redisson.
fn hash_key(name: &str) -> String {
    name.to_string()
}

/// A client whose clean-up runs every 100 ms, holding a handle for `name` so that it cleans that cache.
async fn cleaner(name: &str) -> (redissun::Client, Cache<String, String>) {
    let client =
        connect_with(|builder| builder.eviction_interval(Duration::from_millis(100))).await;
    let cache = client.hash_map_cache::<String, String>(name.to_string());
    (client, cache)
}

fn client_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[tokio::test]
async fn insert_get_remove_and_contains() {
    let cache = client()
        .await
        .hash_map_cache::<String, i64>(unique("cache"));
    assert_eq!(cache.insert("a", &1i64).await.unwrap(), None);
    assert_eq!(cache.insert("a", &2i64).await.unwrap(), Some(1));
    assert_eq!(cache.get("a").await.unwrap(), Some(2));
    assert!(cache.contains_key("a").await.unwrap());
    assert_eq!(cache.remove("a").await.unwrap(), Some(2));
    assert_eq!(cache.remove("a").await.unwrap(), None);
    assert!(!cache.contains_key("a").await.unwrap());
    assert_eq!(cache.get("missing").await.unwrap(), None);
}

#[tokio::test]
async fn an_entry_disappears_when_its_time_is_up() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    cache
        .insert("short", "x")
        .ttl(Duration::from_millis(300))
        .await
        .unwrap();
    cache.insert("forever", "y").await.unwrap();
    assert_eq!(cache.get("short").await.unwrap(), Some("x".to_string()));
    sleep(Duration::from_millis(500)).await;
    assert_eq!(cache.get("short").await.unwrap(), None);
    assert!(!cache.contains_key("short").await.unwrap());
    assert_eq!(cache.get("forever").await.unwrap(), Some("y".to_string()));
}

#[tokio::test]
async fn a_read_leaves_an_expired_entry_for_the_clean_up() {
    let name = unique("cache");
    let cache = client()
        .await
        .hash_map_cache::<String, String>(name.clone());
    cache
        .insert("a", "x")
        .ttl(Duration::from_millis(100))
        .await
        .unwrap();
    sleep(Duration::from_millis(250)).await;
    assert_eq!(cache.get("a").await.unwrap(), None);
    assert_eq!(cache.remove("a").await.unwrap(), None);
    assert_eq!(raw_command(&["HLEN", &hash_key(&name)]).await.trim(), ":1");
    assert_eq!(cache.len().await.unwrap(), 1);
    let _cleaner = cleaner(&name).await;
    eventually(
        "the expired entry stayed",
        Duration::from_secs(5),
        || async { cache.len().await.unwrap() == 0 },
    )
    .await;
}

#[tokio::test]
async fn entry_ttl_reports_the_time_left() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    cache
        .insert("a", "x")
        .ttl(Duration::from_secs(10))
        .await
        .unwrap();
    cache.insert("b", "y").await.unwrap();
    let left = cache.entry_ttl("a").await.unwrap().unwrap();
    assert!(left > Duration::from_secs(8) && left <= Duration::from_secs(10));
    assert_eq!(cache.entry_ttl("b").await.unwrap(), None);
    assert_eq!(cache.entry_ttl("missing").await.unwrap(), None);
}

#[tokio::test]
async fn a_plain_insert_removes_an_old_time_limit() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    cache
        .insert("a", "x")
        .ttl(Duration::from_millis(400))
        .await
        .unwrap();
    cache.insert("a", "y").await.unwrap();
    sleep(Duration::from_millis(700)).await;
    assert_eq!(cache.get("a").await.unwrap(), Some("y".to_string()));
    assert_eq!(cache.entry_ttl("a").await.unwrap(), None);
}

#[tokio::test]
async fn a_zero_ttl_means_no_time_limit() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    cache
        .insert("a", "x")
        .ttl(Duration::ZERO)
        .max_idle(Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(cache.entry_ttl("a").await.unwrap(), None);
    assert!(cache
        .try_insert("b", "y")
        .ttl(Duration::ZERO)
        .await
        .unwrap()
        .is_none());
    assert_eq!(cache.entry_ttl("b").await.unwrap(), None);
}

#[tokio::test]
async fn insert_does_not_return_an_expired_previous_value() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    cache
        .insert("a", "old")
        .ttl(Duration::from_millis(200))
        .await
        .unwrap();
    sleep(Duration::from_millis(400)).await;
    assert_eq!(cache.insert("a", "new").await.unwrap(), None);
    assert_eq!(cache.get("a").await.unwrap(), Some("new".to_string()));
}

#[tokio::test]
async fn try_insert_only_inserts_over_a_missing_or_expired_entry() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    assert_eq!(cache.try_insert("a", "1").await.unwrap(), None);
    assert_eq!(
        cache.try_insert("a", "2").await.unwrap(),
        Some("1".to_string())
    );
    assert_eq!(cache.get("a").await.unwrap(), Some("1".to_string()));
    cache
        .insert("b", "x")
        .ttl(Duration::from_millis(200))
        .await
        .unwrap();
    sleep(Duration::from_millis(400)).await;
    assert_eq!(cache.try_insert("b", "y").await.unwrap(), None);
    assert_eq!(cache.get("b").await.unwrap(), Some("y".to_string()));
}

#[tokio::test]
async fn only_one_of_many_concurrent_try_insert_wins() {
    let cache = client()
        .await
        .hash_map_cache::<String, u32>(unique("cache"));
    let tasks: Vec<_> = (0..20u32)
        .map(|i| {
            let cache = cache.clone();
            tokio::spawn(async move { cache.try_insert("k", &i).await.unwrap() })
        })
        .collect();
    let mut winners = 0;
    for task in tasks {
        if task.await.unwrap().is_none() {
            winners += 1;
        }
    }
    assert_eq!(winners, 1);
}

#[tokio::test]
async fn len_counts_expired_entries_until_the_clean_up_and_iteration_skips_them() {
    let cache = client()
        .await
        .hash_map_cache::<String, i64>(unique("cache"));
    cache.insert("keep", &1i64).await.unwrap();
    cache
        .insert("gone", &2i64)
        .ttl(Duration::from_millis(200))
        .await
        .unwrap();
    assert_eq!(cache.len().await.unwrap(), 2);
    sleep(Duration::from_millis(400)).await;
    assert_eq!(cache.len().await.unwrap(), 2);
    let entries: HashMap<String, i64> = cache.iter().try_collect().await.unwrap();
    assert_eq!(entries, HashMap::from([("keep".to_string(), 1)]));
    let keys: Vec<String> = cache.keys().try_collect().await.unwrap();
    assert_eq!(keys, ["keep"]);
    let values: Vec<i64> = cache.values().try_collect().await.unwrap();
    assert_eq!(values, [1]);
    let _cleaner = cleaner(cache.name()).await;
    eventually(
        "the expired entry stayed",
        Duration::from_secs(5),
        || async { cache.len().await.unwrap() == 1 },
    )
    .await;
    assert!(!cache.is_empty().await.unwrap());
}

#[tokio::test]
async fn iteration_reads_more_than_one_page() {
    let cache = client().await.hash_map_cache::<u32, u32>(unique("cache"));
    for i in 0..250u32 {
        cache.insert(&i, &i).await.unwrap();
    }
    let entries: HashMap<u32, u32> = cache.iter().try_collect().await.unwrap();
    assert_eq!(entries.len(), 250);
}

#[tokio::test]
async fn iteration_starts_the_idle_time_again() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    cache
        .insert("a", "x")
        .max_idle(Duration::from_millis(500))
        .await
        .unwrap();
    for _ in 0..4 {
        sleep(Duration::from_millis(250)).await;
        assert_eq!(keys(&cache).await, ["a"]);
    }
    sleep(Duration::from_millis(700)).await;
    assert!(keys(&cache).await.is_empty());
}

#[tokio::test]
async fn clear_removes_both_keys() {
    let name = unique("cache");
    let cache = client()
        .await
        .hash_map_cache::<String, String>(name.clone());
    cache
        .insert("a", "x")
        .ttl(Duration::from_secs(60))
        .await
        .unwrap();
    cache.clear().await.unwrap();
    assert!(!cache.exists().await.unwrap());
    let expires = raw_command(&["EXISTS", &format!("redissun__timeout__set:{{{name}}}")]).await;
    assert_eq!(expires.trim(), ":0");
}

#[tokio::test]
async fn object_methods_cover_both_keys() {
    let client = client().await;
    let name = unique("cache");
    let cache = client.hash_map_cache::<String, String>(name.clone());
    cache
        .insert("a", "x")
        .ttl(Duration::from_secs(60))
        .await
        .unwrap();
    assert!(cache.exists().await.unwrap());
    assert!(cache.expire(Duration::from_secs(100)).await.unwrap());
    assert!(cache.ttl().await.unwrap().is_some());
    let expires_ttl = raw_command(&["TTL", &format!("redissun__timeout__set:{{{name}}}")]).await;
    assert!(
        expires_ttl
            .trim()
            .trim_start_matches(':')
            .parse::<i64>()
            .unwrap()
            > 0
    );
    assert!(cache.persist().await.unwrap());
    let expires_ttl = raw_command(&["TTL", &format!("redissun__timeout__set:{{{name}}}")]).await;
    assert_eq!(expires_ttl.trim(), ":-1");
    assert!(cache.del().await.unwrap());
    assert!(!cache.exists().await.unwrap());
    let expires = raw_command(&["EXISTS", &format!("redissun__timeout__set:{{{name}}}")]).await;
    assert_eq!(expires.trim(), ":0");
}

#[tokio::test]
async fn debug_shows_the_name() {
    let name = unique("cache");
    let cache = client()
        .await
        .hash_map_cache::<String, String>(name.clone());
    assert!(format!("{cache:?}").contains(&name));
}

#[tokio::test]
async fn try_insert_takes_a_time_limit() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    assert!(cache
        .try_insert("a", "x")
        .ttl(Duration::from_millis(200))
        .await
        .unwrap()
        .is_none());
    assert!(cache
        .try_insert("a", "y")
        .ttl(Duration::from_millis(200))
        .await
        .unwrap()
        .is_some());
    assert!(cache.entry_ttl("a").await.unwrap().is_some());
    sleep(Duration::from_millis(400)).await;
    assert_eq!(cache.get("a").await.unwrap(), None);
}

#[tokio::test]
async fn an_idle_entry_expires_and_a_read_keeps_it_alive() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    cache
        .insert("a", "x")
        .max_idle(Duration::from_millis(500))
        .await
        .unwrap();
    for _ in 0..4 {
        sleep(Duration::from_millis(250)).await;
        assert_eq!(cache.get("a").await.unwrap(), Some("x".to_string()));
    }
    sleep(Duration::from_millis(800)).await;
    assert_eq!(cache.get("a").await.unwrap(), None);
    assert!(!cache.contains_key("a").await.unwrap());
}

#[tokio::test]
async fn contains_key_keeps_an_idle_entry_alive() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    cache
        .insert("a", "x")
        .max_idle(Duration::from_millis(500))
        .await
        .unwrap();
    for _ in 0..4 {
        sleep(Duration::from_millis(250)).await;
        assert!(cache.contains_key("a").await.unwrap());
    }
    sleep(Duration::from_millis(800)).await;
    assert!(!cache.contains_key("a").await.unwrap());
}

#[tokio::test]
async fn ttl_and_max_idle_work_together_and_the_earlier_one_wins() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    cache
        .insert("a", "x")
        .ttl(Duration::from_millis(600))
        .max_idle(Duration::from_secs(60))
        .await
        .unwrap();
    for _ in 0..2 {
        sleep(Duration::from_millis(200)).await;
        assert!(cache.get("a").await.unwrap().is_some());
    }
    sleep(Duration::from_millis(500)).await;
    assert_eq!(cache.get("a").await.unwrap(), None);
}

#[tokio::test]
async fn the_background_task_deletes_expired_entries_nobody_touches() {
    let client =
        connect_with(|builder| builder.eviction_interval(Duration::from_millis(100))).await;
    let name = unique("cache");
    let cache = client.hash_map_cache::<String, String>(name.clone());
    for i in 0..5 {
        cache
            .insert(&format!("k{i}"), "x")
            .ttl(Duration::from_millis(200))
            .await
            .unwrap();
    }
    cache.insert("keep", "y").await.unwrap();
    let key = hash_key(&name);
    eventually("expired entries stayed", Duration::from_secs(5), || async {
        raw_command(&["HLEN", &key]).await.trim() == ":1"
    })
    .await;
    let timeouts = raw_command(&["ZCARD", &format!("redissun__timeout__set:{{{name}}}")]).await;
    assert_eq!(timeouts.trim(), ":0");
    assert_eq!(cache.get("keep").await.unwrap(), Some("y".to_string()));
}

#[tokio::test]
async fn the_clean_up_takes_a_lock_so_one_client_cleans_per_run() {
    let name = unique("cache");
    let cache = client()
        .await
        .hash_map_cache::<String, String>(name.clone());
    cache
        .insert("a", "x")
        .ttl(Duration::from_millis(50))
        .await
        .unwrap();
    let latch = format!("redissun__execute_task_once_latch:{{{name}}}");
    raw_command(&["SET", &latch, "1", "PX", "60000"]).await;
    let other = connect_with(|builder| builder.eviction_interval(Duration::from_millis(100))).await;
    let _cleaner = other.hash_map_cache::<String, String>(name.clone());
    sleep(Duration::from_millis(600)).await;
    assert_eq!(raw_command(&["HLEN", &hash_key(&name)]).await.trim(), ":1");
    raw_command(&["DEL", &latch]).await;
    eventually("the clean-up never ran", Duration::from_secs(5), || async {
        raw_command(&["HLEN", &hash_key(&name)]).await.trim() == ":0"
    })
    .await;
}

#[tokio::test]
async fn a_zero_eviction_interval_is_rejected() {
    let result = redissun::Client::builder()
        .url("redis://127.0.0.1:1")
        .eviction_interval(Duration::ZERO)
        .build()
        .await;
    assert!(matches!(result, Err(Error::Config(_))));
}

#[tokio::test]
async fn a_huge_ttl_is_a_config_error() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    let result = cache.insert("a", "x").ttl(Duration::MAX).await;
    assert!(matches!(result, Err(Error::Config(_))));
}

#[tokio::test]
async fn the_background_task_also_removes_orphans_from_the_sets() {
    let name = unique("cache");
    let timeouts = format!("redissun__timeout__set:{{{name}}}");
    raw_command(&["ZADD", &timeouts, "1", "\"ghost\""]).await;
    let _cleaner = cleaner(&name).await;
    eventually("the orphan stayed", Duration::from_secs(5), || async {
        raw_command(&["ZCARD", &timeouts]).await.trim() == ":0"
    })
    .await;
}

#[tokio::test]
async fn lru_drops_the_entry_that_was_used_longest_ago() {
    let cache = client()
        .await
        .hash_map_cache::<String, u32>(unique("cache"));
    cache.set_max_size(3, EvictionMode::Lru).await.unwrap();
    for (key, value) in [("a", 1u32), ("b", 2), ("c", 3)] {
        cache.insert(key, &value).await.unwrap();
        pause().await;
    }
    assert_eq!(cache.get("a").await.unwrap(), Some(1));
    pause().await;
    cache.insert("d", &4u32).await.unwrap();
    assert_eq!(cache.len().await.unwrap(), 3);
    assert_eq!(cache.get("b").await.unwrap(), None);
    for key in ["a", "c", "d"] {
        assert!(cache.contains_key(key).await.unwrap(), "{key}");
    }
}

#[tokio::test]
async fn contains_key_counts_as_a_use_for_the_size_limit() {
    let cache = client()
        .await
        .hash_map_cache::<String, u32>(unique("cache"));
    cache.set_max_size(2, EvictionMode::Lru).await.unwrap();
    cache.insert("a", &1u32).await.unwrap();
    pause().await;
    cache.insert("b", &2u32).await.unwrap();
    pause().await;
    assert!(cache.contains_key("a").await.unwrap());
    pause().await;
    cache.insert("c", &3u32).await.unwrap();
    assert_eq!(cache.get("b").await.unwrap(), None);
    assert_eq!(cache.get("a").await.unwrap(), Some(1));
}

#[tokio::test]
async fn an_insert_with_a_time_limit_makes_room_first_even_when_it_replaces() {
    let cache = client()
        .await
        .hash_map_cache::<String, u32>(unique("cache"));
    cache.set_max_size(2, EvictionMode::Lru).await.unwrap();
    cache.insert("a", &1u32).await.unwrap();
    pause().await;
    cache.insert("b", &2u32).await.unwrap();
    pause().await;
    cache
        .insert("b", &3u32)
        .ttl(Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(cache.len().await.unwrap(), 1);
    assert_eq!(cache.get("a").await.unwrap(), None);
    assert_eq!(cache.get("b").await.unwrap(), Some(3));
}

#[tokio::test]
async fn lfu_drops_the_entry_that_was_used_least_often() {
    let cache = client()
        .await
        .hash_map_cache::<String, u32>(unique("cache"));
    cache.set_max_size(2, EvictionMode::Lfu).await.unwrap();
    cache.insert("a", &1u32).await.unwrap();
    cache.insert("b", &2u32).await.unwrap();
    for _ in 0..3 {
        cache.get("a").await.unwrap();
    }
    cache.insert("c", &3u32).await.unwrap();
    assert_eq!(cache.len().await.unwrap(), 2);
    assert_eq!(cache.get("b").await.unwrap(), None);
    assert!(cache.contains_key("a").await.unwrap());
    assert!(cache.contains_key("c").await.unwrap());
}

#[tokio::test]
async fn the_size_limit_is_shared_by_every_handle_and_zero_removes_it() {
    let client = client().await;
    let name = unique("cache");
    let first = client.hash_map_cache::<String, u32>(name.clone());
    let second = client.hash_map_cache::<String, u32>(name);
    first.set_max_size(1, EvictionMode::Lru).await.unwrap();
    first.insert("a", &1u32).await.unwrap();
    pause().await;
    second.insert("b", &2u32).await.unwrap();
    assert_eq!(first.len().await.unwrap(), 1);
    first.set_max_size(0, EvictionMode::Lru).await.unwrap();
    first.insert("c", &3u32).await.unwrap();
    first.insert("d", &4u32).await.unwrap();
    assert_eq!(first.len().await.unwrap(), 3);
}

#[tokio::test]
async fn init_max_size_only_sets_it_once() {
    let cache = client()
        .await
        .hash_map_cache::<String, u32>(unique("cache"));
    assert!(cache.init_max_size(2, EvictionMode::Lru).await.unwrap());
    assert!(!cache.init_max_size(5, EvictionMode::Lfu).await.unwrap());
    for i in 0..4u32 {
        cache.insert(&format!("k{i}"), &i).await.unwrap();
        pause().await;
    }
    assert_eq!(cache.len().await.unwrap(), 2);
}

#[tokio::test]
async fn events_report_created_updated_and_removed_entries() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    let mut events = cache.events().await.unwrap();
    cache.insert("a:1", "x:y").await.unwrap();
    assert_eq!(
        next(&mut events).await,
        EntryEvent::Created {
            key: "a:1".into(),
            value: "x:y".into()
        }
    );
    cache.insert("a:1", "z").await.unwrap();
    assert_eq!(
        next(&mut events).await,
        EntryEvent::Updated {
            key: "a:1".into(),
            value: "z".into(),
            previous: "x:y".into()
        }
    );
    cache.remove("a:1").await.unwrap();
    assert_eq!(
        next(&mut events).await,
        EntryEvent::Removed {
            key: "a:1".into(),
            value: "z".into()
        }
    );
}

#[tokio::test]
async fn events_report_expired_entries_and_size_evictions() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    let mut events = cache.events().await.unwrap();
    cache
        .insert("short", "1")
        .ttl(Duration::from_millis(150))
        .await
        .unwrap();
    assert!(matches!(
        next(&mut events).await,
        EntryEvent::Created { .. }
    ));
    sleep(Duration::from_millis(300)).await;
    let cleaner = cleaner(cache.name()).await;
    assert_eq!(
        next(&mut events).await,
        EntryEvent::Expired {
            key: "short".into(),
            value: "1".into()
        }
    );
    drop(cleaner);

    cache.set_max_size(1, EvictionMode::Lru).await.unwrap();
    cache.insert("a", "1").await.unwrap();
    assert!(matches!(
        next(&mut events).await,
        EntryEvent::Created { .. }
    ));
    pause().await;
    cache.insert("b", "2").await.unwrap();
    sleep(Duration::from_millis(100)).await;
    assert_eq!(
        next(&mut events).await,
        EntryEvent::Removed {
            key: "a".into(),
            value: "1".into()
        }
    );
    assert_eq!(
        next(&mut events).await,
        EntryEvent::Created {
            key: "b".into(),
            value: "2".into()
        }
    );
}

#[tokio::test]
async fn events_of_listens_to_the_chosen_kinds_only() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    let mut removals = cache.events_of(&[EntryEventKind::Removed]).await.unwrap();
    cache.insert("a", "1").await.unwrap();
    cache.insert("a", "2").await.unwrap();
    cache.remove("a").await.unwrap();
    assert_eq!(
        next(&mut removals).await,
        EntryEvent::Removed {
            key: "a".into(),
            value: "2".into()
        }
    );
    nothing_arrives(&mut removals).await;
    assert!(matches!(cache.events_of(&[]).await, Err(Error::Config(_))));
}

#[tokio::test]
async fn every_listener_gets_every_event_and_stopping_one_does_not_affect_the_other() {
    let client = client().await;
    let name = unique("cache");
    let writer = client.hash_map_cache::<String, String>(name.clone());
    let mut first = writer.events().await.unwrap();
    let mut second = client
        .hash_map_cache::<String, String>(name)
        .events()
        .await
        .unwrap();
    writer.insert("a", "1").await.unwrap();
    assert!(matches!(next(&mut first).await, EntryEvent::Created { .. }));
    assert!(matches!(
        next(&mut second).await,
        EntryEvent::Created { .. }
    ));
    drop(first);
    writer.insert("b", "2").await.unwrap();
    assert!(matches!(
        next(&mut second).await,
        EntryEvent::Created { .. }
    ));
}

#[tokio::test]
async fn deleting_the_cache_stops_the_events_until_somebody_listens_again() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    let mut events = cache.events().await.unwrap();
    cache.insert("a", "1").await.unwrap();
    assert!(matches!(
        next(&mut events).await,
        EntryEvent::Created { .. }
    ));
    cache.del().await.unwrap();
    cache.insert("b", "2").await.unwrap();
    nothing_arrives(&mut events).await;
    let _again = cache.events().await.unwrap();
    cache.insert("c", "3").await.unwrap();
    assert_eq!(
        next(&mut events).await,
        EntryEvent::Created {
            key: "c".into(),
            value: "3".into()
        }
    );
}

#[tokio::test]
async fn entries_written_before_the_limit_are_not_tracked() {
    let cache = client()
        .await
        .hash_map_cache::<String, u32>(unique("cache"));
    for i in 0..10u32 {
        cache.insert(&format!("k{i}"), &i).await.unwrap();
    }
    cache.set_max_size(5, EvictionMode::Lru).await.unwrap();
    pause().await;
    cache.insert("new", &99u32).await.unwrap();
    assert_eq!(cache.len().await.unwrap(), 11);
    assert!(cache.contains_key("new").await.unwrap());
}

#[tokio::test]
async fn an_expired_entry_is_dropped_first_when_it_was_used_longest_ago() {
    let cache = client()
        .await
        .hash_map_cache::<String, u32>(unique("cache"));
    cache.set_max_size(2, EvictionMode::Lru).await.unwrap();
    cache
        .insert("old", &1u32)
        .ttl(Duration::from_millis(100))
        .await
        .unwrap();
    pause().await;
    cache.insert("keep", &2u32).await.unwrap();
    sleep(Duration::from_millis(250)).await;
    cache.insert("new", &3u32).await.unwrap();
    assert_eq!(cache.len().await.unwrap(), 2);
    assert!(cache.contains_key("keep").await.unwrap());
    assert!(cache.contains_key("new").await.unwrap());
}

#[tokio::test]
async fn changing_the_mode_keeps_the_old_scores() {
    let name = unique("cache");
    let cache = client().await.hash_map_cache::<String, u32>(name.clone());
    cache.set_max_size(2, EvictionMode::Lru).await.unwrap();
    cache.insert("a", &1u32).await.unwrap();
    cache.insert("b", &2u32).await.unwrap();
    cache.set_max_size(2, EvictionMode::Lfu).await.unwrap();
    let access = format!("redissun__map_cache__last_access__set:{{{name}}}");
    assert_eq!(raw_command(&["ZCARD", &access]).await.trim(), ":2");
    cache.get("b").await.unwrap();
    cache.insert("c", &3u32).await.unwrap();
    assert_eq!(cache.len().await.unwrap(), 2);
    assert!(cache.contains_key("b").await.unwrap());
    assert_eq!(cache.get("a").await.unwrap(), None);
}

#[tokio::test]
async fn racing_init_max_size_calls_agree_on_one_winner() {
    let client = client().await;
    let name = unique("cache");
    let tasks: Vec<_> = (1..=10usize)
        .map(|n| {
            let cache = client.hash_map_cache::<String, u32>(name.clone());
            tokio::spawn(async move { cache.init_max_size(n, EvictionMode::Lru).await.unwrap() })
        })
        .collect();
    let mut winners = 0;
    for task in tasks {
        if task.await.unwrap() {
            winners += 1;
        }
    }
    assert_eq!(winners, 1);
}

#[tokio::test]
async fn clear_keeps_the_size_limit_and_del_removes_it() {
    let name = unique("cache");
    let cache = client().await.hash_map_cache::<String, u32>(name.clone());
    cache.set_max_size(1, EvictionMode::Lru).await.unwrap();
    cache.insert("a", &1u32).await.unwrap();
    cache.clear().await.unwrap();
    let options = format!("{{{name}}}:redissun_options");
    assert_eq!(raw_command(&["EXISTS", &options]).await.trim(), ":1");
    cache.del().await.unwrap();
    assert_eq!(raw_command(&["EXISTS", &options]).await.trim(), ":0");
}

// Redisson: testFastPutExpiration
#[tokio::test]
async fn an_insert_after_expiry_stores_the_new_value() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("testFastPutExpiration"));
    cache
        .insert("k1", "v1")
        .ttl(Duration::from_millis(300))
        .await
        .unwrap();
    sleep(Duration::from_millis(350)).await;
    cache.insert("k1", "v2").await.unwrap();
    assert_eq!(cache.get("k1").await.unwrap(), Some("v2".to_string()));
}

// Redisson: testRemoveListener
#[tokio::test]
async fn a_size_eviction_sends_a_removed_event() {
    let cache = client().await.hash_map_cache::<i64, String>(unique("test"));
    cache.init_max_size(5, EvictionMode::Lru).await.unwrap();
    let mut removed = cache.events_of(&[EntryEventKind::Removed]).await.unwrap();
    for i in 1..=5i64 {
        cache.insert(&i, &i.to_string()).await.unwrap();
    }
    cache.insert(&6i64, "6").await.unwrap();
    assert!(matches!(
        next(&mut removed).await,
        EntryEvent::Removed { .. }
    ));
}

// Redisson: testRemainTimeToLive
#[tokio::test]
async fn entry_ttl_reports_the_remaining_ttl_capped_by_max_idle() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("test"));
    cache
        .insert("1", "2")
        .ttl(Duration::from_secs(2))
        .await
        .unwrap();
    let left = cache.entry_ttl("1").await.unwrap().unwrap();
    assert!(left >= Duration::from_millis(1900) && left <= Duration::from_secs(2));
    cache.insert("3", "4").await.unwrap();
    assert_eq!(cache.entry_ttl("3").await.unwrap(), None);
    assert_eq!(cache.entry_ttl("0").await.unwrap(), None);
    cache
        .insert("5", "6")
        .ttl(Duration::from_secs(20))
        .max_idle(Duration::from_secs(10))
        .await
        .unwrap();
    assert!(cache.entry_ttl("5").await.unwrap().unwrap() <= Duration::from_secs(10));
}

// Redisson: testFastPutTtl
#[tokio::test]
async fn an_entry_with_a_ttl_expires_in_a_size_limited_cache() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("getAll"));
    cache.init_max_size(1, EvictionMode::Lru).await.unwrap();
    cache
        .insert("1", "3")
        .ttl(Duration::from_millis(500))
        .await
        .unwrap();
    sleep(Duration::from_millis(550)).await;
    assert_eq!(cache.get("1").await.unwrap(), None);
    cache
        .insert("1", "4")
        .ttl(Duration::from_millis(500))
        .await
        .unwrap();
    sleep(Duration::from_millis(1000)).await;
    assert_eq!(cache.get("1").await.unwrap(), None);
}

// Redisson: testExpirationWithMaxSize
#[tokio::test]
async fn the_eviction_task_removes_expired_entries_and_their_access_set() {
    let client =
        connect_with(|builder| builder.eviction_interval(Duration::from_millis(200))).await;
    let name = unique("test");
    let cache = client.hash_map_cache::<String, String>(name.clone());
    assert!(cache.init_max_size(2, EvictionMode::Lru).await.unwrap());
    cache
        .insert("1", "1")
        .ttl(Duration::from_millis(500))
        .await
        .unwrap();
    cache
        .insert("2", "2")
        .max_idle(Duration::from_millis(500))
        .await
        .unwrap();
    cache
        .insert("3", "3")
        .ttl(Duration::from_millis(500))
        .await
        .unwrap();
    cache
        .insert("4", "4")
        .max_idle(Duration::from_millis(500))
        .await
        .unwrap();
    eventually("entries stayed", Duration::from_secs(8), || async {
        cache.len().await.unwrap() == 0
    })
    .await;
    let access = format!("redissun__map_cache__last_access__set:{{{name}}}");
    assert_eq!(raw_command(&["EXISTS", &access]).await.trim(), ":0");
    let options = format!("{{{name}}}:redissun_options");
    assert_eq!(raw_command(&["EXISTS", &options]).await.trim(), ":1");
}

// Redisson: testMaxSizeLfu
#[tokio::test]
async fn lfu_evicts_the_least_read_entry_when_full() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("test"));
    cache.set_max_size(3, EvictionMode::Lfu).await.unwrap();
    cache.insert("1", "2").await.unwrap();
    cache.insert("2", "4").await.unwrap();
    cache.insert("3", "5").await.unwrap();

    assert_eq!(cache.get("1").await.unwrap().as_deref(), Some("2"));
    assert_eq!(cache.get("2").await.unwrap().as_deref(), Some("4"));
    assert_eq!(cache.get("3").await.unwrap().as_deref(), Some("5"));

    for key in ["1", "3", "1", "3", "2"] {
        cache.get(key).await.unwrap();
    }
    cache.insert("4", "7").await.unwrap();

    assert_eq!(cache.get("1").await.unwrap().as_deref(), Some("2"));
    assert_eq!(cache.get("3").await.unwrap().as_deref(), Some("5"));
    assert_eq!(cache.get("2").await.unwrap(), None);
}

// Redisson: testMaxSize
#[tokio::test]
async fn the_size_limit_keeps_the_most_recent_entries_for_every_insert_kind() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("test"));
    assert!(cache.init_max_size(2, EvictionMode::Lru).await.unwrap());
    assert!(!cache.init_max_size(1, EvictionMode::Lru).await.unwrap());

    let minute = Duration::from_secs(60);
    assert!(cache.try_insert("01", "00").await.unwrap().is_none());
    assert!(cache.try_insert("02", "00").await.unwrap().is_none());
    assert_eq!(cache.insert("03", "00").await.unwrap(), None);
    assert!(cache
        .try_insert("04", "00")
        .ttl(minute)
        .await
        .unwrap()
        .is_none());
    assert_eq!(cache.insert("1", "11").ttl(minute).await.unwrap(), None);
    assert_eq!(cache.len().await.unwrap(), 2);
    assert_eq!(cache.insert("2", "22").ttl(minute).await.unwrap(), None);
    assert_eq!(cache.insert("3", "33").ttl(minute).await.unwrap(), None);
    assert_eq!(cache.len().await.unwrap(), 2);

    assert_eq!(cache.get("2").await.unwrap().as_deref(), Some("22"));
    assert_eq!(cache.get("0").await.unwrap(), None);
    assert!(cache.try_insert("2", "3").await.unwrap().is_some());
    assert!(cache
        .try_insert("3", "4")
        .ttl(minute)
        .max_idle(minute)
        .await
        .unwrap()
        .is_some());
    assert!(cache.contains_key("2").await.unwrap());
    assert!(!cache.contains_key("0").await.unwrap());
    assert_eq!(cache.remove("2").await.unwrap().as_deref(), Some("22"));
    assert_eq!(cache.remove("0").await.unwrap(), None);
    assert_eq!(cache.remove("3").await.unwrap().as_deref(), Some("33"));

    cache.set_max_size(6, EvictionMode::Lru).await.unwrap();
    let names = |range: std::ops::RangeInclusive<u32>| -> Vec<String> {
        range.map(|i| format!("{i:02}")).collect()
    };
    for key in names(1..=7) {
        cache.insert(&key, "00").await.unwrap();
        pause().await;
    }
    assert_eq!(cache.len().await.unwrap(), 6);
    assert_eq!(keys(&cache).await, names(2..=7));

    for key in names(8..=14) {
        cache.insert(&key, "00").await.unwrap();
        pause().await;
    }
    assert_eq!(cache.len().await.unwrap(), 6);
    assert_eq!(keys(&cache).await, names(9..=14));

    for key in names(15..=21) {
        cache
            .try_insert(&key, "00")
            .ttl(Duration::from_secs(1))
            .await
            .unwrap();
        pause().await;
    }
    assert_eq!(cache.len().await.unwrap(), 6);
    assert_eq!(keys(&cache).await, names(16..=21));

    for key in names(22..=28) {
        cache.try_insert(&key, "00").await.unwrap();
        pause().await;
    }
    assert_eq!(cache.len().await.unwrap(), 6);
    assert_eq!(keys(&cache).await, names(23..=28));

    for key in names(29..=35) {
        cache
            .insert(&key, "00")
            .ttl(Duration::from_secs(1))
            .await
            .unwrap();
        pause().await;
    }
    assert_eq!(cache.len().await.unwrap(), 6);
    assert_eq!(keys(&cache).await, names(30..=35));
}

// Redisson: testCacheValues
#[tokio::test]
async fn values_returns_an_entry_stored_with_max_idle() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("testRMapCacheValues"));
    cache
        .insert("1234", "5678")
        .max_idle(Duration::from_secs(3600))
        .await
        .unwrap();
    let values: Vec<String> = cache.values().try_collect().await.unwrap();
    assert_eq!(values, ["5678"]);
}

// Redisson: testExpiredIterator
#[tokio::test]
async fn keys_skip_entries_whose_ttl_has_passed() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("simple"));
    cache.insert("0", "8").await.unwrap();
    cache
        .insert("1", "6")
        .ttl(Duration::from_millis(300))
        .await
        .unwrap();
    cache
        .insert("2", "4")
        .ttl(Duration::from_secs(3))
        .await
        .unwrap();
    cache
        .insert("3", "2")
        .ttl(Duration::from_secs(4))
        .await
        .unwrap();
    cache
        .insert("4", "4")
        .ttl(Duration::from_millis(300))
        .await
        .unwrap();
    sleep(Duration::from_millis(400)).await;
    let found: HashSet<String> = cache.keys().try_collect().await.unwrap();
    assert_eq!(
        found,
        HashSet::from(["0".to_string(), "2".to_string(), "3".to_string()])
    );
}

// Redisson: testExpire
#[tokio::test]
async fn expire_on_the_cache_removes_all_entries() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("simple"));
    cache
        .insert("0", "8")
        .ttl(Duration::from_secs(1))
        .await
        .unwrap();
    cache.expire(Duration::from_millis(100)).await.unwrap();
    sleep(Duration::from_millis(500)).await;
    assert_eq!(cache.len().await.unwrap(), 0);
}

// Redisson: testClearExpire
#[tokio::test]
async fn persist_cancels_an_earlier_expire_of_the_cache() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("simple"));
    cache
        .insert("0", "8")
        .ttl(Duration::from_secs(1))
        .await
        .unwrap();
    cache.expire(Duration::from_millis(100)).await.unwrap();
    cache.persist().await.unwrap();
    sleep(Duration::from_millis(500)).await;
    assert_eq!(cache.len().await.unwrap(), 1);
}

// Redisson: testEntrySet
#[tokio::test]
async fn iter_returns_entries_with_and_without_ttl() {
    let cache = client()
        .await
        .hash_map_cache::<i32, String>(unique("simple12"));
    cache.insert(&1, "12").await.unwrap();
    cache
        .insert(&2, "33")
        .ttl(Duration::from_secs(1))
        .await
        .unwrap();
    cache.insert(&3, "43").await.unwrap();
    let entries: HashMap<i32, String> = cache.iter().try_collect().await.unwrap();
    assert_eq!(entries.get(&1).map(String::as_str), Some("12"));
    assert_eq!(entries.get(&3).map(String::as_str), Some("43"));
    assert_eq!(cache.len().await.unwrap(), 3);
}

// Redisson: testKeySet
#[tokio::test]
async fn keys_drop_an_entry_once_its_ttl_passes() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("simple03"));
    cache
        .insert("33", "44")
        .ttl(Duration::from_millis(300))
        .await
        .unwrap();
    cache.insert("1", "2").await.unwrap();
    let found: HashSet<String> = cache.keys().try_collect().await.unwrap();
    assert!(found.contains("33"));
    assert!(!found.contains("44"));
    assert!(found.contains("1"));
    sleep(Duration::from_millis(400)).await;
    let found: HashSet<String> = cache.keys().try_collect().await.unwrap();
    assert!(!found.contains("33"));
    assert!(found.contains("1"));
}

// Redisson: testValues
#[tokio::test]
async fn values_drop_an_entry_once_its_ttl_passes() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("simple05"));
    cache
        .insert("33", "44")
        .ttl(Duration::from_millis(300))
        .await
        .unwrap();
    cache.insert("1", "2").await.unwrap();
    let found: HashSet<String> = cache.values().try_collect().await.unwrap();
    assert!(found.contains("44"));
    assert!(!found.contains("33"));
    assert!(found.contains("2"));
    sleep(Duration::from_millis(400)).await;
    let found: HashSet<String> = cache.values().try_collect().await.unwrap();
    assert!(!found.contains("44"));
    assert!(found.contains("2"));
}

// Redisson: testContainsKeyTtl
#[tokio::test]
async fn contains_key_is_false_once_the_ttl_passes() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("simple30"));
    cache
        .insert("33", "44")
        .ttl(Duration::from_millis(300))
        .await
        .unwrap();
    assert!(cache.contains_key("33").await.unwrap());
    assert!(!cache.contains_key("34").await.unwrap());
    sleep(Duration::from_millis(350)).await;
    assert!(!cache.contains_key("33").await.unwrap());
}

// Redisson: testScheduler
#[tokio::test]
async fn the_eviction_task_removes_entries_expired_by_ttl_or_idle_time() {
    let client =
        connect_with(|builder| builder.eviction_interval(Duration::from_millis(500))).await;
    let cache = client.hash_map_cache::<String, String>(unique("simple3"));
    assert_eq!(cache.get("33").await.unwrap(), None);
    cache
        .insert("33", "44")
        .ttl(Duration::from_secs(1))
        .await
        .unwrap();
    cache
        .insert("10", "32")
        .ttl(Duration::from_secs(1))
        .max_idle(Duration::from_millis(400))
        .await
        .unwrap();
    cache
        .insert("01", "92")
        .max_idle(Duration::from_millis(400))
        .await
        .unwrap();
    assert_eq!(cache.len().await.unwrap(), 3);
    eventually(
        "the scheduler left entries",
        Duration::from_secs(6),
        || async { cache.len().await.unwrap() == 0 },
    )
    .await;
}

// Redisson: testPutGetTtl
#[tokio::test]
async fn get_returns_an_entry_until_its_ttl_passes() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("simple04"));
    assert_eq!(cache.get("33").await.unwrap(), None);
    cache
        .insert("33", "44")
        .ttl(Duration::from_millis(600))
        .await
        .unwrap();
    assert_eq!(cache.get("33").await.unwrap().as_deref(), Some("44"));
    sleep(Duration::from_millis(300)).await;
    assert_eq!(cache.len().await.unwrap(), 1);
    assert_eq!(cache.get("33").await.unwrap().as_deref(), Some("44"));
    assert_eq!(cache.len().await.unwrap(), 1);
    sleep(Duration::from_millis(350)).await;
    assert_eq!(cache.get("33").await.unwrap(), None);
}

// Redisson: testPutIfAbsentTtl
#[tokio::test]
async fn try_insert_with_a_ttl_inserts_only_over_missing_or_expired_keys() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("simple"));
    cache.insert("1", "2").await.unwrap();
    assert!(cache
        .try_insert("1", "3")
        .ttl(Duration::from_millis(300))
        .await
        .unwrap()
        .is_some());
    assert_eq!(cache.get("1").await.unwrap().as_deref(), Some("2"));
    cache
        .try_insert("4", "4")
        .ttl(Duration::from_millis(300))
        .await
        .unwrap();
    assert_eq!(cache.get("4").await.unwrap().as_deref(), Some("4"));
    sleep(Duration::from_millis(300)).await;
    assert_eq!(cache.get("4").await.unwrap(), None);
    assert!(cache
        .try_insert("4", "4")
        .ttl(Duration::from_millis(300))
        .await
        .unwrap()
        .is_none());
    assert_eq!(cache.get("4").await.unwrap().as_deref(), Some("4"));
    assert!(cache
        .try_insert("2", "4")
        .ttl(Duration::from_secs(2))
        .await
        .unwrap()
        .is_none());
    assert_eq!(cache.get("2").await.unwrap().as_deref(), Some("4"));
}

// Redisson: testPutIfAbsentTtlKeepsFirstValue
#[tokio::test]
async fn try_insert_with_a_ttl_keeps_the_first_value_and_its_ttl() {
    let client =
        connect_with(|builder| builder.eviction_interval(Duration::from_millis(200))).await;
    let cache =
        client.hash_map_cache::<String, String>(unique("testPutIfAbsentTTLKeepsFirstValue"));
    let ttl = Duration::from_secs(300);
    assert!(cache
        .try_insert("key", "value-1")
        .ttl(ttl)
        .await
        .unwrap()
        .is_none());
    assert!(cache
        .try_insert("key", "value-2")
        .ttl(ttl)
        .await
        .unwrap()
        .is_some());
    sleep(Duration::from_millis(1000)).await;
    assert_eq!(cache.get("key").await.unwrap().as_deref(), Some("value-1"));
    assert!(cache
        .try_insert("key", "value-3")
        .ttl(ttl)
        .await
        .unwrap()
        .is_some());
    let left = cache.entry_ttl("key").await.unwrap().unwrap();
    assert!(left >= Duration::from_secs(290) && left <= ttl);
}

// Redisson: testFastPutIfAbsentTtl
#[tokio::test]
async fn try_insert_succeeds_once_the_existing_entry_expired() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("simple"));
    cache.insert("1", "2").await.unwrap();
    assert!(cache.try_insert("1", "3").await.unwrap().is_some());
    assert_eq!(cache.get("1").await.unwrap().as_deref(), Some("2"));
    assert!(cache.try_insert("2", "4").await.unwrap().is_none());
    assert_eq!(cache.get("2").await.unwrap().as_deref(), Some("4"));
    cache
        .insert("3", "31")
        .ttl(Duration::from_millis(500))
        .await
        .unwrap();
    assert!(cache.try_insert("3", "32").await.unwrap().is_some());
    sleep(Duration::from_millis(550)).await;
    assert!(cache.try_insert("3", "32").await.unwrap().is_none());
    assert_eq!(cache.get("3").await.unwrap().as_deref(), Some("32"));
}

// Redisson: testCreatedListener
#[tokio::test]
async fn created_events_arrive_for_every_kind_of_new_insert() {
    let cache = client().await.hash_map_cache::<i32, i32>(unique("simple"));
    let mut created = cache.events_of(&[EntryEventKind::Created]).await.unwrap();
    let ttl = Duration::from_secs(2);
    cache.insert(&1, &2).await.unwrap();
    assert_eq!(
        next(&mut created).await,
        EntryEvent::Created { key: 1, value: 2 }
    );
    cache.insert(&10, &2).ttl(ttl).await.unwrap();
    assert_eq!(
        next(&mut created).await,
        EntryEvent::Created { key: 10, value: 2 }
    );
    cache.try_insert(&4, &1).await.unwrap();
    assert_eq!(
        next(&mut created).await,
        EntryEvent::Created { key: 4, value: 1 }
    );
    cache.try_insert(&15, &2).ttl(ttl).await.unwrap();
    assert_eq!(
        next(&mut created).await,
        EntryEvent::Created { key: 15, value: 2 }
    );
    nothing_arrives(&mut created).await;
}

// Redisson: testUpdatedListener
#[tokio::test]
async fn updated_events_carry_the_new_and_previous_value() {
    let cache = client().await.hash_map_cache::<i32, i32>(unique("simple"));
    let mut updated = cache.events_of(&[EntryEventKind::Updated]).await.unwrap();
    cache.insert(&1, &1).await.unwrap();
    cache.insert(&1, &3).await.unwrap();
    assert_eq!(
        next(&mut updated).await,
        EntryEvent::Updated {
            key: 1,
            value: 3,
            previous: 1
        }
    );
    cache.insert(&10, &1).await.unwrap();
    cache
        .insert(&10, &2)
        .ttl(Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(
        next(&mut updated).await,
        EntryEvent::Updated {
            key: 10,
            value: 2,
            previous: 1
        }
    );
    nothing_arrives(&mut updated).await;
}

// Redisson: testExpiredListener
#[tokio::test]
async fn expired_events_arrive_for_entries_whose_ttl_passed() {
    let client =
        connect_with(|builder| builder.eviction_interval(Duration::from_millis(200))).await;
    let cache = client.hash_map_cache::<i32, i32>(unique("simple"));
    let mut expired = cache.events_of(&[EntryEventKind::Expired]).await.unwrap();
    let ttl = Duration::from_millis(300);
    cache.insert(&10, &2).ttl(ttl).await.unwrap();
    assert_eq!(
        next(&mut expired).await,
        EntryEvent::Expired { key: 10, value: 2 }
    );
    cache.try_insert(&15, &2).ttl(ttl).await.unwrap();
    assert_eq!(
        next(&mut expired).await,
        EntryEvent::Expired { key: 15, value: 2 }
    );
}

// Redisson: testEntryUpdate
#[tokio::test]
async fn reinserting_an_expired_entry_returns_none_and_stores_it_again() {
    let cache = client().await.hash_map_cache::<i32, i32>(unique("simple"));
    cache
        .insert(&1, &1)
        .ttl(Duration::from_millis(300))
        .await
        .unwrap();
    assert_eq!(cache.get(&1).await.unwrap(), Some(1));
    sleep(Duration::from_millis(350)).await;
    assert_eq!(
        cache.insert(&1, &1).ttl(Duration::ZERO).await.unwrap(),
        None
    );
    assert_eq!(cache.get(&1).await.unwrap(), Some(1));
}

// Redisson: testRemovedListener
#[tokio::test]
async fn remove_sends_a_removed_event_with_the_old_value() {
    let cache = client().await.hash_map_cache::<i32, i32>(unique("simple"));
    let mut removed = cache.events_of(&[EntryEventKind::Removed]).await.unwrap();
    cache.insert(&10, &1).await.unwrap();
    cache.remove(&10).await.unwrap();
    assert_eq!(
        next(&mut removed).await,
        EntryEvent::Removed { key: 10, value: 1 }
    );
    nothing_arrives(&mut removed).await;
}

async fn check_idle_expiration(cache: &Cache<String, i32>) {
    sleep(Duration::from_millis(300)).await;
    assert_eq!(cache.get("12").await.unwrap(), None);
    assert_eq!(cache.get("14").await.unwrap(), Some(2));
    assert_eq!(cache.get("15").await.unwrap(), Some(3));
    sleep(Duration::from_millis(1000)).await;
    assert_eq!(cache.get("12").await.unwrap(), None);
    assert_eq!(cache.get("14").await.unwrap(), None);
    assert_eq!(cache.get("15").await.unwrap(), Some(3));
    sleep(Duration::from_millis(2000)).await;
    assert_eq!(cache.get("15").await.unwrap(), None);
}

// Redisson: testIdle
#[tokio::test]
async fn entries_expire_after_their_own_max_idle_time() {
    let client = client().await;
    let cache = client.hash_map_cache::<String, i32>(unique("simple"));
    cache
        .insert("12", &1)
        .max_idle(Duration::from_millis(200))
        .await
        .unwrap();
    cache
        .insert("14", &2)
        .max_idle(Duration::from_millis(800))
        .await
        .unwrap();
    cache
        .insert("15", &3)
        .max_idle(Duration::from_millis(1500))
        .await
        .unwrap();
    check_idle_expiration(&cache).await;

    let cache = client.hash_map_cache::<String, i32>(unique("simple"));
    cache
        .try_insert("12", &1)
        .max_idle(Duration::from_millis(200))
        .await
        .unwrap();
    cache
        .try_insert("14", &2)
        .max_idle(Duration::from_millis(800))
        .await
        .unwrap();
    cache
        .try_insert("15", &3)
        .max_idle(Duration::from_millis(1500))
        .await
        .unwrap();
    check_idle_expiration(&cache).await;
}

async fn check_ttl_expiration(cache: &Cache<String, i32>, started: Instant) {
    tokio::time::sleep_until(started + Duration::from_millis(900)).await;
    assert_eq!(cache.get("12").await.unwrap(), None);
    assert_eq!(cache.get("14").await.unwrap(), Some(2));
    assert_eq!(cache.get("15").await.unwrap(), Some(3));
    tokio::time::sleep_until(started + Duration::from_millis(1500)).await;
    assert_eq!(cache.get("12").await.unwrap(), None);
    assert_eq!(cache.get("14").await.unwrap(), None);
    assert_eq!(cache.get("15").await.unwrap(), Some(3));
    tokio::time::sleep_until(started + Duration::from_millis(2100)).await;
    assert_eq!(cache.get("15").await.unwrap(), None);
}

// Redisson: testTtl
#[tokio::test]
async fn entries_expire_after_their_own_ttl() {
    let client = client().await;
    let ttl = |i: u64| Duration::from_millis(600 * i);
    let cache = client.hash_map_cache::<String, i32>(unique("simple"));
    let started = Instant::now();
    cache.insert("12", &1).ttl(ttl(1)).await.unwrap();
    cache.insert("14", &2).ttl(ttl(2)).await.unwrap();
    cache.insert("15", &3).ttl(ttl(3)).await.unwrap();
    check_ttl_expiration(&cache, started).await;

    let cache = client.hash_map_cache::<String, i32>(unique("simple"));
    let started = Instant::now();
    cache.try_insert("12", &1).ttl(ttl(1)).await.unwrap();
    cache.try_insert("14", &2).ttl(ttl(2)).await.unwrap();
    cache.try_insert("15", &3).ttl(ttl(3)).await.unwrap();
    check_ttl_expiration(&cache, started).await;
}

// Redisson: testExpireOverwrite
#[tokio::test]
async fn reinserting_with_a_ttl_restarts_the_time_limit() {
    let cache = client()
        .await
        .hash_map_cache::<String, i32>(unique("simple"));
    let ttl = Duration::from_millis(500);
    cache.insert("123", &3).ttl(ttl).await.unwrap();
    sleep(Duration::from_millis(400)).await;
    cache.insert("123", &3).ttl(ttl).await.unwrap();
    sleep(Duration::from_millis(400)).await;
    assert_eq!(cache.get("123").await.unwrap(), Some(3));
    sleep(Duration::from_millis(150)).await;
    assert!(!cache.contains_key("123").await.unwrap());
}

// Redisson: testRMapCacheValues
#[tokio::test]
async fn values_returns_an_entry_stored_with_ttl_and_max_idle() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("testRMapCacheValues"));
    cache
        .insert("1234", "5678")
        .ttl(Duration::from_secs(60))
        .max_idle(Duration::from_secs(3600))
        .await
        .unwrap();
    let values: Vec<String> = cache.values().try_collect().await.unwrap();
    assert_eq!(values, ["5678"]);
}

// Redisson: testSize
#[tokio::test]
async fn len_counts_distinct_keys_across_overwrites_and_removes() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("simple"));
    cache.insert("1", "2").await.unwrap();
    cache.insert("3", "4").await.unwrap();
    cache.insert("5", "6").await.unwrap();
    assert_eq!(cache.len().await.unwrap(), 3);
    cache.insert("1", "2").await.unwrap();
    cache.insert("3", "4").await.unwrap();
    assert_eq!(cache.len().await.unwrap(), 3);
    cache.insert("1", "21").await.unwrap();
    cache.insert("3", "41").await.unwrap();
    assert_eq!(cache.len().await.unwrap(), 3);
    cache.insert("51", "6").await.unwrap();
    assert_eq!(cache.len().await.unwrap(), 4);
    cache.remove("3").await.unwrap();
    assert_eq!(cache.len().await.unwrap(), 3);
}

// Redisson: testContainsKey
#[tokio::test]
async fn contains_key_is_true_only_for_stored_keys() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("simple"));
    cache.insert("1", "2").await.unwrap();
    cache.insert("33", "44").await.unwrap();
    cache.insert("5", "6").await.unwrap();
    assert!(cache.contains_key("33").await.unwrap());
    assert!(!cache.contains_key("34").await.unwrap());
}

// Redisson: testPutGet
#[tokio::test]
async fn get_returns_the_value_stored_for_a_key() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("simple"));
    cache.insert("1", "2").await.unwrap();
    cache.insert("33", "44").await.unwrap();
    cache.insert("5", "6").await.unwrap();
    assert_eq!(cache.get("33").await.unwrap().as_deref(), Some("44"));
    assert_eq!(cache.get("5").await.unwrap().as_deref(), Some("6"));
}

// Redisson: testRemove
#[tokio::test]
async fn remove_returns_the_old_value_or_none_for_a_missing_key() {
    let cache = client().await.hash_map_cache::<i32, i32>(unique("simple"));
    cache.insert(&1, &3).await.unwrap();
    cache.insert(&3, &5).await.unwrap();
    cache.insert(&7, &8).await.unwrap();
    assert_eq!(cache.remove(&1).await.unwrap(), Some(3));
    assert_eq!(cache.remove(&3).await.unwrap(), Some(5));
    assert_eq!(cache.remove(&10).await.unwrap(), None);
    assert_eq!(cache.remove(&7).await.unwrap(), Some(8));
}

// Redisson: testFastPutIfAbsent
#[tokio::test]
async fn try_insert_keeps_an_existing_value_and_adds_a_new_key() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("simple"));
    cache.insert("1", "2").await.unwrap();
    assert!(cache.try_insert("1", "3").await.unwrap().is_some());
    assert_eq!(cache.get("1").await.unwrap().as_deref(), Some("2"));
    assert!(cache.try_insert("2", "4").await.unwrap().is_none());
    assert_eq!(cache.get("2").await.unwrap().as_deref(), Some("4"));
}

// Redisson: testIterator
#[tokio::test]
async fn keys_values_and_iter_return_all_thousand_entries() {
    let cache = client().await.hash_map_cache::<i32, i32>(unique("123"));
    for i in 0..1000 {
        cache.insert(&i, &i).await.unwrap();
    }
    assert_eq!(cache.len().await.unwrap(), 1000);
    let keys: HashSet<i32> = cache.keys().try_collect().await.unwrap();
    assert_eq!(keys.len(), 1000);
    let values: Vec<i32> = cache.values().try_collect().await.unwrap();
    assert!(values.len() >= 1000);
    let entries: HashMap<i32, i32> = cache.iter().try_collect().await.unwrap();
    assert_eq!(entries.len(), 1000);
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
struct SimpleKey {
    key: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
struct SimpleValue {
    value: String,
}

fn simple_key(key: &str) -> SimpleKey {
    SimpleKey { key: key.into() }
}

fn simple_value(value: &str) -> SimpleValue {
    SimpleValue {
        value: value.into(),
    }
}

#[tokio::test]
async fn try_insert_with_a_time_limit_wins_again_only_after_the_entry_expired() {
    let cache = client()
        .await
        .hash_map_cache::<SimpleKey, SimpleValue>(unique("simpleTTL"));
    let key = simple_key("1");
    let value = simple_value("2");
    let second = Duration::from_secs(1);
    let idle = Duration::from_millis(500);
    cache.try_insert(&key, &value).ttl(second).await.unwrap();
    assert!(cache
        .try_insert(&key, &simple_value("3"))
        .ttl(second)
        .await
        .unwrap()
        .is_some());
    assert_eq!(cache.get(&key).await.unwrap(), Some(value));

    sleep(Duration::from_millis(1100)).await;

    assert!(cache
        .try_insert(&key, &simple_value("3"))
        .ttl(second)
        .await
        .unwrap()
        .is_none());
    assert_eq!(cache.get(&key).await.unwrap(), Some(simple_value("3")));

    assert!(cache
        .try_insert(&key, &simple_value("4"))
        .ttl(second)
        .await
        .unwrap()
        .is_some());
    assert_eq!(cache.get(&key).await.unwrap(), Some(simple_value("3")));

    sleep(Duration::from_millis(1100)).await;
    assert!(cache
        .try_insert(&key, &simple_value("4"))
        .ttl(second)
        .max_idle(idle)
        .await
        .unwrap()
        .is_none());

    sleep(Duration::from_millis(550)).await;
    assert!(cache
        .try_insert(&key, &simple_value("5"))
        .ttl(second)
        .max_idle(idle)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn integer_keys_and_values_are_stored_and_read_back() {
    let cache = client()
        .await
        .hash_map_cache::<i32, i32>(unique("test_int"));
    cache.insert(&1, &2).await.unwrap();
    cache.insert(&3, &4).await.unwrap();
    assert_eq!(cache.len().await.unwrap(), 2);
    assert_eq!(cache.get(&1).await.unwrap(), Some(2));
    assert_eq!(cache.get(&3).await.unwrap(), Some(4));
}

#[tokio::test]
async fn long_keys_and_values_are_stored_and_read_back() {
    let cache = client()
        .await
        .hash_map_cache::<i64, i64>(unique("test_long"));
    cache.insert(&1i64, &2i64).await.unwrap();
    cache.insert(&3i64, &4i64).await.unwrap();
    assert_eq!(cache.len().await.unwrap(), 2);
    assert_eq!(cache.get(&1i64).await.unwrap(), Some(2));
    assert_eq!(cache.get(&3i64).await.unwrap(), Some(4));
}

#[tokio::test]
async fn a_small_cache_is_iterated_in_the_order_it_was_written() {
    let written = [
        ("name", "123"),
        ("ip", "4124"),
        ("rank", "none"),
        ("tokens", "0"),
        ("coins", "0"),
        ("ar_score", "0"),
        ("ar_gameswon", "0"),
        ("ar_gameslost", "0"),
        ("ar_kills", "0"),
        ("ar_deaths", "0"),
    ];
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("123"));
    for (key, value) in written {
        cache.insert(key, value).await.unwrap();
    }
    let expected: Vec<(String, String)> = written
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
    let found: Vec<String> = cache.keys().try_collect().await.unwrap();
    let expected_keys: Vec<String> = expected.iter().map(|(key, _)| key.clone()).collect();
    assert_eq!(found, expected_keys);
    let found: Vec<String> = cache.values().try_collect().await.unwrap();
    let expected_values: Vec<String> = expected.iter().map(|(_, value)| value.clone()).collect();
    assert_eq!(found, expected_values);
    let found: Vec<(String, String)> = cache.iter().try_collect().await.unwrap();
    assert_eq!(found, expected);
}

#[tokio::test]
async fn a_value_of_the_wrong_shape_is_an_error_at_once() {
    let client = client().await;
    let name = unique("deserializationFailure");
    let writer = client.hash_map_cache::<String, String>(name.clone());
    let reader = client.hash_map_cache::<String, SimpleValue>(name);
    writer.insert("test-key", "test-val").await.unwrap();
    let read = tokio::time::timeout(Duration::from_secs(5), reader.get("test-key"))
        .await
        .expect("the read did not fail in time");
    assert!(matches!(read, Err(Error::Codec(_))), "{read:?}");
}

#[tokio::test]
async fn remove_returns_the_value_of_a_struct_key() {
    let cache = client()
        .await
        .hash_map_cache::<SimpleKey, SimpleValue>(unique("simple"));
    for (key, value) in [("1", "2"), ("33", "44"), ("5", "6")] {
        cache
            .insert(&simple_key(key), &simple_value(value))
            .await
            .unwrap();
    }
    assert_eq!(
        cache.remove(&simple_key("33")).await.unwrap(),
        Some(simple_value("44"))
    );
    assert_eq!(
        cache.remove(&simple_key("5")).await.unwrap(),
        Some(simple_value("6"))
    );
    assert_eq!(cache.remove(&simple_key("11")).await.unwrap(), None);
    assert_eq!(cache.len().await.unwrap(), 1);
}

#[tokio::test]
async fn values_streams_every_value_once() {
    let cache = client().await.hash_map_cache::<i32, i32>(unique("simple"));
    for (key, value) in [(1, 0), (3, 5), (4, 6), (7, 8)] {
        cache.insert(&key, &value).await.unwrap();
    }
    let mut values: Vec<i32> = cache.values().try_collect().await.unwrap();
    let distinct: HashSet<i32> = values.iter().copied().collect();
    assert_eq!(distinct, HashSet::from([0, 5, 6, 8]));
    let streamed: Vec<i32> = cache.values().try_collect().await.unwrap();
    for value in streamed {
        let position = values.iter().position(|other| *other == value);
        let position = position.unwrap_or_else(|| panic!("unable to remove value {value}"));
        values.remove(position);
    }
    assert!(values.is_empty());
}

#[tokio::test]
async fn insert_tells_a_new_entry_from_a_replaced_one() {
    let cache = client().await.hash_map_cache::<i32, i32>(unique("simple"));
    assert!(cache.insert(&1, &2).await.unwrap().is_none());
    assert_eq!(cache.get(&1).await.unwrap(), Some(2));
    assert!(cache.insert(&1, &3).await.unwrap().is_some());
    assert_eq!(cache.get(&1).await.unwrap(), Some(3));
    assert_eq!(cache.len().await.unwrap(), 1);
}

#[tokio::test]
async fn the_entries_equal_the_map_that_was_written() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("simple"));
    cache.insert("1", "7").await.unwrap();
    cache.insert("2", "4").await.unwrap();
    cache.insert("3", "5").await.unwrap();
    let expected: HashMap<String, String> = [("1", "7"), ("2", "4"), ("3", "5")]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
    let entries: Vec<(String, String)> = cache.iter().try_collect().await.unwrap();
    assert_eq!(entries.len(), expected.len());
    assert_eq!(entries.into_iter().collect::<HashMap<_, _>>(), expected);
}

#[tokio::test]
async fn the_string_codec_stores_and_iterates_plain_strings() {
    let cache = client()
        .await
        .with_codec(StringCodec)
        .hash_map_cache::<String, String>(unique("TestRMap01"));
    cache.insert("A", "1").await.unwrap();
    cache.insert("B", "2").await.unwrap();
    let allowed = [
        ("A".to_string(), "1".to_string()),
        ("B".to_string(), "2".to_string()),
    ];
    let entries: Vec<(String, String)> = cache.iter().try_collect().await.unwrap();
    assert_eq!(entries.len(), 2);
    for entry in entries {
        assert!(allowed.contains(&entry), "{entry:?}");
    }
}

#[tokio::test]
async fn an_entry_expires_and_is_reported_on_a_cluster() {
    let topology = Topology::cluster().await;
    let client = redissun::Client::builder()
        .url(topology.cluster_url())
        .build()
        .await
        .expect("could not connect to the cluster");
    let cache = client.hash_map_cache::<String, String>(unique("simple"));
    let mut expired = cache.events_of(&[EntryEventKind::Expired]).await.unwrap();
    cache
        .insert("1", "2")
        .ttl(Duration::from_secs(1))
        .await
        .unwrap();
    let event = tokio::time::timeout(Duration::from_secs(10), expired.recv())
        .await
        .expect("no expired event on a cluster")
        .unwrap();
    assert_eq!(
        event,
        EntryEvent::Expired {
            key: "1".into(),
            value: "2".into()
        }
    );
}

#[tokio::test]
async fn expire_also_covers_the_sets_that_are_created_later() {
    let name = unique("cache");
    let cache = client()
        .await
        .hash_map_cache::<String, String>(name.clone());
    cache.insert("plain", "x").await.unwrap();
    assert!(cache.expire(Duration::from_secs(100)).await.unwrap());
    cache
        .insert("a", "x")
        .ttl(Duration::from_secs(60))
        .max_idle(Duration::from_secs(60))
        .await
        .unwrap();
    for set in ["redissun__timeout__set", "redissun__idle__set"] {
        let ttl = raw_command(&["TTL", &format!("{set}:{{{name}}}")]).await;
        let ttl: i64 = ttl.trim().trim_start_matches(':').parse().unwrap();
        assert!(
            ttl > 0,
            "{set} must expire with the cache, its ttl is {ttl}"
        );
    }
    assert_eq!(cache.len().await.unwrap(), 2);
    assert_eq!(cache.get("a").await.unwrap(), Some("x".to_string()));

    assert!(cache.persist().await.unwrap());
    for set in ["redissun__timeout__set", "redissun__idle__set"] {
        let ttl = raw_command(&["TTL", &format!("{set}:{{{name}}}")]).await;
        assert_eq!(ttl.trim(), ":-1");
        let size = raw_command(&["ZCARD", &format!("{set}:{{{name}}}")]).await;
        assert_eq!(size.trim(), ":1", "{set} must hold only the entry");
    }
}

#[tokio::test]
async fn expire_and_persist_on_a_missing_cache_leave_nothing_behind() {
    let name = unique("cache");
    let cache = client()
        .await
        .hash_map_cache::<String, String>(name.clone());
    assert!(!cache.expire(Duration::from_millis(200)).await.unwrap());
    assert!(!cache.persist().await.unwrap());
    let found = raw_command(&["EXISTS", &format!("redissun__timeout__set:{{{name}}}")]).await;
    assert_eq!(found.trim(), ":0");
}

#[tokio::test]
async fn rename_moves_the_entries_with_their_limits_and_the_size_limit() {
    let client = client().await;
    let name = unique("cache");
    let new_name = unique("renamed");
    let cache = client.hash_map_cache::<String, String>(name.clone());
    cache.set_max_size(2, EvictionMode::Lru).await.unwrap();
    cache.insert("plain", "x").await.unwrap();
    cache
        .insert("short", "y")
        .ttl(Duration::from_millis(300))
        .await
        .unwrap();
    cache.rename(&new_name).await.unwrap();

    assert!(!cache.exists().await.unwrap());
    for key in [
        format!("redissun__timeout__set:{{{name}}}"),
        format!("redissun__map_cache__last_access__set:{{{name}}}"),
        format!("{{{name}}}:redissun_options"),
    ] {
        let found = raw_command(&["EXISTS", &key]).await;
        assert_eq!(found.trim(), ":0", "{key} must move with the cache");
    }

    let renamed = client.hash_map_cache::<String, String>(new_name.clone());
    assert_eq!(renamed.get("plain").await.unwrap(), Some("x".to_string()));
    assert_eq!(renamed.get("short").await.unwrap(), Some("y".to_string()));
    sleep(Duration::from_millis(500)).await;
    assert_eq!(renamed.get("short").await.unwrap(), None);
    renamed.insert("b", "1").await.unwrap();
    renamed.insert("c", "2").await.unwrap();
    let _cleaner = cleaner(&new_name).await;
    eventually(
        "the expired entry stayed",
        Duration::from_secs(5),
        || async { renamed.len().await.unwrap() == 2 },
    )
    .await;
}

#[tokio::test]
async fn stores_values_in_redissons_layout() {
    let name = unique("cache");
    let cache = client().await.hash_map_cache::<String, i64>(name.clone());
    let before = client_now();
    cache
        .insert("a", &1i64)
        .max_idle(Duration::from_secs(30))
        .await
        .unwrap();
    let unpacked = raw_command(&[
        "EVAL",
        "local t, v = struct.unpack('dLc0', redis.call('hget', KEYS[1], ARGV[1])) return {tostring(t), v}",
        "1",
        &name,
        "\"a\"",
    ])
    .await;
    assert_eq!(unpacked, "*2\r\n$5\r\n30000\r\n$1\r\n1\r\n");
    assert_eq!(
        raw_command(&["HSTRLEN", &name, "\"a\""]).await.trim(),
        ":17"
    );
    let score = raw_command(&[
        "ZSCORE",
        &format!("redissun__idle__set:{{{name}}}"),
        "\"a\"",
    ])
    .await;
    let score: f64 = score.lines().nth(1).unwrap().parse().unwrap();
    let expected = (before + 30_000) as f64;
    assert!((score - expected).abs() <= 2000.0, "{score} vs {expected}");
}

#[tokio::test]
async fn reads_an_entry_written_by_redissons_script() {
    let name = unique("cache");
    let cache = client()
        .await
        .hash_map_cache::<String, String>(name.clone());
    let now = client_now();
    // The writing lines of Redisson's fastPutOperationAsync(key, value, ttl, idle), with its KEYS and ARGV.
    let script = "if tonumber(ARGV[2]) > 0 then \
                      redis.call('zadd', KEYS[2], ARGV[2], ARGV[5]); \
                  else \
                      redis.call('zrem', KEYS[2], ARGV[5]); \
                  end; \
                  if tonumber(ARGV[3]) > 0 then \
                      redis.call('zadd', KEYS[3], ARGV[3], ARGV[5]); \
                  else \
                      redis.call('zrem', KEYS[3], ARGV[5]); \
                  end; \
                  local value = struct.pack('dLc0', ARGV[4], string.len(ARGV[6]), ARGV[6]); \
                  redis.call('hset', KEYS[1], ARGV[5], value); \
                  return 1;";
    raw_command(&[
        "EVAL",
        script,
        "3",
        &name,
        &format!("redissun__timeout__set:{{{name}}}"),
        &format!("redissun__idle__set:{{{name}}}"),
        &now.to_string(),
        &(now + 60_000).to_string(),
        &(now + 400).to_string(),
        "400",
        "\"a\"",
        "\"x\"",
    ])
    .await;
    assert_eq!(cache.get("a").await.unwrap(), Some("x".to_string()));
    let left = cache.entry_ttl("a").await.unwrap().unwrap();
    assert!(left <= Duration::from_millis(400), "{left:?}");
    assert_eq!(keys(&cache).await, ["a"]);
    sleep(Duration::from_millis(600)).await;
    assert_eq!(cache.get("a").await.unwrap(), None);
}

#[tokio::test]
async fn names_with_a_hash_tag_follow_redissons_rules() {
    let name = format!("{{t}}:{}", unique("cache"));
    let cache = client()
        .await
        .hash_map_cache::<String, String>(name.clone());
    cache
        .insert("a", "x")
        .ttl(Duration::from_secs(60))
        .await
        .unwrap();
    cache.set_max_size(10, EvictionMode::Lru).await.unwrap();
    cache.insert("b", "y").await.unwrap();
    let _events = cache.events().await.unwrap();
    for key in [
        name.clone(),
        format!("redissun__timeout__set:{name}"),
        format!("redissun__map_cache__last_access__set:{name}"),
        format!("{name}:redissun_options"),
    ] {
        assert_eq!(
            raw_command(&["EXISTS", &key]).await.trim(),
            ":1",
            "{key} must exist"
        );
    }
    assert_eq!(
        common::subscribed_channels(&format!("redissun_map_cache_created:{name}")).await,
        1
    );
}

#[tokio::test]
async fn expiry_follows_the_client_clock() {
    let name = unique("cache");
    let cache = client()
        .await
        .hash_map_cache::<String, String>(name.clone());
    cache
        .insert("a", "x")
        .ttl(Duration::from_secs(60))
        .await
        .unwrap();
    let timeouts = format!("redissun__timeout__set:{{{name}}}");
    raw_command(&["ZADD", &timeouts, &(client_now() - 1).to_string(), "\"a\""]).await;
    assert_eq!(cache.get("a").await.unwrap(), None);
    raw_command(&[
        "ZADD",
        &timeouts,
        &(client_now() + 400).to_string(),
        "\"a\"",
    ])
    .await;
    assert_eq!(cache.get("a").await.unwrap(), Some("x".to_string()));
    sleep(Duration::from_millis(600)).await;
    assert_eq!(cache.get("a").await.unwrap(), None);
}

#[tokio::test]
async fn an_old_layout_value_is_an_error_not_a_wrong_value() {
    let name = format!("{{t}}:{}", unique("cache"));
    let cache = client()
        .await
        .hash_map_cache::<String, String>(name.clone());
    raw_command(&["HSET", &name, "\"k\"", "0:\"v\""]).await;
    let result = cache.get("k").await;
    assert!(matches!(result, Err(Error::Redis(_))), "{result:?}");
}

#[tokio::test]
async fn try_insert_returns_the_live_value() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    let ttl = Duration::from_millis(300);
    assert_eq!(cache.try_insert("a", "1").ttl(ttl).await.unwrap(), None);
    assert_eq!(
        cache.try_insert("a", "2").await.unwrap(),
        Some("1".to_string())
    );
    sleep(Duration::from_millis(500)).await;
    assert_eq!(cache.try_insert("a", "3").await.unwrap(), None);
    assert_eq!(cache.get("a").await.unwrap(), Some("3".to_string()));
    assert_eq!(cache.entry_ttl("a").await.unwrap(), None);
}

#[tokio::test]
async fn insert_clears_the_limits_of_a_live_entry() {
    let name = unique("cache");
    let cache = client().await.hash_map_cache::<String, i64>(name.clone());
    cache
        .insert("a", &1i64)
        .ttl(Duration::from_secs(60))
        .max_idle(Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(cache.insert("a", &2i64).await.unwrap(), Some(1));
    assert_eq!(cache.entry_ttl("a").await.unwrap(), None);
    for set in ["redissun__timeout__set", "redissun__idle__set"] {
        let score = raw_command(&["ZSCORE", &format!("{set}:{{{name}}}"), "\"a\""]).await;
        assert_eq!(score.trim(), "$-1", "{set} still has the entry");
    }
}

/// A client on its own server with `notify-keyspace-events` set to `flags`.
async fn keyspace_client(url: &str, flags: &str) -> redissun::Client {
    common::raw_command_at(url, &["CONFIG", "SET", "notify-keyspace-events", flags]).await;
    redissun::Client::builder()
        .url(url.to_string())
        .build()
        .await
        .unwrap()
}

async fn next_keyspace_event(
    events: &mut redissun::MapEvents<String, JsonCodec>,
) -> redissun::MapEvent<String> {
    tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await
        .expect("no event within 2 s")
        .unwrap()
}

// Redisson: MapPutListener, MapRemoveListener on an RMapCache
#[tokio::test]
async fn keyspace_events_report_put_and_remove() {
    let (_container, url) = common::own_redis8().await;
    let client = keyspace_client(&url, "ETh").await;
    let cache = client.hash_map_cache::<String, String>(unique("cache"));
    let mut events = cache.keyspace_events().await.unwrap();
    cache.insert("a", "1").await.unwrap();
    assert_eq!(
        next_keyspace_event(&mut events).await,
        redissun::MapEvent::Put {
            key: Some("a".to_string())
        }
    );
    cache.remove("a").await.unwrap();
    assert_eq!(
        next_keyspace_event(&mut events).await,
        redissun::MapEvent::Removed {
            key: Some("a".to_string())
        }
    );
}

// Redisson: MapIncrListener; addAndGet writes and deletes `temp_val__redisson`
#[tokio::test]
async fn keyspace_events_skip_the_temporary_incr_field() {
    let (_container, url) = common::own_redis8().await;
    let client = keyspace_client(&url, "ETh").await;
    let name = unique("cache");
    let cache = client.hash_map_cache::<String, i64>(name.clone());
    let mut events = cache.keyspace_events().await.unwrap();
    // What ADD_AND_GET does to the temporary field (Task 3 brings incr_by).
    common::raw_command_at(
        &url,
        &[
            "EVAL",
            "redis.call('hset', KEYS[1], 'redissun__temp_val', '1'); \
             redis.call('hincrbyfloat', KEYS[1], 'redissun__temp_val', '1'); \
             redis.call('hdel', KEYS[1], 'redissun__temp_val'); return 1",
            "1",
            &name,
        ],
    )
    .await;
    cache.insert("n", &1).await.unwrap();
    assert_eq!(
        next_keyspace_event(&mut events).await,
        redissun::MapEvent::Put {
            key: Some("n".to_string())
        }
    );
    let outcome = tokio::time::timeout(Duration::from_millis(300), events.recv()).await;
    assert!(
        !matches!(outcome, Ok(Err(_))),
        "unexpected error {outcome:?}"
    );
}

/// `HGET` of the raw field and the `ZSCORE`s of the timeout and idle sets, as RESP text.
async fn stored_state(name: &str, field: &str) -> (String, String, String) {
    let hget = raw_command(&["HGET", name, field]).await;
    let timeout = raw_command(&[
        "ZSCORE",
        &format!("redissun__timeout__set:{{{name}}}"),
        field,
    ])
    .await;
    let idle = raw_command(&["ZSCORE", &format!("redissun__idle__set:{{{name}}}"), field]).await;
    (hget, timeout, idle)
}

#[tokio::test]
async fn set_reports_a_new_key() {
    let cache = client()
        .await
        .hash_map_cache::<String, i64>(unique("cache"));
    assert!(cache.set("a", &1i64).await.unwrap());
    assert!(!cache.set("a", &2i64).await.unwrap());
    assert_eq!(cache.get("a").await.unwrap(), Some(2));
    assert!(cache
        .set("b", &1i64)
        .ttl(Duration::from_millis(100))
        .await
        .unwrap());
    sleep(Duration::from_millis(250)).await;
    assert!(cache.set("b", &2i64).await.unwrap());
}

#[tokio::test]
async fn set_reports_what_insert_reports() {
    for limited in [false, true] {
        let c = client().await;
        let by_set = c.hash_map_cache::<String, i64>(unique("cache"));
        let by_insert = c.hash_map_cache::<String, i64>(unique("cache"));
        let ttl = Duration::from_secs(60);
        let set = |k: &'static str, v: i64| {
            let by_set = by_set.clone();
            async move {
                if limited {
                    by_set.set(k, &v).ttl(ttl).await.unwrap()
                } else {
                    by_set.set(k, &v).await.unwrap()
                }
            }
        };
        let insert = |k: &'static str, v: i64| {
            let by_insert = by_insert.clone();
            async move {
                if limited {
                    by_insert.insert(k, &v).ttl(ttl).await.unwrap()
                } else {
                    by_insert.insert(k, &v).await.unwrap()
                }
            }
        };
        // Missing, then live.
        assert!(set("a", 1).await);
        assert_eq!(insert("a", 1).await, None);
        assert!(!set("a", 2).await);
        assert_eq!(insert("a", 2).await, Some(1));
        // Expired but not evicted yet.
        for cache in [&by_set, &by_insert] {
            cache
                .insert("e", &1i64)
                .ttl(Duration::from_millis(100))
                .await
                .unwrap();
        }
        sleep(Duration::from_millis(250)).await;
        assert!(set("e", 2).await, "limited: {limited}");
        assert_eq!(insert("e", 2).await, None, "limited: {limited}");
    }
}

#[tokio::test]
async fn set_and_insert_both_clear_the_limits() {
    let c = client().await;
    let by_set_name = unique("cache");
    let by_insert_name = unique("cache");
    let by_set = c.hash_map_cache::<String, i64>(by_set_name.clone());
    let by_insert = c.hash_map_cache::<String, i64>(by_insert_name.clone());
    for cache in [&by_set, &by_insert] {
        cache
            .insert("a", &1i64)
            .ttl(Duration::from_secs(60))
            .max_idle(Duration::from_secs(60))
            .await
            .unwrap();
    }
    assert!(!by_set.set("a", &2i64).await.unwrap());
    assert_eq!(by_insert.insert("a", &2i64).await.unwrap(), Some(1));
    for cache in [&by_set, &by_insert] {
        assert_eq!(cache.entry_ttl("a").await.unwrap(), None);
        assert_eq!(cache.get("a").await.unwrap(), Some(2));
    }
    let after_set = stored_state(&by_set_name, "\"a\"").await;
    let after_insert = stored_state(&by_insert_name, "\"a\"").await;
    assert_eq!(after_set.1.trim(), "$-1");
    assert_eq!(after_set.2.trim(), "$-1");
    assert_eq!(after_set, after_insert);
}

#[tokio::test]
async fn set_with_limits_sends_created_and_updated_events() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    let mut events = cache
        .events_of(&[EntryEventKind::Created, EntryEventKind::Updated])
        .await
        .unwrap();
    let ttl = Duration::from_secs(60);
    assert!(cache.set("a", "x").ttl(ttl).await.unwrap());
    assert_eq!(
        next(&mut events).await,
        EntryEvent::Created {
            key: "a".into(),
            value: "x".into()
        }
    );
    assert!(!cache.set("a", "y").max_idle(ttl).await.unwrap());
    assert_eq!(
        next(&mut events).await,
        EntryEvent::Updated {
            key: "a".into(),
            value: "y".into(),
            previous: "x".into()
        }
    );
}

#[tokio::test]
async fn replace_keeps_the_idle_time() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    cache
        .insert("a", "x")
        .max_idle(Duration::from_millis(400))
        .await
        .unwrap();
    assert_eq!(cache.replace("a", "y").await.unwrap(), Some("x".into()));
    sleep(Duration::from_millis(200)).await;
    assert_eq!(cache.get("a").await.unwrap(), Some("y".into()));
    sleep(Duration::from_millis(600)).await;
    assert_eq!(cache.get("a").await.unwrap(), None);
    assert_eq!(cache.replace("missing", "y").await.unwrap(), None);
    assert!(!cache.contains_key("missing").await.unwrap());
}

#[tokio::test]
async fn compare_and_set_and_remove_if_eq_follow_the_value() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    cache.insert("a", "x").await.unwrap();
    assert!(!cache.compare_and_set("a", "wrong", "y").await.unwrap());
    assert_eq!(cache.get("a").await.unwrap(), Some("x".into()));
    assert!(cache.compare_and_set("a", "x", "y").await.unwrap());
    assert_eq!(cache.get("a").await.unwrap(), Some("y".into()));
    assert!(!cache.remove_if_eq("a", "x").await.unwrap());
    assert_eq!(cache.get("a").await.unwrap(), Some("y".into()));
    assert!(cache.remove_if_eq("a", "y").await.unwrap());
    assert_eq!(cache.get("a").await.unwrap(), None);
    cache
        .insert("e", "x")
        .ttl(Duration::from_millis(100))
        .await
        .unwrap();
    sleep(Duration::from_millis(250)).await;
    assert!(!cache.compare_and_set("e", "x", "y").await.unwrap());
    assert!(!cache.remove_if_eq("e", "x").await.unwrap());
    assert!(!cache.compare_and_set("missing", "x", "y").await.unwrap());
}

#[tokio::test]
async fn remove_many_counts_and_publishes() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    let mut events = cache.events_of(&[EntryEventKind::Removed]).await.unwrap();
    for k in ["a", "b", "c"] {
        cache.insert(k, "v").await.unwrap();
    }
    assert_eq!(cache.remove_many(&["a", "b", "missing"]).await.unwrap(), 2);
    let mut removed = HashSet::new();
    for _ in 0..2 {
        match next(&mut events).await {
            EntryEvent::Removed { key, value } => {
                assert_eq!(value, "v");
                removed.insert(key);
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(removed, HashSet::from(["a".to_string(), "b".to_string()]));
    nothing_arrives(&mut events).await;
    assert_eq!(keys(&cache).await, vec!["c".to_string()]);
    assert_eq!(cache.remove_many::<str>(&[]).await.unwrap(), 0);
}

#[tokio::test]
async fn incr_by_creates_and_adds() {
    let cache = client()
        .await
        .hash_map_cache::<String, f64>(unique("cache"));
    assert_eq!(cache.incr_by("n", 5).await.unwrap(), 5);
    assert_eq!(cache.incr_by("n", 2).await.unwrap(), 7);
    assert_eq!(cache.incr_by_float("n", 0.5).await.unwrap(), 7.5);
    assert_eq!(cache.get("n").await.unwrap(), Some(7.5));
    assert!(matches!(
        cache.incr_by_float("n", f64::NAN).await,
        Err(Error::Config(_))
    ));
}

#[tokio::test]
async fn extend_with_ttl_and_batches() {
    let cache = client()
        .await
        .hash_map_cache::<String, i64>(unique("cache"));
    let entries: Vec<(String, i64)> = (0..250).map(|i| (format!("k{i}"), i)).collect();
    cache
        .extend(entries.iter().map(|(k, v)| (k.as_str(), v)))
        .batch_size(100)
        .ttl(Duration::from_millis(300))
        .await
        .unwrap();
    assert_eq!(cache.len().await.unwrap(), 250);
    assert_eq!(cache.get("k249").await.unwrap(), Some(249));
    sleep(Duration::from_millis(500)).await;
    for (k, _) in &entries {
        assert_eq!(cache.get(k.as_str()).await.unwrap(), None, "{k}");
    }
}

#[tokio::test]
async fn extend_with_max_idle_only() {
    let cache = client()
        .await
        .hash_map_cache::<String, i64>(unique("cache"));
    cache
        .extend([("a", &1i64)])
        .max_idle(Duration::from_millis(300))
        .await
        .unwrap();
    let left = cache.entry_ttl("a").await.unwrap().unwrap();
    assert!(left <= Duration::from_millis(300), "{left:?}");
}

#[tokio::test]
async fn extend_without_limits_clears_them() {
    let name = unique("cache");
    let cache = client().await.hash_map_cache::<String, i64>(name.clone());
    cache
        .insert("a", &1i64)
        .ttl(Duration::from_secs(60))
        .max_idle(Duration::from_secs(60))
        .await
        .unwrap();
    cache.extend([("a", &2i64)]).await.unwrap();
    assert_eq!(cache.entry_ttl("a").await.unwrap(), None);
    let (_, timeout, idle) = stored_state(&name, "\"a\"").await;
    assert_eq!(timeout.trim(), "$-1");
    assert_eq!(idle.trim(), "$-1");
    assert_eq!(cache.get("a").await.unwrap(), Some(2));
    cache
        .extend(std::iter::empty::<(&str, &i64)>())
        .await
        .unwrap();
}

#[tokio::test]
async fn extend_respects_the_size_limit() {
    let cache = client()
        .await
        .hash_map_cache::<String, i64>(unique("cache"));
    cache.set_max_size(10, EvictionMode::Lru).await.unwrap();
    let entries: Vec<(String, i64)> = (0..25).map(|i| (format!("k{i}"), i)).collect();
    cache
        .extend(entries.iter().map(|(k, v)| (k.as_str(), v)))
        .await
        .unwrap();
    assert_eq!(cache.len().await.unwrap(), 10);
}

#[tokio::test]
async fn get_many_skips_expired_and_keeps_order() {
    let cache = client()
        .await
        .hash_map_cache::<String, i64>(unique("cache"));
    cache.insert("a", &1i64).await.unwrap();
    cache
        .insert("b", &2i64)
        .ttl(Duration::from_millis(100))
        .await
        .unwrap();
    sleep(Duration::from_millis(250)).await;
    assert_eq!(
        cache.get_many(&["a", "b", "missing"]).await.unwrap(),
        vec![Some(1), None, None]
    );
    assert_eq!(
        cache.get_many(&["missing", "a"]).await.unwrap(),
        vec![None, Some(1)]
    );
    assert!(cache.get_many::<str>(&[]).await.unwrap().is_empty());
}

#[tokio::test]
async fn get_many_refreshes_the_idle_time() {
    let cache = client()
        .await
        .hash_map_cache::<String, i64>(unique("cache"));
    cache
        .insert("a", &1i64)
        .max_idle(Duration::from_millis(400))
        .await
        .unwrap();
    for _ in 0..4 {
        sleep(Duration::from_millis(200)).await;
        assert_eq!(cache.get_many(&["a"]).await.unwrap(), vec![Some(1)]);
    }
    sleep(Duration::from_millis(600)).await;
    assert_eq!(cache.get_many(&["a"]).await.unwrap(), vec![None]);
}

#[tokio::test]
async fn contains_value_ignores_expired_entries() {
    let cache = client()
        .await
        .hash_map_cache::<String, i64>(unique("cache"));
    cache.insert("a", &1i64).await.unwrap();
    cache
        .insert("b", &2i64)
        .ttl(Duration::from_millis(100))
        .await
        .unwrap();
    assert!(cache.contains_value(&2i64).await.unwrap());
    sleep(Duration::from_millis(250)).await;
    assert!(cache.contains_value(&1i64).await.unwrap());
    assert!(!cache.contains_value(&2i64).await.unwrap());
    assert!(!cache.contains_value(&3i64).await.unwrap());
}

#[tokio::test]
async fn value_len_counts_the_encoded_value() {
    let c = client().await;
    let name = unique("cache");
    let cache = c.hash_map_cache::<String, String>(name.clone());
    cache.insert("a", "xy").await.unwrap();
    assert_eq!(cache.value_len("a").await.unwrap(), 4);
    let map = c.hash_map::<String, String>(unique("map"));
    map.insert("a", "xy").await.unwrap();
    assert_eq!(map.value_len("a").await.unwrap(), 4);
    let raw = raw_command(&["HSTRLEN", &hash_key(&name), "\"a\""]).await;
    assert_eq!(raw.trim(), ":20");
    assert_eq!(cache.value_len("missing").await.unwrap(), 0);
}

#[tokio::test]
async fn random_keys_and_entries_skip_expired() {
    let cache = client()
        .await
        .hash_map_cache::<String, i64>(unique("cache"));
    for i in 0..5i64 {
        cache.insert(&format!("live{i}"), &i).await.unwrap();
        cache
            .insert(&format!("dead{i}"), &i)
            .ttl(Duration::from_millis(100))
            .await
            .unwrap();
    }
    sleep(Duration::from_millis(250)).await;
    let keys = cache.random_keys(10).await.unwrap();
    assert_eq!(keys.len(), 5);
    assert!(keys.iter().all(|k| k.starts_with("live")));
    let entries = cache.random_entries(10).await.unwrap();
    assert_eq!(entries.len(), 5);
    assert!(entries.keys().all(|k| k.starts_with("live")));
    assert_eq!(entries["live3"], 3);
    assert!(cache.random_keys(0).await.unwrap().is_empty());
}

#[tokio::test]
async fn to_hash_map_returns_live_entries_only() {
    let cache = client()
        .await
        .hash_map_cache::<String, i64>(unique("cache"));
    assert!(cache.to_hash_map().await.unwrap().is_empty());
    cache.insert("a", &1i64).await.unwrap();
    cache
        .insert("b", &2i64)
        .max_idle(Duration::from_secs(60))
        .await
        .unwrap();
    cache
        .insert("c", &3i64)
        .ttl(Duration::from_millis(100))
        .await
        .unwrap();
    sleep(Duration::from_millis(250)).await;
    let all = cache.to_hash_map().await.unwrap();
    assert_eq!(
        all,
        HashMap::from([("a".to_string(), 1), ("b".to_string(), 2)])
    );
}

#[tokio::test]
async fn scan_with_pattern_reads_every_page() {
    let cache = client()
        .await
        .hash_map_cache::<String, i64>(unique("cache"));
    let keys: Vec<String> = (0..1000).map(|i| format!("k{i}")).collect();
    let values: Vec<i64> = (0..1000).collect();
    cache
        .extend(keys.iter().map(String::as_str).zip(values.iter()))
        .await
        .unwrap();
    let found: HashSet<String> = cache
        .keys_with(redissun::MapScan::new().pattern("\"k1*").page_size(10))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(found.len(), 111);
    let values: Vec<i64> = cache
        .values_with(redissun::MapScan::new().pattern("\"k99*"))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(values.len(), 11);
    let entries: Vec<(String, i64)> = cache
        .iter_with(redissun::MapScan::new().page_size(7))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(entries.len(), 1000);
}

#[tokio::test]
async fn keys_with_uses_the_pattern_and_skips_expired() {
    let cache = client()
        .await
        .hash_map_cache::<String, i64>(unique("cache"));
    cache.insert("a1", &1i64).await.unwrap();
    cache.insert("a2", &2i64).await.unwrap();
    cache
        .insert("a3", &3i64)
        .ttl(Duration::from_millis(100))
        .await
        .unwrap();
    cache.insert("b1", &4i64).await.unwrap();
    sleep(Duration::from_millis(250)).await;
    let found: HashSet<String> = cache
        .keys_with(redissun::MapScan::new().pattern("\"a*"))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(found, HashSet::from(["a1".to_string(), "a2".to_string()]));
    let entries: HashMap<String, i64> = cache
        .iter_with(redissun::MapScan::new().pattern("\"a*").page_size(1))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        entries,
        HashMap::from([("a1".to_string(), 1), ("a2".to_string(), 2)])
    );
}
