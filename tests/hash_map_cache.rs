mod common;

use common::{client, connect_with, raw_command, unique};
use futures::TryStreamExt;
use redissun::{Error, Event, EventKind, Events, EvictionMode, HashMapCache, JsonCodec, Object};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::time::{sleep, Instant};

type Cache<K, V> = HashMapCache<K, V, JsonCodec>;

async fn pause() {
    sleep(Duration::from_millis(15)).await;
}

async fn next<K, V>(events: &mut Events<K, V, JsonCodec>) -> Event<K, V>
where
    K: serde::de::DeserializeOwned + Send,
    V: serde::de::DeserializeOwned + Send,
{
    tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("no event arrived")
        .unwrap()
}

async fn nothing_arrives<K, V>(events: &mut Events<K, V, JsonCodec>)
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

fn hash_key(name: &str) -> String {
    format!("{{{name}}}")
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
    assert_eq!(cache.evict_expired().await.unwrap(), 1);
    assert_eq!(cache.len().await.unwrap(), 0);
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
    assert!(cache.insert_nx("b", "y").ttl(Duration::ZERO).await.unwrap());
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
async fn insert_nx_only_inserts_over_a_missing_or_expired_entry() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    assert!(cache.insert_nx("a", "1").await.unwrap());
    assert!(!cache.insert_nx("a", "2").await.unwrap());
    assert_eq!(cache.get("a").await.unwrap(), Some("1".to_string()));
    cache
        .insert("b", "x")
        .ttl(Duration::from_millis(200))
        .await
        .unwrap();
    sleep(Duration::from_millis(400)).await;
    assert!(cache.insert_nx("b", "y").await.unwrap());
    assert_eq!(cache.get("b").await.unwrap(), Some("y".to_string()));
}

#[tokio::test]
async fn only_one_of_many_concurrent_insert_nx_wins() {
    let cache = client()
        .await
        .hash_map_cache::<String, u32>(unique("cache"));
    let tasks: Vec<_> = (0..20u32)
        .map(|i| {
            let cache = cache.clone();
            tokio::spawn(async move { cache.insert_nx("k", &i).await.unwrap() })
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
    cache.evict_expired().await.unwrap();
    assert_eq!(cache.len().await.unwrap(), 1);
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
async fn evict_expired_deletes_the_entries_from_redis() {
    let name = unique("cache");
    let cache = client()
        .await
        .hash_map_cache::<String, String>(name.clone());
    for i in 0..5 {
        cache
            .insert(&format!("k{i}"), "x")
            .ttl(Duration::from_millis(200))
            .await
            .unwrap();
    }
    cache.insert("keep", "y").await.unwrap();
    sleep(Duration::from_millis(400)).await;
    assert_eq!(raw_command(&["HLEN", &hash_key(&name)]).await.trim(), ":6");
    assert_eq!(cache.evict_expired().await.unwrap(), 5);
    assert_eq!(raw_command(&["HLEN", &hash_key(&name)]).await.trim(), ":1");
    assert_eq!(cache.evict_expired().await.unwrap(), 0);
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
    assert!(matches!(
        cache.rename("other").await,
        Err(Error::Unsupported(_))
    ));
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
async fn insert_nx_takes_a_time_limit() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    assert!(cache
        .insert_nx("a", "x")
        .ttl(Duration::from_millis(200))
        .await
        .unwrap());
    assert!(!cache
        .insert_nx("a", "y")
        .ttl(Duration::from_millis(200))
        .await
        .unwrap());
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
    let timeouts = raw_command(&["ZCARD", &format!("redissun__timeout__set:{key}")]).await;
    assert_eq!(timeouts.trim(), ":0");
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
    let latch = format!("redissun__execute_task_once_latch:{}", hash_key(&name));
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
async fn a_value_without_the_idle_prefix_is_read_as_having_no_idle_time() {
    let name = unique("cache");
    let cache = client()
        .await
        .hash_map_cache::<String, String>(name.clone());
    raw_command(&["HSET", &hash_key(&name), "\"k\"", "\"v\""]).await;
    assert_eq!(cache.get("k").await.unwrap(), Some("v".to_string()));
    assert_eq!(cache.len().await.unwrap(), 1);
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
async fn evict_expired_also_removes_orphans_from_the_sets() {
    let name = unique("cache");
    let cache = client()
        .await
        .hash_map_cache::<String, String>(name.clone());
    let timeouts = format!("redissun__timeout__set:{{{name}}}");
    raw_command(&["ZADD", &timeouts, "1", "\"ghost\""]).await;
    assert_eq!(cache.evict_expired().await.unwrap(), 1);
    assert_eq!(raw_command(&["ZCARD", &timeouts]).await.trim(), ":0");
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
async fn try_set_max_size_only_sets_it_once() {
    let cache = client()
        .await
        .hash_map_cache::<String, u32>(unique("cache"));
    assert!(cache.try_set_max_size(2, EvictionMode::Lru).await.unwrap());
    assert!(!cache.try_set_max_size(5, EvictionMode::Lfu).await.unwrap());
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
        Event::Created {
            key: "a:1".into(),
            value: "x:y".into()
        }
    );
    cache.insert("a:1", "z").await.unwrap();
    assert_eq!(
        next(&mut events).await,
        Event::Updated {
            key: "a:1".into(),
            value: "z".into(),
            previous: "x:y".into()
        }
    );
    cache.remove("a:1").await.unwrap();
    assert_eq!(
        next(&mut events).await,
        Event::Removed {
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
    assert!(matches!(next(&mut events).await, Event::Created { .. }));
    sleep(Duration::from_millis(300)).await;
    assert_eq!(cache.evict_expired().await.unwrap(), 1);
    assert_eq!(
        next(&mut events).await,
        Event::Expired {
            key: "short".into(),
            value: "1".into()
        }
    );

    cache.set_max_size(1, EvictionMode::Lru).await.unwrap();
    cache.insert("a", "1").await.unwrap();
    assert!(matches!(next(&mut events).await, Event::Created { .. }));
    pause().await;
    cache.insert("b", "2").await.unwrap();
    sleep(Duration::from_millis(100)).await;
    assert_eq!(
        next(&mut events).await,
        Event::Removed {
            key: "a".into(),
            value: "1".into()
        }
    );
    assert_eq!(
        next(&mut events).await,
        Event::Created {
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
    let mut removals = cache.events_of(&[EventKind::Removed]).await.unwrap();
    cache.insert("a", "1").await.unwrap();
    cache.insert("a", "2").await.unwrap();
    cache.remove("a").await.unwrap();
    assert_eq!(
        next(&mut removals).await,
        Event::Removed {
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
    assert!(matches!(next(&mut first).await, Event::Created { .. }));
    assert!(matches!(next(&mut second).await, Event::Created { .. }));
    drop(first);
    writer.insert("b", "2").await.unwrap();
    assert!(matches!(next(&mut second).await, Event::Created { .. }));
}

#[tokio::test]
async fn deleting_the_cache_stops_the_events_until_somebody_listens_again() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    let mut events = cache.events().await.unwrap();
    cache.insert("a", "1").await.unwrap();
    assert!(matches!(next(&mut events).await, Event::Created { .. }));
    cache.del().await.unwrap();
    cache.insert("b", "2").await.unwrap();
    nothing_arrives(&mut events).await;
    let _again = cache.events().await.unwrap();
    cache.insert("c", "3").await.unwrap();
    assert_eq!(
        next(&mut events).await,
        Event::Created {
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
    let access = format!("redissun__map_cache_last_access__set:{}", hash_key(&name));
    assert_eq!(raw_command(&["ZCARD", &access]).await.trim(), ":2");
    cache.get("b").await.unwrap();
    cache.insert("c", &3u32).await.unwrap();
    assert_eq!(cache.len().await.unwrap(), 2);
    assert!(cache.contains_key("b").await.unwrap());
    assert_eq!(cache.get("a").await.unwrap(), None);
}

#[tokio::test]
async fn racing_try_set_max_size_calls_agree_on_one_winner() {
    let client = client().await;
    let name = unique("cache");
    let tasks: Vec<_> = (1..=10usize)
        .map(|n| {
            let cache = client.hash_map_cache::<String, u32>(name.clone());
            tokio::spawn(async move { cache.try_set_max_size(n, EvictionMode::Lru).await.unwrap() })
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
    let options = format!("redissun__map_cache_options:{{{name}}}");
    assert_eq!(raw_command(&["EXISTS", &options]).await.trim(), ":1");
    cache.del().await.unwrap();
    assert_eq!(raw_command(&["EXISTS", &options]).await.trim(), ":0");
}

#[tokio::test]
async fn test_fast_put_expiration() {
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

#[tokio::test]
async fn test_remove_listener() {
    let cache = client().await.hash_map_cache::<i64, String>(unique("test"));
    cache.try_set_max_size(5, EvictionMode::Lru).await.unwrap();
    let mut removed = cache.events_of(&[EventKind::Removed]).await.unwrap();
    for i in 1..=5i64 {
        cache.insert(&i, &i.to_string()).await.unwrap();
    }
    cache.insert(&6i64, "6").await.unwrap();
    assert!(matches!(next(&mut removed).await, Event::Removed { .. }));
}

#[tokio::test]
async fn test_remain_time_to_live() {
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

#[tokio::test]
async fn test_fast_put_ttl() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("getAll"));
    cache.try_set_max_size(1, EvictionMode::Lru).await.unwrap();
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

#[tokio::test]
async fn test_expiration_with_max_size() {
    let client =
        connect_with(|builder| builder.eviction_interval(Duration::from_millis(200))).await;
    let name = unique("test");
    let cache = client.hash_map_cache::<String, String>(name.clone());
    assert!(cache.try_set_max_size(2, EvictionMode::Lru).await.unwrap());
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
    let access = format!("redissun__map_cache_last_access__set:{}", hash_key(&name));
    assert_eq!(raw_command(&["EXISTS", &access]).await.trim(), ":0");
    let options = format!("redissun__map_cache_options:{}", hash_key(&name));
    assert_eq!(raw_command(&["EXISTS", &options]).await.trim(), ":1");
}

#[tokio::test]
async fn test_max_size_lfu() {
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

#[tokio::test]
async fn test_max_size() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("test"));
    assert!(cache.try_set_max_size(2, EvictionMode::Lru).await.unwrap());
    assert!(!cache.try_set_max_size(1, EvictionMode::Lru).await.unwrap());

    let minute = Duration::from_secs(60);
    assert!(cache.insert_nx("01", "00").await.unwrap());
    assert!(cache.insert_nx("02", "00").await.unwrap());
    assert_eq!(cache.insert("03", "00").await.unwrap(), None);
    assert!(cache.insert_nx("04", "00").ttl(minute).await.unwrap());
    assert_eq!(cache.insert("1", "11").ttl(minute).await.unwrap(), None);
    assert_eq!(cache.len().await.unwrap(), 2);
    assert_eq!(cache.insert("2", "22").ttl(minute).await.unwrap(), None);
    assert_eq!(cache.insert("3", "33").ttl(minute).await.unwrap(), None);
    assert_eq!(cache.len().await.unwrap(), 2);

    assert_eq!(cache.get("2").await.unwrap().as_deref(), Some("22"));
    assert_eq!(cache.get("0").await.unwrap(), None);
    assert!(!cache.insert_nx("2", "3").await.unwrap());
    assert!(!cache
        .insert_nx("3", "4")
        .ttl(minute)
        .max_idle(minute)
        .await
        .unwrap());
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
            .insert_nx(&key, "00")
            .ttl(Duration::from_secs(1))
            .await
            .unwrap();
        pause().await;
    }
    assert_eq!(cache.len().await.unwrap(), 6);
    assert_eq!(keys(&cache).await, names(16..=21));

    for key in names(22..=28) {
        cache.insert_nx(&key, "00").await.unwrap();
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

#[tokio::test]
async fn test_cache_values() {
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

#[tokio::test]
async fn test_expired_iterator() {
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

#[tokio::test]
async fn test_expire() {
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

#[tokio::test]
async fn test_clear_expire() {
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

#[tokio::test]
async fn test_entry_set() {
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

#[tokio::test]
async fn test_key_set() {
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

#[tokio::test]
async fn test_values() {
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

#[tokio::test]
async fn test_contains_key_ttl() {
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

#[tokio::test]
async fn test_scheduler() {
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

#[tokio::test]
async fn test_put_get_ttl() {
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

#[tokio::test]
async fn test_put_if_absent_ttl() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("simple"));
    cache.insert("1", "2").await.unwrap();
    assert!(!cache
        .insert_nx("1", "3")
        .ttl(Duration::from_millis(300))
        .await
        .unwrap());
    assert_eq!(cache.get("1").await.unwrap().as_deref(), Some("2"));
    cache
        .insert_nx("4", "4")
        .ttl(Duration::from_millis(300))
        .await
        .unwrap();
    assert_eq!(cache.get("4").await.unwrap().as_deref(), Some("4"));
    sleep(Duration::from_millis(300)).await;
    assert_eq!(cache.get("4").await.unwrap(), None);
    assert!(cache
        .insert_nx("4", "4")
        .ttl(Duration::from_millis(300))
        .await
        .unwrap());
    assert_eq!(cache.get("4").await.unwrap().as_deref(), Some("4"));
    assert!(cache
        .insert_nx("2", "4")
        .ttl(Duration::from_secs(2))
        .await
        .unwrap());
    assert_eq!(cache.get("2").await.unwrap().as_deref(), Some("4"));
}

#[tokio::test]
async fn test_put_if_absent_ttl_keeps_first_value() {
    let client =
        connect_with(|builder| builder.eviction_interval(Duration::from_millis(200))).await;
    let cache =
        client.hash_map_cache::<String, String>(unique("testPutIfAbsentTTLKeepsFirstValue"));
    let ttl = Duration::from_secs(300);
    assert!(cache.insert_nx("key", "value-1").ttl(ttl).await.unwrap());
    assert!(!cache.insert_nx("key", "value-2").ttl(ttl).await.unwrap());
    sleep(Duration::from_millis(1000)).await;
    assert_eq!(cache.get("key").await.unwrap().as_deref(), Some("value-1"));
    assert!(!cache.insert_nx("key", "value-3").ttl(ttl).await.unwrap());
    let left = cache.entry_ttl("key").await.unwrap().unwrap();
    assert!(left >= Duration::from_secs(290) && left <= ttl);
}

#[tokio::test]
async fn test_fast_put_if_absent_ttl() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("simple"));
    cache.insert("1", "2").await.unwrap();
    assert!(!cache.insert_nx("1", "3").await.unwrap());
    assert_eq!(cache.get("1").await.unwrap().as_deref(), Some("2"));
    assert!(cache.insert_nx("2", "4").await.unwrap());
    assert_eq!(cache.get("2").await.unwrap().as_deref(), Some("4"));
    cache
        .insert("3", "31")
        .ttl(Duration::from_millis(500))
        .await
        .unwrap();
    assert!(!cache.insert_nx("3", "32").await.unwrap());
    sleep(Duration::from_millis(550)).await;
    assert!(cache.insert_nx("3", "32").await.unwrap());
    assert_eq!(cache.get("3").await.unwrap().as_deref(), Some("32"));
}

#[tokio::test]
async fn test_created_listener() {
    let cache = client().await.hash_map_cache::<i32, i32>(unique("simple"));
    let mut created = cache.events_of(&[EventKind::Created]).await.unwrap();
    let ttl = Duration::from_secs(2);
    cache.insert(&1, &2).await.unwrap();
    assert_eq!(
        next(&mut created).await,
        Event::Created { key: 1, value: 2 }
    );
    cache.insert(&10, &2).ttl(ttl).await.unwrap();
    assert_eq!(
        next(&mut created).await,
        Event::Created { key: 10, value: 2 }
    );
    cache.insert_nx(&4, &1).await.unwrap();
    assert_eq!(
        next(&mut created).await,
        Event::Created { key: 4, value: 1 }
    );
    cache.insert_nx(&15, &2).ttl(ttl).await.unwrap();
    assert_eq!(
        next(&mut created).await,
        Event::Created { key: 15, value: 2 }
    );
    nothing_arrives(&mut created).await;
}

#[tokio::test]
async fn test_updated_listener() {
    let cache = client().await.hash_map_cache::<i32, i32>(unique("simple"));
    let mut updated = cache.events_of(&[EventKind::Updated]).await.unwrap();
    cache.insert(&1, &1).await.unwrap();
    cache.insert(&1, &3).await.unwrap();
    assert_eq!(
        next(&mut updated).await,
        Event::Updated {
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
        Event::Updated {
            key: 10,
            value: 2,
            previous: 1
        }
    );
    nothing_arrives(&mut updated).await;
}

#[tokio::test]
async fn test_expired_listener() {
    let client =
        connect_with(|builder| builder.eviction_interval(Duration::from_millis(200))).await;
    let cache = client.hash_map_cache::<i32, i32>(unique("simple"));
    let mut expired = cache.events_of(&[EventKind::Expired]).await.unwrap();
    let ttl = Duration::from_millis(300);
    cache.insert(&10, &2).ttl(ttl).await.unwrap();
    assert_eq!(
        next(&mut expired).await,
        Event::Expired { key: 10, value: 2 }
    );
    cache.insert_nx(&15, &2).ttl(ttl).await.unwrap();
    assert_eq!(
        next(&mut expired).await,
        Event::Expired { key: 15, value: 2 }
    );
}

#[tokio::test]
async fn test_entry_update() {
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

#[tokio::test]
async fn test_removed_listener() {
    let cache = client().await.hash_map_cache::<i32, i32>(unique("simple"));
    let mut removed = cache.events_of(&[EventKind::Removed]).await.unwrap();
    cache.insert(&10, &1).await.unwrap();
    cache.remove(&10).await.unwrap();
    assert_eq!(
        next(&mut removed).await,
        Event::Removed { key: 10, value: 1 }
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

#[tokio::test]
async fn test_idle() {
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
        .insert_nx("12", &1)
        .max_idle(Duration::from_millis(200))
        .await
        .unwrap();
    cache
        .insert_nx("14", &2)
        .max_idle(Duration::from_millis(800))
        .await
        .unwrap();
    cache
        .insert_nx("15", &3)
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

#[tokio::test]
async fn test_ttl() {
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
    cache.insert_nx("12", &1).ttl(ttl(1)).await.unwrap();
    cache.insert_nx("14", &2).ttl(ttl(2)).await.unwrap();
    cache.insert_nx("15", &3).ttl(ttl(3)).await.unwrap();
    check_ttl_expiration(&cache, started).await;
}

#[tokio::test]
async fn test_expire_overwrite() {
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

#[tokio::test]
async fn test_r_map_cache_values() {
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

#[tokio::test]
async fn test_size() {
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

#[tokio::test]
async fn test_contains_key() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("simple"));
    cache.insert("1", "2").await.unwrap();
    cache.insert("33", "44").await.unwrap();
    cache.insert("5", "6").await.unwrap();
    assert!(cache.contains_key("33").await.unwrap());
    assert!(!cache.contains_key("34").await.unwrap());
}

#[tokio::test]
async fn test_put_get() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("simple"));
    cache.insert("1", "2").await.unwrap();
    cache.insert("33", "44").await.unwrap();
    cache.insert("5", "6").await.unwrap();
    assert_eq!(cache.get("33").await.unwrap().as_deref(), Some("44"));
    assert_eq!(cache.get("5").await.unwrap().as_deref(), Some("6"));
}

#[tokio::test]
async fn test_remove() {
    let cache = client().await.hash_map_cache::<i32, i32>(unique("simple"));
    cache.insert(&1, &3).await.unwrap();
    cache.insert(&3, &5).await.unwrap();
    cache.insert(&7, &8).await.unwrap();
    assert_eq!(cache.remove(&1).await.unwrap(), Some(3));
    assert_eq!(cache.remove(&3).await.unwrap(), Some(5));
    assert_eq!(cache.remove(&10).await.unwrap(), None);
    assert_eq!(cache.remove(&7).await.unwrap(), Some(8));
}

#[tokio::test]
async fn test_fast_put_if_absent() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("simple"));
    cache.insert("1", "2").await.unwrap();
    assert!(!cache.insert_nx("1", "3").await.unwrap());
    assert_eq!(cache.get("1").await.unwrap().as_deref(), Some("2"));
    assert!(cache.insert_nx("2", "4").await.unwrap());
    assert_eq!(cache.get("2").await.unwrap().as_deref(), Some("4"));
}

#[tokio::test]
async fn test_iterator() {
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
