mod common;

use common::{client, connect_with, raw_command, unique};
use futures::TryStreamExt;
use redissun::{Error, Event, EvictionMode, Object};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;

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
async fn len_and_iteration_skip_expired_entries() {
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
    assert_eq!(cache.len().await.unwrap(), 1);
    assert!(!cache.is_empty().await.unwrap());
    let entries: HashMap<String, i64> = cache.iter().await.unwrap().try_collect().await.unwrap();
    assert_eq!(entries, HashMap::from([("keep".to_string(), 1)]));
    let keys: Vec<String> = cache.keys().await.unwrap().try_collect().await.unwrap();
    assert_eq!(keys, ["keep"]);
    let values: Vec<i64> = cache.values().await.unwrap().try_collect().await.unwrap();
    assert_eq!(values, [1]);
}

#[tokio::test]
async fn iteration_reads_more_than_one_page() {
    let cache = client().await.hash_map_cache::<u32, u32>(unique("cache"));
    for i in 0..250u32 {
        cache.insert(&i, &i).await.unwrap();
    }
    let entries: HashMap<u32, u32> = cache.iter().await.unwrap().try_collect().await.unwrap();
    assert_eq!(entries.len(), 250);
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
    assert_eq!(
        raw_command(&["HLEN", &format!("{{{name}}}")]).await.trim(),
        ":6"
    );
    assert_eq!(cache.evict_expired().await.unwrap(), 5);
    assert_eq!(
        raw_command(&["HLEN", &format!("{{{name}}}")]).await.trim(),
        ":1"
    );
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
async fn insert_nx_does_not_inherit_a_stale_expiry() {
    let name = unique("cache");
    let cache = client()
        .await
        .hash_map_cache::<String, String>(name.clone());
    raw_command(&[
        "ZADD",
        &format!("redissun__timeout__set:{{{name}}}"),
        "99999999999999",
        "\"k\"",
    ])
    .await;
    assert!(cache.insert_nx("k", "v").await.unwrap());
    assert_eq!(cache.entry_ttl("k").await.unwrap(), None);
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
async fn contains_key_does_not_keep_an_idle_entry_alive() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    cache
        .insert("a", "x")
        .max_idle(Duration::from_millis(500))
        .await
        .unwrap();
    for _ in 0..4 {
        sleep(Duration::from_millis(200)).await;
        let _ = cache.contains_key("a").await.unwrap();
    }
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
    sleep(Duration::from_millis(900)).await;
    let key = format!("{{{name}}}");
    assert_eq!(raw_command(&["HLEN", &key]).await.trim(), ":1");
    let timeouts = raw_command(&["ZCARD", &format!("redissun__timeout__set:{key}")]).await;
    assert_eq!(timeouts.trim(), ":0");
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
    raw_command(&["HSET", &format!("{{{name}}}"), "\"k\"", "\"v\""]).await;
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
async fn a_new_expiring_entry_wakes_up_a_backed_off_evictor() {
    let client = connect_with(|builder| builder.eviction_interval(Duration::from_millis(50))).await;
    let name = unique("cache");
    let cache = client.hash_map_cache::<String, String>(name.clone());
    sleep(Duration::from_millis(2000)).await;
    cache
        .insert("a", "x")
        .ttl(Duration::from_millis(100))
        .await
        .unwrap();
    sleep(Duration::from_millis(700)).await;
    let key = format!("{{{name}}}");
    assert_eq!(raw_command(&["HLEN", &key]).await.trim(), ":0");
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

async fn pause() {
    sleep(Duration::from_millis(15)).await;
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

async fn next(
    events: &mut redissun::Events<String, String, redissun::JsonCodec>,
) -> Event<String, String> {
    tokio::time::timeout(Duration::from_secs(3), events.recv())
        .await
        .expect("no event arrived")
        .unwrap()
}

#[tokio::test]
async fn events_report_created_updated_and_removed_entries() {
    let cache = client()
        .await
        .hash_map_cache::<String, String>(unique("cache"));
    let mut events = cache.events().await.unwrap();
    cache.insert("a:1", "x:y").await.unwrap();
    cache.insert("a:1", "z").await.unwrap();
    cache.remove("a:1").await.unwrap();
    assert_eq!(
        next(&mut events).await,
        Event::Created {
            key: "a:1".into(),
            value: "x:y".into()
        }
    );
    assert_eq!(
        next(&mut events).await,
        Event::Updated {
            key: "a:1".into(),
            value: "z".into(),
            previous: "x:y".into()
        }
    );
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
    pause().await;
    cache.insert("b", "2").await.unwrap();
    assert!(matches!(next(&mut events).await, Event::Created { .. }));
    assert!(matches!(next(&mut events).await, Event::Created { .. }));
    assert_eq!(
        next(&mut events).await,
        Event::Removed {
            key: "a".into(),
            value: "1".into()
        }
    );
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
async fn nothing_is_published_until_somebody_listens() {
    let name = unique("cache");
    let cache = client()
        .await
        .hash_map_cache::<String, String>(name.clone());
    cache.insert("a", "1").await.unwrap();
    let options = raw_command(&[
        "HGET",
        &format!("redissun__map_cache_options:{{{name}}}"),
        "has-listeners",
    ])
    .await;
    assert!(options.starts_with("$-1"), "{options}");
    let _events = cache.events().await.unwrap();
    let options = raw_command(&[
        "HGET",
        &format!("redissun__map_cache_options:{{{name}}}"),
        "has-listeners",
    ])
    .await;
    assert!(options.contains('1'), "{options}");
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
