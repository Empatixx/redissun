mod common;

use common::{client, redis_url, unique};
use futures::TryStreamExt;
use redissun::{
    Client, EvictionPolicy, JsonCodec, LocalCachedMap, Object, ReconnectionStrategy, SyncStrategy,
};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::{sleep, Instant};

type Map = LocalCachedMap<String, String, JsonCodec>;

static GATE: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());

async fn shared() -> tokio::sync::RwLockReadGuard<'static, ()> {
    GATE.read().await
}

async fn exclusive() -> tokio::sync::RwLockWriteGuard<'static, ()> {
    GATE.write().await
}

async fn cached(client: &Client, name: &str) -> Map {
    client
        .local_cached_map::<String, String>(name.to_string())
        .build()
        .await
        .unwrap()
}

async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(5);
    while !check().await {
        assert!(Instant::now() < deadline, "{what}");
        sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn a_read_is_cached_locally() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcm");
    let map = cached(&client, &name).await;
    let plain = client.hash_map::<String, String>(name);
    plain.insert("k", "one").await.unwrap();

    assert_eq!(map.local_len(), 0);
    assert_eq!(map.get("k").await.unwrap().as_deref(), Some("one"));
    assert_eq!(map.local_len(), 1);

    plain.insert("k", "two").await.unwrap();
    assert_eq!(map.get("k").await.unwrap().as_deref(), Some("one"));
    map.clear_local();
    assert_eq!(map.get("k").await.unwrap().as_deref(), Some("two"));
}

#[tokio::test]
async fn a_missing_key_is_not_cached() {
    let _gate = shared().await;
    let map = cached(&client().await, &unique("lcm")).await;
    assert_eq!(map.get("none").await.unwrap(), None);
    assert_eq!(map.local_len(), 0);
}

#[tokio::test]
async fn a_write_updates_the_local_cache_of_the_writer() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcm");
    let map = cached(&client, &name).await;
    let plain = client.hash_map::<String, String>(name);
    assert_eq!(map.insert("k", "one").await.unwrap(), None);
    assert_eq!(
        map.insert("k", "two").await.unwrap().as_deref(),
        Some("one")
    );
    plain.insert("k", "hidden").await.unwrap();
    assert_eq!(map.get("k").await.unwrap().as_deref(), Some("two"));
}

#[tokio::test]
async fn a_write_on_another_instance_invalidates_my_cache() {
    let _gate = shared().await;
    let name = unique("lcm");
    let (first, second) = (client().await, client().await);
    let reader = cached(&first, &name).await;
    let writer = cached(&second, &name).await;
    writer.insert("k", "one").await.unwrap();
    sleep(Duration::from_millis(200)).await;
    assert_eq!(reader.get("k").await.unwrap().as_deref(), Some("one"));
    assert_eq!(reader.local_len(), 1);

    writer.insert("k", "two").await.unwrap();
    eventually("the invalidation never arrived", || async {
        reader.local_len() == 0
    })
    .await;
    assert_eq!(reader.get("k").await.unwrap().as_deref(), Some("two"));
}

#[tokio::test]
async fn removing_on_another_instance_invalidates_my_cache() {
    let _gate = shared().await;
    let name = unique("lcm");
    let (first, second) = (client().await, client().await);
    let reader = cached(&first, &name).await;
    let writer = cached(&second, &name).await;
    writer.insert("k", "one").await.unwrap();
    sleep(Duration::from_millis(200)).await;
    reader.get("k").await.unwrap();
    assert_eq!(writer.remove("k").await.unwrap().as_deref(), Some("one"));
    eventually("the invalidation never arrived", || async {
        reader.local_len() == 0
    })
    .await;
    assert_eq!(reader.get("k").await.unwrap(), None);
}

#[tokio::test]
async fn clearing_on_another_instance_clears_my_cache() {
    let _gate = shared().await;
    let name = unique("lcm");
    let (first, second) = (client().await, client().await);
    let reader = cached(&first, &name).await;
    let writer = cached(&second, &name).await;
    writer.insert("a", "1").await.unwrap();
    writer.insert("b", "2").await.unwrap();
    sleep(Duration::from_millis(200)).await;
    reader.get("a").await.unwrap();
    reader.get("b").await.unwrap();
    assert_eq!(reader.local_len(), 2);
    writer.clear().await.unwrap();
    eventually("the clear never arrived", || async {
        reader.local_len() == 0
    })
    .await;
    assert_eq!(reader.len().await.unwrap(), 0);
}

#[tokio::test]
async fn the_update_strategy_sends_the_new_value() {
    let _gate = shared().await;
    let name = unique("lcm");
    let (first, second) = (client().await, client().await);
    let reader = first
        .local_cached_map::<String, String>(name.clone())
        .sync_strategy(SyncStrategy::Update)
        .build()
        .await
        .unwrap();
    let writer = cached(&second, &name).await;
    drop(writer);
    let writer = second
        .local_cached_map::<String, String>(name.clone())
        .sync_strategy(SyncStrategy::Update)
        .build()
        .await
        .unwrap();
    writer.insert("k", "pushed").await.unwrap();
    eventually("the update never arrived", || async {
        reader.local_len() == 1
    })
    .await;
    let plain = first.hash_map::<String, String>(name);
    plain.insert("k", "hidden").await.unwrap();
    assert_eq!(reader.get("k").await.unwrap().as_deref(), Some("pushed"));
}

#[tokio::test]
async fn the_cache_size_limit_drops_the_least_recently_used() {
    let _gate = shared().await;
    let client = client().await;
    let map = client
        .local_cached_map::<String, String>(unique("lcm"))
        .eviction_policy(EvictionPolicy::Lru)
        .cache_size(2)
        .build()
        .await
        .unwrap();
    for key in ["a", "b", "c"] {
        map.insert(key, "v").await.unwrap();
    }
    assert_eq!(map.local_len(), 2);
    assert_eq!(map.len().await.unwrap(), 3);
}

#[tokio::test]
async fn local_entries_expire_after_their_ttl() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcm");
    let map = client
        .local_cached_map::<String, String>(name.clone())
        .ttl(Duration::from_millis(200))
        .build()
        .await
        .unwrap();
    let plain = client.hash_map::<String, String>(name);
    map.insert("k", "one").await.unwrap();
    plain.insert("k", "two").await.unwrap();
    assert_eq!(map.get("k").await.unwrap().as_deref(), Some("one"));
    sleep(Duration::from_millis(400)).await;
    assert_eq!(map.get("k").await.unwrap().as_deref(), Some("two"));
}

#[tokio::test]
async fn contains_key_and_object_methods() {
    let _gate = shared().await;
    let map = cached(&client().await, &unique("lcm")).await;
    map.insert("k", "v").await.unwrap();
    assert!(map.contains_key("k").await.unwrap());
    assert!(!map.contains_key("other").await.unwrap());
    assert!(map.exists().await.unwrap());
    assert!(map.del().await.unwrap());
}

async fn kill_pubsub_connections() {
    let url = redis_url().await;
    let mut stream = TcpStream::connect(url.trim_start_matches("redis://"))
        .await
        .unwrap();
    stream
        .write_all(b"*4\r\n$6\r\nCLIENT\r\n$4\r\nKILL\r\n$4\r\nTYPE\r\n$6\r\npubsub\r\n")
        .await
        .unwrap();
}

async fn with_reconnection(client: &Client, name: &str, strategy: ReconnectionStrategy) -> Map {
    client
        .local_cached_map::<String, String>(name.to_string())
        .reconnection_strategy(strategy)
        .build()
        .await
        .unwrap()
}

#[tokio::test]
async fn the_clear_strategy_clears_the_cache_after_a_reconnect() {
    let _gate = exclusive().await;
    let client = client().await;
    let map = with_reconnection(&client, &unique("lcm"), ReconnectionStrategy::Clear).await;
    map.insert("k", "v").await.unwrap();
    assert_eq!(map.local_len(), 1);
    kill_pubsub_connections().await;
    eventually("the cache was not cleared", || async {
        map.local_len() == 0
    })
    .await;
    assert_eq!(map.get("k").await.unwrap().as_deref(), Some("v"));
}

#[tokio::test]
async fn the_default_strategy_keeps_the_cache_after_a_reconnect() {
    let _gate = exclusive().await;
    let client = client().await;
    let name = unique("lcm");
    let map = cached(&client, &name).await;
    let probe = with_reconnection(&client, &unique("lcm"), ReconnectionStrategy::Clear).await;
    map.insert("k", "v").await.unwrap();
    probe.insert("p", "v").await.unwrap();
    kill_pubsub_connections().await;
    eventually("the client did not reconnect", || async {
        probe.local_len() == 0
    })
    .await;
    assert_eq!(map.local_len(), 1);
}

#[tokio::test]
async fn the_load_strategy_drops_only_the_keys_changed_while_away() {
    let _gate = exclusive().await;
    let name = unique("lcm");
    let (first, second) = (client().await, client().await);
    let reader = with_reconnection(&first, &name, ReconnectionStrategy::Load).await;
    let writer = with_reconnection(&second, &name, ReconnectionStrategy::Load).await;
    writer.insert("a", "1").await.unwrap();
    writer.insert("b", "1").await.unwrap();
    eventually("no message arrived", || async {
        reader.get("a").await.unwrap().as_deref() == Some("1")
    })
    .await;
    reader.get("b").await.unwrap();
    assert_eq!(reader.local_len(), 2);
    writer.insert("a", "2").await.unwrap();
    eventually("the reader never got a message", || async {
        reader.local_len() == 1
    })
    .await;
    assert_eq!(reader.get("a").await.unwrap().as_deref(), Some("2"));
    assert_eq!(reader.local_len(), 2);
    let log = format!("redissun__cache_updates_log:{{{name}}}");
    assert_eq!(common::raw_command(&["ZCARD", &log]).await.trim(), ":3");

    let plain = second.hash_map::<String, String>(name.clone());
    plain.insert("b", "2").await.unwrap();
    common::raw_command(&[
        "ZADD",
        &log,
        "99999999999999",
        &format!("12345678{}", "\"b\""),
    ])
    .await;
    kill_pubsub_connections().await;
    eventually("b was not dropped", || async {
        reader.get("b").await.unwrap().as_deref() == Some("2")
    })
    .await;
    assert_eq!(reader.get("a").await.unwrap().as_deref(), Some("2"));
}

#[tokio::test]
async fn max_idle_drops_an_entry_nobody_reads() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcm");
    let map = client
        .local_cached_map::<String, String>(name.clone())
        .max_idle(Duration::from_millis(300))
        .build()
        .await
        .unwrap();
    let plain = client.hash_map::<String, String>(name);
    map.insert("k", "one").await.unwrap();
    plain.insert("k", "two").await.unwrap();
    for _ in 0..3 {
        sleep(Duration::from_millis(150)).await;
        assert_eq!(map.get("k").await.unwrap().as_deref(), Some("one"));
    }
    sleep(Duration::from_millis(450)).await;
    assert_eq!(map.get("k").await.unwrap().as_deref(), Some("two"));
}

#[tokio::test]
async fn removing_a_missing_key_or_clearing_an_empty_map_sends_nothing() {
    let _gate = shared().await;
    let name = unique("lcm");
    let (first, second) = (client().await, client().await);
    let reader = cached(&first, &name).await;
    let writer = cached(&second, &name).await;
    let plain = first.hash_map::<String, String>(name.clone());
    plain.insert("k", "v").await.unwrap();
    reader.get("k").await.unwrap();
    plain.remove("k").await.unwrap();
    assert_eq!(writer.remove("k").await.unwrap(), None);
    writer.clear_local();
    assert!(!writer.clear().await.unwrap());
    sleep(Duration::from_millis(300)).await;
    assert_eq!(reader.local_len(), 1);
}

#[tokio::test]
async fn an_lfu_cache_keeps_the_entries_read_most() {
    let _gate = shared().await;
    let map = client()
        .await
        .local_cached_map::<String, String>(unique("lcm"))
        .eviction_policy(EvictionPolicy::Lfu)
        .cache_size(2)
        .build()
        .await
        .unwrap();
    map.insert("a", "1").await.unwrap();
    map.insert("b", "2").await.unwrap();
    map.get("a").await.unwrap();
    map.get("a").await.unwrap();
    map.insert("c", "3").await.unwrap();
    let cached: HashSet<String> = map
        .local_entries()
        .unwrap()
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    assert_eq!(cached, HashSet::from(["a".to_string(), "c".to_string()]));
}

type Ints = LocalCachedMap<String, i32, JsonCodec>;

async fn lfu_pair(
    client: &Client,
    name: &str,
    sync: SyncStrategy,
    reconnection: ReconnectionStrategy,
) -> (Ints, Ints) {
    let build = || {
        client
            .local_cached_map::<String, i32>(name.to_string())
            .eviction_policy(EvictionPolicy::Lfu)
            .cache_size(5)
            .sync_strategy(sync)
            .reconnection_strategy(reconnection)
            .build()
    };
    (build().await.unwrap(), build().await.unwrap())
}

async fn invalidation_test(client: &Client) -> (Ints, Ints) {
    let (map1, map2) = lfu_pair(
        client,
        &unique("test"),
        SyncStrategy::Invalidate,
        ReconnectionStrategy::None,
    )
    .await;
    map1.insert("1", &1).await.unwrap();
    map1.insert("2", &2).await.unwrap();
    sleep(Duration::from_millis(300)).await;
    assert_eq!(map2.get("1").await.unwrap(), Some(1));
    assert_eq!(map2.get("2").await.unwrap(), Some(2));
    assert_eq!(map1.local_len(), 2);
    assert_eq!(map2.local_len(), 2);
    (map1, map2)
}

async fn update_test(client: &Client) -> (Ints, Ints) {
    let (map1, map2) = lfu_pair(
        client,
        &unique("test2"),
        SyncStrategy::Update,
        ReconnectionStrategy::Clear,
    )
    .await;
    map1.insert("1", &1).await.unwrap();
    map1.insert("2", &2).await.unwrap();
    sleep(Duration::from_millis(300)).await;
    assert_eq!(map1.local_len(), 2);
    assert_eq!(map2.local_len(), 2);
    (map1, map2)
}

// Redisson: testUpdateStrategy
#[tokio::test]
async fn a_removed_key_is_gone_with_the_update_strategy() {
    let _gate = shared().await;
    let map = client()
        .await
        .local_cached_map::<String, String>(unique("myMap11"))
        .sync_strategy(SyncStrategy::Update)
        .build()
        .await
        .unwrap();
    map.insert("a", "b").await.unwrap();
    map.remove("a").await.unwrap();
    assert_eq!(map.get("a").await.unwrap(), None);
    assert!(!map.contains_key("a").await.unwrap());
}

// Redisson: testPutAfterDelete
#[tokio::test]
async fn insert_after_clear_returns_the_latest_value() {
    let _gate = shared().await;
    let map = cached(&client().await, &unique("test")).await;
    for _ in 0..1_000 {
        map.clear().await.unwrap();
        map.insert("key", "val1").await.unwrap();
        map.get("key").await.unwrap();
        map.insert("key", "val2").await.unwrap();
        assert_eq!(map.get("key").await.unwrap().as_deref(), Some("val2"));
    }
}

// Redisson: testReadAllValues2
#[tokio::test]
async fn values_are_read_through_a_second_instance_of_the_map() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("test");
    let map1 = cached(&client, &name).await;
    let map2 = cached(&client, &name).await;
    map1.insert("key", "3").await.unwrap();
    let values: Vec<String> = map1.values().try_collect().await.unwrap();
    assert_eq!(values.len(), 1);
    let values: Vec<String> = map2.values().try_collect().await.unwrap();
    assert_eq!(values.len(), 1);
}

// Redisson: testReadValuesAndEntries
#[tokio::test]
async fn values_and_iter_return_all_stored_values_and_entries() {
    let _gate = shared().await;
    let map = client()
        .await
        .local_cached_map::<String, i32>(unique("test"))
        .build()
        .await
        .unwrap();
    map.clear().await.unwrap();
    map.insert("a", &1).await.unwrap();
    map.insert("b", &2).await.unwrap();
    map.insert("c", &3).await.unwrap();
    let values: HashSet<i32> = map.values().try_collect().await.unwrap();
    assert_eq!(values, HashSet::from([1, 2, 3]));
    let entries: HashMap<String, i32> = map.iter().try_collect().await.unwrap();
    assert_eq!(
        entries,
        HashMap::from([("a".into(), 1), ("b".into(), 2), ("c".into(), 3)])
    );
}

// Redisson: testClearEmpty
#[tokio::test]
async fn clearing_an_empty_map_succeeds() {
    let _gate = shared().await;
    let map = cached(&client().await, &unique("test")).await;
    map.clear().await.unwrap();
}

// Redisson: testDelete
#[tokio::test]
async fn clear_returns_whether_the_map_existed() {
    let _gate = shared().await;
    let map = cached(&client().await, &unique("test")).await;
    assert!(!map.clear().await.unwrap());
    map.insert("1", "2").await.unwrap();
    assert!(map.clear().await.unwrap());
}

// Redisson: testInvalidationOnClear
#[tokio::test]
async fn clear_empties_the_local_cache_of_every_instance() {
    let _gate = shared().await;
    let client = client().await;
    let (map1, map2) = invalidation_test(&client).await;
    map1.clear().await.unwrap();
    sleep(Duration::from_millis(300)).await;
    assert_eq!(map1.local_len(), 0);
    assert_eq!(map2.local_len(), 0);
    assert_eq!(map1.len().await.unwrap(), 0);
    assert_eq!(map2.len().await.unwrap(), 0);
}

// Redisson: testInvalidationOnUpdateNonBinaryCodec
#[tokio::test]
async fn an_update_invalidates_the_key_in_the_other_instance() {
    let _gate = shared().await;
    let client = client().await;
    let (map1, map2) = invalidation_test(&client).await;
    map1.insert("1", &3).await.unwrap();
    map2.insert("2", &4).await.unwrap();
    sleep(Duration::from_millis(300)).await;
    assert_eq!(map1.local_len(), 1);
    assert_eq!(map2.local_len(), 1);
}

// Redisson: testSyncOnUpdate
#[tokio::test]
async fn invalidate_drops_and_update_keeps_entries_updated_elsewhere() {
    let _gate = shared().await;
    let client = client().await;
    let (map1, map2) = invalidation_test(&client).await;
    map1.insert("1", &3).await.unwrap();
    map2.insert("2", &4).await.unwrap();
    sleep(Duration::from_millis(300)).await;
    assert_eq!(map1.local_len(), 1);
    assert_eq!(map2.local_len(), 1);

    let (map1, map2) = update_test(&client).await;
    map1.insert("1", &3).await.unwrap();
    map2.insert("2", &4).await.unwrap();
    sleep(Duration::from_millis(300)).await;
    assert_eq!(map1.local_len(), 2);
    assert_eq!(map2.local_len(), 2);
}

// Redisson: testNoInvalidationOnUpdate
#[tokio::test]
async fn without_sync_an_update_keeps_the_other_cache_intact() {
    let _gate = shared().await;
    let client = client().await;
    let (map1, map2) = lfu_pair(
        &client,
        &unique("test"),
        SyncStrategy::None,
        ReconnectionStrategy::None,
    )
    .await;
    map1.insert("1", &1).await.unwrap();
    map1.insert("2", &2).await.unwrap();
    assert_eq!(map2.get("1").await.unwrap(), Some(1));
    assert_eq!(map2.get("2").await.unwrap(), Some(2));
    sleep(Duration::from_millis(300)).await;
    assert_eq!(map1.local_len(), 2);
    assert_eq!(map2.local_len(), 2);
    map1.insert("1", &3).await.unwrap();
    map2.insert("2", &4).await.unwrap();
    sleep(Duration::from_millis(300)).await;
    assert_eq!(map1.local_len(), 2);
    assert_eq!(map2.local_len(), 2);
}

// Redisson: testLocalCacheState
#[tokio::test]
async fn local_entries_returns_the_locally_cached_entries() {
    let _gate = shared().await;
    let map = client()
        .await
        .local_cached_map::<String, String>(unique("test"))
        .eviction_policy(EvictionPolicy::Lfu)
        .cache_size(5)
        .build()
        .await
        .unwrap();
    map.insert("1", "11").await.unwrap();
    map.insert("2", "22").await.unwrap();
    let cached: HashMap<String, String> = map.local_entries().unwrap().into_iter().collect();
    assert_eq!(
        cached,
        HashMap::from([("1".into(), "11".into()), ("2".into(), "22".into())])
    );
}

// Redisson: testLocalCacheClear
#[tokio::test]
async fn clear_local_everywhere_empties_caches_but_keeps_the_map() {
    let _gate = shared().await;
    let name = unique("test");
    let (first, second) = (client().await, client().await);
    let (map1, _) = lfu_pair(
        &first,
        &name,
        SyncStrategy::Invalidate,
        ReconnectionStrategy::None,
    )
    .await;
    let (map2, _) = lfu_pair(
        &second,
        &name,
        SyncStrategy::Invalidate,
        ReconnectionStrategy::None,
    )
    .await;
    map1.insert("1", &1).await.unwrap();
    map1.insert("2", &2).await.unwrap();
    sleep(Duration::from_millis(300)).await;
    assert_eq!(map2.get("1").await.unwrap(), Some(1));
    assert_eq!(map2.get("2").await.unwrap(), Some(2));
    assert_eq!(map1.local_len(), 2);
    assert_eq!(map2.local_len(), 2);
    map1.clear_local_everywhere().await.unwrap();
    sleep(Duration::from_millis(100)).await;
    assert!(map1.exists().await.unwrap());
    assert_eq!(map1.local_len(), 0);
    assert_eq!(map2.local_len(), 0);
}

// Redisson: testNoInvalidationOnRemove
#[tokio::test]
async fn without_sync_a_remove_drops_only_the_local_entry() {
    let _gate = shared().await;
    let client = client().await;
    let (map1, map2) = lfu_pair(
        &client,
        &unique("test"),
        SyncStrategy::None,
        ReconnectionStrategy::None,
    )
    .await;
    map1.insert("1", &1).await.unwrap();
    map1.insert("2", &2).await.unwrap();
    assert_eq!(map2.get("1").await.unwrap(), Some(1));
    assert_eq!(map2.get("2").await.unwrap(), Some(2));
    assert_eq!(map1.local_len(), 2);
    assert_eq!(map2.local_len(), 2);
    map1.remove("1").await.unwrap();
    map2.remove("2").await.unwrap();
    sleep(Duration::from_millis(300)).await;
    assert_eq!(map1.local_len(), 1);
    assert_eq!(map2.local_len(), 1);
}

// Redisson: testSyncOnRemove
#[tokio::test]
async fn a_remove_drops_the_key_from_every_cache_with_invalidate_and_update() {
    let _gate = shared().await;
    let client = client().await;
    let (map1, map2) = invalidation_test(&client).await;
    map1.remove("1").await.unwrap();
    map2.remove("2").await.unwrap();
    sleep(Duration::from_millis(300)).await;
    assert_eq!(map1.local_len(), 0);
    assert_eq!(map2.local_len(), 0);

    let (map1, map2) = update_test(&client).await;
    map1.remove("1").await.unwrap();
    map2.remove("2").await.unwrap();
    sleep(Duration::from_millis(300)).await;
    assert_eq!(map1.local_len(), 0);
    assert_eq!(map2.local_len(), 0);
}

async fn check_bounded(policy: EvictionPolicy) {
    let map = client()
        .await
        .local_cached_map::<String, i32>(unique("test"))
        .eviction_policy(policy)
        .cache_size(5)
        .build()
        .await
        .unwrap();
    for (key, value) in [
        ("12", 1),
        ("14", 2),
        ("15", 3),
        ("16", 4),
        ("17", 5),
        ("18", 6),
    ] {
        map.insert(key, &value).await.unwrap();
    }
    assert_eq!(map.local_len(), 5);
    assert_eq!(map.len().await.unwrap(), 6);
    let keys: HashSet<String> = map.keys().try_collect().await.unwrap();
    assert_eq!(keys.len(), 6);
    let values: HashSet<i32> = map.values().try_collect().await.unwrap();
    assert_eq!(values, (1..=6).collect());
}

// Redisson: testLfu
#[tokio::test]
async fn an_lfu_cache_holds_at_most_its_size_while_redis_keeps_all() {
    let _gate = shared().await;
    check_bounded(EvictionPolicy::Lfu).await;
}

// Redisson: testLru
#[tokio::test]
async fn an_lru_cache_holds_at_most_its_size_while_redis_keeps_all() {
    let _gate = shared().await;
    check_bounded(EvictionPolicy::Lru).await;
}

// Redisson: testSizeCache
#[tokio::test]
async fn local_len_reports_three_entries_with_a_cache_size_of_two() {
    let _gate = shared().await;
    let map = client()
        .await
        .local_cached_map::<String, i32>(unique("test"))
        .cache_size(2)
        .build()
        .await
        .unwrap();
    map.insert("12", &1).await.unwrap();
    map.insert("14", &2).await.unwrap();
    map.insert("15", &3).await.unwrap();
    assert_eq!(map.local_len(), 3);
    assert_eq!(map.len().await.unwrap(), 3);
}

// Redisson: testInvalidationOnPut
#[tokio::test]
async fn an_insert_invalidates_the_keys_cached_by_the_other_instance() {
    let _gate = shared().await;
    let client = client().await;
    let (map1, map2) = invalidation_test(&client).await;
    map1.insert("1", &10).await.unwrap();
    map1.insert("2", &20).await.unwrap();
    sleep(Duration::from_millis(300)).await;
    assert_eq!(map1.local_len(), 2);
    assert_eq!(map2.local_len(), 0);
}

// Redisson: testPutGetCache
#[tokio::test]
async fn inserted_values_are_cached_and_readable_from_another_instance() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("test");
    let map = client
        .local_cached_map::<String, i32>(name.clone())
        .build()
        .await
        .unwrap();
    map.insert("12", &1).await.unwrap();
    map.insert("14", &2).await.unwrap();
    map.insert("15", &3).await.unwrap();
    let cached: HashSet<i32> = map
        .local_entries()
        .unwrap()
        .into_iter()
        .map(|(_, v)| v)
        .collect();
    assert_eq!(cached, HashSet::from([1, 2, 3]));
    assert_eq!(map.get("12").await.unwrap(), Some(1));
    assert_eq!(map.get("14").await.unwrap(), Some(2));
    assert_eq!(map.get("15").await.unwrap(), Some(3));
    let other = client
        .local_cached_map::<String, i32>(name)
        .build()
        .await
        .unwrap();
    assert_eq!(other.get("12").await.unwrap(), Some(1));
    assert_eq!(other.get("14").await.unwrap(), Some(2));
    assert_eq!(other.get("15").await.unwrap(), Some(3));
}

// Redisson: testGetStoringCacheMiss
#[tokio::test]
async fn a_stored_cache_miss_counts_locally_but_is_not_an_entry() {
    let _gate = shared().await;
    let map = client()
        .await
        .local_cached_map::<String, i32>(unique("test"))
        .store_cache_miss(true)
        .build()
        .await
        .unwrap();
    assert_eq!(map.get("19").await.unwrap(), None);
    assert_eq!(map.local_len(), 1);
    assert!(!map.contains_key("19").await.unwrap());
    assert!(map.local_entries().unwrap().is_empty());
}

// Redisson: testGetNotStoringCacheMiss
#[tokio::test]
async fn a_cache_miss_is_not_stored_when_store_cache_miss_is_off() {
    let _gate = shared().await;
    let map = client()
        .await
        .local_cached_map::<String, i32>(unique("test"))
        .store_cache_miss(false)
        .build()
        .await
        .unwrap();
    assert_eq!(map.get("19").await.unwrap(), None);
    assert_eq!(map.local_len(), 0);
}

// Redisson: testGetBeforePut
#[tokio::test]
async fn an_insert_after_a_get_returns_the_new_value_on_read() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("test");
    let map1 = cached(&client, &name).await;
    for i in 0..300 {
        map1.insert(&format!("key{i}"), "val").await.unwrap();
    }
    let map2 = cached(&client, &name).await;
    for i in 0..300 {
        let key = format!("key{i}");
        map2.get(&key).await.unwrap();
        map2.insert(&key, &format!("value{i}")).await.unwrap();
        assert_eq!(map2.get(&key).await.unwrap(), Some(format!("value{i}")));
    }
}

// Redisson: testReadAllEntrySet
#[tokio::test]
async fn iter_returns_all_entries_and_caches_them_locally() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("test");
    let map = cached(&client, &name).await;
    map.insert("1", "2").await.unwrap();
    map.insert("33", "44").await.unwrap();
    map.insert("5", "6").await.unwrap();
    let entries: HashMap<String, String> = map.iter().try_collect().await.unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(map.local_len(), 3);
    let other = cached(&client, &name).await;
    let again: HashMap<String, String> = other.iter().try_collect().await.unwrap();
    assert_eq!(again, entries);
}

// Redisson: testRemove
#[tokio::test]
async fn remove_returns_the_old_value_and_drops_it_from_the_cache() {
    let _gate = shared().await;
    let map = client()
        .await
        .local_cached_map::<String, i32>(unique("test"))
        .build()
        .await
        .unwrap();
    map.insert("12", &1).await.unwrap();
    assert_eq!(map.local_len(), 1);
    assert_eq!(map.remove("12").await.unwrap(), Some(1));
    assert_eq!(map.local_len(), 0);
    assert_eq!(map.remove("14").await.unwrap(), None);
}

// Redisson: testFastPut
#[tokio::test]
async fn insert_returns_the_previous_value_and_overwrites_it() {
    let _gate = shared().await;
    let map = client()
        .await
        .local_cached_map::<String, i32>(unique("test"))
        .build()
        .await
        .unwrap();
    assert_eq!(map.insert("1", &2).await.unwrap(), None);
    assert_eq!(map.get("1").await.unwrap(), Some(2));
    assert_eq!(map.insert("1", &3).await.unwrap(), Some(2));
    assert_eq!(map.get("1").await.unwrap(), Some(3));
    assert_eq!(map.len().await.unwrap(), 1);
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

type Simple = LocalCachedMap<SimpleKey, SimpleValue, JsonCodec>;

async fn simple(client: &Client, name: &str) -> Simple {
    client
        .local_cached_map::<SimpleKey, SimpleValue>(name.to_string())
        .build()
        .await
        .unwrap()
}

async fn simple_with_three_entries(client: &Client, name: &str) -> Simple {
    let map = simple(client, name).await;
    for (key, value) in [("1", "2"), ("33", "44"), ("5", "6")] {
        map.insert(&simple_key(key), &simple_value(value))
            .await
            .unwrap();
    }
    map
}

#[tokio::test]
async fn del_on_one_instance_clears_the_cache_of_the_others_and_removes_the_updates_log() {
    let _gate = shared().await;
    let name = unique("lcm");
    let (first, second) = (client().await, client().await);
    let a = with_reconnection(&first, &name, ReconnectionStrategy::Load).await;
    let b = with_reconnection(&second, &name, ReconnectionStrategy::Load).await;
    a.insert("k", "v").await.unwrap();
    sleep(Duration::from_millis(200)).await;
    assert_eq!(b.get("k").await.unwrap().as_deref(), Some("v"));
    assert_eq!(a.local_len(), 1);
    assert_eq!(b.local_len(), 1);
    let log = format!("redissun__cache_updates_log:{{{name}}}");
    assert_eq!(common::raw_command(&["EXISTS", &log]).await.trim(), ":1");

    assert!(a.del().await.unwrap());
    assert_eq!(a.local_len(), 0);
    eventually("the clear never arrived", || async { b.local_len() == 0 }).await;
    assert_eq!(common::raw_command(&["EXISTS", &log]).await.trim(), ":0");
    assert_eq!(common::raw_command(&["EXISTS", &name]).await.trim(), ":0");
    assert_eq!(b.get("k").await.unwrap(), None);
    assert!(!a.del().await.unwrap());
}

#[tokio::test]
async fn del_with_the_default_options_clears_the_cache_of_the_others() {
    let _gate = shared().await;
    let name = unique("lcm");
    let (first, second) = (client().await, client().await);
    let a = cached(&first, &name).await;
    let b = cached(&second, &name).await;
    a.insert("k", "v").await.unwrap();
    sleep(Duration::from_millis(200)).await;
    assert_eq!(b.get("k").await.unwrap().as_deref(), Some("v"));
    assert_eq!(b.local_len(), 1);
    assert!(a.del().await.unwrap());
    eventually("the clear never arrived", || async { b.local_len() == 0 }).await;
    assert_eq!(b.get("k").await.unwrap(), None);
}

#[tokio::test]
async fn an_expired_map_clears_the_cache_of_every_instance() {
    let _gate = shared().await;
    common::raw_command(&["CONFIG", "SET", "notify-keyspace-events", "EKx"]).await;
    let name = unique("lcm");
    let (first, second) = (client().await, client().await);
    let a = cached(&first, &name).await;
    let b = cached(&second, &name).await;
    let other = cached(&first, &unique("lcm")).await;
    a.insert("k", "v").await.unwrap();
    other.insert("k", "v").await.unwrap();
    sleep(Duration::from_millis(200)).await;
    assert_eq!(b.get("k").await.unwrap().as_deref(), Some("v"));
    assert!(a.expire(Duration::from_millis(300)).await.unwrap());
    eventually("the caches were not cleared", || async {
        a.local_len() == 0 && b.local_len() == 0
    })
    .await;
    assert_eq!(other.local_len(), 1);
}

// Redisson: testExpiration
#[tokio::test]
async fn an_expired_map_drops_its_cached_entries() {
    let _gate = shared().await;
    common::raw_command(&["CONFIG", "SET", "notify-keyspace-events", "EKx"]).await;
    let map = cached(&client().await, &unique("test")).await;
    map.insert("12", "32").await.unwrap();
    assert_eq!(map.local_entries().unwrap().len(), 1);
    assert!(map.expire(Duration::from_secs(1)).await.unwrap());
    sleep(Duration::from_millis(1500)).await;
    assert_eq!(map.local_entries().unwrap().len(), 0);
    assert_eq!(map.get("12").await.unwrap(), None);
}

// Redisson: testReadAllValues
#[tokio::test]
async fn values_match_the_entries_on_this_and_another_instance() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("test");
    let map = simple_with_three_entries(&client, &name).await;
    assert_eq!(map.local_len(), 3);

    let values: Vec<SimpleValue> = map.values().try_collect().await.unwrap();
    assert_eq!(values.len(), 3);
    let expected: HashMap<SimpleKey, SimpleValue> = map.iter().try_collect().await.unwrap();
    assert_eq!(expected.len(), 3);
    assert!(expected.values().all(|value| values.contains(value)));

    let map2 = simple(&client, &name).await;
    let values2: Vec<SimpleValue> = map2.values().try_collect().await.unwrap();
    assert!(expected.values().all(|value| values2.contains(value)));
}

// Redisson: testInteger
#[tokio::test]
async fn i32_keys_and_values_round_trip() {
    let _gate = shared().await;
    let map = client()
        .await
        .local_cached_map::<i32, i32>(unique("test_int"))
        .build()
        .await
        .unwrap();
    map.insert(&1, &2).await.unwrap();
    map.insert(&3, &4).await.unwrap();
    assert_eq!(map.len().await.unwrap(), 2);
    assert_eq!(map.get(&1).await.unwrap(), Some(2));
    assert_eq!(map.get(&3).await.unwrap(), Some(4));
}

// Redisson: testLong
#[tokio::test]
async fn i64_keys_and_values_round_trip() {
    let _gate = shared().await;
    let map = client()
        .await
        .local_cached_map::<i64, i64>(unique("test_long"))
        .build()
        .await
        .unwrap();
    map.insert(&1, &2).await.unwrap();
    map.insert(&3, &4).await.unwrap();
    assert_eq!(map.len().await.unwrap(), 2);
    assert_eq!(map.get(&1).await.unwrap(), Some(2));
    assert_eq!(map.get(&3).await.unwrap(), Some(4));
}

// Redisson: testIterator
#[tokio::test]
async fn keys_values_and_iter_return_all_thousand_entries() {
    let _gate = shared().await;
    let map = client()
        .await
        .local_cached_map::<i32, i32>(unique("123"))
        .build()
        .await
        .unwrap();
    let size = 1000;
    for i in 0..size {
        map.insert(&i, &i).await.unwrap();
    }
    assert_eq!(map.len().await.unwrap(), 1000);
    let keys: Vec<i32> = map.keys().try_collect().await.unwrap();
    assert_eq!(keys.len(), size as usize);
    let values: Vec<i32> = map.values().try_collect().await.unwrap();
    assert_eq!(values.len(), size as usize);
    let entries: Vec<(i32, i32)> = map.iter().try_collect().await.unwrap();
    assert_eq!(entries.len(), size as usize);
}

// Redisson: testSize
#[tokio::test]
async fn len_counts_distinct_keys_across_overwrites_and_removes() {
    let _gate = shared().await;
    let map = simple(&client().await, &unique("simple")).await;
    let put = |key: &'static str, value: &'static str| {
        let map = &map;
        async move {
            map.insert(&simple_key(key), &simple_value(value))
                .await
                .unwrap();
        }
    };
    put("1", "2").await;
    put("3", "4").await;
    put("5", "6").await;
    assert_eq!(map.len().await.unwrap(), 3);

    put("1", "2").await;
    put("3", "4").await;
    assert_eq!(map.len().await.unwrap(), 3);

    put("1", "21").await;
    put("3", "41").await;
    assert_eq!(map.len().await.unwrap(), 3);

    put("51", "6").await;
    assert_eq!(map.len().await.unwrap(), 4);

    map.remove(&simple_key("3")).await.unwrap();
    assert_eq!(map.len().await.unwrap(), 3);
}

// Redisson: testPutGet
#[tokio::test]
async fn get_returns_the_struct_value_stored_for_a_struct_key() {
    let _gate = shared().await;
    let map = simple_with_three_entries(&client().await, &unique("simple")).await;
    let val1 = map.get(&simple_key("33")).await.unwrap().unwrap();
    assert_eq!(val1.value, "44");
    let val2 = map.get(&simple_key("5")).await.unwrap().unwrap();
    assert_eq!(val2.value, "6");
}

// Redisson: testContainsKey
#[tokio::test]
async fn contains_key_is_true_only_for_stored_struct_keys() {
    let _gate = shared().await;
    let map = simple_with_three_entries(&client().await, &unique("simple")).await;
    assert!(map.contains_key(&simple_key("33")).await.unwrap());
    assert!(!map.contains_key(&simple_key("34")).await.unwrap());
}

// Redisson: testRemoveObject
#[tokio::test]
async fn remove_returns_the_struct_value_or_none_for_a_missing_key() {
    let _gate = shared().await;
    let map = simple_with_three_entries(&client().await, &unique("simple")).await;
    assert_eq!(
        map.remove(&simple_key("33")).await.unwrap(),
        Some(simple_value("44"))
    );
    assert_eq!(
        map.remove(&simple_key("5")).await.unwrap(),
        Some(simple_value("6"))
    );
    assert_eq!(map.remove(&simple_key("11")).await.unwrap(), None);
    assert_eq!(map.len().await.unwrap(), 1);
}

// Redisson: testValueIterator
#[tokio::test]
async fn values_returns_each_value_exactly_once() {
    let _gate = shared().await;
    let map = client()
        .await
        .local_cached_map::<i32, i32>(unique("simple"))
        .build()
        .await
        .unwrap();
    for (key, value) in [(1, 0), (3, 5), (4, 6), (7, 8)] {
        map.insert(&key, &value).await.unwrap();
    }
    let mut values: Vec<i32> = map.values().try_collect().await.unwrap();
    assert_eq!(
        values.iter().copied().collect::<HashSet<i32>>(),
        HashSet::from([0, 5, 6, 8])
    );
    let again: Vec<i32> = map.values().try_collect().await.unwrap();
    for value in again {
        let position = values.iter().position(|candidate| *candidate == value);
        values.remove(position.unwrap_or_else(|| panic!("unable to remove value {value}")));
    }
    assert_eq!(values.len(), 0);
}

// Redisson: testEquals
#[tokio::test]
async fn iter_returns_exactly_the_inserted_entries() {
    let _gate = shared().await;
    let map = cached(&client().await, &unique("simple")).await;
    map.insert("1", "7").await.unwrap();
    map.insert("2", "4").await.unwrap();
    map.insert("3", "5").await.unwrap();
    let expected: HashMap<String, String> = HashMap::from([
        ("1".into(), "7".into()),
        ("2".into(), "4".into()),
        ("3".into(), "5".into()),
    ]);
    let entries: HashMap<String, String> = map.iter().try_collect().await.unwrap();
    assert_eq!(entries, expected);
}

// Redisson: testReadAllKeySet
#[tokio::test]
async fn keys_returns_every_key_of_the_map() {
    let _gate = shared().await;
    let map = simple_with_three_entries(&client().await, &unique("simple")).await;
    let keys: Vec<SimpleKey> = map.keys().try_collect().await.unwrap();
    assert_eq!(keys.len(), 3);
    let entries: HashMap<SimpleKey, SimpleValue> = map.iter().try_collect().await.unwrap();
    assert!(keys.iter().all(|key| entries.contains_key(key)));
}

// Redisson: testReadAllKeySetHighAmount
#[tokio::test]
async fn keys_returns_all_thousand_keys_of_a_large_map() {
    let _gate = shared().await;
    let map = simple(&client().await, &unique("simple")).await;
    for i in 0..1000 {
        let text = i.to_string();
        map.insert(&simple_key(&text), &simple_value(&text))
            .await
            .unwrap();
    }
    let keys: Vec<SimpleKey> = map.keys().try_collect().await.unwrap();
    assert_eq!(keys.len(), 1000);
    let entries: HashMap<SimpleKey, SimpleValue> = map.iter().try_collect().await.unwrap();
    assert!(keys.iter().all(|key| entries.contains_key(key)));
}

#[tokio::test]
async fn an_expired_map_still_clears_the_cache_after_a_reconnect() {
    let _gate = exclusive().await;
    common::raw_command(&["CONFIG", "SET", "notify-keyspace-events", "EKx"]).await;
    let client = client().await;
    let map = cached(&client, &unique("lcm")).await;
    let probe = with_reconnection(&client, &unique("lcm"), ReconnectionStrategy::Clear).await;
    map.insert("k", "v").await.unwrap();
    probe.insert("p", "v").await.unwrap();
    kill_pubsub_connections().await;
    eventually("the client did not reconnect", || async {
        probe.local_len() == 0
    })
    .await;
    assert_eq!(map.local_len(), 1);
    assert!(map.expire(Duration::from_millis(300)).await.unwrap());
    eventually("the cache was not cleared", || async {
        map.local_len() == 0
    })
    .await;
    assert_eq!(map.get("k").await.unwrap(), None);
}
