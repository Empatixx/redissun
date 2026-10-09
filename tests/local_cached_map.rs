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
    let log = format!("redissun__cache_updates_log:{{{name}}}");
    assert_eq!(common::raw_command(&["ZCARD", &log]).await.trim(), ":2");

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
    eventually("b was not dropped", || async { reader.local_len() == 1 }).await;
    assert_eq!(reader.get("a").await.unwrap().as_deref(), Some("1"));
    assert_eq!(reader.get("b").await.unwrap().as_deref(), Some("2"));
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
        .cached_entries()
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

#[tokio::test]
async fn test_update_strategy() {
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

#[tokio::test]
async fn test_put_after_delete() {
    let _gate = shared().await;
    let map = cached(&client().await, &unique("test")).await;
    for _ in 0..200 {
        map.clear().await.unwrap();
        map.insert("key", "val1").await.unwrap();
        map.get("key").await.unwrap();
        map.insert("key", "val2").await.unwrap();
        assert_eq!(map.get("key").await.unwrap().as_deref(), Some("val2"));
    }
}

#[tokio::test]
async fn test_read_all_values2() {
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

#[tokio::test]
async fn test_read_values_and_entries() {
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

#[tokio::test]
async fn test_clear_empty() {
    let _gate = shared().await;
    let map = cached(&client().await, &unique("test")).await;
    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_delete() {
    let _gate = shared().await;
    let map = cached(&client().await, &unique("test")).await;
    assert!(!map.clear().await.unwrap());
    map.insert("1", "2").await.unwrap();
    assert!(map.clear().await.unwrap());
}

#[tokio::test]
async fn test_invalidation_on_clear() {
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

#[tokio::test]
async fn test_invalidation_on_update_non_binary_codec() {
    let _gate = shared().await;
    let client = client().await;
    let (map1, map2) = invalidation_test(&client).await;
    map1.insert("1", &3).await.unwrap();
    map2.insert("2", &4).await.unwrap();
    sleep(Duration::from_millis(300)).await;
    assert_eq!(map1.local_len(), 1);
    assert_eq!(map2.local_len(), 1);
}

#[tokio::test]
async fn test_sync_on_update() {
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

#[tokio::test]
async fn test_no_invalidation_on_update() {
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

#[tokio::test]
async fn test_local_cache_state() {
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
    let cached: HashMap<String, String> = map.cached_entries().unwrap().into_iter().collect();
    assert_eq!(
        cached,
        HashMap::from([("1".into(), "11".into()), ("2".into(), "22".into())])
    );
}

#[tokio::test]
async fn test_local_cache_clear() {
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

#[tokio::test]
async fn test_no_invalidation_on_remove() {
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

#[tokio::test]
async fn test_sync_on_remove() {
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

#[tokio::test]
async fn test_lfu() {
    let _gate = shared().await;
    check_bounded(EvictionPolicy::Lfu).await;
}

#[tokio::test]
async fn test_lru() {
    let _gate = shared().await;
    check_bounded(EvictionPolicy::Lru).await;
}

#[tokio::test]
async fn test_size_cache() {
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

#[tokio::test]
async fn test_invalidation_on_put() {
    let _gate = shared().await;
    let client = client().await;
    let (map1, map2) = invalidation_test(&client).await;
    map1.insert("1", &10).await.unwrap();
    map1.insert("2", &20).await.unwrap();
    sleep(Duration::from_millis(300)).await;
    assert_eq!(map1.local_len(), 2);
    assert_eq!(map2.local_len(), 0);
}

#[tokio::test]
async fn test_put_get_cache() {
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
        .cached_entries()
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

#[tokio::test]
async fn test_get_storing_cache_miss() {
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
    assert!(map.cached_entries().unwrap().is_empty());
}

#[tokio::test]
async fn test_get_not_storing_cache_miss() {
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

#[tokio::test]
async fn test_get_before_put() {
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

#[tokio::test]
async fn test_read_all_entry_set() {
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

#[tokio::test]
async fn test_remove() {
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

#[tokio::test]
async fn test_fast_put() {
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
