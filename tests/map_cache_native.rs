//! Tests of `MapCacheNative`, Redisson's `RMapCacheNative`.
//!
//! Ported from Redisson 4.8.0 `RedissonMapCacheNativeTest` (named `redisson_<snake_case>`): filled in as the methods arrive.

mod common;

use common::{client, own_redis_tag, raw_command, unique};
use futures::TryStreamExt;
use redissun::{Client, Error, JsonCodec, MapCacheNative, MapScan, Object};
use std::collections::HashMap;

type Native<K, V> = MapCacheNative<K, V, JsonCodec>;

async fn native(name: &str) -> Native<String, String> {
    client().await.map_cache_native(name.to_string())
}

// Review Focus 1: the docs of every write rest on this server behaviour.
#[tokio::test]
async fn the_server_clears_a_field_ttl_on_hset_and_keeps_it_on_hincrby() {
    let h = unique("mcn-contract");
    assert_eq!(raw_command(&["HSET", &h, "f", "1"]).await, ":1\r\n");
    assert_eq!(
        raw_command(&["HPEXPIRE", &h, "60000", "FIELDS", "1", "f"]).await,
        "*1\r\n:1\r\n"
    );
    raw_command(&["HSET", &h, "f", "2"]).await;
    assert_eq!(
        raw_command(&["HPTTL", &h, "FIELDS", "1", "f"]).await,
        "*1\r\n:-1\r\n"
    );
    raw_command(&["HPEXPIRE", &h, "60000", "FIELDS", "1", "f"]).await;
    assert_eq!(raw_command(&["HINCRBY", &h, "f", "1"]).await, ":3\r\n");
    let ttl = raw_command(&["HPTTL", &h, "FIELDS", "1", "f"]).await;
    let ms: i64 = ttl.trim_start_matches("*1\r\n:").trim().parse().unwrap();
    assert!(ms > 0, "HINCRBY dropped the TTL: {ttl:?}");
    raw_command(&["DEL", &h]).await;
}

#[tokio::test]
async fn rmap_methods_work_like_hash_map() {
    let client = client().await;
    let base = unique("mcn-rmap");
    let native: Native<String, String> = client.map_cache_native(format!("{base}:native"));
    let plain = client.hash_map::<String, String>(format!("{base}:plain"));
    let counters_native: Native<String, i64> = client.map_cache_native(format!("{base}:nn"));
    let counters_plain = client.hash_map::<String, i64>(format!("{base}:pn"));

    // insert and set arrive in Task 2; a HashMap on the same name writes the data.
    let native_writer = client.hash_map::<String, String>(format!("{base}:native"));
    let data = [("k1", "a"), ("k10", "b"), ("k2", "c"), ("k3", "d")];
    for (k, v) in data {
        native_writer.set(k, v).await.unwrap();
        plain.set(k, v).await.unwrap();
    }
    assert_eq!(
        native.get("k1").await.unwrap(),
        plain.get("k1").await.unwrap()
    );
    assert_eq!(
        native.get("x").await.unwrap(),
        plain.get("x").await.unwrap()
    );
    assert_eq!(
        native.contains_key("k2").await.unwrap(),
        plain.contains_key("k2").await.unwrap()
    );
    assert_eq!(native.len().await.unwrap(), plain.len().await.unwrap());
    assert_eq!(
        native.get_many(&["k1", "x", "k3"]).await.unwrap(),
        plain.get_many(&["k1", "x", "k3"]).await.unwrap()
    );
    assert_eq!(
        native.compare_and_set("k2", "c", "c2").await.unwrap(),
        plain.compare_and_set("k2", "c", "c2").await.unwrap()
    );
    assert_eq!(
        native.compare_and_set("k2", "c", "c3").await.unwrap(),
        plain.compare_and_set("k2", "c", "c3").await.unwrap()
    );
    assert_eq!(
        native.remove_if_eq("k3", "z").await.unwrap(),
        plain.remove_if_eq("k3", "z").await.unwrap()
    );
    assert_eq!(
        native.contains_value("c2").await.unwrap(),
        plain.contains_value("c2").await.unwrap()
    );
    assert_eq!(
        native.value_len("k1").await.unwrap(),
        plain.value_len("k1").await.unwrap()
    );
    let mut scanned_native: Vec<(String, String)> = native
        .iter_with(MapScan::new().pattern("\"k1*"))
        .try_collect()
        .await
        .unwrap();
    let mut scanned_plain: Vec<(String, String)> = plain
        .iter_with(MapScan::new().pattern("\"k1*"))
        .try_collect()
        .await
        .unwrap();
    scanned_native.sort();
    scanned_plain.sort();
    assert_eq!(scanned_native, scanned_plain);
    assert_eq!(scanned_native.len(), 2);
    assert_eq!(
        native.to_hash_map().await.unwrap(),
        plain.to_hash_map().await.unwrap()
    );
    assert_eq!(
        native.remove("k1").await.unwrap(),
        plain.remove("k1").await.unwrap()
    );
    assert_eq!(
        native.remove_many(&["k2", "k3", "x"]).await.unwrap(),
        plain.remove_many(&["k2", "k3", "x"]).await.unwrap()
    );
    assert_eq!(
        native.to_hash_map().await.unwrap(),
        plain.to_hash_map().await.unwrap()
    );

    assert_eq!(
        counters_native.incr_by("n", 5).await.unwrap(),
        counters_plain.incr_by("n", 5).await.unwrap()
    );
    assert_eq!(
        counters_native.incr_by("n", -2).await.unwrap(),
        counters_plain.incr_by("n", -2).await.unwrap()
    );
    for name in ["native", "plain", "nn", "pn"] {
        raw_command(&["DEL", &format!("{base}:{name}")]).await;
    }
}

#[tokio::test]
async fn shares_data_with_hash_map() {
    let client = client().await;
    let name = unique("mcn-shared");
    client
        .hash_map::<String, String>(name.clone())
        .insert("a", "1")
        .await
        .unwrap();
    let map: Native<String, String> = client.map_cache_native(name.clone());
    assert_eq!(map.get("a").await.unwrap(), Some("1".to_string()));
    assert_eq!(
        raw_command(&["HGETALL", &name]).await,
        "*2\r\n$3\r\n\"a\"\r\n$3\r\n\"1\"\r\n"
    );
    map.del().await.unwrap();
}

#[tokio::test]
async fn object_methods_cover_the_hash() {
    let name = unique("mcn-object");
    let renamed = unique("mcn-object");
    let map = native(&name).await;
    client()
        .await
        .hash_map::<String, String>(name.clone())
        .set("a", "1")
        .await
        .unwrap();
    assert!(map.expire(std::time::Duration::from_secs(1)).await.unwrap());
    assert!(map.ttl().await.unwrap().is_some());
    assert!(map.persist().await.unwrap());
    assert_eq!(map.ttl().await.unwrap(), None);
    map.rename(&renamed).await.unwrap();
    assert!(!map.exists().await.unwrap());
    let moved = native(&renamed).await;
    assert!(moved.exists().await.unwrap());
    assert_eq!(moved.get("a").await.unwrap(), Some("1".to_string()));
    assert!(moved.del().await.unwrap());
    assert!(!moved.exists().await.unwrap());
}

// The same name and expected hash as the HashMap test in tests/it/hash_map.rs.
#[tokio::test]
async fn per_key_lock_lives_under_the_redisson_name() {
    let name = unique("mcn-lock");
    let map = native(&name).await;
    let lock_key = format!("{{{name}}}:IKm9EBQhlzpVIBxUXDHBWw:lock");
    let guard = map.lock("key").unwrap().lock().await.unwrap();
    assert_eq!(raw_command(&["EXISTS", &lock_key]).await, ":1\r\n");
    guard.unlock().await.unwrap();
    assert_eq!(raw_command(&["EXISTS", &lock_key]).await, ":0\r\n");
}

// Review Focus 3: a server without field TTLs refuses every TTL method before it writes.
#[tokio::test]
async fn ttl_methods_need_redis_7_4_and_write_nothing() {
    let (_container, url) = own_redis_tag("7.2-alpine").await;
    let client = Client::builder().url(url.clone()).build().await.unwrap();
    let name = unique("mcn-old");
    let map: Native<String, String> = client.map_cache_native(name.clone());
    match map.entry_ttl("a").await {
        Err(Error::Unsupported(message)) => assert!(message.contains("7.4"), "{message}"),
        other => panic!("expected Unsupported, got {other:?}"),
    }
    client
        .hash_map::<String, String>(name.clone())
        .insert("a", "1")
        .await
        .unwrap();
    assert_eq!(map.get("a").await.unwrap(), Some("1".to_string()));
    assert_eq!(map.len().await.unwrap(), 1);
}

#[tokio::test]
async fn builder_without_options_reads_like_the_factory() {
    let client = client().await;
    let name = unique("mcn-builder");
    client
        .hash_map::<String, String>(name.clone())
        .set("a", "1")
        .await
        .unwrap();
    let built: Native<String, String> = client
        .map_cache_native_builder(name.clone())
        .build()
        .await
        .unwrap();
    assert_eq!(built.get("a").await.unwrap(), Some("1".to_string()));
    let all: HashMap<String, String> = built.to_hash_map().await.unwrap();
    assert_eq!(all.len(), 1);
    built.del().await.unwrap();
}
