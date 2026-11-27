//! Tests of `MapCacheNative`, Redisson's `RMapCacheNative`.
//!
//! Ported from Redisson 4.8.0 `RedissonMapCacheNativeTest` (named `redisson_<snake_case>`), and 0a's ports of
//! `BaseMapTest` run on a `MapCacheNative` (named `<0a name>_on_native`, in `base_map_on_native`).
//!
//! Not ported:
//! - `testSizeInMemory`: redissun has no `sizeInMemory`.
//! - `testExpireAt`: `Object` has no `expireAt`; `testExpire` covers `Object::expire`.
//! - `testPutIfExistWithNullValueDuration`, `testPutIfExistWithNullValueInstant`: Rust has no null value.
//! - The loader and writer cases of `BaseMapTest` (`testWriter*`, `testMapLoader*`, `testLoadAll*`, `testWriteBehind*`):
//!   covered by the `native_` tests in `tests/map_store.rs`.
//!
//! Recorded deviations the ports follow: a zero TTL means none, `expire_entry` without a time removes the TTL,
//! `entry_ttls` answers in key order, and `replace`/`compare_and_set` keep the entry's TTL.

mod common;

use common::{client, own_redis_tag, raw_command, raw_command_at, unique};
use futures::TryStreamExt;
use redissun::{
    Client, Error, JsonCodec, MapCacheNative, MapEvent, MapEventKind, MapEvents, MapScan, Object,
};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

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
    assert!(map.expire(Duration::from_secs(1)).await.unwrap());
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
    assert_eq!(map.insert("a", "1").await.unwrap(), None);
    assert!(map.set("c", "1").await.unwrap());
    assert_eq!(map.get("a").await.unwrap(), Some("1".to_string()));
    // replace and compare_and_set keep a TTL where the server has them; here they are plain writes.
    assert_eq!(map.replace("a", "2").await.unwrap(), Some("1".to_string()));
    assert!(map.compare_and_set("a", "2", "3").await.unwrap());
    assert_eq!(map.len().await.unwrap(), 2);

    let minute = Duration::from_secs(60);
    for outcome in [
        map.insert("b", "1").ttl(minute).await.map(|_| ()),
        map.set("b", "1").ttl(minute).await.map(|_| ()),
        map.try_insert("b", "1").ttl(minute).await.map(|_| ()),
        map.replace("a", "9").ttl(minute).await.map(|_| ()),
        map.entry_ttls(&["a"]).await.map(|_| ()),
        map.expire_entry("a").ttl(minute).await.map(|_| ()),
        map.expire_entry("a").await.map(|_| ()),
        map.expire_entries(&["a"]).ttl(minute).await.map(|_| ()),
        map.persist_entry("a").await.map(|_| ()),
        map.persist_entries(&["a"]).await.map(|_| ()),
    ] {
        match outcome {
            Err(Error::Unsupported(message)) => assert!(message.contains("7.4"), "{message}"),
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }
    assert_eq!(
        raw_command_at(&url, &["HEXISTS", &name, "\"b\""]).await,
        ":0\r\n"
    );
    assert_eq!(map.get("a").await.unwrap(), Some("3".to_string()));
}

// The server probe uses only hash commands: an ACL user denied COMMAND (`-command`) still gets every method.
#[tokio::test]
async fn ttl_methods_work_for_a_user_without_the_command_command() {
    let (_container, url) = common::own_redis8().await;
    let created = raw_command_at(
        &url,
        &[
            "ACL",
            "SETUSER",
            "app",
            "on",
            ">pw",
            "~*",
            "&*",
            "+@read",
            "+@write",
            "+@hash",
            "+@scripting",
            "+@connection",
            "-command",
        ],
    )
    .await;
    assert_eq!(created, "+OK\r\n");
    let client = Client::builder()
        .url(url.clone())
        .username("app")
        .password("pw")
        .build()
        .await
        .unwrap();
    let map: Native<String, String> = client.map_cache_native(unique("mcn-acl"));
    map.insert("a", "1").ttl(secs(60)).await.unwrap();
    let left = map.entry_ttl("a").await.unwrap().unwrap();
    assert!(left > secs(50) && left <= secs(60), "{left:?}");
    assert_eq!(map.replace("a", "2").await.unwrap(), s("1"));
    assert!(map.compare_and_set("a", "2", "3").await.unwrap());
    assert!(map.entry_ttl("a").await.unwrap().is_some());
    let at = SystemTime::now() + secs(60);
    map.extend([("b", "1")]).expire_at(at).await.unwrap();
    assert!(map.entry_ttl("b").await.unwrap().is_some());
}

// A probe answer that is neither a reply nor "unknown command" (here NOPERM) leaves plain replace and compare_and_set on HashMap's scripts.
#[tokio::test]
async fn replace_works_for_a_user_denied_hpttl() {
    let (_container, url) = common::own_redis8().await;
    let created = raw_command_at(
        &url,
        &[
            "ACL", "SETUSER", "app", "on", ">pw", "~*", "&*", "+@all", "-hpttl",
        ],
    )
    .await;
    assert_eq!(created, "+OK\r\n");
    let client = Client::builder()
        .url(url.clone())
        .username("app")
        .password("pw")
        .build()
        .await
        .unwrap();
    let map: Native<String, String> = client.map_cache_native(unique("mcn-acl-hpttl"));
    map.insert("a", "1").await.unwrap();
    assert_eq!(map.replace("a", "2").await.unwrap(), s("1"));
    assert!(map.compare_and_set("a", "2", "3").await.unwrap());
    assert_eq!(map.get("a").await.unwrap(), s("3"));
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

// --- Task 2: single-key writes with a time ---

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

fn s(v: &str) -> Option<String> {
    Some(v.to_string())
}

async fn native_i64(name: &str) -> Native<String, i64> {
    client().await.map_cache_native(name.to_string())
}

async fn native_f64(name: &str) -> Native<String, f64> {
    client().await.map_cache_native(name.to_string())
}

#[tokio::test]
async fn insert_with_ttl_returns_the_old_value_and_expires() {
    let map = native(&unique("mcn-insert-ttl")).await;
    assert_eq!(map.insert("a", "1").ttl(ms(300)).await.unwrap(), None);
    assert_eq!(map.insert("a", "2").ttl(ms(300)).await.unwrap(), s("1"));
    let left = map.entry_ttl("a").await.unwrap().unwrap();
    assert!(left <= ms(300), "{left:?}");
    sleep(ms(500)).await;
    assert_eq!(map.get("a").await.unwrap(), None);
}

#[tokio::test]
async fn insert_with_expire_at_expires() {
    let map = native(&unique("mcn-insert-at")).await;
    let at = SystemTime::now() + ms(300);
    assert_eq!(map.insert("a", "1").expire_at(at).await.unwrap(), None);
    assert_eq!(map.insert("a", "2").expire_at(at).await.unwrap(), s("1"));
    let left = map.entry_ttl("a").await.unwrap().unwrap();
    assert!(left <= ms(300), "{left:?}");
    sleep(ms(500)).await;
    assert_eq!(map.get("a").await.unwrap(), None);
}

#[tokio::test]
async fn the_last_time_setting_wins() {
    let map = native(&unique("mcn-last-wins")).await;
    map.insert("a", "1")
        .expire_at(SystemTime::now() + ms(100))
        .ttl(secs(60))
        .await
        .unwrap();
    sleep(ms(300)).await;
    assert_eq!(map.get("a").await.unwrap(), s("1"));
    assert!(map.entry_ttl("a").await.unwrap().unwrap() > secs(50));
    map.set("b", "1")
        .ttl(secs(60))
        .expire_at(SystemTime::now() + ms(100))
        .await
        .unwrap();
    sleep(ms(300)).await;
    assert_eq!(map.get("b").await.unwrap(), None);
}

#[tokio::test]
async fn set_with_ttl_reports_a_new_key() {
    let map = native(&unique("mcn-set-ttl")).await;
    assert!(map.set("a", "1").ttl(ms(300)).await.unwrap());
    assert!(!map.set("a", "2").ttl(ms(300)).await.unwrap());
    assert!(map.entry_ttl("a").await.unwrap().is_some());
    sleep(ms(500)).await;
    assert!(map.set("a", "3").ttl(ms(300)).await.unwrap());
}

#[tokio::test]
async fn try_insert_with_ttl_keeps_the_live_value() {
    let map = native(&unique("mcn-try-ttl")).await;
    assert_eq!(map.try_insert("a", "1").ttl(ms(300)).await.unwrap(), None);
    assert_eq!(map.try_insert("a", "2").ttl(ms(300)).await.unwrap(), s("1"));
    assert_eq!(map.get("a").await.unwrap(), s("1"));
    sleep(ms(500)).await;
    assert_eq!(map.try_insert("a", "3").ttl(secs(60)).await.unwrap(), None);
    assert_eq!(map.get("a").await.unwrap(), s("3"));
    assert!(map.entry_ttl("a").await.unwrap().is_some());
}

// Redisson testPutIfExistWithDuration, testPutIfExistWithInstant.
#[tokio::test]
async fn replace_with_ttl_only_touches_present_keys() {
    let map = native(&unique("mcn-replace-ttl")).await;
    assert_eq!(map.replace("k1", "v1").ttl(secs(10)).await.unwrap(), None);
    assert_eq!(map.get("k1").await.unwrap(), None);
    assert_eq!(
        map.replace("k1", "v1")
            .expire_at(SystemTime::now() + secs(60))
            .await
            .unwrap(),
        None
    );
    assert_eq!(map.get("k1").await.unwrap(), None);

    map.insert("k2", "v2").ttl(secs(30)).await.unwrap();
    assert_eq!(
        map.replace("k2", "n2").ttl(secs(10)).await.unwrap(),
        s("v2")
    );
    assert_eq!(map.get("k2").await.unwrap(), s("n2"));
    let left = map.entry_ttl("k2").await.unwrap().unwrap();
    assert!(left <= secs(10), "{left:?}");

    assert_eq!(
        map.replace("k2", "n3")
            .expire_at(SystemTime::now() + secs(60))
            .await
            .unwrap(),
        s("n2")
    );
    let left = map.entry_ttl("k2").await.unwrap().unwrap();
    assert!(left > secs(50) && left <= secs(60), "{left:?}");
    map.replace("k2", "n4").ttl(ms(200)).await.unwrap();
    sleep(ms(400)).await;
    assert_eq!(map.get("k2").await.unwrap(), None);
}

// Review Focus 1: a write without a time makes the entry permanent.
#[tokio::test]
async fn plain_writes_clear_the_ttl() {
    let map = native(&unique("mcn-plain-clears")).await;
    map.insert("a", "1").ttl(secs(60)).await.unwrap();
    map.insert("a", "2").await.unwrap();
    assert_eq!(map.entry_ttl("a").await.unwrap(), None);
    assert_eq!(map.get("a").await.unwrap(), s("2"));

    map.insert("a", "1").ttl(secs(60)).await.unwrap();
    map.set("a", "3").await.unwrap();
    assert_eq!(map.entry_ttl("a").await.unwrap(), None);
    assert_eq!(map.get("a").await.unwrap(), s("3"));
}

// Owner decision 1: replace keeps the entry's TTL, like HashMapCache::replace.
#[tokio::test]
async fn replace_keeps_the_ttl() {
    let map = native(&unique("mcn-replace-keeps")).await;
    map.insert("a", "1").ttl(secs(60)).await.unwrap();
    assert_eq!(map.replace("a", "2").await.unwrap(), s("1"));
    assert_eq!(map.get("a").await.unwrap(), s("2"));
    let left = map.entry_ttl("a").await.unwrap().unwrap();
    assert!(left > secs(50) && left <= secs(60), "{left:?}");

    map.insert("b", "1").await.unwrap();
    assert_eq!(map.replace("b", "2").await.unwrap(), s("1"));
    assert_eq!(map.entry_ttl("b").await.unwrap(), None);

    assert_eq!(map.replace("missing", "2").await.unwrap(), None);
    assert_eq!(map.get("missing").await.unwrap(), None);

    map.insert("c", "1").ttl(ms(300)).await.unwrap();
    map.replace("c", "2").await.unwrap();
    sleep(ms(500)).await;
    assert_eq!(map.get("c").await.unwrap(), None);
}

// Owner decision 1: compare_and_set keeps the entry's TTL.
#[tokio::test]
async fn compare_and_set_keeps_the_ttl() {
    let map = native(&unique("mcn-cas-keeps")).await;
    map.insert("a", "1").ttl(secs(60)).await.unwrap();
    assert!(map.compare_and_set("a", "1", "2").await.unwrap());
    assert_eq!(map.get("a").await.unwrap(), s("2"));
    let left = map.entry_ttl("a").await.unwrap().unwrap();
    assert!(left > secs(50) && left <= secs(60), "{left:?}");
    assert!(!map.compare_and_set("a", "1", "3").await.unwrap());
    assert_eq!(map.get("a").await.unwrap(), s("2"));

    map.insert("b", "1").await.unwrap();
    assert!(map.compare_and_set("b", "1", "2").await.unwrap());
    assert_eq!(map.entry_ttl("b").await.unwrap(), None);
    assert!(!map.compare_and_set("missing", "1", "2").await.unwrap());
}

// Review Focus 1: HINCRBY keeps the field's TTL.
#[tokio::test]
async fn incr_by_keeps_the_ttl() {
    let map = native_i64(&unique("mcn-incr-keeps")).await;
    map.insert("n", &1).ttl(secs(60)).await.unwrap();
    assert_eq!(map.incr_by("n", 2).await.unwrap(), 3);
    assert!(map.entry_ttl("n").await.unwrap().is_some());
}

// Review Focus 2: zero means no TTL, never "delete now".
#[tokio::test]
async fn a_zero_ttl_means_no_ttl() {
    let map = native(&unique("mcn-zero")).await;
    assert_eq!(
        map.insert("a", "1").ttl(Duration::ZERO).await.unwrap(),
        None
    );
    assert_eq!(map.get("a").await.unwrap(), s("1"));
    assert_eq!(map.entry_ttl("a").await.unwrap(), None);

    assert!(map.set("b", "1").ttl(Duration::ZERO).await.unwrap());
    assert_eq!(map.get("b").await.unwrap(), s("1"));
    assert_eq!(map.entry_ttl("b").await.unwrap(), None);

    assert_eq!(
        map.try_insert("c", "1").ttl(Duration::ZERO).await.unwrap(),
        None
    );
    assert_eq!(map.get("c").await.unwrap(), s("1"));
    assert_eq!(map.entry_ttl("c").await.unwrap(), None);

    // A zero TTL on replace is Redisson's putIfExists(k, v): stored, and without a TTL.
    map.insert("d", "1").ttl(secs(60)).await.unwrap();
    assert_eq!(
        map.replace("d", "2").ttl(Duration::ZERO).await.unwrap(),
        s("1")
    );
    assert_eq!(map.get("d").await.unwrap(), s("2"));
    assert_eq!(map.entry_ttl("d").await.unwrap(), None);
}

// Review Focus 2: Redis deletes a field whose deadline has passed.
#[tokio::test]
async fn a_past_expire_at_deletes_at_once() {
    let map = native(&unique("mcn-past")).await;
    map.insert("a", "1").await.unwrap();
    assert_eq!(
        map.insert("a", "2")
            .expire_at(UNIX_EPOCH + secs(1))
            .await
            .unwrap(),
        s("1")
    );
    assert_eq!(map.get("a").await.unwrap(), None);
}

#[tokio::test]
async fn a_time_before_1970_is_a_config_error_and_writes_nothing() {
    let map = native(&unique("mcn-1969")).await;
    let outcome = map.insert("a", "1").expire_at(UNIX_EPOCH - secs(1)).await;
    assert!(matches!(outcome, Err(Error::Config(_))), "{outcome:?}");
    assert_eq!(map.get("a").await.unwrap(), None);
}

// Redisson testRemainTimeToLive.
#[tokio::test]
async fn entry_ttls_keeps_order_and_reports_none() {
    let map = native(&unique("mcn-ttls")).await;
    map.insert("1", "2").ttl(secs(2)).await.unwrap();
    let left = map.entry_ttl("1").await.unwrap().unwrap();
    assert!(left >= ms(1900) && left <= secs(2), "{left:?}");
    map.insert("3", "4").await.unwrap();
    assert_eq!(map.entry_ttl("3").await.unwrap(), None);
    assert_eq!(map.entry_ttl("0").await.unwrap(), None);
    map.insert("5", "6").ttl(secs(20)).await.unwrap();
    map.insert("6", "7")
        .expire_at(SystemTime::now() + secs(20))
        .await
        .unwrap();

    let ttls = map
        .entry_ttls(&["0", "1", "3", "5", "6", "7"])
        .await
        .unwrap();
    assert_eq!(ttls.len(), 6);
    assert_eq!(ttls[0], None);
    assert!(ttls[1].unwrap() > ms(1));
    assert_eq!(ttls[2], None);
    assert!(ttls[3].unwrap() > ms(1));
    assert!(ttls[4].unwrap() > ms(1));
    assert_eq!(ttls[5], None);

    let twice = map.entry_ttls(&["5", "5"]).await.unwrap();
    assert_eq!(twice.len(), 2);
    assert!(twice.iter().all(Option::is_some));
    assert!(map.entry_ttls::<str>(&[]).await.unwrap().is_empty());
}

#[tokio::test]
async fn redisson_fast_put_ttl() {
    let map = native(&unique("mcn-getAll")).await;
    map.set("1", "3").ttl(secs(2)).await.unwrap();
    map.set("2", "6")
        .expire_at(SystemTime::now() + secs(2))
        .await
        .unwrap();
    sleep(ms(2010)).await;
    assert_eq!(map.get("1").await.unwrap(), None);
    assert_eq!(map.get("2").await.unwrap(), None);

    map.set("1", "4").ttl(secs(2)).await.unwrap();
    map.set("2", "5")
        .expire_at(SystemTime::now() + secs(2))
        .await
        .unwrap();
    sleep(ms(4010)).await;
    assert_eq!(map.get("1").await.unwrap(), None);
    assert_eq!(map.get("2").await.unwrap(), None);
}

#[tokio::test]
async fn redisson_put_get_ttl() {
    let map = native(&unique("mcn-simple04")).await;
    assert_eq!(map.get("33").await.unwrap(), None);
    assert_eq!(map.get("55").await.unwrap(), None);
    map.insert("33", "44").ttl(secs(2)).await.unwrap();
    map.insert("55", "66")
        .expire_at(SystemTime::now() + secs(2))
        .await
        .unwrap();
    assert_eq!(map.get("33").await.unwrap(), s("44"));
    assert_eq!(map.get("55").await.unwrap(), s("66"));
    sleep(secs(1)).await;
    assert_eq!(map.len().await.unwrap(), 2);
    assert_eq!(map.get("33").await.unwrap(), s("44"));
    assert_eq!(map.get("55").await.unwrap(), s("66"));
    assert_eq!(map.len().await.unwrap(), 2);
    sleep(ms(1100)).await;
    assert_eq!(map.get("33").await.unwrap(), None);
    assert_eq!(map.get("55").await.unwrap(), None);
    map.del().await.unwrap();
}

#[tokio::test]
async fn redisson_put_if_absent_ttl() {
    let map = native(&unique("mcn-simple")).await;
    map.insert("1", "2").await.unwrap();
    assert_eq!(map.try_insert("1", "3").ttl(secs(1)).await.unwrap(), s("2"));
    assert_eq!(
        map.try_insert("1", "3")
            .expire_at(SystemTime::now() + secs(1))
            .await
            .unwrap(),
        s("2")
    );
    assert_eq!(map.get("1").await.unwrap(), s("2"));

    map.try_insert("4", "4").ttl(secs(1)).await.unwrap();
    assert_eq!(map.get("4").await.unwrap(), s("4"));
    map.try_insert("5", "5")
        .expire_at(SystemTime::now() + secs(1))
        .await
        .unwrap();
    assert_eq!(map.get("5").await.unwrap(), s("5"));

    sleep(ms(1100)).await;
    assert_eq!(map.get("4").await.unwrap(), None);
    assert_eq!(map.get("5").await.unwrap(), None);

    map.try_insert("4", "4").ttl(secs(1)).await.unwrap();
    assert_eq!(map.get("4").await.unwrap(), s("4"));
    map.try_insert("5", "5")
        .expire_at(SystemTime::now() + secs(1))
        .await
        .unwrap();
    assert_eq!(map.get("5").await.unwrap(), s("5"));

    assert_eq!(map.try_insert("2", "4").ttl(secs(2)).await.unwrap(), None);
    assert_eq!(
        map.try_insert("7", "8")
            .expire_at(SystemTime::now() + secs(2))
            .await
            .unwrap(),
        None
    );
    assert_eq!(map.get("2").await.unwrap(), s("4"));
    assert_eq!(map.get("7").await.unwrap(), s("8"));
    map.del().await.unwrap();
}

// fastPutIfAbsent is try_insert, whose `None` is Redisson's `true`.
#[tokio::test]
async fn redisson_fast_put_if_absent_ttl() {
    let map = native(&unique("mcn-simple")).await;
    map.insert("1", "2").await.unwrap();
    assert!(map.try_insert("1", "3").await.unwrap().is_some());
    assert_eq!(map.get("1").await.unwrap(), s("2"));

    assert!(map.try_insert("2", "4").await.unwrap().is_none());
    assert_eq!(map.get("2").await.unwrap(), s("4"));

    map.insert("3", "31").ttl(ms(500)).await.unwrap();
    assert!(map.try_insert("3", "32").await.unwrap().is_some());
    map.insert("4", "32")
        .expire_at(SystemTime::now() + ms(500))
        .await
        .unwrap();
    assert!(map.try_insert("4", "33").await.unwrap().is_some());

    sleep(ms(600)).await;
    assert!(map.try_insert("3", "32").await.unwrap().is_none());
    assert_eq!(map.get("3").await.unwrap(), s("32"));
    assert!(map.try_insert("4", "33").await.unwrap().is_none());
    assert_eq!(map.get("4").await.unwrap(), s("33"));
    map.del().await.unwrap();
}

#[tokio::test]
async fn redisson_fast_put_if_absent_with_ttl() {
    let map = native(&unique("mcn-simpleTTL")).await;
    let at = || SystemTime::now() + secs(1);
    map.try_insert("1", "2").ttl(secs(1)).await.unwrap();
    assert!(map
        .try_insert("1", "3")
        .ttl(secs(1))
        .await
        .unwrap()
        .is_some());
    assert_eq!(map.get("1").await.unwrap(), s("2"));

    map.try_insert("5", "6").expire_at(at()).await.unwrap();
    assert!(map
        .try_insert("5", "7")
        .expire_at(at())
        .await
        .unwrap()
        .is_some());
    assert_eq!(map.get("5").await.unwrap(), s("6"));

    sleep(ms(1100)).await;

    assert!(map
        .try_insert("1", "3")
        .ttl(secs(1))
        .await
        .unwrap()
        .is_none());
    assert_eq!(map.get("1").await.unwrap(), s("3"));
    assert!(map
        .try_insert("1", "4")
        .ttl(secs(1))
        .await
        .unwrap()
        .is_some());
    assert_eq!(map.get("1").await.unwrap(), s("3"));

    assert!(map
        .try_insert("5", "7")
        .expire_at(at())
        .await
        .unwrap()
        .is_none());
    assert_eq!(map.get("5").await.unwrap(), s("7"));
    assert!(map
        .try_insert("5", "8")
        .expire_at(at())
        .await
        .unwrap()
        .is_some());
    assert_eq!(map.get("5").await.unwrap(), s("7"));

    sleep(ms(1100)).await;
    assert!(map
        .try_insert("1", "4")
        .ttl(secs(1))
        .await
        .unwrap()
        .is_none());
    assert!(map
        .try_insert("5", "8")
        .expire_at(at())
        .await
        .unwrap()
        .is_none());
    map.del().await.unwrap();
}

#[tokio::test]
async fn redisson_fast_put_expiration() {
    let map = native(&unique("mcn-testFastPutExpiration")).await;
    map.set("k1", "v1").ttl(secs(1)).await.unwrap();
    map.set("k2", "v1")
        .expire_at(SystemTime::now() + secs(1))
        .await
        .unwrap();
    sleep(secs(1)).await;
    map.set("k1", "v2").await.unwrap();
    map.set("k2", "v2").await.unwrap();
    assert_eq!(map.get("k1").await.unwrap(), s("v2"));
    assert_eq!(map.get("k2").await.unwrap(), s("v2"));
}

#[tokio::test]
async fn redisson_expire_overwrite() {
    let map = native_i64(&unique("mcn-simple")).await;
    map.insert("123", &3).ttl(secs(1)).await.unwrap();
    map.insert("456", &6)
        .expire_at(SystemTime::now() + secs(1))
        .await
        .unwrap();
    sleep(ms(800)).await;
    map.insert("123", &3).ttl(secs(1)).await.unwrap();
    map.insert("456", &6)
        .expire_at(SystemTime::now() + secs(1))
        .await
        .unwrap();
    sleep(ms(800)).await;
    assert_eq!(map.get("123").await.unwrap(), Some(3));
    assert_eq!(map.get("456").await.unwrap(), Some(6));
    sleep(ms(300)).await;
    assert!(!map.contains_key("123").await.unwrap());
    assert!(!map.contains_key("456").await.unwrap());
    map.del().await.unwrap();
}

#[tokio::test]
async fn redisson_replace_value_ttl() {
    let map = native(&unique("mcn-simple")).await;
    map.insert("1", "2").ttl(secs(1)).await.unwrap();
    map.insert("2", "3")
        .expire_at(SystemTime::now() + secs(1))
        .await
        .unwrap();
    sleep(ms(1050)).await;
    assert_eq!(map.replace("1", "3").await.unwrap(), None);
    assert_eq!(map.replace("2", "4").await.unwrap(), None);
    assert_eq!(map.get("1").await.unwrap(), None);
    assert_eq!(map.get("2").await.unwrap(), None);
    map.del().await.unwrap();
}

// JsonCodec numbers in place of LongCodec and DoubleCodec.
#[tokio::test]
async fn redisson_add_and_get_ttl() {
    let map = native_i64(&unique("mcn-test_put_if_absent")).await;
    assert_eq!(
        map.try_insert("4", &0).ttl(secs(10000)).await.unwrap(),
        None
    );
    assert_eq!(map.incr_by("4", 1).await.unwrap(), 1);
    assert_eq!(map.try_insert("4", &0).await.unwrap(), Some(1));
    assert_eq!(
        map.try_insert("5", &0)
            .expire_at(SystemTime::now() + secs(10000))
            .await
            .unwrap(),
        None
    );
    assert_eq!(map.incr_by("5", 1).await.unwrap(), 1);
    assert_eq!(map.try_insert("5", &0).await.unwrap(), Some(1));
    assert_eq!(
        map.incr_by("key", i64::MAX - 10).await.unwrap(),
        i64::MAX - 10
    );
    assert_eq!(map.incr_by("key", 10).await.unwrap(), i64::MAX);
    assert_eq!(map.get("4").await.unwrap(), Some(1));
    assert_eq!(map.get("5").await.unwrap(), Some(1));
    map.del().await.unwrap();

    let other = native_i64(&unique("mcn-test_put_if_absent_1")).await;
    other.try_insert("4", &0).await.unwrap();
    other.incr_by("4", 1).await.unwrap();
    other.try_insert("4", &0).await.unwrap();
    assert_eq!(other.get("4").await.unwrap(), Some(1));
    other.del().await.unwrap();

    let floats = native_f64(&unique("mcn-test_put_if_absent_3")).await;
    floats
        .try_insert("4", &1.23)
        .ttl(secs(10000))
        .await
        .unwrap();
    floats.incr_by_float("4", 1.0).await.unwrap();
    assert_eq!(floats.get("4").await.unwrap(), Some(2.23));
    floats
        .try_insert("4", &2.23)
        .expire_at(SystemTime::now() + secs(10000))
        .await
        .unwrap();
    floats.incr_by_float("4", 1.0).await.unwrap();
    assert_eq!(floats.get("4").await.unwrap(), Some(3.23));
    floats.del().await.unwrap();
}

// --- Task 3: expire_entry, expire_entries, persist_entry, persist_entries ---

async fn native_ints(name: &str) -> Native<i64, i64> {
    client().await.map_cache_native(name.to_string())
}

fn between(left: Option<Duration>, low: Duration, high: Duration) {
    let left = left.expect("the entry has no TTL");
    assert!(
        left >= low && left <= high,
        "{left:?} not in {low:?}..={high:?}"
    );
}

// Redisson testExpireEntry, testUpdateEntryExpiration.
#[tokio::test]
async fn expire_entry_sets_and_replaces_the_ttl() {
    let map = native(&unique("mcn-expire-entry")).await;
    map.insert("key", "value").await.unwrap();
    assert!(map.expire_entry("key").ttl(ms(20000)).await.unwrap());
    between(map.entry_ttl("key").await.unwrap(), ms(19800), ms(20000));

    map.insert("key2", "value").await.unwrap();
    assert!(map
        .expire_entry("key2")
        .expire_at(SystemTime::now() + ms(20000))
        .await
        .unwrap());
    between(map.entry_ttl("key2").await.unwrap(), ms(19800), ms(20500));

    // A new time replaces the old one, shorter or longer.
    assert!(map.expire_entry("key").ttl(secs(5)).await.unwrap());
    between(map.entry_ttl("key").await.unwrap(), ms(4800), secs(5));

    // HPEXPIRE revives a field whose time ran out until the server deletes it (Redis 7.4 and 8.8); a read deletes it first.
    map.insert("short", "value").ttl(ms(200)).await.unwrap();
    sleep(ms(400)).await;
    assert_eq!(map.get("short").await.unwrap(), None);
    assert!(!map.expire_entry("short").ttl(secs(2)).await.unwrap());
}

// The first half of Redisson testEntryEntryIfNotSet, for every condition.
#[tokio::test]
async fn expire_entry_on_a_missing_key_is_false() {
    let map = native(&unique("mcn-expire-missing")).await;
    assert!(!map.expire_entry("x").ttl(secs(2)).await.unwrap());
    assert!(!map
        .expire_entry("x")
        .expire_at(SystemTime::now() + secs(2))
        .await
        .unwrap());
    assert!(!map
        .expire_entry("x")
        .ttl(secs(2))
        .if_not_set()
        .await
        .unwrap());
    assert_eq!(map.get("x").await.unwrap(), None);
}

// Review Focus 2: zero and no time clear the TTL, unlike Redisson's expireEntry(k, ZERO), which deletes.
#[tokio::test]
async fn expire_entry_with_zero_clears_the_ttl() {
    let map = native(&unique("mcn-expire-zero")).await;
    map.insert("a", "1").ttl(secs(60)).await.unwrap();
    assert!(map.expire_entry("a").ttl(Duration::ZERO).await.unwrap());
    assert_eq!(map.entry_ttl("a").await.unwrap(), None);
    assert_eq!(map.get("a").await.unwrap(), s("1"));

    map.insert("b", "1").ttl(secs(60)).await.unwrap();
    assert!(map.expire_entry("b").await.unwrap());
    assert_eq!(map.entry_ttl("b").await.unwrap(), None);
    assert_eq!(map.get("b").await.unwrap(), s("1"));

    // An entry without a TTL is still there: true, as HashMapCache::expire_entry.
    assert!(map.expire_entry("b").await.unwrap());
    assert!(!map.expire_entry("missing").await.unwrap());
    assert!(!map
        .expire_entry("missing")
        .ttl(Duration::ZERO)
        .await
        .unwrap());

    map.insert("c", "1").ttl(secs(60)).await.unwrap();
    map.insert("d", "1").await.unwrap();
    assert_eq!(map.expire_entries(&["c", "d", "missing"]).await.unwrap(), 2);
    assert_eq!(map.entry_ttl("c").await.unwrap(), None);
    assert_eq!(map.get("c").await.unwrap(), s("1"));
}

#[tokio::test]
async fn a_condition_without_a_time_is_a_config_error() {
    let map = native(&unique("mcn-condition-config")).await;
    map.insert("a", "1").ttl(secs(60)).await.unwrap();
    for outcome in [
        map.expire_entry("a").if_not_set().await,
        map.expire_entry("a").if_greater().await,
        map.expire_entry("a").ttl(Duration::ZERO).if_less().await,
    ] {
        match outcome {
            Err(Error::Config(message)) => {
                assert!(message.contains("ttl or expire_at"), "{message}")
            }
            other => panic!("expected Config, got {other:?}"),
        }
    }
    for outcome in [
        map.expire_entries(&["a"]).if_greater().await,
        map.expire_entries::<str>(&[]).if_less().await,
    ] {
        assert!(matches!(outcome, Err(Error::Config(_))), "{outcome:?}");
    }
    // Nothing changed.
    assert!(map.entry_ttl("a").await.unwrap().is_some());
}

// Review Focus 4: in Redis, a field without a TTL counts as an infinite TTL.
#[tokio::test]
async fn conditions_follow_redis_on_persistent_and_missing_entries() {
    let map = native(&unique("mcn-conditions")).await;
    for k in ["p1", "p2", "p3"] {
        map.insert(k, "1").await.unwrap();
    }
    assert!(!map
        .expire_entry("p1")
        .ttl(secs(10))
        .if_greater()
        .await
        .unwrap());
    assert_eq!(map.entry_ttl("p1").await.unwrap(), None);
    assert!(map
        .expire_entry("p2")
        .ttl(secs(10))
        .if_less()
        .await
        .unwrap());
    assert!(map.entry_ttl("p2").await.unwrap().is_some());
    assert!(map
        .expire_entry("p3")
        .ttl(secs(10))
        .if_not_set()
        .await
        .unwrap());
    assert!(map.entry_ttl("p3").await.unwrap().is_some());

    assert!(!map
        .expire_entry("x")
        .ttl(secs(10))
        .if_greater()
        .await
        .unwrap());
    assert!(!map.expire_entry("x").ttl(secs(10)).if_less().await.unwrap());
    assert!(!map
        .expire_entry("x")
        .ttl(secs(10))
        .if_not_set()
        .await
        .unwrap());

    map.insert("t", "1").ttl(secs(60)).await.unwrap();
    assert!(!map
        .expire_entry("t")
        .ttl(secs(10))
        .if_greater()
        .await
        .unwrap());
    between(map.entry_ttl("t").await.unwrap(), secs(50), secs(60));
    assert!(map
        .expire_entry("t")
        .ttl(secs(120))
        .if_greater()
        .await
        .unwrap());
    between(map.entry_ttl("t").await.unwrap(), secs(110), secs(120));
    assert!(map.expire_entry("t").ttl(secs(10)).if_less().await.unwrap());
    between(map.entry_ttl("t").await.unwrap(), secs(9), secs(10));
    assert!(!map
        .expire_entry("t")
        .ttl(secs(30))
        .if_not_set()
        .await
        .unwrap());
    assert!(map
        .expire_entry("t")
        .expire_at(SystemTime::now() + secs(5))
        .if_less()
        .await
        .unwrap());
    between(map.entry_ttl("t").await.unwrap(), secs(4), ms(5500));

    // The last condition wins.
    map.insert("w", "1").await.unwrap();
    assert!(map
        .expire_entry("w")
        .ttl(secs(10))
        .if_greater()
        .if_less()
        .await
        .unwrap());

    // The same rules for several entries.
    for k in ["q1", "q2"] {
        map.insert(k, "1").await.unwrap();
    }
    map.insert("q3", "1").ttl(secs(60)).await.unwrap();
    assert_eq!(
        map.expire_entries(&["q1", "q3", "x"])
            .ttl(secs(10))
            .if_greater()
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        map.expire_entries(&["q1", "q3", "x"])
            .ttl(secs(10))
            .if_less()
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        map.expire_entries(&["q1", "q2", "x"])
            .expire_at(SystemTime::now() + secs(30))
            .if_not_set()
            .await
            .unwrap(),
        1
    );
}

// Redisson testExpireEntries, testEntryEntries, testEntryEntriesIfNotSet, testExpireEntriesIfNotSet.
#[tokio::test]
async fn expire_entries_counts_changed_entries() {
    let map = native(&unique("mcn-expire-entries")).await;
    for k in ["key1", "key2", "key3", "key4"] {
        map.insert(k, "value").await.unwrap();
    }
    assert_eq!(
        map.expire_entries(&["key1", "key2", "missing"])
            .ttl(ms(20000))
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        map.expire_entries(&["key3", "key4"])
            .expire_at(SystemTime::now() + ms(20000))
            .await
            .unwrap(),
        2
    );
    between(map.entry_ttl("key1").await.unwrap(), ms(19800), ms(20000));
    between(map.entry_ttl("key3").await.unwrap(), ms(19800), ms(20500));

    map.insert("plain", "value").await.unwrap();
    assert_eq!(
        map.expire_entries(&["key1", "plain"])
            .ttl(secs(2))
            .if_not_set()
            .await
            .unwrap(),
        1
    );
    between(map.entry_ttl("key1").await.unwrap(), ms(19000), ms(20000));
    between(map.entry_ttl("plain").await.unwrap(), ms(1900), secs(2));

    // A key listed twice counts twice.
    assert_eq!(
        map.expire_entries(&["key2", "key2"])
            .ttl(ms(300))
            .await
            .unwrap(),
        2
    );
    sleep(ms(500)).await;
    // A read deletes the expired field; HPEXPIRE alone would revive it until the server's own clean-up runs.
    assert_eq!(map.get("key2").await.unwrap(), None);
    assert_eq!(map.expire_entries(&["key2"]).ttl(secs(2)).await.unwrap(), 0);
    assert_eq!(
        map.expire_entries::<str>(&[]).ttl(secs(2)).await.unwrap(),
        0
    );
    assert_eq!(map.expire_entries::<str>(&[]).await.unwrap(), 0);
}

// Redisson testClearExpire for entries: clearExpire(K) and clearExpire(Set).
#[tokio::test]
async fn persist_entry_reports_a_removed_ttl() {
    let map = native(&unique("mcn-persist")).await;
    map.insert("ttl", "1").ttl(secs(60)).await.unwrap();
    map.insert("plain", "1").await.unwrap();
    assert!(map.persist_entry("ttl").await.unwrap());
    assert_eq!(map.entry_ttl("ttl").await.unwrap(), None);
    assert_eq!(map.get("ttl").await.unwrap(), s("1"));
    assert!(!map.persist_entry("plain").await.unwrap());
    assert!(!map.persist_entry("missing").await.unwrap());

    map.insert("ttl", "1").ttl(secs(60)).await.unwrap();
    assert_eq!(
        map.persist_entries(&["ttl", "plain", "missing"])
            .await
            .unwrap(),
        vec![true, false, false]
    );
    assert_eq!(map.entry_ttl("ttl").await.unwrap(), None);
    assert!(map.persist_entries::<str>(&[]).await.unwrap().is_empty());
}

#[tokio::test]
async fn redisson_expire_entry_if_not_set() {
    let map = native(&unique("mcn-map")).await;
    map.insert("key", "value").await.unwrap();
    map.expire_entry("key")
        .ttl(ms(20000))
        .if_not_set()
        .await
        .unwrap();
    between(map.entry_ttl("key").await.unwrap(), ms(19800), ms(20000));

    map.insert("key2", "value").await.unwrap();
    map.expire_entry("key2")
        .expire_at(SystemTime::now() + ms(20000))
        .if_not_set()
        .await
        .unwrap();
    between(map.entry_ttl("key2").await.unwrap(), ms(19800), ms(20500));
}

#[tokio::test]
async fn redisson_entry_entry_if_not_set() {
    let cache = native_ints(&unique("mcn-testUpdateEntryExpiration")).await;
    cache.insert(&1, &10).await.unwrap();
    cache.insert(&2, &20).await.unwrap();
    cache.insert(&3, &30).ttl(secs(1)).await.unwrap();
    cache
        .insert(&4, &30)
        .expire_at(SystemTime::now() + secs(1))
        .await
        .unwrap();

    assert!(cache
        .expire_entry(&2)
        .ttl(secs(2))
        .if_not_set()
        .await
        .unwrap());
    assert!(!cache
        .expire_entry(&3)
        .ttl(secs(4))
        .if_not_set()
        .await
        .unwrap());
    assert!(!cache
        .expire_entry(&4)
        .expire_at(SystemTime::now() + secs(4))
        .if_not_set()
        .await
        .unwrap());
    between(cache.entry_ttl(&2).await.unwrap(), ms(1900), secs(2));

    sleep(ms(1200)).await;
    assert!(cache.contains_key(&2).await.unwrap());
    assert!(!cache.contains_key(&3).await.unwrap());
    assert!(!cache.contains_key(&4).await.unwrap());
    sleep(ms(1300)).await;
    assert!(!cache.contains_key(&2).await.unwrap());
}

// --- Task 4: many entries: extend with a time, try_extend, update_many ---

async fn native8(name: &str) -> Native<String, String> {
    common::redis8_client()
        .await
        .map_cache_native(name.to_string())
}

fn pairs(n: usize) -> Vec<(String, String)> {
    (0..n).map(|i| (format!("k{i}"), format!("v{i}"))).collect()
}

#[tokio::test]
async fn extend_with_ttl_and_batches() {
    let map = native(&unique("mcn-extend-ttl")).await;
    let entries = pairs(250);
    map.extend(entries.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .batch_size(100)
        .ttl(ms(300))
        .await
        .unwrap();
    assert_eq!(map.len().await.unwrap(), 250);
    between(map.entry_ttl("k249").await.unwrap(), ms(1), ms(300));
    sleep(ms(500)).await;
    for (k, _) in &entries {
        assert_eq!(map.get(k.as_str()).await.unwrap(), None, "{k}");
    }
}

// Review Focus 1: a plain extend makes the entries permanent.
#[tokio::test]
async fn extend_without_ttl_clears_it() {
    let map = native(&unique("mcn-extend-plain")).await;
    map.insert("a", "1").ttl(secs(60)).await.unwrap();
    map.extend([("a", "2"), ("b", "3")]).await.unwrap();
    assert_eq!(map.entry_ttl("a").await.unwrap(), None);
    assert_eq!(map.get("a").await.unwrap(), s("2"));
    assert_eq!(map.get("b").await.unwrap(), s("3"));
}

#[tokio::test]
async fn extend_with_a_zero_ttl_stores_permanent_entries() {
    let map = native(&unique("mcn-extend-zero")).await;
    map.insert("a", "1").ttl(secs(60)).await.unwrap();
    map.extend([("a", "2"), ("b", "3")])
        .ttl(Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(map.entry_ttls(&["a", "b"]).await.unwrap(), vec![None, None]);
    assert_eq!(map.len().await.unwrap(), 2);
}

#[tokio::test]
async fn extend_with_expire_at_and_keep_ttl() {
    let map = native8(&unique("mcn-extend-at")).await;
    map.extend([("a", "1"), ("b", "2")])
        .expire_at(SystemTime::now() + ms(300))
        .await
        .unwrap();
    assert_eq!(map.get("a").await.unwrap(), s("1"));
    between(map.entry_ttl("b").await.unwrap(), ms(1), ms(300));
    sleep(ms(500)).await;
    assert_eq!(map.get("a").await.unwrap(), None);
    assert_eq!(map.get("b").await.unwrap(), None);

    // Redisson testPutIfAllKeysExistWithKeepTTL's pattern on putAll(PutArgs.keepTTL()).
    map.insert("c", "old").ttl(secs(30)).await.unwrap();
    let before = map.entry_ttl("c").await.unwrap().unwrap();
    map.extend([("c", "new")]).keep_ttl().await.unwrap();
    assert_eq!(map.get("c").await.unwrap(), s("new"));
    between(map.entry_ttl("c").await.unwrap(), ms(1), before);
    // The last setting wins.
    map.extend([("c", "newer")])
        .keep_ttl()
        .ttl(Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(map.entry_ttl("c").await.unwrap(), None);
}

// Redisson testPutIfAllKeysAbsentWithTimeToLive, testPutIfAllKeysAbsentWithExpireAt, testPutIfAllKeysAbsentExpiration.
#[tokio::test]
async fn try_extend_is_all_or_nothing() {
    let map = native8(&unique("mcn-try-extend")).await;
    let entries = [("key1", "value1"), ("key2", "value2")];
    assert!(map.try_extend(entries).ttl(secs(10)).await.unwrap());
    assert_eq!(map.get("key1").await.unwrap(), s("value1"));
    assert_eq!(map.get("key2").await.unwrap(), s("value2"));
    between(map.entry_ttl("key1").await.unwrap(), secs(9), secs(10));

    let newer = [("key1", "newValue1"), ("key2", "newValue2")];
    assert!(!map.try_extend(newer).ttl(secs(10)).await.unwrap());
    assert!(!map
        .try_extend(newer)
        .expire_at(SystemTime::now() + secs(10))
        .await
        .unwrap());
    assert_eq!(map.get("key1").await.unwrap(), s("value1"));
    assert_eq!(map.get("key2").await.unwrap(), s("value2"));

    map.del().await.unwrap();
    map.insert("key1", "existingValue1")
        .ttl(secs(30))
        .await
        .unwrap();
    assert!(!map.try_extend(entries).ttl(secs(10)).await.unwrap());
    assert_eq!(map.get("key1").await.unwrap(), s("existingValue1"));
    assert_eq!(map.get("key2").await.unwrap(), None);

    map.del().await.unwrap();
    assert!(map
        .try_extend(entries)
        .expire_at(SystemTime::now() + secs(10))
        .await
        .unwrap());
    between(map.entry_ttl("key2").await.unwrap(), secs(9), secs(10));

    map.del().await.unwrap();
    assert!(map.try_extend(entries).ttl(secs(1)).await.unwrap());
    assert_eq!(map.get("key1").await.unwrap(), s("value1"));
    sleep(ms(1100)).await;
    assert_eq!(map.get("key1").await.unwrap(), None);
    assert_eq!(map.get("key2").await.unwrap(), None);

    // Without a time the entries are permanent.
    assert!(map.try_extend(entries).await.unwrap());
    assert_eq!(map.entry_ttl("key1").await.unwrap(), None);
}

// Redisson testPutIfAllKeysExistWithTimeToLive, testPutIfAllKeysExistWithExpireAt, testPutIfAllKeysExistWithKeepTTL.
#[tokio::test]
async fn update_many_is_all_or_nothing() {
    let map = native8(&unique("mcn-update-many")).await;
    let entries = [("key1", "value1"), ("key2", "value2")];
    assert!(!map.update_many(entries).ttl(secs(10)).await.unwrap());
    assert_eq!(map.get("key1").await.unwrap(), None);
    assert_eq!(map.get("key2").await.unwrap(), None);

    map.insert("key1", "oldValue1").ttl(secs(30)).await.unwrap();
    map.insert("key2", "oldValue2").ttl(secs(30)).await.unwrap();
    assert!(map.update_many(entries).ttl(secs(10)).await.unwrap());
    assert_eq!(map.get("key1").await.unwrap(), s("value1"));
    assert_eq!(map.get("key2").await.unwrap(), s("value2"));
    between(map.entry_ttl("key1").await.unwrap(), secs(9), secs(10));

    assert!(map
        .update_many(entries)
        .expire_at(SystemTime::now() + secs(20))
        .await
        .unwrap());
    between(map.entry_ttl("key2").await.unwrap(), secs(19), secs(20));

    let before = map.entry_ttl("key1").await.unwrap().unwrap();
    assert!(map
        .update_many([("key1", "kept1"), ("key2", "kept2")])
        .keep_ttl()
        .await
        .unwrap());
    assert_eq!(map.get("key1").await.unwrap(), s("kept1"));
    between(map.entry_ttl("key1").await.unwrap(), ms(1), before);

    map.del().await.unwrap();
    map.insert("key1", "oldValue1").ttl(secs(30)).await.unwrap();
    assert!(!map.update_many(entries).ttl(secs(10)).await.unwrap());
    assert!(!map
        .update_many(entries)
        .expire_at(SystemTime::now() + secs(10))
        .await
        .unwrap());
    assert_eq!(map.get("key1").await.unwrap(), s("oldValue1"));
    assert_eq!(map.get("key2").await.unwrap(), None);
}

// Redisson testPutIfAllKeysAbsentWithEmptyMap.
#[tokio::test]
async fn empty_input_sends_nothing() {
    let map = native8(&unique("mcn-empty")).await;
    let none: [(&str, &str); 0] = [];
    assert!(!map.try_extend(none).ttl(secs(10)).await.unwrap());
    assert!(!map
        .try_extend(none)
        .expire_at(SystemTime::now() + secs(10))
        .await
        .unwrap());
    assert!(!map.update_many(none).ttl(secs(10)).await.unwrap());
    map.extend(none).await.unwrap();
    map.extend(none).ttl(secs(10)).await.unwrap();
    map.extend(none).keep_ttl().await.unwrap();
    assert!(!map.exists().await.unwrap());
}

// Review Focus 3: on Redis 7.4 the HSETEX methods refuse before they write.
#[tokio::test]
async fn hsetex_methods_need_redis_8() {
    let name = unique("mcn-hsetex-74");
    let map = native(&name).await;
    map.insert("a", "1").await.unwrap();
    for outcome in [
        map.try_extend([("b", "1")]).await.map(|_| ()),
        map.try_extend([("b", "1")]).ttl(secs(1)).await.map(|_| ()),
        map.update_many([("a", "2")]).ttl(secs(1)).await.map(|_| ()),
        map.extend([("b", "1")]).keep_ttl().await,
        map.extend([("b", "1")])
            .expire_at(SystemTime::now() + secs(10))
            .await,
    ] {
        match outcome {
            Err(Error::Unsupported(message)) => assert!(message.contains("8.0"), "{message}"),
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }
    assert_eq!(raw_command(&["HLEN", &name]).await, ":1\r\n");
    assert_eq!(map.get("a").await.unwrap(), s("1"));
    map.extend([("b", "1")]).ttl(secs(1)).await.unwrap();
    assert_eq!(map.len().await.unwrap(), 2);
}

#[tokio::test]
async fn redisson_put_all_get_ttl() {
    let map = native(&unique("mcn-simple06")).await;
    assert_eq!(map.get("33").await.unwrap(), None);
    assert_eq!(map.get("55").await.unwrap(), None);
    map.extend([("33", "44"), ("55", "66")])
        .ttl(secs(2))
        .await
        .unwrap();
    assert_eq!(map.get("33").await.unwrap(), s("44"));
    assert_eq!(map.get("55").await.unwrap(), s("66"));
    sleep(secs(1)).await;
    assert_eq!(map.len().await.unwrap(), 2);
    assert_eq!(map.get("33").await.unwrap(), s("44"));
    assert_eq!(map.get("55").await.unwrap(), s("66"));
    sleep(ms(1050)).await;
    assert_eq!(map.get("33").await.unwrap(), None);
    assert_eq!(map.get("55").await.unwrap(), None);
}

// --- Task 5: entry() with a time ---

#[tokio::test]
async fn entry_with_ttl_inserts_an_expiring_value() {
    let map = native(&unique("mcn-entry-ttl")).await;
    let value = map
        .entry("a")
        .ttl(ms(300))
        .await
        .unwrap()
        .or_insert("1".to_string())
        .await
        .unwrap();
    assert_eq!(value, "1");
    between(map.entry_ttl("a").await.unwrap(), ms(1), ms(300));
    sleep(ms(500)).await;
    assert_eq!(map.get("a").await.unwrap(), None);
}

#[tokio::test]
async fn entry_with_expire_at_writes_that_deadline() {
    let map = native(&unique("mcn-entry-at")).await;
    map.insert("a", "1").await.unwrap();
    map.entry("a")
        .expire_at(SystemTime::now() + secs(20))
        .await
        .unwrap()
        .and_modify(|v| v.push('!'))
        .or_insert("x".to_string())
        .await
        .unwrap();
    assert_eq!(map.get("a").await.unwrap(), s("1!"));
    between(map.entry_ttl("a").await.unwrap(), ms(19500), secs(20));
}

// Review Focus 1: a write through a plain entry is an HSET.
#[tokio::test]
async fn entry_without_ttl_clears_the_ttl() {
    let map = native(&unique("mcn-entry-plain")).await;
    map.insert("a", "1").ttl(secs(60)).await.unwrap();
    map.entry("a")
        .await
        .unwrap()
        .and_modify(|v| v.push('!'))
        .or_insert("x".to_string())
        .await
        .unwrap();
    assert_eq!(map.get("a").await.unwrap(), s("1!"));
    assert_eq!(map.entry_ttl("a").await.unwrap(), None);
    // A zero time is no time.
    map.insert("b", "1").ttl(secs(60)).await.unwrap();
    match map.entry("b").ttl(Duration::ZERO).await.unwrap() {
        redissun::map_cache_native::Entry::Occupied(entry) => {
            entry.insert("2".to_string()).await.unwrap();
        }
        redissun::map_cache_native::Entry::Vacant(_) => panic!("b is present"),
    }
    assert_eq!(map.entry_ttl("b").await.unwrap(), None);
    // Reading through an entry writes nothing and keeps the TTL.
    map.insert("c", "1").ttl(secs(60)).await.unwrap();
    map.entry("c")
        .await
        .unwrap()
        .or_insert("x".to_string())
        .await
        .unwrap();
    assert!(map.entry_ttl("c").await.unwrap().is_some());
}

#[tokio::test]
async fn entry_sees_an_expired_entry_as_vacant() {
    let map = native(&unique("mcn-entry-expired")).await;
    map.insert("a", "old").ttl(ms(100)).await.unwrap();
    sleep(ms(250)).await;
    match map.entry("a").ttl(secs(60)).await.unwrap() {
        redissun::map_cache_native::Entry::Vacant(entry) => {
            entry.insert("new".to_string()).await.unwrap();
        }
        redissun::map_cache_native::Entry::Occupied(entry) => {
            panic!("an expired entry is occupied: {:?}", entry.get())
        }
    }
    assert_eq!(map.get("a").await.unwrap(), s("new"));
    between(map.entry_ttl("a").await.unwrap(), secs(59), secs(60));
}

#[tokio::test]
async fn entry_is_atomic_across_tasks() {
    let name = unique("mcn-entry-atomic");
    let map = native_i64(&name).await;
    let tasks: Vec<_> = (0..20)
        .map(|_| {
            let map = map.clone();
            tokio::spawn(async move {
                map.entry("n")
                    .ttl(secs(60))
                    .await?
                    .and_modify(|n| *n += 1)
                    .or_insert(1)
                    .await
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    assert_eq!(map.get("n").await.unwrap(), Some(20));
    assert!(map.entry_ttl("n").await.unwrap().is_some());
}

#[tokio::test]
async fn a_dropped_entry_releases_the_lock() {
    let map = native(&unique("mcn-entry-drop")).await;
    let entry = map.entry("a").ttl(secs(60)).await.unwrap();
    drop(entry);
    let again = tokio::time::timeout(secs(5), map.entry("a"))
        .await
        .expect("the dropped entry kept the lock")
        .unwrap();
    again.or_insert("1".to_string()).await.unwrap();
    assert_eq!(map.get("a").await.unwrap(), s("1"));
}

#[tokio::test]
async fn entry_with_a_bad_time_fails_before_locking() {
    let map = native(&unique("mcn-entry-bad")).await;
    match map.entry("a").expire_at(UNIX_EPOCH - secs(1)).await {
        Err(Error::Config(_)) => {}
        other => panic!("expected Config, got {other:?}"),
    }
    // Another task is another lock owner, so a lock left taken would refuse it.
    let other = map.clone();
    let taken = tokio::spawn(async move {
        let guard = other.lock("a").unwrap().try_lock().await.unwrap();
        let taken = guard.is_some();
        if let Some(guard) = guard {
            guard.unlock().await.unwrap();
        }
        taken
    })
    .await
    .unwrap();
    assert!(taken, "the lock was left taken");
    assert!(!map.contains_key("a").await.unwrap());
}

// --- Task 6: events ---

async fn events_client(url: &str, flags: &str) -> Client {
    raw_command_at(url, &["CONFIG", "SET", "notify-keyspace-events", flags]).await;
    Client::builder()
        .url(url.to_string())
        .build()
        .await
        .unwrap()
}

/// Reads events for up to `within` until every one of `wanted` arrived; other events are skipped.
async fn wait_for_events(
    events: &mut MapEvents<String, JsonCodec>,
    wanted: &[MapEvent<String>],
    within: Duration,
) {
    let mut missing: Vec<MapEvent<String>> = wanted.to_vec();
    let deadline = tokio::time::Instant::now() + within;
    while !missing.is_empty() {
        let event = tokio::time::timeout_at(deadline, events.recv())
            .await
            .unwrap_or_else(|_| panic!("still missing {missing:?}"))
            .unwrap();
        missing.retain(|wanted| *wanted != event);
    }
}

// Redisson testExpiration.
#[tokio::test]
async fn events_report_field_expiry() {
    let (_container, url) = common::own_redis8().await;
    let client = events_client(&url, "ETh").await;
    let map: Native<String, String> = client.map_cache_native(unique("simple"));
    let mut events = map.events().await.unwrap();
    map.insert("1", "2").ttl(secs(1)).await.unwrap();
    map.insert("3", "4")
        .expire_at(SystemTime::now() + secs(2))
        .await
        .unwrap();
    wait_for_events(
        &mut events,
        &[
            MapEvent::EntryExpired { key: s("1") },
            MapEvent::EntryExpired { key: s("3") },
        ],
        secs(10),
    )
    .await;
}

// Redisson testClearExpireListener.
#[tokio::test]
async fn events_report_cleared_ttls() {
    let (_container, url) = common::own_redis8().await;
    let client = events_client(&url, "ETh").await;
    let map: Native<String, String> = client.map_cache_native(unique("simple"));
    let mut events = map
        .events_of(&[MapEventKind::EntryPersisted])
        .await
        .unwrap();
    map.insert("1", "2").ttl(secs(60)).await.unwrap();
    map.insert("3", "4").ttl(secs(60)).await.unwrap();
    assert!(map.persist_entry("1").await.unwrap());
    assert_eq!(map.persist_entries(&["3"]).await.unwrap(), vec![true]);
    wait_for_events(
        &mut events,
        &[
            MapEvent::EntryPersisted { key: s("1") },
            MapEvent::EntryPersisted { key: s("3") },
        ],
        secs(10),
    )
    .await;
}

// Redisson testRemoveListener.
#[tokio::test]
async fn events_report_put_and_remove() {
    let (_container, url) = common::own_redis8().await;
    let client = events_client(&url, "ETh").await;
    let map: Native<String, String> = client.map_cache_native(unique("testRemoveListener"));
    let mut events = map.events().await.unwrap();
    map.insert("1", "1").await.unwrap();
    map.remove("1").await.unwrap();
    wait_for_events(
        &mut events,
        &[
            MapEvent::Put { key: s("1") },
            MapEvent::Removed { key: s("1") },
        ],
        secs(5),
    )
    .await;
}

#[tokio::test]
async fn field_events_without_subkey_names_have_no_key() {
    let (_container, url) = common::own_redis().await;
    let client = events_client(&url, "Eh").await;
    let map: Native<String, String> = client.map_cache_native(unique("mcn-events-74"));
    let mut events = map.events_of(&[MapEventKind::EntryExpired]).await.unwrap();
    map.insert("1", "2").ttl(ms(200)).await.unwrap();
    wait_for_events(
        &mut events,
        &[MapEvent::EntryExpired { key: None }],
        secs(10),
    )
    .await;
}

#[tokio::test]
async fn hash_map_rejects_the_field_ttl_kinds() {
    let client = client().await;
    let name = unique("mcn-events-plain");
    for kind in [MapEventKind::EntryExpired, MapEventKind::EntryPersisted] {
        match client
            .hash_map::<String, String>(name.clone())
            .events_of(&[kind])
            .await
        {
            Err(Error::Config(message)) => assert!(message.contains("MapCacheNative"), "{message}"),
            other => panic!("expected Config, got {other:?}"),
        }
        match client
            .hash_map_cache::<String, String>(name.clone())
            .keyspace_events_of(&[MapEventKind::Put, kind])
            .await
        {
            Err(Error::Config(_)) => {}
            other => panic!("expected Config, got {other:?}"),
        }
    }
}

// --- Task 6: the rest of RedissonMapCacheNativeTest ---

async fn native_of<K, V>(name: &str) -> Native<K, V> {
    client().await.map_cache_native(unique(name))
}

async fn sorted<T: Ord>(stream: impl futures::Stream<Item = redissun::Result<T>>) -> Vec<T> {
    let mut items: Vec<T> = stream.try_collect().await.unwrap();
    items.sort();
    items
}

#[tokio::test]
async fn redisson_cache_values() {
    let map: Native<String, String> = native_of("testRMapCacheValues").await;
    map.insert("1234", "5678").ttl(secs(60)).await.unwrap();
    map.insert("4321", "8765")
        .expire_at(SystemTime::now() + secs(60))
        .await
        .unwrap();
    assert_eq!(sorted(map.values()).await, ["5678", "8765"]);
    map.del().await.unwrap();
}

#[tokio::test]
async fn redisson_get_all_ttl() {
    let map: Native<i32, i32> = native_of("getAll").await;
    map.insert(&1, &100).await.unwrap();
    map.insert(&2, &200).ttl(secs(1)).await.unwrap();
    map.insert(&3, &300).ttl(secs(1)).await.unwrap();
    map.insert(&4, &400).await.unwrap();
    map.insert(&5, &500)
        .expire_at(SystemTime::now() + secs(1))
        .await
        .unwrap();
    assert_eq!(
        map.get_many(&[&2, &3, &5, &6]).await.unwrap(),
        [Some(200), Some(300), Some(500), None]
    );
    sleep(ms(1050)).await;
    assert_eq!(
        map.get_many(&[&2, &3, &5, &6]).await.unwrap(),
        [None, None, None, None]
    );
    map.del().await.unwrap();
}

#[tokio::test]
async fn redisson_get_all_with_string_keys() {
    let map: Native<String, i32> = native_of("getAllStrings").await;
    for (k, v) in [("A", 100), ("B", 200), ("C", 300), ("D", 400)] {
        map.insert(k, &v).await.unwrap();
    }
    assert_eq!(
        map.get_many(&["B", "C", "E"]).await.unwrap(),
        [Some(200), Some(300), None]
    );
    map.del().await.unwrap();
}

#[tokio::test]
async fn redisson_expired_iterator() {
    let cache: Native<String, String> = native_of("simple").await;
    cache.insert("0", "8").await.unwrap();
    cache.insert("1", "6").ttl(secs(1)).await.unwrap();
    cache.insert("2", "4").ttl(secs(3)).await.unwrap();
    cache.insert("3", "2").ttl(secs(4)).await.unwrap();
    cache.insert("4", "4").ttl(secs(1)).await.unwrap();
    let now = SystemTime::now();
    cache
        .insert("5", "6")
        .expire_at(now + secs(1))
        .await
        .unwrap();
    cache
        .insert("6", "4")
        .expire_at(now + secs(3))
        .await
        .unwrap();
    cache
        .insert("7", "2")
        .expire_at(now + secs(4))
        .await
        .unwrap();
    cache
        .insert("8", "4")
        .expire_at(now + secs(1))
        .await
        .unwrap();
    sleep(ms(1050)).await;
    assert_eq!(sorted(cache.keys()).await, ["0", "2", "3", "6", "7"]);
    cache.del().await.unwrap();
}

#[tokio::test]
async fn redisson_expire() {
    let cache: Native<String, String> = native_of("simple").await;
    cache.insert("0", "8").ttl(secs(1)).await.unwrap();
    cache
        .insert("1", "8")
        .expire_at(SystemTime::now() + secs(1))
        .await
        .unwrap();
    assert!(cache.expire(ms(100)).await.unwrap());
    sleep(ms(500)).await;
    assert_eq!(cache.len().await.unwrap(), 0);
}

// Redisson testClear: clearExpire(k) is Boolean there, null for a missing key; here persist_entry gives false.
#[tokio::test]
async fn redisson_clear() {
    let cache: Native<String, String> = native_of("simple").await;
    for k in ["0", "02", "03"] {
        cache.insert(k, "8").ttl(secs(1)).await.unwrap();
    }
    for k in ["04", "05", "06"] {
        cache
            .insert(k, "8")
            .expire_at(SystemTime::now() + secs(1))
            .await
            .unwrap();
    }
    assert!(cache.persist_entry("0").await.unwrap());
    assert!(!cache.persist_entry("01").await.unwrap());
    assert!(cache.persist_entry("04").await.unwrap());
    assert_eq!(
        cache
            .persist_entries(&["0", "02", "03", "04", "05", "06", "07"])
            .await
            .unwrap(),
        [false, true, true, false, true, true, false]
    );
    cache.del().await.unwrap();
}

// Redisson testClearExpire: Object::expire and Object::persist on the whole hash.
#[tokio::test]
async fn redisson_clear_expire() {
    let cache: Native<String, String> = native_of("simple").await;
    cache.insert("0", "8").ttl(secs(1)).await.unwrap();
    cache
        .insert("1", "8")
        .expire_at(SystemTime::now() + secs(1))
        .await
        .unwrap();
    assert!(cache.expire(ms(100)).await.unwrap());
    assert!(cache.persist().await.unwrap());
    sleep(ms(500)).await;
    assert_eq!(cache.len().await.unwrap(), 2);
    cache.del().await.unwrap();
}

#[tokio::test]
async fn redisson_entry_set() {
    let map: Native<i32, String> = native_of("simple12").await;
    map.insert(&1, "12").await.unwrap();
    map.insert(&2, "33").ttl(secs(1)).await.unwrap();
    map.insert(&3, "43").await.unwrap();
    map.insert(&4, "56")
        .expire_at(SystemTime::now() + secs(1))
        .await
        .unwrap();
    let entries = sorted(map.iter()).await;
    assert!(entries.contains(&(1, "12".to_string())));
    assert!(entries.contains(&(3, "43".to_string())));
    assert_eq!(map.len().await.unwrap(), 4);
    map.del().await.unwrap();
}

#[tokio::test]
async fn redisson_key_set() {
    let map: Native<String, String> = native_of("simple03").await;
    map.insert("33", "44").ttl(secs(1)).await.unwrap();
    map.insert("1", "2").await.unwrap();
    map.insert("55", "2")
        .expire_at(SystemTime::now() + secs(1))
        .await
        .unwrap();
    assert_eq!(sorted(map.keys()).await, ["1", "33", "55"]);
    sleep(ms(1050)).await;
    assert_eq!(sorted(map.keys()).await, ["1"]);
    map.del().await.unwrap();
}

#[tokio::test]
async fn redisson_values() {
    let map: Native<String, String> = native_of("simple05").await;
    map.insert("33", "44").ttl(secs(1)).await.unwrap();
    map.insert("55", "66")
        .expire_at(SystemTime::now() + secs(1))
        .await
        .unwrap();
    map.insert("1", "2").await.unwrap();
    assert_eq!(sorted(map.values()).await, ["2", "44", "66"]);
    sleep(ms(1050)).await;
    assert_eq!(sorted(map.values()).await, ["2"]);
    map.del().await.unwrap();
}

async fn pattern_map() -> MapCacheNative<String, String, redissun::StringCodec> {
    let map = client()
        .await
        .with_codec(redissun::StringCodec)
        .map_cache_native::<String, String>(unique("simple"));
    map.insert("10", "100").await.unwrap();
    map.insert("20", "200").ttl(secs(60)).await.unwrap();
    map.insert("30", "300").await.unwrap();
    map.insert("40", "400")
        .expire_at(SystemTime::now() + secs(60))
        .await
        .unwrap();
    map
}

fn pattern(p: &str) -> MapScan {
    MapScan::new().pattern(p)
}

#[tokio::test]
async fn redisson_key_set_by_pattern_ttl() {
    let map = pattern_map().await;
    assert_eq!(
        sorted(map.keys_with(pattern("?0"))).await,
        ["10", "20", "30", "40"]
    );
    assert!(sorted(map.keys_with(pattern("1"))).await.is_empty());
    assert_eq!(sorted(map.keys_with(pattern("10"))).await, ["10"]);
    map.del().await.unwrap();
}

#[tokio::test]
async fn redisson_values_by_pattern_ttl() {
    let map = pattern_map().await;
    assert_eq!(
        sorted(map.values_with(pattern("?0"))).await,
        ["100", "200", "300", "400"]
    );
    assert!(sorted(map.values_with(pattern("1"))).await.is_empty());
    assert_eq!(sorted(map.values_with(pattern("10"))).await, ["100"]);
    map.del().await.unwrap();
}

#[tokio::test]
async fn redisson_entry_set_by_pattern_ttl() {
    let map = pattern_map().await;
    let pair = |k: &str, v: &str| (k.to_string(), v.to_string());
    assert_eq!(
        sorted(map.iter_with(pattern("?0"))).await,
        [
            pair("10", "100"),
            pair("20", "200"),
            pair("30", "300"),
            pair("40", "400")
        ]
    );
    assert!(sorted(map.iter_with(pattern("1"))).await.is_empty());
    assert_eq!(
        sorted(map.iter_with(pattern("10"))).await,
        [pair("10", "100")]
    );
    map.del().await.unwrap();
}

#[tokio::test]
async fn redisson_contains_value_ttl() {
    let map: Native<String, String> = native_of("simple01").await;
    assert!(!map.contains_value("34").await.unwrap());
    map.insert("33", "44").ttl(secs(1)).await.unwrap();
    map.insert("55", "66")
        .expire_at(SystemTime::now() + secs(1))
        .await
        .unwrap();
    assert!(map.contains_value("44").await.unwrap());
    assert!(map.contains_value("66").await.unwrap());
    assert!(!map.contains_value("34").await.unwrap());
    sleep(ms(1050)).await;
    assert!(!map.contains_value("44").await.unwrap());
    assert!(!map.contains_value("66").await.unwrap());
    map.del().await.unwrap();
}

#[tokio::test]
async fn redisson_contains_key_ttl() {
    let map: Native<String, String> = native_of("simple30").await;
    map.insert("33", "44").ttl(secs(1)).await.unwrap();
    map.insert("55", "66")
        .expire_at(SystemTime::now() + secs(1))
        .await
        .unwrap();
    assert!(map.contains_key("33").await.unwrap());
    assert!(map.contains_key("55").await.unwrap());
    assert!(!map.contains_key("34").await.unwrap());
    sleep(ms(1050)).await;
    assert!(!map.contains_key("33").await.unwrap());
    assert!(!map.contains_key("55").await.unwrap());
    map.del().await.unwrap();
}

#[tokio::test]
async fn redisson_remove_value_ttl() {
    let map: Native<String, String> = native_of("simple").await;
    map.insert("1", "2").ttl(secs(1)).await.unwrap();
    map.insert("3", "4")
        .expire_at(SystemTime::now() + secs(1))
        .await
        .unwrap();
    assert!(map.remove_if_eq("1", "2").await.unwrap());
    assert!(map.remove_if_eq("3", "4").await.unwrap());
    assert_eq!(map.get("1").await.unwrap(), None);
    assert_eq!(map.get("3").await.unwrap(), None);
    assert_eq!(map.len().await.unwrap(), 0);

    map.insert("5", "6").ttl(secs(1)).await.unwrap();
    map.insert("7", "8")
        .expire_at(SystemTime::now() + secs(1))
        .await
        .unwrap();
    sleep(ms(1050)).await;
    assert!(!map.remove_if_eq("5", "6").await.unwrap());
    assert!(!map.remove_if_eq("7", "8").await.unwrap());
    assert_eq!(map.get("5").await.unwrap(), None);
    assert_eq!(map.get("7").await.unwrap(), None);
    map.del().await.unwrap();
}

#[tokio::test]
async fn redisson_remove_value_fail() {
    let map: Native<String, String> = native_of("simple").await;
    map.insert("1", "2").await.unwrap();
    assert!(!map.remove_if_eq("2", "1").await.unwrap());
    assert!(!map.remove_if_eq("1", "3").await.unwrap());
    assert_eq!(map.get("1").await.unwrap(), s("2"));
    map.del().await.unwrap();
}

#[tokio::test]
async fn redisson_replace_old_value_fail() {
    let map: Native<String, String> = native_of("simple").await;
    map.insert("1", "2").await.unwrap();
    assert!(!map.compare_and_set("1", "43", "31").await.unwrap());
    assert_eq!(map.get("1").await.unwrap(), s("2"));
    map.del().await.unwrap();
}

#[tokio::test]
async fn redisson_replace_old_value_success() {
    let map: Native<String, String> = native_of("simple").await;
    map.insert("1", "2").await.unwrap();
    assert!(map.compare_and_set("1", "2", "3").await.unwrap());
    assert!(!map.compare_and_set("1", "2", "3").await.unwrap());
    assert_eq!(map.get("1").await.unwrap(), s("3"));
    map.del().await.unwrap();
}

// Redisson testEntryUpdate: a zero Duration is no TTL in both; Instant.ofEpochMilli(0) deletes at once in both.
#[tokio::test]
async fn redisson_entry_update() {
    let map: Native<i32, i32> = native_of("simple").await;
    map.insert(&1, &1).ttl(secs(1)).await.unwrap();
    assert_eq!(map.get(&1).await.unwrap(), Some(1));
    map.insert(&2, &2)
        .expire_at(SystemTime::now() + secs(1))
        .await
        .unwrap();
    assert_eq!(map.get(&2).await.unwrap(), Some(2));
    sleep(ms(1050)).await;
    assert_eq!(map.insert(&1, &1).ttl(Duration::ZERO).await.unwrap(), None);
    assert_eq!(
        map.insert(&2, &2).expire_at(UNIX_EPOCH).await.unwrap(),
        None
    );
    assert_eq!(map.get(&1).await.unwrap(), Some(1));
    assert_eq!(map.get(&2).await.unwrap(), None);
    map.del().await.unwrap();
}

/// Redisson testTTLExpiration: entries of 1, 2 and 3 seconds, by `ttl` and by `expire_at`, checked each second.
async fn ttl_expiration(map: &Native<String, i32>) {
    let check = |left: [Option<i32>; 6]| async move {
        let got = map
            .get_many(&["12", "14", "15", "16", "18", "19"])
            .await
            .unwrap();
        assert_eq!(got, left);
    };
    sleep(ms(1050)).await;
    check([None, Some(2), Some(3), None, Some(2), Some(3)]).await;
    sleep(secs(1)).await;
    check([None, None, Some(3), None, None, Some(3)]).await;
    sleep(secs(1)).await;
    check([None; 6]).await;
    map.clear().await.unwrap();
}

// Redisson testTTL with put, fastPut, putIfAbsent and fastPutIfAbsent.
#[tokio::test]
async fn redisson_ttl() {
    let map: Native<String, i32> = native_of("simple").await;
    let spec = [("12", 1, 1), ("14", 2, 2), ("15", 3, 3)];
    let at = [("16", 1, 1), ("18", 2, 2), ("19", 3, 3)];

    for (k, v, n) in spec {
        map.insert(k, &v).ttl(secs(n)).await.unwrap();
    }
    let now = SystemTime::now();
    for (k, v, n) in at {
        map.insert(k, &v).expire_at(now + secs(n)).await.unwrap();
    }
    ttl_expiration(&map).await;

    for (k, v, n) in spec {
        map.set(k, &v).ttl(secs(n)).await.unwrap();
    }
    let now = SystemTime::now();
    for (k, v, n) in at {
        map.set(k, &v).expire_at(now + secs(n)).await.unwrap();
    }
    ttl_expiration(&map).await;

    // putIfAbsent and fastPutIfAbsent are both try_insert.
    for (k, v, n) in spec {
        map.try_insert(k, &v).ttl(secs(n)).await.unwrap();
    }
    let now = SystemTime::now();
    for (k, v, n) in at {
        map.try_insert(k, &v)
            .expire_at(now + secs(n))
            .await
            .unwrap();
    }
    ttl_expiration(&map).await;
    map.del().await.unwrap();
}

#[tokio::test]
async fn redisson_rmap_cache_values() {
    let map: Native<String, String> = native_of("testRMapCacheValues").await;
    map.insert("1234", "5678").ttl(secs(60)).await.unwrap();
    map.insert("4321", "8765")
        .expire_at(SystemTime::now() + secs(60))
        .await
        .unwrap();
    assert_eq!(sorted(map.values()).await, ["5678", "8765"]);
    map.del().await.unwrap();
}

#[tokio::test]
async fn redisson_read_all_entry_set() {
    let map: Native<i32, String> = native_of("simple12").await;
    map.insert(&1, "12").await.unwrap();
    map.insert(&2, "33").ttl(secs(600)).await.unwrap();
    map.insert(&3, "43").await.unwrap();
    map.insert(&4, "55")
        .expire_at(SystemTime::now() + secs(600))
        .await
        .unwrap();
    let all: HashMap<i32, String> = map.to_hash_map().await.unwrap();
    let streamed: HashMap<i32, String> = map.iter().try_collect().await.unwrap();
    assert_eq!(all, streamed);
    assert_eq!(all.len(), 4);
    map.del().await.unwrap();
}

#[tokio::test]
async fn redisson_read_all_values_ttl() {
    let map: Native<String, String> = native_of("testRMapCacheAllValues").await;
    map.insert("1234", "5678").ttl(secs(60)).await.unwrap();
    map.insert("4321", "8765").ttl(secs(60)).await.unwrap();
    let mut values: Vec<String> = map.to_hash_map().await.unwrap().into_values().collect();
    values.sort();
    assert_eq!(values, ["5678", "8765"]);
    map.del().await.unwrap();
}

// --- Task 6: 0a's ports of Redisson 4.8.0 BaseMapTest, run on a MapCacheNative ---

mod base_map_on_native {
    use super::common::{client, own_redis, own_redis8, raw_command_at, unique};
    use super::events_client;
    use futures::TryStreamExt;
    use redissun::map_cache_native::Entry;
    use redissun::{
        JsonCodec, MapCacheNative, MapEvent, MapEventKind, MapEvents, MapScan, StringCodec,
    };
    use serde::{Deserialize, Serialize};
    use std::time::Duration;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct User {
        name: String,
    }

    async fn native<K, V>(prefix: &str) -> MapCacheNative<K, V, JsonCodec> {
        client().await.map_cache_native(unique(prefix))
    }

    async fn string_map(prefix: &str) -> MapCacheNative<String, String, StringCodec> {
        client()
            .await
            .with_codec(StringCodec)
            .map_cache_native(unique(prefix))
    }

    async fn next_event(events: &mut MapEvents<String, JsonCodec>) -> MapEvent<String> {
        tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("no event within 2 s")
            .unwrap()
    }

    // Redisson: testRandomEntries
    #[tokio::test]
    async fn redisson_random_entries_on_native() {
        let map = native::<i32, i32>("map").await;
        assert!(map.random_entries(1).await.unwrap().is_empty());
        for (k, v) in [(1, 11), (2, 21), (3, 31), (4, 41)] {
            map.insert(&k, &v).await.unwrap();
        }
        let picked = map.random_entries(2).await.unwrap();
        assert_eq!(picked.len(), 2);
        assert!(picked.keys().all(|k| (1..=4).contains(k)));
    }

    // Redisson: testComputeIfPresent
    #[tokio::test]
    async fn redisson_compute_if_present_on_native() {
        let map = native::<String, String>("map").await;
        if let Entry::Occupied(entry) = map.entry("1").await.unwrap() {
            entry.insert("12".to_string()).await.unwrap();
        }
        assert_eq!(map.get("1").await.unwrap(), None);
        map.insert("1", "10").await.unwrap();
        if let Entry::Occupied(entry) = map.entry("1").await.unwrap() {
            assert_eq!(entry.get(), "10");
            entry.insert("12".to_string()).await.unwrap();
        }
        assert_eq!(map.get("1").await.unwrap().as_deref(), Some("12"));
    }

    // Redisson: testComputeIfPresentMutable
    #[tokio::test]
    async fn redisson_compute_if_present_mutable_on_native() {
        let map = native::<String, User>("map").await;
        let user = User {
            name: "value1".into(),
        };
        map.insert("1", &user).await.unwrap();
        map.entry("1")
            .await
            .unwrap()
            .and_modify(|value| {
                assert_eq!(value.name, "value1");
                value.name = "value2".into();
            })
            .or_insert(user)
            .await
            .unwrap();
        assert_eq!(
            map.get("1").await.unwrap(),
            Some(User {
                name: "value2".into()
            })
        );
    }

    // Redisson: testComputeIfAbsent
    #[tokio::test]
    async fn redisson_compute_if_absent_on_native() {
        let map = native::<String, String>("map").await;
        map.entry("1")
            .await
            .unwrap()
            .or_insert("12".into())
            .await
            .unwrap();
        assert_eq!(map.get("1").await.unwrap().as_deref(), Some("12"));
        map.entry("1")
            .await
            .unwrap()
            .or_insert("13".into())
            .await
            .unwrap();
        assert_eq!(map.get("1").await.unwrap().as_deref(), Some("12"));
    }

    // Redisson: testMerge
    #[tokio::test]
    async fn redisson_merge_on_native() {
        let map = native::<String, String>("map").await;
        map.entry("1")
            .await
            .unwrap()
            .and_modify(|v| *v = "12".into())
            .or_insert("2".into())
            .await
            .unwrap();
        assert_eq!(map.get("1").await.unwrap().as_deref(), Some("2"));
        map.entry("1")
            .await
            .unwrap()
            .and_modify(|v| *v = "12".into())
            .or_insert("2".into())
            .await
            .unwrap();
        assert_eq!(map.get("1").await.unwrap().as_deref(), Some("12"));
        if let Entry::Occupied(entry) = map.entry("1").await.unwrap() {
            entry.remove().await.unwrap();
        }
        assert_eq!(map.get("1").await.unwrap(), None);
    }

    // Redisson: testCompute, testComputeAsync
    #[tokio::test]
    async fn redisson_compute_on_native() {
        let map = native::<String, String>("map").await;
        let first = map
            .entry("1")
            .await
            .unwrap()
            .or_insert("12".into())
            .await
            .unwrap();
        assert_eq!(first, "12");
        let second = map
            .entry("1")
            .await
            .unwrap()
            .and_modify(|old| old.push_str("34"))
            .or_insert("12".into())
            .await
            .unwrap();
        assert_eq!(second, "1234");
        assert_eq!(map.get("1").await.unwrap().as_deref(), Some("1234"));
        if let Entry::Occupied(entry) = map.entry("1").await.unwrap() {
            assert_eq!(entry.remove().await.unwrap(), "1234");
        }
        assert_eq!(map.get("1").await.unwrap(), None);
    }

    // Redisson: testGetAll, testGetAllWithStringKeys
    #[tokio::test]
    async fn redisson_get_all_on_native() {
        let map = native::<i32, i32>("map").await;
        for (k, v) in [(1, 100), (2, 200), (3, 300), (4, 400)] {
            map.insert(&k, &v).await.unwrap();
        }
        let found = map.get_many(&[&2, &3, &5]).await.unwrap();
        assert_eq!(found, [Some(200), Some(300), None]);
    }

    // Redisson: testGetAllBig
    #[tokio::test]
    async fn redisson_get_all_big_on_native() {
        let map = native::<i32, String>("map").await;
        let keys: Vec<i32> = (0..10_000).collect();
        let values: Vec<String> = keys.iter().map(i32::to_string).collect();
        map.extend(keys.iter().zip(values.iter())).await.unwrap();
        let wanted: Vec<&i32> = keys.iter().collect();
        let all = map.get_many(&wanted).await.unwrap();
        assert_eq!(all.len(), 10_000);
        assert_eq!(all[9_999].as_deref(), Some("9999"));
        assert_eq!(map.len().await.unwrap(), 10_000);
    }

    // Redisson: testPutAllBatched
    #[tokio::test]
    async fn redisson_put_all_batched_on_native() {
        let map = native::<i32, String>("map").await;
        for k in 1..=3 {
            map.insert(&k, &k.to_string()).await.unwrap();
        }
        for range in [4..=6, 7..=11, 12..=18] {
            let keys: Vec<i32> = range.collect();
            let values: Vec<String> = keys.iter().map(i32::to_string).collect();
            map.extend(keys.iter().zip(values.iter()))
                .batch_size(5)
                .await
                .unwrap();
        }
        let mut keys = map.keys().try_collect::<Vec<_>>().await.unwrap();
        keys.sort_unstable();
        assert_eq!(keys, (1..=18).collect::<Vec<_>>());
    }

    // Redisson: testKeySetByPattern, testValuesByPattern, testEntrySetByPattern
    #[tokio::test]
    async fn redisson_scans_by_pattern_on_native() {
        let map = string_map("simple").await;
        for (k, v) in [("10", "100"), ("20", "200"), ("30", "300")] {
            map.insert(k, v).await.unwrap();
        }
        let keys = |pattern: &'static str| {
            let map = map.clone();
            async move {
                let mut keys: Vec<String> = map
                    .keys_with(MapScan::new().pattern(pattern))
                    .try_collect()
                    .await
                    .unwrap();
                keys.sort();
                keys
            }
        };
        assert_eq!(keys("?0").await, ["10", "20", "30"]);
        assert!(keys("1").await.is_empty());
        assert_eq!(keys("10").await, ["10"]);
        let mut values: Vec<String> = map
            .values_with(MapScan::new().pattern("?0"))
            .try_collect()
            .await
            .unwrap();
        values.sort();
        assert_eq!(values, ["100", "200", "300"]);
        let entries: Vec<(String, String)> = map
            .iter_with(MapScan::new().pattern("10"))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(entries, [("10".to_string(), "100".to_string())]);
    }

    // Redisson: testReadAllEntrySetWithPattern, testReadAllKeySetWithPattern, testReadAllValuesWithPattern
    #[tokio::test]
    async fn redisson_read_all_with_pattern_on_native() {
        let map = string_map("simple12").await;
        for (k, v) in [("10", "12"), ("12", "33"), ("21", "43")] {
            map.insert(k, v).await.unwrap();
        }
        let scan = || MapScan::new().pattern("1?");
        let entries: Vec<(String, String)> = map.iter_with(scan()).try_collect().await.unwrap();
        assert_eq!(entries.len(), 2);
        let mut keys: Vec<String> = map.keys_with(scan()).try_collect().await.unwrap();
        keys.sort();
        assert_eq!(keys, ["10", "12"]);
        let mut values: Vec<String> = map.values_with(scan()).try_collect().await.unwrap();
        values.sort();
        assert_eq!(values, ["12", "33"]);
    }

    // Redisson: testIncrListener (notify-keyspace-events "Th": subkey events only)
    #[tokio::test]
    async fn redisson_incr_listener_on_native() {
        let (_container, url) = own_redis8().await;
        let client = events_client(&url, "Th").await;
        let map = client.map_cache_native::<String, f64>(unique("testIncrListener"));
        let mut events = map.events_of(&[MapEventKind::Incremented]).await.unwrap();
        map.incr_by_float("1", 1.0).await.unwrap();
        match next_event(&mut events).await {
            MapEvent::Incremented { key } => assert_eq!(key.as_deref(), Some("1")),
            other => panic!("expected an increment, got {other:?}"),
        }
        // Redisson's removeListener: dropping the handle unsubscribes.
        drop(events);
        for _ in 0..20 {
            if raw_command_at(&url, &["PUBSUB", "NUMPAT"]).await == ":0\r\n" {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("the patterns stayed subscribed after the events were dropped");
    }

    // Redisson: testPutRemoveListener
    #[tokio::test]
    async fn redisson_put_remove_listener_on_native() {
        let (_container, url) = own_redis8().await;
        let client = events_client(&url, "Th").await;
        let map = client
            .with_codec(StringCodec)
            .map_cache_native::<String, String>(unique("testPutRemoveListener"));
        let mut events = map.events().await.unwrap();
        map.insert("1", "2").await.unwrap();
        map.remove("1").await.unwrap();
        let mut seen = vec![];
        for _ in 0..2 {
            seen.push(
                tokio::time::timeout(Duration::from_secs(2), events.recv())
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        assert!(seen.contains(&MapEvent::Put {
            key: Some("1".into())
        }));
        assert!(seen.contains(&MapEvent::Removed {
            key: Some("1".into())
        }));
    }

    // Redisson: testPutListenerWithKeyevent
    #[tokio::test]
    async fn redisson_put_listener_with_keyevent_on_native() {
        let (_container, url) = own_redis().await;
        let client = events_client(&url, "Eh").await;
        let map = client.map_cache_native::<String, String>(unique("testPutListenerWithKeyevent"));
        let mut events = map.events_of(&[MapEventKind::Put]).await.unwrap();
        map.insert("1", "2").await.unwrap();
        assert_eq!(next_event(&mut events).await, MapEvent::Put { key: None });
    }

    // Redisson: testPutListenerWithKeyeventAndSubkeyevent
    #[tokio::test]
    async fn redisson_put_listener_with_keyevent_and_subkeyevent_on_native() {
        let (_container, url) = own_redis8().await;
        let client = events_client(&url, "TEh").await;
        let map = client.map_cache_native::<String, String>(unique(
            "testPutListenerWithKeyeventAndSubkeyevent",
        ));
        let mut events = map.events_of(&[MapEventKind::Put]).await.unwrap();
        map.insert("1", "2").await.unwrap();
        assert_eq!(
            next_event(&mut events).await,
            MapEvent::Put {
                key: Some("1".into())
            }
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(200), events.recv())
                .await
                .is_err()
        );
    }
}

// --- Task 6: Review Focus 5, Redis Cluster ---

#[tokio::test]
async fn every_operation_runs_on_a_cluster() {
    let topology = common::topology::Topology::cluster().await;
    let client = Client::builder()
        .url(topology.cluster_url())
        .build()
        .await
        .expect("could not connect to the cluster");
    let u = unique("mcn-cluster");
    for name in [u.clone(), format!("{{t}}:{u}")] {
        let map: Native<String, String> = client.map_cache_native(name.clone());
        let minute = secs(60);
        assert_eq!(
            map.insert("a", "1").ttl(minute).await.unwrap(),
            None,
            "{name}"
        );
        assert!(map
            .set("b", "2")
            .expire_at(SystemTime::now() + minute)
            .await
            .unwrap());
        assert_eq!(map.try_insert("c", "3").ttl(minute).await.unwrap(), None);
        assert_eq!(map.replace("a", "4").ttl(minute).await.unwrap(), s("1"));
        map.extend([("d", "5"), ("e", "6")])
            .ttl(minute)
            .await
            .unwrap();
        between(map.entry_ttl("a").await.unwrap(), secs(59), minute);
        assert_eq!(map.entry_ttls(&["b", "x"]).await.unwrap().len(), 2);
        assert!(map.expire_entry("a").ttl(secs(30)).if_less().await.unwrap());
        assert_eq!(
            map.expire_entries(&["d", "e"]).ttl(secs(30)).await.unwrap(),
            2
        );
        assert_eq!(
            map.persist_entries(&["d", "x"]).await.unwrap(),
            vec![true, false]
        );
        map.entry("f")
            .ttl(minute)
            .await
            .unwrap()
            .or_insert("7".to_string())
            .await
            .unwrap();
        assert!(map.entry_ttl("f").await.unwrap().is_some());
        let guard = map.lock("a").unwrap().lock().await.unwrap();
        guard.unlock().await.unwrap();
        let entries: Vec<(String, String)> =
            map.iter_with(MapScan::new()).try_collect().await.unwrap();
        assert_eq!(entries.len(), 6, "{name}");
        assert!(map.del().await.unwrap());
    }
}
