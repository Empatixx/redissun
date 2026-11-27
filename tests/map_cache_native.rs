//! Tests of `MapCacheNative`, Redisson's `RMapCacheNative`.
//!
//! Ported from Redisson 4.8.0 `RedissonMapCacheNativeTest` (named `redisson_<snake_case>`): filled in as the methods arrive.

mod common;

use common::{client, own_redis_tag, raw_command, raw_command_at, unique};
use futures::TryStreamExt;
use redissun::{Client, Error, JsonCodec, MapCacheNative, MapScan, Object};
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
