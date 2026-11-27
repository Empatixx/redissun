mod common;

use common::{client, raw_command, unique};
use futures::TryStreamExt;
use redissun::{JsonCodec, Object, SetMultimap};
use std::time::Duration;
use tokio::time::sleep;

async fn set(name: &str) -> SetMultimap<String, String, JsonCodec> {
    client().await.set_multimap(name.to_string())
}

#[tokio::test]
async fn layout_matches_redisson() {
    let name = unique("smm");
    let map = set(&name).await;
    assert!(map.insert("key", "v").await.unwrap());
    assert_eq!(
        raw_command(&["HGET", &name, "\"key\""]).await,
        "$22\r\nIKm9EBQhlzpVIBxUXDHBWw\r\n"
    );
    let values = format!("{{{name}}}:IKm9EBQhlzpVIBxUXDHBWw");
    assert_eq!(
        raw_command(&["SMEMBERS", &values]).await,
        "*1\r\n$3\r\n\"v\"\r\n"
    );
}

#[tokio::test]
async fn insert_appends_or_adds() {
    let map = set(&unique("smm")).await;
    assert!(map.insert("k", "1").await.unwrap());
    assert!(map.insert("k", "3").await.unwrap());
    assert!(!map.insert("k", "3").await.unwrap());
    let mut all = map.get_all("k").await.unwrap();
    all.sort();
    assert_eq!(all, ["1", "3"]);
}

#[tokio::test]
async fn len_counts_values_and_key_len_counts_keys() {
    let map = set(&unique("smm")).await;
    map.insert("a", "1").await.unwrap();
    map.insert("a", "2").await.unwrap();
    map.insert("b", "3").await.unwrap();
    assert_eq!(map.len().await.unwrap(), 3);
    assert_eq!(map.key_len().await.unwrap(), 2);
    assert!(!map.is_empty().await.unwrap());

    let empty = set(&unique("smm")).await;
    assert_eq!(empty.len().await.unwrap(), 0);
    assert_eq!(empty.key_len().await.unwrap(), 0);
    assert!(empty.is_empty().await.unwrap());
}

#[tokio::test]
async fn contains_key_reads_the_values() {
    let map = set(&unique("smm")).await;
    map.insert("a", "1").await.unwrap();
    assert!(map.contains_key("a").await.unwrap());
    assert!(!map.contains_key("b").await.unwrap());
}

#[tokio::test]
async fn reads_data_written_like_redisson() {
    let name = unique("smm");
    let values = format!("{{{name}}}:IKm9EBQhlzpVIBxUXDHBWw");
    raw_command(&["HSET", &name, "\"key\"", "IKm9EBQhlzpVIBxUXDHBWw"]).await;
    raw_command(&["SADD", &values, "\"x\"", "\"y\""]).await;
    let map = set(&name).await;
    let mut all = map.get_all("key").await.unwrap();
    all.sort();
    assert_eq!(all, ["x", "y"]);
    assert_eq!(map.len().await.unwrap(), 2);
    assert_eq!(map.key_len().await.unwrap(), 1);
}

fn values_key(name: &str, hash: &str) -> String {
    format!("{{{name}}}:{hash}")
}

async fn sorted(map: &SetMultimap<String, String, JsonCodec>, k: &str) -> Vec<String> {
    let mut all = map.get_all(k).await.unwrap();
    all.sort();
    all
}

#[tokio::test]
async fn extend_adds_new_values() {
    let map = set(&unique("smm")).await;
    assert!(map.extend("0", ["1", "2", "2"]).await.unwrap());
    assert!(!map.extend("0", ["1"]).await.unwrap());
    assert_eq!(sorted(&map, "0").await, ["1", "2"]);
}

#[tokio::test]
async fn remove_drops_the_key_with_its_last_value() {
    let name = unique("smm");
    let map = set(&name).await;
    map.insert("key", "1").await.unwrap();
    assert!(!map.remove("key", "2").await.unwrap());
    assert!(map.remove("key", "1").await.unwrap());
    assert_eq!(map.key_len().await.unwrap(), 0);
    assert_eq!(raw_command(&["HEXISTS", &name, "\"key\""]).await, ":0\r\n");
    assert!(!map.remove("key", "1").await.unwrap());
}

#[tokio::test]
async fn remove_all_returns_the_values() {
    let map = set(&unique("smm")).await;
    map.extend("0", ["1", "1", "2"]).await.unwrap();
    let mut removed = map.remove_all("0").await.unwrap();
    removed.sort();
    assert_eq!(removed, ["1", "2"]);
    assert!(map.remove_all("0").await.unwrap().is_empty());
    assert_eq!(map.len().await.unwrap(), 0);
    assert_eq!(map.key_len().await.unwrap(), 0);
}

#[tokio::test]
async fn replace_values_returns_the_old_values() {
    let map = set(&unique("smm")).await;
    map.insert("0", "1").await.unwrap();
    assert_eq!(map.replace_values("0", ["2", "3"]).await.unwrap(), ["1"]);
    assert_eq!(sorted(&map, "0").await, ["2", "3"]);
    map.set_values("0", ["4", "5"]).await.unwrap();
    assert_eq!(sorted(&map, "0").await, ["4", "5"]);
    map.set_values("1", ["6"]).await.unwrap();
    assert_eq!(sorted(&map, "1").await, ["6"]);
    assert_eq!(map.key_len().await.unwrap(), 2);
}

// Review Focus 2
#[tokio::test]
async fn empty_inputs_send_nothing_and_leave_no_key() {
    let name = unique("smm");
    let map = set(&name).await;
    let none: [&str; 0] = [];
    assert!(!map.extend("k", none).await.unwrap());
    assert_eq!(raw_command(&["HEXISTS", &name, "\"k\""]).await, ":0\r\n");

    map.extend("0", ["1", "2"]).await.unwrap();
    map.insert("1", "3").await.unwrap();
    assert_eq!(map.key_len().await.unwrap(), 2);
    let mut old = map.replace_values("0", none).await.unwrap();
    old.sort();
    assert_eq!(old, ["1", "2"]);
    assert_eq!(map.key_len().await.unwrap(), 1);
    assert!(!map.contains_key("0").await.unwrap());
    map.set_values("1", none).await.unwrap();
    assert_eq!(map.key_len().await.unwrap(), 0);
    assert!(map.get_all("1").await.unwrap().is_empty());

    let no_keys: [&str; 0] = [];
    assert_eq!(map.remove_many(&no_keys).await.unwrap(), 0);
    assert_eq!(map.remove_from_all_keys(&no_keys).await.unwrap(), 0);
}

/// The hash field value Redisson stores for `encoded_key`, or `None`.
async fn redis_hash(name: &str, encoded_key: &str) -> Option<String> {
    let reply = raw_command(&["HGET", name, encoded_key]).await;
    reply.lines().nth(1).map(str::to_string)
}

#[tokio::test]
async fn remove_many_counts_present_keys() {
    let name = unique("smm");
    let map = set(&name).await;
    map.extend("0", ["1", "2"]).await.unwrap();
    let hash = redis_hash(&name, "\"0\"").await.unwrap();
    assert_eq!(map.remove_many(&["0", "1"]).await.unwrap(), 1);
    assert!(redis_hash(&name, "\"0\"").await.is_none());
    assert_eq!(
        raw_command(&["EXISTS", &values_key(&name, &hash)]).await,
        ":0\r\n"
    );
    assert_eq!(map.key_len().await.unwrap(), 0);
}

#[tokio::test]
async fn remove_from_all_keys_counts_removals() {
    let name = unique("smm");
    let map = set(&name).await;
    map.extend("a", ["x", "y"]).await.unwrap();
    map.extend("b", ["x"]).await.unwrap();
    assert_eq!(map.remove_from_all_keys(&["x"]).await.unwrap(), 2);
    assert_eq!(sorted(&map, "a").await, ["y"]);
    assert_eq!(raw_command(&["HEXISTS", &name, "\"b\""]).await, ":0\r\n");
    assert_eq!(map.key_len().await.unwrap(), 1);
}

// Redisson: testContainsValue, testContainsEntry
#[tokio::test]
async fn contains_value_and_entry() {
    let map = set(&format!("{{1}}{}", unique("test1"))).await;
    map.insert("0", "1").await.unwrap();
    assert!(map.contains_value("1").await.unwrap());
    assert!(!map.contains_value("0").await.unwrap());
    assert!(map.contains_entry("0", "1").await.unwrap());
    assert!(!map.contains_entry("0", "2").await.unwrap());
    assert!(!map.contains_entry("1", "1").await.unwrap());
}

#[tokio::test]
async fn keys_to_vec_lists_every_key() {
    let map = set(&unique("smm")).await;
    map.insert("a", "1").await.unwrap();
    map.insert("a", "2").await.unwrap();
    map.insert("b", "3").await.unwrap();
    let mut keys = map.keys_to_vec().await.unwrap();
    keys.sort();
    assert_eq!(keys, ["a", "b"]);
}

#[tokio::test]
async fn clear_deletes_hash_and_values() {
    let name = unique("smm");
    let map = set(&name).await;
    map.insert("key", "1").await.unwrap();
    map.clear().await.unwrap();
    assert_eq!(raw_command(&["EXISTS", &name]).await, ":0\r\n");
    assert_eq!(
        raw_command(&["EXISTS", &values_key(&name, "IKm9EBQhlzpVIBxUXDHBWw")]).await,
        ":0\r\n"
    );
    assert_eq!(map.len().await.unwrap(), 0);
}

// Redisson: RMultimap.getLock(key), named by RedissonObject.getLockByMapKey
#[tokio::test]
async fn per_key_lock_lives_under_the_redisson_name() {
    let name = unique("smm");
    let map = set(&name).await;
    let lock_key = format!("{{{name}}}:IKm9EBQhlzpVIBxUXDHBWw:lock");
    let guard = map.lock("key").unwrap().lock().await.unwrap();
    assert_eq!(raw_command(&["EXISTS", &lock_key]).await, ":1\r\n");
    guard.unlock().await.unwrap();
    assert_eq!(raw_command(&["EXISTS", &lock_key]).await, ":0\r\n");
}

async fn exists(key: &str) -> bool {
    raw_command(&["EXISTS", key]).await == ":1\r\n"
}

/// The values keys of `ks` in the multimap `name`, read from its hash.
async fn values_keys(name: &str, ks: &[&str]) -> Vec<String> {
    let mut keys = Vec::new();
    for k in ks {
        let hash = redis_hash(name, &format!("\"{k}\"")).await.unwrap();
        keys.push(values_key(name, &hash));
    }
    keys
}

// Redisson: testDelete
#[tokio::test]
async fn del_removes_hash_and_values() {
    let name = unique("smm");
    let map = set(&name).await;
    map.insert("a", "1").await.unwrap();
    map.insert("b", "2").await.unwrap();
    let values = values_keys(&name, &["a", "b"]).await;
    assert!(map.del().await.unwrap());
    assert!(!exists(&name).await);
    for key in &values {
        assert!(!exists(key).await, "{key}");
    }
    assert!(!map.del().await.unwrap());
}

// Redisson: testExpire
#[tokio::test]
async fn expire_covers_the_values() {
    let name = unique("smm");
    let map = set(&name).await;
    map.insert("a", "1").await.unwrap();
    map.insert("b", "2").await.unwrap();
    let values = values_keys(&name, &["a", "b"]).await;
    assert!(map.expire(Duration::from_millis(100)).await.unwrap());
    sleep(Duration::from_millis(500)).await;
    assert_eq!(map.len().await.unwrap(), 0);
    for key in &values {
        assert!(!exists(key).await, "{key}");
    }
}

// Redisson: testClearExpire
#[tokio::test]
async fn persist_clears_every_ttl() {
    let name = unique("smm");
    let map = set(&name).await;
    map.insert("a", "1").await.unwrap();
    map.insert("b", "2").await.unwrap();
    assert!(map.expire(Duration::from_millis(100)).await.unwrap());
    assert!(map.persist().await.unwrap());
    sleep(Duration::from_millis(500)).await;
    assert_eq!(map.len().await.unwrap(), 2);
    assert!(!map.persist().await.unwrap());
}

// Redisson: testRename
#[tokio::test]
async fn rename_moves_every_key() {
    let name = unique("smm");
    let renamed = unique("smm");
    let map = set(&name).await;
    map.insert("a", "1").await.unwrap();
    map.insert("b", "2").await.unwrap();
    let old_values = values_keys(&name, &["a", "b"]).await;
    map.rename(&renamed).await.unwrap();
    let moved = set(&renamed).await;
    assert_eq!(moved.get_all("a").await.unwrap(), ["1"]);
    assert_eq!(moved.get_all("b").await.unwrap(), ["2"]);
    assert_eq!(moved.len().await.unwrap(), 2);
    assert!(!exists(&name).await);
    for key in &old_values {
        assert!(!exists(key).await, "{key}");
    }
    for key in values_keys(&renamed, &["a", "b"]).await {
        assert!(exists(&key).await, "{key}");
    }
}

#[tokio::test]
async fn ttl_and_exists_read_the_hash() {
    let map = set(&unique("smm")).await;
    assert!(!map.exists().await.unwrap());
    assert_eq!(map.ttl().await.unwrap(), None);
    map.insert("a", "1").await.unwrap();
    assert!(map.exists().await.unwrap());
    assert_eq!(map.ttl().await.unwrap(), None);
    map.expire(Duration::from_secs(10)).await.unwrap();
    let ttl = map.ttl().await.unwrap().unwrap();
    assert!(ttl <= Duration::from_secs(10) && ttl > Duration::from_secs(8));
}

// Redisson: testSetMultimapEntriesCount
#[tokio::test]
async fn streams_cover_every_entry() {
    let map = set(&unique("smm")).await;
    for i in 0..137 {
        let k = format!("k{i}");
        map.extend(
            &k,
            [format!("v{i}-a"), format!("v{i}-b"), format!("v{i}-c")]
                .iter()
                .map(String::as_str),
        )
        .await
        .unwrap();
    }
    let seen: std::collections::HashSet<(String, String)> =
        map.iter_paged(50).try_collect().await.unwrap();
    assert_eq!(seen.len(), 411);
    let keys: Vec<String> = map.keys_paged(50).try_collect().await.unwrap();
    assert_eq!(keys.len(), 137);
    let values: Vec<String> = map.values_paged(50).try_collect().await.unwrap();
    assert_eq!(values.len(), 411);
    let with_count: std::collections::HashSet<(String, String)> =
        map.iter_paged(500).try_collect().await.unwrap();
    assert_eq!(seen, with_count);
    let default: std::collections::HashSet<(String, String)> =
        map.iter().try_collect().await.unwrap();
    assert_eq!(seen, default);
    let zero: Vec<String> = map.values_paged(0).try_collect().await.unwrap();
    assert_eq!(zero.len(), 411);
}

// Redisson: testKeySet
#[tokio::test]
async fn keys_stream() {
    let map = set(&unique("smm")).await;
    map.insert("0", "1").await.unwrap();
    map.insert("3", "4").await.unwrap();
    let mut keys: Vec<String> = map.keys().try_collect().await.unwrap();
    keys.sort();
    assert_eq!(keys, ["0", "3"]);
    let empty = set(&unique("smm")).await;
    let none: Vec<String> = empty.keys().try_collect().await.unwrap();
    assert!(none.is_empty());
}

// Redisson: testValues
#[tokio::test]
async fn values_stream_keeps_duplicates() {
    let map = set(&unique("smm")).await;
    map.insert("0", "1").await.unwrap();
    map.insert("0", "3").await.unwrap();
    map.insert("2", "5").await.unwrap();
    map.insert("3", "4").await.unwrap();
    map.insert("4", "1").await.unwrap();
    let mut values: Vec<String> = map.values().try_collect().await.unwrap();
    values.sort();
    assert_eq!(values, ["1", "1", "3", "4", "5"]);
}

// Redisson: testEntrySet
#[tokio::test]
async fn entries_stream() {
    let map = set(&unique("smm")).await;
    map.insert("0", "1").await.unwrap();
    map.insert("3", "4").await.unwrap();
    let mut entries: Vec<(String, String)> = map.iter().try_collect().await.unwrap();
    entries.sort();
    assert_eq!(
        entries,
        [
            ("0".to_string(), "1".to_string()),
            ("3".to_string(), "4".to_string())
        ]
    );
}

#[tokio::test]
async fn streams_read_big_sets_with_sscan() {
    let map = set(&unique("smm")).await;
    let values: Vec<String> = (0..300).map(|i| i.to_string()).collect();
    map.extend("k", values.iter().map(String::as_str))
        .await
        .unwrap();
    let read: std::collections::HashSet<String> = map.values_paged(7).try_collect().await.unwrap();
    assert_eq!(read.len(), 300);
}

async fn sorted_values(
    values: &redissun::multimap::SetMultimapValues<String, JsonCodec>,
) -> Vec<String> {
    let mut all = values.to_vec().await.unwrap();
    all.sort();
    all
}

#[tokio::test]
async fn handle_writes_go_through_the_multimap() {
    let name = unique("smm");
    let map = set(&name).await;
    let values = map.get("0").unwrap();
    assert!(values.insert("1").await.unwrap());
    assert!(!values.insert("1").await.unwrap());
    assert_eq!(values.extend(["2", "3", "1"]).await.unwrap(), 2);
    assert_eq!(map.key_len().await.unwrap(), 1);
    assert!(map.contains_key("0").await.unwrap());
    assert_eq!(raw_command(&["HEXISTS", &name, "\"0\""]).await, ":1\r\n");
    assert_eq!(sorted(&map, "0").await, ["1", "2", "3"]);
}

#[tokio::test]
async fn handle_remove_drops_the_key() {
    let name = unique("smm");
    let map = set(&name).await;
    map.extend("0", ["1", "2", "3"]).await.unwrap();
    let values = map.get("0").unwrap();
    assert!(values.remove("1").await.unwrap());
    assert!(!values.remove("1").await.unwrap());
    assert!(values.remove_values(&["2"]).await.unwrap());
    assert_eq!(values.len().await.unwrap(), 1);
    assert!(values.remove_values(&["3"]).await.unwrap());
    assert_eq!(values.len().await.unwrap(), 0);
    assert!(!values.remove_values(&["3"]).await.unwrap());
    assert_eq!(raw_command(&["HEXISTS", &name, "\"0\""]).await, ":0\r\n");

    map.insert("1", "x").await.unwrap();
    assert!(map.get("1").unwrap().remove("x").await.unwrap());
    assert_eq!(raw_command(&["HEXISTS", &name, "\"1\""]).await, ":0\r\n");
}

#[tokio::test]
async fn handle_clear_removes_the_key() {
    let map = set(&unique("smm")).await;
    map.extend("1", ["01", "02", "03"]).await.unwrap();
    map.get("1").unwrap().clear().await.unwrap();
    assert_eq!(map.len().await.unwrap(), 0);
    assert_eq!(map.get("1").unwrap().len().await.unwrap(), 0);
    assert_eq!(map.key_len().await.unwrap(), 0);
}

#[tokio::test]
async fn handle_reads() {
    let map = set(&unique("smm")).await;
    map.extend("1", ["1", "2", "3"]).await.unwrap();
    let values = map.get("1").unwrap();
    assert!(values.contains("2").await.unwrap());
    assert!(!values.contains("9").await.unwrap());
    assert!(values.contains_all(&["1", "3"]).await.unwrap());
    assert!(!values.contains_all(&["1", "9"]).await.unwrap());
    assert_eq!(values.len().await.unwrap(), 3);
    assert!(!values.is_empty().await.unwrap());
    assert_eq!(sorted_values(&values).await, ["1", "2", "3"]);
    let mut streamed: Vec<String> = values.iter().try_collect().await.unwrap();
    streamed.sort();
    assert_eq!(streamed, ["1", "2", "3"]);
    assert!(values.retain(&["1", "2"]).await.unwrap());
    assert!(!values.retain(&["1", "2"]).await.unwrap());
    assert_eq!(sorted(&map, "1").await, ["1", "2"]);
}

#[tokio::test]
async fn handle_empty_inputs() {
    let name = unique("smm");
    let map = set(&name).await;
    let values = map.get("k").unwrap();
    let none: [&str; 0] = [];
    assert_eq!(values.extend(none).await.unwrap(), 0);
    assert_eq!(raw_command(&["HEXISTS", &name, "\"k\""]).await, ":0\r\n");
    assert_eq!(map.key_len().await.unwrap(), 0);
    let none: &[&str] = &[];
    assert!(!values.remove_values(none).await.unwrap());
    assert!(values.contains_all(none).await.unwrap());
}

#[tokio::test]
async fn index_writes_can_leave_an_empty_key_like_redisson() {
    let map = set(&unique("smm")).await;
    map.insert("k", "a").await.unwrap();
    assert_eq!(
        map.get("k")
            .unwrap()
            .as_set()
            .pop()
            .await
            .unwrap()
            .as_deref(),
        Some("a")
    );
    assert!(!map.contains_key("k").await.unwrap());
    assert!(map.get_all("k").await.unwrap().is_empty());
    let entries: Vec<(String, String)> = map.iter().try_collect().await.unwrap();
    assert!(entries.is_empty());
    assert_eq!(map.key_len().await.unwrap(), 1);
    map.remove_all("k").await.unwrap();
    assert_eq!(map.key_len().await.unwrap(), 0);
}

#[tokio::test]
async fn as_set_reaches_the_raw_set() {
    let name = unique("smm");
    let map = set(&name).await;
    map.extend("key", ["a", "b"]).await.unwrap();
    map.extend("other", ["b", "c"]).await.unwrap();
    let values = map.get("key").unwrap();
    let other = map.get("other").unwrap();
    assert_eq!(values.name(), values_key(&name, "IKm9EBQhlzpVIBxUXDHBWw"));
    let mut union = values.as_set().union(&[other.as_set()]).await.unwrap();
    union.sort();
    assert_eq!(union, ["a", "b", "c"]);
}
