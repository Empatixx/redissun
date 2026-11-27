mod common;

use common::{client, raw_command, unique};
use redissun::{JsonCodec, ListMultimap, Object};
use std::time::Duration;
use tokio::time::sleep;

async fn list(name: &str) -> ListMultimap<String, String, JsonCodec> {
    client().await.list_multimap(name.to_string())
}

#[tokio::test]
async fn layout_matches_redisson() {
    let name = unique("lmm");
    let map = list(&name).await;
    assert!(map.insert("key", "v").await.unwrap());
    assert_eq!(
        raw_command(&["HGET", &name, "\"key\""]).await,
        "$22\r\nIKm9EBQhlzpVIBxUXDHBWw\r\n"
    );
    let values = format!("{{{name}}}:IKm9EBQhlzpVIBxUXDHBWw");
    assert_eq!(
        raw_command(&["LRANGE", &values, "0", "-1"]).await,
        "*1\r\n$3\r\n\"v\"\r\n"
    );
}

#[tokio::test]
async fn insert_appends_or_adds() {
    let map = list(&unique("lmm")).await;
    for v in ["1", "2", "3", "3"] {
        assert!(map.insert("k", v).await.unwrap());
    }
    assert_eq!(map.get_all("k").await.unwrap(), ["1", "2", "3", "3"]);
}

#[tokio::test]
async fn len_counts_values_and_key_len_counts_keys() {
    let map = list(&unique("lmm")).await;
    map.insert("a", "1").await.unwrap();
    map.insert("a", "2").await.unwrap();
    map.insert("b", "3").await.unwrap();
    assert_eq!(map.len().await.unwrap(), 3);
    assert_eq!(map.key_len().await.unwrap(), 2);
    assert!(!map.is_empty().await.unwrap());

    let empty = list(&unique("lmm")).await;
    assert_eq!(empty.len().await.unwrap(), 0);
    assert_eq!(empty.key_len().await.unwrap(), 0);
    assert!(empty.is_empty().await.unwrap());
}

#[tokio::test]
async fn contains_key_reads_the_values() {
    let map = list(&unique("lmm")).await;
    map.insert("a", "1").await.unwrap();
    assert!(map.contains_key("a").await.unwrap());
    assert!(!map.contains_key("b").await.unwrap());
}

#[tokio::test]
async fn reads_data_written_like_redisson() {
    let name = unique("lmm");
    let values = format!("{{{name}}}:IKm9EBQhlzpVIBxUXDHBWw");
    raw_command(&["HSET", &name, "\"key\"", "IKm9EBQhlzpVIBxUXDHBWw"]).await;
    raw_command(&["RPUSH", &values, "\"x\"", "\"y\"", "\"x\""]).await;
    let map = list(&name).await;
    assert_eq!(map.get_all("key").await.unwrap(), ["x", "y", "x"]);
    assert_eq!(map.len().await.unwrap(), 3);
    assert_eq!(map.key_len().await.unwrap(), 1);
}

fn values_key(name: &str, hash: &str) -> String {
    format!("{{{name}}}:{hash}")
}

#[tokio::test]
async fn extend_appends_in_order() {
    let map = list(&unique("lmm")).await;
    assert!(map.extend("0", ["1", "2", "3", "3"]).await.unwrap());
    assert!(map.extend("0", ["1"]).await.unwrap());
    assert_eq!(map.get_all("0").await.unwrap(), ["1", "2", "3", "3", "1"]);
}

#[tokio::test]
async fn remove_drops_the_key_with_its_last_value() {
    let name = unique("lmm");
    let map = list(&name).await;
    map.insert("key", "1").await.unwrap();
    assert!(!map.remove("key", "2").await.unwrap());
    assert!(map.remove("key", "1").await.unwrap());
    assert_eq!(map.key_len().await.unwrap(), 0);
    assert_eq!(raw_command(&["HEXISTS", &name, "\"key\""]).await, ":0\r\n");
    assert!(!map.remove("key", "1").await.unwrap());
}

#[tokio::test]
async fn remove_all_returns_the_values() {
    let map = list(&unique("lmm")).await;
    map.extend("0", ["1", "1", "2"]).await.unwrap();
    assert_eq!(map.remove_all("0").await.unwrap(), ["1", "1", "2"]);
    assert!(map.remove_all("0").await.unwrap().is_empty());
    assert_eq!(map.len().await.unwrap(), 0);
    assert_eq!(map.key_len().await.unwrap(), 0);
}

#[tokio::test]
async fn replace_values_returns_the_old_values() {
    let map = list(&unique("lmm")).await;
    map.insert("0", "1").await.unwrap();
    assert_eq!(
        map.replace_values("0", ["2", "3", "2"]).await.unwrap(),
        ["1"]
    );
    assert_eq!(map.get_all("0").await.unwrap(), ["2", "3", "2"]);
    map.set_values("0", ["4", "5"]).await.unwrap();
    assert_eq!(map.get_all("0").await.unwrap(), ["4", "5"]);
    map.set_values("1", ["6"]).await.unwrap();
    assert_eq!(map.get_all("1").await.unwrap(), ["6"]);
    assert_eq!(map.key_len().await.unwrap(), 2);
}

// Review Focus 2
#[tokio::test]
async fn empty_inputs_send_nothing_and_leave_no_key() {
    let name = unique("lmm");
    let map = list(&name).await;
    let none: [&str; 0] = [];
    assert!(!map.extend("k", none).await.unwrap());
    assert_eq!(raw_command(&["HEXISTS", &name, "\"k\""]).await, ":0\r\n");

    map.extend("0", ["1", "2"]).await.unwrap();
    map.insert("1", "3").await.unwrap();
    assert_eq!(map.key_len().await.unwrap(), 2);
    assert_eq!(map.replace_values("0", none).await.unwrap(), ["1", "2"]);
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
    let name = unique("lmm");
    let map = list(&name).await;
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
    let map = list(&unique("lmm")).await;
    map.extend("a", ["x", "x", "y"]).await.unwrap();
    map.extend("b", ["x"]).await.unwrap();
    assert_eq!(map.remove_from_all_keys(&["x"]).await.unwrap(), 2);
    assert_eq!(map.get_all("a").await.unwrap(), ["x", "y"]);
    assert!(map.get_all("b").await.unwrap().is_empty());
}

// Redisson: testContainsValue, testContainsEntry
#[tokio::test]
async fn contains_value_and_entry() {
    let map = list(&format!("{{1}}{}", unique("test1"))).await;
    map.insert("0", "1").await.unwrap();
    assert!(map.contains_value("1").await.unwrap());
    assert!(!map.contains_value("0").await.unwrap());
    assert!(map.contains_entry("0", "1").await.unwrap());
    assert!(!map.contains_entry("0", "2").await.unwrap());
    assert!(!map.contains_entry("1", "1").await.unwrap());
}

#[tokio::test]
async fn keys_to_vec_lists_every_key() {
    let map = list(&unique("lmm")).await;
    map.insert("a", "1").await.unwrap();
    map.insert("a", "2").await.unwrap();
    map.insert("b", "3").await.unwrap();
    let mut keys = map.keys_to_vec().await.unwrap();
    keys.sort();
    assert_eq!(keys, ["a", "b"]);
}

#[tokio::test]
async fn clear_deletes_hash_and_values() {
    let name = unique("lmm");
    let map = list(&name).await;
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
    let name = unique("lmm");
    let map = list(&name).await;
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
    let name = unique("lmm");
    let map = list(&name).await;
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
    let name = unique("lmm");
    let map = list(&name).await;
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
    let name = unique("lmm");
    let map = list(&name).await;
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
    let name = unique("lmm");
    let renamed = unique("lmm");
    let map = list(&name).await;
    map.insert("a", "1").await.unwrap();
    map.insert("b", "2").await.unwrap();
    let old_values = values_keys(&name, &["a", "b"]).await;
    map.rename(&renamed).await.unwrap();
    let moved = list(&renamed).await;
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
    let map = list(&unique("lmm")).await;
    assert!(!map.exists().await.unwrap());
    assert_eq!(map.ttl().await.unwrap(), None);
    map.insert("a", "1").await.unwrap();
    assert!(map.exists().await.unwrap());
    assert_eq!(map.ttl().await.unwrap(), None);
    map.expire(Duration::from_secs(10)).await.unwrap();
    let ttl = map.ttl().await.unwrap().unwrap();
    assert!(ttl <= Duration::from_secs(10) && ttl > Duration::from_secs(8));
}
