mod common;

use common::{client, raw_command, unique};
use redissun::{JsonCodec, SetMultimap};

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
