//! Loaders and writers of the maps: Redisson's `MapOptions` store features.

mod common;

use common::store::MemoryStore;
use common::{client, raw_command, unique};
use redissun::hash_map::Entry;
use redissun::{Error, HashMap, JsonCodec, WriteMode};
use std::time::Duration;
use tokio::time::timeout;

type Map = HashMap<String, String, JsonCodec>;

fn numbered(n: usize) -> MemoryStore {
    let entries: Vec<(String, String)> =
        (0..n).map(|i| (i.to_string(), format!("{i}{i}"))).collect();
    let refs: Vec<(&str, &str)> = entries
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    MemoryStore::with(&refs)
}

async fn loading(store: &MemoryStore) -> Map {
    loading_named(store, &unique("ms")).await
}

async fn loading_named(store: &MemoryStore, name: &str) -> Map {
    client()
        .await
        .hash_map_builder::<String, String>(name)
        .loader(store.clone())
        .build()
        .await
        .unwrap()
}

// Redisson testMapLoaderGet and testLoaderGetAsync.
#[tokio::test]
async fn hash_map_get_loads_a_missing_key() {
    let store = MemoryStore::with(&[("1", "11"), ("2", "22"), ("3", "33")]);
    let map = loading(&store).await;
    assert_eq!(map.len().await.unwrap(), 0);
    assert_eq!(map.get("1").await.unwrap(), Some("11".to_string()));
    assert_eq!(map.len().await.unwrap(), 1);
    assert_eq!(map.get("0").await.unwrap(), None);
    map.insert("0", "00").await.unwrap();
    assert_eq!(map.get("0").await.unwrap(), Some("00".to_string()));
    assert_eq!(map.len().await.unwrap(), 2);
    assert!(map.contains_key("2").await.unwrap());
    assert_eq!(map.len().await.unwrap(), 3);
    assert_eq!(
        map.get_many(&["1", "2", "9", "3"]).await.unwrap(),
        vec![
            Some("11".to_string()),
            Some("22".to_string()),
            None,
            Some("33".to_string())
        ]
    );
    assert_eq!(map.len().await.unwrap(), 4);
}

// Redisson testMapLoaderGetMulipleNulls: a key the loader does not have is asked again and never hangs.
#[tokio::test]
async fn hash_map_a_missing_key_is_asked_again() {
    let store = MemoryStore::with(&[("1", "11")]);
    let map = loading(&store).await;
    let wait = Duration::from_secs(5);
    assert_eq!(timeout(wait, map.get("0")).await.unwrap().unwrap(), None);
    assert_eq!(
        timeout(wait, map.get("1")).await.unwrap().unwrap(),
        Some("11".to_string())
    );
    assert_eq!(timeout(wait, map.get("0")).await.unwrap().unwrap(), None);
    assert_eq!(store.loads(), 3);
}

// Redisson testMapLoaderGetWithException expects null; redissun returns the loader's error.
#[tokio::test]
async fn hash_map_a_failing_loader_is_an_error() {
    let store = MemoryStore::with(&[("1", "11")]);
    let map = loading(&store).await;
    store.fail_next(1);
    assert!(matches!(map.get("1").await, Err(Error::Loader(_))));
    assert_eq!(map.len().await.unwrap(), 0);
    assert_eq!(map.get("1").await.unwrap(), Some("11".to_string()));
}

#[tokio::test]
async fn hash_map_concurrent_misses_load_once() {
    let store = MemoryStore::with(&[("1", "11")]);
    store.slow(Duration::from_millis(100));
    let map = loading(&store).await;
    let gets: Vec<_> = (0..10)
        .map(|_| {
            let map = map.clone();
            tokio::spawn(async move { map.get("1").await })
        })
        .collect();
    for get in gets {
        assert_eq!(get.await.unwrap().unwrap(), Some("11".to_string()));
    }
    assert_eq!(store.loads(), 1);
}

#[tokio::test]
async fn hash_map_loaded_values_are_not_written_back() {
    let store = MemoryStore::with(&[("1", "11")]);
    let map = client()
        .await
        .hash_map_builder::<String, String>(unique("ms"))
        .loader(store.clone())
        .writer(store.clone())
        .build()
        .await
        .unwrap();
    assert_eq!(map.get("1").await.unwrap(), Some("11".to_string()));
    assert!(store.write_calls().is_empty());
}

#[tokio::test]
async fn hash_map_entry_loads_under_its_lock() {
    let store = MemoryStore::with(&[("1", "11")]);
    let name = unique("ms");
    let map = loading_named(&store, &name).await;
    let entry = timeout(Duration::from_secs(5), map.entry("1"))
        .await
        .unwrap()
        .unwrap();
    match &entry {
        Entry::Occupied(occupied) => assert_eq!(occupied.get(), "11"),
        Entry::Vacant(_) => panic!("the entry did not load the value"),
    }
    let value = entry
        .and_modify(|v| v.push('!'))
        .or_insert("x".to_string())
        .await
        .unwrap();
    assert_eq!(value, "11!");
    assert_eq!(store.loads(), 1);
    assert!(raw_command(&["HGET", &name, "\"1\""])
        .await
        .contains("\"11!\""));
}

// Redisson testLoadAllReplaceValues, first half.
#[tokio::test]
async fn hash_map_load_all_keeps_present_values() {
    let store = numbered(10);
    let map = loading(&store).await;
    map.insert("0", "010").await.unwrap();
    map.insert("5", "555").await.unwrap();
    map.load_all().parallelism(2).await.unwrap();
    assert_eq!(map.len().await.unwrap(), 10);
    assert_eq!(map.get("0").await.unwrap(), Some("010".to_string()));
    assert_eq!(map.get("5").await.unwrap(), Some("555".to_string()));
}

// Redisson testLoadAllReplaceValues, second half.
#[tokio::test]
async fn hash_map_load_all_can_replace_values() {
    let store = numbered(10);
    let map = loading(&store).await;
    map.clear().await.unwrap();
    map.insert("0", "010").await.unwrap();
    map.insert("5", "555").await.unwrap();
    map.load_all()
        .replace_existing()
        .parallelism(2)
        .await
        .unwrap();
    assert_eq!(map.get("0").await.unwrap(), Some("00".to_string()));
    assert_eq!(map.get("5").await.unwrap(), Some("55".to_string()));
}

// Redisson testLoadAll and testLoadAllAsync.
#[tokio::test]
async fn hash_map_load_all_loads_every_key() {
    let store = numbered(100);
    let map = loading(&store).await;
    map.load_all().parallelism(2).await.unwrap();
    assert_eq!(map.len().await.unwrap(), 100);
    let loads = store.loads();
    for i in 0..100 {
        assert!(map.contains_key(&i.to_string()).await.unwrap());
    }
    assert_eq!(store.loads(), loads);
}

#[tokio::test]
async fn hash_map_load_all_respects_parallelism() {
    let store = numbered(30);
    store.slow(Duration::from_millis(20));
    let map = loading(&store).await;
    map.load_all().parallelism(3).await.unwrap();
    let max = store.max_in_flight();
    assert!((2..=3).contains(&max), "max in flight {max}");
}

#[tokio::test]
async fn hash_map_load_many_loads_only_those_keys() {
    let store = numbered(10);
    let map = loading(&store).await;
    map.load_many(&["1", "3"]).await.unwrap();
    assert_eq!(map.len().await.unwrap(), 2);
    assert_eq!(store.loads(), 2);
}

#[tokio::test]
async fn hash_map_load_all_without_a_loader_is_a_config_error() {
    let plain: Map = client().await.hash_map(unique("ms"));
    assert!(matches!(plain.load_all().await, Err(Error::Config(_))));
    assert!(matches!(
        plain.load_many(&["1"]).await,
        Err(Error::Config(_))
    ));
    let map = loading(&numbered(3)).await;
    assert!(matches!(
        map.load_all().parallelism(0).await,
        Err(Error::Config(_))
    ));
}

#[tokio::test]
async fn hash_map_builder_checks_its_options() {
    let client = client().await;
    let store = MemoryStore::default();
    let builder = || client.hash_map_builder::<String, String>(unique("ms"));
    assert!(matches!(
        builder()
            .writer(store.clone())
            .writer_retry_attempts(0)
            .build()
            .await,
        Err(Error::Config(_))
    ));
    assert!(matches!(
        builder()
            .writer(store.clone())
            .write_mode(WriteMode::WriteBehind {
                batch_size: 0,
                delay: Duration::from_secs(1)
            })
            .build()
            .await,
        Err(Error::Config(_))
    ));
    assert!(matches!(
        builder()
            .writer(store.clone())
            .write_mode(WriteMode::WriteBehind {
                batch_size: 10,
                delay: Duration::ZERO
            })
            .build()
            .await,
        Err(Error::Config(_))
    ));
    assert!(matches!(
        builder()
            .write_mode(WriteMode::write_behind())
            .build()
            .await,
        Err(Error::Config(_))
    ));
    assert!(builder().build().await.is_ok());
}

#[tokio::test]
async fn hash_map_builder_without_options_reads_like_hash_map() {
    let name = unique("ms");
    let map: Map = client()
        .await
        .hash_map_builder(name.clone())
        .build()
        .await
        .unwrap();
    raw_command(&["HSET", &name, "\"a\"", "\"1\""]).await;
    assert_eq!(map.get("a").await.unwrap(), Some("1".to_string()));
}
