//! Loaders and writers of the maps: Redisson's `MapOptions` store features.

mod common;

use common::store::MemoryStore;
use common::{client, raw_command, unique};
use redissun::hash_map::Entry;
use redissun::{Error, HashMap, JsonCodec, Object, WriteMode};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
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

async fn writing(store: &MemoryStore) -> Map {
    client()
        .await
        .hash_map_builder::<String, String>(unique("ms"))
        .writer(store.clone())
        .build()
        .await
        .unwrap()
}

fn expected(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
    entries
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

// Redisson testWriterPut.
#[tokio::test]
async fn hash_map_writer_put() {
    let store = MemoryStore::default();
    let map = writing(&store).await;
    map.insert("1", "11").await.unwrap();
    map.insert("2", "22").await.unwrap();
    map.insert("3", "33").await.unwrap();
    assert_eq!(
        store.snapshot(),
        expected(&[("1", "11"), ("2", "22"), ("3", "33")])
    );
}

// Redisson testWriterFastPut.
#[tokio::test]
async fn hash_map_writer_set() {
    let store = MemoryStore::default();
    let map = writing(&store).await;
    map.set("1", "11").await.unwrap();
    map.set("2", "22").await.unwrap();
    map.set("3", "33").await.unwrap();
    assert_eq!(
        store.snapshot(),
        expected(&[("1", "11"), ("2", "22"), ("3", "33")])
    );
}

// Redisson testWriterRemove.
#[tokio::test]
async fn hash_map_writer_remove() {
    let store = MemoryStore::default();
    let map = writing(&store).await;
    map.insert("1", "11").await.unwrap();
    map.remove("1").await.unwrap();
    map.insert("3", "33").await.unwrap();
    assert_eq!(store.snapshot(), expected(&[("3", "33")]));
}

// Redisson testWriterReplaceKeyValue.
#[tokio::test]
async fn hash_map_writer_replace() {
    let store = MemoryStore::default();
    let map = writing(&store).await;
    map.insert("1", "11").await.unwrap();
    map.replace("1", "00").await.unwrap();
    map.replace("2", "22").await.unwrap();
    map.insert("3", "33").await.unwrap();
    assert_eq!(store.snapshot(), expected(&[("1", "00"), ("3", "33")]));
    assert_eq!(store.write_calls(), vec![vec!["1"], vec!["1"], vec!["3"]]);
}

// Redisson testWriterReplaceKeyOldNewValue.
#[tokio::test]
async fn hash_map_writer_compare_and_set() {
    let store = MemoryStore::default();
    let map = writing(&store).await;
    map.insert("1", "11").await.unwrap();
    assert!(map.compare_and_set("1", "11", "00").await.unwrap());
    assert!(!map.compare_and_set("1", "11", "22").await.unwrap());
    map.insert("3", "33").await.unwrap();
    assert_eq!(store.snapshot(), expected(&[("1", "00"), ("3", "33")]));
    assert_eq!(store.write_calls().len(), 3);
}

// Redisson testWriterRemoveKeyValue.
#[tokio::test]
async fn hash_map_writer_remove_if_eq() {
    let store = MemoryStore::default();
    let map = writing(&store).await;
    map.insert("1", "11").await.unwrap();
    map.insert("2", "22").await.unwrap();
    map.insert("3", "33").await.unwrap();
    assert_eq!(
        store.snapshot(),
        expected(&[("1", "11"), ("2", "22"), ("3", "33")])
    );
    assert!(!map.remove_if_eq("2", "00").await.unwrap());
    assert!(map.remove_if_eq("1", "11").await.unwrap());
    assert_eq!(store.snapshot(), expected(&[("2", "22"), ("3", "33")]));
    assert_eq!(store.delete_calls(), vec![vec!["1"]]);
}

// Redisson testWriterPutIfAbsent and testWriterFastPutIfAbsent.
#[tokio::test]
async fn hash_map_writer_try_insert() {
    let store = MemoryStore::default();
    let map = writing(&store).await;
    assert_eq!(map.try_insert("1", "11").await.unwrap(), None);
    assert_eq!(
        map.try_insert("1", "00").await.unwrap(),
        Some("11".to_string())
    );
    map.try_insert("2", "22").await.unwrap();
    assert_eq!(store.snapshot(), expected(&[("1", "11"), ("2", "22")]));
    assert_eq!(store.write_calls(), vec![vec!["1"], vec!["2"]]);
}

// Redisson testWriterPutAll.
#[tokio::test]
async fn hash_map_writer_extend() {
    let store = MemoryStore::default();
    let map = writing(&store).await;
    map.extend([("1", "11"), ("2", "22"), ("3", "33")].map(|(k, v)| (k, v)))
        .await
        .unwrap();
    assert_eq!(
        store.snapshot(),
        expected(&[("1", "11"), ("2", "22"), ("3", "33")])
    );
}

// Redisson testWriterFastRemove.
#[tokio::test]
async fn hash_map_writer_remove_many() {
    let store = MemoryStore::default();
    let map = writing(&store).await;
    map.insert("1", "11").await.unwrap();
    map.insert("2", "22").await.unwrap();
    map.insert("3", "33").await.unwrap();
    assert_eq!(map.remove_many(&["1", "2", "4"]).await.unwrap(), 2);
    assert_eq!(store.delete_calls(), vec![vec!["1", "2"]]);
    assert_eq!(store.snapshot(), expected(&[("3", "33")]));
}

// Redisson testWriterAddAndGet and RedissonMapTest.testAddAndGetMapWriter.
#[tokio::test]
async fn hash_map_writer_incr_by() {
    let store = MemoryStore::default();
    let map: HashMap<String, i64, JsonCodec> = client()
        .await
        .hash_map_builder(unique("ms"))
        .writer(store.clone())
        .build()
        .await
        .unwrap();
    assert_eq!(map.incr_by("1", 11).await.unwrap(), 11);
    assert_eq!(map.incr_by("1", 7).await.unwrap(), 18);
    assert_eq!(store.snapshot(), expected(&[("1", "18")]));
    let floats: HashMap<String, f64, JsonCodec> = client()
        .await
        .hash_map_builder(unique("ms"))
        .writer(store.clone())
        .build()
        .await
        .unwrap();
    assert_eq!(floats.incr_by_float("2", 1.5).await.unwrap(), 1.5);
    assert_eq!(store.snapshot()["2"], "1.5");
}

// Redisson testRetryableWriterSuccessAtLastRetry.
#[tokio::test]
async fn hash_map_writer_retries_until_it_succeeds() {
    let store = MemoryStore::default();
    let map: Map = client()
        .await
        .hash_map_builder(unique("ms"))
        .writer(store.clone())
        .writer_retry_attempts(3)
        .writer_retry_interval(Duration::from_millis(10))
        .build()
        .await
        .unwrap();
    store.fail_next(2);
    map.insert("1", "11").await.unwrap();
    assert_eq!(store.snapshot(), expected(&[("1", "11")]));
    assert_eq!(store.write_calls().len(), 3);
    store.fail_next(2);
    map.remove("1").await.unwrap();
    assert!(store.snapshot().is_empty());
    assert_eq!(store.delete_calls().len(), 3);
}

#[tokio::test]
async fn hash_map_a_failing_writer_is_an_error_after_redis_changed() {
    let store = MemoryStore::default();
    store.fail_always();
    let name = unique("ms");
    let client = client().await;
    let map: Map = client
        .hash_map_builder(name.clone())
        .writer(store.clone())
        .writer_retry_attempts(2)
        .writer_retry_interval(Duration::from_millis(200))
        .build()
        .await
        .unwrap();
    let started = Instant::now();
    assert!(matches!(map.insert("a", "1").await, Err(Error::Writer(_))));
    assert!(started.elapsed() >= Duration::from_millis(200));
    assert_eq!(store.write_calls().len(), 2);
    let plain: Map = client.hash_map(name);
    assert_eq!(plain.get("a").await.unwrap(), Some("1".to_string()));
}

#[tokio::test]
async fn hash_map_remove_calls_delete_even_for_a_missing_key() {
    let store = MemoryStore::default();
    let map = writing(&store).await;
    assert_eq!(map.remove("x").await.unwrap(), None);
    assert_eq!(store.delete_calls(), vec![vec!["x"]]);
}

#[tokio::test]
async fn hash_map_remove_many_without_a_removed_key_skips_the_writer() {
    let store = MemoryStore::default();
    let map = writing(&store).await;
    assert_eq!(map.remove_many(&["x", "y"]).await.unwrap(), 0);
    assert!(store.delete_calls().is_empty());
}

#[tokio::test]
async fn hash_map_entry_writes_go_to_the_writer() {
    let store = MemoryStore::default();
    let map = writing(&store).await;
    map.entry("a")
        .await
        .unwrap()
        .or_insert("1".to_string())
        .await
        .unwrap();
    assert_eq!(store.write_calls(), vec![vec!["a"]]);
    assert_eq!(store.snapshot(), expected(&[("a", "1")]));
    match map.entry("a").await.unwrap() {
        Entry::Occupied(entry) => assert_eq!(entry.remove().await.unwrap(), "1"),
        Entry::Vacant(_) => panic!("the entry is missing"),
    }
    assert_eq!(store.delete_calls(), vec![vec!["a"]]);
    assert!(store.snapshot().is_empty());
}

#[tokio::test]
async fn hash_map_extend_calls_the_writer_once_per_batch() {
    let store = MemoryStore::default();
    let map = writing(&store).await;
    let entries = [
        ("1", "11"),
        ("2", "22"),
        ("3", "33"),
        ("4", "44"),
        ("5", "55"),
    ];
    map.extend(entries.map(|(k, v)| (k, v)))
        .batch_size(2)
        .await
        .unwrap();
    let sizes: Vec<usize> = store.write_calls().iter().map(Vec::len).collect();
    assert_eq!(sizes, vec![2, 2, 1]);
    assert_eq!(store.snapshot().len(), 5);
}

#[tokio::test]
async fn hash_map_clear_and_del_skip_the_writer() {
    let store = MemoryStore::default();
    let map = writing(&store).await;
    map.insert("a", "1").await.unwrap();
    map.clear().await.unwrap();
    map.insert("b", "2").await.unwrap();
    assert!(map.del().await.unwrap());
    assert_eq!(store.write_calls().len(), 2);
    assert!(store.delete_calls().is_empty());
    assert_eq!(store.snapshot(), expected(&[("a", "1"), ("b", "2")]));
}
