//! Loaders and writers of the maps: Redisson's `MapOptions` store features.
//!
//! Ported from Redisson 4.8.0's `BaseMapTest` (run there for `RMap`, `RMapCache` and `RLocalCachedMap`):
//! `testMapLoaderGet`, `testMapLoaderGetMulipleNulls`, `testLoadAll`, `testLoadAllReplaceValues`,
//! `testWriterPut`, `testWriterFastPut`, `testWriterRemove`, `testWriterReplaceKeyValue`,
//! `testWriterReplaceKeyOldNewValue`, `testWriterRemoveKeyValue`, `testWriterPutIfAbsent`,
//! `testWriterFastPutIfAbsent`, `testWriterPutAll`, `testWriterFastRemove`, `testWriterAddAndGet`,
//! `testRetryableWriterSuccessAtLastRetry`, `testRetryableWriterAsyncSuccessAtLastRetry`,
//! `testWriteBehindFastRemove`; from `RedissonMapTest` `testAddAndGetMapWriter`; from `RedissonMapCacheTest`
//! `testWriterPutTTL`, `testWriterFastPutTTL`, `testWriterPutIfAbsentTTL` and `testWriterFastPutIfAbsentTTL`.
//!
//! Not ported one to one:
//! - `testLoaderGetAsync`, `testLoadAllAsync` and `testWriteBehindAsyncFastRemove` are merged with their sync
//!   twins: redissun has one async `MapLoader`/`MapWriter` and no sync API.
//! - `testMapLoaderGetWithException` is inverted (owner decision 1): Redisson returns null, redissun returns
//!   `Error::Loader` (`hash_map_a_failing_loader_is_an_error`).

mod common;

use common::store::MemoryStore;
use common::{client, raw_command, unique};
use redissun::hash_map::Entry;
use redissun::{
    EntryEvent, EntryEventKind, EntryEvents, Error, HashMap, HashMapCache, JsonCodec, Lease,
    LocalCachedMap, Object, WriteMode,
};
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

fn queue_of(name: &str) -> String {
    if name.contains('{') {
        format!("{name}:redissun_write-behind-queue")
    } else {
        format!("{{{name}}}:redissun_write-behind-queue")
    }
}

async fn queue_len(name: &str) -> String {
    raw_command(&["LLEN", &queue_of(name)]).await
}

async fn behind_on(
    client: &redissun::Client,
    name: &str,
    store: &MemoryStore,
    batch_size: usize,
    delay: Duration,
) -> Map {
    client
        .hash_map_builder::<String, String>(name)
        .writer(store.clone())
        .write_mode(WriteMode::WriteBehind { batch_size, delay })
        .build()
        .await
        .unwrap()
}

/// Waits up to five seconds for `check` to hold.
async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{what} did not happen within 5 s");
}

const LATER: Duration = Duration::from_secs(60);

// Redisson testWriteBehindFastRemove and testWriteBehindAsyncFastRemove.
#[tokio::test]
async fn hash_map_write_behind_remove_many() {
    let store = MemoryStore::default();
    let map: Map = client()
        .await
        .hash_map_builder(unique("ms"))
        .writer(store.clone())
        .write_mode(WriteMode::write_behind())
        .build()
        .await
        .unwrap();
    map.insert("1", "11").await.unwrap();
    map.insert("2", "22").await.unwrap();
    map.insert("3", "33").await.unwrap();
    tokio::time::sleep(Duration::from_millis(1400)).await;
    assert_eq!(map.remove_many(&["1", "2", "4"]).await.unwrap(), 2);
    tokio::time::sleep(Duration::from_millis(1400)).await;
    assert_eq!(store.snapshot(), expected(&[("3", "33")]));
}

// Redisson testRetryableWriterAsyncSuccessAtLastRetry.
#[tokio::test]
async fn hash_map_write_behind_retries() {
    let store = MemoryStore::default();
    let map: Map = client()
        .await
        .hash_map_builder(unique("ms"))
        .writer(store.clone())
        .write_mode(WriteMode::write_behind())
        .writer_retry_attempts(3)
        .writer_retry_interval(Duration::from_millis(100))
        .build()
        .await
        .unwrap();
    store.fail_next(2);
    map.insert("1", "11").await.unwrap();
    tokio::time::sleep(Duration::from_millis(2400)).await;
    assert_eq!(store.snapshot(), expected(&[("1", "11")]));
    assert_eq!(store.write_calls().len(), 3);
    store.fail_next(2);
    map.remove("1").await.unwrap();
    tokio::time::sleep(Duration::from_millis(2400)).await;
    assert!(store.snapshot().is_empty());
    assert_eq!(store.delete_calls().len(), 3);
}

#[tokio::test]
async fn write_behind_returns_before_the_writer_runs() {
    let store = MemoryStore::default();
    let name = unique("ms");
    let client = client().await;
    let map = behind_on(&client, &name, &store, 50, LATER).await;
    map.insert("a", "1").await.unwrap();
    assert!(store.snapshot().is_empty());
    assert_eq!(queue_len(&name).await, ":1\r\n");
    map.flush().await.unwrap();
    assert_eq!(store.snapshot(), expected(&[("a", "1")]));
    assert_eq!(queue_len(&name).await, ":0\r\n");
}

#[tokio::test]
async fn write_behind_batches_by_batch_size() {
    let store = MemoryStore::default();
    let client = client().await;
    let map = behind_on(&client, &unique("ms"), &store, 2, LATER).await;
    for i in 1..=5 {
        map.insert(&i.to_string(), "v").await.unwrap();
    }
    map.flush().await.unwrap();
    let sizes: Vec<usize> = store.write_calls().iter().map(Vec::len).collect();
    assert_eq!(sizes, vec![2, 2, 1]);
    assert_eq!(store.snapshot().len(), 5);
}

#[tokio::test]
async fn write_behind_coalesces_a_write_and_a_delete() {
    let store = MemoryStore::default();
    let client = client().await;
    let map = behind_on(&client, &unique("ms"), &store, 50, LATER).await;
    map.insert("a", "1").await.unwrap();
    map.remove("a").await.unwrap();
    map.flush().await.unwrap();
    assert!(!store.snapshot().contains_key("a"));
    assert!(store.write_calls().is_empty());
    assert_eq!(store.delete_calls(), vec![vec!["a"]]);
}

#[tokio::test]
async fn dropping_the_last_handle_flushes() {
    let store = MemoryStore::default();
    let name = unique("ms");
    let map = behind_on(&client().await, &name, &store, 50, LATER).await;
    map.insert("a", "1").await.unwrap();
    let clone = map.clone();
    drop(map);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(store.snapshot().is_empty(), "a clone is still alive");
    drop(clone);
    eventually("the final drain", || async {
        store.snapshot() == expected(&[("a", "1")])
    })
    .await;
    assert_eq!(queue_len(&name).await, ":0\r\n");
}

#[tokio::test]
async fn another_client_processes_queued_writes() {
    let name = unique("ms");
    let first = MemoryStore::default();
    let a = common::connect_with(|builder| builder).await;
    let map_a = behind_on(&a, &name, &first, 50, LATER).await;
    map_a.insert("x", "1").await.unwrap();
    let second = MemoryStore::default();
    let b = common::connect_with(|builder| builder).await;
    let _map_b = behind_on(&b, &name, &second, 50, Duration::from_millis(100)).await;
    eventually("the second client's drain", || async {
        second.snapshot() == expected(&[("x", "1")])
    })
    .await;
    assert_eq!(queue_len(&name).await, ":0\r\n");
    assert!(first.snapshot().is_empty());
    drop(map_a);
}

#[tokio::test]
async fn write_behind_skips_a_foreign_queue_entry() {
    let store = MemoryStore::default();
    let name = unique("ms");
    let client = client().await;
    let map = behind_on(&client, &name, &store, 50, LATER).await;
    // A Java-serialized task starts with 0xac 0xed; raw_command sends text, so these are its UTF-8 bytes, just as foreign.
    raw_command(&["RPUSH", &queue_of(&name), "\u{ac}\u{ed}\u{0}\u{5}"]).await;
    map.insert("a", "1").await.unwrap();
    map.flush().await.unwrap();
    assert_eq!(store.snapshot(), expected(&[("a", "1")]));
    assert_eq!(queue_len(&name).await, ":0\r\n");
}

#[tokio::test]
async fn a_failing_write_behind_batch_is_dropped_and_reported() {
    let store = MemoryStore::default();
    store.fail_always();
    let name = unique("ms");
    let client = client().await;
    let map = behind_on(&client, &name, &store, 50, LATER).await;
    map.insert("a", "1").await.unwrap();
    assert!(matches!(map.flush().await, Err(Error::Writer(_))));
    assert_eq!(queue_len(&name).await, ":0\r\n");
    map.flush().await.unwrap();
}

#[tokio::test]
async fn flush_without_write_behind_is_a_no_op() {
    let store = MemoryStore::default();
    let map = writing(&store).await;
    map.flush().await.unwrap();
    client()
        .await
        .hash_map::<String, String>(unique("ms"))
        .flush()
        .await
        .unwrap();
    assert!(store.write_calls().is_empty());
}

type Cache = HashMapCache<String, String, JsonCodec>;

async fn cache_loading(store: &MemoryStore) -> Cache {
    client()
        .await
        .hash_map_cache_builder::<String, String>(unique("ms"))
        .loader(store.clone())
        .build()
        .await
        .unwrap()
}

async fn cache_writing(store: &MemoryStore) -> Cache {
    client()
        .await
        .hash_map_cache_builder::<String, String>(unique("ms"))
        .writer(store.clone())
        .build()
        .await
        .unwrap()
}

async fn next_event(
    events: &mut EntryEvents<String, String, JsonCodec>,
) -> EntryEvent<String, String> {
    timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("no event arrived")
        .unwrap()
}

#[tokio::test]
async fn cache_get_loads_and_stores_without_limits() {
    let store = MemoryStore::with(&[("1", "11")]);
    let cache = cache_loading(&store).await;
    let mut events = cache.events_of(&[EntryEventKind::Created]).await.unwrap();
    assert_eq!(cache.get("1").await.unwrap(), Some("11".to_string()));
    assert_eq!(cache.entry_ttl("1").await.unwrap(), None);
    assert_eq!(
        next_event(&mut events).await,
        EntryEvent::Created {
            key: "1".into(),
            value: "11".into()
        }
    );
    assert_eq!(store.loads(), 1);
    assert_eq!(cache.get("1").await.unwrap(), Some("11".to_string()));
    assert_eq!(store.loads(), 1);
}

#[tokio::test]
async fn cache_an_expired_entry_is_loaded_again() {
    let store = MemoryStore::with(&[("1", "11")]);
    let cache = cache_loading(&store).await;
    cache
        .insert("1", "00")
        .ttl(Duration::from_millis(200))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(cache.get("1").await.unwrap(), Some("11".to_string()));
    assert_eq!(store.loads(), 1);
}

#[tokio::test]
async fn cache_get_many_peek_and_contains_key_load() {
    let store = MemoryStore::with(&[("1", "11"), ("2", "22"), ("3", "33")]);
    let cache = cache_loading(&store).await;
    assert_eq!(
        cache.get_many(&["1", "9"]).await.unwrap(),
        vec![Some("11".to_string()), None]
    );
    assert_eq!(cache.peek("2").await.unwrap(), Some("22".to_string()));
    assert_eq!(
        cache.peek_many(&["3"]).await.unwrap(),
        vec![Some("33".to_string())]
    );
    let before = store.loads();
    assert!(!cache.contains_key("x").await.unwrap());
    assert_eq!(store.loads(), before + 1);
    assert!(cache.contains_key("1").await.unwrap());
    assert_eq!(store.loads(), before + 1);
}

#[tokio::test]
async fn cache_entry_loads_under_its_lock() {
    let store = MemoryStore::with(&[("1", "11")]);
    let cache = cache_loading(&store).await;
    let entry = timeout(
        Duration::from_secs(5),
        cache.entry("1").ttl(Duration::from_secs(60)),
    )
    .await
    .expect("the entry hangs")
    .unwrap();
    match entry {
        redissun::hash_map::Entry::Occupied(entry) => {
            assert_eq!(entry.get(), "11");
            entry.into_value().await.unwrap();
        }
        redissun::hash_map::Entry::Vacant(_) => panic!("the loader was not asked"),
    }
    assert_eq!(cache.get("1").await.unwrap(), Some("11".to_string()));
}

// Redisson testLoadAllReplaceValues, on the cache.
#[tokio::test]
async fn cache_load_all_keeps_present_values() {
    let store = numbered(10);
    let cache = cache_loading(&store).await;
    cache.insert("0", "010").await.unwrap();
    cache.insert("1", "011").await.unwrap();
    cache.load_all().await.unwrap();
    assert_eq!(cache.get("0").await.unwrap(), Some("010".to_string()));
    assert_eq!(cache.get("1").await.unwrap(), Some("011".to_string()));
    for i in 2..10 {
        assert_eq!(
            cache.get(&i.to_string()).await.unwrap(),
            Some(format!("{i}{i}"))
        );
    }
    cache.load_many(&["0"]).replace_existing().await.unwrap();
    assert_eq!(cache.get("0").await.unwrap(), Some("00".to_string()));
    assert_eq!(cache.len().await.unwrap(), 10);
}

// RedissonMapCacheTest testWriterPutTTL, testWriterFastPutTTL, testWriterPutIfAbsentTTL and testWriterFastPutIfAbsentTTL.
#[tokio::test]
async fn cache_writer_with_limits() {
    let ttl = Duration::from_secs(10);
    let all = expected(&[("1", "11"), ("2", "22"), ("3", "33")]);
    let store = MemoryStore::default();
    let cache = cache_writing(&store).await;
    for (k, v) in [("1", "11"), ("2", "22"), ("3", "33")] {
        cache.insert(k, v).ttl(ttl).await.unwrap();
    }
    assert_eq!(store.snapshot(), all);

    let store = MemoryStore::default();
    let cache = cache_writing(&store).await;
    for (k, v) in [("1", "11"), ("2", "22"), ("3", "33")] {
        cache.set(k, v).ttl(ttl).await.unwrap();
    }
    assert_eq!(store.snapshot(), all);

    let store = MemoryStore::default();
    let cache = cache_writing(&store).await;
    assert_eq!(cache.try_insert("1", "11").ttl(ttl).await.unwrap(), None);
    assert_eq!(
        cache.try_insert("1", "00").ttl(ttl).await.unwrap(),
        Some("11".to_string())
    );
    assert_eq!(store.snapshot(), expected(&[("1", "11")]));
    assert_eq!(store.write_calls(), vec![vec!["1"]]);
}

// Task 3's RMap writer ports, on the cache.
#[tokio::test]
async fn cache_writer_rmap_family() {
    let store = MemoryStore::default();
    let cache = cache_writing(&store).await;
    cache.insert("1", "11").await.unwrap();
    cache.replace("1", "00").await.unwrap();
    cache.replace("2", "22").await.unwrap();
    assert_eq!(store.snapshot(), expected(&[("1", "00")]));
    assert_eq!(store.write_calls(), vec![vec!["1"], vec!["1"]]);

    assert!(cache.compare_and_set("1", "00", "01").await.unwrap());
    assert!(!cache.compare_and_set("1", "00", "02").await.unwrap());
    assert_eq!(store.snapshot(), expected(&[("1", "01")]));
    assert_eq!(store.write_calls().len(), 3);

    cache.insert("2", "22").await.unwrap();
    cache.insert("3", "33").await.unwrap();
    assert!(!cache.remove_if_eq("2", "00").await.unwrap());
    assert!(cache.remove_if_eq("2", "22").await.unwrap());
    assert_eq!(store.delete_calls(), vec![vec!["2"]]);
    assert_eq!(cache.remove("1").await.unwrap(), Some("01".to_string()));
    assert_eq!(cache.remove("x").await.unwrap(), None);
    assert_eq!(store.delete_calls(), vec![vec!["2"], vec!["1"], vec!["x"]]);
    assert_eq!(store.snapshot(), expected(&[("3", "33")]));

    let numbers = MemoryStore::default();
    let counters: HashMapCache<String, i64, JsonCodec> = client()
        .await
        .hash_map_cache_builder(unique("ms"))
        .writer(numbers.clone())
        .build()
        .await
        .unwrap();
    assert_eq!(counters.incr_by("1", 11).await.unwrap(), 11);
    assert_eq!(counters.incr_by("1", 7).await.unwrap(), 18);
    assert_eq!(numbers.snapshot(), expected(&[("1", "18")]));
}

#[tokio::test]
async fn cache_remove_many_tells_the_writer_and_publishes() {
    let store = MemoryStore::default();
    let cache = cache_writing(&store).await;
    cache.insert("a", "1").await.unwrap();
    cache.insert("b", "2").await.unwrap();
    let mut events = cache.events_of(&[EntryEventKind::Removed]).await.unwrap();
    assert_eq!(cache.remove_many(&["a", "b", "zz"]).await.unwrap(), 2);
    assert_eq!(store.delete_calls(), vec![vec!["a", "b"]]);
    let mut removed = vec![next_event(&mut events).await, next_event(&mut events).await];
    removed.sort_by_key(|event| format!("{event:?}"));
    assert_eq!(
        removed,
        vec![
            EntryEvent::Removed {
                key: "a".into(),
                value: "1".into()
            },
            EntryEvent::Removed {
                key: "b".into(),
                value: "2".into()
            }
        ]
    );
    assert_eq!(cache.remove_many(&["zz"]).await.unwrap(), 0);
    assert_eq!(store.delete_calls().len(), 1);
}

#[tokio::test]
async fn cache_extend_with_ttl_writes_each_batch() {
    let store = MemoryStore::default();
    let cache = cache_writing(&store).await;
    let entries = [
        ("1", "11"),
        ("2", "22"),
        ("3", "33"),
        ("4", "44"),
        ("5", "55"),
    ];
    cache
        .extend(entries.map(|(k, v)| (k, v)))
        .batch_size(2)
        .ttl(Duration::from_secs(10))
        .await
        .unwrap();
    let sizes: Vec<usize> = store.write_calls().iter().map(Vec::len).collect();
    assert_eq!(sizes, vec![2, 2, 1]);
    assert_eq!(store.snapshot().len(), 5);
}

#[tokio::test]
async fn cache_leases_bypass_the_loader_and_the_writer() {
    let store = MemoryStore::with(&[("1", "11")]);
    let cache: Cache = client()
        .await
        .hash_map_cache_builder(unique("ms"))
        .loader(store.clone())
        .writer(store.clone())
        .build()
        .await
        .unwrap();
    let guard = match cache.lease("1", Duration::from_secs(1)).await.unwrap() {
        Lease::Acquired(guard) => guard,
        other => panic!("expected a lease, got {other:?}"),
    };
    assert_eq!(store.loads(), 0);
    guard.insert(&"v".to_string()).await.unwrap();
    assert!(store.write_calls().is_empty());
    cache.invalidate("1").await.unwrap();
    assert!(store.delete_calls().is_empty());
}

#[tokio::test]
async fn cache_write_behind_flush() {
    let store = MemoryStore::default();
    let cache: Cache = client()
        .await
        .hash_map_cache_builder(unique("ms"))
        .writer(store.clone())
        .write_mode(WriteMode::WriteBehind {
            batch_size: 50,
            delay: LATER,
        })
        .build()
        .await
        .unwrap();
    cache
        .insert("a", "1")
        .ttl(Duration::from_secs(60))
        .await
        .unwrap();
    assert!(store.snapshot().is_empty());
    cache.flush().await.unwrap();
    assert_eq!(store.snapshot(), expected(&[("a", "1")]));
}

type Local = LocalCachedMap<String, String, JsonCodec>;

async fn local_on(client: &redissun::Client, name: &str, store: &MemoryStore) -> Local {
    client
        .local_cached_map::<String, String>(name)
        .loader(store.clone())
        .writer(store.clone())
        .build()
        .await
        .unwrap()
}

// Redisson testMapLoaderGet on RedissonLocalCachedMap: a loaded value is cached and stored in Redis.
#[tokio::test]
async fn local_get_loads_caches_and_shares() {
    let store = MemoryStore::with(&[("1", "11")]);
    let name = unique("ms");
    let a = local_on(&client().await, &name, &store).await;
    let b = local_on(&client().await, &name, &store).await;
    assert_eq!(a.get("1").await.unwrap(), Some("11".to_string()));
    assert_eq!(a.local_len(), 1);
    assert_eq!(
        raw_command(&["HGET", &name, "\"1\""]).await,
        "$4\r\n\"11\"\r\n"
    );
    assert_eq!(b.get("1").await.unwrap(), Some("11".to_string()));
    assert_eq!(store.loads(), 1);
    assert!(store.write_calls().is_empty());
}

#[tokio::test]
async fn local_a_miss_is_cached_only_with_store_cache_miss() {
    let store = MemoryStore::default();
    let plain = local_on(&client().await, &unique("ms"), &store).await;
    assert_eq!(plain.get("x").await.unwrap(), None);
    assert_eq!(plain.get("x").await.unwrap(), None);
    assert_eq!(store.loads(), 2);
    assert_eq!(plain.local_len(), 0);

    let store = MemoryStore::default();
    let caching: Local = client()
        .await
        .local_cached_map(unique("ms"))
        .loader(store.clone())
        .store_cache_miss(true)
        .build()
        .await
        .unwrap();
    assert_eq!(caching.get("x").await.unwrap(), None);
    assert_eq!(caching.get("x").await.unwrap(), None);
    assert_eq!(store.loads(), 1);
    assert_eq!(caching.local_len(), 1);
}

#[tokio::test]
async fn local_contains_key_loads() {
    let store = MemoryStore::with(&[("1", "11")]);
    let map = local_on(&client().await, &unique("ms"), &store).await;
    assert!(map.contains_key("1").await.unwrap());
    assert_eq!(map.local_len(), 1);
    assert!(!map.contains_key("2").await.unwrap());
    assert_eq!(store.loads(), 2);
    assert_eq!(map.len().await.unwrap(), 1);
}

#[tokio::test]
async fn local_insert_and_remove_call_the_writer() {
    let store = MemoryStore::default();
    let map = local_on(&client().await, &unique("ms"), &store).await;
    map.insert("a", "1").await.unwrap();
    assert_eq!(store.write_calls(), vec![vec!["a"]]);
    assert_eq!(store.snapshot(), expected(&[("a", "1")]));
    map.remove("a").await.unwrap();
    assert_eq!(store.delete_calls(), vec![vec!["a"]]);
    assert!(store.snapshot().is_empty());
    map.remove("missing").await.unwrap();
    assert_eq!(store.delete_calls(), vec![vec!["a"], vec!["missing"]]);
}

#[tokio::test]
async fn local_load_all_loads_every_key() {
    let store = numbered(20);
    let map = local_on(&client().await, &unique("ms"), &store).await;
    map.load_all().parallelism(4).await.unwrap();
    assert_eq!(map.len().await.unwrap(), 20);
    assert_eq!(map.local_len(), 20);
    assert!(store.write_calls().is_empty());
    map.load_many(&["3"]).replace_existing().await.unwrap();
    assert_eq!(store.loads(), 21);
}

#[tokio::test]
async fn local_write_behind_flush() {
    let store = MemoryStore::default();
    let map: Local = client()
        .await
        .local_cached_map(unique("ms"))
        .writer(store.clone())
        .write_mode(WriteMode::WriteBehind {
            batch_size: 50,
            delay: LATER,
        })
        .build()
        .await
        .unwrap();
    map.insert("a", "1").await.unwrap();
    map.insert("b", "2").await.unwrap();
    map.remove("b").await.unwrap();
    assert!(store.snapshot().is_empty());
    map.flush().await.unwrap();
    assert_eq!(store.snapshot(), expected(&[("a", "1")]));
}

// Review Focus 5: loading (lock plus write), write-behind (RPUSH/LPOP) and FAST_REMOVE_BATCH stay in one slot.
#[tokio::test]
async fn loader_and_writer_work_on_a_cluster() {
    let topology = common::topology::Topology::cluster().await;
    let client = redissun::Client::builder()
        .url(topology.cluster_url())
        .build()
        .await
        .unwrap();
    let behind = WriteMode::WriteBehind {
        batch_size: 50,
        delay: Duration::from_millis(100),
    };
    for name in [unique("ms"), format!("{{{}}}:m", unique("t"))] {
        let store = MemoryStore::with(&[("1", "11")]);
        let map: Map = client
            .hash_map_builder(format!("{name}:map"))
            .loader(store.clone())
            .writer(store.clone())
            .write_mode(behind)
            .build()
            .await
            .unwrap();
        assert_eq!(map.get("1").await.unwrap(), Some("11".to_string()));
        map.insert("a", "1").await.unwrap();
        map.insert("b", "2").await.unwrap();
        map.flush().await.unwrap();
        assert_eq!(
            store.snapshot(),
            expected(&[("1", "11"), ("a", "1"), ("b", "2")])
        );
        assert_eq!(map.remove_many(&["a", "b"]).await.unwrap(), 2);
        map.flush().await.unwrap();
        assert_eq!(store.delete_calls(), vec![vec!["a", "b"]], "{name}");
        assert_eq!(store.snapshot(), expected(&[("1", "11")]));

        let store = MemoryStore::with(&[("1", "11")]);
        let cache: Cache = client
            .hash_map_cache_builder(format!("{name}:cache"))
            .loader(store.clone())
            .writer(store.clone())
            .write_mode(behind)
            .build()
            .await
            .unwrap();
        assert_eq!(cache.get("1").await.unwrap(), Some("11".to_string()));
        cache
            .insert("a", "1")
            .ttl(Duration::from_secs(60))
            .await
            .unwrap();
        cache.insert("b", "2").await.unwrap();
        cache.flush().await.unwrap();
        assert_eq!(
            store.snapshot(),
            expected(&[("1", "11"), ("a", "1"), ("b", "2")])
        );
        assert_eq!(cache.remove_many(&["a", "b"]).await.unwrap(), 2);
        cache.flush().await.unwrap();
        assert_eq!(store.delete_calls(), vec![vec!["a", "b"]], "{name}");
        assert_eq!(store.snapshot(), expected(&[("1", "11")]));

        let store = MemoryStore::with(&[("1", "11")]);
        let local = local_on(&client, &format!("{name}:local"), &store).await;
        assert_eq!(local.get("1").await.unwrap(), Some("11".to_string()));
        assert_eq!(local.local_len(), 1);
        assert_eq!(local.len().await.unwrap(), 1);
        local.insert("a", "1").await.unwrap();
        assert_eq!(store.write_calls(), vec![vec!["a"]]);
    }
}

// Review fix: every build of one name in one client shares one write-behind drainer, as Redisson's WriteBehindService keeps one task per name, so one key's writes reach the writer in order.
#[tokio::test]
async fn two_builds_of_one_name_share_one_drainer() {
    let store = MemoryStore::default();
    store.slow(Duration::from_millis(300));
    let name = unique("ms");
    let client = client().await;
    let fast = Duration::from_millis(50);
    let first = behind_on(&client, &name, &store, 50, fast).await;
    let second = behind_on(&client, &name, &store, 50, fast).await;
    first.insert("a", "1").await.unwrap();
    // A drain pops `a=1` and sits in the slow writer.
    tokio::time::sleep(Duration::from_millis(150)).await;
    second.insert("a", "2").await.unwrap();
    // A second drainer would pop `a=2` now and call the writer next to the first call.
    tokio::time::sleep(Duration::from_millis(150)).await;
    first.flush().await.unwrap();
    second.flush().await.unwrap();
    assert_eq!(store.max_in_flight(), 1, "two drains ran at once");
    assert_eq!(store.write_calls(), vec![vec!["a"], vec!["a"]]);
    assert_eq!(store.snapshot(), expected(&[("a", "2")]));
}

// Review fix: a cut reply must not be retried into a wrong answer when a writer depends on it.
async fn cut_writing(cutter: &common::cutter::Cutter, store: &MemoryStore, name: &str) -> Map {
    cutter
        .client()
        .await
        .hash_map_builder::<String, String>(name)
        .writer(store.clone())
        .build()
        .await
        .unwrap()
}

#[tokio::test]
async fn hash_map_compare_and_set_with_a_writer_is_not_retried_after_a_lost_reply() {
    let cutter = common::cutter::Cutter::start().await;
    let store = MemoryStore::default();
    let name = unique("ms");
    let map = cut_writing(&cutter, &store, &name).await;
    map.insert("1", "11").await.unwrap();
    map.compare_and_set("warm", "x", "y").await.unwrap();
    cutter.arm(&["EVALSHA", &name, "\"00\""]);
    let replaced = map.compare_and_set("1", "11", "00").await;
    assert_eq!(cutter.cuts(), 1);
    assert!(replaced.is_err(), "a retry answered {replaced:?}");
    assert_eq!(map.get("1").await.unwrap(), Some("00".to_string()));
}

#[tokio::test]
async fn hash_map_remove_if_eq_with_a_writer_is_not_retried_after_a_lost_reply() {
    let cutter = common::cutter::Cutter::start().await;
    let store = MemoryStore::default();
    let name = unique("ms");
    let map = cut_writing(&cutter, &store, &name).await;
    map.insert("1", "11").await.unwrap();
    map.remove_if_eq("warm", "x").await.unwrap();
    cutter.arm(&["EVALSHA", &name, "\"1\""]);
    let removed = map.remove_if_eq("1", "11").await;
    assert_eq!(cutter.cuts(), 1);
    assert!(removed.is_err(), "a retry answered {removed:?}");
    assert!(store.delete_calls().is_empty());
}

#[tokio::test]
async fn hash_map_remove_many_with_a_writer_is_not_retried_after_a_lost_reply() {
    let cutter = common::cutter::Cutter::start().await;
    let store = MemoryStore::default();
    let name = unique("ms");
    let map = cut_writing(&cutter, &store, &name).await;
    map.insert("1", "11").await.unwrap();
    map.remove_many(&["warm"]).await.unwrap();
    cutter.arm(&["EVALSHA", &name, "\"1\""]);
    let removed = map.remove_many(&["1"]).await;
    assert_eq!(cutter.cuts(), 1);
    assert!(removed.is_err(), "a retry answered {removed:?}");
}

async fn cut_cache(cutter: &common::cutter::Cutter, store: &MemoryStore, name: &str) -> Cache {
    cutter
        .client()
        .await
        .hash_map_cache_builder::<String, String>(name)
        .writer(store.clone())
        .build()
        .await
        .unwrap()
}

#[tokio::test]
async fn cache_compare_and_set_with_a_writer_is_not_retried_after_a_lost_reply() {
    let cutter = common::cutter::Cutter::start().await;
    let store = MemoryStore::default();
    let name = unique("ms");
    let cache = cut_cache(&cutter, &store, &name).await;
    cache.insert("1", "11").await.unwrap();
    cache.compare_and_set("warm", "x", "y").await.unwrap();
    cutter.arm(&["EVALSHA", &name, "\"00\""]);
    let replaced = cache.compare_and_set("1", "11", "00").await;
    assert_eq!(cutter.cuts(), 1);
    assert!(replaced.is_err(), "a retry answered {replaced:?}");
}

#[tokio::test]
async fn cache_remove_if_eq_with_a_writer_is_not_retried_after_a_lost_reply() {
    let cutter = common::cutter::Cutter::start().await;
    let store = MemoryStore::default();
    let name = unique("ms");
    let cache = cut_cache(&cutter, &store, &name).await;
    cache.insert("1", "11").await.unwrap();
    cache.remove_if_eq("warm", "x").await.unwrap();
    cutter.arm(&["EVALSHA", &name, "\"1\""]);
    let removed = cache.remove_if_eq("1", "11").await;
    assert_eq!(cutter.cuts(), 1);
    assert!(removed.is_err(), "a retry answered {removed:?}");
    assert!(store.delete_calls().is_empty());
}

#[tokio::test]
async fn cache_remove_many_with_a_writer_is_not_retried_after_a_lost_reply() {
    let cutter = common::cutter::Cutter::start().await;
    let store = MemoryStore::default();
    let name = unique("ms");
    let cache = cut_cache(&cutter, &store, &name).await;
    cache.insert("1", "11").await.unwrap();
    cache.remove_many(&["warm"]).await.unwrap();
    cutter.arm(&["EVALSHA", &name, "\"1\""]);
    let removed = cache.remove_many(&["1"]).await;
    assert_eq!(cutter.cuts(), 1);
    assert!(removed.is_err(), "a retry answered {removed:?}");
}

#[tokio::test]
async fn write_behind_push_is_not_retried_after_a_lost_reply() {
    let cutter = common::cutter::Cutter::start().await;
    let store = MemoryStore::default();
    let name = unique("ms");
    let client = cutter.client().await;
    let map = behind_on(&client, &name, &store, 50, LATER).await;
    cutter.arm(&["RPUSH", &queue_of(&name)]);
    let pushed = map.insert("a", "1").await;
    assert_eq!(cutter.cuts(), 1);
    assert!(pushed.is_err(), "a retry answered {pushed:?}");
    assert_eq!(queue_len(&name).await, ":1\r\n");
}

#[tokio::test]
async fn write_behind_poll_is_not_retried_after_a_lost_reply() {
    let cutter = common::cutter::Cutter::start().await;
    let store = MemoryStore::default();
    let name = unique("ms");
    let client = cutter.client().await;
    let map = behind_on(&client, &name, &store, 50, LATER).await;
    map.insert("a", "1").await.unwrap();
    map.insert("b", "2").await.unwrap();
    cutter.arm(&["LPOP", &queue_of(&name)]);
    let flushed = map.flush().await;
    assert_eq!(cutter.cuts(), 1);
    assert!(flushed.is_err(), "a retry answered {flushed:?}");
    // The popped task is lost, as in Redisson; the rest stays queued for the next drain.
    assert_eq!(queue_len(&name).await, ":1\r\n");
    map.flush().await.unwrap();
    assert_eq!(store.snapshot(), expected(&[("b", "2")]));
}

// Review fix: a get cancelled while it loads must not keep the key out of the local cache.
#[tokio::test]
async fn local_a_cancelled_read_does_not_stop_caching() {
    let store = MemoryStore::with(&[("1", "11")]);
    let name = unique("ms");
    let map = local_on(&client().await, &name, &store).await;
    store.slow(Duration::from_millis(500));
    assert!(timeout(Duration::from_millis(100), map.get("1"))
        .await
        .is_err());
    map.clear_local();
    // Another client stores the key, so the next get reads it from Redis and caches it.
    raw_command(&["HSET", &name, "\"1\"", "\"11\""]).await;
    assert_eq!(map.get("1").await.unwrap(), Some("11".to_string()));
    assert_eq!(map.local_len(), 1, "the key was not cached");
}

// --- MapCacheNative ---

type NativeMap = redissun::MapCacheNative<String, String, JsonCodec>;

async fn native_on(
    client: &redissun::Client,
    store: &MemoryStore,
    loader: bool,
    writer: bool,
) -> NativeMap {
    let mut builder = client.map_cache_native_builder::<String, String>(unique("mcn"));
    if loader {
        builder = builder.loader(store.clone());
    }
    if writer {
        builder = builder.writer(store.clone());
    }
    builder.build().await.unwrap()
}

#[tokio::test]
async fn native_get_loads_a_missing_key_without_a_ttl() {
    let store = MemoryStore::with(&[("1", "11")]);
    let map = native_on(&client().await, &store, true, false).await;
    assert_eq!(map.get("1").await.unwrap(), Some("11".to_string()));
    assert_eq!(map.len().await.unwrap(), 1);
    assert_eq!(map.entry_ttl("1").await.unwrap(), None);
    assert_eq!(store.loads(), 1);
}

#[tokio::test]
async fn native_entry_loads_under_its_lock() {
    let store = MemoryStore::with(&[("1", "11")]);
    let map = native_on(&client().await, &store, true, false).await;
    let entry = timeout(
        Duration::from_secs(5),
        map.entry("1").ttl(Duration::from_secs(60)),
    )
    .await
    .expect("the entry hangs")
    .unwrap();
    match &entry {
        Entry::Occupied(occupied) => assert_eq!(occupied.get(), "11"),
        Entry::Vacant(_) => panic!("the loader was not asked"),
    }
    // The loaded value is stored without a TTL; the entry's write gives it one.
    assert_eq!(map.entry_ttl("1").await.unwrap(), None);
    let value = entry
        .and_modify(|v| v.push('!'))
        .or_insert("x".to_string())
        .await
        .unwrap();
    assert_eq!(value, "11!");
    assert!(map.entry_ttl("1").await.unwrap().is_some());
    assert_eq!(store.loads(), 1);
}

// RedissonMapCacheNativeTest testWriterPutTTL, testWriterFastPutTTL, testWriterPutIfAbsentTTL, testWriterFastPutIfAbsentTTL.
#[tokio::test]
async fn native_writer_gets_ttl_writes() {
    let ttl = Duration::from_secs(10);
    let all = expected(&[("1", "11"), ("2", "22"), ("3", "33")]);
    let client = client().await;

    let store = MemoryStore::default();
    let map = native_on(&client, &store, false, true).await;
    for (k, v) in [("1", "11"), ("2", "22"), ("3", "33")] {
        map.insert(k, v).ttl(ttl).await.unwrap();
    }
    assert_eq!(store.snapshot(), all);
    assert_eq!(store.write_calls().len(), 3);

    let store = MemoryStore::default();
    let map = native_on(&client, &store, false, true).await;
    for (k, v) in [("1", "11"), ("2", "22"), ("3", "33")] {
        map.set(k, v).ttl(ttl).await.unwrap();
    }
    assert_eq!(store.snapshot(), all);
    assert_eq!(store.write_calls().len(), 3);

    let store = MemoryStore::default();
    let map = native_on(&client, &store, false, true).await;
    assert_eq!(map.try_insert("1", "11").ttl(ttl).await.unwrap(), None);
    assert_eq!(
        map.try_insert("1", "00").ttl(ttl).await.unwrap(),
        Some("11".to_string())
    );
    assert_eq!(map.replace("2", "22").ttl(ttl).await.unwrap(), None);
    assert_eq!(store.snapshot(), expected(&[("1", "11")]));
    assert_eq!(store.write_calls(), vec![vec!["1"]]);
    assert_eq!(
        map.replace("1", "12").ttl(ttl).await.unwrap(),
        Some("11".to_string())
    );
    assert_eq!(store.snapshot(), expected(&[("1", "12")]));
}

#[tokio::test]
async fn native_writer_gets_all_or_nothing_writes() {
    let store = MemoryStore::default();
    let map = native_on(&common::redis8_client().await, &store, false, true).await;
    let ttl = Duration::from_secs(10);
    assert!(map
        .try_extend([("1", "11"), ("2", "22")])
        .ttl(ttl)
        .await
        .unwrap());
    assert_eq!(store.snapshot(), expected(&[("1", "11"), ("2", "22")]));
    assert!(!map
        .try_extend([("2", "x"), ("3", "33")])
        .ttl(ttl)
        .await
        .unwrap());
    assert!(!map.update_many([("1", "x"), ("3", "33")]).await.unwrap());
    assert_eq!(store.write_calls().len(), 1);
    assert!(map
        .update_many([("1", "12"), ("2", "23")])
        .keep_ttl()
        .await
        .unwrap());
    assert_eq!(store.snapshot(), expected(&[("1", "12"), ("2", "23")]));
    assert_eq!(store.write_calls().len(), 2);
}

#[tokio::test]
async fn native_expire_and_persist_do_not_call_the_writer() {
    let store = MemoryStore::default();
    let map = native_on(&client().await, &store, false, true).await;
    map.insert("1", "11").await.unwrap();
    let calls = store.write_calls().len();
    let ttl = Duration::from_secs(10);
    assert!(map.expire_entry("1").ttl(ttl).await.unwrap());
    assert_eq!(
        map.expire_entries(&["1"]).ttl(ttl).if_less().await.unwrap(),
        0
    );
    assert!(map.persist_entry("1").await.unwrap());
    map.persist_entries(&["1"]).await.unwrap();
    assert!(map.expire_entry("1").await.unwrap());
    assert_eq!(store.write_calls().len(), calls);
    assert!(store.delete_calls().is_empty());
}

#[tokio::test]
async fn native_write_behind_flushes() {
    let store = MemoryStore::default();
    let map: NativeMap = client()
        .await
        .map_cache_native_builder(unique("mcn"))
        .writer(store.clone())
        .write_mode(WriteMode::WriteBehind {
            batch_size: 10,
            delay: Duration::from_millis(100),
        })
        .build()
        .await
        .unwrap();
    let entries: Vec<(String, String)> =
        (0..25).map(|i| (i.to_string(), format!("v{i}"))).collect();
    map.extend(entries.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .ttl(Duration::from_secs(60))
        .await
        .unwrap();
    map.flush().await.unwrap();
    assert_eq!(store.snapshot().len(), 25);
    assert_eq!(map.len().await.unwrap(), 25);
}
