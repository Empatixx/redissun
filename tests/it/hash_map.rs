use crate::common::{client, unique};
use futures::TryStreamExt;
use redissun::Object;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct User {
    name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct SimpleKey(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct SimpleValue(String);

fn key(value: &str) -> SimpleKey {
    SimpleKey(value.into())
}

fn value(value: &str) -> SimpleValue {
    SimpleValue(value.into())
}

async fn simple() -> redissun::HashMap<SimpleKey, SimpleValue, redissun::JsonCodec> {
    client().await.hash_map(unique("simple"))
}

async fn simple_three() -> redissun::HashMap<SimpleKey, SimpleValue, redissun::JsonCodec> {
    let map = simple().await;
    map.insert(&key("1"), &value("2")).await.unwrap();
    map.insert(&key("33"), &value("44")).await.unwrap();
    map.insert(&key("5"), &value("6")).await.unwrap();
    map
}

#[tokio::test]
async fn insert_returns_the_previous_value() {
    let map = client().await.hash_map::<String, i64>(unique("map"));
    assert_eq!(map.insert("a", &1i64).await.unwrap(), None);
    assert_eq!(map.insert("a", &2i64).await.unwrap(), Some(1));
    assert_eq!(map.get("a").await.unwrap(), Some(2));
}

#[tokio::test]
async fn get_accepts_a_borrowed_key() {
    let map = client().await.hash_map::<String, User>(unique("map"));
    let jirka = User {
        name: "Jirka".into(),
    };
    map.insert("jirka", &jirka).await.unwrap();
    assert_eq!(jirka.name, "Jirka");
    let found = map.get("jirka").await.unwrap();
    assert_eq!(
        found,
        Some(User {
            name: "Jirka".into()
        })
    );
    assert_eq!(map.get("missing").await.unwrap(), None);
}

#[tokio::test]
async fn remove_returns_the_removed_value() {
    let map = client().await.hash_map::<String, i64>(unique("map"));
    map.insert("a", &1i64).await.unwrap();
    assert_eq!(map.remove("a").await.unwrap(), Some(1));
    assert_eq!(map.remove("a").await.unwrap(), None);
    assert!(!map.contains_key("a").await.unwrap());
}

#[tokio::test]
async fn len_is_empty_and_clear() {
    let map = client().await.hash_map::<String, i64>(unique("map"));
    assert!(map.is_empty().await.unwrap());
    map.insert("a", &1i64).await.unwrap();
    map.insert("b", &2i64).await.unwrap();
    assert_eq!(map.len().await.unwrap(), 2);
    assert!(map.contains_key("a").await.unwrap());
    map.clear().await.unwrap();
    assert!(map.is_empty().await.unwrap());
    assert!(!map.exists().await.unwrap());
}

#[tokio::test]
async fn extend_and_get_many() {
    let map = client().await.hash_map::<String, i64>(unique("map"));
    map.extend([("a", &1i64), ("b", &2i64)]).await.unwrap();
    let found = map.get_many(&["a", "missing", "b"]).await.unwrap();
    assert_eq!(found, vec![Some(1), None, Some(2)]);
}

#[tokio::test]
async fn extend_and_get_many_accept_empty_input() {
    let map = client().await.hash_map::<String, i64>(unique("map"));
    map.extend(Vec::<(&str, &i64)>::new()).await.unwrap();
    assert!(map.get_many::<str>(&[]).await.unwrap().is_empty());
    assert!(map.is_empty().await.unwrap());
}

#[tokio::test]
async fn insert_nx_only_inserts_missing_keys() {
    let map = client().await.hash_map::<String, i64>(unique("map"));
    assert!(map.insert_nx("a", &1i64).await.unwrap());
    assert!(!map.insert_nx("a", &2i64).await.unwrap());
    assert_eq!(map.get("a").await.unwrap(), Some(1));
}

#[tokio::test]
async fn incr_by_creates_and_accumulates() {
    let map = client().await.hash_map::<String, i64>(unique("map"));
    assert_eq!(map.incr_by("hits", 5).await.unwrap(), 5);
    assert_eq!(map.incr_by("hits", -2).await.unwrap(), 3);
    assert_eq!(map.get("hits").await.unwrap(), Some(3));
}

#[tokio::test]
async fn incr_by_float_accumulates() {
    let map = client().await.hash_map::<String, f64>(unique("map"));
    assert_eq!(map.incr_by_float("score", 1.5).await.unwrap(), 1.5);
    assert_eq!(map.incr_by_float("score", 1.0).await.unwrap(), 2.5);
}

#[tokio::test]
async fn iteration_spans_more_than_one_scan_page() {
    let map = client().await.hash_map::<String, i64>(unique("map"));
    let expected: HashMap<String, i64> = (0..250).map(|i| (format!("key-{i}"), i)).collect();
    map.extend(expected.iter()).await.unwrap();

    let entries: HashMap<String, i64> = map.iter().try_collect().await.unwrap();
    assert_eq!(entries, expected);

    let mut keys: Vec<String> = map.keys().try_collect().await.unwrap();
    keys.sort();
    assert_eq!(keys.len(), 250);

    let sum: i64 = map
        .values()
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .iter()
        .sum();
    assert_eq!(sum, (0..250i64).sum::<i64>());
}

#[tokio::test]
async fn iterating_an_empty_map_yields_nothing() {
    let map = client().await.hash_map::<String, i64>(unique("map"));
    let entries: Vec<(String, i64)> = map.iter().try_collect().await.unwrap();
    assert!(entries.is_empty());
}

#[tokio::test]
async fn empty_unicode_and_numeric_keys_round_trip() {
    let strings = client().await.hash_map::<String, i64>(unique("map"));
    for (i, key) in ["", "žluťoučký 🐎", "a:b/c d"].into_iter().enumerate() {
        strings.insert(key, &(i as i64)).await.unwrap();
        assert_eq!(strings.get(key).await.unwrap(), Some(i as i64));
    }

    let numbers = client().await.hash_map::<u64, String>(unique("map"));
    numbers.insert(&42u64, "answer").await.unwrap();
    assert_eq!(numbers.get(&42).await.unwrap().as_deref(), Some("answer"));
}

#[tokio::test]
async fn null_values_are_distinct_from_missing_keys() {
    let map = client()
        .await
        .hash_map::<String, Option<i64>>(unique("map"));
    map.insert("nothing", &None::<i64>).await.unwrap();
    assert_eq!(map.get("nothing").await.unwrap(), Some(None));
    assert_eq!(map.get("missing").await.unwrap(), None);
}

#[tokio::test]
async fn test_put_if_absent() {
    let map = simple().await;
    map.insert(&key("1"), &value("2")).await.unwrap();
    assert_eq!(
        map.insert_if_absent(&key("1"), &value("3")).await.unwrap(),
        Some(value("2"))
    );
    assert_eq!(map.get(&key("1")).await.unwrap(), Some(value("2")));
    assert_eq!(
        map.insert_if_absent(&key("2"), &value("4")).await.unwrap(),
        None
    );
    assert_eq!(map.get(&key("2")).await.unwrap(), Some(value("4")));
}

#[tokio::test]
async fn test_fast_put_if_absent() {
    let map = simple().await;
    map.insert(&key("1"), &value("2")).await.unwrap();
    assert!(!map.insert_nx(&key("1"), &value("3")).await.unwrap());
    assert_eq!(map.get(&key("1")).await.unwrap(), Some(value("2")));
    assert!(map.insert_nx(&key("2"), &value("4")).await.unwrap());
    assert_eq!(map.get(&key("2")).await.unwrap(), Some(value("4")));
}

#[tokio::test]
async fn test_fast_put_if_exists() {
    let map = simple().await;
    assert!(!map
        .fast_insert_if_exists(&key("1"), &value("3"))
        .await
        .unwrap());
    assert_eq!(map.get(&key("1")).await.unwrap(), None);
    map.insert(&key("1"), &value("2")).await.unwrap();
    assert!(map
        .fast_insert_if_exists(&key("1"), &value("3"))
        .await
        .unwrap());
    assert_eq!(map.get(&key("1")).await.unwrap(), Some(value("3")));
}

#[tokio::test]
async fn test_put_if_exists() {
    let map = simple().await;
    assert_eq!(
        map.insert_if_exists(&key("1"), &value("3")).await.unwrap(),
        None
    );
    assert_eq!(map.get(&key("1")).await.unwrap(), None);
    map.insert(&key("1"), &value("2")).await.unwrap();
    assert_eq!(
        map.insert_if_exists(&key("1"), &value("3")).await.unwrap(),
        Some(value("2"))
    );
    assert_eq!(map.get(&key("1")).await.unwrap(), Some(value("3")));
}

#[tokio::test]
async fn test_deserialization_error_returns_error_immediately() {
    let client = client().await;
    let name = unique("map");
    client
        .hash_map::<String, String>(name.clone())
        .insert("test-key", "test-val")
        .await
        .unwrap();
    let map = client.hash_map::<String, User>(name);
    assert!(matches!(
        map.get("test-key").await,
        Err(redissun::Error::Codec(_))
    ));
}

#[tokio::test]
async fn test_replace_old_value_fail() {
    let map = simple().await;
    map.insert(&key("1"), &value("2")).await.unwrap();
    assert!(!map
        .replace_if(&key("1"), &value("43"), &value("31"))
        .await
        .unwrap());
    assert_eq!(map.get(&key("1")).await.unwrap(), Some(value("2")));
}

#[tokio::test]
async fn test_replace_old_value_success() {
    let map = simple().await;
    map.insert(&key("1"), &value("2")).await.unwrap();
    assert!(map
        .replace_if(&key("1"), &value("2"), &value("3"))
        .await
        .unwrap());
    assert!(!map
        .replace_if(&key("1"), &value("2"), &value("3"))
        .await
        .unwrap());
    assert_eq!(map.get(&key("1")).await.unwrap(), Some(value("3")));
}

#[tokio::test]
async fn test_replace_value() {
    let map = simple().await;
    map.insert(&key("1"), &value("2")).await.unwrap();
    assert_eq!(
        map.replace(&key("1"), &value("3")).await.unwrap(),
        Some(value("2"))
    );
    assert_eq!(map.get(&key("1")).await.unwrap(), Some(value("3")));
    assert_eq!(map.replace(&key("9"), &value("3")).await.unwrap(), None);
    assert!(!map.contains_key(&key("9")).await.unwrap());
}

#[tokio::test]
async fn test_replace() {
    let map = simple_three().await;
    assert_eq!(map.get(&key("33")).await.unwrap(), Some(value("44")));
    map.insert(&key("33"), &value("abc")).await.unwrap();
    assert_eq!(map.get(&key("33")).await.unwrap(), Some(value("abc")));
}

#[tokio::test]
async fn test_contains_value() {
    let map = simple_three().await;
    assert!(map.contains_value(&value("2")).await.unwrap());
    assert!(!map.contains_value(&value("441")).await.unwrap());
    assert!(!map.contains_value(&value("5")).await.unwrap());
}

#[tokio::test]
async fn test_contains_key() {
    let map = simple_three().await;
    assert!(map.contains_key(&key("33")).await.unwrap());
    assert!(!map.contains_key(&key("34")).await.unwrap());
}

#[tokio::test]
async fn test_remove_value_fail() {
    let map = simple().await;
    map.insert(&key("1"), &value("2")).await.unwrap();
    assert!(!map.remove_if(&key("2"), &value("1")).await.unwrap());
    assert!(!map.remove_if(&key("1"), &value("3")).await.unwrap());
    assert_eq!(map.get(&key("1")).await.unwrap(), Some(value("2")));
}

#[tokio::test]
async fn test_remove_value() {
    let map = simple().await;
    map.insert(&key("1"), &value("2")).await.unwrap();
    assert!(map.remove_if(&key("1"), &value("2")).await.unwrap());
    assert_eq!(map.get(&key("1")).await.unwrap(), None);
    assert_eq!(map.len().await.unwrap(), 0);
}

#[tokio::test]
async fn test_remove_object() {
    let map = simple_three().await;
    assert_eq!(map.remove(&key("33")).await.unwrap(), Some(value("44")));
    assert_eq!(map.remove(&key("5")).await.unwrap(), Some(value("6")));
    assert_eq!(map.remove(&key("11")).await.unwrap(), None);
    assert_eq!(map.len().await.unwrap(), 1);
}

#[tokio::test]
async fn test_remove() {
    let map = client().await.hash_map::<i32, i32>(unique("simple"));
    map.insert(&1, &3).await.unwrap();
    map.insert(&3, &5).await.unwrap();
    map.insert(&7, &8).await.unwrap();
    assert_eq!(map.remove(&1).await.unwrap(), Some(3));
    assert_eq!(map.remove(&3).await.unwrap(), Some(5));
    assert_eq!(map.remove(&10).await.unwrap(), None);
    assert_eq!(map.remove(&7).await.unwrap(), Some(8));
}

#[tokio::test]
async fn test_fast_remove() {
    let map = client().await.hash_map::<i32, i32>(unique("simple"));
    map.extend([(&1, &3), (&3, &5), (&4, &6), (&7, &8)])
        .await
        .unwrap();
    assert_eq!(map.fast_remove(&[&1, &3, &7]).await.unwrap(), 3);
    assert_eq!(map.len().await.unwrap(), 1);
}

#[tokio::test]
async fn test_fast_remove_empty() {
    let map = client().await.hash_map::<i32, i32>(unique("simple"));
    map.insert(&1, &3).await.unwrap();
    assert_eq!(map.fast_remove::<i32>(&[]).await.unwrap(), 0);
    assert_eq!(map.len().await.unwrap(), 1);
}

#[tokio::test]
async fn test_fast_put() {
    let map = client().await.hash_map::<i32, i32>(unique("simple"));
    assert!(map.fast_insert(&1, &2).await.unwrap());
    assert_eq!(map.get(&1).await.unwrap(), Some(2));
    assert!(!map.fast_insert(&1, &3).await.unwrap());
    assert_eq!(map.get(&1).await.unwrap(), Some(3));
    assert_eq!(map.len().await.unwrap(), 1);
}

#[tokio::test]
async fn test_fast_replace() {
    let map = client().await.hash_map::<i32, i32>(unique("simple"));
    map.insert(&1, &2).await.unwrap();
    assert!(map.fast_replace(&1, &3).await.unwrap());
    assert!(!map.fast_replace(&2, &0).await.unwrap());
    assert_eq!(map.len().await.unwrap(), 1);
    assert_eq!(map.get(&1).await.unwrap(), Some(3));
}

#[tokio::test]
async fn test_empty_remove() {
    let map = client().await.hash_map::<i32, i32>(unique("simple"));
    assert!(!map.remove_if(&1, &3).await.unwrap());
    map.insert(&4, &5).await.unwrap();
    assert!(map.remove_if(&4, &5).await.unwrap());
}

#[tokio::test]
async fn test_value_size() {
    let map = client().await.hash_map::<String, String>(unique("getAll"));
    map.insert("1", "1234").await.unwrap();
    assert_eq!(map.value_size("4").await.unwrap(), 0);
    assert_eq!(map.value_size("1").await.unwrap(), 6);
}

#[tokio::test]
async fn test_add_and_get() {
    let client = client().await;
    let ints = client.hash_map::<i32, i64>(unique("getAll"));
    ints.insert(&1, &100).await.unwrap();
    assert_eq!(ints.incr_by(&1, 12).await.unwrap(), 112);
    assert_eq!(ints.get(&1).await.unwrap(), Some(112));

    let doubles = client.hash_map::<i32, f64>(unique("getAll2"));
    doubles.insert(&1, &100.2).await.unwrap();
    assert_eq!(doubles.incr_by_float(&1, 12.1).await.unwrap(), 112.3);
    assert_eq!(doubles.get(&1).await.unwrap(), Some(112.3));

    let strings = client.hash_map::<String, i64>(unique("mapStr"));
    assert_eq!(strings.insert("1", &100).await.unwrap(), None);
    assert_eq!(strings.incr_by("1", 12).await.unwrap(), 112);
    assert_eq!(strings.get("1").await.unwrap(), Some(112));
}

#[tokio::test]
async fn test_get_all() {
    let map = client().await.hash_map::<i32, i32>(unique("getAll"));
    map.extend([(&1, &100), (&2, &200), (&3, &300), (&4, &400)])
        .await
        .unwrap();
    assert_eq!(
        map.get_many(&[&2, &3, &5]).await.unwrap(),
        vec![Some(200), Some(300), None]
    );
}

#[tokio::test]
async fn test_get_all_with_string_keys() {
    let map = client()
        .await
        .hash_map::<String, i32>(unique("getAllStrings"));
    map.extend([("A", &100), ("B", &200), ("C", &300), ("D", &400)])
        .await
        .unwrap();
    assert_eq!(
        map.get_many(&["B", "C", "E"]).await.unwrap(),
        vec![Some(200), Some(300), None]
    );
}

#[tokio::test]
async fn test_get_all_big() {
    let map = client().await.hash_map::<i32, String>(unique("simple"));
    let entries: Vec<(i32, String)> = (0..10_000).map(|i| (i, i.to_string())).collect();
    map.extend(entries.iter().map(|(k, v)| (k, v)))
        .await
        .unwrap();
    let keys: Vec<i32> = (0..10_000).collect();
    let refs: Vec<&i32> = keys.iter().collect();
    let found = map.get_many(&refs).await.unwrap();
    assert_eq!(found.len(), 10_000);
    assert!(found
        .iter()
        .enumerate()
        .all(|(i, v)| v.as_deref() == Some(i.to_string().as_str())));
}

#[tokio::test]
async fn test_size() {
    let map = simple().await;
    map.insert(&key("1"), &value("2")).await.unwrap();
    map.insert(&key("3"), &value("4")).await.unwrap();
    map.insert(&key("5"), &value("6")).await.unwrap();
    assert_eq!(map.len().await.unwrap(), 3);
    map.insert(&key("1"), &value("2")).await.unwrap();
    map.insert(&key("3"), &value("4")).await.unwrap();
    assert_eq!(map.len().await.unwrap(), 3);
    map.insert(&key("1"), &value("21")).await.unwrap();
    map.insert(&key("3"), &value("41")).await.unwrap();
    assert_eq!(map.len().await.unwrap(), 3);
    map.insert(&key("51"), &value("6")).await.unwrap();
    assert_eq!(map.len().await.unwrap(), 4);
    map.remove(&key("3")).await.unwrap();
    assert_eq!(map.len().await.unwrap(), 3);
}

#[tokio::test]
async fn test_put_get() {
    let map = simple_three().await;
    assert_eq!(map.get(&key("33")).await.unwrap(), Some(value("44")));
    assert_eq!(map.get(&key("5")).await.unwrap(), Some(value("6")));
}

#[tokio::test]
async fn test_simple_types() {
    let map = client().await.hash_map::<i32, String>(unique("simple12"));
    map.insert(&1, "12").await.unwrap();
    map.insert(&2, "33").await.unwrap();
    map.insert(&3, "43").await.unwrap();
    assert_eq!(map.get(&2).await.unwrap().as_deref(), Some("33"));
}

#[tokio::test]
async fn test_integer() {
    let map = client().await.hash_map::<i32, i32>(unique("test_int"));
    map.insert(&1, &2).await.unwrap();
    map.insert(&3, &4).await.unwrap();
    assert_eq!(map.len().await.unwrap(), 2);
    assert_eq!(map.get(&1).await.unwrap(), Some(2));
    assert_eq!(map.get(&3).await.unwrap(), Some(4));
}

#[tokio::test]
async fn test_long() {
    let map = client().await.hash_map::<i64, i64>(unique("test_long"));
    map.insert(&1, &2).await.unwrap();
    map.insert(&3, &4).await.unwrap();
    assert_eq!(map.len().await.unwrap(), 2);
    assert_eq!(map.get(&1).await.unwrap(), Some(2));
    assert_eq!(map.get(&3).await.unwrap(), Some(4));
}

#[tokio::test]
async fn test_put_all() {
    let map = client().await.hash_map::<i32, String>(unique("simple"));
    map.insert(&1, "1").await.unwrap();
    map.insert(&2, "2").await.unwrap();
    map.insert(&3, "3").await.unwrap();
    map.extend([(&4, "4"), (&5, "5"), (&6, "6")]).await.unwrap();
    let mut keys = map.read_all_keys().await.unwrap();
    keys.sort();
    assert_eq!(keys, vec![1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn test_put_all_big() {
    let map = client().await.hash_map::<i32, String>(unique("simple"));
    let entries: Vec<(i32, String)> = (0..100_000).map(|i| (i, i.to_string())).collect();
    map.extend(entries.iter().map(|(k, v)| (k, v)))
        .await
        .unwrap();
    assert_eq!(map.len().await.unwrap(), 100_000);
}

#[tokio::test]
async fn test_random_keys() {
    let map = client().await.hash_map::<i32, i32>(unique("map"));
    assert!(map.random_keys(1).await.unwrap().is_empty());
    map.extend([(&1, &11), (&2, &21), (&3, &31), (&4, &41)])
        .await
        .unwrap();
    let keys = map.random_keys(2).await.unwrap();
    assert_eq!(keys.len(), 2);
    assert!(keys.iter().all(|k| (1..=4).contains(k)));
    assert_ne!(keys[0], keys[1]);
}

#[tokio::test]
async fn test_read_all_values() {
    let map = simple_three().await;
    let mut values = map.read_all_values().await.unwrap();
    values.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(values, vec![value("2"), value("44"), value("6")]);
}

#[tokio::test]
async fn test_read_all_key_set() {
    let map = simple_three().await;
    let mut keys = map.read_all_keys().await.unwrap();
    keys.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(keys, vec![key("1"), key("33"), key("5")]);
}

#[tokio::test]
async fn test_read_all_key_set_high_amount() {
    let map = simple().await;
    let entries: Vec<(SimpleKey, SimpleValue)> = (0..1000)
        .map(|i| (key(&i.to_string()), value(&i.to_string())))
        .collect();
    map.extend(entries.iter().map(|(k, v)| (k, v)))
        .await
        .unwrap();
    assert_eq!(map.read_all_keys().await.unwrap().len(), 1000);
}

#[tokio::test]
async fn test_read_all_entry_set() {
    let map = client().await.hash_map::<i32, String>(unique("simple12"));
    map.insert(&1, "12").await.unwrap();
    map.insert(&2, "33").await.unwrap();
    map.insert(&3, "43").await.unwrap();
    let entries: HashMap<i32, String> = map.read_all_entries().await.unwrap().into_iter().collect();
    let expected: HashMap<i32, String> = [(1, "12"), (2, "33"), (3, "43")]
        .into_iter()
        .map(|(k, v)| (k, v.to_string()))
        .collect();
    assert_eq!(entries, expected);
}

#[tokio::test]
async fn test_entry_set() {
    let map = client().await.hash_map::<i32, String>(unique("simple12"));
    map.insert(&1, "12").await.unwrap();
    map.insert(&2, "33").await.unwrap();
    map.insert(&3, "43").await.unwrap();
    let entries: HashMap<i32, String> = map.iter().try_collect().await.unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries.get(&2).map(String::as_str), Some("33"));
}

#[tokio::test]
async fn test_key_set() {
    let map = simple_three().await;
    let mut keys: Vec<SimpleKey> = map.keys().try_collect().await.unwrap();
    keys.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(keys, vec![key("1"), key("33"), key("5")]);
}

#[tokio::test]
async fn test_key_iterator() {
    let map = client().await.hash_map::<i32, i32>(unique("simple"));
    map.extend([(&1, &0), (&3, &5), (&4, &6), (&7, &8)])
        .await
        .unwrap();
    let mut keys: Vec<i32> = map.keys().try_collect().await.unwrap();
    keys.sort();
    assert_eq!(keys, vec![1, 3, 4, 7]);
}

#[tokio::test]
async fn test_value_iterator() {
    let map = client().await.hash_map::<i32, i32>(unique("simple"));
    map.extend([(&1, &0), (&3, &5), (&4, &6), (&7, &8)])
        .await
        .unwrap();
    let mut values: Vec<i32> = map.values().try_collect().await.unwrap();
    values.sort();
    assert_eq!(values, vec![0, 5, 6, 8]);
}

#[tokio::test]
async fn test_iterator() {
    let map = client().await.hash_map::<i32, i32>(unique("123"));
    let entries: Vec<(i32, i32)> = (0..1000).map(|i| (i, i)).collect();
    map.extend(entries.iter().map(|(k, v)| (k, v)))
        .await
        .unwrap();
    assert_eq!(map.len().await.unwrap(), 1000);
    let keys: Vec<i32> = map.keys().try_collect().await.unwrap();
    assert_eq!(keys.len(), 1000);
    let values: Vec<i32> = map.values().try_collect().await.unwrap();
    assert_eq!(values.len(), 1000);
    let all: Vec<(i32, i32)> = map.iter().try_collect().await.unwrap();
    assert_eq!(all.len(), 1000);
}

#[tokio::test]
async fn test_ordering() {
    let map = client().await.hash_map::<String, String>(unique("123"));
    let entries = [
        ("name", "123"),
        ("ip", "4124"),
        ("rank", "none"),
        ("tokens", "0"),
        ("coins", "0"),
        ("ar_score", "0"),
        ("ar_gameswon", "0"),
        ("ar_gameslost", "0"),
        ("ar_kills", "0"),
        ("ar_deaths", "0"),
    ];
    for (k, v) in entries {
        map.insert(k, v).await.unwrap();
    }
    let keys: Vec<String> = entries.iter().map(|(k, _)| k.to_string()).collect();
    let values: Vec<String> = entries.iter().map(|(_, v)| v.to_string()).collect();
    let pairs: Vec<(String, String)> = entries
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    assert_eq!(map.keys().try_collect::<Vec<_>>().await.unwrap(), keys);
    assert_eq!(map.read_all_keys().await.unwrap(), keys);
    assert_eq!(map.values().try_collect::<Vec<_>>().await.unwrap(), values);
    assert_eq!(map.read_all_values().await.unwrap(), values);
    assert_eq!(map.iter().try_collect::<Vec<_>>().await.unwrap(), pairs);
    let mut all = map.read_all_entries().await.unwrap();
    let mut sorted = pairs.clone();
    all.sort();
    sorted.sort();
    assert_eq!(all, sorted);
}
