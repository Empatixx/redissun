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
async fn try_insert_only_inserts_missing_keys() {
    let map = client().await.hash_map::<String, i64>(unique("map"));
    assert!(map.try_insert("a", &1i64).await.unwrap().is_none());
    assert!(!map.try_insert("a", &2i64).await.unwrap().is_none());
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

// Redisson: testPutIfAbsent
#[tokio::test]
async fn try_insert_returns_the_existing_value_and_keeps_it() {
    let map = simple().await;
    map.insert(&key("1"), &value("2")).await.unwrap();
    assert_eq!(
        map.try_insert(&key("1"), &value("3")).await.unwrap(),
        Some(value("2"))
    );
    assert_eq!(map.get(&key("1")).await.unwrap(), Some(value("2")));
    assert_eq!(map.try_insert(&key("2"), &value("4")).await.unwrap(), None);
    assert_eq!(map.get(&key("2")).await.unwrap(), Some(value("4")));
}

// Redisson: testFastPutIfAbsent
#[tokio::test]
async fn try_insert_does_not_overwrite_an_existing_key() {
    let map = simple().await;
    map.insert(&key("1"), &value("2")).await.unwrap();
    assert!(!map
        .try_insert(&key("1"), &value("3"))
        .await
        .unwrap()
        .is_none());
    assert_eq!(map.get(&key("1")).await.unwrap(), Some(value("2")));
    assert!(map
        .try_insert(&key("2"), &value("4"))
        .await
        .unwrap()
        .is_none());
    assert_eq!(map.get(&key("2")).await.unwrap(), Some(value("4")));
}

// Redisson: testFastPutIfExists
#[tokio::test]
async fn replace_updates_only_an_existing_key() {
    let map = simple().await;
    assert!(!map.replace(&key("1"), &value("3")).await.unwrap().is_some());
    assert_eq!(map.get(&key("1")).await.unwrap(), None);
    map.insert(&key("1"), &value("2")).await.unwrap();
    assert!(map.replace(&key("1"), &value("3")).await.unwrap().is_some());
    assert_eq!(map.get(&key("1")).await.unwrap(), Some(value("3")));
}

// Redisson: testPutIfExists
#[tokio::test]
async fn replace_returns_the_previous_value_of_an_existing_key() {
    let map = simple().await;
    assert_eq!(map.replace(&key("1"), &value("3")).await.unwrap(), None);
    assert_eq!(map.get(&key("1")).await.unwrap(), None);
    map.insert(&key("1"), &value("2")).await.unwrap();
    assert_eq!(
        map.replace(&key("1"), &value("3")).await.unwrap(),
        Some(value("2"))
    );
    assert_eq!(map.get(&key("1")).await.unwrap(), Some(value("3")));
}

// Redisson: testDeserializationErrorReturnsErrorImmediately
#[tokio::test]
async fn a_value_of_another_type_fails_with_a_codec_error() {
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

// Redisson: testReplaceOldValueFail
#[tokio::test]
async fn compare_and_set_keeps_the_value_when_the_old_value_differs() {
    let map = simple().await;
    map.insert(&key("1"), &value("2")).await.unwrap();
    assert!(!map
        .compare_and_set(&key("1"), &value("43"), &value("31"))
        .await
        .unwrap());
    assert_eq!(map.get(&key("1")).await.unwrap(), Some(value("2")));
}

// Redisson: testReplaceOldValueSuccess
#[tokio::test]
async fn compare_and_set_swaps_the_value_when_the_old_value_matches() {
    let map = simple().await;
    map.insert(&key("1"), &value("2")).await.unwrap();
    assert!(map
        .compare_and_set(&key("1"), &value("2"), &value("3"))
        .await
        .unwrap());
    assert!(!map
        .compare_and_set(&key("1"), &value("2"), &value("3"))
        .await
        .unwrap());
    assert_eq!(map.get(&key("1")).await.unwrap(), Some(value("3")));
}

// Redisson: testReplaceValue
#[tokio::test]
async fn replace_returns_the_previous_value_and_skips_missing_keys() {
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

// Redisson: testReplace
#[tokio::test]
async fn insert_overwrites_the_value_of_an_existing_key() {
    let map = simple_three().await;
    assert_eq!(map.get(&key("33")).await.unwrap(), Some(value("44")));
    map.insert(&key("33"), &value("abc")).await.unwrap();
    assert_eq!(map.get(&key("33")).await.unwrap(), Some(value("abc")));
}

// Redisson: testContainsValue
#[tokio::test]
async fn contains_value_finds_only_stored_values() {
    let map = simple_three().await;
    assert!(map.contains_value(&value("2")).await.unwrap());
    assert!(!map.contains_value(&value("441")).await.unwrap());
    assert!(!map.contains_value(&value("5")).await.unwrap());
}

// Redisson: testContainsKey
#[tokio::test]
async fn contains_key_finds_only_stored_keys() {
    let map = simple_three().await;
    assert!(map.contains_key(&key("33")).await.unwrap());
    assert!(!map.contains_key(&key("34")).await.unwrap());
}

// Redisson: testRemoveValueFail
#[tokio::test]
async fn remove_if_eq_keeps_the_entry_when_key_or_value_differs() {
    let map = simple().await;
    map.insert(&key("1"), &value("2")).await.unwrap();
    assert!(!map.remove_if_eq(&key("2"), &value("1")).await.unwrap());
    assert!(!map.remove_if_eq(&key("1"), &value("3")).await.unwrap());
    assert_eq!(map.get(&key("1")).await.unwrap(), Some(value("2")));
}

// Redisson: testRemoveValue
#[tokio::test]
async fn remove_if_eq_deletes_the_entry_when_the_value_matches() {
    let map = simple().await;
    map.insert(&key("1"), &value("2")).await.unwrap();
    assert!(map.remove_if_eq(&key("1"), &value("2")).await.unwrap());
    assert_eq!(map.get(&key("1")).await.unwrap(), None);
    assert_eq!(map.len().await.unwrap(), 0);
}

// Redisson: testRemoveObject
#[tokio::test]
async fn remove_returns_the_value_of_a_structured_key() {
    let map = simple_three().await;
    assert_eq!(map.remove(&key("33")).await.unwrap(), Some(value("44")));
    assert_eq!(map.remove(&key("5")).await.unwrap(), Some(value("6")));
    assert_eq!(map.remove(&key("11")).await.unwrap(), None);
    assert_eq!(map.len().await.unwrap(), 1);
}

// Redisson: testRemove
#[tokio::test]
async fn remove_returns_the_value_or_none_for_a_missing_key() {
    let map = client().await.hash_map::<i32, i32>(unique("simple"));
    map.insert(&1, &3).await.unwrap();
    map.insert(&3, &5).await.unwrap();
    map.insert(&7, &8).await.unwrap();
    assert_eq!(map.remove(&1).await.unwrap(), Some(3));
    assert_eq!(map.remove(&3).await.unwrap(), Some(5));
    assert_eq!(map.remove(&10).await.unwrap(), None);
    assert_eq!(map.remove(&7).await.unwrap(), Some(8));
}

// Redisson: testFastRemove
#[tokio::test]
async fn remove_many_returns_the_number_of_removed_keys() {
    let map = client().await.hash_map::<i32, i32>(unique("simple"));
    map.extend([(&1, &3), (&3, &5), (&4, &6), (&7, &8)])
        .await
        .unwrap();
    assert_eq!(map.remove_many(&[&1, &3, &7]).await.unwrap(), 3);
    assert_eq!(map.len().await.unwrap(), 1);
}

// Redisson: testFastRemoveEmpty
#[tokio::test]
async fn remove_many_without_keys_removes_nothing() {
    let map = client().await.hash_map::<i32, i32>(unique("simple"));
    map.insert(&1, &3).await.unwrap();
    assert_eq!(map.remove_many::<i32>(&[]).await.unwrap(), 0);
    assert_eq!(map.len().await.unwrap(), 1);
}

// Redisson: testFastPut
#[tokio::test]
async fn set_reports_whether_the_key_was_new() {
    let map = client().await.hash_map::<i32, i32>(unique("simple"));
    assert!(map.set(&1, &2).await.unwrap());
    assert_eq!(map.get(&1).await.unwrap(), Some(2));
    assert!(!map.set(&1, &3).await.unwrap());
    assert_eq!(map.get(&1).await.unwrap(), Some(3));
    assert_eq!(map.len().await.unwrap(), 1);
}

// Redisson: testFastReplace
#[tokio::test]
async fn replace_reports_whether_it_updated_only_an_existing_key() {
    let map = client().await.hash_map::<i32, i32>(unique("simple"));
    map.insert(&1, &2).await.unwrap();
    assert!(map.replace(&1, &3).await.unwrap().is_some());
    assert!(!map.replace(&2, &0).await.unwrap().is_some());
    assert_eq!(map.len().await.unwrap(), 1);
    assert_eq!(map.get(&1).await.unwrap(), Some(3));
}

// Redisson: testEmptyRemove
#[tokio::test]
async fn remove_if_eq_on_a_missing_key_returns_false() {
    let map = client().await.hash_map::<i32, i32>(unique("simple"));
    assert!(!map.remove_if_eq(&1, &3).await.unwrap());
    map.insert(&4, &5).await.unwrap();
    assert!(map.remove_if_eq(&4, &5).await.unwrap());
}

// Redisson: testValueSize
#[tokio::test]
async fn value_len_returns_the_encoded_length_or_zero() {
    let map = client().await.hash_map::<String, String>(unique("getAll"));
    map.insert("1", "1234").await.unwrap();
    assert_eq!(map.value_len("4").await.unwrap(), 0);
    assert_eq!(map.value_len("1").await.unwrap(), 6);
}

// Redisson: testAddAndGet
#[tokio::test]
async fn incr_by_and_incr_by_float_add_to_the_stored_number() {
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

// Redisson: testGetAll
#[tokio::test]
async fn get_many_returns_none_for_missing_keys() {
    let map = client().await.hash_map::<i32, i32>(unique("getAll"));
    map.extend([(&1, &100), (&2, &200), (&3, &300), (&4, &400)])
        .await
        .unwrap();
    assert_eq!(
        map.get_many(&[&2, &3, &5]).await.unwrap(),
        vec![Some(200), Some(300), None]
    );
}

// Redisson: testGetAllWithStringKeys
#[tokio::test]
async fn get_many_with_string_keys_returns_none_for_missing_keys() {
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

// Redisson: testGetAllBig
#[tokio::test]
async fn get_many_reads_ten_thousand_keys_in_order() {
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

// Redisson: testSize
#[tokio::test]
async fn len_counts_each_key_once_after_overwrites() {
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

// Redisson: testPutGet
#[tokio::test]
async fn get_returns_the_inserted_value() {
    let map = simple_three().await;
    assert_eq!(map.get(&key("33")).await.unwrap(), Some(value("44")));
    assert_eq!(map.get(&key("5")).await.unwrap(), Some(value("6")));
}

// Redisson: testSimpleTypes
#[tokio::test]
async fn integer_keys_map_to_string_values() {
    let map = client().await.hash_map::<i32, String>(unique("simple12"));
    map.insert(&1, "12").await.unwrap();
    map.insert(&2, "33").await.unwrap();
    map.insert(&3, "43").await.unwrap();
    assert_eq!(map.get(&2).await.unwrap().as_deref(), Some("33"));
}

// Redisson: testInteger
#[tokio::test]
async fn i32_keys_and_values_round_trip() {
    let map = client().await.hash_map::<i32, i32>(unique("test_int"));
    map.insert(&1, &2).await.unwrap();
    map.insert(&3, &4).await.unwrap();
    assert_eq!(map.len().await.unwrap(), 2);
    assert_eq!(map.get(&1).await.unwrap(), Some(2));
    assert_eq!(map.get(&3).await.unwrap(), Some(4));
}

// Redisson: testLong
#[tokio::test]
async fn i64_keys_and_values_round_trip() {
    let map = client().await.hash_map::<i64, i64>(unique("test_long"));
    map.insert(&1, &2).await.unwrap();
    map.insert(&3, &4).await.unwrap();
    assert_eq!(map.len().await.unwrap(), 2);
    assert_eq!(map.get(&1).await.unwrap(), Some(2));
    assert_eq!(map.get(&3).await.unwrap(), Some(4));
}

// Redisson: testPutAll
#[tokio::test]
async fn extend_adds_entries_to_existing_ones() {
    let map = client().await.hash_map::<i32, String>(unique("simple"));
    map.insert(&1, "1").await.unwrap();
    map.insert(&2, "2").await.unwrap();
    map.insert(&3, "3").await.unwrap();
    map.extend([(&4, "4"), (&5, "5"), (&6, "6")]).await.unwrap();
    let mut keys = map.keys().try_collect::<Vec<_>>().await.unwrap();
    keys.sort();
    assert_eq!(keys, vec![1, 2, 3, 4, 5, 6]);
}

// Redisson: testPutAllBig
#[tokio::test]
async fn extend_inserts_a_hundred_thousand_entries() {
    let map = client().await.hash_map::<i32, String>(unique("simple"));
    let entries: Vec<(i32, String)> = (0..100_000).map(|i| (i, i.to_string())).collect();
    map.extend(entries.iter().map(|(k, v)| (k, v)))
        .await
        .unwrap();
    assert_eq!(map.len().await.unwrap(), 100_000);
}

// Redisson: testRandomKeys
#[tokio::test]
async fn random_keys_returns_distinct_stored_keys() {
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

// Redisson: testReadAllValues
#[tokio::test]
async fn values_returns_every_value() {
    let map = simple_three().await;
    let mut values = map.values().try_collect::<Vec<_>>().await.unwrap();
    values.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(values, vec![value("2"), value("44"), value("6")]);
}

// Redisson: testReadAllKeySet
#[tokio::test]
async fn keys_returns_every_key() {
    let map = simple_three().await;
    let mut keys = map.keys().try_collect::<Vec<_>>().await.unwrap();
    keys.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(keys, vec![key("1"), key("33"), key("5")]);
}

// Redisson: testReadAllKeySetHighAmount
#[tokio::test]
async fn keys_returns_a_thousand_keys() {
    let map = simple().await;
    let entries: Vec<(SimpleKey, SimpleValue)> = (0..1000)
        .map(|i| (key(&i.to_string()), value(&i.to_string())))
        .collect();
    map.extend(entries.iter().map(|(k, v)| (k, v)))
        .await
        .unwrap();
    assert_eq!(
        map.keys().try_collect::<Vec<_>>().await.unwrap().len(),
        1000
    );
}

// Redisson: testReadAllEntrySet
#[tokio::test]
async fn iter_returns_every_entry() {
    let map = client().await.hash_map::<i32, String>(unique("simple12"));
    map.insert(&1, "12").await.unwrap();
    map.insert(&2, "33").await.unwrap();
    map.insert(&3, "43").await.unwrap();
    let entries: HashMap<i32, String> = map
        .iter()
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .collect();
    let expected: HashMap<i32, String> = [(1, "12"), (2, "33"), (3, "43")]
        .into_iter()
        .map(|(k, v)| (k, v.to_string()))
        .collect();
    assert_eq!(entries, expected);
}

// Redisson: testEntrySet
#[tokio::test]
async fn iter_yields_every_entry() {
    let map = client().await.hash_map::<i32, String>(unique("simple12"));
    map.insert(&1, "12").await.unwrap();
    map.insert(&2, "33").await.unwrap();
    map.insert(&3, "43").await.unwrap();
    let entries: HashMap<i32, String> = map.iter().try_collect().await.unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries.get(&2).map(String::as_str), Some("33"));
}

// Redisson: testKeySet
#[tokio::test]
async fn keys_yields_every_structured_key() {
    let map = simple_three().await;
    let mut keys: Vec<SimpleKey> = map.keys().try_collect().await.unwrap();
    keys.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(keys, vec![key("1"), key("33"), key("5")]);
}

// Redisson: testKeyIterator
#[tokio::test]
async fn keys_yields_every_integer_key() {
    let map = client().await.hash_map::<i32, i32>(unique("simple"));
    map.extend([(&1, &0), (&3, &5), (&4, &6), (&7, &8)])
        .await
        .unwrap();
    let mut keys: Vec<i32> = map.keys().try_collect().await.unwrap();
    keys.sort();
    assert_eq!(keys, vec![1, 3, 4, 7]);
}

// Redisson: testValueIterator
#[tokio::test]
async fn values_yields_every_value() {
    let map = client().await.hash_map::<i32, i32>(unique("simple"));
    map.extend([(&1, &0), (&3, &5), (&4, &6), (&7, &8)])
        .await
        .unwrap();
    let mut values: Vec<i32> = map.values().try_collect().await.unwrap();
    values.sort();
    assert_eq!(values, vec![0, 5, 6, 8]);
}

// Redisson: testIterator
#[tokio::test]
async fn keys_values_and_iter_each_yield_a_thousand_items() {
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

// Redisson: testOrdering
#[tokio::test]
async fn small_map_reads_keep_the_insertion_order() {
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
    assert_eq!(map.keys().try_collect::<Vec<_>>().await.unwrap(), keys);
    assert_eq!(map.values().try_collect::<Vec<_>>().await.unwrap(), values);
    assert_eq!(map.values().try_collect::<Vec<_>>().await.unwrap(), values);
    assert_eq!(map.iter().try_collect::<Vec<_>>().await.unwrap(), pairs);
    let mut all = map.iter().try_collect::<Vec<_>>().await.unwrap();
    let mut sorted = pairs.clone();
    all.sort();
    sorted.sort();
    assert_eq!(all, sorted);
}

// Redisson: RMap.getLock(key), named by RedissonObject.getLockByMapKey
#[tokio::test]
async fn per_key_lock_lives_under_the_redisson_name() {
    let name = unique("map");
    let map = client().await.hash_map::<String, i64>(name.clone());
    let lock_key = format!("{{{name}}}:IKm9EBQhlzpVIBxUXDHBWw:lock");
    let guard = map.lock("key").unwrap().lock().await.unwrap();
    assert_eq!(
        crate::common::raw_command(&["EXISTS", &lock_key]).await,
        ":1\r\n"
    );
    guard.unlock().await.unwrap();
    assert_eq!(
        crate::common::raw_command(&["EXISTS", &lock_key]).await,
        ":0\r\n"
    );
}

#[tokio::test]
async fn per_key_semaphore_and_latch_work() {
    let map = client().await.hash_map::<String, i64>(unique("map"));
    let semaphore = map.semaphore("k").unwrap();
    assert!(semaphore.init_permits(2).await.unwrap());
    let permits = semaphore.acquire().await.unwrap();
    assert_eq!(semaphore.available_permits().await.unwrap(), 1);
    permits.release().await.unwrap();
    let latch = map.count_down_latch("k").unwrap();
    assert!(latch.init_count(1).await.unwrap());
    latch.count_down().await.unwrap();
    assert_eq!(latch.count().await.unwrap(), 0);
    let _ = map.fair_lock("k").unwrap();
    let _ = map.rw_lock("k").unwrap();
    let _ = map.permit_expirable_semaphore("k").unwrap();
}

#[tokio::test]
async fn entry_inserts_updates_and_removes() {
    use redissun::hash_map::Entry;
    let map = client().await.hash_map::<String, i64>(unique("map"));
    assert_eq!(map.entry("k").await.unwrap().or_insert(1).await.unwrap(), 1);
    assert_eq!(
        map.entry("k")
            .await
            .unwrap()
            .and_modify(|v| *v += 1)
            .or_insert(1)
            .await
            .unwrap(),
        2
    );
    assert_eq!(map.get("k").await.unwrap(), Some(2));
    let Entry::Occupied(entry) = map.entry("k").await.unwrap() else {
        panic!("k is present");
    };
    assert_eq!(*entry.get(), 2);
    assert_eq!(entry.remove().await.unwrap(), 2);
    assert!(!map.contains_key("k").await.unwrap());
}

#[tokio::test]
async fn a_dropped_entry_writes_nothing() {
    let map = client().await.hash_map::<String, i64>(unique("map"));
    let entry = map.entry("k").await.unwrap();
    assert!(matches!(entry, redissun::hash_map::Entry::Vacant(_)));
    drop(entry);
    assert_eq!(map.len().await.unwrap(), 0);
}

#[tokio::test]
async fn or_insert_with_keeps_the_present_value() {
    let map = client().await.hash_map::<String, i64>(unique("map"));
    map.insert("a", &1).await.unwrap();
    let called = std::cell::Cell::new(false);
    let value = map
        .entry("a")
        .await
        .unwrap()
        .or_insert_with(|| {
            called.set(true);
            5
        })
        .await
        .unwrap();
    assert_eq!(value, 1);
    assert!(!called.get());
    assert_eq!(map.entry("b").await.unwrap().or_default().await.unwrap(), 0);
    assert_eq!(map.get("b").await.unwrap(), Some(0));
}

#[tokio::test]
async fn and_modify_leaves_a_vacant_key_to_or_insert() {
    let map = client().await.hash_map::<String, i64>(unique("map"));
    let value = map
        .entry("a")
        .await
        .unwrap()
        .and_modify(|v| *v += 100)
        .or_insert(7)
        .await
        .unwrap();
    assert_eq!(value, 7);
}

#[tokio::test]
async fn occupied_insert_returns_the_old_value() {
    use redissun::hash_map::Entry;
    let map = client().await.hash_map::<String, i64>(unique("map"));
    map.insert("a", &1).await.unwrap();
    match map.entry("a").await.unwrap() {
        Entry::Occupied(entry) => assert_eq!(entry.insert(5).await.unwrap(), 1),
        Entry::Vacant(_) => panic!("a is present"),
    }
    assert_eq!(map.get("a").await.unwrap(), Some(5));
    match map.entry("b").await.unwrap() {
        Entry::Vacant(entry) => assert_eq!(entry.insert(3).await.unwrap(), 3),
        Entry::Occupied(_) => panic!("b is missing"),
    }
    assert_eq!(map.get("b").await.unwrap(), Some(3));
}

#[tokio::test]
async fn entry_is_atomic_across_tasks() {
    let map = client().await.hash_map::<String, i64>(unique("map"));
    let tasks: Vec<_> = (0..20)
        .map(|_| {
            let map = map.clone();
            tokio::spawn(async move {
                let entry = map.entry("k").await.unwrap();
                tokio::task::yield_now().await;
                entry.and_modify(|v| *v += 1).or_insert(1).await.unwrap();
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(map.get("k").await.unwrap(), Some(20));
}

#[tokio::test]
async fn a_dropped_entry_releases_the_lock() {
    let map = client().await.hash_map::<String, i64>(unique("map"));
    drop(map.entry("k").await.unwrap());
    let lock = map.lock("k").unwrap();
    for _ in 0..40 {
        if !lock.is_locked().await.unwrap() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("the per-key lock stayed held after the entry was dropped");
}

async fn thousand_keys() -> redissun::HashMap<String, i64, redissun::JsonCodec> {
    let map = client().await.hash_map::<String, i64>(unique("map"));
    let keys: Vec<String> = (0..1000).map(|i| format!("k{i}")).collect();
    let values: Vec<i64> = (0..1000).collect();
    map.extend(keys.iter().map(String::as_str).zip(values.iter()))
        .await
        .unwrap();
    map
}

#[tokio::test]
async fn scan_with_pattern_reads_every_page() {
    let map = thousand_keys().await;
    let found: std::collections::HashSet<String> = map
        .keys_with(redissun::MapScan::new().pattern("\"k1*").page_size(10))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(found.len(), 111);
    let values: Vec<i64> = map
        .values_with(redissun::MapScan::new().pattern("\"k99*"))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(values.len(), 11);
    let entries: Vec<(String, i64)> = map
        .iter_with(redissun::MapScan::new().page_size(7))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(entries.len(), 1000);
}

#[tokio::test]
async fn scan_pattern_without_matches_is_empty() {
    let map = thousand_keys().await;
    let found: Vec<String> = map
        .keys_with(redissun::MapScan::new().pattern("nothing*"))
        .try_collect()
        .await
        .unwrap();
    assert!(found.is_empty());
}

#[tokio::test]
async fn to_hash_map_reads_the_whole_map() {
    let map = client().await.hash_map::<String, i64>(unique("map"));
    map.insert("a", &1).await.unwrap();
    map.insert("b", &2).await.unwrap();
    assert_eq!(
        map.to_hash_map().await.unwrap(),
        HashMap::from([("a".to_string(), 1), ("b".to_string(), 2)])
    );
}

#[tokio::test]
async fn random_entries_returns_existing_pairs() {
    let map = client().await.hash_map::<String, i64>(unique("map"));
    for (k, v) in [("a", 1), ("b", 2), ("c", 3)] {
        map.insert(k, &v).await.unwrap();
    }
    let picked = map.random_entries(2).await.unwrap();
    assert_eq!(picked.len(), 2);
    for (k, v) in picked {
        assert_eq!(map.get(&k).await.unwrap(), Some(v));
    }
}

// Redisson: putAll(map, batchSize)
#[tokio::test]
async fn extend_in_batches_writes_everything() {
    let map = client().await.hash_map::<String, i64>(unique("map"));
    let keys: Vec<String> = (0..250).map(|i| format!("k{i}")).collect();
    let values: Vec<i64> = (0..250).collect();
    map.extend(keys.iter().map(String::as_str).zip(values.iter()))
        .batch_size(100)
        .await
        .unwrap();
    assert_eq!(map.len().await.unwrap(), 250);
    assert_eq!(map.get("k249").await.unwrap(), Some(249));
}

#[tokio::test]
async fn extend_batch_of_zero_is_one_call() {
    let map = client().await.hash_map::<String, i64>(unique("map"));
    map.extend([("a", &1i64), ("b", &2i64)])
        .batch_size(0)
        .await
        .unwrap();
    assert_eq!(map.len().await.unwrap(), 2);
}

async fn events_client(url: &str, flags: &str) -> redissun::Client {
    crate::common::raw_command_at(url, &["CONFIG", "SET", "notify-keyspace-events", flags]).await;
    redissun::Client::builder()
        .url(url.to_string())
        .build()
        .await
        .unwrap()
}

async fn next_event(
    events: &mut redissun::MapEvents<String, redissun::JsonCodec>,
) -> redissun::MapEvent<String> {
    tokio::time::timeout(std::time::Duration::from_secs(2), events.recv())
        .await
        .expect("no event within 2 s")
        .unwrap()
}

// Redisson: MapPutListener, MapRemoveListener, DeletedObjectListener on a server without subkey notifications
#[tokio::test]
async fn events_without_subkey_notifications_have_no_key() {
    let (_container, url) = crate::common::own_redis().await;
    let client = events_client(&url, "Egxh").await;
    let map = client.hash_map::<String, i64>(unique("map"));
    let mut events = map.events().await.unwrap();
    map.insert("a", &1).await.unwrap();
    assert!(matches!(
        next_event(&mut events).await,
        redissun::MapEvent::Put { key: None }
    ));
    // Removing the last entry deletes the hash, so Redis also sends `del`; events of different kinds may come in either order.
    map.remove("a").await.unwrap();
    let both = [next_event(&mut events).await, next_event(&mut events).await];
    assert!(both.contains(&redissun::MapEvent::Removed { key: None }));
    assert!(both.contains(&redissun::MapEvent::Deleted));
    map.insert("b", &1).await.unwrap();
    assert!(matches!(
        next_event(&mut events).await,
        redissun::MapEvent::Put { key: None }
    ));
    map.del().await.unwrap();
    assert!(matches!(
        next_event(&mut events).await,
        redissun::MapEvent::Deleted
    ));
}

#[tokio::test]
async fn events_with_subkey_notifications_name_the_key() {
    let (_container, url) = crate::common::own_redis8().await;
    let client = events_client(&url, "ETh").await;
    let map = client.hash_map::<String, f64>(unique("map"));
    let mut events = map.events().await.unwrap();
    map.insert("a", &1.0).await.unwrap();
    match next_event(&mut events).await {
        redissun::MapEvent::Put { key } => assert_eq!(key.as_deref(), Some("a")),
        other => panic!("expected a put, got {other:?}"),
    }
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), events.recv())
            .await
            .is_err(),
        "the key event of the same write came through as a duplicate"
    );
    map.incr_by_float("a", 1.5).await.unwrap();
    match next_event(&mut events).await {
        redissun::MapEvent::Incremented { key } => assert_eq!(key.as_deref(), Some("a")),
        other => panic!("expected an increment, got {other:?}"),
    }
}

#[tokio::test]
async fn events_of_filters_kinds() {
    let (_container, url) = crate::common::own_redis().await;
    let client = events_client(&url, "Egxh").await;
    let map = client.hash_map::<String, i64>(unique("map"));
    let mut events = map
        .events_of(&[redissun::MapEventKind::Removed])
        .await
        .unwrap();
    map.insert("a", &1).await.unwrap();
    map.remove("a").await.unwrap();
    assert!(matches!(
        next_event(&mut events).await,
        redissun::MapEvent::Removed { key: None }
    ));
}

// ---- Ported from Redisson 4.8.0 BaseMapTest / RedissonMapTest ----

async fn string_map(prefix: &str) -> redissun::HashMap<String, String, redissun::StringCodec> {
    client()
        .await
        .with_codec(redissun::StringCodec)
        .hash_map(unique(prefix))
}

// Redisson: testRandomEntries
#[tokio::test]
async fn redisson_random_entries() {
    let map = client().await.hash_map::<i32, i32>(unique("map"));
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
async fn redisson_compute_if_present() {
    use redissun::hash_map::Entry;
    let map = client().await.hash_map::<String, String>(unique("map"));
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
async fn redisson_compute_if_present_mutable() {
    let map = client().await.hash_map::<String, User>(unique("map"));
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
async fn redisson_compute_if_absent() {
    let map = client().await.hash_map::<String, String>(unique("map"));
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
async fn redisson_merge() {
    use redissun::hash_map::Entry;
    let map = client().await.hash_map::<String, String>(unique("map"));
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
async fn redisson_compute() {
    use redissun::hash_map::Entry;
    let map = client().await.hash_map::<String, String>(unique("map"));
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
async fn redisson_get_all() {
    let map = client().await.hash_map::<i32, i32>(unique("map"));
    for (k, v) in [(1, 100), (2, 200), (3, 300), (4, 400)] {
        map.insert(&k, &v).await.unwrap();
    }
    let found = map.get_many(&[&2, &3, &5]).await.unwrap();
    assert_eq!(found, [Some(200), Some(300), None]);
}

// Redisson: testGetAllBig
#[tokio::test]
async fn redisson_get_all_big() {
    let map = client().await.hash_map::<i32, String>(unique("map"));
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
async fn redisson_put_all_batched() {
    let map = client().await.hash_map::<i32, String>(unique("map"));
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
async fn redisson_scans_by_pattern() {
    let map = string_map("simple").await;
    for (k, v) in [("10", "100"), ("20", "200"), ("30", "300")] {
        map.insert(k, v).await.unwrap();
    }
    let keys = |pattern: &'static str| {
        let map = map.clone();
        async move {
            let mut keys: Vec<String> = map
                .keys_with(redissun::MapScan::new().pattern(pattern))
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
        .values_with(redissun::MapScan::new().pattern("?0"))
        .try_collect()
        .await
        .unwrap();
    values.sort();
    assert_eq!(values, ["100", "200", "300"]);
    let entries: Vec<(String, String)> = map
        .iter_with(redissun::MapScan::new().pattern("10"))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(entries, [("10".to_string(), "100".to_string())]);
}

// Redisson: testReadAllEntrySetWithPattern, testReadAllKeySetWithPattern, testReadAllValuesWithPattern
#[tokio::test]
async fn redisson_read_all_with_pattern() {
    let map = string_map("simple12").await;
    for (k, v) in [("10", "12"), ("12", "33"), ("21", "43")] {
        map.insert(k, v).await.unwrap();
    }
    let scan = || redissun::MapScan::new().pattern("1?");
    let entries: Vec<(String, String)> = map.iter_with(scan()).try_collect().await.unwrap();
    assert_eq!(entries.len(), 2);
    let mut keys: Vec<String> = map.keys_with(scan()).try_collect().await.unwrap();
    keys.sort();
    assert_eq!(keys, ["10", "12"]);
    let mut values: Vec<String> = map.values_with(scan()).try_collect().await.unwrap();
    values.sort();
    assert_eq!(values, ["12", "33"]);
}

async fn pattern_count(url: &str) -> String {
    crate::common::raw_command_at(url, &["PUBSUB", "NUMPAT"]).await
}

// Redisson: testIncrListener (notify-keyspace-events "Th": subkey events only)
#[tokio::test]
async fn redisson_incr_listener() {
    let (_container, url) = crate::common::own_redis8().await;
    let client = events_client(&url, "Th").await;
    let map = client.hash_map::<String, f64>(unique("testIncrListener"));
    let mut events = map
        .events_of(&[redissun::MapEventKind::Incremented])
        .await
        .unwrap();
    map.incr_by_float("1", 1.0).await.unwrap();
    match next_event(&mut events).await {
        redissun::MapEvent::Incremented { key } => assert_eq!(key.as_deref(), Some("1")),
        other => panic!("expected an increment, got {other:?}"),
    }
    // Redisson's removeListener: dropping the handle unsubscribes.
    drop(events);
    for _ in 0..20 {
        if pattern_count(&url).await == ":0\r\n" {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("the patterns stayed subscribed after the events were dropped");
}

// Redisson: testPutRemoveListener
#[tokio::test]
async fn redisson_put_remove_listener() {
    let (_container, url) = crate::common::own_redis8().await;
    let client = events_client(&url, "Th").await;
    let map = client
        .with_codec(redissun::StringCodec)
        .hash_map::<String, String>(unique("testPutRemoveListener"));
    let mut events = map.events().await.unwrap();
    map.insert("1", "2").await.unwrap();
    map.remove("1").await.unwrap();
    let mut seen = vec![];
    for _ in 0..2 {
        seen.push(
            tokio::time::timeout(std::time::Duration::from_secs(2), events.recv())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert!(seen.contains(&redissun::MapEvent::Put {
        key: Some("1".into())
    }));
    assert!(seen.contains(&redissun::MapEvent::Removed {
        key: Some("1".into())
    }));
}

// Redisson: testPutListenerWithKeyevent
#[tokio::test]
async fn redisson_put_listener_with_keyevent() {
    let (_container, url) = crate::common::own_redis().await;
    let client = events_client(&url, "Eh").await;
    let map = client.hash_map::<String, String>(unique("testPutListenerWithKeyevent"));
    let mut events = map.events_of(&[redissun::MapEventKind::Put]).await.unwrap();
    map.insert("1", "2").await.unwrap();
    assert_eq!(
        next_event(&mut events).await,
        redissun::MapEvent::Put { key: None }
    );
}

// Redisson: testPutListenerWithKeyeventAndSubkeyevent
#[tokio::test]
async fn redisson_put_listener_with_keyevent_and_subkeyevent() {
    let (_container, url) = crate::common::own_redis8().await;
    let client = events_client(&url, "TEh").await;
    let map =
        client.hash_map::<String, String>(unique("testPutListenerWithKeyeventAndSubkeyevent"));
    let mut events = map.events_of(&[redissun::MapEventKind::Put]).await.unwrap();
    map.insert("1", "2").await.unwrap();
    assert_eq!(
        next_event(&mut events).await,
        redissun::MapEvent::Put {
            key: Some("1".into())
        }
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(200), events.recv())
            .await
            .is_err()
    );
}

// Futures joined in one task share the task's lock owner; entries must still not lose updates.
#[tokio::test]
async fn entry_is_atomic_within_one_task() {
    let map = client().await.hash_map::<String, i64>(unique("map"));
    let increments = (0..20).map(|_| async {
        let entry = map.entry("k").await?;
        tokio::task::yield_now().await;
        entry.and_modify(|v| *v += 1).or_insert(1).await
    });
    for result in futures::future::join_all(increments).await {
        result.unwrap();
    }
    assert_eq!(map.get("k").await.unwrap(), Some(20));
}

// Redisson's lock is reentrant: an entry taken under the key's lock held by the same task must not wait for itself.
#[tokio::test]
async fn entry_inside_the_held_key_lock_does_not_deadlock() {
    let map = client().await.hash_map::<String, i64>(unique("map"));
    let guard = map.lock("k").unwrap().lock().await.unwrap();
    let value = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        map.entry("k").await?.or_insert(1).await
    })
    .await
    .expect("entry waited for the lock its own task holds")
    .unwrap();
    assert_eq!(value, 1);
    guard.unlock().await.unwrap();
}
