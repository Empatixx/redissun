mod common;

use common::{client, unique};
use futures::TryStreamExt;
use redissun::Object;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct User {
    name: String,
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
