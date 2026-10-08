mod common;

use common::{client, unique};
use futures::TryStreamExt;
use redissun::{Error, Object};

async fn vec_of(values: &[&str]) -> redissun::Vec<String, redissun::JsonCodec> {
    let vec = client().await.vec::<String>(unique("vec"));
    vec.extend(values.iter().copied()).await.unwrap();
    vec
}

async fn contents(vec: &redissun::Vec<String, redissun::JsonCodec>) -> Vec<String> {
    vec.iter().try_collect().await.unwrap()
}

use contents as contents_of;

#[tokio::test]
async fn push_get_and_len() {
    let vec = client().await.vec::<String>(unique("vec"));
    assert!(vec.is_empty().await.unwrap());
    vec.push("a").await.unwrap();
    vec.push("b").await.unwrap();
    assert_eq!(vec.len().await.unwrap(), 2);
    assert_eq!(vec.get(0).await.unwrap(), Some("a".to_string()));
    assert_eq!(vec.get(1).await.unwrap(), Some("b".to_string()));
    assert_eq!(vec.get(2).await.unwrap(), None);
}

#[tokio::test]
async fn pop_takes_the_last_element() {
    let vec = vec_of(&["a", "b"]).await;
    assert_eq!(vec.pop().await.unwrap(), Some("b".to_string()));
    assert_eq!(vec.pop().await.unwrap(), Some("a".to_string()));
    assert_eq!(vec.pop().await.unwrap(), None);
}

#[tokio::test]
async fn set_replaces_an_element_and_rejects_a_missing_index() {
    let vec = vec_of(&["a", "b"]).await;
    vec.set(1, "z").await.unwrap();
    assert_eq!(contents(&vec).await, ["a", "z"]);
    assert!(matches!(vec.set(2, "x").await, Err(Error::OutOfRange)));
    let empty = client().await.vec::<String>(unique("vec"));
    assert!(matches!(empty.set(0, "x").await, Err(Error::OutOfRange)));
}

#[tokio::test]
async fn insert_at_the_start_middle_and_end() {
    let vec = vec_of(&["a", "c"]).await;
    vec.insert(1, "b").await.unwrap();
    vec.insert(0, "start").await.unwrap();
    vec.insert(4, "end").await.unwrap();
    assert_eq!(contents(&vec).await, ["start", "a", "b", "c", "end"]);
}

#[tokio::test]
async fn insert_with_duplicate_values_keeps_the_order() {
    let vec = vec_of(&["x", "x", "x"]).await;
    vec.insert(2, "y").await.unwrap();
    assert_eq!(contents(&vec).await, ["x", "x", "y", "x"]);
}

#[tokio::test]
async fn insert_past_the_end_is_out_of_range() {
    let vec = vec_of(&["a"]).await;
    assert!(matches!(vec.insert(2, "b").await, Err(Error::OutOfRange)));
    assert_eq!(contents(&vec).await, ["a"]);
}

#[tokio::test]
async fn insert_into_a_missing_list_at_zero_creates_it() {
    let vec = client().await.vec::<String>(unique("vec"));
    vec.insert(0, "a").await.unwrap();
    assert_eq!(contents(&vec).await, ["a"]);
}

#[tokio::test]
async fn remove_returns_the_element_and_keeps_duplicates() {
    let vec = vec_of(&["x", "y", "x"]).await;
    assert_eq!(vec.remove(2).await.unwrap(), Some("x".to_string()));
    assert_eq!(contents(&vec).await, ["x", "y"]);
    assert_eq!(vec.remove(0).await.unwrap(), Some("x".to_string()));
    assert_eq!(contents(&vec).await, ["y"]);
    assert_eq!(vec.remove(5).await.unwrap(), None);
}

#[tokio::test]
async fn range_uses_an_exclusive_end() {
    let vec = vec_of(&["a", "b", "c", "d"]).await;
    assert_eq!(vec.range(1..3).await.unwrap(), ["b", "c"]);
    assert_eq!(vec.range(2..2).await.unwrap(), Vec::<String>::new());
    assert_eq!(vec.range(0..10).await.unwrap().len(), 4);
}

#[tokio::test]
async fn trim_keeps_only_the_range() {
    let vec = vec_of(&["a", "b", "c", "d"]).await;
    vec.trim(1..3).await.unwrap();
    assert_eq!(contents(&vec).await, ["b", "c"]);
    vec.trim(0..0).await.unwrap();
    assert!(vec.is_empty().await.unwrap());
    assert!(!vec.exists().await.unwrap());
}

#[tokio::test]
async fn position_and_contains() {
    let vec = vec_of(&["a", "b", "a"]).await;
    assert_eq!(vec.position("a").await.unwrap(), Some(0));
    assert_eq!(vec.position("b").await.unwrap(), Some(1));
    assert_eq!(vec.position("z").await.unwrap(), None);
    assert!(vec.contains("b").await.unwrap());
    assert!(!vec.contains("z").await.unwrap());
}

#[tokio::test]
async fn remove_value_removes_the_first_match_and_remove_all_every_match() {
    let vec = vec_of(&["a", "b", "a", "a"]).await;
    assert!(vec.remove_value("a").await.unwrap());
    assert_eq!(contents(&vec).await, ["b", "a", "a"]);
    assert_eq!(vec.remove_all("a").await.unwrap(), 2);
    assert_eq!(contents(&vec).await, ["b"]);
    assert!(!vec.remove_value("z").await.unwrap());
}

#[tokio::test]
async fn extend_with_nothing_is_a_no_op() {
    let vec = client().await.vec::<String>(unique("vec"));
    vec.extend(std::iter::empty::<&str>()).await.unwrap();
    assert!(!vec.exists().await.unwrap());
}

#[tokio::test]
async fn iter_reads_more_than_one_page_in_order() {
    let vec = client().await.vec::<u32>(unique("vec"));
    let numbers: Vec<u32> = (0..250).collect();
    vec.extend(numbers.iter()).await.unwrap();
    let read: Vec<u32> = vec.iter().try_collect().await.unwrap();
    assert_eq!(read, numbers);
}

#[tokio::test]
async fn object_methods() {
    let client = client().await;
    let vec = client.vec::<String>(unique("vec"));
    vec.push("a").await.unwrap();
    assert!(vec.exists().await.unwrap());
    assert!(vec
        .expire(std::time::Duration::from_secs(60))
        .await
        .unwrap());
    assert!(vec.ttl().await.unwrap().is_some());
    assert!(vec.persist().await.unwrap());
    let renamed = unique("vec");
    vec.rename(&renamed).await.unwrap();
    let moved = client.vec::<String>(renamed);
    assert_eq!(contents_of(&moved).await, ["a"]);
    assert!(!vec.exists().await.unwrap());
}

#[tokio::test]
async fn clear_removes_the_key() {
    let vec = vec_of(&["a"]).await;
    vec.clear().await.unwrap();
    assert!(!vec.exists().await.unwrap());
}

#[tokio::test]
async fn concurrent_pushes_are_not_lost() {
    let vec = client().await.vec::<u32>(unique("vec"));
    let tasks: Vec<_> = (0..100u32)
        .map(|i| {
            let vec = vec.clone();
            tokio::spawn(async move { vec.push(&i).await.unwrap() })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
    let mut read: Vec<u32> = vec.iter().try_collect().await.unwrap();
    read.sort();
    assert_eq!(read, (0..100).collect::<Vec<_>>());
}
