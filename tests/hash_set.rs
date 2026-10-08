mod common;

use common::{client, unique};
use futures::TryStreamExt;
use redissun::Object;
use std::collections::HashSet;

async fn set_of(values: &[&str]) -> redissun::HashSet<String, redissun::JsonCodec> {
    let set = client().await.hash_set::<String>(unique("set"));
    set.extend(values.iter().copied()).await.unwrap();
    set
}

fn sorted(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values
}

#[tokio::test]
async fn insert_and_remove_report_whether_the_set_changed() {
    let set = client().await.hash_set::<String>(unique("set"));
    assert!(set.insert("a").await.unwrap());
    assert!(!set.insert("a").await.unwrap());
    assert!(set.remove("a").await.unwrap());
    assert!(!set.remove("a").await.unwrap());
}

#[tokio::test]
async fn contains_and_contains_many() {
    let set = set_of(&["a", "b"]).await;
    assert!(set.contains("a").await.unwrap());
    assert!(!set.contains("z").await.unwrap());
    assert_eq!(
        set.contains_many(&["a", "z", "b"]).await.unwrap(),
        [true, false, true]
    );
    assert_eq!(
        set.contains_many::<str>(&[]).await.unwrap(),
        Vec::<bool>::new()
    );
}

#[tokio::test]
async fn len_is_empty_clear_and_extend() {
    let set = set_of(&["a", "b", "a"]).await;
    assert_eq!(set.len().await.unwrap(), 2);
    assert!(!set.is_empty().await.unwrap());
    set.clear().await.unwrap();
    assert!(set.is_empty().await.unwrap());
    set.extend(std::iter::empty::<&str>()).await.unwrap();
    assert!(!set.exists().await.unwrap());
}

#[tokio::test]
async fn pop_removes_and_random_keeps() {
    let set = set_of(&["a"]).await;
    assert_eq!(set.random().await.unwrap(), Some("a".to_string()));
    assert_eq!(set.len().await.unwrap(), 1);
    assert_eq!(set.pop().await.unwrap(), Some("a".to_string()));
    assert_eq!(set.pop().await.unwrap(), None);
    assert_eq!(set.random().await.unwrap(), None);
}

#[tokio::test]
async fn move_to_moves_one_member() {
    let client = client().await;
    let from = client.hash_set::<String>(unique("from"));
    let to = client.hash_set::<String>(unique("to"));
    from.extend(["a", "b"]).await.unwrap();
    assert!(from.move_to(&to, "a").await.unwrap());
    assert!(!from.move_to(&to, "a").await.unwrap());
    assert!(!from.contains("a").await.unwrap());
    assert!(to.contains("a").await.unwrap());
}

#[tokio::test]
async fn iter_reads_more_than_one_page() {
    let set = client().await.hash_set::<u32>(unique("set"));
    let numbers: Vec<u32> = (0..250).collect();
    set.extend(numbers.iter()).await.unwrap();
    let read: Vec<u32> = set.iter().try_collect().await.unwrap();
    let unique_read: HashSet<u32> = read.into_iter().collect();
    assert_eq!(unique_read, numbers.into_iter().collect());
}

#[tokio::test]
async fn union_intersection_and_difference() {
    let client = client().await;
    let a = client.hash_set::<String>(unique("a"));
    let b = client.hash_set::<String>(unique("b"));
    a.extend(["1", "2", "3"]).await.unwrap();
    b.extend(["2", "3", "4"]).await.unwrap();
    assert_eq!(sorted(a.union(&[&b]).await.unwrap()), ["1", "2", "3", "4"]);
    assert_eq!(sorted(a.intersection(&[&b]).await.unwrap()), ["2", "3"]);
    assert_eq!(sorted(a.difference(&[&b]).await.unwrap()), ["1"]);
}

#[tokio::test]
async fn set_operations_treat_a_missing_set_as_empty() {
    let client = client().await;
    let a = client.hash_set::<String>(unique("a"));
    let missing = client.hash_set::<String>(unique("missing"));
    a.extend(["1"]).await.unwrap();
    assert_eq!(a.union(&[&missing]).await.unwrap(), ["1"]);
    assert!(a.intersection(&[&missing]).await.unwrap().is_empty());
    assert_eq!(a.difference(&[&missing]).await.unwrap(), ["1"]);
}

#[tokio::test]
async fn object_methods() {
    let client = client().await;
    let set = client.hash_set::<String>(unique("set"));
    set.insert("a").await.unwrap();
    assert!(set.exists().await.unwrap());
    assert!(set
        .expire(std::time::Duration::from_secs(60))
        .await
        .unwrap());
    assert!(set.ttl().await.unwrap().is_some());
    assert!(set.persist().await.unwrap());
    let renamed = unique("set");
    set.rename(&renamed).await.unwrap();
    assert!(client
        .hash_set::<String>(renamed)
        .contains("a")
        .await
        .unwrap());
    assert!(!set.exists().await.unwrap());
}

#[tokio::test]
async fn concurrent_inserts_are_not_lost() {
    let set = client().await.hash_set::<u32>(unique("set"));
    let tasks: Vec<_> = (0..100u32)
        .map(|i| {
            let set = set.clone();
            tokio::spawn(async move { set.insert(&i).await.unwrap() })
        })
        .collect();
    for task in tasks {
        assert!(task.await.unwrap());
    }
    assert_eq!(set.len().await.unwrap(), 100);
}
