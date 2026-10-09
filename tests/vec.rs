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
    assert_eq!(vec.set(1, "z").await.unwrap(), "b");
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
    assert_eq!(vec.remove(2).await.unwrap(), "x");
    assert_eq!(contents(&vec).await, ["x", "y"]);
    assert_eq!(vec.remove(0).await.unwrap(), "x");
    assert_eq!(contents(&vec).await, ["y"]);
    assert!(matches!(vec.remove(5).await, Err(Error::OutOfRange)));
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

#[tokio::test]
async fn huge_indexes_do_not_count_from_the_end() {
    let vec = vec_of(&["a", "b", "c"]).await;
    assert_eq!(vec.get(usize::MAX).await.unwrap(), None);
    assert!(matches!(
        vec.set(usize::MAX, "x").await,
        Err(Error::OutOfRange)
    ));
    assert!(matches!(
        vec.insert(usize::MAX, "x").await,
        Err(Error::OutOfRange)
    ));
    assert!(matches!(
        vec.remove(usize::MAX).await,
        Err(Error::OutOfRange)
    ));
    assert_eq!(vec.range(0..usize::MAX).await.unwrap(), ["a", "b", "c"]);
    vec.trim(0..usize::MAX).await.unwrap();
    assert_eq!(contents(&vec).await, ["a", "b", "c"]);
}

#[tokio::test]
async fn remove_at_the_length_and_on_a_missing_list_is_out_of_range() {
    let vec = vec_of(&["a"]).await;
    assert!(matches!(vec.remove(1).await, Err(Error::OutOfRange)));
    let missing = client().await.vec::<String>(unique("vec"));
    assert!(matches!(missing.remove(0).await, Err(Error::OutOfRange)));
    assert!(matches!(missing.remove(1).await, Err(Error::OutOfRange)));
    assert!(matches!(
        missing.fast_remove(1).await,
        Err(Error::OutOfRange)
    ));
}

#[tokio::test]
async fn trim_past_the_end_empties_the_list() {
    let vec = vec_of(&["a", "b"]).await;
    vec.trim(5..9).await.unwrap();
    assert!(!vec.exists().await.unwrap());
}

#[tokio::test]
async fn iter_handles_exact_page_boundaries() {
    for count in [100u32, 200] {
        let vec = client().await.vec::<u32>(unique("vec"));
        let numbers: Vec<u32> = (0..count).collect();
        vec.extend(numbers.iter()).await.unwrap();
        let read: Vec<u32> = vec.iter().try_collect().await.unwrap();
        assert_eq!(read, numbers);
    }
}

#[tokio::test]
async fn a_key_of_another_type_is_a_redis_error() {
    let client = client().await;
    let name = unique("wrong");
    client
        .bucket::<String>(name.clone())
        .set("x")
        .await
        .unwrap();
    let vec = client.vec::<String>(name);
    assert!(matches!(vec.insert(0, "a").await, Err(Error::Redis(_))));
    assert!(matches!(vec.set(0, "a").await, Err(Error::Redis(_))));
}

async fn ints(values: &[i32]) -> redissun::Vec<i32, redissun::JsonCodec> {
    let vec = client().await.vec::<i32>(unique("vec"));
    vec.extend(values.iter()).await.unwrap();
    vec
}

async fn all(vec: &redissun::Vec<i32, redissun::JsonCodec>) -> Vec<i32> {
    vec.read_all().await.unwrap()
}

#[tokio::test]
async fn test_range() {
    let vec = ints(&[1, 2, 3, 4, 5]).await;
    assert_eq!(vec.range(..=1).await.unwrap(), [1, 2]);
    assert_eq!(vec.range(1..=3).await.unwrap(), [2, 3, 4]);
    vec.del().await.unwrap();
    assert!(vec.range(0..=2).await.unwrap().is_empty());
}

#[tokio::test]
async fn test_add_before() {
    let vec = vec_of(&["1", "2", "3"]).await;
    assert_eq!(vec.insert_before("2", "0").await.unwrap(), Some(4));
    assert_eq!(contents(&vec).await, ["1", "0", "2", "3"]);
    assert_eq!(vec.insert_before("9", "0").await.unwrap(), None);
}

#[tokio::test]
async fn test_add_after() {
    let vec = vec_of(&["1", "2", "3"]).await;
    assert_eq!(vec.insert_after("2", "0").await.unwrap(), Some(4));
    assert_eq!(contents(&vec).await, ["1", "2", "0", "3"]);
}

#[tokio::test]
async fn test_trim() {
    let vec = vec_of(&["1", "2", "3", "4", "5", "6"]).await;
    vec.trim(0..=3).await.unwrap();
    assert_eq!(contents(&vec).await, ["1", "2", "3", "4"]);
}

#[tokio::test]
async fn test_add_all_big_list() {
    let vec = client().await.vec::<String>(unique("vec"));
    let values: Vec<String> = (0..10000).map(|i| i.to_string()).collect();
    vec.extend(values.iter()).await.unwrap();
    vec.insert(3, "123123").await.unwrap();
    assert_eq!(vec.len().await.unwrap(), 10001);
    assert_eq!(vec.get(3).await.unwrap(), Some("123123".to_string()));
    assert_eq!(vec.get(4).await.unwrap(), Some("3".to_string()));
}

#[tokio::test]
async fn test_add_by_index() {
    let vec = vec_of(&["foo"]).await;
    vec.insert(0, "bar").await.unwrap();
    assert_eq!(contents(&vec).await, ["bar", "foo"]);
}

#[tokio::test]
async fn test_long() {
    let vec = client().await.vec::<i64>(unique("vec"));
    vec.push(&1).await.unwrap();
    vec.push(&2).await.unwrap();
    assert_eq!(vec.read_all().await.unwrap(), [1, 2]);
}

#[tokio::test]
async fn test_last_index_of_none() {
    let vec = ints(&[1, 2, 3, 4, 5]).await;
    assert_eq!(vec.rposition(&10).await.unwrap(), None);
}

#[tokio::test]
async fn test_last_index_of2() {
    let vec = ints(&[1, 2, 3, 4, 5, 0, 7, 8, 0, 10]).await;
    assert_eq!(vec.rposition(&3).await.unwrap(), Some(2));
}

#[tokio::test]
async fn test_last_index_of1() {
    let vec = ints(&[1, 2, 3, 4, 5, 3, 7, 8, 0, 10]).await;
    assert_eq!(vec.rposition(&3).await.unwrap(), Some(5));
}

#[tokio::test]
async fn test_last_index_of() {
    let vec = ints(&[1, 2, 3, 4, 5, 3, 7, 8, 3, 10]).await;
    assert_eq!(vec.rposition(&3).await.unwrap(), Some(8));
}

#[tokio::test]
async fn test_index_of() {
    let values: Vec<i32> = (1..200).collect();
    let vec = ints(&values).await;
    assert_eq!(vec.position(&56).await.unwrap(), Some(55));
    assert_eq!(vec.position(&100).await.unwrap(), Some(99));
    assert_eq!(vec.position(&200).await.unwrap(), None);
    assert_eq!(vec.position(&0).await.unwrap(), None);
}

#[tokio::test]
async fn test_remove() {
    let vec = ints(&[1, 2, 3, 4, 5]).await;
    assert_eq!(vec.remove(0).await.unwrap(), 1);
    assert_eq!(all(&vec).await, [2, 3, 4, 5]);
    assert_eq!(vec.remove(2).await.unwrap(), 4);
    assert_eq!(all(&vec).await, [2, 3, 5]);
}

#[tokio::test]
async fn test_remove_with_count() {
    let vec = ints(&[1, 2, 3, 3, 4]).await;
    assert_eq!(vec.remove_value_n(&1, 5).await.unwrap(), 1);
    assert_eq!(all(&vec).await, [2, 3, 3, 4]);
    assert_eq!(vec.remove_value_n(&3, 5).await.unwrap(), 2);
    assert_eq!(all(&vec).await, [2, 4]);
}

#[tokio::test]
async fn test_set() {
    let vec = ints(&[1, 2, 3, 4, 5]).await;
    assert_eq!(vec.set(4, &6).await.unwrap(), 5);
    assert_eq!(all(&vec).await, [1, 2, 3, 4, 6]);
}

#[tokio::test]
async fn test_set_fail() {
    let vec = ints(&[1, 2, 3, 4, 5]).await;
    assert!(matches!(vec.set(5, &6).await, Err(Error::OutOfRange)));
}

#[tokio::test]
async fn test_remove_all_empty() {
    let vec = ints(&[1, 2, 3, 4, 5]).await;
    assert!(!vec.remove_values(std::iter::empty::<&i32>()).await.unwrap());
}

#[tokio::test]
async fn test_remove_all() {
    let vec = ints(&[1, 2, 3, 4, 5]).await;
    assert!(!vec.remove_values(std::iter::empty::<&i32>()).await.unwrap());
    assert!(vec.remove_values(&[3, 2, 10, 6]).await.unwrap());
    assert_eq!(all(&vec).await, [1, 4, 5]);
    assert!(vec.remove_values(&[4]).await.unwrap());
    assert_eq!(all(&vec).await, [1, 5]);
    assert!(vec.remove_values(&[1, 5, 1, 5]).await.unwrap());
    assert!(vec.is_empty().await.unwrap());
}

#[tokio::test]
async fn test_retain_all() {
    let vec = ints(&[1, 2, 3, 4, 5]).await;
    assert!(vec.retain_values(&[3, 2, 10, 6]).await.unwrap());
    assert_eq!(all(&vec).await, [2, 3]);
    assert_eq!(vec.len().await.unwrap(), 2);
}

#[tokio::test]
async fn test_fast_set() {
    let vec = ints(&[1, 2]).await;
    vec.fast_set(0, &3).await.unwrap();
    assert_eq!(vec.get(0).await.unwrap(), Some(3));
    assert!(matches!(vec.fast_set(2, &3).await, Err(Error::OutOfRange)));
}

#[tokio::test]
async fn test_retain_all_empty() {
    let vec = ints(&[1, 2, 3, 4, 5]).await;
    assert!(vec.retain_values(std::iter::empty::<&i32>()).await.unwrap());
    assert_eq!(vec.len().await.unwrap(), 0);
}

#[tokio::test]
async fn test_retain_all_no_modify() {
    let vec = ints(&[1, 2]).await;
    assert!(!vec.retain_values(&[1, 2]).await.unwrap());
    assert_eq!(all(&vec).await, [1, 2]);
}

#[tokio::test]
async fn test_add_all_index_error() {
    let vec = client().await.vec::<i32>(unique("vec"));
    assert!(matches!(
        vec.insert_all(2, &[7, 8, 9]).await,
        Err(Error::OutOfRange)
    ));
}

#[tokio::test]
async fn test_add_all_index() {
    let vec = ints(&[1, 2, 3, 4, 5]).await;
    vec.insert_all(2, &[7, 8, 9]).await.unwrap();
    assert_eq!(all(&vec).await, [1, 2, 7, 8, 9, 3, 4, 5]);
    let len = vec.len().await.unwrap();
    vec.insert_all(len - 1, &[9, 1, 9]).await.unwrap();
    assert_eq!(all(&vec).await, [1, 2, 7, 8, 9, 3, 4, 9, 1, 9, 5]);
    let len = vec.len().await.unwrap();
    vec.insert_all(len, &[0, 5]).await.unwrap();
    assert_eq!(all(&vec).await, [1, 2, 7, 8, 9, 3, 4, 9, 1, 9, 5, 0, 5]);
    vec.insert_all(0, &[6, 7]).await.unwrap();
    assert_eq!(
        all(&vec).await,
        [6, 7, 1, 2, 7, 8, 9, 3, 4, 9, 1, 9, 5, 0, 5]
    );
}

#[tokio::test]
async fn test_add_all() {
    let vec = ints(&[1, 2, 3, 4, 5]).await;
    vec.extend(&[7, 8, 9]).await.unwrap();
    vec.extend(&[9, 1, 9]).await.unwrap();
    assert_eq!(all(&vec).await, [1, 2, 3, 4, 5, 7, 8, 9, 9, 1, 9]);
}

#[tokio::test]
async fn test_add_all_empty() {
    let vec = client().await.vec::<i32>(unique("vec"));
    vec.extend(std::iter::empty::<&i32>()).await.unwrap();
    assert_eq!(vec.len().await.unwrap(), 0);
}

#[tokio::test]
async fn test_contains_all() {
    let values: Vec<i32> = (0..200).collect();
    let vec = ints(&values).await;
    assert!(vec.contains_all(&[30, 11]).await.unwrap());
    assert!(!vec.contains_all(&[30, 711, 11]).await.unwrap());
    assert!(vec.contains_all(&[30]).await.unwrap());
    assert!(vec.contains_all(&[30, 30]).await.unwrap());
    assert!(vec.contains_all(&[30, 11, 30]).await.unwrap());
}

#[tokio::test]
async fn test_contains_all_empty() {
    let values: Vec<i32> = (0..200).collect();
    let vec = ints(&values).await;
    assert!(vec.contains_all(std::iter::empty::<&i32>()).await.unwrap());
}

#[tokio::test]
async fn test_to_array() {
    let vec = vec_of(&["1", "4", "2", "5", "3"]).await;
    assert_eq!(vec.read_all().await.unwrap(), ["1", "4", "2", "5", "3"]);
}

#[tokio::test]
async fn test_iterator_sequence() {
    let vec = vec_of(&["1", "4", "2", "5", "3"]).await;
    for _ in 0..2 {
        assert_eq!(contents(&vec).await, ["1", "4", "2", "5", "3"]);
    }
}

#[tokio::test]
async fn test_contains() {
    let vec = vec_of(&["1", "4", "2", "5", "3"]).await;
    assert!(vec.contains("3").await.unwrap());
    assert!(!vec.contains("31").await.unwrap());
    assert!(vec.contains("1").await.unwrap());
}

#[tokio::test]
async fn test_get_fail() {
    let vec = client().await.vec::<String>(unique("vec"));
    assert_eq!(vec.get(0).await.unwrap(), None);
}

#[tokio::test]
async fn test_add_get() {
    let vec = vec_of(&["1", "4", "2", "5", "3"]).await;
    assert_eq!(vec.get(0).await.unwrap(), Some("1".to_string()));
    assert_eq!(vec.get(3).await.unwrap(), Some("5".to_string()));
}

#[tokio::test]
async fn test_duplicates() {
    let vec = client().await.vec::<(String, String)>(unique("vec"));
    for (a, b) in [("1", "2"), ("1", "2"), ("2", "3"), ("3", "4"), ("5", "6")] {
        vec.push(&(a.to_string(), b.to_string())).await.unwrap();
    }
    assert_eq!(vec.len().await.unwrap(), 5);
}

#[tokio::test]
async fn test_size() {
    let vec = vec_of(&["1", "2", "3", "4", "5", "6"]).await;
    assert_eq!(contents(&vec).await, ["1", "2", "3", "4", "5", "6"]);
    vec.remove_value("2").await.unwrap();
    assert_eq!(contents(&vec).await, ["1", "3", "4", "5", "6"]);
    vec.remove_value("4").await.unwrap();
    assert_eq!(contents(&vec).await, ["1", "3", "5", "6"]);
}

#[tokio::test]
async fn test_codec() {
    let vec = client().await.vec::<serde_json::Value>(unique("vec"));
    let values = [
        serde_json::json!(1),
        serde_json::json!(2),
        serde_json::json!("3"),
        serde_json::json!("e"),
    ];
    vec.extend(values.iter()).await.unwrap();
    assert_eq!(vec.read_all().await.unwrap(), values);
}

#[tokio::test]
async fn insert_past_the_length_fails_and_at_the_length_appends() {
    let vec = ints(&[1]).await;
    assert!(matches!(vec.insert(3, &2).await, Err(Error::OutOfRange)));
    vec.insert(1, &2).await.unwrap();
    assert_eq!(all(&vec).await, [1, 2]);
}
