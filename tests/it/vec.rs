use crate::common::{client, unique};
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
async fn replace_swaps_an_element_and_rejects_a_missing_index() {
    let vec = vec_of(&["a", "b"]).await;
    assert_eq!(vec.replace(1, "z").await.unwrap(), "b");
    assert_eq!(contents(&vec).await, ["a", "z"]);
    assert!(matches!(vec.replace(2, "x").await, Err(Error::OutOfRange)));
    let empty = client().await.vec::<String>(unique("vec"));
    assert!(matches!(
        empty.replace(0, "x").await,
        Err(Error::OutOfRange)
    ));
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
async fn remove_value_removes_the_first_match_and_remove_value_all_every_match() {
    let vec = vec_of(&["a", "b", "a", "a"]).await;
    assert!(vec.remove_value("a").await.unwrap());
    assert_eq!(contents(&vec).await, ["b", "a", "a"]);
    assert_eq!(vec.remove_value_all("a").await.unwrap(), 2);
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
async fn a_vec_supports_exists_expire_persist_and_rename() {
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
        vec.replace(usize::MAX, "x").await,
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
    assert!(matches!(vec.replace(0, "a").await, Err(Error::Redis(_))));
}

async fn ints(values: &[i32]) -> redissun::Vec<i32, redissun::JsonCodec> {
    let vec = client().await.vec::<i32>(unique("vec"));
    vec.extend(values.iter()).await.unwrap();
    vec
}

async fn all(vec: &redissun::Vec<i32, redissun::JsonCodec>) -> Vec<i32> {
    vec.to_vec().await.unwrap()
}

// Redisson: testRange
#[tokio::test]
async fn range_returns_inclusive_slices_and_nothing_once_deleted() {
    let vec = ints(&[1, 2, 3, 4, 5]).await;
    assert_eq!(vec.range(..=1).await.unwrap(), [1, 2]);
    assert_eq!(vec.range(1..=3).await.unwrap(), [2, 3, 4]);
    vec.del().await.unwrap();
    assert!(vec.range(0..=2).await.unwrap().is_empty());
}

// Redisson: testAddBefore
#[tokio::test]
async fn insert_before_puts_the_value_before_the_pivot() {
    let vec = vec_of(&["1", "2", "3"]).await;
    assert_eq!(vec.insert_before("2", "0").await.unwrap(), Some(4));
    assert_eq!(contents(&vec).await, ["1", "0", "2", "3"]);
    assert_eq!(vec.insert_before("9", "0").await.unwrap(), None);
}

// Redisson: testAddAfter
#[tokio::test]
async fn insert_after_puts_the_value_after_the_pivot() {
    let vec = vec_of(&["1", "2", "3"]).await;
    assert_eq!(vec.insert_after("2", "0").await.unwrap(), Some(4));
    assert_eq!(contents(&vec).await, ["1", "2", "0", "3"]);
}

// Redisson: testTrim
#[tokio::test]
async fn trim_to_an_inclusive_range_keeps_the_first_four() {
    let vec = vec_of(&["1", "2", "3", "4", "5", "6"]).await;
    vec.trim(0..=3).await.unwrap();
    assert_eq!(contents(&vec).await, ["1", "2", "3", "4"]);
}

// Redisson: testAddAllBigList
#[tokio::test]
async fn insert_into_a_ten_thousand_element_vec_shifts_the_rest() {
    let vec = client().await.vec::<String>(unique("vec"));
    let values: Vec<String> = (0..10000).map(|i| i.to_string()).collect();
    vec.extend(values.iter()).await.unwrap();
    vec.insert(3, "123123").await.unwrap();
    assert_eq!(vec.len().await.unwrap(), 10001);
    assert_eq!(vec.get(3).await.unwrap(), Some("123123".to_string()));
    assert_eq!(vec.get(4).await.unwrap(), Some("3".to_string()));
}

// Redisson: testAddByIndex
#[tokio::test]
async fn insert_at_zero_puts_the_value_first() {
    let vec = vec_of(&["foo"]).await;
    vec.insert(0, "bar").await.unwrap();
    assert_eq!(contents(&vec).await, ["bar", "foo"]);
}

// Redisson: testLong
#[tokio::test]
async fn i64_values_round_trip_in_push_order() {
    let vec = client().await.vec::<i64>(unique("vec"));
    vec.push(&1).await.unwrap();
    vec.push(&2).await.unwrap();
    assert_eq!(vec.to_vec().await.unwrap(), [1, 2]);
}

// Redisson: testLastIndexOfNone
#[tokio::test]
async fn rposition_of_a_missing_value_is_none() {
    let vec = ints(&[1, 2, 3, 4, 5]).await;
    assert_eq!(vec.rposition(&10).await.unwrap(), None);
}

// Redisson: testLastIndexOf2
#[tokio::test]
async fn rposition_finds_the_only_occurrence() {
    let vec = ints(&[1, 2, 3, 4, 5, 0, 7, 8, 0, 10]).await;
    assert_eq!(vec.rposition(&3).await.unwrap(), Some(2));
}

// Redisson: testLastIndexOf1
#[tokio::test]
async fn rposition_finds_the_later_of_two_occurrences() {
    let vec = ints(&[1, 2, 3, 4, 5, 3, 7, 8, 0, 10]).await;
    assert_eq!(vec.rposition(&3).await.unwrap(), Some(5));
}

// Redisson: testLastIndexOf
#[tokio::test]
async fn rposition_finds_the_last_of_three_occurrences() {
    let vec = ints(&[1, 2, 3, 4, 5, 3, 7, 8, 3, 10]).await;
    assert_eq!(vec.rposition(&3).await.unwrap(), Some(8));
}

// Redisson: testIndexOf
#[tokio::test]
async fn position_returns_the_index_or_none_when_absent() {
    let values: Vec<i32> = (1..200).collect();
    let vec = ints(&values).await;
    assert_eq!(vec.position(&56).await.unwrap(), Some(55));
    assert_eq!(vec.position(&100).await.unwrap(), Some(99));
    assert_eq!(vec.position(&200).await.unwrap(), None);
    assert_eq!(vec.position(&0).await.unwrap(), None);
}

// Redisson: testRemove
#[tokio::test]
async fn remove_by_index_returns_the_element_and_shifts_the_rest() {
    let vec = ints(&[1, 2, 3, 4, 5]).await;
    assert_eq!(vec.remove(0).await.unwrap(), 1);
    assert_eq!(all(&vec).await, [2, 3, 4, 5]);
    assert_eq!(vec.remove(2).await.unwrap(), 4);
    assert_eq!(all(&vec).await, [2, 3, 5]);
}

// Redisson: testRemoveWithCount
#[tokio::test]
async fn remove_value_n_removes_up_to_n_matches() {
    let vec = ints(&[1, 2, 3, 3, 4]).await;
    assert_eq!(vec.remove_value_n(&1, 5).await.unwrap(), 1);
    assert_eq!(all(&vec).await, [2, 3, 3, 4]);
    assert_eq!(vec.remove_value_n(&3, 5).await.unwrap(), 2);
    assert_eq!(all(&vec).await, [2, 4]);
}

// Redisson: testSet
#[tokio::test]
async fn replace_returns_the_old_value_and_replaces_it() {
    let vec = ints(&[1, 2, 3, 4, 5]).await;
    assert_eq!(vec.replace(4, &6).await.unwrap(), 5);
    assert_eq!(all(&vec).await, [1, 2, 3, 4, 6]);
}

// Redisson: testSetFail
#[tokio::test]
async fn replace_at_the_length_is_out_of_range() {
    let vec = ints(&[1, 2, 3, 4, 5]).await;
    assert!(matches!(vec.replace(5, &6).await, Err(Error::OutOfRange)));
}

// Redisson: testRemoveAllEmpty
#[tokio::test]
async fn remove_values_with_nothing_reports_no_change() {
    let vec = ints(&[1, 2, 3, 4, 5]).await;
    assert!(!vec.remove_values(std::iter::empty::<&i32>()).await.unwrap());
}

// Redisson: testRemoveAll
#[tokio::test]
async fn remove_values_removes_every_match_and_reports_a_change() {
    let vec = ints(&[1, 2, 3, 4, 5]).await;
    assert!(!vec.remove_values(std::iter::empty::<&i32>()).await.unwrap());
    assert!(vec.remove_values(&[3, 2, 10, 6]).await.unwrap());
    assert_eq!(all(&vec).await, [1, 4, 5]);
    assert!(vec.remove_values(&[4]).await.unwrap());
    assert_eq!(all(&vec).await, [1, 5]);
    assert!(vec.remove_values(&[1, 5, 1, 5]).await.unwrap());
    assert!(vec.is_empty().await.unwrap());
}

// Redisson: testRetainAll
#[tokio::test]
async fn retain_values_keeps_only_the_given_values() {
    let vec = ints(&[1, 2, 3, 4, 5]).await;
    assert!(vec.retain_values(&[3, 2, 10, 6]).await.unwrap());
    assert_eq!(all(&vec).await, [2, 3]);
    assert_eq!(vec.len().await.unwrap(), 2);
}

// Redisson: testFastSet
#[tokio::test]
async fn set_replaces_an_element_and_rejects_a_missing_index() {
    let vec = ints(&[1, 2]).await;
    vec.set(0, &3).await.unwrap();
    assert_eq!(vec.get(0).await.unwrap(), Some(3));
    assert!(matches!(vec.set(2, &3).await, Err(Error::OutOfRange)));
}

// Redisson: testRetainAllEmpty
#[tokio::test]
async fn retain_values_with_nothing_empties_the_vec() {
    let vec = ints(&[1, 2, 3, 4, 5]).await;
    assert!(vec.retain_values(std::iter::empty::<&i32>()).await.unwrap());
    assert_eq!(vec.len().await.unwrap(), 0);
}

// Redisson: testRetainAllNoModify
#[tokio::test]
async fn retain_values_of_every_element_reports_no_change() {
    let vec = ints(&[1, 2]).await;
    assert!(!vec.retain_values(&[1, 2]).await.unwrap());
    assert_eq!(all(&vec).await, [1, 2]);
}

// Redisson: testAddAllIndexError
#[tokio::test]
async fn insert_many_past_the_end_of_an_empty_vec_is_out_of_range() {
    let vec = client().await.vec::<i32>(unique("vec"));
    assert!(matches!(
        vec.insert_many(2, &[7, 8, 9]).await,
        Err(Error::OutOfRange)
    ));
}

// Redisson: testAddAllIndex
#[tokio::test]
async fn insert_many_puts_values_at_the_start_middle_and_end() {
    let vec = ints(&[1, 2, 3, 4, 5]).await;
    vec.insert_many(2, &[7, 8, 9]).await.unwrap();
    assert_eq!(all(&vec).await, [1, 2, 7, 8, 9, 3, 4, 5]);
    let len = vec.len().await.unwrap();
    vec.insert_many(len - 1, &[9, 1, 9]).await.unwrap();
    assert_eq!(all(&vec).await, [1, 2, 7, 8, 9, 3, 4, 9, 1, 9, 5]);
    let len = vec.len().await.unwrap();
    vec.insert_many(len, &[0, 5]).await.unwrap();
    assert_eq!(all(&vec).await, [1, 2, 7, 8, 9, 3, 4, 9, 1, 9, 5, 0, 5]);
    vec.insert_many(0, &[6, 7]).await.unwrap();
    assert_eq!(
        all(&vec).await,
        [6, 7, 1, 2, 7, 8, 9, 3, 4, 9, 1, 9, 5, 0, 5]
    );
}

// Redisson: testAddAll
#[tokio::test]
async fn extend_appends_values_in_order() {
    let vec = ints(&[1, 2, 3, 4, 5]).await;
    vec.extend(&[7, 8, 9]).await.unwrap();
    vec.extend(&[9, 1, 9]).await.unwrap();
    assert_eq!(all(&vec).await, [1, 2, 3, 4, 5, 7, 8, 9, 9, 1, 9]);
}

// Redisson: testAddAllEmpty
#[tokio::test]
async fn extend_with_nothing_leaves_a_missing_vec_empty() {
    let vec = client().await.vec::<i32>(unique("vec"));
    vec.extend(std::iter::empty::<&i32>()).await.unwrap();
    assert_eq!(vec.len().await.unwrap(), 0);
}

// Redisson: testContainsAll
#[tokio::test]
async fn contains_all_is_true_only_when_every_value_is_present() {
    let values: Vec<i32> = (0..200).collect();
    let vec = ints(&values).await;
    assert!(vec.contains_all(&[30, 11]).await.unwrap());
    assert!(!vec.contains_all(&[30, 711, 11]).await.unwrap());
    assert!(vec.contains_all(&[30]).await.unwrap());
    assert!(vec.contains_all(&[30, 30]).await.unwrap());
    assert!(vec.contains_all(&[30, 11, 30]).await.unwrap());
}

// Redisson: testContainsAllEmpty
#[tokio::test]
async fn contains_all_of_nothing_is_true() {
    let values: Vec<i32> = (0..200).collect();
    let vec = ints(&values).await;
    assert!(vec.contains_all(std::iter::empty::<&i32>()).await.unwrap());
}

// Redisson: testToArray
#[tokio::test]
async fn to_vec_returns_elements_in_insertion_order() {
    let vec = vec_of(&["1", "4", "2", "5", "3"]).await;
    assert_eq!(vec.to_vec().await.unwrap(), ["1", "4", "2", "5", "3"]);
}

// Redisson: testIteratorSequence
#[tokio::test]
async fn iter_returns_the_same_order_on_every_pass() {
    let vec = vec_of(&["1", "4", "2", "5", "3"]).await;
    for _ in 0..2 {
        assert_eq!(contents(&vec).await, ["1", "4", "2", "5", "3"]);
    }
}

// Redisson: testContains
#[tokio::test]
async fn contains_is_true_only_for_stored_values() {
    let vec = vec_of(&["1", "4", "2", "5", "3"]).await;
    assert!(vec.contains("3").await.unwrap());
    assert!(!vec.contains("31").await.unwrap());
    assert!(vec.contains("1").await.unwrap());
}

// Redisson: testGetFail
#[tokio::test]
async fn get_on_a_missing_vec_returns_none() {
    let vec = client().await.vec::<String>(unique("vec"));
    assert_eq!(vec.get(0).await.unwrap(), None);
}

// Redisson: testAddGet
#[tokio::test]
async fn get_returns_the_element_at_an_index() {
    let vec = vec_of(&["1", "4", "2", "5", "3"]).await;
    assert_eq!(vec.get(0).await.unwrap(), Some("1".to_string()));
    assert_eq!(vec.get(3).await.unwrap(), Some("5".to_string()));
}

// Redisson: testDuplicates
#[tokio::test]
async fn push_keeps_duplicate_tuples() {
    let vec = client().await.vec::<(String, String)>(unique("vec"));
    for (a, b) in [("1", "2"), ("1", "2"), ("2", "3"), ("3", "4"), ("5", "6")] {
        vec.push(&(a.to_string(), b.to_string())).await.unwrap();
    }
    assert_eq!(vec.len().await.unwrap(), 5);
}

// Redisson: testSize
#[tokio::test]
async fn remove_value_drops_one_element_and_keeps_the_order() {
    let vec = vec_of(&["1", "2", "3", "4", "5", "6"]).await;
    assert_eq!(contents(&vec).await, ["1", "2", "3", "4", "5", "6"]);
    vec.remove_value("2").await.unwrap();
    assert_eq!(contents(&vec).await, ["1", "3", "4", "5", "6"]);
    vec.remove_value("4").await.unwrap();
    assert_eq!(contents(&vec).await, ["1", "3", "5", "6"]);
}

// Redisson: testCodec
#[tokio::test]
async fn json_values_of_mixed_types_round_trip() {
    let vec = client().await.vec::<serde_json::Value>(unique("vec"));
    let values = [
        serde_json::json!(1),
        serde_json::json!(2),
        serde_json::json!("3"),
        serde_json::json!("e"),
    ];
    vec.extend(values.iter()).await.unwrap();
    assert_eq!(vec.to_vec().await.unwrap(), values);
}

#[tokio::test]
async fn insert_past_the_length_fails_and_at_the_length_appends() {
    let vec = ints(&[1]).await;
    assert!(matches!(vec.insert(3, &2).await, Err(Error::OutOfRange)));
    vec.insert(1, &2).await.unwrap();
    assert_eq!(all(&vec).await, [1, 2]);
}
