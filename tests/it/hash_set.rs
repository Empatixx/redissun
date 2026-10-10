use crate::common::{client, unique};
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

async fn ints(values: &[i32]) -> redissun::HashSet<i32, redissun::JsonCodec> {
    let set = client().await.hash_set::<i32>(unique("set"));
    set.extend(values.iter()).await.unwrap();
    set
}

async fn all<V: serde::de::DeserializeOwned + serde::Serialize + Send + Sync + Ord>(
    set: &redissun::HashSet<V, redissun::JsonCodec>,
) -> Vec<V> {
    let mut values: Vec<V> = set.iter().try_collect().await.unwrap();
    values.sort();
    values
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
struct TestObject {
    name: String,
    value: String,
}

fn object(name: &str, value: &str) -> TestObject {
    TestObject {
        name: name.into(),
        value: value.into(),
    }
}

#[tokio::test]
async fn test_contains_each() {
    let set = ints(&[0, 1]).await;
    assert!(set.contains_many::<i32>(&[]).await.unwrap().is_empty());
    assert_eq!(set.contains_many(&[&0, &1]).await.unwrap(), [true, true]);
    assert_eq!(
        set.contains_many(&[&0, &1, &2]).await.unwrap(),
        [true, true, false]
    );
    assert_eq!(
        set.contains_many(&[&2, &3, &4]).await.unwrap(),
        [false, false, false]
    );
}

#[tokio::test]
async fn test_remove_all_counted() {
    let set = ints(&[0, 1, 2, 3]).await;
    assert_eq!(set.remove_many(&[&1, &2, &3, &4, &5]).await.unwrap(), 3);
}

#[tokio::test]
async fn test_try_add() {
    let set = client().await.hash_set::<String>(unique("set"));
    let elements = 20_000;
    let names: Vec<String> = (0..elements).map(|i| format!("name{i}")).collect();
    assert!(set.try_extend(names.iter()).await.unwrap());
    assert_eq!(set.len().await.unwrap(), elements);
    let mut names2: Vec<String> = (elements + 1..elements + 1000)
        .map(|i| format!("name{i}"))
        .collect();
    names2.push("name10".into());
    assert!(!set.try_extend(names2.iter()).await.unwrap());
    assert_eq!(set.len().await.unwrap(), elements);
}

#[tokio::test]
async fn test_remove_random() {
    let set = ints(&[1, 2, 3]).await;
    for _ in 0..3 {
        assert!([1, 2, 3].contains(&set.pop().await.unwrap().unwrap()));
    }
    assert_eq!(set.pop().await.unwrap(), None);
}

#[tokio::test]
async fn test_remove_random_amount() {
    let set = ints(&[1, 2, 3, 4, 5, 6]).await;
    let mut seen = Vec::new();
    for count in [3, 2, 1] {
        let popped = set.pop_many(count).await.unwrap();
        assert_eq!(popped.len(), count);
        seen.extend(popped);
    }
    seen.sort();
    assert_eq!(seen, [1, 2, 3, 4, 5, 6]);
    assert!(set.pop_many(4).await.unwrap().is_empty());
}

#[tokio::test]
async fn test_random_limited() {
    let set = ints(&(0..10).collect::<Vec<_>>()).await;
    let random = set.random_many(3).await.unwrap();
    assert_eq!(random.len(), 3);
    assert!(random.iter().all(|value| (0..10).contains(value)));
    assert_eq!(set.len().await.unwrap(), 10);
}

#[tokio::test]
async fn test_random() {
    let set = ints(&[1, 2, 3]).await;
    for _ in 0..3 {
        assert!([1, 2, 3].contains(&set.random().await.unwrap().unwrap()));
    }
    assert_eq!(all(&set).await, [1, 2, 3]);
}

#[tokio::test]
async fn test_add_bean() {
    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct SimpleBean {
        lng: i64,
    }
    let set = client().await.hash_set::<SimpleBean>(unique("set"));
    set.insert(&SimpleBean { lng: 1 }).await.unwrap();
    let read: Vec<SimpleBean> = set.iter().try_collect().await.unwrap();
    assert_eq!(read, [SimpleBean { lng: 1 }]);
}

#[tokio::test]
async fn test_add_long() {
    let set = client().await.hash_set::<i64>(unique("set"));
    set.insert(&1i64).await.unwrap();
    assert_eq!(all(&set).await, [1i64]);
}

#[tokio::test]
async fn test_add_async() {
    let set = client().await.hash_set::<i32>(unique("set"));
    assert!(set.insert(&2).await.unwrap());
    assert!(set.contains(&2).await.unwrap());
}

#[tokio::test]
async fn test_remove_async() {
    let set = ints(&[1, 3, 7]).await;
    assert!(set.remove(&1).await.unwrap());
    assert!(!set.contains(&1).await.unwrap());
    assert_eq!(all(&set).await, [3, 7]);
    assert!(!set.remove(&1).await.unwrap());
    assert_eq!(all(&set).await, [3, 7]);
    set.remove(&3).await.unwrap();
    assert!(!set.contains(&3).await.unwrap());
    assert_eq!(all(&set).await, [7]);
}

#[tokio::test]
async fn test_iterator_remove() {
    let set = set_of(&["1", "4", "2", "5", "3"]).await;
    let values: Vec<String> = set.iter().try_collect().await.unwrap();
    for value in &values {
        if value == "2" {
            set.remove(value).await.unwrap();
        }
    }
    assert_eq!(all(&set).await, ["1", "3", "4", "5"]);
    let mut iterations = 0;
    let values: Vec<String> = set.iter().try_collect().await.unwrap();
    for value in &values {
        set.remove(value).await.unwrap();
        iterations += 1;
    }
    assert_eq!(iterations, 4);
    assert_eq!(set.len().await.unwrap(), 0);
    assert!(set.is_empty().await.unwrap());
}

#[tokio::test]
async fn test_iterator_sequence() {
    let set = client().await.hash_set::<i64>(unique("set"));
    let values: Vec<i64> = (0..1000).collect();
    set.extend(values.iter()).await.unwrap();
    let read: HashSet<i64> = set.iter().try_collect().await.unwrap();
    assert_eq!(read, values.into_iter().collect());
}

#[tokio::test]
async fn test_long() {
    let set = client().await.hash_set::<i64>(unique("set"));
    set.insert(&1).await.unwrap();
    set.insert(&2).await.unwrap();
    assert_eq!(all(&set).await, [1, 2]);
}

#[tokio::test]
async fn test_retain_all() {
    let set = ints(&(0..20_000).collect::<Vec<_>>()).await;
    assert!(set.retain(&[&1, &2]).await.unwrap());
    assert_eq!(all(&set).await, [1, 2]);
    assert_eq!(set.len().await.unwrap(), 2);
}

#[tokio::test]
async fn test_iterator_remove_high_volume() {
    let set = ints(&(0..10_000).collect::<Vec<_>>()).await;
    let values: Vec<i32> = set.iter().try_collect().await.unwrap();
    let mut count = 0;
    for value in values.iter().collect::<HashSet<_>>() {
        set.remove(value).await.unwrap();
        count += 1;
    }
    assert_eq!(set.len().await.unwrap(), 0);
    assert_eq!(count, 10_000);
}

#[tokio::test]
async fn test_contains_all() {
    let set = ints(&(0..200).collect::<Vec<_>>()).await;
    assert!(set.contains_all::<i32>(&[]).await.unwrap());
    assert!(set.contains_all(&[&30, &11]).await.unwrap());
    assert!(!set.contains_all(&[&30, &711, &11]).await.unwrap());
}

#[tokio::test]
async fn test_to_array() {
    let set = set_of(&["1", "4", "2", "5", "3"]).await;
    assert_eq!(all(&set).await, ["1", "2", "3", "4", "5"]);
}

#[tokio::test]
async fn test_contains() {
    let set = client().await.hash_set::<TestObject>(unique("set"));
    for (name, value) in [("1", "2"), ("1", "2"), ("2", "3"), ("3", "4"), ("5", "6")] {
        set.insert(&object(name, value)).await.unwrap();
    }
    assert!(set.contains(&object("2", "3")).await.unwrap());
    assert!(set.contains(&object("1", "2")).await.unwrap());
    assert!(!set.contains(&object("1", "9")).await.unwrap());
}

#[tokio::test]
async fn test_duplicates() {
    let set = client().await.hash_set::<TestObject>(unique("set"));
    for (name, value) in [("1", "2"), ("1", "2"), ("2", "3"), ("3", "4"), ("5", "6")] {
        set.insert(&object(name, value)).await.unwrap();
    }
    assert_eq!(set.len().await.unwrap(), 4);
}

#[tokio::test]
async fn test_size() {
    let set = ints(&[1, 2, 3, 3, 4, 5, 5]).await;
    assert_eq!(set.len().await.unwrap(), 5);
}

#[tokio::test]
async fn test_retain_all_empty() {
    let set = ints(&[1, 2, 3, 4, 5]).await;
    assert!(set.retain::<i32>(&[]).await.unwrap());
    assert_eq!(set.len().await.unwrap(), 0);
}

#[tokio::test]
async fn test_retain_all_no_modify() {
    let set = ints(&[1, 2]).await;
    assert!(!set.retain(&[&1, &2]).await.unwrap());
    assert_eq!(all(&set).await, [1, 2]);
}

async fn three_sets(
    own: &[i32],
    first: &[i32],
    second: &[i32],
) -> [redissun::HashSet<i32, redissun::JsonCodec>; 3] {
    let client = client().await;
    let tag = unique("sets");
    let sets = [
        client.hash_set::<i32>(format!("{{{tag}}}:set")),
        client.hash_set::<i32>(format!("{{{tag}}}:set1")),
        client.hash_set::<i32>(format!("{{{tag}}}:set2")),
    ];
    for (set, values) in sets.iter().zip([own, first, second]) {
        set.extend(values.iter()).await.unwrap();
    }
    sets
}

#[tokio::test]
async fn test_union() {
    let [set, set1, set2] = three_sets(&[5, 6], &[1, 2], &[3, 4]).await;
    assert_eq!(set.store_union(&[&set1, &set2]).await.unwrap(), 4);
    assert_eq!(all(&set).await, [1, 2, 3, 4]);
}

#[tokio::test]
async fn test_read_union() {
    let [set, set1, set2] = three_sets(&[5, 6], &[1, 2], &[3, 4]).await;
    let mut union = set.union(&[&set1, &set2]).await.unwrap();
    union.sort();
    assert_eq!(union, [1, 2, 3, 4, 5, 6]);
    assert_eq!(all(&set).await, [5, 6]);
}

#[tokio::test]
async fn test_diff() {
    let [set, set1, set2] = three_sets(&[5, 6], &[1, 2, 3], &[3, 4, 5]).await;
    assert_eq!(set.store_difference(&[&set1, &set2]).await.unwrap(), 2);
    assert_eq!(all(&set).await, [1, 2]);
}

#[tokio::test]
async fn test_read_diff() {
    let [set, set1, set2] = three_sets(&[5, 7, 6], &[1, 2, 5], &[3, 4, 5]).await;
    let mut diff = set.difference(&[&set1, &set2]).await.unwrap();
    diff.sort();
    assert_eq!(diff, [6, 7]);
    assert_eq!(all(&set).await, [5, 6, 7]);
}

#[tokio::test]
async fn test_count_intersection() {
    let [set, set1, set2] = three_sets(&[1, 2, 3, 4], &[2, 3, 4], &[3, 4, 5]).await;
    assert_eq!(set.intersection_len(&[&set1, &set2], 0).await.unwrap(), 2);
    assert_eq!(set.intersection_len(&[&set1], 0).await.unwrap(), 3);
    assert_eq!(set.intersection_len(&[&set1], 2).await.unwrap(), 2);
    assert_eq!(all(&set).await, [1, 2, 3, 4]);
}

#[tokio::test]
async fn test_intersection() {
    let [set, set1, set2] = three_sets(&[5, 6], &[1, 2, 3], &[3, 4, 5]).await;
    assert_eq!(set.store_intersection(&[&set1, &set2]).await.unwrap(), 1);
    assert_eq!(all(&set).await, [3]);
}

#[tokio::test]
async fn test_read_intersection() {
    let [set, set1, set2] = three_sets(&[5, 7, 6], &[1, 2, 5], &[3, 4, 5]).await;
    assert_eq!(set.intersection(&[&set1, &set2]).await.unwrap(), [5]);
    assert_eq!(all(&set).await, [5, 6, 7]);
}

#[tokio::test]
async fn test_move() {
    let [set, other, _] = three_sets(&[1, 2], &[], &[]).await;
    assert!(set.move_to(&other, &1).await.unwrap());
    assert_eq!(all(&set).await, [2]);
    assert_eq!(all(&other).await, [1]);
}

#[tokio::test]
async fn test_move_no_member() {
    let [set, other, _] = three_sets(&[1], &[], &[]).await;
    assert!(!set.move_to(&other, &2).await.unwrap());
    assert_eq!(set.len().await.unwrap(), 1);
    assert_eq!(other.len().await.unwrap(), 0);
}

#[tokio::test]
async fn test_remove_all_empty() {
    let set = ints(&[1, 2, 3, 4, 5]).await;
    assert_eq!(set.remove_many::<i32>(&[]).await.unwrap(), 0);
}

#[tokio::test]
async fn test_remove_all() {
    let set = ints(&[1, 2, 3, 4, 5]).await;
    assert_eq!(set.remove_many::<i32>(&[]).await.unwrap(), 0);
    assert!(set.remove_many(&[&3, &2, &10, &6]).await.unwrap() > 0);
    assert_eq!(all(&set).await, [1, 4, 5]);
    assert!(set.remove_many(&[&4]).await.unwrap() > 0);
    assert_eq!(all(&set).await, [1, 5]);
    assert!(set.remove_many(&[&1, &5, &1, &5]).await.unwrap() > 0);
    assert!(set.is_empty().await.unwrap());
}

#[tokio::test]
async fn extend_counts_new_values() {
    let set = client().await.hash_set::<i32>(unique("set"));
    assert_eq!(set.extend([1, 2, 2].iter()).await.unwrap(), 2);
    assert_eq!(set.extend([2, 3].iter()).await.unwrap(), 1);
    assert_eq!(set.extend(std::iter::empty::<&i32>()).await.unwrap(), 0);
}
