use crate::common::{client, unique};
use futures::StreamExt;
use redissun::{Object, SortedSet};

async fn board() -> SortedSet<String, redissun::JsonCodec> {
    let set = client().await.sorted_set::<String>(unique("zset"));
    for (name, score) in [("c", 3.0), ("a", 1.0), ("d", 4.0), ("b", 2.0)] {
        assert!(set.insert(name, score).await.unwrap());
    }
    set
}

fn names(items: &[(String, f64)]) -> Vec<&str> {
    items.iter().map(|(name, _)| name.as_str()).collect()
}

#[tokio::test]
async fn insert_reports_new_values_and_updates_scores() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    assert!(set.insert("a", 1.0).await.unwrap());
    assert!(!set.insert("a", 5.0).await.unwrap());
    assert_eq!(set.score("a").await.unwrap(), Some(5.0));
    assert_eq!(set.score("missing").await.unwrap(), None);
    assert_eq!(set.len().await.unwrap(), 1);
}

#[tokio::test]
async fn len_contains_remove_and_clear() {
    let set = board().await;
    assert_eq!(set.len().await.unwrap(), 4);
    assert!(!set.is_empty().await.unwrap());
    assert!(set.contains("a").await.unwrap());
    assert!(set.remove("a").await.unwrap());
    assert!(!set.remove("a").await.unwrap());
    assert!(!set.contains("a").await.unwrap());
    set.clear().await.unwrap();
    assert!(set.is_empty().await.unwrap());
}

#[tokio::test]
async fn add_score_increments_and_creates() {
    let set = board().await;
    assert_eq!(set.add_score("a", 10.5).await.unwrap(), 11.5);
    assert_eq!(set.add_score("new", -2.0).await.unwrap(), -2.0);
    assert_eq!(set.first().await.unwrap().unwrap().0, "new");
}

#[tokio::test]
async fn rank_counts_from_the_lowest_or_the_highest_score() {
    let set = board().await;
    assert_eq!(set.rank("a").await.unwrap(), Some(0));
    assert_eq!(set.rank("d").await.unwrap(), Some(3));
    assert_eq!(set.rev_rank("d").await.unwrap(), Some(0));
    assert_eq!(set.rev_rank("a").await.unwrap(), Some(3));
    assert_eq!(set.rank("missing").await.unwrap(), None);
}

#[tokio::test]
async fn first_and_last_read_without_removing() {
    let set = board().await;
    assert_eq!(set.first().await.unwrap(), Some(("a".to_string(), 1.0)));
    assert_eq!(set.last().await.unwrap(), Some(("d".to_string(), 4.0)));
    assert_eq!(set.len().await.unwrap(), 4);
    let empty = client().await.sorted_set::<String>(unique("zset"));
    assert_eq!(empty.first().await.unwrap(), None);
    assert_eq!(empty.last().await.unwrap(), None);
}

#[tokio::test]
async fn pop_removes_from_either_end() {
    let set = board().await;
    assert_eq!(set.pop_first().await.unwrap(), Some(("a".to_string(), 1.0)));
    assert_eq!(set.pop_last().await.unwrap(), Some(("d".to_string(), 4.0)));
    assert_eq!(set.len().await.unwrap(), 2);
    set.clear().await.unwrap();
    assert_eq!(set.pop_first().await.unwrap(), None);
    assert_eq!(set.pop_last().await.unwrap(), None);
}

#[tokio::test]
async fn range_uses_an_exclusive_end_like_a_slice() {
    let set = board().await;
    let items = set.range(1..3).await.unwrap();
    assert_eq!(names(&items), ["b", "c"]);
    assert_eq!(items[0].1, 2.0);
    assert_eq!(
        names(&set.range(0..100).await.unwrap()),
        ["a", "b", "c", "d"]
    );
    assert!(set.range(2..2).await.unwrap().is_empty());
    assert!(set.range(9..12).await.unwrap().is_empty());
    assert_eq!(
        names(&set.range(usize::MAX - 1..usize::MAX).await.unwrap()),
        Vec::<&str>::new()
    );
}

#[tokio::test]
async fn rev_range_starts_from_the_highest_score() {
    let set = board().await;
    assert_eq!(names(&set.rev_range(0..2).await.unwrap()), ["d", "c"]);
}

#[tokio::test]
async fn score_ranges_are_inclusive() {
    let set = board().await;
    assert_eq!(
        names(&set.range_by_score(2.0..=3.0).await.unwrap()),
        ["b", "c"]
    );
    assert_eq!(set.count_by_score(2.0..=4.0).await.unwrap(), 3);
    assert_eq!(
        names(
            &set.range_by_score(f64::NEG_INFINITY..=f64::INFINITY)
                .await
                .unwrap()
        ),
        ["a", "b", "c", "d"]
    );
    assert!(set.range_by_score(10.0..=20.0).await.unwrap().is_empty());
    assert_eq!(set.remove_by_score(1.0..=2.0).await.unwrap(), 2);
    assert_eq!(set.len().await.unwrap(), 2);
}

#[tokio::test]
async fn equal_scores_are_ordered_by_the_encoded_value() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    for name in ["b", "c", "a"] {
        set.insert(name, 1.0).await.unwrap();
    }
    assert_eq!(names(&set.range(0..3).await.unwrap()), ["a", "b", "c"]);
}

#[tokio::test]
async fn iter_walks_more_than_one_page() {
    let set = client().await.sorted_set::<u32>(unique("zset"));
    for value in 0..250u32 {
        set.insert(&value, f64::from(value)).await.unwrap();
    }
    let items: Vec<(u32, f64)> = set
        .iter()
        .map(|item| item.unwrap())
        .collect::<Vec<_>>()
        .await;
    assert_eq!(items.len(), 250);
    assert!(items.iter().all(|(v, s)| *s == f64::from(*v)));
    let mut values: Vec<u32> = items.iter().map(|(v, _)| *v).collect();
    values.sort_unstable();
    values.dedup();
    assert_eq!(values, (0..250).collect::<Vec<_>>());
}

#[tokio::test]
async fn structured_values_round_trip() {
    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct Player {
        name: String,
    }
    let set = client().await.sorted_set::<Player>(unique("zset"));
    let ann = Player { name: "ann".into() };
    set.insert(&ann, 7.0).await.unwrap();
    assert_eq!(set.first().await.unwrap(), Some((ann, 7.0)));
}

#[tokio::test]
async fn object_methods_apply_to_the_key() {
    let set = board().await;
    assert!(set.exists().await.unwrap());
    assert!(set
        .expire(std::time::Duration::from_secs(60))
        .await
        .unwrap());
    assert!(set.ttl().await.unwrap().is_some());
    assert!(set.persist().await.unwrap());
    assert!(set.del().await.unwrap());
    assert!(!set.exists().await.unwrap());
}

#[tokio::test]
async fn concurrent_pops_hand_out_each_value_once() {
    let set = client().await.sorted_set::<u32>(unique("zset"));
    for value in 0..100u32 {
        set.insert(&value, f64::from(value)).await.unwrap();
    }
    let mut tasks = Vec::new();
    for _ in 0..10 {
        let set = set.clone();
        tasks.push(tokio::spawn(async move {
            let mut taken = Vec::new();
            while let Some((value, _)) = set.pop_first().await.unwrap() {
                taken.push(value);
            }
            taken
        }));
    }
    let mut all = Vec::new();
    for task in tasks {
        all.extend(task.await.unwrap());
    }
    all.sort_unstable();
    assert_eq!(all, (0..100).collect::<Vec<_>>());
}

async fn strings(entries: &[(f64, &str)]) -> SortedSet<String, redissun::JsonCodec> {
    let set = client().await.sorted_set::<String>(unique("zset"));
    for (score, value) in entries {
        set.insert(*value, *score).await.unwrap();
    }
    set
}

async fn numbers(entries: &[(f64, i32)]) -> SortedSet<i32, redissun::JsonCodec> {
    let set = client().await.sorted_set::<i32>(unique("zset"));
    for (score, value) in entries {
        set.insert(value, *score).await.unwrap();
    }
    set
}

async fn letters() -> SortedSet<String, redissun::JsonCodec> {
    strings(&[(0.0, "a"), (1.0, "b"), (2.0, "c"), (3.0, "d"), (4.0, "e")]).await
}

async fn seven() -> SortedSet<String, redissun::JsonCodec> {
    strings(&[
        (0.1, "a"),
        (0.2, "b"),
        (0.3, "c"),
        (0.4, "d"),
        (0.5, "e"),
        (0.6, "f"),
        (0.7, "g"),
    ])
    .await
}

async fn ordered<V>(set: &SortedSet<V, redissun::JsonCodec>) -> Vec<V>
where
    V: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
{
    set.range(0..usize::MAX)
        .await
        .unwrap()
        .into_iter()
        .map(|(v, _)| v)
        .collect()
}

fn values<V>(items: Vec<(V, f64)>) -> Vec<V> {
    items.into_iter().map(|(v, _)| v).collect()
}

fn sorted_names(items: Vec<(String, f64)>) -> Vec<String> {
    let mut names = values(items);
    names.sort();
    names
}

fn entry(value: &str, score: f64) -> (String, f64) {
    (value.to_string(), score)
}

fn sorted_entries(mut items: Vec<(String, f64)>) -> Vec<(String, f64)> {
    items.sort_by(|a, b| a.0.cmp(&b.0));
    items
}

async fn pair(
    first: &[(f64, &str)],
    second: &[(f64, &str)],
) -> [SortedSet<String, redissun::JsonCodec>; 3] {
    let client = client().await;
    let tag = unique("zsets");
    let sets = [
        client.sorted_set::<String>(format!("{{{tag}}}:simple1")),
        client.sorted_set::<String>(format!("{{{tag}}}:simple2")),
        client.sorted_set::<String>(format!("{{{tag}}}:out")),
    ];
    for (set, entries) in sets.iter().zip([first, second]) {
        for (score, value) in entries {
            set.insert(*value, *score).await.unwrap();
        }
    }
    sets
}

#[tokio::test]
async fn test_entries() {
    let set = strings(&[(1.1, "v1"), (1.2, "v2"), (1.3, "v3")]).await;
    let empty = client().await.sorted_set::<String>(unique("zset"));
    assert_eq!(empty.first().await.unwrap(), None);
    assert_eq!(set.first().await.unwrap(), Some(entry("v1", 1.1)));
    assert_eq!(set.last().await.unwrap(), Some(entry("v3", 1.3)));
}

#[tokio::test]
async fn test_poll_entry() {
    let set = strings(&[(1.1, "v1"), (1.2, "v2"), (1.3, "v3")]).await;
    assert_eq!(set.pop_first().await.unwrap(), Some(entry("v1", 1.1)));
    assert_eq!(set.pop_last().await.unwrap(), Some(entry("v3", 1.3)));
    assert_eq!(set.len().await.unwrap(), 1);
}

#[tokio::test]
async fn test_entry_scan_iterator() {
    let set = strings(&[(1.1, "v1"), (1.2, "v2"), (1.3, "v3")]).await;
    let entries: Vec<(String, f64)> = set
        .iter()
        .map(|item| item.unwrap())
        .collect::<Vec<_>>()
        .await;
    assert_eq!(
        sorted_entries(entries),
        [entry("v1", 1.1), entry("v2", 1.2), entry("v3", 1.3)]
    );
}

#[tokio::test]
async fn test_rank_entry() {
    let set = strings(&[(1.1, "v1"), (1.2, "v2"), (1.3, "v3")]).await;
    assert_eq!(set.rank_with_score("v1").await.unwrap(), Some((0, 1.1)));
    assert_eq!(set.rank_with_score("v3").await.unwrap(), Some((2, 1.3)));
    assert_eq!(set.rank_with_score("v4").await.unwrap(), None);
}

#[tokio::test]
async fn test_replace() {
    let set = numbers(&[(1.0, 10), (2.0, 20), (3.0, 30)]).await;
    assert!(set.replace(&10, &60).await.unwrap());
    assert_eq!(set.score(&60).await.unwrap(), Some(1.0));
    assert_eq!(set.len().await.unwrap(), 3);
    assert!(!set.replace(&10, &80).await.unwrap());
    assert_eq!(set.score(&60).await.unwrap(), Some(1.0));
    assert_eq!(set.score(&80).await.unwrap(), None);
    assert_eq!(set.len().await.unwrap(), 3);
}

#[tokio::test]
async fn test_random() {
    let set = numbers(&[(1.0, 10), (2.0, 20), (3.0, 30)]).await;
    assert!([10, 20, 30].contains(&set.random().await.unwrap().unwrap()));
    let many = set.random_many(2).await.unwrap();
    assert_eq!(many.len(), 2);
    assert!(many.iter().all(|v| [10, 20, 30].contains(v)));
    let entries = set.random_entries(2).await.unwrap();
    assert_eq!(entries.len(), 2);
    assert!(entries
        .iter()
        .all(|(v, s)| [(10, 1.0), (20, 2.0), (30, 3.0)].contains(&(*v, *s))));
}

#[tokio::test]
async fn test_count() {
    let set = strings(&[(0.0, "1"), (1.0, "4"), (2.0, "2"), (3.0, "5"), (4.0, "3")]).await;
    assert_eq!(set.count_by_score(0.0..3.0).await.unwrap(), 3);
}

#[tokio::test]
async fn test_read_all() {
    let set = strings(&[(0.0, "1"), (1.0, "4"), (2.0, "2"), (3.0, "5"), (4.0, "3")]).await;
    assert_eq!(
        sorted_names(set.range(0..usize::MAX).await.unwrap()),
        ["1", "2", "3", "4", "5"]
    );
}

#[tokio::test]
async fn test_add_all() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    assert_eq!(
        set.extend([("1", 0.1), ("2", 0.2), ("3", 0.3)])
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        set.range(0..usize::MAX).await.unwrap(),
        [entry("1", 0.1), entry("2", 0.2), entry("3", 0.3)]
    );
}

fn years() -> [(&'static str, f64); 4] {
    [
        ("1981", 111.0),
        ("1982", 112.0),
        ("1983", 113.0),
        ("1984", 114.0),
    ]
}

#[tokio::test]
async fn test_add_all_if_absent() {
    let set = strings(&[(10.0, "1981"), (11.0, "1984")]).await;
    assert_eq!(set.extend_if_absent(years()).await.unwrap(), 2);
    assert_eq!(set.score("1981").await.unwrap(), Some(10.0));
    assert_eq!(set.score("1984").await.unwrap(), Some(11.0));
    assert!(set
        .contains_all(&["1981", "1982", "1983", "1984"])
        .await
        .unwrap());
}

#[tokio::test]
async fn test_add_all_if_exist() {
    let set = strings(&[(10.0, "1981"), (11.0, "1984")]).await;
    assert_eq!(set.extend_if_exists(years()).await.unwrap(), 2);
    assert_eq!(set.score("1981").await.unwrap(), Some(111.0));
    assert_eq!(set.score("1984").await.unwrap(), Some(114.0));
}

fn mixed() -> [(&'static str, f64); 5] {
    [
        ("1981", 111.0),
        ("1982", 112.0),
        ("1983", 113.0),
        ("1984", 8.0),
        ("1985", 3.0),
    ]
}

#[tokio::test]
async fn test_add_all_if_greater() {
    let set = strings(&[(10.0, "1981"), (11.0, "1984"), (13.0, "1985")]).await;
    assert_eq!(set.extend_if_greater(mixed()).await.unwrap(), 3);
    assert_eq!(set.len().await.unwrap(), 5);
    assert_eq!(
        set.scores(&["1981", "1982", "1983", "1984", "1985"])
            .await
            .unwrap(),
        [
            Some(111.0),
            Some(112.0),
            Some(113.0),
            Some(11.0),
            Some(13.0)
        ]
    );
}

#[tokio::test]
async fn test_add_all_if_less() {
    let set = strings(&[(10.0, "1981"), (11.0, "1984"), (13.0, "1985")]).await;
    assert_eq!(set.extend_if_less(mixed()).await.unwrap(), 4);
    assert_eq!(set.len().await.unwrap(), 5);
    assert_eq!(
        set.scores(&["1981", "1982", "1983", "1984", "1985"])
            .await
            .unwrap(),
        [Some(10.0), Some(112.0), Some(113.0), Some(8.0), Some(3.0)]
    );
}

#[tokio::test]
async fn test_add_if_greater() {
    let set = strings(&[(123.0, "1980")]).await;
    assert!(!set.insert_if_greater("1980", 120.0).await.unwrap());
    assert_eq!(set.score("1980").await.unwrap(), Some(123.0));
    assert!(set.insert_if_greater("1980", 125.0).await.unwrap());
    assert_eq!(set.score("1980").await.unwrap(), Some(125.0));
}

#[tokio::test]
async fn test_add_if_less() {
    let set = strings(&[(123.0, "1980")]).await;
    assert!(set.insert_if_less("1980", 120.0).await.unwrap());
    assert_eq!(set.score("1980").await.unwrap(), Some(120.0));
    assert!(!set.insert_if_less("1980", 125.0).await.unwrap());
    assert_eq!(set.score("1980").await.unwrap(), Some(120.0));
}

#[tokio::test]
async fn test_add_if_exists() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    assert!(!set.insert_if_exists("1980", 123.81).await.unwrap());
    assert_eq!(set.score("1980").await.unwrap(), None);
    set.insert("1980", 111.0).await.unwrap();
    assert!(set.insert_if_exists("1980", 32.0).await.unwrap());
    assert_eq!(set.score("1980").await.unwrap(), Some(32.0));
}

#[tokio::test]
async fn test_try_add() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    assert!(set.try_insert("1980", 123.81).await.unwrap());
    assert!(!set.try_insert("1980", 99.0).await.unwrap());
    assert_eq!(set.score("1980").await.unwrap(), Some(123.81));
}

#[tokio::test]
async fn test_poll_last() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    assert_eq!(set.pop_last().await.unwrap(), None);
    for (value, score) in [("a", 0.1), ("b", 0.2), ("c", 0.3)] {
        set.insert(value, score).await.unwrap();
    }
    assert_eq!(set.pop_last().await.unwrap().unwrap().0, "c");
    assert_eq!(ordered(&set).await, ["a", "b"]);
}

#[tokio::test]
async fn test_poll_last_amount() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    assert!(set.pop_last_many(2).await.unwrap().is_empty());
    for (value, score) in [("a", 0.1), ("b", 0.2), ("c", 0.3)] {
        set.insert(value, score).await.unwrap();
    }
    assert_eq!(values(set.pop_last_many(2).await.unwrap()), ["b", "c"]);
    assert_eq!(ordered(&set).await, ["a"]);
}

#[tokio::test]
async fn test_poll_fist_amount() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    assert!(set.pop_first_many(2).await.unwrap().is_empty());
    for (value, score) in [("a", 0.1), ("b", 0.2), ("c", 0.3)] {
        set.insert(value, score).await.unwrap();
    }
    assert_eq!(values(set.pop_first_many(2).await.unwrap()), ["a", "b"]);
    assert_eq!(ordered(&set).await, ["c"]);
}

#[tokio::test]
async fn test_poll_first() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    assert_eq!(set.pop_first().await.unwrap(), None);
    for (value, score) in [("a", 0.1), ("b", 0.2), ("c", 0.3)] {
        set.insert(value, score).await.unwrap();
    }
    assert_eq!(set.pop_first().await.unwrap().unwrap().0, "a");
    assert_eq!(ordered(&set).await, ["b", "c"]);
}

#[tokio::test]
async fn test_poll_first_entries() {
    let set = strings(&[(0.1, "a"), (0.2, "b"), (0.3, "c")]).await;
    assert_eq!(
        set.pop_first_many(2).await.unwrap(),
        [entry("a", 0.1), entry("b", 0.2)]
    );
}

#[tokio::test]
async fn test_first_last() {
    let set = strings(&[(0.1, "a"), (0.2, "b"), (0.3, "c"), (0.4, "d")]).await;
    let empty = client().await.sorted_set::<String>(unique("zset"));
    assert_eq!(empty.first().await.unwrap(), None);
    assert_eq!(empty.last().await.unwrap(), None);
    assert_eq!(set.first().await.unwrap().unwrap().0, "a");
    assert_eq!(set.last().await.unwrap().unwrap().0, "d");
}

#[tokio::test]
async fn test_first_last_score() {
    let set = strings(&[(0.1, "a"), (0.2, "b"), (0.3, "c"), (0.4, "d")]).await;
    let empty = client().await.sorted_set::<String>(unique("zset"));
    assert_eq!(empty.first_score().await.unwrap(), None);
    assert_eq!(empty.last_score().await.unwrap(), None);
    assert_eq!(set.first_score().await.unwrap(), Some(0.1));
    assert_eq!(set.last_score().await.unwrap(), Some(0.4));
}

#[tokio::test]
async fn test_remove_range_by_score() {
    use std::ops::Bound::{Excluded, Included};
    let set = seven().await;
    assert_eq!(
        set.remove_by_score((Excluded(0.1), Included(0.3)))
            .await
            .unwrap(),
        2
    );
    assert_eq!(ordered(&set).await, ["a", "d", "e", "f", "g"]);
}

#[tokio::test]
async fn test_remove_range_by_score_negative_inf() {
    use std::ops::Bound::{Excluded, Included};
    let set = seven().await;
    assert_eq!(
        set.remove_by_score((Excluded(f64::NEG_INFINITY), Included(0.3)))
            .await
            .unwrap(),
        3
    );
    assert_eq!(ordered(&set).await, ["d", "e", "f", "g"]);
}

#[tokio::test]
async fn test_remove_range_by_score_positive_inf() {
    use std::ops::Bound::{Excluded, Included};
    let set = seven().await;
    assert_eq!(
        set.remove_by_score((Excluded(0.4), Included(f64::INFINITY)))
            .await
            .unwrap(),
        3
    );
    assert_eq!(ordered(&set).await, ["a", "b", "c", "d"]);
}

#[tokio::test]
async fn test_remove_range_by_rank() {
    let set = seven().await;
    assert_eq!(set.remove_range(0..2).await.unwrap(), 2);
    assert_eq!(ordered(&set).await, ["c", "d", "e", "f", "g"]);
}

#[tokio::test]
async fn test_rank() {
    let set = seven().await;
    assert_eq!(set.rev_rank("d").await.unwrap(), Some(3));
    assert_eq!(
        set.rev_rank_many(&["d", "a", "g", "abc", "f"])
            .await
            .unwrap(),
        [Some(3), Some(6), Some(0), None, Some(1)]
    );
    assert_eq!(set.rank("abc").await.unwrap(), None);
}

#[tokio::test]
async fn test_rev_rank() {
    let set = seven().await;
    assert_eq!(set.rev_rank("f").await.unwrap(), Some(1));
    assert_eq!(set.rev_rank("abc").await.unwrap(), None);
}

#[tokio::test]
async fn test_add_async() {
    let set = client().await.sorted_set::<i32>(unique("zset"));
    assert!(set.insert(&2, 0.323).await.unwrap());
    assert!(!set.insert(&2, 0.323).await.unwrap());
    assert!(set.contains(&2).await.unwrap());
}

#[tokio::test]
async fn test_add_and_get_rank() {
    let set = client().await.sorted_set::<i32>(unique("zset"));
    assert_eq!(set.insert_and_rank(&1, 0.3).await.unwrap(), 0);
    assert_eq!(set.insert_and_rank(&2, 0.4).await.unwrap(), 1);
    assert_eq!(set.insert_and_rank(&3, 0.2).await.unwrap(), 0);
    assert!(set.contains(&3).await.unwrap());
}

#[tokio::test]
async fn test_add_and_get_rev_rank() {
    let set = client().await.sorted_set::<i32>(unique("zset"));
    assert_eq!(set.insert_and_rev_rank(&1, 0.3).await.unwrap(), 0);
    assert_eq!(set.insert_and_rev_rank(&2, 0.4).await.unwrap(), 0);
    assert_eq!(set.insert_and_rev_rank(&3, 0.2).await.unwrap(), 2);
    assert!(set.contains(&3).await.unwrap());
}

#[tokio::test]
async fn test_remove_async() {
    let set = numbers(&[(0.11, 1), (0.22, 3), (0.33, 7)]).await;
    assert!(set.remove(&1).await.unwrap());
    assert!(!set.contains(&1).await.unwrap());
    assert_eq!(ordered(&set).await, [3, 7]);
    assert!(!set.remove(&1).await.unwrap());
    assert_eq!(ordered(&set).await, [3, 7]);
    set.remove(&3).await.unwrap();
    assert!(!set.contains(&3).await.unwrap());
    assert_eq!(ordered(&set).await, [7]);
}

#[tokio::test]
async fn test_iterator_next_next() {
    let set = strings(&[(1.0, "1"), (2.0, "4")]).await;
    let mut iter = Box::pin(set.iter());
    let mut seen = vec![
        iter.next().await.unwrap().unwrap().0,
        iter.next().await.unwrap().unwrap().0,
    ];
    seen.sort();
    assert_eq!(seen, ["1", "4"]);
    assert!(iter.next().await.is_none());
}

#[tokio::test]
async fn test_iterator_remove() {
    let set = strings(&[(1.0, "1"), (2.0, "4"), (3.0, "2"), (4.0, "5"), (5.0, "3")]).await;
    let all: Vec<(String, f64)> = set
        .iter()
        .map(|item| item.unwrap())
        .collect::<Vec<_>>()
        .await;
    for (value, _) in &all {
        if value == "2" {
            set.remove(value).await.unwrap();
        }
    }
    assert_eq!(ordered(&set).await, ["1", "4", "5", "3"]);
    let all: Vec<(String, f64)> = set
        .iter()
        .map(|item| item.unwrap())
        .collect::<Vec<_>>()
        .await;
    let mut iterations = 0;
    for (value, _) in &all {
        set.remove(value).await.unwrap();
        iterations += 1;
    }
    assert_eq!(iterations, 4);
    assert_eq!(set.len().await.unwrap(), 0);
    assert!(set.is_empty().await.unwrap());
}

#[tokio::test]
async fn test_iterator_sequence() {
    let set = client().await.sorted_set::<i32>(unique("zset"));
    let entries: Vec<(i32, f64)> = (0..1000).map(|i| (i, f64::from(i))).collect();
    set.extend(entries.iter().map(|(v, s)| (v, *s)))
        .await
        .unwrap();
    let read: std::collections::HashSet<i32> = set
        .iter()
        .map(|item| item.unwrap().0)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect();
    assert_eq!(read, (0..1000).collect());
}

#[tokio::test]
async fn test_retain_all() {
    let set = client().await.sorted_set::<i32>(unique("zset"));
    let entries: Vec<(i32, f64)> = (0..20_000).map(|i| (i, f64::from(i * 10))).collect();
    set.extend(entries.iter().map(|(v, s)| (v, *s)))
        .await
        .unwrap();
    assert!(set.retain(&[&1, &2]).await.unwrap());
    assert_eq!(ordered(&set).await, [1, 2]);
    assert_eq!(set.len().await.unwrap(), 2);
    assert_eq!(set.score(&1).await.unwrap(), Some(10.0));
    assert_eq!(set.score(&2).await.unwrap(), Some(20.0));
}

#[tokio::test]
async fn test_remove_all() {
    let set = numbers(&[(0.1, 1), (0.2, 2), (0.3, 3)]).await;
    assert_eq!(set.remove_many(&[&1, &2]).await.unwrap(), 2);
    assert_eq!(ordered(&set).await, [3]);
    assert_eq!(set.len().await.unwrap(), 1);
}

#[tokio::test]
async fn test_sort() {
    let set = client().await.sorted_set::<i32>(unique("zset"));
    for (score, value) in [
        (4.0, 2),
        (5.0, 3),
        (3.0, 1),
        (6.0, 4),
        (1000.0, 10),
        (1.0, -1),
        (2.0, 0),
    ] {
        assert!(set.insert(&value, score).await.unwrap());
    }
    assert_eq!(ordered(&set).await, [-1, 0, 1, 2, 3, 4, 10]);
}

#[tokio::test]
async fn test_remove() {
    let set = numbers(&[(4.0, 5), (2.0, 3), (0.0, 1), (1.0, 2), (3.0, 4)]).await;
    assert!(!set.remove(&0).await.unwrap());
    assert!(set.remove(&3).await.unwrap());
    assert_eq!(ordered(&set).await, [1, 2, 4, 5]);
}

#[tokio::test]
async fn test_contains_all() {
    let set = client().await.sorted_set::<i32>(unique("zset"));
    let entries: Vec<(i32, f64)> = (0..200).map(|i| (i, f64::from(i))).collect();
    set.extend(entries.iter().map(|(v, s)| (v, *s)))
        .await
        .unwrap();
    assert!(set.contains_all(&[&30, &11]).await.unwrap());
    assert!(!set.contains_all(&[&30, &711, &11]).await.unwrap());
    assert!(set.contains_all::<i32>(&[]).await.unwrap());
}

#[tokio::test]
async fn test_to_array() {
    let set = strings(&[(0.0, "1"), (1.0, "4"), (2.0, "2"), (3.0, "5"), (4.0, "3")]).await;
    assert_eq!(ordered(&set).await, ["1", "4", "2", "5", "3"]);
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
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
async fn test_contains() {
    let set = client().await.sorted_set::<TestObject>(unique("zset"));
    for (score, (name, value)) in [("1", "2"), ("1", "2"), ("2", "3"), ("3", "4"), ("5", "6")]
        .into_iter()
        .enumerate()
    {
        set.insert(&object(name, value), score as f64)
            .await
            .unwrap();
    }
    assert!(set.contains(&object("2", "3")).await.unwrap());
    assert!(set.contains(&object("1", "2")).await.unwrap());
    assert!(!set.contains(&object("1", "9")).await.unwrap());
}

#[tokio::test]
async fn test_duplicates() {
    let set = client().await.sorted_set::<TestObject>(unique("zset"));
    assert!(set.insert(&object("1", "2"), 0.0).await.unwrap());
    assert!(!set.insert(&object("1", "2"), 0.0).await.unwrap());
    assert!(set.insert(&object("2", "3"), 2.0).await.unwrap());
    assert!(set.insert(&object("3", "4"), 3.0).await.unwrap());
    assert!(set.insert(&object("5", "6"), 4.0).await.unwrap());
    assert_eq!(set.len().await.unwrap(), 4);
}

#[tokio::test]
async fn test_size() {
    let set = numbers(&[
        (0.0, 1),
        (1.0, 2),
        (2.0, 3),
        (2.0, 3),
        (3.0, 4),
        (4.0, 5),
        (4.0, 5),
    ])
    .await;
    assert_eq!(set.len().await.unwrap(), 5);
}

#[tokio::test]
async fn test_value_range() {
    let set = numbers(&[(0.0, 1), (1.0, 2), (2.0, 3), (3.0, 4), (4.0, 5), (4.0, 5)]).await;
    assert_eq!(
        values(set.range(0..usize::MAX).await.unwrap()),
        [1, 2, 3, 4, 5]
    );
}

#[tokio::test]
async fn test_value_range_reversed_infinity() {
    let set = numbers(&[(1.0, 1), (2.0, 2)]).await;
    assert_eq!(
        values(set.rev_range_by_score_limit(.., 0, 1).await.unwrap()),
        [2]
    );
}

#[tokio::test]
async fn test_value_range_reversed() {
    let set = numbers(&[(0.0, 1), (1.0, 2), (2.0, 3), (3.0, 4), (4.0, 5), (4.0, 5)]).await;
    assert_eq!(
        values(set.rev_range(0..usize::MAX).await.unwrap()),
        [5, 4, 3, 2, 1]
    );
}

#[tokio::test]
async fn test_entry_range() {
    let set = numbers(&[(10.0, 1), (20.0, 2), (30.0, 3), (40.0, 4), (50.0, 5)]).await;
    assert_eq!(
        set.range(0..usize::MAX).await.unwrap(),
        [(1, 10.0), (2, 20.0), (3, 30.0), (4, 40.0), (5, 50.0)]
    );
}

#[tokio::test]
async fn test_entry_range_reversed() {
    let set = numbers(&[(10.0, 1), (20.0, 2), (30.0, 3), (40.0, 4), (50.0, 5)]).await;
    assert_eq!(
        set.rev_range(0..usize::MAX).await.unwrap(),
        [(5, 50.0), (4, 40.0), (3, 30.0), (2, 20.0), (1, 10.0)]
    );
}

#[tokio::test]
async fn test_value_range_limit() {
    let set = letters().await;
    assert_eq!(
        values(set.range_by_score_limit(1.0..4.0, 1, 2).await.unwrap()),
        ["c", "d"]
    );
}

#[tokio::test]
async fn test_value_range_alpha() {
    let set = letters().await;
    assert_eq!(
        values(set.range_by_score(1.0..4.0).await.unwrap()),
        ["b", "c", "d"]
    );
}

#[tokio::test]
async fn test_value_range_reversed_limit() {
    let set = letters().await;
    assert_eq!(
        values(set.rev_range_by_score_limit(1.0..4.0, 1, 2).await.unwrap()),
        ["c", "b"]
    );
}

#[tokio::test]
async fn test_value_range_reversed_alpha() {
    let set = letters().await;
    assert_eq!(
        values(set.rev_range_by_score(1.0..4.0).await.unwrap()),
        ["d", "c", "b"]
    );
}

#[tokio::test]
async fn test_value_range_negative_inf() {
    let set = letters().await;
    assert_eq!(
        values(
            set.range_by_score_limit(f64::NEG_INFINITY..4.0, 1, 2)
                .await
                .unwrap()
        ),
        ["b", "c"]
    );
}

#[tokio::test]
async fn test_value_range_positive_inf() {
    use std::ops::Bound::{Excluded, Included};
    let set = letters().await;
    assert_eq!(
        values(
            set.range_by_score_limit((Included(1.0), Excluded(f64::INFINITY)), 1, 2)
                .await
                .unwrap()
        ),
        ["c", "d"]
    );
}

#[tokio::test]
async fn test_entry_range_alpha() {
    let set = letters().await;
    assert_eq!(
        set.range_by_score_limit(1.0..4.0, 1, 2).await.unwrap(),
        [entry("c", 2.0), entry("d", 3.0)]
    );
}

#[tokio::test]
async fn test_entry_range_reversed_alpha() {
    let set = letters().await;
    assert_eq!(
        set.rev_range_by_score_limit(1.0..4.0, 1, 2).await.unwrap(),
        [entry("c", 2.0), entry("b", 1.0)]
    );
}

#[tokio::test]
async fn test_entry_range_negative_inf() {
    let set = letters().await;
    assert_eq!(
        set.range_by_score_limit(..4.0, 1, 2).await.unwrap(),
        [entry("b", 1.0), entry("c", 2.0)]
    );
}

#[tokio::test]
async fn test_entry_range_positive_inf() {
    let set = letters().await;
    assert_eq!(
        set.range_by_score_limit(1.0.., 1, 2).await.unwrap(),
        [entry("c", 2.0), entry("d", 3.0)]
    );
}

#[tokio::test]
async fn test_add_and_get() {
    let set = strings(&[(1.0, "100")]).await;
    assert_eq!(set.add_score("100", 11.0).await.unwrap(), 12.0);
    assert_eq!(set.score("100").await.unwrap(), Some(12.0));
    set.insert("1", 100.2).await.unwrap();
    assert_eq!(set.add_score("1", 12.1).await.unwrap(), 112.3);
    assert_eq!(set.score("1").await.unwrap(), Some(112.3));
}

#[tokio::test]
async fn test_add_and_get_all() {
    let set = strings(&[(100.2, "1")]).await;
    assert_eq!(set.add_score("1", 12.1).await.unwrap(), 112.3);
    assert_eq!(set.score("1").await.unwrap(), Some(112.3));
    assert_eq!(
        set.scores(&["1", "42", "100"]).await.unwrap(),
        [Some(112.3), None, None]
    );
}

#[tokio::test]
async fn test_add_score_and_get_rank() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    assert_eq!(set.add_score_and_rank("12", 12.0).await.unwrap(), 0);
    assert_eq!(set.add_score_and_rank("15", 10.0).await.unwrap(), 0);
    assert_eq!(set.rank("12").await.unwrap(), Some(1));
    assert_eq!(set.rank("15").await.unwrap(), Some(0));
    assert_eq!(set.add_score_and_rank("12", 2.0).await.unwrap(), 1);
    assert_eq!(set.score("12").await.unwrap(), Some(14.0));
}

#[tokio::test]
async fn test_add_score_and_get_rev_rank() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    assert_eq!(set.add_score_and_rev_rank("12", 12.0).await.unwrap(), 0);
    assert_eq!(set.add_score_and_rev_rank("15", 10.0).await.unwrap(), 1);
    assert_eq!(set.rev_rank("12").await.unwrap(), Some(0));
    assert_eq!(set.rev_rank("15").await.unwrap(), Some(1));
    assert_eq!(set.add_score_and_rev_rank("12", 2.0).await.unwrap(), 0);
    assert_eq!(set.add_score_and_rev_rank("15", -1.0).await.unwrap(), 1);
    assert_eq!(set.score("12").await.unwrap(), Some(14.0));
}

#[tokio::test]
async fn test_add_and_get_rev_rank_collection() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    set.extend([("one", 1.0), ("three", 3.0), ("two", 2.0)])
        .await
        .unwrap();
    assert_eq!(
        set.rev_rank_many(&["one", "three", "two"]).await.unwrap(),
        [Some(2), Some(0), Some(1)]
    );
}

#[tokio::test]
async fn test_read_intersection() {
    let [set1, set2, _] = pair(
        &[(1.0, "one"), (2.0, "two"), (2.0, "four")],
        &[(1.0, "one"), (2.0, "two"), (3.0, "three")],
    )
    .await;
    assert_eq!(
        sorted_names(set1.intersection(&[&set2]).await.unwrap()),
        ["one", "two"]
    );
}

#[tokio::test]
async fn test_read_intersection_entries() {
    let [set1, set2, _] = pair(
        &[(1.0, "one"), (2.0, "two"), (2.0, "four")],
        &[(1.0, "one"), (3.0, "two"), (3.0, "three")],
    )
    .await;
    assert_eq!(
        sorted_entries(set1.intersection(&[&set2]).await.unwrap()),
        [entry("one", 2.0), entry("two", 5.0)]
    );
    assert_eq!(
        sorted_entries(
            set1.intersection_with(&[&set2], &[2.0, 3.0], Default::default())
                .await
                .unwrap()
        ),
        [entry("one", 5.0), entry("two", 13.0)]
    );
}

#[tokio::test]
async fn test_intersection() {
    let [set1, set2, out] = pair(
        &[(1.0, "one"), (2.0, "two")],
        &[(1.0, "one"), (2.0, "two"), (3.0, "three")],
    )
    .await;
    assert_eq!(out.store_intersection(&[&set1, &set2]).await.unwrap(), 2);
    assert_eq!(ordered(&out).await, ["one", "two"]);
    assert_eq!(out.score("one").await.unwrap(), Some(2.0));
    assert_eq!(out.score("two").await.unwrap(), Some(4.0));
}

#[tokio::test]
async fn test_intersection_empty() {
    let [set1, set2, out] = pair(
        &[(1.0, "one"), (2.0, "two")],
        &[(3.0, "three"), (4.0, "four")],
    )
    .await;
    assert_eq!(out.store_intersection(&[&set1, &set2]).await.unwrap(), 0);
    assert!(out.is_empty().await.unwrap());
}

#[tokio::test]
async fn test_intersection_with_weight() {
    let [set1, set2, out] = pair(
        &[(1.0, "one"), (2.0, "two")],
        &[(1.0, "one"), (2.0, "two"), (3.0, "three")],
    )
    .await;
    assert_eq!(
        out.store_intersection_with(&[&set1, &set2], &[2.0, 3.0], Default::default())
            .await
            .unwrap(),
        2
    );
    assert_eq!(ordered(&out).await, ["one", "two"]);
    assert_eq!(out.score("one").await.unwrap(), Some(5.0));
    assert_eq!(out.score("two").await.unwrap(), Some(10.0));
}

#[tokio::test]
async fn test_read_union() {
    let [set1, set2, _] = pair(
        &[(1.0, "one"), (2.0, "two"), (4.0, "four")],
        &[(1.0, "one"), (2.0, "two"), (3.0, "three")],
    )
    .await;
    assert_eq!(
        sorted_names(set1.union(&[&set2]).await.unwrap()),
        ["four", "one", "three", "two"]
    );
}

#[tokio::test]
async fn test_read_union_entries() {
    let [set1, set2, _] = pair(
        &[(1.0, "one"), (2.0, "two"), (2.0, "four")],
        &[(1.0, "one"), (3.0, "two"), (3.0, "three")],
    )
    .await;
    assert_eq!(
        sorted_entries(set1.union(&[&set2]).await.unwrap()),
        [
            entry("four", 2.0),
            entry("one", 2.0),
            entry("three", 3.0),
            entry("two", 5.0)
        ]
    );
    assert_eq!(
        sorted_entries(
            set1.union_with(&[&set2], &[2.0, 3.0], Default::default())
                .await
                .unwrap()
        ),
        [
            entry("four", 4.0),
            entry("one", 5.0),
            entry("three", 9.0),
            entry("two", 13.0)
        ]
    );
}

#[tokio::test]
async fn test_union() {
    let [set1, set2, out] = pair(
        &[(1.0, "one"), (2.0, "two")],
        &[(1.0, "one"), (2.0, "two"), (3.0, "three")],
    )
    .await;
    assert_eq!(out.store_union(&[&set1, &set2]).await.unwrap(), 3);
    assert_eq!(ordered(&out).await, ["one", "three", "two"]);
    assert_eq!(
        out.scores(&["one", "two", "three"]).await.unwrap(),
        [Some(2.0), Some(4.0), Some(3.0)]
    );
}

#[tokio::test]
async fn test_union_with_weight() {
    let [set1, set2, out] = pair(
        &[(1.0, "one"), (2.0, "two")],
        &[(1.0, "one"), (2.0, "two"), (3.0, "three")],
    )
    .await;
    assert_eq!(
        out.store_union_with(&[&set1, &set2], &[2.0, 3.0], Default::default())
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        out.scores(&["one", "two", "three"]).await.unwrap(),
        [Some(5.0), Some(10.0), Some(9.0)]
    );
}

#[tokio::test]
async fn test_read_diff_entries() {
    let [set1, set2, _] = pair(
        &[(1.0, "one"), (2.0, "two")],
        &[(1.0, "one"), (2.0, "two"), (3.0, "three")],
    )
    .await;
    assert_eq!(
        set2.difference(&[&set1]).await.unwrap(),
        [entry("three", 3.0)]
    );
    assert!(set1.difference(&[&set2]).await.unwrap().is_empty());
    set1.insert("three", 3.0).await.unwrap();
    assert!(set2.difference(&[&set1]).await.unwrap().is_empty());
}

#[tokio::test]
async fn score_ranges_accept_any_bounds() {
    use std::ops::Bound::{Excluded, Included};
    let set = seven().await;
    assert_eq!(set.count_by_score(..).await.unwrap(), 7);
    assert_eq!(set.count_by_score(0.2..0.4).await.unwrap(), 2);
    assert_eq!(
        set.count_by_score((Excluded(0.2), Included(0.4)))
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        values(set.rev_range_by_score(0.5..).await.unwrap()),
        ["g", "f", "e"]
    );
}
