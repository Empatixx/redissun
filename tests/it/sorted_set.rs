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

// Redisson: testEntries
#[tokio::test]
async fn first_and_last_return_the_entry_with_its_score() {
    let set = strings(&[(1.1, "v1"), (1.2, "v2"), (1.3, "v3")]).await;
    let empty = client().await.sorted_set::<String>(unique("zset"));
    assert_eq!(empty.first().await.unwrap(), None);
    assert_eq!(set.first().await.unwrap(), Some(entry("v1", 1.1)));
    assert_eq!(set.last().await.unwrap(), Some(entry("v3", 1.3)));
}

// Redisson: testPollEntry
#[tokio::test]
async fn pop_first_and_pop_last_return_entries_with_scores() {
    let set = strings(&[(1.1, "v1"), (1.2, "v2"), (1.3, "v3")]).await;
    assert_eq!(set.pop_first().await.unwrap(), Some(entry("v1", 1.1)));
    assert_eq!(set.pop_last().await.unwrap(), Some(entry("v3", 1.3)));
    assert_eq!(set.len().await.unwrap(), 1);
}

// Redisson: testEntryScanIterator
#[tokio::test]
async fn iter_yields_every_entry_with_its_score() {
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

// Redisson: testRankEntry
#[tokio::test]
async fn rank_with_score_returns_rank_and_score_of_a_value() {
    let set = strings(&[(1.1, "v1"), (1.2, "v2"), (1.3, "v3")]).await;
    assert_eq!(set.rank_with_score("v1").await.unwrap(), Some((0, 1.1)));
    assert_eq!(set.rank_with_score("v3").await.unwrap(), Some((2, 1.3)));
    assert_eq!(set.rank_with_score("v4").await.unwrap(), None);
}

// Redisson: testReplace
#[tokio::test]
async fn replace_swaps_a_value_and_keeps_its_score() {
    let set = numbers(&[(1.0, 10), (2.0, 20), (3.0, 30)]).await;
    assert!(set.replace(&10, &60).await.unwrap());
    assert_eq!(set.score(&60).await.unwrap(), Some(1.0));
    assert_eq!(set.len().await.unwrap(), 3);
    assert!(!set.replace(&10, &80).await.unwrap());
    assert_eq!(set.score(&60).await.unwrap(), Some(1.0));
    assert_eq!(set.score(&80).await.unwrap(), None);
    assert_eq!(set.len().await.unwrap(), 3);
}

// Redisson: testRandom
#[tokio::test]
async fn random_picks_return_values_from_the_set() {
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

// Redisson: testCount
#[tokio::test]
async fn count_by_score_counts_values_in_a_half_open_range() {
    let set = strings(&[(0.0, "1"), (1.0, "4"), (2.0, "2"), (3.0, "5"), (4.0, "3")]).await;
    assert_eq!(set.count_by_score(0.0..3.0).await.unwrap(), 3);
}

// Redisson: testReadAll
#[tokio::test]
async fn full_range_reads_every_value() {
    let set = strings(&[(0.0, "1"), (1.0, "4"), (2.0, "2"), (3.0, "5"), (4.0, "3")]).await;
    assert_eq!(
        sorted_names(set.range(0..usize::MAX).await.unwrap()),
        ["1", "2", "3", "4", "5"]
    );
}

// Redisson: testAddAll
#[tokio::test]
async fn extend_inserts_all_values_with_their_scores() {
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

// Redisson: testAddAllIfAbsent
#[tokio::test]
async fn extend_if_absent_keeps_scores_of_existing_values() {
    let set = strings(&[(10.0, "1981"), (11.0, "1984")]).await;
    assert_eq!(set.extend_if_absent(years()).await.unwrap(), 2);
    assert_eq!(set.score("1981").await.unwrap(), Some(10.0));
    assert_eq!(set.score("1984").await.unwrap(), Some(11.0));
    assert!(set
        .contains_all(&["1981", "1982", "1983", "1984"])
        .await
        .unwrap());
}

// Redisson: testAddAllIfExist
#[tokio::test]
async fn extend_if_exists_updates_only_existing_values() {
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

// Redisson: testAddAllIfGreater
#[tokio::test]
async fn extend_if_greater_raises_scores_and_adds_new_values() {
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

// Redisson: testAddAllIfLess
#[tokio::test]
async fn extend_if_less_lowers_scores_and_adds_new_values() {
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

// Redisson: testAddIfGreater
#[tokio::test]
async fn insert_if_greater_updates_only_with_a_higher_score() {
    let set = strings(&[(123.0, "1980")]).await;
    assert!(!set.insert_if_greater("1980", 120.0).await.unwrap());
    assert_eq!(set.score("1980").await.unwrap(), Some(123.0));
    assert!(set.insert_if_greater("1980", 125.0).await.unwrap());
    assert_eq!(set.score("1980").await.unwrap(), Some(125.0));
}

// Redisson: testAddIfLess
#[tokio::test]
async fn insert_if_less_updates_only_with_a_lower_score() {
    let set = strings(&[(123.0, "1980")]).await;
    assert!(set.insert_if_less("1980", 120.0).await.unwrap());
    assert_eq!(set.score("1980").await.unwrap(), Some(120.0));
    assert!(!set.insert_if_less("1980", 125.0).await.unwrap());
    assert_eq!(set.score("1980").await.unwrap(), Some(120.0));
}

// Redisson: testAddIfExists
#[tokio::test]
async fn insert_if_exists_updates_only_an_existing_value() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    assert!(!set.insert_if_exists("1980", 123.81).await.unwrap());
    assert_eq!(set.score("1980").await.unwrap(), None);
    set.insert("1980", 111.0).await.unwrap();
    assert!(set.insert_if_exists("1980", 32.0).await.unwrap());
    assert_eq!(set.score("1980").await.unwrap(), Some(32.0));
}

// Redisson: testTryAdd
#[tokio::test]
async fn try_insert_does_not_overwrite_an_existing_value() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    assert!(set.try_insert("1980", 123.81).await.unwrap());
    assert!(!set.try_insert("1980", 99.0).await.unwrap());
    assert_eq!(set.score("1980").await.unwrap(), Some(123.81));
}

// Redisson: testPollLast
#[tokio::test]
async fn pop_last_removes_the_highest_scored_value() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    assert_eq!(set.pop_last().await.unwrap(), None);
    for (value, score) in [("a", 0.1), ("b", 0.2), ("c", 0.3)] {
        set.insert(value, score).await.unwrap();
    }
    assert_eq!(set.pop_last().await.unwrap().unwrap().0, "c");
    assert_eq!(ordered(&set).await, ["a", "b"]);
}

// Redisson: testPollLastAmount
#[tokio::test]
async fn pop_last_many_removes_the_highest_scored_values() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    assert!(set.pop_last_many(2).await.unwrap().is_empty());
    for (value, score) in [("a", 0.1), ("b", 0.2), ("c", 0.3)] {
        set.insert(value, score).await.unwrap();
    }
    assert_eq!(values(set.pop_last_many(2).await.unwrap()), ["b", "c"]);
    assert_eq!(ordered(&set).await, ["a"]);
}

// Redisson: testPollFistAmount
#[tokio::test]
async fn pop_first_many_removes_the_lowest_scored_values() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    assert!(set.pop_first_many(2).await.unwrap().is_empty());
    for (value, score) in [("a", 0.1), ("b", 0.2), ("c", 0.3)] {
        set.insert(value, score).await.unwrap();
    }
    assert_eq!(values(set.pop_first_many(2).await.unwrap()), ["a", "b"]);
    assert_eq!(ordered(&set).await, ["c"]);
}

// Redisson: testPollFirst
#[tokio::test]
async fn pop_first_removes_the_lowest_scored_value() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    assert_eq!(set.pop_first().await.unwrap(), None);
    for (value, score) in [("a", 0.1), ("b", 0.2), ("c", 0.3)] {
        set.insert(value, score).await.unwrap();
    }
    assert_eq!(set.pop_first().await.unwrap().unwrap().0, "a");
    assert_eq!(ordered(&set).await, ["b", "c"]);
}

// Redisson: testPollFirstEntries
#[tokio::test]
async fn pop_first_many_returns_entries_with_their_scores() {
    let set = strings(&[(0.1, "a"), (0.2, "b"), (0.3, "c")]).await;
    assert_eq!(
        set.pop_first_many(2).await.unwrap(),
        [entry("a", 0.1), entry("b", 0.2)]
    );
}

// Redisson: testFirstLast
#[tokio::test]
async fn first_and_last_return_the_values_at_both_ends() {
    let set = strings(&[(0.1, "a"), (0.2, "b"), (0.3, "c"), (0.4, "d")]).await;
    let empty = client().await.sorted_set::<String>(unique("zset"));
    assert_eq!(empty.first().await.unwrap(), None);
    assert_eq!(empty.last().await.unwrap(), None);
    assert_eq!(set.first().await.unwrap().unwrap().0, "a");
    assert_eq!(set.last().await.unwrap().unwrap().0, "d");
}

// Redisson: testFirstLastScore
#[tokio::test]
async fn first_score_and_last_score_return_the_extreme_scores() {
    let set = strings(&[(0.1, "a"), (0.2, "b"), (0.3, "c"), (0.4, "d")]).await;
    let empty = client().await.sorted_set::<String>(unique("zset"));
    assert_eq!(empty.first_score().await.unwrap(), None);
    assert_eq!(empty.last_score().await.unwrap(), None);
    assert_eq!(set.first_score().await.unwrap(), Some(0.1));
    assert_eq!(set.last_score().await.unwrap(), Some(0.4));
}

// Redisson: testRemoveRangeByScore
#[tokio::test]
async fn remove_by_score_honors_excluded_and_included_bounds() {
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

// Redisson: testRemoveRangeByScoreNegativeInf
#[tokio::test]
async fn remove_by_score_accepts_a_negative_infinite_start() {
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

// Redisson: testRemoveRangeByScorePositiveInf
#[tokio::test]
async fn remove_by_score_accepts_a_positive_infinite_end() {
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

// Redisson: testRemoveRangeByRank
#[tokio::test]
async fn remove_range_drops_values_by_rank() {
    let set = seven().await;
    assert_eq!(set.remove_range(0..2).await.unwrap(), 2);
    assert_eq!(ordered(&set).await, ["c", "d", "e", "f", "g"]);
}

// Redisson: testRank
#[tokio::test]
async fn rev_rank_many_ranks_values_and_reports_missing_as_none() {
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

// Redisson: testRevRank
#[tokio::test]
async fn rev_rank_counts_from_the_highest_score() {
    let set = seven().await;
    assert_eq!(set.rev_rank("f").await.unwrap(), Some(1));
    assert_eq!(set.rev_rank("abc").await.unwrap(), None);
}

// Redisson: testAddAsync
#[tokio::test]
async fn insert_reports_whether_the_value_was_new() {
    let set = client().await.sorted_set::<i32>(unique("zset"));
    assert!(set.insert(&2, 0.323).await.unwrap());
    assert!(!set.insert(&2, 0.323).await.unwrap());
    assert!(set.contains(&2).await.unwrap());
}

// Redisson: testAddAndGetRank
#[tokio::test]
async fn insert_and_rank_returns_the_rank_of_the_new_value() {
    let set = client().await.sorted_set::<i32>(unique("zset"));
    assert_eq!(set.insert_and_rank(&1, 0.3).await.unwrap(), 0);
    assert_eq!(set.insert_and_rank(&2, 0.4).await.unwrap(), 1);
    assert_eq!(set.insert_and_rank(&3, 0.2).await.unwrap(), 0);
    assert!(set.contains(&3).await.unwrap());
}

// Redisson: testAddAndGetRevRank
#[tokio::test]
async fn insert_and_rev_rank_returns_the_reverse_rank_of_the_new_value() {
    let set = client().await.sorted_set::<i32>(unique("zset"));
    assert_eq!(set.insert_and_rev_rank(&1, 0.3).await.unwrap(), 0);
    assert_eq!(set.insert_and_rev_rank(&2, 0.4).await.unwrap(), 0);
    assert_eq!(set.insert_and_rev_rank(&3, 0.2).await.unwrap(), 2);
    assert!(set.contains(&3).await.unwrap());
}

// Redisson: testRemoveAsync
#[tokio::test]
async fn remove_deletes_a_value_once_and_keeps_the_rest() {
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

// Redisson: testIteratorNextNext
#[tokio::test]
async fn iter_ends_after_the_last_value() {
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

// Redisson: testIteratorRemove
#[tokio::test]
async fn removing_values_while_iterating_empties_the_set() {
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

// Redisson: testIteratorSequence
#[tokio::test]
async fn iter_yields_a_thousand_values_exactly_once() {
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

// Redisson: testRetainAll
#[tokio::test]
async fn retain_keeps_only_the_given_values_and_their_scores() {
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

// Redisson: testRemoveAll
#[tokio::test]
async fn remove_many_deletes_the_given_values() {
    let set = numbers(&[(0.1, 1), (0.2, 2), (0.3, 3)]).await;
    assert_eq!(set.remove_many(&[&1, &2]).await.unwrap(), 2);
    assert_eq!(ordered(&set).await, [3]);
    assert_eq!(set.len().await.unwrap(), 1);
}

// Redisson: testSort
#[tokio::test]
async fn values_are_ordered_by_score() {
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

// Redisson: testRemove
#[tokio::test]
async fn remove_reports_whether_the_value_was_present() {
    let set = numbers(&[(4.0, 5), (2.0, 3), (0.0, 1), (1.0, 2), (3.0, 4)]).await;
    assert!(!set.remove(&0).await.unwrap());
    assert!(set.remove(&3).await.unwrap());
    assert_eq!(ordered(&set).await, [1, 2, 4, 5]);
}

// Redisson: testContainsAll
#[tokio::test]
async fn contains_all_is_true_only_when_every_value_is_present() {
    let set = client().await.sorted_set::<i32>(unique("zset"));
    let entries: Vec<(i32, f64)> = (0..200).map(|i| (i, f64::from(i))).collect();
    set.extend(entries.iter().map(|(v, s)| (v, *s)))
        .await
        .unwrap();
    assert!(set.contains_all(&[&30, &11]).await.unwrap());
    assert!(!set.contains_all(&[&30, &711, &11]).await.unwrap());
    assert!(set.contains_all::<i32>(&[]).await.unwrap());
}

// Redisson: testToArray
#[tokio::test]
async fn full_range_lists_values_in_score_order() {
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

// Redisson: testContains
#[tokio::test]
async fn contains_finds_structured_values_by_equality() {
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

// Redisson: testDuplicates
#[tokio::test]
async fn inserting_a_duplicate_structured_value_is_ignored() {
    let set = client().await.sorted_set::<TestObject>(unique("zset"));
    assert!(set.insert(&object("1", "2"), 0.0).await.unwrap());
    assert!(!set.insert(&object("1", "2"), 0.0).await.unwrap());
    assert!(set.insert(&object("2", "3"), 2.0).await.unwrap());
    assert!(set.insert(&object("3", "4"), 3.0).await.unwrap());
    assert!(set.insert(&object("5", "6"), 4.0).await.unwrap());
    assert_eq!(set.len().await.unwrap(), 4);
}

// Redisson: testSize
#[tokio::test]
async fn len_counts_each_value_once_despite_duplicates() {
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

// Redisson: testValueRange
#[tokio::test]
async fn range_returns_values_in_ascending_score_order() {
    let set = numbers(&[(0.0, 1), (1.0, 2), (2.0, 3), (3.0, 4), (4.0, 5), (4.0, 5)]).await;
    assert_eq!(
        values(set.range(0..usize::MAX).await.unwrap()),
        [1, 2, 3, 4, 5]
    );
}

// Redisson: testValueRangeReversedInfinity
#[tokio::test]
async fn rev_range_by_score_limit_accepts_an_unbounded_range() {
    let set = numbers(&[(1.0, 1), (2.0, 2)]).await;
    assert_eq!(
        values(set.rev_range_by_score_limit(.., 0, 1).await.unwrap()),
        [2]
    );
}

// Redisson: testValueRangeReversed
#[tokio::test]
async fn rev_range_returns_values_in_descending_score_order() {
    let set = numbers(&[(0.0, 1), (1.0, 2), (2.0, 3), (3.0, 4), (4.0, 5), (4.0, 5)]).await;
    assert_eq!(
        values(set.rev_range(0..usize::MAX).await.unwrap()),
        [5, 4, 3, 2, 1]
    );
}

// Redisson: testEntryRange
#[tokio::test]
async fn range_returns_entries_with_their_scores() {
    let set = numbers(&[(10.0, 1), (20.0, 2), (30.0, 3), (40.0, 4), (50.0, 5)]).await;
    assert_eq!(
        set.range(0..usize::MAX).await.unwrap(),
        [(1, 10.0), (2, 20.0), (3, 30.0), (4, 40.0), (5, 50.0)]
    );
}

// Redisson: testEntryRangeReversed
#[tokio::test]
async fn rev_range_returns_entries_with_their_scores() {
    let set = numbers(&[(10.0, 1), (20.0, 2), (30.0, 3), (40.0, 4), (50.0, 5)]).await;
    assert_eq!(
        set.rev_range(0..usize::MAX).await.unwrap(),
        [(5, 50.0), (4, 40.0), (3, 30.0), (2, 20.0), (1, 10.0)]
    );
}

// Redisson: testValueRangeLimit
#[tokio::test]
async fn range_by_score_limit_skips_offset_and_takes_count() {
    let set = letters().await;
    assert_eq!(
        values(set.range_by_score_limit(1.0..4.0, 1, 2).await.unwrap()),
        ["c", "d"]
    );
}

// Redisson: testValueRangeAlpha
#[tokio::test]
async fn range_by_score_excludes_the_end_of_a_half_open_range() {
    let set = letters().await;
    assert_eq!(
        values(set.range_by_score(1.0..4.0).await.unwrap()),
        ["b", "c", "d"]
    );
}

// Redisson: testValueRangeReversedLimit
#[tokio::test]
async fn rev_range_by_score_limit_skips_offset_from_the_top() {
    let set = letters().await;
    assert_eq!(
        values(set.rev_range_by_score_limit(1.0..4.0, 1, 2).await.unwrap()),
        ["c", "b"]
    );
}

// Redisson: testValueRangeReversedAlpha
#[tokio::test]
async fn rev_range_by_score_lists_the_range_from_the_top() {
    let set = letters().await;
    assert_eq!(
        values(set.rev_range_by_score(1.0..4.0).await.unwrap()),
        ["d", "c", "b"]
    );
}

// Redisson: testValueRangeNegativeInf
#[tokio::test]
async fn range_by_score_limit_accepts_a_negative_infinite_start() {
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

// Redisson: testValueRangePositiveInf
#[tokio::test]
async fn range_by_score_limit_accepts_a_positive_infinite_end() {
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

// Redisson: testEntryRangeAlpha
#[tokio::test]
async fn range_by_score_limit_returns_entries_with_scores() {
    let set = letters().await;
    assert_eq!(
        set.range_by_score_limit(1.0..4.0, 1, 2).await.unwrap(),
        [entry("c", 2.0), entry("d", 3.0)]
    );
}

// Redisson: testEntryRangeReversedAlpha
#[tokio::test]
async fn rev_range_by_score_limit_returns_entries_with_scores() {
    let set = letters().await;
    assert_eq!(
        set.rev_range_by_score_limit(1.0..4.0, 1, 2).await.unwrap(),
        [entry("c", 2.0), entry("b", 1.0)]
    );
}

// Redisson: testEntryRangeNegativeInf
#[tokio::test]
async fn range_by_score_limit_accepts_a_range_without_a_start() {
    let set = letters().await;
    assert_eq!(
        set.range_by_score_limit(..4.0, 1, 2).await.unwrap(),
        [entry("b", 1.0), entry("c", 2.0)]
    );
}

// Redisson: testEntryRangePositiveInf
#[tokio::test]
async fn range_by_score_limit_accepts_a_range_without_an_end() {
    let set = letters().await;
    assert_eq!(
        set.range_by_score_limit(1.0.., 1, 2).await.unwrap(),
        [entry("c", 2.0), entry("d", 3.0)]
    );
}

// Redisson: testAddAndGet
#[tokio::test]
async fn add_score_returns_and_stores_the_new_score() {
    let set = strings(&[(1.0, "100")]).await;
    assert_eq!(set.add_score("100", 11.0).await.unwrap(), 12.0);
    assert_eq!(set.score("100").await.unwrap(), Some(12.0));
    set.insert("1", 100.2).await.unwrap();
    assert_eq!(set.add_score("1", 12.1).await.unwrap(), 112.3);
    assert_eq!(set.score("1").await.unwrap(), Some(112.3));
}

// Redisson: testAddAndGetAll
#[tokio::test]
async fn scores_returns_none_for_missing_values() {
    let set = strings(&[(100.2, "1")]).await;
    assert_eq!(set.add_score("1", 12.1).await.unwrap(), 112.3);
    assert_eq!(set.score("1").await.unwrap(), Some(112.3));
    assert_eq!(
        set.scores(&["1", "42", "100"]).await.unwrap(),
        [Some(112.3), None, None]
    );
}

// Redisson: testAddScoreAndGetRank
#[tokio::test]
async fn add_score_and_rank_returns_the_rank_after_the_update() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    assert_eq!(set.add_score_and_rank("12", 12.0).await.unwrap(), 0);
    assert_eq!(set.add_score_and_rank("15", 10.0).await.unwrap(), 0);
    assert_eq!(set.rank("12").await.unwrap(), Some(1));
    assert_eq!(set.rank("15").await.unwrap(), Some(0));
    assert_eq!(set.add_score_and_rank("12", 2.0).await.unwrap(), 1);
    assert_eq!(set.score("12").await.unwrap(), Some(14.0));
}

// Redisson: testAddScoreAndGetRevRank
#[tokio::test]
async fn add_score_and_rev_rank_returns_the_reverse_rank_after_the_update() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    assert_eq!(set.add_score_and_rev_rank("12", 12.0).await.unwrap(), 0);
    assert_eq!(set.add_score_and_rev_rank("15", 10.0).await.unwrap(), 1);
    assert_eq!(set.rev_rank("12").await.unwrap(), Some(0));
    assert_eq!(set.rev_rank("15").await.unwrap(), Some(1));
    assert_eq!(set.add_score_and_rev_rank("12", 2.0).await.unwrap(), 0);
    assert_eq!(set.add_score_and_rev_rank("15", -1.0).await.unwrap(), 1);
    assert_eq!(set.score("12").await.unwrap(), Some(14.0));
}

// Redisson: testAddAndGetRevRankCollection
#[tokio::test]
async fn rev_rank_many_ranks_several_values_from_the_top() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    set.extend([("one", 1.0), ("three", 3.0), ("two", 2.0)])
        .await
        .unwrap();
    assert_eq!(
        set.rev_rank_many(&["one", "three", "two"]).await.unwrap(),
        [Some(2), Some(0), Some(1)]
    );
}

// Redisson: testReadIntersection
#[tokio::test]
async fn intersection_returns_values_present_in_every_set() {
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

// Redisson: testReadIntersectionEntries
#[tokio::test]
async fn intersection_sums_scores_and_applies_weights() {
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

// Redisson: testIntersection
#[tokio::test]
async fn store_intersection_writes_common_values_with_summed_scores() {
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

// Redisson: testIntersectionEmpty
#[tokio::test]
async fn store_intersection_of_disjoint_sets_is_empty() {
    let [set1, set2, out] = pair(
        &[(1.0, "one"), (2.0, "two")],
        &[(3.0, "three"), (4.0, "four")],
    )
    .await;
    assert_eq!(out.store_intersection(&[&set1, &set2]).await.unwrap(), 0);
    assert!(out.is_empty().await.unwrap());
}

// Redisson: testIntersectionWithWeight
#[tokio::test]
async fn store_intersection_with_weights_scales_the_scores() {
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

// Redisson: testReadUnion
#[tokio::test]
async fn union_returns_values_present_in_any_set() {
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

// Redisson: testReadUnionEntries
#[tokio::test]
async fn union_sums_scores_and_applies_weights() {
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

// Redisson: testUnion
#[tokio::test]
async fn store_union_writes_all_values_with_summed_scores() {
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

// Redisson: testUnionWithWeight
#[tokio::test]
async fn store_union_with_weights_scales_the_scores() {
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

// Redisson: testReadDiffEntries
#[tokio::test]
async fn difference_returns_entries_missing_from_the_other_sets() {
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
