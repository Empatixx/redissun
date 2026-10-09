mod common;

use common::{client, unique};
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
        names(&set.range_by_score(2.0, 3.0).await.unwrap()),
        ["b", "c"]
    );
    assert_eq!(set.count_by_score(2.0, 4.0).await.unwrap(), 3);
    assert_eq!(
        names(
            &set.range_by_score(f64::NEG_INFINITY, f64::INFINITY)
                .await
                .unwrap()
        ),
        ["a", "b", "c", "d"]
    );
    assert!(set.range_by_score(10.0, 20.0).await.unwrap().is_empty());
    assert_eq!(set.remove_by_score(1.0, 2.0).await.unwrap(), 2);
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
async fn iter_walks_more_than_one_page_in_order() {
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
    assert!(items
        .iter()
        .enumerate()
        .all(|(i, (v, s))| *v == i as u32 && *s == f64::from(*v)));
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
