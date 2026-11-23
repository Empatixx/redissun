use crate::common::{client, unique};
use futures::TryStreamExt;
use redissun::Object;
use std::collections::HashSet;

#[tokio::test]
async fn fifo_with_push_back_and_pop_front() {
    let deque = client().await.vec_deque::<String>(unique("deque"));
    deque.push_back("a").await.unwrap();
    deque.push_back("b").await.unwrap();
    assert_eq!(deque.pop_front().await.unwrap(), Some("a".to_string()));
    assert_eq!(deque.pop_front().await.unwrap(), Some("b".to_string()));
    assert_eq!(deque.pop_front().await.unwrap(), None);
}

#[tokio::test]
async fn lifo_with_push_back_and_pop_back() {
    let deque = client().await.vec_deque::<String>(unique("deque"));
    deque.push_back("a").await.unwrap();
    deque.push_back("b").await.unwrap();
    assert_eq!(deque.pop_back().await.unwrap(), Some("b".to_string()));
    assert_eq!(deque.pop_back().await.unwrap(), Some("a".to_string()));
    assert_eq!(deque.pop_back().await.unwrap(), None);
}

#[tokio::test]
async fn push_front_puts_the_value_first() {
    let deque = client().await.vec_deque::<String>(unique("deque"));
    deque.push_back("b").await.unwrap();
    deque.push_front("a").await.unwrap();
    let read: Vec<String> = deque.iter().try_collect().await.unwrap();
    assert_eq!(read, ["a", "b"]);
}

#[tokio::test]
async fn front_and_back_do_not_remove() {
    let deque = client().await.vec_deque::<String>(unique("deque"));
    assert_eq!(deque.front().await.unwrap(), None);
    assert_eq!(deque.back().await.unwrap(), None);
    deque.push_back("a").await.unwrap();
    deque.push_back("b").await.unwrap();
    assert_eq!(deque.front().await.unwrap(), Some("a".to_string()));
    assert_eq!(deque.back().await.unwrap(), Some("b".to_string()));
    assert_eq!(deque.len().await.unwrap(), 2);
}

#[tokio::test]
async fn len_is_empty_and_clear() {
    let deque = client().await.vec_deque::<u32>(unique("deque"));
    assert!(deque.is_empty().await.unwrap());
    deque.push_back(&1).await.unwrap();
    assert_eq!(deque.len().await.unwrap(), 1);
    deque.clear().await.unwrap();
    assert!(!deque.exists().await.unwrap());
}

#[tokio::test]
async fn iter_reads_more_than_one_page_in_order() {
    let deque = client().await.vec_deque::<u32>(unique("deque"));
    for i in 0..250u32 {
        deque.push_back(&i).await.unwrap();
    }
    let read: Vec<u32> = deque.iter().try_collect().await.unwrap();
    assert_eq!(read, (0..250).collect::<Vec<_>>());
}

#[tokio::test]
async fn vec_and_vec_deque_share_one_list() {
    let client = client().await;
    let name = unique("shared");
    let vec = client.vec::<String>(name.clone());
    let deque = client.vec_deque::<String>(name);
    vec.push("a").await.unwrap();
    deque.push_front("z").await.unwrap();
    assert_eq!(vec.get(0).await.unwrap(), Some("z".to_string()));
    assert_eq!(deque.pop_back().await.unwrap(), Some("a".to_string()));
}

#[tokio::test]
async fn expire_persist_and_rename_work_on_a_deque() {
    let client = client().await;
    let deque = client.vec_deque::<String>(unique("deque"));
    deque.push_back("a").await.unwrap();
    assert!(deque.exists().await.unwrap());
    assert!(deque
        .expire(std::time::Duration::from_secs(60))
        .await
        .unwrap());
    assert!(deque.ttl().await.unwrap().is_some());
    assert!(deque.persist().await.unwrap());
    let renamed = unique("deque");
    deque.rename(&renamed).await.unwrap();
    assert_eq!(
        client
            .vec_deque::<String>(renamed)
            .pop_front()
            .await
            .unwrap(),
        Some("a".to_string())
    );
    assert!(!deque.exists().await.unwrap());
}

#[tokio::test]
async fn every_element_is_consumed_exactly_once() {
    let deque = client().await.vec_deque::<u32>(unique("deque"));
    for i in 0..200u32 {
        deque.push_back(&i).await.unwrap();
    }
    let consumers: Vec<_> = (0..8)
        .map(|_| {
            let deque = deque.clone();
            tokio::spawn(async move {
                let mut taken = Vec::new();
                while let Some(value) = deque.pop_front().await.unwrap() {
                    taken.push(value);
                }
                taken
            })
        })
        .collect();
    let mut all = Vec::new();
    for consumer in consumers {
        all.extend(consumer.await.unwrap());
    }
    assert_eq!(all.len(), 200);
    assert_eq!(all.into_iter().collect::<HashSet<_>>().len(), 200);
}

async fn deque() -> redissun::VecDeque<i32, redissun::JsonCodec> {
    client().await.vec_deque::<i32>(unique("deque"))
}

async fn all(deque: &redissun::VecDeque<i32, redissun::JsonCodec>) -> Vec<i32> {
    deque.read_all().await.unwrap()
}

// Redisson: testAddIfExists
#[tokio::test]
async fn extend_if_exists_only_adds_to_an_existing_deque() {
    let deque = deque().await;
    assert_eq!(deque.extend_front_if_exists(&[4, 5]).await.unwrap(), 0);
    assert!(!deque.exists().await.unwrap());
    deque.extend_back(&[1, 2, 3]).await.unwrap();
    assert_eq!(deque.extend_front_if_exists(&[4, 5]).await.unwrap(), 5);
    assert_eq!(all(&deque).await, [5, 4, 1, 2, 3]);
    assert_eq!(deque.extend_back_if_exists(&[6]).await.unwrap(), 6);
    assert_eq!(all(&deque).await, [5, 4, 1, 2, 3, 6]);
}

// Redisson: testMove
#[tokio::test]
async fn pop_push_moves_an_element_between_two_deques() {
    let deque1 = deque().await;
    let deque2 = deque().await;
    deque1.extend_back(&[1, 2, 3]).await.unwrap();
    deque2.extend_back(&[4, 5, 6]).await.unwrap();
    assert_eq!(deque1.pop_front_push_back(&deque2).await.unwrap(), Some(1));
    assert_eq!(all(&deque1).await, [2, 3]);
    assert_eq!(all(&deque2).await, [4, 5, 6, 1]);
    assert_eq!(deque2.pop_back_push_front(&deque1).await.unwrap(), Some(1));
    assert_eq!(all(&deque1).await, [1, 2, 3]);
    assert_eq!(all(&deque2).await, [4, 5, 6]);
}

// Redisson: testRemoveLastOccurrence
#[tokio::test]
async fn remove_last_occurrence_removes_the_value_nearest_the_back() {
    let deque = deque().await;
    for value in [3, 1, 2, 3] {
        deque.push_front(&value).await.unwrap();
    }
    assert!(deque.remove_last_occurrence(&3).await.unwrap());
    assert_eq!(all(&deque).await, [3, 2, 1]);
    assert!(!deque.remove_last_occurrence(&9).await.unwrap());
}

// Redisson: testRemoveFirstOccurrence
#[tokio::test]
async fn remove_first_occurrence_removes_the_value_nearest_the_front() {
    let deque = deque().await;
    for value in [3, 1, 2, 3] {
        deque.push_front(&value).await.unwrap();
    }
    assert!(deque.remove_first_occurrence(&3).await.unwrap());
    assert_eq!(all(&deque).await, [2, 1, 3]);
}

// Redisson: testRemoveLast
#[tokio::test]
async fn pop_back_returns_the_values_from_the_back() {
    let deque = deque().await;
    for value in [1, 2, 3] {
        deque.push_front(&value).await.unwrap();
    }
    assert_eq!(deque.pop_back().await.unwrap(), Some(1));
    assert_eq!(deque.pop_back().await.unwrap(), Some(2));
    assert_eq!(deque.pop_back().await.unwrap(), Some(3));
}

// Redisson: testRemoveFirst
#[tokio::test]
async fn pop_front_returns_the_values_from_the_front() {
    let deque = deque().await;
    for value in [1, 2, 3] {
        deque.push_front(&value).await.unwrap();
    }
    assert_eq!(deque.pop_front().await.unwrap(), Some(3));
    assert_eq!(deque.pop_front().await.unwrap(), Some(2));
    assert_eq!(deque.pop_front().await.unwrap(), Some(1));
}

// Redisson: testPeek
#[tokio::test]
async fn front_and_back_are_none_on_an_empty_deque() {
    let deque = deque().await;
    assert_eq!(deque.front().await.unwrap(), None);
    assert_eq!(deque.back().await.unwrap(), None);
    deque.push_front(&2).await.unwrap();
    assert_eq!(deque.front().await.unwrap(), Some(2));
    assert_eq!(deque.back().await.unwrap(), Some(2));
}

// Redisson: testPollLastAndOfferFirstTo
#[tokio::test]
async fn pop_back_push_front_moves_the_last_value_over() {
    let deque1 = deque().await;
    for value in [3, 2, 1] {
        deque1.push_front(&value).await.unwrap();
    }
    let deque2 = deque().await;
    for value in [6, 5, 4] {
        deque2.push_front(&value).await.unwrap();
    }
    assert_eq!(deque1.pop_back_push_front(&deque2).await.unwrap(), Some(3));
    assert_eq!(all(&deque2).await, [3, 4, 5, 6]);
}

// Redisson: testAddFirstLastMulti
#[tokio::test]
async fn extend_front_and_extend_back_add_several_values_at_once() {
    let deque = deque().await;
    deque.extend_back(&[1, 2, 3, 4]).await.unwrap();
    assert_eq!(deque.extend_front(&[0, 1, 0]).await.unwrap(), 7);
    assert_eq!(deque.extend_back(&[10, 20, 10]).await.unwrap(), 10);
    assert_eq!(all(&deque).await, [0, 1, 0, 1, 2, 3, 4, 10, 20, 10]);
    deque.clear().await.unwrap();
    deque.extend_front(&[1, 2, 3]).await.unwrap();
    assert_eq!(all(&deque).await, [3, 2, 1]);
}

// Redisson: testAddFirst
#[tokio::test]
async fn push_front_puts_each_value_before_the_previous_ones() {
    let deque = deque().await;
    for value in [1, 2, 3] {
        deque.push_front(&value).await.unwrap();
    }
    assert_eq!(all(&deque).await, [3, 2, 1]);
}

// Redisson: testAddLast
#[tokio::test]
async fn push_back_keeps_the_insertion_order() {
    let deque = deque().await;
    for value in [1, 2, 3] {
        deque.push_back(&value).await.unwrap();
    }
    assert_eq!(all(&deque).await, [1, 2, 3]);
}

// Redisson: testDescendingIterator
#[tokio::test]
async fn iter_rev_yields_the_values_back_to_front() {
    let deque = deque().await;
    deque.extend_back(&[1, 2, 3]).await.unwrap();
    let read: Vec<i32> = deque.iter_rev().try_collect().await.unwrap();
    assert_eq!(read, [3, 2, 1]);
}

#[tokio::test]
async fn iter_rev_reads_more_than_one_page() {
    let deque = deque().await;
    let values: Vec<i32> = (0..250).collect();
    deque.extend_back(values.iter()).await.unwrap();
    let read: Vec<i32> = deque.iter_rev().try_collect().await.unwrap();
    assert_eq!(read, values.into_iter().rev().collect::<Vec<_>>());
}

// Redisson: testPollLimited
#[tokio::test]
async fn pop_front_many_returns_at_most_the_asked_count() {
    let deque = deque().await;
    deque.extend_back(&[1, 2, 3, 4, 5, 6, 7]).await.unwrap();
    assert_eq!(deque.pop_front_many(3).await.unwrap(), [1, 2, 3]);
    assert_eq!(deque.pop_front_many(10).await.unwrap(), [4, 5, 6, 7]);
    assert!(deque.pop_front_many(5).await.unwrap().is_empty());
}

#[tokio::test]
async fn poll_last_limited() {
    let deque = deque().await;
    deque.extend_back(&[1, 2, 3, 4]).await.unwrap();
    assert_eq!(deque.pop_back_many(3).await.unwrap(), [4, 3, 2]);
    assert!(deque.pop_back_many(0).await.unwrap().is_empty());
    assert_eq!(deque.pop_back_many(3).await.unwrap(), [1]);
}

// Redisson: testAddOffer
#[tokio::test]
async fn pop_front_after_push_back_returns_the_oldest_value() {
    let deque = deque().await;
    for value in [1, 2, 3, 4] {
        deque.push_back(&value).await.unwrap();
    }
    assert_eq!(all(&deque).await, [1, 2, 3, 4]);
    assert_eq!(deque.pop_front().await.unwrap(), Some(1));
    assert_eq!(all(&deque).await, [2, 3, 4]);
    assert_eq!(deque.front().await.unwrap(), Some(2));
}

// Redisson: testRemoveWithCodec
#[tokio::test]
async fn contains_finds_a_tuple_value_read_by_front() {
    let deque = client()
        .await
        .vec_deque::<(String, String, i64)>(unique("deque"));
    deque
        .push_back(&("key".to_string(), "traceId".to_string(), 0))
        .await
        .unwrap();
    let peeked = deque.front().await.unwrap().unwrap();
    assert!(deque.contains(&peeked).await.unwrap());
}

// Redisson: testRemove
#[tokio::test]
async fn pop_front_until_the_deque_is_empty() {
    let deque = deque().await;
    deque.extend_back(&[1, 2, 3, 4]).await.unwrap();
    deque.pop_front().await.unwrap();
    deque.pop_front().await.unwrap();
    assert_eq!(all(&deque).await, [3, 4]);
    deque.pop_front().await.unwrap();
    deque.pop_front().await.unwrap();
    assert!(deque.is_empty().await.unwrap());
}

// Redisson: testRemoveEmpty
#[tokio::test]
async fn pop_front_on_an_empty_deque_is_none() {
    let deque = deque().await;
    assert_eq!(deque.pop_front().await.unwrap(), None);
}

// Redisson: testIndexOf
#[tokio::test]
async fn position_returns_the_index_or_none() {
    let deque = deque().await;
    deque.extend_back(&[1, 2, 3, 4]).await.unwrap();
    assert_eq!(deque.position(&4).await.unwrap(), Some(3));
    assert_eq!(deque.position(&9).await.unwrap(), None);
}

// Redisson: testDrainToSingle
#[tokio::test]
async fn drain_takes_a_single_value_and_empties_the_deque() {
    let deque = deque().await;
    deque.push_back(&1).await.unwrap();
    assert_eq!(deque.len().await.unwrap(), 1);
    assert_eq!(deque.drain().await.unwrap(), [1]);
    assert!(deque.is_empty().await.unwrap());
}

// Redisson: testDrainTo
#[tokio::test]
async fn drain_up_to_takes_the_values_in_order_in_chunks() {
    let deque = deque().await;
    let values: Vec<i32> = (0..100).collect();
    deque.extend_back(values.iter()).await.unwrap();
    assert_eq!(deque.len().await.unwrap(), 100);
    let mut batch = deque.drain_up_to(10).await.unwrap();
    assert_eq!(batch, (0..10).collect::<Vec<_>>());
    assert_eq!(deque.len().await.unwrap(), 90);
    for max in [10, 20, 60] {
        batch.extend(deque.drain_up_to(max).await.unwrap());
    }
    assert_eq!(deque.len().await.unwrap(), 0);
    assert_eq!(batch, values);
    assert!(deque.drain_up_to(0).await.unwrap().is_empty());
}

// Redisson: testDrainToCollection
#[tokio::test]
async fn drain_returns_mixed_json_values_in_order() {
    let deque = client()
        .await
        .vec_deque::<serde_json::Value>(unique("deque"));
    let values = [
        serde_json::json!(1),
        serde_json::json!(2),
        serde_json::json!("e"),
    ];
    deque.extend_back(values.iter()).await.unwrap();
    assert_eq!(deque.drain().await.unwrap(), values);
    assert_eq!(deque.len().await.unwrap(), 0);
}

// Redisson: testDrainToCollectionLimited
#[tokio::test]
async fn drain_up_to_stops_at_the_limit() {
    let deque = client()
        .await
        .vec_deque::<serde_json::Value>(unique("deque"));
    let values = [
        serde_json::json!(1),
        serde_json::json!(2),
        serde_json::json!("e"),
    ];
    deque.extend_back(values.iter()).await.unwrap();
    assert_eq!(deque.drain_up_to(2).await.unwrap(), values[..2]);
    assert_eq!(deque.len().await.unwrap(), 1);
    assert_eq!(deque.drain_up_to(2).await.unwrap(), values[2..]);
}
