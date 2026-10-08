mod common;

use common::{client, unique};
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
async fn object_methods() {
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
