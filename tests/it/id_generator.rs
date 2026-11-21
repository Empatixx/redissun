use crate::common::{client, unique};
use redissun::Object;
use std::time::Duration;

#[tokio::test]
async fn an_empty_generator_starts_at_one() {
    let generator = client().await.id_generator(unique("ids"));
    for expected in 1..=100_103 {
        assert_eq!(generator.next_id().await.unwrap(), expected);
    }
}

#[tokio::test]
async fn try_init_sets_the_first_id_once() {
    let generator = client().await.id_generator(unique("ids"));
    assert!(generator.try_init(12, 2931).await.unwrap());
    assert!(!generator.try_init(0, 1000).await.unwrap());

    for expected in 12..=5000 {
        assert_eq!(generator.next_id().await.unwrap(), expected);
    }
}

#[tokio::test]
async fn two_generators_never_hand_out_the_same_id() {
    let client = client().await;
    let name = unique("ids");
    let first = client.id_generator(name.clone());
    let second = client.id_generator(name);
    first.try_init(1, 10).await.unwrap();

    let mut ids = std::collections::HashSet::new();
    for _ in 0..55 {
        assert!(ids.insert(first.next_id().await.unwrap()));
        assert!(ids.insert(second.next_id().await.unwrap()));
    }
}

#[tokio::test]
async fn a_block_size_that_is_not_positive_is_refused() {
    let generator = client().await.id_generator(unique("ids"));
    assert!(generator.try_init(1, 0).await.is_err());
}

#[tokio::test]
async fn the_common_methods_cover_both_keys() {
    let client = client().await;
    let name = unique("ids");
    let renamed = unique("ids");
    let generator = client.id_generator(name.clone());
    generator.try_init(12, 2931).await.unwrap();
    let allocation = client.atomic_i64(format!("{{{name}}}:allocation"));
    assert_eq!(allocation.get().await.unwrap(), 2931);

    assert!(generator.expire(Duration::from_secs(60)).await.unwrap());
    assert!(allocation.ttl().await.unwrap().is_some());
    assert!(generator.persist().await.unwrap());
    assert!(allocation.ttl().await.unwrap().is_none());

    generator.rename(&renamed).await.unwrap();
    assert!(!allocation.exists().await.unwrap());
    let moved = client.id_generator(renamed.clone());
    assert_eq!(moved.next_id().await.unwrap(), 12);

    assert!(moved.del().await.unwrap());
    let allocation = client.atomic_i64(format!("{{{renamed}}}:allocation"));
    assert!(!allocation.exists().await.unwrap());
}
