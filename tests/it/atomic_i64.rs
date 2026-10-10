use crate::common::{client, unique};
use redissun::{Comparison, Object};
use std::time::Duration;

#[tokio::test]
async fn a_missing_counter_reads_as_zero() {
    let counter = client().await.atomic_i64(unique("atomic"));
    assert_eq!(counter.get().await.unwrap(), 0);
    assert!(!counter.exists().await.unwrap());
}

#[tokio::test]
async fn set_then_get() {
    let counter = client().await.atomic_i64(unique("atomic"));
    counter.set(41).await.unwrap();
    assert_eq!(counter.get().await.unwrap(), 41);
    counter.set(-7).await.unwrap();
    assert_eq!(counter.get().await.unwrap(), -7);
}

#[tokio::test]
async fn add_and_get_and_get_and_add_differ_in_what_they_return() {
    let counter = client().await.atomic_i64(unique("atomic"));
    assert_eq!(counter.add_and_get(5).await.unwrap(), 5);
    assert_eq!(counter.get_and_add(3).await.unwrap(), 5);
    assert_eq!(counter.get().await.unwrap(), 8);
    assert_eq!(counter.add_and_get(-10).await.unwrap(), -2);
}

#[tokio::test]
async fn incr_and_decr_return_the_new_value() {
    let counter = client().await.atomic_i64(unique("atomic"));
    assert_eq!(counter.incr().await.unwrap(), 1);
    assert_eq!(counter.incr().await.unwrap(), 2);
    assert_eq!(counter.decr().await.unwrap(), 1);
}

#[tokio::test]
async fn get_and_set_returns_the_previous_value() {
    let counter = client().await.atomic_i64(unique("atomic"));
    assert_eq!(counter.get_and_set(10).await.unwrap(), 0);
    assert_eq!(counter.get_and_set(20).await.unwrap(), 10);
    assert_eq!(counter.get().await.unwrap(), 20);
}

#[tokio::test]
async fn get_and_delete_removes_the_key() {
    let counter = client().await.atomic_i64(unique("atomic"));
    counter.set(9).await.unwrap();
    assert_eq!(counter.get_and_delete().await.unwrap(), 9);
    assert!(!counter.exists().await.unwrap());
    assert_eq!(counter.get_and_delete().await.unwrap(), 0);
}

#[tokio::test]
async fn compare_and_set_swaps_only_on_a_match() {
    let counter = client().await.atomic_i64(unique("atomic"));
    counter.set(5).await.unwrap();
    assert!(!counter.compare_and_set(4, 9).await.unwrap());
    assert!(counter.compare_and_set(5, 9).await.unwrap());
    assert_eq!(counter.get().await.unwrap(), 9);
}

#[tokio::test]
async fn compare_and_set_treats_a_missing_key_as_zero() {
    let counter = client().await.atomic_i64(unique("atomic"));
    assert!(!counter.compare_and_set(1, 5).await.unwrap());
    assert!(counter.compare_and_set(0, 5).await.unwrap());
    assert_eq!(counter.get().await.unwrap(), 5);
}

#[tokio::test]
async fn concurrent_increments_are_not_lost() {
    let client = client().await;
    let name = unique("atomic");
    let tasks: Vec<_> = (0..20)
        .map(|_| {
            let counter = client.atomic_i64(name.clone());
            tokio::spawn(async move {
                for _ in 0..25 {
                    counter.incr().await.unwrap();
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(client.atomic_i64(name).get().await.unwrap(), 500);
}

#[tokio::test]
async fn object_operations_work_on_a_counter() {
    let counter = client().await.atomic_i64(unique("atomic"));
    counter.set(1).await.unwrap();
    assert!(counter.expire(Duration::from_secs(60)).await.unwrap());
    assert!(counter.ttl().await.unwrap().is_some());
    assert!(counter.del().await.unwrap());
}

#[tokio::test]
async fn a_value_that_is_not_an_integer_is_an_error_not_a_panic() {
    let client = client().await;
    let name = unique("atomic");
    client
        .bucket::<String>(name.clone())
        .set(&"text".to_string())
        .await
        .unwrap();
    let counter = client.atomic_i64(name);
    assert!(counter.get().await.is_err());
    assert!(counter.incr().await.is_err());
}

#[tokio::test]
async fn debug_shows_the_name() {
    let name = unique("atomic");
    let counter = client().await.atomic_i64(name.clone());
    assert!(format!("{counter:?}").contains(&name));
}

#[tokio::test]
async fn test_compare_and_delete() {
    let al = client().await.atomic_i64(unique("test"));
    al.set(10).await.unwrap();
    assert!(!al.compare_and_delete(Comparison::Less, 5).await.unwrap());
    assert!(al.exists().await.unwrap());
    assert!(al.compare_and_delete(Comparison::Less, 15).await.unwrap());
    assert!(!al.exists().await.unwrap());

    al.set(10).await.unwrap();
    assert!(!al
        .compare_and_delete(Comparison::LessOrEqual, 9)
        .await
        .unwrap());
    assert!(al
        .compare_and_delete(Comparison::LessOrEqual, 10)
        .await
        .unwrap());
    assert!(!al.exists().await.unwrap());

    al.set(10).await.unwrap();
    assert!(!al
        .compare_and_delete(Comparison::Greater, 15)
        .await
        .unwrap());
    assert!(al.exists().await.unwrap());
    assert!(al.compare_and_delete(Comparison::Greater, 5).await.unwrap());
    assert!(!al.exists().await.unwrap());

    al.set(10).await.unwrap();
    assert!(!al
        .compare_and_delete(Comparison::GreaterOrEqual, 11)
        .await
        .unwrap());
    assert!(al
        .compare_and_delete(Comparison::GreaterOrEqual, 10)
        .await
        .unwrap());
    assert!(!al.exists().await.unwrap());

    al.set(10).await.unwrap();
    assert!(!al.compare_and_delete(Comparison::Equal, 11).await.unwrap());
    assert!(al.compare_and_delete(Comparison::Equal, 10).await.unwrap());
    assert!(!al.exists().await.unwrap());

    al.set(10).await.unwrap();
    assert!(!al
        .compare_and_delete(Comparison::NotEqual, 10)
        .await
        .unwrap());
    assert!(al
        .compare_and_delete(Comparison::NotEqual, 11)
        .await
        .unwrap());
    assert!(!al.exists().await.unwrap());

    assert!(!al.compare_and_delete(Comparison::Equal, 0).await.unwrap());
    assert!(!al.compare_and_delete(Comparison::Less, 0).await.unwrap());
}

#[tokio::test]
async fn test_set_if_less() {
    let al = client().await.atomic_i64(unique("test"));
    assert!(!al.set_if_less(0, 1).await.unwrap());
    assert_eq!(al.get().await.unwrap(), 0);

    al.set(12).await.unwrap();
    assert!(al.set_if_less(13, 1).await.unwrap());
    assert_eq!(al.get().await.unwrap(), 1);
}

#[tokio::test]
async fn test_set_if_greater() {
    let al = client().await.atomic_i64(unique("test"));
    assert!(!al.set_if_less(0, 1).await.unwrap());
    assert_eq!(al.get().await.unwrap(), 0);

    al.set(12).await.unwrap();
    assert!(al.set_if_greater(11, 1).await.unwrap());
    assert_eq!(al.get().await.unwrap(), 1);
}

#[tokio::test]
async fn test_get_and_set() {
    let al = client().await.atomic_i64(unique("test"));
    assert_eq!(al.get_and_set(12).await.unwrap(), 0);
}

#[tokio::test]
async fn test_get_zero() {
    let al = client().await.atomic_i64(unique("test"));
    assert_eq!(al.get().await.unwrap(), 0);
}

#[tokio::test]
async fn test_get_and_delete() {
    let client = client().await;
    let al = client.atomic_i64(unique("test"));
    al.set(10).await.unwrap();
    assert_eq!(al.get_and_delete().await.unwrap(), 10);
    assert!(!al.exists().await.unwrap());

    let al2 = client.atomic_i64(unique("test2"));
    assert_eq!(al2.get_and_delete().await.unwrap(), 0);
}

#[tokio::test]
async fn test_compare_and_set_zero() {
    let client = client().await;
    let al = client.atomic_i64(unique("test"));
    assert!(al.compare_and_set(0, 2).await.unwrap());
    assert_eq!(al.get().await.unwrap(), 2);

    let al2 = client.atomic_i64(unique("test1"));
    al2.set(0).await.unwrap();
    assert!(al2.compare_and_set(0, 2).await.unwrap());
    assert_eq!(al2.get().await.unwrap(), 2);
}

#[tokio::test]
async fn test_compare_and_set() {
    let al = client().await.atomic_i64(unique("test"));
    assert!(!al.compare_and_set(-1, 2).await.unwrap());
    assert_eq!(al.get().await.unwrap(), 0);
    assert!(al.compare_and_set(0, 2).await.unwrap());
    assert_eq!(al.get().await.unwrap(), 2);
}

#[tokio::test]
async fn test_set_then_increment() {
    let al = client().await.atomic_i64(unique("test"));
    al.set(2).await.unwrap();
    assert_eq!(al.get_and_incr().await.unwrap(), 2);
    assert_eq!(al.get().await.unwrap(), 3);
}

#[tokio::test]
async fn test_increment_and_get() {
    let al = client().await.atomic_i64(unique("test"));
    assert_eq!(al.incr().await.unwrap(), 1);
    assert_eq!(al.get().await.unwrap(), 1);
}

#[tokio::test]
async fn test_get_and_increment() {
    let al = client().await.atomic_i64(unique("test"));
    assert_eq!(al.get_and_incr().await.unwrap(), 0);
    assert_eq!(al.get().await.unwrap(), 1);
}

#[tokio::test]
async fn test() {
    let client = client().await;
    let name = unique("test");
    let al = client.atomic_i64(name.clone());
    assert_eq!(al.get().await.unwrap(), 0);
    assert_eq!(al.get_and_incr().await.unwrap(), 0);
    assert_eq!(al.get().await.unwrap(), 1);
    assert_eq!(al.get_and_decr().await.unwrap(), 1);
    assert_eq!(al.get().await.unwrap(), 0);
    assert_eq!(al.get_and_incr().await.unwrap(), 0);
    assert_eq!(al.get_and_set(12).await.unwrap(), 1);
    assert_eq!(al.get().await.unwrap(), 12);
    al.set(1).await.unwrap();

    assert_eq!(client.atomic_i64(name.clone()).get().await.unwrap(), 1);
    al.set(i64::MAX - 1000).await.unwrap();
    assert_eq!(
        client.atomic_i64(name).get().await.unwrap(),
        i64::MAX - 1000
    );
}
