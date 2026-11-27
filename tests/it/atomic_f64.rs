use crate::common::{client, unique};
use redissun::{Comparison, Object};

#[tokio::test]
async fn compare_and_delete() {
    let number = client().await.atomic_f64(unique("atomic-f64"));
    let cases = [
        (Comparison::Less, 5.0, 15.0),
        (Comparison::LessOrEqual, 10.4, 10.5),
        (Comparison::Greater, 15.0, 5.0),
        (Comparison::GreaterOrEqual, 10.6, 10.5),
        (Comparison::Equal, 10.6, 10.5),
        (Comparison::NotEqual, 10.5, 10.6),
    ];
    for (comparison, kept, deleted) in cases {
        number.set(10.5).await.unwrap();
        assert!(!number.compare_and_delete(comparison, kept).await.unwrap());
        assert!(number.exists().await.unwrap());
        assert!(number
            .compare_and_delete(comparison, deleted)
            .await
            .unwrap());
        assert!(!number.exists().await.unwrap());
    }

    assert!(!number
        .compare_and_delete(Comparison::Equal, 0.0)
        .await
        .unwrap());
    assert!(!number
        .compare_and_delete(Comparison::Less, 0.0)
        .await
        .unwrap());
}

#[tokio::test]
async fn set_if_less() {
    let number = client().await.atomic_f64(unique("atomic-f64"));
    assert!(!number.set_if_less(0.0, 1.0).await.unwrap());
    assert_eq!(number.get().await.unwrap(), 0.0);

    number.set(12.0).await.unwrap();
    assert!(number.set_if_less(13.0, 1.0).await.unwrap());
    assert_eq!(number.get().await.unwrap(), 1.0);
}

#[tokio::test]
async fn set_if_greater() {
    let number = client().await.atomic_f64(unique("atomic-f64"));
    assert!(!number.set_if_greater(0.0, 1.0).await.unwrap());
    assert_eq!(number.get().await.unwrap(), 0.0);

    number.set(12.0).await.unwrap();
    assert!(number.set_if_greater(11.0, 1.0).await.unwrap());
    assert_eq!(number.get().await.unwrap(), 1.0);
}

#[tokio::test]
async fn a_missing_number_reads_as_zero() {
    let number = client().await.atomic_f64(unique("atomic-f64"));
    assert_eq!(number.get().await.unwrap(), 0.0);
    assert_eq!(number.swap(12.0).await.unwrap(), 0.0);
}

#[tokio::test]
async fn take() {
    let client = client().await;
    let number = client.atomic_f64(unique("atomic-f64"));
    number.set(10.34).await.unwrap();
    assert_eq!(number.take().await.unwrap(), 10.34);
    assert!(!number.exists().await.unwrap());

    let missing = client.atomic_f64(unique("atomic-f64"));
    assert_eq!(missing.take().await.unwrap(), 0.0);
}

#[tokio::test]
async fn compare_and_set() {
    let number = client().await.atomic_f64(unique("atomic-f64"));
    assert!(!number.compare_and_set(-1.0, 2.5).await.unwrap());
    assert_eq!(number.get().await.unwrap(), 0.0);

    assert!(number.compare_and_set(0.0, 2.5).await.unwrap());
    assert_eq!(number.get().await.unwrap(), 2.5);
}

#[tokio::test]
async fn set_then_increment() {
    let number = client().await.atomic_f64(unique("atomic-f64"));
    number.set(2.81).await.unwrap();
    assert_eq!(number.fetch_add(1.0).await.unwrap(), 2.81);
    assert_eq!(number.get().await.unwrap(), 3.81);
}

#[tokio::test]
async fn decrement_and_get() {
    let number = client().await.atomic_f64(unique("atomic-f64"));
    number.set(19.30).await.unwrap();
    assert_eq!(number.decr().await.unwrap(), 18.30);
    assert_eq!(number.get().await.unwrap(), 18.30);
}

#[tokio::test]
async fn add_one() {
    let number = client().await.atomic_f64(unique("atomic-f64"));
    assert_eq!(number.incr().await.unwrap(), 1.0);
    assert_eq!(number.get().await.unwrap(), 1.0);
}

#[tokio::test]
async fn fetch_add_one() {
    let number = client().await.atomic_f64(unique("atomic-f64"));
    assert_eq!(number.fetch_add(1.0).await.unwrap(), 0.0);
    assert_eq!(number.get().await.unwrap(), 1.0);
}

#[tokio::test]
async fn a_walk_through_every_step() {
    let client = client().await;
    let name = unique("atomic-f64");
    let number = client.atomic_f64(name.clone());
    assert_eq!(number.get().await.unwrap(), 0.0);
    assert_eq!(number.fetch_add(1.0).await.unwrap(), 0.0);
    assert_eq!(number.get().await.unwrap(), 1.0);
    assert_eq!(number.fetch_add(-1.0).await.unwrap(), 1.0);
    assert_eq!(number.get().await.unwrap(), 0.0);
    assert_eq!(number.fetch_add(1.0).await.unwrap(), 0.0);
    assert_eq!(number.swap(12.8012).await.unwrap(), 1.0);
    assert_eq!(number.get().await.unwrap(), 12.8012);

    number.set(1.00123).await.unwrap();
    assert_eq!(
        client.atomic_f64(name.clone()).get().await.unwrap(),
        1.00123
    );

    number.set(i64::MAX as f64).await.unwrap();
    number.incr().await.unwrap();
    assert_eq!(
        client.atomic_f64(name).get().await.unwrap(),
        i64::MAX as f64
    );
}
