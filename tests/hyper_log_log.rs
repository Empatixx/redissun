mod common;

use common::{client, unique};
use redissun::Object;

#[tokio::test]
async fn a_missing_log_counts_zero() {
    let log = client().await.hyper_log_log::<String>(unique("hll"));
    assert_eq!(log.count().await.unwrap(), 0);
}

#[tokio::test]
async fn insert_reports_whether_the_estimate_changed() {
    let log = client().await.hyper_log_log::<String>(unique("hll"));
    assert!(log.insert("a").await.unwrap());
    assert!(!log.insert("a").await.unwrap());
    assert_eq!(log.count().await.unwrap(), 1);
}

#[tokio::test]
async fn the_estimate_is_close_for_many_values() {
    let log = client().await.hyper_log_log::<u32>(unique("hll"));
    let values: Vec<u32> = (0..5000).collect();
    log.extend(values.iter()).await.unwrap();
    let estimate = log.count().await.unwrap() as f64;
    assert!(
        (estimate - 5000.0).abs() / 5000.0 < 0.05,
        "estimate {estimate}"
    );
}

#[tokio::test]
async fn merge_counts_the_union() {
    let client = client().await;
    let (a, b) = (unique("hll"), unique("hll"));
    let first = client.hyper_log_log::<u32>(a.clone());
    let second = client.hyper_log_log::<u32>(b.clone());
    first.extend([1u32, 2, 3].iter()).await.unwrap();
    second.extend([3u32, 4, 5].iter()).await.unwrap();
    assert_eq!(first.count_with(&[b.as_str()]).await.unwrap(), 5);
    assert_eq!(first.count().await.unwrap(), 3);
    first.merge_from(&[b.as_str()]).await.unwrap();
    assert_eq!(first.count().await.unwrap(), 5);
}

#[tokio::test]
async fn object_methods_apply_to_the_key() {
    let log = client().await.hyper_log_log::<String>(unique("hll"));
    log.insert("a").await.unwrap();
    assert!(log.exists().await.unwrap());
    assert!(log.del().await.unwrap());
    assert_eq!(log.count().await.unwrap(), 0);
}

#[tokio::test]
async fn extend_accepts_a_very_large_batch() {
    let log = client().await.hyper_log_log::<u32>(unique("hll"));
    let values: Vec<u32> = (0..30_000).collect();
    assert!(log.extend(values.iter()).await.unwrap());
    let estimate = log.count().await.unwrap() as f64;
    assert!(
        (estimate - 30_000.0).abs() / 30_000.0 < 0.05,
        "estimate {estimate}"
    );
}
