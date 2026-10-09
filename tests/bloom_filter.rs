mod common;

use common::{client, unique};
use redissun::{Error, Object};

#[tokio::test]
async fn it_must_be_initialised_first() {
    let filter = client().await.bloom_filter::<String>(unique("bloom"));
    assert!(matches!(filter.insert("a").await, Err(Error::Config(_))));
    assert!(matches!(filter.contains("a").await, Err(Error::Config(_))));
}

#[tokio::test]
async fn try_init_works_once() {
    let filter = client().await.bloom_filter::<String>(unique("bloom"));
    assert!(filter.try_init(1000, 0.01).await.unwrap());
    assert!(!filter.try_init(5000, 0.5).await.unwrap());
    assert_eq!(filter.expected_insertions().await.unwrap(), 1000);
    assert!(filter.size_bits().await.unwrap() > 1000);
    assert!(filter.hash_iterations().await.unwrap() >= 1);
}

#[tokio::test]
async fn invalid_parameters_are_config_errors() {
    let filter = client().await.bloom_filter::<String>(unique("bloom"));
    assert!(matches!(
        filter.try_init(0, 0.01).await,
        Err(Error::Config(_))
    ));
    assert!(matches!(
        filter.try_init(10, 0.0).await,
        Err(Error::Config(_))
    ));
    assert!(matches!(
        filter.try_init(10, 1.0).await,
        Err(Error::Config(_))
    ));
}

#[tokio::test]
async fn inserted_values_are_always_found() {
    let filter = client().await.bloom_filter::<u32>(unique("bloom"));
    filter.try_init(2000, 0.01).await.unwrap();
    for value in 0..1000u32 {
        filter.insert(&value).await.unwrap();
    }
    for value in 0..1000u32 {
        assert!(filter.contains(&value).await.unwrap(), "{value} was lost");
    }
}

#[tokio::test]
async fn insert_reports_whether_the_value_was_new() {
    let filter = client().await.bloom_filter::<String>(unique("bloom"));
    filter.try_init(100, 0.001).await.unwrap();
    assert!(filter.insert("a").await.unwrap());
    assert!(!filter.insert("a").await.unwrap());
}

#[tokio::test]
async fn false_positives_stay_near_the_requested_rate() {
    let filter = client().await.bloom_filter::<u32>(unique("bloom"));
    filter.try_init(1000, 0.01).await.unwrap();
    for value in 0..1000u32 {
        filter.insert(&value).await.unwrap();
    }
    let mut false_positives = 0;
    for value in 100_000..102_000u32 {
        if filter.contains(&value).await.unwrap() {
            false_positives += 1;
        }
    }
    assert!(false_positives < 80, "{false_positives} of 2000");
}

#[tokio::test]
async fn count_estimates_the_number_of_values() {
    let filter = client().await.bloom_filter::<u32>(unique("bloom"));
    filter.try_init(1000, 0.01).await.unwrap();
    for value in 0..500u32 {
        filter.insert(&value).await.unwrap();
    }
    let estimate = filter.count().await.unwrap() as f64;
    assert!(
        (estimate - 500.0).abs() / 500.0 < 0.1,
        "estimate {estimate}"
    );
}

#[tokio::test]
async fn another_client_shares_the_filter_and_its_settings() {
    let name = unique("bloom");
    let first = client().await.bloom_filter::<String>(name.clone());
    first.try_init(1000, 0.01).await.unwrap();
    first.insert("shared").await.unwrap();
    let second = client().await.bloom_filter::<String>(name);
    assert!(second.contains("shared").await.unwrap());
    assert!(!second.contains("other").await.unwrap());
}

#[tokio::test]
async fn object_methods_cover_the_config() {
    let filter = client().await.bloom_filter::<String>(unique("bloom"));
    filter.try_init(100, 0.01).await.unwrap();
    filter.insert("a").await.unwrap();
    assert!(filter.exists().await.unwrap());
    assert!(filter.del().await.unwrap());
    assert!(filter.try_init(100, 0.01).await.unwrap());
}
