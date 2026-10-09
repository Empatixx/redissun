mod common;

use common::{client, raw_command, unique};
use redissun::{Error, Object};
use std::time::Duration;

#[tokio::test]
async fn it_must_be_initialised_before_inserting_and_contains_nothing_before() {
    let filter = client().await.bloom_filter::<String>(unique("bloom"));
    assert!(matches!(filter.insert("a").await, Err(Error::Config(_))));
    assert!(!filter.contains("a").await.unwrap());
    assert!(matches!(filter.count().await, Err(Error::Config(_))));
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
    assert!(client()
        .await
        .bloom_filter::<String>(unique("bloom"))
        .try_init(10, 0.0)
        .await
        .unwrap());
    let filter = client().await.bloom_filter::<String>(unique("bloom"));
    assert!(matches!(
        filter.try_init(0, 0.01).await,
        Err(Error::Config(_))
    ));
    assert!(matches!(
        filter.try_init(1_000_000_000, 0.0).await,
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

#[tokio::test]
async fn a_filter_set_up_again_with_the_same_size_but_other_hashes_is_detected() {
    let name = unique("bloom");
    let first = client().await.bloom_filter::<u32>(name.clone());
    first.try_init(1000, 0.01).await.unwrap();
    first.insert(&1).await.unwrap();
    let k = first.hash_iterations().await.unwrap();
    let size = first.size_bits().await.unwrap();
    first.del().await.unwrap();
    common::raw_command(&[
        "HSET",
        &format!("{{{name}}}:config"),
        "size",
        &size.to_string(),
        "hashIterations",
        &(k + 1).to_string(),
        "expectedInsertions",
        "1000",
        "falseProbability",
        "0.01",
    ])
    .await;
    assert!(matches!(first.insert(&2).await, Err(Error::Config(_))));
    assert_eq!(first.hash_iterations().await.unwrap(), k + 1);
}

#[tokio::test]
async fn keys_are_laid_out_as_in_redisson() {
    let name = unique("bloom");
    let filter = client().await.bloom_filter::<String>(name.clone());
    filter.try_init(100, 0.03).await.unwrap();
    assert_eq!(remaining_keys(&name).await, 1);
    filter.insert("a").await.unwrap();
    let config = format!("{{{name}}}:config");
    assert_eq!(raw_command(&["EXISTS", &name, &config]).await, ":2\r\n");
    assert_eq!(raw_command(&["TYPE", &name]).await, "+string\r\n");
    assert_eq!(raw_command(&["TYPE", &config]).await, "+hash\r\n");
}

#[tokio::test]
async fn contains_after_the_filter_was_set_up_again_is_false_then_follows_the_new_settings() {
    let name = unique("bloom");
    let client = client().await;
    let stale = client.bloom_filter::<String>(name.clone());
    stale.try_init(100, 0.03).await.unwrap();
    stale.insert("1").await.unwrap();
    assert!(stale.contains("1").await.unwrap());
    let fresh = client.bloom_filter::<String>(name);
    fresh.del().await.unwrap();
    fresh.try_init(200, 0.03).await.unwrap();
    fresh.insert("1").await.unwrap();
    assert!(!stale.contains("1").await.unwrap());
    assert!(stale.contains("1").await.unwrap());
}

async fn remaining_keys(name: &str) -> usize {
    let config = format!("{{{name}}}:config");
    let reply = raw_command(&["EXISTS", name, &config]).await;
    reply.trim_start_matches(':').trim().parse().unwrap()
}

#[tokio::test]
async fn test_contains_all() {
    let filter = client().await.bloom_filter::<String>(unique("filter"));
    filter.try_init(100, 0.03).await.unwrap();
    let list = ["1", "2", "3"];
    assert_eq!(filter.contains_all(&list).await.unwrap(), 0);
    assert_eq!(filter.insert_all(&list).await.unwrap(), 3);
    assert_eq!(filter.contains_all(&list).await.unwrap(), 3);
    assert_eq!(filter.contains_all(&["1", "5"]).await.unwrap(), 1);
}

#[tokio::test]
async fn test_exists_all() {
    let filter = client().await.bloom_filter::<String>(unique("filter"));
    filter.try_init(100, 0.03).await.unwrap();
    let list = ["1", "2", "3"];
    assert_eq!(
        filter.contains_each(&list).await.unwrap(),
        [false, false, false]
    );
    assert_eq!(filter.insert_all(&list).await.unwrap(), 3);
    assert_eq!(
        filter.contains_each(&list).await.unwrap(),
        [true, true, true]
    );
    assert_eq!(
        filter.contains_each(&["1", "5"]).await.unwrap(),
        [true, false]
    );
}

#[tokio::test]
async fn test_add_all() {
    let filter = client().await.bloom_filter::<String>(unique("filter"));
    filter.try_init(100, 0.03).await.unwrap();
    let list = ["1", "2", "3"];
    assert_eq!(filter.insert_all(&list).await.unwrap(), 3);
    assert_eq!(filter.insert_all(&list).await.unwrap(), 0);
    assert_eq!(filter.count().await.unwrap(), 3);
    assert_eq!(filter.insert_all(&["1", "5"]).await.unwrap(), 1);
    assert_eq!(filter.count().await.unwrap(), 4);
    for value in list {
        assert!(filter.contains(value).await.unwrap());
    }
}

#[tokio::test]
async fn test_false_probability1() {
    let filter = client().await.bloom_filter::<String>(unique("filter"));
    assert!(matches!(
        filter.try_init(1, -1.0).await,
        Err(Error::Config(_))
    ));
}

#[tokio::test]
async fn test_false_probability2() {
    let filter = client().await.bloom_filter::<String>(unique("filter"));
    assert!(matches!(
        filter.try_init(1, 2.0).await,
        Err(Error::Config(_))
    ));
}

#[tokio::test]
async fn test_size_zero() {
    let filter = client().await.bloom_filter::<String>(unique("filter"));
    assert!(matches!(
        filter.try_init(1, 1.0).await,
        Err(Error::Config(_))
    ));
}

#[tokio::test]
async fn test_config() {
    let filter = client().await.bloom_filter::<String>(unique("filter"));
    filter.try_init(100, 0.03).await.unwrap();
    assert_eq!(filter.expected_insertions().await.unwrap(), 100);
    assert_eq!(filter.false_probability().await.unwrap(), 0.03);
    assert_eq!(filter.hash_iterations().await.unwrap(), 5);
    assert_eq!(filter.size_bits().await.unwrap(), 729);
}

#[tokio::test]
async fn test_init() {
    let name = unique("filter");
    let filter = client().await.bloom_filter::<String>(name.clone());
    assert!(filter.try_init(55_000_000, 0.03).await.unwrap());
    assert!(!filter.try_init(55_000_001, 0.03).await.unwrap());
    filter.del().await.unwrap();
    assert_eq!(remaining_keys(&name).await, 0);
    assert!(filter.try_init(55_000_001, 0.03).await.unwrap());
}

#[tokio::test]
async fn test_not_initialized_on_expected_insertions() {
    let filter = client().await.bloom_filter::<String>(unique("filter"));
    assert!(filter.expected_insertions().await.is_err());
}

#[tokio::test]
async fn test_expire() {
    let name = unique("filter");
    let filter = client().await.bloom_filter::<String>(name.clone());
    filter.try_init(550_000, 0.03).await.unwrap();
    filter.insert("test").await.unwrap();
    assert!(filter.expire(Duration::from_secs(2)).await.unwrap());
    tokio::time::sleep(Duration::from_millis(2100)).await;
    assert_eq!(remaining_keys(&name).await, 0);
}

#[tokio::test]
async fn test_not_initialized_on_add() {
    let filter = client().await.bloom_filter::<String>(unique("filter"));
    assert!(filter.insert("123").await.is_err());
}

#[tokio::test]
async fn test_empty_rename() {
    let client = client().await;
    let name = unique("test");
    let renamed = unique("test1");
    let filter = client.bloom_filter::<String>(name.clone());
    filter.try_init(1000, 0.01).await.unwrap();
    filter.rename(&renamed).await.unwrap();
    assert!(client
        .bloom_filter::<String>(renamed)
        .exists()
        .await
        .unwrap());
    assert!(!client.bloom_filter::<String>(name).exists().await.unwrap());
}

#[tokio::test]
async fn test() {
    let filter = client().await.bloom_filter::<String>(unique("filter"));
    filter.try_init(550_000, 0.5).await.unwrap();
    check(&filter).await;
    filter.del().await.unwrap();
    assert!(filter.try_init(550_000, 0.03).await.unwrap());
    check(&filter).await;
}

async fn check(filter: &redissun::BloomFilter<String, redissun::JsonCodec>) {
    assert!(!filter.contains("123").await.unwrap());
    assert!(filter.insert("123").await.unwrap());
    assert!(filter.contains("123").await.unwrap());
    assert!(!filter.insert("123").await.unwrap());
    assert_eq!(filter.count().await.unwrap(), 1);

    let other = "hflgs;jl;ao1-32471320o31803-24";
    assert!(!filter.contains(other).await.unwrap());
    assert!(filter.insert(other).await.unwrap());
    assert!(filter.contains(other).await.unwrap());
    assert_eq!(filter.count().await.unwrap(), 2);
}

#[tokio::test]
async fn test_rename() {
    let client = client().await;
    let name = unique("filter");
    let renamed = unique("new_filter");
    let filter = client.bloom_filter::<String>(name.clone());
    filter.try_init(550_000, 0.03).await.unwrap();
    assert!(filter.insert("123").await.unwrap());
    filter.rename(&renamed).await.unwrap();

    let filter2 = client.bloom_filter::<String>(renamed);
    assert_eq!(filter2.count().await.unwrap(), 1);
    let filter3 = client.bloom_filter::<String>(name);
    assert!(!filter3.exists().await.unwrap());
}

#[tokio::test]
async fn test_contains_exception() {
    let client = client().await;
    let name = unique("filter");
    let f1 = client.bloom_filter::<String>(name.clone());
    assert!(!f1.contains("1").await.unwrap());
    f1.try_init(100, 0.03).await.unwrap();

    let f2 = client.bloom_filter::<String>(name);
    f2.del().await.unwrap();
    f2.try_init(200, 0.03).await.unwrap();

    assert!(!f1.contains("1").await.unwrap());
}
