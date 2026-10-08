mod common;

use common::{client, unique};
use redissun::{Error, Object, RateType};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, Instant};

#[tokio::test]
async fn an_unconfigured_limiter_is_an_error() {
    let limiter = client().await.rate_limiter(unique("rate"));
    let error = limiter.try_acquire(1).await.unwrap_err();
    assert!(error.to_string().contains("not initialized"), "{error}");
    assert!(limiter.available_permits().await.is_err());
}

#[tokio::test]
async fn try_set_rate_only_works_once() {
    let limiter = client().await.rate_limiter(unique("rate"));
    let second = Duration::from_secs(1);
    assert!(limiter
        .try_set_rate(RateType::Overall, 3, second)
        .await
        .unwrap());
    assert!(!limiter
        .try_set_rate(RateType::Overall, 9, second)
        .await
        .unwrap());
    for _ in 0..3 {
        assert!(limiter.try_acquire(1).await.unwrap());
    }
    assert!(!limiter.try_acquire(1).await.unwrap());
}

#[tokio::test]
async fn permits_come_back_after_the_interval() {
    let limiter = client().await.rate_limiter(unique("rate"));
    limiter
        .try_set_rate(RateType::Overall, 3, Duration::from_millis(600))
        .await
        .unwrap();
    for _ in 0..3 {
        assert!(limiter.try_acquire(1).await.unwrap());
    }
    assert!(!limiter.try_acquire(1).await.unwrap());
    sleep(Duration::from_millis(750)).await;
    for _ in 0..3 {
        assert!(limiter.try_acquire(1).await.unwrap());
    }
    assert!(!limiter.try_acquire(1).await.unwrap());
}

#[tokio::test]
async fn the_window_slides_instead_of_resetting() {
    let limiter = client().await.rate_limiter(unique("rate"));
    limiter
        .try_set_rate(RateType::Overall, 2, Duration::from_millis(800))
        .await
        .unwrap();
    assert!(limiter.try_acquire(1).await.unwrap());
    sleep(Duration::from_millis(450)).await;
    assert!(limiter.try_acquire(1).await.unwrap());
    assert!(!limiter.try_acquire(1).await.unwrap());
    sleep(Duration::from_millis(450)).await;
    assert!(limiter.try_acquire(1).await.unwrap());
    assert!(!limiter.try_acquire(1).await.unwrap());
}

#[tokio::test]
async fn several_permits_can_be_taken_in_one_call() {
    let limiter = client().await.rate_limiter(unique("rate"));
    limiter
        .try_set_rate(RateType::Overall, 5, Duration::from_secs(10))
        .await
        .unwrap();
    assert!(limiter.try_acquire(3).await.unwrap());
    assert!(!limiter.try_acquire(3).await.unwrap());
    assert!(limiter.try_acquire(2).await.unwrap());
    assert!(!limiter.try_acquire(1).await.unwrap());
}

#[tokio::test]
async fn asking_for_more_than_the_rate_is_an_error() {
    let limiter = client().await.rate_limiter(unique("rate"));
    limiter
        .try_set_rate(RateType::Overall, 2, Duration::from_secs(10))
        .await
        .unwrap();
    let error = limiter.try_acquire(3).await.unwrap_err();
    assert!(error.to_string().contains("cannot exceed"), "{error}");
}

#[tokio::test]
async fn invalid_arguments_are_config_errors() {
    let limiter = client().await.rate_limiter(unique("rate"));
    let second = Duration::from_secs(1);
    assert!(matches!(
        limiter.try_set_rate(RateType::Overall, 0, second).await,
        Err(Error::Config(_))
    ));
    assert!(matches!(
        limiter
            .try_set_rate(RateType::Overall, 1, Duration::ZERO)
            .await,
        Err(Error::Config(_))
    ));
    limiter
        .try_set_rate(RateType::Overall, 1, second)
        .await
        .unwrap();
    assert!(matches!(
        limiter.try_acquire(0).await,
        Err(Error::Config(_))
    ));
    assert!(matches!(limiter.acquire(0).await, Err(Error::Config(_))));
}

#[tokio::test]
async fn an_overall_limit_is_shared_between_clients() {
    let name = unique("rate");
    let first = client().await.rate_limiter(name.clone());
    let second = client().await.rate_limiter(name);
    first
        .try_set_rate(RateType::Overall, 3, Duration::from_secs(10))
        .await
        .unwrap();
    assert!(first.try_acquire(3).await.unwrap());
    assert!(!second.try_acquire(1).await.unwrap());
}

#[tokio::test]
async fn a_per_client_limit_is_counted_for_each_client() {
    let name = unique("rate");
    let first = client().await.rate_limiter(name.clone());
    let second = client().await.rate_limiter(name);
    first
        .try_set_rate(RateType::PerClient, 3, Duration::from_secs(10))
        .await
        .unwrap();
    assert!(first.try_acquire(3).await.unwrap());
    assert!(!first.try_acquire(1).await.unwrap());
    assert!(second.try_acquire(3).await.unwrap());
    assert!(!second.try_acquire(1).await.unwrap());
}

#[tokio::test]
async fn try_acquire_for_waits_for_the_window_to_move() {
    let limiter = client().await.rate_limiter(unique("rate"));
    limiter
        .try_set_rate(RateType::Overall, 1, Duration::from_millis(500))
        .await
        .unwrap();
    assert!(limiter.try_acquire(1).await.unwrap());

    let started = Instant::now();
    assert!(limiter
        .try_acquire_for(1, Duration::from_secs(3))
        .await
        .unwrap());
    assert!(started.elapsed() >= Duration::from_millis(350));
    assert!(started.elapsed() < Duration::from_millis(1500));

    let started = Instant::now();
    assert!(!limiter
        .try_acquire_for(1, Duration::from_millis(100))
        .await
        .unwrap());
    assert!(started.elapsed() >= Duration::from_millis(100));
    assert!(started.elapsed() < Duration::from_millis(450));
}

#[tokio::test]
async fn acquire_blocks_until_a_permit_is_free() {
    let limiter = client().await.rate_limiter(unique("rate"));
    limiter
        .try_set_rate(RateType::Overall, 1, Duration::from_millis(500))
        .await
        .unwrap();
    limiter.acquire(1).await.unwrap();
    let started = Instant::now();
    limiter.acquire(1).await.unwrap();
    assert!(started.elapsed() >= Duration::from_millis(350));
    assert!(started.elapsed() < Duration::from_millis(1500));
}

#[tokio::test]
async fn concurrent_callers_never_get_more_than_the_rate() {
    let name = unique("rate");
    client()
        .await
        .rate_limiter(name.clone())
        .try_set_rate(RateType::Overall, 7, Duration::from_secs(30))
        .await
        .unwrap();
    let granted = Arc::new(AtomicU32::new(0));
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let limiter = client().await.rate_limiter(name.clone());
        let granted = granted.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..10 {
                if limiter.try_acquire(1).await.unwrap() {
                    granted.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(granted.load(Ordering::SeqCst), 7);
}

#[tokio::test]
async fn available_permits_reflect_what_was_taken() {
    let limiter = client().await.rate_limiter(unique("rate"));
    limiter
        .try_set_rate(RateType::Overall, 5, Duration::from_millis(600))
        .await
        .unwrap();
    assert_eq!(limiter.available_permits().await.unwrap(), 5);
    assert!(limiter.try_acquire(2).await.unwrap());
    assert_eq!(limiter.available_permits().await.unwrap(), 3);
    sleep(Duration::from_millis(750)).await;
    assert_eq!(limiter.available_permits().await.unwrap(), 5);
}

#[tokio::test]
async fn set_rate_replaces_the_configuration_and_resets_usage() {
    let limiter = client().await.rate_limiter(unique("rate"));
    limiter
        .try_set_rate(RateType::Overall, 1, Duration::from_secs(30))
        .await
        .unwrap();
    assert!(limiter.try_acquire(1).await.unwrap());
    assert!(!limiter.try_acquire(1).await.unwrap());
    limiter
        .set_rate(RateType::Overall, 4, Duration::from_secs(30))
        .await
        .unwrap();
    assert!(limiter.try_acquire(4).await.unwrap());
    assert!(!limiter.try_acquire(1).await.unwrap());
}

#[tokio::test]
async fn object_operations_cover_every_key_of_the_limiter() {
    let client = client().await;
    let name = unique("rate");
    let limiter = client.rate_limiter(name.clone());
    assert!(!limiter.exists().await.unwrap());
    limiter
        .try_set_rate(RateType::Overall, 2, Duration::from_secs(30))
        .await
        .unwrap();
    limiter.try_acquire(1).await.unwrap();
    assert!(limiter.exists().await.unwrap());
    assert_eq!(limiter.name(), name);

    assert!(limiter.expire(Duration::from_secs(60)).await.unwrap());
    assert!(limiter.ttl().await.unwrap().is_some());
    assert!(limiter.persist().await.unwrap());
    assert_eq!(limiter.ttl().await.unwrap(), None);

    assert!(matches!(
        limiter.rename("other").await,
        Err(Error::Unsupported(_))
    ));

    assert!(limiter.del().await.unwrap());
    assert!(!limiter.exists().await.unwrap());
    assert!(limiter.try_acquire(1).await.is_err());
    assert!(limiter
        .try_set_rate(RateType::Overall, 2, Duration::from_secs(30))
        .await
        .unwrap());
    assert!(limiter.try_acquire(2).await.unwrap());
}

#[tokio::test]
async fn debug_shows_the_name() {
    let name = unique("rate");
    let limiter = client().await.rate_limiter(name.clone());
    assert!(format!("{limiter:?}").contains(&name));
}
