mod common;

use common::{client, subscribed_channels, unique};
use redissun::{Error, Object};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout, Instant};

async fn eventually_permits(semaphore: &redissun::Semaphore, expected: i64) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while semaphore.available_permits().await.unwrap() != expected {
        assert!(
            Instant::now() < deadline,
            "permits never reached {expected}"
        );
        sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn try_set_permits_only_works_once() {
    let semaphore = client().await.semaphore(unique("sem"));
    assert!(semaphore.try_set_permits(3).await.unwrap());
    assert!(!semaphore.try_set_permits(10).await.unwrap());
    assert_eq!(semaphore.available_permits().await.unwrap(), 3);
}

#[tokio::test]
async fn a_missing_semaphore_has_no_permits() {
    let semaphore = client().await.semaphore(unique("sem"));
    assert_eq!(semaphore.available_permits().await.unwrap(), 0);
    assert!(semaphore.try_acquire(1).await.unwrap().is_none());
    let waited = semaphore.acquire_for(1, Duration::from_millis(200)).await;
    assert!(waited.unwrap().is_none());
}

#[tokio::test]
async fn try_acquire_takes_permits_until_none_are_left() {
    let semaphore = client().await.semaphore(unique("sem"));
    semaphore.try_set_permits(3).await.unwrap();
    let two = semaphore.try_acquire(2).await.unwrap().unwrap();
    assert_eq!(two.count(), 2);
    assert_eq!(semaphore.available_permits().await.unwrap(), 1);
    assert!(semaphore.try_acquire(2).await.unwrap().is_none());
    let one = semaphore.try_acquire(1).await.unwrap().unwrap();
    assert_eq!(semaphore.available_permits().await.unwrap(), 0);
    two.release().await.unwrap();
    one.release().await.unwrap();
    assert_eq!(semaphore.available_permits().await.unwrap(), 3);
}

#[tokio::test]
async fn dropping_the_permits_releases_them() {
    let semaphore = client().await.semaphore(unique("sem"));
    semaphore.try_set_permits(2).await.unwrap();
    let permits = semaphore.acquire(2).await.unwrap();
    assert_eq!(semaphore.available_permits().await.unwrap(), 0);
    drop(permits);
    eventually_permits(&semaphore, 2).await;
}

#[tokio::test]
async fn forgetting_the_permits_keeps_them_taken_until_released_by_hand() {
    let semaphore = client().await.semaphore(unique("sem"));
    semaphore.try_set_permits(2).await.unwrap();
    semaphore.acquire(2).await.unwrap().forget();
    sleep(Duration::from_millis(200)).await;
    assert_eq!(semaphore.available_permits().await.unwrap(), 0);
    semaphore.release(2).await.unwrap();
    assert_eq!(semaphore.available_permits().await.unwrap(), 2);
}

#[tokio::test]
async fn a_waiter_is_woken_by_release_long_before_the_fallback_poll() {
    let client = client().await;
    let name = unique("sem");
    let semaphore = client.semaphore(name.clone());
    semaphore.try_set_permits(1).await.unwrap();
    let held = semaphore.acquire(1).await.unwrap();

    let waiter_semaphore = client.semaphore(name);
    let started = Instant::now();
    let waiter = tokio::spawn(async move { waiter_semaphore.acquire(1).await.unwrap() });
    sleep(Duration::from_millis(300)).await;
    assert!(!waiter.is_finished());

    held.release().await.unwrap();
    let permits = timeout(Duration::from_secs(5), waiter)
        .await
        .unwrap()
        .unwrap();
    assert!(started.elapsed() < Duration::from_millis(1500));
    permits.release().await.unwrap();
}

#[tokio::test]
async fn adding_permits_wakes_a_waiter() {
    let client = client().await;
    let name = unique("sem");
    let semaphore = client.semaphore(name.clone());
    semaphore.try_set_permits(0).await.unwrap();

    let waiter_semaphore = client.semaphore(name);
    let started = Instant::now();
    let waiter = tokio::spawn(async move { waiter_semaphore.acquire(2).await.unwrap() });
    sleep(Duration::from_millis(300)).await;
    semaphore.add_permits(2).await.unwrap();

    let permits = timeout(Duration::from_secs(5), waiter)
        .await
        .unwrap()
        .unwrap();
    assert!(started.elapsed() < Duration::from_millis(1500));
    assert_eq!(permits.count(), 2);
}

#[tokio::test]
async fn acquire_for_gives_up_after_the_wait() {
    let semaphore = client().await.semaphore(unique("sem"));
    semaphore.try_set_permits(1).await.unwrap();
    let _held = semaphore.acquire(1).await.unwrap();

    let started = Instant::now();
    let result = semaphore.acquire_for(1, Duration::from_millis(300)).await;
    assert!(result.unwrap().is_none());
    assert!(started.elapsed() >= Duration::from_millis(300));
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[tokio::test]
async fn drain_permits_returns_everything_and_leaves_zero() {
    let semaphore = client().await.semaphore(unique("sem"));
    semaphore.try_set_permits(5).await.unwrap();
    assert_eq!(semaphore.drain_permits().await.unwrap(), 5);
    assert_eq!(semaphore.available_permits().await.unwrap(), 0);
    assert_eq!(semaphore.drain_permits().await.unwrap(), 0);
}

#[tokio::test]
async fn zero_permits_is_a_config_error() {
    let semaphore = client().await.semaphore(unique("sem"));
    assert!(matches!(
        semaphore.try_acquire(0).await,
        Err(Error::Config(_))
    ));
    assert!(matches!(semaphore.acquire(0).await, Err(Error::Config(_))));
    assert!(matches!(semaphore.release(0).await, Err(Error::Config(_))));
    assert!(matches!(
        semaphore.try_set_permits(0).await.map(|_| ()),
        Ok(())
    ));
}

#[tokio::test]
async fn never_more_holders_than_permits_across_clients() {
    let name = unique("sem");
    client()
        .await
        .semaphore(name.clone())
        .try_set_permits(3)
        .await
        .unwrap();
    let inside = Arc::new(AtomicU32::new(0));
    let peak = Arc::new(AtomicU32::new(0));

    let mut tasks = Vec::new();
    for _ in 0..12 {
        let semaphore = client().await.semaphore(name.clone());
        let inside = inside.clone();
        let peak = peak.clone();
        tasks.push(tokio::spawn(async move {
            let permits = semaphore.acquire(1).await.unwrap();
            let now = inside.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            sleep(Duration::from_millis(30)).await;
            inside.fetch_sub(1, Ordering::SeqCst);
            permits.release().await.unwrap();
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert!(peak.load(Ordering::SeqCst) <= 3);
    assert!(peak.load(Ordering::SeqCst) >= 2);
    assert_eq!(
        client()
            .await
            .semaphore(name)
            .available_permits()
            .await
            .unwrap(),
        3
    );
}

#[tokio::test]
async fn the_subscription_is_released_after_waiting() {
    let client = client().await;
    let name = unique("sem");
    let pattern = format!("redissun_sc:{name}");
    let semaphore = client.semaphore(name);
    semaphore.try_set_permits(0).await.unwrap();

    let waiting = semaphore.clone();
    let wait =
        tokio::spawn(async move { waiting.acquire_for(1, Duration::from_millis(800)).await });
    sleep(Duration::from_millis(300)).await;
    assert_eq!(subscribed_channels(&pattern).await, 1);
    assert!(wait.await.unwrap().unwrap().is_none());

    let deadline = Instant::now() + Duration::from_secs(5);
    while subscribed_channels(&pattern).await > 0 {
        assert!(Instant::now() < deadline, "subscription was not released");
        sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn object_operations_and_debug() {
    let name = unique("sem");
    let semaphore = client().await.semaphore(name.clone());
    semaphore.try_set_permits(1).await.unwrap();
    assert!(semaphore.exists().await.unwrap());
    assert!(format!("{semaphore:?}").contains(&name));
    assert!(semaphore.del().await.unwrap());
    assert!(!semaphore.exists().await.unwrap());
}
