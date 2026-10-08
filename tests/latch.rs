mod common;

use common::{client, subscribed_channels, unique};
use redissun::{Error, Object};
use std::time::Duration;
use tokio::time::{sleep, timeout, Instant};

#[tokio::test]
async fn try_set_count_only_works_while_the_latch_does_not_exist() {
    let latch = client().await.count_down_latch(unique("latch"));
    assert!(latch.try_set_count(3).await.unwrap());
    assert!(!latch.try_set_count(9).await.unwrap());
    assert_eq!(latch.count().await.unwrap(), 3);
}

#[tokio::test]
async fn a_missing_latch_is_already_open() {
    let latch = client().await.count_down_latch(unique("latch"));
    assert_eq!(latch.count().await.unwrap(), 0);
    timeout(Duration::from_secs(2), latch.wait())
        .await
        .expect("waiting on a missing latch must return at once")
        .unwrap();
    assert!(latch
        .wait()
        .timeout(Duration::from_millis(50))
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn count_down_reaches_zero_and_removes_the_key() {
    let latch = client().await.count_down_latch(unique("latch"));
    latch.try_set_count(2).await.unwrap();
    latch.count_down().await.unwrap();
    assert_eq!(latch.count().await.unwrap(), 1);
    assert!(latch.exists().await.unwrap());
    latch.count_down().await.unwrap();
    assert_eq!(latch.count().await.unwrap(), 0);
    assert!(!latch.exists().await.unwrap());
}

#[tokio::test]
async fn counting_down_an_open_latch_does_not_recreate_it() {
    let latch = client().await.count_down_latch(unique("latch"));
    latch.count_down().await.unwrap();
    latch.count_down().await.unwrap();
    assert!(!latch.exists().await.unwrap());
    assert_eq!(latch.count().await.unwrap(), 0);
}

#[tokio::test]
async fn the_last_count_down_releases_every_waiter_quickly() {
    let name = unique("latch");
    let latch = client().await.count_down_latch(name.clone());
    latch.try_set_count(2).await.unwrap();

    let started = Instant::now();
    let mut waiters = Vec::new();
    for _ in 0..3 {
        let waiting = client().await.count_down_latch(name.clone());
        waiters.push(tokio::spawn(async move { waiting.wait().await.unwrap() }));
    }
    sleep(Duration::from_millis(300)).await;
    assert!(waiters.iter().all(|waiter| !waiter.is_finished()));

    latch.count_down().await.unwrap();
    sleep(Duration::from_millis(100)).await;
    assert!(waiters.iter().all(|waiter| !waiter.is_finished()));

    latch.count_down().await.unwrap();
    for waiter in waiters {
        timeout(Duration::from_secs(5), waiter)
            .await
            .unwrap()
            .unwrap();
    }
    assert!(started.elapsed() < Duration::from_millis(1500));
}

#[tokio::test]
async fn wait_for_gives_up_while_the_count_is_positive() {
    let latch = client().await.count_down_latch(unique("latch"));
    latch.try_set_count(1).await.unwrap();
    let started = Instant::now();
    assert!(latch
        .wait()
        .timeout(Duration::from_millis(300))
        .await
        .unwrap()
        .is_none());
    assert!(started.elapsed() >= Duration::from_millis(300));
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[tokio::test]
async fn concurrent_count_downs_open_the_latch_exactly_once() {
    let name = unique("latch");
    let client = client().await;
    client
        .count_down_latch(name.clone())
        .try_set_count(40)
        .await
        .unwrap();
    let waiting = client.count_down_latch(name.clone());
    let waiter = tokio::spawn(async move { waiting.wait().await.unwrap() });

    let tasks: Vec<_> = (0..8)
        .map(|_| {
            let latch = client.count_down_latch(name.clone());
            tokio::spawn(async move {
                for _ in 0..5 {
                    latch.count_down().await.unwrap();
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
    timeout(Duration::from_secs(5), waiter)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(client.count_down_latch(name).count().await.unwrap(), 0);
}

#[tokio::test]
async fn a_finished_latch_can_be_set_again() {
    let latch = client().await.count_down_latch(unique("latch"));
    latch.try_set_count(1).await.unwrap();
    latch.count_down().await.unwrap();
    assert!(latch.try_set_count(2).await.unwrap());
    assert_eq!(latch.count().await.unwrap(), 2);
}

#[tokio::test]
async fn deleting_the_latch_opens_it_for_waiters() {
    let name = unique("latch");
    let latch = client().await.count_down_latch(name.clone());
    latch.try_set_count(5).await.unwrap();
    let waiting = client().await.count_down_latch(name);
    let waiter = tokio::spawn(async move { waiting.wait().await.unwrap() });
    sleep(Duration::from_millis(300)).await;
    assert!(latch.del().await.unwrap());
    timeout(Duration::from_secs(5), waiter)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn the_subscription_is_released_after_waiting() {
    let name = unique("latch");
    let pattern = format!("redissun_countdownlatch__channel__{name}");
    let latch = client().await.count_down_latch(name);
    latch.try_set_count(1).await.unwrap();

    let waiting = latch.clone();
    let wait =
        tokio::spawn(async move { waiting.wait().timeout(Duration::from_millis(800)).await });
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
async fn debug_shows_the_name() {
    let name = unique("latch");
    let latch = client().await.count_down_latch(name.clone());
    assert!(format!("{latch:?}").contains(&name));
}

#[tokio::test]
async fn a_zero_count_is_rejected_so_no_dead_key_is_left_behind() {
    let latch = client().await.count_down_latch(unique("latch"));
    assert!(matches!(
        latch.try_set_count(0).await,
        Err(Error::Config(_))
    ));
    assert!(!latch.exists().await.unwrap());
}
