mod common;

use common::{client, subscribed_channels, unique};
use redissun::Object;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
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
    let pattern = format!("redissun_countdownlatch__channel__{{{name}}}");
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
async fn a_zero_count_creates_an_open_latch() {
    let latch = client().await.count_down_latch(unique("latch"));
    assert!(latch.try_set_count(0).await.unwrap());
    assert!(latch.exists().await.unwrap());
    assert_eq!(latch.count().await.unwrap(), 0);
    timeout(Duration::from_secs(2), latch.wait())
        .await
        .unwrap()
        .unwrap();
    latch.count_down().await.unwrap();
    assert!(!latch.exists().await.unwrap());
}

#[tokio::test]
async fn test_await_timeout() {
    let client = client().await;
    let name = unique("latch1");
    let latch = client.count_down_latch(name.clone());
    assert!(latch.try_set_count(1).await.unwrap());
    let counting = client.count_down_latch(name);
    let counter = tokio::spawn(async move {
        sleep(Duration::from_millis(500)).await;
        counting.count_down().await.unwrap();
    });
    assert_eq!(latch.count().await.unwrap(), 1);
    let opened = latch
        .wait()
        .timeout(Duration::from_millis(550))
        .await
        .unwrap();
    assert!(opened.is_some());
    counter.await.unwrap();
}

#[tokio::test]
async fn test_await_timeout_fail() {
    let client = client().await;
    let name = unique("latch1");
    let latch = client.count_down_latch(name.clone());
    assert!(latch.try_set_count(1).await.unwrap());
    let counting = client.count_down_latch(name);
    let counter = tokio::spawn(async move {
        sleep(Duration::from_millis(1000)).await;
        counting.count_down().await.unwrap();
    });
    assert_eq!(latch.count().await.unwrap(), 1);
    let opened = latch
        .wait()
        .timeout(Duration::from_millis(500))
        .await
        .unwrap();
    assert!(opened.is_none());
    counter.await.unwrap();
}

#[tokio::test]
async fn test_multi_await() {
    let client = client().await;
    let name = unique("latch");
    let latch = client.count_down_latch(name.clone());
    latch.try_set_count(5).await.unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let mut waiters = Vec::new();
    for _ in 0..5 {
        let waiting = client.count_down_latch(name.clone());
        let counter = counter.clone();
        waiters.push(tokio::spawn(async move {
            if waiting
                .wait()
                .timeout(Duration::from_secs(10))
                .await
                .unwrap()
                .is_some()
            {
                counter.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }
    for _ in 0..5 {
        let counting = client.count_down_latch(name.clone());
        tokio::spawn(async move {
            sleep(Duration::from_secs(1)).await;
            counting.count_down().await.unwrap();
        });
    }
    let deadline = Instant::now() + Duration::from_secs(7);
    while latch.count().await.unwrap() != 0 {
        assert!(Instant::now() < deadline);
        sleep(Duration::from_millis(50)).await;
    }
    for waiter in waiters {
        timeout(Duration::from_secs(5), waiter)
            .await
            .unwrap()
            .unwrap();
    }
    assert_eq!(counter.load(Ordering::SeqCst), 5);
}

#[tokio::test]
async fn test_count_down() {
    let client = client().await;
    let latch = client.count_down_latch(unique("latch"));
    latch.try_set_count(2).await.unwrap();
    assert_eq!(latch.count().await.unwrap(), 2);
    latch.count_down().await.unwrap();
    assert_eq!(latch.count().await.unwrap(), 1);
    latch.count_down().await.unwrap();
    assert_eq!(latch.count().await.unwrap(), 0);
    latch.wait().await.unwrap();
    latch.count_down().await.unwrap();
    assert_eq!(latch.count().await.unwrap(), 0);
    latch.wait().await.unwrap();
    latch.count_down().await.unwrap();
    assert_eq!(latch.count().await.unwrap(), 0);
    latch.wait().await.unwrap();

    let latch1 = client.count_down_latch(unique("latch1"));
    latch1.try_set_count(1).await.unwrap();
    latch1.count_down().await.unwrap();
    assert_eq!(latch1.count().await.unwrap(), 0);
    latch1.count_down().await.unwrap();
    assert_eq!(latch1.count().await.unwrap(), 0);
    latch1.wait().await.unwrap();

    let latch2 = client.count_down_latch(unique("latch2"));
    latch2.try_set_count(1).await.unwrap();
    latch2.count_down().await.unwrap();
    latch2.wait().await.unwrap();
    latch2.wait().await.unwrap();

    let latch3 = client.count_down_latch(unique("latch3"));
    assert_eq!(latch3.count().await.unwrap(), 0);
    latch3.wait().await.unwrap();

    let latch4 = client.count_down_latch(unique("latch4"));
    assert_eq!(latch4.count().await.unwrap(), 0);
    latch4.count_down().await.unwrap();
    assert_eq!(latch4.count().await.unwrap(), 0);
    latch4.wait().await.unwrap();
}

#[tokio::test]
async fn test_delete() {
    let latch = client().await.count_down_latch(unique("latch"));
    latch.try_set_count(1).await.unwrap();
    assert!(latch.del().await.unwrap());
}

#[tokio::test]
async fn test_delete_failed() {
    let latch = client().await.count_down_latch(unique("latch"));
    assert!(!latch.del().await.unwrap());
}

#[tokio::test]
async fn test_try_set_count() {
    let latch = client().await.count_down_latch(unique("latch"));
    assert!(latch.try_set_count(1).await.unwrap());
    assert!(!latch.try_set_count(2).await.unwrap());
}

#[tokio::test]
async fn test_count() {
    let latch = client().await.count_down_latch(unique("latch"));
    assert_eq!(latch.count().await.unwrap(), 0);
}

#[tokio::test]
async fn test_single_count_down_await_single_instance() {
    let iterations = 12;
    let client = client().await;
    let name = unique("latch");
    let latch = client.count_down_latch(name.clone());
    latch.try_set_count(iterations as u64).await.unwrap();
    let counter = Arc::new(AtomicUsize::new(0));

    let mut waiters = Vec::new();
    for _ in 0..iterations {
        let waiting = client.count_down_latch(name.clone());
        waiters.push(tokio::spawn(async move {
            waiting.wait().await.unwrap();
            assert_eq!(waiting.count().await.unwrap(), 0);
        }));
    }
    for _ in 0..iterations {
        let counting = client.count_down_latch(name.clone());
        let counter = counter.clone();
        tokio::spawn(async move {
            counting.count_down().await.unwrap();
            counter.fetch_add(1, Ordering::SeqCst);
        });
    }
    for waiter in waiters {
        timeout(Duration::from_secs(10), waiter)
            .await
            .unwrap()
            .unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while counter.load(Ordering::SeqCst) != iterations {
        assert!(Instant::now() < deadline);
        sleep(Duration::from_millis(10)).await;
    }
}
