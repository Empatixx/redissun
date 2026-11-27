mod common;

use common::{client, subscribed_channels, unique};
use redissun::{Client, Object, Semaphore};
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
async fn init_permits_only_works_once() {
    let semaphore = client().await.semaphore(unique("sem"));
    assert!(semaphore.init_permits(3).await.unwrap());
    assert!(!semaphore.init_permits(10).await.unwrap());
    assert_eq!(semaphore.available_permits().await.unwrap(), 3);
}

#[tokio::test]
async fn a_missing_semaphore_has_no_permits() {
    let semaphore = client().await.semaphore(unique("sem"));
    assert_eq!(semaphore.available_permits().await.unwrap(), 0);
    assert!(semaphore.try_acquire().await.unwrap().is_none());
    let waited = semaphore
        .acquire()
        .timeout(Duration::from_millis(200))
        .await;
    assert!(waited.unwrap().is_none());
}

#[tokio::test]
async fn try_acquire_takes_permits_until_none_are_left() {
    let semaphore = client().await.semaphore(unique("sem"));
    semaphore.init_permits(3).await.unwrap();
    let two = semaphore.try_acquire_many(2).await.unwrap().unwrap();
    assert_eq!(two.count(), 2);
    assert_eq!(semaphore.available_permits().await.unwrap(), 1);
    assert!(semaphore.try_acquire_many(2).await.unwrap().is_none());
    let one = semaphore.try_acquire().await.unwrap().unwrap();
    assert_eq!(semaphore.available_permits().await.unwrap(), 0);
    two.release().await.unwrap();
    one.release().await.unwrap();
    assert_eq!(semaphore.available_permits().await.unwrap(), 3);
}

#[tokio::test]
async fn dropping_the_permits_releases_them() {
    let semaphore = client().await.semaphore(unique("sem"));
    semaphore.init_permits(2).await.unwrap();
    let permits = semaphore.acquire_many(2).await.unwrap();
    assert_eq!(semaphore.available_permits().await.unwrap(), 0);
    drop(permits);
    eventually_permits(&semaphore, 2).await;
}

#[tokio::test]
async fn forgetting_the_permits_keeps_them_taken_until_released_by_hand() {
    let semaphore = client().await.semaphore(unique("sem"));
    semaphore.init_permits(2).await.unwrap();
    semaphore.acquire_many(2).await.unwrap().forget();
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
    semaphore.init_permits(1).await.unwrap();
    let held = semaphore.acquire().await.unwrap();

    let waiter_semaphore = client.semaphore(name);
    let started = Instant::now();
    let waiter = tokio::spawn(async move { waiter_semaphore.acquire().await.unwrap() });
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
    semaphore.init_permits(0).await.unwrap();

    let waiter_semaphore = client.semaphore(name);
    let started = Instant::now();
    let waiter = tokio::spawn(async move { waiter_semaphore.acquire_many(2).await.unwrap() });
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
    semaphore.init_permits(1).await.unwrap();
    let _held = semaphore.acquire().await.unwrap();

    let started = Instant::now();
    let result = semaphore
        .acquire()
        .timeout(Duration::from_millis(300))
        .await;
    assert!(result.unwrap().is_none());
    assert!(started.elapsed() >= Duration::from_millis(300));
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[tokio::test]
async fn drain_permits_returns_everything_and_leaves_zero() {
    let semaphore = client().await.semaphore(unique("sem"));
    semaphore.init_permits(5).await.unwrap();
    assert_eq!(semaphore.drain_permits().await.unwrap(), 5);
    assert_eq!(semaphore.available_permits().await.unwrap(), 0);
    assert_eq!(semaphore.drain_permits().await.unwrap(), 0);
}

#[tokio::test]
async fn zero_permits_succeed_without_touching_redis() {
    let semaphore = client().await.semaphore(unique("sem"));
    let none = semaphore.try_acquire_many(0).await.unwrap().unwrap();
    assert_eq!(none.count(), 0);
    none.release().await.unwrap();
    semaphore.acquire_many(0).await.unwrap().forget();
    semaphore.release(0).await.unwrap();
    assert!(!semaphore.release_if_exists(0).await.unwrap());
    assert!(!semaphore.exists().await.unwrap());
}

#[tokio::test]
async fn never_more_holders_than_permits_across_clients() {
    let name = unique("sem");
    client()
        .await
        .semaphore(name.clone())
        .init_permits(3)
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
            let permits = semaphore.acquire().await.unwrap();
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
    let pattern = format!("redissun_sc:{{{name}}}");
    let semaphore = client.semaphore(name);
    semaphore.init_permits(0).await.unwrap();

    let waiting = semaphore.clone();
    let wait =
        tokio::spawn(async move { waiting.acquire().timeout(Duration::from_millis(800)).await });
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
    semaphore.init_permits(1).await.unwrap();
    assert!(semaphore.exists().await.unwrap());
    assert!(format!("{semaphore:?}").contains(&name));
    assert!(semaphore.del().await.unwrap());
    assert!(!semaphore.exists().await.unwrap());
}

#[tokio::test]
async fn cancelling_an_acquire_at_any_moment_does_not_lose_permits() {
    let semaphore = client().await.semaphore(unique("sem"));
    semaphore.init_permits(3).await.unwrap();
    for micros in (0..900).step_by(10) {
        let _ = timeout(Duration::from_micros(micros), semaphore.acquire()).await;
    }
    eventually_permits(&semaphore, 3).await;
}

async fn take(semaphore: &Semaphore, permits: u64) {
    semaphore.acquire_many(permits).await.unwrap().forget();
}

async fn run_concurrently<F, Fut>(instances: usize, shared: Option<Client>, work: F)
where
    F: Fn(Client) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let mut tasks = Vec::new();
    for _ in 0..instances {
        let client = match &shared {
            Some(client) => client.clone(),
            None => client().await,
        };
        tasks.push(tokio::spawn(work(client)));
    }
    for task in tasks {
        timeout(Duration::from_secs(120), task)
            .await
            .unwrap()
            .unwrap();
    }
}

// Redisson: testAcquireAfterAddPermits
#[tokio::test]
async fn a_waiter_for_two_permits_wakes_once_enough_are_added() {
    let client = client().await;
    let name = unique("test");
    let semaphore = client.semaphore(name.clone());
    let acquired = Arc::new(tokio::sync::Notify::new());
    let waiting = client.semaphore(name);
    let notify = acquired.clone();
    let waiter = tokio::spawn(async move {
        waiting.add_permits(1).await.unwrap();
        take(&waiting, 2).await;
        notify.notify_one();
    });
    sleep(Duration::from_millis(1000)).await;
    assert!(timeout(Duration::from_secs(1), acquired.notified())
        .await
        .is_err());
    take(&semaphore, 1).await;
    sleep(Duration::from_millis(1000)).await;
    semaphore.release(1).await.unwrap();
    assert!(timeout(Duration::from_secs(1), acquired.notified())
        .await
        .is_err());
    semaphore.add_permits(1).await.unwrap();
    assert!(timeout(Duration::from_secs(1), acquired.notified())
        .await
        .is_ok());
    waiter.await.unwrap();
}

// Redisson: testZero
#[tokio::test]
async fn acquiring_and_releasing_zero_permits_changes_nothing() {
    let semaphore = client().await.semaphore(unique("test"));
    assert!(semaphore
        .acquire_many(0)
        .timeout(Duration::from_secs(600))
        .await
        .unwrap()
        .is_some());
    semaphore.release(0).await.unwrap();
    assert_eq!(semaphore.available_permits().await.unwrap(), 0);
}

// Redisson: testAcquireWithoutSetPermits
#[tokio::test]
async fn released_permits_can_be_acquired_without_setting_permits() {
    let semaphore = client().await.semaphore(unique("test"));
    semaphore.release(1).await.unwrap();
    semaphore.release(1).await.unwrap();
    take(&semaphore, 2).await;
}

// Redisson: testTrySetPermits
#[tokio::test]
async fn init_permits_sets_once_and_the_ttl_variant_expires() {
    let semaphore = client().await.semaphore(unique("test"));
    assert!(semaphore.init_permits(10).await.unwrap());
    assert_eq!(semaphore.available_permits().await.unwrap(), 10);
    assert!(!semaphore.init_permits(15).await.unwrap());
    assert_eq!(semaphore.available_permits().await.unwrap(), 10);
    semaphore.del().await.unwrap();

    assert!(!semaphore.exists().await.unwrap());
    assert!(semaphore
        .init_permits(1)
        .ttl(Duration::from_secs(2))
        .await
        .unwrap());
    sleep(Duration::from_millis(1000)).await;
    assert_eq!(semaphore.available_permits().await.unwrap(), 1);
    sleep(Duration::from_millis(1100)).await;
    assert_eq!(semaphore.available_permits().await.unwrap(), 0);
    assert!(!semaphore.exists().await.unwrap());
}

// Redisson: testAddPermits
#[tokio::test]
async fn add_permits_and_release_both_raise_the_available_permits() {
    let semaphore = client().await.semaphore(unique("test"));
    semaphore.init_permits(10).await.unwrap();
    take(&semaphore, 10).await;
    assert_eq!(semaphore.available_permits().await.unwrap(), 0);
    semaphore.add_permits(4).await.unwrap();
    assert_eq!(semaphore.available_permits().await.unwrap(), 4);
    semaphore.release(10).await.unwrap();
    assert_eq!(semaphore.available_permits().await.unwrap(), 14);
    take(&semaphore, 5).await;
    assert_eq!(semaphore.available_permits().await.unwrap(), 9);
}

// Redisson: testReducePermits
#[tokio::test]
async fn negative_add_permits_can_make_the_available_permits_negative() {
    let semaphore = client().await.semaphore(unique("test2"));
    semaphore.init_permits(10).await.unwrap();
    take(&semaphore, 10).await;
    semaphore.add_permits(-5).await.unwrap();
    assert_eq!(semaphore.available_permits().await.unwrap(), -5);
    semaphore.release(10).await.unwrap();
    assert_eq!(semaphore.available_permits().await.unwrap(), 5);
    take(&semaphore, 5).await;
    assert_eq!(semaphore.available_permits().await.unwrap(), 0);
}

// Redisson: testBlockingAcquire
#[tokio::test]
async fn acquire_waits_until_another_owner_releases_a_permit() {
    let client = client().await;
    let name = unique("test");
    let semaphore = client.semaphore(name.clone());
    semaphore.init_permits(1).await.unwrap();
    take(&semaphore, 1).await;

    let releasing = client.semaphore(name);
    tokio::spawn(async move {
        sleep(Duration::from_millis(1000)).await;
        releasing.release(1).await.unwrap();
    });

    assert_eq!(semaphore.available_permits().await.unwrap(), 0);
    take(&semaphore, 1).await;
    assert!(semaphore.try_acquire().await.unwrap().is_none());
    assert_eq!(semaphore.available_permits().await.unwrap(), 0);
}

// Redisson: testBlockingNAcquire
#[tokio::test]
async fn acquire_of_several_permits_waits_until_enough_are_released() {
    let client = client().await;
    let name = unique("test");
    let semaphore = client.semaphore(name.clone());
    semaphore.init_permits(5).await.unwrap();
    take(&semaphore, 3).await;

    let releasing = client.semaphore(name);
    assert_eq!(semaphore.available_permits().await.unwrap(), 2);
    tokio::spawn(async move {
        sleep(Duration::from_millis(500)).await;
        releasing.release(1).await.unwrap();
        sleep(Duration::from_millis(500)).await;
        releasing.release(1).await.unwrap();
    });

    timeout(Duration::from_secs(5), take(&semaphore, 4))
        .await
        .unwrap();
    assert_eq!(semaphore.available_permits().await.unwrap(), 0);
}

// Redisson: testTryNAcquire
#[tokio::test]
async fn acquire_with_a_timeout_gets_several_permits_once_released() {
    let client = client().await;
    let name = unique("test");
    let semaphore = client.semaphore(name.clone());
    semaphore.init_permits(5).await.unwrap();
    semaphore
        .try_acquire_many(3)
        .await
        .unwrap()
        .unwrap()
        .forget();

    let releasing = client.semaphore(name);
    assert!(semaphore.try_acquire_many(4).await.unwrap().is_none());

    let started = Instant::now();
    tokio::spawn(async move {
        sleep(Duration::from_millis(500)).await;
        releasing.release(1).await.unwrap();
        sleep(Duration::from_millis(500)).await;
        releasing.release(1).await.unwrap();
    });

    let taken = semaphore
        .acquire_many(4)
        .timeout(Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    taken.forget();
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_millis(900), "{elapsed:?}");
    assert!(elapsed <= Duration::from_millis(1500), "{elapsed:?}");
    assert_eq!(semaphore.available_permits().await.unwrap(), 0);
}

// Redisson: testReleaseWithoutPermits
#[tokio::test]
async fn release_on_a_missing_semaphore_creates_the_permits() {
    let semaphore = client().await.semaphore(unique("test"));
    semaphore.release(1).await.unwrap();
    assert_eq!(semaphore.available_permits().await.unwrap(), 1);
}

// Redisson: testDrainPermits
#[tokio::test]
async fn drain_permits_returns_only_the_free_permits() {
    let semaphore = client().await.semaphore(unique("test"));
    assert_eq!(semaphore.drain_permits().await.unwrap(), 0);
    semaphore.init_permits(10).await.unwrap();
    take(&semaphore, 3).await;
    assert_eq!(semaphore.drain_permits().await.unwrap(), 7);
    assert_eq!(semaphore.available_permits().await.unwrap(), 0);
}

// Redisson: testReleaseAcquire
#[tokio::test]
async fn release_returns_the_acquired_permits() {
    let semaphore = client().await.semaphore(unique("test"));
    semaphore.init_permits(10).await.unwrap();
    take(&semaphore, 1).await;
    assert_eq!(semaphore.available_permits().await.unwrap(), 9);
    semaphore.release(1).await.unwrap();
    assert_eq!(semaphore.available_permits().await.unwrap(), 10);
    take(&semaphore, 5).await;
    assert_eq!(semaphore.available_permits().await.unwrap(), 5);
    semaphore.release(5).await.unwrap();
    assert_eq!(semaphore.available_permits().await.unwrap(), 10);
}

// Redisson: testReleaseIfExists
#[tokio::test]
async fn release_if_exists_does_not_create_a_missing_semaphore() {
    let client = client().await;
    let semaphore = client.semaphore(unique("test"));
    semaphore.init_permits(10).await.unwrap();
    take(&semaphore, 1).await;
    assert_eq!(semaphore.available_permits().await.unwrap(), 9);
    assert!(semaphore.release_if_exists(1).await.unwrap());
    assert_eq!(semaphore.available_permits().await.unwrap(), 10);

    let missing = client.semaphore(unique("test2"));
    assert!(!missing.release_if_exists(1).await.unwrap());
    assert!(!missing.exists().await.unwrap());
}

// Redisson: testConcurrencySingleInstance
#[tokio::test]
async fn one_permit_on_one_client_serializes_every_task() {
    let client = client().await;
    let name = unique("test");
    client
        .semaphore(name.clone())
        .init_permits(1)
        .await
        .unwrap();
    let counter = Arc::new(AtomicU32::new(0));
    let iterations = 15;
    let shared = counter.clone();
    run_concurrently(iterations, Some(client), move |client| {
        let semaphore = client.semaphore(name.clone());
        let counter = shared.clone();
        async move {
            take(&semaphore, 1).await;
            let value = counter.load(Ordering::SeqCst);
            counter.store(value + 1, Ordering::SeqCst);
            semaphore.release(1).await.unwrap();
        }
    })
    .await;
    assert_eq!(counter.load(Ordering::SeqCst), iterations as u32);
}

// Redisson: testConcurrencyLoopMaxMultiInstance
#[tokio::test]
async fn huge_and_single_permit_acquires_from_many_clients_all_finish() {
    let iterations = 10;
    let name = unique("test");
    client()
        .await
        .semaphore(name.clone())
        .init_permits(i64::from(i32::MAX))
        .await
        .unwrap();
    let counter = Arc::new(AtomicU32::new(0));
    let shared = counter.clone();
    run_concurrently(4, None, move |client| {
        let semaphore = client.semaphore(name.clone());
        let counter = shared.clone();
        async move {
            for _ in 0..iterations {
                let permits = if uuid::Uuid::new_v4().as_bytes()[0].is_multiple_of(2) {
                    i32::MAX as u64
                } else {
                    1
                };
                take(&semaphore, permits).await;
                sleep(Duration::from_millis(10)).await;
                counter.fetch_add(1, Ordering::SeqCst);
                semaphore.release(permits).await.unwrap();
            }
        }
    })
    .await;
    assert_eq!(counter.load(Ordering::SeqCst), 4 * iterations);
}

// Redisson: testConcurrencyLoopMultiInstance
#[tokio::test]
async fn one_permit_serializes_repeated_acquires_from_many_clients() {
    let iterations = 100;
    let name = unique("test");
    client()
        .await
        .semaphore(name.clone())
        .init_permits(1)
        .await
        .unwrap();
    let counter = Arc::new(AtomicU32::new(0));
    let shared = counter.clone();
    run_concurrently(16, None, move |client| {
        let semaphore = client.semaphore(name.clone());
        let counter = shared.clone();
        async move {
            for _ in 0..iterations {
                take(&semaphore, 1).await;
                sleep(Duration::from_millis(10)).await;
                let value = counter.load(Ordering::SeqCst);
                counter.store(value + 1, Ordering::SeqCst);
                semaphore.release(1).await.unwrap();
            }
        }
    })
    .await;
    assert_eq!(counter.load(Ordering::SeqCst), 16 * iterations);
}

// Redisson: testConcurrencyMultiInstance1Permits
#[tokio::test]
async fn one_permit_serializes_tasks_on_separate_clients() {
    let iterations = 30;
    let name = unique("test");
    client()
        .await
        .semaphore(name.clone())
        .init_permits(1)
        .await
        .unwrap();
    let counter = Arc::new(AtomicU32::new(0));
    let shared = counter.clone();
    run_concurrently(iterations, None, move |client| {
        let semaphore = client.semaphore(name.clone());
        let counter = shared.clone();
        async move {
            take(&semaphore, 1).await;
            let value = counter.load(Ordering::SeqCst);
            counter.store(value + 1, Ordering::SeqCst);
            semaphore.release(1).await.unwrap();
        }
    })
    .await;
    assert_eq!(counter.load(Ordering::SeqCst), iterations as u32);
}
