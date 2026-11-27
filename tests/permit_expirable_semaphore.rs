// Ported from Redisson's RedissonPermitExpirableSemaphoreTest.
mod common;

use common::{client, connect_with, unique};
use redissun::{Error, Object, PermitExpirableSemaphore};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, Instant};

async fn semaphore() -> PermitExpirableSemaphore {
    client()
        .await
        .permit_expirable_semaphore(unique("semaphore"))
}

const MS: fn(u64) -> Duration = Duration::from_millis;

async fn try_acquire(s: &PermitExpirableSemaphore, wait: u64, lease: u64) -> Option<String> {
    s.acquire()
        .lease(MS(lease))
        .timeout(MS(wait))
        .await
        .unwrap()
}

#[tokio::test]
async fn update_lease_works_only_for_a_held_permit() {
    let s = semaphore().await;
    s.init_permits(1).await.unwrap();
    assert!(!s.update_lease("1234", Some(MS(1000))).await.unwrap());
    let id = s.acquire().await.unwrap();
    assert!(s.update_lease(&id, Some(MS(1000))).await.unwrap());
    sleep(MS(1200)).await;
    assert!(!s.update_lease(&id, Some(MS(1000))).await.unwrap());
    let id2 = try_acquire(&s, 1000, 1000).await.unwrap();
    assert!(s.update_lease(&id2, Some(MS(3000))).await.unwrap());
    sleep(MS(2800)).await;
    assert_eq!(s.available_permits().await.unwrap(), 0);
    sleep(MS(500)).await;
    assert_eq!(s.available_permits().await.unwrap(), 1);
    assert!(!s.update_lease(&id2, Some(MS(2000))).await.unwrap());
}

#[tokio::test]
async fn a_missing_semaphore_has_no_permits() {
    assert_eq!(semaphore().await.available_permits().await.unwrap(), 0);
}

#[tokio::test]
async fn permits_with_a_lease_come_back_by_themselves() {
    // Redisson's testAvailablePermits, testAcquiredPermits and testGetPermits.
    let s = semaphore().await;
    assert!(s.init_permits(2).await.unwrap());
    assert_eq!(s.available_permits().await.unwrap(), 2);
    assert_eq!(s.acquired_permits().await.unwrap(), 0);
    assert_eq!(s.permits().await.unwrap(), 2);
    assert!(try_acquire(&s, 200, 1000).await.is_some());
    assert_eq!(s.acquired_permits().await.unwrap(), 1);
    assert!(try_acquire(&s, 200, 1000).await.is_some());
    assert!(try_acquire(&s, 200, 1000).await.is_none());
    assert_eq!(s.available_permits().await.unwrap(), 0);
    assert_eq!(s.acquired_permits().await.unwrap(), 2);
    assert_eq!(s.permits().await.unwrap(), 2);
    sleep(MS(1100)).await;
    assert!(try_acquire(&s, 200, 1000).await.is_some());
    sleep(MS(1100)).await;
    assert_eq!(s.available_permits().await.unwrap(), 2);
    assert_eq!(s.acquired_permits().await.unwrap(), 0);
    assert_eq!(s.permits().await.unwrap(), 2);
}

#[tokio::test]
async fn expire_reaches_the_held_permits_too() {
    let client = client().await;
    let name = unique("some-key");
    let s = client.permit_expirable_semaphore(&*name);
    s.init_permits(1).await.unwrap();
    s.expire(Duration::from_secs(3)).await.unwrap();
    try_acquire(&s, 1000, 1000).await.unwrap();
    assert!(client
        .keys()
        .ttl(&format!("{{{name}}}:timeout"))
        .await
        .unwrap()
        .is_some());
    sleep(MS(3100)).await;
    let names = [name.clone(), format!("{{{name}}}:timeout")];
    assert_eq!(client.keys().count_exists(&names).await.unwrap(), 0);
}

#[tokio::test]
async fn a_waiter_gets_the_permit_when_the_lease_ends() {
    // Redisson's testExpire and testExpireTryAcquire.
    let s = semaphore().await;
    s.init_permits(1).await.unwrap();
    let id = s.acquire().lease(MS(2000)).await.unwrap();
    let start = Instant::now();
    let other = s.clone();
    let waiter = tokio::spawn(async move {
        let id = other.acquire().await.unwrap();
        let spent = start.elapsed();
        other.release(&id).await.unwrap();
        spent
    });
    let spent = waiter.await.unwrap();
    assert!(spent >= MS(1900) && spent <= MS(2300), "waited {spent:?}");
    assert!(!s.try_release(&id).await.unwrap());
}

#[tokio::test]
async fn init_permits_sets_only_a_missing_semaphore() {
    let s = semaphore().await;
    assert!(s.init_permits(10).await.unwrap());
    assert_eq!(s.available_permits().await.unwrap(), 10);
    assert!(!s.init_permits(15).await.unwrap());
    assert_eq!(s.available_permits().await.unwrap(), 10);
}

async fn counts(s: &PermitExpirableSemaphore) -> (i64, i64, i64) {
    (
        s.permits().await.unwrap(),
        s.available_permits().await.unwrap(),
        s.acquired_permits().await.unwrap(),
    )
}

#[tokio::test]
async fn set_permits_sets_free_and_held_together() {
    let s = semaphore().await;
    s.set_permits(10).await.unwrap();
    assert_eq!(counts(&s).await, (10, 10, 0));
    assert!(!s.init_permits(15).await.unwrap());
    assert_eq!(counts(&s).await, (10, 10, 0));
    s.set_permits(15).await.unwrap();
    assert_eq!(counts(&s).await, (15, 15, 0));
    s.set_permits(15).await.unwrap();
    assert_eq!(counts(&s).await, (15, 15, 0));
    s.set_permits(5).await.unwrap();
    assert_eq!(counts(&s).await, (5, 5, 0));
    for _ in 0..3 {
        assert!(try_acquire(&s, 200, 1000).await.is_some());
    }
    assert_eq!(counts(&s).await, (5, 2, 3));
    s.set_permits(3).await.unwrap();
    assert_eq!(counts(&s).await, (3, 0, 3));
    s.set_permits(2).await.unwrap();
    assert_eq!(counts(&s).await, (2, -1, 3));
}

#[tokio::test]
async fn add_permits_may_go_below_zero() {
    let s = semaphore().await;
    s.init_permits(10).await.unwrap();
    s.add_permits(5).await.unwrap();
    assert_eq!(s.available_permits().await.unwrap(), 15);
    s.add_permits(-10).await.unwrap();
    assert_eq!(s.available_permits().await.unwrap(), 5);
    s.add_permits(-10).await.unwrap();
    assert_eq!(s.available_permits().await.unwrap(), -5);
}

#[tokio::test]
async fn acquire_waits_for_a_release() {
    let s = semaphore().await;
    s.init_permits(1).await.unwrap();
    let id = s.acquire().await.unwrap();
    assert_eq!(id.len(), 32);
    let other = s.clone();
    tokio::spawn(async move {
        sleep(MS(1000)).await;
        other.release(&id).await.unwrap();
    });
    assert_eq!(s.available_permits().await.unwrap(), 0);
    s.acquire().await.unwrap();
    assert_eq!(s.try_acquire().await.unwrap(), None);
    assert_eq!(s.available_permits().await.unwrap(), 0);
}

#[tokio::test]
async fn acquire_many_and_release_many() {
    let s = semaphore().await;
    s.init_permits(10).await.unwrap();
    let ids = s.acquire_many(6).await.unwrap();
    assert_eq!(ids.len(), 6);
    assert_eq!(s.available_permits().await.unwrap(), 4);
    s.release_many(&ids[..4]).await.unwrap();
    assert_eq!(s.available_permits().await.unwrap(), 8);
    s.release_many(&ids[4..]).await.unwrap();
    assert_eq!(s.available_permits().await.unwrap(), 10);
    assert!(matches!(
        s.release_many(&ids).await,
        Err(Error::PermitNotHeld(_))
    ));
}

#[tokio::test]
async fn try_acquire_takes_a_permit_only_when_one_is_free() {
    let s = semaphore().await;
    s.init_permits(1).await.unwrap();
    let id = s.try_acquire().await.unwrap().unwrap();
    assert_eq!(id.len(), 32);
    assert_eq!(s.try_acquire().await.unwrap(), None);
    let other = s.clone();
    tokio::spawn(async move {
        sleep(MS(500)).await;
        other.release(&id).await.unwrap();
    });
    let start = Instant::now();
    let id2 = s.acquire().timeout(MS(1000)).await.unwrap().unwrap();
    assert_eq!(id2.len(), 32);
    let spent = start.elapsed();
    assert!(spent >= MS(450) && spent <= MS(700), "waited {spent:?}");
    assert_eq!(s.available_permits().await.unwrap(), 0);
}

#[tokio::test]
async fn try_acquire_many_takes_all_or_none() {
    let s = semaphore().await;
    s.init_permits(10).await.unwrap();
    assert_eq!(s.try_acquire_many(4).await.unwrap().len(), 4);
    assert_eq!(s.try_acquire_many(5).await.unwrap().len(), 5);
    assert!(s.try_acquire_many(2).await.unwrap().is_empty());
    assert_eq!(s.available_permits().await.unwrap(), 1);
    assert!(s.try_acquire_many(0).await.unwrap().is_empty());
}

#[tokio::test]
async fn releasing_a_permit_that_is_not_held_fails() {
    let s = semaphore().await;
    assert!(matches!(
        s.release("1234").await,
        Err(Error::PermitNotHeld(_))
    ));
    assert!(matches!(s.release("xyz").await, Err(Error::Config(_))));
}

#[tokio::test]
async fn releasing_an_expired_permit_gives_false() {
    let s = semaphore().await;
    s.init_permits(1).await.unwrap();
    let id = try_acquire(&s, 100, 100).await.unwrap();
    sleep(MS(200)).await;
    assert!(!s.try_release(&id).await.unwrap());
}

#[tokio::test]
async fn one_permit_is_held_by_one_task_at_a_time() {
    // Redisson's testConcurrency_SingleInstance and testConcurrencyLoop_MultiInstance.
    let name = unique("semaphore");
    let first = client().await.permit_expirable_semaphore(&*name);
    first.init_permits(1).await.unwrap();
    let inside = Arc::new(AtomicU32::new(0));
    let counter = Arc::new(AtomicU32::new(0));
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let s = connect_with(|builder| builder)
            .await
            .permit_expirable_semaphore(&*name);
        let (inside, counter) = (inside.clone(), counter.clone());
        tasks.push(tokio::spawn(async move {
            for _ in 0..20 {
                let id = s.acquire().await.unwrap();
                assert_eq!(inside.fetch_add(1, Ordering::SeqCst), 0);
                sleep(MS(2)).await;
                counter.fetch_add(1, Ordering::SeqCst);
                inside.fetch_sub(1, Ordering::SeqCst);
                s.release(&id).await.unwrap();
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(counter.load(Ordering::SeqCst), 160);
}

#[tokio::test]
async fn try_release_many_counts_the_released_permits() {
    // Redisson's testReleaseAsyncMany.
    let s = semaphore().await;
    s.init_permits(10).await.unwrap();
    let ids = s.acquire_many(6).await.unwrap();
    assert_eq!(s.try_release_many(&ids[..4]).await.unwrap(), 4);
    assert_eq!(s.available_permits().await.unwrap(), 8);
    assert_eq!(s.try_release_many(&ids[4..]).await.unwrap(), 2);
    assert_eq!(s.available_permits().await.unwrap(), 10);
    let none: [&str; 0] = [];
    assert!(matches!(
        s.try_release_many(&none).await,
        Err(Error::Config(_))
    ));
}

#[tokio::test]
async fn expired_permits_come_back_with_the_next_acquire() {
    // Redisson's testReleaseManyExpiredDoesNotThrow.
    let s = semaphore().await;
    s.init_permits(10).await.unwrap();
    let ids = s.acquire_many(6).lease(MS(100)).await.unwrap();
    assert_eq!(ids.len(), 6);
    assert_eq!(s.available_permits().await.unwrap(), 4);
    sleep(MS(250)).await;
    s.acquire().lease(MS(100)).await.unwrap();
    assert_eq!(s.available_permits().await.unwrap(), 9);
    let deadline = Instant::now() + MS(1000);
    while s.available_permits().await.unwrap() != 10 {
        assert!(Instant::now() < deadline);
        sleep(MS(10)).await;
    }
}

#[tokio::test]
async fn try_release_many_releases_nothing_when_one_expired() {
    // Redisson's testTryReleaseManyExpired.
    let s = semaphore().await;
    s.init_permits(10).await.unwrap();
    let timed = s
        .acquire_many(3)
        .lease(MS(100))
        .timeout(MS(100))
        .await
        .unwrap()
        .unwrap();
    let infinite = s.try_acquire_many(3).await.unwrap();
    assert_eq!(s.available_permits().await.unwrap(), 4);
    sleep(MS(200)).await;
    assert_eq!(s.try_release_many(&infinite).await.unwrap(), 3);
    assert_eq!(s.available_permits().await.unwrap(), 10);
    assert_eq!(s.try_release_many(&timed).await.unwrap(), 0);
    assert_eq!(s.available_permits().await.unwrap(), 10);
}

#[tokio::test]
async fn release_many_fails_unless_every_permit_is_held() {
    // Redisson's testReleaseManyNotExistOrExpired.
    let s = semaphore().await;
    s.init_permits(10).await.unwrap();
    let timed = s.acquire_many(2).lease(MS(100)).await.unwrap();
    let ids = s.try_acquire_many(8).await.unwrap();
    assert_eq!(s.try_release_many(&ids[..2]).await.unwrap(), 2);
    assert_eq!(s.available_permits().await.unwrap(), 2);
    sleep(MS(100)).await;
    assert!(s.release_many(&timed).await.is_err());
    assert_eq!(s.available_permits().await.unwrap(), 4);
    let mut second: Vec<String> = ids[2..4].to_vec();
    second.extend_from_slice(&ids[..2]);
    assert!(s.release_many(&second).await.is_err());
    assert_eq!(s.available_permits().await.unwrap(), 4);
    let mut third: Vec<String> = ids[4..6].to_vec();
    third.push("4567".into());
    assert!(s.release_many(&third).await.is_err());
    assert_eq!(s.available_permits().await.unwrap(), 4);
}

#[tokio::test]
async fn lease_time_is_what_the_permit_has_left() {
    let s = semaphore().await;
    s.init_permits(1).await.unwrap();
    assert!(matches!(
        s.lease_time("1234").await,
        Err(Error::PermitNotHeld(_))
    ));
    let id = s.acquire().await.unwrap();
    assert_eq!(s.lease_time(&id).await.unwrap(), None);
    s.release(&id).await.unwrap();
    let id = s.acquire().lease(MS(1100)).await.unwrap();
    sleep(MS(100)).await;
    assert!(s.lease_time(&id).await.unwrap().unwrap() <= MS(1000));
    let deadline = Instant::now() + MS(1500);
    while s.available_permits().await.unwrap() != 1 {
        assert!(Instant::now() < deadline);
        sleep(MS(20)).await;
    }
}

#[tokio::test]
async fn two_waiters_for_two_permits_both_get_them() {
    // Redisson's testConcurrencyMultiPermits and testConcurrencyMultiPermits2.
    for first_wants in [2, 1] {
        let s = semaphore().await;
        s.init_permits(2).await.unwrap();
        let succeeded = Arc::new(AtomicU32::new(0));
        let run = |wants: usize| {
            let (s, succeeded) = (s.clone(), succeeded.clone());
            tokio::spawn(async move {
                let ids = s
                    .acquire_many(wants)
                    .timeout(MS(2000))
                    .await
                    .unwrap()
                    .unwrap_or_default();
                sleep(MS(500)).await;
                if !ids.is_empty() {
                    succeeded.fetch_add(1, Ordering::SeqCst);
                    s.try_release_many(&ids).await.unwrap();
                }
            })
        };
        let first = run(first_wants);
        sleep(MS(100)).await;
        let second = run(2);
        first.await.unwrap();
        second.await.unwrap();
        assert_eq!(succeeded.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn a_cancelled_acquire_leaves_no_permit_held() {
    let s = semaphore().await;
    s.init_permits(1).await.unwrap();
    for _ in 0..20 {
        // An acquire that finished in time hands its permit over; only a cancelled one must release it by itself.
        if let Ok(acquired) = tokio::time::timeout(Duration::from_micros(200), s.acquire()).await {
            s.release(&acquired.unwrap()).await.unwrap();
        }
    }
    let deadline = Instant::now() + MS(2000);
    while s.available_permits().await.unwrap() != 1 {
        assert!(
            Instant::now() < deadline,
            "a cancelled acquire kept its permit"
        );
        sleep(MS(20)).await;
    }
}

#[tokio::test]
async fn rename_and_delete_take_the_held_permits_along() {
    let client = client().await;
    let (name, new_name) = (unique("semaphore"), unique("semaphore"));
    let s = client.permit_expirable_semaphore(&*name);
    s.init_permits(2).await.unwrap();
    let id = s.acquire().await.unwrap();
    s.rename(&new_name).await.unwrap();
    let renamed = client.permit_expirable_semaphore(&*new_name);
    assert_eq!(renamed.acquired_permits().await.unwrap(), 1);
    renamed.release(&id).await.unwrap();
    renamed.acquire().await.unwrap();
    assert!(renamed.del().await.unwrap());
    assert_eq!(renamed.acquired_permits().await.unwrap(), 0);
    assert_eq!(renamed.permits().await.unwrap(), 0);
}
