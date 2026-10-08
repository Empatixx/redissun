mod common;

use common::{client, connect_with, unique};
use redissun::{Client, Error, LockOptions};
use std::time::Duration;
use tokio::time::{sleep, timeout, Instant};

async fn short_lease_client(lease: Duration) -> Client {
    connect_with(|builder| builder.lock_lease(lease)).await
}

#[tokio::test]
async fn lock_then_unlock() {
    let lock = client().await.lock(unique("lock"));
    assert!(!lock.is_locked().await.unwrap());
    let guard = lock.lock().await.unwrap();
    assert!(lock.is_locked().await.unwrap());
    assert!(lock.is_held_by_current().await.unwrap());
    guard.unlock().await.unwrap();
    assert!(!lock.is_locked().await.unwrap());
}

#[tokio::test]
async fn lock_is_reentrant_and_counts_holds() {
    let lock = client().await.lock(unique("lock"));
    let first = lock.lock().await.unwrap();
    let second = lock.lock().await.unwrap();
    assert_eq!(lock.hold_count().await.unwrap(), 2);

    second.unlock().await.unwrap();
    assert_eq!(lock.hold_count().await.unwrap(), 1);
    assert!(lock.is_locked().await.unwrap());

    first.unlock().await.unwrap();
    assert_eq!(lock.hold_count().await.unwrap(), 0);
    assert!(!lock.is_locked().await.unwrap());
}

#[tokio::test]
async fn try_lock_fails_while_another_client_holds_the_lock() {
    let name = unique("lock");
    let holder = client().await.lock(name.clone()).lock().await.unwrap();
    let other = client().await.lock(name);
    assert!(other.try_lock().await.unwrap().is_none());
    holder.unlock().await.unwrap();
    assert!(other.try_lock().await.unwrap().is_some());
}

#[tokio::test]
async fn tasks_of_the_same_client_are_distinct_owners() {
    let lock = client().await.lock(unique("lock"));
    let guard = lock.lock().await.unwrap();

    let in_other_task = lock.clone();
    let outcome = tokio::spawn(async move {
        let held = in_other_task.is_held_by_current().await.unwrap();
        let acquired = in_other_task.try_lock().await.unwrap().is_some();
        (held, acquired)
    })
    .await
    .unwrap();

    assert_eq!(outcome, (false, false));
    guard.unlock().await.unwrap();
}

#[tokio::test]
async fn lock_for_gives_up_after_the_wait() {
    let name = unique("lock");
    let _holder = client().await.lock(name.clone()).lock().await.unwrap();
    let other = client().await.lock(name);

    let started = Instant::now();
    let result = other.lock_for(Duration::from_millis(300)).await.unwrap();
    assert!(result.is_none());
    assert!(started.elapsed() >= Duration::from_millis(300));
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[tokio::test]
async fn waiter_is_woken_by_unlock_long_before_the_lease_expires() {
    let name = unique("lock");
    let holder = client().await.lock(name.clone()).lock().await.unwrap();
    let waiter_lock = client().await.lock(name);

    let waiter = tokio::spawn(async move { waiter_lock.lock().await.unwrap() });
    sleep(Duration::from_millis(200)).await;
    assert!(!waiter.is_finished());

    holder.unlock().await.unwrap();
    let guard = timeout(Duration::from_secs(5), waiter)
        .await
        .expect("waiter should be notified through pub/sub, not wait for the 30 s lease")
        .unwrap();
    guard.unlock().await.unwrap();
}

#[tokio::test]
async fn explicit_lease_expires_without_a_watchdog() {
    let lock = client().await.lock(unique("lock"));
    let options = LockOptions::new().lease(Duration::from_millis(300));
    let _guard = lock.lock_with(options).await.unwrap().unwrap();
    assert!(lock.is_locked().await.unwrap());
    sleep(Duration::from_millis(700)).await;
    assert!(!lock.is_locked().await.unwrap());
}

#[tokio::test]
async fn watchdog_keeps_the_lock_alive_past_the_lease() {
    let client = short_lease_client(Duration::from_millis(600)).await;
    let lock = client.lock(unique("lock"));
    let guard = lock.lock().await.unwrap();
    sleep(Duration::from_millis(1800)).await;
    assert!(lock.is_locked().await.unwrap());
    guard.unlock().await.unwrap();
    assert!(!lock.is_locked().await.unwrap());
}

#[tokio::test]
async fn dropping_the_guard_releases_the_lock() {
    let lock = client().await.lock(unique("lock"));
    let guard = lock.lock().await.unwrap();
    drop(guard);
    let deadline = Instant::now() + Duration::from_secs(3);
    while lock.is_locked().await.unwrap() {
        assert!(
            Instant::now() < deadline,
            "lock was not released after drop"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn unlocking_an_expired_lock_reports_lock_not_held() {
    let lock = client().await.lock(unique("lock"));
    let options = LockOptions::new().lease(Duration::from_millis(200));
    let guard = lock.lock_with(options).await.unwrap().unwrap();
    sleep(Duration::from_millis(500)).await;
    assert!(matches!(guard.unlock().await, Err(Error::LockNotHeld)));
}

#[tokio::test]
async fn a_lock_name_can_be_reused_after_release() {
    let lock = client().await.lock(unique("lock"));
    for _ in 0..3 {
        let guard = lock.lock().await.unwrap();
        guard.unlock().await.unwrap();
    }
    assert!(!lock.is_locked().await.unwrap());
}

#[tokio::test]
async fn force_unlock_releases_a_lock_held_by_someone_else() {
    let name = unique("lock");
    let _holder = client().await.lock(name.clone()).lock().await.unwrap();
    let admin = client().await.lock(name);
    assert!(admin.force_unlock().await.unwrap());
    assert!(!admin.is_locked().await.unwrap());
    assert!(!admin.force_unlock().await.unwrap());
}

#[tokio::test]
async fn contended_lock_gives_mutual_exclusion() {
    let name = unique("lock");
    let counter = std::sync::Arc::new(tokio::sync::Mutex::new(0u32));
    let inside = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));

    let mut tasks = Vec::new();
    for _ in 0..4 {
        let lock = client().await.lock(name.clone());
        let counter = counter.clone();
        let inside = inside.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..5 {
                let guard = lock.lock().await.unwrap();
                let now = inside.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                assert_eq!(now, 0, "two holders inside the critical section");
                *counter.lock().await += 1;
                sleep(Duration::from_millis(5)).await;
                inside.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                guard.unlock().await.unwrap();
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(*counter.lock().await, 20);
}

#[tokio::test]
async fn zero_lease_is_rejected_and_a_sub_millisecond_lease_is_accepted() {
    let lock = client().await.lock(unique("lock"));
    let zero = lock
        .lock_with(LockOptions::new().lease(Duration::ZERO))
        .await;
    assert!(matches!(zero, Err(Error::Config(_))));
    let tiny = LockOptions::new().lease(Duration::from_micros(500));
    assert!(lock.lock_with(tiny).await.unwrap().is_some());
}

#[tokio::test]
async fn cancelling_a_lock_at_any_moment_does_not_leave_it_held() {
    let lock = client().await.lock(unique("lock"));
    for micros in (0..900).step_by(10) {
        let _ = timeout(Duration::from_micros(micros), lock.try_lock()).await;
    }
    let deadline = Instant::now() + Duration::from_secs(3);
    while lock.is_locked().await.unwrap() {
        assert!(
            Instant::now() < deadline,
            "an abandoned acquire left the lock held"
        );
        sleep(Duration::from_millis(20)).await;
    }
}
