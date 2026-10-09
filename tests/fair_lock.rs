mod common;

use common::{client, unique};
use redissun::{Client, FairLock, Object};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout, Instant};

fn lock_of(client: &Client, name: &str) -> FairLock {
    client.fair_lock(name.to_string())
}

async fn queued(lock: &FairLock, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while lock.queue_len().await.unwrap() != expected {
        assert!(Instant::now() < deadline, "queue never reached {expected}");
        sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn lock_then_unlock() {
    let lock = lock_of(&client().await, &unique("fair"));
    assert!(!lock.is_locked().await.unwrap());
    let guard = lock.lock().await.unwrap();
    assert!(lock.is_locked().await.unwrap());
    assert!(lock.is_held_by_current().await.unwrap());
    guard.unlock().await.unwrap();
    assert!(!lock.is_locked().await.unwrap());
}

#[tokio::test]
async fn lock_is_reentrant_and_counts_holds() {
    let lock = lock_of(&client().await, &unique("fair"));
    let first = lock.lock().await.unwrap();
    let second = lock.lock().await.unwrap();
    assert_eq!(lock.hold_count().await.unwrap(), 2);
    second.unlock().await.unwrap();
    assert_eq!(lock.hold_count().await.unwrap(), 1);
    first.unlock().await.unwrap();
    assert!(!lock.is_locked().await.unwrap());
}

#[tokio::test]
async fn waiters_get_the_lock_in_arrival_order() {
    let client = client().await;
    let name = unique("fair");
    let lock = lock_of(&client, &name);
    let holder = lock.lock().await.unwrap();
    let order = Arc::new(Mutex::new(Vec::new()));

    let mut tasks = Vec::new();
    for index in 0..4usize {
        let waiter = lock_of(&client, &name);
        let order = order.clone();
        tasks.push(tokio::spawn(async move {
            let guard = waiter.lock().await.unwrap();
            order.lock().await.push(index);
            sleep(Duration::from_millis(30)).await;
            guard.unlock().await.unwrap();
        }));
        queued(&lock, index + 1).await;
    }

    holder.unlock().await.unwrap();
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(*order.lock().await, vec![0, 1, 2, 3]);
    assert_eq!(lock.queue_len().await.unwrap(), 0);
    assert!(!lock.is_locked().await.unwrap());
}

#[tokio::test]
async fn try_lock_does_not_jump_the_queue() {
    let client = client().await;
    let name = unique("fair");
    let lock = lock_of(&client, &name);
    let holder = lock.lock().await.unwrap();

    let waiter = lock_of(&client, &name);
    let waiting = tokio::spawn(async move { waiter.lock().await.unwrap() });
    queued(&lock, 1).await;

    let intruder = lock_of(&client, &name);
    let attempt = tokio::spawn(async move { intruder.try_lock().await.unwrap().is_none() });
    assert!(attempt.await.unwrap());
    assert_eq!(lock.queue_len().await.unwrap(), 1);

    holder.unlock().await.unwrap();
    waiting.await.unwrap().unlock().await.unwrap();
}

#[tokio::test]
async fn a_timed_out_waiter_leaves_the_queue() {
    let client = client().await;
    let name = unique("fair");
    let lock = lock_of(&client, &name);
    let holder = lock.lock().await.unwrap();

    let waiter = lock_of(&client, &name);
    let outcome = tokio::spawn(async move {
        waiter
            .lock()
            .timeout(Duration::from_millis(200))
            .await
            .unwrap()
            .is_none()
    })
    .await
    .unwrap();
    assert!(outcome);
    assert_eq!(lock.queue_len().await.unwrap(), 0);
    holder.unlock().await.unwrap();
}

#[tokio::test]
async fn a_cancelled_waiter_leaves_the_queue() {
    let client = client().await;
    let name = unique("fair");
    let lock = lock_of(&client, &name);
    let holder = lock.lock().await.unwrap();

    let waiter = lock_of(&client, &name);
    let cancelled = tokio::spawn(async move {
        timeout(Duration::from_millis(200), waiter.lock())
            .await
            .is_err()
    })
    .await
    .unwrap();
    assert!(cancelled);
    let deadline = Instant::now() + Duration::from_secs(5);
    while lock.queue_len().await.unwrap() != 0 {
        assert!(Instant::now() < deadline, "the cancelled waiter stayed");
        sleep(Duration::from_millis(10)).await;
    }
    holder.unlock().await.unwrap();
}

#[tokio::test]
async fn leaving_waiter_does_not_block_the_next_one() {
    let client = client().await;
    let name = unique("fair");
    let lock = lock_of(&client, &name);
    let holder = lock.lock().await.unwrap();

    let first = lock_of(&client, &name);
    let leaving = tokio::spawn(async move {
        first
            .lock()
            .timeout(Duration::from_millis(300))
            .await
            .unwrap()
            .is_none()
    });
    queued(&lock, 1).await;
    let second = lock_of(&client, &name);
    let staying = tokio::spawn(async move { second.lock().await.unwrap() });
    queued(&lock, 2).await;

    assert!(leaving.await.unwrap());
    holder.unlock().await.unwrap();
    let guard = timeout(Duration::from_secs(3), staying)
        .await
        .expect("the second waiter was never woken")
        .unwrap();
    guard.unlock().await.unwrap();
}

#[tokio::test]
async fn a_lease_that_runs_out_hands_the_lock_over() {
    let client = client().await;
    let lock = lock_of(&client, &unique("fair"));
    let holder = lock.clone();
    tokio::spawn(async move {
        let guard = holder
            .lock()
            .lease(Duration::from_millis(600))
            .await
            .unwrap();
        std::mem::forget(guard);
    })
    .await
    .unwrap();
    assert!(lock.is_locked().await.unwrap());

    let started = Instant::now();
    let waiter = lock.clone();
    let guard = tokio::spawn(async move { waiter.lock().await.unwrap() })
        .await
        .unwrap();
    assert!(started.elapsed() >= Duration::from_millis(300));
    assert!(started.elapsed() < Duration::from_secs(5));
    guard.unlock().await.unwrap();
}

#[tokio::test]
async fn force_unlock_wakes_the_first_waiter() {
    let client = client().await;
    let name = unique("fair");
    let lock = lock_of(&client, &name);
    let _holder = lock.lock().await.unwrap();

    let waiter = lock_of(&client, &name);
    let waiting = tokio::spawn(async move { waiter.lock().await.unwrap() });
    queued(&lock, 1).await;

    assert!(lock.force_unlock().await.unwrap());
    let guard = timeout(Duration::from_secs(3), waiting)
        .await
        .expect("the waiter was never woken")
        .unwrap();
    guard.unlock().await.unwrap();
}

#[tokio::test]
async fn the_lock_works_across_clients() {
    let name = unique("fair");
    let holder = lock_of(&client().await, &name).lock().await.unwrap();
    let other = lock_of(&client().await, &name);
    assert!(other.try_lock().await.unwrap().is_none());
    holder.unlock().await.unwrap();
    assert!(other.try_lock().await.unwrap().is_some());
}

#[tokio::test]
async fn object_methods_cover_the_queue_keys() {
    let client = client().await;
    let name = unique("fair");
    let lock = lock_of(&client, &name);
    let holder = lock.lock().await.unwrap();
    let waiter = lock_of(&client, &name);
    let waiting =
        tokio::spawn(async move { waiter.lock().timeout(Duration::from_millis(500)).await });
    queued(&lock, 1).await;

    assert!(lock.exists().await.unwrap());
    assert!(lock.del().await.unwrap());
    assert_eq!(lock.queue_len().await.unwrap(), 0);
    assert!(!lock.exists().await.unwrap());
    let _ = waiting.await;
    drop(holder);
}
