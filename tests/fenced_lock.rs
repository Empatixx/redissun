mod common;

use common::{client, unique};
use redissun::{Client, FencedLock, Object};
use std::time::Duration;

fn lock_of(client: &Client, name: &str) -> FencedLock {
    client.fenced_lock(name.to_string())
}

#[tokio::test]
async fn every_new_acquisition_gets_a_higher_token() {
    let lock = lock_of(&client().await, &unique("fenced"));
    assert_eq!(lock.current_token().await.unwrap(), 0);
    let mut last = 0;
    for _ in 0..3 {
        let guard = lock.lock().await.unwrap();
        let token = guard.fencing_token().unwrap();
        assert!(token > last);
        assert_eq!(lock.current_token().await.unwrap(), token);
        last = token;
        guard.unlock().await.unwrap();
    }
}

#[tokio::test]
async fn a_reentrant_hold_gets_a_new_token() {
    let lock = lock_of(&client().await, &unique("fenced"));
    let first = lock.lock().await.unwrap();
    let second = lock.lock().await.unwrap();
    assert_eq!(first.fencing_token(), Some(1));
    assert_eq!(second.fencing_token(), Some(2));
    assert_eq!(lock.current_token().await.unwrap(), 2);
    second.unlock().await.unwrap();
    first.unlock().await.unwrap();
}

#[tokio::test]
async fn the_next_owner_gets_a_higher_token_across_clients() {
    let name = unique("fenced");
    let first = lock_of(&client().await, &name).lock().await.unwrap();
    let token = first.fencing_token().unwrap();
    let other = lock_of(&client().await, &name);
    assert!(other.try_lock().await.unwrap().is_none());
    first.unlock().await.unwrap();
    let second = other.try_lock().await.unwrap().unwrap();
    assert!(second.fencing_token().unwrap() > token);
}

#[tokio::test]
async fn a_lease_that_runs_out_still_moves_the_token_forward() {
    let lock = lock_of(&client().await, &unique("fenced"));
    let holder = lock.clone();
    let token = tokio::spawn(async move {
        let guard = holder
            .lock()
            .lease(Duration::from_millis(300))
            .await
            .unwrap();
        let token = guard.fencing_token().unwrap();
        std::mem::forget(guard);
        token
    })
    .await
    .unwrap();
    let guard = lock.lock().await.unwrap();
    assert!(guard.fencing_token().unwrap() > token);
    guard.unlock().await.unwrap();
}

#[tokio::test]
async fn a_plain_lock_guard_has_no_token() {
    let guard = client().await.lock(unique("plain")).lock().await.unwrap();
    assert_eq!(guard.fencing_token(), None);
}

#[tokio::test]
async fn lock_info_methods_work() {
    let lock = lock_of(&client().await, &unique("fenced"));
    assert!(!lock.is_locked().await.unwrap());
    let guard = lock.lock().await.unwrap();
    assert!(lock.is_locked().await.unwrap());
    assert!(lock.is_held_by_current().await.unwrap());
    assert_eq!(lock.hold_count().await.unwrap(), 1);
    assert!(lock.force_unlock().await.unwrap());
    drop(guard);
}

#[tokio::test]
async fn test_token_increase() {
    let lock = lock_of(&client().await, &unique("lock"));
    let guard = lock.lock().await.unwrap();
    let token1 = guard.fencing_token().unwrap();
    assert_eq!(token1, 1);
    guard.unlock().await.unwrap();
    assert_eq!(token1, 1);

    let guard = lock.lock().await.unwrap();
    assert_eq!(guard.fencing_token(), Some(2));
    guard.unlock().await.unwrap();

    let guard = lock.lock().await.unwrap();
    assert_eq!(lock.current_token().await.unwrap(), 3);
    guard.unlock().await.unwrap();

    let guard = lock.lock().lease(Duration::from_secs(10)).await.unwrap();
    assert_eq!(lock.current_token().await.unwrap(), 4);
    guard.unlock().await.unwrap();

    let guard = lock.try_lock().await.unwrap().unwrap();
    assert_eq!(guard.fencing_token(), Some(5));
    guard.unlock().await.unwrap();
}

#[tokio::test]
async fn deleting_the_lock_deletes_the_token() {
    let lock = lock_of(&client().await, &unique("fenced"));
    lock.lock().await.unwrap().unlock().await.unwrap();
    assert_eq!(lock.current_token().await.unwrap(), 1);
    assert!(lock.del().await.unwrap());
    assert_eq!(lock.current_token().await.unwrap(), 0);
}
