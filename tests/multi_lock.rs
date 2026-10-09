mod common;

use common::{client, unique};
use redissun::{Client, Error, Lock, MultiLock, Object};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout, Instant};

fn locks(client: &Client, names: &[&str]) -> Vec<Lock> {
    names
        .iter()
        .map(|name| client.lock(name.to_string()))
        .collect()
}

#[tokio::test]
async fn it_needs_at_least_one_lock() {
    let client = client().await;
    assert!(matches!(
        client.multi_lock(Vec::<Lock>::new()),
        Err(Error::Config(_))
    ));
}

#[tokio::test]
async fn every_lock_is_held_until_unlock() {
    let client = client().await;
    let (a, b, c) = (unique("m"), unique("m"), unique("m"));
    let parts = locks(&client, &[&a, &b, &c]);
    let multi = client.multi_lock(parts.clone()).unwrap();
    let guard = multi.lock().await.unwrap();
    for part in &parts {
        assert!(part.is_locked().await.unwrap());
    }
    guard.unlock().await.unwrap();
    for part in &parts {
        assert!(!part.is_locked().await.unwrap());
    }
}

#[tokio::test]
async fn try_lock_gives_up_and_leaves_nothing_locked() {
    let client = client().await;
    let (a, b, c) = (unique("m"), unique("m"), unique("m"));
    let parts = locks(&client, &[&a, &b, &c]);
    let blocker = client.lock(c.clone());
    let blocking = tokio::spawn(async move { blocker.lock().await.unwrap() })
        .await
        .unwrap();

    let multi = client.multi_lock(parts.clone()).unwrap();
    let attempt = tokio::spawn(async move { multi.try_lock().await.unwrap().is_none() });
    assert!(attempt.await.unwrap());
    assert!(!parts[0].is_locked().await.unwrap());
    assert!(!parts[1].is_locked().await.unwrap());
    blocking.unlock().await.unwrap();
}

#[tokio::test]
async fn a_timeout_resolves_to_none_and_releases_what_it_took() {
    let client = client().await;
    let (a, b) = (unique("m"), unique("m"));
    let parts = locks(&client, &[&a, &b]);
    let blocker = client.lock(b.clone());
    let blocking = tokio::spawn(async move { blocker.lock().await.unwrap() })
        .await
        .unwrap();

    let multi = client.multi_lock(parts.clone()).unwrap();
    let started = Instant::now();
    let outcome = tokio::spawn(async move {
        multi
            .lock()
            .timeout(Duration::from_millis(400))
            .await
            .unwrap()
            .is_none()
    })
    .await
    .unwrap();
    assert!(outcome);
    assert!(started.elapsed() >= Duration::from_millis(350));
    assert!(!parts[0].is_locked().await.unwrap());
    blocking.unlock().await.unwrap();
}

#[tokio::test]
async fn it_waits_until_the_last_lock_is_free() {
    let client = client().await;
    let (a, b) = (unique("m"), unique("m"));
    let blocker = client.lock(b.clone());
    let blocking = tokio::spawn(async move { blocker.lock().await.unwrap() })
        .await
        .unwrap();

    let multi = client.multi_lock(locks(&client, &[&a, &b])).unwrap();
    let waiting = tokio::spawn(async move { multi.lock().await.unwrap() });
    sleep(Duration::from_millis(300)).await;
    assert!(!waiting.is_finished());
    blocking.unlock().await.unwrap();
    let guard = timeout(Duration::from_secs(10), waiting)
        .await
        .expect("the multi lock never got the free locks")
        .unwrap();
    guard.unlock().await.unwrap();
}

#[tokio::test]
async fn dropping_the_guard_releases_every_lock() {
    let client = client().await;
    let (a, b) = (unique("m"), unique("m"));
    let parts = locks(&client, &[&a, &b]);
    let multi = client.multi_lock(parts.clone()).unwrap();
    drop(multi.lock().await.unwrap());
    let deadline = Instant::now() + Duration::from_secs(5);
    while parts[0].is_locked().await.unwrap() || parts[1].is_locked().await.unwrap() {
        assert!(Instant::now() < deadline, "the locks stayed held");
        sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn different_lock_kinds_can_be_mixed() {
    let client = client().await;
    let (a, b, c) = (unique("m"), unique("m"), unique("m"));
    let multi: MultiLock = client
        .multi_lock([
            redissun::LockTarget::from(client.lock(a.clone())),
            redissun::LockTarget::from(client.fair_lock(b.clone())),
            redissun::LockTarget::from(client.fenced_lock(c.clone())),
        ])
        .unwrap();
    let guard = multi.lock().await.unwrap();
    assert!(client.fair_lock(b).is_locked().await.unwrap());
    assert_eq!(client.fenced_lock(c).current_token().await.unwrap(), 1);
    guard.unlock().await.unwrap();
}

#[tokio::test]
async fn guards_keep_the_order_the_locks_were_given_in() {
    let client = client().await;
    let names = [unique("z"), unique("a"), unique("m")];
    let multi = client
        .multi_lock(names.iter().map(|name| client.lock(name.clone())))
        .unwrap();
    let guard = multi.lock().await.unwrap();
    assert_eq!(guard.guards().len(), 3);
    for (part, name) in guard.guards().iter().zip(&names) {
        assert!(format!("{part:?}").contains(name.as_str()));
    }
    guard.unlock().await.unwrap();
}

#[tokio::test]
async fn test_lock_unlock() {
    let client = client().await;
    let (a, b, c) = (unique("lock1"), unique("lock2"), unique("lock3"));
    let multi = client.multi_lock(locks(&client, &[&a, &b, &c])).unwrap();
    let guard = multi.lock().lease(Duration::from_secs(10)).await.unwrap();
    assert!(multi.is_held_by_current().await.unwrap());
    sleep(Duration::from_secs(1)).await;
    guard.unlock().await.unwrap();
    assert!(!multi.is_held_by_current().await.unwrap());
}

#[tokio::test]
async fn test_wait_and_lease_timeouts() {
    let client = client().await;
    let (a, b, c) = (unique("lock1"), unique("lock2"), unique("lock3"));
    let counter = Arc::new(AtomicU32::new(0));
    let mut tasks = Vec::new();
    for _ in 0..10 {
        let multi = client.multi_lock(locks(&client, &[&a, &b, &c])).unwrap();
        let counter = counter.clone();
        tasks.push(tokio::spawn(async move {
            let taken = multi
                .lock()
                .lease(Duration::from_secs(10))
                .timeout(Duration::ZERO)
                .await
                .unwrap();
            if let Some(guard) = taken {
                counter.fetch_add(1, Ordering::SeqCst);
                std::mem::forget(guard);
            }
        }));
    }
    timeout(Duration::from_secs(5), futures::future::join_all(tasks))
        .await
        .unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_multi_threads() {
    let client = client().await;
    let (a, b, c) = (unique("lock1"), unique("lock2"), unique("lock3"));
    let holder = client.multi_lock(locks(&client, &[&a, &b, &c])).unwrap();
    let holding = tokio::spawn(async move {
        let guard = holder.lock().await.unwrap();
        sleep(Duration::from_millis(1500)).await;
        guard.unlock().await.unwrap();
    });
    sleep(Duration::from_millis(500)).await;
    let multi = client.multi_lock(locks(&client, &[&a, &b, &c])).unwrap();
    let guard = timeout(Duration::from_secs(20), multi.lock())
        .await
        .unwrap()
        .unwrap();
    guard.unlock().await.unwrap();
    holding.await.unwrap();
}

#[tokio::test]
async fn test() {
    let first = client().await;
    let second = client().await;
    let third = client().await;
    let (a, b, c) = (unique("lock1"), unique("lock2"), unique("lock3"));
    let multi = first
        .multi_lock([
            first.lock(a.clone()),
            second.lock(b.clone()),
            third.lock(c.clone()),
        ])
        .unwrap();
    let guard = multi.lock().await.unwrap();
    let other = first
        .multi_lock([first.lock(a), second.lock(b), third.lock(c)])
        .unwrap();
    let executed = tokio::spawn(async move {
        assert!(other.try_lock().await.unwrap().is_none());
        assert!(other.try_lock().await.unwrap().is_none());
        true
    });
    assert!(timeout(Duration::from_secs(5), executed)
        .await
        .unwrap()
        .unwrap());
    guard.unlock().await.unwrap();
}

#[tokio::test]
async fn a_failed_round_starts_again_from_the_first_lock_until_the_wait_runs_out() {
    let client = client().await;
    let (a, b) = (unique("m"), unique("m"));
    let blocker = client.lock(b.clone());
    let blocking = tokio::spawn(async move {
        blocker
            .lock()
            .lease(Duration::from_millis(700))
            .await
            .unwrap()
    })
    .await
    .unwrap();
    let multi = client.multi_lock(locks(&client, &[&a, &b])).unwrap();
    let guard = multi
        .lock()
        .timeout(Duration::from_secs(5))
        .await
        .unwrap()
        .expect("the round after the lease ran out should take both locks");
    assert!(client.lock(a).is_locked().await.unwrap());
    guard.unlock().await.unwrap();
    drop(blocking);
}

#[tokio::test]
async fn a_lease_with_a_timeout_ends_up_as_the_requested_lease() {
    let client = client().await;
    let (a, b) = (unique("m"), unique("m"));
    let parts = locks(&client, &[&a, &b]);
    let multi = client.multi_lock(parts.clone()).unwrap();
    let guard = multi
        .lock()
        .lease(Duration::from_secs(20))
        .timeout(Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    for part in &parts {
        let ttl = part.ttl().await.unwrap().unwrap();
        assert!(
            ttl <= Duration::from_secs(20) && ttl > Duration::from_secs(19),
            "{ttl:?}"
        );
    }
    guard.unlock().await.unwrap();
}
