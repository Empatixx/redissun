mod common;

use common::{client, unique};
use redissun::{Client, Error, Lock, MultiLock};
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
async fn opposite_orders_do_not_deadlock() {
    let client = client().await;
    let (a, b) = (unique("m"), unique("m"));
    let forward = client.multi_lock(locks(&client, &[&a, &b])).unwrap();
    let backward = client.multi_lock(locks(&client, &[&b, &a])).unwrap();
    let one = tokio::spawn(async move {
        for _ in 0..3 {
            let guard = forward.lock().await.unwrap();
            sleep(Duration::from_millis(20)).await;
            guard.unlock().await.unwrap();
        }
    });
    let two = tokio::spawn(async move {
        for _ in 0..3 {
            let guard = backward.lock().await.unwrap();
            sleep(Duration::from_millis(20)).await;
            guard.unlock().await.unwrap();
        }
    });
    timeout(Duration::from_secs(60), async {
        one.await.unwrap();
        two.await.unwrap();
    })
    .await
    .expect("the multi locks deadlocked");
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
async fn owners_with_opposite_orders_and_a_timeout_both_get_through() {
    let first_client = client().await;
    let second_client = client().await;
    let (a, b) = (unique("m"), unique("m"));
    let forward = first_client
        .multi_lock(locks(&first_client, &[&a, &b]))
        .unwrap();
    let backward = second_client
        .multi_lock(locks(&second_client, &[&b, &a]))
        .unwrap();

    let run = |multi: MultiLock| async move {
        let mut taken = 0;
        for _ in 0..20 {
            if let Some(guard) = multi.lock().timeout(Duration::from_secs(2)).await.unwrap() {
                sleep(Duration::from_millis(5)).await;
                guard.unlock().await.unwrap();
                taken += 1;
            }
        }
        taken
    };
    let (forward_taken, backward_taken) = timeout(
        Duration::from_secs(30),
        futures::future::join(tokio::spawn(run(forward)), tokio::spawn(run(backward))),
    )
    .await
    .expect("opposite orders deadlocked");
    assert_eq!(forward_taken.unwrap(), 20);
    assert_eq!(backward_taken.unwrap(), 20);
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
