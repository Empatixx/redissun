mod common;

use common::{client, connect_with, redis_url, unique};
use redissun::{Client, RwLock};
use std::time::Duration;
use tokio::time::{sleep, Instant};

async fn in_task<F, Fut, T>(work: F) -> T
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    tokio::spawn(async move { work().await }).await.unwrap()
}

fn lock_of(client: &Client, name: &str) -> RwLock {
    client.rw_lock(name.to_string())
}

#[tokio::test]
async fn many_readers_hold_the_lock_at_once() {
    let client = client().await;
    let name = unique("rw");
    let mut guards = Vec::new();
    for _ in 0..3 {
        let lock = lock_of(&client, &name);
        guards.push(
            tokio::spawn(async move { lock.read().await.unwrap() })
                .await
                .unwrap(),
        );
    }
    let writer = lock_of(&client, &name);
    assert!(in_task(move || async move { writer.try_write().await.unwrap().is_none() }).await);
    for guard in guards {
        guard.unlock().await.unwrap();
    }
    let writer = lock_of(&client, &name);
    assert!(in_task(move || async move { writer.try_write().await.unwrap().is_some() }).await);
}

#[tokio::test]
async fn a_writer_excludes_readers_and_writers() {
    let client = client().await;
    let name = unique("rw");
    let lock = lock_of(&client, &name);
    let guard = lock.write().await.unwrap();
    assert!(lock.is_write_locked().await.unwrap());
    let other = lock_of(&client, &name);
    assert!(
        in_task(move || async move {
            other.try_read().await.unwrap().is_none() && other.try_write().await.unwrap().is_none()
        })
        .await
    );
    guard.unlock().await.unwrap();
    assert!(!lock.is_write_locked().await.unwrap());
    let other = lock_of(&client, &name);
    assert!(in_task(move || async move { other.try_read().await.unwrap().is_some() }).await);
}

#[tokio::test]
async fn read_and_write_holds_are_reentrant() {
    let client = client().await;
    let name = unique("rw");
    let lock = lock_of(&client, &name);
    let first = lock.read().await.unwrap();
    let second = lock.read().await.unwrap();
    first.unlock().await.unwrap();
    let other = lock_of(&client, &name);
    assert!(in_task(move || async move { other.try_write().await.unwrap().is_none() }).await);
    second.unlock().await.unwrap();

    let first = lock.write().await.unwrap();
    let second = lock.write().await.unwrap();
    first.unlock().await.unwrap();
    assert!(lock.is_write_locked().await.unwrap());
    second.unlock().await.unwrap();
    assert!(!lock.is_write_locked().await.unwrap());
}

#[tokio::test]
async fn a_writer_can_also_read_but_a_reader_cannot_upgrade() {
    let client = client().await;
    let name = unique("rw");
    let lock = lock_of(&client, &name);
    let write = lock.write().await.unwrap();
    let read = lock.try_read().await.unwrap();
    assert!(read.is_some());
    write.unlock().await.unwrap();
    read.unwrap().unlock().await.unwrap();

    let read = lock.read().await.unwrap();
    assert!(lock.try_write().await.unwrap().is_none());
    read.unlock().await.unwrap();
    assert!(lock.try_write().await.unwrap().is_some());
}

#[tokio::test]
async fn waiting_times_out() {
    let client = client().await;
    let name = unique("rw");
    let lock = lock_of(&client, &name);
    let read = lock.read().await.unwrap();
    let other = lock_of(&client, &name);
    let started = Instant::now();
    let got =
        in_task(move || async move { other.write_for(Duration::from_millis(400)).await.unwrap() })
            .await;
    assert!(got.is_none());
    assert!(started.elapsed() >= Duration::from_millis(350));
    read.unlock().await.unwrap();
}

#[tokio::test]
async fn a_waiting_writer_wakes_up_when_the_last_reader_leaves() {
    let client = client().await;
    let name = unique("rw");
    let lock = lock_of(&client, &name);
    let read = lock.read().await.unwrap();
    let writer = lock_of(&client, &name);
    let waiting = tokio::spawn(async move { writer.write().await.unwrap() });
    sleep(Duration::from_millis(300)).await;
    let released_at = Instant::now();
    read.unlock().await.unwrap();
    let guard = waiting.await.unwrap();
    assert!(released_at.elapsed() < Duration::from_secs(1));
    guard.unlock().await.unwrap();
}

#[tokio::test]
async fn waiting_readers_wake_up_when_the_writer_leaves() {
    let client = client().await;
    let name = unique("rw");
    let lock = lock_of(&client, &name);
    let write = lock.write().await.unwrap();
    let reader = lock_of(&client, &name);
    let waiting = tokio::spawn(async move { reader.read().await.unwrap() });
    sleep(Duration::from_millis(300)).await;
    let released_at = Instant::now();
    write.unlock().await.unwrap();
    let guard = waiting.await.unwrap();
    assert!(released_at.elapsed() < Duration::from_secs(1));
    guard.unlock().await.unwrap();
}

#[tokio::test]
async fn dropping_a_guard_releases_the_lock() {
    let client = client().await;
    let name = unique("rw");
    let lock = lock_of(&client, &name);
    drop(lock.read().await.unwrap());
    let writer = lock_of(&client, &name);
    let got = in_task(move || async move {
        writer
            .write_for(Duration::from_secs(3))
            .await
            .unwrap()
            .is_some()
    })
    .await;
    assert!(got);
}

#[tokio::test]
async fn a_crashed_reader_stops_blocking_writers_when_its_lease_ends() {
    let name = unique("rw");
    let url = redis_url().await;
    let crashing_name = name.clone();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async move {
            let client = Client::builder()
                .url(url)
                .lock_lease(Duration::from_millis(900))
                .build()
                .await
                .unwrap();
            let guard = client.rw_lock(crashing_name).read().await.unwrap();
            std::mem::forget(guard);
        });
    })
    .join()
    .unwrap();

    let client = connect_with(|builder| builder.lock_lease(Duration::from_millis(900))).await;
    let writer = lock_of(&client, &name);
    let started = Instant::now();
    let got = in_task(move || async move {
        writer
            .write_for(Duration::from_secs(5))
            .await
            .unwrap()
            .is_some()
    })
    .await;
    assert!(got);
    assert!(started.elapsed() < Duration::from_secs(4));
}

#[tokio::test]
async fn force_unlock_releases_readers_and_writer() {
    let client = client().await;
    let name = unique("rw");
    let lock = lock_of(&client, &name);
    let _read = lock.read().await.unwrap();
    assert!(lock.force_unlock().await.unwrap());
    assert!(!lock.force_unlock().await.unwrap());
    let other = lock_of(&client, &name);
    assert!(in_task(move || async move { other.try_write().await.unwrap().is_some() }).await);
}

#[tokio::test]
async fn debug_shows_the_name() {
    let client = client().await;
    let name = unique("rw");
    assert!(format!("{:?}", lock_of(&client, &name)).contains(&name));
}

#[tokio::test]
async fn the_lock_is_one_hash_with_a_mode() {
    let client = client().await;
    let name = unique("rw");
    let key = format!("{{{name}}}");
    let lock = lock_of(&client, &name);
    let read = lock.read().await.unwrap();
    let mode = common::raw_command(&["HGET", &key, "mode"]).await;
    assert!(mode.contains("read"), "{mode}");
    let ttl = common::raw_command(&["PTTL", &key]).await;
    let ttl: i64 = ttl.trim().trim_start_matches(':').parse().unwrap();
    assert!(ttl > 0, "{ttl}");
    read.unlock().await.unwrap();
    assert_eq!(common::raw_command(&["EXISTS", &key]).await.trim(), ":0");

    let write = lock.write().await.unwrap();
    let mode = common::raw_command(&["HGET", &key, "mode"]).await;
    assert!(mode.contains("write"), "{mode}");
    write.unlock().await.unwrap();
    assert_eq!(common::raw_command(&["EXISTS", &key]).await.trim(), ":0");
}
