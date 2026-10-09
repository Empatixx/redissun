mod common;

use common::{client, connect_with, redis_url, unique};
use redissun::{Client, ClientBuilder, Error, Object, RwLock};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, timeout, Instant};

type Job = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

#[derive(Clone)]
struct Owner(mpsc::UnboundedSender<Job>);

impl Owner {
    fn new() -> Self {
        let (sender, mut receiver) = mpsc::unbounded_channel::<Job>();
        tokio::spawn(async move {
            while let Some(job) = receiver.recv().await {
                job().await;
            }
        });
        Self(sender)
    }

    async fn run<T, F, Fut>(&self, work: F) -> T
    where
        T: Send + 'static,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        let job: Job = Box::new(move || {
            Box::pin(async move {
                let _ = sender.send(work().await);
            })
        });
        let _ = self.0.send(job);
        receiver.await.unwrap()
    }
}

async fn abandoned<F, Fut>(configure: fn(ClientBuilder) -> ClientBuilder, work: F)
where
    F: FnOnce(Client) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let url = redis_url().await;
    tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async move {
            let client = configure(Client::builder().url(url)).build().await.unwrap();
            work(client).await;
        });
    })
    .await
    .unwrap();
}

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
    let got = in_task(move || async move {
        other
            .write()
            .timeout(Duration::from_millis(400))
            .await
            .unwrap()
    })
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
            .write()
            .timeout(Duration::from_secs(3))
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
            .write()
            .timeout(Duration::from_secs(5))
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

#[tokio::test]
async fn test_read_lock_expiration() {
    let client = client().await;
    let name = unique("test");
    let first = lock_of(&client, &name);
    tokio::spawn(async move {
        let guard = first.read().lease(Duration::from_secs(5)).await.unwrap();
        sleep(Duration::from_millis(4550)).await;
        guard.unlock().await.unwrap();
    });
    sleep(Duration::from_millis(150)).await;
    let second = lock_of(&client, &name);
    tokio::spawn(async move {
        let guard = second
            .read()
            .lease(Duration::from_millis(1500))
            .await
            .unwrap();
        sleep(Duration::from_millis(1400)).await;
        guard.unlock().await.unwrap();
    });
    sleep(Duration::from_millis(150)).await;
    let started = Instant::now();
    let flag = Arc::new(AtomicBool::new(false));
    let third = lock_of(&client, &name);
    let set = flag.clone();
    let writer = tokio::spawn(async move {
        let guard = third.write().lease(Duration::from_secs(5)).await.unwrap();
        set.store(true, Ordering::SeqCst);
        guard.unlock().await.unwrap();
    });
    timeout(Duration::from_secs(10), writer)
        .await
        .unwrap()
        .unwrap();
    assert!(flag.load(Ordering::SeqCst));
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(3800) && elapsed < Duration::from_secs(6),
        "{elapsed:?}"
    );
}

#[tokio::test]
async fn test_read_lock_is_locked() {
    let lock = lock_of(&client().await, &unique("TEST"));
    let write = lock.write().await.unwrap();
    assert!(!lock.is_read_locked().await.unwrap());
    let read = lock
        .read()
        .timeout(Duration::from_secs(10))
        .await
        .unwrap()
        .unwrap();
    assert!(lock.is_read_locked().await.unwrap());
    read.unlock().await.unwrap();
    write.unlock().await.unwrap();
}

#[tokio::test]
async fn test_read_lock_expiration_renewal() {
    let lease = Duration::from_millis(1000);
    let client = connect_with(|builder| builder.lock_lease(Duration::from_millis(1000))).await;
    let name = unique("mytestlock");
    let exceptions = Arc::new(AtomicU32::new(0));
    let mut tasks = Vec::new();
    for _ in 0..10 {
        let lock = lock_of(&client, &name);
        let exceptions = exceptions.clone();
        tasks.push(tokio::spawn(async move {
            match lock.read().await {
                Ok(guard) => {
                    sleep(lease + Duration::from_millis(1500)).await;
                    if guard.unlock().await.is_err() {
                        exceptions.fetch_add(1, Ordering::SeqCst);
                    }
                }
                Err(_) => {
                    exceptions.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
    }
    timeout(Duration::from_secs(180), futures::future::join_all(tasks))
        .await
        .unwrap();
    assert_eq!(exceptions.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn test_name() {
    let client = client().await;
    let name = format!("{{{}}}:abc:key", unique("test"));
    let mut tasks = Vec::new();
    for _ in 0..10 {
        let lock = lock_of(&client, &name);
        tasks.push(tokio::spawn(async move {
            for _ in 0..10 {
                lock.read().await.unwrap().unlock().await.unwrap();
            }
        }));
    }
    for task in tasks {
        timeout(Duration::from_secs(60), task)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn test_write_lock_expiration() {
    let client = client().await;
    let name = unique("test2s3");
    let lock = lock_of(&client, &name);
    let first = lock
        .write()
        .lease(Duration::from_secs(10))
        .timeout(Duration::from_secs(10))
        .await
        .unwrap();
    assert!(first.is_some());
    let second = lock
        .write()
        .lease(Duration::from_secs(1))
        .timeout(Duration::from_secs(1))
        .await
        .unwrap();
    assert!(second.is_some());
    let other = lock_of(&client, &name);
    let refused = tokio::spawn(async move {
        other
            .write()
            .lease(Duration::from_secs(1))
            .timeout(Duration::from_secs(3))
            .await
            .unwrap()
            .is_none()
    });
    assert!(timeout(Duration::from_secs(10), refused)
        .await
        .unwrap()
        .unwrap());
}

#[tokio::test]
async fn test_read_lock_lease_timeout_diff_threads_wrr() {
    let client = client().await;
    let name = unique("my_read_write_lock");
    let lock = lock_of(&client, &name);
    let write = lock
        .write()
        .lease(Duration::from_secs(2))
        .timeout(Duration::from_secs(1))
        .await
        .unwrap();
    assert!(write.is_some());
    let executed = Arc::new(AtomicU32::new(0));
    let mut readers = Vec::new();
    for _ in 0..2 {
        let reader = lock_of(&client, &name);
        let executed = executed.clone();
        readers.push(tokio::spawn(async move {
            std::mem::forget(reader.read().await.unwrap());
            executed.fetch_add(1, Ordering::SeqCst);
        }));
    }
    timeout(Duration::from_secs(3), futures::future::join_all(readers))
        .await
        .unwrap();
    assert_eq!(executed.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_read_lock_lease_timeout_diff_threads_rrw() {
    let client = client().await;
    let name = unique("my_read_write_lock");
    let first = lock_of(&client, &name);
    tokio::spawn(async move {
        let guard = first
            .read()
            .lease(Duration::from_secs(5))
            .timeout(Duration::from_secs(1))
            .await
            .unwrap();
        std::mem::forget(guard.unwrap());
    })
    .await
    .unwrap();
    sleep(Duration::from_millis(2500)).await;
    let second = lock_of(&client, &name);
    tokio::spawn(async move {
        let guard = second
            .read()
            .lease(Duration::from_secs(5))
            .timeout(Duration::from_secs(1))
            .await
            .unwrap()
            .unwrap();
        guard.unlock().await.unwrap();
    })
    .await
    .unwrap();
    let writer = lock_of(&client, &name);
    let written = tokio::spawn(async move {
        writer
            .write()
            .lease(Duration::from_secs(5))
            .timeout(Duration::from_secs(5))
            .await
            .unwrap()
            .map(std::mem::forget)
            .is_some()
    });
    assert!(timeout(Duration::from_secs(4), written)
        .await
        .unwrap()
        .unwrap());
}

#[tokio::test]
async fn test_read_lock_lease_timeout() {
    let client = client().await;
    let name = unique("my_read_write_lock");
    let lock = lock_of(&client, &name);
    let first = lock
        .read()
        .lease(Duration::from_secs(4))
        .timeout(Duration::from_secs(1))
        .await
        .unwrap();
    assert!(first.is_some());
    sleep(Duration::from_secs(3)).await;
    let second = lock
        .read()
        .lease(Duration::from_secs(4))
        .timeout(Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();
    second.unlock().await.unwrap();
    sleep(Duration::from_secs(2)).await;
    assert!(lock.try_write().await.unwrap().is_some());
    std::mem::forget(first);
}

#[tokio::test]
async fn test_wr() {
    let lock = lock_of(&client().await, &unique("my_read_write_lock"));
    let write = lock.write().await.unwrap();
    let read = lock.read().await.unwrap();
    assert!(lock.is_write_locked().await.unwrap());
    read.unlock().await.unwrap();
    assert!(lock.is_write_locked().await.unwrap());
    write.unlock().await.unwrap();
    assert!(!lock.is_write_locked().await.unwrap());
}

#[tokio::test]
async fn test_write_read() {
    let client = client().await;
    let name = unique("TEST");
    let lock = lock_of(&client, &name);
    let write = lock.write().await.unwrap();
    let mut readers = Vec::new();
    for _ in 0..20 {
        let reader = lock_of(&client, &name);
        readers.push(tokio::spawn(async move {
            let guard = reader.read().await.unwrap();
            sleep(Duration::from_millis(800)).await;
            guard.unlock().await.unwrap();
        }));
        sleep(Duration::from_millis(100)).await;
    }
    write.unlock().await.unwrap();
    timeout(Duration::from_secs(2), futures::future::join_all(readers))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_write_read_reentrancy() {
    let client = client().await;
    let name = unique("TEST");
    let lock = lock_of(&client, &name);
    let write = lock.write().await.unwrap();
    let read = lock.try_read().await.unwrap().unwrap();
    let other = lock_of(&client, &name);
    assert!(
        !tokio::spawn(async move { other.try_read().await.unwrap().is_some() })
            .await
            .unwrap()
    );
    write.unlock().await.unwrap();
    assert!(lock.try_write().await.unwrap().is_none());
    read.unlock().await.unwrap();
    let write = lock.try_write().await.unwrap().unwrap();
    write.unlock().await.unwrap();
}

#[tokio::test]
async fn test_write_lock() {
    let client = client().await;
    let name = unique("lock");
    let lock = lock_of(&client, &name);
    let write = lock.write().await.unwrap();
    let again = lock.try_write().await.unwrap().unwrap();

    let other = lock_of(&client, &name);
    let checks = tokio::spawn(async move {
        assert!(!other.is_write_held_by_current().await.unwrap());
        assert!(other.is_write_locked().await.unwrap());
        assert!(other.try_read().await.unwrap().is_none());
        sleep(Duration::from_millis(1000)).await;
        let first = other.try_read().await.unwrap();
        let second = other.try_read().await.unwrap();
        assert!(first.is_some() && second.is_some());
        std::mem::forget(first);
        std::mem::forget(second);
    });
    sleep(Duration::from_millis(50)).await;

    write.unlock().await.unwrap();
    let read = lock.try_read().await.unwrap();
    assert!(read.is_some());
    assert!(lock.is_write_held_by_current().await.unwrap());
    again.unlock().await.unwrap();
    sleep(Duration::from_millis(1000)).await;
    checks.await.unwrap();

    assert!(lock.try_write().await.unwrap().is_none());
    assert!(!lock.is_write_locked().await.unwrap());
    assert!(!lock.is_write_held_by_current().await.unwrap());
    lock.force_unlock_write().await.unwrap();
    std::mem::forget(read);
}

#[tokio::test]
async fn test_multi_read() {
    let client = client().await;
    let name = unique("lock");
    let lock = lock_of(&client, &name);
    assert!(!lock.force_unlock_read().await.unwrap());
    let read1 = lock.read().await.unwrap();
    assert!(lock.try_write().await.unwrap().is_none());

    let other = lock_of(&client, &name);
    let reader = tokio::spawn(async move {
        assert!(!other.is_read_held_by_current().await.unwrap());
        assert!(other.is_read_locked().await.unwrap());
        let guard = other.read().await.unwrap();
        sleep(Duration::from_millis(1000)).await;
        guard.unlock().await.unwrap();
    });
    sleep(Duration::from_millis(50)).await;
    assert!(lock.is_read_locked().await.unwrap());

    read1.unlock().await.unwrap();
    assert!(lock.try_write().await.unwrap().is_none());
    assert!(!lock.is_read_held_by_current().await.unwrap());
    reader.await.unwrap();

    assert!(!lock.is_read_locked().await.unwrap());
    let write = lock.try_write().await.unwrap().unwrap();
    assert!(lock.is_write_locked().await.unwrap());
    assert!(lock.is_write_held_by_current().await.unwrap());
    write.unlock().await.unwrap();
    assert!(!lock.is_write_locked().await.unwrap());
    assert!(!lock.is_write_held_by_current().await.unwrap());
    let _write = lock.try_write().await.unwrap().unwrap();
    lock.force_unlock_write().await.unwrap();
}

#[tokio::test]
async fn test_force_unlock() {
    let client = client().await;
    let name = unique("lock");
    let lock = lock_of(&client, &name);
    let _read = lock.read().await.unwrap();
    assert!(lock.is_read_locked().await.unwrap());
    lock.force_unlock_write().await.unwrap();
    assert!(lock.is_read_locked().await.unwrap());
    lock.force_unlock_read().await.unwrap();
    assert!(!lock.is_read_locked().await.unwrap());

    let _write = lock.write().await.unwrap();
    assert!(lock.is_write_locked().await.unwrap());
    lock.force_unlock_read().await.unwrap();
    assert!(lock.is_write_locked().await.unwrap());
    lock.force_unlock_write().await.unwrap();
    assert!(!lock.is_write_locked().await.unwrap());

    let lock = lock_of(&client, &name);
    assert!(!lock.is_read_locked().await.unwrap());
    assert!(!lock.is_write_locked().await.unwrap());
}

#[tokio::test]
async fn test_read_write_ttl() {
    let lease = Duration::from_millis(3000);
    let client = connect_with(|builder| builder.lock_lease(Duration::from_millis(3000))).await;
    let lock = lock_of(&client, &unique("rwlock"));
    let write = lock.write().await.unwrap();
    let read = lock.read().await.unwrap();
    let floor = lease * 6 / 10;
    for _ in 0..5 {
        assert!(lock.ttl().await.unwrap().unwrap() > floor);
        sleep(Duration::from_millis(500)).await;
    }
    write.unlock().await.unwrap();
    for _ in 0..5 {
        assert!(lock.ttl().await.unwrap().unwrap() > floor);
        sleep(Duration::from_millis(500)).await;
    }
    read.unlock().await.unwrap();
}

#[tokio::test]
async fn test_expire_read() {
    let client = client().await;
    let name = unique("lock");
    let lock = lock_of(&client, &name);
    let guard = lock.read().lease(Duration::from_secs(2)).await.unwrap();
    let started = Instant::now();
    let other = lock_of(&client, &name);
    tokio::spawn(async move {
        let guard = other.read().await.unwrap();
        assert!(started.elapsed() < Duration::from_millis(2050));
        guard.unlock().await.unwrap();
    })
    .await
    .unwrap();
    guard.unlock().await.unwrap();
}

#[tokio::test]
async fn test_expire_write() {
    let client = client().await;
    let name = unique("lock");
    let lock = lock_of(&client, &name);
    let guard = lock.write().lease(Duration::from_secs(2)).await.unwrap();
    let started = Instant::now();
    let other = lock_of(&client, &name);
    tokio::spawn(async move {
        let guard = other.write().await.unwrap();
        assert!(started.elapsed() < Duration::from_millis(2500));
        guard.unlock().await.unwrap();
    })
    .await
    .unwrap();
    assert!(matches!(guard.unlock().await, Err(Error::LockNotHeld)));
}

#[tokio::test]
async fn test_auto_expire() {
    let name = unique("lock");
    let holder_name = name.clone();
    abandoned(
        |builder| builder.lock_lease(Duration::from_millis(1000)),
        move |client| async move {
            std::mem::forget(client.rw_lock(holder_name).write().await.unwrap());
        },
    )
    .await;
    let lock = lock_of(&client().await, &name);
    let deadline = Instant::now() + Duration::from_millis(2000);
    while lock.is_write_locked().await.unwrap() {
        assert!(
            Instant::now() < deadline,
            "the write lock of a crashed owner never expired"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn test_hold_count() {
    let lock = lock_of(&client().await, &unique("lock"));
    assert_eq!(lock.read_hold_count().await.unwrap(), 0);
    let guard = lock.read().await.unwrap();
    assert_eq!(lock.read_hold_count().await.unwrap(), 1);
    guard.unlock().await.unwrap();
    assert_eq!(lock.read_hold_count().await.unwrap(), 0);
    let first = lock.read().await.unwrap();
    let second = lock.read().await.unwrap();
    assert_eq!(lock.read_hold_count().await.unwrap(), 2);
    second.unlock().await.unwrap();
    assert_eq!(lock.read_hold_count().await.unwrap(), 1);
    first.unlock().await.unwrap();
    assert_eq!(lock.read_hold_count().await.unwrap(), 0);

    assert_eq!(lock.write_hold_count().await.unwrap(), 0);
    let guard = lock.write().await.unwrap();
    assert_eq!(lock.write_hold_count().await.unwrap(), 1);
    guard.unlock().await.unwrap();
    assert_eq!(lock.write_hold_count().await.unwrap(), 0);
    let first = lock.write().await.unwrap();
    let second = lock.write().await.unwrap();
    assert_eq!(lock.write_hold_count().await.unwrap(), 2);
    second.unlock().await.unwrap();
    assert_eq!(lock.write_hold_count().await.unwrap(), 1);
    first.unlock().await.unwrap();
    assert_eq!(lock.write_hold_count().await.unwrap(), 0);
}

#[tokio::test]
async fn test_is_held_by_current_thread_other_thread() {
    let client = client().await;
    let name = unique("lock");
    let guard = lock_of(&client, &name).read().await.unwrap();
    let other = lock_of(&client, &name);
    assert!(
        !tokio::spawn(async move { other.is_read_held_by_current().await.unwrap() })
            .await
            .unwrap()
    );
    guard.unlock().await.unwrap();
    let other = lock_of(&client, &name);
    assert!(
        !tokio::spawn(async move { other.is_read_held_by_current().await.unwrap() })
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_is_held_by_current_thread() {
    let lock = lock_of(&client().await, &unique("lock"));
    assert!(!lock.is_read_held_by_current().await.unwrap());
    let guard = lock.read().await.unwrap();
    assert!(lock.is_read_held_by_current().await.unwrap());
    guard.unlock().await.unwrap();
    assert!(!lock.is_read_held_by_current().await.unwrap());
}

#[tokio::test]
async fn test_is_locked_other_thread() {
    let client = client().await;
    let name = unique("lock");
    let guard = lock_of(&client, &name).read().await.unwrap();
    let other = lock_of(&client, &name);
    assert!(
        tokio::spawn(async move { other.is_read_locked().await.unwrap() })
            .await
            .unwrap()
    );
    guard.unlock().await.unwrap();
    let other = lock_of(&client, &name);
    assert!(
        !tokio::spawn(async move { other.is_read_locked().await.unwrap() })
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_is_locked() {
    let lock = lock_of(&client().await, &unique("lock"));
    assert!(!lock.is_read_locked().await.unwrap());
    let guard = lock.read().await.unwrap();
    assert!(lock.is_read_locked().await.unwrap());
    guard.unlock().await.unwrap();
    assert!(!lock.is_read_locked().await.unwrap());
}

#[tokio::test]
async fn test_unlock_fail() {
    let client = client().await;
    let name = unique("lock");
    let lock = lock_of(&client, &name);
    let stale = lock.read().await.unwrap();
    lock.force_unlock_read().await.unwrap();
    let other = lock_of(&client, &name);
    let held = Owner::new()
        .run(move || async move { other.read().await.unwrap() })
        .await;
    assert!(matches!(stale.unlock().await, Err(Error::LockNotHeld)));
    held.unlock().await.unwrap();
}

#[tokio::test]
async fn test_lock_unlock() {
    let lock = lock_of(&client().await, &unique("lock"));
    lock.read().await.unwrap().unlock().await.unwrap();
    lock.read().await.unwrap().unlock().await.unwrap();
}

#[tokio::test]
async fn test_reentrancy() {
    let client = client().await;
    let lock = lock_of(&client, &unique("lock"));
    let first = lock.try_read().await.unwrap().unwrap();
    let second = lock.try_read().await.unwrap().unwrap();
    second.unlock().await.unwrap();
    let other = lock_of(&client, &unique("lock1"));
    assert!(
        tokio::spawn(async move { other.try_read().await.unwrap().is_some() })
            .await
            .unwrap()
    );
    first.unlock().await.unwrap();
}

async fn take_and_release(lock: &RwLock, write: bool, hold: Duration) {
    let guard = if write {
        lock.write().await.unwrap()
    } else {
        lock.read().await.unwrap()
    };
    sleep(hold).await;
    guard.unlock().await.unwrap();
}

#[tokio::test]
async fn test_concurrency_single_instance() {
    let client = client().await;
    let name = unique("testConcurrency_SingleInstance");
    let counter = Arc::new(AtomicU32::new(0));
    let mut tasks = Vec::new();
    for i in 0..15 {
        let lock = lock_of(&client, &name);
        let counter = counter.clone();
        tasks.push(tokio::spawn(async move {
            take_and_release(&lock, i % 2 == 0, Duration::ZERO).await;
            counter.fetch_add(1, Ordering::SeqCst);
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(counter.load(Ordering::SeqCst), 15);
}

#[tokio::test]
async fn test_concurrency_loop_multi_instance() {
    let name = unique("testConcurrency_MultiInstance1");
    let counter = Arc::new(AtomicU32::new(0));
    let mut tasks = Vec::new();
    for worker in 0..16u64 {
        let lock = lock_of(&client().await, &name);
        let counter = counter.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..10u64 {
                take_and_release(&lock, (worker + i) % 3 == 0, Duration::from_millis(10)).await;
                counter.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(counter.load(Ordering::SeqCst), 160);
}

#[tokio::test]
async fn test_concurrency_multi_instance() {
    let name = unique("testConcurrency_MultiInstance2");
    let counter = Arc::new(AtomicU32::new(0));
    let clients = [
        client().await,
        client().await,
        client().await,
        client().await,
    ];
    let mut tasks = Vec::new();
    for i in 0..100 {
        let lock = lock_of(&clients[i % clients.len()], &name);
        let counter = counter.clone();
        tasks.push(tokio::spawn(async move {
            take_and_release(&lock, i % 2 == 1, Duration::ZERO).await;
            counter.fetch_add(1, Ordering::SeqCst);
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(counter.load(Ordering::SeqCst), 100);
}

#[tokio::test]
async fn test_multi_instance_read_lock_zombie_cleanup() {
    let lease = Duration::from_millis(1000);
    let configure: fn(ClientBuilder) -> ClientBuilder =
        |builder| builder.lock_lease(Duration::from_millis(1000));
    let name = unique("zombie_test");
    let zombie_name = name.clone();
    abandoned(configure, move |client| async move {
        std::mem::forget(client.rw_lock(zombie_name).read().await.unwrap());
    })
    .await;

    let client = connect_with(configure).await;
    let lock = lock_of(&client, &name);
    let read = lock.read().await.unwrap();
    sleep(lease + Duration::from_millis(1500)).await;
    read.unlock().await.unwrap();

    let write = lock
        .write()
        .timeout(Duration::from_secs(5))
        .await
        .unwrap()
        .expect("Write lock should succeed after zombie reader cleanup");
    write.unlock().await.unwrap();
}
