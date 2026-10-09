mod common;

use common::{client, connect_with, raw_command, redis_url, unique};
use redissun::{Client, ClientBuilder, Error, FairLock, LockGuard, Object};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, Mutex};
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

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn bulk_strings(reply: &str) -> Vec<String> {
    reply
        .split("\r\n")
        .filter(|line| !line.is_empty() && !line.starts_with('*') && !line.starts_with('$'))
        .map(str::to_string)
        .collect()
}

async fn waiting_owners(name: &str) -> Vec<String> {
    let key = format!("redissun__lock_queue:{{{name}}}");
    bulk_strings(&raw_command(&["LRANGE", &key, "0", "-1"]).await)
}

async fn timeout_of(name: &str, owner: &str) -> i64 {
    let key = format!("redissun__lock_timeout:{{{name}}}");
    let reply = raw_command(&["ZSCORE", &key, owner]).await;
    bulk_strings(&reply)[0].parse::<f64>().unwrap() as i64
}

async fn all_timeouts(name: &str) -> Vec<i64> {
    let key = format!("redissun__lock_timeout:{{{name}}}");
    let reply = raw_command(&["ZRANGE", &key, "0", "-1", "WITHSCORES"]).await;
    bulk_strings(&reply)
        .chunks(2)
        .map(|pair| pair[1].parse::<f64>().unwrap() as i64)
        .collect()
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
    sleep(Duration::from_millis(100)).await;

    assert!(lock.exists().await.unwrap());
    assert!(lock.del().await.unwrap());
    assert_eq!(lock.queue_len().await.unwrap(), 0);
    assert!(!lock.exists().await.unwrap());
    let _ = waiting.await;
    drop(holder);
}

#[tokio::test]
async fn a_waiter_without_a_timeout_keeps_its_place_for_the_fair_lock_wait_timeout() {
    let client = client().await;
    let name = unique("fair");
    let lock = lock_of(&client, &name);
    let order = Arc::new(Mutex::new(Vec::new()));

    let holder = lock.clone();
    let first = tokio::spawn(async move {
        let guard = holder.lock().lease(Duration::from_secs(3)).await.unwrap();
        sleep(Duration::from_secs(1)).await;
        guard.unlock().await.unwrap();
    });
    sleep(Duration::from_millis(100)).await;

    let mut tasks = Vec::new();
    for (index, hold) in [(0usize, 9u64), (1, 0), (2, 0)] {
        let waiter = lock.clone();
        let order = order.clone();
        tasks.push(tokio::spawn(async move {
            if index == 2 {
                sleep(Duration::from_secs(5)).await;
            }
            let guard = waiter.lock().await.unwrap();
            order.lock().await.push(index);
            sleep(Duration::from_secs(hold)).await;
            guard.unlock().await.unwrap();
        }));
        if index < 2 {
            queued(&lock, index + 1).await;
        }
    }
    first.await.unwrap();
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(*order.lock().await, vec![0, 1, 2]);
}

#[tokio::test]
async fn test_lease_timeout() {
    let client = client().await;
    let name = unique("lock");
    let list = Arc::new(Mutex::new(Vec::new()));
    let mut tasks = Vec::new();
    for index in 0..8usize {
        sleep(Duration::from_millis(100)).await;
        let lock = lock_of(&client, &name);
        let list = list.clone();
        tasks.push(tokio::spawn(async move {
            let acquired = lock
                .lock()
                .lease(Duration::from_secs(1))
                .timeout(Duration::from_secs(10))
                .await
                .unwrap();
            if let Some(guard) = acquired {
                list.lock().await.push(index);
                std::mem::forget(guard);
            }
        }));
    }
    timeout(Duration::from_secs(11), futures::future::join_all(tasks))
        .await
        .unwrap();
    assert_eq!(*list.lock().await, (0..8).collect::<Vec<_>>());
}

#[tokio::test]
async fn test_multiple_locks() {
    let client = client().await;
    let name = unique("lock");
    let acquired = Arc::new(AtomicU32::new(0));
    let mut tasks = Vec::new();
    for _ in 0..20 {
        let lock = lock_of(&client, &name);
        let acquired = acquired.clone();
        tasks.push(tokio::spawn(async move {
            let guard = lock.lock().lease(Duration::from_secs(5)).await.unwrap();
            sleep(Duration::from_millis(50)).await;
            acquired.fetch_add(1, Ordering::SeqCst);
            guard.unlock().await.unwrap();
        }));
    }
    timeout(Duration::from_secs(180), futures::future::join_all(tasks))
        .await
        .unwrap();
    assert_eq!(acquired.load(Ordering::SeqCst), 20);
}

async fn drift_scenario(jobs: usize, wait: Duration, hold: Duration) {
    let lease = Duration::from_secs(30);
    let client = client().await;
    let name = unique("test-fair-lock");
    let lock = lock_of(&client, &name);
    let (sender, receiver) = mpsc::unbounded_channel::<()>();
    let receiver = Arc::new(Mutex::new(receiver));
    let mut workers = Vec::new();
    for _ in 0..3 {
        let lock = lock.clone();
        let receiver = receiver.clone();
        workers.push(tokio::spawn(async move {
            while receiver.lock().await.recv().await.is_some() {
                if let Ok(Some(guard)) = lock.lock().lease(lease).timeout(wait).await {
                    sleep(hold).await;
                    let _ = guard.unlock().await;
                }
            }
        }));
        sleep(Duration::from_millis(50)).await;
    }
    for _ in 0..jobs {
        sender.send(()).unwrap();
    }
    drop(sender);
    for worker in workers {
        worker.await.unwrap();
    }

    let final_name = name.clone();
    abandoned(
        |builder| builder,
        move |client| async move {
            let lock = client.fair_lock(final_name);
            let _ = timeout(
                Duration::from_millis(300),
                lock.lock().lease(lease).timeout(Duration::from_secs(30)),
            )
            .await;
        },
    )
    .await;
    lock.force_unlock().await.unwrap();
    assert!(!lock.is_locked().await.unwrap());

    let now = now_millis();
    for (index, due) in all_timeouts(&name).await.into_iter().enumerate() {
        let expiry = (due - now) / 1000;
        assert!(
            expiry <= 30 + 300 * (index as i64 + 1),
            "item {index} expires in {expiry} seconds"
        );
    }
}

#[tokio::test]
async fn test_wait_timeout_drift() {
    drift_scenario(12, Duration::from_millis(500), Duration::from_secs(1)).await;
}

#[tokio::test]
async fn test_lock_acquired_timeout_drift() {
    drift_scenario(3, Duration::from_secs(3), Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_first_thread_death_timeout_drift() {
    drift_scenario(10, Duration::from_secs(3), Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_acquire_failed_timeout_drift_descrete() {
    let wait = Duration::from_secs(5);
    let lease = Duration::from_secs(30);
    let client = connect_with(|builder| builder.fair_lock_wait_timeout(wait)).await;
    let name = unique("testAcquireFailedTimeoutDrift_Descrete");
    let lock = lock_of(&client, &name);

    let init = Owner::new();
    let holder = lock.clone();
    let init_guard = init
        .run(move || async move { holder.lock().lease(lease).await.unwrap() })
        .await;

    let started = now_millis();
    let mut waiters = Vec::new();
    for count in 1..=4 {
        let waiter = lock.clone();
        waiters.push(tokio::spawn(async move {
            waiter.lock().lease(lease).await.unwrap()
        }));
        queued(&lock, count).await;
    }
    let owners = waiting_owners(&name).await;
    let mut due = Vec::new();
    for owner in &owners {
        due.push(timeout_of(&name, owner).await);
    }
    let first = due[0] - started;
    assert!((34_000..=35_100).contains(&first), "{first}");
    assert_eq!(due[1] - due[0], 5000);
    assert_eq!(due[2] - due[0], 10_000);
    assert_eq!(due[3] - due[0], 15_000);

    waiters.remove(1).abort();
    queued(&lock, 3).await;
    assert_eq!(timeout_of(&name, &owners[0]).await, due[0]);
    assert_eq!(timeout_of(&name, &owners[2]).await, due[2] - 5000);
    assert_eq!(timeout_of(&name, &owners[3]).await, due[3] - 5000);

    init_guard.unlock().await.unwrap();
    queued(&lock, 2).await;
    assert_eq!(
        waiting_owners(&name).await,
        vec![owners[2].clone(), owners[3].clone()]
    );
    assert_eq!(timeout_of(&name, &owners[2]).await, due[2] - 10_000);
    assert_eq!(timeout_of(&name, &owners[3]).await, due[3] - 10_000);

    let first_guard = waiters.remove(0).await.unwrap();
    for waiter in &waiters {
        waiter.abort();
    }
    first_guard.unlock().await.unwrap();
}

#[tokio::test]
async fn test_lock_acquired_boolean_timeout_drift_descrete() {
    let lease = Duration::from_millis(500);
    let configure: fn(ClientBuilder) -> ClientBuilder = |builder| {
        builder
            .fair_lock_wait_timeout(Duration::from_millis(100))
            .lock_lease(Duration::from_millis(500))
    };
    let client = connect_with(configure).await;
    let name = unique("testLockAcquiredTimeoutDrift_Descrete");
    let lock = lock_of(&client, &name);
    let try_lock = |owner: Owner| {
        let lock = lock.clone();
        async move {
            owner
                .run(move || async move { lock.try_lock().await.unwrap() })
                .await
        }
    };
    let (init, second, third) = (Owner::new(), Owner::new(), Owner::new());

    let init_guard = try_lock(init.clone()).await.unwrap();
    let first_name = name.clone();
    assert!(abandoned_try_lock(configure, first_name.clone())
        .await
        .is_none());
    assert!(try_lock(second.clone()).await.is_none());

    init_guard.unlock().await.unwrap();

    assert!(abandoned_try_lock(configure, first_name).await.is_some());
    assert!(try_lock(third.clone()).await.is_none());
    assert!(try_lock(second.clone()).await.is_none());
    assert!(try_lock(third.clone()).await.is_none());

    sleep(lease + Duration::from_millis(100)).await;

    let guard = try_lock(third).await;
    assert!(guard.is_some());
    guard.unwrap().unlock().await.unwrap();
}

async fn abandoned_try_lock(
    configure: fn(ClientBuilder) -> ClientBuilder,
    name: String,
) -> Option<()> {
    let (sender, receiver) = oneshot::channel();
    abandoned(configure, move |client| async move {
        let guard = client.fair_lock(name).try_lock().await.unwrap();
        let _ = sender.send(guard.map(std::mem::forget));
    })
    .await;
    receiver.await.unwrap()
}

#[tokio::test]
async fn test_lock_acquired_timeout_drift_descrete() {
    let wait = Duration::from_secs(5);
    let lease = Duration::from_secs(300);
    let client = connect_with(|builder| builder.fair_lock_wait_timeout(wait)).await;
    let name = unique("testLockAcquiredTimeoutDrift_Descrete");
    let lock = lock_of(&client, &name);

    let init = Owner::new();
    let holder = lock.clone();
    let init_guard = init
        .run(move || async move { holder.lock().lease(lease).await.unwrap() })
        .await;
    let mut waiters = Vec::new();
    for count in 1..=2 {
        let waiter = lock.clone();
        waiters.push(tokio::spawn(async move {
            waiter.lock().lease(lease).await.unwrap()
        }));
        queued(&lock, count).await;
    }
    let owners = waiting_owners(&name).await;
    let second_due = timeout_of(&name, &owners[1]).await;

    init_guard.unlock().await.unwrap();
    queued(&lock, 1).await;
    let first_guard = waiters.remove(0).await.unwrap();

    let third = lock.clone();
    let third_waiter = tokio::spawn(async move { third.lock().lease(lease).await.unwrap() });
    queued(&lock, 2).await;
    let owners_now = waiting_owners(&name).await;
    let second_again = timeout_of(&name, &owners_now[0]).await;
    let third_due = timeout_of(&name, &owners_now[1]).await;
    assert_eq!(owners_now[0], owners[1]);
    assert_eq!(second_due - second_again, 5000);
    assert_eq!(third_due - second_again, 5000);

    waiters.remove(0).abort();
    third_waiter.abort();
    first_guard.unlock().await.unwrap();
}

#[tokio::test]
async fn test_abandoned_timeout_drift_descrete() {
    let lease = Duration::from_millis(500);
    let thread_wait = Duration::from_millis(100);
    let configure: fn(ClientBuilder) -> ClientBuilder =
        |builder| builder.fair_lock_wait_timeout(Duration::from_millis(100));
    let client = connect_with(configure).await;
    let name = unique("testAbandonedTimeoutDrift_Descrete");
    let lock = lock_of(&client, &name);

    let init = Owner::new();
    let holder = lock.clone();
    let _init_guard = init
        .run(move || async move { holder.lock().lease(lease).await.unwrap() })
        .await;

    let abandoned_name = name.clone();
    abandoned(configure, move |client| async move {
        for count in 1..=2 {
            let waiter = client.fair_lock(abandoned_name.clone());
            tokio::spawn(async move {
                let _ = waiter.lock().lease(Duration::from_millis(500)).await;
            });
            let lock = client.fair_lock(abandoned_name.clone());
            while lock.queue_len().await.unwrap() != count {
                sleep(Duration::from_millis(5)).await;
            }
        }
    })
    .await;

    let third = lock.clone();
    let started = Instant::now();
    let third_waiter = tokio::spawn(async move { third.lock().lease(lease).await.unwrap() });
    queued(&lock, 3).await;
    let owners = waiting_owners(&name).await;
    let first_due = timeout_of(&name, &owners[0]).await;
    let third_due = timeout_of(&name, &owners[2]).await;
    assert_eq!(third_due - first_due, 2 * thread_wait.as_millis() as i64);

    let guard = timeout(Duration::from_secs(5), third_waiter)
        .await
        .expect("the abandoned waiters kept the lock away")
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(3));
    guard.unlock().await.unwrap();
}

#[tokio::test]
async fn test_try_lock_non_delayed() {
    let client = client().await;
    let name = unique("SOME_LOCK");
    let first = lock_of(&client, &name);
    let t1 = tokio::spawn(async move {
        let guard = first
            .lock()
            .timeout(Duration::ZERO)
            .await
            .unwrap()
            .expect("Unable to acquire lock for some reason");
        sleep(Duration::from_millis(1000)).await;
        guard.unlock().await.unwrap();
    });
    let second = lock_of(&client, &name);
    let t2 = tokio::spawn(async move {
        sleep(Duration::from_millis(200)).await;
        let got = second
            .lock()
            .timeout(Duration::from_millis(200))
            .await
            .unwrap();
        assert!(got.is_none(), "Should not be inside second block");
    });
    t1.await.unwrap();
    t2.await.unwrap();

    let lock = lock_of(&client, &name);
    let guard = lock
        .lock()
        .timeout(Duration::ZERO)
        .await
        .unwrap()
        .expect("Could not get unlocked lock");
    guard.unlock().await.unwrap();
}

#[tokio::test]
async fn test_try_lock_wait() {
    let name = unique("lock");
    let holder = lock_of(&client().await, &name);
    let _guard = tokio::spawn(async move { holder.lock().await.unwrap() })
        .await
        .unwrap();
    let lock = lock_of(&client().await, &name);
    let started = Instant::now();
    assert!(lock
        .lock()
        .timeout(Duration::from_secs(3))
        .await
        .unwrap()
        .is_none());
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(2990) && elapsed < Duration::from_millis(4500),
        "{elapsed:?}"
    );
}

#[tokio::test]
async fn test_force_unlock() {
    let client = client().await;
    let name = unique("lock");
    let lock = lock_of(&client, &name);
    let _guard = lock.lock().await.unwrap();
    lock.force_unlock().await.unwrap();
    assert!(!lock.is_locked().await.unwrap());
    assert!(!lock_of(&client, &name).is_locked().await.unwrap());
}

#[tokio::test]
async fn test_expire() {
    let client = client().await;
    let name = unique("lock");
    let lock = lock_of(&client, &name);
    let guard = lock.lock().lease(Duration::from_secs(2)).await.unwrap();
    let started = Instant::now();
    let other = lock_of(&client, &name);
    tokio::spawn(async move {
        let guard = other.lock().await.unwrap();
        assert!(started.elapsed() < Duration::from_millis(2500));
        guard.unlock().await.unwrap();
    })
    .await
    .unwrap();
    assert!(guard.unlock().await.is_ok());
}

#[tokio::test]
async fn test_auto_expire() {
    let name = unique("lock");
    let lease = Duration::from_millis(1000);
    let holder_name = name.clone();
    abandoned(
        |builder| builder.lock_lease(Duration::from_millis(1000)),
        move |client| async move {
            std::mem::forget(client.fair_lock(holder_name).lock().await.unwrap());
        },
    )
    .await;
    let lock = lock_of(&client().await, &name);
    let deadline = Instant::now() + lease + Duration::from_millis(1000);
    while lock.is_locked().await.unwrap() {
        assert!(
            Instant::now() < deadline,
            "the lock of a crashed owner never expired"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn test_get_hold_count() {
    let lock = lock_of(&client().await, &unique("lock"));
    assert_eq!(lock.hold_count().await.unwrap(), 0);
    let guard = lock.lock().await.unwrap();
    assert_eq!(lock.hold_count().await.unwrap(), 1);
    guard.unlock().await.unwrap();
    assert_eq!(lock.hold_count().await.unwrap(), 0);
    let first = lock.lock().await.unwrap();
    let second = lock.lock().await.unwrap();
    assert_eq!(lock.hold_count().await.unwrap(), 2);
    second.unlock().await.unwrap();
    assert_eq!(lock.hold_count().await.unwrap(), 1);
    first.unlock().await.unwrap();
    assert_eq!(lock.hold_count().await.unwrap(), 0);
}

#[tokio::test]
async fn test_is_held_by_current_thread_other_thread() {
    let client = client().await;
    let name = unique("lock");
    let guard = lock_of(&client, &name).lock().await.unwrap();
    let other = lock_of(&client, &name);
    assert!(
        !tokio::spawn(async move { other.is_held_by_current().await.unwrap() })
            .await
            .unwrap()
    );
    guard.unlock().await.unwrap();
    let other = lock_of(&client, &name);
    assert!(
        !tokio::spawn(async move { other.is_held_by_current().await.unwrap() })
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_is_held_by_current_thread() {
    let lock = lock_of(&client().await, &unique("lock"));
    assert!(!lock.is_held_by_current().await.unwrap());
    let guard = lock.lock().await.unwrap();
    assert!(lock.is_held_by_current().await.unwrap());
    guard.unlock().await.unwrap();
    assert!(!lock.is_held_by_current().await.unwrap());
}

#[tokio::test]
async fn test_is_locked_other_thread() {
    let client = client().await;
    let name = unique("lock");
    let guard = lock_of(&client, &name).lock().await.unwrap();
    let other = lock_of(&client, &name);
    assert!(
        tokio::spawn(async move { other.is_locked().await.unwrap() })
            .await
            .unwrap()
    );
    guard.unlock().await.unwrap();
    let other = lock_of(&client, &name);
    assert!(
        !tokio::spawn(async move { other.is_locked().await.unwrap() })
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_is_locked() {
    let lock = lock_of(&client().await, &unique("lock"));
    assert!(!lock.is_locked().await.unwrap());
    let guard = lock.lock().await.unwrap();
    assert!(lock.is_locked().await.unwrap());
    guard.unlock().await.unwrap();
    assert!(!lock.is_locked().await.unwrap());
}

#[tokio::test]
async fn test_unlock_fail() {
    let client = client().await;
    let name = unique("lock");
    let lock = lock_of(&client, &name);
    let stale = lock.lock().await.unwrap();
    lock.force_unlock().await.unwrap();
    let other = lock_of(&client, &name);
    let held = Owner::new()
        .run(move || async move { other.lock().await.unwrap() })
        .await;
    assert!(matches!(stale.unlock().await, Err(Error::LockNotHeld)));
    held.unlock().await.unwrap();
}

#[tokio::test]
async fn test_lock_unlock() {
    let lock = lock_of(&client().await, &unique("lock1"));
    lock.lock().await.unwrap().unlock().await.unwrap();
    lock.lock().await.unwrap().unlock().await.unwrap();
}

#[tokio::test]
async fn test_reentrancy() {
    let client = client().await;
    let name = unique("lock1");
    let lock = lock_of(&client, &name);
    let first = lock.try_lock().await.unwrap().unwrap();
    let second = lock.try_lock().await.unwrap().unwrap();
    second.unlock().await.unwrap();
    let other = lock_of(&client, &name);
    assert!(
        tokio::spawn(async move { other.try_lock().await.unwrap().is_none() })
            .await
            .unwrap()
    );
    first.unlock().await.unwrap();
}

#[tokio::test]
async fn test_concurrency_single_instance() {
    let client = client().await;
    let name = unique("testConcurrency_SingleInstance");
    let counter = Arc::new(AtomicU32::new(0));
    let mut tasks = Vec::new();
    for _ in 0..15 {
        let lock = lock_of(&client, &name);
        let counter = counter.clone();
        tasks.push(tokio::spawn(async move {
            let guard = lock.lock().await.unwrap();
            counter.fetch_add(1, Ordering::SeqCst);
            guard.unlock().await.unwrap();
        }));
    }
    futures::future::join_all(tasks).await;
    assert_eq!(counter.load(Ordering::SeqCst), 15);
}

#[tokio::test]
async fn test_concurrency_loop_multi_instance() {
    let name = unique("testConcurrency_MultiInstance1");
    let counter = Arc::new(AtomicU32::new(0));
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let lock = lock_of(&client().await, &name);
        let counter = counter.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..5 {
                let guard = lock.lock().await.unwrap();
                sleep(Duration::from_millis(10)).await;
                counter.fetch_add(1, Ordering::SeqCst);
                guard.unlock().await.unwrap();
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(counter.load(Ordering::SeqCst), 80);
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
            let guard = lock.lock().await.unwrap();
            counter.fetch_add(1, Ordering::SeqCst);
            guard.unlock().await.unwrap();
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(counter.load(Ordering::SeqCst), 100);
}

#[tokio::test]
async fn test_concurrency_multi_instance_ordering() {
    let client = client().await;
    let name = unique("testConcurrency_MultiInstance2");
    let arrivals: Arc<std::sync::Mutex<std::collections::VecDeque<usize>>> = Arc::default();
    let counter = Arc::new(AtomicU32::new(0));
    let total = 6;
    let mut tasks = Vec::new();
    for index in 0..total {
        let lock = lock_of(&client, &name);
        let arrivals = arrivals.clone();
        let counter = counter.clone();
        tasks.push(tokio::spawn(async move {
            arrivals.lock().unwrap().push_back(index);
            let guard: LockGuard = lock.lock().await.unwrap();
            assert_eq!(arrivals.lock().unwrap().pop_front(), Some(index));
            sleep(Duration::from_millis(300)).await;
            counter.fetch_add(1, Ordering::SeqCst);
            guard.unlock().await.unwrap();
        }));
        sleep(Duration::from_millis(50)).await;
    }
    timeout(Duration::from_secs(45), async {
        for task in tasks {
            task.await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), total as u32);
}
