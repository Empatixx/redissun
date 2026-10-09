mod common;

use common::{client, connect_with, redis_url, subscribed_channels, unique};
use redissun::{Client, Error, Object};
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

fn crash_while_holding(name: String, lease: Duration) {
    let url = futures::executor::block_on(redis_url());
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async move {
            let client = Client::builder()
                .url(url)
                .lock_lease(lease)
                .build()
                .await
                .unwrap();
            std::mem::forget(client.lock(name).lock().await.unwrap());
        });
    })
    .join()
    .unwrap();
}

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
    let result = other
        .lock()
        .timeout(Duration::from_millis(300))
        .await
        .unwrap();
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
    let _guard = lock.lock().lease(Duration::from_millis(300)).await.unwrap();
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
    let guard = lock.lock().lease(Duration::from_millis(200)).await.unwrap();
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
    let zero = lock.lock().lease(Duration::ZERO).await;
    assert!(matches!(zero, Err(Error::Config(_))));
    let tiny = lock.lock().lease(Duration::from_micros(500)).await;
    assert!(tiny.is_ok());
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

#[tokio::test]
async fn a_cancelled_unlock_still_releases_the_lock() {
    let lock = client().await.lock(unique("lock"));
    let guard = lock.lock().await.unwrap();
    let _ = timeout(Duration::from_nanos(1), guard.unlock()).await;
    let deadline = Instant::now() + Duration::from_secs(3);
    while lock.is_locked().await.unwrap() {
        assert!(Instant::now() < deadline, "the lock stayed held");
        sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn test_subscriptions_per_connection() {
    let client = client().await;
    let prefix = unique("lock-sub");
    let errors = Arc::new(AtomicU32::new(0));
    let ops = Arc::new(AtomicU32::new(0));
    let mut tasks = Vec::new();
    for i in 0..200u32 {
        let lock = client.lock(format!("{prefix}-{}", i % 5));
        let (errors, ops) = (errors.clone(), ops.clone());
        tasks.push(tokio::spawn(async move {
            match lock.lock().await {
                Ok(guard) => {
                    sleep(Duration::from_millis((i % 20) as u64)).await;
                    match guard.unlock().await {
                        Ok(()) => ops.fetch_add(1, Ordering::SeqCst),
                        Err(_) => errors.fetch_add(1, Ordering::SeqCst),
                    };
                }
                Err(_) => {
                    errors.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
    }
    for task in tasks {
        timeout(Duration::from_secs(150), task)
            .await
            .unwrap()
            .unwrap();
    }
    assert_eq!(errors.load(Ordering::SeqCst), 0);
    assert_eq!(ops.load(Ordering::SeqCst), 200);
    let deadline = Instant::now() + Duration::from_secs(5);
    while subscribed_channels(&format!("redissun__unlock__{prefix}*")).await != 0 {
        assert!(Instant::now() < deadline, "lock channels stayed subscribed");
        sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn test_single_pub_sub() {
    let client = client().await;
    let prefix = unique("lock-single");
    let has_fails = Arc::new(AtomicBool::new(false));
    for i in 0..2 {
        let mut tasks = Vec::new();
        for name in [
            format!("{prefix}-1-{i}"),
            format!("{prefix}-1-{i}"),
            format!("{prefix}-2-{i}"),
            format!("{prefix}-2-{i}"),
        ] {
            let lock = client.lock(name);
            let has_fails = has_fails.clone();
            tasks.push(tokio::spawn(async move {
                match lock.lock().timeout(Duration::from_millis(100)).await {
                    Ok(Some(guard)) => guard.unlock().await.unwrap(),
                    _ => has_fails.store(true, Ordering::SeqCst),
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
    }
    assert!(!has_fails.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_lock_is_not_renewed_after_interrupted_try_lock() {
    let client = short_lease_client(Duration::from_millis(600)).await;
    let lock = client.lock(unique("lock"));
    assert!(!lock.is_locked().await.unwrap());
    let trying = lock.clone();
    let task = tokio::spawn(async move {
        if let Some(guard) = trying.try_lock().await.unwrap() {
            guard.unlock().await.unwrap();
        }
    });
    sleep(Duration::from_millis(5)).await;
    task.abort();
    sleep(Duration::from_secs(2)).await;
    assert!(!lock.is_locked().await.unwrap());
}

#[tokio::test]
async fn test_remain_time_to_live2() {
    let lock = client().await.lock(unique("lock"));
    let lease = Duration::from_secs(10);
    let first = lock.lock().lease(lease).await.unwrap();
    let second = lock.lock().lease(lease).await.unwrap();
    let _third = lock.lock().lease(lease).await.unwrap();
    first.unlock().await.unwrap();
    second.unlock().await.unwrap();
    let ttl = lock.ttl().await.unwrap().unwrap();
    assert!(ttl >= Duration::from_secs(9) && ttl <= lease, "{ttl:?}");
}

#[tokio::test]
async fn test_try_lock_wait() {
    let name = unique("lock");
    let holder = client().await.lock(name.clone());
    let _guard = tokio::spawn(async move { holder.lock().await.unwrap() })
        .await
        .unwrap();
    let lock = client().await.lock(name);
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
    let lock = client.lock(name.clone());
    let _guard = lock.lock().await.unwrap();
    lock.force_unlock().await.unwrap();
    assert!(!lock.is_locked().await.unwrap());
    assert!(!client.lock(name).is_locked().await.unwrap());
}

#[tokio::test]
async fn test_expire() {
    let client = client().await;
    let name = unique("lock");
    let lock = client.lock(name.clone());
    let guard = lock.lock().lease(Duration::from_secs(2)).await.unwrap();
    let started = Instant::now();
    let other = client.lock(name);
    tokio::spawn(async move {
        let guard = other.lock().await.unwrap();
        assert!(started.elapsed() < Duration::from_millis(2500));
        guard.unlock().await.unwrap();
    })
    .await
    .unwrap();
    assert!(matches!(guard.unlock().await, Err(Error::LockNotHeld)));
}

#[tokio::test]
async fn test_remain_time_to_live() {
    let client = client().await;
    let lock = client.lock(unique("test-lock:1"));
    let hour = Duration::from_secs(3600);
    let guard = lock.lock().lease(hour).await.unwrap();
    let ttl = lock.ttl().await.unwrap().unwrap();
    assert!(ttl >= hour - Duration::from_millis(10) && ttl <= hour);
    guard.unlock().await.unwrap();
    assert_eq!(lock.ttl().await.unwrap(), None);
    let guard = lock.lock().await.unwrap();
    let ttl = lock.ttl().await.unwrap().unwrap();
    let watchdog = Duration::from_secs(30);
    assert!(ttl >= watchdog - Duration::from_millis(100) && ttl <= watchdog);
    guard.unlock().await.unwrap();
}

#[tokio::test]
async fn test_auto_expire() {
    let name = unique("lock");
    let lease = Duration::from_millis(1000);
    crash_while_holding(name.clone(), lease);
    let lock = client().await.lock(name);
    assert!(lock.is_locked().await.unwrap());
    let deadline = Instant::now() + lease + Duration::from_millis(500);
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
    let lock = client().await.lock(unique("lock"));
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
    let lock = client.lock(name.clone());
    let guard = lock.lock().await.unwrap();
    let other = client.lock(name.clone());
    assert!(
        !tokio::spawn(async move { other.is_held_by_current().await.unwrap() })
            .await
            .unwrap()
    );
    guard.unlock().await.unwrap();
    let other = client.lock(name);
    assert!(
        !tokio::spawn(async move { other.is_held_by_current().await.unwrap() })
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_is_held_by_current_thread() {
    let lock = client().await.lock(unique("lock"));
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
    let guard = client.lock(name.clone()).lock().await.unwrap();
    let other = client.lock(name.clone());
    assert!(
        tokio::spawn(async move { other.is_locked().await.unwrap() })
            .await
            .unwrap()
    );
    guard.unlock().await.unwrap();
    let other = client.lock(name);
    assert!(
        !tokio::spawn(async move { other.is_locked().await.unwrap() })
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_is_locked() {
    let lock = client().await.lock(unique("lock"));
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
    let lock = client.lock(name.clone());
    let stale = lock.lock().await.unwrap();
    lock.force_unlock().await.unwrap();
    let other = client.lock(name);
    let holder = Owner::new();
    let held = holder
        .run(move || async move { other.lock().await.unwrap() })
        .await;
    assert!(matches!(stale.unlock().await, Err(Error::LockNotHeld)));
    assert!(lock.is_locked().await.unwrap());
    held.unlock().await.unwrap();
}

#[tokio::test]
async fn test_lock_unlock() {
    let lock = client().await.lock(unique("lock1"));
    lock.lock().await.unwrap().unlock().await.unwrap();
    lock.lock().await.unwrap().unlock().await.unwrap();
}

#[tokio::test]
async fn test_reentrancy() {
    let client = client().await;
    let name = unique("lock1");
    let lock = client.lock(name.clone());
    let first = lock.try_lock().await.unwrap().unwrap();
    let second = lock.try_lock().await.unwrap().unwrap();
    second.unlock().await.unwrap();
    let other = client.lock(name);
    assert!(
        tokio::spawn(async move { other.try_lock().await.unwrap().is_none() })
            .await
            .unwrap()
    );
    first.unlock().await.unwrap();
}

#[tokio::test]
async fn test_concurrency_try_lock_un_lock() {
    let client = client().await;
    let name = unique("1testConcurrencyTryLockUnLock");
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let lock = client.lock(name.clone());
        tasks.push(tokio::spawn(async move {
            for _ in 0..100 {
                if let Some(guard) = lock.try_lock().await.unwrap() {
                    guard.unlock().await.unwrap();
                }
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert!(!client.lock(name).is_locked().await.unwrap());
}

#[tokio::test]
async fn test_concurrency_single_instance() {
    let client = client().await;
    let name = unique("testConcurrency_SingleInstance");
    let counter = Arc::new(AtomicU32::new(0));
    let mut tasks = Vec::new();
    for _ in 0..15 {
        let lock = client.lock(name.clone());
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
    assert_eq!(counter.load(Ordering::SeqCst), 15);
}

#[tokio::test]
async fn test_concurrency_loop_multi_instance() {
    let name = unique("testConcurrency_MultiInstance1");
    let counter = Arc::new(AtomicU32::new(0));
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let lock = client().await.lock(name.clone());
        let counter = counter.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..10 {
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
        let lock = clients[i % clients.len()].lock(name.clone());
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
async fn test_lock_reentrant_renew() {
    let lease = Duration::from_millis(1000);
    let client = short_lease_client(lease).await;
    let name = unique("LOCK_KEY");
    let lock = client.lock(name.clone());
    let first = lock.lock().await.unwrap();
    let second = lock.lock().await.unwrap();
    second.unlock().await.unwrap();
    first.unlock().await.unwrap();

    let other = client.lock(name);
    let holder = tokio::spawn(async move {
        let guard = other.lock().await.unwrap();
        sleep(Duration::from_secs(3600)).await;
        drop(guard);
    });
    sleep(lease * 5).await;
    assert!(lock.exists().await.unwrap());
    holder.abort();
}
