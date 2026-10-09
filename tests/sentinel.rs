mod common;

use common::topology::Topology;
use common::unique;
use redissun::{Client, Object};
use std::future::Future;
use std::time::Duration;
use tokio::time::{sleep, timeout, Instant};

async fn connect(topology: &Topology) -> Client {
    Client::builder()
        .url(topology.sentinel_url())
        .lock_lease(Duration::from_secs(5))
        .build()
        .await
        .expect("could not connect through sentinel")
}

async fn eventually<T, F, Fut>(what: &str, mut attempt: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(value) = attempt().await {
            return value;
        }
        assert!(Instant::now() < deadline, "timed out: {what}");
        sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_replicas(topology: &Topology, key: &str) {
    let master = topology.sentinel_master_port().await;
    let replicas: std::vec::Vec<u16> = topology.ports().take(3).filter(|p| *p != master).collect();
    eventually("replicas copy the key", || async {
        for port in &replicas {
            if topology.cli(*port, &["EXISTS", key]).await.trim() != "1" {
                return None;
            }
        }
        Some(())
    })
    .await;
}

#[tokio::test]
#[ignore = "starts a sentinel topology in docker; run with --ignored"]
async fn objects_work_through_sentinel() {
    let topology = Topology::sentinel().await;
    let client = connect(&topology).await;

    let bucket = client.bucket::<String>(unique("bucket"));
    bucket.set("hello").await.unwrap();
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("hello"));

    let lock = client.lock(unique("lock"));
    let guard = lock.lock().await.unwrap();
    assert!(lock.is_locked().await.unwrap());
    guard.unlock().await.unwrap();
    assert!(!lock.is_locked().await.unwrap());
}

#[tokio::test]
#[ignore = "starts a sentinel topology in docker; run with --ignored"]
async fn reads_and_writes_continue_after_sentinel_failover() {
    let topology = Topology::sentinel().await;
    let client = connect(&topology).await;
    let name = unique("bucket");
    let bucket = client.bucket::<String>(name.clone());
    bucket.set("before").await.unwrap();
    wait_for_replicas(&topology, &name).await;

    let (old, new) = topology.sentinel_failover().await;
    assert_ne!(old, new);

    let value = eventually("bucket readable after failover", || async {
        bucket.get().await.ok()
    })
    .await;
    assert_eq!(value.as_deref(), Some("before"));

    let counter = client.atomic_i64(unique("counter"));
    eventually("writes accepted after failover", || async {
        counter.incr().await.ok()
    })
    .await;
    bucket.set("after").await.unwrap();
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("after"));
}

#[tokio::test]
#[ignore = "starts a sentinel topology in docker; run with --ignored"]
async fn lock_waiter_wakes_after_sentinel_failover() {
    let topology = Topology::sentinel().await;
    let holder_client = connect(&topology).await;
    let waiter_client = connect(&topology).await;
    let name = unique("lock");

    let holder = holder_client.lock(name.clone()).lock().await.unwrap();
    wait_for_replicas(&topology, &name).await;
    let waiter_lock = waiter_client.lock(name.clone());
    let waiter = tokio::spawn(async move { waiter_lock.lock().await });
    sleep(Duration::from_millis(300)).await;

    topology.sentinel_failover().await;
    eventually("holder can talk to the new master", || async {
        holder_client
            .bucket::<String>(unique("probe"))
            .get()
            .await
            .ok()
    })
    .await;
    let _ = holder.unlock().await;

    let guard = timeout(Duration::from_secs(20), waiter)
        .await
        .expect("waiter never got the lock after failover")
        .unwrap()
        .expect("waiter failed after failover");
    assert!(holder_client.lock(name).is_locked().await.unwrap());
    guard.unlock().await.unwrap();
}

#[tokio::test]
#[ignore = "starts a sentinel topology in docker; run with --ignored"]
async fn topic_subscriber_receives_after_sentinel_failover() {
    let topology = Topology::sentinel().await;
    let client = connect(&topology).await;
    let topic = client.topic::<String>(unique("topic"));
    let mut subscriber = topic.subscribe().await.unwrap();

    topology.sentinel_failover().await;

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let _ = topic.publish("ping").await;
        if let Ok(Ok(message)) = timeout(Duration::from_millis(500), subscriber.recv()).await {
            assert_eq!(message, "ping");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "subscriber never received a message after failover"
        );
    }
}

#[tokio::test]
#[ignore = "starts a sentinel topology in docker; run with --ignored"]
async fn blocking_pop_survives_sentinel_failover() {
    let topology = Topology::sentinel().await;
    let client = connect(&topology).await;
    let name = unique("queue");
    let queue = client.vec_deque::<String>(name.clone());
    let consumer = client.vec_deque::<String>(name);
    let waiter = tokio::spawn(async move { consumer.pop_front_wait().await });
    sleep(Duration::from_millis(300)).await;

    topology.sentinel_failover().await;

    eventually("push accepted after failover", || async {
        queue.push_back("job").await.ok()
    })
    .await;
    let popped = timeout(Duration::from_secs(20), waiter)
        .await
        .expect("blocked pop never returned after failover")
        .unwrap();
    match popped {
        Ok(value) => assert_eq!(value, "job"),
        Err(error) => {
            let retried = client
                .vec_deque::<String>(queue.name().to_string())
                .pop_front()
                .await
                .unwrap();
            assert_eq!(
                retried.as_deref(),
                Some("job"),
                "pop failed with {error:?} and the value is gone"
            );
        }
    }
}
