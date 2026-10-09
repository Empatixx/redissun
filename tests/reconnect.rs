mod common;

use common::{client, redis_url, unique};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout, Instant};

async fn kill_every_client_connection() {
    let url = redis_url().await;
    let address = url.trim_start_matches("redis://").to_string();
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"*4\r\n$6\r\nCLIENT\r\n$4\r\nKILL\r\n$4\r\nTYPE\r\n$6\r\nnormal\r\n")
        .await
        .unwrap();
}

#[tokio::test]
async fn clients_recover_and_waiters_still_wake_after_every_connection_is_killed() {
    let holder_client = client().await;
    let waiter_client = client().await;
    let name = unique("lock");
    let bucket = holder_client.bucket::<String>(unique("bucket"));
    bucket.set(&"before".to_string()).await.unwrap();

    let holder = holder_client.lock(name.clone()).lock().await.unwrap();
    let waiter_lock = waiter_client.lock(name);
    let waiter = tokio::spawn(async move { waiter_lock.lock().await.unwrap() });
    sleep(Duration::from_millis(300)).await;

    kill_every_client_connection().await;

    let deadline = Instant::now() + Duration::from_secs(10);
    let recovered = loop {
        if let Ok(value) = bucket.get().await {
            break value;
        }
        assert!(Instant::now() < deadline, "client never reconnected");
        sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(recovered.as_deref(), Some("before"));

    holder.unlock().await.unwrap();
    let guard = timeout(Duration::from_secs(5), waiter)
        .await
        .expect("waiter should be notified after the pub/sub connection came back")
        .unwrap();
    guard.unlock().await.unwrap();
}

async fn kill_pubsub_connections() {
    common::raw_command(&["CLIENT", "KILL", "TYPE", "pubsub"]).await;
}

#[tokio::test]
async fn topic_subscriber_keeps_receiving_after_the_pubsub_connection_is_killed() {
    let client = client().await;
    let topic = client.topic::<String>(unique("topic"));
    let mut subscriber = topic.subscribe().await.unwrap();

    kill_pubsub_connections().await;

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        topic.publish("ping").await.unwrap();
        if let Ok(message) = timeout(Duration::from_millis(300), subscriber.recv()).await {
            assert_eq!(message.unwrap(), "ping");
            break;
        }
        assert!(Instant::now() < deadline, "subscription was never restored");
    }
}

#[tokio::test]
async fn local_cached_map_sees_remote_changes_after_the_pubsub_connection_is_killed() {
    let reader_client = client().await;
    let writer_client = client().await;
    let name = unique("lcm");
    let reader = reader_client
        .local_cached_map::<String, String>(name.clone())
        .build()
        .await
        .unwrap();
    let writer = writer_client
        .local_cached_map::<String, String>(name)
        .build()
        .await
        .unwrap();
    writer.insert("key", "v1").await.unwrap();
    assert_eq!(reader.get("key").await.unwrap().as_deref(), Some("v1"));

    kill_pubsub_connections().await;
    sleep(Duration::from_secs(2)).await;
    assert_eq!(reader.get("key").await.unwrap().as_deref(), Some("v1"));

    writer.insert("key", "v2").await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while reader.get("key").await.unwrap().as_deref() != Some("v2") {
        assert!(
            Instant::now() < deadline,
            "reader kept serving a stale value from its local cache"
        );
        sleep(Duration::from_millis(100)).await;
    }
}
