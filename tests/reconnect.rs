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
