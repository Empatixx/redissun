mod common;

use common::{connect_with, raw_command, unique};
use std::time::Duration;

async fn connections() -> usize {
    let list = raw_command(&["CLIENT", "LIST"]).await;
    list.lines().filter(|line| line.contains("id=")).count() - 1
}

#[tokio::test]
async fn the_subscriber_connection_opens_on_first_use() {
    let before = connections().await;
    let client = connect_with(|builder| builder.pool_size(2)).await;
    assert_eq!(connections().await, before + 2);

    let bucket = client.bucket::<String>(unique("bucket"));
    bucket.set("value").await.unwrap();
    let lock = client.lock(unique("lock"));
    let guard = lock.lock().await.unwrap();
    guard.unlock().await.unwrap();
    assert_eq!(connections().await, before + 2);

    let guard = lock.lock().await.unwrap();
    let waiter = {
        let lock = lock.clone();
        tokio::spawn(async move { lock.lock().await.unwrap().unlock().await.unwrap() })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(connections().await, before + 3);
    guard.unlock().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(connections().await, before + 3);
}
