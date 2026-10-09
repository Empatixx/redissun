mod common;

use common::{connect_with, raw_command, unique};
use std::time::Duration;

async fn connections(name: &str) -> usize {
    let list = raw_command(&["CLIENT", "LIST"]).await;
    let needle = format!(" name={name} ");
    list.lines().filter(|line| line.contains(&needle)).count()
}

#[tokio::test]
async fn the_subscriber_connection_opens_on_first_use() {
    let name = unique("lazy").replace(':', "-");
    let client = connect_with(|builder| builder.pool_size(2).client_name(name.clone())).await;
    assert_eq!(connections(&name).await, 2);

    let bucket = client.bucket::<String>(unique("bucket"));
    bucket.set("value").await.unwrap();
    let lock = client.lock(unique("lock"));
    let guard = lock.lock().await.unwrap();
    guard.unlock().await.unwrap();
    assert_eq!(connections(&name).await, 2);

    let guard = lock.lock().await.unwrap();
    let waiter = {
        let lock = lock.clone();
        tokio::spawn(async move { lock.lock().await.unwrap().unlock().await.unwrap() })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(connections(&name).await, 3);
    guard.unlock().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(connections(&name).await, 3);
}

#[tokio::test]
async fn a_topic_subscriber_opens_the_subscriber_connection() {
    let name = unique("lazy").replace(':', "-");
    let client = connect_with(|builder| builder.pool_size(1).client_name(name.clone())).await;
    let topic = client.topic::<String>(unique("topic"));
    assert_eq!(topic.listener_count().await, 0);
    topic.remove_all_listeners().await;
    assert_eq!(connections(&name).await, 1);

    let mut subscriber = topic.subscribe().await.unwrap();
    assert_eq!(connections(&name).await, 2);
    topic.publish("hello").await.unwrap();
    assert_eq!(subscriber.recv().await.unwrap(), "hello");
}
