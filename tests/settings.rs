mod common;

use common::{connect_with, raw_command, unique};
use redissun::DelayStrategy;
use std::time::Duration;

async fn named(name: &str) -> Vec<String> {
    let list = raw_command(&["CLIENT", "LIST"]).await;
    let needle = format!(" name={name} ");
    list.lines()
        .filter(|line| line.contains(&needle))
        .filter_map(|line| line.split_whitespace().next().map(str::to_string))
        .collect()
}

fn client_name() -> String {
    unique("settings").replace(':', "-")
}

#[tokio::test]
async fn every_pooled_connection_gets_the_client_name() {
    let name = client_name();
    let _client = connect_with(|builder| builder.pool_size(3).client_name(name.clone())).await;
    assert_eq!(named(&name).await.len(), 3);
}

#[tokio::test]
async fn the_database_setting_selects_the_database() {
    let key = unique("db");
    let one = connect_with(|builder| builder.database(1)).await;
    let zero = connect_with(|builder| builder).await;
    one.bucket::<String>(key.clone())
        .set("in one")
        .await
        .unwrap();
    assert_eq!(
        zero.bucket::<String>(key.clone()).get().await.unwrap(),
        None
    );
    let again = connect_with(|builder| builder.database(1)).await;
    assert_eq!(
        again.bucket::<String>(key).get().await.unwrap().as_deref(),
        Some("in one")
    );
}

#[tokio::test]
async fn blocking_calls_are_not_cut_by_the_timeout() {
    let client = connect_with(|builder| builder.timeout(Duration::from_millis(200))).await;
    let queue = client.vec_deque::<String>(unique("queue"));
    let waiting = {
        let queue = queue.clone();
        tokio::spawn(async move { queue.pop_front_wait().timeout(Duration::from_secs(5)).await })
    };
    tokio::time::sleep(Duration::from_millis(1200)).await;
    queue.push_back("late").await.unwrap();
    let popped = waiting.await.unwrap().unwrap();
    assert_eq!(popped.as_deref(), Some("late"));
}

#[tokio::test]
async fn lock_writes_and_batches_take_the_client_retry_settings() {
    let client = connect_with(|builder| {
        builder
            .retry_attempts(1)
            .retry_delay(DelayStrategy::Constant(Duration::from_millis(10)))
    })
    .await;
    let lock = client.lock(unique("lock"));
    lock.lock().await.unwrap().unlock().await.unwrap();
    let batch = client.batch();
    let value = batch.bucket::<String>(unique("bucket")).get();
    batch.execute().await.unwrap();
    assert_eq!(value.await.unwrap(), None);
}

#[tokio::test]
async fn batch_and_blocking_connections_get_the_client_name_too() {
    let name = client_name();
    let client = connect_with(|builder| builder.pool_size(1).client_name(name.clone())).await;
    let batch = client.batch().atomic();
    let read = batch.bucket::<String>(unique("bucket")).get();
    batch.execute().await.unwrap();
    read.await.unwrap();
    assert_eq!(named(&name).await.len(), 2);

    let queue = client.vec_deque::<String>(unique("queue"));
    let waiting = {
        let queue = queue.clone();
        tokio::spawn(async move { queue.pop_front_wait().timeout(Duration::from_secs(5)).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(named(&name).await.len(), 3);
    queue.push_back("done").await.unwrap();
    waiting.await.unwrap().unwrap();
}

/// Redisson sends `CLIENT SETNAME` on every connection it opens, so a connection opened again after it was killed has the name too.
#[tokio::test]
async fn the_client_name_is_set_again_after_a_reconnect() {
    let name = unique("renamed").replace(':', "-");
    let client = connect_with(|builder| {
        builder
            .pool_size(2)
            .client_name(name.clone())
            .reconnection_delay(DelayStrategy::Constant(Duration::from_millis(50)))
    })
    .await;
    let bucket = client.bucket::<String>(unique("renamed"));
    bucket.set("before").await.unwrap();
    let before = named(&name).await;
    assert_eq!(before.len(), 2);

    for id in &before {
        let killed = raw_command(&["CLIENT", "KILL", "ID", id.trim_start_matches("id=")]).await;
        assert_eq!(killed, ":1\r\n");
    }
    for _ in 0..4 {
        bucket.set("after").await.unwrap();
    }

    let after = named(&name).await;
    assert_eq!(after.len(), 2, "{after:?}");
    assert!(after.iter().all(|id| !before.contains(id)));
}
