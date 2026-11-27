// Ported from Redisson's RedissonShardedTopicTest; the cluster tests are in tests/cluster.rs.
mod common;

use common::{client, unique};
use redissun::{Client, Error};
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers::ImageExt;
use testcontainers_modules::redis::Redis;
use tokio::time::{sleep, timeout, Instant};

const WAIT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn a_server_without_shard_channels_rejects_the_subscriber() {
    let container = Redis::default()
        .with_tag("6.2-alpine")
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(6379).await.unwrap();
    let client = Client::builder()
        .url(format!("redis://127.0.0.1:{port}"))
        .build()
        .await
        .unwrap();
    let error = client
        .sharded_topic::<String>("ttt")
        .subscribe()
        .await
        .unwrap_err();
    assert!(
        matches!(&error, Error::Redis(message) if message.to_lowercase().contains("unknown command")),
        "{error:?}"
    );
}

#[tokio::test]
async fn a_subscriber_gets_what_is_published() {
    let topic = client().await.sharded_topic::<String>(unique("orders"));
    let mut subscriber = topic.subscribe().await.unwrap();
    assert_eq!(topic.publish("first").await.unwrap(), 1);
    assert_eq!(
        timeout(WAIT, subscriber.recv()).await.unwrap().unwrap(),
        "first"
    );
}

#[tokio::test]
async fn subscribers_of_one_client_share_a_connection() {
    let topic = client().await.sharded_topic::<u32>(unique("shared"));
    assert_eq!(topic.local_subscriber_count(), 0);
    let mut first = topic.subscribe().await.unwrap();
    let mut second = topic.clone().subscribe().await.unwrap();
    assert_eq!(topic.local_subscriber_count(), 2);
    assert_eq!(topic.subscriber_count().await.unwrap(), 1);
    assert_eq!(topic.publish(&7).await.unwrap(), 1);
    assert_eq!(timeout(WAIT, first.recv()).await.unwrap().unwrap(), 7);
    assert_eq!(timeout(WAIT, second.recv()).await.unwrap().unwrap(), 7);
}

async fn eventually_no_subscriber(topic: &redissun::ShardedTopic<String, redissun::JsonCodec>) {
    let deadline = Instant::now() + WAIT;
    while topic.subscriber_count().await.unwrap() != 0 {
        assert!(Instant::now() < deadline, "the channel kept its subscriber");
        sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn dropping_the_last_subscriber_unsubscribes() {
    let topic = client().await.sharded_topic::<String>(unique("dropped"));
    let first = topic.subscribe().await.unwrap();
    let second = topic.subscribe().await.unwrap();
    drop(first);
    assert_eq!(topic.local_subscriber_count(), 1);
    assert_eq!(topic.subscriber_count().await.unwrap(), 1);
    drop(second);
    assert_eq!(topic.local_subscriber_count(), 0);
    eventually_no_subscriber(&topic).await;
    assert_eq!(topic.publish("nobody").await.unwrap(), 0);
}

#[tokio::test]
async fn unsubscribe_all_ends_the_subscribers() {
    let topic = client().await.sharded_topic::<String>(unique("removed"));
    let mut subscriber = topic.subscribe().await.unwrap();
    topic.publish("before").await.unwrap();
    sleep(Duration::from_millis(100)).await;
    topic.unsubscribe_all();
    assert_eq!(topic.local_subscriber_count(), 0);
    assert_eq!(
        timeout(WAIT, subscriber.recv()).await.unwrap().unwrap(),
        "before"
    );
    assert!(timeout(WAIT, subscriber.recv()).await.unwrap().is_err());
    eventually_no_subscriber(&topic).await;
    // A new subscriber listens again.
    let mut again = topic.subscribe().await.unwrap();
    topic.publish("after").await.unwrap();
    assert_eq!(timeout(WAIT, again.recv()).await.unwrap().unwrap(), "after");
}

#[tokio::test]
async fn a_plain_topic_of_the_same_name_does_not_hear_it() {
    let client = client().await;
    let name = unique("separate");
    let mut plain = client.topic::<String>(&*name).subscribe().await.unwrap();
    let sharded = client.sharded_topic::<String>(&*name);
    let mut subscriber = sharded.subscribe().await.unwrap();
    sharded.publish("shard").await.unwrap();
    assert_eq!(
        timeout(WAIT, subscriber.recv()).await.unwrap().unwrap(),
        "shard"
    );
    assert!(timeout(Duration::from_millis(200), plain.recv())
        .await
        .is_err());
}
