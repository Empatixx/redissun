// Ported from Redisson's RedissonReliableTopicTest. Redisson's listeners are callbacks; here a test reads each subscriber.
mod common;

use common::{client, raw_command, unique};
use futures::StreamExt;
use redissun::{Client, JsonCodec, Object, ReliableSubscriber};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, timeout, Instant};

const WAIT: Duration = Duration::from_secs(5);

async fn next<M: serde::de::DeserializeOwned + Send>(
    subscriber: &mut ReliableSubscriber<M, JsonCodec>,
) -> M {
    timeout(WAIT, subscriber.recv())
        .await
        .expect("no message")
        .unwrap()
}

async fn nothing_more<M: serde::de::DeserializeOwned + Send + std::fmt::Debug>(
    subscriber: &mut ReliableSubscriber<M, JsonCodec>,
) {
    let more = timeout(Duration::from_millis(200), subscriber.recv()).await;
    assert!(more.is_err(), "got {more:?}");
}

async fn eventually_size<M, C: redissun::Codec>(
    topic: &redissun::ReliableTopic<M, C>,
    expected: u64,
) {
    let deadline = Instant::now() + WAIT;
    while topic.size().await.unwrap() != expected {
        assert!(
            Instant::now() < deadline,
            "the stream never had {expected} messages"
        );
        sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn remove_all_listeners_without_a_subscriber_does_nothing() {
    let topic = client()
        .await
        .reliable_topic::<String>(unique("testRemoveAllFresh"));
    topic.remove_all_listeners().await.unwrap();
    assert_eq!(topic.subscriber_count().await.unwrap(), 0);
}

#[tokio::test]
async fn every_message_published_concurrently_arrives() {
    let client = client().await;
    let name = unique("test1");
    let topic = client.reliable_topic::<i32>(&*name);
    let publishers: Vec<_> = (0..8)
        .map(|task| {
            let topic = client.reliable_topic::<i32>(&*name);
            tokio::spawn(async move {
                for i in 0..63 {
                    topic.publish(&(task * 100 + i)).await.unwrap();
                    sleep(Duration::from_millis(i as u64 % 3)).await;
                }
            })
        })
        .collect();
    let mut subscriber = topic.subscribe().await.unwrap();
    for publisher in publishers {
        publisher.await.unwrap();
    }
    let mut received = std::collections::HashSet::new();
    while received.len() < 8 * 63 {
        received.insert(next(&mut subscriber).await);
    }
    nothing_more(&mut subscriber).await;
    topic.remove_all_listeners().await.unwrap();
}

#[tokio::test]
async fn an_expired_subscriber_is_removed_with_its_unread_messages() {
    // Redisson's testRemoveExpiredSubscribers. A client that crashed leaves its group and its entry in `{name}:timeout`; here they are written by hand, with an expiry one second ahead.
    let client = client().await;
    let name = unique("test1");
    let topic = client.reliable_topic::<i32>(&*name);
    let mut subscriber = topic.subscribe().await.unwrap();
    let expires = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        + 1000;
    raw_command(&["XGROUP", "CREATE", &name, "crashed", "0", "MKSTREAM"]).await;
    raw_command(&[
        "ZADD",
        &format!("{{{name}}}:timeout"),
        &expires.to_string(),
        "crashed",
    ])
    .await;
    assert_eq!(topic.subscriber_count().await.unwrap(), 2);
    sleep(Duration::from_millis(1500)).await;
    for i in 0..10 {
        // Redisson trims the stream up to the millisecond of the last message read, so a message in the same millisecond as the next one stays; a pause gives each message its own.
        topic.publish(&i).await.unwrap();
        sleep(Duration::from_millis(2)).await;
    }
    for i in 0..10 {
        assert_eq!(next(&mut subscriber).await, i);
    }
    let deadline = Instant::now() + WAIT;
    while topic.subscriber_count().await.unwrap() != 1 {
        assert!(Instant::now() < deadline);
        sleep(Duration::from_millis(20)).await;
    }
    eventually_size(&topic, 0).await;
}

#[tokio::test]
async fn a_subscriber_keeps_renewing_itself() {
    let client = common::connect_with(|builder| {
        builder.reliable_topic_watchdog_timeout(Duration::from_millis(600))
    })
    .await;
    let name = unique("renewed");
    let topic = client.reliable_topic::<i32>(&*name);
    let mut subscriber = topic.subscribe().await.unwrap();
    sleep(Duration::from_millis(1500)).await;
    // Another subscriber's poll removes expired ones; this one renewed itself, so it stays.
    let other = client.reliable_topic::<i32>(&*name);
    let mut other_subscriber = other.subscribe().await.unwrap();
    other.publish(&1).await.unwrap();
    assert_eq!(next(&mut other_subscriber).await, 1);
    assert_eq!(next(&mut subscriber).await, 1);
    assert_eq!(topic.subscriber_count().await.unwrap(), 2);
}

#[tokio::test]
async fn messages_are_trimmed_once_every_subscriber_read_them() {
    // Redisson's testAutoTrim.
    let client = client().await;
    let name = unique("test1");
    let (first, second) = (
        client.reliable_topic::<i32>(&*name),
        client.reliable_topic::<i32>(&*name),
    );
    let mut a = first.subscribe().await.unwrap();
    let mut b = second.subscribe().await.unwrap();
    for i in 0..10 {
        // Redisson trims the stream up to the millisecond of the last message read, so a message in the same millisecond as the next one stays; a pause gives each message its own.
        assert_eq!(first.publish(&i).await.unwrap(), 2);
        sleep(Duration::from_millis(2)).await;
    }
    for i in 0..10 {
        assert_eq!(next(&mut a).await, i);
        assert_eq!(next(&mut b).await, i);
    }
    eventually_size(&first, 0).await;
}

#[tokio::test]
async fn a_new_subscriber_reads_the_messages_still_in_the_stream() {
    // Redisson's testListenerOldMessages.
    let topic = client().await.reliable_topic::<String>(unique("test2"));
    topic.publish("1").await.unwrap();
    let mut subscriber = topic.subscribe().await.unwrap();
    assert_eq!(next(&mut subscriber).await, "1");
    topic.publish("2").await.unwrap();
    assert_eq!(next(&mut subscriber).await, "2");
    drop(subscriber);
    eventually_size(&topic, 0).await;
    let mut again = topic.subscribe().await.unwrap();
    nothing_more(&mut again).await;
}

#[tokio::test]
async fn a_subscriber_that_comes_back_reads_what_was_published_meanwhile() {
    // Redisson's testReattach and testListener.
    let topic = client().await.reliable_topic::<String>(unique("test2"));
    let mut subscriber = topic.subscribe().await.unwrap();
    assert_eq!(topic.listener_count(), 1);
    topic.publish("1").await.unwrap();
    assert_eq!(next(&mut subscriber).await, "1");
    drop(subscriber);
    assert_eq!(topic.listener_count(), 0);
    let deadline = Instant::now() + WAIT;
    while topic.subscriber_count().await.unwrap() != 0 {
        assert!(Instant::now() < deadline);
        sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(topic.publish("2").await.unwrap(), 0);
    let mut again = topic.subscribe().await.unwrap();
    assert_eq!(topic.publish("3").await.unwrap(), 1);
    assert_eq!(next(&mut again).await, "2");
    assert_eq!(next(&mut again).await, "3");
}

#[tokio::test]
async fn each_handle_is_a_subscriber_and_its_listeners_share_it() {
    // Redisson's test.
    let client = client().await;
    let name = unique("test3");
    let topic = client.reliable_topic::<String>(&*name);
    let mut first = topic.subscribe().await.unwrap();
    let mut first_again = topic.clone().subscribe().await.unwrap();
    assert_eq!(topic.listener_count(), 2);
    assert_eq!(topic.publish("m1").await.unwrap(), 1);
    assert_eq!(next(&mut first).await, "m1");
    assert_eq!(next(&mut first_again).await, "m1");
    eventually_size(&topic, 0).await;
    let other = client.reliable_topic::<String>(&*name);
    let mut second = other.subscribe().await.unwrap();
    assert_eq!(topic.publish("m2").await.unwrap(), 2);
    assert_eq!(next(&mut first).await, "m2");
    assert_eq!(next(&mut second).await, "m2");
    nothing_more(&mut second).await;
    eventually_size(&topic, 0).await;
}

#[tokio::test]
async fn remove_all_listeners_ends_the_subscribers() {
    let topic = client().await.reliable_topic::<String>(unique("ended"));
    let subscriber = topic.subscribe().await.unwrap();
    topic.remove_all_listeners().await.unwrap();
    assert_eq!(topic.subscriber_count().await.unwrap(), 0);
    let rest: Vec<_> = timeout(WAIT, subscriber.into_stream().collect::<Vec<_>>())
        .await
        .unwrap();
    assert!(rest.is_empty());
}

#[tokio::test]
async fn a_message_published_while_the_subscriber_reads_elsewhere_is_not_lost() {
    // The subscriber waits in a blocking read on a connection of its own; a message from another client wakes it.
    let topic = client().await.reliable_topic::<String>(unique("blocking"));
    let mut subscriber = topic.subscribe().await.unwrap();
    sleep(Duration::from_millis(300)).await;
    let other = Client::builder()
        .url(common::redis_url().await)
        .build()
        .await
        .unwrap()
        .reliable_topic::<String>(topic.name());
    other.publish("late").await.unwrap();
    assert_eq!(next(&mut subscriber).await, "late");
}

#[tokio::test]
async fn delete_and_expire_reach_the_subscribers_too() {
    let client = client().await;
    let name = unique("deleted");
    let topic = client.reliable_topic::<String>(&*name);
    let mut subscriber = topic.subscribe().await.unwrap();
    topic.publish("1").await.unwrap();
    assert_eq!(next(&mut subscriber).await, "1");
    assert!(topic.expire(Duration::from_secs(60)).await.unwrap());
    assert!(client
        .keys()
        .ttl(&format!("{{{name}}}:timeout"))
        .await
        .unwrap()
        .is_some());
    assert!(topic.del().await.unwrap());
    let names = [name.clone(), format!("{{{name}}}:timeout")];
    assert_eq!(client.keys().count_exists(&names).await.unwrap(), 0);
}
