mod common;

use common::{client, raw_command, subscribed_channels, unique};
use futures::StreamExt;
use redissun::Error;
use std::time::Duration;
use tokio::time::{sleep, timeout, Instant};

async fn eventually_unsubscribed(name: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while subscribed_channels(name).await > 0 {
        assert!(Instant::now() < deadline, "channel {name} was not released");
        sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn a_message_reaches_the_subscriber() {
    let topic = client().await.topic::<String>(unique("topic"));
    let mut subscriber = topic.subscribe().await.unwrap();
    assert_eq!(topic.publish("hello").await.unwrap(), 1);
    assert_eq!(subscriber.recv().await.unwrap(), "hello");
}

#[tokio::test]
async fn every_subscriber_gets_every_message() {
    let name = unique("topic");
    let first = client().await.topic::<String>(name.clone());
    let second = client().await.topic::<String>(name);
    let mut a = first.subscribe().await.unwrap();
    let mut b = second.subscribe().await.unwrap();
    let mut c = second.subscribe().await.unwrap();
    assert_eq!(first.publish("x").await.unwrap(), 2);
    for subscriber in [&mut a, &mut b, &mut c] {
        assert_eq!(subscriber.recv().await.unwrap(), "x");
    }
}

#[tokio::test]
async fn a_late_subscriber_does_not_see_old_messages() {
    let topic = client().await.topic::<String>(unique("topic"));
    topic.publish("old").await.unwrap();
    let mut subscriber = topic.subscribe().await.unwrap();
    topic.publish("new").await.unwrap();
    assert_eq!(subscriber.recv().await.unwrap(), "new");
}

#[tokio::test]
async fn messages_arrive_in_order() {
    let topic = client().await.topic::<u32>(unique("topic"));
    let mut subscriber = topic.subscribe().await.unwrap();
    for i in 0..50u32 {
        topic.publish(&i).await.unwrap();
    }
    for i in 0..50u32 {
        assert_eq!(subscriber.recv().await.unwrap(), i);
    }
}

#[tokio::test]
async fn a_slow_subscriber_is_told_how_many_messages_it_missed() {
    let topic = client().await.topic::<u32>(unique("topic"));
    let mut subscriber = topic.subscribe().await.unwrap();
    for i in 0..400u32 {
        topic.publish(&i).await.unwrap();
    }
    sleep(Duration::from_millis(500)).await;
    let missed = match subscriber.recv().await {
        Err(Error::Lagged(missed)) => missed,
        other => panic!("expected Lagged, got {other:?}"),
    };
    assert!(missed > 0);
    let mut last = 0;
    while let Ok(Ok(value)) = timeout(Duration::from_millis(300), subscriber.recv()).await {
        last = value;
    }
    assert_eq!(last, 399);
}

#[tokio::test]
async fn the_channel_is_released_after_the_last_subscriber_leaves() {
    let name = unique("topic");
    let topic = client().await.topic::<String>(name.clone());
    let first = topic.subscribe().await.unwrap();
    let second = topic.subscribe().await.unwrap();
    assert_eq!(subscribed_channels(&name).await, 1);
    drop(first);
    sleep(Duration::from_millis(200)).await;
    assert_eq!(subscribed_channels(&name).await, 1);
    drop(second);
    eventually_unsubscribed(&name).await;
}

#[tokio::test]
async fn a_plain_redis_publish_is_received() {
    let name = unique("topic");
    let topic = client().await.topic::<i64>(name.clone());
    let mut subscriber = topic.subscribe().await.unwrap();
    raw_command(&["PUBLISH", &name, "42"]).await;
    assert_eq!(subscriber.recv().await.unwrap(), 42);
}

#[tokio::test]
async fn a_message_that_does_not_decode_is_a_codec_error_and_the_next_one_works() {
    let name = unique("topic");
    let topic = client().await.topic::<i64>(name.clone());
    let mut subscriber = topic.subscribe().await.unwrap();
    raw_command(&["PUBLISH", &name, "not-a-number"]).await;
    topic.publish(&7).await.unwrap();
    assert!(matches!(subscriber.recv().await, Err(Error::Codec(_))));
    assert_eq!(subscriber.recv().await.unwrap(), 7);
}

#[tokio::test]
async fn subscriber_count_counts_clients() {
    let name = unique("topic");
    let first = client().await.topic::<String>(name.clone());
    let second = client().await.topic::<String>(name);
    assert_eq!(first.subscriber_count().await.unwrap(), 0);
    let _a = first.subscribe().await.unwrap();
    let _b = first.subscribe().await.unwrap();
    let _c = second.subscribe().await.unwrap();
    assert_eq!(first.subscriber_count().await.unwrap(), 2);
}

#[tokio::test]
async fn into_stream_yields_messages() {
    let topic = client().await.topic::<u32>(unique("topic"));
    let subscriber = topic.subscribe().await.unwrap();
    for i in 0..3u32 {
        topic.publish(&i).await.unwrap();
    }
    let received: Vec<u32> = subscriber
        .into_stream()
        .take(3)
        .map(|message| message.unwrap())
        .collect()
        .await;
    assert_eq!(received, [0, 1, 2]);
}

#[tokio::test]
async fn debug_does_not_print_connection_details() {
    let name = unique("topic");
    let topic = client().await.topic::<String>(name.clone());
    assert!(format!("{topic:?}").contains(&name));
}

#[tokio::test]
async fn a_message_published_right_after_subscribe_is_never_missed() {
    let first = client().await;
    let second = client().await;
    for _ in 0..100 {
        let name = unique("topic");
        let a = first.topic::<String>(name.clone());
        let b = second.topic::<String>(name);
        let mut x = a.subscribe().await.unwrap();
        let mut y = b.subscribe().await.unwrap();
        assert_eq!(a.publish("now").await.unwrap(), 2);
        assert_eq!(x.recv().await.unwrap(), "now");
        assert_eq!(y.recv().await.unwrap(), "now");
    }
}
