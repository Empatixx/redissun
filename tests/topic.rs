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

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct Message {
    name: String,
}

fn message(name: &str) -> Message {
    Message { name: name.into() }
}

async fn nothing_arrives<M: serde::de::DeserializeOwned + Send + std::fmt::Debug>(
    subscriber: &mut redissun::Subscriber<M, redissun::JsonCodec>,
    wait: Duration,
) {
    if let Ok(received) = timeout(wait, subscriber.recv()).await {
        panic!("expected no message, got {received:?}");
    }
}

async fn eventually_subscribers(topic: &redissun::Topic<i64, redissun::JsonCodec>, count: usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while topic.subscriber_count().await.unwrap() != count {
        assert!(
            Instant::now() < deadline,
            "subscriber count never became {count}"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn count_subscribers() {
    let topic = client().await.topic::<i64>(unique("topic"));
    assert_eq!(topic.subscriber_count().await.unwrap(), 0);
    let subscriber = topic.subscribe().await.unwrap();
    assert_eq!(topic.subscriber_count().await.unwrap(), 1);
    drop(subscriber);
    eventually_subscribers(&topic, 0).await;
}

#[tokio::test]
async fn count_listeners() {
    let client = client().await;
    let first = client.topic::<i64>(unique("topic"));
    assert_eq!(first.listener_count().await, 0);
    let a = first.subscribe().await.unwrap();
    assert_eq!(first.listener_count().await, 1);

    let second = client.topic::<i64>(unique("topic"));
    assert_eq!(second.listener_count().await, 0);
    let b = second.subscribe().await.unwrap();
    assert_eq!(second.listener_count().await, 1);

    drop(a);
    assert_eq!(first.listener_count().await, 0);
    drop(b);
    assert_eq!(second.listener_count().await, 0);
}

#[tokio::test]
async fn ping() {
    let topic = client().await.topic::<String>(unique("topic"));
    let mut subscriber = topic.subscribe().await.unwrap();
    let count = 300;
    let receiving = tokio::spawn(async move {
        for _ in 0..count {
            subscriber.recv().await.unwrap();
        }
    });
    for _ in 0..count {
        topic.publish(&unique("message")).await.unwrap();
        sleep(Duration::from_millis(1)).await;
    }
    timeout(Duration::from_secs(60), receiving)
        .await
        .expect("not every message arrived")
        .unwrap();
}

#[tokio::test]
async fn concurrent_topic() {
    let client = client().await;
    let prefix = unique("PUBSUB");
    let mut workers = Vec::new();
    for _ in 0..16 {
        let client = client.clone();
        let prefix = prefix.clone();
        workers.push(tokio::spawn(async move {
            for j in 0..100 {
                let topic = client.topic::<String>(format!("{prefix}_{j}"));
                let subscriber = topic.subscribe().await.unwrap();
                topic.publish("message").await.unwrap();
                drop(subscriber);
            }
        }));
    }
    for worker in workers {
        timeout(Duration::from_secs(120), worker)
            .await
            .expect("workers did not finish")
            .unwrap();
    }
}

#[tokio::test]
async fn commands_ordering() {
    let topic = client().await.topic::<i64>(unique("topic"));
    let mut subscriber = topic.subscribe().await.unwrap();
    topic.publish(&123).await.unwrap();
    let received = timeout(Duration::from_secs(1), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received, 123);
    topic.remove_all_listeners().await;
}

#[tokio::test]
async fn topic_state() {
    let client = client().await;
    let name = unique("test1");
    let topic = client.topic::<String>(name.clone());
    let pattern = client.pattern_topic::<String>(format!("{name}*"));
    for _ in 0..3 {
        let mut plain = topic.subscribe().await.unwrap();
        let mut matching = pattern.subscribe().await.unwrap();
        topic.publish("testmsg").await.unwrap();
        let received = timeout(Duration::from_secs(1), plain.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received, "testmsg");
        let (channel, received) = timeout(Duration::from_secs(1), matching.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(channel, name);
        assert_eq!(received, "testmsg");
    }
}

#[tokio::test]
async fn multi_type_connection() {
    let client = client().await;
    let strings = client.topic::<String>(unique("test1"));
    let mut string_subscriber = strings.subscribe().await.unwrap();
    strings.publish("testmsg").await.unwrap();

    let longs = client.topic::<i64>(unique("test2"));
    let mut long_subscriber = longs.subscribe().await.unwrap();
    longs.publish(&1).await.unwrap();

    let text = timeout(Duration::from_secs(1), string_subscriber.recv()).await;
    assert_eq!(text.unwrap().unwrap(), "testmsg");
    let number = timeout(Duration::from_secs(1), long_subscriber.recv()).await;
    assert_eq!(number.unwrap().unwrap(), 1);
    strings.remove_all_listeners().await;
}

#[tokio::test]
async fn sync_commands() {
    let client = client().await;
    let topic = client.topic::<String>(unique("system_bus"));
    let set = client.hash_set::<String>(unique("set1"));
    let mut subscriber = topic.subscribe().await.unwrap();
    let listener = tokio::spawn(async move {
        subscriber.recv().await.unwrap();
        for j in 0..1000 {
            set.contains(&j.to_string()).await.unwrap();
        }
    });
    topic.publish("sometext").await.unwrap();
    timeout(Duration::from_secs(60), listener)
        .await
        .expect("commands from a listener must not block")
        .unwrap();
    topic.remove_all_listeners().await;
}

#[tokio::test]
async fn lambda_optimization_by_jvm() {
    let topic = client().await.topic::<String>(unique("topic"));
    let mut tasks = Vec::new();
    for _ in 0..50 {
        let topic = topic.clone();
        tasks.push(tokio::spawn(async move {
            let subscriber = topic.subscribe().await.unwrap();
            drop(subscriber);
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(topic.listener_count().await, 0);
}

#[tokio::test]
async fn inner_publish() {
    let first_client = client().await;
    let second_client = client().await;
    let topic1 = first_client.topic::<Message>(unique("topic1"));
    let topic2 = second_client.topic::<Message>(unique("topic2"));
    let mut first = topic1.subscribe().await.unwrap();
    let mut second = topic2.subscribe().await.unwrap();

    let republisher = {
        let topic1 = topic1.clone();
        let topic2 = topic2.clone();
        tokio::spawn(async move {
            let mut received = 0;
            while received < 2 {
                let incoming = second.recv().await.unwrap();
                received += 1;
                let expected = message("test");
                if incoming != expected {
                    topic1.publish(&expected).await.unwrap();
                    topic2.publish(&expected).await.unwrap();
                }
            }
        })
    };
    topic2.publish(&message("123")).await.unwrap();

    let received = timeout(Duration::from_secs(5), first.recv()).await;
    assert_eq!(received.unwrap().unwrap(), message("test"));
    timeout(Duration::from_secs(5), republisher)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn unsubscribe() {
    let client = client().await;
    let name = unique("topic1");
    let topic = client.topic::<Message>(name.clone());
    let removed = topic.subscribe().await.unwrap();
    let mut kept = topic.subscribe().await.unwrap();
    drop(removed);

    let topic = client.topic::<Message>(name);
    topic.publish(&message("123")).await.unwrap();
    let received = timeout(Duration::from_secs(5), kept.recv()).await;
    assert_eq!(received.unwrap().unwrap(), message("123"));
    topic.remove_all_listeners().await;
}

#[tokio::test]
async fn remove_all_listeners() {
    let client = client().await;
    let name = unique("topic1");
    let topic = client.topic::<Message>(name.clone());
    let mut subscribers = Vec::new();
    for _ in 0..10 {
        subscribers.push(topic.subscribe().await.unwrap());
    }

    let topic = client.topic::<Message>(name.clone());
    topic.remove_all_listeners().await;
    assert_eq!(topic.listener_count().await, 0);
    eventually_unsubscribed(&name).await;
    assert_eq!(topic.publish(&message("123")).await.unwrap(), 0);
    for subscriber in &mut subscribers {
        let ended = timeout(Duration::from_secs(1), subscriber.recv()).await;
        assert!(matches!(ended, Ok(Err(Error::Redis(_)))), "{ended:?}");
    }
}

#[tokio::test]
async fn subscribe_limit() {
    let client = client().await;
    let prefix = unique("limit");
    let mut subscribers = std::collections::VecDeque::new();
    for i in 0..1000 {
        let topic = client.topic::<String>(format!("{prefix}:{i}"));
        subscribers.push_back(topic.subscribe().await.unwrap());
        if subscribers.len() > 50 {
            subscribers.pop_front();
        }
    }
    drop(subscribers);
    let deadline = Instant::now() + Duration::from_secs(10);
    while subscribed_channels(&format!("{prefix}:*")).await > 0 {
        assert!(Instant::now() < deadline, "channels were not released");
        sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn remove_all_listeners2() {
    let client = client().await;
    let name = unique("topic1");
    let mut subscribers = Vec::new();
    for _ in 0..100 {
        let topic = client.topic::<Message>(name.clone());
        for _ in 0..10 {
            subscribers.push(topic.subscribe().await.unwrap());
        }
        let topic = client.topic::<Message>(name.clone());
        topic.remove_all_listeners().await;
        topic.publish(&message("123")).await.unwrap();
    }
    sleep(Duration::from_millis(500)).await;
    for subscriber in &mut subscribers {
        let ended = timeout(Duration::from_secs(1), subscriber.recv()).await;
        assert!(matches!(ended, Ok(Err(Error::Redis(_)))), "{ended:?}");
    }
}

#[tokio::test]
async fn remove_by_instance() {
    let client = client().await;
    let name = unique("topic1");
    let topic = client.topic::<Message>(name.clone());
    let listener = topic.subscribe().await.unwrap();
    drop(listener);

    let topic = client.topic::<Message>(name.clone());
    eventually_unsubscribed(&name).await;
    assert_eq!(topic.publish(&message("123")).await.unwrap(), 0);
}

#[tokio::test]
async fn lazy_unsubscribe() {
    let name = unique("topic");
    let first = client().await.topic::<Message>(name.clone());
    let removed = first.subscribe().await.unwrap();
    sleep(Duration::from_millis(300)).await;
    drop(removed);
    sleep(Duration::from_millis(300)).await;

    let second = client().await.topic::<Message>(name);
    let mut subscriber = second.subscribe().await.unwrap();
    assert_eq!(second.publish(&message("123")).await.unwrap(), 1);
    let received = timeout(Duration::from_secs(5), subscriber.recv()).await;
    assert_eq!(received.unwrap().unwrap(), message("123"));
    nothing_arrives(&mut subscriber, Duration::from_millis(300)).await;
}

#[tokio::test]
async fn test() {
    let name = unique("topic");
    let topic1 = client().await.topic::<Message>(name.clone());
    let mut first = topic1.subscribe().await.unwrap();
    let topic2 = client().await.topic::<Message>(name);
    let mut second = topic2.subscribe().await.unwrap();
    topic2.publish(&message("123")).await.unwrap();
    assert_eq!(first.recv().await.unwrap(), message("123"));
    assert_eq!(second.recv().await.unwrap(), message("123"));
    topic1.remove_all_listeners().await;
}

#[tokio::test]
async fn heavy_load() {
    let name = unique("topic");
    let topic1 = client().await.topic::<Message>(name.clone());
    let mut first = topic1.subscribe().await.unwrap();
    let topic2 = client().await.topic::<Message>(name);
    let mut second = topic2.subscribe().await.unwrap();
    let count = 10_000;

    let counting = tokio::spawn(async move {
        let mut received = 0;
        while received < count {
            assert_eq!(first.recv().await.unwrap(), message("123"));
            received += 1;
        }
        first
    });
    let draining = tokio::spawn(async move {
        for _ in 0..count {
            assert_eq!(second.recv().await.unwrap(), message("123"));
        }
    });
    for _ in 0..count {
        topic2.publish(&message("123")).await.unwrap();
    }
    let mut first = timeout(Duration::from_secs(60), counting)
        .await
        .expect("not every message arrived")
        .unwrap();
    timeout(Duration::from_secs(60), draining)
        .await
        .unwrap()
        .unwrap();
    nothing_arrives(&mut first, Duration::from_millis(500)).await;
    topic1.remove_all_listeners().await;
}

#[tokio::test]
async fn listener_remove() {
    let name = unique("topic");
    let topic1 = client().await.topic::<Message>(name.clone());
    let listener = topic1.subscribe().await.unwrap();
    let topic2 = client().await.topic::<Message>(name.clone());
    drop(listener);
    eventually_unsubscribed(&name).await;
    assert_eq!(topic2.publish(&message("123")).await.unwrap(), 0);
}

#[tokio::test]
async fn a_pattern_subscriber_gets_messages_from_every_matching_channel() {
    let client = client().await;
    let prefix = unique("news");
    let pattern = client.pattern_topic::<String>(format!("{prefix}.*"));
    assert_eq!(pattern.pattern(), format!("{prefix}.*"));
    let mut subscriber = pattern.subscribe().await.unwrap();
    client
        .topic::<String>(format!("{prefix}.art"))
        .publish("a")
        .await
        .unwrap();
    client
        .topic::<String>(format!("{prefix}.sport"))
        .publish("b")
        .await
        .unwrap();
    client
        .topic::<String>(format!("{prefix}x"))
        .publish("ignored")
        .await
        .unwrap();
    assert_eq!(
        subscriber.recv().await.unwrap(),
        (format!("{prefix}.art"), "a".to_string())
    );
    assert_eq!(
        subscriber.recv().await.unwrap(),
        (format!("{prefix}.sport"), "b".to_string())
    );
    let extra = timeout(Duration::from_millis(300), subscriber.recv()).await;
    assert!(extra.is_err(), "{extra:?}");
}

#[tokio::test]
async fn overlapping_patterns_each_get_a_message_once() {
    let client = client().await;
    let prefix = unique("overlap");
    let channel = format!("{prefix}.abc");
    let mut wide = client
        .pattern_topic::<u32>(format!("{prefix}.*"))
        .subscribe()
        .await
        .unwrap();
    let mut narrow = client
        .pattern_topic::<u32>(format!("{prefix}.a?c"))
        .subscribe()
        .await
        .unwrap();
    let mut plain = client
        .topic::<u32>(channel.clone())
        .subscribe()
        .await
        .unwrap();
    let topic = client.topic::<u32>(channel.clone());
    for i in 0..5u32 {
        assert_eq!(topic.publish(&i).await.unwrap(), 3);
    }
    for i in 0..5u32 {
        assert_eq!(wide.recv().await.unwrap(), (channel.clone(), i));
        assert_eq!(narrow.recv().await.unwrap(), (channel.clone(), i));
        assert_eq!(plain.recv().await.unwrap(), i);
    }
    for extra in [
        timeout(Duration::from_millis(300), wide.recv())
            .await
            .is_ok(),
        timeout(Duration::from_millis(300), narrow.recv())
            .await
            .is_ok(),
    ] {
        assert!(!extra, "a pattern subscriber got a duplicate");
    }
}

#[tokio::test]
async fn a_pattern_is_released_after_the_last_subscriber_leaves() {
    let client = client().await;
    let prefix = unique("released");
    let pattern = client.pattern_topic::<String>(format!("{prefix}.*"));
    let first = pattern.subscribe().await.unwrap();
    let second = pattern.subscribe().await.unwrap();
    assert_eq!(pattern.listener_count().await, 2);
    let publisher = client.topic::<String>(format!("{prefix}.x"));
    assert_eq!(publisher.publish("a").await.unwrap(), 1);
    drop(first);
    drop(second);
    let deadline = Instant::now() + Duration::from_secs(5);
    while publisher.publish("b").await.unwrap() != 0 {
        assert!(Instant::now() < deadline, "pattern was not released");
        sleep(Duration::from_millis(50)).await;
    }
}
