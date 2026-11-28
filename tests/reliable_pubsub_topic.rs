//! Tests of `ReliablePubSubTopic`, the functionality of Redisson PRO's `RReliablePubSubTopic` on redissun's own backend.

mod common;

use common::{client, unique};
use futures::StreamExt;
use redissun::reliable_pubsub_topic::{
    MessageArgs, Nack, Position, PubSubOperation, Retention, TopicConfig, TopicEvent,
};
use redissun::{Error, JsonCodec, ReliablePubSubTopic};
use std::time::{Duration, SystemTime};
use tokio::time::sleep;

type T = ReliablePubSubTopic<String, JsonCodec>;

async fn topic(name: &str) -> T {
    client().await.reliable_pubsub_topic(name.to_string())
}

#[tokio::test]
async fn every_subscription_gets_every_message() {
    let t = topic(&unique("rt")).await;
    let a = t.create_subscription().name("a").await.unwrap();
    let b = t.create_subscription().name("b").await.unwrap();
    let id = t.publish("hello").await.unwrap().id;
    for sub in [&a, &b] {
        let consumer = sub.pull_consumer().await.unwrap();
        let m = consumer.pull().await.unwrap().unwrap();
        assert_eq!(m.id, id);
        assert_eq!(m.payload, "hello");
        assert!(m.ack().await.unwrap());
    }
    assert_eq!(t.subscriptions().await.unwrap(), ["a", "b"]);
    assert!(t.has_subscription("a").await.unwrap());
    assert!(t.subscription("zz").await.unwrap().is_none());
}

#[tokio::test]
async fn a_subscription_name_is_unique_and_can_be_generated() {
    let t = topic(&unique("rt")).await;
    let generated = t.create_subscription().await.unwrap();
    assert!(!generated.name().is_empty());
    t.create_subscription().name("x").await.unwrap();
    assert!(matches!(
        t.create_subscription().name("x").await,
        Err(Error::Config(_))
    ));
    assert!(t.remove_subscription("x").await.unwrap());
    assert!(!t.has_subscription("x").await.unwrap());
}

#[tokio::test]
async fn retention_modes() {
    let required = topic(&unique("rt")).await;
    assert!(required.publish("lost").await.is_err());

    let optional = topic(&unique("rt")).await;
    optional
        .set_config(&TopicConfig::new().retention(Retention::OptionalRetainAll))
        .await
        .unwrap();
    optional.publish("kept").await.unwrap();
    assert_eq!(optional.len().await.unwrap(), 1);
    let late = optional
        .create_subscription()
        .position(Position::Earliest)
        .await
        .unwrap();
    let m = late
        .pull_consumer()
        .await
        .unwrap()
        .pull()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.payload, "kept");

    let processed = topic(&unique("rt")).await;
    let s = processed.create_subscription().name("s").await.unwrap();
    processed.publish("one").await.unwrap();
    s.pull_consumer()
        .await
        .unwrap()
        .pull()
        .await
        .unwrap()
        .unwrap()
        .ack()
        .await
        .unwrap();
    // The next publish trims the log of messages no subscription holds.
    processed.publish("two").await.unwrap();
    assert_eq!(processed.len().await.unwrap(), 1);
}

#[tokio::test]
async fn positions_and_seek() {
    let t = topic(&unique("rt")).await;
    t.set_config(&TopicConfig::new().retention(Retention::RequiredRetainAll))
        .await
        .unwrap();
    let first = t.create_subscription().name("first").await.unwrap();
    let ids: Vec<String> = t
        .publish_many((0..3).map(|i| MessageArgs::new(format!("m{i}"))))
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.id)
        .collect();
    let latest = t.create_subscription().name("latest").await.unwrap();
    assert_eq!(latest.queue().len().await.unwrap(), 0);
    let from_id = t
        .create_subscription()
        .position(Position::Id {
            id: ids[1].clone(),
            inclusive: true,
        })
        .await
        .unwrap();
    assert_eq!(from_id.queue().len().await.unwrap(), 2);
    let after_id = t
        .create_subscription()
        .position(Position::Id {
            id: ids[1].clone(),
            inclusive: false,
        })
        .await
        .unwrap();
    assert_eq!(after_id.queue().len().await.unwrap(), 1);
    let by_time = t
        .create_subscription()
        .position(Position::Time {
            at: SystemTime::UNIX_EPOCH,
            inclusive: true,
        })
        .await
        .unwrap();
    assert_eq!(by_time.queue().len().await.unwrap(), 3);
    assert_eq!(first.queue().len().await.unwrap(), 3);
    first.seek(Position::Latest).await.unwrap();
    assert_eq!(first.queue().len().await.unwrap(), 0);
    first.seek(Position::Earliest).await.unwrap();
    assert_eq!(first.queue().len().await.unwrap(), 3);
}

#[tokio::test]
async fn nack_redelivers_and_the_limit_dead_letters() {
    let dlq = unique("dlq");
    let t = topic(&unique("rt")).await;
    let s = t
        .create_subscription()
        .delivery_limit(1)
        .dead_letter_queue(&dlq)
        .await
        .unwrap();
    t.publish("x").await.unwrap();
    let consumer = s.pull_consumer().await.unwrap();
    consumer
        .pull()
        .await
        .unwrap()
        .unwrap()
        .nack(Nack::Failed {
            delay: Duration::ZERO,
        })
        .await
        .unwrap();
    let dead: redissun::ReliableQueue<String, JsonCodec> = client().await.reliable_queue(dlq);
    assert_eq!(dead.len().await.unwrap(), 1);
}

#[tokio::test]
async fn consumers_are_registered() {
    let t = topic(&unique("rt")).await;
    let s = t.create_subscription().name("s").await.unwrap();
    s.pull_consumer().name("c1").await.unwrap();
    s.pull_consumer().name("c2").await.unwrap();
    assert_eq!(s.consumer_names().await.unwrap(), ["c1", "c2"]);
    assert!(s.has_consumer("c1").await.unwrap());
    assert!(s.remove_consumer("c1").await.unwrap());
    assert!(!s.has_consumer("c1").await.unwrap());
    let stats = s.statistics().await.unwrap();
    assert_eq!(stats.consumers, 1);
}

#[tokio::test]
async fn a_waiting_pull_and_the_push_stream_see_new_messages() {
    let t = topic(&unique("rt")).await;
    let s = t.create_subscription().name("s").await.unwrap();
    let publisher = t.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(300)).await;
        publisher.publish("late").await.unwrap();
        publisher.publish("later").await.unwrap();
    });
    let consumer = s.pull_consumer().await.unwrap();
    let m = consumer
        .pull()
        .timeout(Duration::from_secs(3))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.payload, "late");
    m.ack().await.unwrap();
    let mut stream = Box::pin(consumer.into_stream(Duration::from_secs(1)));
    let next = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(next.payload, "later");
}

#[tokio::test]
async fn disabled_operations_fail() {
    let t = topic(&unique("rt")).await;
    let s = t.create_subscription().name("s").await.unwrap();
    t.disable(PubSubOperation::Publish).await.unwrap();
    assert!(matches!(
        t.publish("x").await,
        Err(Error::OperationDisabled(_))
    ));
    t.enable(PubSubOperation::Publish).await.unwrap();
    t.publish("x").await.unwrap();
    t.disable(PubSubOperation::Pull).await.unwrap();
    let consumer = s.pull_consumer().await.unwrap();
    assert!(matches!(
        consumer.pull().await,
        Err(Error::OperationDisabled(_))
    ));
}

#[tokio::test]
async fn inspect_statistics_clear_delete() {
    let t = topic(&unique("rt")).await;
    let s = t.create_subscription().name("s").await.unwrap();
    let m = t.publish("a").header("k", &1u8).await.unwrap();
    t.publish("b").delay(Duration::from_secs(60)).await.unwrap();
    assert!(t.contains(&m.id).await.unwrap());
    assert_eq!(
        t.get(&m.id)
            .await
            .unwrap()
            .unwrap()
            .header::<u8>("k")
            .unwrap(),
        Some(1)
    );
    assert_eq!(t.list_all().await.unwrap().len(), 2);
    let stats = t.statistics().await.unwrap();
    assert_eq!(stats.published, 2);
    assert_eq!(stats.subscriptions, 1);
    assert_eq!(stats.delayed, 1);
    assert!(t.clear().await.unwrap());
    assert!(t.is_empty().await.unwrap());
    assert_eq!(s.queue().delayed_len().await.unwrap(), 0);
    assert!(t.delete().await.unwrap());
    assert!(t.subscriptions().await.unwrap().is_empty());
}

#[tokio::test]
async fn events_report_publish_pull_and_ack() {
    let t = topic(&unique("rt")).await;
    let s = t.create_subscription().name("s").await.unwrap();
    let mut events = t.events().await.unwrap();
    let id = t.publish("a").await.unwrap().id;
    let mut seen = Vec::new();
    let consumer = s.pull_consumer().await.unwrap();
    consumer.pull().await.unwrap().unwrap().ack().await.unwrap();
    for _ in 0..3 {
        seen.push(
            tokio::time::timeout(Duration::from_secs(3), events.recv())
                .await
                .expect("no event")
                .unwrap(),
        );
    }
    assert!(seen.contains(&TopicEvent::Published(vec![id.clone()])));
    assert!(seen.contains(&TopicEvent::Pulled {
        subscription: "s".into(),
        ids: vec![id.clone()]
    }));
    assert!(seen.contains(&TopicEvent::Acknowledged {
        subscription: "s".into(),
        ids: vec![id]
    }));
}

#[tokio::test]
async fn every_operation_runs_on_a_cluster() {
    let topology = common::topology::Topology::cluster().await;
    let client = redissun::Client::builder()
        .url(topology.cluster_url())
        .build()
        .await
        .expect("could not connect to the cluster");
    let t: T = client.reliable_pubsub_topic(unique("rt"));
    let a = t.create_subscription().name("a").await.unwrap();
    let b = t.create_subscription().name("b").await.unwrap();
    t.publish_many((0..3).map(|i| MessageArgs::new(i.to_string())))
        .await
        .unwrap();
    for s in [&a, &b] {
        let c = s.pull_consumer().await.unwrap();
        let got = c.pull_many(10).await.unwrap();
        assert_eq!(got.len(), 3);
        for d in got {
            d.ack().await.unwrap();
        }
    }
    a.seek(Position::Earliest).await.unwrap();
    assert!(t.delete().await.unwrap());
}
