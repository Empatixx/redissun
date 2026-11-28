//! Tests of `ReliableQueue`, the functionality of Redisson PRO's `RReliableQueue` on redissun's own backend.
//!
//! Redisson publishes no tests for the PRO object, so these are redissun's own, one per behaviour of the spec.

mod common;

use common::{client, unique};
use redissun::reliable_queue::{
    AckMode, MessageArgs, Nack, ProcessingMode, QueueConfig, QueueEvent, QueueMove, QueueOperation,
};
use redissun::{Error, JsonCodec, Object, ReliableQueue};
use std::collections::HashSet;
use std::time::{Duration, Instant};
use tokio::time::sleep;

type Q = ReliableQueue<String, JsonCodec>;

async fn queue(name: &str) -> Q {
    client().await.reliable_queue(name.to_string())
}

#[tokio::test]
async fn add_poll_ack() {
    let q = queue(&unique("rq")).await;
    let added = q.add("a").await.unwrap().unwrap();
    assert!(added.id.contains('-'));
    assert_eq!(q.len().await.unwrap(), 1);
    let d = q.poll().await.unwrap().unwrap();
    assert_eq!(d.payload, "a");
    assert_eq!(d.id, added.id);
    assert_eq!(d.deliveries, 1);
    assert_eq!(q.len().await.unwrap(), 0);
    assert_eq!(q.unacked_len().await.unwrap(), 1);
    assert!(d.ack().await.unwrap());
    assert_eq!(q.unacked_len().await.unwrap(), 0);
    assert!(!q.contains(&added.id).await.unwrap());
    assert!(q.poll().await.unwrap().is_none());
}

#[tokio::test]
async fn priorities_first_then_fifo() {
    let q = queue(&unique("rq")).await;
    q.add("low1").priority(1).await.unwrap();
    q.add("high").priority(9).await.unwrap();
    q.add("low2").priority(1).await.unwrap();
    q.add("mid").await.unwrap();
    let mut order = Vec::new();
    while let Some(d) = q.poll().ack_mode(AckMode::Auto).await.unwrap() {
        order.push(d.payload.clone());
    }
    assert_eq!(order, ["high", "mid", "low1", "low2"]);
}

#[tokio::test]
async fn an_unacked_message_comes_back_after_its_visibility() {
    let q = queue(&unique("rq")).await;
    q.add("a").await.unwrap();
    let first = q
        .poll()
        .visibility(Duration::from_millis(200))
        .await
        .unwrap()
        .unwrap();
    let id = first.id.clone();
    drop(first);
    assert!(q.poll().await.unwrap().is_none());
    sleep(Duration::from_millis(300)).await;
    let again = q.poll().await.unwrap().unwrap();
    assert_eq!(again.id, id);
    assert_eq!(again.deliveries, 2);
}

#[tokio::test]
async fn the_delivery_limit_moves_a_message_to_the_dead_letter_queue() {
    let name = unique("rq");
    let dlq_name = unique("dlq");
    let q = queue(&name).await;
    let dlq = queue(&dlq_name).await;
    q.set_config(
        &QueueConfig::new()
            .delivery_limit(2)
            .dead_letter_queue(&dlq_name),
    )
    .await
    .unwrap();
    let id = q.add("poison").await.unwrap().unwrap().id;
    for _ in 0..2 {
        let d = q.poll().await.unwrap().unwrap();
        d.nack(Nack::Failed {
            delay: Duration::ZERO,
        })
        .await
        .unwrap();
    }
    assert!(!q.contains(&id).await.unwrap());
    let dead = dlq.poll().await.unwrap().unwrap();
    assert_eq!(dead.id, id);
    assert_eq!(dead.payload, "poison");
    assert_eq!(
        dlq.dead_letter_sources().await.unwrap(),
        [format!("{{{name}}}")]
    );
}

#[tokio::test]
async fn a_rejected_message_goes_to_the_dead_letter_queue_or_away() {
    let dlq_name = unique("dlq");
    let q = queue(&unique("rq")).await;
    let dlq = queue(&dlq_name).await;
    q.set_config(&QueueConfig::new().dead_letter_queue(&dlq_name))
        .await
        .unwrap();
    q.add("bad").await.unwrap();
    q.poll()
        .await
        .unwrap()
        .unwrap()
        .nack(Nack::Rejected)
        .await
        .unwrap();
    assert_eq!(dlq.len().await.unwrap(), 1);

    let plain = queue(&unique("rq")).await;
    let id = plain.add("bad").await.unwrap().unwrap().id;
    plain
        .poll()
        .await
        .unwrap()
        .unwrap()
        .nack(Nack::Rejected)
        .await
        .unwrap();
    assert!(!plain.contains(&id).await.unwrap());
}

#[tokio::test]
async fn delays_hold_a_message_back() {
    let q = queue(&unique("rq")).await;
    q.add("later")
        .delay(Duration::from_millis(300))
        .await
        .unwrap();
    assert_eq!(q.delayed_len().await.unwrap(), 1);
    assert!(q.poll().await.unwrap().is_none());
    let started = Instant::now();
    let d = q
        .poll()
        .timeout(Duration::from_secs(3))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(d.payload, "later");
    assert!(started.elapsed() < Duration::from_millis(2500));
}

#[tokio::test]
async fn a_failed_message_with_a_delay_waits() {
    let q = queue(&unique("rq")).await;
    q.add("a").await.unwrap();
    q.poll()
        .await
        .unwrap()
        .unwrap()
        .nack(Nack::Failed {
            delay: Duration::from_millis(300),
        })
        .await
        .unwrap();
    assert!(q.poll().await.unwrap().is_none());
    sleep(Duration::from_millis(400)).await;
    assert!(q.poll().await.unwrap().is_some());
}

#[tokio::test]
async fn a_message_expires_with_its_ttl() {
    let q = queue(&unique("rq")).await;
    let id = q
        .add("short")
        .ttl(Duration::from_millis(200))
        .await
        .unwrap()
        .unwrap()
        .id;
    sleep(Duration::from_millis(300)).await;
    assert!(q.poll().await.unwrap().is_none());
    assert!(!q.contains(&id).await.unwrap());
}

#[tokio::test]
async fn dedup_by_hash_and_by_id() {
    let q = queue(&unique("rq")).await;
    let w = Duration::from_secs(60);
    assert!(q.add("same").dedup_by_hash(w).await.unwrap().is_some());
    assert!(q.add("same").dedup_by_hash(w).await.unwrap().is_none());
    assert!(q
        .add("x")
        .dedup_by_id("order-1", w)
        .await
        .unwrap()
        .is_some());
    assert!(q
        .add("y")
        .dedup_by_id("order-1", w)
        .await
        .unwrap()
        .is_none());
    assert_eq!(q.len().await.unwrap(), 2);
    let short = queue(&unique("rq")).await;
    short
        .add("z")
        .dedup_by_id("k", Duration::from_millis(100))
        .await
        .unwrap();
    sleep(Duration::from_millis(200)).await;
    assert!(short
        .add("z")
        .dedup_by_id("k", Duration::from_millis(100))
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn max_size_with_a_waiting_add() {
    let q = queue(&unique("rq")).await;
    q.set_config(&QueueConfig::new().max_size(1)).await.unwrap();
    q.add("one").await.unwrap();
    assert!(q.add("two").await.is_err());
    let q2 = q.clone();
    let consumer = tokio::spawn(async move {
        sleep(Duration::from_millis(300)).await;
        let d = q2.poll().await.unwrap().unwrap();
        d.ack().await.unwrap();
    });
    assert!(q
        .add("two")
        .timeout(Duration::from_secs(3))
        .await
        .unwrap()
        .is_some());
    consumer.await.unwrap();
}

#[tokio::test]
async fn max_message_size_is_a_config_error() {
    let q = queue(&unique("rq")).await;
    q.set_config(&QueueConfig::new().max_message_size(5))
        .await
        .unwrap();
    assert!(matches!(
        q.add("much too long").await,
        Err(Error::Config(_))
    ));
}

#[tokio::test]
async fn sequential_mode_hands_out_one_at_a_time() {
    let q = queue(&unique("rq")).await;
    q.set_config(&QueueConfig::new().processing_mode(ProcessingMode::Sequential))
        .await
        .unwrap();
    q.add("a").await.unwrap();
    q.add("b").await.unwrap();
    let a = q.poll().await.unwrap().unwrap();
    assert!(q.poll().await.unwrap().is_none());
    a.ack().await.unwrap();
    assert_eq!(q.poll().await.unwrap().unwrap().payload, "b");
}

#[tokio::test]
async fn disabled_operations_fail() {
    let q = queue(&unique("rq")).await;
    q.disable(QueueOperation::Add).await.unwrap();
    assert!(matches!(q.add("a").await, Err(Error::OperationDisabled(_))));
    q.enable(QueueOperation::Add).await.unwrap();
    q.add("a").await.unwrap();
    q.disable(QueueOperation::Poll).await.unwrap();
    assert!(matches!(q.poll().await, Err(Error::OperationDisabled(_))));
}

#[tokio::test]
async fn headers_round_trip() {
    let q = queue(&unique("rq")).await;
    q.add("a")
        .header("attempt", &3u32)
        .header("source", "api")
        .await
        .unwrap();
    let d = q.poll().await.unwrap().unwrap();
    assert_eq!(d.header::<u32>("attempt").unwrap(), Some(3));
    assert_eq!(
        d.header::<String>("source").unwrap().as_deref(),
        Some("api")
    );
    assert_eq!(d.header::<u32>("missing").unwrap(), None);
}

#[tokio::test]
async fn inspect_remove_clear() {
    let q = queue(&unique("rq")).await;
    let ids: Vec<String> = q
        .add_many((0..3).map(|i| MessageArgs::new(format!("m{i}"))))
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.unwrap().id)
        .collect();
    let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
    assert_eq!(q.contains_many(&refs).await.unwrap(), 3);
    assert_eq!(q.get(refs[1]).await.unwrap().unwrap().payload, "m1");
    assert!(q.get("0-0").await.unwrap().is_none());
    assert_eq!(q.list_all().await.unwrap().len(), 3);
    assert_eq!(q.remove(&[refs[0], "0-0"]).await.unwrap(), 1);
    assert!(q.clear().await.unwrap());
    assert!(q.list_all().await.unwrap().is_empty());
    assert!(!q.clear().await.unwrap());
}

#[tokio::test]
async fn move_to_another_queue() {
    let dest_name = unique("rq");
    let q = queue(&unique("rq")).await;
    let dest = queue(&dest_name).await;
    for i in 0..3 {
        q.add(&format!("m{i}")).await.unwrap();
    }
    assert_eq!(q.move_to(&dest_name, QueueMove::Count(2)).await.unwrap(), 2);
    assert_eq!(dest.len().await.unwrap(), 2);
    assert_eq!(q.len().await.unwrap(), 1);
    assert_eq!(dest.poll().await.unwrap().unwrap().payload, "m0");
}

async fn next(events: &mut redissun::reliable_queue::QueueEvents) -> QueueEvent {
    tokio::time::timeout(Duration::from_secs(3), events.recv())
        .await
        .expect("no event")
        .unwrap()
}

#[tokio::test]
async fn events_are_published() {
    let q = queue(&unique("rq")).await;
    let mut events = q.events().await.unwrap();
    let id = q.add("a").await.unwrap().unwrap().id;
    assert_eq!(next(&mut events).await, QueueEvent::Added(vec![id.clone()]));
    let d = q.poll().await.unwrap().unwrap();
    assert_eq!(
        next(&mut events).await,
        QueueEvent::Polled(vec![id.clone()])
    );
    d.ack().await.unwrap();
    assert_eq!(next(&mut events).await, QueueEvent::Acknowledged(vec![id]));
    q.disable(QueueOperation::Poll).await.unwrap();
    assert_eq!(
        next(&mut events).await,
        QueueEvent::Disabled(QueueOperation::Poll)
    );
}

#[tokio::test]
async fn many_consumers_get_every_message_exactly_once() {
    let q = queue(&unique("rq")).await;
    let n = 100;
    q.add_many((0..n).map(|i| MessageArgs::new(i.to_string())))
        .await
        .unwrap();
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let q = q.clone();
        tasks.push(tokio::spawn(async move {
            let mut got = Vec::new();
            while let Some(d) = q.poll().await.unwrap() {
                got.push(d.payload.clone());
                d.ack().await.unwrap();
            }
            got
        }));
    }
    let mut all = Vec::new();
    for t in tasks {
        all.extend(t.await.unwrap());
    }
    assert_eq!(all.len(), n);
    assert_eq!(all.iter().collect::<HashSet<_>>().len(), n);
}

#[tokio::test]
async fn object_methods_cover_every_key() {
    let q = queue(&unique("rq")).await;
    q.set_config(&QueueConfig::new()).await.unwrap();
    q.add("a").await.unwrap();
    assert!(q.exists().await.unwrap());
    assert!(q.del().await.unwrap());
    assert!(!q.exists().await.unwrap());
}

#[tokio::test]
async fn every_operation_runs_on_a_cluster() {
    let topology = common::topology::Topology::cluster().await;
    let client = redissun::Client::builder()
        .url(topology.cluster_url())
        .build()
        .await
        .expect("could not connect to the cluster");
    let dlq_name = unique("dlq");
    let q: Q = client.reliable_queue(unique("rq"));
    let dlq: Q = client.reliable_queue(dlq_name.clone());
    q.set_config(
        &QueueConfig::new()
            .delivery_limit(1)
            .dead_letter_queue(&dlq_name),
    )
    .await
    .unwrap();
    q.add("a").header("h", &1).await.unwrap();
    let d = q.poll().await.unwrap().unwrap();
    d.nack(Nack::Failed {
        delay: Duration::ZERO,
    })
    .await
    .unwrap();
    assert_eq!(dlq.len().await.unwrap(), 1);
    q.add("b").await.unwrap();
    assert_eq!(q.move_to(&dlq_name, QueueMove::Count(1)).await.unwrap(), 1);
    assert_eq!(dlq.list_all().await.unwrap().len(), 2);
    assert!(dlq.clear().await.unwrap());
}
