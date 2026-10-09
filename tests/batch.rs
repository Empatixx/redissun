mod common;

use common::{client, unique};
use redissun::{Error, Object};
use std::time::Duration;

#[tokio::test]
async fn nothing_is_sent_before_execute() {
    let client = client().await;
    let name = unique("batch");
    let batch = client.batch();
    drop(
        batch
            .hash_map::<String, String>(name.clone())
            .insert("k", "v"),
    );
    let plain = client.hash_map::<String, String>(name);
    assert_eq!(plain.get("k").await.unwrap(), None);
    assert_eq!(batch.len(), 1);
    batch.execute().await.unwrap();
    assert_eq!(plain.get("k").await.unwrap().as_deref(), Some("v"));
}

#[tokio::test]
async fn handles_give_typed_results_in_queue_order() {
    let client = client().await;
    let batch = client.batch();
    let map = batch.hash_map::<String, String>(unique("batch"));
    let first = map.insert("k", "one");
    let read = map.get("k");
    let second = map.insert("k", "two");
    let removed = map.remove("k");
    let missing = map.get("k");
    let has = map.contains_key("k");
    let len = map.len();
    assert_eq!(batch.execute().await.unwrap(), 7);
    assert_eq!(first.await.unwrap(), None);
    assert_eq!(read.await.unwrap().as_deref(), Some("one"));
    assert_eq!(second.await.unwrap().as_deref(), Some("one"));
    assert_eq!(removed.await.unwrap().as_deref(), Some("two"));
    assert_eq!(missing.await.unwrap(), None);
    assert!(!has.await.unwrap());
    assert_eq!(len.await.unwrap(), 0);
}

#[tokio::test]
async fn many_kinds_of_objects_share_one_batch() {
    let client = client().await;
    let batch = client.batch();
    let bucket = batch.bucket::<String>(unique("batch"));
    let set = batch.hash_set::<String>(unique("batch"));
    let counter = batch.atomic_i64(unique("batch"));
    let list = batch.vec::<u32>(unique("batch"));
    let queue = batch.vec_deque::<u32>(unique("batch"));
    let zset = batch.sorted_set::<String>(unique("batch"));
    let topic = batch.topic::<String>(unique("batch"));

    let set_bucket = bucket.set("hello");
    let get_bucket = bucket.get();
    let added = set.insert("a");
    let again = set.insert("a");
    let present = set.contains("a");
    let set_len = set.len();
    let removed = set.remove("a");
    let c1 = counter.incr();
    let c2 = counter.add_and_get(10);
    let c3 = counter.get();
    let pushed = list.push(&7);
    let list_len = list.len();
    let item = list.get(0);
    let front = queue.push_back(&1);
    drop(queue.push_front(&0));
    let popped = queue.pop_front();
    let back = queue.pop_back();
    let ranked = zset.insert("x", 2.5);
    let score = zset.score("x");
    let bumped = zset.add_score("x", 1.0);
    let zlen = zset.len();
    let receivers = topic.publish("news");

    batch.execute().await.unwrap();
    set_bucket.await.unwrap();
    assert_eq!(get_bucket.await.unwrap().as_deref(), Some("hello"));
    assert!(added.await.unwrap());
    assert!(!again.await.unwrap());
    assert!(present.await.unwrap());
    assert_eq!(set_len.await.unwrap(), 1);
    assert!(removed.await.unwrap());
    assert_eq!(c1.await.unwrap(), 1);
    assert_eq!(c2.await.unwrap(), 11);
    assert_eq!(c3.await.unwrap(), 11);
    pushed.await.unwrap();
    assert_eq!(list_len.await.unwrap(), 1);
    assert_eq!(item.await.unwrap(), Some(7));
    front.await.unwrap();
    assert_eq!(popped.await.unwrap(), Some(0));
    assert_eq!(back.await.unwrap(), Some(1));
    assert!(ranked.await.unwrap());
    assert_eq!(score.await.unwrap(), Some(2.5));
    assert_eq!(bumped.await.unwrap(), 3.5);
    assert_eq!(zlen.await.unwrap(), 1);
    assert_eq!(receivers.await.unwrap(), 0);
}

#[tokio::test]
async fn one_failing_command_does_not_stop_the_others() {
    let client = client().await;
    let name = unique("batch");
    client
        .hash_map::<String, String>(name.clone())
        .insert("k", "v")
        .await
        .unwrap();
    let batch = client.batch();
    let wrong = batch.bucket::<String>(name).get();
    let fine = batch.atomic_i64(unique("batch")).incr();
    batch.execute().await.unwrap();
    assert!(matches!(wrong.await, Err(Error::Redis(_))));
    assert_eq!(fine.await.unwrap(), 1);
}

#[tokio::test]
async fn atomic_mode_runs_everything_in_one_transaction() {
    let client = client().await;
    let batch = client.batch().atomic();
    let counter = batch.atomic_i64(unique("batch"));
    let a = counter.incr();
    let b = counter.incr();
    let map = batch.hash_map::<String, String>(unique("batch"));
    let inserted = map.insert("k", "v");
    assert_eq!(batch.execute().await.unwrap(), 3);
    assert_eq!(a.await.unwrap(), 1);
    assert_eq!(b.await.unwrap(), 2);
    assert_eq!(inserted.await.unwrap(), None);
}

#[tokio::test]
async fn skip_result_applies_the_commands_but_hides_the_replies() {
    let client = client().await;
    let name = unique("batch");
    let batch = client.batch().skip_result();
    let handle = batch.atomic_i64(name.clone()).add_and_get(5);
    batch.execute().await.unwrap();
    assert!(matches!(handle.await, Err(Error::Config(_))));
    assert_eq!(client.atomic_i64(name).get().await.unwrap(), 5);
}

#[tokio::test]
async fn a_batch_that_is_dropped_resolves_its_handles_with_an_error() {
    let client = client().await;
    let handle = {
        let batch = client.batch();
        batch.atomic_i64(unique("batch")).incr()
    };
    assert!(matches!(handle.await, Err(Error::Config(_))));
}

#[tokio::test]
async fn an_empty_batch_executes() {
    assert_eq!(client().await.batch().execute().await.unwrap(), 0);
}

#[tokio::test]
async fn key_commands_work_in_a_batch() {
    let client = client().await;
    let name = unique("batch");
    let batch = client.batch();
    drop(batch.atomic_i64(name.clone()).add_and_get(1));
    let ttl = batch.expire(name.clone(), Duration::from_secs(60));
    let gone = batch.del(unique("batch"));
    batch.execute().await.unwrap();
    assert!(ttl.await.unwrap());
    assert!(!gone.await.unwrap());
    assert!(client.atomic_i64(name).ttl().await.unwrap().is_some());
}

#[tokio::test]
async fn a_thousand_commands_go_out_together() {
    let client = client().await;
    let batch = client.batch();
    let counter = batch.atomic_i64(unique("batch"));
    let handles: Vec<_> = (0..1000).map(|_| counter.incr()).collect();
    assert_eq!(batch.execute().await.unwrap(), 1000);
    let mut last = 0;
    for handle in handles {
        last = handle.await.unwrap();
    }
    assert_eq!(last, 1000);
}
