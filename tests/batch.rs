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
    assert_eq!(batch.execute().await.unwrap().commands, 7);
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
    assert_eq!(batch.execute().await.unwrap().commands, 3);
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
    assert_eq!(client().await.batch().execute().await.unwrap().commands, 0);
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
    assert_eq!(batch.execute().await.unwrap().commands, 1000);
    let mut last = 0;
    for handle in handles {
        last = handle.await.unwrap();
    }
    assert_eq!(last, 1000);
}

#[tokio::test]
async fn stored_in_redis_commands_wait_in_a_transaction_until_execute() {
    let client = client().await;
    let name = unique("batch");
    let batch = client.batch().stored_in_redis();
    let counter = batch.atomic_i64(name.clone());
    let first = counter.incr();
    let second = counter.add_and_get(10);
    let map = batch.hash_map::<String, String>(unique("batch"));
    let inserted = map.insert("k", "v");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(client.atomic_i64(name.clone()).get().await.unwrap(), 0);

    assert_eq!(batch.execute().await.unwrap().commands, 3);
    assert_eq!(first.await.unwrap(), 1);
    assert_eq!(second.await.unwrap(), 11);
    assert_eq!(inserted.await.unwrap(), None);
    assert_eq!(client.atomic_i64(name).get().await.unwrap(), 11);
}

#[tokio::test]
async fn stored_in_redis_handles_a_large_batch() {
    let client = client().await;
    let batch = client.batch().stored_in_redis();
    let counter = batch.atomic_i64(unique("batch"));
    let handles: Vec<_> = (0..500).map(|_| counter.incr()).collect();
    assert_eq!(batch.execute().await.unwrap().commands, 500);
    let mut last = 0;
    for handle in handles {
        last = handle.await.unwrap();
    }
    assert_eq!(last, 500);
}

#[tokio::test]
async fn discard_drops_the_queued_commands() {
    let client = client().await;
    for stored in [false, true] {
        let name = unique("batch");
        let batch = if stored {
            client.batch().stored_in_redis()
        } else {
            client.batch()
        };
        let handle = batch.atomic_i64(name.clone()).incr();
        batch.discard().await.unwrap();
        assert!(matches!(handle.await, Err(Error::Config(_))));
        assert_eq!(client.atomic_i64(name).get().await.unwrap(), 0);
    }
}

#[tokio::test]
async fn dropping_a_stored_batch_applies_nothing() {
    let client = client().await;
    let name = unique("batch");
    {
        let batch = client.batch().stored_in_redis();
        drop(batch.atomic_i64(name.clone()).incr());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(client.atomic_i64(name).get().await.unwrap(), 0);
}

#[tokio::test]
async fn sync_waits_for_replicas_and_reports_how_many_answered() {
    let client = client().await;
    let batch = client.batch().sync(1, Duration::from_millis(100));
    let handle = batch.atomic_i64(unique("batch")).incr();
    let started = std::time::Instant::now();
    let result = batch.execute().await.unwrap();
    assert_eq!(result.commands, 1);
    assert_eq!(result.synced_slaves, 0);
    assert!(started.elapsed() < Duration::from_secs(3));
    assert_eq!(handle.await.unwrap(), 1);
}

#[tokio::test]
async fn sync_also_works_in_the_atomic_modes() {
    let client = client().await;
    for batch in [
        client.batch().atomic().sync(1, Duration::from_millis(50)),
        client
            .batch()
            .stored_in_redis()
            .sync(1, Duration::from_millis(50)),
    ] {
        let handle = batch.atomic_i64(unique("batch")).incr();
        let result = batch.execute().await.unwrap();
        assert_eq!(result.commands, 1);
        assert_eq!(result.synced_slaves, 0);
        assert_eq!(handle.await.unwrap(), 1);
    }
}

#[tokio::test]
async fn a_response_timeout_fails_the_execution_after_the_retries() {
    let client = client().await;
    let batch = client
        .batch()
        .sync(1, Duration::from_secs(5))
        .response_timeout(Duration::from_millis(150))
        .retry_attempts(1)
        .retry_interval(Duration::from_millis(10));
    let handle = batch.atomic_i64(unique("batch")).incr();
    let started = std::time::Instant::now();
    assert!(matches!(batch.execute().await, Err(Error::Timeout)));
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_millis(300), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(3), "{elapsed:?}");
    assert!(matches!(handle.await, Err(Error::Timeout)));
}

#[tokio::test]
async fn retries_do_not_change_a_successful_execution() {
    let client = client().await;
    let batch = client
        .batch()
        .retry_attempts(3)
        .retry_interval(Duration::from_millis(5));
    let handle = batch.atomic_i64(unique("batch")).incr();
    assert_eq!(batch.execute().await.unwrap().commands, 1);
    assert_eq!(handle.await.unwrap(), 1);
}

#[tokio::test]
async fn in_atomic_mode_a_failing_command_fails_only_its_own_handle() {
    let client = client().await;
    for stored in [false, true] {
        let wrong_name = unique("batch");
        client
            .hash_map::<String, String>(wrong_name.clone())
            .insert("k", "v")
            .await
            .unwrap();
        let batch = if stored {
            client.batch().stored_in_redis()
        } else {
            client.batch().atomic()
        };
        let wrong = batch.bucket::<String>(wrong_name).get();
        let counter_name = unique("batch");
        let counter = batch.atomic_i64(counter_name.clone());
        let first = counter.incr();
        let second = counter.incr();
        let summary = batch.execute().await.unwrap();
        assert_eq!(summary.commands, 3);
        assert!(matches!(wrong.await, Err(Error::Redis(_))));
        assert_eq!(first.await.unwrap(), 1);
        assert_eq!(second.await.unwrap(), 2);
        assert_eq!(client.atomic_i64(counter_name).get().await.unwrap(), 2);
    }
}

#[tokio::test]
async fn skip_result_still_reports_a_failed_command() {
    let client = client().await;
    let name = unique("batch");
    client
        .hash_map::<String, String>(name.clone())
        .insert("k", "v")
        .await
        .unwrap();
    let batch = client.batch().skip_result();
    drop(batch.bucket::<String>(name).get());
    assert!(matches!(batch.execute().await, Err(Error::Redis(_))));
}

#[tokio::test]
async fn a_nan_score_is_rejected_when_queued() {
    let client = client().await;
    let batch = client.batch();
    let handle = batch
        .sorted_set::<String>(unique("batch"))
        .insert("x", f64::NAN);
    assert!(matches!(handle.await, Err(Error::Config(_))));
    assert!(batch.is_empty());
}
