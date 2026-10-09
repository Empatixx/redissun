mod common;

use common::{client, connect_with, unique};
use redissun::{Error, Object};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

async fn wrong_type_key(client: &redissun::Client) -> String {
    let name = unique("batch");
    client
        .hash_map::<String, String>(name.clone())
        .insert("k", "v")
        .await
        .unwrap();
    name
}

fn modes(client: &redissun::Client) -> [redissun::Batch<redissun::JsonCodec>; 2] {
    [client.batch(), client.batch().stored_in_redis()]
}

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
async fn one_failing_command_fails_execute_but_not_the_other_handles() {
    let client = client().await;
    let name = unique("batch");
    client
        .hash_map::<String, String>(name.clone())
        .insert("k", "v")
        .await
        .unwrap();
    let batch = client.batch();
    let wrong = batch.bucket::<String>(name).get();
    let counter = unique("batch");
    let fine = batch.atomic_i64(counter.clone()).incr();
    assert!(matches!(batch.execute().await, Err(Error::Redis(_))));
    assert!(matches!(wrong.await, Err(Error::Redis(_))));
    assert_eq!(fine.await.unwrap(), 1);
    assert_eq!(client.atomic_i64(counter).get().await.unwrap(), 1);
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
        .response_timeout(Duration::from_nanos(1))
        .retry_attempts(2)
        .retry_interval(Duration::from_millis(150));
    let name = unique("batch");
    let handle = batch.atomic_i64(name.clone()).incr();
    for _ in 0..5000 {
        let _ = batch.atomic_i64(name.clone()).get();
    }
    let started = std::time::Instant::now();
    assert!(matches!(batch.execute().await, Err(Error::Timeout)));
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_millis(300), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(3), "{elapsed:?}");
    assert!(matches!(handle.await, Err(Error::Timeout)));
}

#[tokio::test]
async fn by_default_a_batch_is_retried_four_times_with_jitter_like_redisson() {
    let client = client().await;
    let batch = client.batch().response_timeout(Duration::from_nanos(1));
    let name = unique("batch");
    let handle = batch.atomic_i64(name.clone()).incr();
    for _ in 0..5000 {
        let _ = batch.atomic_i64(name.clone()).get();
    }
    let started = std::time::Instant::now();
    assert!(matches!(batch.execute().await, Err(Error::Timeout)));
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_millis(3500), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(10), "{elapsed:?}");
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
async fn in_atomic_mode_a_failing_command_fails_execute_but_not_the_other_handles() {
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
        assert!(matches!(batch.execute().await, Err(Error::Redis(_))));
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

#[tokio::test]
async fn order() {
    let client = connect_with(|builder| builder.pool_size(1)).await;
    let mut tasks = Vec::new();
    for _ in 0..2 {
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            let batch = client.batch().stored_in_redis();
            let removed = batch.sorted_set::<String>(unique("batch")).remove("aaa");
            let read = batch.bucket::<String>(unique("batch")).get();
            assert_eq!(batch.execute().await.unwrap().commands, 2);
            assert!(!removed.await.unwrap());
            assert_eq!(read.await.unwrap(), None);
        }));
    }
    for task in tasks {
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn convertor() {
    let client = client().await;
    for batch in modes(&client) {
        let zset = unique("myZKey");
        let bucket = unique("test");
        let f1 = batch
            .sorted_set::<String>(zset.clone())
            .add_score("abc", 1.0);
        let f2 = batch.bucket::<String>(bucket.clone()).set("1");
        batch.execute().await.unwrap();
        assert_eq!(f1.await.unwrap(), 1.0);
        f2.await.unwrap();
        assert_eq!(
            client
                .sorted_set::<String>(zset)
                .score("abc")
                .await
                .unwrap(),
            Some(1.0)
        );
        assert_eq!(
            client
                .bucket::<String>(bucket)
                .get()
                .await
                .unwrap()
                .as_deref(),
            Some("1")
        );
    }
    for batch in modes(&client) {
        let zset = unique("myZKey2");
        let b2f1 = batch
            .sorted_set::<String>(zset.clone())
            .add_score("abc", 1.0);
        let b2f2 = batch.sorted_set::<String>(zset).add_score("abc", 1.0);
        batch.execute().await.unwrap();
        assert_eq!(b2f1.await.unwrap(), 1.0);
        assert_eq!(b2f2.await.unwrap(), 2.0);
    }
}

#[tokio::test]
async fn performance() {
    let client = client().await;
    let name = unique("map");
    let map = client.hash_map::<String, String>(name.clone());
    for j in 0..100 {
        map.insert(&j.to_string(), &j.to_string()).await.unwrap();
    }
    for stored in [false, true] {
        for _ in 0..200 {
            let batch = if stored {
                client.batch().stored_in_redis()
            } else {
                client.batch()
            };
            let view = batch.hash_map::<String, String>(name.clone());
            let reads: Vec<_> = (0..100).map(|j| view.get(&j.to_string())).collect();
            batch.execute().await.unwrap();
            for (j, read) in reads.into_iter().enumerate() {
                assert_eq!(read.await.unwrap(), Some(j.to_string()));
            }
        }
    }
}

#[tokio::test]
async fn skip_result() {
    let client = client().await;
    let shared = unique("test");
    let mut tasks = Vec::new();
    for t in 0..8 {
        let client = client.clone();
        let shared = shared.clone();
        tasks.push(tokio::spawn(async move {
            for j in 0..200 {
                if (t + j) % 2 == 0 {
                    let batch = client.batch();
                    drop(batch.bucket::<String>(unique("batch")).set("test"));
                    batch.execute().await.unwrap();
                } else {
                    client
                        .atomic_i64(shared.clone())
                        .add_and_get(2)
                        .await
                        .unwrap();
                }
            }
        }));
    }
    for task in tasks {
        tokio::time::timeout(Duration::from_secs(60), task)
            .await
            .unwrap()
            .unwrap();
    }
    assert_eq!(client.atomic_i64(shared).get().await.unwrap(), 1600);
}

#[tokio::test]
async fn connection_leak_after_error() {
    let client = connect_with(|builder| builder.pool_size(1)).await;
    let name = unique("test");
    client
        .bucket::<String>(name.clone())
        .set("test")
        .await
        .unwrap();
    for stored in [false, true] {
        for _ in 0..5 {
            let batch = if stored {
                client.batch().stored_in_redis()
            } else {
                client.batch()
            };
            drop(batch.atomic_i64(name.clone()).incr());
            match batch.execute().await {
                Err(Error::Timeout) => panic!("a failed batch must not leak its connection"),
                Err(_) => {}
                Ok(result) => panic!("INCR on a string must fail, got {result:?}"),
            }
        }
    }
}

#[tokio::test]
async fn connection_leak_after_error_in_stored_mode() {
    let client = client().await;
    let name = unique("test");
    let batch = client
        .batch()
        .stored_in_redis()
        .response_timeout(Duration::from_millis(1));
    for _ in 0..20_000 {
        drop(batch.bucket::<i64>(name.clone()).set(&123));
    }
    assert!(batch.execute().await.is_err());

    let other = client.bucket::<i64>(unique("test3"));
    other.set(&4).await.unwrap();
    assert_eq!(other.get().await.unwrap(), Some(4));

    let batch = client.batch().stored_in_redis();
    let first = unique("test1");
    let second = unique("test2");
    drop(batch.bucket::<i64>(first.clone()).set(&1));
    drop(batch.bucket::<i64>(second.clone()).set(&2));
    batch.execute().await.unwrap();
    assert_eq!(client.bucket::<i64>(first).get().await.unwrap(), Some(1));
    assert_eq!(client.bucket::<i64>(second).get().await.unwrap(), Some(2));
}

#[tokio::test]
async fn big_request_atomic() {
    let client = client().await;
    let batch = client
        .batch()
        .atomic()
        .response_timeout(Duration::from_secs(15))
        .retry_interval(Duration::from_secs(1))
        .retry_attempts(5);
    let prefix = unique("big");
    let mut reads = Vec::new();
    for i in 0..100i64 {
        let bucket = batch.bucket::<i64>(format!("{prefix}:{i}"));
        drop(bucket.set(&i));
        reads.push(bucket.get());
    }
    assert_eq!(batch.execute().await.unwrap().commands, 200);
    for (i, read) in reads.into_iter().enumerate() {
        assert_eq!(read.await.unwrap(), Some(i as i64));
    }
}

#[tokio::test]
async fn sync_slaves_wait() {
    let client = connect_with(|builder| builder.pool_size(1)).await;
    for batch in modes(&client) {
        let batch = batch.skip_result().sync(2, Duration::from_secs(1));
        drop(batch.bucket::<i64>(unique("1")).set(&1));
        let started = std::time::Instant::now();
        let result = batch.execute().await.unwrap();
        assert_eq!(result.synced_slaves, 0);
        assert!(started.elapsed() < Duration::from_secs(5));
    }
}

#[tokio::test]
async fn write_timeout() {
    let client = client().await;
    for batch in modes(&client) {
        let name = unique("test");
        let map = batch.hash_map::<String, String>(name.clone());
        let total = 10_000;
        for i in 0..total {
            drop(map.insert(&i.to_string(), &i.to_string()));
        }
        let started = std::time::Instant::now();
        batch.execute().await.unwrap();
        assert!(started.elapsed() < Duration::from_secs(15));
        assert_eq!(
            client.hash_map::<String, String>(name).len().await.unwrap(),
            total
        );
    }
}

#[tokio::test]
async fn skip_result_in_both_modes() {
    let client = client().await;
    for batch in modes(&client) {
        let batch = batch.skip_result();
        let a1 = unique("A1");
        let a2 = unique("A2");
        let a3 = unique("A3");
        for name in [&a1, &a2, &a3] {
            drop(batch.bucket::<String>(name.clone()).set("001"));
        }
        drop(batch.del(a1.clone()));
        drop(batch.del(a2.clone()));
        batch.execute().await.unwrap();
        assert!(!client.bucket::<String>(a1).exists().await.unwrap());
        assert!(client.bucket::<String>(a3).exists().await.unwrap());
    }
}

#[tokio::test]
async fn batch_npe() {
    let client = client().await;
    for batch in modes(&client) {
        let a1 = unique("A1");
        let a2 = unique("A2");
        for name in [&a1, &a2, &unique("A3")] {
            drop(batch.bucket::<String>(name.clone()).set("001"));
        }
        drop(batch.del(a1));
        drop(batch.del(a2));
        batch.execute().await.unwrap();
    }
}

#[tokio::test]
async fn atomic() {
    let client = client().await;
    let batch = client.batch().atomic();
    let (a1, a2, a3) = (unique("A1"), unique("A2"), unique("A3"));
    let f1 = batch.atomic_i64(a1.clone()).add_and_get(1);
    let f2 = batch.atomic_i64(a2.clone()).add_and_get(2);
    let f3 = batch.atomic_i64(a3).add_and_get(3);
    let d1 = batch.del(a1);
    let d2 = batch.del(a2);
    assert_eq!(batch.execute().await.unwrap().commands, 5);
    assert_eq!(f1.await.unwrap(), 1);
    assert_eq!(f2.await.unwrap(), 2);
    assert_eq!(f3.await.unwrap(), 3);
    assert!(d1.await.unwrap());
    assert!(d2.await.unwrap());
}

#[tokio::test]
async fn different_codecs() {
    let client = client().await;
    for batch in [
        client.batch(),
        client.batch().stored_in_redis(),
        client.batch().atomic(),
    ] {
        let (test1, test2) = (unique("test1"), unique("test2"));
        drop(
            batch
                .hash_map::<String, String>(test1.clone())
                .insert("1", "2"),
        );
        drop(
            batch
                .hash_map::<String, i64>(test2.clone())
                .insert("21", &3),
        );
        let val1 = batch.hash_map::<String, String>(test1).get("1");
        let val2 = batch.hash_map::<String, i64>(test2).get("21");
        batch.execute().await.unwrap();
        assert_eq!(val1.await.unwrap().as_deref(), Some("2"));
        assert_eq!(val2.await.unwrap(), Some(3));
    }
}

#[tokio::test]
async fn batch_list() {
    let client = client().await;
    for batch in modes(&client) {
        let list = batch.vec::<i64>(unique("list"));
        for i in 1..540 {
            drop(list.push(&i));
        }
        assert_eq!(batch.execute().await.unwrap().commands, 539);
    }
}

#[tokio::test]
async fn batch_ping() {
    let client = client().await;
    for batch in modes(&client) {
        let handle = batch.bucket::<String>(unique("test")).set("1232");
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(batch.execute().await.unwrap().commands, 1);
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn shutdown_timeout() {
    for stored in [false, true] {
        let client = client().await;
        let batch = if stored {
            client.batch().stored_in_redis()
        } else {
            client.batch()
        };
        let name = unique("test");
        for _ in 0..10 {
            drop(batch.bucket::<i64>(name.clone()).set(&123));
        }
        tokio::time::timeout(Duration::from_secs(3), batch.execute())
            .await
            .unwrap()
            .unwrap();
        drop(client);
    }
}

#[tokio::test]
async fn batch_big_request() {
    let client = client().await;
    for batch in modes(&client) {
        let map = batch.hash_map::<String, String>(unique("test"));
        let counter = batch.atomic_i64(unique("counter"));
        for _ in 0..210 {
            drop(map.insert("1", "2"));
            drop(map.insert("2", "3"));
            drop(map.insert("2", "5"));
            drop(counter.incr());
            drop(counter.incr());
        }
        assert_eq!(batch.execute().await.unwrap().commands, 210 * 5);
    }
}

#[tokio::test]
async fn empty() {
    let client = client().await;
    for batch in modes(&client) {
        assert_eq!(batch.execute().await.unwrap().commands, 0);
    }
}

#[tokio::test]
async fn ordering() {
    let client = client().await;
    for stored in [false, true] {
        let batch = Arc::new(if stored {
            client.batch().stored_in_redis()
        } else {
            client.batch()
        });
        let prefix = unique("test");
        let queued = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = Vec::new();
        for j in 0..500i64 {
            let batch = batch.clone();
            let queued = queued.clone();
            let prefix = prefix.clone();
            tasks.push(tokio::spawn(async move {
                let mut queued = queued.lock().await;
                let key = j % 3;
                let handle = batch.atomic_i64(format!("{prefix}{key}")).add_and_get(j);
                queued.push((key, j, handle));
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        let batch = Arc::try_unwrap(batch).unwrap();
        batch.execute().await.unwrap();
        let queued = std::mem::take(&mut *queued.lock().await);
        let mut sums = [0i64; 3];
        for (key, j, handle) in queued {
            sums[key as usize] += j;
            assert_eq!(handle.await.unwrap(), sums[key as usize]);
        }
    }
}

#[tokio::test]
async fn test() {
    let client = client().await;
    for batch in modes(&client) {
        let name = unique("test");
        let counter = unique("counter");
        let map = batch.hash_map::<String, String>(name.clone());
        let r0 = map.insert("1", "2");
        let r1 = map.insert("2", "3");
        let r2 = map.insert("2", "5");
        let r3 = batch.atomic_i64(counter.clone()).incr();
        let r4 = batch.atomic_i64(counter.clone()).incr();
        assert_eq!(batch.execute().await.unwrap().commands, 5);
        assert_eq!(r0.await.unwrap(), None);
        assert_eq!(r1.await.unwrap(), None);
        assert_eq!(r2.await.unwrap().as_deref(), Some("3"));
        assert_eq!(r3.await.unwrap(), 1);
        assert_eq!(r4.await.unwrap(), 2);
        let map = client.hash_map::<String, String>(name);
        assert_eq!(map.get("1").await.unwrap().as_deref(), Some("2"));
        assert_eq!(map.get("2").await.unwrap().as_deref(), Some("5"));
        assert_eq!(map.len().await.unwrap(), 2);
        assert_eq!(client.atomic_i64(counter).get().await.unwrap(), 2);
    }
}

#[tokio::test]
async fn a_queue_time_error_aborts_the_transaction_of_its_node() {
    let client = client().await;
    let wrong = wrong_type_key(&client).await;
    let counter = unique("batch");
    let batch = client.batch().stored_in_redis();
    let applied = batch.atomic_i64(counter.clone()).incr();
    let failed = batch.bucket::<String>(wrong).get();
    assert!(batch.execute().await.is_err());
    assert!(matches!(failed.await, Err(Error::Redis(_))));
    assert_eq!(applied.await.unwrap(), 1);
}

mod cluster {
    use super::common::topology::Topology;
    use super::common::unique;
    use redissun::Client;
    use std::time::Duration;

    async fn connect(topology: &Topology) -> Client {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        loop {
            let mut online = 0;
            for port in topology.ports() {
                let info = topology.cli(port, &["INFO", "replication"]).await;
                if info.contains("role:master") && info.contains("state=online") {
                    online += 1;
                }
            }
            if online == 3 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "replicas did not come online"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Client::builder()
            .url(topology.cluster_url())
            .build()
            .await
            .expect("could not connect to the cluster")
    }

    #[tokio::test]
    #[ignore = "starts a redis cluster in docker; run with --ignored"]
    async fn memory_atomic_in_cluster() {
        let topology = Topology::cluster().await;
        let client = connect(&topology).await;
        let (a, b) = (
            topology.cluster_master_of("{a}").await,
            topology.cluster_master_of("{b}").await,
        );
        assert_ne!(a, b);

        let batch = client.batch().atomic().sync(1, Duration::from_secs(1));
        let t1 = batch
            .bucket::<String>(format!("{{a}}{}", unique("Test1")))
            .set("test1");
        let t2 = batch
            .bucket::<String>(format!("{{a}}{}", unique("Test2")))
            .set("test2");
        let t3 = batch
            .bucket::<String>(format!("{{b}}{}", unique("Test3")))
            .set("test3");
        let result = batch.execute().await.unwrap();
        assert_eq!(result.synced_slaves, 2);
        assert_eq!(result.commands, 3);
        for handle in [t1, t2, t3] {
            handle.await.unwrap();
        }

        let batch = client.batch().atomic().skip_result();
        drop(batch.bucket::<String>("{a}Test1").get());
        drop(batch.bucket::<String>("{a}Test2").get());
        drop(batch.bucket::<String>("{b}Test3").get());
        assert_eq!(batch.execute().await.unwrap().commands, 3);
    }

    #[tokio::test]
    #[ignore = "starts a redis cluster in docker; run with --ignored"]
    async fn sync_slaves_aof() {
        let topology = Topology::cluster().await;
        let client = connect(&topology).await;
        for batch in [client.batch(), client.batch().stored_in_redis()] {
            let batch = batch.sync_aof(1, 1, Duration::from_secs(1));
            let map = batch.hash_map::<String, String>(unique("test"));
            for i in 0..20 {
                drop(map.insert(&i.to_string(), &i.to_string()));
            }
            assert_eq!(batch.execute().await.unwrap().commands, 20);
        }
    }

    #[tokio::test]
    #[ignore = "starts a redis cluster in docker; run with --ignored"]
    async fn sync_slaves() {
        let topology = Topology::cluster().await;
        let client = connect(&topology).await;
        for batch in [client.batch(), client.batch().stored_in_redis()] {
            let batch = batch.sync(1, Duration::from_secs(1));
            let map = batch.hash_map::<String, String>(unique("test"));
            for i in 0..100 {
                drop(map.insert(&i.to_string(), &i.to_string()));
            }
            let result = batch.execute().await.unwrap();
            assert_eq!(result.commands, 100);
            assert_eq!(result.synced_slaves, 1);
        }
    }

    #[tokio::test]
    #[ignore = "starts a redis cluster in docker; run with --ignored"]
    async fn atomic_sync_slaves() {
        let topology = Topology::cluster().await;
        let client = connect(&topology).await;
        let batch = client.batch().atomic().sync(1, Duration::from_secs(1));
        let prefix = unique("{test}");
        let handles: Vec<_> = (0..10)
            .map(|i| batch.atomic_i64(format!("{prefix}{i}")).add_and_get(i))
            .collect();
        let result = batch.execute().await.unwrap();
        assert_eq!(result.synced_slaves, 1);
        for (i, handle) in handles.into_iter().enumerate() {
            assert_eq!(handle.await.unwrap(), i as i64);
        }
    }

    #[tokio::test]
    #[ignore = "starts a redis cluster in docker; run with --ignored"]
    async fn atomic_and_stored_batches_span_several_nodes() {
        let topology = Topology::cluster().await;
        let client = connect(&topology).await;
        for batch in [client.batch().atomic(), client.batch().stored_in_redis()] {
            let names: Vec<String> = ["{a}", "{b}", "{c}"]
                .iter()
                .flat_map(|tag| (0..10).map(move |i| unique(&format!("{tag}spread{i}"))))
                .collect();
            let handles: Vec<_> = names
                .iter()
                .map(|name| batch.atomic_i64(name.clone()).add_and_get(7))
                .collect();
            let result = batch.execute().await.unwrap();
            assert_eq!(result.commands, 30);
            for handle in handles {
                assert_eq!(handle.await.unwrap(), 7);
            }
            for name in names {
                assert_eq!(client.atomic_i64(name).get().await.unwrap(), 7);
            }
        }
    }
}
