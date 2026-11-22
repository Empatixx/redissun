mod common;

use common::{client, connect_with, redis_url, unique};
use redissun::{Error, Object};
use std::sync::Arc;
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers::ImageExt;
use testcontainers_modules::redis::Redis;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

async fn raw_reply(url: &str, command: &str) -> String {
    let mut stream = TcpStream::connect(url.trim_start_matches("redis://"))
        .await
        .unwrap();
    stream
        .write_all(format!("{command}\r\n").as_bytes())
        .await
        .unwrap();
    let mut reply = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let read = stream.read(&mut chunk).await.unwrap();
        assert!(read > 0, "redis closed the connection");
        reply.extend_from_slice(&chunk[..read]);
        let Some(header) = reply.iter().position(|byte| *byte == b'\n') else {
            continue;
        };
        if reply[0] != b'$' {
            return String::from_utf8_lossy(&reply[..header]).trim().to_string();
        }
        let length: usize = std::str::from_utf8(&reply[1..header - 1])
            .unwrap()
            .parse()
            .unwrap();
        if reply.len() >= header + 1 + length {
            return String::from_utf8_lossy(&reply[header + 1..header + 1 + length]).to_string();
        }
    }
}

async fn clients_named(url: &str, name: &str) -> Vec<String> {
    raw_reply(url, "CLIENT LIST")
        .await
        .lines()
        .filter(|line| line.contains(&format!("name={name} ")))
        .map(str::to_string)
        .collect()
}

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

/// A Redis of its own for a test that stops the server from answering with `CLIENT PAUSE`, which the shared one must not suffer.
async fn own_redis() -> (
    testcontainers::ContainerAsync<Redis>,
    String,
    redissun::Client,
) {
    let container = Redis::default().with_tag("7.4").start().await.unwrap();
    let port = container.get_host_port_ipv4(6379).await.unwrap();
    let url = format!("redis://127.0.0.1:{port}");
    for _ in 0..50 {
        if let Ok(client) = redissun::Client::builder().url(url.clone()).build().await {
            return (container, url, client);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("could not connect to redis");
}

/// Opens the connection that the next batch of `client` uses, so the batch is written before Redis stops answering.
async fn open_batch_connection(client: &redissun::Client) {
    let batch = client.batch();
    drop(batch.atomic_i64(unique("batch")).incr());
    batch.execute().await.unwrap();
}

#[tokio::test]
async fn a_response_timeout_fails_the_execution_after_the_retries() {
    let (container, url, client) = own_redis().await;
    open_batch_connection(&client).await;
    let batch = client
        .batch()
        .response_timeout(Duration::from_millis(100))
        .retry_attempts(2)
        .retry_interval(Duration::from_millis(150));
    let name = unique("batch");
    let handle = batch.atomic_i64(name.clone()).incr();
    let _reads: std::vec::Vec<_> = (0..5000)
        .map(|_| batch.atomic_i64(name.clone()).get())
        .collect();
    assert_eq!(raw_reply(&url, "CLIENT PAUSE 10000").await, "+OK");
    let started = std::time::Instant::now();
    assert!(matches!(batch.execute().await, Err(Error::Timeout)));
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_millis(300), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(3), "{elapsed:?}");
    assert!(matches!(handle.await, Err(Error::Timeout)));
    drop(client);
    container.rm().await.unwrap();
}

#[tokio::test]
async fn by_default_a_batch_is_retried_four_times_with_jitter_like_redisson() {
    let (container, url, client) = own_redis().await;
    open_batch_connection(&client).await;
    let batch = client.batch().response_timeout(Duration::from_millis(10));
    let name = unique("batch");
    let handle = batch.atomic_i64(name.clone()).incr();
    let _reads: std::vec::Vec<_> = (0..5000)
        .map(|_| batch.atomic_i64(name.clone()).get())
        .collect();
    assert_eq!(raw_reply(&url, "CLIENT PAUSE 30000").await, "+OK");
    let started = std::time::Instant::now();
    assert!(matches!(batch.execute().await, Err(Error::Timeout)));
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_millis(3500), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(10), "{elapsed:?}");
    assert!(matches!(handle.await, Err(Error::Timeout)));
    drop(client);
    container.rm().await.unwrap();
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
    let client = connect_with(|builder| builder.pool_size(1)).await;
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
    let client =
        connect_with(|builder| builder.ping_connection_interval(Duration::from_millis(100))).await;
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

#[tokio::test]
async fn atomic_batch_discard() {
    let client = connect_with(|builder| builder.pool_size(1)).await;
    let key = unique("key");
    client
        .bucket::<String>(key.clone())
        .set("value")
        .await
        .unwrap();

    let batch = client.batch().stored_in_redis();
    drop(batch.bucket::<String>(key.clone()).get());
    drop(batch.bucket::<String>(key.clone()).set("changed"));
    batch.discard().await.unwrap();

    assert_eq!(
        client
            .bucket::<String>(key.clone())
            .get()
            .await
            .unwrap()
            .as_deref(),
        Some("value")
    );

    let next = client.batch().stored_in_redis();
    let read = next.bucket::<String>(key).get();
    assert_eq!(next.execute().await.unwrap().commands, 1);
    assert_eq!(read.await.unwrap().as_deref(), Some("value"));
}

#[tokio::test]
async fn ping_during_long_batch_execution() {
    for skip_result in [true, false] {
        let client = connect_with(|builder| {
            builder
                .pool_size(1)
                .ping_connection_interval(Duration::from_millis(50))
                .timeout(Duration::from_millis(50))
        })
        .await;
        let mut batch = client
            .batch()
            .response_timeout(Duration::from_secs(5))
            .retry_attempts(0);
        if skip_result {
            batch = batch.skip_result();
        }
        let prefix = unique("pingDuringBatch");
        let size = 300_000;
        for i in 0..size {
            drop(batch.bucket::<String>(format!("{prefix}:{i}")).set("v"));
        }
        batch.execute().await.unwrap();

        assert!(exists_soon(&client, format!("{prefix}:0")).await);
        assert!(exists_soon(&client, format!("{prefix}:{}", size - 1)).await);
    }
}

/// `exists` with the 50 ms timeout of the client above. Queueing 300000 commands keeps the test's only thread busy for longer than that, so a ping or this call can time out for a reason that is not Redis; it is asked again until Redis answers.
async fn exists_soon(client: &redissun::Client, name: String) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match client.bucket::<String>(name.clone()).exists().await {
            Ok(exists) => return exists,
            Err(error) => assert!(
                tokio::time::Instant::now() < deadline,
                "redis does not answer after the batch: {error:?}"
            ),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn batch_ping_while_queued() {
    let name = unique("batch-ping");
    let client = connect_with(|builder| {
        builder
            .ping_connection_interval(Duration::from_millis(100))
            .client_name(name.clone())
    })
    .await;
    let key = unique("test");
    client
        .bucket::<String>(key.clone())
        .set("1232")
        .await
        .unwrap();

    let batch = client.batch().stored_in_redis();
    let read = batch.bucket::<String>(key).get();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(batch.execute().await.unwrap().commands, 1);
    assert_eq!(read.await.unwrap().as_deref(), Some("1232"));

    let url = redis_url().await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let clients = clients_named(&url, &name).await;
        if !clients.is_empty() && clients.iter().all(|line| !line.contains("cmd=exec")) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "a connection of the batch is not pinged after EXEC: {clients:#?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn batch_ping_after_failed_discard() {
    let container = Redis::default().with_tag("7.4").start().await.unwrap();
    let port = container.get_host_port_ipv4(6379).await.unwrap();
    let url = format!("redis://127.0.0.1:{port}");
    let name = "discard-ping";
    let mut connected = None;
    for _ in 0..50 {
        let built = redissun::Client::builder()
            .url(url.clone())
            .ping_connection_interval(Duration::from_millis(100))
            .timeout(Duration::from_millis(200))
            .retry_attempts(0)
            .client_name(name)
            .build()
            .await;
        if let Ok(client) = built {
            connected = Some(client);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let client = connected.expect("could not connect to redis");

    let batch = client.batch().stored_in_redis();
    drop(batch.bucket::<String>("test").set("1"));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(raw_reply(&url, "CLIENT PAUSE 1000").await, "+OK");
    assert!(batch.discard().await.is_err());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let clients = clients_named(&url, name).await;
        if clients.iter().all(|line| !line.contains("cmd=discard")) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "a connection is left after a failed DISCARD: {clients:#?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    drop(client);
    container.rm().await.unwrap();
}

/// The connection of a batch signs in like the other connections of the client: with its password and in its database.
#[tokio::test]
async fn batches_use_the_password_and_the_database_of_the_client() {
    let (container, url, plain) = own_redis().await;
    assert_eq!(
        raw_reply(&url, "CONFIG SET requirepass secret").await,
        "+OK"
    );
    drop(plain);
    let client = redissun::Client::builder()
        .url(url)
        .password("secret")
        .database(3)
        .build()
        .await
        .unwrap();
    for batch in [
        client.batch(),
        client.batch().atomic(),
        client.batch().stored_in_redis(),
    ] {
        let name = unique("batch");
        let written = batch.atomic_i64(name.clone()).add_and_get(7);
        assert_eq!(batch.execute().await.unwrap().commands, 1);
        assert_eq!(written.await.unwrap(), 7);
        assert_eq!(client.atomic_i64(name).get().await.unwrap(), 7);
    }
    drop(client);
    container.rm().await.unwrap();
}

/// The connection of a batch speaks TLS like the other connections of the client, also with a client certificate.
#[cfg(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc"))]
#[tokio::test]
async fn batches_work_over_tls() {
    use common::tls::{Authority, TlsRedis};
    let authority = Authority::new("redissun batch test CA");
    let server = authority.server(&["127.0.0.1"]);
    let identity = authority.client();
    let redis = TlsRedis::start(&authority, &server, true).await;
    let mut connected = None;
    for _ in 0..50 {
        let built = redissun::Client::builder()
            .url(redis.url())
            .tls_ca_pem(authority.pem.clone())
            .tls_client_auth_pem(identity.cert.clone(), identity.key.clone())
            .build()
            .await;
        if let Ok(client) = built {
            connected = Some(client);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let client = connected.expect("could not connect to redis over TLS");
    for batch in [
        client.batch(),
        client.batch().atomic(),
        client.batch().stored_in_redis(),
    ] {
        let counter = batch.atomic_i64(unique("batch"));
        let handles: Vec<_> = (0..2000).map(|_| counter.incr()).collect();
        assert_eq!(batch.execute().await.unwrap().commands, 2000);
        let mut last = 0;
        for handle in handles {
            last = handle.await.unwrap();
        }
        assert_eq!(last, 2000);
    }
}

mod cluster {
    use super::common::topology::Topology;
    use super::common::unique;
    use redissun::{Client, DelayStrategy};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    // Each of these tests starts its own cluster and some of them are timed, as in Redisson, where the tests of one class run one after another.
    static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
        let mut last = None;
        for _ in 0..30 {
            match Client::builder().url(topology.cluster_url()).build().await {
                Ok(client) => return client,
                Err(error) => last = Some(error),
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        panic!("could not connect to the cluster: {last:?}");
    }

    #[tokio::test]
    async fn memory_atomic_in_cluster() {
        let _turn = ONE_AT_A_TIME.lock().await;
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
    async fn sync_slaves_aof() {
        let _turn = ONE_AT_A_TIME.lock().await;
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
    async fn sync_slaves() {
        let _turn = ONE_AT_A_TIME.lock().await;
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
    async fn atomic_sync_slaves() {
        let _turn = ONE_AT_A_TIME.lock().await;
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
    async fn atomic_and_stored_batches_span_several_nodes() {
        let _turn = ONE_AT_A_TIME.lock().await;
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

    async fn wait_for_replicas(topology: &Topology) {
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
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "replicas did not come online"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    async fn masters(topology: &Topology) -> Vec<u16> {
        let mut masters = Vec::new();
        for port in topology.ports() {
            let info = topology.cli(port, &["INFO", "replication"]).await;
            if info.contains("role:master") {
                masters.push(port);
            }
        }
        masters
    }

    async fn slot_migration(stored: bool) {
        let _turn = ONE_AT_A_TIME.lock().await;
        let topology = Topology::cluster().await;
        let client = connect(&topology).await;
        let name = "test{kaO}";
        let slot = topology
            .cli(topology.base, &["CLUSTER", "KEYSLOT", name])
            .await;
        let slot = slot.trim();
        let masters = masters(&topology).await;
        let source = topology.cluster_master_of(name).await;
        let destination = *masters.iter().find(|port| **port != source).unwrap();
        let source_id = topology.cli(source, &["CLUSTER", "MYID"]).await;
        let destination_id = topology.cli(destination, &["CLUSTER", "MYID"]).await;
        let (source_id, destination_id) = (source_id.trim(), destination_id.trim());

        let batch = if stored {
            client.batch().stored_in_redis()
        } else {
            client.batch()
        };
        let map = batch.hash_map::<String, i64>(name);
        let handles: Vec<_> = (0..5i64).map(|i| map.insert(&i.to_string(), &i)).collect();

        topology
            .cli(
                destination,
                &["CLUSTER", "SETSLOT", slot, "IMPORTING", source_id],
            )
            .await;
        topology
            .cli(
                source,
                &["CLUSTER", "SETSLOT", slot, "MIGRATING", destination_id],
            )
            .await;
        let keys = topology
            .cli(source, &["CLUSTER", "GETKEYSINSLOT", slot, "100"])
            .await;
        let keys: Vec<&str> = keys.lines().filter(|key| !key.trim().is_empty()).collect();
        if !keys.is_empty() {
            let port = destination.to_string();
            let mut args = vec!["MIGRATE", "127.0.0.1", &port, "", "0", "2000", "KEYS"];
            args.extend(keys);
            topology.cli(source, &args).await;
        }
        for master in &masters {
            topology
                .cli(
                    *master,
                    &["CLUSTER", "SETSLOT", slot, "NODE", destination_id],
                )
                .await;
        }
        assert_eq!(topology.cluster_master_of(name).await, destination);
        tokio::time::sleep(Duration::from_secs(2)).await;

        batch.execute().await.unwrap();
        for handle in handles {
            let _ = tokio::time::timeout(Duration::from_millis(1), handle)
                .await
                .expect("a handle was not resolved by execute");
        }
    }

    #[tokio::test]
    async fn slot_migration_in_cluster() {
        slot_migration(false).await;
    }

    #[tokio::test]
    async fn slot_migration_in_cluster_in_stored_mode() {
        slot_migration(true).await;
    }

    async fn execute_batch(client: &Client) -> redissun::Result<redissun::BatchResult> {
        let batch = client.batch();
        for i in 0..100_000 {
            drop(batch.bucket::<String>(i.to_string()).get());
        }
        batch.execute().await
    }

    /// Behind sentinels the connection of a batch goes to the master that the client uses.
    #[tokio::test]
    async fn batches_work_through_sentinel() {
        let _turn = ONE_AT_A_TIME.lock().await;
        let topology = Topology::sentinel().await;
        let client = Client::builder()
            .url(topology.sentinel_url())
            .build()
            .await
            .expect("could not connect through the sentinels");
        for batch in [
            client.batch(),
            client.batch().atomic(),
            client.batch().stored_in_redis(),
        ] {
            let name = unique("batch");
            let counter = batch.atomic_i64(name.clone());
            let first = counter.incr();
            let second = counter.add_and_get(10);
            assert_eq!(batch.execute().await.unwrap().commands, 2);
            assert_eq!(first.await.unwrap(), 1);
            assert_eq!(second.await.unwrap(), 11);
            assert_eq!(client.atomic_i64(name).get().await.unwrap(), 11);
        }
    }

    /// A slot that is being moved answers `ASK` for a key that is not on the old node any more. As in Redisson, a transaction then goes to the node that imports the slot, after `ASKING`. A pipelined batch is not covered: fred 10.1 sends a single command back to the old node after `ASKING`.
    #[tokio::test]
    async fn ask_is_followed_in_the_atomic_modes() {
        let _turn = ONE_AT_A_TIME.lock().await;
        let topology = Topology::cluster().await;
        let client = connect(&topology).await;
        let slot = topology
            .cli(topology.base, &["CLUSTER", "KEYSLOT", "{ask}"])
            .await;
        let slot = slot.trim();
        let masters = masters(&topology).await;
        let source = topology.cluster_master_of("{ask}").await;
        let destination = *masters.iter().find(|port| **port != source).unwrap();
        let source_id = topology.cli(source, &["CLUSTER", "MYID"]).await;
        let destination_id = topology.cli(destination, &["CLUSTER", "MYID"]).await;
        let (source_id, destination_id) = (source_id.trim(), destination_id.trim());
        let importing = ["CLUSTER", "SETSLOT", slot, "IMPORTING", source_id];
        assert_eq!(topology.cli(destination, &importing).await.trim(), "OK");
        let migrating = ["CLUSTER", "SETSLOT", slot, "MIGRATING", destination_id];
        assert_eq!(topology.cli(source, &migrating).await.trim(), "OK");

        let modes = [
            ("atomic", client.batch().atomic()),
            ("stored", client.batch().stored_in_redis()),
        ];
        for (mode, batch) in modes {
            let counter = batch.atomic_i64(format!("{{ask}}{mode}"));
            let first = counter.incr();
            let second = counter.add_and_get(10);
            let read = counter.get();
            let result = batch.execute().await;
            assert_eq!(result.unwrap().commands, 3, "{mode}");
            assert_eq!(first.await.unwrap(), 1, "{mode}");
            assert_eq!(second.await.unwrap(), 11, "{mode}");
            assert_eq!(read.await.unwrap(), 11, "{mode}");
        }
        assert_eq!(topology.cli(source, &["DBSIZE"]).await.trim(), "0");
        assert_eq!(topology.cli(destination, &["DBSIZE"]).await.trim(), "2");

        for master in &masters {
            let node = ["CLUSTER", "SETSLOT", slot, "NODE", destination_id];
            topology.cli(*master, &node).await;
        }
        assert_eq!(topology.cluster_master_of("{ask}").await, destination);
        for mode in ["atomic", "stored"] {
            let counter = client.atomic_i64(format!("{{ask}}{mode}"));
            assert_eq!(counter.get().await.unwrap(), 11, "{mode}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn connection_leak() {
        let _turn = ONE_AT_A_TIME.lock().await;
        let topology = Topology::cluster().await;
        let client = Client::builder()
            .url(topology.cluster_url())
            .connect_timeout(Duration::from_millis(500))
            .ping_connection_interval(Duration::from_secs(2))
            .pool_size(1)
            .timeout(Duration::from_millis(100))
            .retry_attempts(0)
            .retry_delay(DelayStrategy::Constant(Duration::from_millis(20)))
            .build()
            .await
            .expect("could not connect to the cluster");

        let has_errors = Arc::new(AtomicBool::new(false));
        let mut tasks = Vec::new();
        for _ in 0..5 * 15 {
            let client = client.clone();
            let has_errors = has_errors.clone();
            tasks.push(tokio::spawn(async move {
                if execute_batch(&client).await.is_err() {
                    has_errors.store(true, Ordering::SeqCst);
                }
            }));
        }
        // Redisson waits 14 seconds, but here the other tests of this file run at the same time on a debug build and a slow machine needs longer. A leaked connection never finishes, so a longer limit still catches it.
        tokio::time::timeout(Duration::from_secs(60), async {
            for task in tasks {
                task.await.unwrap();
            }
        })
        .await
        .expect("the batches did not finish");
        assert!(has_errors.load(Ordering::SeqCst));

        execute_batch(&client).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failover() {
        let _turn = ONE_AT_A_TIME.lock().await;
        let topology = Topology::cluster().await;
        wait_for_replicas(&topology).await;
        let client = connect(&topology).await;
        let keys_amount = 100_000;

        let task = {
            let client = client.clone();
            tokio::spawn(async move {
                let batch = client.batch().retry_attempts(20);
                for i in 0..keys_amount {
                    let key = i.to_string();
                    drop(batch.bucket::<String>(key.clone()).set(&key));
                }
                batch.execute().await
            })
        };

        topology.cluster_failover_of("0").await;

        let outcome = tokio::time::timeout(Duration::from_secs(60), task)
            .await
            .expect("the batch did not finish after the failover")
            .unwrap();
        assert_eq!(outcome.unwrap().commands, keys_amount);
        let mut count = 0;
        for master in masters(&topology).await {
            let size = topology.cli(master, &["DBSIZE"]).await;
            count += size.trim().parse::<usize>().unwrap();
        }
        assert_eq!(count, keys_amount);
    }
}
