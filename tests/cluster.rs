mod common;

use common::topology::Topology;
use common::unique;
use futures::StreamExt;
use redissun::{Client, EvictionMode, GeoPoint, Object, RateType, StreamId};
use std::time::Duration;
use tokio::time::{sleep, timeout, Instant};

async fn connect(topology: &Topology) -> Client {
    Client::builder()
        .url(topology.cluster_url())
        .lock_lease(Duration::from_secs(5))
        .build()
        .await
        .expect("could not connect to the cluster")
}

#[tokio::test]
#[ignore = "starts a redis cluster in docker; run with --ignored"]
async fn plain_collections_work_on_a_cluster() {
    let topology = Topology::cluster().await;
    let client = connect(&topology).await;

    let bucket = client.bucket::<String>(unique("bucket"));
    bucket.set("v").await.unwrap();
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("v"));

    let map = client.hash_map::<String, i64>(unique("map"));
    map.insert("a", &1).await.unwrap();
    assert_eq!(map.incr_by("a", 2).await.unwrap(), 3);
    let keys: std::vec::Vec<_> = map.keys().collect().await;
    assert_eq!(keys.len(), 1);

    let list = client.vec::<String>(unique("vec"));
    list.push("a").await.unwrap();
    list.insert(0, "b").await.unwrap();
    assert_eq!(list.len().await.unwrap(), 2);

    let set = client.hash_set::<String>(unique("set"));
    set.insert("a").await.unwrap();
    assert!(set.contains("a").await.unwrap());

    let sorted = client.sorted_set::<String>(unique("zset"));
    sorted.insert("a", 1.0).await.unwrap();
    assert_eq!(
        sorted.pop_first().await.unwrap().map(|(v, _)| v).as_deref(),
        Some("a")
    );

    let counter = client.atomic_i64(unique("counter"));
    assert!(counter.compare_and_set(0, 5).await.unwrap());

    let bits = client.bit_set(unique("bits"));
    bits.set(7, true).await.unwrap();
    assert_eq!(bits.count().await.unwrap(), 1);

    let geo = client.geo::<String>(unique("geo"));
    geo.add(
        "prague",
        GeoPoint {
            longitude: 14.43,
            latitude: 50.07,
        },
    )
    .await
    .unwrap();
    assert_eq!(geo.len().await.unwrap(), 1);
}

#[tokio::test]
#[ignore = "starts a redis cluster in docker; run with --ignored"]
async fn objects_with_several_keys_work_on_a_cluster() {
    let topology = Topology::cluster().await;
    let client = connect(&topology).await;

    let cache = client.hash_map_cache::<String, String>(unique("cache"));
    cache.set_max_size(10, EvictionMode::Lru).await.unwrap();
    let mut events = cache.events().await.unwrap();
    cache
        .insert("a", "1")
        .ttl(Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(cache.get("a").await.unwrap().as_deref(), Some("1"));
    timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("no cache event on a cluster")
        .unwrap();
    assert!(cache.del().await.unwrap());

    let seen = client.hash_set_cache::<String>(unique("seen"));
    seen.insert("a").ttl(Duration::from_secs(60)).await.unwrap();
    assert!(seen.contains("a").await.unwrap());

    let filter = client.bloom_filter::<String>(unique("bloom"));
    assert!(filter.try_init(1000, 0.01).await.unwrap());
    filter.insert("a").await.unwrap();
    assert!(filter.contains("a").await.unwrap());

    let local = client
        .local_cached_map::<String, String>(unique("lcm"))
        .build()
        .await
        .unwrap();
    local.insert("a", "1").await.unwrap();
    assert_eq!(local.get("a").await.unwrap().as_deref(), Some("1"));

    let log = client.stream::<String, String>(unique("stream"));
    log.create_group("g", &StreamId::zero()).await.unwrap();
    log.add([("k", "v")]).await.unwrap();
    let entries = log.read_group("g", "c", None).await.unwrap();
    assert_eq!(entries.len(), 1);

    let limiter = client.rate_limiter(unique("limiter"));
    limiter
        .try_set_rate(RateType::Overall, 2, Duration::from_secs(10))
        .await
        .unwrap();
    assert!(limiter.try_acquire(1).await.unwrap());
    assert!(limiter.try_acquire(1).await.unwrap());
    assert!(!limiter.try_acquire(1).await.unwrap());

    let latch = client.count_down_latch(unique("latch"));
    latch.try_set_count(1).await.unwrap();
    latch.count_down().await.unwrap();
    latch
        .wait()
        .timeout(Duration::from_secs(5))
        .await
        .unwrap()
        .expect("latch never opened on a cluster");

    let semaphore = client.semaphore(unique("semaphore"));
    semaphore.try_set_permits(1).await.unwrap();
    let permits = semaphore.acquire(1).await.unwrap();
    assert!(semaphore.try_acquire(1).await.unwrap().is_none());
    permits.release().await.unwrap();
}

#[tokio::test]
#[ignore = "starts a redis cluster in docker; run with --ignored"]
async fn locks_work_on_a_cluster() {
    let topology = Topology::cluster().await;
    let client = connect(&topology).await;

    for lock in [client.lock(unique("lock"))] {
        let guard = lock.lock().await.unwrap();
        assert!(lock.is_locked().await.unwrap());
        guard.unlock().await.unwrap();
    }

    let fair = client.fair_lock(unique("fair"));
    let guard = fair.lock().await.unwrap();
    guard.unlock().await.unwrap();

    let fenced = client.fenced_lock(unique("fenced"));
    let first = fenced.lock().await.unwrap();
    let token = first.fencing_token().unwrap();
    first.unlock().await.unwrap();
    let second = fenced.lock().await.unwrap();
    assert!(second.fencing_token().unwrap() > token);
    second.unlock().await.unwrap();

    let rw = client.rw_lock(unique("rw"));
    let read = rw.read().await.unwrap();
    assert!(rw.try_write().await.unwrap().is_none());
    read.unlock().await.unwrap();
    let write = rw.write().await.unwrap();
    write.unlock().await.unwrap();

    let names: std::vec::Vec<String> = (0..6).map(|i| unique(&format!("multi{i}"))).collect();
    let multi = client
        .multi_lock(names.iter().map(|name| client.lock(name.clone())))
        .unwrap();
    let guard = multi
        .lock()
        .timeout(Duration::from_secs(10))
        .await
        .unwrap()
        .expect("multi lock over several cluster nodes hung");
    for name in &names {
        assert!(client.lock(name.clone()).is_locked().await.unwrap());
    }
    guard.unlock().await.unwrap();
}

#[tokio::test]
#[ignore = "starts a redis cluster in docker; run with --ignored"]
async fn queues_and_topics_work_on_a_cluster() {
    let topology = Topology::cluster().await;
    let client = connect(&topology).await;

    let jobs = client.vec_deque::<String>(unique("jobs"));
    let consumer = client.vec_deque::<String>(jobs.name().to_string());
    let waiter = tokio::spawn(async move {
        consumer
            .pop_front_wait()
            .timeout(Duration::from_secs(10))
            .await
    });
    sleep(Duration::from_millis(200)).await;
    jobs.push_back("job").await.unwrap();
    assert_eq!(waiter.await.unwrap().unwrap().as_deref(), Some("job"));

    let delayed_target = client.vec_deque::<String>(unique("delayed-target"));
    let delayed = client.delayed_queue(&delayed_target);
    delayed
        .push("later", Duration::from_millis(300))
        .await
        .unwrap();
    let moved = delayed_target
        .pop_front_wait()
        .timeout(Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(moved.as_deref(), Some("later"));

    let topic = client.topic::<String>(unique("topic"));
    let mut subscriber = topic.subscribe().await.unwrap();
    topic.publish("hi").await.unwrap();
    let message = timeout(Duration::from_secs(5), subscriber.recv())
        .await
        .expect("no topic message on a cluster")
        .unwrap();
    assert_eq!(message, "hi");

    let batch = client.batch();
    let counters: std::vec::Vec<_> = (0..6)
        .map(|i| batch.atomic_i64(unique(&format!("batch{i}"))).incr())
        .collect();
    batch.execute().await.unwrap();
    for counter in counters {
        assert_eq!(counter.await.unwrap(), 1);
    }
}

#[tokio::test]
#[ignore = "starts a redis cluster in docker; run with --ignored"]
async fn lock_and_data_survive_a_cluster_master_failover() {
    let topology = Topology::cluster().await;
    let holder_client = connect(&topology).await;
    let waiter_client = connect(&topology).await;
    let name = unique("lock");
    let bucket_name = format!("{{{name}}}:bucket");
    let bucket = holder_client.bucket::<String>(bucket_name.clone());
    bucket.set("before").await.unwrap();

    let holder = holder_client.lock(name.clone()).lock().await.unwrap();
    let master = topology.cluster_master_of(&name).await;
    topology.cli(master, &["WAIT", "1", "2000"]).await;
    sleep(Duration::from_millis(500)).await;

    let waiter_lock = waiter_client.lock(name.clone());
    let waiter = tokio::spawn(async move { waiter_lock.lock().await });
    sleep(Duration::from_millis(300)).await;

    topology.cluster_failover_of(&name).await;

    let deadline = Instant::now() + Duration::from_secs(30);
    let value = loop {
        if let Ok(value) = bucket.get().await {
            break value;
        }
        assert!(
            Instant::now() < deadline,
            "bucket never readable after failover"
        );
        sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(value.as_deref(), Some("before"));
    let _ = holder.unlock().await;

    let guard = timeout(Duration::from_secs(20), waiter)
        .await
        .expect("waiter never got the lock after a cluster failover")
        .unwrap()
        .expect("waiter failed after a cluster failover");
    guard.unlock().await.unwrap();
}

#[tokio::test]
#[ignore = "starts a redis cluster in docker; run with --ignored"]
async fn scripts_and_functions_work_on_a_cluster() {
    let topology = Topology::cluster().await;
    let client = connect(&topology).await.with_codec(redissun::StringCodec);
    let script = client.script();
    let lua = redissun::LuaScript::new("return redis.call('INCRBY', KEYS[1], ARGV[1])");
    let sha = script.script_load(lua.source()).await.unwrap();
    assert_eq!(script.script_exists(&[&sha]).await.unwrap(), [true]);

    let mut masters = std::collections::HashSet::new();
    for n in 0..20 {
        let key = format!("{}-{n}", unique("script"));
        masters.insert(topology.cluster_master_of(&key).await);
        let total: i64 = script.run(&lua).key(key.clone()).arg(&n).await.unwrap();
        assert_eq!(total, n);
        let read: i64 = script
            .eval("return tonumber(redis.call('GET', KEYS[1]))")
            .key(key)
            .read_only()
            .await
            .unwrap();
        assert_eq!(read, n);
    }
    assert!(masters.len() > 1);

    script.script_flush().await.unwrap();
    assert_eq!(script.script_exists(&[&sha]).await.unwrap(), [false]);

    let functions = client.function();
    let code = "#!lua name=clusterlib\nredis.register_function('cluster_incr', function(keys, args) return redis.call('INCRBY', keys[1], args[1]) end)";
    assert_eq!(functions.load(code).await.unwrap(), "clusterlib");
    for n in 0..10 {
        let key = format!("{}-{n}", unique("fcall"));
        let total: i64 = functions
            .call("cluster_incr")
            .key(key)
            .arg(&n)
            .await
            .unwrap();
        assert_eq!(total, n);
    }
    functions.delete("clusterlib").await.unwrap();
}
