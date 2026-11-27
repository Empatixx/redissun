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
    assert_eq!(bits.count_ones().await.unwrap(), 1);

    let geo = client.geo::<String>(unique("geo"));
    geo.insert(
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
    assert!(filter.init(1000, 0.01).await.unwrap());
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
        .init_rate(RateType::Overall, 2, Duration::from_secs(10))
        .await
        .unwrap();
    assert!(limiter.try_acquire(1).await.unwrap());
    assert!(limiter.try_acquire(1).await.unwrap());
    assert!(!limiter.try_acquire(1).await.unwrap());

    let latch = client.count_down_latch(unique("latch"));
    latch.init_count(1).await.unwrap();
    latch.count_down().await.unwrap();
    latch
        .wait()
        .timeout(Duration::from_secs(5))
        .await
        .unwrap()
        .expect("latch never opened on a cluster");

    let semaphore = client.semaphore(unique("semaphore"));
    semaphore.init_permits(1).await.unwrap();
    let permits = semaphore.acquire().await.unwrap();
    assert!(semaphore.try_acquire().await.unwrap().is_none());
    permits.release().await.unwrap();
}

#[tokio::test]
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
async fn lock_and_data_survive_a_cluster_master_failover() {
    let topology = Topology::cluster().await;
    let holder_client = connect(&topology).await;
    // Taking a lock is sent once, as in Redisson, so a client that must ride out a failover needs a timeout longer than the failover: here at least the 2 s node timeout plus the election.
    let waiter_client = Client::builder()
        .url(topology.cluster_url())
        .lock_lease(Duration::from_secs(5))
        .timeout(Duration::from_secs(15))
        .build()
        .await
        .expect("could not connect to the cluster");
    let name = unique("lock");
    let bucket_name = format!("{{{name}}}:bucket");
    let bucket = holder_client.bucket::<String>(bucket_name.clone());
    bucket.set("before").await.unwrap();

    let holder = holder_client.lock(name.clone()).lock().await.unwrap();
    let master = topology.cluster_master_of(&name).await;
    topology.wait_until_replicated(master).await;

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
async fn scripts_and_functions_work_on_a_cluster() {
    let topology = Topology::cluster().await;
    let client = connect(&topology).await.with_codec(redissun::StringCodec);
    let script = client.script();
    let lua = redissun::LuaScript::new("return redis.call('INCRBY', KEYS[1], ARGV[1])");
    let sha = script.load(lua.source()).await.unwrap();
    assert_eq!(script.exists(&[&sha]).await.unwrap(), [true]);

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

    script.flush().await.unwrap();
    assert_eq!(script.exists(&[&sha]).await.unwrap(), [false]);

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

async fn masters(topology: &Topology) -> std::vec::Vec<u16> {
    let mut masters = std::vec::Vec::new();
    for port in topology.ports() {
        if topology.cli(port, &["ROLE"]).await.starts_with("master") {
            masters.push(port);
        }
    }
    masters
}

/// Redisson: `clientName` names every connection, on every master of a cluster too.
#[tokio::test]
async fn every_cluster_node_connection_gets_the_client_name() {
    let topology = Topology::cluster().await;
    let name = unique("cluster-name").replace(':', "-");
    let client = Client::builder()
        .url(topology.cluster_url())
        .pool_size(2)
        .client_name(name.clone())
        .build()
        .await
        .expect("could not connect to the cluster");
    client
        .bucket::<String>(unique("named"))
        .set("v")
        .await
        .unwrap();

    let masters = masters(&topology).await;
    assert_eq!(masters.len(), 3);
    for port in masters {
        let list = topology.cli(port, &["CLIENT", "LIST"]).await;
        let named = list
            .lines()
            .filter(|line| line.contains(&format!(" name={name} ")))
            .count();
        assert!(
            named >= 2,
            "master {port} has {named} connections named {name}"
        );
    }
}

// Redisson's testExistsInCluster, testDeleteInCluster, testKeysByPattern and testGetInCluster on one fresh cluster, whose keys spread over every master.
#[tokio::test]
async fn keys_and_buckets_span_every_master_of_a_cluster() {
    let topology = Topology::cluster().await;
    let client = connect(&topology).await;
    let keys = client.keys();
    let size = 2000;
    let names: std::vec::Vec<String> = (0..size).map(|i| format!("test{i}")).collect();
    let buckets = client.buckets::<i32>();
    buckets
        .set(
            names
                .iter()
                .enumerate()
                .map(|(i, name)| (name.as_str(), &VALUES[i % 3])),
        )
        .await
        .unwrap();
    assert_eq!(keys.count().await.unwrap(), size as u64);
    assert_eq!(
        keys.count_exists(&["test1", "test2", "test34", "test45", "asdfl;jasf"])
            .await
            .unwrap(),
        4
    );

    let mut queried = names.clone();
    queried.push("test_invalid".into());
    let found = buckets.get(&queried).await.unwrap();
    assert_eq!(found.len(), size);
    assert_eq!(found["test5"], VALUES[5 % 3]);

    let scanned: std::collections::HashSet<String> = keys
        .iter_with(redissun::KeysScan::new().pattern("test*").chunk_size(20))
        .map(|key| key.unwrap())
        .collect()
        .await;
    assert_eq!(scanned.len(), size);
    assert_eq!(
        keys.iter_with(redissun::KeysScan::new().limit(7))
            .count()
            .await,
        7
    );

    // test1 and fresh are in different slots; as in Redisson, each slot is set on its own and one that was set makes it true.
    assert_ne!(keys.slot("test1"), keys.slot("fresh"));
    assert!(buckets
        .try_set([("test1", &1), ("fresh", &2)])
        .await
        .unwrap());
    assert_eq!(
        client.bucket::<i32>("test1").get().await.unwrap(),
        Some(VALUES[1])
    );
    assert_eq!(client.bucket::<i32>("fresh").get().await.unwrap(), Some(2));
    assert_eq!(keys.delete(&names[..1000]).await.unwrap(), 1000);
    assert_eq!(keys.delete_by_pattern("test*").await.unwrap(), 1000);
    assert_eq!(keys.count().await.unwrap(), 1);
    keys.flush_db().await.unwrap();
    assert_eq!(keys.count().await.unwrap(), 0);
    assert_eq!(keys.random_key().await.unwrap(), None);
}

const VALUES: [i32; 3] = [7, 8, 9];

// Redisson's testClusterSharding and testGetInClusterNameMapper of RPermitExpirableSemaphore.
#[tokio::test]
async fn sharded_topics_and_permits_work_on_a_cluster() {
    let topology = Topology::cluster().await;
    let client = connect(&topology).await;
    let mut subscribers = std::vec::Vec::new();
    for i in 0..10 {
        let topic = client.sharded_topic::<i32>(format!("test{i}"));
        subscribers.push(topic.subscribe().await.unwrap());
    }
    for i in 0..10 {
        let topic = client.sharded_topic::<i32>(format!("test{i}"));
        assert_eq!(topic.publish(&i).await.unwrap(), 1);
    }
    for (i, subscriber) in subscribers.iter_mut().enumerate() {
        let message = timeout(Duration::from_secs(5), subscriber.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message, i as i32);
    }
    for i in 0..10 {
        client
            .sharded_topic::<i32>(format!("test{i}"))
            .unsubscribe_all()
            .await;
    }

    let semaphore = client.permit_expirable_semaphore("semaphore");
    semaphore.init_permits(1).await.unwrap();
    let id = semaphore.acquire().await.unwrap();
    semaphore.release(&id).await.unwrap();

    let reliable = client.reliable_topic::<String>("reliable");
    let mut reader = reliable.subscribe().await.unwrap();
    reliable.publish("on a cluster").await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(5), reader.recv())
            .await
            .unwrap()
            .unwrap(),
        "on a cluster"
    );
}

// Redisson's testReattachInClusterMaster: the master of the channel's slot goes down, and the subscriber listens on the replica that takes over.
#[tokio::test]
async fn a_sharded_subscriber_follows_its_slot_to_the_new_master() {
    let topology = Topology::cluster().await;
    let client = connect(&topology).await;
    let topic = client.sharded_topic::<i32>("3");
    let mut subscriber = topic.subscribe().await.unwrap();
    assert_eq!(topic.subscriber_count().await.unwrap(), 1);
    topology.cluster_failover_of("3").await;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        assert!(Instant::now() < deadline, "the subscriber never came back");
        if topic.publish(&1).await.unwrap_or(0) == 1 {
            break;
        }
        sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(
        timeout(Duration::from_secs(5), subscriber.recv())
            .await
            .unwrap()
            .unwrap(),
        1
    );
}
