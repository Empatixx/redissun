//! Redisson's `RedisNodes`: the masters, replicas and sentinels of the client, and commands for each of them.

mod common;

use common::switch::Switch;
use common::topology::Topology;
use common::{redis_url, unique};
use redissun::{Client, DelayStrategy, InfoSection, NodeType, ReadMode, SetSlot};
use std::time::{Duration, SystemTime};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::redis::Redis;

async fn own_redis() -> (ContainerAsync<Redis>, Client) {
    let container = Redis::default().with_tag("7.4").start().await.unwrap();
    let port = container.get_host_port_ipv4(6379).await.unwrap();
    let mut last = None;
    for _ in 0..50 {
        match Client::builder()
            .url(format!("redis://127.0.0.1:{port}"))
            .build()
            .await
        {
            Ok(client) => return (container, client),
            Err(error) => last = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("redis on port {port} never answered: {last:?}");
}

fn near_now(time: SystemTime) -> bool {
    let now = SystemTime::now();
    let gap = now
        .duration_since(time)
        .or_else(|_| time.duration_since(now))
        .unwrap();
    gap < Duration::from_secs(5)
}

#[tokio::test]
async fn a_single_server_is_one_master_without_replicas_or_sentinels() {
    let client = common::client().await;
    let nodes = client.nodes();
    let address = redis_url().await.trim_start_matches("redis://").to_string();
    let masters = nodes.masters().await;
    assert_eq!(masters.len(), 1);
    assert_eq!(masters[0].address(), address);
    assert_eq!(masters[0].node_type(), NodeType::Master);
    assert!(nodes.master(&address).await.is_some());
    assert!(nodes.master("127.0.0.1:1").await.is_none());
    assert!(nodes.replicas().await.is_empty());
    assert!(nodes.sentinels().is_empty());
    assert!(nodes.ping_all().await);
}

#[tokio::test]
async fn a_node_answers_info_time_and_size() {
    let client = common::client().await;
    let master = client.nodes().masters().await.remove(0);
    assert!(master.ping().await);
    let server = master.info(InfoSection::Server).await.unwrap();
    assert!(server.contains_key("redis_version"), "{server:?}");
    assert!(!server.contains_key("used_memory"));
    let all = master.info(InfoSection::All).await.unwrap();
    assert!(all.contains_key("used_memory") && all.contains_key("redis_version"));
    assert!(near_now(master.time().await.unwrap()));
    client
        .bucket::<String>(unique("size"))
        .set("x")
        .await
        .unwrap();
    assert!(master.size().await.unwrap() >= 1);
    let memory = master.memory_statistics().await.unwrap();
    assert!(memory.contains_key("peak.allocated"), "{memory:?}");
}

#[tokio::test]
async fn a_node_reads_and_writes_its_config_and_saves() {
    let (_redis, client) = own_redis().await;
    let master = client.nodes().masters().await.remove(0);
    master
        .set_config("maxmemory-policy", "allkeys-lru")
        .await
        .unwrap();
    let config = master.config("maxmemory-*").await.unwrap();
    assert_eq!(
        config.get("maxmemory-policy").map(String::as_str),
        Some("allkeys-lru"),
        "{config:?}"
    );
    master.save().await.unwrap();
    assert!(near_now(master.last_save_time().await.unwrap()));
    master.bg_rewrite_aof().await.unwrap();
    // A rewrite is running, so a background save can only be scheduled.
    master.schedule_bg_save().await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    master.bg_save().await.unwrap();
}

#[tokio::test]
async fn a_node_that_does_not_answer_fails_its_ping() {
    let switch = Switch::start().await;
    let client = Client::builder()
        .url(format!("redis://127.0.0.1:{}", switch.port))
        .reconnection_delay(DelayStrategy::Constant(Duration::from_millis(50)))
        .ping_connection_interval(Duration::ZERO)
        .build()
        .await
        .unwrap();
    let nodes = client.nodes();
    assert!(nodes.ping_all().await);
    switch.off();
    let master = nodes.masters().await.remove(0);
    assert!(!master.ping_within(Duration::from_millis(300)).await);
    assert!(!nodes.ping_all_within(Duration::from_millis(300)).await);
}

#[tokio::test]
async fn a_cluster_lists_its_masters_and_replicas_and_their_cluster_state() {
    let cluster = Topology::cluster().await;
    let client = Client::builder()
        .url(cluster.cluster_url())
        .build()
        .await
        .unwrap();
    let nodes = client.nodes();
    let masters = nodes.masters().await;
    assert_eq!(masters.len(), 3);
    assert!(nodes.ping_all().await);
    if cluster.cluster_slots_list_replicas().await {
        let replicas = nodes.replicas().await;
        assert_eq!(replicas.len(), 3);
        assert!(replicas
            .iter()
            .all(|node| node.node_type() == NodeType::Replica));
        assert!(nodes.replica(replicas[0].address()).await.is_some());
        // A replica serves no slots, so it has none to give up.
        replicas[0].cluster_flush_slots().await.unwrap();
    }
    let master = &masters[0];
    let info = master.cluster_info().await.unwrap();
    assert_eq!(info.get("cluster_state").map(String::as_str), Some("ok"));
    let id = master.cluster_id().await.unwrap();
    assert_eq!(id.len(), 40, "{id}");
    let slots = master.cluster_slots().await.unwrap();
    let covered: u32 = slots
        .keys()
        .map(|range| u32::from(*range.end()) - u32::from(*range.start()) + 1)
        .sum();
    assert_eq!(covered, 16384, "{slots:?}");
    assert_eq!(master.cluster_count_failure_reports(&id).await.unwrap(), 0);
}

#[tokio::test]
async fn a_cluster_node_runs_the_slot_commands() {
    let cluster = Topology::cluster().await;
    let client = Client::builder()
        .url(cluster.cluster_url())
        .build()
        .await
        .unwrap();
    let key = unique("slot");
    client.bucket::<String>(key.clone()).set("x").await.unwrap();
    let port = cluster.cluster_master_of(&key).await;
    let master = client
        .nodes()
        .master(&format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    let slot: u16 = cluster
        .cli(port, &["CLUSTER", "KEYSLOT", &key])
        .await
        .trim()
        .parse()
        .unwrap();
    assert_eq!(master.cluster_count_keys_in_slot(slot).await.unwrap(), 1);
    assert_eq!(
        master.cluster_keys_in_slot(slot, 10).await.unwrap(),
        vec![key.clone()]
    );
    master
        .cluster_set_slot(slot, SetSlot::Stable)
        .await
        .unwrap();
    // The master forgets its slot and takes it back.
    master.cluster_delete_slots(&[slot]).await.unwrap();
    master.cluster_add_slots(&[slot]).await.unwrap();
    assert_eq!(master.cluster_count_keys_in_slot(slot).await.unwrap(), 1);
    let id = master.cluster_id().await.unwrap();
    assert!(master.cluster_forget(&id).await.is_err());
    assert!(master.cluster_replicate(&id).await.is_err());
    master
        .cluster_meet(&format!("127.0.0.1:{port}"))
        .await
        .unwrap();
}

#[tokio::test]
async fn sentinel_lists_its_sentinels_and_answers_for_the_master() {
    let sentinel = Topology::sentinel().await;
    let client = Client::builder()
        .url(sentinel.sentinel_url())
        .read_mode(ReadMode::Replica)
        .build()
        .await
        .unwrap();
    let nodes = client.nodes();
    let sentinels = nodes.sentinels();
    assert_eq!(sentinels.len(), 3);
    let first = &sentinels[0];
    assert!(nodes.sentinel(first.address()).is_some());
    assert!(first.ping().await);
    assert!(first
        .info(InfoSection::Server)
        .await
        .unwrap()
        .contains_key("redis_version"));
    let master = nodes.masters().await.remove(0);
    assert_eq!(
        first.master_addr("mymaster").await.unwrap(),
        Some(master.address().to_string())
    );
    assert_eq!(first.master_addr("unknown").await.unwrap(), None);
    assert_eq!(
        first
            .master("mymaster")
            .await
            .unwrap()
            .get("name")
            .map(String::as_str),
        Some("mymaster")
    );
    assert_eq!(first.masters().await.unwrap().len(), 1);
    assert_eq!(first.replicas("mymaster").await.unwrap().len(), 2);
    assert_eq!(first.sentinels("mymaster").await.unwrap().len(), 2);
    // The replicas are those the client heard of at its last scan (every second), which may predate them being online.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while nodes.replicas().await.len() != 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{:?}",
            nodes.replicas().await
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(nodes.ping_all().await);
    let old = master.address().to_string();
    first.failover("mymaster").await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while first.master_addr("mymaster").await.unwrap() == Some(old.clone()) {
        assert!(tokio::time::Instant::now() < deadline, "no failover");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
