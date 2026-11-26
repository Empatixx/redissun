mod common;

use common::topology::Topology;
use common::{connect_with, unique};
use redissun::{Client, ReadMode};
use std::time::Duration;
use tokio::time::{sleep, Instant};

const READS: u64 = 20;

/// How many times the node on `port` ran `command`, from `INFO commandstats`.
async fn calls(topology: &Topology, port: u16, command: &str) -> u64 {
    let stats = topology.cli(port, &["INFO", "commandstats"]).await;
    let prefix = format!("cmdstat_{command}:calls=");
    stats
        .lines()
        .find_map(|line| line.trim().strip_prefix(prefix.as_str()))
        .and_then(|rest| rest.split(',').next())
        .and_then(|count| count.parse().ok())
        .unwrap_or(0)
}

/// The ports of the replicas of the master on `port`, from its `INFO replication`.
async fn replicas_of(topology: &Topology, port: u16) -> Vec<u16> {
    let info = topology.cli(port, &["INFO", "replication"]).await;
    info.lines()
        .filter(|line| line.starts_with("slave"))
        .filter_map(|line| {
            line.split(',')
                .find_map(|field| field.strip_prefix("port="))
                .and_then(|port| port.trim().parse().ok())
        })
        .collect()
}

/// The `GET` calls of the master on `master` and of all its replicas together.
async fn get_calls(topology: &Topology, master: u16) -> (u64, u64) {
    let on_master = calls(topology, master, "get").await;
    let mut on_replicas = 0;
    for replica in replicas_of(topology, master).await {
        on_replicas += calls(topology, replica, "get").await;
    }
    (on_master, on_replicas)
}

async fn cluster_client(topology: &Topology, mode: ReadMode) -> Client {
    Client::builder()
        .url(topology.cluster_url())
        .read_mode(mode)
        .build()
        .await
        .expect("could not connect to the cluster")
}

/// Writes a bucket, waits until its replicas have it, reads it `READS` times and returns how many of those `GET`s reached the master and how many the replicas.
async fn read_a_bucket(
    topology: &Topology,
    client: &Client,
    name: &str,
    master: u16,
) -> (u64, u64) {
    let bucket = client.bucket::<String>(name);
    bucket.set("written").await.unwrap();
    topology.wait_until_replicated(master).await;
    let before = get_calls(topology, master).await;
    for _ in 0..READS {
        assert_eq!(bucket.get().await.unwrap().as_deref(), Some("written"));
    }
    let after = get_calls(topology, master).await;
    (after.0 - before.0, after.1 - before.1)
}

async fn cluster_master(topology: &Topology, name: &str) -> u16 {
    topology.cluster_master_of(name).await
}

#[tokio::test]
async fn reads_stay_on_the_master_by_default() {
    let topology = Topology::cluster().await;
    let client = cluster_client(&topology, ReadMode::default()).await;
    let name = unique("read-mode");
    let master = cluster_master(&topology, &name).await;
    let (on_master, on_replicas) = read_a_bucket(&topology, &client, &name, master).await;
    assert_eq!((on_master, on_replicas), (READS, 0));
}

#[tokio::test]
async fn replica_mode_reads_from_replicas_but_keeps_coordination_on_the_master() {
    let topology = Topology::cluster().await;
    let client = cluster_client(&topology, ReadMode::Replica).await;
    let name = unique("read-mode");
    let master = cluster_master(&topology, &name).await;
    let (on_master, on_replicas) = read_a_bucket(&topology, &client, &name, master).await;
    assert_eq!((on_master, on_replicas), (0, READS));

    let semaphore = client.semaphore(unique("read-mode-semaphore"));
    assert!(semaphore.try_set_permits(3).await.unwrap());
    let master = cluster_master(&topology, semaphore_name(&semaphore)).await;
    topology.wait_until_replicated(master).await;
    let before = get_calls(&topology, master).await;
    for _ in 0..READS {
        assert_eq!(semaphore.available_permits().await.unwrap(), 3);
    }
    let after = get_calls(&topology, master).await;
    assert_eq!((after.0 - before.0, after.1 - before.1), (READS, 0));
}

fn semaphore_name(semaphore: &redissun::Semaphore) -> &str {
    redissun::Object::name(semaphore)
}

#[tokio::test]
async fn master_and_replica_mode_splits_reads_between_them() {
    let topology = Topology::cluster().await;
    let client = cluster_client(&topology, ReadMode::MasterAndReplica).await;
    let name = unique("read-mode");
    let master = cluster_master(&topology, &name).await;
    let (on_master, on_replicas) = read_a_bucket(&topology, &client, &name, master).await;
    assert_eq!(on_master + on_replicas, READS);
    assert_eq!(on_master, READS / 2);
}

#[tokio::test]
async fn replica_mode_reads_from_the_replicas_that_sentinel_reports() {
    let topology = Topology::sentinel().await;
    let client = Client::builder()
        .url(topology.sentinel_url())
        .read_mode(ReadMode::Replica)
        .build()
        .await
        .expect("could not connect through sentinel");
    let master = topology.sentinel_master_port().await;
    let name = unique("read-mode");
    let (on_master, on_replicas) = read_a_bucket(&topology, &client, &name, master).await;
    assert_eq!((on_master, on_replicas), (0, READS));
}

#[tokio::test]
async fn replica_mode_reads_from_the_master_when_there_are_no_replicas() {
    let client = connect_with(|builder| builder.read_mode(ReadMode::Replica)).await;
    let bucket = client.bucket::<String>(unique("read-mode"));
    bucket.set("only a master").await.unwrap();
    assert_eq!(
        bucket.get().await.unwrap().as_deref(),
        Some("only a master")
    );
}

#[tokio::test]
async fn a_replica_listed_after_the_client_connected_gets_reads_after_a_scan() {
    let topology = Topology::cluster_with_unlisted_replicas().await;
    assert!(
        !topology.cluster_slots_list_replicas().await,
        "the replicas are listed already, so this test would prove nothing"
    );
    let client = Client::builder()
        .url(topology.cluster_url())
        .read_mode(ReadMode::Replica)
        .replica_scan_interval(Duration::from_millis(300))
        .build()
        .await
        .expect("could not connect to the cluster");
    let name = unique("read-mode");
    let bucket = client.bucket::<String>(name.clone());
    bucket.set("written").await.unwrap();
    let master = topology.cluster_master_of(&name).await;
    topology.wait_until_replicated(master).await;

    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let before = get_calls(&topology, master).await;
        assert_eq!(bucket.get().await.unwrap().as_deref(), Some("written"));
        let after = get_calls(&topology, master).await;
        if after.1 > before.1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "reads never reached the replica that joined CLUSTER SLOTS later"
        );
        sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn a_read_goes_to_the_master_while_its_replica_is_down() {
    let topology = Topology::cluster().await;
    let client = cluster_client(&topology, ReadMode::Replica).await;
    let name = unique("read-mode");
    let master = cluster_master(&topology, &name).await;
    let bucket = client.bucket::<String>(name.clone());
    bucket.set("written").await.unwrap();
    topology.wait_until_replicated(master).await;
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("written"));

    for replica in replicas_of(&topology, master).await {
        topology.kill(replica).await;
    }
    let before = calls(&topology, master, "get").await;
    for _ in 0..READS {
        assert_eq!(bucket.get().await.unwrap().as_deref(), Some("written"));
    }
    let after = calls(&topology, master, "get").await;
    assert_eq!(after - before, READS);
}

/// The value that `merge` and `compute` change must come from the master: a lagging replica would make them lose updates.
#[tokio::test]
async fn replica_mode_computes_from_the_master() {
    let topology = Topology::cluster().await;
    let client = cluster_client(&topology, ReadMode::Replica).await;
    let map = client.hash_map::<String, i64>(unique("read-mode-map"));
    let master = cluster_master(&topology, redissun::Object::name(&map)).await;
    map.insert("k", &0).await.unwrap();
    topology.wait_until_replicated(master).await;
    let mut on_replicas_before = 0;
    for replica in replicas_of(&topology, master).await {
        on_replicas_before += calls(&topology, replica, "hget").await;
    }
    for _ in 0..READS {
        map.merge("k", 1, |a, b| async move { Some(a + b) })
            .await
            .unwrap();
        map.compute("k", |v| async move { v }).await.unwrap();
    }
    let mut on_replicas_after = 0;
    for replica in replicas_of(&topology, master).await {
        on_replicas_after += calls(&topology, replica, "hget").await;
    }
    assert_eq!(on_replicas_after - on_replicas_before, 0);
    assert_eq!(map.get("k").await.unwrap(), Some(READS as i64));
}
