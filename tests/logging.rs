//! redissun logs through `tracing` where Redisson logs through SLF4J, at the same levels.

mod common;

use common::logs::capture;
use common::switch::Switch;
use common::topology::Topology;
use common::{redis_url, unique};
use redissun::{Client, DelayStrategy, ReadMode};
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::redis::Redis;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::sleep;

const WAIT: Duration = Duration::from_secs(10);

async fn through(switch: &Switch) -> redissun::ClientBuilder {
    Client::builder()
        .url(format!("redis://127.0.0.1:{}", switch.port))
        .reconnection_delay(DelayStrategy::Constant(Duration::from_millis(50)))
        .ping_connection_interval(Duration::ZERO)
}

/// A Redis of its own, started with `args`, for a test that pauses it or sets a password.
async fn own_redis(args: &[&str]) -> (ContainerAsync<Redis>, u16) {
    let mut cmd = vec!["redis-server".to_string()];
    cmd.extend(args.iter().map(|arg| arg.to_string()));
    let container = Redis::default()
        .with_tag("7.4")
        .with_cmd(cmd)
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(6379).await.unwrap();
    for _ in 0..100 {
        if let Ok(mut stream) = tokio::net::TcpStream::connect(("127.0.0.1", port)).await {
            let mut reply = [0u8; 1];
            if stream.write_all(b"PING\r\n").await.is_ok()
                && stream.read(&mut reply).await.is_ok_and(|read| read == 1)
            {
                return (container, port);
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("redis on port {port} never answered");
}

// Redisson: RedisExecutor, "attempt {} for {} to {}" at debug.
#[tokio::test]
async fn a_command_sent_again_logs_each_attempt() {
    let (_guard, logs) = capture();
    let switch = Switch::start().await;
    let client = through(&switch)
        .await
        .retry_attempts(2)
        .timeout(Duration::ZERO)
        .build()
        .await
        .unwrap();
    let bucket = client.bucket::<String>(unique("attempts"));
    bucket.set("before").await.unwrap();
    switch.off();
    assert!(bucket.set("after").await.is_err());
    assert_eq!(
        logs.lines_with("attempt 1 for SET").len(),
        1,
        "{}",
        logs.text()
    );
    assert_eq!(
        logs.lines_with("attempt 2 for SET").len(),
        1,
        "{}",
        logs.text()
    );
    assert!(logs.lines_with("attempt 1 for SET")[0].contains("DEBUG"));
}

// Redisson: ConnectionWatchdog, "reconnecting {} to {}" and "{} connected to {}" at debug.
#[tokio::test]
async fn a_lost_connection_logs_its_reconnect() {
    let (_guard, logs) = capture();
    let switch = Switch::start().await;
    let client = through(&switch)
        .await
        .timeout(Duration::from_secs(5))
        .build()
        .await
        .unwrap();
    let bucket = client.bucket::<String>(unique("reconnect"));
    bucket.set("before").await.unwrap();
    switch.off();
    sleep(Duration::from_millis(100)).await;
    let write = tokio::spawn(async move { bucket.set("after").await });
    sleep(Duration::from_millis(300)).await;
    switch.on();
    write.await.unwrap().unwrap();
    let address = format!("127.0.0.1:{}", switch.port);
    assert!(
        logs.contains(&format!("reconnecting to {address}")),
        "{}",
        logs.text()
    );
    assert!(
        logs.contains(&format!("connected to {address}")),
        "{}",
        logs.text()
    );
}

// Redisson: CommandEncoder logs every message at trace.
#[tokio::test]
async fn every_command_is_logged_at_trace() {
    let (_guard, logs) = capture();
    let client = common::client().await;
    let address = redis_url().await.trim_start_matches("redis://").to_string();
    let _: Option<String> = client
        .bucket::<String>(unique("trace"))
        .get()
        .await
        .unwrap();
    let lines = logs.lines_with(&format!("command GET to {address}"));
    assert!(
        lines.iter().any(|line| line.contains("TRACE")),
        "{}",
        logs.text()
    );
}

// Redisson: ClusterConnectionManager, "Can't update cluster state using nodes: {}" at error.
#[tokio::test]
async fn a_cluster_whose_seeds_do_not_answer_logs_the_failed_update() {
    let (_guard, logs) = capture();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let built = Client::builder()
        .url(format!("redis-cluster://127.0.0.1:{port}"))
        .connect_timeout(Duration::from_secs(2))
        .build()
        .await;
    assert!(built.is_err());
    let lines = logs.lines_with("Can't update cluster state");
    assert!(
        lines.iter().any(|line| line.contains("ERROR")),
        "{}",
        logs.text()
    );
}

// Redisson: PingConnectionHandler, "channel: {} closed due to PING response timeout set in {} ms" at debug.
#[tokio::test]
async fn a_ping_without_a_reply_logs_the_closed_connection() {
    let (_guard, logs) = capture();
    let (_redis, port) = own_redis(&[]).await;
    let client = Client::builder()
        .url(format!("redis://127.0.0.1:{port}"))
        .ping_connection_interval(Duration::from_millis(200))
        .timeout(Duration::from_millis(300))
        .build()
        .await
        .unwrap();
    let _: Option<String> = client.bucket::<String>("pinged").get().await.unwrap();
    let mut admin = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    admin
        .write_all(b"*3\r\n$6\r\nCLIENT\r\n$5\r\nPAUSE\r\n$4\r\n3000\r\n")
        .await
        .unwrap();
    let needle =
        format!("connection to 127.0.0.1:{port} closed due to PING response timeout set in 300 ms");
    assert!(logs.wait_for(&needle, WAIT).await, "{}", logs.text());
}

// Redisson: BaseConnectionHandler, "channel: {} closed due to AUTH error response" at debug.
#[tokio::test]
async fn a_rejected_login_logs_the_closed_connection() {
    let (_guard, logs) = capture();
    let (_redis, port) = own_redis(&["--requirepass", "secret"]).await;
    let built = Client::builder()
        .url(format!("redis://127.0.0.1:{port}"))
        .password("wrong")
        .connect_timeout(Duration::from_secs(2))
        .build()
        .await;
    assert!(built.is_err());
    let needle = format!("connection to 127.0.0.1:{port} closed due to AUTH error response");
    assert!(logs.contains(&needle), "{}", logs.text());
}

// Redisson: BaseConnectionHandler, "Renewal cycle failed, retrying" at warn.
#[tokio::test]
async fn a_failed_credentials_renewal_logs_a_warning() {
    let (_guard, logs) = capture();
    let sealed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seal = sealed.clone();
    let client = common::connect_with(move |builder| {
        let seal = seal.clone();
        builder
            .credentials_resolver(move |_address| {
                let sealed = seal.load(std::sync::atomic::Ordering::SeqCst);
                async move {
                    if sealed {
                        Err(redissun::Error::Redis("the vault is sealed".into()))
                    } else {
                        Ok(redissun::Credentials::new("default", "any"))
                    }
                }
            })
            .credentials_refresh_interval(Duration::from_millis(100))
    })
    .await;
    let _: Option<String> = client
        .bucket::<String>(unique("renew"))
        .get()
        .await
        .unwrap();
    sealed.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(
        logs.wait_for("Renewal cycle failed, retrying", WAIT).await,
        "{}",
        logs.text()
    );
    assert!(logs.lines_with("Renewal cycle failed")[0].contains("WARN"));
}

// Redisson: RenewalTask, "Can't update locks {} expiration" at error.
#[tokio::test]
async fn a_lock_whose_lease_cannot_be_renewed_logs_an_error() {
    let (_guard, logs) = capture();
    let switch = Switch::start().await;
    let client = through(&switch)
        .await
        .lock_lease(Duration::from_millis(300))
        .retry_attempts(0)
        .timeout(Duration::from_millis(200))
        .build()
        .await
        .unwrap();
    let name = unique("renewed");
    let lock = client.lock(name.as_str());
    let _held = lock.lock().await.unwrap();
    switch.off();
    let needle = format!("Can't update lock {name} expiration");
    assert!(logs.wait_for(&needle, WAIT).await, "{}", logs.text());
    assert!(logs.lines_with(&needle)[0].contains("ERROR"));
}

// Redisson: PublishSubscribeService, "listeners of '{}' channel have been resubscribed to '{}'" at info.
#[tokio::test]
async fn a_restored_subscription_logs_where_it_went() {
    let (_guard, logs) = capture();
    let switch = Switch::start().await;
    let client = through(&switch)
        .await
        .timeout(Duration::from_secs(5))
        .build()
        .await
        .unwrap();
    let channel = unique("restored");
    let topic = client.topic::<String>(channel.as_str());
    let _subscriber = topic.subscribe().await.unwrap();
    switch.off();
    sleep(Duration::from_millis(300)).await;
    switch.on();
    let needle = format!(
        "listeners of '{channel}' channel have been resubscribed to '127.0.0.1:{}'",
        switch.port
    );
    assert!(logs.wait_for(&needle, WAIT).await, "{}", logs.text());
}

// Redisson: ClusterConnectionManager, "master: {} added for slot ranges: {}" at info.
#[tokio::test]
async fn a_cluster_logs_each_master_it_finds() {
    let (_guard, logs) = capture();
    let cluster = Topology::cluster().await;
    let _client = Client::builder()
        .url(cluster.cluster_url())
        .build()
        .await
        .unwrap();
    let lines = logs.lines_with("added for slot ranges");
    assert_eq!(lines.len(), 3, "{}", logs.text());
    assert!(lines
        .iter()
        .all(|line| line.contains("INFO") && line.contains("master: 127.0.0.1:")));
}

// Redisson: ClusterConnectionManager, "slave: {} added for master {} slot ranges: {}" at info.
#[tokio::test]
async fn a_cluster_read_from_replicas_logs_each_replica_it_finds() {
    let (_guard, logs) = capture();
    let cluster = Topology::cluster().await;
    let _client = Client::builder()
        .url(cluster.cluster_url())
        .read_mode(ReadMode::Replica)
        .replica_scan_interval(Duration::from_millis(200))
        .build()
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while logs.lines_with("added for master").len() < 3 {
        assert!(tokio::time::Instant::now() < deadline, "{}", logs.text());
        sleep(Duration::from_millis(100)).await;
    }
    assert!(logs
        .lines_with("added for master")
        .iter()
        .all(|line| line.contains("INFO") && line.contains("replica: 127.0.0.1:")));
}

// Redisson: MasterSlaveEntry, "ReadMode = {}, but slave nodes are not found!" at warn.
#[tokio::test]
async fn reading_from_replicas_that_are_not_there_logs_a_warning() {
    let (_guard, logs) = capture();
    let cluster = Topology::cluster_with_unlisted_replicas().await;
    let client = Client::builder()
        .url(cluster.cluster_url())
        .read_mode(ReadMode::Replica)
        .build()
        .await
        .unwrap();
    let _: Option<String> = client
        .bucket::<String>(unique("lonely"))
        .get()
        .await
        .unwrap();
    let lines = logs.lines_with("but replica nodes are not found");
    assert!(!lines.is_empty(), "{}", logs.text());
    assert!(lines
        .iter()
        .all(|line| line.contains("WARN") && line.contains("ReadMode = Replica")));
}

// Redisson: SentinelConnectionManager, "slave: {} added" at info and "slave: {} is down" at warn;
// MasterSlaveEntry, "master {} has been changed to {}" at info.
#[tokio::test]
async fn a_sentinel_failover_logs_the_replicas_and_the_new_master() {
    let (_guard, logs) = capture();
    let sentinel = Topology::sentinel().await;
    let client = Client::builder()
        .url(sentinel.sentinel_url())
        .read_mode(ReadMode::Replica)
        .replica_scan_interval(Duration::from_millis(200))
        .timeout(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    let bucket = client.bucket::<String>(unique("failover"));
    bucket.set("before").await.unwrap();
    let deadline = tokio::time::Instant::now() + WAIT;
    while logs
        .lines_with("added")
        .iter()
        .filter(|line| line.contains("replica: 127.0.0.1:"))
        .count()
        < 2
    {
        assert!(tokio::time::Instant::now() < deadline, "{}", logs.text());
        sleep(Duration::from_millis(100)).await;
    }
    let (old, new) = sentinel.sentinel_failover().await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while bucket.set("after").await.is_err() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the write never went through"
        );
        sleep(Duration::from_millis(200)).await;
    }
    let needle = format!("master 127.0.0.1:{old} has been changed to 127.0.0.1:{new}");
    assert!(logs.contains(&needle), "{}", logs.text());
    let replica = sentinel
        .ports()
        .take(3)
        .find(|port| *port != old && *port != new)
        .unwrap();
    let added = format!("replica: 127.0.0.1:{replica} added");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while logs.lines_with(&added).len() < 2 {
        assert!(tokio::time::Instant::now() < deadline, "{}", logs.text());
        let _: Option<String> = bucket.get().await.unwrap_or(None);
        sleep(Duration::from_millis(100)).await;
    }
    sentinel.kill(replica).await;
    let down = format!("replica: 127.0.0.1:{replica} is down");
    assert!(
        logs.wait_for(&down, Duration::from_secs(30)).await,
        "{}",
        logs.text()
    );
    assert!(logs.lines_with(&down)[0].contains("WARN"));
}
