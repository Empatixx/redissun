mod common;

use common::{connect_with, raw_command, unique};
use redissun::{DelayStrategy, Error};
use std::time::Duration;
use tokio::sync::Mutex;

static BUSY: Mutex<()> = Mutex::const_new(());

async fn named(name: &str) -> Vec<String> {
    let list = raw_command(&["CLIENT", "LIST"]).await;
    let needle = format!(" name={name} ");
    list.lines()
        .filter(|line| line.contains(&needle))
        .filter_map(|line| line.split_whitespace().next().map(str::to_string))
        .collect()
}

const BUSY_SCRIPT: &str = "local start = redis.call('TIME') \
    while true do \
        local now = redis.call('TIME') \
        if (now[1] - start[1]) * 1000000 + (now[2] - start[2]) > tonumber(ARGV[1]) then return 1 end \
    end";

#[tokio::test]
async fn a_command_fails_after_the_timeout() {
    let client = connect_with(|builder| {
        builder
            .timeout(Duration::from_millis(200))
            .retry_attempts(0)
            .ping_connection_interval(Duration::ZERO)
    })
    .await;
    let _busy = BUSY.lock().await;
    let bucket = client.bucket::<String>(unique("slow"));
    bucket.get().await.unwrap();
    let busy = tokio::spawn(raw_command(&["EVAL", BUSY_SCRIPT, "0", "800000"]));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let started = std::time::Instant::now();
    let outcome = bucket.get().await;
    assert!(matches!(outcome, Err(Error::Redis(_))), "{outcome:?}");
    assert!(started.elapsed() < Duration::from_millis(600));
    busy.await.unwrap();
    bucket.get().await.unwrap();
}

#[tokio::test]
async fn a_silent_connection_is_replaced_after_a_failed_ping() {
    let _busy = BUSY.lock().await;
    let name = unique("ping").replace(':', "-");
    let client = connect_with(|builder| {
        builder
            .pool_size(1)
            .client_name(name.clone())
            .timeout(Duration::from_millis(300))
            .ping_connection_interval(Duration::from_millis(200))
            .reconnection_delay(DelayStrategy::Constant(Duration::from_millis(50)))
    })
    .await;
    let before = named(&name).await;
    assert_eq!(before.len(), 1);
    raw_command(&["EVAL", BUSY_SCRIPT, "0", "1200000"]).await;
    let mut after = Vec::new();
    for _ in 0..50 {
        after = named(&name).await;
        if after.len() == 1 && after != before {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(after.len(), 1);
    assert_ne!(after, before);
    client
        .bucket::<String>(unique("alive"))
        .set("yes")
        .await
        .unwrap();
}
