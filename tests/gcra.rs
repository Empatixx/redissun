//! Tests of `Gcra`, Redisson's `RGcra`.
//!
//! Ported from Redisson 4.8.0 `RedissonGcraTest` (named `redisson_<snake_case>`).

mod common;

use common::{client, raw_command, unique};
use redissun::{Client, Error, Gcra, Object};
use std::time::Duration;

fn gcra(client: &Client) -> Gcra {
    client.gcra(unique("gcra"))
}

fn config_key(name: &str) -> String {
    format!("{{{name}}}:config")
}

fn secs(seconds: u64) -> Duration {
    Duration::from_secs(seconds)
}

/// The integer of a RESP `:n` reply.
fn integer(reply: &str) -> i64 {
    reply.trim_start_matches(':').trim().parse().unwrap()
}

/// The text of a RESP bulk string reply, `None` for a nil.
fn bulk(reply: &str) -> Option<String> {
    let (header, rest) = reply.split_once("\r\n")?;
    if header == "$-1" {
        return None;
    }
    Some(rest.trim_end_matches("\r\n").to_string())
}

#[tokio::test]
async fn redisson_try_set_rate() {
    let gcra = gcra(&client().await);
    assert!(gcra.init_rate(2, 4, secs(1)).await.unwrap());
    assert!(!gcra.init_rate(10, 20, secs(5)).await.unwrap());
    let config = gcra.config().await.unwrap().unwrap();
    assert_eq!(config.max_burst, 2);
    assert_eq!(config.tokens_per_period, 4);
    assert_eq!(config.period, secs(1));
}

#[tokio::test]
async fn redisson_get_config_not_set() {
    let gcra = gcra(&client().await);
    assert!(gcra.config().await.unwrap().is_none());
}

#[tokio::test]
async fn redisson_validation() {
    let gcra = gcra(&client().await);
    match gcra.init_rate(1, 0, secs(1)).await {
        Err(Error::Config(message)) => assert!(message.contains("tokens_per_period"), "{message}"),
        other => panic!("expected a config error, got {other:?}"),
    }
    match gcra.init_rate(1, 1, Duration::ZERO).await {
        Err(Error::Config(message)) => assert!(message.contains("period"), "{message}"),
        other => panic!("expected a config error, got {other:?}"),
    }
    let exists = raw_command(&["EXISTS", &config_key(gcra.name())]).await;
    assert_eq!(integer(&exists), 0);
}

#[tokio::test]
async fn set_rate_replaces_the_config() {
    let gcra = gcra(&client().await);
    gcra.init_rate(2, 4, secs(1)).await.unwrap();
    gcra.set_rate(5, 10, secs(2)).await.unwrap();
    let config = gcra.config().await.unwrap().unwrap();
    assert_eq!(
        (config.max_burst, config.tokens_per_period, config.period),
        (5, 10, secs(2))
    );
}

// Review Focus 5.
#[tokio::test]
async fn config_is_stored_like_redisson() {
    let gcra = gcra(&client().await);
    gcra.init_rate(2, 4, Duration::from_millis(250))
        .await
        .unwrap();
    let config = config_key(gcra.name());
    for (field, value) in [("maxBurst", "2"), ("rate", "4"), ("period", "0.25")] {
        let reply = raw_command(&["HGET", &config, field]).await;
        assert_eq!(bulk(&reply).as_deref(), Some(value), "{field}");
    }
    assert_eq!(integer(&raw_command(&["HLEN", &config]).await), 3);
    assert_eq!(integer(&raw_command(&["EXISTS", gcra.name()]).await), 0);
}

#[tokio::test]
async fn config_reads_redissons_period_strings() {
    let gcra = gcra(&client().await);
    let config = config_key(gcra.name());
    raw_command(&[
        "HSET", &config, "maxBurst", "1", "rate", "3", "period", "1.5",
    ])
    .await;
    let read = gcra.config().await.unwrap().unwrap();
    assert_eq!(
        (read.max_burst, read.tokens_per_period, read.period),
        (1, 3, Duration::from_millis(1500))
    );
    raw_command(&["HSET", &config, "period", "1e3"]).await;
    assert!(matches!(gcra.config().await, Err(Error::Config(_))));
    raw_command(&["DEL", &config]).await;
}

#[tokio::test]
async fn object_methods_cover_both_keys() {
    let client = client().await;
    let name = unique("gcra");
    let gcra = client.gcra(name.clone());
    let config = config_key(&name);
    gcra.init_rate(2, 4, secs(1)).await.unwrap();
    raw_command(&["SET", &name, "x"]).await;
    assert!(gcra.exists().await.unwrap());
    assert!(gcra.expire(secs(10)).await.unwrap());
    assert!(integer(&raw_command(&["PTTL", &name]).await) > 0);
    assert!(integer(&raw_command(&["PTTL", &config]).await) > 0);
    assert!(gcra.ttl().await.unwrap().is_some());
    assert!(gcra.persist().await.unwrap());
    assert_eq!(integer(&raw_command(&["PTTL", &config]).await), -1);
    let renamed = format!("{name}2");
    gcra.rename(&renamed).await.unwrap();
    assert_eq!(
        integer(&raw_command(&["EXISTS", &renamed, &config_key(&renamed)]).await),
        2
    );
    assert_eq!(integer(&raw_command(&["EXISTS", &name, &config]).await), 0);
    let moved = client.gcra(renamed.clone());
    assert!(moved.del().await.unwrap());
    assert_eq!(
        integer(&raw_command(&["EXISTS", &renamed, &config_key(&renamed)]).await),
        0
    );
}

#[tokio::test]
async fn exists_is_true_before_the_first_acquire() {
    let gcra = gcra(&client().await);
    assert!(!gcra.exists().await.unwrap());
    gcra.init_rate(2, 4, secs(1)).await.unwrap();
    assert!(gcra.exists().await.unwrap());
    assert!(gcra.del().await.unwrap());
}
