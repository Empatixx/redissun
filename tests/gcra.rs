//! Tests of `Gcra`, Redisson's `RGcra`.
//!
//! Ported from Redisson 4.8.0 `RedissonGcraTest` (named `redisson_<snake_case>`).

mod common;

use common::{
    client, own_redis_tag, raw_command, raw_command_at, redis_gcra_client, redis_gcra_url, unique,
    GCRA_TAG,
};
use redissun::{Client, Error, Gcra, GcraResult, Object};
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

fn between(value: Duration, low: Duration, high: Duration) {
    assert!(
        value >= low && value <= high,
        "{value:?} not in {low:?}..={high:?}"
    );
}

async fn acquire(gcra: &Gcra) -> GcraResult {
    gcra.try_acquire().await.unwrap()
}

/// `calls` of `cmdstat_gcra` in `INFO commandstats` of the server at `url`, 0 when it has none.
async fn gcra_calls(url: &str) -> u64 {
    let info = raw_command_at(url, &["INFO", "commandstats"]).await;
    info.lines()
        .find_map(|line| line.strip_prefix("cmdstat_gcra:calls="))
        .and_then(|rest| rest.split(',').next())
        .map_or(0, |calls| calls.parse().unwrap())
}

async fn client_at(url: &str) -> Client {
    Client::builder().url(url).build().await.unwrap()
}

// The cases that run on both paths (Task 3 adds the `_lua` runs).

async fn try_acquire_case(client: Client) {
    let gcra = gcra(&client);
    gcra.init_rate(2, 4, secs(1)).await.unwrap();
    let first = acquire(&gcra).await;
    let second = acquire(&gcra).await;
    let third = acquire(&gcra).await;
    let fourth = acquire(&gcra).await;
    assert!(!first.limited && !second.limited && !third.limited);
    assert!(fourth.limited, "{fourth:?}");
    assert_eq!(first.max_tokens, 3);
    assert_eq!(fourth.max_tokens, 3);
    assert_eq!(first.retry_after, None);
    assert!(
        fourth.retry_after.is_some_and(|d| d >= secs(1)),
        "{fourth:?}"
    );
}

async fn try_acquire_with_tokens_case(client: Client) {
    let gcra = gcra(&client);
    gcra.init_rate(2, 4, Duration::from_millis(250))
        .await
        .unwrap();
    let result = gcra.try_acquire().tokens(2).await.unwrap();
    assert!(!result.limited, "{result:?}");
    assert_eq!(result.max_tokens, 3);
    assert_eq!(result.retry_after, None);
}

async fn set_rate_case(client: Client) {
    let gcra = gcra(&client);
    gcra.init_rate(2, 4, secs(1)).await.unwrap();
    for _ in 0..3 {
        assert!(!acquire(&gcra).await.limited);
    }
    assert!(acquire(&gcra).await.limited);
    gcra.set_rate(5, 10, secs(2)).await.unwrap();
    let config = gcra.config().await.unwrap().unwrap();
    assert_eq!(
        (config.max_burst, config.tokens_per_period, config.period),
        (5, 10, secs(2))
    );
    assert!(!acquire(&gcra).await.limited);
}

async fn try_acquire_rate_not_set_case(client: Client) {
    let gcra = gcra(&client);
    match gcra.try_acquire().await {
        Err(Error::Redis(message)) => {
            assert!(message.contains("GCRA rate is not set"), "{message}")
        }
        other => panic!("expected a server error, got {other:?}"),
    }
}

async fn delete_case(client: Client) {
    let gcra = gcra(&client);
    gcra.init_rate(2, 4, secs(1)).await.unwrap();
    acquire(&gcra).await;
    assert!(gcra.del().await.unwrap());
    assert!(gcra.config().await.unwrap().is_none());
}

async fn retry_after_is_none_for_a_request_that_never_fits_case(client: Client) {
    let gcra = gcra(&client);
    gcra.init_rate(2, 4, secs(1)).await.unwrap();
    let result = gcra.try_acquire().tokens(4).await.unwrap();
    assert!(result.limited, "{result:?}");
    assert_eq!(result.retry_after, None);
    assert_eq!(result.max_tokens, 3);
}

async fn available_tokens_count_down_case(client: Client) {
    let gcra = gcra(&client);
    gcra.init_rate(4, 1, secs(60)).await.unwrap();
    for expected in [4, 3, 2, 1, 0] {
        let result = acquire(&gcra).await;
        assert!(!result.limited, "{result:?}");
        assert_eq!(result.available_tokens, expected, "{result:?}");
    }
    let sixth = acquire(&gcra).await;
    assert!(sixth.limited, "{sixth:?}");
    assert_eq!(sixth.available_tokens, 0);
    between(sixth.retry_after.unwrap(), secs(59), secs(60));
    between(sixth.full_burst_after, secs(299), secs(300));
}

// Task 2: the server's GCRA command.

#[tokio::test]
async fn the_server_has_gcra() {
    let reply = raw_command_at(&redis_gcra_url().await, &["COMMAND", "INFO", "GCRA"]).await;
    assert!(
        reply.to_lowercase().contains("gcra"),
        "the image {GCRA_TAG} no longer has GCRA: {reply:?}"
    );
}

#[tokio::test]
async fn redisson_try_acquire() {
    try_acquire_case(redis_gcra_client().await).await;
}

#[tokio::test]
async fn redisson_try_acquire_with_tokens() {
    try_acquire_with_tokens_case(redis_gcra_client().await).await;
}

#[tokio::test]
async fn redisson_set_rate() {
    set_rate_case(redis_gcra_client().await).await;
}

#[tokio::test]
async fn redisson_try_acquire_rate_not_set() {
    try_acquire_rate_not_set_case(redis_gcra_client().await).await;
}

#[tokio::test]
async fn redisson_delete() {
    delete_case(redis_gcra_client().await).await;
}

#[tokio::test]
async fn retry_after_is_none_for_a_request_that_never_fits() {
    retry_after_is_none_for_a_request_that_never_fits_case(redis_gcra_client().await).await;
}

#[tokio::test]
async fn available_tokens_count_down() {
    available_tokens_count_down_case(redis_gcra_client().await).await;
}

#[tokio::test]
async fn redisson_validation_of_tokens() {
    // A server of its own, so that no other test's GCRA calls show in its statistics.
    let (_container, url) = own_redis_tag(GCRA_TAG).await;
    let gcra = gcra(&client_at(&url).await);
    gcra.init_rate(2, 4, secs(1)).await.unwrap();
    match gcra.try_acquire().tokens(0).await {
        Err(Error::Config(message)) => assert!(message.contains("tokens"), "{message}"),
        other => panic!("expected a config error, got {other:?}"),
    }
    assert!(matches!(
        gcra.try_acquire().tokens(u64::MAX).await,
        Err(Error::Config(_))
    ));
    assert_eq!(
        integer(&raw_command_at(&url, &["EXISTS", gcra.name()]).await),
        0
    );
    assert_eq!(gcra_calls(&url).await, 0, "the probe ran");
}

#[tokio::test]
async fn the_state_is_a_gcra_key() {
    let url = redis_gcra_url().await;
    let gcra = gcra(&client_at(&url).await);
    gcra.init_rate(2, 4, secs(1)).await.unwrap();
    acquire(&gcra).await;
    assert_eq!(
        raw_command_at(&url, &["TYPE", gcra.name()]).await,
        "+gcra\r\n"
    );
    let ttl = integer(&raw_command_at(&url, &["PTTL", gcra.name()]).await);
    assert!(ttl > 0 && ttl <= 250, "{ttl}");
}

// Review Focus 2.
#[tokio::test]
async fn the_probe_writes_nothing() {
    let (_container, url) = own_redis_tag(GCRA_TAG).await;
    let client = client_at(&url).await;
    let gcra = gcra(&client);
    gcra.init_rate(2, 4, secs(1)).await.unwrap();
    // The first acquire of the client probes with `GCRA <m> 0 1 1 TOKENS` on its own key `m`, which has no rate:
    // the server refuses the probe's arguments before it looks at `m`, and the script stops at "not set".
    let unset = client.gcra(unique("gcra"));
    assert!(unset.try_acquire().await.is_err());
    assert_eq!(
        integer(&raw_command_at(&url, &["EXISTS", unset.name()]).await),
        0
    );
    assert_eq!(gcra_calls(&url).await, 1, "the probe alone");
    acquire(&gcra).await;
    assert_eq!(gcra_calls(&url).await, 2, "the probe and one acquire");
    acquire(&gcra).await;
    assert_eq!(gcra_calls(&url).await, 3, "the probe ran again");
}
