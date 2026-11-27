//! Tests of `Gcra`, Redisson's `RGcra`.
//!
//! Ported from Redisson 4.8.0 `RedissonGcraTest` (named `redisson_<snake_case>`).

mod common;

use common::{
    client, own_redis_image, own_redis_tag, raw_command, raw_command_at, redis_gcra_client,
    redis_gcra_url, redis_url, unique, GCRA_TAG,
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

// Task 3: the Lua port on servers without GCRA (Redis 7.4 here).

#[tokio::test]
async fn redisson_try_acquire_lua() {
    try_acquire_case(client().await).await;
}

#[tokio::test]
async fn redisson_try_acquire_with_tokens_lua() {
    try_acquire_with_tokens_case(client().await).await;
}

#[tokio::test]
async fn redisson_set_rate_lua() {
    set_rate_case(client().await).await;
}

#[tokio::test]
async fn redisson_try_acquire_rate_not_set_lua() {
    try_acquire_rate_not_set_case(client().await).await;
}

#[tokio::test]
async fn redisson_delete_lua() {
    delete_case(client().await).await;
}

#[tokio::test]
async fn available_tokens_count_down_lua() {
    available_tokens_count_down_case(client().await).await;
}

#[tokio::test]
async fn retry_after_is_none_for_a_request_that_never_fits_lua() {
    retry_after_is_none_for_a_request_that_never_fits_case(client().await).await;
}

#[tokio::test]
async fn the_server_has_no_gcra() {
    let reply = raw_command(&["COMMAND", "INFO", "GCRA"]).await;
    // A nil entry, as RESP2 writes it.
    assert_eq!(reply, "*1\r\n$-1\r\n", "Redis 7.4 has GCRA now: {reply:?}");
}

/// The server's `TIME` in microseconds.
async fn server_micros(url: &str) -> i64 {
    let reply = raw_command_at(url, &["TIME"]).await;
    let parts: Vec<i64> = reply
        .split("\r\n")
        .filter(|line| !line.is_empty() && !line.starts_with(['*', '$']))
        .map(|line| line.parse().unwrap())
        .collect();
    parts[0] * 1_000_000 + parts[1]
}

#[tokio::test]
async fn the_lua_state_is_a_string_with_a_deadline() {
    let url = redis_url().await;
    let gcra = gcra(&client().await);
    gcra.init_rate(2, 4, secs(1)).await.unwrap();
    acquire(&gcra).await;
    let now = server_micros(&url).await;
    assert_eq!(raw_command(&["TYPE", gcra.name()]).await, "+string\r\n");
    let tat: i64 = bulk(&raw_command(&["GET", gcra.name()]).await)
        .unwrap()
        .parse()
        .unwrap();
    let expected = now + 250_000;
    assert!((tat - expected).abs() <= 5_000_000, "{tat} vs {expected}");
    let ttl = integer(&raw_command(&["PTTL", gcra.name()]).await);
    assert!(ttl > 0 && ttl <= 250, "{ttl}");
}

/// A seeded xorshift64: the generated cases repeat for one seed.
struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// A number in `low..=high`.
    fn range(&mut self, low: u64, high: u64) -> u64 {
        low + self.next() % (high - low + 1)
    }
}

async fn run_sequence(gcra: &Gcra, config: (u64, u64, Duration), calls: &[u64]) -> Vec<GcraResult> {
    gcra.init_rate(config.0, config.1, config.2).await.unwrap();
    let mut results = Vec::new();
    for &tokens in calls {
        results.push(gcra.try_acquire().tokens(tokens).await.unwrap());
    }
    results
}

// Review Focus 1: the Lua port answers what the server's own gcra.c answers.
#[tokio::test]
async fn lua_matches_native_on_generated_sequences() {
    let lua = client().await;
    let native = redis_gcra_client().await;
    let seed = 0x9e37_79b9_7f4a_7c15;
    let mut random = XorShift(seed);
    for case in 0..200 {
        let max_burst = random.range(0, 5);
        let tokens_per_period = random.range(1, 5);
        let k = [1, 2, 60][random.range(0, 2) as usize];
        let period = secs(tokens_per_period * k);
        let calls: Vec<u64> = (0..random.range(1, 10))
            .map(|_| random.range(1, 7))
            .collect();
        let config = (max_burst, tokens_per_period, period);
        let from_lua = run_sequence(&gcra(&lua), config, &calls).await;
        let from_native = run_sequence(&gcra(&native), config, &calls).await;
        assert_eq!(
            from_lua, from_native,
            "seed {seed:#x}, case {case}: config {config:?}, calls {calls:?}"
        );
    }
}

#[tokio::test]
async fn lua_matches_native_on_fractional_periods() {
    let lua = client().await;
    let native = redis_gcra_client().await;
    for config in [
        (2, 4, Duration::from_millis(250)),
        (0, 3, Duration::from_millis(1500)),
    ] {
        let from_lua = run_sequence(&gcra(&lua), config, &[1, 1, 1]).await;
        let from_native = run_sequence(&gcra(&native), config, &[1, 1, 1]).await;
        for (l, n) in from_lua.iter().zip(&from_native) {
            assert_eq!(
                (l.limited, l.max_tokens, l.available_tokens),
                (n.limited, n.max_tokens, n.available_tokens),
                "{config:?}: {l:?} vs {n:?}"
            );
            let close = |a: Duration, b: Duration| a.abs_diff(b) <= secs(1);
            match (l.retry_after, n.retry_after) {
                (None, None) => {}
                (Some(a), Some(b)) => assert!(close(a, b), "{config:?}: {l:?} vs {n:?}"),
                _ => panic!("{config:?}: {l:?} vs {n:?}"),
            }
            assert!(
                close(l.full_burst_after, n.full_burst_after),
                "{config:?}: {l:?} vs {n:?}"
            );
        }
    }
}

/// The error text of a `try_acquire` on a limiter whose settings are written raw at the server `url`.
async fn raw_config_error(client: &Client, url: &str, fields: &[&str]) -> String {
    let gcra = gcra(client);
    let config = config_key(gcra.name());
    let mut command = vec!["HSET", config.as_str()];
    command.extend_from_slice(fields);
    raw_command_at(url, &command).await;
    match gcra.try_acquire().await {
        Err(Error::Redis(message)) => message,
        other => panic!("expected a server error, got {other:?}"),
    }
}

#[tokio::test]
async fn lua_matches_native_on_errors() {
    let lua = client().await;
    let native = redis_gcra_client().await;
    let lua_url = redis_url().await;
    let native_url = redis_gcra_url().await;
    let period = ["maxBurst", "2", "rate", "4", "period", "1e12"];
    for message in [
        raw_config_error(&lua, &lua_url, &period).await,
        raw_config_error(&native, &native_url, &period).await,
    ] {
        assert!(
            message.contains("period must be > 0 and < 1e12"),
            "{message}"
        );
    }
    let overflow = [
        "maxBurst",
        "9007199254740992",
        "rate",
        "1",
        "period",
        "1000000",
    ];
    let from_lua = raw_config_error(&lua, &lua_url, &overflow).await;
    let from_native = raw_config_error(&native, &native_url, &overflow).await;
    let text = |message: &str| -> String {
        let start = message
            .find("GCRA limiting")
            .unwrap_or_else(|| panic!("{message}"));
        let end = message
            .find("would cause an overflow")
            .unwrap_or_else(|| panic!("{message}"));
        message[start..end].to_string()
    };
    assert_eq!(text(&from_lua), text(&from_native));
}

// Review Focus 3.
#[tokio::test]
async fn a_lua_state_on_a_gcra_server_is_wrongtype_until_reset() {
    let url = redis_gcra_url().await;
    let gcra = gcra(&redis_gcra_client().await);
    gcra.init_rate(2, 4, secs(1)).await.unwrap();
    raw_command_at(&url, &["SET", gcra.name(), "1", "PX", "60000"]).await;
    match gcra.try_acquire().await {
        Err(Error::Redis(message)) => assert!(message.contains("WRONGTYPE"), "{message}"),
        other => panic!("expected WRONGTYPE, got {other:?}"),
    }
    gcra.set_rate(2, 4, secs(1)).await.unwrap();
    assert!(!acquire(&gcra).await.limited);
}

async fn allowed_then_limited(url: &str) {
    let gcra = gcra(&client_at(url).await);
    gcra.init_rate(1, 1, secs(60)).await.unwrap();
    assert!(!acquire(&gcra).await.limited);
    assert!(!acquire(&gcra).await.limited);
    assert!(acquire(&gcra).await.limited);
}

#[tokio::test]
async fn works_on_redis_6_2() {
    let (_container, url) = own_redis_tag("6.2-alpine").await;
    allowed_then_limited(&url).await;
}

#[tokio::test]
async fn works_on_valkey() {
    let (_container, url) = own_redis_image("valkey/valkey", "9.2").await;
    allowed_then_limited(&url).await;
}

async fn concurrent_acquires(client: Client) {
    let gcra = gcra(&client);
    gcra.init_rate(9, 1, secs(3600)).await.unwrap();
    let tasks: Vec<_> = (0..50)
        .map(|_| {
            let gcra = gcra.clone();
            tokio::spawn(async move { gcra.try_acquire().await.unwrap().limited })
        })
        .collect();
    let mut allowed = 0;
    for task in tasks {
        if !task.await.unwrap() {
            allowed += 1;
        }
    }
    assert_eq!(allowed, 10);
}

#[tokio::test]
async fn concurrent_acquires_never_exceed_the_burst() {
    concurrent_acquires(client().await).await;
    concurrent_acquires(redis_gcra_client().await).await;
}
