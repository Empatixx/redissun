mod common;

use common::{client, raw_command, unique};
use redissun::{Error, Object, RateLimiter, RateLimiterArgs, RateType};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, Instant};

#[tokio::test]
async fn an_unconfigured_limiter_is_an_error() {
    let limiter = client().await.rate_limiter(unique("rate"));
    let error = limiter.try_acquire(1).await.unwrap_err();
    assert!(error.to_string().contains("not initialized"), "{error}");
    assert!(limiter.available_permits().await.is_err());
}

#[tokio::test]
async fn try_set_rate_only_works_once() {
    let limiter = client().await.rate_limiter(unique("rate"));
    let second = Duration::from_secs(1);
    assert!(limiter
        .try_set_rate(RateType::Overall, 3, second)
        .await
        .unwrap());
    assert!(!limiter
        .try_set_rate(RateType::Overall, 9, second)
        .await
        .unwrap());
    for _ in 0..3 {
        assert!(limiter.try_acquire(1).await.unwrap());
    }
    assert!(!limiter.try_acquire(1).await.unwrap());
}

#[tokio::test]
async fn permits_come_back_after_the_interval() {
    let limiter = client().await.rate_limiter(unique("rate"));
    limiter
        .try_set_rate(RateType::Overall, 3, Duration::from_millis(600))
        .await
        .unwrap();
    for _ in 0..3 {
        assert!(limiter.try_acquire(1).await.unwrap());
    }
    assert!(!limiter.try_acquire(1).await.unwrap());
    sleep(Duration::from_millis(750)).await;
    for _ in 0..3 {
        assert!(limiter.try_acquire(1).await.unwrap());
    }
    assert!(!limiter.try_acquire(1).await.unwrap());
}

#[tokio::test]
async fn the_window_slides_instead_of_resetting() {
    let limiter = client().await.rate_limiter(unique("rate"));
    limiter
        .try_set_rate(RateType::Overall, 2, Duration::from_millis(800))
        .await
        .unwrap();
    assert!(limiter.try_acquire(1).await.unwrap());
    sleep(Duration::from_millis(450)).await;
    assert!(limiter.try_acquire(1).await.unwrap());
    assert!(!limiter.try_acquire(1).await.unwrap());
    sleep(Duration::from_millis(450)).await;
    assert!(limiter.try_acquire(1).await.unwrap());
    assert!(!limiter.try_acquire(1).await.unwrap());
}

#[tokio::test]
async fn several_permits_can_be_taken_in_one_call() {
    let limiter = client().await.rate_limiter(unique("rate"));
    limiter
        .try_set_rate(RateType::Overall, 5, Duration::from_secs(10))
        .await
        .unwrap();
    assert!(limiter.try_acquire(3).await.unwrap());
    assert!(!limiter.try_acquire(3).await.unwrap());
    assert!(limiter.try_acquire(2).await.unwrap());
    assert!(!limiter.try_acquire(1).await.unwrap());
}

#[tokio::test]
async fn asking_for_more_than_the_rate_is_an_error() {
    let limiter = client().await.rate_limiter(unique("rate"));
    limiter
        .try_set_rate(RateType::Overall, 2, Duration::from_secs(10))
        .await
        .unwrap();
    let error = limiter.try_acquire(3).await.unwrap_err();
    assert!(error.to_string().contains("cannot exceed"), "{error}");
}

#[tokio::test]
async fn invalid_arguments_are_config_errors() {
    let limiter = client().await.rate_limiter(unique("rate"));
    let second = Duration::from_secs(1);
    assert!(matches!(
        limiter.try_set_rate(RateType::Overall, 0, second).await,
        Err(Error::Config(_))
    ));
    assert!(matches!(
        limiter
            .try_set_rate(RateType::Overall, 1, Duration::ZERO)
            .await,
        Err(Error::Config(_))
    ));
    limiter
        .try_set_rate(RateType::Overall, 1, second)
        .await
        .unwrap();
    assert!(limiter.try_acquire(0).await.unwrap());
    limiter.acquire(0).await.unwrap();
    assert!(matches!(
        limiter
            .try_set_rate_with(
                RateLimiterArgs::new(RateType::Overall, 1, Duration::from_secs(5))
                    .keep_alive(Duration::from_secs(1))
            )
            .await,
        Err(Error::Config(_))
    ));
}

#[tokio::test]
async fn an_overall_limit_is_shared_between_clients() {
    let name = unique("rate");
    let first = client().await.rate_limiter(name.clone());
    let second = client().await.rate_limiter(name);
    first
        .try_set_rate(RateType::Overall, 3, Duration::from_secs(10))
        .await
        .unwrap();
    assert!(first.try_acquire(3).await.unwrap());
    assert!(!second.try_acquire(1).await.unwrap());
}

#[tokio::test]
async fn a_per_client_limit_is_counted_for_each_client() {
    let name = unique("rate");
    let first = client().await.rate_limiter(name.clone());
    let second = client().await.rate_limiter(name);
    first
        .try_set_rate(RateType::PerClient, 3, Duration::from_secs(10))
        .await
        .unwrap();
    assert!(first.try_acquire(3).await.unwrap());
    assert!(!first.try_acquire(1).await.unwrap());
    assert!(second.try_acquire(3).await.unwrap());
    assert!(!second.try_acquire(1).await.unwrap());
}

#[tokio::test]
async fn try_acquire_for_waits_for_the_window_to_move() {
    let limiter = client().await.rate_limiter(unique("rate"));
    limiter
        .try_set_rate(RateType::Overall, 1, Duration::from_millis(500))
        .await
        .unwrap();
    assert!(limiter.try_acquire(1).await.unwrap());

    let started = Instant::now();
    assert!(limiter
        .acquire(1)
        .timeout(Duration::from_secs(3))
        .await
        .unwrap()
        .is_some());
    assert!(started.elapsed() >= Duration::from_millis(350));
    assert!(started.elapsed() < Duration::from_millis(1500));

    let started = Instant::now();
    assert!(limiter
        .acquire(1)
        .timeout(Duration::from_millis(100))
        .await
        .unwrap()
        .is_none());
    assert!(started.elapsed() >= Duration::from_millis(100));
    assert!(started.elapsed() < Duration::from_millis(450));
}

#[tokio::test]
async fn acquire_blocks_until_a_permit_is_free() {
    let limiter = client().await.rate_limiter(unique("rate"));
    limiter
        .try_set_rate(RateType::Overall, 1, Duration::from_millis(500))
        .await
        .unwrap();
    limiter.acquire(1).await.unwrap();
    let started = Instant::now();
    limiter.acquire(1).await.unwrap();
    assert!(started.elapsed() >= Duration::from_millis(350));
    assert!(started.elapsed() < Duration::from_millis(1500));
}

#[tokio::test]
async fn concurrent_callers_never_get_more_than_the_rate() {
    let name = unique("rate");
    client()
        .await
        .rate_limiter(name.clone())
        .try_set_rate(RateType::Overall, 7, Duration::from_secs(30))
        .await
        .unwrap();
    let granted = Arc::new(AtomicU32::new(0));
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let limiter = client().await.rate_limiter(name.clone());
        let granted = granted.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..10 {
                if limiter.try_acquire(1).await.unwrap() {
                    granted.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(granted.load(Ordering::SeqCst), 7);
}

#[tokio::test]
async fn available_permits_reflect_what_was_taken() {
    let limiter = client().await.rate_limiter(unique("rate"));
    limiter
        .try_set_rate(RateType::Overall, 5, Duration::from_millis(600))
        .await
        .unwrap();
    assert_eq!(limiter.available_permits().await.unwrap(), 5);
    assert!(limiter.try_acquire(2).await.unwrap());
    assert_eq!(limiter.available_permits().await.unwrap(), 3);
    sleep(Duration::from_millis(750)).await;
    assert_eq!(limiter.available_permits().await.unwrap(), 5);
}

#[tokio::test]
async fn set_rate_replaces_the_configuration_and_resets_usage() {
    let limiter = client().await.rate_limiter(unique("rate"));
    limiter
        .try_set_rate(RateType::Overall, 1, Duration::from_secs(30))
        .await
        .unwrap();
    assert!(limiter.try_acquire(1).await.unwrap());
    assert!(!limiter.try_acquire(1).await.unwrap());
    limiter
        .set_rate(RateType::Overall, 4, Duration::from_secs(30))
        .await
        .unwrap();
    assert!(limiter.try_acquire(4).await.unwrap());
    assert!(!limiter.try_acquire(1).await.unwrap());
}

#[tokio::test]
async fn object_operations_cover_every_key_of_the_limiter() {
    let client = client().await;
    let name = unique("rate");
    let limiter = client.rate_limiter(name.clone());
    assert!(!limiter.exists().await.unwrap());
    limiter
        .try_set_rate(RateType::Overall, 2, Duration::from_secs(30))
        .await
        .unwrap();
    limiter.try_acquire(1).await.unwrap();
    assert!(limiter.exists().await.unwrap());
    assert_eq!(limiter.name(), name);

    assert!(limiter.expire(Duration::from_secs(60)).await.unwrap());
    assert!(limiter.ttl().await.unwrap().is_some());
    assert!(limiter.persist().await.unwrap());
    assert_eq!(limiter.ttl().await.unwrap(), None);

    assert!(matches!(
        limiter.rename("other").await,
        Err(Error::Unsupported(_))
    ));

    assert!(limiter.del().await.unwrap());
    assert!(!limiter.exists().await.unwrap());
    assert!(limiter.try_acquire(1).await.is_err());
    assert!(limiter
        .try_set_rate(RateType::Overall, 2, Duration::from_secs(30))
        .await
        .unwrap());
    assert!(limiter.try_acquire(2).await.unwrap());
}

#[tokio::test]
async fn debug_shows_the_name() {
    let name = unique("rate");
    let limiter = client().await.rate_limiter(name.clone());
    assert!(format!("{limiter:?}").contains(&name));
}

async fn pttl(key: &str) -> i64 {
    let reply = raw_command(&["PTTL", key]).await;
    reply.trim().trim_start_matches(':').parse().unwrap()
}

#[tokio::test]
async fn refreshing_expired_permits_keeps_the_ttl_of_the_value_key() {
    let name = unique("rate");
    let limiter = client().await.rate_limiter(name.clone());
    limiter
        .try_set_rate(RateType::Overall, 2, Duration::from_millis(400))
        .await
        .unwrap();
    assert!(limiter.try_acquire(1).await.unwrap());
    assert!(limiter.expire(Duration::from_secs(60)).await.unwrap());
    assert!(limiter.try_acquire(1).await.unwrap());
    let value_key = format!("{{{name}}}:value");
    assert!(pttl(&value_key).await > 0);

    sleep(Duration::from_millis(600)).await;
    assert_eq!(limiter.available_permits().await.unwrap(), 2);
    assert!(
        pttl(&value_key).await > 0,
        "the value key lost its expiry when expired permits were released"
    );
}

async fn keys_of(name: &str) -> usize {
    let reply = raw_command(&["KEYS", &format!("*{name}*")]).await;
    reply.lines().filter(|line| line.starts_with('$')).count()
}

fn args(rate_type: RateType, rate: u64, interval: Duration) -> RateLimiterArgs {
    RateLimiterArgs::new(rate_type, rate, interval)
}

async fn take(limiter: &RateLimiter, permits: u64) {
    limiter.acquire(permits).await.unwrap();
}

fn now_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

#[tokio::test]
async fn the_client_clock_and_redisson_key_layout_are_used() {
    let name = unique("rate");
    let limiter = client().await.rate_limiter(name.clone());
    limiter
        .try_set_rate(RateType::Overall, 2, Duration::from_secs(10))
        .await
        .unwrap();
    let before = now_millis();
    assert!(limiter.try_acquire(1).await.unwrap());
    let after = now_millis();
    assert_eq!(raw_command(&["TYPE", &name]).await, "+hash\r\n");
    assert_eq!(
        raw_command(&["GET", &format!("{{{name}}}:value")]).await,
        "$1\r\n1\r\n"
    );
    let reply = raw_command(&[
        "ZRANGE",
        &format!("{{{name}}}:permits"),
        "0",
        "-1",
        "WITHSCORES",
    ])
    .await;
    // The member starts with random bytes that may hold a newline, so the
    // score is read as the reply's last value rather than by line number.
    let score: u128 = reply
        .trim_end_matches("\r\n")
        .rsplit("\r\n")
        .next()
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        before <= score && score <= after,
        "{before} {score} {after}"
    );
}

// Redisson: testKeepAliveTime
#[tokio::test]
async fn keep_alive_deletes_an_idle_limiter_and_acquiring_extends_it() {
    let limiter = client().await.rate_limiter(unique("testKeepAliveTime"));
    limiter
        .try_set_rate_with(
            args(RateType::Overall, 1, Duration::from_secs(1)).keep_alive(Duration::from_secs(1)),
        )
        .await
        .unwrap();
    sleep(Duration::from_millis(1100)).await;
    assert!(!limiter.exists().await.unwrap());
    limiter
        .try_set_rate_with(
            args(RateType::Overall, 10, Duration::from_secs(2)).keep_alive(Duration::from_secs(2)),
        )
        .await
        .unwrap();
    sleep(Duration::from_secs(1)).await;
    assert!(limiter.try_acquire(1).await.unwrap());
    assert!(limiter.ttl().await.unwrap().unwrap() > Duration::from_millis(1500));
}

// Redisson: testExpire2
#[tokio::test]
async fn expire_removes_the_limiter_and_its_permit_keys() {
    let name = unique("test1");
    let limiter = client().await.rate_limiter(name.clone());
    limiter
        .try_set_rate(RateType::Overall, 5, Duration::from_secs(5))
        .await
        .unwrap();
    limiter.expire(Duration::from_secs(2)).await.unwrap();
    take(&limiter, 1).await;
    sleep(Duration::from_millis(2500)).await;
    assert_eq!(keys_of(&name).await, 0);
}

// Redisson: testRateValue
#[tokio::test]
async fn the_permit_set_never_exceeds_the_rate_under_load() {
    let client = client().await;
    let name = unique("test1");
    let rate = 50;
    let limiter = client.rate_limiter(name.clone());
    limiter
        .set_rate(RateType::Overall, rate, Duration::from_secs(1))
        .await
        .unwrap();
    let workers: Vec<_> = (0..20)
        .map(|_| {
            let limiter = limiter.clone();
            tokio::spawn(async move {
                loop {
                    take(&limiter, 1).await;
                }
            })
        })
        .collect();
    sleep(Duration::from_millis(1500)).await;
    let permits = format!("{{{name}}}:permits");
    let mut sizes = Vec::new();
    for _ in 0..8 {
        let reply = raw_command(&["ZCARD", &permits]).await;
        sizes.push(reply.trim().trim_start_matches(':').parse::<u64>().unwrap());
        sleep(Duration::from_millis(250)).await;
    }
    workers.iter().for_each(|worker| worker.abort());
    assert!(sizes.iter().all(|size| *size <= rate), "{sizes:?}");
    assert!(
        sizes.iter().filter(|size| **size == rate).count() >= 5,
        "{sizes:?}"
    );
}

// Redisson: testExpire
#[tokio::test]
async fn an_expired_limiter_leaves_no_keys_behind() {
    let name = unique("limiter");
    let limiter = client().await.rate_limiter(name.clone());
    limiter
        .try_set_rate(RateType::Overall, 2, Duration::from_secs(5))
        .await
        .unwrap();
    limiter.try_acquire(1).await.unwrap();
    limiter.expire(Duration::from_secs(1)).await.unwrap();
    sleep(Duration::from_millis(1100)).await;
    assert_eq!(keys_of(&name).await, 0);
}

// Redisson: testAcquisitionInterval
#[tokio::test]
async fn a_permit_is_free_again_one_interval_later() {
    let limiter = client().await.rate_limiter(unique("acquire"));
    limiter
        .try_set_rate(RateType::Overall, 2, Duration::from_secs(5))
        .await
        .unwrap();
    assert!(limiter.try_acquire(1).await.unwrap());
    sleep(Duration::from_millis(4000)).await;
    assert!(limiter.try_acquire(1).await.unwrap());
    sleep(Duration::from_millis(1050)).await;
    assert!(limiter.try_acquire(1).await.unwrap());
    assert!(!limiter.try_acquire(1).await.unwrap());
}

// Redisson: testRateConfig
#[tokio::test]
async fn config_returns_the_rate_interval_and_type_that_were_set() {
    let limiter = client().await.rate_limiter(unique("acquire"));
    assert!(limiter
        .try_set_rate(RateType::Overall, 1, Duration::from_secs(5))
        .await
        .unwrap());
    let config = limiter.config().await.unwrap().unwrap();
    assert_eq!(config.rate, 1);
    assert_eq!(config.interval, Duration::from_millis(5000));
    assert_eq!(config.rate_type, RateType::Overall);
}

// Redisson: testAvailablePermits
#[tokio::test]
async fn available_permits_are_full_again_after_the_interval() {
    let limiter = client().await.rate_limiter(unique("rt2"));
    limiter
        .try_set_rate(RateType::Overall, 10, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(limiter.available_permits().await.unwrap(), 10);
    take(&limiter, 1).await;
    sleep(Duration::from_millis(6000)).await;
    assert_eq!(limiter.available_permits().await.unwrap(), 10);
}

// Redisson: testUpdateRateConfig
#[tokio::test]
async fn set_rate_overwrites_the_stored_config() {
    let limiter = client().await.rate_limiter(unique("acquire"));
    assert!(limiter
        .try_set_rate(RateType::Overall, 1, Duration::from_secs(5))
        .await
        .unwrap());
    limiter
        .set_rate(RateType::Overall, 2, Duration::from_secs(5))
        .await
        .unwrap();
    let config = limiter.config().await.unwrap().unwrap();
    assert_eq!(config.rate, 2);
    assert_eq!(config.interval, Duration::from_millis(5000));
    assert_eq!(config.rate_type, RateType::Overall);
}

// Redisson: testPermitsExceeding
#[tokio::test]
async fn acquiring_above_the_rate_fails_with_the_redisson_message() {
    let limiter = client().await.rate_limiter(unique("myLimiter"));
    limiter
        .try_set_rate(RateType::PerClient, 1, Duration::from_secs(1))
        .await
        .unwrap();
    let error = limiter.try_acquire(20).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Requested permits amount cannot exceed defined rate"),
        "{error}"
    );
    assert!(limiter.try_acquire(1).await.unwrap());
}

// Redisson: testZeroTimeout
#[tokio::test]
async fn acquire_with_a_zero_timeout_takes_only_free_permits() {
    let limiter = client().await.rate_limiter(unique("myLimiter"));
    limiter
        .try_set_rate(RateType::Overall, 5, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(limiter.available_permits().await.unwrap(), 5);
    let zero = || limiter.acquire(1).timeout(Duration::ZERO);

    assert!(zero().await.unwrap().is_some());
    assert!(zero().await.unwrap().is_some());
    assert_eq!(limiter.available_permits().await.unwrap(), 3);
    assert!(zero().await.unwrap().is_some());
    assert!(zero().await.unwrap().is_some());
    assert_eq!(limiter.available_permits().await.unwrap(), 1);
    assert!(zero().await.unwrap().is_some());
    assert_eq!(limiter.available_permits().await.unwrap(), 0);
    for _ in 0..5 {
        assert!(zero().await.unwrap().is_none());
    }

    sleep(Duration::from_millis(1000)).await;

    for _ in 0..5 {
        assert!(zero().await.unwrap().is_some());
    }
    for _ in 0..5 {
        assert!(zero().await.unwrap().is_none());
    }
}

// Redisson: testTryAcquire
#[tokio::test]
async fn acquire_with_a_timeout_gives_up_without_a_free_permit() {
    let limiter = client().await.rate_limiter(unique("acquire"));
    assert!(limiter
        .try_set_rate(RateType::Overall, 1, Duration::from_secs(5))
        .await
        .unwrap());
    let started = Instant::now();
    let second = Duration::from_secs(1);
    assert!(limiter.acquire(1).timeout(second).await.unwrap().is_some());
    assert!(limiter.acquire(1).timeout(second).await.unwrap().is_none());
    assert!(!limiter.try_acquire(1).await.unwrap());
    assert!(started.elapsed() < Duration::from_secs(2));
}

// Redisson: testAcquire
#[tokio::test]
async fn acquire_waits_for_the_next_interval_once_permits_run_out() {
    let limiter = client().await.rate_limiter(unique("acquire"));
    assert!(limiter
        .try_set_rate(RateType::Overall, 1, Duration::from_millis(500))
        .await
        .unwrap());
    for _ in 0..10 {
        take(&limiter, 1).await;
    }
    assert!(!limiter.try_acquire(1).await.unwrap());
}

#[tokio::test]
async fn try_acquire_grants_exactly_the_rate_in_every_interval() {
    let limiter = client().await.rate_limiter(unique("test"));
    assert!(limiter
        .try_set_rate(RateType::Overall, 10, Duration::from_secs(1))
        .await
        .unwrap());
    assert!(!limiter
        .try_set_rate(RateType::Overall, 20, Duration::from_secs(1))
        .await
        .unwrap());
    for _ in 0..3 {
        for _ in 0..10 {
            assert!(limiter.try_acquire(1).await.unwrap());
        }
        for _ in 0..10 {
            assert!(!limiter.try_acquire(1).await.unwrap());
        }
        sleep(Duration::from_millis(1050)).await;
    }
}

// Redisson: testRemove
#[tokio::test]
async fn del_removes_the_limiter_keys_and_reports_whether_it_existed() {
    let name = unique("test");
    let limiter = client().await.rate_limiter(name.clone());
    assert!(!limiter.del().await.unwrap());
    limiter
        .try_set_rate(RateType::Overall, 5, Duration::from_secs(300))
        .await
        .unwrap();
    assert_eq!(keys_of(&name).await, 1);
    limiter.try_acquire(1).await.unwrap();
    let deleted = limiter.del().await.unwrap();
    assert_eq!(keys_of(&name).await, 0);
    assert!(deleted);
}

async fn acquisition_times(
    limiter: &RateLimiter,
    workers: usize,
    total: u64,
    blocking: bool,
) -> Vec<u128> {
    let times = Arc::new(std::sync::Mutex::new(Vec::new()));
    let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let tasks: Vec<_> = (0..workers)
        .map(|_| {
            let limiter = limiter.clone();
            let times = times.clone();
            let counter = counter.clone();
            tokio::spawn(async move {
                loop {
                    let granted = if blocking {
                        take(&limiter, 1).await;
                        true
                    } else {
                        limiter.try_acquire(1).await.unwrap()
                    };
                    if granted {
                        if counter.fetch_add(1, Ordering::SeqCst) + 1 > total {
                            break;
                        }
                        times.lock().unwrap().push(now_millis());
                    }
                    if !blocking {
                        sleep(Duration::from_millis(u64::from(
                            uuid::Uuid::new_v4().as_bytes()[0] % 10,
                        )))
                        .await;
                    }
                }
            })
        })
        .collect();
    for task in tasks {
        tokio::time::timeout(Duration::from_secs(120), task)
            .await
            .unwrap()
            .unwrap();
    }
    let mut times = times.lock().unwrap().clone();
    times.sort_unstable();
    times
}

/// How late the first acquisition may be written down. A task that the machine pauses writes its time late.
const LATE_RECORD_MS: u128 = 500;

/// Checks that `times`, the sorted moments the permits were granted, never go faster than `rate` permits per `interval_ms`.
///
/// A time is written after the permit was granted, so it can be late but never early, and the `k`-th written time is never before the `k`-th grant. The grants from the first to the `rate * m`-th span at least `m` intervals, so the written times do as well, less how late the first one was written. Counting the permits of each second instead fails whenever a paused task writes the times of one second in the next.
fn assert_rate(times: &[u128], rate: usize, interval_ms: u128) {
    let intervals = (times.len() - 1) / rate;
    assert!(intervals >= 2, "too few acquisitions to check the rate");
    let span = times[intervals * rate] - times[0];
    let least = intervals as u128 * interval_ms - LATE_RECORD_MS;
    assert!(
        span >= least,
        "{} permits came within {span} ms, the rate allows them no sooner than {} ms",
        intervals * rate + 1,
        intervals as u128 * interval_ms
    );
}

// Redisson: testConcurrency2
#[tokio::test]
async fn concurrent_blocking_acquires_keep_to_the_rate() {
    let limiter = client().await.rate_limiter(unique("test"));
    limiter
        .try_set_rate(RateType::Overall, 18, Duration::from_secs(1))
        .await
        .unwrap();
    let times = acquisition_times(&limiter, 8, 100, true).await;
    assert_rate(&times, 18, 1000);
}

// Redisson: testConcurrency
#[tokio::test]
async fn concurrent_try_acquires_keep_to_the_rate() {
    let limiter = client().await.rate_limiter(unique("test"));
    assert!(limiter
        .try_set_rate(RateType::Overall, 10, Duration::from_secs(1))
        .await
        .unwrap());
    assert!(!limiter
        .try_set_rate(RateType::Overall, 20, Duration::from_secs(1))
        .await
        .unwrap());
    let times = acquisition_times(&limiter, 8, 50, false).await;
    assert_rate(&times, 10, 1000);
}

// Redisson: testChangeRate
#[tokio::test]
async fn set_rate_changes_the_rate_in_both_modes() {
    let limiter = client().await.rate_limiter(unique("test_change_rate"));
    let second = Duration::from_secs(1);
    limiter
        .set_rate(RateType::PerClient, 10, second)
        .await
        .unwrap();
    assert_eq!(limiter.config().await.unwrap().unwrap().rate, 10);
    take(&limiter, 1).await;
    assert_eq!(limiter.available_permits().await.unwrap(), 9);

    limiter
        .set_rate(RateType::PerClient, 20, second)
        .await
        .unwrap();
    assert_eq!(limiter.config().await.unwrap().unwrap().rate, 20);
    take(&limiter, 1).await;
    assert_eq!(limiter.available_permits().await.unwrap(), 19);

    limiter
        .set_rate(RateType::Overall, 10, second)
        .await
        .unwrap();
    assert_eq!(limiter.config().await.unwrap().unwrap().rate, 10);
    take(&limiter, 1).await;
    assert_eq!(limiter.available_permits().await.unwrap(), 9);

    limiter
        .set_rate(RateType::Overall, 20, second)
        .await
        .unwrap();
    assert_eq!(limiter.config().await.unwrap().unwrap().rate, 20);
    take(&limiter, 1).await;
    assert_eq!(limiter.available_permits().await.unwrap(), 19);
}

// Redisson: testSetStateArgs
#[tokio::test]
async fn permits_taken_with_keep_state_still_expire_after_the_interval() {
    let limiter = client().await.rate_limiter(unique("testSetStateArgs"));
    limiter
        .set_rate_with(args(RateType::Overall, 10, Duration::from_secs(1)).keep_state(true))
        .await
        .unwrap();
    take(&limiter, 3).await;
    assert_eq!(limiter.available_permits().await.unwrap(), 7);
    sleep(Duration::from_millis(1100)).await;
    take(&limiter, 3).await;
    assert_eq!(limiter.available_permits().await.unwrap(), 7);
}

// Redisson: testSetStateArgsKeepState
#[tokio::test]
async fn set_rate_with_keep_state_carries_the_used_permits_over() {
    let limiter = client()
        .await
        .rate_limiter(unique("testSetStateArgsKeepState"));
    let five = Duration::from_secs(5);
    limiter
        .set_rate_with(args(RateType::Overall, 10, five).keep_state(true))
        .await
        .unwrap();
    take(&limiter, 3).await;
    assert_eq!(limiter.available_permits().await.unwrap(), 7);
    limiter
        .set_rate_with(args(RateType::Overall, 20, five).keep_state(true))
        .await
        .unwrap();
    assert_eq!(limiter.available_permits().await.unwrap(), 17);
}

// Redisson: testSetStateArgsNotKeepState
#[tokio::test]
async fn set_rate_without_keep_state_frees_every_permit() {
    let limiter = client()
        .await
        .rate_limiter(unique("testSetStateArgsNotKeepState"));
    let five = Duration::from_secs(5);
    limiter
        .set_rate_with(args(RateType::Overall, 10, five).keep_state(true))
        .await
        .unwrap();
    take(&limiter, 3).await;
    assert_eq!(limiter.available_permits().await.unwrap(), 7);
    limiter
        .set_rate_with(args(RateType::Overall, 20, five))
        .await
        .unwrap();
    assert_eq!(limiter.available_permits().await.unwrap(), 20);
}

// Redisson: testUpdateRateNonExisting
#[tokio::test]
async fn update_rate_of_a_missing_limiter_creates_nothing() {
    let limiter = client()
        .await
        .rate_limiter(unique("testUpdateRateNonExisting"));
    let five = Duration::from_secs(5);
    assert!(!limiter
        .update_rate(args(RateType::Overall, 10, five).keep_state(true))
        .await
        .unwrap());
    assert!(!limiter.exists().await.unwrap());
    assert!(!limiter
        .update_rate(args(RateType::PerClient, 10, five).keep_state(true))
        .await
        .unwrap());
    assert!(!limiter.exists().await.unwrap());
}

// Redisson: testUpdateRateKeepsState
#[tokio::test]
async fn update_rate_with_the_same_settings_keeps_the_limiter_working() {
    let limiter = client()
        .await
        .rate_limiter(unique("testUpdateRateKeepsState"));
    let five = Duration::from_secs(5);
    limiter
        .set_rate_with(args(RateType::Overall, 10, five).keep_state(true))
        .await
        .unwrap();
    assert!(limiter
        .update_rate(args(RateType::Overall, 10, five).keep_state(true))
        .await
        .unwrap());
    assert_eq!(limiter.available_permits().await.unwrap(), 10);
    take(&limiter, 2).await;
    assert_eq!(limiter.available_permits().await.unwrap(), 8);
    assert!(!limiter.try_acquire(10).await.unwrap());
    assert!(limiter.try_acquire(8).await.unwrap());
}

// Redisson: testUpdateRateHigherRate
#[tokio::test]
async fn update_rate_to_a_higher_rate_adds_free_permits() {
    let limiter = client()
        .await
        .rate_limiter(unique("testUpdateRateExistingKeyHigherRate"));
    limiter
        .set_rate_with(args(RateType::Overall, 10, Duration::from_secs(2)).keep_state(true))
        .await
        .unwrap();
    sleep(Duration::from_millis(1000)).await;
    take(&limiter, 4).await;
    assert_eq!(limiter.available_permits().await.unwrap(), 6);
    assert!(limiter
        .update_rate(args(RateType::Overall, 20, Duration::from_secs(5)).keep_state(true))
        .await
        .unwrap());
    assert_eq!(limiter.available_permits().await.unwrap(), 16);
    assert!(limiter.try_acquire(16).await.unwrap());
}

// Redisson: testUpdateRateLowerRateNewValuePositive
#[tokio::test]
async fn update_rate_to_a_lower_rate_removes_free_permits() {
    let limiter = client()
        .await
        .rate_limiter(unique("testUpdateRateLowerRateNewValuePositive"));
    limiter
        .set_rate_with(args(RateType::Overall, 10, Duration::from_secs(2)).keep_state(true))
        .await
        .unwrap();
    sleep(Duration::from_millis(1000)).await;
    take(&limiter, 3).await;
    assert_eq!(limiter.available_permits().await.unwrap(), 7);
    assert!(limiter
        .update_rate(args(RateType::Overall, 5, Duration::from_secs(5)).keep_state(true))
        .await
        .unwrap());
    assert_eq!(limiter.available_permits().await.unwrap(), 2);
    assert!(!limiter.try_acquire(3).await.unwrap());
    assert!(limiter.try_acquire(2).await.unwrap());
}

// Redisson: testUpdateRateLowerRateNewValueNegative
#[tokio::test]
async fn update_rate_below_the_used_permits_leaves_none_free() {
    let limiter = client()
        .await
        .rate_limiter(unique("testUpdateRateLowerRateNewValueNegative"));
    limiter
        .set_rate_with(args(RateType::Overall, 10, Duration::from_secs(2)).keep_state(true))
        .await
        .unwrap();
    sleep(Duration::from_millis(1000)).await;
    take(&limiter, 8).await;
    assert_eq!(limiter.available_permits().await.unwrap(), 2);
    assert!(limiter
        .update_rate(args(RateType::Overall, 5, Duration::from_secs(5)).keep_state(true))
        .await
        .unwrap());
    assert_eq!(limiter.available_permits().await.unwrap(), 0);
    assert!(!limiter.try_acquire(1).await.unwrap());
    assert!(!limiter.try_acquire(2).await.unwrap());
}

// Redisson: testUpdateRatePermitResetFull
#[tokio::test]
async fn update_rate_after_the_interval_starts_with_every_permit_free() {
    let limiter = client()
        .await
        .rate_limiter(unique("testUpdateRatePermitResetFull"));
    let second = Duration::from_secs(1);
    limiter
        .set_rate_with(args(RateType::Overall, 10, second).keep_state(true))
        .await
        .unwrap();
    take(&limiter, 8).await;
    assert_eq!(limiter.available_permits().await.unwrap(), 2);
    sleep(Duration::from_millis(2000)).await;
    assert!(limiter
        .update_rate(args(RateType::Overall, 10, second).keep_state(true))
        .await
        .unwrap());
    assert_eq!(limiter.available_permits().await.unwrap(), 10);
}

// Redisson: testUpdateRateModeChangeClearsState
#[tokio::test]
async fn update_rate_to_another_rate_type_clears_the_used_permits() {
    let limiter = client()
        .await
        .rate_limiter(unique("testUpdateRateModeChangeClearsState"));
    let five = Duration::from_secs(5);
    limiter
        .set_rate_with(args(RateType::Overall, 10, five).keep_state(true))
        .await
        .unwrap();
    take(&limiter, 4).await;
    assert_eq!(limiter.available_permits().await.unwrap(), 6);
    assert!(limiter
        .update_rate(args(RateType::PerClient, 10, five).keep_state(true))
        .await
        .unwrap());
    assert_eq!(limiter.available_permits().await.unwrap(), 10);
}

// Redisson: testUpdateRateKeepAliveTimeLessThanIntervalThrows
#[tokio::test]
async fn update_rate_rejects_a_keep_alive_shorter_than_the_interval() {
    let limiter = client()
        .await
        .rate_limiter(unique("testUpdateRateKeepAliveTimeLessThanIntervalThrows"));
    let result = limiter
        .update_rate(
            args(RateType::Overall, 10, Duration::from_secs(5))
                .keep_state(true)
                .keep_alive(Duration::from_secs(1)),
        )
        .await;
    assert!(matches!(result, Err(Error::Config(_))));
}

// Redisson: testUpdateRateRateIntervalChangeDropsExpired
#[tokio::test]
async fn update_rate_to_a_shorter_interval_drops_old_permits() {
    let limiter = client()
        .await
        .rate_limiter(unique("testUpdateRateRateIntervalChangeDropsExpired"));
    limiter
        .set_rate_with(args(RateType::Overall, 10, Duration::from_secs(5)).keep_state(true))
        .await
        .unwrap();
    take(&limiter, 3).await;
    assert_eq!(limiter.available_permits().await.unwrap(), 7);
    sleep(Duration::from_millis(1100)).await;
    assert!(limiter
        .update_rate(args(RateType::Overall, 10, Duration::from_secs(1)).keep_state(true))
        .await
        .unwrap());
    assert_eq!(limiter.available_permits().await.unwrap(), 10);
}

// Redisson: testUpdateRatePerClientUsesClientState
#[tokio::test]
async fn update_rate_in_per_client_mode_keeps_the_client_usage() {
    let limiter = client()
        .await
        .rate_limiter(unique("testUpdateRatePerClientUsesClientState"));
    let five = Duration::from_secs(5);
    limiter
        .set_rate_with(args(RateType::PerClient, 10, five).keep_state(true))
        .await
        .unwrap();
    take(&limiter, 3).await;
    assert_eq!(limiter.available_permits().await.unwrap(), 7);
    assert!(limiter
        .update_rate(args(RateType::PerClient, 20, five).keep_state(true))
        .await
        .unwrap());
    assert_eq!(limiter.available_permits().await.unwrap(), 17);
}

// Redisson: testRelease
#[tokio::test]
async fn release_returns_permits_but_never_above_the_rate() {
    let limiter = client().await.rate_limiter(unique("test_release"));
    for rate_type in [RateType::PerClient, RateType::Overall] {
        limiter
            .set_rate(rate_type, 10, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(limiter.config().await.unwrap().unwrap().rate, 10);
        take(&limiter, 3).await;
        assert_eq!(limiter.available_permits().await.unwrap(), 7);
        limiter.release(3).await.unwrap();
        assert_eq!(limiter.available_permits().await.unwrap(), 10);
        limiter.release(10).await.unwrap();
        assert_eq!(limiter.available_permits().await.unwrap(), 10);
        limiter.release(0).await.unwrap();
        assert_eq!(limiter.available_permits().await.unwrap(), 10);
    }
}

// Redisson: testReleasedPermitsPreservedAfterExpiry
#[tokio::test]
async fn acquire_and_release_cycles_lose_no_permits_after_expiry() {
    let limiter = client().await.rate_limiter(unique("testNoCollapse"));
    limiter
        .try_set_rate(RateType::Overall, 10, Duration::from_secs(1))
        .await
        .unwrap();
    limiter.try_acquire(1).await.unwrap();
    limiter.release(1).await.unwrap();
    sleep(Duration::from_millis(300)).await;
    for _ in 0..8 {
        limiter.try_acquire(1).await.unwrap();
        limiter.release(1).await.unwrap();
    }
    sleep(Duration::from_millis(900)).await;
    limiter.try_acquire(1).await.unwrap();
    assert_eq!(limiter.available_permits().await.unwrap(), 9);
}
