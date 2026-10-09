mod common;

use common::{client, unique};
use std::time::Duration;
use tokio::time::{sleep, Instant};

#[tokio::test]
async fn returns_at_once_when_a_value_exists() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    set.insert("a", 2.0).await.unwrap();
    set.insert("b", 1.0).await.unwrap();
    assert_eq!(set.pop_first_wait().await.unwrap(), ("b".to_string(), 1.0));
    assert_eq!(set.pop_last_wait().await.unwrap(), ("a".to_string(), 2.0));
}

#[tokio::test]
async fn waits_for_a_value_from_another_task() {
    let client = client().await;
    let name = unique("zset");
    let set = client.sorted_set::<String>(name.clone());
    let waiting = tokio::spawn({
        let set = set.clone();
        async move { set.pop_first_wait().await.unwrap() }
    });
    sleep(Duration::from_millis(200)).await;
    assert!(!waiting.is_finished());
    set.insert("late", 5.0).await.unwrap();
    assert_eq!(waiting.await.unwrap(), ("late".to_string(), 5.0));
}

#[tokio::test]
async fn timeout_gives_none() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    let started = Instant::now();
    let popped = set
        .pop_last_wait()
        .timeout(Duration::from_millis(300))
        .await
        .unwrap();
    assert!(popped.is_none());
    assert!(started.elapsed() >= Duration::from_millis(250));
}

#[tokio::test]
async fn zero_timeout_pops_once_without_waiting() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    assert!(set
        .pop_first_wait()
        .timeout(Duration::ZERO)
        .await
        .unwrap()
        .is_none());
    set.insert("a", 1.0).await.unwrap();
    assert!(set
        .pop_first_wait()
        .timeout(Duration::ZERO)
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn a_waiting_pop_does_not_stall_other_calls() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    let waiting = tokio::spawn({
        let set = set.clone();
        async move {
            set.pop_first_wait()
                .timeout(Duration::from_secs(2))
                .await
                .unwrap()
        }
    });
    sleep(Duration::from_millis(100)).await;
    let started = Instant::now();
    for _ in 0..20 {
        assert_eq!(set.len().await.unwrap(), 0);
    }
    assert!(started.elapsed() < Duration::from_millis(500));
    waiting.await.unwrap();
}

#[tokio::test]
async fn each_value_goes_to_exactly_one_consumer() {
    let set = client().await.sorted_set::<u32>(unique("zset"));
    let mut consumers = Vec::new();
    for _ in 0..5 {
        let set = set.clone();
        consumers.push(tokio::spawn(async move {
            let mut taken = Vec::new();
            while let Some((value, _)) = set
                .pop_first_wait()
                .timeout(Duration::from_millis(600))
                .await
                .unwrap()
            {
                taken.push(value);
            }
            taken
        }));
    }
    sleep(Duration::from_millis(100)).await;
    for value in 0..50u32 {
        set.insert(&value, f64::from(value)).await.unwrap();
    }
    let mut all = Vec::new();
    for consumer in consumers {
        all.extend(consumer.await.unwrap());
    }
    all.sort_unstable();
    assert_eq!(all, (0..50).collect::<Vec<_>>());
}
