mod common;

use common::{client, unique};
use redissun::Error;
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::{sleep, Instant};

#[tokio::test]
async fn returns_at_once_when_an_element_is_there() {
    let deque = client().await.vec_deque::<String>(unique("deque"));
    deque.push_back("a").await.unwrap();
    let started = Instant::now();
    let value = deque
        .pop_front_wait()
        .timeout(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(value, Some("a".to_string()));
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn times_out_with_none() {
    let deque = client().await.vec_deque::<String>(unique("deque"));
    let started = Instant::now();
    assert_eq!(
        deque
            .pop_front_wait()
            .timeout(Duration::from_millis(300))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        deque
            .pop_back_wait()
            .timeout(Duration::from_millis(300))
            .await
            .unwrap(),
        None
    );
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_millis(550), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(3), "{elapsed:?}");
}

#[tokio::test]
async fn zero_timeout_is_a_config_error() {
    let deque = client().await.vec_deque::<String>(unique("deque"));
    assert!(matches!(
        deque.pop_front_wait().timeout(Duration::ZERO).await,
        Err(Error::Config(_))
    ));
}

#[tokio::test]
async fn wakes_up_when_another_task_pushes() {
    let deque = client().await.vec_deque::<String>(unique("deque"));
    let waiter = {
        let deque = deque.clone();
        tokio::spawn(async move { deque.pop_front_wait().await })
    };
    sleep(Duration::from_millis(300)).await;
    let pushed_at = Instant::now();
    deque.push_back("job").await.unwrap();
    assert_eq!(waiter.await.unwrap().unwrap(), "job");
    assert!(pushed_at.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn pop_back_wait_takes_from_the_back() {
    let deque = client().await.vec_deque::<String>(unique("deque"));
    let waiter = {
        let deque = deque.clone();
        tokio::spawn(async move { deque.pop_back_wait().await })
    };
    sleep(Duration::from_millis(300)).await;
    deque.push_front("a").await.unwrap();
    assert_eq!(waiter.await.unwrap().unwrap(), "a");
}

#[tokio::test]
async fn a_waiting_pop_does_not_stall_other_commands() {
    let client = client().await;
    let deque = client.vec_deque::<String>(unique("deque"));
    let waiter = {
        let deque = deque.clone();
        tokio::spawn(async move { deque.pop_front_wait().await })
    };
    sleep(Duration::from_millis(300)).await;
    let bucket = client.bucket::<u32>(unique("bucket"));
    let started = Instant::now();
    for i in 0..40u32 {
        bucket.set(&i).await.unwrap();
        assert_eq!(bucket.get().await.unwrap(), Some(i));
    }
    assert!(started.elapsed() < Duration::from_secs(2));
    deque.push_back("done").await.unwrap();
    assert_eq!(waiter.await.unwrap().unwrap(), "done");
}

#[tokio::test]
async fn each_element_goes_to_exactly_one_waiting_consumer() {
    let deque = client().await.vec_deque::<u32>(unique("deque"));
    let consumers: Vec<_> = (0..5)
        .map(|_| {
            let deque = deque.clone();
            tokio::spawn(async move { deque.pop_front_wait().await.unwrap() })
        })
        .collect();
    sleep(Duration::from_millis(400)).await;
    for i in 0..5u32 {
        deque.push_back(&i).await.unwrap();
    }
    let mut received = HashSet::new();
    for consumer in consumers {
        received.insert(consumer.await.unwrap());
    }
    assert_eq!(received, (0..5).collect());
    assert!(deque.is_empty().await.unwrap());
}

#[tokio::test]
async fn dropping_a_pending_pop_does_not_break_the_next_one() {
    let deque = client().await.vec_deque::<String>(unique("deque"));
    let waiter = {
        let deque = deque.clone();
        tokio::spawn(async move { deque.pop_front_wait().await })
    };
    sleep(Duration::from_millis(300)).await;
    waiter.abort();
    sleep(Duration::from_millis(300)).await;
    deque.push_back("x").await.unwrap();
    sleep(Duration::from_millis(200)).await;
    assert_eq!(deque.pop_front().await.unwrap(), Some("x".to_string()));
}

#[tokio::test]
async fn a_pop_dropped_right_after_it_starts_leaves_no_ghost_consumer() {
    let deque = client().await.vec_deque::<String>(unique("deque"));
    for micros in [0u64, 100, 300, 600, 1000, 2000, 4000, 8000, 15000] {
        let _ = tokio::time::timeout(Duration::from_micros(micros), deque.pop_front_wait()).await;
    }
    sleep(Duration::from_millis(500)).await;
    deque.push_back("x").await.unwrap();
    sleep(Duration::from_millis(300)).await;
    assert_eq!(deque.pop_front().await.unwrap(), Some("x".to_string()));
}
