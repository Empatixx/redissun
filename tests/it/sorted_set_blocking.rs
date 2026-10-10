use crate::common::{client, unique};
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

#[tokio::test]
async fn test_take_first() {
    let set = client().await.sorted_set::<i32>(unique("zset"));
    let producer = set.clone();
    tokio::spawn(async move {
        sleep(Duration::from_secs(1)).await;
        producer.insert(&1, 0.1).await.unwrap();
    });
    let started = Instant::now();
    assert_eq!(set.pop_first_wait().await.unwrap(), (1, 0.1));
    assert!(started.elapsed() > Duration::from_millis(900));
}

async fn abc() -> redissun::SortedSet<String, redissun::JsonCodec> {
    let set = client().await.sorted_set::<String>(unique("zset"));
    for (value, score) in [("a", 0.1), ("b", 0.2), ("c", 0.3)] {
        set.insert(value, score).await.unwrap();
    }
    set
}

async fn names(set: &redissun::SortedSet<String, redissun::JsonCodec>) -> Vec<String> {
    set.range(0..usize::MAX)
        .await
        .unwrap()
        .into_iter()
        .map(|(v, _)| v)
        .collect()
}

#[tokio::test]
async fn test_poll_last_timeout() {
    let empty = client().await.sorted_set::<String>(unique("zset"));
    assert!(empty
        .pop_last_wait()
        .timeout(Duration::from_secs(1))
        .await
        .unwrap()
        .is_none());
    let set = abc().await;
    let popped = set
        .pop_last_wait()
        .timeout(Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(popped.unwrap().0, "c");
    assert_eq!(names(&set).await, ["a", "b"]);
}

#[tokio::test]
async fn test_poll_first_timeout() {
    let empty = client().await.sorted_set::<String>(unique("zset"));
    assert!(empty
        .pop_first_wait()
        .timeout(Duration::from_secs(1))
        .await
        .unwrap()
        .is_none());
    let set = abc().await;
    let popped = set
        .pop_first_wait()
        .timeout(Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(popped.unwrap().0, "a");
    assert_eq!(names(&set).await, ["b", "c"]);
}

async fn six() -> redissun::SortedSet<String, redissun::JsonCodec> {
    let set = client().await.sorted_set::<String>(unique("zset"));
    for (value, score) in [
        ("a", 0.1),
        ("b", 0.2),
        ("c", 0.3),
        ("d", 0.4),
        ("e", 0.5),
        ("f", 0.6),
    ] {
        set.insert(value, score).await.unwrap();
    }
    set
}

#[tokio::test]
async fn test_poll_first_timeout_count() {
    let set = six().await;
    let popped = set
        .pop_first_many_wait(2)
        .timeout(Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(popped, [("a".to_string(), 0.1), ("b".to_string(), 0.2)]);
    assert_eq!(names(&set).await, ["c", "d", "e", "f"]);
    let empty = client().await.sorted_set::<String>(unique("zset"));
    assert!(empty
        .pop_first_many_wait(2)
        .timeout(Duration::from_secs(1))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn test_poll_last_timeout_count() {
    let set = six().await;
    let popped = set
        .pop_last_many_wait(2)
        .timeout(Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(popped, [("f".to_string(), 0.6), ("e".to_string(), 0.5)]);
    assert_eq!(names(&set).await, ["a", "b", "c", "d"]);
    let empty = client().await.sorted_set::<String>(unique("zset"));
    assert!(empty
        .pop_last_many_wait(2)
        .timeout(Duration::from_secs(1))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn test_poll_entry_duration() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    for (value, score) in [
        ("v1", 1.1),
        ("v2", 1.2),
        ("v3", 1.3),
        ("v4", 1.4),
        ("v5", 1.5),
    ] {
        set.insert(value, score).await.unwrap();
    }
    let first = set
        .pop_first_many_wait(2)
        .timeout(Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first, [("v1".to_string(), 1.1), ("v2".to_string(), 1.2)]);
    let mut last = set
        .pop_last_many_wait(2)
        .timeout(Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();
    last.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(last, [("v4".to_string(), 1.4), ("v5".to_string(), 1.5)]);
    assert_eq!(set.len().await.unwrap(), 1);
}

#[tokio::test]
async fn sub_second_timeouts_wait_a_whole_second_like_redisson() {
    let set = client().await.sorted_set::<String>(unique("zset"));
    let started = Instant::now();
    assert!(set
        .pop_first_wait()
        .timeout(Duration::from_millis(200))
        .await
        .unwrap()
        .is_none());
    assert!(started.elapsed() >= Duration::from_millis(900));
}

#[tokio::test]
async fn many_wait_with_zero_timeout_pops_without_waiting() {
    let set = six().await;
    let popped = set
        .pop_first_many_wait(10)
        .timeout(Duration::ZERO)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(popped.len(), 6);
    assert!(set
        .pop_first_many_wait(1)
        .timeout(Duration::ZERO)
        .await
        .unwrap()
        .is_none());
}
