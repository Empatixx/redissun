mod common;

use common::{client, unique};
use redissun::Object;
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
async fn sub_second_timeouts_wait_one_whole_second() {
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
    assert!(elapsed >= Duration::from_millis(1900), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
}

#[tokio::test]
async fn zero_timeout_pops_once_without_waiting() {
    let deque = client().await.vec_deque::<String>(unique("deque"));
    let started = Instant::now();
    assert_eq!(
        deque
            .pop_front_wait()
            .timeout(Duration::ZERO)
            .await
            .unwrap(),
        None
    );
    assert!(started.elapsed() < Duration::from_millis(500));
    deque.push_back("a").await.unwrap();
    assert_eq!(
        deque.pop_back_wait().timeout(Duration::ZERO).await.unwrap(),
        Some("a".to_string())
    );
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

type Deque = redissun::VecDeque<i32, redissun::JsonCodec>;

async fn deque() -> Deque {
    client().await.vec_deque::<i32>(unique("deque"))
}

async fn all(deque: &Deque) -> Vec<i32> {
    deque.read_all().await.unwrap()
}

fn later(delay: Duration, work: impl std::future::Future<Output = ()> + Send + 'static) {
    tokio::spawn(async move {
        sleep(delay).await;
        work.await;
    });
}

#[tokio::test]
async fn test_move() {
    let deque1 = deque().await;
    let deque2 = deque().await;
    deque2.extend_back(&[4, 5, 6]).await.unwrap();
    {
        let deque1 = deque1.clone();
        later(Duration::from_millis(1500), async move {
            deque1.extend_back(&[1, 2, 3]).await.unwrap();
        });
    }
    let started = Instant::now();
    let moved = deque1
        .pop_front_push_back_wait(&deque2)
        .timeout(Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(moved, None);
    assert!(started.elapsed() >= Duration::from_millis(900));
    let moved = deque1
        .pop_front_push_back_wait(&deque2)
        .timeout(Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(moved, Some(1));
    assert_eq!(all(&deque1).await, [2, 3]);
    assert_eq!(all(&deque2).await, [4, 5, 6, 1]);
    let moved = deque2
        .pop_back_push_front_wait(&deque1)
        .timeout(Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(moved, Some(1));
    assert_eq!(all(&deque1).await, [1, 2, 3]);
    assert_eq!(all(&deque2).await, [4, 5, 6]);
}

#[tokio::test]
async fn test_poll_last_and_offer_first_to_times_out() {
    let source = deque().await;
    let target = deque().await;
    let started = Instant::now();
    let moved = source
        .pop_back_push_front_wait(&target)
        .timeout(Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(moved, None);
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_millis(1000), "{elapsed:?}");
    assert!(elapsed < Duration::from_millis(3000), "{elapsed:?}");
}

#[tokio::test]
async fn test_short_poll() {
    let deque = deque().await;
    let started = Instant::now();
    assert_eq!(
        deque
            .pop_back_wait()
            .timeout(Duration::from_millis(500))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        deque
            .pop_front_wait()
            .timeout(Duration::from_micros(10))
            .await
            .unwrap(),
        None
    );
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_millis(1900), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
}

async fn push_later(first: &Deque, second: &Deque, third: &Deque) {
    let (first, second, third) = (first.clone(), second.clone(), third.clone());
    later(Duration::from_millis(1500), async move {
        third.push_back(&2).await.unwrap();
        first.push_back(&1).await.unwrap();
        second.push_back(&3).await.unwrap();
    });
}

#[tokio::test]
async fn test_poll_last_from_any() {
    let (queue1, queue2, queue3) = (deque().await, deque().await, deque().await);
    push_later(&queue1, &queue2, &queue3).await;
    let started = Instant::now();
    let (name, value) = queue1
        .pop_back_wait_any(&[&queue2, &queue3])
        .timeout(Duration::from_secs(4))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, 2);
    assert_eq!(name, queue3.name());
    assert!(started.elapsed() > Duration::from_millis(1000));
}

#[tokio::test]
async fn test_poll_from_any() {
    let (queue1, queue2, queue3) = (deque().await, deque().await, deque().await);
    push_later(&queue1, &queue2, &queue3).await;
    let started = Instant::now();
    let (_, value) = queue1
        .pop_front_wait_any(&[&queue2, &queue3])
        .timeout(Duration::from_secs(4))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, 2);
    assert!(started.elapsed() > Duration::from_millis(1000));
}

#[tokio::test]
async fn test_poll_from_any_with_name() {
    let (queue1, queue2, queue3) = (deque().await, deque().await, deque().await);
    push_later(&queue1, &queue2, &queue3).await;
    let (name, value) = queue1
        .pop_front_wait_any(&[&queue2, &queue3])
        .timeout(Duration::from_secs(4))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(name, queue3.name());
    assert_eq!(value, 2);
}

#[tokio::test]
async fn test_poll_last_from_any_with_name() {
    let (queue1, queue2, queue3) = (deque().await, deque().await, deque().await);
    let missing = deque().await;
    queue3.extend_back(&[1, 2, 3]).await.unwrap();
    queue1.extend_back(&[4, 5, 6]).await.unwrap();
    queue2.push_back(&7).await.unwrap();
    let wait = Duration::from_secs(4);
    let first = queue1
        .pop_back_wait_any(&[&queue2, &missing])
        .timeout(wait)
        .await
        .unwrap();
    assert_eq!(first, Some((queue1.name().to_string(), 6)));
    let second = queue2
        .pop_back_wait_any(&[&queue1, &queue3])
        .timeout(wait)
        .await
        .unwrap();
    assert_eq!(second, Some((queue2.name().to_string(), 7)));
    let third = queue2
        .pop_back_wait_any(&[&queue3, &queue1])
        .timeout(wait)
        .await
        .unwrap();
    assert_eq!(third, Some((queue3.name().to_string(), 3)));
}

#[tokio::test]
async fn zero_timeout_pops_from_any_once_without_waiting() {
    let (queue1, queue2) = (deque().await, deque().await);
    assert_eq!(
        queue1
            .pop_front_wait_any(&[&queue2])
            .timeout(Duration::ZERO)
            .await
            .unwrap(),
        None
    );
    queue2.push_back(&5).await.unwrap();
    assert_eq!(
        queue1
            .pop_back_wait_any(&[&queue2])
            .timeout(Duration::ZERO)
            .await
            .unwrap(),
        Some((queue2.name().to_string(), 5))
    );
}

#[tokio::test]
async fn test_first_last() {
    let deque = deque().await;
    deque.push_front(&1).await.unwrap();
    deque.push_front(&2).await.unwrap();
    deque.push_back(&3).await.unwrap();
    deque.push_back(&4).await.unwrap();
    assert_eq!(all(&deque).await, [2, 1, 3, 4]);
}

#[tokio::test]
async fn test_take_first() {
    let deque = deque().await;
    deque.push_front(&1).await.unwrap();
    deque.push_front(&2).await.unwrap();
    deque.push_back(&3).await.unwrap();
    deque.push_back(&4).await.unwrap();
    for expected in [2, 1, 3, 4] {
        assert_eq!(deque.pop_front_wait().await.unwrap(), expected);
    }
    assert_eq!(deque.len().await.unwrap(), 0);
}

#[tokio::test]
async fn test_take_last() {
    let deque = deque().await;
    deque.push_front(&1).await.unwrap();
    deque.push_front(&2).await.unwrap();
    deque.push_back(&3).await.unwrap();
    deque.push_back(&4).await.unwrap();
    for expected in [4, 3, 1, 2] {
        assert_eq!(deque.pop_back_wait().await.unwrap(), expected);
    }
    assert_eq!(deque.len().await.unwrap(), 0);
}

fn fill_later(deque: &Deque) {
    let deque = deque.clone();
    later(Duration::from_secs(2), async move {
        deque.push_front(&1).await.unwrap();
        deque.push_front(&2).await.unwrap();
        deque.push_back(&3).await.unwrap();
        deque.push_back(&4).await.unwrap();
    });
}

#[tokio::test]
async fn test_take_first_await() {
    let deque = deque().await;
    fill_later(&deque);
    let started = Instant::now();
    assert_eq!(deque.pop_front_wait().await.unwrap(), 1);
    assert!(started.elapsed() > Duration::from_millis(1900));
    sleep(Duration::from_millis(50)).await;
    for expected in [2, 3, 4] {
        assert_eq!(deque.pop_front_wait().await.unwrap(), expected);
    }
}

#[tokio::test]
async fn test_take_last_await() {
    let deque = deque().await;
    fill_later(&deque);
    let started = Instant::now();
    assert_eq!(deque.pop_back_wait().await.unwrap(), 1);
    assert!(started.elapsed() > Duration::from_millis(1900));
    sleep(Duration::from_millis(50)).await;
    for expected in [4, 3, 2] {
        assert_eq!(deque.pop_back_wait().await.unwrap(), expected);
    }
}

#[tokio::test]
async fn test_poll_first() {
    let deque = deque().await;
    deque.extend_back(&[1, 2, 3]).await.unwrap();
    for expected in [1, 2, 3] {
        let value = deque
            .pop_front_wait()
            .timeout(Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(value, Some(expected));
    }
    let started = Instant::now();
    let value = deque
        .pop_front_wait()
        .timeout(Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(value, None);
    assert!(started.elapsed() >= Duration::from_millis(1900));
}

#[tokio::test]
async fn test_poll_last() {
    let deque = deque().await;
    deque.extend_back(&[1, 2, 3]).await.unwrap();
    for expected in [3, 2, 1] {
        let value = deque
            .pop_back_wait()
            .timeout(Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(value, Some(expected));
    }
    let started = Instant::now();
    let value = deque
        .pop_back_wait()
        .timeout(Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(value, None);
    assert!(started.elapsed() >= Duration::from_millis(1900));
}

#[tokio::test]
async fn test_take() {
    let deque = deque().await;
    {
        let deque = deque.clone();
        later(Duration::from_secs(2), async move {
            deque.push_back(&3).await.unwrap();
        });
    }
    let started = Instant::now();
    assert_eq!(deque.pop_front_wait().await.unwrap(), 3);
    assert!(started.elapsed() > Duration::from_millis(1900));
}

#[tokio::test]
async fn test_poll() {
    let deque = deque().await;
    deque.push_back(&1).await.unwrap();
    let value = deque
        .pop_front_wait()
        .timeout(Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(value, Some(1));
    let started = Instant::now();
    let value = deque
        .pop_front_wait()
        .timeout(Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(value, None);
    assert!(started.elapsed() >= Duration::from_millis(1900));
}

#[tokio::test]
async fn test_await() {
    let deque = deque().await;
    deque.push_back(&1).await.unwrap();
    let value = deque
        .pop_front_wait()
        .timeout(Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(value, Some(1));
}

#[tokio::test]
async fn test_poll_last_and_offer_first_to() {
    let client = client().await;
    let tag = unique("queue");
    let queue1 = client.vec_deque::<i32>(format!("{{{tag}}}1"));
    let queue2 = client.vec_deque::<i32>(format!("{{{tag}}}2"));
    {
        let queue1 = queue1.clone();
        later(Duration::from_millis(1500), async move {
            queue1.push_back(&3).await.unwrap();
        });
    }
    queue2.extend_back(&[4, 5, 6]).await.unwrap();
    let value = queue1
        .pop_back_push_front_wait(&queue2)
        .timeout(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(value, Some(3));
    assert_eq!(all(&queue2).await, [3, 4, 5, 6]);
    let value = queue1
        .pop_back_push_front_wait(&queue2)
        .timeout(Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(value, None);
}

#[tokio::test]
async fn test_take_last_and_offer_first_to() {
    let client = client().await;
    let tag = unique("queue");
    let queue1 = client.vec_deque::<i32>(format!("{{{tag}}}1"));
    let queue2 = client.vec_deque::<i32>(format!("{{{tag}}}2"));
    {
        let queue1 = queue1.clone();
        later(Duration::from_millis(1500), async move {
            queue1.push_back(&3).await.unwrap();
        });
    }
    queue2.extend_back(&[4, 5, 6]).await.unwrap();
    let started = Instant::now();
    let value = queue1.pop_back_push_front_wait(&queue2).await.unwrap();
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_millis(1400), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(4), "{elapsed:?}");
    assert_eq!(value, 3);
    assert_eq!(all(&queue2).await, [3, 4, 5, 6]);
}

#[tokio::test]
async fn test_single_char_as_key_name() {
    let client = client().await;
    let value = "Long Test Message;".repeat(40);
    for i in 0..10 {
        let queue = client.vec_deque::<String>(i.to_string());
        queue.clear().await.unwrap();
        queue.push_back(&value).await.unwrap();
        queue.expire(Duration::from_secs(60)).await.unwrap();
        let polled = queue
            .pop_front_wait()
            .timeout(Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(polled.as_deref(), Some(value.as_str()));
    }
}

#[tokio::test]
async fn test_move_timeout() {
    let source = deque().await;
    let destination = deque().await;
    let started = Instant::now();
    let moved = source
        .pop_front_push_back_wait(&destination)
        .timeout(Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(moved, None);
    assert!(started.elapsed() >= Duration::from_millis(900));
    assert!(destination.is_empty().await.unwrap());
}

#[tokio::test]
async fn test_move_sub_second_timeout() {
    let source = deque().await;
    let destination = deque().await;
    let moved = source
        .pop_front_push_back_wait(&destination)
        .timeout(Duration::from_millis(500))
        .await
        .unwrap();
    assert_eq!(moved, None);
    assert!(destination.is_empty().await.unwrap());
}

#[tokio::test]
async fn test_take_async_cancel() {
    let deque = deque().await;
    for _ in 0..10 {
        let waiter = {
            let deque = deque.clone();
            tokio::spawn(async move { deque.pop_front_wait().await })
        };
        sleep(Duration::from_millis(20)).await;
        waiter.abort();
    }
    sleep(Duration::from_millis(500)).await;
    deque.push_back(&1).await.unwrap();
    deque.push_back(&2).await.unwrap();
    sleep(Duration::from_millis(300)).await;
    assert_eq!(all(&deque).await, [1, 2]);
}
