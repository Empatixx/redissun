use crate::common::{client, unique};
use futures::TryStreamExt;
use redissun::{Client, DelayedQueue, JsonCodec, Object, VecDeque};
use std::time::Duration;
use tokio::time::{sleep, Instant};

fn pair(
    client: &Client,
    name: &str,
) -> (VecDeque<String, JsonCodec>, DelayedQueue<String, JsonCodec>) {
    let destination = client.vec_deque::<String>(name.to_string());
    let delayed = client.delayed_queue(&destination);
    (destination, delayed)
}

#[tokio::test]
async fn a_value_arrives_in_the_destination_after_its_delay() {
    let client = client().await;
    let (destination, delayed) = pair(&client, &unique("delayed"));
    let started = Instant::now();
    delayed
        .push("later", Duration::from_millis(600))
        .await
        .unwrap();
    assert_eq!(delayed.len().await.unwrap(), 1);
    assert_eq!(destination.len().await.unwrap(), 0);

    let value = destination
        .pop_front_wait()
        .timeout(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(value.as_deref(), Some("later"));
    assert!(started.elapsed() >= Duration::from_millis(550));
    assert_eq!(delayed.len().await.unwrap(), 0);
}

#[tokio::test]
async fn values_arrive_in_the_order_of_their_due_time() {
    let client = client().await;
    let (destination, delayed) = pair(&client, &unique("delayed"));
    delayed
        .push("third", Duration::from_millis(900))
        .await
        .unwrap();
    delayed
        .push("first", Duration::from_millis(100))
        .await
        .unwrap();
    delayed
        .push("second", Duration::from_millis(500))
        .await
        .unwrap();
    let mut seen = Vec::new();
    for _ in 0..3 {
        seen.push(
            destination
                .pop_front_wait()
                .timeout(Duration::from_secs(5))
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert_eq!(seen, ["first", "second", "third"]);
}

#[tokio::test]
async fn a_shorter_delay_pushed_later_overtakes_a_pending_one() {
    let client = client().await;
    let (destination, delayed) = pair(&client, &unique("delayed"));
    delayed.push("slow", Duration::from_secs(30)).await.unwrap();
    sleep(Duration::from_millis(100)).await;
    delayed
        .push("quick", Duration::from_millis(200))
        .await
        .unwrap();
    let value = destination
        .pop_front_wait()
        .timeout(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(value.as_deref(), Some("quick"));
    assert_eq!(delayed.len().await.unwrap(), 1);
}

#[tokio::test]
async fn equal_values_are_kept_separately() {
    let client = client().await;
    let (destination, delayed) = pair(&client, &unique("delayed"));
    delayed
        .push("same", Duration::from_millis(100))
        .await
        .unwrap();
    delayed
        .push("same", Duration::from_millis(100))
        .await
        .unwrap();
    assert_eq!(delayed.len().await.unwrap(), 2);
    for _ in 0..2 {
        assert!(destination
            .pop_front_wait()
            .timeout(Duration::from_secs(5))
            .await
            .unwrap()
            .is_some());
    }
}

#[tokio::test]
async fn zero_delay_is_due_at_once() {
    let client = client().await;
    let (destination, delayed) = pair(&client, &unique("delayed"));
    delayed.push("now", Duration::ZERO).await.unwrap();
    let value = destination
        .pop_front_wait()
        .timeout(Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(value.as_deref(), Some("now"));
}

#[tokio::test]
async fn remove_takes_a_pending_value_out() {
    let client = client().await;
    let (destination, delayed) = pair(&client, &unique("delayed"));
    delayed.push("keep", Duration::from_secs(30)).await.unwrap();
    delayed.push("drop", Duration::from_secs(30)).await.unwrap();
    assert!(delayed.remove("drop").await.unwrap());
    assert!(!delayed.remove("drop").await.unwrap());
    assert_eq!(delayed.len().await.unwrap(), 1);
    assert_eq!(delayed.values().await.unwrap(), ["keep"]);
    assert_eq!(destination.len().await.unwrap(), 0);
}

#[tokio::test]
async fn clear_and_object_methods_cover_both_keys() {
    let client = client().await;
    let (_destination, delayed) = pair(&client, &unique("delayed"));
    delayed.push("a", Duration::from_secs(30)).await.unwrap();
    assert!(delayed.exists().await.unwrap());
    assert!(delayed.del().await.unwrap());
    assert!(delayed.is_empty().await.unwrap());
    delayed.push("b", Duration::from_secs(30)).await.unwrap();
    delayed.clear().await.unwrap();
    assert!(delayed.is_empty().await.unwrap());
}

#[tokio::test]
async fn another_client_delivers_the_values_too() {
    let name = unique("delayed");
    let (destination, delayed) = pair(&client().await, &name);
    delayed
        .push("shared", Duration::from_millis(300))
        .await
        .unwrap();
    drop(delayed);
    let other = client().await;
    let (_, _keeps_the_timer_alive) = pair(&other, &name);
    let value = destination
        .pop_front_wait()
        .timeout(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(value.as_deref(), Some("shared"));
}

async fn contents<V>(queue: &VecDeque<V, JsonCodec>) -> Vec<V>
where
    V: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
{
    queue.iter().try_collect().await.unwrap()
}

#[tokio::test]
async fn the_transfer_stops_when_the_last_handle_is_dropped() {
    let client = client().await;
    let (destination, delayed) = pair(&client, &unique("delayed"));
    let copy = delayed.clone();
    drop(delayed);
    copy.push("first", Duration::from_millis(200))
        .await
        .unwrap();
    sleep(Duration::from_millis(700)).await;
    assert_eq!(destination.len().await.unwrap(), 1);
    copy.push("second", Duration::from_millis(200))
        .await
        .unwrap();
    drop(copy);
    sleep(Duration::from_millis(700)).await;
    assert_eq!(destination.len().await.unwrap(), 1);
    let _again = client.delayed_queue(&destination);
    let deadline = Instant::now() + Duration::from_secs(5);
    while destination.len().await.unwrap() < 2 {
        assert!(Instant::now() < deadline, "the new task did not deliver");
        sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn due_times_come_from_the_client_clock() {
    let client = client().await;
    let name = unique("delayed");
    let (_destination, delayed) = pair(&client, &name);
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    delayed.push("x", Duration::from_secs(60)).await.unwrap();
    let reply = crate::common::raw_command(&[
        "ZRANGE",
        &format!("redissun__delay_timeout:{{{name}}}"),
        "0",
        "0",
        "WITHSCORES",
    ])
    .await;
    // The member starts with random bytes that may hold a newline, so the
    // score is read as the reply's last value rather than by line number.
    let score: i64 = reply
        .trim_end_matches("\r\n")
        .rsplit("\r\n")
        .next()
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        score >= before + 60_000 && score < before + 61_000,
        "{score}"
    );
}

// Redisson: testDestroy
#[tokio::test]
async fn dropping_every_handle_stops_the_delivery_of_pending_values() {
    let client = client().await;
    let (destination, delayed) = numbers(&client, &unique("test"));
    delayed.push(&1, Duration::from_secs(2)).await.unwrap();
    drop(delayed);
    let second = client.delayed_queue(&destination);
    drop(second);
    sleep(Duration::from_secs(1)).await;
    let value = destination
        .pop_front_wait()
        .timeout(Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(value, None);
}

// Redisson: testRemove
#[tokio::test]
async fn a_removed_value_is_never_delivered_but_the_others_are() {
    let client = client().await;
    let (destination, delayed) = pair(&client, &unique("delay_queue"));
    delayed
        .push("1_1_1", Duration::from_millis(300))
        .await
        .unwrap();
    delayed
        .push("1_1_2", Duration::from_millis(700))
        .await
        .unwrap();
    assert!(delayed.contains("1_1_1").await.unwrap());
    assert!(delayed.remove("1_1_1").await.unwrap());
    assert!(!delayed.contains("1_1_1").await.unwrap());
    sleep(Duration::from_millis(1200)).await;
    assert_eq!(contents(&destination).await, ["1_1_2"]);
}

// Redisson: testRemoveAll
#[tokio::test]
async fn remove_all_drops_every_listed_value_before_delivery() {
    let client = client().await;
    let (destination, delayed) = pair(&client, &unique("delay_queue"));
    delayed
        .push("1_1_1", Duration::from_millis(300))
        .await
        .unwrap();
    delayed
        .push("1_1_2", Duration::from_millis(700))
        .await
        .unwrap();
    assert!(delayed.contains("1_1_1").await.unwrap());
    assert!(delayed.contains("1_1_2").await.unwrap());
    assert!(delayed.remove_all(&["1_1_1", "1_1_2"]).await.unwrap());
    assert!(!delayed.contains("1_1_1").await.unwrap());
    assert!(!delayed.contains("1_1_2").await.unwrap());
    sleep(Duration::from_millis(1200)).await;
    assert!(destination.is_empty().await.unwrap());
}

fn numbers(
    client: &Client,
    name: &str,
) -> (VecDeque<i32, JsonCodec>, DelayedQueue<i32, JsonCodec>) {
    let destination = client.vec_deque::<i32>(name.to_string());
    let delayed = client.delayed_queue(&destination);
    (destination, delayed)
}

async fn three_two_one(delayed: &DelayedQueue<i32, JsonCodec>) {
    delayed.push(&3, Duration::from_secs(5)).await.unwrap();
    delayed.push(&1, Duration::from_secs(2)).await.unwrap();
    delayed.push(&2, Duration::from_secs(1)).await.unwrap();
}

// Redisson: testDealyedQueueRetainAll
#[tokio::test]
async fn retain_all_keeps_only_the_listed_values_and_delivers_them() {
    let client = client().await;
    let (destination, delayed) = numbers(&client, &unique("test"));
    three_two_one(&delayed).await;
    assert!(!delayed.retain_all(&[&1, &2, &3]).await.unwrap());
    assert!(!delayed.retain_all(&[&3, &1, &2, &8]).await.unwrap());
    assert_eq!(delayed.values().await.unwrap(), [3, 1, 2]);
    assert!(delayed.retain_all(&[&1, &2]).await.unwrap());
    assert_eq!(delayed.values().await.unwrap(), [1, 2]);
    sleep(Duration::from_millis(5500)).await;
    let mut delivered = contents(&destination).await;
    delivered.sort_unstable();
    assert_eq!(delivered, [1, 2]);
}

// Redisson: testDealyedQueueReadAll
#[tokio::test]
async fn values_lists_the_pending_values_in_insertion_order() {
    let client = client().await;
    let (_destination, delayed) = numbers(&client, &unique("test"));
    three_two_one(&delayed).await;
    assert_eq!(delayed.values().await.unwrap(), [3, 1, 2]);
}

// Redisson: testDealyedQueueRemoveAll
#[tokio::test]
async fn remove_all_reports_true_when_any_listed_value_was_removed() {
    let client = client().await;
    let (_destination, delayed) = numbers(&client, &unique("test"));
    three_two_one(&delayed).await;
    assert!(delayed.remove_all(&[&1, &2]).await.unwrap());
    assert_eq!(delayed.values().await.unwrap(), [3]);
    assert!(delayed.remove_all(&[&3, &4]).await.unwrap());
    assert!(delayed.is_empty().await.unwrap());
}

// Redisson: testDealyedQueueContainsAll
#[tokio::test]
async fn contains_all_is_true_when_every_value_is_pending() {
    let client = client().await;
    let (_destination, delayed) = numbers(&client, &unique("test"));
    three_two_one(&delayed).await;
    assert!(delayed.contains_all(&[&1, &2]).await.unwrap());
    assert!(!delayed.contains_all(&[&1, &2, &4]).await.unwrap());
    assert!(delayed.contains_all(&[&1, &1]).await.unwrap());
    assert!(delayed.contains_all(&[&1, &2, &1]).await.unwrap());
}

// Redisson: testDealyedQueueContains
#[tokio::test]
async fn contains_finds_only_pending_values() {
    let client = client().await;
    let (_destination, delayed) = numbers(&client, &unique("test"));
    three_two_one(&delayed).await;
    assert!(delayed.contains(&1).await.unwrap());
    assert!(!delayed.contains(&4).await.unwrap());
}

// Redisson: testDealyedQueueRemove
#[tokio::test]
async fn remove_reports_whether_the_value_was_pending() {
    let client = client().await;
    let (_destination, delayed) = numbers(&client, &unique("test"));
    three_two_one(&delayed).await;
    assert!(!delayed.remove(&4).await.unwrap());
    assert!(delayed.remove(&3).await.unwrap());
    assert_eq!(delayed.values().await.unwrap(), [1, 2]);
}

// Redisson: testDealyedQueuePeek
#[tokio::test]
async fn peek_returns_the_first_inserted_value() {
    let client = client().await;
    let (_destination, delayed) = numbers(&client, &unique("test"));
    three_two_one(&delayed).await;
    assert_eq!(delayed.peek().await.unwrap(), Some(3));
}

// Redisson: testDealyedQueuePollLastAndOfferFirstTo
#[tokio::test]
async fn poll_last_and_offer_first_to_moves_the_newest_value() {
    let client = client().await;
    let (_destination, delayed) = numbers(&client, &unique("test"));
    delayed.push(&3, Duration::from_secs(5)).await.unwrap();
    delayed.push(&2, Duration::from_secs(2)).await.unwrap();
    delayed.push(&1, Duration::from_secs(1)).await.unwrap();
    let other = client.vec_deque::<i32>(unique("deque2"));
    for value in [6, 5, 4] {
        other.push_back(&value).await.unwrap();
    }
    assert_eq!(
        delayed.poll_last_and_offer_first_to(&other).await.unwrap(),
        Some(1)
    );
    assert_eq!(contents(&other).await, [1, 6, 5, 4]);
}

// Redisson: testDelayedQueueOrder
#[tokio::test]
async fn poll_returns_the_values_in_insertion_order_not_due_order() {
    let client = client().await;
    let (destination, delayed) = pair(&client, &unique("test"));
    for (value, seconds) in [("1", 1), ("4", 4), ("3", 3), ("2", 2)] {
        delayed
            .push(value, Duration::from_secs(seconds))
            .await
            .unwrap();
    }
    assert_eq!(delayed.values().await.unwrap(), ["1", "4", "3", "2"]);
    for value in ["1", "4", "3", "2"] {
        assert_eq!(delayed.poll().await.unwrap().as_deref(), Some(value));
    }
    assert!(destination.is_empty().await.unwrap());
    assert_eq!(destination.pop_front().await.unwrap(), None);
}

// Redisson: testPollLimited
#[tokio::test]
async fn poll_many_takes_pending_values_so_none_are_delivered() {
    let client = client().await;
    let (destination, delayed) = pair(&client, &unique("test"));
    for (value, seconds) in [("1", 1), ("2", 2), ("3", 3), ("4", 4)] {
        delayed
            .push(value, Duration::from_secs(seconds))
            .await
            .unwrap();
    }
    assert_eq!(delayed.poll_many(3).await.unwrap(), ["1", "2", "3"]);
    assert_eq!(delayed.poll_many(2).await.unwrap(), ["4"]);
    assert!(delayed.poll_many(2).await.unwrap().is_empty());
    sleep(Duration::from_millis(1500)).await;
    assert!(destination.is_empty().await.unwrap());
    assert_eq!(destination.pop_front().await.unwrap(), None);
}

// Redisson: testPoll
#[tokio::test]
async fn polled_values_are_never_delivered() {
    let client = client().await;
    let (destination, delayed) = pair(&client, &unique("test"));
    for (value, seconds) in [("1", 1), ("2", 2), ("3", 3), ("4", 4)] {
        delayed
            .push(value, Duration::from_secs(seconds))
            .await
            .unwrap();
    }
    for value in ["1", "2", "3", "4"] {
        assert_eq!(delayed.poll().await.unwrap().as_deref(), Some(value));
    }
    sleep(Duration::from_millis(1500)).await;
    assert!(destination.is_empty().await.unwrap());
    assert_eq!(destination.pop_front().await.unwrap(), None);
}

// Redisson: testDealyedQueue
#[tokio::test]
async fn each_value_is_delivered_at_its_own_due_time() {
    let client = client().await;
    let (destination, delayed) = pair(&client, &unique("test"));
    for (value, seconds) in [("1", 1), ("2", 5), ("4", 4), ("2", 2), ("3", 3)] {
        delayed
            .push(value, Duration::from_secs(seconds))
            .await
            .unwrap();
    }
    let started = Instant::now();
    let at = |millis: u64| sleep_until_offset(started, millis);
    assert_eq!(delayed.values().await.unwrap(), ["1", "2", "4", "2", "3"]);

    at(500).await;
    assert!(destination.is_empty().await.unwrap());
    at(1300).await;
    assert_eq!(contents(&destination).await, ["1"]);
    assert_eq!(delayed.values().await.unwrap(), ["2", "4", "2", "3"]);
    at(1700).await;
    assert_eq!(contents(&destination).await, ["1"]);
    at(2300).await;
    assert_eq!(contents(&destination).await, ["1", "2"]);
    assert_eq!(delayed.values().await.unwrap(), ["2", "4", "3"]);
    at(2700).await;
    assert_eq!(contents(&destination).await, ["1", "2"]);
    at(3300).await;
    assert_eq!(contents(&destination).await, ["1", "2", "3"]);
    assert_eq!(delayed.values().await.unwrap(), ["2", "4"]);
    at(3700).await;
    assert_eq!(contents(&destination).await, ["1", "2", "3"]);
    at(4300).await;
    assert_eq!(contents(&destination).await, ["1", "2", "3", "4"]);
    assert_eq!(delayed.values().await.unwrap(), ["2"]);
    at(4700).await;
    assert_eq!(contents(&destination).await, ["1", "2", "3", "4"]);
    at(5300).await;
    assert_eq!(contents(&destination).await, ["1", "2", "3", "4", "2"]);
    assert!(delayed.is_empty().await.unwrap());

    for value in ["1", "2", "3", "4", "2"] {
        assert_eq!(
            destination.pop_front().await.unwrap().as_deref(),
            Some(value)
        );
    }
}

async fn sleep_until_offset(started: Instant, millis: u64) {
    tokio::time::sleep_until(started + Duration::from_millis(millis)).await;
}
