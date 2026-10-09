mod common;

use common::{client, unique};
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
