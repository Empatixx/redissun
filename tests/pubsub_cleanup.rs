mod common;

use common::{client, subscribed_channels, unique};
use std::time::Duration;
use tokio::time::{sleep, Instant};

async fn eventually_no_channels(pattern: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while subscribed_channels(pattern).await > 0 {
        assert!(
            Instant::now() < deadline,
            "subscriptions were not released for {pattern}"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn a_subscription_exists_while_waiting_and_is_released_afterwards() {
    let name = unique("cleanup");
    let pattern = format!("redissun__unlock__{name}");
    let holder = client().await.lock(name.clone()).lock().await.unwrap();
    let waiter = client().await.lock(name);

    let waiting = tokio::spawn(async move { waiter.lock_for(Duration::from_millis(1200)).await });
    sleep(Duration::from_millis(400)).await;
    assert_eq!(subscribed_channels(&pattern).await, 1);

    assert!(waiting.await.unwrap().unwrap().is_none());
    eventually_no_channels(&pattern).await;
    holder.unlock().await.unwrap();
}

#[tokio::test]
async fn waiters_on_one_lock_share_a_subscription_until_the_last_one_leaves() {
    let name = unique("cleanup");
    let pattern = format!("redissun__unlock__{name}");
    let holder = client().await.lock(name.clone()).lock().await.unwrap();
    let client = client().await;
    let short = client.lock(name.clone());
    let long = client.lock(name);

    let first = tokio::spawn(async move { short.lock_for(Duration::from_millis(500)).await });
    let second = tokio::spawn(async move { long.lock_for(Duration::from_millis(1800)).await });
    sleep(Duration::from_millis(900)).await;
    assert!(first.await.unwrap().unwrap().is_none());
    assert_eq!(subscribed_channels(&pattern).await, 1);

    assert!(second.await.unwrap().unwrap().is_none());
    eventually_no_channels(&pattern).await;
    holder.unlock().await.unwrap();
}

#[tokio::test]
async fn waiting_works_again_after_the_subscription_was_released() {
    let name = unique("cleanup");
    let pattern = format!("redissun__unlock__{name}");
    let first_client = client().await;
    let second_client = client().await;

    let holder = first_client.lock(name.clone()).lock().await.unwrap();
    let gave_up = second_client
        .lock(name.clone())
        .lock_for(Duration::from_millis(300))
        .await
        .unwrap();
    assert!(gave_up.is_none());
    eventually_no_channels(&pattern).await;

    let waiter_lock = second_client.lock(name);
    let waiter = tokio::spawn(async move { waiter_lock.lock().await.unwrap() });
    sleep(Duration::from_millis(300)).await;
    holder.unlock().await.unwrap();

    let guard = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("a fresh subscription must still deliver the unlock message")
        .unwrap();
    guard.unlock().await.unwrap();
    eventually_no_channels(&pattern).await;
}

#[tokio::test]
async fn many_distinct_lock_names_leave_no_subscriptions_behind() {
    let prefix = unique("cleanup-many");
    let pattern = format!("redissun__unlock__{prefix}:*");
    let holder_client = client().await;
    let waiter_client = client().await;

    let mut holders = Vec::new();
    for i in 0..20 {
        holders.push(
            holder_client
                .lock(format!("{prefix}:{i}"))
                .lock()
                .await
                .unwrap(),
        );
    }
    for i in 0..20 {
        let gave_up = waiter_client
            .lock(format!("{prefix}:{i}"))
            .lock_for(Duration::from_millis(30))
            .await
            .unwrap();
        assert!(gave_up.is_none());
    }
    eventually_no_channels(&pattern).await;
    drop(holders);
}
