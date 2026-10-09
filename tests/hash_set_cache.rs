mod common;

use common::{client, connect_with, raw_command, unique};
use futures::StreamExt;
use redissun::Object;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn insert_reports_whether_the_value_was_new() {
    let set = client().await.hash_set_cache::<String>(unique("sc"));
    assert!(set.insert("a").await.unwrap());
    assert!(!set.insert("a").await.unwrap());
    assert!(set.contains("a").await.unwrap());
    assert_eq!(set.len().await.unwrap(), 1);
}

#[tokio::test]
async fn a_value_with_a_ttl_disappears_when_it_runs_out() {
    let set = client().await.hash_set_cache::<String>(unique("sc"));
    assert!(set
        .insert("a")
        .ttl(Duration::from_millis(300))
        .await
        .unwrap());
    set.insert("b").await.unwrap();
    assert!(set.contains("a").await.unwrap());
    sleep(Duration::from_millis(500)).await;
    assert!(!set.contains("a").await.unwrap());
    assert!(set.contains("b").await.unwrap());
    assert_eq!(set.len().await.unwrap(), 1);
    assert!(set.insert("a").await.unwrap());
}

#[tokio::test]
async fn inserting_again_replaces_the_ttl() {
    let set = client().await.hash_set_cache::<String>(unique("sc"));
    set.insert("a")
        .ttl(Duration::from_millis(300))
        .await
        .unwrap();
    assert!(!set.insert("a").await.unwrap());
    sleep(Duration::from_millis(500)).await;
    assert!(set.contains("a").await.unwrap());
    assert_eq!(set.entry_ttl("a").await.unwrap(), None);
}

#[tokio::test]
async fn insert_nx_keeps_a_live_value_and_its_ttl() {
    let set = client().await.hash_set_cache::<String>(unique("sc"));
    assert!(set
        .insert_nx("a")
        .ttl(Duration::from_secs(60))
        .await
        .unwrap());
    assert!(!set.insert_nx("a").await.unwrap());
    let left = set.entry_ttl("a").await.unwrap().unwrap();
    assert!(left > Duration::from_secs(50));
}

#[tokio::test]
async fn insert_nx_replaces_an_expired_value() {
    let set = client().await.hash_set_cache::<String>(unique("sc"));
    set.insert("a")
        .ttl(Duration::from_millis(200))
        .await
        .unwrap();
    sleep(Duration::from_millis(400)).await;
    assert!(set.insert_nx("a").await.unwrap());
    assert!(set.contains("a").await.unwrap());
}

#[tokio::test]
async fn remove_only_counts_live_values() {
    let set = client().await.hash_set_cache::<String>(unique("sc"));
    set.insert("a").await.unwrap();
    set.insert("b")
        .ttl(Duration::from_millis(200))
        .await
        .unwrap();
    assert!(set.remove("a").await.unwrap());
    assert!(!set.remove("a").await.unwrap());
    sleep(Duration::from_millis(400)).await;
    assert!(!set.remove("b").await.unwrap());
}

#[tokio::test]
async fn entry_ttl_reports_the_time_left() {
    let set = client().await.hash_set_cache::<String>(unique("sc"));
    assert_eq!(set.entry_ttl("missing").await.unwrap(), None);
    set.insert("a").ttl(Duration::from_secs(10)).await.unwrap();
    let left = set.entry_ttl("a").await.unwrap().unwrap();
    assert!(left > Duration::from_secs(8) && left <= Duration::from_secs(10));
}

#[tokio::test]
async fn evict_expired_deletes_and_counts() {
    let name = unique("sc");
    let set = client().await.hash_set_cache::<String>(name.clone());
    for value in ["a", "b", "c"] {
        set.insert(value)
            .ttl(Duration::from_millis(200))
            .await
            .unwrap();
    }
    set.insert("keep").await.unwrap();
    sleep(Duration::from_millis(400)).await;
    assert_eq!(set.evict_expired().await.unwrap(), 3);
    assert_eq!(set.evict_expired().await.unwrap(), 0);
    let raw = raw_command(&["ZCARD", &format!("{{{name}}}")]).await;
    assert_eq!(raw.trim(), ":1");
}

#[tokio::test]
async fn the_background_task_removes_expired_values() {
    let client =
        connect_with(|builder| builder.eviction_interval(Duration::from_millis(100))).await;
    let name = unique("sc");
    let set = client.hash_set_cache::<String>(name.clone());
    set.insert("a")
        .ttl(Duration::from_millis(200))
        .await
        .unwrap();
    sleep(Duration::from_millis(1200)).await;
    let raw = raw_command(&["ZCARD", &format!("{{{name}}}")]).await;
    assert_eq!(raw.trim(), ":0");
}

#[tokio::test]
async fn iter_skips_expired_values_and_walks_pages() {
    let set = client().await.hash_set_cache::<u32>(unique("sc"));
    for value in 0..250u32 {
        set.insert(&value).await.unwrap();
    }
    set.insert(&1000)
        .ttl(Duration::from_millis(100))
        .await
        .unwrap();
    sleep(Duration::from_millis(300)).await;
    let mut seen: Vec<u32> = set
        .iter()
        .map(|item| item.unwrap())
        .collect::<Vec<_>>()
        .await;
    seen.sort_unstable();
    assert_eq!(seen, (0..250).collect::<Vec<_>>());
}

#[tokio::test]
async fn clear_and_object_methods() {
    let set = client().await.hash_set_cache::<String>(unique("sc"));
    set.insert("a").await.unwrap();
    assert!(set.exists().await.unwrap());
    set.clear().await.unwrap();
    assert!(set.is_empty().await.unwrap());
    set.insert("b").await.unwrap();
    assert!(set.del().await.unwrap());
}
