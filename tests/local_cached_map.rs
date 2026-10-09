mod common;

use common::{client, redis_url, unique};
use redissun::{Client, JsonCodec, LocalCachedMap, Object, SyncStrategy};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::{sleep, Instant};

type Map = LocalCachedMap<String, String, JsonCodec>;

async fn cached(client: &Client, name: &str) -> Map {
    client
        .local_cached_map::<String, String>(name.to_string())
        .build()
        .await
        .unwrap()
}

async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(5);
    while !check().await {
        assert!(Instant::now() < deadline, "{what}");
        sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn a_read_is_cached_locally() {
    let client = client().await;
    let name = unique("lcm");
    let map = cached(&client, &name).await;
    let plain = client.hash_map::<String, String>(name);
    plain.insert("k", "one").await.unwrap();

    assert_eq!(map.local_len(), 0);
    assert_eq!(map.get("k").await.unwrap().as_deref(), Some("one"));
    assert_eq!(map.local_len(), 1);

    plain.insert("k", "two").await.unwrap();
    assert_eq!(map.get("k").await.unwrap().as_deref(), Some("one"));
    map.clear_local();
    assert_eq!(map.get("k").await.unwrap().as_deref(), Some("two"));
}

#[tokio::test]
async fn a_missing_key_is_not_cached() {
    let map = cached(&client().await, &unique("lcm")).await;
    assert_eq!(map.get("none").await.unwrap(), None);
    assert_eq!(map.local_len(), 0);
}

#[tokio::test]
async fn a_write_updates_the_local_cache_of_the_writer() {
    let client = client().await;
    let name = unique("lcm");
    let map = cached(&client, &name).await;
    let plain = client.hash_map::<String, String>(name);
    assert_eq!(map.insert("k", "one").await.unwrap(), None);
    assert_eq!(
        map.insert("k", "two").await.unwrap().as_deref(),
        Some("one")
    );
    plain.insert("k", "hidden").await.unwrap();
    assert_eq!(map.get("k").await.unwrap().as_deref(), Some("two"));
}

#[tokio::test]
async fn a_write_on_another_instance_invalidates_my_cache() {
    let name = unique("lcm");
    let (first, second) = (client().await, client().await);
    let reader = cached(&first, &name).await;
    let writer = cached(&second, &name).await;
    writer.insert("k", "one").await.unwrap();
    sleep(Duration::from_millis(200)).await;
    assert_eq!(reader.get("k").await.unwrap().as_deref(), Some("one"));
    assert_eq!(reader.local_len(), 1);

    writer.insert("k", "two").await.unwrap();
    eventually("the invalidation never arrived", || async {
        reader.local_len() == 0
    })
    .await;
    assert_eq!(reader.get("k").await.unwrap().as_deref(), Some("two"));
}

#[tokio::test]
async fn removing_on_another_instance_invalidates_my_cache() {
    let name = unique("lcm");
    let (first, second) = (client().await, client().await);
    let reader = cached(&first, &name).await;
    let writer = cached(&second, &name).await;
    writer.insert("k", "one").await.unwrap();
    sleep(Duration::from_millis(200)).await;
    reader.get("k").await.unwrap();
    assert_eq!(writer.remove("k").await.unwrap().as_deref(), Some("one"));
    eventually("the invalidation never arrived", || async {
        reader.local_len() == 0
    })
    .await;
    assert_eq!(reader.get("k").await.unwrap(), None);
}

#[tokio::test]
async fn clearing_on_another_instance_clears_my_cache() {
    let name = unique("lcm");
    let (first, second) = (client().await, client().await);
    let reader = cached(&first, &name).await;
    let writer = cached(&second, &name).await;
    writer.insert("a", "1").await.unwrap();
    writer.insert("b", "2").await.unwrap();
    sleep(Duration::from_millis(200)).await;
    reader.get("a").await.unwrap();
    reader.get("b").await.unwrap();
    assert_eq!(reader.local_len(), 2);
    writer.clear().await.unwrap();
    eventually("the clear never arrived", || async {
        reader.local_len() == 0
    })
    .await;
    assert_eq!(reader.len().await.unwrap(), 0);
}

#[tokio::test]
async fn the_update_strategy_sends_the_new_value() {
    let name = unique("lcm");
    let (first, second) = (client().await, client().await);
    let reader = first
        .local_cached_map::<String, String>(name.clone())
        .sync_strategy(SyncStrategy::Update)
        .build()
        .await
        .unwrap();
    let writer = cached(&second, &name).await;
    drop(writer);
    let writer = second
        .local_cached_map::<String, String>(name.clone())
        .sync_strategy(SyncStrategy::Update)
        .build()
        .await
        .unwrap();
    writer.insert("k", "pushed").await.unwrap();
    eventually("the update never arrived", || async {
        reader.local_len() == 1
    })
    .await;
    let plain = first.hash_map::<String, String>(name);
    plain.insert("k", "hidden").await.unwrap();
    assert_eq!(reader.get("k").await.unwrap().as_deref(), Some("pushed"));
}

#[tokio::test]
async fn the_cache_size_limit_drops_the_least_recently_used() {
    let client = client().await;
    let map = client
        .local_cached_map::<String, String>(unique("lcm"))
        .cache_size(2)
        .build()
        .await
        .unwrap();
    for key in ["a", "b", "c"] {
        map.insert(key, "v").await.unwrap();
    }
    assert_eq!(map.local_len(), 2);
    assert_eq!(map.len().await.unwrap(), 3);
}

#[tokio::test]
async fn local_entries_expire_after_their_ttl() {
    let client = client().await;
    let name = unique("lcm");
    let map = client
        .local_cached_map::<String, String>(name.clone())
        .ttl(Duration::from_millis(200))
        .build()
        .await
        .unwrap();
    let plain = client.hash_map::<String, String>(name);
    map.insert("k", "one").await.unwrap();
    plain.insert("k", "two").await.unwrap();
    assert_eq!(map.get("k").await.unwrap().as_deref(), Some("one"));
    sleep(Duration::from_millis(400)).await;
    assert_eq!(map.get("k").await.unwrap().as_deref(), Some("two"));
}

#[tokio::test]
async fn contains_key_and_object_methods() {
    let map = cached(&client().await, &unique("lcm")).await;
    map.insert("k", "v").await.unwrap();
    assert!(map.contains_key("k").await.unwrap());
    assert!(!map.contains_key("other").await.unwrap());
    assert!(map.exists().await.unwrap());
    assert!(map.del().await.unwrap());
}

#[tokio::test]
async fn the_cache_is_cleared_after_a_reconnect() {
    let client = client().await;
    let name = unique("lcm");
    let map = cached(&client, &name).await;
    map.insert("k", "v").await.unwrap();
    assert_eq!(map.local_len(), 1);

    let url = redis_url().await;
    let mut stream = TcpStream::connect(url.trim_start_matches("redis://"))
        .await
        .unwrap();
    stream
        .write_all(b"*4\r\n$6\r\nCLIENT\r\n$4\r\nKILL\r\n$4\r\nTYPE\r\n$6\r\npubsub\r\n")
        .await
        .unwrap();
    eventually("the cache was not cleared", || async {
        map.local_len() == 0
    })
    .await;
    assert_eq!(map.get("k").await.unwrap().as_deref(), Some("v"));
}
