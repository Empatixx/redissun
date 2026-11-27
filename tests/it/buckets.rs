// Ported from Redisson's RedissonBucketsTest. The `update` and `try_set(..).expiry(..)` tests need `MSETEX`, so they run on Redis 8.
use crate::common::{client, redis8_client, unique};
use redissun::{Expiry, Object, StringCodec};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

#[tokio::test]
async fn get_gives_the_keys_that_exist() {
    let client = client().await;
    let names: Vec<String> = (1..=4).map(|i| unique(&format!("test{i}"))).collect();
    client
        .bucket::<String>(&*names[0])
        .set("someValue1")
        .await
        .unwrap();
    client
        .bucket::<String>(&*names[2])
        .set("someValue3")
        .await
        .unwrap();
    let found = client.buckets::<String>().get(&names).await.unwrap();
    let expected = HashMap::from([
        (names[0].clone(), "someValue1".to_string()),
        (names[2].clone(), "someValue3".to_string()),
    ]);
    assert_eq!(found, expected);
}

#[tokio::test]
async fn buckets_use_the_codec_of_their_client() {
    let client = client().await.with_codec(StringCodec);
    let buckets = client.buckets::<String>();
    let names: Vec<String> = ["A", "B", "C"]
        .iter()
        .map(|name| unique(&format!("buckets:{name}")))
        .collect();
    assert!(buckets.get(&names).await.unwrap().is_empty());
    buckets
        .set([(&names[0], "XYZ"), (&names[1], "OPM"), (&names[2], "123")])
        .await
        .unwrap();
    let found = buckets.get(&names).await.unwrap();
    assert_eq!(found.len(), 3);
    assert_eq!(found[&names[0]], "XYZ");
    assert_eq!(
        client
            .bucket::<String>(&*names[2])
            .get()
            .await
            .unwrap()
            .as_deref(),
        Some("123")
    );
}

#[tokio::test]
async fn set_stores_every_value() {
    let client = client().await;
    let (a, b) = (unique("12"), unique("41"));
    client
        .buckets::<i32>()
        .set([(&a, &1), (&b, &2)])
        .await
        .unwrap();
    assert_eq!(client.bucket::<i32>(&*a).get().await.unwrap(), Some(1));
    assert_eq!(client.bucket::<i32>(&*b).get().await.unwrap(), Some(2));
}

#[tokio::test]
async fn try_set_stores_nothing_when_a_key_exists() {
    let client = client().await;
    let (k12, k41, k61) = (unique("12"), unique("41"), unique("61"));
    client.bucket::<String>(&*k12).set("341").await.unwrap();
    let buckets = client.buckets::<i32>();
    assert!(!buckets.try_set([(&k12, &1), (&k41, &2)]).await.unwrap());
    assert_eq!(client.bucket::<i32>(&*k41).get().await.unwrap(), None);
    assert!(buckets.try_set([(&k61, &1), (&k41, &2)]).await.unwrap());
    assert_eq!(client.bucket::<i32>(&*k61).get().await.unwrap(), Some(1));
    assert_eq!(client.bucket::<i32>(&*k41).get().await.unwrap(), Some(2));
}

#[tokio::test]
async fn nothing_to_write_writes_nothing() {
    let buckets = client().await.buckets::<i32>();
    let none: [(&str, &i32); 0] = [];
    buckets.set(none).await.unwrap();
    assert!(!buckets.try_set(none).await.unwrap());
    let names: [&str; 0] = [];
    assert!(buckets.get(&names).await.unwrap().is_empty());
}

#[tokio::test]
async fn update_stores_only_when_every_key_exists() {
    let client = redis8_client().await;
    let (k12, k23, k41) = (unique("12"), unique("23"), unique("41"));
    client.bucket::<String>(&*k12).set("341").await.unwrap();
    client.bucket::<String>(&*k23).set("234").await.unwrap();
    let buckets = client.buckets::<i32>();
    assert!(!buckets
        .update([(&k12, &1), (&k41, &2)])
        .expiry(Expiry::None)
        .await
        .unwrap());
    client.bucket::<String>(&*k41).set("123").await.unwrap();
    let at = SystemTime::now() + Duration::from_secs(10);
    assert!(buckets
        .update([(&k12, &1), (&k41, &2)])
        .expiry(Expiry::At(at))
        .await
        .unwrap());
    assert!(buckets
        .update([(&k41, &3), (&k23, &4)])
        .expiry(Expiry::After(Duration::from_secs(2)))
        .await
        .unwrap());
    assert!(client.keys().ttl(&k41).await.unwrap().unwrap() <= Duration::from_secs(2));
    let left = client.keys().ttl(&k12).await.unwrap().unwrap();
    assert!(left > Duration::from_secs(8) && left <= Duration::from_secs(10));
    assert_eq!(client.bucket::<i32>(&*k41).get().await.unwrap(), Some(3));
    assert_eq!(client.bucket::<i32>(&*k23).get().await.unwrap(), Some(4));
    assert!(buckets
        .update([(&k41, &3), (&k23, &4), (&k12, &5)])
        .expiry(Expiry::KeepTtl)
        .await
        .unwrap());
    assert_eq!(client.bucket::<i32>(&*k12).get().await.unwrap(), Some(5));
    assert!(client.keys().ttl(&k12).await.unwrap().is_some());
}

#[tokio::test]
async fn try_set_with_expiry_stores_only_when_no_key_exists() {
    let client = redis8_client().await;
    let (k12, k23, k34, k41, k45, k56) = (
        unique("12"),
        unique("23"),
        unique("34"),
        unique("41"),
        unique("45"),
        unique("56"),
    );
    client.bucket::<String>(&*k12).set("341").await.unwrap();
    let buckets = client.buckets::<i32>();
    assert!(!buckets
        .try_set([(&k12, &1), (&k41, &2)])
        .expiry(Expiry::None)
        .await
        .unwrap());
    assert!(buckets
        .try_set([(&k41, &3), (&k23, &4)])
        .expiry(Expiry::After(Duration::from_secs(2)))
        .await
        .unwrap());
    let at = SystemTime::now() + Duration::from_secs(10);
    assert!(buckets
        .try_set([(&k34, &5), (&k45, &6)])
        .expiry(Expiry::At(at))
        .await
        .unwrap());
    assert_eq!(client.bucket::<String>(&*k12).ttl().await.unwrap(), None);
    assert!(client.keys().ttl(&k41).await.unwrap().unwrap() <= Duration::from_secs(2));
    let left = client.keys().ttl(&k34).await.unwrap().unwrap();
    assert!(left > Duration::from_secs(8) && left <= Duration::from_secs(10));
    assert_eq!(client.bucket::<i32>(&*k41).get().await.unwrap(), Some(3));
    assert_eq!(client.bucket::<i32>(&*k34).get().await.unwrap(), Some(5));
    assert!(buckets
        .try_set([(&k56, &5)])
        .expiry(Expiry::KeepTtl)
        .await
        .unwrap());
    assert_eq!(client.bucket::<i32>(&*k56).get().await.unwrap(), Some(5));
}
