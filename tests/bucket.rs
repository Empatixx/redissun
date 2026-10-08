mod common;

use common::{client, unique};
use redissun::{Error, Object};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct User {
    name: String,
    age: u32,
}

#[tokio::test]
async fn set_then_get_round_trips_a_struct() {
    let bucket = client().await.bucket::<User>(unique("bucket"));
    let user = User {
        name: "jirka".into(),
        age: 30,
    };
    bucket.set(&user).await.unwrap();
    assert_eq!(bucket.get().await.unwrap(), Some(user));
}

#[tokio::test]
async fn get_on_missing_key_is_none() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    assert_eq!(bucket.get().await.unwrap(), None);
}

#[tokio::test]
async fn set_ex_applies_a_ttl() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    bucket
        .set_ex(&"v".to_string(), Duration::from_secs(60))
        .await
        .unwrap();
    let ttl = bucket.ttl().await.unwrap().unwrap();
    assert!(ttl > Duration::from_secs(50) && ttl <= Duration::from_secs(60));
}

#[tokio::test]
async fn set_nx_only_stores_the_first_value() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    assert!(bucket.set_nx(&"first".to_string()).await.unwrap());
    assert!(!bucket.set_nx(&"second".to_string()).await.unwrap());
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("first"));
}

#[tokio::test]
async fn get_set_returns_the_previous_value() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    assert_eq!(bucket.get_set(&"a".to_string()).await.unwrap(), None);
    assert_eq!(
        bucket.get_set(&"b".to_string()).await.unwrap().as_deref(),
        Some("a")
    );
}

#[tokio::test]
async fn get_del_removes_the_key() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    bucket.set(&"v".to_string()).await.unwrap();
    assert_eq!(bucket.get_del().await.unwrap().as_deref(), Some("v"));
    assert!(!bucket.exists().await.unwrap());
}

#[tokio::test]
async fn compare_and_set_swaps_only_on_match() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    bucket.set(&"a".to_string()).await.unwrap();
    assert!(!bucket
        .compare_and_set(&"x".to_string(), &"b".to_string())
        .await
        .unwrap());
    assert!(bucket
        .compare_and_set(&"a".to_string(), &"b".to_string())
        .await
        .unwrap());
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("b"));
}

#[tokio::test]
async fn object_operations_manage_the_key() {
    let client = client().await;
    let name = unique("bucket");
    let bucket = client.bucket::<String>(name.clone());
    assert_eq!(bucket.name(), name);
    assert!(!bucket.exists().await.unwrap());

    bucket.set(&"v".to_string()).await.unwrap();
    assert!(bucket.exists().await.unwrap());
    assert_eq!(bucket.ttl().await.unwrap(), None);

    assert!(bucket.expire(Duration::from_secs(30)).await.unwrap());
    assert!(bucket.ttl().await.unwrap().is_some());
    assert!(bucket.persist().await.unwrap());
    assert_eq!(bucket.ttl().await.unwrap(), None);

    let renamed = unique("bucket");
    bucket.rename(&renamed).await.unwrap();
    assert!(!bucket.exists().await.unwrap());
    let moved = client.bucket::<String>(renamed);
    assert_eq!(moved.get().await.unwrap().as_deref(), Some("v"));

    assert!(moved.del().await.unwrap());
    assert!(!moved.del().await.unwrap());
}

#[tokio::test]
async fn reading_with_the_wrong_type_is_a_codec_error_not_a_panic() {
    let client = client().await;
    let name = unique("bucket");
    client
        .bucket::<String>(name.clone())
        .set(&"not a number".to_string())
        .await
        .unwrap();
    let result = client.bucket::<i64>(name).get().await;
    assert!(matches!(result, Err(Error::Codec(_))));
}

#[tokio::test]
async fn unicode_and_empty_values_round_trip() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    for value in ["", "žluťoučký kůň 🐎", "line\nbreak \"quoted\""] {
        bucket.set(&value.to_string()).await.unwrap();
        assert_eq!(bucket.get().await.unwrap().as_deref(), Some(value));
    }
}

#[tokio::test]
async fn set_ex_rejects_a_zero_ttl_and_accepts_a_sub_millisecond_ttl() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    let zero = bucket.set_ex(&"v".to_string(), Duration::ZERO).await;
    assert!(matches!(zero, Err(Error::Config(_))));
    bucket
        .set_ex(&"v".to_string(), Duration::from_micros(500))
        .await
        .unwrap();
}

#[tokio::test]
async fn expire_rejects_a_zero_ttl_and_rounds_a_sub_millisecond_ttl_up() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    bucket.set(&"v".to_string()).await.unwrap();
    assert!(matches!(
        bucket.expire(Duration::ZERO).await,
        Err(Error::Config(_))
    ));
    assert!(bucket.expire(Duration::from_micros(500)).await.unwrap());
}

#[tokio::test]
async fn a_string_bucket_accepts_str_references() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    bucket.set("hello").await.unwrap();
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("hello"));
    assert!(!bucket.set_nx("other").await.unwrap());
    assert_eq!(
        bucket.get_set("world").await.unwrap().as_deref(),
        Some("hello")
    );
    assert!(bucket.compare_and_set("world", "again").await.unwrap());
    bucket
        .set_ex("short", Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("short"));

    let owned = "owned".to_string();
    bucket.set(&owned).await.unwrap();
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("owned"));
}
