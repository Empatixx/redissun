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
    assert!(!bucket.compare_and_set(Some("x"), Some("b")).await.unwrap());
    assert!(bucket.compare_and_set(Some("a"), Some("b")).await.unwrap());
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
    assert!(bucket
        .compare_and_set(Some("world"), Some("again"))
        .await
        .unwrap());
    bucket
        .set_ex("short", Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("short"));

    let owned = "owned".to_string();
    bucket.set(&owned).await.unwrap();
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("owned"));
}

#[tokio::test]
async fn test_get_and_clear_expire() {
    let bucket = client().await.bucket::<i32>(unique("bucket"));
    bucket.set_ex(&1, Duration::from_secs(1)).await.unwrap();
    assert_eq!(bucket.get_and_clear_expire().await.unwrap(), Some(1));
    assert_eq!(bucket.ttl().await.unwrap(), None);
    assert!(bucket.exists().await.unwrap());
}

#[tokio::test]
async fn test_get_and_expire() {
    let bucket = client().await.bucket::<i32>(unique("bucket"));
    bucket.set(&1).await.unwrap();
    assert_eq!(
        bucket.get_and_expire(Duration::from_secs(1)).await.unwrap(),
        Some(1)
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(bucket.get().await.unwrap(), Some(1));
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert!(gone(&bucket).await);
}

#[tokio::test]
async fn test_keep_ttl() {
    let bucket = client().await.bucket::<i32>(unique("bucket"));
    bucket.set_ex(&1234, Duration::from_secs(10)).await.unwrap();
    bucket.set_keep_ttl(&222).await.unwrap();
    assert!(bucket.ttl().await.unwrap().unwrap() > Duration::from_millis(9900));
    assert_eq!(bucket.get().await.unwrap(), Some(222));
}

#[tokio::test]
async fn test_get_and_delete() {
    let bucket = client().await.bucket::<i32>(unique("bucket"));
    bucket.set(&10).await.unwrap();
    assert_eq!(bucket.get_del().await.unwrap(), Some(10));
    assert!(!bucket.exists().await.unwrap());
    assert_eq!(bucket.get_del().await.unwrap(), None);
}

#[tokio::test]
async fn test_size() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    assert_eq!(bucket.size().await.unwrap(), 0);
    bucket.set("1234").await.unwrap();
    assert_eq!(bucket.size().await.unwrap(), 6);
}

#[tokio::test]
async fn test_compare_and_set() {
    let bucket = client().await.bucket::<Vec<String>>(unique("bucket"));
    let list = |value: &str| vec![value.to_string()];
    assert!(bucket
        .compare_and_set(None, Some(&list("81")))
        .await
        .unwrap());
    assert!(!bucket
        .compare_and_set(None, Some(&list("12")))
        .await
        .unwrap());
    assert!(bucket
        .compare_and_set(Some(&list("81")), Some(&list("0")))
        .await
        .unwrap());
    assert_eq!(bucket.get().await.unwrap(), Some(list("0")));
    assert!(!bucket
        .compare_and_set(Some(&list("1")), Some(&list("2")))
        .await
        .unwrap());
    assert_eq!(bucket.get().await.unwrap(), Some(list("0")));
    assert!(bucket
        .compare_and_set(Some(&list("0")), None)
        .await
        .unwrap());
    assert_eq!(bucket.get().await.unwrap(), None);
    assert!(!bucket.exists().await.unwrap());
}

#[tokio::test]
async fn test_compare_and_set_args_expected_null_to_null() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    assert!(bucket.compare_and_set(None::<&str>, None).await.unwrap());
    bucket.set("value").await.unwrap();
    assert!(!bucket.compare_and_set(None::<&str>, None).await.unwrap());
    assert!(!bucket.compare_and_set(Some("other"), None).await.unwrap());
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("value"));
}

#[tokio::test]
async fn test_get_and_set_ttl() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    bucket.set("value1").await.unwrap();
    assert_eq!(
        bucket
            .get_set_ex("value2", Duration::from_millis(500))
            .await
            .unwrap()
            .as_deref(),
        Some("value1")
    );
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("value2"));
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert_eq!(bucket.get().await.unwrap(), None);
}

#[tokio::test]
async fn test_get_and_set() {
    let bucket = client().await.bucket::<Vec<String>>(unique("bucket"));
    assert_eq!(bucket.get_set(&vec!["81".to_string()]).await.unwrap(), None);
    assert_eq!(
        bucket.get_set(&vec!["1".to_string()]).await.unwrap(),
        Some(vec!["81".to_string()])
    );
    assert_eq!(bucket.get().await.unwrap(), Some(vec!["1".to_string()]));
    assert_eq!(bucket.get_del().await.unwrap(), Some(vec!["1".to_string()]));
    assert!(!bucket.exists().await.unwrap());
}

#[tokio::test]
async fn get_set_clears_the_ttl_like_getset() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    bucket.set_ex("a", Duration::from_secs(60)).await.unwrap();
    bucket.get_set("b").await.unwrap();
    assert_eq!(bucket.ttl().await.unwrap(), None);
}

#[tokio::test]
async fn test_set_if_exists() {
    let client = client().await;
    let first = client.bucket::<String>(unique("bucket"));
    assert!(!first.set_xx("0").await.unwrap());
    assert!(!first.exists().await.unwrap());
    first.set("1").await.unwrap();
    assert!(first.set_xx("2").await.unwrap());
    assert_eq!(first.get().await.unwrap().as_deref(), Some("2"));

    let second = client.bucket::<String>(unique("bucket"));
    second.set("1").await.unwrap();
    assert!(second.set_xx_ex("2", Duration::from_secs(1)).await.unwrap());
    assert_eq!(second.get().await.unwrap().as_deref(), Some("2"));
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert!(gone(&second).await);
}

#[tokio::test]
async fn test_try_set() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    assert!(bucket.set_nx("3").await.unwrap());
    assert!(!bucket.set_nx("4").await.unwrap());
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("3"));
}

#[tokio::test]
async fn test_try_set_ttl() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    assert!(bucket
        .set_nx_ex("3", Duration::from_millis(500))
        .await
        .unwrap());
    assert!(!bucket
        .set_nx_ex("4", Duration::from_millis(500))
        .await
        .unwrap());
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("3"));
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert_eq!(bucket.get().await.unwrap(), None);
}

#[tokio::test]
async fn test_expire() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    bucket
        .set_ex("someValue", Duration::from_secs(1))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert!(gone(&bucket).await);
}

#[tokio::test]
async fn test_rename() {
    let client = client().await;
    let old = unique("bucket");
    let new = unique("bucket");
    let bucket = client.bucket::<String>(old.clone());
    bucket.set("someValue").await.unwrap();
    bucket.rename(&new).await.unwrap();
    let renamed = client.bucket::<String>(new);
    renamed.set("value1").await.unwrap();
    assert_eq!(client.bucket::<String>(old).get().await.unwrap(), None);
    assert_eq!(renamed.get().await.unwrap().as_deref(), Some("value1"));
}

#[tokio::test]
async fn rename_of_a_missing_key_fails() {
    let client = client().await;
    let bucket = client.bucket::<String>(unique("bucket"));
    assert!(bucket.rename(&unique("bucket")).await.is_err());
}

#[tokio::test]
async fn test_set_get() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    assert_eq!(bucket.get().await.unwrap(), None);
    bucket.set("somevalue").await.unwrap();
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("somevalue"));
}

#[tokio::test]
async fn test_set_delete() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    bucket.set("somevalue").await.unwrap();
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("somevalue"));
    assert!(bucket.del().await.unwrap());
    assert_eq!(bucket.get().await.unwrap(), None);
    assert!(!bucket.del().await.unwrap());
}

#[tokio::test]
async fn test_set_exist() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    assert_eq!(bucket.get().await.unwrap(), None);
    bucket.set("somevalue").await.unwrap();
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("somevalue"));
    assert!(bucket.exists().await.unwrap());
}

#[tokio::test]
async fn test_set_delete_not_exist() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    assert_eq!(bucket.get().await.unwrap(), None);
    bucket.set("somevalue").await.unwrap();
    assert!(bucket.exists().await.unwrap());
    bucket.del().await.unwrap();
    assert!(!bucket.exists().await.unwrap());
}

#[tokio::test]
async fn test_compare_and_delete_expected() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    bucket.set("value1").await.unwrap();
    assert!(bucket.compare_and_delete("value1").await.unwrap());
    assert!(!bucket.exists().await.unwrap());
    bucket.set("value2").await.unwrap();
    assert!(!bucket.compare_and_delete("wrongValue").await.unwrap());
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("value2"));
    bucket.del().await.unwrap();
    assert!(!bucket.compare_and_delete("anyValue").await.unwrap());
}

#[tokio::test]
async fn test_keys_expire() {
    let client = client().await;
    let first = client.bucket::<i32>(unique("expire-test1"));
    let second = client.bucket::<i32>(unique("expire-test2"));
    let day = Duration::from_secs(24 * 3600);
    for bucket in [&first, &second] {
        bucket.set_ex(&23, Duration::from_secs(3600)).await.unwrap();
        assert!(bucket.expire(day).await.unwrap());
        assert!(bucket.ttl().await.unwrap().unwrap() > Duration::from_secs(23 * 3600));
    }
    let missing = client.bucket::<i32>(unique("expire-miss"));
    assert!(!missing.expire(day).await.unwrap());
    assert!(!missing.persist().await.unwrap());
    assert_eq!(missing.ttl().await.unwrap(), None);
}

#[tokio::test]
async fn test_keys_exists() {
    let client = client().await;
    let bucket = client.bucket::<String>(unique("bucket"));
    assert!(!bucket.exists().await.unwrap());
    bucket.set("1").await.unwrap();
    assert!(bucket.exists().await.unwrap());
}

async fn gone<V, C>(bucket: &redissun::Bucket<V, C>) -> bool
where
    V: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    C: redissun::Codec,
{
    for _ in 0..50 {
        if !bucket.exists().await.unwrap() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}
