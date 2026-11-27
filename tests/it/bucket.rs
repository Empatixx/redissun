use crate::common::{client, unique};
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
async fn set_with_ttl_applies_a_ttl() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    bucket
        .set(&"v".to_string())
        .ttl(Duration::from_secs(60))
        .await
        .unwrap();
    let ttl = bucket.ttl().await.unwrap().unwrap();
    assert!(ttl > Duration::from_secs(50) && ttl <= Duration::from_secs(60));
}

#[tokio::test]
async fn try_set_only_stores_the_first_value() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    assert!(bucket.try_set(&"first".to_string()).await.unwrap());
    assert!(!bucket.try_set(&"second".to_string()).await.unwrap());
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("first"));
}

#[tokio::test]
async fn replace_returns_the_previous_value() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    assert_eq!(bucket.replace(&"a".to_string()).await.unwrap(), None);
    assert_eq!(
        bucket.replace(&"b".to_string()).await.unwrap().as_deref(),
        Some("a")
    );
}

#[tokio::test]
async fn take_removes_the_key() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    bucket.set(&"v".to_string()).await.unwrap();
    assert_eq!(bucket.take().await.unwrap().as_deref(), Some("v"));
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
async fn set_ttl_rejects_zero_and_accepts_a_sub_millisecond_ttl() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    let zero = bucket.set(&"v".to_string()).ttl(Duration::ZERO).await;
    assert!(matches!(zero, Err(Error::Config(_))));
    bucket
        .set(&"v".to_string())
        .ttl(Duration::from_micros(500))
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
    assert!(!bucket.try_set("other").await.unwrap());
    assert_eq!(
        bucket.replace("world").await.unwrap().as_deref(),
        Some("hello")
    );
    assert!(bucket
        .compare_and_set(Some("world"), Some("again"))
        .await
        .unwrap());
    bucket
        .set("short")
        .ttl(Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("short"));

    let owned = "owned".to_string();
    bucket.set(&owned).await.unwrap();
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("owned"));
}

// Redisson: testGetAndClearExpire
#[tokio::test]
async fn get_persist_returns_the_value_without_a_ttl() {
    let bucket = client().await.bucket::<i32>(unique("bucket"));
    bucket.set(&1).ttl(Duration::from_secs(1)).await.unwrap();
    assert_eq!(bucket.get_persist().await.unwrap(), Some(1));
    assert_eq!(bucket.ttl().await.unwrap(), None);
    assert!(bucket.exists().await.unwrap());
}

// Redisson: testGetAndExpire
#[tokio::test]
async fn get_expire_returns_the_value_and_sets_a_ttl() {
    let bucket = client().await.bucket::<i32>(unique("bucket"));
    bucket.set(&1).await.unwrap();
    assert_eq!(
        bucket.get_expire(Duration::from_secs(1)).await.unwrap(),
        Some(1)
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(bucket.get().await.unwrap(), Some(1));
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert!(gone(&bucket).await);
}

// Redisson: testKeepTtl
#[tokio::test]
async fn keep_ttl_replaces_the_value_but_keeps_the_ttl() {
    let bucket = client().await.bucket::<i32>(unique("bucket"));
    bucket
        .set(&1234)
        .ttl(Duration::from_secs(10))
        .await
        .unwrap();
    bucket.set(&222).keep_ttl().await.unwrap();
    assert!(bucket.ttl().await.unwrap().unwrap() > Duration::from_millis(9900));
    assert_eq!(bucket.get().await.unwrap(), Some(222));
}

// Redisson: testGetAndDelete
#[tokio::test]
async fn take_returns_the_value_once_and_then_none() {
    let bucket = client().await.bucket::<i32>(unique("bucket"));
    bucket.set(&10).await.unwrap();
    assert_eq!(bucket.take().await.unwrap(), Some(10));
    assert!(!bucket.exists().await.unwrap());
    assert_eq!(bucket.take().await.unwrap(), None);
}

// Redisson: testSize
#[tokio::test]
async fn len_is_the_length_of_the_encoded_value() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    assert_eq!(bucket.len().await.unwrap(), 0);
    bucket.set("1234").await.unwrap();
    assert_eq!(bucket.len().await.unwrap(), 6);
}

// Redisson: testCompareAndSet
#[tokio::test]
async fn compare_and_set_handles_missing_values_and_deletes_on_none() {
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

// Redisson: testCompareAndSetArgsExpectedNullToNull
#[tokio::test]
async fn compare_and_set_none_to_none_needs_a_missing_key() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    assert!(bucket.compare_and_set(None::<&str>, None).await.unwrap());
    bucket.set("value").await.unwrap();
    assert!(!bucket.compare_and_set(None::<&str>, None).await.unwrap());
    assert!(!bucket.compare_and_set(Some("other"), None).await.unwrap());
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("value"));
}

// Redisson: testGetAndSetTtl
#[tokio::test]
async fn replace_with_ttl_swaps_the_value_and_sets_a_ttl() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    bucket.set("value1").await.unwrap();
    assert_eq!(
        bucket
            .replace("value2")
            .ttl(Duration::from_millis(500))
            .await
            .unwrap()
            .as_deref(),
        Some("value1")
    );
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("value2"));
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert_eq!(bucket.get().await.unwrap(), None);
}

// Redisson: testGetAndSet
#[tokio::test]
async fn replace_returns_none_first_and_then_the_previous_value() {
    let bucket = client().await.bucket::<Vec<String>>(unique("bucket"));
    assert_eq!(bucket.replace(&vec!["81".to_string()]).await.unwrap(), None);
    assert_eq!(
        bucket.replace(&vec!["1".to_string()]).await.unwrap(),
        Some(vec!["81".to_string()])
    );
    assert_eq!(bucket.get().await.unwrap(), Some(vec!["1".to_string()]));
    assert_eq!(bucket.take().await.unwrap(), Some(vec!["1".to_string()]));
    assert!(!bucket.exists().await.unwrap());
}

#[tokio::test]
async fn replace_clears_the_ttl_like_getset() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    bucket.set("a").ttl(Duration::from_secs(60)).await.unwrap();
    bucket.replace("b").await.unwrap();
    assert_eq!(bucket.ttl().await.unwrap(), None);
}

// Redisson: testSetIfExists
#[tokio::test]
async fn update_only_overwrites_an_existing_value() {
    let client = client().await;
    let first = client.bucket::<String>(unique("bucket"));
    assert!(!first.update("0").await.unwrap());
    assert!(!first.exists().await.unwrap());
    first.set("1").await.unwrap();
    assert!(first.update("2").await.unwrap());
    assert_eq!(first.get().await.unwrap().as_deref(), Some("2"));

    let second = client.bucket::<String>(unique("bucket"));
    second.set("1").await.unwrap();
    assert!(second
        .update("2")
        .ttl(Duration::from_secs(1))
        .await
        .unwrap());
    assert_eq!(second.get().await.unwrap().as_deref(), Some("2"));
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert!(gone(&second).await);
}

// Redisson: testTrySet
#[tokio::test]
async fn try_set_keeps_the_first_value() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    assert!(bucket.try_set("3").await.unwrap());
    assert!(!bucket.try_set("4").await.unwrap());
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("3"));
}

// Redisson: testTrySetTtl
#[tokio::test]
async fn try_set_with_ttl_keeps_the_first_value_until_it_expires() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    assert!(bucket
        .try_set("3")
        .ttl(Duration::from_millis(500))
        .await
        .unwrap());
    assert!(!bucket
        .try_set("4")
        .ttl(Duration::from_millis(500))
        .await
        .unwrap());
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("3"));
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert_eq!(bucket.get().await.unwrap(), None);
}

// Redisson: testExpire
#[tokio::test]
async fn a_value_set_with_a_ttl_is_gone_after_it() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    bucket
        .set("someValue")
        .ttl(Duration::from_secs(1))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert!(gone(&bucket).await);
}

// Redisson: testRename
#[tokio::test]
async fn rename_moves_the_value_and_leaves_the_old_name_empty() {
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

// Redisson: testSetGet
#[tokio::test]
async fn get_returns_the_value_that_was_set() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    assert_eq!(bucket.get().await.unwrap(), None);
    bucket.set("somevalue").await.unwrap();
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("somevalue"));
}

// Redisson: testSetDelete
#[tokio::test]
async fn del_removes_the_value_and_reports_whether_it_existed() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    bucket.set("somevalue").await.unwrap();
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("somevalue"));
    assert!(bucket.del().await.unwrap());
    assert_eq!(bucket.get().await.unwrap(), None);
    assert!(!bucket.del().await.unwrap());
}

// Redisson: testSetExist
#[tokio::test]
async fn exists_is_true_after_set() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    assert_eq!(bucket.get().await.unwrap(), None);
    bucket.set("somevalue").await.unwrap();
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("somevalue"));
    assert!(bucket.exists().await.unwrap());
}

// Redisson: testSetDeleteNotExist
#[tokio::test]
async fn exists_is_false_after_del() {
    let bucket = client().await.bucket::<String>(unique("bucket"));
    assert_eq!(bucket.get().await.unwrap(), None);
    bucket.set("somevalue").await.unwrap();
    assert!(bucket.exists().await.unwrap());
    bucket.del().await.unwrap();
    assert!(!bucket.exists().await.unwrap());
}

// Redisson: testCompareAndDeleteExpected
#[tokio::test]
async fn compare_and_delete_removes_only_a_matching_value() {
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

// Redisson: testKeysExpire
#[tokio::test]
async fn expire_extends_the_ttl_and_skips_a_missing_key() {
    let client = client().await;
    let first = client.bucket::<i32>(unique("expire-test1"));
    let second = client.bucket::<i32>(unique("expire-test2"));
    let day = Duration::from_secs(24 * 3600);
    for bucket in [&first, &second] {
        bucket
            .set(&23)
            .ttl(Duration::from_secs(3600))
            .await
            .unwrap();
        assert!(bucket.expire(day).await.unwrap());
        assert!(bucket.ttl().await.unwrap().unwrap() > Duration::from_secs(23 * 3600));
    }
    let missing = client.bucket::<i32>(unique("expire-miss"));
    assert!(!missing.expire(day).await.unwrap());
    assert!(!missing.persist().await.unwrap());
    assert_eq!(missing.ttl().await.unwrap(), None);
}

// Redisson: testKeysExists
#[tokio::test]
async fn exists_follows_whether_a_value_was_set() {
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
