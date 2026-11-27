//! `ListMultimapCacheNative` and `SetMultimapCacheNative`: Redisson's `RedissonListMultimapCacheNativeTest`, `RedissonSetMultimapCacheNativeTest` and `RedissonBaseMultimapCacheTest`, plus the field-TTL behaviour of `expire_key`.

mod common;

use common::{client, raw_command, unique};
use redissun::{Client, JsonCodec, ListMultimapCacheNative, SetMultimapCacheNative};
use std::time::Duration;
use tokio::time::sleep;

fn list_native(client: &Client, name: &str) -> ListMultimapCacheNative<String, String, JsonCodec> {
    client.list_multimap_cache_native(name.to_string())
}

fn set_native(client: &Client, name: &str) -> SetMultimapCacheNative<String, String, JsonCodec> {
    client.set_multimap_cache_native(name.to_string())
}

/// The integer of a one-element RESP array reply, such as `HPTTL`'s.
fn first_int(reply: &str) -> i64 {
    reply
        .lines()
        .find_map(|line| line.strip_prefix(':'))
        .and_then(|n| n.trim().parse().ok())
        .unwrap_or_else(|| panic!("no integer in {reply:?}"))
}

async fn field_pttl(name: &str, field: &str) -> i64 {
    first_int(&raw_command(&["HPTTL", name, "FIELDS", "1", field]).await)
}

async fn pttl(key: &str) -> i64 {
    first_int(&raw_command(&["PTTL", key]).await)
}

/// The tests of `expire_key` over field TTLs, for the multimap that `$factory(&Client, &str)` returns.
macro_rules! native_tests {
    ($factory:path) => {
        use super::*;

        #[tokio::test]
        async fn native_expire_key_on_a_missing_key_is_false() {
            let map = $factory(&client().await, &unique("mmn"));
            assert!(!map.expire_key("k", Duration::from_secs(60)).await.unwrap());
            map.insert("other", "x").await.unwrap();
            assert!(!map.expire_key("k", Duration::from_secs(60)).await.unwrap());
        }

        #[tokio::test]
        async fn native_extend_keeps_the_ttl() {
            let name = unique("mmn");
            let map = $factory(&client().await, &name);
            map.insert("k", "a").await.unwrap();
            assert!(map.expire_key("k", Duration::from_secs(60)).await.unwrap());
            assert!(map.extend("k", ["x"]).await.unwrap());
            assert!(field_pttl(&name, "\"k\"").await > 0);
            let values = map.get("k").unwrap().name().to_string();
            assert!(pttl(&values).await > 0);
        }

        #[tokio::test]
        async fn native_expiry_removes_field_and_values_together() {
            let name = unique("mmn");
            let map = $factory(&client().await, &name);
            map.extend("k", ["a", "b"]).await.unwrap();
            map.insert("stay", "c").await.unwrap();
            assert!(map
                .expire_key("k", Duration::from_millis(300))
                .await
                .unwrap());
            sleep(Duration::from_millis(600)).await;
            assert_eq!(map.key_len().await.unwrap(), 1);
            assert_eq!(map.len().await.unwrap(), 1);
            assert!(!map.contains_key("k").await.unwrap());
            let values = map.get("k").unwrap().name().to_string();
            assert_eq!(raw_command(&["EXISTS", &values]).await, ":0\r\n");
            map.remove_all("stay").await.unwrap();
            assert_eq!(map.key_len().await.unwrap(), 0);
        }

        #[tokio::test]
        async fn native_zero_ttl_clears_both() {
            let name = unique("mmn");
            let map = $factory(&client().await, &name);
            map.insert("k", "a").await.unwrap();
            assert!(map.expire_key("k", Duration::from_secs(60)).await.unwrap());
            assert!(map.expire_key("k", Duration::ZERO).await.unwrap());
            assert_eq!(field_pttl(&name, "\"k\"").await, -1);
            let values = map.get("k").unwrap().name().to_string();
            assert_eq!(pttl(&values).await, -1);
            assert!(!map.expire_key("missing", Duration::ZERO).await.unwrap());
        }

        #[tokio::test]
        async fn native_replace_values_makes_the_key_permanent() {
            let name = unique("mmn");
            let map = $factory(&client().await, &name);
            map.insert("k", "a").await.unwrap();
            map.insert("j", "a").await.unwrap();
            assert!(map.expire_key("k", Duration::from_secs(60)).await.unwrap());
            assert!(map.expire_key("j", Duration::from_secs(60)).await.unwrap());
            map.replace_values("k", ["b"]).await.unwrap();
            map.set_values("j", ["b"]).await.unwrap();
            for k in ["k", "j"] {
                assert_eq!(field_pttl(&name, &format!("\"{k}\"")).await, -1);
                let values = map.get(k).unwrap().name().to_string();
                assert_eq!(pttl(&values).await, -1);
            }
            assert_eq!(map.get_all("k").await.unwrap(), ["b"]);
        }

        #[tokio::test]
        async fn native_expire_key_needs_redis_7_4() {
            let (_redis, url) = common::own_redis_tag("7.2-alpine").await;
            let client = Client::builder().url(url.clone()).build().await.unwrap();
            let name = unique("mmn");
            let map = $factory(&client, &name);
            assert!(map.insert("k", "a").await.unwrap());
            assert!(map.extend("k", ["b"]).await.unwrap());
            match map.expire_key("k", Duration::from_secs(60)).await {
                Err(redissun::Error::Unsupported(message)) => {
                    assert!(message.contains("7.4"), "{message}");
                }
                other => panic!("expected Unsupported, got {other:?}"),
            }
            let mut all = map.get_all("k").await.unwrap();
            all.sort();
            assert_eq!(all, ["a", "b"]);
            assert_eq!(map.len().await.unwrap(), 2);
            assert!(map.contains_key("k").await.unwrap());
            let values = map.get("k").unwrap().name().to_string();
            assert_eq!(
                common::raw_command_at(&url, &["PTTL", &values]).await,
                ":-1\r\n"
            );
        }
    };
}

mod list {
    base_multimap_cache_tests!(super::list_native);
    native_tests!(super::list_native);
    every_operation_runs_on_a_cluster!(super::list_native, expire_key);

    // Redisson: RedissonListMultimapCacheNativeTest.testValues
    #[tokio::test]
    async fn redisson_values() {
        let multimap = list_native(&client().await, &unique("test"));
        for v in ["1", "2", "3", "3"] {
            multimap.insert("1", v).await.unwrap();
        }
        let values = multimap.get("1").unwrap();
        assert_eq!(values.len().await.unwrap(), 4);
        assert_eq!(values.to_vec().await.unwrap(), ["1", "2", "3", "3"]);
        assert!(values.remove_value("3").await.unwrap());
        assert!(values.remove_value("3").await.unwrap());
        assert!(!values.remove_value("3").await.unwrap());
        assert!(!values.contains("3").await.unwrap());
        assert!(values.contains("2").await.unwrap());
        assert!(values.contains_all(["1"]).await.unwrap());
        assert!(values.contains_all(["1", "2"]).await.unwrap());
        assert!(values.retain_values(["1"]).await.unwrap());
        assert!(values.remove_values(["1"]).await.unwrap());
    }
}

mod set {
    base_multimap_cache_tests!(super::set_native);
    native_tests!(super::set_native);
    every_operation_runs_on_a_cluster!(super::set_native, expire_key);

    // Redisson: RedissonSetMultimapCacheNativeTest.testValues
    #[tokio::test]
    async fn redisson_values() {
        let multimap = set_native(&client().await, &unique("test"));
        for v in ["1", "2", "3", "3"] {
            multimap.insert("1", v).await.unwrap();
        }
        let values = multimap.get("1").unwrap();
        assert_eq!(values.len().await.unwrap(), 3);
        let mut all = values.to_vec().await.unwrap();
        all.sort();
        assert_eq!(all, ["1", "2", "3"]);
        assert!(values.remove("3").await.unwrap());
        assert!(!values.contains("3").await.unwrap());
        assert!(values.contains("2").await.unwrap());
        assert!(values.contains_all(&["1"]).await.unwrap());
        assert!(values.contains_all(&["1", "2"]).await.unwrap());
        assert!(values.retain(&["1"]).await.unwrap());
        assert!(values.remove_values(&["1"]).await.unwrap());
    }
}
