//! Ports of Redisson's `RedissonBaseMultimapCacheTest`, shared by the multimap types with per-key TTLs.

use redissun::multimap::{ListMultimapValues, SetMultimapValues};
use redissun::JsonCodec;

/// The handle methods the shared tests call, under one name for the list and the set handle.
#[allow(async_fn_in_trait)]
pub trait Values {
    async fn size(&self) -> usize;
    async fn empty(&self) -> bool;
    async fn has(&self, v: &str) -> bool;
    async fn remove_one(&self, v: &str) -> bool;
    async fn retain_of(&self, vs: &[&str]) -> bool;
    async fn contains_all_of(&self, vs: &[&str]) -> bool;
    async fn remove_all_of(&self, vs: &[&str]) -> bool;
}

impl Values for ListMultimapValues<String, JsonCodec> {
    async fn size(&self) -> usize {
        self.len().await.unwrap()
    }
    async fn empty(&self) -> bool {
        self.is_empty().await.unwrap()
    }
    async fn has(&self, v: &str) -> bool {
        self.contains(v).await.unwrap()
    }
    async fn remove_one(&self, v: &str) -> bool {
        self.remove_value(v).await.unwrap()
    }
    async fn retain_of(&self, vs: &[&str]) -> bool {
        self.retain_values(vs.iter().copied()).await.unwrap()
    }
    async fn contains_all_of(&self, vs: &[&str]) -> bool {
        self.contains_all(vs.iter().copied()).await.unwrap()
    }
    async fn remove_all_of(&self, vs: &[&str]) -> bool {
        self.remove_values(vs.iter().copied()).await.unwrap()
    }
}

impl Values for SetMultimapValues<String, JsonCodec> {
    async fn size(&self) -> usize {
        self.len().await.unwrap()
    }
    async fn empty(&self) -> bool {
        self.is_empty().await.unwrap()
    }
    async fn has(&self, v: &str) -> bool {
        self.contains(v).await.unwrap()
    }
    async fn remove_one(&self, v: &str) -> bool {
        self.remove(v).await.unwrap()
    }
    async fn retain_of(&self, vs: &[&str]) -> bool {
        self.retain(vs).await.unwrap()
    }
    async fn contains_all_of(&self, vs: &[&str]) -> bool {
        self.contains_all(vs).await.unwrap()
    }
    async fn remove_all_of(&self, vs: &[&str]) -> bool {
        self.remove_values(vs).await.unwrap()
    }
}

/// The tests of `RedissonBaseMultimapCacheTest` for the multimap that `$factory(&Client, &str)` returns, with `String` keys and values and `JsonCodec`.
#[macro_export]
macro_rules! base_multimap_cache_tests {
    ($factory:path) => {
        use redissun::Object as _;
        use $crate::common::multimap_cache::Values as _;

        #[tokio::test]
        async fn redisson_remove_all() {
            let client = $crate::common::client().await;
            let multimap = $factory(&client, &$crate::common::unique("test"));
            multimap.insert("1", "1").await.unwrap();
            multimap.insert("1", "2").await.unwrap();
            multimap.remove_all("1").await.unwrap();
            assert_eq!(multimap.len().await.unwrap(), 0);
        }

        #[tokio::test]
        async fn redisson_contains() {
            let client = $crate::common::client().await;
            let multimap = $factory(&client, &$crate::common::unique("test"));
            for v in ["1", "2", "3"] {
                multimap.insert("1", v).await.unwrap();
            }
            assert!(multimap.contains_key("1").await.unwrap());
            assert!(!multimap.contains_key("2").await.unwrap());
            assert!(multimap.contains_value("1").await.unwrap());
            assert!(multimap.contains_value("3").await.unwrap());
            assert!(!multimap.contains_value("4").await.unwrap());
            assert!(multimap.contains_entry("1", "1").await.unwrap());
            assert!(multimap.contains_entry("1", "3").await.unwrap());
            assert!(!multimap.contains_entry("1", "4").await.unwrap());
        }

        #[tokio::test]
        async fn redisson_contains_expired() {
            let client = $crate::common::client().await;
            let multimap = $factory(&client, &$crate::common::unique("test"));
            for v in ["1", "2", "3"] {
                multimap.insert("1", v).await.unwrap();
            }
            assert!(multimap
                .expire_key("1", ::std::time::Duration::from_secs(1))
                .await
                .unwrap());
            ::tokio::time::sleep(::std::time::Duration::from_millis(1100)).await;
            assert!(!multimap.contains_key("1").await.unwrap());
            assert!(!multimap.contains_key("2").await.unwrap());
            assert!(!multimap.contains_value("1").await.unwrap());
            assert!(!multimap.contains_value("3").await.unwrap());
            assert!(!multimap.contains_value("4").await.unwrap());
            assert!(!multimap.contains_entry("1", "1").await.unwrap());
            assert!(!multimap.contains_entry("1", "3").await.unwrap());
            assert!(!multimap.contains_entry("1", "4").await.unwrap());
        }

        #[tokio::test]
        async fn redisson_get_all() {
            let client = $crate::common::client().await;
            let multimap = $factory(&client, &$crate::common::unique("test"));
            for v in ["1", "2", "3"] {
                multimap.insert("1", v).await.unwrap();
            }
            let mut all = multimap.get_all("1").await.unwrap();
            all.sort();
            assert_eq!(all, ["1", "2", "3"]);
        }

        #[tokio::test]
        async fn redisson_get_all_expired() {
            let client = $crate::common::client().await;
            let multimap = $factory(&client, &$crate::common::unique("test"));
            for v in ["1", "2", "3"] {
                multimap.insert("1", v).await.unwrap();
            }
            multimap
                .expire_key("1", ::std::time::Duration::from_secs(1))
                .await
                .unwrap();
            ::tokio::time::sleep(::std::time::Duration::from_millis(1100)).await;
            assert!(multimap.get_all("1").await.unwrap().is_empty());
        }

        #[tokio::test]
        async fn redisson_values_expired() {
            let client = $crate::common::client().await;
            let multimap = $factory(&client, &$crate::common::unique("test"));
            for v in ["1", "2", "3"] {
                multimap.insert("1", v).await.unwrap();
            }
            multimap
                .expire_key("1", ::std::time::Duration::from_secs(1))
                .await
                .unwrap();
            ::tokio::time::sleep(::std::time::Duration::from_millis(1500)).await;
            let values = multimap.get("1").unwrap();
            assert_eq!(values.size().await, 0);
            assert!(values.empty().await);
            assert!(!values.remove_one("3").await);
            assert!(!values.has("3").await);
            assert!(!values.retain_of(&["1"]).await);
            assert!(!values.contains_all_of(&["1"]).await);
            assert!(!values.remove_all_of(&["1"]).await);
        }

        #[tokio::test]
        async fn redisson_scheduler() {
            let client = $crate::common::client().await;
            let cache = $factory(&client, &$crate::common::unique("simple33"));
            for k in ["1", "2"] {
                for v in ["1", "2", "3"] {
                    assert!(cache.insert(k, v).await.unwrap());
                }
            }
            assert!(cache
                .expire_key("1", ::std::time::Duration::from_secs(2))
                .await
                .unwrap());
            assert!(cache
                .expire_key("2", ::std::time::Duration::from_secs(3))
                .await
                .unwrap());
            assert!(!cache
                .expire_key("3", ::std::time::Duration::from_secs(3))
                .await
                .unwrap());
            assert_eq!(cache.len().await.unwrap(), 6);
            let deadline = ::tokio::time::Instant::now() + ::std::time::Duration::from_secs(13);
            while cache.len().await.unwrap() != 0 {
                assert!(
                    ::tokio::time::Instant::now() < deadline,
                    "not empty after 13 s"
                );
                ::tokio::time::sleep(::std::time::Duration::from_millis(200)).await;
            }
        }

        #[tokio::test]
        async fn redisson_expire() {
            let client = $crate::common::client().await;
            let map = $factory(&client, &$crate::common::unique("simple"));
            map.insert("1", "2").await.unwrap();
            map.insert("2", "3").await.unwrap();
            map.expire(::std::time::Duration::from_millis(100))
                .await
                .unwrap();
            ::tokio::time::sleep(::std::time::Duration::from_millis(500)).await;
            assert_eq!(map.len().await.unwrap(), 0);
        }

        #[tokio::test]
        async fn redisson_clear_expire() {
            let client = $crate::common::client().await;
            let map = $factory(&client, &$crate::common::unique("simple"));
            map.insert("1", "2").await.unwrap();
            map.insert("2", "3").await.unwrap();
            map.expire(::std::time::Duration::from_millis(100))
                .await
                .unwrap();
            map.persist().await.unwrap();
            ::tokio::time::sleep(::std::time::Duration::from_millis(500)).await;
            assert_eq!(map.len().await.unwrap(), 2);
        }

        #[tokio::test]
        async fn redisson_delete() {
            let client = $crate::common::client().await;
            let map = $factory(&client, &$crate::common::unique("simple"));
            map.insert("1", "2").await.unwrap();
            map.insert("2", "3").await.unwrap();
            assert!(map.del().await.unwrap());
            let map2 = $factory(&client, &$crate::common::unique("simple1"));
            assert!(!map2.del().await.unwrap());
        }

        #[tokio::test]
        async fn redisson_contains_all() {
            let client = $crate::common::client().await;
            let multimap = $factory(&client, &$crate::common::unique("test"));
            for v in ["1", "2", "3", "3"] {
                multimap.insert("1", v).await.unwrap();
            }
            let values = multimap.get("1").unwrap();
            assert!(values.contains_all_of(&["1", "1", "1"]).await);
            assert!(!values.contains_all_of(&["1", "2", "4"]).await);
            assert!(values.contains_all_of(&["1", "2", "1"]).await);
            assert!(values.contains_all_of(&["1", "1"]).await);
        }
    };
}
