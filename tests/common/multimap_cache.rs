//! Tests shared by the multimap test files: the ports of Redisson's `RedissonBaseMultimapCacheTest` for the types with per-key TTLs, and the cluster run of every operation.

use redissun::multimap::{ListMultimapValues, SetMultimapValues};
use redissun::JsonCodec;

/// The handle methods the shared tests call, under one name for the list and the set handle.
#[allow(async_fn_in_trait)]
pub trait Values {
    async fn add_one(&self, v: &str);
    async fn size(&self) -> usize;
    async fn empty(&self) -> bool;
    async fn has(&self, v: &str) -> bool;
    async fn remove_one(&self, v: &str) -> bool;
    async fn retain_of(&self, vs: &[&str]) -> bool;
    async fn contains_all_of(&self, vs: &[&str]) -> bool;
    async fn remove_all_of(&self, vs: &[&str]) -> bool;
}

impl Values for ListMultimapValues<String, JsonCodec> {
    async fn add_one(&self, v: &str) {
        self.push(v).await.unwrap()
    }
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
    async fn add_one(&self, v: &str) {
        self.insert(v).await.unwrap();
    }
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

/// The tests of `RedissonBaseMultimapCacheTest` for the multimap that `$factory(&Client, &str)` returns, with `String` keys and values and `JsonCodec`. `handle = #[attr]` and `scheduler = #[attr]` put an attribute (such as `#[ignore]`) on the tests of the value handle and on `redisson_scheduler`.
#[macro_export]
macro_rules! base_multimap_cache_tests {
    ($factory:path $(, handle = #[$handle:meta])? $(, scheduler = #[$scheduler:meta])?) => {
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
        $(#[$handle])?
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
        $(#[$scheduler])?
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
        $(#[$handle])?
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

/// `every_operation_runs_on_a_cluster` (Review Focus 1) for the multimap that `$factory(&Client, &str)` returns: every method, on a plain and a hash-tagged name, on a 3-master Redis 7.4 cluster. With `expire_key`, the native `expire_key` runs too.
#[macro_export]
macro_rules! every_operation_runs_on_a_cluster {
    ($factory:path $(, $expire_key:ident)?) => {
        #[tokio::test]
        async fn every_operation_runs_on_a_cluster() {
            use $crate::common::multimap_cache::Values as _;
            use ::futures::TryStreamExt as _;
            use redissun::Object as _;

            let topology = $crate::common::topology::Topology::cluster().await;
            let client = redissun::Client::builder()
                .url(topology.cluster_url())
                .build()
                .await
                .expect("could not connect to the cluster");
            let u = $crate::common::unique("mm-cluster");
            // A rename stays in the slot of the name: `u` and `{u}2` share the slot of `u`.
            for (name, renamed) in [
                (u.clone(), format!("{{{u}}}2")),
                (format!("{{t}}:{u}"), format!("{{t}}:{u}2")),
            ] {
                let map = $factory(&client, &name);
                assert!(map.insert("a", "1").await.unwrap(), "{name}");
                assert!(map.extend("a", ["2", "3"]).await.unwrap());
                assert!(map.extend("b", ["1", "4"]).await.unwrap());
                assert_eq!(map.get_all("a").await.unwrap().len(), 3);
                assert!(map.contains_key("a").await.unwrap());
                assert!(map.contains_value("4").await.unwrap());
                assert!(map.contains_entry("b", "4").await.unwrap());
                assert_eq!(map.len().await.unwrap(), 5);
                assert_eq!(map.key_len().await.unwrap(), 2);
                assert!(!map.is_empty().await.unwrap());
                assert_eq!(map.keys_to_vec().await.unwrap().len(), 2);
                assert!(map.remove("a", "3").await.unwrap());
                assert_eq!(map.replace_values("a", ["5", "6"]).await.unwrap().len(), 2);
                map.set_values("c", ["7"]).await.unwrap();
                assert_eq!(map.remove_many(&["c", "x"]).await.unwrap(), 1);
                map.insert("d", "7").await.unwrap();
                assert_eq!(map.remove_from_all_keys(&["7"]).await.unwrap(), 1);
                map.remove_all("d").await.unwrap();
                let values = map.get("a").unwrap();
                values.add_one("8").await;
                assert_eq!(values.size().await, 3);
                assert!(values.has("8").await);
                assert!(values.contains_all_of(&["5", "8"]).await);
                assert!(values.remove_one("8").await);
                assert!(values.remove_all_of(&["6"]).await);
                $(
                    assert!(map.$expire_key("a", ::std::time::Duration::from_secs(60)).await.unwrap());
                    assert!(map.$expire_key("a", ::std::time::Duration::ZERO).await.unwrap());
                )?
                let pairs: Vec<(String, String)> = map.iter_paged(2).try_collect().await.unwrap();
                assert_eq!(pairs.len(), 3, "{name}");
                let keys: Vec<String> = map.keys_paged(2).try_collect().await.unwrap();
                assert_eq!(keys.len(), 2);
                let all: Vec<String> = map.values_paged(2).try_collect().await.unwrap();
                assert_eq!(all.len(), 3);
                let guard = map.lock("a").unwrap().lock().await.unwrap();
                guard.unlock().await.unwrap();
                assert!(map.expire(::std::time::Duration::from_secs(60)).await.unwrap());
                assert!(map.persist().await.unwrap());
                map.rename(&renamed).await.unwrap();
                let map = $factory(&client, &renamed);
                assert_eq!(map.len().await.unwrap(), 3, "{renamed}");
                assert_eq!(map.remove_all("b").await.unwrap().len(), 2);
                assert!(map.del().await.unwrap());
                map.insert("z", "1").await.unwrap();
                map.clear().await.unwrap();
                assert_eq!(map.key_len().await.unwrap(), 0);
            }
        }
    };
}
