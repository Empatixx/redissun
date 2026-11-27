//! `ListMultimapCache` and `SetMultimapCache`: Redisson's `RedissonListMultimapCacheTest`, `RedissonSetMultimapCacheTest` and `RedissonBaseMultimapCacheTest`, plus the key TTLs in Redisson's ttl zset.

mod common;

use common::{client, connect_with, raw_command, unique};
use futures::TryStreamExt;
use redissun::{Client, JsonCodec, ListMultimapCache, SetMultimapCache};
use std::time::Duration;
use tokio::time::sleep;

fn list_cache(client: &Client, name: &str) -> ListMultimapCache<String, String, JsonCodec> {
    client.list_multimap_cache(name.to_string())
}

fn set_cache(client: &Client, name: &str) -> SetMultimapCache<String, String, JsonCodec> {
    client.set_multimap_cache(name.to_string())
}

/// Redisson's `suffixName(name, suffix)`.
fn suffix_name(name: &str, suffix: &str) -> String {
    if name.contains('{') {
        format!("{name}:{suffix}")
    } else {
        format!("{{{name}}}:{suffix}")
    }
}

/// The score of `member` in the zset `key`, or `None` when it has none.
async fn zscore(key: &str, member: &str) -> Option<i64> {
    let reply = raw_command(&["ZSCORE", key, member]).await;
    if reply.starts_with("$-1") || reply.starts_with('_') {
        return None;
    }
    reply
        .lines()
        .nth(1)
        .map(|score| score.trim().parse::<f64>().unwrap() as i64)
}

async fn exists(key: &str) -> bool {
    raw_command(&["EXISTS", key]).await == ":1\r\n"
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// A client whose clean-up task does not run during a test.
async fn client_without_clean_up() -> Client {
    connect_with(|builder| builder.eviction_interval(Duration::from_secs(3600))).await
}

/// The tests of the key TTLs, for the multimap that `$factory(&Client, &str)` returns and its ttl zset suffix `$ttl`.
macro_rules! cache_tests {
    ($factory:path, $ttl:literal) => {
        use super::*;

        fn ttl_set(name: &str) -> String {
            suffix_name(name, $ttl)
        }

        /// The cache parts of `every_operation_runs_on_a_cluster`: key TTLs, a cache handle read, `rename` within the tag with the ttl zset (D4), and one clean-up run on a tagged name (D1).
        async fn cluster_ttls_and_clean_up(client: &Client, topology: &common::topology::Topology) {
            let name = format!("{{t}}:{}", unique("mmc-cluster"));
            let renamed = format!("{name}2");
            let map = $factory(client, &name);
            map.extend("k", ["a", "b"]).await.unwrap();
            map.insert("stay", "c").await.unwrap();
            assert!(map.expire_key("k", Duration::from_secs(60)).await.unwrap());
            assert_eq!(map.get("k").unwrap().len().await.unwrap(), 2);
            assert!(map.contains_key("k").await.unwrap());
            map.rename(&renamed).await.unwrap();
            let map = $factory(client, &renamed);
            let port = topology.cluster_master_of(&renamed).await;
            let score = topology
                .cli(port, &["ZSCORE", &ttl_set(&renamed), "\"k\""])
                .await;
            assert!(score.trim().parse::<f64>().is_ok(), "{score:?}");
            assert_eq!(
                topology
                    .cli(port, &["EXISTS", &ttl_set(&name)])
                    .await
                    .trim(),
                "0"
            );
            let values = map.get("k").unwrap().name().to_string();
            assert!(map
                .expire_key("k", Duration::from_millis(200))
                .await
                .unwrap());
            // `redisson_scheduler`'s condition: the clean-up runs within 13 s.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(13);
            while map.len().await.unwrap() != 1 {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "not cleaned after 13 s"
                );
                sleep(Duration::from_millis(200)).await;
            }
            assert_eq!(topology.cli(port, &["EXISTS", &values]).await.trim(), "0");
            assert_eq!(map.key_len().await.unwrap(), 1);
            assert_eq!(map.get_all("stay").await.unwrap(), ["c"]);
            assert!(map.del().await.unwrap());
        }

        #[tokio::test]
        async fn expire_key_records_a_deadline() {
            let name = unique("mmc");
            let map = $factory(&client().await, &name);
            map.insert("1", "only").await.unwrap();
            let expected = now_millis() + 2000;
            assert!(map.expire_key("1", Duration::from_secs(2)).await.unwrap());
            let score = zscore(&ttl_set(&name), "\"1\"").await.unwrap();
            assert!((score - expected).abs() <= 1000, "{score} vs {expected}");
            assert!(!map
                .expire_key("missing", Duration::from_secs(2))
                .await
                .unwrap());
        }

        #[tokio::test]
        async fn expired_keys_hide_from_key_reads_but_count_until_cleaned() {
            let map = $factory(&client_without_clean_up().await, &unique("mmc"));
            for v in ["a", "b", "c"] {
                map.insert("1", v).await.unwrap();
            }
            assert!(map
                .expire_key("1", Duration::from_millis(200))
                .await
                .unwrap());
            sleep(Duration::from_millis(400)).await;
            assert!(map.get_all("1").await.unwrap().is_empty());
            assert!(!map.contains_key("1").await.unwrap());
            assert!(!map.contains_entry("1", "a").await.unwrap());
            assert!(!map.contains_value("a").await.unwrap());
            assert_eq!(map.len().await.unwrap(), 3);
            assert_eq!(map.key_len().await.unwrap(), 1);
        }

        #[tokio::test]
        async fn a_reinserted_key_starts_without_the_old_ttl() {
            let name = unique("mmc");
            let map = $factory(&client_without_clean_up().await, &name);
            map.insert("1", "only").await.unwrap();
            assert!(map
                .expire_key("1", Duration::from_millis(200))
                .await
                .unwrap());
            assert!(map.remove("1", "only").await.unwrap());
            sleep(Duration::from_millis(400)).await;
            map.insert("1", "new").await.unwrap();
            assert_eq!(map.get_all("1").await.unwrap(), ["new"]);
            assert_eq!(zscore(&ttl_set(&name), "\"1\"").await, None);
        }

        #[tokio::test]
        async fn every_write_of_a_new_key_drops_a_stale_score() {
            let name = unique("mmc");
            let map = $factory(&client_without_clean_up().await, &name);
            for k in ["e", "r", "s"] {
                map.insert(k, "old").await.unwrap();
                assert!(map.expire_key(k, Duration::from_millis(200)).await.unwrap());
                assert_eq!(map.remove_many(&[k]).await.unwrap(), 1);
            }
            sleep(Duration::from_millis(400)).await;
            map.extend("e", ["new"]).await.unwrap();
            map.replace_values("r", ["new"]).await.unwrap();
            map.set_values("s", ["new"]).await.unwrap();
            for k in ["e", "r", "s"] {
                assert_eq!(map.get_all(k).await.unwrap(), ["new"], "{k}");
                assert_eq!(zscore(&ttl_set(&name), &format!("\"{k}\"")).await, None);
            }
        }

        #[tokio::test]
        async fn writes_to_a_live_key_keep_its_ttl() {
            let name = unique("mmc");
            let map = $factory(&client().await, &name);
            map.insert("1", "a").await.unwrap();
            assert!(map.expire_key("1", Duration::from_secs(60)).await.unwrap());
            map.insert("1", "b").await.unwrap();
            map.extend("1", ["c"]).await.unwrap();
            map.replace_values("1", ["d"]).await.unwrap();
            map.set_values("1", ["e"]).await.unwrap();
            assert!(zscore(&ttl_set(&name), "\"1\"").await.is_some());
        }

        #[tokio::test]
        async fn remove_all_drops_the_score() {
            let name = unique("mmc");
            let map = $factory(&client().await, &name);
            map.extend("1", ["a", "b"]).await.unwrap();
            assert!(map.expire_key("1", Duration::from_secs(60)).await.unwrap());
            let mut old = map.remove_all("1").await.unwrap();
            old.sort();
            assert_eq!(old, ["a", "b"]);
            assert_eq!(zscore(&ttl_set(&name), "\"1\"").await, None);
            assert_eq!(map.key_len().await.unwrap(), 0);
        }

        #[tokio::test]
        async fn zero_ttl_clears_a_key_ttl() {
            let name = unique("mmc");
            let map = $factory(&client().await, &name);
            map.insert("1", "a").await.unwrap();
            assert!(map.expire_key("1", Duration::from_secs(60)).await.unwrap());
            assert!(map.expire_key("1", Duration::ZERO).await.unwrap());
            assert_eq!(zscore(&ttl_set(&name), "\"1\"").await, None);
            assert!(!map.expire_key("missing", Duration::ZERO).await.unwrap());
        }

        #[tokio::test]
        async fn rename_keeps_key_ttls() {
            let name = unique("mmc");
            let renamed = format!("{name}2");
            let client = client().await;
            let map = $factory(&client, &name);
            map.extend("1", ["a", "b"]).await.unwrap();
            assert!(map.expire_key("1", Duration::from_secs(60)).await.unwrap());
            map.rename(&renamed).await.unwrap();
            assert!(zscore(&ttl_set(&renamed), "\"1\"").await.is_some());
            assert!(!exists(&ttl_set(&name)).await);
            let map = $factory(&client, &renamed);
            let mut all = map.get_all("1").await.unwrap();
            all.sort();
            assert_eq!(all, ["a", "b"]);
        }

        #[tokio::test]
        async fn rename_without_key_ttls_works() {
            let name = unique("mmc");
            let renamed = format!("{name}2");
            let client = client().await;
            let map = $factory(&client, &name);
            map.insert("1", "a").await.unwrap();
            map.rename(&renamed).await.unwrap();
            assert_eq!(
                $factory(&client, &renamed).get_all("1").await.unwrap(),
                ["a"]
            );
        }

        #[tokio::test]
        async fn del_removes_the_ttl_zset() {
            let name = unique("mmc");
            let map = $factory(&client().await, &name);
            map.extend("1", ["a", "b"]).await.unwrap();
            assert!(map.expire_key("1", Duration::from_secs(60)).await.unwrap());
            let values = map.get("1").unwrap().name().to_string();
            assert!(exists(&ttl_set(&name)).await);
            assert!(map.del().await.unwrap());
            assert!(!exists(&ttl_set(&name)).await);
            assert!(!exists(&values).await);
            assert!(!exists(&name).await);
        }

        #[tokio::test]
        async fn clean_up_deletes_values_of_a_tagged_name() {
            let name = format!("{{t}}:{}", unique("mmc"));
            let client =
                connect_with(|builder| builder.eviction_interval(Duration::from_millis(300))).await;
            let map = $factory(&client, &name);
            map.extend("1", ["a", "b"]).await.unwrap();
            map.insert("2", "stay").await.unwrap();
            let values = map.get("1").unwrap().name().to_string();
            assert!(values.starts_with(&format!("{name}:")), "{values}");
            assert!(map
                .expire_key("1", Duration::from_millis(200))
                .await
                .unwrap());
            let deadline = tokio::time::Instant::now() + Duration::from_secs(13);
            while exists(&values).await {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "not cleaned after 13 s"
                );
                sleep(Duration::from_millis(200)).await;
            }
            assert_eq!(raw_command(&["HEXISTS", &name, "\"1\""]).await, ":0\r\n");
            assert_eq!(raw_command(&["ZCARD", &ttl_set(&name)]).await, ":0\r\n");
            assert_eq!(map.get_all("2").await.unwrap(), ["stay"]);
            assert_eq!(map.key_len().await.unwrap(), 1);
        }

        #[tokio::test]
        async fn clean_up_runs_once_per_client() {
            let (_redis, url) = common::own_redis().await;
            let client = Client::builder()
                .url(url.clone())
                .eviction_interval(Duration::from_millis(500))
                .build()
                .await
                .unwrap();
            let name = unique("mmc");
            let first = $factory(&client, &name);
            let second = $factory(&client, &name);
            first.insert("1", "a").await.unwrap();
            second.insert("1", "b").await.unwrap();
            // Let the first run load the script, then count the runs of two more periods.
            sleep(Duration::from_millis(700)).await;
            common::raw_command_at(&url, &["CONFIG", "RESETSTAT"]).await;
            sleep(Duration::from_millis(1000)).await;
            let stats = common::raw_command_at(&url, &["INFO", "commandstats"]).await;
            let runs: u64 = stats
                .lines()
                .filter(|line| {
                    line.starts_with("cmdstat_evalsha:") || line.starts_with("cmdstat_eval:")
                })
                .filter_map(|line| {
                    line.split(['=', ','])
                        .nth(1)
                        .and_then(|n| n.parse::<u64>().ok())
                })
                .sum();
            assert!(
                (1..=3).contains(&runs),
                "{runs} script runs in 1 s: {stats}"
            );
        }

        #[tokio::test]
        async fn cache_handle_push_registers_the_key() {
            let map = $factory(&client().await, &unique("mmc"));
            map.get("new").unwrap().add_one("a").await;
            assert_eq!(map.key_len().await.unwrap(), 1);
            assert_eq!(map.keys_to_vec().await.unwrap(), ["new"]);
            assert_eq!(map.get_all("new").await.unwrap(), ["a"]);
        }

        #[tokio::test]
        async fn cache_handle_clear_removes_a_key_without_ttl() {
            let name = unique("mmc");
            let map = $factory(&client().await, &name);
            map.extend("1", ["a", "b"]).await.unwrap();
            map.get("1").unwrap().clear().await.unwrap();
            assert!(!map.contains_key("1").await.unwrap());
            assert_eq!(raw_command(&["HEXISTS", &name, "\"1\""]).await, ":0\r\n");
            assert_eq!(map.key_len().await.unwrap(), 0);
        }

        #[tokio::test]
        async fn cache_handle_clear_drops_the_score() {
            let name = unique("mmc");
            let map = $factory(&client().await, &name);
            map.insert("1", "a").await.unwrap();
            assert!(map.expire_key("1", Duration::from_secs(60)).await.unwrap());
            map.get("1").unwrap().clear().await.unwrap();
            assert_eq!(zscore(&ttl_set(&name), "\"1\"").await, None);
        }

        #[tokio::test]
        async fn cache_handle_writes_ignore_an_expired_key() {
            let map = $factory(&client_without_clean_up().await, &unique("mmc"));
            map.extend("1", ["a", "b"]).await.unwrap();
            assert!(map
                .expire_key("1", Duration::from_millis(200))
                .await
                .unwrap());
            sleep(Duration::from_millis(400)).await;
            let values = map.get("1").unwrap();
            assert!(!values.remove_one("a").await);
            assert!(!values.remove_all_of(&["a", "b"]).await);
            assert!(!values.retain_of(&["x"]).await);
            map.expire_key("1", Duration::ZERO).await.unwrap();
            assert_eq!(values.size().await, 2);
        }

        #[tokio::test]
        async fn cache_handle_empty_inputs() {
            let map = $factory(&client().await, &unique("mmc"));
            map.insert("1", "a").await.unwrap();
            let values = map.get("1").unwrap();
            assert!(values.contains_all_of(&[]).await);
            assert!(!values.remove_all_of(&[]).await);
            assert_eq!(values.size().await, 1);
        }

        #[tokio::test]
        async fn expire_and_persist_cover_the_ttl_zset() {
            let name = unique("mmc");
            let map = $factory(&client().await, &name);
            map.insert("1", "a").await.unwrap();
            assert!(map.expire_key("1", Duration::from_secs(60)).await.unwrap());
            assert!(map.expire(Duration::from_secs(60)).await.unwrap());
            let pttl = raw_command(&["PTTL", &ttl_set(&name)]).await;
            assert!(!pttl.starts_with(":-"), "{pttl}");
            assert!(map.persist().await.unwrap());
            assert_eq!(raw_command(&["PTTL", &ttl_set(&name)]).await, ":-1\r\n");
            assert!(zscore(&ttl_set(&name), "redissun__expiretag")
                .await
                .is_none());
        }
    };
}

mod list {
    base_multimap_cache_tests!(super::list_cache);
    cache_tests!(super::list_cache, "redissun_list_multimap_ttl");
    every_operation_runs_on_a_cluster!(super::list_cache, expire_key, cluster_ttls_and_clean_up);

    #[tokio::test]
    async fn cache_handle_reads_respect_the_ttl() {
        let map = list_cache(&client_without_clean_up().await, &unique("mmc"));
        map.extend("1", ["1", "2", "1"]).await.unwrap();
        let values = map.get("1").unwrap();
        assert_eq!(values.get(0).await.unwrap().as_deref(), Some("1"));
        assert_eq!(values.get(5).await.unwrap(), None);
        assert_eq!(values.range(..).await.unwrap(), ["1", "2", "1"]);
        assert_eq!(values.range(1..=2).await.unwrap(), ["2", "1"]);
        let (start, end) = (2, 1);
        assert!(values.range(start..end).await.unwrap().is_empty());
        assert_eq!(values.position("1").await.unwrap(), Some(0));
        assert_eq!(values.rposition("1").await.unwrap(), Some(2));
        assert_eq!(values.rposition("3").await.unwrap(), None);
        assert!(values.contains("2").await.unwrap());
        assert_eq!(values.to_vec().await.unwrap(), ["1", "2", "1"]);
        let all: Vec<String> = values.iter().try_collect().await.unwrap();
        assert_eq!(all, ["1", "2", "1"]);
        assert_eq!(values.len().await.unwrap(), 3);
        assert!(map
            .expire_key("1", Duration::from_millis(200))
            .await
            .unwrap());
        sleep(Duration::from_millis(400)).await;
        assert_eq!(values.get(0).await.unwrap(), None);
        assert!(values.range(..).await.unwrap().is_empty());
        assert_eq!(values.position("1").await.unwrap(), None);
        assert_eq!(values.rposition("1").await.unwrap(), None);
        assert!(!values.contains("2").await.unwrap());
        assert!(values.to_vec().await.unwrap().is_empty());
        let all: Vec<String> = values.iter().try_collect().await.unwrap();
        assert!(all.is_empty());
        assert_eq!(values.len().await.unwrap(), 0);
        assert!(values.is_empty().await.unwrap());
        assert_eq!(values.remove_value_all("1").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn cache_handle_iter_reads_long_lists_in_pages() {
        let map = list_cache(&client().await, &unique("mmc"));
        let input: Vec<String> = (0..250).map(|i| i.to_string()).collect();
        map.extend("1", input.iter().map(String::as_str))
            .await
            .unwrap();
        assert!(map.expire_key("1", Duration::from_secs(60)).await.unwrap());
        let all: Vec<String> = map.get("1").unwrap().iter().try_collect().await.unwrap();
        assert_eq!(all, input);
    }

    #[tokio::test]
    async fn cache_handle_remove_value_n_counts() {
        let map = list_cache(&client().await, &unique("mmc"));
        map.extend("1", ["x", "x", "x"]).await.unwrap();
        let values = map.get("1").unwrap();
        assert_eq!(values.remove_value_n("x", 0).await.unwrap(), 0);
        assert_eq!(values.remove_value_n("x", 2).await.unwrap(), 2);
        assert_eq!(values.remove_value_all("x").await.unwrap(), 1);
        assert_eq!(values.remove_value_all("x").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn cache_handle_remove_values_with_duplicates_reports_true() {
        let map = list_cache(&client().await, &unique("mmc"));
        map.extend("1", ["x", "x", "y"]).await.unwrap();
        let values = map.get("1").unwrap();
        assert!(values.remove_values(["x"]).await.unwrap());
        assert_eq!(values.to_vec().await.unwrap(), ["y"]);
    }

    // Redisson: RedissonListMultimapCacheTest.testValues
    #[tokio::test]
    async fn redisson_values() {
        let multimap = list_cache(&client().await, &unique("test"));
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
    base_multimap_cache_tests!(super::set_cache);
    cache_tests!(super::set_cache, "redissun_set_multimap_ttl");
    every_operation_runs_on_a_cluster!(super::set_cache, expire_key, cluster_ttls_and_clean_up);

    #[tokio::test]
    async fn cache_handle_reads_respect_the_ttl() {
        let map = set_cache(&client_without_clean_up().await, &unique("mmc"));
        map.extend("1", ["1", "2", "3"]).await.unwrap();
        let values = map.get("1").unwrap();
        assert!(values.contains("2").await.unwrap());
        assert!(values.contains_all(&["1", "3"]).await.unwrap());
        let mut all = values.to_vec().await.unwrap();
        all.sort();
        assert_eq!(all, ["1", "2", "3"]);
        assert_eq!(values.len().await.unwrap(), 3);
        assert!(map
            .expire_key("1", Duration::from_millis(200))
            .await
            .unwrap());
        sleep(Duration::from_millis(400)).await;
        assert!(!values.contains("2").await.unwrap());
        assert!(!values.contains_all(&["1"]).await.unwrap());
        assert!(values.to_vec().await.unwrap().is_empty());
        let all: Vec<String> = values.iter().try_collect().await.unwrap();
        assert!(all.is_empty());
        assert_eq!(values.len().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn cache_handle_iter_reads_big_sets_with_sscan() {
        let map = set_cache(&client().await, &unique("mmc"));
        let input: Vec<String> = (0..250).map(|i| i.to_string()).collect();
        map.extend("1", input.iter().map(String::as_str))
            .await
            .unwrap();
        assert!(map.expire_key("1", Duration::from_secs(60)).await.unwrap());
        let mut all: Vec<String> = map.get("1").unwrap().iter().try_collect().await.unwrap();
        all.sort();
        all.dedup();
        let mut expected = input.clone();
        expected.sort();
        assert_eq!(all, expected);
    }

    // Redisson: RedissonSetMultimapCacheTest.testIteratorAsync
    #[tokio::test]
    async fn redisson_iterator_async() {
        let multimap = set_cache(&client().await, &unique("test"));
        for v in ["1", "2", "3", "3"] {
            multimap.insert("1", v).await.unwrap();
        }
        let values: std::collections::HashSet<String> = multimap
            .get("1")
            .unwrap()
            .iter()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(values.len(), 3);
    }

    // Redisson: RedissonSetMultimapCacheTest.testValues
    #[tokio::test]
    async fn redisson_values() {
        let multimap = set_cache(&client().await, &unique("test"));
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
