// Ported from Redisson's RedissonKeysTest. Tests that need an empty database get one of their own: no other test in this binary uses databases 1 to 15.
use crate::common::{client, connect_with, own_redis, raw_command, unique};
use futures::{StreamExt, TryStreamExt};
use redissun::{Client, KeyEvent, KeyType, KeysScan, Migrate, Object};
use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

async fn empty_db(database: u8) -> Client {
    let client = connect_with(|builder| builder.database(database)).await;
    client.keys().flush_db().await.unwrap();
    client
}

async fn all_keys(client: &Client, scan: KeysScan) -> HashSet<String> {
    client.keys().iter_with(scan).try_collect().await.unwrap()
}

#[tokio::test]
async fn time_is_the_server_clock() {
    let time = client().await.keys().time().await.unwrap();
    let since = time.duration_since(UNIX_EPOCH).unwrap();
    assert!(since > Duration::from_secs(1_700_000_000));
    let skew = SystemTime::now()
        .duration_since(time)
        .unwrap_or_else(|e| e.duration());
    assert!(skew < Duration::from_secs(60));
}

async fn wait_for(events: &mut redissun::KeyEvents, key: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if events.recv().await.unwrap() == key {
                return;
            }
        }
    })
    .await
    .expect("no event for the key");
}

#[tokio::test]
async fn a_listener_hears_new_and_deleted_keys() {
    // Redisson's testNewObjectListener and testDeleteListener; the events of other tests are skipped by name.
    raw_command(&["CONFIG", "SET", "notify-keyspace-events", "AKEn"]).await;
    let client = client().await;
    let keys = client.keys();
    let mut created = keys.listen(KeyEvent::New).await.unwrap();
    let mut deleted = keys.listen(KeyEvent::Deleted).await.unwrap();
    let name = unique("listened");
    let bucket = client.bucket::<String>(&*name);
    bucket.set("123").await.unwrap();
    wait_for(&mut created, &name).await;
    bucket.del().await.unwrap();
    wait_for(&mut deleted, &name).await;
}

#[tokio::test]
async fn a_dropped_listener_stops_listening() {
    raw_command(&["CONFIG", "SET", "notify-keyspace-events", "AKEn"]).await;
    let client = client().await;
    let events = client.keys().listen(KeyEvent::Set).await.unwrap();
    let topic = client.pattern_topic::<String>("__keyevent@*:set");
    assert_eq!(topic.local_subscriber_count().await, 1);
    drop(events);
    tokio::time::timeout(Duration::from_secs(5), async {
        while topic.local_subscriber_count().await != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn keys_with_a_limit_give_at_most_that_many() {
    let client = empty_db(1).await;
    for i in 0..10 {
        client
            .bucket::<i32>(format!("test{i}"))
            .set(&i)
            .await
            .unwrap();
    }
    assert_eq!(all_keys(&client, KeysScan::new().limit(3)).await.len(), 3);
    assert_eq!(all_keys(&client, KeysScan::new().limit(20)).await.len(), 10);
}

#[tokio::test]
async fn keys_with_a_pattern_and_a_limit() {
    let client = empty_db(2).await;
    for i in 0..10 {
        client
            .bucket::<i32>(format!("test{i}"))
            .set(&i)
            .await
            .unwrap();
    }
    for i in 0..5 {
        client
            .bucket::<i32>(format!("red{i}"))
            .set(&i)
            .await
            .unwrap();
    }
    let count = |pattern: &'static str, limit| {
        let client = client.clone();
        async move {
            all_keys(&client, KeysScan::new().pattern(pattern).limit(limit))
                .await
                .len()
        }
    };
    assert_eq!(count("test*", 3).await, 3);
    assert_eq!(count("test*", 20).await, 10);
    assert_eq!(count("red*", 3).await, 3);
    assert_eq!(count("red*", 10).await, 5);
}

#[tokio::test]
async fn touch_counts_the_keys_that_exist() {
    let client = client().await;
    let (test, test2, test3, test10) = (
        unique("test"),
        unique("test2"),
        unique("test3"),
        unique("test10"),
    );
    client.hash_set::<String>(&*test).insert("1").await.unwrap();
    client
        .hash_set::<String>(&*test10)
        .insert("1")
        .await
        .unwrap();
    let keys = client.keys();
    assert_eq!(keys.touch(&[&test]).await.unwrap(), 1);
    assert_eq!(keys.touch(&[&test, &test2]).await.unwrap(), 1);
    assert_eq!(keys.touch(&[&test3, &test2]).await.unwrap(), 0);
    assert_eq!(keys.touch(&[&test3, &test10, &test]).await.unwrap(), 2);
    let none: [&str; 0] = [];
    assert_eq!(keys.touch(&none).await.unwrap(), 0);
}

#[tokio::test]
async fn count_exists_counts_the_keys_that_exist() {
    let client = client().await;
    let (test, test2, test3, test10) = (
        unique("test"),
        unique("test2"),
        unique("test3"),
        unique("test10"),
    );
    client.hash_set::<String>(&*test).insert("1").await.unwrap();
    client
        .hash_set::<String>(&*test10)
        .insert("1")
        .await
        .unwrap();
    let keys = client.keys();
    assert_eq!(keys.count_exists(&[&test]).await.unwrap(), 1);
    assert_eq!(keys.count_exists(&[&test, &test2]).await.unwrap(), 1);
    assert_eq!(keys.count_exists(&[&test3, &test2]).await.unwrap(), 0);
    assert_eq!(
        keys.count_exists(&[&test3, &test10, &test]).await.unwrap(),
        2
    );
}

#[tokio::test]
async fn key_type_names_the_type_and_none_for_a_missing_key() {
    let client = client().await;
    let name = unique("test");
    client.hash_set::<String>(&*name).insert("1").await.unwrap();
    assert_eq!(
        client.keys().key_type(&name).await.unwrap(),
        Some(KeyType::Set)
    );
    assert_eq!(
        client.keys().key_type(&unique("test1")).await.unwrap(),
        None
    );
}

#[tokio::test]
async fn keys_of_a_pattern_nothing_matches_are_empty() {
    let client = client().await;
    let pattern = format!("{}*", unique("empty"));
    let first = client
        .keys()
        .iter_with(KeysScan::new().pattern(pattern).chunk_size(10))
        .next()
        .await;
    assert!(first.is_none());
}

#[tokio::test]
async fn keys_of_a_type() {
    let client = empty_db(3).await;
    client
        .bucket::<String>("test1")
        .set("someValue")
        .await
        .unwrap();
    client
        .bucket::<String>("test2")
        .set("someValue")
        .await
        .unwrap();
    client
        .stream::<String, String>("test12")
        .add([("1", "2")])
        .await
        .unwrap();
    let strings = all_keys(&client, KeysScan::new().key_type(KeyType::String)).await;
    assert_eq!(strings, HashSet::from(["test1".into(), "test2".into()]));
    let streams = all_keys(&client, KeysScan::new().key_type(KeyType::Stream)).await;
    assert_eq!(streams, HashSet::from(["test12".into()]));
}

#[tokio::test]
async fn keys_of_a_pattern_with_one_wildcard_character() {
    let client = empty_db(4).await;
    for name in ["test1", "test2", "test12"] {
        client
            .bucket::<String>(name)
            .set("someValue")
            .await
            .unwrap();
    }
    let found = all_keys(&client, KeysScan::new().pattern("test?")).await;
    assert_eq!(found, HashSet::from(["test1".into(), "test2".into()]));
}

#[tokio::test]
async fn iterating_gives_every_key_once_deleted_keys_are_gone() {
    let client = empty_db(5).await;
    let mut names = HashSet::new();
    for _ in 0..115 {
        let name = unique("key");
        client
            .bucket::<String>(&*name)
            .set("someValue")
            .await
            .unwrap();
        names.insert(name);
    }
    let found: HashSet<String> = client.keys().iter().try_collect().await.unwrap();
    assert_eq!(found, names);
    assert_eq!(client.keys().count().await.unwrap(), 115);
    let found: Vec<String> = found.into_iter().collect();
    assert_eq!(client.keys().delete(&found).await.unwrap(), 115);
    assert!(client.keys().iter().next().await.is_none());
}

#[tokio::test]
async fn random_key_is_one_of_the_keys_or_none_after_a_flush() {
    let client = empty_db(6).await;
    client
        .bucket::<String>("test1")
        .set("someValue1")
        .await
        .unwrap();
    client
        .bucket::<String>("test2")
        .set("someValue2")
        .await
        .unwrap();
    let keys = client.keys();
    let random = keys.random_key().await.unwrap().unwrap();
    assert!(random == "test1" || random == "test2");
    keys.delete(&["test1"]).await.unwrap();
    assert_eq!(keys.random_key().await.unwrap().as_deref(), Some("test2"));
    keys.flush_db().await.unwrap();
    assert_eq!(keys.random_key().await.unwrap(), None);
}

async fn fill_for_pattern(client: &Client) {
    client
        .bucket::<String>("test0")
        .set("someValue3")
        .await
        .unwrap();
    client
        .bucket::<String>("test9")
        .set("someValue4")
        .await
        .unwrap();
    client
        .hash_map::<String, String>("test2")
        .insert("1", "2")
        .await
        .unwrap();
    client
        .hash_map::<String, String>("test3")
        .insert("1", "5")
        .await
        .unwrap();
}

#[tokio::test]
async fn delete_by_pattern_deletes_the_matching_keys() {
    let client = empty_db(7).await;
    fill_for_pattern(&client).await;
    assert_eq!(client.keys().delete_by_pattern("test?").await.unwrap(), 4);
    assert_eq!(client.keys().delete_by_pattern("test?").await.unwrap(), 0);
    assert_eq!(client.keys().count().await.unwrap(), 0);
}

#[tokio::test]
async fn unlink_by_pattern_unlinks_the_matching_keys() {
    let client = empty_db(8).await;
    fill_for_pattern(&client).await;
    assert_eq!(client.keys().unlink_by_pattern("test?").await.unwrap(), 4);
    assert_eq!(client.keys().unlink_by_pattern("test?").await.unwrap(), 0);
    assert_eq!(client.keys().count().await.unwrap(), 0);
}

#[tokio::test]
async fn delete_by_pattern_reads_more_than_one_scan_page() {
    let client = empty_db(14).await;
    let names: Vec<String> = (0..1200).map(|i| format!("many{i}")).collect();
    let buckets = client.buckets::<i32>();
    buckets
        .set(names.iter().map(|name| (name.as_str(), &1)))
        .await
        .unwrap();
    client.bucket::<i32>("other").set(&1).await.unwrap();
    assert_eq!(
        client.keys().delete_by_pattern("many*").await.unwrap(),
        1200
    );
    assert_eq!(client.keys().count().await.unwrap(), 1);
}

#[tokio::test]
async fn delete_and_unlink_count_the_keys_that_existed() {
    let client = client().await;
    let names: Vec<String> = (0..7).map(|_| unique("test")).collect();
    for name in &names[..6] {
        client
            .bucket::<String>(&**name)
            .set("someValue")
            .await
            .unwrap();
    }
    client
        .hash_map::<String, String>(&*names[6])
        .insert("1", "2")
        .await
        .unwrap();
    assert_eq!(client.keys().delete(&names).await.unwrap(), 7);
    assert_eq!(client.keys().delete(&names).await.unwrap(), 0);
    client.bucket::<String>(&*names[0]).set("v").await.unwrap();
    assert_eq!(client.keys().unlink(&names).await.unwrap(), 1);
}

#[tokio::test]
async fn count_is_the_number_of_keys() {
    let client = empty_db(9).await;
    assert_eq!(client.keys().count().await.unwrap(), 0);
    client.bucket::<i32>("test1").set(&23).await.unwrap();
    assert_eq!(client.keys().count().await.unwrap(), 1);
}

#[tokio::test]
async fn flush_db_parallel_empties_the_database() {
    let client = empty_db(10).await;
    client.bucket::<i32>("test1").set(&23).await.unwrap();
    client.keys().flush_db_parallel().await.unwrap();
    assert_eq!(client.keys().count().await.unwrap(), 0);
}

#[tokio::test]
async fn flush_all_empties_every_database() {
    let (_container, url) = own_redis().await;
    let connect = |database| {
        let url = url.clone();
        async move {
            Client::builder()
                .url(url)
                .database(database)
                .build()
                .await
                .unwrap()
        }
    };
    let (zero, one) = (connect(0).await, connect(1).await);
    zero.bucket::<i32>("a").set(&1).await.unwrap();
    one.bucket::<i32>("b").set(&1).await.unwrap();
    zero.keys().flush_all().await.unwrap();
    assert_eq!(one.keys().count().await.unwrap(), 0);
    zero.bucket::<i32>("a").set(&1).await.unwrap();
    one.keys().flush_all_parallel().await.unwrap();
    assert_eq!(zero.keys().count().await.unwrap(), 0);
}

#[tokio::test]
async fn expire_many_and_expire_at_many_count_the_keys_that_exist() {
    let client = client().await;
    let (first, second) = (unique("expire-test1"), unique("expire-test2"));
    for name in [&first, &second] {
        client
            .bucket::<i32>(&**name)
            .set(&23)
            .ttl(Duration::from_secs(3600))
            .await
            .unwrap();
    }
    let keys = client.keys();
    let day = Duration::from_secs(86_400);
    assert_eq!(keys.expire_many(&[&first, &second], day).await.unwrap(), 2);
    for name in [&first, &second] {
        assert!(keys.ttl(name).await.unwrap().unwrap() > Duration::from_secs(23 * 3600));
    }
    let at = SystemTime::now() + 2 * day;
    assert_eq!(
        keys.expire_at_many(&[&first, &second], at).await.unwrap(),
        2
    );
    for name in [&first, &second] {
        assert!(keys.ttl(name).await.unwrap().unwrap() > day);
    }
    let missing = unique("expire-miss");
    assert_eq!(keys.expire_many(&[&missing], day).await.unwrap(), 0);
    assert_eq!(keys.expire_at_many(&[&missing], at).await.unwrap(), 0);
}

#[tokio::test]
async fn one_key_expires_persists_and_is_renamed_by_name() {
    let client = client().await;
    let keys = client.keys();
    let (name, other, taken) = (unique("k"), unique("k"), unique("k"));
    client.bucket::<i32>(&*name).set(&1).await.unwrap();
    client.bucket::<i32>(&*taken).set(&2).await.unwrap();
    assert_eq!(keys.ttl(&name).await.unwrap(), None);
    assert!(keys.expire(&name, Duration::from_secs(60)).await.unwrap());
    assert!(keys.ttl(&name).await.unwrap().unwrap() > Duration::from_secs(50));
    assert!(keys.persist(&name).await.unwrap());
    assert_eq!(keys.ttl(&name).await.unwrap(), None);
    assert!(keys
        .expire_at(&name, SystemTime::now() + Duration::from_secs(60))
        .await
        .unwrap());
    assert!(!keys.try_rename(&name, &taken).await.unwrap());
    keys.rename(&name, &other).await.unwrap();
    assert_eq!(client.bucket::<i32>(&*other).get().await.unwrap(), Some(1));
    assert!(keys.try_rename(&other, &name).await.unwrap());
    assert!(!keys
        .expire(&unique("missing"), Duration::from_secs(1))
        .await
        .unwrap());
}

#[tokio::test]
async fn move_to_db_and_swap_db_move_keys_between_databases() {
    let eleven = empty_db(11).await;
    let twelve = empty_db(12).await;
    eleven.bucket::<i32>("moved").set(&1).await.unwrap();
    assert!(eleven.keys().move_to_db("moved", 12).await.unwrap());
    assert!(!eleven.keys().move_to_db("moved", 12).await.unwrap());
    assert_eq!(twelve.bucket::<i32>("moved").get().await.unwrap(), Some(1));
    eleven.keys().swap_db(11, 12).await.unwrap();
    assert_eq!(eleven.bucket::<i32>("moved").get().await.unwrap(), Some(1));
    assert_eq!(twelve.keys().count().await.unwrap(), 0);
}

#[tokio::test]
async fn migrate_copies_keys_to_another_server() {
    // As Redisson's testMigrate, to a second server, which the first reaches by its address on the Docker bridge. A server cannot migrate to itself: it would wait for its own reply.
    // The source is a server of its own too: MIGRATE blocks the source until the target answers, which would stall the other tests.
    let (_source_container, source_url) = own_redis().await;
    let source = Client::builder().url(source_url).build().await.unwrap();
    let (container, url) = own_redis().await;
    let host = container.get_bridge_ip_address().await.unwrap().to_string();
    let target = Client::builder().url(url).build().await.unwrap();
    let tag = unique("testMigrate");
    let keys = [format!("{{{tag}}}key1"), format!("{{{tag}}}key2")];
    for key in &keys {
        source.bucket::<String>(&**key).set(key).await.unwrap();
    }
    source
        .keys()
        .migrate(
            Migrate::new(keys.clone(), host, 6379, 0, Duration::from_secs(5))
                .copy()
                .replace(),
        )
        .await
        .unwrap();
    for key in &keys {
        assert_eq!(
            target
                .bucket::<String>(&**key)
                .get()
                .await
                .unwrap()
                .as_deref(),
            Some(key.as_str())
        );
        assert!(source.bucket::<String>(&**key).exists().await.unwrap());
    }
}

#[tokio::test]
async fn slot_is_the_cluster_hash_slot() {
    let client = client().await;
    assert_eq!(client.keys().slot("foo"), 12182);
    assert_eq!(client.keys().slot("{foo}bar"), 12182);
}
