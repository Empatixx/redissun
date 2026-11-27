mod common;

use common::store::MemoryStore;
use common::{client, raw_command, unique};
use redissun::{Client, HashMapCache, JsonCodec, LocalCachedMapCache, Object, SyncStrategy};
use std::time::Duration;
use tokio::time::{sleep, Instant};

type Cache = LocalCachedMapCache<String, String, JsonCodec>;
type Plain = HashMapCache<String, String, JsonCodec>;

static GATE: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());

async fn shared() -> tokio::sync::RwLockReadGuard<'static, ()> {
    GATE.read().await
}

#[allow(dead_code)]
async fn exclusive() -> tokio::sync::RwLockWriteGuard<'static, ()> {
    GATE.write().await
}

async fn lcmc(client: &Client, name: &str) -> Cache {
    client
        .local_cached_map_cache::<String, String>(name.to_string())
        .build()
        .await
        .unwrap()
}

async fn updating(client: &Client, name: &str) -> Cache {
    client
        .local_cached_map_cache::<String, String>(name.to_string())
        .sync_strategy(SyncStrategy::Update)
        .build()
        .await
        .unwrap()
}

fn plain(client: &Client, name: &str) -> Plain {
    client.hash_map_cache::<String, String>(name.to_string())
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

/// The text of a bulk string reply, `None` for nil.
fn bulk(reply: &str) -> Option<String> {
    let mut lines = reply.split("\r\n");
    let header = lines.next()?;
    if header == "$-1" {
        return None;
    }
    lines.next().map(str::to_string)
}

const MS: Duration = Duration::from_millis(1);

#[tokio::test]
async fn shares_data_with_hash_map_cache() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcmc");
    let cache = lcmc(&client, &name).await;
    let plain = plain(&client, &name);
    cache
        .insert("a", "1")
        .ttl(Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(plain.get("a").await.unwrap().as_deref(), Some("1"));
    let ttl = plain.entry_ttl("a").await.unwrap().unwrap();
    assert!(ttl <= Duration::from_secs(60));
    plain.insert("b", "2").await.unwrap();
    assert_eq!(cache.get("b").await.unwrap().as_deref(), Some("2"));
}

#[tokio::test]
async fn writes_the_layout_of_hash_map_cache() {
    let _gate = shared().await;
    let client = client().await;
    let (n1, n2) = (unique("lcmc"), unique("lcmc"));
    let cache = lcmc(&client, &n1).await;
    cache
        .insert("a", "1")
        .ttl(Duration::from_secs(60))
        .await
        .unwrap();
    plain(&client, &n2)
        .insert("a", "1")
        .ttl(Duration::from_secs(60))
        .await
        .unwrap();
    let field = "\"a\"";
    assert_eq!(
        raw_command(&["HGET", &n1, field]).await,
        raw_command(&["HGET", &n2, field]).await
    );
    let score = |name: String| async move {
        let reply = raw_command(&[
            "ZSCORE",
            &format!("redissun__timeout__set:{{{name}}}"),
            field,
        ])
        .await;
        bulk(&reply).unwrap().parse::<f64>().unwrap()
    };
    let (s1, s2) = (score(n1).await, score(n2).await);
    assert!((s1 - s2).abs() <= 1000.0, "{s1} vs {s2}");
}

#[tokio::test]
async fn a_read_is_cached_locally() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcmc");
    let cache = lcmc(&client, &name).await;
    let plain = plain(&client, &name);
    plain.insert("a", "1").await.unwrap();
    assert_eq!(cache.get("a").await.unwrap().as_deref(), Some("1"));
    assert_eq!(cache.local_len(), 1);
    plain.insert("a", "2").await.unwrap();
    assert_eq!(cache.get("a").await.unwrap().as_deref(), Some("1"));
}

#[tokio::test]
async fn a_write_on_another_instance_invalidates_my_copy() {
    let _gate = shared().await;
    let name = unique("lcmc");
    let (first, second) = (client().await, client().await);
    let a = lcmc(&first, &name).await;
    let b = lcmc(&second, &name).await;
    plain(&first, &name).insert("a", "1").await.unwrap();
    assert_eq!(a.get("a").await.unwrap().as_deref(), Some("1"));
    b.insert("a", "2").await.unwrap();
    eventually("the invalidation never arrived", || async {
        a.get("a").await.unwrap().as_deref() == Some("2")
    })
    .await;
}

#[tokio::test]
async fn the_update_strategy_sends_value_and_deadline() {
    let _gate = shared().await;
    let name = unique("lcmc");
    let (first, second) = (client().await, client().await);
    let a = updating(&first, &name).await;
    let b = updating(&second, &name).await;
    b.insert("k", "v").ttl(1000 * MS).await.unwrap();
    eventually("the update never arrived", || async {
        a.local_entries().unwrap() == vec![("k".to_string(), "v".to_string())]
    })
    .await;
    sleep(1100 * MS).await;
    assert_eq!(a.local_len(), 0);
    assert_eq!(a.get("k").await.unwrap(), None);
    assert_eq!(plain(&first, &name).len().await.unwrap(), 1);
}

#[tokio::test]
async fn a_local_copy_expires_with_the_entry_ttl() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcmc");
    let a = lcmc(&client, &name).await;
    plain(&client, &name)
        .insert("k", "v")
        .ttl(300 * MS)
        .await
        .unwrap();
    assert_eq!(a.get("k").await.unwrap().as_deref(), Some("v"));
    assert_eq!(a.local_len(), 1);
    sleep(350 * MS).await;
    assert!(a.local_entries().unwrap().is_empty());
    assert_eq!(a.get("k").await.unwrap(), None);
}

#[tokio::test]
async fn a_copy_of_an_idle_entry_lives_half_the_max_idle() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcmc");
    let a = lcmc(&client, &name).await;
    let plain = plain(&client, &name);
    plain.insert("k", "v").max_idle(400 * MS).await.unwrap();
    assert_eq!(a.get("k").await.unwrap().as_deref(), Some("v"));
    sleep(250 * MS).await;
    plain.replace("k", "new").await.unwrap();
    assert_eq!(a.get("k").await.unwrap().as_deref(), Some("new"));
}

#[tokio::test]
async fn reading_locally_keeps_an_idle_entry_alive() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcmc");
    let a = lcmc(&client, &name).await;
    let plain = plain(&client, &name);
    plain.insert("k", "v").max_idle(400 * MS).await.unwrap();
    assert_eq!(a.get("k").await.unwrap().as_deref(), Some("v"));
    for _ in 0..6 {
        sleep(150 * MS).await;
        assert_eq!(a.get("k").await.unwrap().as_deref(), Some("v"));
    }
    // `peek` ignores the idle limit; `get` shows the entry is still alive on the server.
    assert_eq!(plain.get("k").await.unwrap().as_deref(), Some("v"));
}

#[tokio::test]
async fn remove_and_clear_invalidate_everywhere() {
    let _gate = shared().await;
    let name = unique("lcmc");
    let (first, second) = (client().await, client().await);
    let a = lcmc(&first, &name).await;
    let b = lcmc(&second, &name).await;
    let plain = plain(&first, &name);
    plain.insert("a", "1").await.unwrap();
    plain.insert("b", "2").await.unwrap();
    assert_eq!(b.get("a").await.unwrap().as_deref(), Some("1"));
    assert_eq!(a.remove("a").await.unwrap().as_deref(), Some("1"));
    eventually("the removal never arrived", || async {
        b.get("a").await.unwrap().is_none()
    })
    .await;
    assert_eq!(b.get("b").await.unwrap().as_deref(), Some("2"));
    a.clear().await.unwrap();
    eventually("the clear never arrived", || async { b.local_len() == 0 }).await;
    assert!(plain.is_empty().await.unwrap());
}

#[tokio::test]
async fn removing_a_missing_key_or_clearing_an_empty_cache_sends_nothing() {
    let _gate = shared().await;
    let name = unique("lcmc");
    let (first, second) = (client().await, client().await);
    let reader = lcmc(&first, &name).await;
    let writer = lcmc(&second, &name).await;
    let plain = plain(&first, &name);
    plain.insert("k", "v").await.unwrap();
    reader.get("k").await.unwrap();
    // HDEL publishes no entry event, so only a message from `writer` could drop the reader's copy.
    raw_command(&["HDEL", &name, "\"k\""]).await;
    assert_eq!(writer.remove("k").await.unwrap(), None);
    writer.clear_local();
    writer.clear().await.unwrap();
    sleep(300 * MS).await;
    assert_eq!(reader.local_len(), 1);
}

#[tokio::test]
async fn a_failed_write_leaves_no_local_copy() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcmc");
    let a = lcmc(&client, &name).await;
    a.insert("k", "1").await.unwrap();
    assert_eq!(a.local_len(), 1);
    raw_command(&["DEL", &name]).await;
    raw_command(&["SET", &name, "x"]).await;
    assert!(a.insert("k", "2").await.is_err());
    assert_eq!(a.local_len(), 0);
    raw_command(&["DEL", &name]).await;
}

#[tokio::test]
async fn build_marks_the_cache_for_entry_events() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcmc");
    let _cache = lcmc(&client, &name).await;
    let reply = raw_command(&[
        "HGET",
        &format!("{{{name}}}:redissun_options"),
        "has-listeners",
    ])
    .await;
    assert_eq!(bulk(&reply).as_deref(), Some("1"));
}

async fn invalidating(client: &Client, name: &str) -> Cache {
    lcmc(client, name).await
}

fn has(entries: &[(String, String)], k: &str, v: &str) -> bool {
    entries.iter().any(|(ek, ev)| ek == k && ev == v)
}

#[tokio::test]
async fn try_insert_announces_only_when_it_stored() {
    let _gate = shared().await;
    let name = unique("lcmc");
    let (first, second) = (client().await, client().await);
    let a = updating(&first, &name).await;
    let b = updating(&second, &name).await;
    plain(&first, &name).insert("k", "v1").await.unwrap();
    assert_eq!(b.get("k").await.unwrap().as_deref(), Some("v1"));
    assert_eq!(a.try_insert("k", "x").await.unwrap().as_deref(), Some("v1"));
    sleep(200 * MS).await;
    assert!(has(&b.local_entries().unwrap(), "k", "v1"));
    assert_eq!(a.try_insert("n", "v").await.unwrap(), None);
    eventually("the stored value never arrived", || async {
        has(&b.local_entries().unwrap(), "n", "v")
    })
    .await;
}

#[tokio::test]
async fn set_with_a_wrong_lease_pushes_nothing() {
    let _gate = shared().await;
    let name = unique("lcmc");
    let (first, second) = (client().await, client().await);
    let a = updating(&first, &name).await;
    let b = updating(&second, &name).await;
    let plain = plain(&first, &name);
    plain.insert("k", "v1").await.unwrap();
    assert_eq!(b.get("k").await.unwrap().as_deref(), Some("v1"));
    plain.remove("k").await.unwrap();
    let lease = plain.lease("k", Duration::from_secs(5)).await.unwrap();
    assert!(matches!(lease, redissun::Lease::Acquired(_)));
    assert!(!a.set("k", "v2").lease("wrong").await.unwrap());
    // The reply does not tell whether it stored, so the key is invalidated (`Touch`), but the unwritten value is never pushed.
    for _ in 0..6 {
        assert!(!has(&b.local_entries().unwrap(), "k", "v2"));
        assert_ne!(b.get("k").await.unwrap().as_deref(), Some("v2"));
        assert!(!has(&a.local_entries().unwrap(), "k", "v2"));
        sleep(50 * MS).await;
    }
    assert_eq!(plain.get("k").await.unwrap(), None);
}

#[tokio::test]
async fn replace_keeps_the_limits_and_invalidates() {
    let _gate = shared().await;
    let name = unique("lcmc");
    let (first, second) = (client().await, client().await);
    let a = lcmc(&first, &name).await;
    let b = lcmc(&second, &name).await;
    let inserted = Instant::now();
    a.insert("k", "v").ttl(1000 * MS).await.unwrap();
    assert_eq!(b.get("k").await.unwrap().as_deref(), Some("v"));
    assert_eq!(a.replace("k", "w").await.unwrap().as_deref(), Some("v"));
    eventually("the replace never arrived", || async {
        b.get("k").await.unwrap().as_deref() == Some("w")
    })
    .await;
    tokio::time::sleep_until(inserted + 1100 * MS).await;
    assert_eq!(b.get("k").await.unwrap(), None);
}

#[tokio::test]
async fn compare_and_set_and_remove_if_eq_announce_on_success_only() {
    let _gate = shared().await;
    let name = unique("lcmc");
    let (first, second) = (client().await, client().await);
    let a = invalidating(&first, &name).await;
    let b = invalidating(&second, &name).await;
    plain(&first, &name).insert("k", "v").await.unwrap();
    b.get("k").await.unwrap();
    assert!(!a.compare_and_set("k", "wrong", "x").await.unwrap());
    sleep(200 * MS).await;
    assert_eq!(b.local_len(), 1);
    assert!(a.compare_and_set("k", "v", "x").await.unwrap());
    eventually("the change never arrived", || async { b.local_len() == 0 }).await;

    assert_eq!(b.get("k").await.unwrap().as_deref(), Some("x"));
    assert!(!a.remove_if_eq("k", "wrong").await.unwrap());
    sleep(200 * MS).await;
    assert_eq!(b.local_len(), 1);
    assert!(a.remove_if_eq("k", "x").await.unwrap());
    eventually("the removal never arrived", || async { b.local_len() == 0 }).await;
}

#[tokio::test]
async fn remove_many_invalidates_each_removed_key() {
    let _gate = shared().await;
    let name = unique("lcmc");
    let (first, second) = (client().await, client().await);
    let a = updating(&first, &name).await;
    let b = updating(&second, &name).await;
    let plain = plain(&first, &name);
    for k in ["a", "b", "c"] {
        plain.insert(k, "1").await.unwrap();
        b.get(k).await.unwrap();
    }
    assert_eq!(a.remove_many(&["a", "b", "zz"]).await.unwrap(), 2);
    eventually("the removals never arrived", || async {
        let keys: Vec<String> = b
            .local_entries()
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        keys == vec!["c".to_string()]
    })
    .await;
}

#[tokio::test]
async fn incr_by_invalidates() {
    let _gate = shared().await;
    let name = unique("lcmc");
    let (first, second) = (client().await, client().await);
    let build = |client: &Client| {
        client
            .local_cached_map_cache::<String, i64>(name.clone())
            .sync_strategy(SyncStrategy::Update)
            .build()
    };
    let a = build(&first).await.unwrap();
    let b = build(&second).await.unwrap();
    first
        .hash_map_cache::<String, i64>(name.clone())
        .insert("n", &1)
        .await
        .unwrap();
    assert_eq!(b.get("n").await.unwrap(), Some(1));
    assert_eq!(a.incr_by("n", 2).await.unwrap(), 3);
    eventually("the increment never arrived", || async {
        b.get("n").await.unwrap() == Some(3)
    })
    .await;
}

#[tokio::test]
async fn extend_with_ttl_caches_and_announces_each_entry() {
    let _gate = shared().await;
    let name = unique("lcmc");
    let (first, second) = (client().await, client().await);
    let a = updating(&first, &name).await;
    let b = updating(&second, &name).await;
    a.extend([("a", "1"), ("b", "2")])
        .ttl(1000 * MS)
        .await
        .unwrap();
    assert_eq!(a.local_len(), 2);
    eventually("the entries never arrived", || async {
        let entries = b.local_entries().unwrap();
        has(&entries, "a", "1") && has(&entries, "b", "2")
    })
    .await;
    sleep(1100 * MS).await;
    assert_eq!(a.local_len(), 0);
    assert_eq!(b.local_len(), 0);
}

#[tokio::test]
async fn expire_entry_on_another_instance_shortens_my_copy() {
    let _gate = shared().await;
    let name = unique("lcmc");
    let (first, second) = (client().await, client().await);
    let a = updating(&first, &name).await;
    let b = updating(&second, &name).await;
    plain(&first, &name).insert("k", "v").await.unwrap();
    assert_eq!(b.get("k").await.unwrap().as_deref(), Some("v"));
    assert!(a.expire_entry("k").ttl(300 * MS).await.unwrap());
    eventually("the expiry change never arrived", || async {
        b.local_len() == 0
    })
    .await;
    assert_eq!(b.get("k").await.unwrap().as_deref(), Some("v"));
    sleep(350 * MS).await;
    assert_eq!(b.get("k").await.unwrap(), None);
}

#[tokio::test]
async fn an_invalidation_during_a_read_is_not_overwritten() {
    let _gate = shared().await;
    let name = unique("lcmc");
    let (first, second) = (client().await, client().await);
    let a = lcmc(&first, &name).await;
    let b = lcmc(&second, &name).await;
    for round in 0..30 {
        let value = round.to_string();
        let (read, written) = tokio::join!(a.get("k"), b.insert("k", &value));
        read.unwrap();
        written.unwrap();
        // Once the invalidation has arrived, a stale read must not have been cached:
        // a stale copy would stay, so waiting for the new value still catches it.
        eventually(
            &format!("round {round}: a stale read stayed cached"),
            || async { a.get("k").await.unwrap().as_deref() == Some(value.as_str()) },
        )
        .await;
    }
}

#[tokio::test]
async fn entry_reads_redis_and_announces_its_write() {
    let _gate = shared().await;
    let name = unique("lcmc");
    let (first, second) = (client().await, client().await);
    let a = lcmc(&first, &name).await;
    let b = lcmc(&second, &name).await;
    let plain = plain(&first, &name);
    a.insert("k", "v").await.unwrap();
    assert_eq!(b.get("k").await.unwrap().as_deref(), Some("v"));
    plain.insert("k", "w").await.unwrap();
    let redissun::hash_map::Entry::Occupied(entry) = a.entry("k").await.unwrap() else {
        panic!("the entry is vacant");
    };
    assert_eq!(entry.get(), "w");
    entry.insert("x".to_string()).await.unwrap();
    eventually("the entry write never arrived", || async {
        b.get("k").await.unwrap().as_deref() == Some("x")
    })
    .await;
    plain.insert("k", "hidden").await.unwrap();
    assert_eq!(a.get("k").await.unwrap().as_deref(), Some("x"));
}

#[tokio::test]
async fn lease_writes_invalidate_other_copies() {
    let _gate = shared().await;
    let name = unique("lcmc");
    let (first, second) = (client().await, client().await);
    let a = invalidating(&first, &name).await;
    let b = invalidating(&second, &name).await;
    let plain = plain(&first, &name);
    plain.insert("k", "old").await.unwrap();
    assert_eq!(b.get("k").await.unwrap().as_deref(), Some("old"));
    plain.remove("k").await.unwrap();
    let redissun::Lease::Acquired(guard) = a.lease("k", Duration::from_secs(5)).await.unwrap()
    else {
        panic!("no lease");
    };
    guard.insert("v").await.unwrap();
    eventually("the lease write never arrived", || async {
        b.local_len() == 0
    })
    .await;
    assert_eq!(b.get("k").await.unwrap().as_deref(), Some("v"));
    assert!(a.invalidate("k").await.unwrap());
    eventually("the invalidation never arrived", || async {
        b.local_len() == 0
    })
    .await;
}

#[tokio::test]
async fn the_loader_fills_redis_and_every_local_cache() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcmc");
    let store = MemoryStore::with(&[("k", "v")]);
    let a: Cache = client
        .local_cached_map_cache::<String, String>(name.clone())
        .loader(store.clone())
        .store_cache_miss(true)
        .build()
        .await
        .unwrap();
    assert_eq!(a.get("k").await.unwrap().as_deref(), Some("v"));
    let plain = plain(&client, &name);
    assert_eq!(plain.get("k").await.unwrap().as_deref(), Some("v"));
    assert_eq!(plain.entry_ttl("k").await.unwrap(), None);
    assert_eq!(a.local_len(), 1);
    let loads = store.loads();
    assert_eq!(a.get("none").await.unwrap(), None);
    assert_eq!(a.local_len(), 2);
    assert_eq!(a.get("none").await.unwrap(), None);
    assert_eq!(store.loads(), loads + 1);
}

#[tokio::test]
async fn the_writer_sees_each_write_once() {
    let _gate = shared().await;
    let client = client().await;
    let store = MemoryStore::default();
    let cache: Cache = client
        .local_cached_map_cache::<String, String>(unique("lcmc"))
        .writer(store.clone())
        .build()
        .await
        .unwrap();
    cache.insert("1", "11").await.unwrap();
    cache.set("2", "22").await.unwrap();
    cache.replace("1", "00").await.unwrap();
    cache.replace("9", "99").await.unwrap();
    cache.extend([("3", "33"), ("4", "44")]).await.unwrap();
    assert_eq!(
        store.write_calls(),
        vec![vec!["1"], vec!["2"], vec!["1"], vec!["3", "4"]]
    );
    assert_eq!(cache.remove("1").await.unwrap().as_deref(), Some("00"));
    assert_eq!(cache.remove_many(&["2", "3", "zz"]).await.unwrap(), 2);
    assert_eq!(store.delete_calls(), vec![vec!["1"], vec!["2", "3"]]);
    let snapshot = store.snapshot();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot.get("4").map(String::as_str), Some("44"));
}

#[tokio::test]
async fn get_many_uses_local_hits_and_caches_misses() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcmc");
    let a = lcmc(&client, &name).await;
    let plain = plain(&client, &name);
    plain.insert("k1", "a").await.unwrap();
    assert_eq!(a.get("k1").await.unwrap().as_deref(), Some("a"));
    plain.insert("k1", "x").await.unwrap();
    plain.insert("k2", "b").await.unwrap();
    assert_eq!(
        a.get_many(&["k1", "k2", "k3"]).await.unwrap(),
        vec![Some("a".to_string()), Some("b".to_string()), None]
    );
    assert_eq!(a.local_len(), 2);
}

#[tokio::test]
async fn peek_neither_caches_nor_restarts_the_idle_time() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcmc");
    let a = lcmc(&client, &name).await;
    let plain = plain(&client, &name);
    plain.insert("k", "v").max_idle(300 * MS).await.unwrap();
    assert_eq!(a.peek("k").await.unwrap().as_deref(), Some("v"));
    assert_eq!(
        a.peek_many(&["k", "zz"]).await.unwrap(),
        vec![Some("v".to_string()), None]
    );
    assert_eq!(a.local_len(), 0);
    sleep(350 * MS).await;
    // `peek` ignores the idle limit; `get` shows the idle time was not restarted.
    assert_eq!(plain.get("k").await.unwrap(), None);
}

#[tokio::test]
async fn contains_key_caches_a_miss_only_with_store_cache_miss() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcmc");
    let a = lcmc(&client, &name).await;
    assert!(!a.contains_key("zz").await.unwrap());
    assert_eq!(a.local_len(), 0);
    let b: Cache = client
        .local_cached_map_cache::<String, String>(name.clone())
        .store_cache_miss(true)
        .build()
        .await
        .unwrap();
    assert!(!b.contains_key("zz").await.unwrap());
    assert_eq!(b.local_len(), 1);
    plain(&client, &name).insert("zz", "1").await.unwrap();
    assert!(!b.contains_key("zz").await.unwrap());
}

#[tokio::test]
async fn preload_cache_fills_the_local_cache() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcmc");
    let plain = plain(&client, &name);
    for i in 0..24 {
        plain.insert(&format!("k{i}"), "v").await.unwrap();
    }
    plain.insert("short", "v").ttl(300 * MS).await.unwrap();
    let a = lcmc(&client, &name).await;
    assert_eq!(a.preload_cache().page_size(7).await.unwrap(), 25);
    assert_eq!(a.local_len(), 25);
    sleep(350 * MS).await;
    assert!(!a.local_entries().unwrap().iter().any(|(k, _)| k == "short"));
    assert_eq!(a.local_len(), 24);
}

#[tokio::test]
async fn clear_local_everywhere_keeps_redis() {
    let _gate = shared().await;
    let name = unique("lcmc");
    let (first, second) = (client().await, client().await);
    let a = lcmc(&first, &name).await;
    let b = lcmc(&second, &name).await;
    let plain = plain(&first, &name);
    plain.insert("k", "v").await.unwrap();
    a.get("k").await.unwrap();
    b.get("k").await.unwrap();
    a.clear_local_everywhere().await.unwrap();
    assert_eq!(a.local_len(), 0);
    eventually("the clear never arrived", || async { b.local_len() == 0 }).await;
    assert_eq!(plain.len().await.unwrap(), 1);
}

#[tokio::test]
async fn delegated_methods_answer_like_hash_map_cache() {
    use futures::TryStreamExt;
    use redissun::{EntryEvent, EvictionMode, MapScan};
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcmc");
    let a = lcmc(&client, &name).await;
    let plain = plain(&client, &name);
    plain
        .insert("k1", "1")
        .ttl(Duration::from_secs(60))
        .await
        .unwrap();
    plain.insert("k2", "2").await.unwrap();
    plain.insert("x", "3").await.unwrap();
    assert!(a.entry_ttl("k1").await.unwrap().is_some());
    assert_eq!(
        a.entry_ttl("k2").await.unwrap(),
        plain.entry_ttl("k2").await.unwrap()
    );
    assert_eq!(
        a.contains_value("2").await.unwrap(),
        plain.contains_value("2").await.unwrap()
    );
    assert_eq!(
        a.value_len("k1").await.unwrap(),
        plain.value_len("k1").await.unwrap()
    );
    assert_eq!(
        a.to_hash_map().await.unwrap(),
        plain.to_hash_map().await.unwrap()
    );
    let mut mine: Vec<(String, String)> = a.iter().try_collect().await.unwrap();
    let mut theirs: Vec<(String, String)> = plain.iter().try_collect().await.unwrap();
    mine.sort();
    theirs.sort();
    assert_eq!(mine, theirs);
    let mut keys: Vec<String> = a
        .keys_with(MapScan::new().pattern("\"k1*"))
        .try_collect()
        .await
        .unwrap();
    keys.sort();
    let mut plain_keys: Vec<String> = plain
        .keys_with(MapScan::new().pattern("\"k1*"))
        .try_collect()
        .await
        .unwrap();
    plain_keys.sort();
    assert_eq!(keys, plain_keys);
    assert_eq!(keys, vec!["k1".to_string()]);
    assert_eq!(a.random_keys(10).await.unwrap().len(), 3);
    assert_eq!(a.random_entries(10).await.unwrap().len(), 3);
    let mut values: Vec<String> = a.values().try_collect().await.unwrap();
    values.sort();
    assert_eq!(values, vec!["1", "2", "3"]);

    a.set_max_size(10, EvictionMode::Lru).await.unwrap();
    let options = format!("{{{name}}}:redissun_options");
    assert_eq!(
        bulk(&raw_command(&["HGET", &options, "max-size"]).await).as_deref(),
        Some("10")
    );
    assert!(!a.init_max_size(5, EvictionMode::Lru).await.unwrap());

    let mut events = a.events().await.unwrap();
    plain.insert("new", "n").await.unwrap();
    let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        event,
        EntryEvent::Created {
            key: "new".into(),
            value: "n".into()
        }
    );
}

#[tokio::test]
async fn lock_lives_under_the_redisson_name() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcmc");
    let a = lcmc(&client, &name).await;
    let lock_key = format!("{{{name}}}:IKm9EBQhlzpVIBxUXDHBWw:lock");
    let guard = a.lock("key").unwrap().lock().await.unwrap();
    assert_eq!(raw_command(&["EXISTS", &lock_key]).await, ":1\r\n");
    guard.unlock().await.unwrap();
}

// ---- Task 5: entries the server drops, expiry of the whole cache, reconnection, limits ----

fn local_keys(cache: &Cache) -> std::vec::Vec<String> {
    let mut keys: std::vec::Vec<String> = cache
        .local_entries()
        .unwrap()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    keys.sort();
    keys
}

#[tokio::test]
async fn max_size_eviction_drops_my_copy() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcmc");
    let a = lcmc(&client, &name).await;
    let plain = plain(&client, &name);
    a.set_max_size(2, redissun::EvictionMode::Lru)
        .await
        .unwrap();
    a.insert("k1", "1").await.unwrap();
    assert_eq!(local_keys(&a), ["k1"]);
    sleep(10 * MS).await;
    plain.insert("k2", "2").await.unwrap();
    sleep(10 * MS).await;
    plain.insert("k3", "3").await.unwrap();
    eventually("the evicted copy stayed", || async {
        !local_keys(&a).contains(&"k1".to_string())
    })
    .await;
    assert_eq!(plain.get("k1").await.unwrap(), None);
}

#[tokio::test]
async fn the_clean_up_drops_a_copy_whose_ttl_was_shortened_behind_its_back() {
    let _gate = shared().await;
    let cleaning = redissun::Client::builder()
        .url(common::redis_url().await)
        .eviction_interval(Duration::from_secs(1))
        .build()
        .await
        .unwrap();
    let name = unique("lcmc");
    let a = lcmc(&cleaning, &name).await;
    let other = client().await;
    let plain = plain(&other, &name);
    a.insert("k", "v").await.unwrap();
    assert_eq!(a.local_len(), 1);
    assert!(plain.expire_entry("k").ttl(200 * MS).await.unwrap());
    eventually("the expired copy stayed", || async { a.local_len() == 0 }).await;
}

#[tokio::test]
async fn a_removal_by_a_plain_cache_client_drops_my_copy() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcmc");
    let a = lcmc(&client, &name).await;
    a.insert("k", "v").await.unwrap();
    assert_eq!(a.local_len(), 1);
    plain(&client, &name).remove("k").await.unwrap();
    eventually("the removed copy stayed", || async { a.local_len() == 0 }).await;
}

#[tokio::test]
async fn sync_none_still_drops_evicted_copies() {
    let _gate = shared().await;
    let (first, second) = (client().await, client().await);
    let name = unique("lcmc");
    let unsynced = |c: Client| {
        let name = name.clone();
        async move {
            c.local_cached_map_cache::<String, String>(name)
                .sync_strategy(SyncStrategy::None)
                .build()
                .await
                .unwrap()
        }
    };
    let a = unsynced(first.clone()).await;
    let b = unsynced(second.clone()).await;
    a.insert("k", "1").await.unwrap();
    b.insert("k", "2").await.unwrap();
    sleep(200 * MS).await;
    assert_eq!(a.get("k").await.unwrap().as_deref(), Some("1"));
    plain(&second, &name).remove("k").await.unwrap();
    eventually("the removed copy stayed", || async { a.local_len() == 0 }).await;
}

#[tokio::test]
async fn the_map_expiring_clears_every_local_cache() {
    let _gate = exclusive().await;
    raw_command(&["CONFIG", "SET", "notify-keyspace-events", "EKx"]).await;
    for policy in [
        redissun::ExpirationEventPolicy::KeyeventPattern,
        redissun::ExpirationEventPolicy::KeyspaceChannel,
    ] {
        let name = unique("lcmc");
        let (first, second) = (client().await, client().await);
        let build = |c: Client| {
            let name = name.clone();
            async move {
                c.local_cached_map_cache::<String, String>(name)
                    .expiration_event_policy(policy)
                    .build()
                    .await
                    .unwrap()
            }
        };
        let a = build(first).await;
        let b = build(second).await;
        a.insert("k", "v").await.unwrap();
        eventually("b never read k", || async {
            b.get("k").await.unwrap().as_deref() == Some("v")
        })
        .await;
        assert!(a.expire(200 * MS).await.unwrap());
        eventually(&format!("{policy:?}: b kept its copy"), || async {
            b.local_len() == 0
        })
        .await;
    }
    let name = unique("lcmc");
    let (first, second) = (client().await, client().await);
    let a = lcmc(&first, &name).await;
    let b = second
        .local_cached_map_cache::<String, String>(name.clone())
        .expiration_event_policy(redissun::ExpirationEventPolicy::None)
        .build()
        .await
        .unwrap();
    a.insert("k", "v").await.unwrap();
    // A read that overlaps the invalidation of the insert is not cached, so wait for a cached copy.
    eventually("b never cached k", || async {
        b.get("k").await.unwrap().as_deref() == Some("v") && b.local_len() == 1
    })
    .await;
    assert!(a.expire(200 * MS).await.unwrap());
    sleep(500 * MS).await;
    assert_eq!(b.local_len(), 1);
}

async fn kill_pubsub_connections() {
    use tokio::io::AsyncWriteExt;
    let url = common::redis_url().await;
    let mut stream = tokio::net::TcpStream::connect(url.trim_start_matches("redis://"))
        .await
        .unwrap();
    stream
        .write_all(b"*4\r\n$6\r\nCLIENT\r\n$4\r\nKILL\r\n$4\r\nTYPE\r\n$6\r\npubsub\r\n")
        .await
        .unwrap();
}

async fn with_reconnection(
    client: &Client,
    name: &str,
    strategy: redissun::ReconnectionStrategy,
) -> Cache {
    client
        .local_cached_map_cache::<String, String>(name.to_string())
        .reconnection_strategy(strategy)
        .build()
        .await
        .unwrap()
}

#[tokio::test]
async fn the_clear_strategy_clears_the_cache_after_a_reconnect() {
    let _gate = exclusive().await;
    let client = client().await;
    let cache = with_reconnection(
        &client,
        &unique("lcmc"),
        redissun::ReconnectionStrategy::Clear,
    )
    .await;
    cache.insert("k", "v").await.unwrap();
    assert_eq!(cache.local_len(), 1);
    kill_pubsub_connections().await;
    eventually("the cache was not cleared", || async {
        cache.local_len() == 0
    })
    .await;
    assert_eq!(cache.get("k").await.unwrap().as_deref(), Some("v"));
}

#[tokio::test]
async fn a_reconnect_clears_the_cache_whatever_the_strategy() {
    use redissun::ReconnectionStrategy::{Load, None as Keep};
    let _gate = exclusive().await;
    for strategy in [Keep, Load] {
        let name = unique("lcmc");
        let client = client().await;
        let cache = with_reconnection(&client, &name, strategy).await;
        cache.insert("a", "1").await.unwrap();
        assert_eq!(cache.local_len(), 1);
        // A removal the client cannot hear about while it is away: HDEL publishes no entry event.
        kill_pubsub_connections().await;
        raw_command(&["HDEL", &name, "\"a\""]).await;
        eventually(
            &format!("{strategy:?}: the cache was not cleared"),
            || async { cache.local_len() == 0 },
        )
        .await;
        assert_eq!(cache.get("a").await.unwrap(), None);
    }
}

#[tokio::test]
async fn the_load_strategy_logs_every_announced_write() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcmc");
    let a = with_reconnection(&client, &name, redissun::ReconnectionStrategy::Load).await;
    let log = format!("redissun__cache_updates_log:{{{name}}}");
    let count = || async { raw_command(&["ZCARD", &log]).await.trim().to_string() };
    a.insert("a", "1").await.unwrap();
    assert_eq!(count().await, ":1");
    assert_eq!(a.try_insert("b", "1").await.unwrap(), None);
    assert_eq!(count().await, ":2");
    assert_eq!(a.remove_many(&["a", "b"]).await.unwrap(), 2);
    assert_eq!(count().await, ":4");
}

#[tokio::test]
async fn the_local_cache_size_limit_is_independent_of_max_size() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcmc");
    let plain = plain(&client, &name);
    for k in ["a", "b", "c"] {
        plain.insert(k, "v").await.unwrap();
    }
    let a = client
        .local_cached_map_cache::<String, String>(name.clone())
        .cache_size(2)
        .eviction_policy(redissun::EvictionPolicy::Lru)
        .build()
        .await
        .unwrap();
    for k in ["a", "b", "c"] {
        assert_eq!(a.get(k).await.unwrap().as_deref(), Some("v"));
    }
    assert_eq!(a.local_len(), 2);
    assert_eq!(plain.len().await.unwrap(), 3);
}

#[cfg(feature = "moka")]
#[tokio::test]
async fn moka_copies_expire_with_the_entry_ttl() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcmc");
    let a = client
        .local_cached_map_cache::<String, String>(name.clone())
        .cache_provider(redissun::CacheProvider::Moka)
        .build()
        .await
        .unwrap();
    plain(&client, &name)
        .insert("k", "v")
        .ttl(300 * MS)
        .await
        .unwrap();
    assert_eq!(a.get("k").await.unwrap().as_deref(), Some("v"));
    assert_eq!(a.local_len(), 1);
    sleep(350 * MS).await;
    assert!(a.local_entries().unwrap().is_empty());
    assert_eq!(a.get("k").await.unwrap(), None);
}

// ---- Task 6: StoreMode::Local ----

async fn local_mode(client: &Client, name: &str, sync: SyncStrategy) -> Cache {
    client
        .local_cached_map_cache::<String, String>(name.to_string())
        .store_mode(redissun::StoreMode::Local)
        .sync_strategy(sync)
        .build()
        .await
        .unwrap()
}

async fn exists(key: &str) -> String {
    raw_command(&["EXISTS", key]).await.trim().to_string()
}

#[tokio::test]
async fn local_mode_keeps_entries_out_of_redis() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcmc");
    let a = local_mode(&client, &name, SyncStrategy::Invalidate).await;
    a.insert("k", "v")
        .ttl(Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(exists(&name).await, ":0");
    assert_eq!(exists(&format!("{{{name}}}:redissun_options")).await, ":0");
    assert_eq!(a.get("k").await.unwrap().as_deref(), Some("v"));
}

#[tokio::test]
async fn local_mode_entries_honour_ttl_and_max_idle() {
    let _gate = shared().await;
    let client = client().await;
    let a = local_mode(&client, &unique("lcmc"), SyncStrategy::Invalidate).await;
    a.insert("t", "v").ttl(200 * MS).await.unwrap();
    a.insert("i", "v").max_idle(200 * MS).await.unwrap();
    a.insert("long", "v")
        .ttl(Duration::from_secs(60))
        .await
        .unwrap();
    let ttl = a.entry_ttl("long").await.unwrap().unwrap();
    assert!(ttl <= Duration::from_secs(60) && ttl > Duration::from_secs(50));
    for _ in 0..5 {
        sleep(100 * MS).await;
        assert_eq!(a.get("i").await.unwrap().as_deref(), Some("v"));
    }
    assert_eq!(a.get("t").await.unwrap(), None);
    sleep(250 * MS).await;
    assert_eq!(a.get("i").await.unwrap(), None);
}

#[tokio::test]
async fn local_mode_syncs_other_instances() {
    let _gate = shared().await;
    let (first, second) = (client().await, client().await);
    let name = unique("lcmc");
    let a = local_mode(&first, &name, SyncStrategy::Update).await;
    let b = local_mode(&second, &name, SyncStrategy::Update).await;
    a.insert("k", "v")
        .ttl(Duration::from_secs(1))
        .await
        .unwrap();
    eventually("b never got the update", || async {
        b.get("k").await.unwrap().as_deref() == Some("v")
    })
    .await;
    sleep(1100 * MS).await;
    assert_eq!(b.get("k").await.unwrap(), None);

    let name = unique("lcmc");
    let a = local_mode(&first, &name, SyncStrategy::Invalidate).await;
    let b = local_mode(&second, &name, SyncStrategy::Invalidate).await;
    b.insert("k", "old").await.unwrap();
    a.insert("k", "new").await.unwrap();
    eventually("b kept its copy", || async { b.local_len() == 0 }).await;
}

#[tokio::test]
async fn local_mode_len_iter_and_clear() {
    use futures::TryStreamExt;
    let _gate = shared().await;
    let (first, second) = (client().await, client().await);
    let name = unique("lcmc");
    let a = local_mode(&first, &name, SyncStrategy::Invalidate).await;
    let b = local_mode(&second, &name, SyncStrategy::Invalidate).await;
    assert!(a.is_empty().await.unwrap());
    a.insert("x", "1").await.unwrap();
    a.insert("y", "2").await.unwrap();
    b.insert("z", "3").await.unwrap();
    assert_eq!(a.len().await.unwrap(), 2);
    let mut entries: std::vec::Vec<(String, String)> = a.iter().try_collect().await.unwrap();
    entries.sort();
    assert_eq!(
        entries,
        [
            ("x".to_string(), "1".to_string()),
            ("y".to_string(), "2".to_string())
        ]
    );
    assert_eq!(a.to_hash_map().await.unwrap().len(), 2);
    a.clear().await.unwrap();
    assert!(a.is_empty().await.unwrap());
    eventually("b was not cleared", || async { b.local_len() == 0 }).await;
}

#[tokio::test]
async fn local_mode_loader_and_writer() {
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcmc");
    let loader = MemoryStore::with(&[("k", "v")]);
    let writer = MemoryStore::default();
    let a: Cache = client
        .local_cached_map_cache::<String, String>(name.clone())
        .store_mode(redissun::StoreMode::Local)
        .loader(loader.clone())
        .writer(writer.clone())
        .build()
        .await
        .unwrap();
    assert_eq!(a.get("k").await.unwrap().as_deref(), Some("v"));
    assert_eq!(a.local_len(), 1);
    let loads = loader.loads();
    assert_eq!(a.get("k").await.unwrap().as_deref(), Some("v"));
    assert_eq!(loader.loads(), loads);
    assert_eq!(exists(&name).await, ":0");
    a.insert("w", "1").await.unwrap();
    assert_eq!(a.remove("w").await.unwrap().as_deref(), Some("1"));
    assert_eq!(writer.write_calls(), vec![vec!["w"]]);
    assert_eq!(writer.delete_calls(), vec![vec!["w"]]);
}

#[tokio::test]
async fn local_mode_redis_only_methods_are_config_errors() {
    use futures::StreamExt;
    use redissun::{Error, EvictionMode, MapScan};
    let _gate = shared().await;
    let client = client().await;
    let name = unique("lcmc");
    let a = local_mode(&client, &name, SyncStrategy::Invalidate).await;
    let check = |what: &str, result: Result<(), Error>| match result {
        Err(Error::Config(m)) => assert!(m.contains("StoreMode::LocalRedis"), "{what}: {m}"),
        other => panic!("{what}: {other:?}"),
    };
    check("incr_by", a.incr_by("n", 1).await.map(drop));
    check(
        "set_max_size",
        a.set_max_size(1, EvictionMode::Lru).await.map(drop),
    );
    check(
        "init_max_size",
        a.init_max_size(1, EvictionMode::Lru).await.map(drop),
    );
    check("events", a.events().await.map(drop));
    check("keyspace_events", a.keyspace_events().await.map(drop));
    check(
        "lease",
        a.lease("k", Duration::from_secs(1)).await.map(drop),
    );
    check("invalidate", a.invalidate("k").await.map(drop));
    check("contains_value", a.contains_value("v").await.map(drop));
    check("value_len", a.value_len("k").await.map(drop));
    check("random_keys", a.random_keys(1).await.map(drop));
    check("random_entries", a.random_entries(1).await.map(drop));
    let first = Box::pin(a.iter_with(MapScan::new())).next().await.unwrap();
    check("iter_with", first.map(drop));
    check("set lease", a.set("k", "v").lease("t").await.map(drop));
    assert_eq!(exists(&name).await, ":0");
}

#[tokio::test]
async fn local_mode_preload_returns_zero() {
    let _gate = shared().await;
    let client = client().await;
    let a = local_mode(&client, &unique("lcmc"), SyncStrategy::Invalidate).await;
    assert_eq!(a.preload_cache().await.unwrap(), 0);
}

// ---- Task 7: cluster and Object ----

#[tokio::test]
async fn every_operation_runs_on_a_cluster() {
    use redissun::{EvictionMode, ReconnectionStrategy};
    let _gate = shared().await;
    let topology = common::topology::Topology::cluster().await;
    let connect = || async {
        Client::builder()
            .url(topology.cluster_url())
            .build()
            .await
            .expect("could not connect to the cluster")
    };
    let (first, second) = (connect().await, connect().await);
    let name = format!("{{{}}}:m", unique("t"));
    let build = |c: Client| {
        let name = name.clone();
        async move {
            c.local_cached_map_cache::<String, String>(name)
                .reconnection_strategy(ReconnectionStrategy::Load)
                .sync_strategy(SyncStrategy::Update)
                .build()
                .await
                .unwrap()
        }
    };
    let a = build(first.clone()).await;
    let b = build(second.clone()).await;
    a.insert("k", "v")
        .ttl(Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(a.get("k").await.unwrap().as_deref(), Some("v"));
    assert_eq!(
        a.get_many(&["k", "zz"]).await.unwrap(),
        [Some("v".to_string()), None]
    );
    assert_eq!(a.try_insert("k", "x").await.unwrap().as_deref(), Some("v"));
    assert_eq!(a.replace("k", "w").await.unwrap().as_deref(), Some("v"));
    a.extend([("e1", "1"), ("e2", "2")])
        .ttl(Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(a.remove_many(&["e1", "zz"]).await.unwrap(), 1);
    assert!(a
        .expire_entry("k")
        .ttl(Duration::from_secs(30))
        .await
        .unwrap());
    let counters = first
        .local_cached_map_cache::<String, i64>(name.clone())
        .build()
        .await
        .unwrap();
    assert_eq!(counters.incr_by("n", 2).await.unwrap(), 2);
    a.entry("en")
        .await
        .unwrap()
        .or_insert("1".to_string())
        .await
        .unwrap();
    assert_eq!(b.preload_cache().await.unwrap(), 4);
    let log = format!("redissun__cache_updates_log:{name}");
    let port = topology.cluster_master_of(&name).await;
    let zcard = topology.cli(port, &["ZCARD", &log]).await;
    assert!(zcard.trim().parse::<i64>().unwrap() > 0, "{zcard}");

    b.clear().await.unwrap();
    b.set_max_size(1, EvictionMode::Lru).await.unwrap();
    b.insert("x", "1").await.unwrap();
    assert_eq!(b.local_len(), 1);
    sleep(10 * MS).await;
    a.insert("y", "2").await.unwrap();
    eventually("the eviction never reached b", || async {
        !local_keys(&b).contains(&"x".to_string())
    })
    .await;
    a.clear().await.unwrap();
}

#[tokio::test]
async fn object_methods_cover_every_key() {
    use redissun::ReconnectionStrategy;
    let _gate = shared().await;
    let (first, second) = (client().await, client().await);
    let name = unique("lcmc");
    let a = with_reconnection(&first, &name, ReconnectionStrategy::Load).await;
    let b = with_reconnection(&second, &name, ReconnectionStrategy::Load).await;
    a.insert("k", "v")
        .ttl(Duration::from_secs(60))
        .await
        .unwrap();
    a.insert("i", "v")
        .max_idle(Duration::from_secs(60))
        .await
        .unwrap();
    a.set_max_size(10, redissun::EvictionMode::Lru)
        .await
        .unwrap();
    assert!(a.expire(Duration::from_secs(1)).await.unwrap());
    assert!(a.ttl().await.unwrap().is_some());
    assert!(a.persist().await.unwrap());
    let keys = |n: &str| {
        vec![
            n.to_string(),
            format!("redissun__timeout__set:{{{n}}}"),
            format!("redissun__idle__set:{{{n}}}"),
            format!("{{{n}}}:redissun_options"),
            format!("redissun__cache_updates_log:{{{n}}}"),
        ]
    };
    for key in keys(&name) {
        assert_eq!(exists(&key).await, ":1", "{key}");
    }
    let n2 = unique("lcmc");
    a.rename(&n2).await.unwrap();
    for key in keys(&n2) {
        assert_eq!(exists(&key).await, ":1", "{key}");
    }
    for key in keys(&name) {
        assert_eq!(exists(&key).await, ":0", "{key}");
    }
    let moved = with_reconnection(&first, &n2, ReconnectionStrategy::Load).await;
    let other = with_reconnection(&second, &n2, ReconnectionStrategy::Load).await;
    assert_eq!(other.get("k").await.unwrap().as_deref(), Some("v"));
    assert_eq!(other.local_len(), 1);
    assert!(moved.del().await.unwrap());
    for key in keys(&n2) {
        assert_eq!(exists(&key).await, ":0", "{key}");
    }
    eventually("the clear never reached the other instance", || async {
        other.local_len() == 0
    })
    .await;
    drop(b);
}

// ---- Final review fixes ----

#[tokio::test]
async fn concurrent_writes_never_leave_a_stale_copy() {
    let _gate = shared().await;
    let (first, second) = (client().await, client().await);
    let name = unique("lcmc");
    let a = lcmc(&first, &name).await;
    let b = lcmc(&second, &name).await;
    let plain = plain(&first, &name);
    for round in 0..30 {
        let (mine, theirs) = (format!("a{round}"), format!("b{round}"));
        let (x, y) = tokio::join!(a.insert("k", &mine), b.insert("k", &theirs));
        x.unwrap();
        y.unwrap();
        let stored = plain.get("k").await.unwrap();
        eventually(&format!("round {round}: a kept a stale copy"), || async {
            a.get("k").await.unwrap() == stored && b.get("k").await.unwrap() == stored
        })
        .await;
    }
}

#[tokio::test]
async fn local_mode_update_keeps_a_read_copy_alive_on_other_instances() {
    let _gate = shared().await;
    let (first, second) = (client().await, client().await);
    let name = unique("lcmc");
    let a = local_mode(&first, &name, SyncStrategy::Update).await;
    let b = local_mode(&second, &name, SyncStrategy::Update).await;
    a.insert("k", "v").max_idle(600 * MS).await.unwrap();
    eventually("b never got the update", || async { b.local_len() == 1 }).await;
    for _ in 0..6 {
        sleep(200 * MS).await;
        assert_eq!(b.get("k").await.unwrap().as_deref(), Some("v"));
    }
}

#[tokio::test]
async fn local_mode_replace_and_compare_and_set_keep_the_limits() {
    let _gate = shared().await;
    let client = client().await;
    let a = local_mode(&client, &unique("lcmc"), SyncStrategy::Invalidate).await;
    a.insert("r", "1").ttl(300 * MS).await.unwrap();
    a.insert("c", "1").ttl(300 * MS).await.unwrap();
    assert_eq!(a.replace("r", "2").await.unwrap().as_deref(), Some("1"));
    assert!(a.compare_and_set("c", "1", "2").await.unwrap());
    assert!(a.entry_ttl("r").await.unwrap().unwrap() <= 300 * MS);
    assert!(a.entry_ttl("c").await.unwrap().unwrap() <= 300 * MS);
    sleep(350 * MS).await;
    assert_eq!(a.get("r").await.unwrap(), None);
    assert_eq!(a.get("c").await.unwrap(), None);
}
