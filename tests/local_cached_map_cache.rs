mod common;

use common::{client, raw_command, unique};
use redissun::{Client, HashMapCache, JsonCodec, LocalCachedMapCache, SyncStrategy};
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
    plain.remove("k").await.unwrap();
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
