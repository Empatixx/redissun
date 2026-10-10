//! Compares redissun with the hand-written equivalent on two other Rust clients.
//!
//! Every library runs the same operations against the same Redis with the same JSON payload. The
//! raw clients do by hand what a redissun object does for you: encode with `serde_json`, run the
//! command, decode. `HashMap::insert` returns the replaced value, so its baseline runs the same
//! `HGET` + `HSET` Lua script. The lock baseline is the usual `SET NX PX` plus a Lua
//! compare-and-delete; a redissun lock does more (reentrancy, wake-up by pub/sub, a watchdog).
//!
//! ```bash
//! REDISSUN_BENCH_URL=redis://127.0.0.1:6379 cargo bench --bench compare
//! ```
//!
//! The results go to `benches/results/rust.json`. See `benches/README.md`.

use fred::interfaces::{ClientLike, HashesInterface, KeysInterface};
use fred::prelude::{Builder, Config};
use fred::types::scripts::Script;
use fred::types::{Expiration, SetOptions, Value};
use redis::AsyncCommands;
use redissun::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

const CONCURRENCY: usize = 64;
const POOL: usize = 4;
const WARMUP: Duration = Duration::from_secs(1);
const MEASURE: Duration = Duration::from_secs(3);
const BATCH: usize = 100;
const FIELD: &str = "field";
const TOKEN: &str = "token";
const INSERT: &str = "local previous = redis.call('HGET', KEYS[1], ARGV[1]) redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]) return previous";
const UNLOCK: &str =
    "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) end return 0";

#[derive(Serialize, Deserialize, Clone)]
struct User {
    id: u64,
    name: String,
    email: String,
    active: bool,
}

fn user() -> User {
    User {
        id: 42,
        name: "Jirka Novak".into(),
        email: "jirka@example.com".into(),
        active: true,
    }
}

type Fut = Pin<Box<dyn Future<Output = ()> + Send>>;
type Op = Arc<dyn Fn(usize) -> Fut + Send + Sync>;
type Ops = Vec<(&'static str, Op)>;

fn op<F, Fu>(f: F) -> Op
where
    F: Fn(usize) -> Fu + Send + Sync + 'static,
    Fu: Future<Output = ()> + Send + 'static,
{
    Arc::new(move |slot| Box::pin(f(slot)))
}

fn key(run: &str, lib: &str, kind: &str, slot: usize) -> String {
    format!("bench:{run}:{lib}:{kind}:{slot}")
}

async fn redissun_ops(url: &str, run: &str) -> Ops {
    let client = Client::builder()
        .url(url)
        .build()
        .await
        .expect("redissun connects");
    let user = Arc::new(user());
    let buckets = Arc::new(
        (0..CONCURRENCY)
            .map(|slot| client.bucket::<User>(key(run, "redissun", "bucket", slot)))
            .collect::<Vec<_>>(),
    );
    let maps = Arc::new(
        (0..CONCURRENCY)
            .map(|slot| client.hash_map::<String, User>(key(run, "redissun", "map", slot)))
            .collect::<Vec<_>>(),
    );
    let locks = Arc::new(
        (0..CONCURRENCY)
            .map(|slot| client.lock(key(run, "redissun", "lock", slot)))
            .collect::<Vec<_>>(),
    );
    for slot in 0..CONCURRENCY {
        buckets[slot].set(&*user).await.unwrap();
        maps[slot].insert(FIELD, &*user).await.unwrap();
    }
    let batch_names: Arc<Vec<String>> = Arc::new(
        (0..BATCH)
            .map(|i| key(run, "redissun", "batch", i))
            .collect(),
    );

    let (b, u) = (buckets.clone(), user.clone());
    let bucket_set = op(move |slot| {
        let (b, u) = (b.clone(), u.clone());
        async move {
            b[slot].set(&*u).await.unwrap();
        }
    });
    let b = buckets.clone();
    let bucket_get = op(move |slot| {
        let b = b.clone();
        async move {
            b[slot].get().await.unwrap().unwrap();
        }
    });
    let (m, u) = (maps.clone(), user.clone());
    let map_insert = op(move |slot| {
        let (m, u) = (m.clone(), u.clone());
        async move {
            m[slot].insert(FIELD, &*u).await.unwrap();
        }
    });
    let m = maps.clone();
    let map_get = op(move |slot| {
        let m = m.clone();
        async move {
            m[slot].get(FIELD).await.unwrap().unwrap();
        }
    });
    let l = locks.clone();
    let lock_unlock = op(move |slot| {
        let l = l.clone();
        async move {
            let guard = l[slot].lock().await.unwrap();
            guard.unlock().await.unwrap();
        }
    });
    let (c, u, names) = (client.clone(), user.clone(), batch_names.clone());
    let batch = op(move |_| {
        let (c, u, names) = (c.clone(), u.clone(), names.clone());
        async move {
            let batch = c.batch();
            for name in names.iter() {
                let _ = batch.bucket::<User>(name.as_str()).set(&*u);
            }
            batch.execute().await.unwrap();
        }
    });
    vec![
        ("bucket_set", bucket_set),
        ("bucket_get", bucket_get),
        ("map_insert", map_insert),
        ("map_get", map_get),
        ("lock_unlock", lock_unlock),
        ("batch_100", batch),
    ]
}

async fn fred_ops(url: &str, run: &str) -> Ops {
    let config = Config::from_url(url).expect("fred url");
    let pool = Builder::from_config(config)
        .build_pool(POOL)
        .expect("fred pool");
    pool.init().await.expect("fred connects");
    let pool = Arc::new(pool);
    let user = Arc::new(user());
    let unlock = Arc::new(Script::from_lua(UNLOCK));
    let insert = Arc::new(Script::from_lua(INSERT));
    for slot in 0..CONCURRENCY {
        let json = serde_json::to_string(&*user).unwrap();
        let () = pool
            .set(
                key(run, "fred", "bucket", slot),
                json.clone(),
                None,
                None,
                false,
            )
            .await
            .unwrap();
        let _: i64 = pool
            .hset(key(run, "fred", "map", slot), (FIELD, json))
            .await
            .unwrap();
    }

    let (p, u, run_id) = (pool.clone(), user.clone(), run.to_string());
    let bucket_set = op(move |slot| {
        let (p, u, run_id) = (p.clone(), u.clone(), run_id.clone());
        async move {
            let json = serde_json::to_string(&*u).unwrap();
            let () = p
                .set(
                    key(&run_id, "fred", "bucket", slot),
                    json,
                    None,
                    None,
                    false,
                )
                .await
                .unwrap();
        }
    });
    let (p, run_id) = (pool.clone(), run.to_string());
    let bucket_get = op(move |slot| {
        let (p, run_id) = (p.clone(), run_id.clone());
        async move {
            let raw: Option<String> = p.get(key(&run_id, "fred", "bucket", slot)).await.unwrap();
            serde_json::from_str::<User>(&raw.unwrap()).unwrap();
        }
    });
    let (p, u, s, run_id) = (pool.clone(), user.clone(), insert.clone(), run.to_string());
    let map_insert = op(move |slot| {
        let (p, u, s, run_id) = (p.clone(), u.clone(), s.clone(), run_id.clone());
        async move {
            let json = serde_json::to_string(&*u).unwrap();
            let previous: Option<String> = s
                .evalsha_with_reload(
                    p.next(),
                    vec![key(&run_id, "fred", "map", slot)],
                    vec![FIELD.to_string(), json],
                )
                .await
                .unwrap();
            serde_json::from_str::<User>(&previous.unwrap()).unwrap();
        }
    });
    let (p, run_id) = (pool.clone(), run.to_string());
    let map_get = op(move |slot| {
        let (p, run_id) = (p.clone(), run_id.clone());
        async move {
            let raw: Option<String> = p
                .hget(key(&run_id, "fred", "map", slot), FIELD)
                .await
                .unwrap();
            serde_json::from_str::<User>(&raw.unwrap()).unwrap();
        }
    });
    let (p, s, run_id) = (pool.clone(), unlock.clone(), run.to_string());
    let lock_unlock = op(move |slot| {
        let (p, s, run_id) = (p.clone(), s.clone(), run_id.clone());
        async move {
            let name = key(&run_id, "fred", "lock", slot);
            let taken: Option<String> = p
                .set(
                    &name,
                    TOKEN,
                    Some(Expiration::PX(30_000)),
                    Some(SetOptions::NX),
                    false,
                )
                .await
                .unwrap();
            assert!(taken.is_some());
            let _: i64 = s
                .evalsha_with_reload(p.next(), vec![name], vec![TOKEN])
                .await
                .unwrap();
        }
    });
    let (p, u, run_id) = (pool.clone(), user.clone(), run.to_string());
    let batch = op(move |_| {
        let (p, u, run_id) = (p.clone(), u.clone(), run_id.clone());
        async move {
            let pipeline = p.next().pipeline();
            let json = serde_json::to_string(&*u).unwrap();
            for i in 0..BATCH {
                let () = pipeline
                    .set(
                        key(&run_id, "fred", "batch", i),
                        json.clone(),
                        None,
                        None,
                        false,
                    )
                    .await
                    .unwrap();
            }
            let _: Vec<Value> = pipeline.all().await.unwrap();
        }
    });
    vec![
        ("bucket_set", bucket_set),
        ("bucket_get", bucket_get),
        ("map_insert", map_insert),
        ("map_get", map_get),
        ("lock_unlock", lock_unlock),
        ("batch_100", batch),
    ]
}

async fn redis_rs_ops(url: &str, run: &str) -> Ops {
    let client = redis::Client::open(url).expect("redis-rs url");
    let connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("redis-rs connects");
    let user = Arc::new(user());
    let unlock = Arc::new(redis::Script::new(UNLOCK));
    let insert = Arc::new(redis::Script::new(INSERT));
    for slot in 0..CONCURRENCY {
        let json = serde_json::to_string(&*user).unwrap();
        let mut c = connection.clone();
        let () = c
            .set(key(run, "redis", "bucket", slot), &json)
            .await
            .unwrap();
        let _: i64 = c
            .hset(key(run, "redis", "map", slot), FIELD, &json)
            .await
            .unwrap();
    }

    let (c, u, run_id) = (connection.clone(), user.clone(), run.to_string());
    let bucket_set = op(move |slot| {
        let (mut c, u, run_id) = (c.clone(), u.clone(), run_id.clone());
        async move {
            let json = serde_json::to_string(&*u).unwrap();
            let () = c
                .set(key(&run_id, "redis", "bucket", slot), json)
                .await
                .unwrap();
        }
    });
    let (c, run_id) = (connection.clone(), run.to_string());
    let bucket_get = op(move |slot| {
        let (mut c, run_id) = (c.clone(), run_id.clone());
        async move {
            let raw: Option<String> = c.get(key(&run_id, "redis", "bucket", slot)).await.unwrap();
            serde_json::from_str::<User>(&raw.unwrap()).unwrap();
        }
    });
    let (c, u, s, run_id) = (
        connection.clone(),
        user.clone(),
        insert.clone(),
        run.to_string(),
    );
    let map_insert = op(move |slot| {
        let (mut c, u, s, run_id) = (c.clone(), u.clone(), s.clone(), run_id.clone());
        async move {
            let json = serde_json::to_string(&*u).unwrap();
            let previous: Option<String> = s
                .key(key(&run_id, "redis", "map", slot))
                .arg(FIELD)
                .arg(json)
                .invoke_async(&mut c)
                .await
                .unwrap();
            serde_json::from_str::<User>(&previous.unwrap()).unwrap();
        }
    });
    let (c, run_id) = (connection.clone(), run.to_string());
    let map_get = op(move |slot| {
        let (mut c, run_id) = (c.clone(), run_id.clone());
        async move {
            let raw: Option<String> = c
                .hget(key(&run_id, "redis", "map", slot), FIELD)
                .await
                .unwrap();
            serde_json::from_str::<User>(&raw.unwrap()).unwrap();
        }
    });
    let (c, s, run_id) = (connection.clone(), unlock.clone(), run.to_string());
    let lock_unlock = op(move |slot| {
        let (mut c, s, run_id) = (c.clone(), s.clone(), run_id.clone());
        async move {
            let name = key(&run_id, "redis", "lock", slot);
            let taken: Option<String> = redis::cmd("SET")
                .arg(&name)
                .arg(TOKEN)
                .arg("NX")
                .arg("PX")
                .arg(30_000)
                .query_async(&mut c)
                .await
                .unwrap();
            assert!(taken.is_some());
            let _: i64 = s.key(&name).arg(TOKEN).invoke_async(&mut c).await.unwrap();
        }
    });
    let (c, u, run_id) = (connection.clone(), user.clone(), run.to_string());
    let batch = op(move |_| {
        let (mut c, u, run_id) = (c.clone(), u.clone(), run_id.clone());
        async move {
            let json = serde_json::to_string(&*u).unwrap();
            let mut pipeline = redis::pipe();
            for i in 0..BATCH {
                pipeline
                    .set(key(&run_id, "redis", "batch", i), &json)
                    .ignore();
            }
            let () = pipeline.query_async(&mut c).await.unwrap();
        }
    });
    vec![
        ("bucket_set", bucket_set),
        ("bucket_get", bucket_get),
        ("map_insert", map_insert),
        ("map_get", map_get),
        ("lock_unlock", lock_unlock),
        ("batch_100", batch),
    ]
}

struct Latency {
    p50_us: f64,
    p99_us: f64,
}

async fn latency(op: &Op) -> Latency {
    let end = Instant::now() + WARMUP;
    while Instant::now() < end {
        op(0).await;
    }
    let mut samples = Vec::new();
    let end = Instant::now() + MEASURE;
    while Instant::now() < end {
        let started = Instant::now();
        op(0).await;
        samples.push(started.elapsed());
    }
    samples.sort();
    let at = |q: f64| samples[((samples.len() - 1) as f64 * q) as usize].as_secs_f64() * 1e6;
    Latency {
        p50_us: at(0.5),
        p99_us: at(0.99),
    }
}

async fn throughput(op: &Op) -> f64 {
    let run = |duration: Duration| {
        let op = op.clone();
        async move {
            let end = Instant::now() + duration;
            let tasks: Vec<_> = (0..CONCURRENCY)
                .map(|slot| {
                    let op = op.clone();
                    tokio::spawn(async move {
                        let mut count = 0u64;
                        while Instant::now() < end {
                            op(slot).await;
                            count += 1;
                        }
                        count
                    })
                })
                .collect();
            let started = Instant::now();
            let mut total = 0;
            for task in tasks {
                total += task.await.unwrap();
            }
            total as f64 / started.elapsed().as_secs_f64().max(duration.as_secs_f64())
        }
    };
    run(WARMUP).await;
    run(MEASURE).await
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let url =
        std::env::var("REDISSUN_BENCH_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let out =
        std::env::var("REDISSUN_BENCH_OUT").unwrap_or_else(|_| "benches/results/rust.json".into());
    let only = std::env::var("REDISSUN_BENCH_ONLY").ok();
    let run = uuid_like();

    let libraries: Vec<(&str, Ops)> = vec![
        ("redissun", redissun_ops(&url, &run).await),
        ("fred", fred_ops(&url, &run).await),
        ("redis-rs", redis_rs_ops(&url, &run).await),
    ];

    let mut results = Vec::new();
    for (library, ops) in &libraries {
        for (name, operation) in ops {
            if only.as_deref().is_some_and(|only| !name.contains(only)) {
                continue;
            }
            let latency = latency(operation).await;
            let per_second = if *name == "batch_100" {
                None
            } else {
                Some(throughput(operation).await)
            };
            eprintln!(
                "{library:9} {name:12} p50 {:8.1} us  p99 {:8.1} us  {}",
                latency.p50_us,
                latency.p99_us,
                per_second.map_or(String::new(), |ops| format!("{ops:9.0} ops/s"))
            );
            results.push(json!({
                "library": library,
                "operation": name,
                "p50_us": latency.p50_us,
                "p99_us": latency.p99_us,
                "ops_per_second": per_second,
            }));
        }
    }

    let document = json!({
        "workload": {
            "payload": "JSON object of about 80 bytes",
            "concurrency": CONCURRENCY,
            "batch_commands": BATCH,
            "measure_seconds": MEASURE.as_secs(),
        },
        "results": results,
    });
    if let Some(parent) = std::path::Path::new(&out).parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(&out, serde_json::to_string_pretty(&document).unwrap()).unwrap();
    eprintln!("wrote {out}");
}

fn uuid_like() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{nanos:x}")
}
