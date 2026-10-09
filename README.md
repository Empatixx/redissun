<p align="center">
  <img src="https://raw.githubusercontent.com/Empatixx/redissun/main/website/public/logo.png" alt="redissun: a crab eating the letter R" width="180">
</p>

# redissun

Shared objects on Redis for Rust. It is inspired by [Redisson](https://github.com/redisson/redisson).

[![CI](https://github.com/Empatixx/redissun/actions/workflows/ci.yml/badge.svg)](https://github.com/Empatixx/redissun/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/redissun.svg)](https://crates.io/crates/redissun)
[![docs.rs](https://img.shields.io/docsrs/redissun)](https://docs.rs/redissun)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

With redissun you use a `HashMap`, a `Bucket` or a `Lock` in your code. The data lives in Redis, so every program that uses the same name sees the same object. It works with tokio, and it is easy to use.

## Install

```bash
cargo add redissun
cargo add tokio --features full
cargo add serde --features derive
```

Or in `Cargo.toml`:

```toml
[dependencies]
redissun = "0.16"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

## Quick start

```rust
use redissun::Client;

#[tokio::main]
async fn main() -> redissun::Result<()> {
    let client = Client::builder().url("redis://127.0.0.1:6379").build().await?;

    let users = client.hash_map::<String, String>("users");
    users.insert("jirka", "Jirka").await?;
    println!("{:?}", users.get("jirka").await?);

    let lock = client.lock("order:42");
    let guard = lock.lock().await?;
    guard.unlock().await?;

    Ok(())
}
```

`Client` is cheap to clone. All clones share the same connections.

## Objects

| Object | Stored in Redis as | Status |
|---|---|---|
| `Bucket` | string | stable |
| `HashMap` | hash | stable |
| `Vec` | list | stable |
| `VecDeque` | list | stable |
| `HashSet` | set | stable |
| `LocalCachedMap` | hash and pub/sub | experimental |
| `Lock` | hash and pub/sub | stable |
| `RwLock` | hash, set, strings and pub/sub | stable |
| `FairLock` | hash, list, sorted set and pub/sub | stable |
| `SortedSet` | sorted set | stable |
| `FencedLock` | hash, string and pub/sub | stable |
| `MultiLock` | the locks it holds | stable |
| `DelayedQueue` | list, sorted set and pub/sub | experimental |
| `BitSet` | string | stable |
| `HyperLogLog` | HyperLogLog | stable |
| `BloomFilter` | string and hash | experimental |
| `HashMapCache` | hash and sorted set | experimental |
| `HashSetCache` | sorted set | experimental |
| `Topic` | pub/sub | stable |
| `Stream` | stream | experimental |
| `Geo` | sorted set | stable |
| `AtomicI64` | string | stable |
| `Semaphore` | string and pub/sub | stable |
| `CountDownLatch` | string and pub/sub | experimental |
| `RateLimiter` | hash, string and sorted set | stable |
| `Batch` | the commands it holds | experimental |

Every object follows Redisson's logic and is tested with Redisson's own tests, ported to Rust. **Stable** objects are also thin wrappers over Redis commands or pass the fault-injection tests (`tests/chaos.rs`) and the Sentinel and Cluster failover tests. **Experimental** objects are not yet tested under faults, and their API may still change.

### Bucket

One value under one key.

```rust
let bucket = client.bucket::<String>("greeting");
bucket.set("hello").await?;
let value = bucket.get().await?;
```

Other methods: `set_ex` (with a time limit), `set_nx` (only if empty), `get_set`, `get_del`, `compare_and_set`.

### HashMap

Works like a `HashMap`. The names are the same: `insert`, `get`, `remove`, `contains_key`, `len`, `is_empty`, `clear`.

```rust
let users = client.hash_map::<String, User>("users");
users.insert("jirka", &user).await?;
let found = users.get("jirka").await?;
```

More methods: `extend`, `get_many`, `insert_nx`, `incr_by`, `incr_by_float`. To read all entries use `iter`, `keys` or `values`. They return a `Stream` and read the map in small pages.

### Lock

A lock that many programs can share. The same owner can take it again (it is reentrant). It is like `tokio::sync::Mutex`.

```rust
let guard = client.lock("order:42").lock().await?;
guard.unlock().await?;
```

- `lock()` waits for the lock.
- `try_lock()` gives up at once if the lock is taken.
- `lock().timeout(wait)` gives up after `wait` and gives `None`.
- `lock().lease(lease)` sets the lease yourself. You can add `.timeout(wait)` too.

A lock has a lease. A watchdog renews it while you hold the lock. If your program crashes, the lock frees itself when the lease ends. Waiting programs wake up as soon as the lock is released. They do not poll.

### Vec

A list with index access. It works like a `Vec`. Use it as `redissun::Vec`, not with a glob import.

```rust
let names = client.vec::<String>("names");
names.push("jirka").await?;
let first = names.get(0).await?;
```

Other methods: `pop`, `set`, `insert`, `remove`, `range`, `trim`, `position`, `contains`, `remove_value`, `remove_all`, `extend`, `iter`.

### VecDeque

A queue or stack. Add and remove at both ends.

```rust
let jobs = client.vec_deque::<String>("jobs");
jobs.push_back("send-email").await?;
let next = jobs.pop_front().await?;
let waited = jobs.pop_front_wait().timeout(Duration::from_secs(5)).await?;
```

`pop_front_wait` and `pop_back_wait` block until a value arrives. Add `.timeout(duration)` to wait at most that long.

### HashSet

A set of unique values.

```rust
let tags = client.hash_set::<String>("tags");
tags.insert("rust").await?;
let found = tags.contains("rust").await?;
```

Other methods: `remove`, `contains_many`, `pop`, `random`, `move_to`, `union`, `intersection`, `difference`, `iter`.

### HashMapCache

A `HashMap` where each entry can expire.

```rust
let sessions = client.hash_map_cache::<String, User>("sessions");
sessions.insert("abc", &user).ttl(Duration::from_secs(60)).await?;
```

Entries can also expire when nobody reads them (`.max_idle(duration)`). Time comes from the Redis server, and a background task deletes expired entries. `set_max_size` limits the size (LRU or LFU), and `events()` tells you about every change.

### HashSetCache

A set whose values expire.

```rust
let seen = client.hash_set_cache::<String>("seen");
seen.insert("request-1").ttl(std::time::Duration::from_secs(60)).await?;
```

### LocalCachedMap

A `HashMap` with a cache in your program. Other instances are told when an entry changes.

```rust
let users = client
    .local_cached_map::<String, String>("users")
    .eviction_policy(redissun::EvictionPolicy::Lru)
    .cache_size(10_000)
    .build()
    .await?;
users.insert("jirka", "Jirka").await?;
let name = users.get("jirka").await?;
```

### Batch

Send many commands in one round trip.

```rust
let batch = client.batch();
let visits = batch.atomic_i64("visits").incr();
let user = batch.hash_map::<String, String>("users").get("jirka");
batch.execute().await?;
let (visits, user) = (visits.await?, user.await?);
```

### Topic

Send messages to every program that listens.

```rust
let news = client.topic::<String>("news");
let mut subscriber = news.subscribe().await?;
news.publish("hello").await?;
let message = subscriber.recv().await?;
```

### Stream

An append-only log with consumer groups, for messages that must not get lost.

```rust
let log = client.stream::<String, String>("orders");
log.create_group("workers", &redissun::StreamId::zero()).await?;
log.add([("id", "42")]).await?;
for entry in log.read_group_wait("workers", "w1", None).await? {
    log.ack("workers", &[entry.id]).await?;
}
```

### Geo

Members with a position, and searches around a point.

```rust
use redissun::{GeoPoint, GeoUnit};

let shops = client.geo::<String>("shops");
shops.add("prague", GeoPoint { longitude: 14.4378, latitude: 50.0755 }).await?;
let near = shops
    .radius(GeoPoint { longitude: 14.4, latitude: 50.1 }, 5.0, GeoUnit::Kilometers, None)
    .await?;
```

### RwLock

Many readers or one writer.

```rust
let lock = client.rw_lock("config");
let read = lock.read().await?;
read.unlock().await?;
```

### FairLock

A lock that serves waiters in the order they arrived.

```rust
let lock = client.fair_lock("jobs");
let guard = lock.lock().await?;
guard.unlock().await?;
```

### FencedLock

A lock that gives every new owner a higher number. A resource can refuse a write with an older number.

```rust
let lock = client.fenced_lock("report");
let guard = lock.lock().await?;
let token = guard.fencing_token();
```

### MultiLock

Several locks as one.

```rust
let multi = client.multi_lock([client.lock("a"), client.lock("b")])?;
let guard = multi.lock().await?;
guard.unlock().await?;
```

### DelayedQueue

Values that show up in a queue after a delay.

```rust
let jobs = client.vec_deque::<String>("jobs");
let delayed = client.delayed_queue(&jobs);
delayed.push("send-email", std::time::Duration::from_secs(60)).await?;
let job = jobs.pop_front_wait().await?;
```

### BitSet, HyperLogLog and BloomFilter

Compact ways to remember many things.

```rust
let online = client.bit_set("online");
online.set(42, true).await?;

let visitors = client.hyper_log_log::<String>("visitors");
visitors.insert("ann").await?;
let about = visitors.count().await?;

let seen = client.bloom_filter::<String>("seen");
seen.try_init(1_000_000, 0.01).await?;
if seen.insert("url").await? { /* certainly new */ }
```

### SortedSet

Values ordered by a score, for example a leaderboard.

```rust
let board = client.sorted_set::<String>("scores");
board.insert("ann", 12.0).await?;
board.add_score("ann", 3.0).await?;
let top = board.rev_range(0..3).await?;
```

### AtomicI64

A shared counter. A missing counter reads as 0.

```rust
let visits = client.atomic_i64("visits");
let total = visits.incr().await?;
```

### Semaphore

Limits how many programs do something at the same time.

```rust
let semaphore = client.semaphore("workers");
semaphore.try_set_permits(3).await?;

let permits = semaphore.acquire(1).await?;
// do the work
permits.release().await?;
```

`Permits` returns the permits when you drop it. Call `forget()` to keep them taken and `semaphore.release(n)` to give them back later.

### CountDownLatch

One side counts down, the other side waits for zero.

```rust
let latch = client.count_down_latch("ready");
latch.try_set_count(3).await?;

latch.count_down().await?;   // in each worker
latch.wait().await?;         // in the coordinator
```

### RateLimiter

At most `rate` permits in any window of `interval`. The window slides, it does not reset.

```rust
use redissun::RateType;
use std::time::Duration;

let limiter = client.rate_limiter("api");
limiter.try_set_rate(RateType::Overall, 100, Duration::from_secs(1)).await?;

if limiter.try_acquire(1).await? {
    // allowed
}
limiter.acquire(1).await?; // or wait until it is allowed
```

`RateType::Overall` shares the limit between all clients. `RateType::PerClient` gives each client its own.

### Common methods

Every object has the `Object` methods: `name`, `del`, `exists`, `rename`, `expire`, `ttl`, `persist`. Add `use redissun::Object;` to call them.

## Limits

redissun follows Redisson's business logic, so it shares Redisson's limits.

- A lock lives on one Redis master. On Sentinel and Cluster, taking or releasing a lock waits until a replica has it (`WAIT`, like Redisson's `checkLockSyncedSlaves`), but a failover can still lose a lock in rare cases. Use `FencedLock` and check the token where correctness matters.
- Commands that Redisson never repeats are sent once, for example taking a lock or a permit. When the connection drops during such a call, it returns an error and nobody knows whether Redis ran it. A lock then holds until its lease ends; semaphore permits can be lost.
- Other commands are sent again up to `retry_attempts` times (4 by default) when their connection fails, so a write such as `incr` can be applied twice.
- `RateLimiter` counts its window with the clients' clocks, like Redisson. Clocks that differ, or requests delayed by the network, can let more permits through within one second, although the long-run rate holds.
- `MultiLock` owners that list the same locks in different orders can block each other until their timeout. List the locks in the same order everywhere.
- `VecDeque` pops and `Topic` messages are delivered at most once.

## Settings

```rust
use redissun::DelayStrategy;

let client = Client::builder()
    .url("redis://127.0.0.1:6379")
    .pool_size(8)
    .timeout(Duration::from_secs(3))
    .retry_attempts(4)
    .retry_delay(DelayStrategy::EqualJitter {
        base: Duration::from_secs(1),
        max: Duration::from_secs(2),
    })
    .client_name("orders")
    .build()
    .await?;
```

The names and defaults follow Redisson's config.

| Setting | Default | Redisson | Meaning |
|---|---|---|---|
| `url` | none, required | `address` | Redis address. `redis-sentinel://` and `redis-cluster://` URLs work too. |
| `pool_size` | 4 | `connectionPoolSize` | number of connections |
| `database` | from the URL, else 0 | `database` | database number |
| `client_name` | none | `clientName` | name of every connection in `CLIENT LIST` |
| `username`, `password` | from the URL | `username`, `password` | credentials for `AUTH`. They replace the ones in the URL. |
| `credentials_resolver` | none | `credentialsResolver` | async function that returns the `Credentials` for each new connection |
| `credentials_refresh_interval` | none | `CredentialsResolver.nextRenewal` | how often open connections ask the resolver again and send a new `AUTH` |
| `timeout` | 3 s | `timeout` | how long a command waits for its reply. Blocking pops and reads are not limited. 0 waits forever. |
| `connect_timeout` | 10 s | `connectTimeout` | how long `build` and one connection attempt may take |
| `retry_attempts` | 4 | `retryAttempts` | how often a command is sent again after its connection failed |
| `retry_delay` | `EqualJitter` 1 s to 2 s | `retryDelay` | pause between two retries of a batch or a lock write |
| `reconnection_delay` | `EqualJitter` 100 ms to 10 s | `reconnectionDelay` | pause between two reconnection attempts |
| `ping_connection_interval` | 30 s | `pingConnectionInterval` | how often each connection is checked with `PING`; 0 turns it off |
| `keep_alive` | false | `keepAlive` | TCP keep-alive, with `tcp_keep_alive_idle` and `tcp_keep_alive_interval` |
| `tcp_no_delay` | true | `tcpNoDelay` | `TCP_NODELAY` |
| `lock_lease` | 30 s | `lockWatchdogTimeout` | lease for locks without their own lease |
| `check_lock_synced_replicas` | true | `checkLockSyncedSlaves` | fail a lock call when no replica got the write |
| `replicas_sync_timeout` | 1 s | `slavesSyncTimeout` | how long a lock write waits for replicas |
| `fair_lock_wait_timeout` | 5 min | `fairLockWaitTimeout` | how long a `FairLock` waiter keeps its place |
| `eviction_interval` | 5 s | `minCleanUpDelay` | first pause of the cache clean-up task |
| `codec` | JSON | `codec` | how values are turned into bytes |

`DelayStrategy` has Redisson's four strategies: `Constant`, `EqualJitter`, `FullJitter` and `DecorrelatedJitter`.

Commands that Redisson sends only once, such as taking a lock, are never retried, whatever `retry_attempts` says.

The client reconnects by itself after a lost connection. The connection for pub/sub is opened only when an object first needs it, for example a lock that has to wait or a `Topic` subscriber. When the last clone of the client is dropped, its connections are closed.

## Codecs

A codec turns values into the bytes stored in Redis. The default is JSON, so the string `jirka` is stored as `"jirka"` with the quotes.

| Codec | Redisson | Stores |
|---|---|---|
| `JsonCodec` | `JsonJacksonCodec` | any serde value as JSON (default) |
| `StringCodec` | `StringCodec` | strings as plain UTF-8 text, numbers and bools as their text |
| `BytesCodec` | `ByteArrayCodec` | `Vec<u8>` and other bytes as they are |

`client.with_codec(codec)` gives a client with another codec that shares the same connections, like Redisson's `getStream(name, StringCodec.INSTANCE)`:

```rust
use redissun::StringCodec;

let events = client.with_codec(StringCodec).stream::<String, String>("events");
events.add([("payload", r#"{"id":42}"#)]).await?; // XADD events * payload {"id":42}
```

To use a codec for every object, set it on the builder: `Client::builder().codec(StringCodec)`.

## TLS

Turn on the `tls-rustls` feature and use a `rediss://` URL. It works for a single server, Sentinel (`rediss-sentinel://`) and Cluster (`rediss-cluster://`).

```toml
redissun = { version = "0.16", features = ["tls-rustls"] }
```

```rust
let client = Client::builder()
    .url("rediss://redis.example.com:6380")
    .tls_ca_file("/etc/redis/ca.pem")                                 // like sslTruststore
    .tls_client_auth_files("/etc/redis/client.pem", "/etc/redis/client.key") // like sslKeystore
    .build()
    .await?;
```

| Setting | Redisson | Meaning |
|---|---|---|
| `tls_ca_pem`, `tls_ca_file` | `sslTruststore` | trust only these CA certificates. Without them the system's certificates are trusted. |
| `tls_client_auth_pem`, `tls_client_auth_files` | `sslKeystore` | client certificate and key for servers with `tls-auth-clients yes` |
| `tls_verification` | `sslVerificationMode` | `TlsVerification::Strict` (default) checks the CA and the host name, `CaOnly` checks only the CA, `None` checks nothing |

`tls-rustls` uses rustls with the `ring` crypto provider. If your program already uses rustls with `aws-lc-rs`, use the `tls-rustls-aws-lc` feature instead. When your program installs a default rustls `CryptoProvider`, redissun uses that one.

## Errors

Every call returns `redissun::Result`. The error is `redissun::Error`. It can grow in future versions, so add a `_` case when you match it.

## Requirements

- Redis or Valkey 6.2 or newer.
- Rust 2021 edition.

## Documentation

- Guides: https://empatixx.github.io/redissun/
- API reference: https://docs.rs/redissun
- Examples: the [`examples/`](examples) folder.
- Coming from Redisson? See the migration page in the guides.

## Development

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test
```

Slower tests are marked `#[ignore]`. They need Docker and start their own Redis, a Sentinel setup and a Cluster:

```bash
cargo test --test chaos --test sentinel --test cluster -- --ignored
```

The tests start Redis with Docker (testcontainers). With Colima, set `DOCKER_HOST` to its socket. To use a Redis you already run, which is faster, set `REDISSUN_TEST_REDIS_URL=redis://localhost:6379`.

## License

Apache-2.0. See [LICENSE](LICENSE).
