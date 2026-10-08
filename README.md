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
redissun = "0.2"
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

| redissun | Redisson | Stored in Redis as |
|---|---|---|
| `Bucket` | `RBucket` | string |
| `HashMap` | `RMap` | hash |
| `Vec` | `RList` | list |
| `VecDeque` | `RDeque` | list |
| `HashSet` | `RSet` | set |
| `Lock` | `RLock` | hash and pub/sub |
| `AtomicI64` | `RAtomicLong` | string |
| `Semaphore` | `RSemaphore` | string and pub/sub |
| `CountDownLatch` | `RCountDownLatch` | string and pub/sub |
| `RateLimiter` | `RRateLimiter` | hash, string and sorted set |

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
- `lock_for(wait)` gives up after `wait`.
- `lock_with(options)` lets you choose both wait time and lease.

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
```

### HashSet

A set of unique values.

```rust
let tags = client.hash_set::<String>("tags");
tags.insert("rust").await?;
let found = tags.contains("rust").await?;
```

Other methods: `remove`, `contains_many`, `pop`, `random`, `move_to`, `union`, `intersection`, `difference`, `iter`.

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

## Settings

```rust
let client = Client::builder()
    .url("redis://127.0.0.1:6379")
    .pool_size(8)
    .lock_lease(Duration::from_secs(30))
    .connect_timeout(Duration::from_secs(10))
    .build()
    .await?;
```

| Setting | Default | Meaning |
|---|---|---|
| `url` | none, required | Redis address |
| `pool_size` | 4 | number of connections |
| `lock_lease` | 30 s | lease for locks without their own lease |
| `connect_timeout` | 10 s | how long `build` waits for the first connection |
| `codec` | JSON | how values are turned into bytes |

The client reconnects by itself after a lost connection. When the last clone of the client is dropped, its connections are closed.

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

The tests start Redis with Docker (testcontainers). With Colima, set `DOCKER_HOST` to its socket. To use a Redis you already run, set `REDISSUN_TEST_REDIS_URL=redis://localhost:6379`.

## License

Apache-2.0. See [LICENSE](LICENSE).
