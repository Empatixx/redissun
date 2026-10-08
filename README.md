# redissun

Shared objects on Redis for Rust. It is inspired by [Redisson](https://github.com/redisson/redisson).

[![CI](https://github.com/Empatixx/redissun/actions/workflows/ci.yml/badge.svg)](https://github.com/Empatixx/redissun/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/redissun.svg)](https://crates.io/crates/redissun)
[![docs.rs](https://img.shields.io/docsrs/redissun)](https://docs.rs/redissun)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

With redissun you use a `Map`, a `Bucket` or a `Lock` in your code. The data lives in Redis, so every program that uses the same name sees the same object. It works with tokio, and it is easy to use.

## Install

```bash
cargo add redissun
cargo add tokio --features full
cargo add serde --features derive
```

Or in `Cargo.toml`:

```toml
[dependencies]
redissun = "0.1"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

## Quick start

```rust
use redissun::Client;

#[tokio::main]
async fn main() -> redissun::Result<()> {
    let client = Client::builder().url("redis://127.0.0.1:6379").build().await?;

    let users = client.map::<String, String>("users");
    users.insert("jirka".into(), "Jirka".into()).await?;
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
| `Map` | `RMap` | hash |
| `Lock` | `RLock` | hash and pub/sub |

### Bucket

One value under one key.

```rust
let bucket = client.bucket::<String>("greeting");
bucket.set(&"hello".to_string()).await?;
let value = bucket.get().await?;
```

Other methods: `set_ex` (with a time limit), `set_nx` (only if empty), `get_set`, `get_del`, `compare_and_set`.

### Map

Works like a `HashMap`. The names are the same: `insert`, `get`, `remove`, `contains_key`, `len`, `is_empty`, `clear`.

```rust
let users = client.map::<String, User>("users");
users.insert("jirka".into(), user).await?;
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

### Common methods

`Bucket`, `Map` and `Lock` all have the `Object` methods: `name`, `del`, `exists`, `rename`, `expire`, `ttl`, `persist`. Add `use redissun::Object;` to call them.

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

MIT. See [LICENSE](LICENSE).
