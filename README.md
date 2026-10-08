# redgrid

Distributed objects on Redis for Rust, closely inspired by [Redisson](https://github.com/redisson/redisson).

[![crates.io](https://img.shields.io/crates/v/redgrid.svg)](https://crates.io/crates/redgrid)
[![docs.rs](https://img.shields.io/docsrs/redgrid)](https://docs.rs/redgrid)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Named objects that live in Redis and behave like their in-process counterparts, shared by every process that connects to the same server. Async on tokio, values through serde, atomic operations through Lua.

## Quick start

```rust
use redgrid::Client;

#[tokio::main]
async fn main() -> redgrid::Result<()> {
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

## Objects

| redgrid | Redisson | Redis type | Since |
|---|---|---|---|
| `Bucket` | `RBucket` | string | 0.1 |
| `Map` | `RMap` | hash | 0.1 |
| `Lock` | `RLock` | hash and pub/sub | 0.1 |

More objects (`List`, `Set`, `AtomicLong`, `Topic`, `Semaphore`, `RateLimiter`, `MapCache`) ship one per release.

## Design

- Method names follow `std` and `tokio` where an equivalent exists (`insert`, `get`, `len`, `lock`, `try_lock`) and Redis command names otherwise (`set_nx`, `incr_by`, `expire`, `ttl`).
- `Lock` is reentrant, kept alive by a watchdog, and wakes waiters through pub/sub instead of polling.
- The Redis client is an implementation detail; no `fred` type appears in the public API.

## Requirements

Redis or Valkey 6.2 or newer.

## Documentation

- Guides and migration table from Redisson: the `website/` directory, published with GitHub Pages.
- API reference: [docs.rs/redgrid](https://docs.rs/redgrid).
- Examples: [`examples/`](examples).

## Development

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test
```

The integration tests start Redis through testcontainers, so Docker must be reachable (set `DOCKER_HOST` for Colima). To use a server you already run, set `REDGRID_TEST_REDIS_URL=redis://localhost:6379`.

## License

MIT, see [LICENSE](LICENSE).
