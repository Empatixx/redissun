<p align="center">
  <img src="https://raw.githubusercontent.com/Empatixx/redissun/main/website/public/logo.png" alt="redissun: a crab eating the letter R" width="180">
</p>

# redissun

[Redisson](https://github.com/redisson/redisson) for Rust: a Redis library with std-style collections, distributed locks, caches and streams.

[![CI](https://github.com/Empatixx/redissun/actions/workflows/ci.yml/badge.svg)](https://github.com/Empatixx/redissun/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/redissun.svg)](https://crates.io/crates/redissun)
[![docs.rs](https://img.shields.io/docsrs/redissun)](https://docs.rs/redissun)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

With redissun you use a `HashMap`, a `VecDeque` or a `Lock` in your code, with the names and methods you know from `std`. The data lives in Redis, so every program that uses the same name sees the same object. Every call is async on tokio.

It uses the same data layout and Lua scripts as Redisson. It runs on [redis-rs](https://crates.io/crates/redis) and talks to a single Redis server, Sentinel or Redis Cluster, with TLS, retries and reads from replicas. Valkey works too.

## Install

```bash
cargo add redissun
cargo add tokio --features full
cargo add serde --features derive
```

Or in `Cargo.toml`:

```toml
[dependencies]
redissun = "0.22"
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
| [`Bucket`](https://empatixx.github.io/redissun/docs/objects/bucket) | string | stable |
| [`HashMap`](https://empatixx.github.io/redissun/docs/objects/hash-map) | hash | stable |
| [`Vec`](https://empatixx.github.io/redissun/docs/objects/vec) | list | stable |
| [`VecDeque`](https://empatixx.github.io/redissun/docs/objects/vec-deque) | list | stable |
| [`HashSet`](https://empatixx.github.io/redissun/docs/objects/hash-set) | set | stable |
| [`LocalCachedMap`](https://empatixx.github.io/redissun/docs/objects/local-cached-map) | hash and pub/sub | stable |
| [`Lock`](https://empatixx.github.io/redissun/docs/objects/lock) | hash and pub/sub | stable |
| [`RwLock`](https://empatixx.github.io/redissun/docs/objects/rw-lock) | hash, set, strings and pub/sub | stable |
| [`FairLock`](https://empatixx.github.io/redissun/docs/objects/fair-lock) | hash, list, sorted set and pub/sub | stable |
| [`FencedLock`](https://empatixx.github.io/redissun/docs/objects/fenced-lock) | hash, string and pub/sub | stable |
| [`MultiLock`](https://empatixx.github.io/redissun/docs/objects/multi-lock) | the locks it holds | stable |
| [`SortedSet`](https://empatixx.github.io/redissun/docs/objects/sorted-set) | sorted set | stable |
| [`DelayedQueue`](https://empatixx.github.io/redissun/docs/objects/delayed-queue) | list, sorted set and pub/sub | stable |
| [`BitSet`](https://empatixx.github.io/redissun/docs/objects/bit-set) | string | stable |
| [`HyperLogLog`](https://empatixx.github.io/redissun/docs/objects/hyper-log-log) | HyperLogLog | stable |
| [`BloomFilter`](https://empatixx.github.io/redissun/docs/objects/bloom-filter) | string and hash | stable |
| [`HashMapCache`](https://empatixx.github.io/redissun/docs/objects/hash-map-cache) | hash and sorted set | stable |
| [`HashSetCache`](https://empatixx.github.io/redissun/docs/objects/hash-set-cache) | sorted set | stable |
| [`Topic`](https://empatixx.github.io/redissun/docs/objects/topic) | pub/sub | stable |
| [`Stream`](https://empatixx.github.io/redissun/docs/objects/stream) | stream | stable |
| [`Geo`](https://empatixx.github.io/redissun/docs/objects/geo) | sorted set | stable |
| [`AtomicI64`](https://empatixx.github.io/redissun/docs/objects/atomic-i64) | string | stable |
| [`AtomicF64`](https://empatixx.github.io/redissun/docs/objects/atomic-f64) | string | experimental |
| [`Adder`](https://empatixx.github.io/redissun/docs/objects/adder) | local memory and pub/sub | experimental |
| [`IdGenerator`](https://empatixx.github.io/redissun/docs/objects/id-generator) | strings | experimental |
| [`Semaphore`](https://empatixx.github.io/redissun/docs/objects/semaphore) | string and pub/sub | stable |
| [`CountDownLatch`](https://empatixx.github.io/redissun/docs/objects/count-down-latch) | string and pub/sub | stable |
| [`RateLimiter`](https://empatixx.github.io/redissun/docs/objects/rate-limiter) | hash, string and sorted set | stable |
| [`Batch`](https://empatixx.github.io/redissun/docs/objects/batch) | the commands it holds | experimental |
| [`Script`](https://empatixx.github.io/redissun/docs/guides/scripts) | Lua scripts | stable |
| [`Function`](https://empatixx.github.io/redissun/docs/guides/scripts) | Redis functions | experimental |

Every object follows Redisson's logic and is tested with Redisson's own tests, ported to Rust. **Stable** objects behave like their Redisson counterpart in every ported test; the locks, `Semaphore`, `AtomicI64`, `RateLimiter` and `VecDeque` also pass the fault-injection tests. **Experimental** objects still differ from Redisson in a known way, and their API may still change. Every object also has the common methods `name`, `del`, `exists`, `rename`, `expire`, `ttl` and `persist` (add `use redissun::Object;`).

## Benchmarks

redissun runs the same operations as hand-written code on [redis-rs](https://crates.io/crates/redis) and [fred](https://crates.io/crates/fred), and as [Redisson](https://github.com/redisson/redisson) in Java. Redis runs on the same machine, so the charts show what the client costs, not the network.

<p align="center">
  <img src="website/public/bench/latency.svg" alt="Latency of one operation: redissun, redis-rs, fred and Redisson are within 3 microseconds for get and set; a redissun lock is slower, its batch is second after redis-rs">
</p>
<p align="center">
  <img src="website/public/bench/throughput.svg" alt="Throughput with 64 tasks: redissun is 6 to 15 percent below redis-rs and as fast as fred; Redisson reaches about a third">
</p>

- **One call:** a get, a set and a map insert take about 75 µs on every library. That is the Redis round trip on this machine, and the client adds under 3 µs.
- **64 tasks:** redissun is 6 to 15 % below the hand-written redis-rs, whose single multiplexed connection is the leanest setup, and as fast as fred. Redisson's blocking API with 64 threads reaches about a third.
- **Lock:** a redissun lock is slower than a bare `SET NX` plus a Lua unlock (170 against 158 µs), because it is reentrant, wakes waiters through pub/sub and renews its lease. Against Redisson it is faster (170 against 206 µs) and handles more than twice as many locks under load.
- **Batch:** 100 queued sets take 159 µs, after a hand-written redis-rs pipeline (136 µs) and ahead of Redisson (176 µs) and fred (229 µs). redissun writes each node's commands as one pipeline, like redis-rs, and adds a connection taken for the batch and a handle for every reply.

Median of five runs (three for Redisson) on an Apple M5 Pro with Redis 8.10 on the same machine, which was not idle. How it was measured, what the hand-written code does, and how to run it yourself: [Benchmarks](https://empatixx.github.io/redissun/docs/guides/benchmarks).

## Documentation

- [Getting started](https://empatixx.github.io/redissun/docs/getting-started/quick-start) and [one page per object](https://empatixx.github.io/redissun/docs)
- [Configuration](https://empatixx.github.io/redissun/docs/guides/configuration): every setting and its default, Redisson's names, TLS, Sentinel and Cluster
- [Codecs](https://empatixx.github.io/redissun/docs/guides/codecs), [Scripts and Functions](https://empatixx.github.io/redissun/docs/guides/scripts)
- [Limitations](https://empatixx.github.io/redissun/docs/guides/limitations): read this before you rely on a lock for correctness
- [Coming from Redisson?](https://empatixx.github.io/redissun/docs/guides/redisson-migration)
- [API reference](https://docs.rs/redissun) and the [`examples/`](examples) folder

## Requirements

- Redis or Valkey 6.2 or newer. `Function` needs 7.0.
- Rust 2021 edition.

## Development

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

The tests start Redis with Docker (testcontainers). With Colima, set `DOCKER_HOST` to its socket or put `docker.host` in `~/.testcontainers.properties`. To use a Redis you already run, which is faster, set `REDISSUN_TEST_REDIS_URL=redis://localhost:6379`.

No test is skipped: the Sentinel, Cluster and fault-injection tests start their own topology in Docker and run with every `cargo test`, as Redisson's tests do. The whole run takes a few minutes. The TLS tests need a TLS feature: `cargo test --features tls-rustls --test tls`. To reproduce the benchmarks, see [`benches/README.md`](benches/README.md).

## License

Apache-2.0. See [LICENSE](LICENSE).
