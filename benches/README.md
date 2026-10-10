# Benchmarks

`compare.rs` runs the same operations on redissun, [redis-rs](https://crates.io/crates/redis) and [fred](https://crates.io/crates/fred). `redisson/` runs them on [Redisson](https://github.com/redisson/redisson), the library redissun is modelled on. `plot.py` draws the charts.

## What is measured

| Operation | redissun | redis-rs and fred (by hand) | Redisson |
|---|---|---|---|
| Bucket set, get | `Bucket::set`, `get` | `SET`, `GET` plus `serde_json` | `RBucket.set`, `get` |
| HashMap insert | `HashMap::insert`, which returns the replaced value | a Lua script with `HGET` and `HSET`, the same as redissun sends | `RMap.put` |
| HashMap get | `HashMap::get` | `HGET` plus `serde_json` | `RMap.get` |
| Lock and unlock | `Lock::lock`, `unlock` | `SET NX PX` and a Lua compare-and-delete | `RLock.lock`, `unlock` |
| Batch of 100 sets | `Batch` | a pipeline | `RBatch` |

- The value is a small JSON object of about 80 bytes. Every library stores it as JSON.
- **Latency** is the median (p50) of one call from one task, after a warm-up. The JVM gets a longer warm-up.
- **Throughput** is the number of operations per second from 64 tasks (64 threads for Redisson), each on its own key, so no operation waits for another.
- Every library runs with its default settings. redissun uses 4 connections, fred is set to 4, redis-rs uses its one multiplexed connection and Redisson uses its default pool of 64.
- Redis runs on the same machine, so the numbers show the cost of the client, not the network. On a real network one round trip is 100 to 1000 times longer than the differences you see here.
- A redissun lock does more than the hand-written one: it is reentrant, wakes waiting owners through pub/sub and renews its lease with a watchdog. Its extra time is the price of those features.
- A chart shows the median of the runs in `results/`. Run the benchmarks several times on a quiet machine; the numbers move by about 10 % on a busy laptop.

## Run

```bash
export REDISSUN_BENCH_URL=redis://127.0.0.1:6379

# Rust: redissun, fred and redis-rs. Writes benches/results/rust.json
cargo bench --bench compare

# Java: Redisson. Needs a JDK 21 or newer and Maven. Writes benches/results/redisson.json
(cd benches/redisson && mvn -q compile exec:java -Dexec.args="$REDISSUN_BENCH_URL ../results/redisson.json")

# the charts, in website/public/bench/
python3 benches/plot.py
```

Set `REDISSUN_BENCH_OUT` to write the Rust results to another file, and `REDISSUN_BENCH_ONLY=bucket_get` to run only the operations whose name contains that text. Use a Redis you can throw away: the benchmark writes keys named `bench:*`.
