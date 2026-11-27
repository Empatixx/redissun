# 0c: Loader, writer and store mode for every map — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `HashMap`, `HashMapCache` and `LocalCachedMap` get Redisson's `MapOptions`/`LocalCachedMapOptions` store features: a `MapLoader` for read-through and `load_all`, a `MapWriter` called write-through or write-behind with Redisson's batching, delay and retries, and for `LocalCachedMap` the store mode, the cache provider and the expiration event policy. Each map method consults the loader or calls the writer exactly where Redisson 4.8.0's `RedissonMap`/`RedissonMapCache`/`RedissonLocalCachedMap` do.

**Architecture:** One shared core, `src/collections/map/store.rs`, holds the public traits, `WriteMode`, the options every map builder takes (through a `store_options!()` macro, like `per_key_objects!()`), and a private `Store<K, V, C>` that the maps keep as `Option<Arc<Store>>`. The maps tell it what they did in encoded form (`written(&[(k, v)])`, `deleted(&[k])`), and it decodes the entries and calls the writer with retries, or pushes them to the write-behind queue in Redis. Read-through runs Redisson's `loadValue` (per-key lock, check again, load, store with the map's plain put) in `src/collections/map/load.rs`, through the private `Target` enum that `Entry` already uses, with a new `Local` variant. Write-behind (`src/collections/map/write_behind.rs`) is Redisson's `MapWriteBehindTask`: a Redis list per map, polled every `delay` by a background task, with batches of `batch_size`. `MapCacheNative` (#1) and `LocalCachedMapCache` (#4) hook in later the same way: `store_options!()` in their builder, an `Option<Arc<Store>>` field, a `Target` variant. This plan adds no code for them.

**Tech Stack:** Rust (edition 2021, return-position `impl Trait` in traits), tokio, redis-rs, futures, testcontainers (Redis 7.4, 8.8, Cluster); optional `moka` (Task 9).

**Spec:** `docs/superpowers/specs/2026-11-26-redisson-4x-objects-design.md` (section "0. Full map parity first", item 0c) and `docs/superpowers/specs/2026-11-27-rust-idiomatic-api.md` (naming rules; it supersedes the "API conventions" naming lines of the first spec).

**Branch:** `feat/map-loader-writer`, from `feat/map-cache-parity` (0b), or from `main` once 0b is merged.

## Owner decisions (2026-11-27, delegated: "nechám to na tobě")

1. Loader errors surface as `Error::Loader` (a `Result`, the Rust way). Redisson returns null; its test that pins null is ported with the Rust expectation and the difference goes in the PR and `limitations.mdx`.
2. The write-behind queue uses redissun's own key `{name}:redissun_write-behind-queue`; Redisson's queue holds Java-serialized tasks.
3. `moka` is added as an optional feature for `CacheProvider` (Task 9 stays).
4. `cacheSize(-1)`, `useObjectAsCacheKey`, `useTopicPattern` and `removeEmptyEvictionTask` stay out of 0c, each with a line in `limitations.mdx`.
5. `ExpirationEventPolicy` replaces the deprecated `useKeyEventsPattern` bool, with the old name as a doc alias.

## Decisions (mine, with the reason)

- **Async traits without a macro.** `MapLoader`/`MapWriter` declare `fn load(&self, key: &K) -> impl Future<Output = …> + Send`. Implementors write a plain `async fn` (the compiler checks that the future is `Send`). Traits with `impl Trait` returns are not dyn-compatible, so each one has a private, object-safe twin (`ErasedLoader`/`ErasedWriter`) that returns `BoxFuture`/`BoxStream`, with a blanket impl over the public trait. The maps store `Arc<dyn ErasedLoader<K, V>>`. Users write no `#[async_trait]` and depend on no macro crate, the public bounds have no private trait (`private_bounds` stays clean), and each call costs one `Box` only when a loader or writer is set. The unused `async-trait` dependency is removed. One trait replaces both Redisson's `MapLoader` and `MapLoaderAsync`, and one replaces both writer variants. A blocking store calls `tokio::task::spawn_blocking` inside its impl.
- **Builders on the factory.** `client.hash_map_builder::<K, V>(name)` and `client.hash_map_cache_builder::<K, V>(name)` return `HashMapBuilder`/`HashMapCacheBuilder`, and `client.local_cached_map(name)` keeps its builder. All three end in `.build().await -> Result<_>`, the same call `LocalCachedMapBuilder` already uses: options are checked there (`Error::Config`), and a write-behind task needs a running tokio runtime. `client.hash_map(name)`/`hash_map_cache(name)` are unchanged and equal `*_builder(name).build()` with no options. A `with_loader(..)` on the map was rejected: it mixes setup into the object, and every clone would have to restart write-behind.
- **Errors.** Two new `Error` variants: `Loader(#[source] BoxError)` ("map loader failed: …") and `Writer(#[source] BoxError)` ("map writer failed: …"). `pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>`. `Error` is `#[non_exhaustive]`, so the new variants do not break callers.
- **Write-through order is Redisson's:** Redis first, then the writer. If the writer still fails after its retries, the call returns `Error::Writer` and Redis keeps the change (Review Focus 3).
- **Write-behind lifecycle.** Redisson keeps the task until `destroy()` and does not flush on shutdown. Pending writes live in Redis, so whichever write-behind instance of that name polls next picks them up, in any process. redissun:
  - The task polls every `delay` while the client lives.
  - `flush().await` on the map drains the queue now and reports writer errors.
  - When the last handle of a built map is dropped, the task drains the queue once more and stops.
  - When the client is dropped, the task stops, and the queue stays in Redis for the next instance.
  - No `Drop` impl blocks.
- **Write-behind coalescing (Rust-side fix).** Redisson's `flushTasks` sends the collected deletes before the collected writes, so `put(k)` followed by `remove(k)` in one window ends with `k` written again. redissun keeps one pending operation per key, the latest, so the writer sees the final state. The PR records this deviation.
- **Writer arguments.** `write(&[(K, V)])` and `delete(&[K])` instead of Java's `Map`/`Collection`. They need no `Hash` bound, the order is kept, and keys within a call are unique. Entries are decoded from the encoded bytes, so `&Q`/`&W` arguments work. A writer that is not set costs nothing.
- **`incr_by` with a writer:** the writer gets the new number decoded as `V` with the map's codec, as `get` would return it. Redisson passes the `addAndGet` result.
- **`remove_many` with nothing removed** does not call the writer. Redisson calls `delete([])`.
- **Store mode `Local`, publish rule:** a write or remove publishes only when sync or reconnection is on (`mode != "0"`). Redisson's `removeOperationAsync` in `LOCALCACHE` mode publishes always. Nobody listens in that case, so the difference is harmless.
- **`use_key_events_pattern`** is Redisson 4.8's non-deprecated `expirationEventPolicy`: the enum `ExpirationEventPolicy::{KeyeventPattern (default, today's behaviour), KeyspaceChannel, None}`, with doc aliases `useKeyEventsPattern` and `expirationEventPolicy`. There is no `bool` setter.

## Where Redisson consults the loader (from `RedissonMap.java`, `RedissonMapCache.java`, `RedissonLocalCachedMap.java`)

| redissun | Redisson | Loader | Maps |
|---|---|---|---|
| `get` | `getAsync` (1238) | on a miss | all three |
| `get_many` | `getAllAsync` (547): missing keys, `loadAllMapAsync(.., parallelism 1)` | each missing key, one at a time | HashMap, HashMapCache |
| `contains_key` | `containsKeyAsync` (475) | on a miss; `true` when it loaded a value | all three |
| `entry(&k)` | compute family → `getAsync(key, threadId)` under the same per-key lock | on a miss, without taking the lock again | HashMap, HashMapCache |
| `peek`, `peek_many` | `getWithTTLOnlyAsync` (2039), `getAllWithTTLOnlyAsync` (2062) | on a miss | HashMapCache |
| `load_all()`, `load_many(&ks)` | `loadAll(replace, parallelism)` (1261), `loadAll(keys, replace, parallelism)` (1384) | always; `replace_existing()` skips the check | all three |
| `lease`, `iter*`, `keys*`, `values*`, `to_hash_map`, `random_*`, `contains_value`, `len`, `value_len`, `entry_ttl`, `incr_by`, every write | — | never | — |

`loadValue` (1776) takes `getLock(key)` (redissun's `KeyLock`, the `entry()` lock). Without `replace_existing` it reads the key again and returns a present value. It then calls the loader and stores a `Some` value with the map's plain put, without calling the writer: HashMap `HSET`, HashMapCache `PUT` (no limits, publishes `Created`), LocalCachedMap `PUT` plus the local cache and the sync message. `None` stores nothing. Redisson's `loadAll` with an `AsyncIterator` loads `parallelism` keys at a time; redissun runs `try_for_each_concurrent(parallelism)` over `load_all_keys()`.

## Where Redisson calls the writer (`mapWriterFuture` and its condition)

| redissun | Redisson | Writer call | When |
|---|---|---|---|
| `insert` (cache: also `.ttl/.max_idle`) | `put`, `put(.., ttl[, idle])` (1389, MC 1868) | `write([(k, v)])` | always |
| `set` (cache: limits) | `fastPut`, `fastPut(.., ttl[, idle])` (1432, MC 1336) | `write([(k, v)])` | always |
| `try_insert` (cache: limits) | `putIfAbsent`, `fastPutIfAbsent` (989, 1023, MC 655, 2770) | `write([(k, v)])` | it stored (`None`) |
| `replace` | `replace(k, v)`, `putIfExists`, `fastReplace` (1169, 958, 1201) | `write([(k, v)])` | the key existed (`Some`) |
| `compare_and_set` | `replace(k, old, new)` (1133) | `write([(k, new)])` | `true` |
| `remove` | `remove(k)` (1411) | `delete([k])` | always, also for a missing key |
| `remove_if_eq` | `remove(k, v)` (1079) | `delete([k])` | `true` |
| `remove_many` | `fastRemove` (1455) → `fastRemoveOperationBatchAsync` | `delete(removed keys)` | at least one key removed |
| `extend` (`.batch_size`, cache: limits) | `putAll(map[, batch])` (646, 613), `putAll(map, ttl[, idle])` (MC 1172, 1193) | `write(batch)` | once per batch |
| `incr_by`, `incr_by_float` | `addAndGet` (1586) | `write([(k, new value)])` | always |
| entry writes (`or_insert*`, `insert`, modified `or_insert`) | `compute`/`merge` → `fastPut`, `computeIfAbsent` → `putIfAbsent` | `write([(k, v)])` | the entry writes |
| `OccupiedEntry::remove` | `compute` → `fastRemove`, `computeIfPresent` → `remove(k, v)` | `delete([k])` | the entry removes |
| `clear`, `Object::del`, `expire_entry*`, `lease`, `LeaseGuard::insert/remove`, `set(..).lease(token)`, `invalidate`, `migrate_from_0_23`, loaded values | `clear`/`delete`, `putWithLease`, `removeWithLease` | none | — |

`LocalCachedMap` has `insert` (always `write`) and `remove` (always `delete`) of these. Batch views (`BatchHashMap`) bypass the loader and the writer, as Redisson's batch maps have no options.

## Not in 0c (each gets a line in `limitations.mdx`)

- Open question 4's options.
- `RDestroyable.destroy()`: dropping the handles plays its role (see "Decisions").
- `Object::rename` of a write-behind map: the task keeps polling the queue of the old name, as in Redisson.

## Global Constraints

- `#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::todo, clippy::unimplemented)]`: no unwrap or expect in library code. Tests may use them.
- Lua: the only new scripts are Redisson's `fastRemoveOperationBatchAsync`, copied verbatim: `FAST_REMOVE_BATCH` in `hash_map.rs` (`RedissonMap.java` 1501) and in `hash_map_cache/scripts.rs` (`RedissonMapCache.java` 2257). Each gets a `/// Redisson<Class>.fastRemoveOperationBatchAsync` comment. The scripts they replace stay for the no-writer path. Existing scripts do not change.
- Key names: the per-key lock is the existing `KeyLock` (`map_key_name(name, k, "lock")`, Redisson's `getLock(key)`). The write-behind queue is `suffix_name(name, "redissun_write-behind-queue")` (open question 2). Both are in the map's slot.
- Public traits use `-> impl Future<Output = …> + Send` / `-> impl Stream<Item = …> + Send`. The maps store `Arc<dyn Erased…>` (private). No private trait in a public bound.
- Rust names with `#[doc(alias = "<Redisson name>")]`. Optional settings are builder methods, never `bool` parameters.
- Defaults are Redisson's: `WriteMode::WriteThrough`; `WriteMode::write_behind()` = `WriteBehind { batch_size: 50, delay: 1000 ms }`; `writer_retry_attempts` = 1 call (Redisson's `0` → `max(1, 0)`); `writer_retry_interval` = 100 ms; `StoreMode::LocalRedis`; `CacheProvider::Builtin`; `ExpirationEventPolicy::KeyeventPattern`. `load_all`'s `parallelism` defaults to 1 (Redisson has no default).
- `Error::Config` at `build()` for: `writer_retry_attempts(0)`; `WriteBehind` with `batch_size == 0` or a zero `delay`; `WriteBehind` without a writer. `Error::Config` at `.await` for `load_all`/`load_many` without a loader and for `parallelism(0)`.
- A map built without a loader and writer does exactly what it does today: no extra round trip, no lock, no decode (`store: None`). Every existing test passes unchanged.
- Logs go through `tracing` with the map name and entry counts, never keys or values.
- One cargo build or test run at a time, with `--test-threads 4`. No git worktrees.
- Docs go in the task that changes the behaviour:
  - the new guide `website/content/docs/guides/loaders-and-writers.mdx` ("Loaders and writers", added to `guides/meta.json` after `"codecs"`);
  - the option tables in `hash-map.mdx`, `hash-map-cache.mdx` and `local-cached-map.mdx`;
  - the `## HashMap`, `## HashMapCache` and `RLocalCachedMap` rows of `guides/redisson-migration.mdx`;
  - `guides/limitations.mdx`: drop "There is no `loadAll` yet, because map loaders are not ported."

## Review Focus

1. **The loader inside `entry()`.** `entry(&k)` already holds the key's lock, and the lock's local mutex is not reentrant. A miss inside an entry must load without taking the lock again; a user expects the entry to resolve and never hang. Pinned in Task 2 (`hash_map_entry_loads_under_its_lock`) and Task 5 (`cache_entry_loads_under_its_lock`).
2. **Concurrent misses and failing loaders.** Ten gets of one missing key at once must call the loader once, and all of them must see the value. A failing loader must give `Err(Error::Loader(_))`, store nothing and release the lock, so the next `get` works. A loader that returns `None` must be asked again next time and never hang (Redisson's `testMapLoaderGetMulipleNulls` bug). Pinned in Task 2 (`hash_map_concurrent_misses_load_once`, `hash_map_a_failing_loader_is_an_error`, `hash_map_a_missing_key_is_asked_again`).
3. **A failing write-through writer.** The call returns `Err(Error::Writer(_))` after the configured attempts. Redis already holds the new value, as in Redisson, and the docs say so. Pinned in Task 3 (`hash_map_a_failing_writer_is_an_error_after_redis_changed`).
4. **The write-behind queue's contents.** This covers a key written and then removed in one window (it must end deleted), a foreign or truncated payload in the queue (skipped and logged, never a panic, the rest still written), and handles or a client dropped with writes pending (written by the final drain or by the next instance). Pinned in Task 4 (`pending_write_then_delete_ends_deleted`, `decode_rejects_truncation_and_foreign_bytes`, `write_behind_skips_a_foreign_queue_entry`, `dropping_the_last_handle_flushes`, `another_client_processes_queued_writes`).
5. **Names with a hash tag, and Redis Cluster.** `{t}:m` gives the queue `{t}:m:redissun_write-behind-queue`. Loading (lock plus write), write-behind (`RPUSH`/`LPOP`) and `FAST_REMOVE_BATCH` must run on a 3-master cluster without `CROSSSLOT`. Pinned in Task 4 (`queue_name_follows_suffix_name`) and Task 10 (`loader_and_writer_work_on_a_cluster`).

---

### Task 1: Traits, errors, `WriteMode` and the shared store core

**Files:**
- Create: `src/collections/map/store.rs`
- Modify: `src/collections/map/mod.rs` (`pub(crate) mod store;`), `src/error.rs`, `src/lib.rs`, `Cargo.toml` (remove `async-trait`)
- Test: unit tests in `src/collections/map/store.rs` and `src/error.rs`

**Interfaces:**
- Produces (public, re-exported at the crate root):
  - `pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;`
  - `pub trait MapLoader<K, V>: Send + Sync + 'static`. Methods: `fn load(&self, key: &K) -> impl Future<Output = Result<Option<V>, BoxError>> + Send;` and `fn load_all_keys(&self) -> impl Stream<Item = Result<K, BoxError>> + Send;`. Doc aliases `MapLoader`, `MapLoaderAsync`. The rustdoc example implements it with `async fn` over a `std::collections::HashMap`.
  - `pub trait MapWriter<K, V>: Send + Sync + 'static`. Methods: `fn write(&self, entries: &[(K, V)]) -> impl Future<Output = Result<(), BoxError>> + Send;` and `fn delete(&self, keys: &[K]) -> impl Future<Output = Result<(), BoxError>> + Send;`. Doc aliases `MapWriter`, `MapWriterAsync`.
  - `#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)] pub enum WriteMode { #[default] WriteThrough, WriteBehind { batch_size: usize, delay: Duration } }`, and `impl WriteMode { pub const fn write_behind() -> Self }` (50, 1000 ms). Doc aliases `WRITE_THROUGH`, `WRITE_BEHIND`, `writeBehindBatchSize`, `writeBehindDelay`.
  - `Error::Loader(BoxError)` and `Error::Writer(BoxError)`, with `#[source]`.
- Produces (crate-private):
  - `trait ErasedLoader<K, V>: Send + Sync` (`load<'a>(&'a self, &'a K) -> BoxFuture<'a, Result<Option<V>, BoxError>>`, `load_all_keys(&self) -> BoxStream<'_, Result<K, BoxError>>`) and `trait ErasedWriter<K, V>: Send + Sync` (the same shape for `write` and `delete`), each with a blanket impl for `T: MapLoader<K, V>` / `T: MapWriter<K, V>`.
  - `pub(crate) struct StoreOptions<K, V>`: `Default` is no loader, no writer, `WriteThrough`, 1 attempt, 100 ms. It has `pub(crate) fn into_store<C: Codec>(self, key: &Key, codec: C) -> Result<Option<Arc<Store<K, V, C>>>>`, which validates (see Global Constraints) and gives `None` when there is neither a loader nor a writer. Task 4 adds the start of write-behind here.
  - `pub(crate) struct Store<K, V, C: Codec>` with:
    - `has_loader(&self) -> bool`
    - `async fn load(&self, k: &Bytes) -> Result<Option<V>>`: decodes `k` as `K` and maps the loader's error to `Error::Loader`
    - `fn load_all_keys(&self) -> Result<BoxStream<'_, Result<K>>>`: `Error::Config("this map has no loader")` without one
    - `fn encode_key(&self, k: &K) -> Result<Bytes>` (for the keys `load_all_keys` yields)
    - `async fn written(&self, entries: &[(Bytes, Bytes)]) -> Result<()>` and `async fn deleted(&self, keys: &[Bytes]) -> Result<()>`: `Ok(())` without a writer and for empty input. Write-through decodes the input and calls the writer through `retry`. Write-behind arrives in Task 4.
  - `macro_rules! store_options` (`pub(crate) use`). It expands, in a builder that has a field `options: StoreOptions<K, V>`, to:
    - `pub fn loader(self, loader: impl MapLoader<K, V>) -> Self` (aliases `loader`, `loaderAsync`)
    - `pub fn writer(self, writer: impl MapWriter<K, V>) -> Self` (aliases `writer`, `writerAsync`)
    - `pub fn write_mode(self, mode: WriteMode) -> Self` (alias `writeMode`)
    - `pub fn writer_retry_attempts(self, attempts: usize) -> Self` ("calls the writer at most `attempts` times", aliases `writerRetryAttempts`, `writeRetryAttempts`)
    - `pub fn writer_retry_interval(self, interval: Duration) -> Self` (aliases `writerRetryInterval`, `writeRetryInterval`)
  - `async fn retry<F, Fut>(attempts: usize, interval: Duration, name: &str, call: F) -> Result<(), BoxError>` where `F: FnMut() -> Fut, Fut: Future<Output = Result<(), BoxError>>`. This is `RetryableMapWriterAsync`: at least one call, `tokio::time::sleep(interval)` between calls, a `tracing::warn!` with the map name and the attempt number, and the last error returned.

- [ ] **Step 1: Write failing unit tests.**
  - `store.rs`:
    - `write_mode_defaults_are_redissons`: `WriteMode::default() == WriteMode::WriteThrough`; `WriteMode::write_behind() == WriteMode::WriteBehind { batch_size: 50, delay: Duration::from_millis(1000) }`.
    - `retry_succeeds_on_the_last_attempt`: a closure that fails twice and then succeeds, with `attempts = 3` and `interval = 10ms`, gives `Ok(())` after 3 calls.
    - `retry_returns_the_last_error`: always failing with `attempts = 2` and `interval = 50ms` gives `Err` with the second error's message, after 2 calls and ≥ 50 ms.
    - `retry_calls_once_by_default`: `attempts = 1`, always failing, 1 call.
    - `options_are_checked`: `into_store` gives `Err(Error::Config(_))` for each of the following, and `Ok(None)` for `StoreOptions::default()`. This test needs a `Key`; if building one needs a live `Core`, move it to Task 2 Step 1 as `hash_map_builder_checks_its_options`.
      - attempts 0
      - `WriteBehind { batch_size: 0, .. }`
      - `WriteBehind { delay: ZERO, .. }`
      - `WriteBehind` without a writer
  - `error.rs` `messages_are_stable`: `Error::Loader("db down".into()).to_string() == "map loader failed: db down"`, and the same for `Writer` with "map writer failed: db down". `std::error::Error::source(&e)` is `Some`.
- [ ] **Step 2: Run** `cargo test --lib map::store -- --test-threads 4`. Expected: FAIL (compile: `store` module missing).
- [ ] **Step 3: Implement** the items above. `lib.rs`: `pub use collections::map::store::{BoxError, MapLoader, MapWriter, WriteMode};`.
- [ ] **Step 4: Run** `cargo test --lib map::store -- --test-threads 4`, then `cargo test --lib error -- --test-threads 4`, then `cargo clippy --all-targets -- -D warnings`. Expected: `test result: ok.` and no warnings. Unused-item warnings for `Store` methods that Task 2 uses are silenced with a scoped `#[allow(dead_code)]` that Task 2 removes.
- [ ] **Step 5: Commit** `feat(map): MapLoader, MapWriter, WriteMode and the shared store core`

### Task 2: `HashMapBuilder`, read-through and `load_all` on `HashMap`

**Files:**
- Create: `src/collections/map/load.rs` (`load_value`, `load_value_locked`, the `LoadAll` builder), `tests/common/store.rs`, `tests/map_store.rs`
- Modify:
  - `src/collections/map/mod.rs` (`pub(crate) mod load;`)
  - `src/collections/map/entry.rs`: `Target::store()`; `Entry::acquire` loads on a miss
  - `src/collections/hash_map.rs`: the field `store: Option<Arc<Store<K, V, C>>>` and `HashMapBuilder`; `get`, `get_many` and `contains_key` load; `load_all`, `load_many`
  - `src/client/mod.rs` (`hash_map_builder`), `src/lib.rs`
  - `tests/common/mod.rs` (`pub mod store;`)
- Docs: create `guides/loaders-and-writers.mdx` (read-through part) and update `guides/meta.json`; `hash-map.mdx` gets "Loader and writer" rows; `limitations.mdx`; `redisson-migration.mdx` `## HashMap` gets `getMap(name, MapOptions)` → `hash_map_builder(name)…build().await`, `loadAll` → `load_all()`/`load_many(&ks)`.

**Interfaces:**
- Consumes: Task 1's `Store`, `StoreOptions`, `store_options!`.
- Produces:
  - `pub fn hash_map_builder<K, V>(&self, name: impl Into<Arc<str>>) -> HashMapBuilder<K, V, C>` on `Client<C>`.
  - `pub struct HashMapBuilder<K, V, C: Codec>` (at the root, `#[must_use]`), with `store_options!()` and `pub async fn build(self) -> Result<HashMap<K, V, C>>`.
  - `HashMap::with_store(key: Key, codec: C, store: Option<Arc<Store<K, V, C>>>) -> Self` (crate). `HashMap::new` stays and passes `None`.
  - `pub fn load_all(&self) -> LoadAll<'_, K, V, C>` and `pub fn load_many<Q>(&self, ks: &[&Q]) -> LoadAll<'_, K, V, C>`, both with alias `loadAll`.
  - `pub struct LoadAll<'m, K, V, C: Codec>` in `redissun::hash_map`, `#[must_use]`, used by all three maps through `Target`. It has `pub fn replace_existing(self) -> Self` and `pub fn parallelism(self, n: usize) -> Self`, and implements `IntoFuture<Output = Result<()>>`. It stops at the first error.
  - In `load.rs`:
    - `pub(crate) async fn load_value<K, V, C>(target: &Target<'_, K, V, C>, k: &Bytes, replace: bool) -> Result<Option<V>>`: `KeyLock::acquire`; without `replace`, `target.read(k)` returns a present value; otherwise `store.load(k)`; `Some(v)` → `target.write(k, &v)` (the raw write, no writer); `lock.release(body)`.
    - `pub(crate) async fn load_value_locked(..)`: the same steps after the lock, for `Entry`.
  - `Target::write`/`Target::delete` stay raw. `Held::write` and `OccupiedEntry::remove` call `store.written`/`store.deleted` after them; the writer calls arrive in Task 3.
- Test helper `tests/common/store.rs`: `#[derive(Clone)] pub struct MemoryStore(Arc<Inner>)`, which implements `MapLoader<String, String>`, `MapWriter<String, String>`, `MapLoader<String, i64>` and `MapWriter<String, i64>` through a generic inner `Mutex<BTreeMap<String, String>>` (values serialized with `serde_json`). It has:
  - `MemoryStore::with(&[("1", "11")])`, `snapshot() -> BTreeMap<String, String>`
  - `loads() -> usize`, `max_in_flight() -> usize`
  - `write_calls() -> Vec<Vec<String>>` (keys per call), `delete_calls() -> Vec<Vec<String>>`
  - `fail_next(n)` (the next `n` calls fail with `"injected"`), `fail_always()`, `slow(Duration)` (every call sleeps first)

- [ ] **Step 1: Write failing tests in `tests/map_store.rs`.** Each builds its map with `client.hash_map_builder::<String, String>(unique("ms")).loader(store.clone()).build().await.unwrap()` unless it says otherwise.
  - `hash_map_get_loads_a_missing_key` (Redisson `testMapLoaderGet` and `testLoaderGetAsync`): with a store of `1→11`, `2→22`, `3→33`:
    - `len() == 0`, `get("1") == Some("11")`, `len() == 1`, `get("0") == None`;
    - `insert("0", "00")`, then `get("0") == Some("00")`, `len() == 2`;
    - `contains_key("2")`, then `len() == 3`;
    - `get_many(&["1", "2", "9", "3"]) == [Some("11"), Some("22"), None, Some("33")]`, then `len() == 4`.
  - `hash_map_a_missing_key_is_asked_again` (`testMapLoaderGetMulipleNulls`): `get("0")`, `get("1")`, `get("0")`, all inside `tokio::time::timeout(5s)`, give `None`, `Some("11")`, `None`, with `loads() == 3`.
  - `hash_map_a_failing_loader_is_an_error` (Review Focus 2; `testMapLoaderGetWithException` inverted, open question 1): `fail_next(1)`, then `get("1")` is `Err(Error::Loader(_))` and `len() == 0`. The next `get("1")` is `Some("11")`.
  - `hash_map_concurrent_misses_load_once` (Review Focus 2): `slow(100ms)`, 10 `tokio::spawn`ed gets of `"1"` on clones all give `Some("11")`, and `loads() == 1`.
  - `hash_map_loaded_values_are_not_written_back`: with a loader and a writer on the same store, `get("1")` leaves `write_calls()` empty.
  - `hash_map_entry_loads_under_its_lock` (Review Focus 1): `timeout(5s, map.entry("1"))` is `Entry::Occupied` with `get() == "11"`. Then `.and_modify(|v| v.push('!')).or_insert(..)` gives `"11!"`, `loads() == 1`, and `HGET` gives `"11!"`.
  - `hash_map_load_all_keeps_present_values` (`testLoadAllReplaceValues`, first half): a store `"0".."9"` → `"00".."99"`; insert `0→010` and `5→555`; `load_all().parallelism(2)`. Then `len() == 10`, `get("0") == "010"`, `get("5") == "555"`.
  - `hash_map_load_all_can_replace_values` (second half): `clear`, insert the same two, `load_all().replace_existing().parallelism(2)`. Then `get("0") == "00"` and `get("5") == "55"`.
  - `hash_map_load_all_loads_every_key` (`testLoadAll`, `testLoadAllAsync`): 100 keys and `parallelism(2)` give `len() == 100`, and every `contains_key` is `true`.
  - `hash_map_load_all_respects_parallelism`: `slow(20ms)`, 30 keys, `parallelism(3)`: `2 <= max_in_flight() <= 3`.
  - `hash_map_load_many_loads_only_those_keys`: `load_many(&["1", "3"])` gives `len() == 2` and `loads() == 2`.
  - `hash_map_load_all_without_a_loader_is_a_config_error`, and `parallelism(0)` is `Error::Config(_)`.
  - `hash_map_builder_checks_its_options` (if not in Task 1).
  - `hash_map_builder_without_options_reads_like_hash_map`: `build()` with no options, raw `HSET`, then `get` reads it.
- [ ] **Step 2: Run** `cargo test --test map_store hash_map_ -- --test-threads 4`. Expected: FAIL (compile: `hash_map_builder` missing).
- [ ] **Step 3: Implement.** `get` on a miss with a loader calls `load_value(&Target::Map(self), &k, false)`. `get_many` loads the missing keys one by one in key order. `contains_key` on a miss loads and gives `loaded.is_some()`. Write the read-through half of `loaders-and-writers.mdx`: the loader table above, the lock, values stored without the writer, errors, `load_all`. Update the other docs listed under Files.
- [ ] **Step 4: Run** `cargo test --test map_store hash_map_ -- --test-threads 4`, then `cargo test --test it hash_map -- --test-threads 4`. Expected: `test result: ok.` for both.
- [ ] **Step 5: Commit** `feat(hash_map): HashMapBuilder with read-through MapLoader and load_all`

### Task 3: Write-through on `HashMap`

**Files:**
- Modify: `src/collections/hash_map.rs` (writer calls per the writer table; `FAST_REMOVE_BATCH` from `RedissonMap.java` 1501 when a writer is set), `src/collections/map/store.rs` (if needed)
- Test: `tests/map_store.rs`
- Docs: `loaders-and-writers.mdx` (the writer table, write-through order, retries), `hash-map.mdx`

**Interfaces:**
- Consumes: `Store::written`/`deleted` (Task 1), `Target`/`Held` (Task 2).
- Produces: no new public items. Each write method calls `self.store.written(..)`/`deleted(..)` after its Redis call, under the condition in the writer table. `incr_by`/`incr_by_float` give the writer the reply as `V`: `codec.decode::<V>(reply.to_string().as_bytes())` for `HINCRBY` and the reply bytes for `HINCRBYFLOAT`. `extend` calls `written` after each batch's `HSET`. `remove_many` with a writer runs `FAST_REMOVE_BATCH` (one `0`/`1` per key) and passes the removed keys.

- [ ] **Step 1: Write failing tests** (map built with `.writer(store.clone())`):
  - Redisson ports, each comparing `snapshot()` with the expected `BTreeMap`:
    - `hash_map_writer_put` (`testWriterPut`): three `insert` calls.
    - `hash_map_writer_set` (`testWriterFastPut`): three `set` calls.
    - `hash_map_writer_remove` (`testWriterRemove`): insert `1`, remove `1`, insert `3` give `{3: 33}`.
    - `hash_map_writer_replace` (`testWriterReplaceKeyValue`): `replace("2", ..)` on a missing key does not call the writer.
    - `hash_map_writer_compare_and_set` (`testWriterReplaceKeyOldNewValue`).
    - `hash_map_writer_remove_if_eq` (`testWriterRemoveKeyValue`).
    - `hash_map_writer_try_insert` (`testWriterPutIfAbsent`, `testWriterFastPutIfAbsent`): a second `try_insert("1", "00")` leaves `11`.
    - `hash_map_writer_extend` (`testWriterPutAll`).
    - `hash_map_writer_remove_many` (`testWriterFastRemove`): `remove_many(&["1", "2", "4"])` gives `delete_calls() == [["1", "2"]]` and `{3: 33}`.
    - `hash_map_writer_incr_by` (`testWriterAddAndGet`, `RedissonMapTest.testAddAndGetMapWriter`): on a `HashMap<String, i64>`, `incr_by("1", 11) == 11`, then `7` gives `18`, and the store holds `{1: 18}`.
    - `hash_map_writer_retries_until_it_succeeds` (`testRetryableWriterSuccessAtLastRetry`): `writer_retry_attempts(3)`, `fail_next(2)`, `insert("1", "11")` is `Ok`, the store has it after 3 calls; the same for `remove("1")`.
  - Own tests:
    - `hash_map_a_failing_writer_is_an_error_after_redis_changed` (Review Focus 3): `fail_always()`, `writer_retry_attempts(2)`, `writer_retry_interval(200ms)`. `insert("a", "1")` is `Err(Error::Writer(_))` after ≥ 200 ms and 2 calls, and a plain `client.hash_map(name).get("a") == Some("1")`.
    - `hash_map_remove_calls_delete_even_for_a_missing_key`: `delete_calls() == [["x"]]`.
    - `hash_map_remove_many_without_a_removed_key_skips_the_writer`: `delete_calls()` is empty and the result is `0`.
    - `hash_map_entry_writes_go_to_the_writer`: `entry("a").await?.or_insert("1")` gives `write_calls() == [["a"]]`, then `OccupiedEntry::remove` gives `delete_calls() == [["a"]]`.
    - `hash_map_extend_calls_the_writer_once_per_batch`: 5 entries with `batch_size(2)` give write calls of sizes `[2, 2, 1]`.
    - `hash_map_clear_and_del_skip_the_writer`.
- [ ] **Step 2: Run** `cargo test --test map_store hash_map_writer hash_map_a_failing_writer hash_map_remove hash_map_entry_writes hash_map_extend hash_map_clear -- --test-threads 4`. Expected: FAIL (assertion: the writer is never called).
- [ ] **Step 3: Implement**, then the docs.
- [ ] **Step 4: Run** the same command, then `cargo test --test map_store -- --test-threads 4`, then `cargo test --test it hash_map -- --test-threads 4`. Expected: `test result: ok.` for each.
- [ ] **Step 5: Commit** `feat(hash_map): write-through MapWriter with Redisson's retries`

### Task 4: Write-behind

**Files:**
- Create: `src/collections/map/write_behind.rs`
- Modify: `src/collections/map/mod.rs`, `src/collections/map/store.rs` (`Store` holds `Option<WriteBehind>`; `into_store` starts the task; `written`/`deleted` push), `src/collections/hash_map.rs` (`flush`)
- Test: unit tests in `write_behind.rs`; `tests/map_store.rs`
- Docs: `loaders-and-writers.mdx` (write-behind: batching, delay, coalescing, lifecycle, `flush`, the queue key and why it is not Redisson's); `limitations.mdx` (rename, foreign queue)

**Interfaces:**
- Produces:
  - `pub(crate) fn queue_name(name: &str) -> String` = `suffix_name(name, "redissun_write-behind-queue")`.
  - `pub(crate) enum Task { Write(Vec<(Bytes, Bytes)>), Delete(Vec<Bytes>) }`.
  - `pub(crate) fn encode(task: &Task) -> Bytes`: the bytes `[1]` (version), `b'W'` or `b'D'`, a `u32` BE count, then each key as a `u32` BE length plus the bytes, followed for `W` by the value in the same form.
  - `pub(crate) fn decode(payload: &[u8]) -> Option<Task>`: `None` for another version, an unknown kind, truncation or trailing bytes. It never panics.
  - `pub(crate) struct Pending` with `fn new(batch_size: usize) -> Self`, `fn push(&mut self, task: Task) -> std::vec::Vec<Task>` (the batches that just reached `batch_size`) and `fn finish(&mut self) -> std::vec::Vec<Task>` (the rest: deletes, then writes, empty ones left out). It keeps one operation per encoded key, the latest: a write of `k` drops a pending delete of `k`, and the reverse. Writes keep the order of their first write with the latest value.
  - `pub(crate) struct WriteBehind { queue: String, batch_size: usize, delay: Duration, drain: tokio::sync::Mutex<()>, alive: Arc<Liveness> }`. `Liveness` holds a `tokio::sync::Notify` and calls `notify_one` on `Drop`; every `Store` clone shares one `Arc<Liveness>` through the `Arc<Store>`. The background task holds a `Weak<Core>`, the writer `Arc`, the codec, the queue name and a `Weak<Liveness>`. It loops `select!` on `sleep(delay)` and the notify, then `drain`. When `Liveness` is gone, it drains once more and returns. When the `Core` is gone, it returns.
  - `Store::drain(&self) -> Result<()>` and the task's drain share one function: under `drain`, `LPOP queue` until nil, `decode` (on `None`: `tracing::error!` and skip), `Pending::push` and send the full batches, then `finish` and send the rest. Each batch goes through `retry`. A failed batch is logged and dropped, as Redisson does, and the first error is returned.
  - `written`/`deleted` in write-behind mode: `RPUSH queue encode(task)`, awaited; an error is returned to the caller.
  - `pub async fn flush(&self) -> Result<()>` on `HashMap`: `Ok(())` without write-behind. It has no doc alias; its rustdoc says that it plays the part Redisson's `destroy()` plays for pending writes.

- [ ] **Step 1: Write failing unit tests in `write_behind.rs`.**
  - `task_payload_round_trips`: a write with an empty value, and a delete with an empty key.
  - `decode_rejects_truncation_and_foreign_bytes` (Review Focus 4): `[]`, `[0xac, 0xed, 0x00, 0x05]` (Java serialization), version `2`, kind `b'X'`, a count larger than the data, and one trailing byte all give `None`.
  - `pending_keeps_the_last_operation_per_key`: write `a=1`, delete `a`, write `b=2`, write `a=3`, then `finish()` gives `[Write([(b, 2), (a, 3)])]`.
  - `pending_write_then_delete_ends_deleted` (Review Focus 4): write `a`, delete `a`, then `finish()` gives `[Delete([a])]`.
  - `pending_flushes_full_batches`: with `batch_size 2`, five writes make `push` return batches of 2 twice, and `finish()` gives one write of 1.
  - `queue_name_follows_suffix_name` (Review Focus 5): `"m"` gives `{m}:redissun_write-behind-queue`, and `"{t}:m"` gives `{t}:m:redissun_write-behind-queue`.
- [ ] **Step 2: Write failing integration tests in `tests/map_store.rs`.** Maps use `.writer(store.clone()).write_mode(WriteMode::WriteBehind { batch_size, delay })`.
  - `hash_map_write_behind_remove_many` (`testWriteBehindFastRemove`, `testWriteBehindAsyncFastRemove`): with `write_behind()`, insert `1`, `2`, `3`, sleep 1400 ms, `remove_many(&["1", "2", "4"])`, sleep 1400 ms, then the store is `{3: 33}`.
  - `hash_map_write_behind_retries` (`testRetryableWriterAsyncSuccessAtLastRetry`): `write_behind()`, `writer_retry_attempts(3)`, `writer_retry_interval(100ms)`, `fail_next(2)`. Insert `1`; after 2400 ms the store is `{1: 11}` after 3 calls. Remove `1` with `fail_next(2)`; after 2400 ms the store is `{}`.
  - `write_behind_returns_before_the_writer_runs`: `delay 60s`. After `insert`, the store is empty and `LLEN` of the queue is `1`. After `flush().await`, the store has it and `LLEN` is `0`.
  - `write_behind_batches_by_batch_size`: `batch_size 2`, `delay 60s`, five inserts, then `flush()` gives write calls of sizes `[2, 2, 1]`.
  - `write_behind_coalesces_a_write_and_a_delete` (Review Focus 4): `delay 60s`, `insert("a")`, `remove("a")`, `flush()`. The store has no `a`, and `delete_calls() == [["a"]]`.
  - `dropping_the_last_handle_flushes` (Review Focus 4): `delay 60s`, insert, a clone, drop both. `eventually` (5 s) the store has the entry.
  - `another_client_processes_queued_writes` (Review Focus 4): client A with `delay 60s` inserts `x`. Client B, a second `connect_with` client, builds the same name with `delay 100ms` and its own store. `eventually` B's store has `x` and `LLEN == 0`.
  - `write_behind_skips_a_foreign_queue_entry` (Review Focus 4): raw `RPUSH <queue> "\xac\xed\x00\x05"`, then `insert("a")` and `flush()` give `Ok` and the store has `a`.
  - `a_failing_write_behind_batch_is_dropped_and_reported`: `fail_always()`. `flush()` is `Err(Error::Writer(_))` and `LLEN == 0`. The next `flush()` is `Ok(())`.
  - `flush_without_write_behind_is_a_no_op`.
- [ ] **Step 3: Run** `cargo test --lib write_behind -- --test-threads 4`, then `cargo test --test map_store write_behind -- --test-threads 4`. Expected: FAIL (compile).
- [ ] **Step 4: Implement**, then the docs.
- [ ] **Step 5: Run** the same two commands, then `cargo test --test map_store -- --test-threads 4`. Expected: `test result: ok.` for each.
- [ ] **Step 6: Commit** `feat(map): write-behind MapWriter with Redisson's batching, delay and retries`

### Task 5: `HashMapCache`

**Files:**
- Modify:
  - `src/collections/hash_map_cache/mod.rs`: the `store` field and `HashMapCacheBuilder`; `get`, `get_many`, `peek`, `peek_many` and `contains_key` load; `replace`, `compare_and_set`, `remove`, `remove_if_eq`, `remove_many`, `incr_by*`, `load_all`, `load_many` and `flush`
  - `src/collections/hash_map_cache/builders.rs`: writer calls in the `IntoFuture` of `Insert`, `TryInsert`, `Set` (none with `.lease(token)`) and `Extend`
  - `src/collections/hash_map_cache/scripts.rs`: `FAST_REMOVE_BATCH` from `RedissonMapCache.java` 2257
  - `src/collections/map/entry.rs`: the store of `Target::Cache`
  - `src/client/mod.rs` (`hash_map_cache_builder`), `src/lib.rs` (`HashMapCacheBuilder` at the root)
- Test: `tests/map_store.rs`
- Docs: `hash-map-cache.mdx`, `redisson-migration.mdx` `## HashMapCache` (`getMapCache(name, MapCacheOptions)` → `hash_map_cache_builder`)

**Interfaces:**
- Consumes: Tasks 1–4. `HashMapCache::new` keeps scheduling the clean-up task; `build()` calls `HashMapCache::with_store(key, codec, store)`, which does the same.
- Produces: `pub fn hash_map_cache_builder<K, V>(&self, name: impl Into<Arc<str>>) -> HashMapCacheBuilder<K, V, C>` on `Client<C>`. `HashMapCacheBuilder` has `store_options!()` and `pub async fn build(self) -> Result<HashMapCache<K, V, C>>`. The cache also gets `load_all`/`load_many` (the shared `LoadAll`) and `flush`. `remove_many` with a writer runs `FAST_REMOVE_BATCH`, which also publishes `Removed`. `incr_by` passes the `ADD_AND_GET` reply decoded as `V`.

- [ ] **Step 1: Write failing tests** (names start with `cache_`):
  - `cache_get_loads_and_stores_without_limits`: `get("1") == Some("11")`, `entry_ttl("1") == None`, and an `events_of(&[Created])` started before it receives `Created { key: "1", value: "11" }`.
  - `cache_an_expired_entry_is_loaded_again`: after `insert(..).ttl(200ms)` and a 400 ms sleep, `get` calls the loader (`loads() == 1`).
  - `cache_get_many_peek_and_contains_key_load`: `get_many(&["1", "9"]) == [Some, None]`; `peek("2") == Some("22")`; `peek_many(&["3"])` gives `[Some("33")]`; `contains_key("x") == false` with `loads()` increased.
  - `cache_entry_loads_under_its_lock` (Review Focus 1): `timeout(5s, cache.entry("1").ttl(60s))` is `Occupied("11")`.
  - `cache_load_all_keeps_present_values`: the `testLoadAllReplaceValues` assertions on the cache.
  - `cache_writer_with_limits` (`RedissonMapCacheTest.testWriterPutTTL`, `testWriterFastPutTTL`, `testWriterPutIfAbsentTTL`, `testWriterFastPutIfAbsentTTL`): `insert(..).ttl(10s)` ×3, `set(..).ttl(10s)` ×3, and `try_insert("1", "11")` then `try_insert("1", "00")`, each with `.ttl(10s)`. The store holds exactly the expected maps.
  - `cache_writer_rmap_family`: `replace`, `compare_and_set`, `remove`, `remove_if_eq` and `incr_by` (on an `i64` cache: `11` then `18`, store `{1: 18}`), with the same expectations as Task 3's ports.
  - `cache_remove_many_tells_the_writer_and_publishes`: with an `events_of(&[Removed])` open, `remove_many(&["a", "b", "zz"])` gives `delete_calls() == [["a", "b"]]` and two `Removed` events.
  - `cache_extend_with_ttl_writes_each_batch`: 5 entries with `.batch_size(2).ttl(10s)` give write calls of sizes `[2, 2, 1]`.
  - `cache_leases_bypass_the_loader_and_the_writer`: with a loader and a writer, `lease("1", 1s)` gives `Lease::Acquired` and `loads() == 0`; then `guard.insert(&"v")` leaves `write_calls()` empty; then `invalidate("1")` leaves `delete_calls()` empty.
  - `cache_write_behind_flush`: `delay 60s`, `insert(..).ttl(60s)`, then `flush()` puts the entry in the store.
- [ ] **Step 2: Run** `cargo test --test map_store cache_ -- --test-threads 4`. Expected: FAIL (compile: `hash_map_cache_builder` missing).
- [ ] **Step 3: Implement**, then the docs.
- [ ] **Step 4: Run** the same command, then `cargo test --test hash_map_cache -- --test-threads 4`. Expected: `test result: ok.` for both.
- [ ] **Step 5: Commit** `feat(hash_map_cache): HashMapCacheBuilder with MapLoader, MapWriter and write-behind`

### Task 6: `LocalCachedMap` with a loader and a writer (`StoreMode::LocalRedis`)

**Files:**
- Modify:
  - `src/collections/local_cached_map.rs`: `store_options!()` on `LocalCachedMapBuilder`; the `store` field; `get` and `contains_key` load; `insert` and `remove` call the writer; `load_all`, `load_many`, `flush`; `pub(crate) async fn put_encoded(&self, k: &Bytes, v: &V) -> Result<()>` (local cache plus the `PUT` script, the path `insert` takes, without the writer)
  - `src/collections/map/entry.rs`: `Target::Local(&LocalCachedMap<K, V, C>)`, used by `load.rs` only, with `read` = local lookup then `HGET` from the master, and `write` = `put_encoded`
- Test: `tests/map_store.rs`
- Docs: `local-cached-map.mdx` (the options table gets the five store options), `redisson-migration.mdx`

**Interfaces:**
- Produces: on `LocalCachedMap`, `load_all`, `load_many` and `flush` with the same signatures as on `HashMap`. The inner `map: HashMap` keeps `store: None`, so the writer is called once.
- Rules from `RedissonLocalCachedMap.getAsync`/`containsKeyAsync` (Redisson lines 243–300): a value the loader returns is cached locally; a `None` from the loader is cached only with `store_cache_miss(true)`.

- [ ] **Step 1: Write failing tests** (names start with `local_`):
  - `local_get_loads_caches_and_shares`: two instances A and B of one name. `A.get("1") == Some("11")`, `A.local_len() == 1`, and a raw `HGET name "\"1\""` gives `"\"11\""`. Then `B.get("1") == Some("11")` with `loads() == 1`.
  - `local_a_miss_is_cached_only_with_store_cache_miss`: a loader for which every key is missing. Without the option, two `get("x")` give `loads() == 2`; with `store_cache_miss(true)`, `loads() == 1`.
  - `local_contains_key_loads`.
  - `local_insert_and_remove_call_the_writer`: `write_calls() == [["a"]]`, then `delete_calls() == [["a"]]`; `remove("missing")` also calls `delete`.
  - `local_load_all_loads_every_key`: 20 keys give `len() == 20`, and `local_len() == 20` on the loading instance.
  - `local_write_behind_flush`.
- [ ] **Step 2: Run** `cargo test --test map_store local_ -- --test-threads 4`. Expected: FAIL (compile).
- [ ] **Step 3: Implement**, then the docs.
- [ ] **Step 4: Run** the same command, then `cargo test --test local_cached_map -- --test-threads 4`. Expected: `test result: ok.` for both.
- [ ] **Step 5: Commit** `feat(local_cached_map): MapLoader, MapWriter and write-behind`

### Task 7: `StoreMode::Local`

**Files:**
- Modify: `src/collections/local_cached_map.rs`, `src/lib.rs` (`StoreMode` at the root)
- Test: `tests/local_cached_map.rs`
- Docs: `local-cached-map.mdx` (a "Store mode" section: what stays local, what is sent, that `Invalidate` drops other instances' only copy)

**Interfaces:**
- Produces:
  - `#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)] pub enum StoreMode { #[default] LocalRedis, Local }`, with aliases `LOCALCACHE_REDIS` and `LOCALCACHE` on the variants and `storeMode` on the builder method `pub fn store_mode(self, mode: StoreMode) -> Self`.
  - The behaviour in `Local` mode follows each `storeMode == LOCALCACHE` branch of `RedissonLocalCachedMap`:
    - `get`, `contains_key`: local only, the loader on a miss (`load_value` with `Target::Local`, whose `read` is the local lookup and whose `write` is a local put plus the sync message, in this mode).
    - `insert`: a local write, then `PUBLISH` of the same sync message the Lua sends when `mode != "0"`, then the writer. It returns the previous local value.
    - `remove`: a local forget, then `PUBLISH` of an invalidation when `mode != "0"`, then the writer.
    - `len`, `is_empty`: the local entries that hold a value (cached misses do not count; Redisson counts them; recorded).
    - `clear`: local, then `PUBLISH` of `CLEAR` when `mode != "0"`. It returns whether the local cache held anything.
    - `Object::del`: the same as `clear`. `del_script` returns a script that only publishes, and the local cache is forgotten first.
    - `iter`, `keys`, `values`: a stream over a snapshot of `local_entries()`.
    - Redis is never read or written except pub/sub and the per-key lock of a load.
  - A private `fn publish(&self, payload: Bytes) -> Result<()>` replaces the inline `PUBLISH` of `clear_local_everywhere`.

- [ ] **Step 1: Write failing tests:**
  - `local_store_mode_keeps_data_out_of_redis`: in `Local` mode, `insert("k", "v")`, then `get == Some("v")` and `len() == 1`, while `EXISTS name` is `0` and `EXISTS redissun__cache_updates_log:{name}` is `0`.
  - `local_store_mode_update_reaches_other_instances`: with `SyncStrategy::Update`, A inserts and `eventually` B's `get` gives the value; B's `local_len() == 1`; Redis holds nothing.
  - `local_store_mode_invalidate_drops_the_other_copy`: both insert `k`, A inserts `k` again, and `eventually` B's `get("k") == None`.
  - `local_store_mode_iter_lists_local_entries`: three inserts give `iter()` collected into a `HashSet` of 3 pairs.
  - `local_store_mode_clear_reaches_others`: A and B each hold `k`, `A.clear()` returns `true`, and `eventually` `B.len() == 0`.
  - `local_store_mode_uses_the_loader_and_the_writer`: a miss loads, and an insert calls the writer.
  - `local_store_mode_len_counts_values_only`: with `store_cache_miss(true)`, a missing `get` gives `len() == 0` and `local_len() == 1`.
- [ ] **Step 2: Run** `cargo test --test local_cached_map local_store_mode -- --test-threads 4`. Expected: FAIL (compile: `StoreMode` missing).
- [ ] **Step 3: Implement**, then the docs.
- [ ] **Step 4: Run** the same command, then `cargo test --test local_cached_map -- --test-threads 4`, then `cargo test --test map_store local_ -- --test-threads 4`. Expected: `test result: ok.` for each.
- [ ] **Step 5: Commit** `feat(local_cached_map): StoreMode::Local`

### Task 8: `ExpirationEventPolicy` (Redisson's `useKeyEventsPattern`)

**Files:**
- Modify:
  - `src/client/core.rs`: `pub(crate) database: i64` in `Core` and `Startup`
  - `src/client/config.rs`: set it from `settings.database`, then the URL's database, else `0`, the value the node `db` uses
  - `src/collections/local_cached_map.rs`, `src/lib.rs`
- Test: `tests/local_cached_map.rs`
- Docs: `local-cached-map.mdx` (option row and the server setting each policy needs)

**Interfaces:**
- Produces:
  - `#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)] pub enum ExpirationEventPolicy`:
    - `#[default] KeyeventPattern`: `PSUBSCRIBE __keyevent@*:expired`, clear when the message is the map key; today's behaviour, needs `Ex`.
    - `KeyspaceChannel`: `SUBSCRIBE __keyspace@<database>__:<redis key>`, clear when the message is `expired`; needs `Kx`.
    - `None`: no subscription.
  - The builder method `pub fn expiration_event_policy(self, policy: ExpirationEventPolicy) -> Self`, aliases `expirationEventPolicy` and `useKeyEventsPattern`.
  - The `listen` loop takes a private `enum ExpiryFeed { Pattern(Subscription<PatternMessage>, Receiver<PatternMessage>), Channel(Subscription, Receiver<Bytes>), None }` instead of the `expired` receiver.

- [ ] **Step 1: Write failing tests** on `own_redis()` with `CONFIG SET notify-keyspace-events KEx`:
  - `keyspace_channel_policy_clears_on_expiry`: build with `KeyspaceChannel`, `insert("k")`, `PEXPIRE name 100`, and `eventually` `local_len() == 0`. `PUBSUB CHANNELS __keyspace@0__:<name>` lists the channel.
  - `no_expiration_policy_keeps_the_local_cache`: build with `None`, `insert`, `PEXPIRE name 100`, sleep 500 ms, and `local_len() == 1`. `PUBSUB NUMPAT` did not increase when it was built.
  - `the_keyevent_pattern_stays_the_default`: the existing expiry test passes unchanged (reference it, no new test).
- [ ] **Step 2: Run** `cargo test --test local_cached_map policy -- --test-threads 4`. Expected: FAIL (compile).
- [ ] **Step 3: Implement**, then the docs.
- [ ] **Step 4: Run** the same command, then `cargo test --test local_cached_map -- --test-threads 4`, then `cargo test --lib config -- --test-threads 4`. Expected: `test result: ok.` for each.
- [ ] **Step 5: Commit** `feat(local_cached_map): ExpirationEventPolicy, Redisson's useKeyEventsPattern`

### Task 9: `CacheProvider` (open question 3; drop this task if the owner declines `moka`)

**Files:**
- Modify:
  - `Cargo.toml`: `moka = { version = "0.12", default-features = false, features = ["sync"], optional = true }` and `[features] moka = ["dep:moka"]`
  - `.github/workflows/ci.yml`: add `cargo test --features moka --test local_cached_map moka`
  - `src/collections/local_cached_map.rs`: move `reads`, `begin_read` and `finish_read` from `Store` into `Local`; a private `enum LocalStore { Builtin(Store), #[cfg(feature = "moka")] Moka(moka::sync::Cache<Bytes, Option<Bytes>>) }` with `get`, `put`, `remove`, `clear`, `len`, `entries`
  - `src/lib.rs`
- Test: `tests/local_cached_map.rs` (`#[cfg(feature = "moka")]`)
- Docs: `local-cached-map.mdx` (option row, feature flag, which options Moka honours)

**Interfaces:**
- Produces:
  - `#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)] #[non_exhaustive] pub enum CacheProvider`, with variants `#[default] Builtin` (alias `REDISSON`) and `#[cfg(feature = "moka")] Moka` (alias `CAFFEINE`).
  - The builder method `pub fn cache_provider(self, provider: CacheProvider) -> Self`, alias `cacheProvider`.
  - `Moka` follows `LocalCacheView.createCache`'s Caffeine branch:
    - `ttl` → `time_to_live`;
    - `max_idle` → `time_to_idle`;
    - `cache_size > 0` → `max_capacity`, whatever the `EvictionPolicy` (Caffeine ignores LRU/LFU);
    - `len` and `entries` call `run_pending_tasks` first.

- [ ] **Step 1: Write failing tests:**
  - `moka_provider_caches_reads`: `local_len() == 1` after a `get`.
  - `moka_provider_expires_after_ttl`: `ttl(200ms)`, then after 400 ms the next `get` reads Redis again (change the value through a plain `HashMap` first and expect the new one).
  - `moka_provider_respects_cache_size`: `cache_size(10)`, 100 reads, then `eventually` `local_len() <= 10`.
  - `moka_provider_receives_invalidations`: the existing invalidate scenario between two instances.
- [ ] **Step 2: Run** `cargo test --features moka --test local_cached_map moka -- --test-threads 4`. Expected: FAIL (compile).
- [ ] **Step 3: Implement**, then the docs.
- [ ] **Step 4: Run** the same command, then `cargo test --test local_cached_map -- --test-threads 4` (without the feature), then `cargo clippy --all-targets --all-features -- -D warnings`. Expected: all green.
- [ ] **Step 5: Commit** `feat(local_cached_map): CacheProvider with an optional moka backend`

### Task 10: Cluster, docs check and full verification

**Files:**
- Test: `tests/map_store.rs` (cluster test), `tests/cluster.rs` if that is where the cluster fixture lives (`tests/common/topology.rs`)
- Docs: re-read `loaders-and-writers.mdx`, the three object pages, `redisson-migration.mdx` and `limitations.mdx` against the two tables in this plan

- [ ] **Step 1: Write the failing test** `loader_and_writer_work_on_a_cluster` (Review Focus 5). On a 3-master cluster, for the names `<u>` and `{t}:<u>`, for a `HashMap` and a `HashMapCache`, each with a loader and a write-behind writer (`delay 100ms`):
  - a `get` miss loads;
  - `remove_many` of two keys calls the writer once with both;
  - `flush()` writes the pending entries;
  - none of it fails with `CROSSSLOT`.

  Also a `LocalCachedMap` with a loader.
- [ ] **Step 2: Run** `cargo test --test map_store cluster -- --test-threads 4`. Expected: PASS when Tasks 2–6 are correct; on FAIL, fix the key routing in the task that owns it.
- [ ] **Step 3: Check the comment at the top of `tests/map_store.rs`.** It lists the Redisson tests that were ported (Tasks 2–5) and the ones that were not, with the reason:
  - `testLoaderGetAsync` and `testLoadAllAsync` are merged with their sync twins (one async trait);
  - `testMapLoaderGetWithException` is inverted (open question 1).
- [ ] **Step 4: Run the full verification, one command at a time:**
  - `cargo fmt --check`;
  - `cargo clippy --all-targets -- -D warnings`;
  - `cargo clippy --all-targets --all-features -- -D warnings`;
  - `cargo test -- --test-threads 4`;
  - `cargo doc --no-deps`;
  - `cargo bench --bench compare`, compared with the base branch: the HashMap and HashMapCache rows must stay within noise (`store: None` adds no work).

  Expected: all green; the bench numbers go into the PR.
- [ ] **Step 5: Commit** `test(map): loaders and writers on a cluster; docs for MapOptions parity`

### Task 11: PR and changelog text

- [ ] **Step 1: Open the PR** `feat: MapLoader, MapWriter and LocalCachedMap store options (0c)` with this body. During the PR, verify on the GitHub CI logs, not by running locally.

```markdown
## Summary
HashMap, HashMapCache and LocalCachedMap get Redisson's MapOptions and LocalCachedMapOptions store features: read-through MapLoader with load_all, MapWriter write-through or write-behind with Redisson's defaults and retries, and LocalCachedMap's store mode, cache provider and expiration event policy. Every map method consults the loader or calls the writer where Redisson's does.

## Changelog
### Added
- `MapLoader` and `MapWriter` traits (async, implemented with plain `async fn`), `BoxError`, `WriteMode::{WriteThrough, WriteBehind { batch_size, delay }}` with `WriteMode::write_behind()` (50 entries, 1 s, Redisson's defaults).
- `Client::hash_map_builder` → `HashMapBuilder`, `Client::hash_map_cache_builder` → `HashMapCacheBuilder`; with `LocalCachedMapBuilder` they take `.loader`, `.writer`, `.write_mode`, `.writer_retry_attempts`, `.writer_retry_interval`, then `.build().await`.
- `load_all()` / `load_many(&keys)` with `.replace_existing()` and `.parallelism(n)` (Redisson's `loadAll`), and `flush()` for write-behind, on all three maps.
- `LocalCachedMapBuilder::store_mode(StoreMode::{LocalRedis, Local})`, `::expiration_event_policy(ExpirationEventPolicy::{KeyeventPattern, KeyspaceChannel, None})` (Redisson's `useKeyEventsPattern`), and `::cache_provider(CacheProvider::{Builtin, Moka})` with the optional `moka` feature.
- `Error::Loader` and `Error::Writer`.
- Docs: the guide "Loaders and writers".

### Changed
- `async-trait` is no longer a dependency.

### Behaviour that differs from Redisson on purpose
- A failing loader returns `Error::Loader`; Redisson returns `null`.
- Write-behind keeps the latest operation per key in a batch, so a write followed by a delete ends deleted; Redisson flushes deletes before writes.
- `remove_many` that removes nothing does not call `delete([])`.
- In `StoreMode::Local`, `len()` counts values only, not cached misses, and a remove publishes only when sync or reconnection is on.

### Redis-side deviations from Redisson (recorded per CLAUDE.md)
- The write-behind queue is the list `{name}:redissun_write-behind-queue` with redissun's own payload, because Redisson's queue holds Java-serialized tasks; sharing one key would make each client drop the other's tasks.
- `fastRemoveOperationBatchAsync` is ported verbatim for `HashMap` and `HashMapCache`; no other script changes.
```

- [ ] **Step 2:** Check that the changelog, `loaders-and-writers.mdx` and `limitations.mdx` describe the same lifecycle, defaults and deviations. No commit: Tasks 1–10 hold the commits.
