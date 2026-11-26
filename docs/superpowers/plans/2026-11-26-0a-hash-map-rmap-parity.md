# 0a: HashMap → full RMap parity — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `HashMap` gets every method of Redisson's `RMap`/`RMapAsync` that it lacks today, built in shared modules that `HashMapCache`, `LocalCachedMap`, `MapCacheNative` and `LocalCachedMapCache` reuse in later PRs.

**Architecture:** New logic lives in small `pub(crate)` modules under `src/collections/map/`, not in `hash_map.rs`. They are a key hash for per-key objects, an `AtomicMap` trait with the generic `compute`/`merge` family, a `MapScan` stream and `MapEvents`. `HashMap` implements the trait and exposes thin public methods. Existing `HashMap` methods and tests stay unchanged.

**Tech Stack:** Rust 2024, tokio, redis-rs, `highway` (already a dependency), testcontainers (Redis 7.4 and 8.8).

**Spec:** `docs/superpowers/specs/2026-11-26-redisson-4x-objects-design.md` (section "0. Full map parity first", item 0a).

## What is missing today

The reference is `javap org.redisson.api.RMap` / `RMapAsync` 4.8.0. Methods already present are not repeated here.

| Redisson | redissun after 0a | Task |
|---|---|---|
| `getLock/getFairLock/getReadWriteLock/getSemaphore/getCountDownLatch/getPermitExpirableSemaphore(K)` | `lock(&k)`, `fair_lock`, `rw_lock`, `semaphore`, `count_down_latch`, `permit_expirable_semaphore` | 1 |
| `compute`, `computeIfAbsent`, `computeIfPresent`, `merge` | same names, async closures | 2 |
| `keySet/values/entrySet(pattern, count)`, `keysAsync(count)`, … | `iter_with(MapScan)`, `keys_with`, `values_with` | 3 |
| `readAllKeySet(pattern)`, `readAllValues(pattern)`, `readAllEntrySet(pattern)` | `read_all_keys_matching`, `read_all_values_matching`, `read_all_entries_matching` | 3 |
| `readAllMap`, `getAll(Set)`, `randomEntries(n)` | `read_all_map`, `get_all`, `random_entries` | 3 |
| `putAll(map, batchSize)` | `extend(entries).batch_size(n)` | 4 |
| `addListener(MapPutListener / MapIncrListener / MapRemoveListener / Deleted / Expired)` | `events()`, `events_of(&[MapEventKind])` | 5 |

**Left out, my call (goes into `limitations.mdx` in Task 6):**
- `mapReduce`: redissun has no executor service.
- `addListener(TrackingListener)`: redissun has no RESP3 client tracking on data connections.
- `loadAll`: needs a `MapLoader`, so it moves to 0c.
- `fastPutIfAbsent`: already exists as `insert_nx`. Task 6 adds `#[doc(alias = "fastPutIfAbsent")]`.

## Global Constraints

- `#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::todo, clippy::unimplemented)]`: no unwrap/expect outside tests.
- Key/channel prefix `redissun_` instead of `redisson_`. This PR adds no prefixed names, but per-key object names must match Redisson's byte for byte (Task 1).
- Existing `HashMap` behaviour and the 58 tests in `tests/it/hash_map.rs` must stay green and unchanged.
- One cargo build or test run at a time, with `--test-threads 4`. Never use worktrees.
- New public types are exported from `src/lib.rs` next to `HashMap`.
- Every public item has a doc comment in the existing style, naming the Redisson counterpart.

## Review Focus

1. **Closure that returns the same value / `None` on a missing key.** `compute` with `None → None` must not write or delete. `merge` on a missing key stores the given value without calling the closure. Pinned in Task 2.
2. **Concurrent `merge`/`compute` from many tasks.** No lost update. Pinned in Task 2 (`merge_is_atomic_across_tasks`).
3. **Cancelled `compute` future (timeout while the closure runs).** The per-key lock must not stay held. The `LockGuard` drop path releases it. Pinned in Task 2 (`cancelled_compute_releases_the_lock`).
4. **`MapScan` pattern with no matches, and a hash larger than one page.** The stream ends cleanly and returns every match, not just the first page (Redisson's `readAllKeySet(pattern)` scans only one page; we scan all). Pinned in Task 3.
5. **Events on a server without subkey notifications (7.4).** Field events arrive with `key: None`, and there are no duplicates on 8.8 when both channels fire. Pinned in Task 5.

---

### Task 1: Redisson key hash and per-key objects

**Files:**
- Create: `src/collections/map/mod.rs` (`pub(crate) mod key_hash; pub(crate) mod ops; pub(crate) mod scan; pub(crate) mod events;`; add the later modules as their tasks land)
- Create: `src/collections/map/key_hash.rs`
- Modify: `src/collections/mod.rs` (add `pub(crate) mod map;`)
- Modify: `src/values/bloom_filter.rs` (use the shared `HASH_KEY`)
- Modify: `src/object.rs` (move `suffix_name` here from `src/coordination/rate_limiter.rs`; rate_limiter imports it)
- Modify: `src/collections/hash_map.rs`
- Test: unit tests in `key_hash.rs`; `tests/it/hash_map.rs`

**Interfaces:**
- Produces:
  - `pub(crate) const HASH_KEY: highway::Key` (moved from bloom_filter);
  - `pub(crate) fn hash128_base64(bytes: &[u8]) -> String`;
  - `pub(crate) fn map_key_name(map_name: &str, encoded_key: &[u8], suffix: &str) -> String`, which returns `suffix_name(map_name, &format!("{}:{suffix}", hash128_base64(encoded_key)))`;
  - on `HashMap`: `pub fn lock<Q>(&self, k: &Q) -> Result<Lock>`, `fair_lock -> Result<FairLock>`, `rw_lock -> Result<RwLock>`, `semaphore -> Result<Semaphore>`, `count_down_latch -> Result<CountDownLatch>`, `permit_expirable_semaphore -> Result<PermitExpirableSemaphore>`. They return `Result` because encoding the key can fail. Bounds: `K: Borrow<Q>, Q: Serialize + ?Sized`.

Redisson (`RedissonObject.getLockByMapKey`, `misc/Hash.hash128toBase64`) computes the name as follows. HighwayHash-128 of the encoded key, with the same key as the bloom filter, written as `h[0]` then `h[1]` big-endian (16 bytes). That is standard base64 with padding and the trailing `==` cut off, so 22 chars. The suffixes are `lock`, `fairlock`, `rw_lock`, `semaphore`, `countdownlatch`, `permitexpirablesemaphore`. Write a 15-line private base64 encoder; do not add a dependency.

- [ ] **Step 1: Write failing tests.** The vectors were computed with jshell against redisson-4.8.0.jar and netty 4.2.20:

```rust
#[test]
fn hash_matches_redisson() {
    assert_eq!(hash128_base64(b""), "t6rZwibGo2uy1OSmNVe8pg");
    assert_eq!(hash128_base64(b"a"), "qwvAS12pR0CgRitaypEjUw");
    assert_eq!(hash128_base64(b"\"key\""), "IKm9EBQhlzpVIBxUXDHBWw");
    let big: Vec<u8> = (0u8..100).collect();
    assert_eq!(hash128_base64(&big), "AuNncGCUwMfMuTk0WEL10g");
}

#[test]
fn per_key_name_uses_the_hash_tag() {
    assert_eq!(map_key_name("m", b"a", "lock"), "{m}:qwvAS12pR0CgRitaypEjUw:lock");
    assert_eq!(map_key_name("{m}", b"a", "lock"), "{m}:qwvAS12pR0CgRitaypEjUw:lock");
}
```

Integration test `per_key_lock_lives_under_the_redisson_name` in `tests/it/hash_map.rs`. Take `map.lock("key")?.lock().await?` on a `hash_map::<String, i64>` and assert `raw_command(&["EXISTS", "{<name>}:IKm9EBQhlzpVIBxUXDHBWw:lock"])` is `"1"`. JsonCodec encodes `"key"` as `"key"` with the quotes. Then unlock and assert `"0"`. Integration test `per_key_semaphore_and_latch_work`: `semaphore(&k)?.try_set_permits(2)`, `acquire(1)`. `count_down_latch(&k)?.try_set_count(1)`, `count_down`, `count == 0`.

- [ ] **Step 2: Run them and check they fail.** `cargo test --lib key_hash -- --test-threads 4` and `cargo test --test it hash_map::per_key -- --test-threads 4` fail because the items don't exist.
- [ ] **Step 3: Implement** the items listed in Interfaces. Construct each object with `Key::new(self.key.core.clone(), name)` and its `pub(crate) fn new(key)`.
- [ ] **Step 4: Run them and check they pass.** Run the same commands. Also run `cargo test --lib bloom_filter` (it moved to the shared key).
- [ ] **Step 5: Commit** `feat(hash_map): per-key locks, semaphores and latches with Redisson's names`.

### Task 2: `AtomicMap` core with compute / merge

**Files:**
- Create: `src/collections/map/ops.rs`
- Modify: `src/collections/hash_map.rs`
- Test: `tests/it/hash_map.rs`

**Interfaces:**
- Consumes: `map_key_name` (Task 1), `Lock::lock()` / `LockGuard::unlock()`.
- Produces: the trait below. Every future must be `Send`, which is why it does not use `async fn`:

```rust
pub(crate) trait AtomicMap<V>: Sync {
    fn key(&self) -> &Key;
    fn get_encoded(&self, k: &Bytes) -> impl Future<Output = Result<Option<V>>> + Send;
    fn fast_insert_encoded(&self, k: &Bytes, v: &V) -> impl Future<Output = Result<()>> + Send;
    fn insert_if_absent_encoded(&self, k: &Bytes, v: &V) -> impl Future<Output = Result<Option<V>>> + Send;
    fn fast_insert_if_exists_encoded(&self, k: &Bytes, v: &V) -> impl Future<Output = Result<bool>> + Send;
    fn remove_if_encoded(&self, k: &Bytes, v: &V) -> impl Future<Output = Result<bool>> + Send;
    fn fast_remove_encoded(&self, k: &Bytes) -> impl Future<Output = Result<()>> + Send;
}
pub(crate) async fn compute<M, V, F, Fut>(map: &M, k: Bytes, f: F) -> Result<Option<V>>
    where M: AtomicMap<V>, F: FnOnce(Option<V>) -> Fut + Send, Fut: Future<Output = Option<V>> + Send;
// compute_if_absent(map, k, f: FnOnce() -> Fut<Output = Option<V>>)
// compute_if_present(map, k, f: FnOnce(V) -> Fut<Output = Option<V>>)
// merge(map, k, value: V, f: FnOnce(V, V) -> Fut<Output = Option<V>>)
```

- The lock name is `map_key_name(map.key().name(), &k, "lock")`, so it is the same lock as `HashMap::lock(&k)`. Java and Rust clients therefore exclude each other.
- The public methods on `HashMap` (`pub async fn compute<Q, F, Fut>(&self, k: &Q, f: F) -> Result<Option<V>>`, etc.) encode the key once and call the generic function. The closure gets the current value and not the key (the caller has the key). It returns `Option<V>`: `None` means remove or don't store. It is infallible, my call: a fallible closure can capture its error in a variable.
- The semantics follow `RedissonMap` exactly:
  - `compute`: under the lock, `get`. Then either `fast_insert(new)`, or `fast_remove` if new is `None` and old was `Some`.
  - `compute_if_absent`: first a `get` without the lock (Redisson's sync fast path). If absent, take the lock, `get` again, call the closure, then `insert_if_absent`. Return the stored winner.
  - `compute_if_present`: under the lock, call the closure on `Some(old)`. Then `fast_insert_if_exists(new)` (returns `None` if the entry vanished), or `remove_if(old)` if new is `None`.
  - `merge`: under the lock, if old is `None` the result is `value` without calling the closure. Otherwise it is `f(old, value)`. Then `fast_insert`, or `fast_remove` on `None`.
- Release the lock with `guard.unlock().await?` on the normal path. On cancellation the guard's `Drop` spawns the release.

- [ ] **Step 1: Write failing tests in `tests/it/hash_map.rs`:**
  - `compute_inserts_updates_and_removes`: missing `+ |_| Some(1)` gives `Some(1)`. `|v| v.map(|x| x+1)` gives `Some(2)`. `|_| None` gives `None` and the key is gone.
  - `compute_none_on_missing_writes_nothing`: the result is `None`, `len() == 0`.
  - `compute_if_absent_keeps_existing`: with `a = 1`, the closure is not called (it sets an `AtomicBool`) and the result is `Some(1)`.
  - `compute_if_present_skips_missing`: the closure is not called and the result is `None`.
  - `merge_inserts_then_combines`: `merge(k, 5, |a, b| Some(a + b))` twice gives `Some(5)`, then `Some(10)`.
  - `merge_none_removes`.
  - `merge_is_atomic_across_tasks`: 20 spawned tasks on clones of the map each `merge(k, 1, |a, b| async move { tokio::task::yield_now().await; Some(a + b) })`. The final value is `20`.
  - `cancelled_compute_releases_the_lock`: `tokio::time::timeout(50ms, map.compute(k, |_| async { sleep(10s).await; Some(1) }))` is `Err`. Then `map.lock(k)?.is_locked()` becomes `false` within 2 s (poll every 50 ms).
- [ ] **Step 2: Run** `cargo test --test it hash_map::compute hash_map::merge -- --test-threads 4`. They should fail to compile.
- [ ] **Step 3: Implement** `ops.rs`, `impl AtomicMap<V> for HashMap<K, V, C>` (reuse the existing scripts `PUT_IF_ABSENT`, `FAST_PUT_IF_EXISTS`, `REMOVE_IF`, `HSET`, `HDEL`), and the four public methods.
- [ ] **Step 4: Run** the same command again. Expected: PASS.
- [ ] **Step 5: Commit** `feat(hash_map): compute, compute_if_absent, compute_if_present and merge under the per-key lock`.

### Task 3: Scans with pattern and page size, bulk reads

**Files:**
- Create: `src/collections/map/scan.rs`
- Modify: `src/collections/hash_map.rs` (the existing `iter`/`keys`/`values` become `self.iter_with(MapScan::new())` etc.; behaviour stays the same, page 100)
- Modify: `src/lib.rs` (export `MapScan`)
- Test: `tests/it/hash_map.rs`

**Interfaces:**
- Produces:
  - `#[derive(Clone, Debug, Default)] pub struct MapScan { pattern: Option<String>, page_size: usize }`, with `new()`, `pattern(impl Into<String>)` (`HSCAN … MATCH`) and `page_size(usize)` (`COUNT`; 0 means the default of 100). It follows the style of `KeysScan`. Redisson's `count` argument maps to `page_size`.
  - `pub(crate) fn hscan<'a, K, V, C>(key: &'a Key, codec: &'a C, scan: MapScan) -> impl Stream<Item = Result<(K, V)>> + 'a`, the existing HSCAN loop moved here with the `decode_pairs` helper.
  - On `HashMap`: `iter_with(MapScan)`, `keys_with(MapScan)`, `values_with(MapScan)`, `read_all_keys_matching(&str) -> Result<Vec<K>>`, `read_all_values_matching(&str) -> Result<Vec<V>>`, `read_all_entries_matching(&str) -> Result<Vec<(K, V)>>`. Plus `read_all_map() -> Result<std::collections::HashMap<K, V>>`, `get_all(&[&Q]) -> Result<std::collections::HashMap<K, V>>` (only the keys that are found, Redisson `getAll`; one `HMGET`), and `random_entries(count) -> Result<std::collections::HashMap<K, V>>` (`HRANDFIELD key count WITHVALUES`, reply RESP2 flat / RESP3 pairs via `decode_pairs`). The map-returning methods need `K: Eq + Hash`.
- The patterns match the **encoded** field. With JsonCodec, `"a*"` does not match `"abc"`; `"\"a*"` does. Document this, as Redisson does with its codec.

- [ ] **Step 1: Write failing tests:**
  - `scan_with_pattern_reads_every_page`: 1000 keys `k0..k999` of `hash_map::<String, i64>`, `keys_with(MapScan::new().pattern("\"k1*").page_size(10))`. The collected set has length 111 (`k1`, `k10..k19`, `k100..k199`).
  - `scan_pattern_without_matches_is_empty`.
  - `read_all_keys_matching_is_not_limited_to_one_page`: the same 1000 keys give 111.
  - `read_all_map_and_get_all`: `get_all(&["a", "missing"])` has length 1.
  - `random_entries_returns_existing_pairs`: `random_entries(2)` has length 2, and each value equals `get(key)`.
- [ ] **Step 2: Run** `cargo test --test it hash_map::scan hash_map::read_all hash_map::random_entries -- --test-threads 4`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the same command, plus the whole `hash_map::` set (the existing iter tests). Expected: PASS.
- [ ] **Step 5: Commit** `feat(hash_map): scans with pattern and page size, read_all_map, get_all, random_entries`.

### Task 4: `extend` with batch size

**Files:**
- Modify: `src/collections/hash_map.rs`
- Test: `tests/it/hash_map.rs`

**Interfaces:**
- Produces: `pub fn extend<'a, Q, W>(&self, entries: impl IntoIterator<Item = (&'a Q, &'a W)>) -> Extend<'_, K, V, C>`. The entries are encoded eagerly into `Result<Vec<(Bytes, Bytes)>>` and stored, so the builder does not carry the iterator's lifetime. `Extend` has `.batch_size(n: usize)` (0 or unset means one `HSET`, today's behaviour) and `impl IntoFuture<Output = Result<()>>`. Existing `map.extend(x).await` call sites compile unchanged. With a batch size, it sends one `HSET` per `n` entries, in order (Redisson `putAll(map, batchSize)`). An encode error is returned on `.await`.
- Export `Extend` as `HashMapExtend` in `lib.rs`, so the name doesn't collide with `std::iter::Extend` for users.

- [ ] **Step 1: Write the failing test** `extend_in_batches_writes_everything`: 250 entries with `.batch_size(100)`, `len() == 250`. Also `extend_batch_of_zero_is_one_call` (`.batch_size(0)` works).
- [ ] **Step 2: Run** `cargo test --test it hash_map::extend -- --test-threads 4`. Expected: FAIL (no `batch_size`).
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the same command and the whole `hash_map::` set. Expected: PASS.
- [ ] **Step 5: Commit** `feat(hash_map): extend(..).batch_size(n)`.

### Task 5: Map events

**Files:**
- Create: `src/collections/map/events.rs`
- Modify: `src/collections/hash_map.rs`, `src/lib.rs` (export `MapEvent`, `MapEventKind`, `MapEvents`)
- Test: `tests/it/hash_map.rs`

**Interfaces:**
- Produces:

```rust
#[non_exhaustive] pub enum MapEvent<K> {
    Put { key: Option<K> },          // MapPutListener (hset)
    Incremented { key: Option<K> },  // MapIncrListener (hincrbyfloat)
    Removed { key: Option<K> },      // MapRemoveListener (hdel)
    Deleted,                         // DeletedObjectListener (del)
    Expired,                         // ExpiredObjectListener (expired)
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)] #[non_exhaustive]
pub enum MapEventKind { Put, Incremented, Removed, Deleted, Expired }  // + const ALL
pub struct MapEvents<K, C: Codec> { .. }  // recv().await -> Result<MapEvent<K>>, into_stream()
// HashMap: pub async fn events(&self) -> Result<MapEvents<K, C>>;
//          pub async fn events_of(&self, kinds: &[MapEventKind]) -> Result<MapEvents<K, C>>;
// pub(crate) async fn subscribe<K, C>(key: &Key, codec: C, kinds: &[MapEventKind]) -> Result<MapEvents<K, C>>
```

- Patterns, the same as Redisson's:
  - `Put`: `__subkeyevent@*:hset` and `__keyevent@*:hset`;
  - `Incremented`: the same pair with `hincrbyfloat`;
  - `Removed`: the same pair with `hdel`;
  - `Deleted`: `__keyevent@*:del`;
  - `Expired`: `__keyevent@*:expired`.

  Subscribe with `core.pubsub().await?.psubscribe_with_messages(pattern)`. The message is `(channel, payload)`.
- A `__keyevent` payload is the key name. Keep it only if it equals `key.redis_key()`. The event name is the text after the last `:` of the channel.
- A `__subkeyevent` payload is `<len>:<name>|<len>:<field>[,<len>:<field>…]`. Port `decodeSubkeyEventMessage`/`decodeSubkeyNames` as a pure `fn parse_subkey(payload: &[u8]) -> Option<(&[u8], Vec<&[u8]>)>`. Keep the message only if the name matches. Emit one event per field, with the field decoded by the codec into `key: Some(K)`. A field that fails to decode gives an `Err` item, and the stream continues.
- Deduplication (Redisson `MAP_FIELD_EVENT_DEDUPLICATION_TIMEOUT = 50` ms, simplified): keep a per-event-name flag `subkey_seen`.
  - Once a subkey event for a name has arrived, drop that name's keyevent messages for good.
  - Hold each keyevent message for 50 ms. If no subkey event for that name arrives in that window, emit it as `{ key: None }`.
  - Implement with a `tokio::time::sleep_until` deadline queue inside `recv()` (a `VecDeque<(Instant, MapEvent<K>)>`), together with `tokio::select!` over the receivers. No spawned task, so dropping `MapEvents` unsubscribes everything.
- Lagging gives `Error::Lagged`, as with other subscribers.
- The docs say this needs `notify-keyspace-events`: `Eh` for key events (plus `g` for del and `x` for expired), and `Th` for field names (Redis 8.8+). The probe showed `T` gives `__subkeyevent`; `S` gives `__subkeyspace`, which Redisson doesn't use.

- [ ] **Step 1: Write failing tests.**
  - Unit tests in `events.rs` for `parse_subkey`:
    - `b"1:m|2:f1,2:f2"` gives `(b"m", [b"f1", b"f2"])`;
    - wrong length, missing `|` and an empty field list give `None`.
  - Integration test `events_without_subkey_notifications_have_no_key`: on `redis_url()` (7.4), `raw_command(["CONFIG", "SET", "notify-keyspace-events", "Egxh"])`. `events()`, `insert`, then `recv()` within 2 s gives `Put { key: None }`. After `remove`: `Removed { key: None }`. After `del()`: `Deleted`. Use `own_redis()` so the config change doesn't leak into other tests.
  - Integration test `events_with_subkey_notifications_name_the_key`: on an own Redis 8.8 container with `ETh`. `insert("a")` gives exactly one `Put { key: Some("a") }`; a second `recv()` with a 300 ms timeout times out (no duplicate).
  - `events_of_filters_kinds`: `events_of(&[MapEventKind::Removed])` ignores an insert.
- [ ] **Step 2: Run** `cargo test --lib map::events` and `cargo test --test it hash_map::events -- --test-threads 4`. Expected: FAIL.
- [ ] **Step 3: Implement.** Add an `own_redis8()` helper to `tests/common/mod.rs` next to `own_redis()` if there isn't one.
- [ ] **Step 4: Run** the same commands. Expected: PASS.
- [ ] **Step 5: Commit** `feat(hash_map): events for put, increment, remove, delete and expiry`.

### Task 6: Ported Redisson tests, docs, final check

**Files:**
- Test: `tests/it/hash_map.rs`
- Modify:
  - `README.md` (HashMap row);
  - `CHANGELOG.md` (Unreleased);
  - `website/content/docs/guides/redisson-migration.mdx` (an RMap method mapping table with the names from "What is missing today");
  - `website/content/docs/guides/limitations.mdx` (mapReduce, TrackingListener, loadAll → 0c, the single-page pattern scan difference);
  - `src/collections/hash_map.rs` (doc aliases).

- [ ] **Step 1: Fetch the Redisson tests for this tag.** Get `RedissonMapTest.java` and `BaseMapTest.java` from GitHub tag `redisson-4.8.0` (`redisson/src/test/java/org/redisson/`) and save them to the scratchpad. Port the tests that cover Task 1 to Task 5 methods and are not already ported: `testCompute*`, `testMerge*`, `testComputeIfAbsent*`, `testComputeIfPresent*`, `testGetAll*`, `testReadAllMap`, `testRandomEntries`, `testKeySetByPattern`, `testValuesByPattern`, `testEntrySetByPattern`, `testPutAllBatched`, `testAddListener*`/`testMapPutListener`/`testMapRemoveListener`/`testMapIncrListener`. Name each test `redisson_<snake_case>`. Skip ones that need a loader/writer (they belong to 0c) and list them in the commit message.
- [ ] **Step 2: Run** `cargo test --test it hash_map:: -- --test-threads 4`. Expected: all pass. Fix the implementation, not the ported assertions, unless Redisson behaviour differs on purpose (only the pattern scan does; document that).
- [ ] **Step 3: Add doc aliases** (`#[doc(alias = "getLock")]`, `"putAll"`, `"fastPutIfAbsent"`, `"readAllMap"`, `"addListener"`, …) and the doc updates listed under Files.
- [ ] **Step 4: Run the full verification, one command at a time:** `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test -- --test-threads 4`, `cargo bench --bench compare` HashMap rows compared with the same run on `main` (`insert`/`get` must not be slower). Expected: all green and the bench within noise.
- [ ] **Step 5: Commit** `test(hash_map): port Redisson RMap tests; docs for RMap parity`, then open the PR `feat: HashMap reaches RMap parity (0a)`. During the PR, verify on GitHub CI logs, not by running locally.
