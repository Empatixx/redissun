# 1: MapCacheNative (Redisson `RMapCacheNative`) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A new, experimental `MapCacheNative<K, V, C>` with every method of Redisson 4.8.0's `RMapCacheNative` (39 methods in `javap org.redisson.api.RMapCacheNative`) and of `RMap`. It is a plain Redis hash whose fields expire on the server (Redis 7.4 hash field expiration). Every name it shares with `HashMap` or `HashMapCache` behaves the same way. It supports 0c's loader and writer, per-key objects, `entry()` with a TTL, `expire_entry` with `.if_not_set()/.if_greater()/.if_less()`, keyspace events including per-field expiry, and `Object`.

**Architecture:** `RedissonMapCacheNative extends RedissonMap`. In the same way, `MapCacheNative` wraps a `HashMap<K, V, C>` on the same raw key and hands every RMap method to it, so the loader, writer, write-behind, scans, `Entry` and per-key names are 0a/0c code and are not copied. The RMapCacheNative surface is built on top: Redisson's nine Lua scripts from `RedissonMapCacheNative.java`, copied verbatim into `src/collections/map_cache_native/scripts.rs`, the plain commands Redisson sends (`HPTTL`, `HPERSIST`, `HSETEX`), and `IntoFuture` builders with `.ttl(d)`/`.expire_at(t)`. Without a time, a builder takes `HashMap`'s path. The map's private `Target` enum gets a `Native` variant, so `entry(&k).ttl(d)` writes through Redisson's `fastPut`-with-TTL script. `MapEvent` gets two variants for Redisson's `MapExpiredListener` and `MapClearExpireListener`.

**Tech Stack:** Rust 2024, tokio, redis-rs, testcontainers (Redis 7.2 for the unsupported path, 7.4, 8.8, a 7.4 Cluster).

**Spec:** `docs/superpowers/specs/2026-11-26-redisson-4x-objects-design.md` (section "### 1. `MapCacheNative`", "API conventions", "Constraints", "Testing") and `docs/superpowers/specs/2026-11-27-rust-idiomatic-api.md`, whose naming rules replace the spec's naming lines. Builds on `docs/superpowers/plans/2026-11-27-0b-hash-map-cache-rmapcache-parity.md` and `2026-11-27-0c-map-loader-writer.md`.

**Branch:** `feat/map-cache-native`, from `main` after `feat/map-loader-writer` (0c) is merged.

## Owner decisions (delegated, 2026-11-27)

1. **Same name ⇒ same behaviour wins: `replace` and `compare_and_set` keep the entry's TTL**, like `HashMapCache::replace`. Plain writes (`insert`, `set`, `extend`, `entry` without `.ttl()`) clear it, like `HashMapCache`. To stay on Redis 7.4, `replace` and `compare_and_set` run Redisson's script with one recorded deviation: they read `HPTTL` of the field before the `HSET` and re-apply it with `HPEXPIRE` after, atomically in the same script. Each such `static` carries a `/// Deviation from Redisson: ...` comment, and the PR lists it. `replace(..).ttl(d)` still sets a new TTL. Tests: `replace_keeps_the_ttl`, `compare_and_set_keeps_the_ttl`.

## Decisions (mine, by the rules: Rust-idiomatic, Redisson parity, same name ⇒ same behaviour)

- **Composition, not copy.** `struct MapCacheNative<K, V, C: Codec> { key: Key, codec: C, map: HashMap<K, V, C> }`. `key` and `codec` are clones of the map's, so `per_key_objects!()` works unchanged. There is no `Deref` to `HashMap`: it would leak `HashMap::events` (fewer kinds) and HashMap's non-builder `insert` into the docs.
- **Names (spec names replaced by the naming rules):**

  | Spec name | Plan name | Why |
  |---|---|---|
  | `fast_insert` | `set(..)[.ttl/.expire_at]` | `HashMap::set` is `fastPut` |
  | `insert_nx` | `try_insert(..)[.ttl/.expire_at]` → `Option<V>` | rule 1; the script returns the present value |
  | `insert_if_exists` | `replace(..)[.ttl/.expire_at]` → `Option<V>` | rule 2, `HashMap::replace` |
  | `extend_with(..).ttl(d)` | `extend(..)[.batch_size][.ttl/.expire_at/.keep_ttl]` | `HashMap::extend` |
  | `insert_if_all_absent(entries, Expiry)` | `try_extend(entries)[.ttl/.expire_at/.keep_ttl]` → `bool` | `HashSet::try_extend` is the crate's all-or-nothing NX |
  | `insert_if_all_exist(entries, Expiry)` | `update_many(entries)[…]` → `bool` | rule 2 (`update` for XX → `bool`) plus the crate's `_many` suffix |
  | `expire_entry(&k, Expiry).condition(c)` | `expire_entry(&k).ttl(d)/.expire_at(t)[.if_not_set()/.if_greater()/.if_less()]` | the same shape as `HashMapCache::expire_entry` and ruling 7 (`.if_*()` on the builder) |
  | `entry_ttls(&keys) -> HashMap<K, Option<Duration>>` | `entry_ttls(&[&Q]) -> Vec<Option<Duration>>` in key order | the same as `get_many`/`peek_many`; no `Hash` bound |
  | `get_or_insert_with(&k, Expiry, f)`, `compute(&k, Expiry, f)` | `entry(&k).ttl(d)` / `.expire_at(t)` | the same as `HashMapCache::entry(&k).ttl(d)` |
  | `persist_entry`, `persist_entries` | the same; `persist_entries` → `Vec<bool>` | Redisson's `clearExpire(Set)` returns one flag per key |

- **A time is `.ttl(Duration)` or `.expire_at(SystemTime)`**, two builder methods (spec "API conventions"). The one set last wins. `Duration::ZERO` means "no TTL", as Redisson's `ms == 0 → put(k, v)` does and as `HashMapCache` does. A `SystemTime` before 1970, or a value that overflows `i64` milliseconds, is `Error::Config`. A time in the past is passed through: Redis deletes the field at once (`HPEXPIREAT` returns 2), as in Redisson.
- **`expire_entry` without a time** (or with `.ttl(ZERO)`) clears the TTL, as `HashMapCache::expire_entry` clears limits. It runs `HPERSIST` and returns `true` when the field exists (`1` or `-1`), the meaning `HashMapCache`'s `bool` has. Redisson's `expireEntry(k, ZERO)` sends `hpexpire 0`, which deletes the entry. That difference is recorded. A condition without a time is `Error::Config("expire_entry with a condition needs ttl or expire_at")`.
- **`persist_entry`** is Redisson's `clearExpire`: `true` only when a TTL was removed (`HPERSIST` = 1, Redisson's `BooleanReplayConvertor`).
- **`extend(..).ttl(d)`** uses Redisson's Lua `putAllOperationAsync(map, ms)` (`hset` + `hpexpire` per entry, which runs on Redis 7.4). `.expire_at(t)` and `.keep_ttl()` use Redisson's `HSETEX` (`putAll(PutArgs)`, Redis 8.0), since Redisson has no Lua for them. Both store the same data. The Lua keeps `.ttl` usable on the object's minimum server.
- **No command swaps for speed.** Spec "Optimisation": `HSETEX` cannot replace the single-key scripts. Their replies (old value, "added") differ, and it would raise the minimum to 8.0. `HPTTL` is already Redisson's single command. Redisson 4.8.0 does not use `HGETEX`, so neither does redissun.
- **Minimum server, checked on first use.** Each method that needs field TTLs first calls `support::require(&key, Needs::FieldTtl | Needs::Hsetex)`. The first call per client sends `COMMAND INFO HPEXPIRE HSETEX`, routed by the map's key, and caches the answer in a `Core` `OnceCell`. A missing command gives `Error::Unsupported("MapCacheNative needs Redis 7.4 or newer (HPEXPIRE)")` or `"… needs Redis 8.0 or newer (HSETEX)"`. This probes for the command, not the version string, so Valkey 9 works. It runs *before* any write, because a Lua script that fails at `hpexpire` has already done its `hset` (Review Focus 3). Methods that come from `RMap` do not check: they run on 6.2.
- **Events.** `MapEvent::EntryExpired { key: Option<K> }` (`hexpired`, Redisson's `MapExpiredListener`) and `MapEvent::EntryPersisted { key: Option<K> }` (`hpersist`, `MapClearExpireListener`), with matching `MapEventKind`s. Both enums are already `#[non_exhaustive]`. `MapCacheNative::events()` listens to all 7 kinds. `HashMap::events_of`/`HashMapCache::keyspace_events_of` with the two new kinds is `Error::Config`, because Redisson's `RMap.addListener` does not take them (parity).
- **Keys listed twice** in `entry_ttls`/`persist_entries`/`expire_entries` are sent as given (Redisson takes a `Set`). The `Vec` replies have one item per input. `expire_entries` counts each listed key. Documented.
- **Loader and writer:** `client.map_cache_native_builder::<K, V>(name)` with `store_options!()` and `.build().await`, Redisson's `getMapCacheNative(MapOptions)`. Read-through is `HashMap`'s (`RedissonMapCacheNative` does not override `getAsync`). A loaded value is stored with `HSET`, so it has no TTL, as with Redisson's `loadValue` → `put`. Writer calls follow `mapWriterFuture` and its conditions (table below). `expire_entry*` and `persist_*` call no writer.
- **What Redisson's `MapCacheNative` lacks compared with `MapCache`, and redissun leaves out (parity):** max size and eviction mode (`setMaxSize`, `trySetMaxSize`), max idle, entry events with values (`MapEntryListener`), `getWithTTLOnly` (`peek`), leases (`RLeasedMap`), `updateEntryExpiration`, the eviction task. Also not ported, as on the other maps: `sizeInMemory`, `destroy`, `mapReduce`, `expireAt`/conditional `expire` on the whole object, batch views (spec non-goal).

## Redis commands per method (Redisson 4.8.0 `RedissonMapCacheNative.java`, line numbers of the method that holds the script)

| redissun | Redisson | Redis | Writer |
|---|---|---|---|
| `insert(&k,&v)` | `RMap.put` | `HashMap::insert` (`HGET`+`HSET`) | always |
| `insert(..).ttl/.expire_at` | `put(k,v,ttl/Instant)` | `PUT` (`putOperationAsync` 105) | always |
| `set(&k,&v)` | `fastPut` | `HashMap::set` (`HSET`) | always |
| `set(..).ttl/.expire_at` | `fastPut(k,v,ttl/Instant)` | `FAST_PUT` (`fastPutOperationAsync` 157) | always |
| `try_insert(&k,&v)` | `putIfAbsent`, `fastPutIfAbsent` | `HashMap::try_insert` | stored (`None`) |
| `try_insert(..).ttl/.expire_at` | `putIfAbsent/fastPutIfAbsent(k,v,ttl/Instant)` | `PUT_IF_ABSENT` (`putIfAbsentOperationAsync` 208) | stored |
| `replace(&k,&v)` | `putIfExists`, `replace(k,v)`, `fastReplace` | `HashMap::replace` | present (`Some`) |
| `replace(..).ttl/.expire_at` | `putIfExist(k,v,ttl/Instant)` | `PUT_IF_EXIST` (`putIfExistOperationAsync` 920) | present |
| `extend(..)` | `putAll(map[, batch])` | `HashMap::extend` (`HSET` per batch) | per batch |
| `extend(..).ttl(d)` | `putAll(map, ttl)` (deprecated) | `PUT_ALL_TTL` (`putAllOperationAsync(map, ms)` 396) per batch | per batch |
| `extend(..).expire_at(t)/.keep_ttl()` | `putAll(PutArgs)` | `HSETEX key PXAT t` or `KEEPTTL`, `FIELDS n …` (`putAllOperationAsync(PutParams)` 351) per batch | per batch |
| `try_extend(..)[…]` | `putIfAllKeysAbsent(PutArgs)` | `HSETEX key FNX [PX, PXAT or KEEPTTL] FIELDS n …` (`putAllKeysOperationAsync` 970) | `true` |
| `update_many(..)[…]` | `putIfAllKeysExist(PutArgs)` | `HSETEX key FXX …` (same) | `true` |
| `entry_ttl(&k)` | `remainTimeToLive(K)` | `HPTTL key FIELDS 1 k` (299) | — |
| `entry_ttls(&ks)` | `remainTimeToLive(Set)` | `HPTTL key FIELDS n …` (312) | — |
| `expire_entry(&k).ttl/.expire_at` | `expireEntry` | `EXPIRE_ENTRY` (`expireEntryAsyncInternal` 429) | none |
| `… .if_not_set()/.if_greater()/.if_less()` | `expireEntryIfNotSet/IfGreater/IfLess` | `EXPIRE_ENTRY_IF` (`expireEntryAsync(param, …)` 462), `NX`/`GT`/`LT` | none |
| `expire_entries(&ks)…` | `expireEntries[IfNotSet/IfGreater/IfLess]` | `EXPIRE_ENTRIES` (495), `EXPIRE_ENTRIES_IF` (534) | none |
| `expire_entry(&k)` / `expire_entries(&ks)` without a time | — (redissun) | `HPERSIST key FIELDS n …` | none |
| `persist_entry`, `persist_entries` | `clearExpire(K)`, `clearExpire(Set)` | `HPERSIST` (640, 651) | none |
| `entry(&k)[.ttl/.expire_at]` | `compute(k[, ttl], f)`, `computeIfAbsent(k[, ttl], f)` (718, 796) | under `KeyLock`: `HGET`; write `HSET` or `FAST_PUT`; remove `HDEL` | entry writes/removes |
| `events()`, `events_of(..)` | `addListener(ObjectListener)` (666) | keyspace patterns, plus `hexpired`/`hpersist` | — |
| every other RMap method | `RedissonMap` | `HashMap`'s | as in 0c |

`.expire_at(t)` sends `hpexpireat` with `t` in Unix milliseconds as the script's `ARGV`, where Redisson's `getExpireCommand(false)` sends it. `.ttl(d)` sends `hpexpire` with `d` in ms. `HSETEX` gets `PX <ms>`, `PXAT <ms>` or `KEEPTTL` from `Expiry` through the existing `expiry_args`.

## Global Constraints

- `#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::todo, clippy::unimplemented)]`: no unwrap/expect in library code. Tests may unwrap.
- Lua: the nine `static`s in `scripts.rs` (`PUT`, `FAST_PUT`, `PUT_IF_ABSENT`, `PUT_IF_EXIST`, `PUT_ALL_TTL`, `EXPIRE_ENTRY`, `EXPIRE_ENTRY_IF`, `EXPIRE_ENTRIES`, `EXPIRE_ENTRIES_IF`) are copied verbatim from the line ranges above, with KEYS and ARGV in Redisson's order. Each has a `/// RedissonMapCacheNative.<method>` comment. No other new Lua.
- Data: the hash lives at the raw name (`Key::new(core, name)`, no tag added), like `HashMap` and Redisson's `getRawName()`. No companion keys. The per-key lock is `map_key_name(name, k, "lock")`, the 0c write-behind queue `suffix_name(name, "redissun_write-behind-queue")`. Redisson and redissun clients share the data.
- Minimum server: Redis 7.4 or Valkey 9 (`HPEXPIRE`) for every TTL method; Redis 8.0 (`HSETEX`) for `extend(..).expire_at/.keep_ttl`, `try_extend` and `update_many`. Checked on first use (see Decisions); RMap methods have no check.
- `Duration::ZERO` passed to `.ttl()` means no TTL. A time that does not fit `i64` ms is `Error::Config` (`object::millis`).
- Every public name that `HashMap`/`HashMapCache` also has keeps their signature shape and behaviour, and carries `#[doc(alias = "<Redisson name>")]`. Optional settings are builder methods, never `bool`/sentinel parameters.
- Builders are exported in `pub mod map_cache_native` in `lib.rs` (`Insert`, `Set`, `TryInsert`, `Replace`, `Extend`, `TryExtend`, `UpdateMany`, `ExpireEntry`, `ExpireEntries`, `EntryRequest`, plus `LoadAll` and `hash_map::{Entry, OccupiedEntry, VacantEntry}`). `MapCacheNative` and `MapCacheNativeBuilder` are at the crate root.
- No private trait in a public bound (`private_bounds`); the shared behaviour goes through the private `Target` enum.
- `MapCacheNative` is **experimental** in the README, its docs page and its rustdoc (first line of the type doc: "**Experimental.**").
- One cargo build or test run at a time, `--test-threads 4`. No git worktrees.
- Docs go in the task that changes the behaviour: `website/content/docs/objects/map-cache-native.mdx` (new, added to `objects/meta.json` after `"hash-map-cache"`), `guides/redisson-migration.mdx` (naming row `RMapCacheNative` → `MapCacheNative`, and a `## MapCacheNative` table), `guides/limitations.mdx`, `guides/loaders-and-writers.mdx` (one line: also `map_cache_native_builder`), `README.md`.

## Review Focus

1. **A write without a time over an entry that has a TTL.** `insert`, `set`, `replace`, `compare_and_set`, `extend` and a plain `entry` write all go through `HSET`, so the entry becomes permanent. `incr_by` keeps the TTL (`HINCRBY`). A user who reads the docs expects exactly that, and expects `insert(..).ttl(d)` to replace an old TTL. Pinned in Task 1 (`the_server_clears_a_field_ttl_on_hset_and_keeps_it_on_hincrby`, a contract test on the server itself) and Task 2 (`plain_writes_clear_the_ttl`, `incr_by_keeps_the_ttl`).
2. **Times in the past and zero.** `insert(..).expire_at(past)` must return the previous value and leave the key missing (Redis deletes it at once). `.ttl(Duration::ZERO)` must store a permanent entry, never delete it (Redisson's `hpexpire 0` would). Pinned in Task 2 (`a_past_expire_at_deletes_at_once`, `a_zero_ttl_means_no_ttl`) and Task 3 (`expire_entry_with_zero_clears_the_ttl`).
3. **A server without field TTLs.** On Redis 7.2, every TTL method must return `Error::Unsupported` and must not write anything. Lua does not roll back an `hset` before a failing `hpexpire`. On 7.4, the `HSETEX` methods must return `Error::Unsupported` the same way. RMap methods keep working. Pinned in Task 1 (`ttl_methods_need_redis_7_4_and_write_nothing`) and Task 4 (`hsetex_methods_need_redis_8`).
4. **Conditions on entries without a TTL or without a value.** In Redis, a field without a TTL counts as an infinite TTL. So `.if_greater()` on such an entry is `false`, `.if_less()` is `true`, `.if_not_set()` is `true`, and each is `false` on a missing key. `expire_entries` counts only the entries it changed. Pinned in Task 3 (`conditions_follow_redis_on_persistent_and_missing_entries`).
5. **Names with a hash tag, and Redis Cluster.** `{t}:m` is one key. Every script, `HPTTL`/`HPERSIST`, the per-key lock and the capability probe must run on a 3-master cluster without `CROSSSLOT`, routed to the key's master. Pinned in Task 6 (`every_operation_runs_on_a_cluster`).

---

### Task 1: The type, factories, builder, RMap surface, `Object` and the capability check

**Files:**
- Create:
  - `src/collections/map_cache_native/mod.rs` (type, `Clone`/`Debug`/`HasKey`, RMap delegations, builder)
  - `src/collections/map_cache_native/support.rs` (`Needs`, `require`, `parse_command_info`)
  - `website/content/docs/objects/map-cache-native.mdx`
- Modify:
  - `src/collections/mod.rs` (`pub(crate) mod map_cache_native;`)
  - `src/client/core.rs` (field `pub(crate) hash_field_support: tokio::sync::OnceCell<HashFieldSupport>`)
  - `src/client/mod.rs` (two factories)
  - `src/collections/hash_map.rs` (make `written`, `deleted`, `writer`, `decode_value` `pub(crate)`; no behaviour change)
  - `src/lib.rs`, `website/content/docs/objects/meta.json`, `README.md`, `guides/redisson-migration.mdx`
  - `tests/common/mod.rs`: `pub async fn own_redis_tag(tag: &str) -> (ContainerAsync<Redis>, String)`; `own_redis` calls it with `"7.4"`
- Test: `tests/map_cache_native.rs` (new), unit tests in `support.rs`

**Interfaces:**
- Produces:
  - `pub struct MapCacheNative<K, V, C: Codec>`, `Clone`, `Debug` (`key.describe(f, "MapCacheNative")`), `HasKey` (`key()` only, so `Object` works on the one hash).
  - `Client::map_cache_native<K, V>(&self, name: impl Into<Arc<str>>) -> MapCacheNative<K, V, C>` and `Client::map_cache_native_builder<K, V>(&self, name: impl Into<Arc<str>>) -> MapCacheNativeBuilder<K, V, C>`, both `#[doc(alias = "getMapCacheNative")]`.
  - `pub struct MapCacheNativeBuilder<K, V, C: Codec> { key, codec, options: StoreOptions<K, V> }` with `store_options!()` and `pub async fn build(self) -> Result<MapCacheNative<K, V, C>>` (`options.into_store(..)` → `HashMap::with_store`).
  - Delegations with `HashMap`'s exact signatures and bounds, each a one-line body over `self.map` and a doc line "Same as [`HashMap::<name>`](crate::HashMap::<name>)" plus anything specific to this type: `get`, `remove`, `contains_key`, `len`, `is_empty`, `clear`, `get_many`, `load_all`, `load_many`, `incr_by`, `incr_by_float` (doc: keeps the entry's TTL), `compare_and_set` (doc: clears the TTL), `remove_if_eq`, `remove_many`, `flush`, `contains_value`, `value_len`, `random_keys`, `random_entries`, `to_hash_map`, `iter`, `keys`, `values`, `iter_with`, `keys_with`, `values_with`. `per_key_objects!();` for the six per-key objects. `insert`, `set`, `try_insert`, `replace` and `extend` come in Tasks 2 and 4, `entry` in Task 5, `events` in Task 6. The loader is consulted exactly where `HashMap` consults it.
  - `support.rs`: `pub(crate) struct HashFieldSupport { field_ttl: bool, hsetex: bool }`, `pub(crate) fn parse_command_info(reply: &redis::Value) -> HashFieldSupport` (a `nil` entry means a missing command), `pub(crate) enum Needs { FieldTtl, Hsetex }` and `pub(crate) async fn require(key: &Key, needs: Needs) -> Result<()>`, which fills the cell with `key.query(COMMAND INFO HPEXPIRE HSETEX)` once per client. `Hsetex` implies `FieldTtl`.
- [ ] **Step 1: Write failing unit tests in `support.rs`.** `parse_command_info` of `[array, array]` gives both `true`; `[array, nil]` gives `field_ttl` only; `[nil, nil]` gives neither; a non-array gives neither.
- [ ] **Step 2: Write failing integration tests in `tests/map_cache_native.rs`.** The file header comment lists the Redisson tests ported (filled in by Task 6). Helper `native(name) -> MapCacheNative<String, String, JsonCodec>` on `redis_url()` (7.4).
  - `the_server_clears_a_field_ttl_on_hset_and_keeps_it_on_hincrby` (Review Focus 1): use `raw_command`: `HSET h f 1`, `HPEXPIRE h 60000 FIELDS 1 f`, `HSET h f 2`, then `HPTTL h FIELDS 1 f` is `-1`. Then `HPEXPIRE …` again, `HINCRBY h f 1`, and `HPTTL` is `> 0`. If this fails on the server, stop and report: the docs of Tasks 2–5 depend on it.
  - `rmap_methods_work_like_hash_map`: `get`, `remove`, `contains_key`, `len`, `get_many`, `compare_and_set`, `remove_if_eq`, `remove_many`, `incr_by`, `contains_value`, `value_len`, `to_hash_map`, `iter_with(MapScan::new().pattern("\"k1*"))` on a `MapCacheNative` give what the same calls give on a `HashMap` with the same name prefix and data.
  - `shares_data_with_hash_map`: after `client.hash_map(n).insert("a", "1")`, `native(n).get("a")` is `Some("1")`. `HGETALL n` (the raw name, no braces) holds it.
  - `object_methods_cover_the_hash`: `expire(1s)`, `ttl()` is `Some`, `persist()`, `rename(n2)`, then `exists()` and `del()`.
  - `per_key_lock_lives_under_the_redisson_name`: `lock("a")` taken, then `EXISTS {<n>}:<hash128 of "\"a\"">:lock` is `:1`. Use the same expected hash as the 0a test.
  - `ttl_methods_need_redis_7_4_and_write_nothing` (Review Focus 3): `let (_c, url) = own_redis_tag("7.2-alpine")`. `entry_ttl("a")` is `Err(Error::Unsupported(m))` with `m` containing `"7.4"`. `get`/`insert` without a time work. After Task 2, add `insert("b", "1").ttl(60s)` → `Unsupported`, then `HEXISTS n "\"b\""` is `:0`.
  - `builder_without_options_reads_like_the_factory`: `map_cache_native_builder(n).build().await?` reads the same data.
- [ ] **Step 3: Run** `cargo test --lib map_cache_native -- --test-threads 4`, then `cargo test --test map_cache_native -- --test-threads 4`. Expected: FAIL (compile: `map_cache_native` missing).
- [ ] **Step 4: Implement.** Add `entry_ttl<Q>(&self, k: &Q) -> Result<Option<Duration>>` here (alias `remainTimeToLive`), because it is the method the unsupported test needs: `require(FieldTtl)`, then `self.key.read(HPTTL key FIELDS 1 k)` → `Vec<i64>`. `-2`/`-1`/missing give `None`, and `n >= 0` gives `Some(Duration::from_millis(n))`. Docs: create `map-cache-native.mdx` with the intro (experimental; Redis 7.4+/Valkey 9, 8.0 for the `HSETEX` methods; a short comparison with `HashMapCache`: native expiry and no clean-up task, but no max size, max idle, entry events or leases), the method table so far and "How it is stored" (one hash at the raw name, shared with Redisson). `meta.json`: `"map-cache-native"` after `"hash-map-cache"`. README row: `| [`MapCacheNative`](https://empatixx.github.io/redissun/docs/objects/map-cache-native) | hash with per-field TTL (Redis 7.4+) | experimental |`, after `HashMapCache`. Migration guide: naming row `| `RMapCacheNative` | `MapCacheNative` |` and a `## MapCacheNative` section ("Every `RMap` method works as on `HashMap`; the table lists the rest"), filled in by each task.
- [ ] **Step 5: Run** the two commands of Step 3, then `cargo test --test it hash_map -- --test-threads 4` (0a tests, unchanged). Expected: `test result: ok.` for each.
- [ ] **Step 6: Commit** `feat(map_cache_native): MapCacheNative over HashMap, factories, builder and the Redis 7.4 check`

### Task 2: Single-key writes with a TTL, `entry_ttls`

**Files:**
- Create: `src/collections/map_cache_native/scripts.rs` (`PUT`, `FAST_PUT`, `PUT_IF_ABSENT`, `PUT_IF_EXIST`), `src/collections/map_cache_native/builders.rs`, `src/collections/map_cache_native/ttl.rs`
- Modify: `mod.rs`, `src/lib.rs` (`pub mod map_cache_native { … }`)
- Test: `tests/map_cache_native.rs`, unit tests in `ttl.rs`

**Interfaces:**
- Produces:
  - `ttl.rs`: `#[derive(Clone, Copy, Debug)] pub(crate) enum FieldTtl { After(Duration), At(SystemTime) }` and `fn script_args(self) -> Result<Option<(i64, &'static str)>>`: `After(ZERO)` → `None`; `After(d)` → `(millis(d)?, "hpexpire")`; `At(t)` → `(unix ms, "hpexpireat")`, or `Error::Config("time is before 1970")`.
  - Builders, each `#[must_use]`, `Debug`, with `pub fn ttl(self, Duration) -> Self` and `pub fn expire_at(self, SystemTime) -> Self` (the last call wins), resolving through `IntoFuture` with a boxed `Send` future:
    - `pub fn insert<'a, Q, W>(&'a self, k: &'a Q, v: &'a W) -> Insert<'a, K, V, C, Q, W>` → `Result<Option<V>>`. Alias `put`.
    - `pub fn set<'a, Q, W>(…) -> Set<'a, K, V, C, Q, W>` → `Result<bool>` (`true` = the key was new, `hset`'s reply). Alias `fastPut`.
    - `pub fn try_insert<'a, Q, W>(…) -> TryInsert<'a, K, V, C, Q, W>` → `Result<Option<V>>` (`None` = stored). Aliases `putIfAbsent`, `fastPutIfAbsent`.
    - `pub fn replace<'a, Q, W>(…) -> Replace<'a, K, V, C, Q, W>` → `Result<Option<V>>` (`None` = missing, nothing stored). Aliases `putIfExist`, `putIfExists`, `fastPutIfExists`, `fastReplace`.
  - Bounds as on `HashMap::insert` (`K: Borrow<Q>`, `V: Borrow<W>`, `Q/W: Serialize + ?Sized + Sync`).
  - Without a time (or with `None` from `script_args`), each awaits `self.map.<same method>(k, v)`, so writer, retries and `eval_once` are `HashMap`'s. With a time, each runs `require(FieldTtl)`, then the script with KEYS `[name]` and ARGV `[ms, k, v, command]`, then the writer per the table (`self.map.written(k, v)`). Like `HashMap::try_insert`, `try_insert` and `replace` are sent with `eval_once`. `set` and `insert` use `eval`.
  - `pub async fn entry_ttls<Q>(&self, ks: &[&Q]) -> Result<Vec<Option<Duration>>>`: in key order; empty input gives an empty `Vec` without a call. Alias `remainTimeToLive`.
- [ ] **Step 1: Write failing unit tests in `ttl.rs`:** `After(ZERO)` gives `None`; `After(1500ms)` gives `(1500, "hpexpire")`; `At(UNIX_EPOCH + 10s)` gives `(10000, "hpexpireat")`; `At(UNIX_EPOCH - 1s)` is `Error::Config`; `After(Duration::MAX)` is `Error::Config`.
- [ ] **Step 2: Write failing integration tests.**
  - `insert_with_ttl_returns_the_old_value_and_expires`: `insert("a","1").ttl(300ms)` is `None`, `insert("a","2").ttl(300ms)` is `Some("1")`, `entry_ttl` is ≤ 300 ms, and after 500 ms `get` is `None`.
  - `insert_with_expire_at_expires`: `.expire_at(now + 300ms)`, then the same checks.
  - `set_with_ttl_reports_a_new_key`: `true`, then `false`; after expiry, `true`.
  - `try_insert_with_ttl_keeps_the_live_value`: `None`, then `Some("1")` with the value unchanged; after expiry, `None` and stored.
  - `replace_with_ttl_only_touches_present_keys` (Redisson `testPutIfExistWithDuration/Instant/…Expiration`): on a missing key `None` and still missing; on a present key `Some(old)`, new value, and the new TTL applies.
  - `plain_writes_clear_the_ttl` (Review Focus 1): for each of `insert`, `set`, `replace`, `compare_and_set`, start from `insert("a","1").ttl(60s)`, write without a time, then `entry_ttl` is `None`.
  - `incr_by_keeps_the_ttl` (Review Focus 1): `insert("n", &1)` on a `MapCacheNative<String, i64>` with `.ttl(60s)`, then `incr_by("n", 2)` is `3` and `entry_ttl` is `Some`.
  - `a_zero_ttl_means_no_ttl` (Review Focus 2): `insert("a","1").ttl(Duration::ZERO)` stores it, and `entry_ttl` is `None`. The same for `set`, `try_insert` and `replace`.
  - `a_past_expire_at_deletes_at_once` (Review Focus 2): `insert("a","1")`, then `insert("a","2").expire_at(UNIX_EPOCH + 1s)` is `Some("1")` and `get` is `None`.
  - `entry_ttls_keeps_order_and_reports_none` (Redisson `testRemainTimeToLive`): keys `[missing, ttl'd, plain, ttl'd]` give `[None, Some, None, Some]`. A key listed twice gives two items. Empty input gives `[]`.
  - Redisson ports: `redisson_fast_put_ttl`, `redisson_put_get_ttl`, `redisson_put_if_absent_ttl`, `redisson_fast_put_if_absent_ttl`, `redisson_fast_put_if_absent_with_ttl`, `redisson_fast_put_expiration`, `redisson_expire_overwrite`, `redisson_replace_value_ttl`, `redisson_add_and_get_ttl` (with `JsonCodec` numbers in place of `LongCodec`/`DoubleCodec`).
- [ ] **Step 3: Run** `cargo test --lib map_cache_native -- --test-threads 4`, then `cargo test --test map_cache_native -- --test-threads 4`. Expected: FAIL (compile).
- [ ] **Step 4: Implement.** Docs: method rows; an "Expiry" section (`.ttl`/`.expire_at`, zero, past times, the server clock for `.ttl` and the caller's clock for `.expire_at`, which writes clear the TTL and that `incr_by` keeps it, open question 1). Migration rows for `put/fastPut/putIfAbsent/fastPutIfAbsent/putIfExist(k, v, Duration|Instant)` and `remainTimeToLive(K|Set)` (note: `None` for both `-1` and `-2`).
- [ ] **Step 5: Run** the same two commands. Expected: `test result: ok.`
- [ ] **Step 6: Commit** `feat(map_cache_native): insert, set, try_insert and replace with ttl or expire_at; entry_ttls`

### Task 3: `expire_entry`, `expire_entries`, `persist_entry`, `persist_entries`

**Files:**
- Modify: `scripts.rs` (`EXPIRE_ENTRY`, `EXPIRE_ENTRY_IF`, `EXPIRE_ENTRIES`, `EXPIRE_ENTRIES_IF`), `builders.rs` (`ExpireEntry`, `ExpireEntries`), `mod.rs`, `src/lib.rs`
- Test: `tests/map_cache_native.rs`

**Interfaces:**
- Produces:
  - Private `enum Condition { Always, IfNotSet, IfGreater, IfLess }` → `NX`/`GT`/`LT`, in `builders.rs`.
  - `pub fn expire_entry<'a, Q>(&'a self, k: &'a Q) -> ExpireEntry<'a, K, V, C, Q>` with `.ttl(Duration)`, `.expire_at(SystemTime)`, `.if_not_set()`, `.if_greater()`, `.if_less()` (the last condition wins), resolving to `Result<bool>`.
    - With a time and `Always`, it runs `EXPIRE_ENTRY` with ARGV `[ms, k, command]`.
    - With a condition, it runs `EXPIRE_ENTRY_IF` with ARGV `[ms, k, param, command]`.
    - Without a time and `Always`, it runs `HPERSIST key FIELDS 1 k` and returns `reply ∈ {1, -1}`.
    - Without a time and with a condition, it is `Error::Config`.
    - Aliases `expireEntry`, `expireEntryIfNotSet`, `expireEntryIfGreater`, `expireEntryIfLess`.
  - `pub fn expire_entries<'a, Q>(&'a self, ks: &'a [&'a Q]) -> ExpireEntries<'a, K, V, C, Q>`, with the same options, resolving to `Result<usize>`.
    - `EXPIRE_ENTRIES` takes ARGV `[ms, command, k…]`, and `EXPIRE_ENTRIES_IF` takes `[param, ms, command, k…]`.
    - Without a time, it runs `HPERSIST` and counts the `1` and `-1` replies.
    - Empty `ks` gives `Ok(0)` without a call, after the options are checked.
    - Aliases `expireEntries`, `expireEntriesIfNotSet`, `expireEntriesIfGreater`, `expireEntriesIfLess`.
  - `pub async fn persist_entry<Q>(&self, k: &Q) -> Result<bool>` (`HPERSIST` = 1) and `pub async fn persist_entries<Q>(&self, ks: &[&Q]) -> Result<Vec<bool>>` (key order; empty input gives `[]` without a call). Alias `clearExpire`.
  - All of them run `require(FieldTtl)` first.
- [ ] **Step 1: Write failing tests.**
  - `expire_entry_sets_and_replaces_the_ttl` (Redisson `testExpireEntry`, `testUpdateEntryExpiration`): with `.ttl(20s)`, `entry_ttl` is in 19.8–20 s; with `.expire_at(now + 20s)`, it is in 19.8–20.5 s; on an expired key it is `false`.
  - `expire_entry_on_a_missing_key_is_false` (`testEntryEntryIfNotSet`'s first half).
  - `expire_entry_with_zero_clears_the_ttl` (Review Focus 2): `insert(..).ttl(60s)`, then `expire_entry("a").ttl(Duration::ZERO)` is `true`, `entry_ttl` is `None`, and `get` is still `Some`. `expire_entry("a").await` without a time does the same. On a missing key, it is `false`.
  - `a_condition_without_a_time_is_a_config_error`.
  - `conditions_follow_redis_on_persistent_and_missing_entries` (Review Focus 4): on a plain entry, `.ttl(10s).if_greater()` is `false`, `.if_less()` is `true`, and `.if_not_set()` is `true`; on a missing key, all three are `false`. On an entry with 60 s, `.ttl(10s).if_greater()` is `false` (TTL unchanged), `.ttl(120s).if_greater()` is `true`, and `.ttl(10s).if_less()` is `true`.
  - `expire_entries_counts_changed_entries` (Redisson `testExpireEntries`, `testEntryEntries`, `testEntryEntriesIfNotSet`, `testExpireEntriesIfNotSet`): `[a, b, missing]` gives `2`. `.if_not_set()` on `[ttl'd, plain]` gives `1`. After expiry, `0`.
  - `persist_entry_reports_a_removed_ttl` (Redisson `testClearExpire` for entries): with a TTL `true` and `entry_ttl` `None`; plain `false`; missing `false`. `persist_entries([ttl'd, plain, missing])` is `[true, false, false]`.
  - Redisson ports: `redisson_expire_entry_if_not_set`, `redisson_entry_entry_if_not_set`.
- [ ] **Step 2: Run** `cargo test --test map_cache_native expire_entr persist_ condition -- --test-threads 4`. Expected: FAIL (compile).
- [ ] **Step 3: Implement.** Docs: method rows and a "Conditions" paragraph (Redis's rule that no TTL counts as infinite; the zero and no-time cases and how they differ from Redisson's `expireEntry(k, ZERO)`). Migration rows for the 18 `expireEntry*`/`expireEntries*` overloads (→ the two builders) and `clearExpire(K|Set)`.
- [ ] **Step 4: Run** the same command, then the whole file. Expected: `test result: ok.`
- [ ] **Step 5: Commit** `feat(map_cache_native): expire_entry and expire_entries with if_not_set, if_greater and if_less; persist_entry`

### Task 4: Many entries: `extend` with a TTL, `try_extend`, `update_many`

**Files:**
- Create: `src/expiry.rs` (move `Expiry` and `expiry_args` here from `src/values/buckets.rs`; `expiry_args` becomes `pub(crate)`; `lib.rs` keeps `pub use expiry::Expiry`, so the public path is unchanged)
- Modify: `src/values/buckets.rs` (imports), `scripts.rs` (`PUT_ALL_TTL`), `builders.rs` (`Extend`, `TryExtend`, `UpdateMany`), `mod.rs`, `src/lib.rs`
- Test: `tests/map_cache_native.rs` (the `HSETEX` tests on `redis8_url()`)

**Interfaces:**
- Produces:
  - `pub fn extend<'a, Q, W>(&self, entries: impl IntoIterator<Item = (&'a Q, &'a W)>) -> Extend<'_, K, V, C>` (bounds as `HashMap::extend`; entries are encoded eagerly).
    - Options: `.batch_size(usize)` (0 = one call), `.ttl(Duration)`, `.expire_at(SystemTime)`, `.keep_ttl()`; the last expiry setting wins. Internal `expiry: Expiry`; `.ttl(ZERO)` → `Expiry::None`.
    - Resolves to `Result<()>`, one call per batch, in order, with the writer after each batch.
    - `Expiry::None` → `HSET`, as `HashMap::extend`. `After` → `PUT_ALL_TTL` with ARGV `[ms, k1, v1, …]` after `require(FieldTtl)`. `At`/`KeepTtl` → `HSETEX key <opt> FIELDS n …` after `require(Hsetex)`.
    - Empty input sends nothing. Alias `putAll`.
  - `pub fn try_extend<'a, Q, W>(&self, entries: …) -> TryExtend<'_, K, V, C>` and `pub fn update_many<'a, Q, W>(&self, entries: …) -> UpdateMany<'_, K, V, C>`.
    - Options: `.ttl`, `.expire_at`, `.keep_ttl()`.
    - Resolve to `Result<bool>`: `HSETEX key FNX|FXX [expiry_args] FIELDS n …` after `require(Hsetex)`, in one call (all or nothing).
    - Empty input gives `Ok(false)` without a call (Redisson).
    - The writer gets every entry when the reply is `true`.
    - Aliases `putIfAllKeysAbsent` / `putIfAllKeysExist`.
- [ ] **Step 1: Write failing tests.**
  - `extend_with_ttl_and_batches` (7.4): 250 entries with `.batch_size(100).ttl(300ms)`, `len() == 250`; after 500 ms, every `get` is `None`.
  - `extend_without_ttl_clears_it` (Review Focus 1).
  - `extend_with_a_zero_ttl_stores_permanent_entries`.
  - `extend_with_expire_at_and_keep_ttl` (8.8): `.expire_at(now + 300ms)` expires; `.keep_ttl()` over entries with 30 s keeps a TTL in (0, 30 s] (Redisson `testPutIfAllKeysExistWithKeepTTL`'s pattern).
  - `try_extend_is_all_or_nothing` (8.8, Redisson `testPutIfAllKeysAbsentWithTimeToLive/WithExpireAt/Expiration`): `true` and stored with TTL when all are absent; `false` and nothing changed when one is present.
  - `update_many_is_all_or_nothing` (8.8, Redisson `testPutIfAllKeysExistWithTimeToLive/WithExpireAt/WithKeepTTL`).
  - `empty_input_sends_nothing`: `try_extend([])` and `update_many([])` give `false` (Redisson `testPutIfAllKeysAbsentWithEmptyMap`), and `extend([])` gives `Ok(())`.
  - `hsetex_methods_need_redis_8` (Review Focus 3, on `redis_url()` 7.4): `try_extend`, `update_many` and `extend(..).keep_ttl()` give `Error::Unsupported` with `"8.0"` in the message, and `HLEN` is unchanged; `extend(..).ttl(1s)` works.
  - Redisson port: `redisson_put_all_get_ttl` (7.4, `.ttl`).
  - `buckets_still_use_expiry` (compile-level): the existing `tests/it/buckets.rs` compiles unchanged.
- [ ] **Step 2: Run** `cargo test --test map_cache_native extend update_many empty_input hsetex -- --test-threads 4`. Expected: FAIL (compile).
- [ ] **Step 3: Implement.** Docs: rows, the Redis 8.0 note, migration rows for `putAll(map, ttl)`, `putAll(PutArgs)` (`PutArgs.timeToLive/expireAt/keepTTL` → `.ttl/.expire_at/.keep_ttl`), `putIfAllKeysAbsent`, `putIfAllKeysExist`.
- [ ] **Step 4: Run** the same command, then `cargo test --test it buckets -- --test-threads 4`, then the whole `map_cache_native` file. Expected: `test result: ok.` for each.
- [ ] **Step 5: Commit** `feat(map_cache_native): extend with ttl, expire_at and keep_ttl; try_extend and update_many over HSETEX`

### Task 5: `entry()` with a TTL; loader and writer

**Files:**
- Modify: `src/collections/map/entry.rs` (`Target::Native`), `builders.rs` (`EntryRequest`), `mod.rs` (`pub(crate) async fn write_encoded_with(&self, k: &Bytes, v: &V, ttl: FieldTtl) -> Result<()>` over `FAST_PUT`), `src/lib.rs`, `guides/loaders-and-writers.mdx`
- Test: `tests/map_cache_native.rs`, `tests/map_store.rs` (tests named `native_…`)

**Interfaces:**
- Consumes: `KeyLock` (`map/ops.rs`), `load_value_locked`, `Store::written_value`.
- Produces:
  - `Target::Native(&'m MapCacheNative<K, V, C>, Option<FieldTtl>)`:
    - `key` returns the map's key, and `read`, `delete` and `store` go to the map.
    - `write` sends `HSET` when there is no time (or a zero one), else `write_encoded_with`.
    - `write_loaded` always sends `HSET`.
    - Public `Entry` types are unchanged.
  - `pub fn entry<Q>(&self, k: &Q) -> EntryRequest<'_, K, V, C>` with `.ttl(Duration)` and `.expire_at(SystemTime)`, `impl IntoFuture<Output = Result<Entry<'a, K, V, C>>>`. The key is encoded at once, and an error comes back on `.await`. A time is checked (and `require(FieldTtl)` runs) before the lock is taken. Aliases `compute`, `computeIfAbsent`, `computeIfPresent`, `merge`.
  - A plain `entry(&k)` write clears the TTL, like Redisson's `compute(k, f)` → `fastPut` → `HSET`: no deviation here, unlike `HashMapCache`.
- [ ] **Step 1: Write failing tests in `tests/map_cache_native.rs`.**
  - `entry_with_ttl_inserts_an_expiring_value`: `entry("a").ttl(300ms).await?.or_insert("1")`; `entry_ttl` ≤ 300 ms; after 500 ms, `None`.
  - `entry_with_expire_at_writes_that_deadline`.
  - `entry_without_ttl_clears_the_ttl` (Review Focus 1).
  - `entry_sees_an_expired_entry_as_vacant`.
  - `entry_is_atomic_across_tasks`: 20 tasks run `and_modify(+1).or_insert(1)` on a `MapCacheNative<String, i64>`; the result is `20`.
  - `a_dropped_entry_releases_the_lock`.
  - `entry_with_a_bad_time_fails_before_locking`: `.expire_at(UNIX_EPOCH - 1s)` gives `Error::Config`, and `lock("a").try_lock()` succeeds right after.
- [ ] **Step 2: Write failing tests in `tests/map_store.rs`** (built with `client.map_cache_native_builder::<String, String>(unique("mcn"))`):
  - `native_get_loads_a_missing_key_without_a_ttl`: the loaded value is stored, and `entry_ttl` is `None`.
  - `native_entry_loads_under_its_lock` (0c Review Focus 1, on this type).
  - `native_writer_gets_ttl_writes` (Redisson `testWriterPutTTL`, `testWriterFastPutTTL`, `testWriterPutIfAbsentTTL`, `testWriterFastPutIfAbsentTTL`): `insert(..).ttl`, `set(..).ttl`, and `try_insert(..).ttl` on a missing key each write once; `try_insert(..).ttl` on a present key and `replace(..).ttl` on a missing key do not write.
  - `native_writer_gets_all_or_nothing_writes` (8.8): `try_extend` that stored writes all entries; one that did not, nothing.
  - `native_expire_and_persist_do_not_call_the_writer`.
  - `native_write_behind_flushes`: `WriteMode::WriteBehind { batch_size: 10, delay: 100ms }`, `extend(..).ttl(60s)` of 25 entries, then `flush()`, and the store holds 25.
- [ ] **Step 3: Run** `cargo test --test map_cache_native entry -- --test-threads 4`, then `cargo test --test map_store native_ -- --test-threads 4`. Expected: FAIL (compile).
- [ ] **Step 4: Implement.** Docs: an "Entries" section (`entry(&k).ttl(d)`; plain writes clear the TTL), a "Loaders and writers" section (builder, where the loader is asked, writer conditions, loaded values have no TTL), and a line in `loaders-and-writers.mdx`. Migration rows for `compute/computeIfAbsent(k, Duration|Instant, f)` and `getMapCacheNative(MapOptions)`.
- [ ] **Step 5: Run** the two commands, then `cargo test --test it hash_map:: -- --test-threads 4` and `cargo test --test hash_map_cache entry -- --test-threads 4` (unchanged `Entry` users). Expected: `test result: ok.` for each.
- [ ] **Step 6: Commit** `feat(map_cache_native): entry() with ttl and expire_at, loader and writer through map_cache_native_builder`

### Task 6: Events, ported Redisson tests, Cluster, limitations, full verification

**Files:**
- Modify:
  - `src/collections/map/events.rs`: two variants, two kinds, `MapEventKind::NATIVE: [MapEventKind; 7]`, patterns `["__subkeyevent@*:hexpired", "__keyevent@*:hexpired"]` and `["__subkeyevent@*:hpersist", "__keyevent@*:hpersist"]`, `field_event` names `hexpired`/`hpersist`. `subscribe` gets `field_ttl: bool` (`false` → the two kinds are `Error::Config("EntryExpired and EntryPersisted need a MapCacheNative")`) and iterates `NATIVE`.
  - `src/collections/hash_map.rs`, `src/collections/hash_map_cache/mod.rs` (pass `false`), `mod.rs` (`events`, `events_of`)
  - `website/content/docs/guides/limitations.mdx`, `map-cache-native.mdx`, `guides/redisson-migration.mdx`
- Test: `tests/map_cache_native.rs`

**Interfaces:**
- Produces:
  - `MapEvent::EntryExpired { key: Option<K> }` (doc alias `MapExpiredListener`) and `MapEvent::EntryPersisted { key: Option<K> }` (`MapClearExpireListener`).
  - `MapEventKind::EntryExpired` and `MapEventKind::EntryPersisted`.
  - `pub async fn events(&self) -> Result<MapEvents<K, C>>` (all 7 kinds, alias `addListener`) and `pub async fn events_of(&self, kinds: &[MapEventKind]) -> Result<MapEvents<K, C>>` on `MapCacheNative`.
- [ ] **Step 1: Write failing tests** on `own_redis8()` with `CONFIG SET notify-keyspace-events ETh` unless noted:
  - `events_report_field_expiry` (Redisson `testExpiration`): `.ttl(1s)` and `.expire_at(now + 2s)` → `EntryExpired { key: Some("1") }` and `Some("3")` within 10 s.
  - `events_report_cleared_ttls` (Redisson `testClearExpireListener`): `persist_entry("1")` and `persist_entries(&["3"])` → `EntryPersisted` for both.
  - `events_report_put_and_remove` (Redisson `testRemoveListener`): `Removed { key: Some("1") }`.
  - `field_events_without_subkey_names_have_no_key`: on `own_redis()` (7.4) with `Eh`, `EntryExpired { key: None }`.
  - `hash_map_rejects_the_field_ttl_kinds`: `client.hash_map::<String, String>(n).events_of(&[MapEventKind::EntryExpired])` is `Err(Error::Config(_))`.
- [ ] **Step 2: Port the rest of `RedissonMapCacheNativeTest.java`.** The file is in the scratchpad `rtests-mcn/`, or fetch it from `https://raw.githubusercontent.com/redisson/redisson/redisson-4.8.0/redisson/src/test/java/org/redisson/RedissonMapCacheNativeTest.java`. Name each port `redisson_<snake_case>`. The `PutArgs` ones run on `redis8_url()`, the rest on `redis_url()`.
  - Port: `testCacheValues`, `testGetAllTTL`, `testGetAllWithStringKeys`, `testExpiredIterator`, `testExpire`, `testClear`, `testClearExpire` (whole object, `Object::persist`), `testEntrySet`, `testKeySet`, `testValues`, `testKeySetByPatternTTL`, `testValuesByPatternTTL`, `testEntrySetByPatternTTL`, `testContainsValueTTL`, `testContainsKeyTTL`, `testRemoveValueTTL`, `testRemoveValueFail`, `testReplaceOldValueFail`, `testReplaceOldValueSuccess`, `testEntryUpdate`, `testTTL` and `testTTLExpiration`, `testRMapCacheValues`, `testReadAllEntrySet`, `testReadAllValuesTTL`.
  - Run 0a's `BaseMapTest` ports on a `MapCacheNative`, named `<0a name>_on_native`. Copy the bodies and swap the factory.
  - Not ported, listed in the header comment with the reason: `testSizeInMemory` (no `sizeInMemory`), `testExpireAt` (`Object` has no `expireAt`), `testPutIfExistWithNullValueDuration/Instant` (Rust has no null value), and the loader and writer `BaseMapTest` cases, which are covered in `tests/map_store.rs` (Task 5).
- [ ] **Step 3: Write** `every_operation_runs_on_a_cluster` (Review Focus 5). Follow `an_entry_expires_and_is_reported_on_a_cluster` in `tests/hash_map_cache.rs`. On the 7.4 cluster, for names `<u>` and `{t}:<u>`, call `insert(..).ttl`, `set(..).expire_at`, `try_insert(..).ttl`, `replace(..).ttl`, `extend(..).ttl`, `entry_ttl`, `entry_ttls`, `expire_entry(..).if_less()`, `expire_entries`, `persist_entries`, `entry(..).ttl`, `lock`, `iter_with` and `del`. Each returns `Ok`, and the probe works on a cluster.
- [ ] **Step 4: Run** `cargo test --test map_cache_native -- --test-threads 4`, then `cargo test --test map_store -- --test-threads 4`, then `cargo test --test it hash_map -- --test-threads 4`, then `cargo test --test hash_map_cache keyspace -- --test-threads 4`. Expected: `test result: ok.` for each. Fix the implementation, not a ported assertion, unless the case is a recorded deviation (zero TTL, `expire_entry` without a time, `entry_ttls` order).
- [ ] **Step 5: Docs.**
  - `map-cache-native.mdx`: an "Events" section with the `notify-keyspace-events` setting (`E` + `h`, `T` on Redis 8.8 for key names, `g`/`x` for the whole object), and "What it does not have" (the parity list in Decisions).
  - `limitations.mdx`: a `MapCacheNative` bullet. It needs Redis 7.4/Valkey 9, and Redis 8.0 for the `HSETEX` methods. Plain writes, `replace` and `compare_and_set` clear the TTL (open question 1). A zero TTL means none (Redisson's `expireEntry(k, ZERO)` deletes). `entry_ttl`/`entry_ttls` give `None` for both missing and permanent. It has no max size, max idle or entry events, as in Redisson. Extend the "Short times" line with `MapCacheNative`'s entry TTLs.
  - The migration table is complete; check every row against `javap org.redisson.api.RMapCacheNative`: 39 methods, each named in a row.
- [ ] **Step 6: Run the full verification, one command at a time:** `cargo fmt --check`; `cargo clippy --all-targets -- -D warnings`; `cargo clippy --all-targets --all-features -- -D warnings`; `cargo test -- --test-threads 4`; `cargo doc --no-deps` (with `RUSTDOCFLAGS="-D warnings"` as CI sets it). Expected: all green, no warnings.
- [ ] **Step 7: Commit** `test(map_cache_native): port Redisson RMapCacheNative tests, events for field expiry, cluster; docs`

### Task 7: PR and changelog text

- [ ] **Step 1: Open the PR** `feat: MapCacheNative, Redisson's RMapCacheNative (#1)` with this body. During the PR, verify on the GitHub CI logs, not by running locally.

```markdown
## Summary
New experimental `MapCacheNative`: a Redis hash whose entries expire on the server (Redis 7.4 hash field expiration), with every method of Redisson's `RMapCacheNative` and `RMap`, Redisson's Lua scripts and data layout (Redisson and redissun clients share the data), and the names and behaviour of `HashMap`/`HashMapCache`.

## Changelog
### Added
- `MapCacheNative` (experimental, Redis 7.4+ or Valkey 9; `client.map_cache_native(name)`, `client.map_cache_native_builder(name)` with a loader and writer). Every `HashMap` method, plus:
  - `insert`, `set`, `try_insert`, `replace` with `.ttl(d)` or `.expire_at(t)`;
  - `extend(..)` with `.batch_size`, `.ttl`, `.expire_at`, `.keep_ttl`; `try_extend` and `update_many` (all or nothing, Redis 8.0);
  - `entry_ttl`, `entry_ttls`;
  - `expire_entry(&k)` / `expire_entries(&ks)` with `.ttl`/`.expire_at` and `.if_not_set()`/`.if_greater()`/`.if_less()`;
  - `persist_entry`, `persist_entries`;
  - `entry(&k)` with `.ttl`/`.expire_at`;
  - per-key `lock`, `fair_lock`, `rw_lock`, `semaphore`, `count_down_latch`, `permit_expirable_semaphore`;
  - `events()` including field expiry and cleared TTLs.
- `MapEvent::EntryExpired` and `MapEvent::EntryPersisted` (Redisson's `MapExpiredListener`, `MapClearExpireListener`), with their `MapEventKind`s; only `MapCacheNative` listens to them.
- Docs: the `MapCacheNative` page, migration rows, limitations.

### Changed
- `Expiry` moved to a shared module; `redissun::Expiry` is unchanged.

### Behaviour that differs from Redisson on purpose
- A zero `.ttl()` means no TTL everywhere; Redisson's `expireEntry(k, Duration.ZERO)` and `putAll(map, Duration.ZERO)` delete the entries.
- `expire_entry(&k)` without a time clears the TTL (`HPERSIST`), like `HashMapCache::expire_entry`; Redisson requires a time.
- `entry_ttls` returns a `Vec` in key order instead of a map; missing and permanent entries are both `None`.
- TTL methods check once per client that the server has `HPEXPIRE`/`HSETEX` and return `Error::Unsupported` before writing anything.

### Redis-side deviations from Redisson (recorded per CLAUDE.md)
- None. Every script is Redisson's, verbatim; `extend(..).ttl(d)` uses Redisson's `putAll(map, ttl)` script so it runs on Redis 7.4.
```

- [ ] **Step 2:** Check that the changelog, `map-cache-native.mdx` and `limitations.mdx` describe the same minimum versions, zero/no-time rules and TTL-clearing writes. No commit: Tasks 1–6 hold the commits.
