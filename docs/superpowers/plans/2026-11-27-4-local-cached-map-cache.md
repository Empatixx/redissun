# 4: LocalCachedMapCache (Redisson `RLocalCachedMapCache`, PRO: interface only): Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A new, experimental `LocalCachedMapCache<K, V, C>` with every method of Redisson 4.8.0's `RLocalCachedMapCache` (`extends RMapCache, RLocalCachedMap` plus `preloadCache()`, checked with `javap`) and every option of `LocalCachedMapCacheOptions`. The data in Redis is `HashMapCache`'s, which is Redisson's `RMapCache` layout and scripts, so Redisson `RMapCache` clients and redissun `HashMapCache` clients read and write the same entries. On top of that it keeps `LocalCachedMap`'s local cache with its sync messages. A local copy never outlives its entry's TTL or max idle on the server, and entries the server drops (clean-up, max size, removals) are dropped locally. Every method name it shares with `HashMapCache` or `LocalCachedMap` behaves the same way.

**Architecture:** Neither object is copied. First, `LocalCachedMap`'s local machinery (store, providers, sync messages, listener, reconnection) moves into a private module `src/collections/local_cache/`, which `LocalCachedMap` then uses unchanged. The module also gains per-entry lifetimes. Second, `HashMapCache` gets one private field `local: Option<Arc<LocalCache>>`. When it is `None` (every plain `HashMapCache`), each call runs exactly the script it runs today. When it is `Some`, a write runs the *announced twin* of Redisson's script. The twin is Redisson's body, verbatim, inside a redissun wrapper that strips the extra arguments and then publishes the sync messages (and writes the updates log) in the same script. Reads first look in the local cache and fill it from a read twin, which also returns the entry's deadlines. `LocalCachedMapCache` wraps such a `HashMapCache`. Its methods are one-line delegations that return the same public builder types (`hash_map_cache::Insert`, `Set`, …), plus the local methods and `preload_cache`.

**Tech Stack:** Rust 2024, tokio, redis-rs, the `moka` feature, testcontainers (Redis 7.4, a 7.4 Cluster).

**Spec:** `docs/superpowers/specs/2026-11-26-redisson-4x-objects-design.md` ("### 4. `LocalCachedMapCache`", "Backend of the PRO objects", "API conventions", "Constraints", "Testing"). The naming rules of `docs/superpowers/specs/2026-11-27-rust-idiomatic-api.md` replace the spec's naming lines. Builds on `HashMapCache` (0b), the loader and writer of 0c, `LocalCachedMap` with `StoreMode`, `CacheProvider`/moka and `ExpirationEventPolicy`, and the shared `src/collections/map/*`.

**Branch:** `feat/local-cached-map-cache`, from `main` after `feat/multimap-cache` (PR 3b) is merged. One PR.

## Owner decisions (from the brief and the spec, 2026-11-27)

1. Backend: `HashMapCache`'s data layout (Redisson's `RMapCache`), plus `LocalCachedMap`'s local cache and invalidation. Share the code of both objects, do not copy it (spec).
2. Every method of `RLocalCachedMapCache` and every option of `LocalCachedMapCacheOptions`. A name it shares with `HashMapCache`/`LocalCachedMap` gets the same behaviour. Experimental. `client.local_cached_map_cache(name)…build().await`, a builder like `local_cached_map`.
3. Our own tests (there is no Redisson source and no Redisson test for this object): two instances see each other's writes and invalidations, local copies expire with TTL and max idle, server-side eviction invalidates local copies, reconnection strategies, Cluster.

## Decisions (mine, by the rules: Rust-idiomatic, Redisson parity, same name ⇒ same behaviour, share don't copy)

- **When the two parents disagree, the data methods follow `HashMapCache` and the local methods follow `LocalCachedMap`.** The object *is* an `RMapCache` with a local layer. So `insert` is `HashMapCache`'s builder (`.ttl`/`.max_idle`), `clear()` returns `Result<()>` like `HashMapCache::clear` (`LocalCachedMap::clear` returns `bool`), `len` counts entries in Redis, expired ones included until the clean-up, and `get` restarts the idle time. `local_len`, `local_entries`, `clear_local`, `clear_local_everywhere` and every builder option come from `LocalCachedMap`.
- **Wire format of the sync messages: `LocalCachedMap`'s, not Redisson's `LocalCachedMessageCodec`.** The channel is `redissun__local_cache:<name>` and the updates log is `redissun__cache_updates_log:{name}`, built by the same `channel`/`updates_log` functions. A message is 16 bytes of instance id, one kind byte, then the body. Compatibility with Redisson on the topic is not possible. `RLocalCachedMapCache` exists only in Redisson PRO, whose format is unpublished, and redissun's `LocalCachedMap` already uses its own format. Only the Redis data has to interoperate, and it does.
- **One new message kind, `E` (an update that expires).** Body: `[ttl_timeout: i64 BE][idle_delta: i64 BE][key_len: u32 BE][key][value]`, where `0` means no limit and `ttl_timeout` is the absolute deadline in the writer's Unix ms (Redisson's client clock). `SyncStrategy::Update` sends `E` for every write whose limits the client knows. A plain `U` would let a receiver keep a copy past the entry's TTL. `LocalCachedMap` ignores unknown kinds, and `Local::apply` handles `E` for both types. `I` and `C` are unchanged.
- **How long a local copy lives** (`copy_lifetime`, Task 1). It lives until the earliest of: the builder's local `ttl`/`max_idle` (as in `LocalCachedMap`); the entry's TTL deadline on the server; and, for an entry with a max idle, **half the max idle after its last read from Redis**. Local hits do not reach Redis, so they cannot restart the server's idle time. With the half-life, a copy read all the time is re-read from Redis every `max_idle / 2`. That read restarts the idle time on the server, so the entry stays alive while it is used, and the copy never outlives it. A copy whose lifetime is zero or past is not cached.
- **Which reads use and fill the local cache:**

  | Method | Local hit answers | Miss goes to | Miss is cached |
  |---|---|---|---|
  | `get`, `get_many` | yes | read twins `GET_EXPIRY`, `GET_ALL_EXPIRY` (value plus deadlines) | yes, with its lifetime; a miss only with `store_cache_miss` |
  | `contains_key` | yes (`Some(None)` is `false`) | `CONTAINS_KEY` | only a miss, with `store_cache_miss` (as `LocalCachedMap`) |
  | `peek`, `peek_many` | yes | `GET_WITH_TTL_ONLY`, `GET_ALL_WITH_TTL_ONLY` | no (they return no idle deadline) |
  | `entry(&k)` | no: under the key's lock it reads Redis with `GET`, so a late invalidation cannot feed a stale value to the closure | `GET` | no; its write is cached like `insert` |
  | `lease(&k, d)` | no (the lease protocol lives in Redis) | `LEASE_GET` | no |
  | everything else (`entry_ttl`, `len`, `contains_value`, `value_len`, `random_*`, `to_hash_map`, the streams) | no | `HashMapCache`'s script | no |

- **Which writes announce what.** Before each write the key is forgotten locally, which also marks reads in flight, so their result is not cached. After the reply, only an unconditional write or a stored `try_insert` caches the new value with its known limits. Everything else stays forgotten until it is read again:

  | Op (private `Op`) | Methods | Changed when (wrapper `when` code) | Message under `Update` | Local copy after |
  |---|---|---|---|---|
  | `Put` | `insert[.ttl/.max_idle]`, `set[.ttl/.max_idle]` without a lease, `extend[…]`, `entry` writes, loader writes | always (`a`) | `E` per key | cached with the write's limits |
  | `TryPut` | `try_insert[…]` | reply is nil (`n`) | `E` | cached when stored |
  | `IfSome` | `replace`, `remove` | reply is not nil (`s`) | `I` (the kept limits are unknown) | forgotten |
  | `IfOne` | `compare_and_set`, `remove_if_eq`, `expire_entry[…]` | reply is `1` (`1`) | `I` | forgotten |
  | `PerKey` | `remove_many` (always `FAST_REMOVE_BATCH` when announced), `invalidate`, `LeaseGuard::remove` (`LEASE_REMOVE`) | element `i` is `1` (`k`) | `I` per removed key | forgotten |
  | `Touch` | `incr_by`, `incr_by_float`, `expire_entries[…]`, `set(..).lease(token)`, `LeaseGuard::insert` | always (`a`) | `I` | forgotten |
  | `Clear` | `clear`, `Object::del` | `DEL` removed something | `C` | all forgotten |

  `set(..).lease(t)` is `Touch` because `FAST_PUT_WITH_LIMITS` replies `0` both for "updated" and for "wrong lease" (Review Focus 2). Under `SyncStrategy::Invalidate` every row sends `I` (or `C`). Under `SyncStrategy::None` without the `Load` reconnection strategy, nothing is published, exactly as `LocalCachedMap`'s `mode == "0"`.
- **Entries the server drops.** At `build` the cache subscribes to its own entry-event channels `redissun_map_cache_removed:{name}` and `redissun_map_cache_expired:{name}` (`names::channel`). Each message forgets the key it names (`entry_events::split_event` with the server's `LengthPrefix`). A message that does not parse, or a lag, clears the whole local cache. This happens whatever the `SyncStrategy`, because the server's truth must win. The expired channel is fed by Redisson's clean-up task (`EVICT` publishes while someone listens). The removed channel carries max-size evictions inside the put scripts, and removals by any client, Redisson's `RMapCache` included. Redisson publishes those only when the options hash has `has-listeners`, so `build` sets it (`HSET {name}:redissun_options has-listeners 1`), as `HashMapCache::events_of` and Redisson's `addListener` do. Recorded in the PR: from then on, every client's writes to this cache publish entry events.
- **Max size vs local `cache_size`.** They are independent. `set_max_size`/`init_max_size` are `HashMapCache`'s (Redis-side `EvictionMode::{Lru, Lfu}`), and their evictions reach the local caches through the removed channel. `cache_size` with `eviction_policy` limits only the local copies, as in `LocalCachedMap`. Local hits do not count as uses on the server, so under a server-side size limit the entries read mostly from local caches look cold there (Owner question 1; documented in the meantime).
- **Changes that are not announced.** Writes through a plain `HashMapCache` or a Redisson `RMapCache` are not announced (as with `LocalCachedMap`), apart from their removals (see above). Other instances see such a write when their copy's lifetime ends. Documented.
- **Loader, writer, write mode, retries: `store_options!()` on the builder, run by `HashMapCache`'s 0c code.** A loaded value is written with `write_encoded` (`Op::Put`, no limits, so it is cached and announced). `store_cache_miss` caches a key the loader lacks, as `LocalCachedMap` does. `load_all`/`load_many` and `flush` delegate.
- **`StoreMode::Local` (Redisson's `LOCALCACHE`, which `LocalCachedMapCacheOptions` has): supported, Task 6.** The entries live only in the local caches, with per-entry TTL and max idle kept locally. Here a local read *does* restart the idle time, because the local copy is the only copy. Redis carries the sync messages, the per-key lock while a loader or `entry` runs, and the write-behind queue. The `has-listeners` mark, the event subscriptions and the clean-up task are skipped. Supported methods: `get`, `get_many`, `contains_key`, `peek`, `peek_many`, `insert`/`set`/`try_insert`/`extend` (with `.ttl`/`.max_idle`), `replace`, `compare_and_set`, `remove`, `remove_if_eq`, `remove_many`, `entry_ttl`, `expire_entry`, `expire_entries`, `len`, `is_empty`, `clear`, `iter`/`keys`/`values`/`to_hash_map` (a snapshot of the local entries), `entry`, `load_all`, `load_many`, `flush`, the local methods, `preload_cache` (returns `0`: Redis holds nothing), `Object::del`. Every other method (`set(..).lease`, `incr_by`, `incr_by_float`, `set_max_size`, `init_max_size`, `events*`, `keyspace_events*`, `lease`, `invalidate`, `contains_value`, `value_len`, `random_keys`, `random_entries`, `iter_with`/`keys_with`/`values_with`) returns `Error::Config("LocalCachedMapCache::<method> needs StoreMode::LocalRedis")` before sending anything. `len` counts the local entries that hold a value, as `LocalCachedMap` does.
- **Names (`javap` 4.8.0 → redissun):**

  | Redisson | redissun | Note |
  |---|---|---|
  | `getLocalCachedMapCache(name, options)`, `(name, codec, options)` | `client[.with_codec(c)].local_cached_map_cache(name)….build().await` | alias `getLocalCachedMapCache` |
  | every `RMapCache`/`RMap`/`RLeasedMap` method | `HashMapCache`'s name, signature and builder | see the migration guide's `## HashMapCache` |
  | `preloadCache()`, `preloadCache(int count)` | `preload_cache()` → `PreloadCache` builder, `.page_size(n)` (default 10, Redisson's `entrySet()` page) → `usize` entries cached | merged overloads (rule 8) |
  | `getCachedMap()`, `cachedEntrySet()`, `cachedKeySet()`, `cachedValues()` | `local_entries() -> Result<Vec<(K, V)>>` | they differ only in what they return; all four aliases |
  | `clearLocalCache()` | `clear_local_everywhere()` | as `LocalCachedMap` |
  | — | `local_len()`, `clear_local()` | as `LocalCachedMap` |
  | `addListener(ObjectListener)` | `keyspace_events()`, `keyspace_events_of(..)` | as `HashMapCache` |
  | `addListener(MapEntryListener)`, `removeListener` | `events()`, `events_of(..)` / drop | as `HashMapCache` |
  | `destroy()` | `flush().await`, then drop (the listener stops with the last handle) | as `LocalCachedMap` |
  | options `cacheSize`, `evictionPolicy`, `timeToLive`, `maxIdle`, `syncStrategy`, `reconnectionStrategy`, `storeCacheMiss`, `storeMode`, `cacheProvider`, `useKeyEventsPattern` | `LocalCachedMapBuilder`'s setters, shared by a macro: `cache_size`, `eviction_policy`, `ttl`, `max_idle`, `sync_strategy`, `reconnection_strategy`, `store_cache_miss`, `store_mode`, `cache_provider`, `expiration_event_policy` | `useKeyEventsPattern(true/false)` is `KeyeventPattern`/`KeyspaceChannel` |
  | `loader`, `loaderAsync`, `writer`, `writerAsync`, `writeMode`, `writeBehindBatchSize`, `writeBehindDelay`, `writerRetryAttempts`, `writerRetryInterval` | `store_options!()` | |

- **Not ported, with the same reasons as on the parents:** `EvictionPolicy.SOFT`/`WEAK` and `cacheSize(-1)` (as `LocalCachedMap`); `MapCacheOptions.removeEmptyEvictionTask`, `mapReduce`, `sizeInMemory`, `expireAt` and the conditional `expire` (as `HashMapCache`); `migrate_from_0_23` (a new object has no 0.23 data).
- **Not `Clone`**, like `LocalCachedMap`: one handle owns the listener. `Debug` prints `LocalCachedMapCache`.

### Recorded Redis-side additions (no Redisson script is edited; the PR lists them)

- A1: the **announced twins** in `hash_map_cache/announced.rs` (`wrap_write`): `PUT`, `PUT_WITH_LIMITS`, `PUT_IF_ABSENT_WITH_LIMITS`, `FAST_PUT_WITH_LIMITS`, `REMOVE`, `REMOVE_IF`, `FAST_REMOVE_BATCH`, `REPLACE`, `REPLACE_IF`, `ADD_AND_GET`, `PUT_ALL_TTL_IDLE`, `EXPIRE_ENTRY`, `EXPIRE_ENTRY_IF_NOT_SET`, `EXPIRE_ENTRIES`, `EXPIRE_ENTRIES_IF_NOT_SET`, `LEASE_REMOVE`. Each is Redisson's script text, unchanged, inside the wrapper below. Its data writes are byte-for-byte those of the plain script.
- A2: the **read twins** (`wrap_read`): `GET_EXPIRY` (`GET` plus `{value, ttl score or '', idle delta}`) and `GET_ALL_EXPIRY` (`GET_ALL` plus the same per key).
- A3: `CLEAR_ANNOUNCED` (`clear`: `DEL` of main, timeout, idle and last-access sets and the updates log, then `C` when something was deleted) and `DEL_ANNOUNCED` (`Object::del`: the same keys plus options), redissun-only like `LocalCachedMap`'s `CLEAR_ALL`.
- A4: `has-listeners` set at `build` (see Decisions).

The write wrapper. It is an algorithm the signatures do not determine, so here is the exact text. `<BODY>` is `scripts::X.source()`:

```lua
local n = #ARGV
local c = tonumber(ARGV[n])
local tail = {}
for i = 1, c do tail[i] = ARGV[n - c - 1 + i] end
local args = {}
for i = 1, n - c - 1 do args[i] = ARGV[i] end
local keys = {}
for i = 1, #KEYS - 1 do keys[i] = KEYS[i] end
local result = (function(KEYS, ARGV)
<BODY>
end)(keys, args)
local channel, mode, when, now, keep = tail[1], tail[2], tail[3], tail[4], tail[5]
if mode ~= '0' then
    for i = 6, c, 2 do
        local j = (i - 4) / 2
        if when == 'a' or (when == 'n' and not result) or (when == 's' and result)
            or (when == '1' and result == 1) or (when == 'k' and result[j] == 1) then
            if mode == '2' then
                redis.call('zadd', KEYS[#KEYS], now, tail[i + 1])
                redis.call('zremrangebyscore', KEYS[#KEYS], '-inf', '(' .. (tonumber(now) - tonumber(keep)))
            end
            redis.call('publish', channel, tail[i])
        end
    end
end
return result
```

KEYS are Redisson's KEYS followed by the updates log (`updates_log`, in the cache's slot). ARGV are Redisson's ARGV, then `channel, mode, when, now, keep`, then one `notice, log entry` pair per key, then the count `c` of tail items. Redisson's variadic scripts (`FAST_REMOVE_BATCH`, `PUT_ALL_TTL_IDLE`, `EXPIRE_ENTRIES*`) loop over `#ARGV` and see only their own arguments. `mode`, `now`, `keep` and the log entries are `LocalCachedMap::log_arguments`' values.

## Redis commands per method (redissun, plain `HashMapCache` → `LocalCachedMapCache`)

| redissun | Redisson | Plain cache runs | Local cached cache runs |
|---|---|---|---|
| `get(&k)` | `get` | `GET` | local, else `GET_EXPIRY` (A2) |
| `get_many(&ks)` | `getAll` | `GET_ALL` | local hits, `GET_ALL_EXPIRY` for the misses |
| `contains_key(&k)` | `containsKey` | `CONTAINS_KEY` | local, else `CONTAINS_KEY` |
| `peek`, `peek_many` | `getWithTTLOnly`, `getAllWithTTLOnly` | `GET_WITH_TTL_ONLY`, `GET_ALL_WITH_TTL_ONLY` | local, else the same |
| `insert`, `set`, `try_insert`, `replace`, `compare_and_set`, `remove`, `remove_if_eq`, `remove_many`, `incr_by*`, `extend`, `expire_entry*`, `expire_entries*`, lease writes | as in `## HashMapCache` | Redisson's script | its announced twin (A1) |
| `clear()` | `clear` | `DEL` of main + sets | `CLEAR_ANNOUNCED` (A3) |
| `Object::del` | `delete` | `DEL` of all keys | `DEL_ANNOUNCED` (A3) |
| `preload_cache()` | `preloadCache` | — | `SCAN_KEYS` pages (+ `TOUCH_IDLE`), `GET_ALL_EXPIRY` per page |
| `clear_local_everywhere()` | `clearLocalCache` | — | `PUBLISH channel C` |
| every other method | — | Redisson's script or command | the same |

## Global Constraints

- `#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::todo, clippy::unimplemented)]`: no unwrap/expect in library code. Tests may unwrap.
- No Redisson script is edited. Twins wrap `scripts::X.source()` at `LazyLock` init (`Lua::from_lua(wrap_write(scripts::X.source()))`). Each twin has a `/// redissun: <X> announced for LocalCachedMapCache` comment. A plain `HashMapCache` (field `local: None`) sends exactly the scripts, KEYS and ARGV it sends today, and the existing `HashMapCache` tests pass unchanged.
- Data: `HashMapCache`'s keys (raw name for the hash; `redissun__timeout__set:{name}`, `redissun__idle__set:{name}`, `redissun__map_cache__last_access__set:{name}`, `{name}:redissun_options`, the entry-event channels), plus `LocalCachedMap`'s channel `redissun__local_cache:<name>` and updates log `redissun__cache_updates_log:{name}`. All keys share the cache's slot.
- `LocalCachedMap`'s public API, scripts, messages and behaviour do not change. All of `tests/local_cached_map.rs` passes unchanged, with and without `--features moka`.
- Every public name shared with `HashMapCache`/`LocalCachedMap` keeps its signature shape and carries its `#[doc(alias)]`s. `#[doc(alias = "getLocalCachedMapCache")]` on the factory.
- No private trait in a public bound. `LocalCache`, `Op`, `Lifetime` are `pub(crate)` at most. Exports: `LocalCachedMapCache` and `LocalCachedMapCacheBuilder` at the crate root, and `pub mod local_cached_map_cache { PreloadCache }`. The builders it returns stay in `redissun::hash_map_cache`.
- Minimum server: Redis 6.2 / Valkey, like `HashMapCache`. `notify-keyspace-events` is needed only for `ExpirationEventPolicy` (as `LocalCachedMap`).
- **Experimental** in the README, its docs page and its rustdoc (first line of the type doc: "**Experimental.**").
- One cargo build or test run at a time, `--test-threads 4`. No git worktrees.
- Docs go in the task that changes the behaviour: `website/content/docs/objects/local-cached-map-cache.mdx` (new, after `"local-cached-map"` in `objects/meta.json`), `guides/redisson-migration.mdx` (naming row `RLocalCachedMapCache` → `LocalCachedMapCache`, a `## LocalCachedMapCache` section), `guides/limitations.mdx`, `guides/loaders-and-writers.mdx` (one line), `README.md` (row after `LocalCachedMap`: `` | [`LocalCachedMapCache`](https://empatixx.github.io/redissun/docs/objects/local-cached-map-cache) | `HashMapCache`'s layout, a local cache and pub/sub | experimental | ``).

## Review Focus

1. **A local copy that outlives its entry on the server.** TTL expiry; an idle entry nobody reads; `expire_entry` that shortens the TTL from another instance; `replace` (which keeps the server's limits); an `E` message received later than sent. A reasonable user expects `get` never to return a value the server already treats as expired. Pinned in Task 1 (`copy_lifetime_takes_the_earliest_limit`), Task 2 (`a_local_copy_expires_with_the_entry_ttl`, `a_copy_of_an_idle_entry_lives_half_the_max_idle`, `reading_locally_keeps_an_idle_entry_alive`, `the_update_strategy_sends_value_and_deadline`) and Task 3 (`replace_keeps_the_limits_and_invalidates`, `expire_entry_on_another_instance_shortens_my_copy`).
2. **Pushing a write that did not happen.** Under `SyncStrategy::Update`, a `try_insert` that found a value, a `set(..).lease(t)` with the wrong token, or a `compare_and_set` that failed must leave other instances' copies as they were. Pinned in Task 3 (`try_insert_announces_only_when_it_stored`, `set_with_a_wrong_lease_pushes_nothing`, `compare_and_set_and_remove_if_eq_announce_on_success_only`).
3. **Entries the server drops behind the cache's back.** A max-size eviction caused by another client's insert, a clean-up of an entry whose TTL a plain client shortened, a `remove` by a plain `HashMapCache` client: the local copy must go. Pinned in Task 5 (`max_size_eviction_drops_my_copy`, `the_clean_up_drops_a_copy_whose_ttl_was_shortened_behind_its_back`, `a_removal_by_a_plain_cache_client_drops_my_copy`, `sync_none_still_drops_evicted_copies`).
4. **Failures and races during a write or a read.** A script that fails (wrong type, connection dropped) must leave no local copy. An invalidation that arrives while a `get` reads from Redis must not let the stale read be cached. Pinned in Task 2 (`a_failed_write_leaves_no_local_copy`) and Task 3 (`an_invalidation_during_a_read_is_not_overwritten`).
5. **Hash-tagged names on Redis Cluster.** `{t}:m`: every twin takes the updates log as its last KEY in the same slot, and messages, the entry-event channels and `Object` over all keys work on a 3-master cluster without `CROSSSLOT`. Pinned in Task 7 (`every_operation_runs_on_a_cluster`).

---

### Task 1: Shared local cache module with per-entry lifetimes

**Files:**
- Create: `src/collections/local_cache/mod.rs` (`LocalCache`, `start`), `options.rs` (the six public enums moved as they are, `LocalOptions`, `local_options!`), `store.rs` (`Slot`, `Store`, `LocalStore`, `Lifetime`, `copy_lifetime`), `local.rs` (`Cache`, `ReadGuard`, `Local`), `messages.rs` (kinds `I`/`U`/`C`/`E`, `message`, `expiring_update`, `channel`, `updates_log`, `log_arguments`), `listener.rs` (`ExpiryFeed`, `listen`, `load_after_reconnection`)
- Modify: `src/collections/local_cached_map.rs` (keeps its type, its four scripts and the builder. Its fields `local, sync, reconnection, store_cache_miss, store_mode` become one `local: Arc<LocalCache>`, and the setters come from `local_options!()`), `src/collections/mod.rs`, `src/lib.rs` (re-exports keep their paths: `redissun::SyncStrategy` etc.)
- Test: unit tests in `store.rs`, `messages.rs`; `tests/local_cached_map.rs` unchanged

**Interfaces:**
- Produces:
  - `#[derive(Clone, Copy, Default)] pub(crate) struct Lifetime { pub(crate) deadline: Option<Instant>, pub(crate) idle: Option<Duration> }` (`tokio::time::Instant`). `deadline` is fixed. `idle` is restarted by local reads and is used only by `StoreMode::Local`.
  - `pub(crate) enum CopyLifetime { Forever, For(Duration), Skip }` and `pub(crate) fn copy_lifetime(ttl_timeout_ms: i64, idle_delta_ms: i64, now_ms: i64) -> CopyLifetime` (`0` = no limit; the earliest of `ttl_timeout - now` and `idle_delta / 2`; `Skip` when that is `<= 0`). `impl CopyLifetime { fn lifetime(self) -> Option<Lifetime> }` (`None` for `Skip`).
  - `Store::put(&mut self, key: Bytes, value: Option<Bytes>, lifetime: Lifetime)`. `Store::expired` also checks `slot.lifetime.deadline` and `slot.touched + slot.lifetime.idle`. `LocalStore::put` takes the same. For moka, the value becomes `struct Copy { value: Option<Bytes>, lifetime: Lifetime }`, and `CacheBuilder::expire_after(CopyExpiry)` implements `moka::Expiry`: `expire_after_create`/`_update` give the time left to `deadline` (or `idle`, whichever is shorter), and `expire_after_read` gives `idle` when it is set, else keeps the current duration. Moka's `time_to_live`/`time_to_idle` stay as today. Check in the moka 0.12 docs that the earliest of the three wins, and pin it with the test below.
  - `ReadGuard::finish(self, value: Option<(Option<Bytes>, Lifetime)>)`, `Local::write(&self, key, value: Option<Bytes>, lifetime: Lifetime)`. `LocalCachedMap` passes `Lifetime::default()` everywhere, so its behaviour is unchanged.
  - `messages::expiring_update(id: &[u8; 16], ttl_timeout: i64, idle_delta: i64, key: &[u8], value: &[u8]) -> Bytes`. `Local::apply` decodes `E` into `write(key, Some(value), copy_lifetime(..).lifetime()?)`, and skips the write when the lifetime is `Skip`, after forgetting the key.
  - `pub(crate) struct LocalOptions { cache_size, ttl, max_idle, sync, reconnection, eviction, store_cache_miss, store_mode, expiration, provider }` with `Default` = Redisson's defaults (as today). `macro_rules! local_options` expands to the ten setters of `LocalCachedMapBuilder` with their current docs and aliases, writing to `self.local`.
  - `pub(crate) struct LocalCache { pub(crate) local: Arc<Local>, pub(crate) sync: SyncStrategy, pub(crate) reconnection: ReconnectionStrategy, pub(crate) store_cache_miss: bool, pub(crate) store_mode: StoreMode, pub(crate) channel: String, pub(crate) updates_log: String }` with `mode() -> Bytes`, `announces() -> bool`, `local_only() -> bool`, `async fn publish(&self, key: &Key, payload: Bytes) -> Result<()>`, all moved from `LocalCachedMap`.
  - `pub(crate) async fn start(key: &Key, watched: String, options: &LocalOptions, evictions: Option<EvictionFeed>) -> Result<Arc<LocalCache>>`, which holds today's `build()` body from the `Local` construction to the spawned listener. `watched` is the Redis key whose expiry clears the cache. `EvictionFeed` arrives in Task 5; until then the parameter does not exist.
- [ ] **Step 1: Write failing unit tests.** In `store.rs`, with `#[tokio::test(start_paused = true)]` and `tokio::time::advance`:
  - `a_slot_past_its_deadline_is_a_miss`: `put(k, Some(v), Lifetime { deadline: Some(now + 50ms), idle: None })`. `get(&k) == Some(Some(v))`; after advancing 50ms, `get(&k) == None` and `slots.len() == 0`.
  - `a_slot_with_an_idle_limit_lives_while_read`: `idle: Some(100ms)`. Advance 60ms, hit; advance 60ms, hit; advance 101ms, miss.
  - `the_store_ttl_and_the_slot_deadline_take_the_earlier`: `Store::new(.., ttl: Some(1s), ..)` with a slot deadline at 200ms is a miss at 200ms. With the deadline at 5s, it is a miss at 1s.
  - `copy_lifetime_takes_the_earliest_limit` (`now = 1_000_000`): `(now + 10_000, 0)` gives `For(10s)`; `(0, 4_000)` gives `For(2s)`; `(now + 1_000, 10_000)` gives `For(1s)`; `(0, 0)` gives `Forever`; `(now - 1, 0)` gives `Skip`; `(0, 1)` gives `Skip`.
  - `#[cfg(feature = "moka")] moka_honours_the_slot_deadline` (real time): a moka `LocalStore` with no ttl, `put` with a deadline 50ms ahead, `get` is a hit, then after `sleep(120ms)` it is a miss. Also `moka_takes_the_earlier_of_ttl_and_deadline` (ttl 50ms, deadline 5s: a miss after 120ms).
  - In `messages.rs`, `an_expiring_update_round_trips`: `Local` A applies a message built by id B with `ttl_timeout = now + 60_000`, `idle_delta = 0`, so `lookup(k) == Some(Some(v))`. One with `ttl_timeout = now - 1` leaves `lookup(k) == None`. A's own `E` is ignored.
- [ ] **Step 2: Run** `cargo test --lib local_cache -- --test-threads 4`. Expected: FAIL (compile: `local_cache` missing).
- [ ] **Step 3: Implement.** Move first and build, then add `Lifetime`. Behaviour of `LocalCachedMap` stays identical. Its `CLEAR_LOCAL`/`CLEAR_ALL`/`PUT`/`REMOVE` stay in `local_cached_map.rs`.
- [ ] **Step 4: Run, one at a time:** `cargo test --lib local_cache -- --test-threads 4`; `cargo test --features moka --lib local_cache -- --test-threads 4`; `cargo test --test local_cached_map -- --test-threads 4`; `cargo test --features moka --test local_cached_map -- --test-threads 4`. Expected: `test result: ok.` for each, with the integration test count equal to `main`'s.
- [ ] **Step 5: Commit** `refactor(local_cache): share LocalCachedMap's local cache; per-entry lifetimes and the E message`

### Task 2: The type, builder, factory, the announced write path and the core methods

**Files:**
- Create: `src/collections/hash_map_cache/announced.rs` (`wrap_write`, `wrap_read`, the A1–A3 statics), `src/collections/hash_map_cache/local.rs` (`Op`, `HashMapCache::eval_write`, `read_cached`), `src/collections/local_cached_map_cache.rs`, `website/content/docs/objects/local-cached-map-cache.mdx`, `tests/local_cached_map_cache.rs`
- Modify: `hash_map_cache/mod.rs` (field `local: Option<Arc<LocalCache>>`; `pub(crate) fn with_local(key, codec, store, local: Arc<LocalCache>) -> Self`; `get`, `contains_key`, `remove`, `clear` and the internal `put` route through `eval_write`/`read_cached`; `HasKey` adds the updates log to `companions`/`renamed_companions` and returns `DEL_ANNOUNCED` from `del_script` when `local` is set; `length_prefix` becomes `pub(crate)`), `hash_map_cache/builders.rs` (`Insert`), `src/client/mod.rs`, `src/lib.rs`, `objects/meta.json`, `README.md`, `guides/redisson-migration.mdx`
- Test: unit tests in `announced.rs`; `tests/local_cached_map_cache.rs`

**Interfaces:**
- Consumes: Task 1's `LocalCache`, `start`, `Lifetime`, `copy_lifetime`, `ReadGuard`, `expiring_update`.
- Produces:
  - `pub(crate) enum Op<'a> { Put { entries: &'a [(Bytes, Bytes)], ttl_timeout: i64, idle_delta: i64 }, TryPut { field: &'a Bytes, value: &'a Bytes, ttl_timeout: i64, idle_delta: i64 }, IfSome(&'a Bytes), IfOne(&'a Bytes), PerKey(&'a [Bytes]), Touch(&'a [Bytes]) }` with `fn when(&self) -> &'static str` (`a`, `n`, `s`, `1`, `k`, `a`) and `fn notices(&self, local: &LocalCache) -> Vec<(Bytes, Bytes)>` (one `(message, log entry)` per key: `E` for `Put`/`TryPut` under `Update`, else `I`).
  - `pub(crate) async fn eval_write<R: redis::FromRedisValue>(&self, plain: &'static Lua, announced: &'static Lua, keys: Vec<String>, args: Vec<Bytes>, op: Op<'_>, once: bool) -> Result<R>`. Without `local` it is `core.eval`/`eval_once(plain, keys, args)`. With `local`, it forgets every key of `op`, evaluates `announced` to a `redis::Value` with `keys + [updates_log]` and `args + tail + [c]`, then on `Ok` caches `Put` entries (and `TryPut` when the reply is nil) with `copy_lifetime(ttl_timeout, idle_delta, now)`, and converts the value to `R`. On `Err` nothing is cached.
  - `pub(crate) async fn read_cached(&self, field: &Bytes) -> Result<Option<Bytes>>` (the `get` flow of `LocalCachedMap` with `GET_EXPIRY`: lookup, `begin_read`, the twin, `finish(Some((raw, lifetime)))`, misses only with `store_cache_miss`; the loader path as `HashMapCache::get`).
  - `announced.rs`: `pub(super) fn wrap_write(body: &str) -> String` (the text in "Recorded Redis-side additions"), `pub(super) fn wrap_read(body: &str, per_key: bool) -> String`, and the statics named in A1–A3. This task needs `PUT`, `PUT_WITH_LIMITS`, `REMOVE`, `GET_EXPIRY`, `CLEAR_ANNOUNCED`, `DEL_ANNOUNCED`; Task 3 adds the rest.
  - `pub struct LocalCachedMapCache<K, V, C: Codec> { cache: HashMapCache<K, V, C>, key: Key, codec: C, local: Arc<LocalCache> }` (`Debug`, `HasKey` delegating to `cache`).
  - `pub struct LocalCachedMapCacheBuilder<K, V, C: Codec> { key, codec, local: LocalOptions, options: StoreOptions<K, V> }` with `local_options!()`, `store_options!()` and `pub async fn build(self) -> Result<LocalCachedMapCache<K, V, C>>`. `build` runs `options.into_store`, then `HSET options has-listeners 1` (not in `StoreMode::Local`), then `start(&key, name, ..)`, then `HashMapCache::with_local` (which schedules the clean-up except in `StoreMode::Local`).
  - `Client::local_cached_map_cache<K, V>(&self, name: impl Into<Arc<str>>) -> LocalCachedMapCacheBuilder<K, V, C>`.
  - Methods in this task, with `HashMapCache`'s signatures: `get`, `contains_key`, `insert` (→ `hash_map_cache::Insert`), `remove`, `clear`, `len`, `is_empty`, plus `local_len`, `local_entries`, `clear_local`, `clear_local_everywhere` with `LocalCachedMap`'s signatures.
- [ ] **Step 1: Write failing unit tests in `announced.rs`.** `every_twin_holds_redissons_body`: for each pair (`PUT`, `PUT_WITH_LIMITS`, `REMOVE`, and in Task 3 the rest), `announced::X.source().contains(scripts::X.source())`. `twins_differ_from_their_body`: the SHA-1s differ.
- [ ] **Step 2: Write failing integration tests** (helpers `lcmc(client, name) -> LocalCachedMapCache<String, String, JsonCodec>` with default options, `plain(client, name) -> HashMapCache<..>`, and `eventually` as in `tests/local_cached_map.rs`):
  - `shares_data_with_hash_map_cache`: `lcmc.insert("a", "1").ttl(60s)`, then `plain.get("a") == Some("1")` and `plain.entry_ttl("a")` is `Some(d)` with `d <= 60s`. After `plain.insert("b", "2")`, `lcmc.get("b") == Some("2")`.
  - `writes_the_layout_of_hash_map_cache`: the same `insert(..).ttl(60s)` through `lcmc` on name `n1` and through `plain` on `n2`. `HGET` raw bytes are equal, and both `ZSCORE redissun__timeout__set:{nX} "\"a\""` are within 1000 ms of each other.
  - `a_read_is_cached_locally`: `plain.insert("a", "1")`; `lcmc.get("a") == Some("1")`, `local_len() == 1`. Then `plain.insert("a", "2")` (not announced), and `lcmc.get("a")` is still `Some("1")`.
  - `a_write_on_another_instance_invalidates_my_copy`: clients A and B. A reads `"a"`, B `insert("a", "2")`, `eventually(A.get("a") == Some("2"))`.
  - `the_update_strategy_sends_value_and_deadline`: both with `SyncStrategy::Update`. B `insert("k", "v").ttl(1s)`; `eventually(A.local_entries() == [("k","v")])`. After `sleep(1100ms)`: `A.local_len() == 0` and `A.get("k") == None` (the clean-up has not run yet: `plain.len() == 1`).
  - `a_local_copy_expires_with_the_entry_ttl`: `plain.insert("k","v").ttl(300ms)`, `A.get` caches it, `sleep(350ms)`, then `A.local_entries()` is empty and `A.get("k") == None`.
  - `a_copy_of_an_idle_entry_lives_half_the_max_idle`: `plain.insert("k","v").max_idle(400ms)`, `A.get` caches it. `sleep(250ms)`, then `plain.replace("k","new")` (not announced) and `A.get("k") == Some("new")`, because the copy died at about 200ms.
  - `reading_locally_keeps_an_idle_entry_alive`: `max_idle(400ms)`; six times `sleep(150ms)` and `A.get("k") == Some("v")`; then `plain.peek("k") == Some("v")` at about 900ms.
  - `remove_and_clear_invalidate_everywhere`: B reads `"a"`; A `remove("a")` returns `Some("1")`; `eventually(B.get("a") == None)`. B reads `"b"`; A `clear()`; `eventually(B.local_len() == 0)`.
  - `removing_a_missing_key_or_clearing_an_empty_cache_sends_nothing`: the same channel check as the `LocalCachedMap` test of that name, on `redissun__local_cache:<n>`.
  - `a_failed_write_leaves_no_local_copy` (Review Focus 4): A caches `"k"`. `raw_command(DEL n)`, then `raw_command(SET n x)`. `A.insert("k","2")` is `Err`, and `A.local_len() == 0`.
  - `build_marks_the_cache_for_entry_events`: `HGET {n}:redissun_options has-listeners` is `"1"` after `build`.
- [ ] **Step 3: Run** `cargo test --lib announced -- --test-threads 4`, then `cargo test --test local_cached_map_cache -- --test-threads 4`. Expected: FAIL (compile).
- [ ] **Step 4: Implement.** Docs: create the page: intro (experimental; `HashMapCache` + local cache; shares data with Redisson `RMapCache`/`HashMapCache`), the options table (link to `local-cached-map`'s for the shared ones), "How long a local copy lives", "Which reads use the local cache", "What a write announces" (the Decisions tables), "How it is stored". Add it to `meta.json`, add the README row, the migration naming row and a `## LocalCachedMapCache` section ("every `HashMapCache` method works and returns the same builders; the table lists the rest").
- [ ] **Step 5: Run** the commands of Step 3, then `cargo test --test hash_map_cache -- --test-threads 4` and `cargo test --test local_cached_map -- --test-threads 4`. Expected: `test result: ok.` for each; the `hash_map_cache` count is unchanged.
- [ ] **Step 6: Commit** `feat(local_cached_map_cache): LocalCachedMapCache over HashMapCache with announced writes and local lifetimes`

### Task 3: The rest of the writes, `entry`, leases, loader and writer

**Files:**
- Modify: `hash_map_cache/announced.rs` (the rest of A1), `hash_map_cache/mod.rs`, `builders.rs` (`TryInsert`, `Set`, `Extend`, `ExpireEntry`, `ExpireEntries`), `lease.rs`, `local_cached_map_cache.rs`, docs page, migration guide, `guides/loaders-and-writers.mdx`
- Test: `tests/local_cached_map_cache.rs`

**Interfaces:**
- Consumes: Task 2's `Op`, `eval_write`.
- Produces: each write site of `HashMapCache` calls `eval_write` with the `Op` of the Decisions table. `remove_many` with `local` set always takes the `FAST_REMOVE_BATCH` path (`Op::PerKey`) and returns the count of `1`s. The new delegations on `LocalCachedMapCache` keep `HashMapCache`'s signatures: `try_insert`, `set`, `replace`, `compare_and_set`, `remove_if_eq`, `remove_many`, `incr_by`, `incr_by_float`, `extend`, `expire_entry`, `expire_entries`, `entry`, `lease`, `invalidate`, `load_all`, `load_many`, `flush`. `Target::Cache` reads use the plain `GET` (`read_encoded`, unchanged), and its writes go through `eval_write`.
- [ ] **Step 1: Write failing tests** (A and B, with `SyncStrategy::Update` unless stated):
  - `try_insert_announces_only_when_it_stored` (Review Focus 2): B caches `"k" = "v1"`. A `try_insert("k","x")` returns `Some("v1")`; after `sleep(200ms)`, `B.local_entries()` still holds `("k","v1")`. A `try_insert("n","v")` returns `None`; `eventually(B.local_entries()` holds `("n","v"))`.
  - `set_with_a_wrong_lease_pushes_nothing` (Review Focus 2): B caches `"k" = "v1"`; `plain.lease("k", 5s)` gives `Lease::Acquired`. Then A `set("k","v2").lease("wrong")`. `B.get("k") == Some("v1")` at every check over 300ms, and `plain.get("k") == Some("v1")`.
  - `replace_keeps_the_limits_and_invalidates`: `A.insert("k","v").ttl(1s)`, B caches it, `A.replace("k","w")`, `eventually(B.get("k") == Some("w"))`. After the first insert plus 1100ms, `B.get("k") == None`.
  - `compare_and_set_and_remove_if_eq_announce_on_success_only` (Invalidate): B caches `"k"`. A `compare_and_set("k","wrong","x") == false`, and B's `local_len()` stays 1 for 200ms. `compare_and_set("k","v","x") == true`, then eventually B's local copy is gone. The same for `remove_if_eq`.
  - `remove_many_invalidates_each_removed_key`: B caches `a`, `b`, `c`; `A.remove_many(&["a","b","zz"]) == 2`; eventually B's local keys are `["c"]`.
  - `incr_by_invalidates`: on a `LocalCachedMapCache<String, i64, _>`, B caches `"n" = 1`; `A.incr_by("n", 2) == 3`; `eventually(B.get("n") == Some(3))`.
  - `extend_with_ttl_caches_and_announces_each_entry`: `A.extend([("a","1"),("b","2")]).ttl(1s)`; `A.local_len() == 2`; eventually B's local entries hold both; after 1100ms both are gone from both local caches.
  - `expire_entry_on_another_instance_shortens_my_copy` (Review Focus 1): B caches `"k"` (no TTL). `A.expire_entry("k").ttl(300ms) == true`; `eventually(B.local_len() == 0)`; `B.get("k") == Some("v")` re-caches it with the deadline; after 350ms `B.get("k") == None`.
  - `an_invalidation_during_a_read_is_not_overwritten` (Review Focus 4): the `LocalCachedMap` race test of `2250fb7`, ported: a `get` held between the request and `finish` (two tasks, a write from B in between) must not leave the old value cached.
  - `entry_reads_redis_and_announces_its_write`: A caches `"k" = "v"`; `plain.insert("k","w")`. `A.entry("k").await?` sees `"w"` (not the stale copy); `and_modify` to `"x"`; eventually `B.get("k") == Some("x")` and `A.get("k") == Some("x")` without a round trip (check with a later unannounced `plain.insert` that `A.get` still returns `"x"`).
  - `lease_writes_invalidate_other_copies` (Invalidate): B caches `"k"`; A `lease("k", 5s)` on a missing key gives a guard; `guard.insert("v")`, and eventually B's copy is dropped. `A.invalidate("k")` drops it again.
  - `the_loader_fills_redis_and_every_local_cache` (`common::store::MemoryStore`): `A.get("k")` loads `"v"`; `plain.get("k") == Some("v")` (no TTL); `A.local_len() == 1`; with `store_cache_miss(true)`, a key the loader lacks is cached as missing (`local_len` 2, and the loader is asked once).
  - `the_writer_sees_each_write_once`: insert, set, extend, remove, remove_many and replace each call the writer exactly as on `HashMapCache` (the same assertions as the `HashMapCache` writer test of 0c).
- [ ] **Step 2: Run** `cargo test --test local_cached_map_cache -- --test-threads 4`. Expected: FAIL.
- [ ] **Step 3: Implement.** Docs: the method table rows; loaders guide line "`local_cached_map_cache` takes the same options".
- [ ] **Step 4: Run** the command of Step 2, then `cargo test --test hash_map_cache -- --test-threads 4` and `cargo test --test map_store -- --test-threads 4`. Expected: `test result: ok.` for each.
- [ ] **Step 5: Commit** `feat(local_cached_map_cache): every write announced; entry, leases, loader and writer`

### Task 4: Reads, `preload_cache` and the delegated surface

**Files:**
- Modify: `hash_map_cache/announced.rs` (`GET_ALL_EXPIRY`), `hash_map_cache/mod.rs` (`get_many`, `peek`, `peek_many` with `local`; `scan_page` becomes `pub(crate)`), `local_cached_map_cache.rs` (`PreloadCache` and the rest of the delegations), `src/lib.rs` (`pub mod local_cached_map_cache { PreloadCache }`), docs page, migration guide
- Test: `tests/local_cached_map_cache.rs`

**Interfaces:**
- Produces:
  - `get_many` with `local`: local hits, then one `GET_ALL_EXPIRY` for the misses (with a `ReadGuard` per miss), each cached with its lifetime. The loader runs for the misses as `HashMapCache::get_many` does.
  - `pub fn preload_cache(&self) -> PreloadCache<'_, K, V, C>`, `#[doc(alias = "preloadCache")]`; `PreloadCache::page_size(self, n: usize) -> Self` (`0` means 10); `IntoFuture<Output = Result<usize>>`. It walks `SCAN_KEYS` pages and runs `GET_ALL_EXPIRY` per page. A key that vanished between the two is skipped.
  - Pure delegations with `HashMapCache`'s signatures and a doc line "Same as [`HashMapCache::<name>`]": `peek`, `peek_many`, `entry_ttl`, `contains_value`, `value_len`, `random_keys`, `random_entries`, `to_hash_map`, `iter`, `keys`, `values`, `iter_with`, `keys_with`, `values_with`, `set_max_size`, `init_max_size`, `events`, `events_of`, `keyspace_events`, `keyspace_events_of`, and `per_key_objects!()`.
- [ ] **Step 1: Write failing tests:**
  - `get_many_uses_local_hits_and_caches_misses`: A caches `k1 = "a"`. `plain.insert("k1","x")` and `plain.insert("k2","b")` (not announced). `A.get_many(&["k1","k2","k3"]) == [Some("a"), Some("b"), None]`, and `A.local_len() == 2`.
  - `peek_neither_caches_nor_restarts_the_idle_time`: `plain.insert("k","v").max_idle(300ms)`; `A.peek("k") == Some("v")`; `A.local_len() == 0`; after 350ms `plain.peek("k") == None`.
  - `contains_key_caches_a_miss_only_with_store_cache_miss`: `contains_key("zz") == false` with `local_len() == 0`. With `store_cache_miss(true)` it is `false` with `local_len() == 1`, and `plain.insert("zz","1")` (not announced) still gives `false`.
  - `preload_cache_fills_the_local_cache`: 25 entries through `plain`, one of them `ttl(300ms)`. `A.preload_cache().page_size(7) == 25`, and `A.local_len() == 25`. After 350ms the TTL entry is not in `A.local_entries()`.
  - `clear_local_everywhere_keeps_redis`: both cached; `A.clear_local_everywhere()`; `eventually(B.local_len() == 0)`; `plain.len() == 1`.
  - `delegated_methods_answer_like_hash_map_cache`: on the same data, `entry_ttl`, `contains_value`, `value_len`, `to_hash_map`, `iter().try_collect()` and `keys_with(MapScan::new().pattern("\"k1*"))` are equal on `A` and on `plain`. `A.set_max_size(10, Lru)` is visible through `plain` (`HGET options max-size`). `A.events()` receives a `Created` event for `plain.insert`.
  - `lock_lives_under_the_redisson_name`: `A.lock("a")` taken, so `EXISTS` of `map_key_name(n, "\"a\"", "lock")` is `:1` (the same expected name as the `HashMapCache` test).
- [ ] **Step 2: Run** `cargo test --test local_cached_map_cache -- --test-threads 4`. Expected: FAIL.
- [ ] **Step 3: Implement.** Docs: the rest of the method table and the `preload_cache` section.
- [ ] **Step 4: Run** the command of Step 2. Expected: `test result: ok.`
- [ ] **Step 5: Commit** `feat(local_cached_map_cache): get_many, peek, preload_cache and the HashMapCache surface`

### Task 5: Server-side removals, expiry policy, reconnection, local limits and moka

**Files:**
- Modify: `local_cache/listener.rs` (`EvictionFeed`, a third `select!` arm), `local_cache/mod.rs` (`start` takes `evictions`), `local_cached_map.rs` (passes `None`), `local_cached_map_cache.rs` (`build` subscribes both channels and passes the server's `LengthPrefix`), docs page, `guides/limitations.mdx`
- Test: `tests/local_cached_map_cache.rs`

**Interfaces:**
- Produces: `pub(crate) struct EvictionFeed { subscriptions: Vec<Subscription>, receivers: Vec<broadcast::Receiver<Bytes>>, prefix: LengthPrefix }` with `async fn next(&mut self) -> Eviction`, where `enum Eviction { Key(Bytes), Clear, Closed }` (`split_event` → `Key`; an unparsable payload or `Lagged` → `Clear`). The listener forgets `Key`, clears on `Clear`, and drops the feed on `Closed`.
- [ ] **Step 1: Write failing tests:**
  - `max_size_eviction_drops_my_copy` (Review Focus 3): `A.set_max_size(2, EvictionMode::Lru)`; `A.insert("k1","1")`, so A holds a copy. Then `plain.insert("k2",..)`, `plain.insert("k3",..)`; `eventually(!A.local_entries()` contains `"k1")`, and `plain.get("k1") == None`.
  - `the_clean_up_drops_a_copy_whose_ttl_was_shortened_behind_its_back` (Review Focus 3): client A with `ClientBuilder::eviction_interval(1s)`. A caches `"k"` (no TTL). `plain.expire_entry("k").ttl(200ms)` comes from a plain client, so nothing is announced. Within 5s, A's copy is gone (`eventually`), driven by the expired channel.
  - `a_removal_by_a_plain_cache_client_drops_my_copy`: A caches `"k"`; `plain.remove("k")`; `eventually(A.local_len() == 0)`.
  - `sync_none_still_drops_evicted_copies`: with `SyncStrategy::None`, the removal of the previous test still drops the copy, while `B.insert("k","2")` does not reach A's copy (A still returns the old value after 200ms).
  - `the_map_expiring_clears_every_local_cache`: as the `LocalCachedMap` tests (`CONFIG SET notify-keyspace-events EKx`, under `exclusive()`), for `KeyeventPattern` and `KeyspaceChannel`. `A.expire(200ms)` on the object clears B's local cache. `ExpirationEventPolicy::None` keeps it.
  - `the_clear_strategy_clears_the_cache_after_a_reconnect`, `the_load_strategy_drops_only_the_keys_changed_while_away`: ported from `tests/local_cached_map.rs` with the same `kill_pubsub_connections` helper. Plus `the_load_strategy_logs_every_announced_write`: after `A.insert`, `A.try_insert` (stored), `A.remove_many`, `ZCARD redissun__cache_updates_log:{n}` grew by the number of changed keys.
  - `the_local_cache_size_limit_is_independent_of_max_size`: `cache_size(2)` with `EvictionPolicy::Lru` and no server max size. After reading 3 keys, `local_len() == 2`, `plain.len() == 3`.
  - `#[cfg(feature = "moka")] moka_copies_expire_with_the_entry_ttl`: `cache_provider(CacheProvider::Moka)`; `a_local_copy_expires_with_the_entry_ttl`'s assertions.
- [ ] **Step 2: Run** `cargo test --test local_cached_map_cache -- --test-threads 4`. Expected: FAIL.
- [ ] **Step 3: Implement.** Docs: "Entries the server drops" (channels, `has-listeners`, the clean-up), "Reconnection", "Expiry of the whole cache". Limitations: writes by non-local clients are not announced (their removals are); local hits do not count as uses for the server's max size; deadlines use the clocks of the clients that write; a message can arrive a few milliseconds late.
- [ ] **Step 4: Run** `cargo test --test local_cached_map_cache -- --test-threads 4`, then `cargo test --features moka --test local_cached_map_cache -- --test-threads 4`, then `cargo test --test local_cached_map -- --test-threads 4`. Expected: `test result: ok.` for each.
- [ ] **Step 5: Commit** `feat(local_cached_map_cache): server-side removals reach local caches; reconnection and expiry policy`

### Task 6: `StoreMode::Local`

**Files:**
- Modify: `hash_map_cache/local.rs` (a `local_only` branch per supported method: `fn local_only(&self) -> Option<&LocalCache>` and the in-memory versions on encoded bytes), `hash_map_cache/mod.rs`, `builders.rs`, `lease.rs` (the `Error::Config` guard), `local_cached_map_cache.rs`, docs page, limitations
- Test: `tests/local_cached_map_cache.rs`

**Interfaces:**
- Produces: in `StoreMode::Local`, each supported method answers from the local cache with the reply `HashMapCache` gives for the same state. `insert` returns the previous local value; `try_insert` returns `Some(live)` or stores; `set` returns whether the key was new; `remove_many` returns how many it removed; `expire_entry` returns whether a live copy exists; `entry_ttl` returns the time left to the copy's TTL deadline. A write stores `Lifetime { deadline: ttl, idle: max_idle }` and publishes the `Op`'s messages with `LocalCache::publish` (no Lua), as `LocalCachedMap` does in this mode. `fn needs_redis(method: &str) -> Error` gives `Error::Config(format!("LocalCachedMapCache::{method} needs StoreMode::LocalRedis"))`. `Object::del` uses `LocalCachedMap`'s `CLEAR_LOCAL`. `entry` takes the per-key lock in Redis and reads and writes locally.
- [ ] **Step 1: Write failing tests** (`store_mode(StoreMode::Local)`):
  - `local_mode_keeps_entries_out_of_redis`: after `insert("k","v").ttl(60s)`, `EXISTS n` is `:0`, `EXISTS {n}:redissun_options` is `:0`, and `get("k") == Some("v")`.
  - `local_mode_entries_honour_ttl_and_max_idle`: a `ttl(200ms)` entry is gone after 250ms. A `max_idle(200ms)` entry read every 100ms is alive at 500ms, and gone 250ms after the last read. `entry_ttl` is `Some(d <= 60s)`.
  - `local_mode_syncs_other_instances`: with `Update`, B holds A's `insert("k","v").ttl(1s)` and drops it after 1.1s. With `Invalidate`, A's insert drops B's only copy.
  - `local_mode_len_iter_and_clear`: `len`, `is_empty`, `iter`, `to_hash_map` over the live local entries; `clear()` empties them and B's.
  - `local_mode_loader_and_writer`: a miss asks the loader, and the loaded value is cached; the writer sees insert and remove.
  - `local_mode_redis_only_methods_are_config_errors`: `incr_by`, `set_max_size`, `init_max_size`, `events`, `keyspace_events`, `lease`, `invalidate`, `contains_value`, `value_len`, `random_keys`, `random_entries`, `iter_with` (first item), `set(..).lease(t)` each give `Err(Error::Config(m))` with `m` containing `"StoreMode::LocalRedis"`, and nothing reaches Redis (`EXISTS n` is `:0`).
  - `local_mode_preload_returns_zero`: `preload_cache() == 0`.
- [ ] **Step 2: Run** `cargo test --test local_cached_map_cache local_mode -- --test-threads 4`. Expected: FAIL.
- [ ] **Step 3: Implement.** Docs: a "Local store mode" section with the supported and the unsupported lists.
- [ ] **Step 4: Run** `cargo test --test local_cached_map_cache -- --test-threads 4`. Expected: `test result: ok.`
- [ ] **Step 5: Commit** `feat(local_cached_map_cache): StoreMode::Local with per-entry TTL and max idle`

### Task 7: Cluster, `Object`, limitations, migration check, full verification

**Files:**
- Modify: `tests/local_cached_map_cache.rs`, `guides/limitations.mdx`, `guides/redisson-migration.mdx`, docs page
- [ ] **Step 1: Write tests:**
  - `every_operation_runs_on_a_cluster` (Review Focus 5): on a 3-master 7.4 cluster (`tests/common/topology.rs`), name `{t}:m`, two instances with `ReconnectionStrategy::Load` and `SyncStrategy::Update`: `insert(..).ttl`, `get`, `get_many`, `try_insert`, `replace`, `remove_many`, `extend(..).ttl`, `expire_entry`, `incr_by`, `entry`, `preload_cache`, `clear`. A max-size eviction reaches the other instance. `ZCARD redissun__cache_updates_log:{t}:m` is `> 0`. No call returns `CROSSSLOT`.
  - `object_methods_cover_every_key`: `expire(1s)` then `ttl()` is `Some`; `persist()`; `rename(n2)` moves the hash, the sets, the options and the updates log; `del()` deletes all of them and clears B's local cache (`C` message).
- [ ] **Step 2: Docs.** `limitations.mdx`: a `LocalCachedMapCache` item (no Redisson peer on the topic, because `RLocalCachedMapCache` is PRO; the topic is `LocalCachedMap`'s format; `rename` does not move other instances' local caches; plus the Task 5 items and the `StoreMode::Local` list). Migration: check the `## LocalCachedMapCache` table against `javap` for `RLocalCachedMapCache`, `RLocalCachedMap`, `RMapCache`, `RLeasedMap` and `LocalCachedMapCacheOptions`. Every method and option is named in a row or covered by "as `HashMapCache`".
- [ ] **Step 3: Run the full verification, one command at a time:** `cargo fmt --check`; `cargo clippy --all-targets -- -D warnings`; `cargo clippy --all-targets --all-features -- -D warnings`; `cargo test -- --test-threads 4`; `cargo test --features moka -- --test-threads 4`; `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps`; `cargo bench --bench compare`, with the HashMapCache and LocalCachedMap rows compared to `main`. Expected: all green, no warnings; the bench rows stay within noise (`local: None` adds one `Option` check). The numbers go into the PR.
- [ ] **Step 4: Commit** `test(local_cached_map_cache): cluster and object methods; limitations and migration rows`

### Task 8: PR text and changelog

- [ ] **Step 1: Open the PR** `feat: LocalCachedMapCache, Redisson's RLocalCachedMapCache (#4)` with this body. During the PR, verify on the GitHub CI logs, not by running locally.

```markdown
## Summary
New experimental `LocalCachedMapCache`: `HashMapCache`'s data (Redisson's `RMapCache` layout and scripts, shared with Redisson and `HashMapCache` clients) plus `LocalCachedMap`'s local cache and sync messages. Local copies honour each entry's TTL and max idle, and entries the server drops (clean-up, max size, removals) are dropped locally. Every method of Redisson's `RLocalCachedMapCache` and every option of `LocalCachedMapCacheOptions`.

## Changelog
### Added
- `LocalCachedMapCache` (experimental; `client.local_cached_map_cache(name)….build().await`): every `HashMapCache` method with the same builders, plus `local_len`, `local_entries`, `clear_local`, `clear_local_everywhere` and `preload_cache().page_size(n)`; every `LocalCachedMap` option, the loader, writer and write modes, and `StoreMode::Local` (per-entry TTL and max idle kept locally; Redis-only methods are `Error::Config`).
- Docs: the `LocalCachedMapCache` page, migration rows, limitations, loaders guide.

### Changed
- `LocalCachedMap`'s local cache moved to a shared private module; its API and behaviour are unchanged.

### Behaviour that differs from Redisson on purpose
- The sync messages use `LocalCachedMap`'s channel and format, not Redisson's `LocalCachedMessageCodec`; Redisson's implementation is PRO-only, so no Java client can be a peer on the topic. The data in Redis is shared.
- A local copy of an entry with a max idle lives for half the max idle after its last read from Redis, so the entry stays alive on the server while it is read locally.
- `SyncStrategy::Update` sends the entry's limits with the value (message `E`); writes whose limits the client does not know (`replace`, `compare_and_set`, `incr_by`, `expire_entry`, lease writes) send an invalidation.
- `entry(&k)` reads Redis under the key's lock, not the local copy.

### Redis-side additions (recorded per CLAUDE.md; no Redisson script is edited)
- A1: announced twins of 16 `RedissonMapCache` write scripts: Redisson's body verbatim inside a wrapper that publishes the sync message and writes the updates log in the same script.
- A2: `GET_EXPIRY`, `GET_ALL_EXPIRY`: Redisson's `GET`/`GET_ALL` plus the entry's TTL score and idle delta in the reply.
- A3: `CLEAR_ANNOUNCED`, `DEL_ANNOUNCED` (redissun-only, like `LocalCachedMap`'s `CLEAR_ALL`).
- A4: `build` sets `has-listeners` in the options hash, as Redisson's `addListener` does, so every client's writes publish entry events from then on (needed for max-size evictions to reach local caches).
```

- [ ] **Step 2:** Check that the changelog, the docs page and `limitations.mdx` describe the same copy lifetime, the same announce table and the same `StoreMode::Local` lists. No commit: Tasks 1–7 hold the commits.

## Owner questions

1. **Server-side LRU/LFU and local hits.** With `set_max_size`, a local hit does not reach Redis, so it does not count as a use there. The entries read most through local caches then look cold on the server and are evicted first. That is the opposite of what a size-limited cache is for. The plan documents this and does nothing else. The alternative is that a local hit on a size-limited cache also sends Redisson's last-access update for that key, at most once per key per N seconds (a new redissun-only script, plus a background task). Keep it documented-only, or add the throttled touch (and with which N)?

## Owner decisions (delegated, 2026-11-27)

1. Local hits are not reported to the server's LRU/LFU: with `set_max_size`, documented only (no throttled last-used update). It can be added later if someone needs it.
2. Task 6 (`StoreMode::Local`) stays.
