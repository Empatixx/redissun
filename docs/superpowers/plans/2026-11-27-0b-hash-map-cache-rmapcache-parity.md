# 0b: HashMapCache → full RMapCache parity — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `HashMapCache` gets every method of Redisson's `RMapCache` 4.8.0, which includes all of `RMap` and the new `RLeasedMap`. It runs on Redisson's own Lua scripts, key names, value layout and event format. Its API stays as close to `std::collections::HashMap` (and to redissun's `HashMap` after 0a) as possible: the same name means the same behaviour.

**Architecture:** First, port the Redis side 1:1. Every script is copied from `RedissonMapCache.java` into `src/collections/hash_map_cache/scripts.rs`. The client clock goes in as `ARGV`, the values are packed with `struct.pack('dLc0', …)`, and the key and channel names follow Redisson's `prefixName`/`suffixName`. Then add the Rust surface as thin methods and `IntoFuture` builders. Which Redisson script a method runs is chosen on the Rust side, so that a method behaves like its `HashMap` namesake. The surface reuses `src/collections/map/*`: `Entry` (through a private target enum), `KeyLock`, `MapScan`, `decode_pairs`, `events::subscribe` and the per-key names.

**Tech Stack:** Rust 2024, tokio, redis-rs, Redis Lua `struct` library, testcontainers (Redis 7.4 and 8.8, Cluster).

**Spec:** `docs/superpowers/specs/2026-11-26-redisson-4x-objects-design.md` (section "0. Full map parity first", item 0b) and `docs/superpowers/specs/2026-11-27-rust-idiomatic-api.md` (naming rules and rulings).

## Owner decisions (2026-11-27)

1. `evict_expired()` is removed.
2. `try_insert` returns `Option<V>`, like `HashMap::try_insert`.
3. Same name ⇒ same behaviour as `HashMap`. `set` and `insert` differ only in what they return. Without `.ttl()`/`.max_idle()`, a write replaces the entry and clears its limits, the way std's `insert` replaces the value. `entry()` writes the same way. See "Write semantics".
4. `getWithTTLOnly`/`getAllWithTTLOnly` become `peek`/`peek_many` (like `lru::LruCache::peek`).
5. Leases are a guard (`Lease` / `LeaseGuard`), not token strings passed around.
6. The keyspace listeners are `keyspace_events()`/`keyspace_events_of()`. `events()` stays the entry listener.
7. `value_len` returns the length of the encoded value, without Redisson's header.
8. Old data is moved by a helper, `migrate_from_0_23()` (Task 9). "Old vs new data format" below describes both layouts. The steps go on a new docs page, `guides/upgrading.mdx` ("Upgrading redissun"), organised by version; future data-format changes get a section there too.
9. `invalidate(&k)` is the token-free `removeWithLease`, next to `LeaseGuard::remove()`.
10. `LeaseGuard::insert` (and `set(..).lease(token)`) keep Redisson's reply: `false` both when the lease was lost (nothing stored) and when the token matched but a live entry was already there (stored over it). Documented; no extra `GET`.
11. No "write but keep the TTL" for now. A later `set(..).keep_ttl()` on Redisson's `FAST_PUT` stays possible (see "Write semantics").
12. `Duration::ZERO` passed to `.ttl()`/`.max_idle()` keeps meaning "no limit", as in Redisson.
13. The migration runs in batches: `migrate_from_0_23()` is an `IntoFuture` builder with `.batch_size(n)` (default 1000). Each call is atomic for up to `n` entries and returns how many it handled; callers loop until it returns `0`. The whole migration is not atomic.

## What the current HashMapCache does differently from Redisson

Checked against `RedissonMapCache.java` and `MapCacheEvictionTask.java` (4.8.0 sources in the scratchpad `rsrc/`). Today's scripts are redissun's own. They are not Redisson's:

| Aspect | redissun today | Redisson 4.8.0 (target, with `redisson` → `redissun`) |
|---|---|---|
| Clock | `redis.call('TIME')` in every script | `System.currentTimeMillis()` as `ARGV[1]`; the TTL and idle deadlines are computed on the client (`now + ttl`) |
| Value in the hash | text `"<idleMs>:<value>"`, parsed with `string.find` | `struct.pack('dLc0', maxIdleMs, #value, value)` (see "Old vs new data format") |
| Main hash key | `{name}` (the client adds a hash tag) | `name` (raw); the companions carry the tag |
| Timeout / idle set | `redissun__timeout__set:{name}`, `redissun__idle__set:{name}` | the same: `prefixName("redissun__timeout__set", name)`, `prefixName("redissun__idle__set", name)` |
| Last access set | `redissun__map_cache_last_access__set:{name}` | `redissun__map_cache__last_access__set:{name}` (double `__` before `last`) |
| Options hash | `redissun__map_cache_options:{name}` | `suffixName(name, "redissun_options")` → `{name}:redissun_options` |
| Lease keys | none | `prefixName("redissun__map__lease", name) + ":" + <encoded key>` |
| Channels | `redissun_map_cache_{created,updated,removed,expired}:{name}` | the same names, but built with `prefixName` from the raw name |
| Event payload | `<C/U/R/E><klen>:<vlen>:<k><v><old>` as text | `struct.pack('Lc0Lc0[Lc0]', …)`: key, value, then the old value for an update. The lengths are 8-byte little-endian, 4-byte LE on a Windows server and 8-byte BE on HP NonStop (`BaseEventCodec`, based on `INFO server` `os`) |
| Publish command | literal `PUBLISH` | `ARGV` `publishCommand` (Redisson sends `SPUBLISH` when sharding is supported; redissun always passes `PUBLISH`, see limitations) |
| Eviction latch | `SET latch 1 NX PX <ms>`, an optional flag | `SETNX latch 1` + `EXPIRE latch <min(delay,30) s>`, always taken; key `prefixName("redissun__execute_task_once_latch", name)` |
| Expire / persist | no `param` argument | `ARGV[2]` = `NX`/`XX`/… or `''` |
| `try_insert` | its own 0/1 script | `putIfAbsent` returns the existing value |

Because of the value layout, the main key and the options key, data written by redissun 0.23 and earlier cannot be read by the new scripts until `migrate_from_0_23()` (Task 9) has moved it. See "Old vs new data format" and "Breaking changes".

## Old vs new data format

`<name>` is the name passed to `client.hash_map_cache(name)`. `<tag>` is the hash-tagged form: `{<name>}` when the name contains no `{`, otherwise `<name>` itself (both versions). Fields are the codec-encoded key (`JsonCodec`: `"\"a\""`). Values are the codec-encoded value `<v>`. All scores are Unix time in milliseconds unless stated.

| What | 0.23 and earlier (old) | after 0b (Redisson's layout) |
|---|---|---|
| Main hash | key `{<name>}` (or `<name>` if it has a tag) | key `<name>` (raw, no tag added) |
| Field value bytes | text: `<idleMs>:<v>` where `<idleMs>` is the decimal max idle in ms, or just `<v>` (read as idle 0; a `:` in such a `<v>` is misread, as today) | binary `struct.pack('dLc0', idleMs, #v, v)`: bytes 0–7 = `idleMs` as an IEEE-754 double in the server's native byte order (little-endian on x86/ARM); bytes 8–15 = `#v` as native `unsigned long` (8 bytes LE on 64-bit Linux/macOS; 4 bytes, so bytes 8–11 and a 12-byte header, on a Windows server); bytes 16.. = `v`. `idleMs` = 0 means no max idle |
| Timeout set | `redissun__timeout__set:<tag>`, score = server `TIME` + ttl | same key; score = client `now` + ttl |
| Idle set | `redissun__idle__set:<tag>`, score = server `TIME` + idle, moved forward on each read | same key; score = client `now` + idle, moved forward on each read |
| Expire tag | member `redissun__expiretag` with score `92233720368547758` in both sets (and the access set when limited) while `Object::expire` is set | the same |
| Last-access set | `redissun__map_cache_last_access__set:<tag>`; LRU score = server time of last use, LFU score = use count | `redissun__map_cache__last_access__set:<tag>`; LRU score = client `now` of last use, LFU score = use count |
| Options hash | `redissun__map_cache_options:<tag>` | `{<name>}:redissun_options` (or `<name>:redissun_options` if the name has a `{`) |
| Options fields | `max-size` (decimal), `mode` (`LRU`/`LFU`), `has-listeners` (`1`) | the same three fields |
| Lease keys | none | string `redissun__map__lease:<tag>:<field>`, value = token, `PX` = lease time |
| Channels | `redissun_map_cache_{created,updated,removed,expired}:<tag>` | the same names |
| Event payload | text `<C\|U\|R\|E><klen>:<vlen>:<k><v>[<old>]`, lengths in decimal | binary `struct.pack('Lc0Lc0', #k, k, #v, v)`, plus `Lc0` with the old value for updated; `L` = 8 bytes LE (4 bytes LE on Windows, 8 bytes BE on HP NonStop) |
| Eviction latch | `redissun__execute_task_once_latch:<tag>`, `SET 1 NX PX <ms>`, only when the flag is on | same key, `SETNX 1` + `EXPIRE min(delay, 30) s`, always |

Consequences: for a name without `{`, the main key and the options and last-access keys change name, and the timeout and idle sets, the channels and the latch keep their names. For a name with a hash tag, the main key keeps its name and only the value bytes change, so a new client reading an unmigrated field gets a `struct.unpack` script error (Review Focus 2). In both cases every key stays in the same cluster slot (`{<name>}` and `<name>` hash the same).

## Write semantics (owner decision 3)

`std::collections::HashMap::insert` replaces the value. Every plain write in `HashMapCache` therefore replaces the entry and clears its TTL and max idle. `.ttl(d)`/`.max_idle(d)` set new limits. Checked against `RedissonMapCache.java`:

| Script (line) | Clears TTL/idle without limits | Frees a lease | Max-size check |
|---|---|---|---|
| `PUT` `putOperationAsync(k,v)` (904) | yes (`zrem` both sets, header 0) | yes (`del KEYS[9]..field`) | after the write, `>`, new keys only |
| `PUT_WITH_LIMITS` (1913) | yes, with zeros (`zrem` when `ARGV` ≤ 0) | no | before the write, `>=` |
| `FAST_PUT_WITH_LIMITS` (1371), empty token | yes, with zeros | no (only with a token) | before the write, `>=` |
| `FAST_PUT` `fastPutOperationAsync(k,v)` (2518) | **no**: keeps a live entry's timeout score and its idle score, but writes header idle 0, so the idle limit is off for reads yet the clean-up task still deletes the entry at the old idle deadline | no | after, `>` |
| `PUT_ALL` (3027) | **no**: keeps the timeout and idle scores, header 0 (same half state as `FAST_PUT`) | no | after, `>`, new keys only |
| `PUT_ALL_TTL` (3125) | the timeout set only; leaves the idle score | no | after, `>` |
| `PUT_ALL_TTL_IDLE` (3237), zeros | yes | no | after, `>`, new keys only |

The choice, all verbatim Lua, picked on the Rust side:

| Call | Script | Reply |
|---|---|---|
| `insert(&k,&v)` | `PUT` | `Option<V>` |
| `insert(&k,&v).ttl/.max_idle` | `PUT_WITH_LIMITS` | `Option<V>` |
| `set(&k,&v)` | `PUT` (same script as `insert`) | `nil` → `true`, a value → `false`; the old value's bytes come back over the wire but are not decoded |
| `set(&k,&v).ttl/.max_idle` | `FAST_PUT_WITH_LIMITS` with an empty token | `1`/`0`; it behaves exactly as `PUT_WITH_LIMITS` |
| `entry(&k)` write | `PUT`, reply dropped | — |
| `entry(&k).ttl(d)` write | `FAST_PUT_WITH_LIMITS` with the TTL only (Redisson's `compute(k, ttl, f)` → `fastPut(k, v, ttl)`) | — |
| `extend(..)` with or without limits | always `PUT_ALL_TTL_IDLE`, zeros when unset | — |

So `set == insert` up to the return type, with and without limits: the same scripts or scripts whose Redis effects are identical (sets, header, eviction rule, events, lease handling). Remaining differences that are Redisson's and stay: a plain `insert`/`set` frees a pending lease on that key and a write with limits does not (Redisson's `put` vs `put(.., ttl)`); a write with limits checks the size before writing with `>=`, so overwriting a live key in a full cache also evicts one other entry (Redisson's quirk in both `*_WITH_LIMITS` scripts); `extend` never frees a lease (no `putAll` script has the lease key).

Deviations recorded in the PR: Redisson's `fastPut(k,v)` and `compute(k,f)` keep a live entry's TTL, and `putAll(map)` keeps both scores; redissun's `set`, plain `entry` and `extend` clear them, like `insert`. `FAST_PUT`, `PUT_ALL` and `PUT_ALL_TTL` are not ported. If a "write but keep the TTL" turns out to be needed, a `set(..).keep_ttl()` on `FAST_PUT` can be added later (owner decision 11).

## What is missing today

The reference is `javap org.redisson.api.RMapCache`/`RMap`/`RLeasedMap` from the 4.8.0 jar. "Script" names the `static` in `scripts.rs` and the Redisson method it is copied from, with line numbers in `RedissonMapCache.java`.

| Redisson | redissun after 0b | Script (source) | Task |
|---|---|---|---|
| `get` | `get` (rewired) | `GET` (`getOperationAsync` 859) | 1 |
| `put(k,v)`, `put(k,v,ttl[,idle])` | `insert(&k,&v)[.ttl(d)][.max_idle(d)]` → `Option<V>` (rewired) | `PUT` (904), `PUT_WITH_LIMITS` (1913) | 1 |
| `putIfAbsent(k,v[,ttl[,idle]])`, `fastPutIfAbsent(..)` | `try_insert(..)` → `Option<V>` | `PUT_IF_ABSENT_WITH_LIMITS` (707), always, see "Not ported" | 1 |
| `remove(k)` | `remove` (rewired) | `REMOVE` (2216) | 1 |
| `containsKey` | `contains_key` (rewired) | `CONTAINS_KEY` (508) | 1 |
| `remainTimeToLive` | `entry_ttl` (rewired) | `REMAIN_TTL` (2130) | 1 |
| `setMaxSize`, `trySetMaxSize` | `set_max_size`, `init_max_size` (new options key) | `TRY_SET_MAX_SIZE` (345) | 1 |
| eviction task | background task (rewired) | `EVICT` (`MapCacheEvictionTask.execute`) | 1 |
| `expire`, `clearExpire`, `delete`, `clear`, `rename` | `Object` methods (rewired) | `EXPIRE` (3544), `CLEAR_EXPIRE` (3600) | 1 |
| `addListener(MapEntryListener)` | `events()`, `events_of(&[EntryEventKind])` → `EntryEvents` | — | 1 (payload), 2 (names) |
| `addListener(MapPutListener/MapIncrListener/MapRemoveListener/Deleted/Expired)` | `keyspace_events()`, `keyspace_events_of(&[MapEventKind])` → `MapEvents` | — (keyspace notifications) | 2 |
| `fastPut(k,v[,ttl[,idle]])` | `set(&k,&v)[.ttl][.max_idle]` → `bool` | `PUT` (904), `FAST_PUT_WITH_LIMITS` (1371); see "Write semantics" | 3 |
| `replace(k,v)`, `fastReplace`, `putIfExists`, `fastPutIfExists` | `replace(&k,&v)` → `Option<V>` | `REPLACE` (2988) | 3 |
| `replace(k,old,new)` | `compare_and_set` | `REPLACE_IF` (2909) | 3 |
| `remove(k,v)` | `remove_if_eq` | `REMOVE_IF` (812) | 3 |
| `fastRemove(k…)` | `remove_many(&[&Q])` → `usize` | `FAST_REMOVE` (2296) | 3 |
| `addAndGet` | `incr_by(&k, i64)`, `incr_by_float(&k, f64)` | `ADD_AND_GET` (1229) | 3 |
| `putAll(map)`, `putAll(map, batch)`, `putAll(map, ttl[, idle])` | `extend(entries)[.batch_size(n)][.ttl(d)][.max_idle(d)]` | `PUT_ALL_TTL_IDLE` (3237), always | 3 |
| `getAll` | `get_many(&[&Q])` → `Vec<Option<V>>` | `GET_ALL` (597) | 4 |
| `containsValue` | `contains_value` | `CONTAINS_VALUE` (549) | 4 |
| `valueSize` | `value_len` (encoded value length: `HSTRLEN` minus the header) | — | 4 |
| `randomKeys`, `randomEntries` | `random_keys`, `random_entries` | `RANDOM_KEYS` (3702), `RANDOM_ENTRIES` (3747) | 4 |
| `readAllMap/EntrySet/KeySet/Values` | `to_hash_map()` | `READ_ALL` (`readAll` 3797) | 4 |
| `keySet/values/entrySet(pattern,count)`, iterators | `iter/keys/values`, `iter_with/keys_with/values_with(MapScan)` (0a's `HashMap` names) | `SCAN_ENTRIES` (2441), `SCAN_KEYS` (2350), `TOUCH_IDLE` (2490) | 4 |
| `getLock/…/getPermitExpirableSemaphore(K)` | `lock`, `fair_lock`, `rw_lock`, `semaphore`, `count_down_latch`, `permit_expirable_semaphore` | — | 5 |
| `compute/computeIfAbsent/computeIfPresent/merge`, `compute(k, ttl, f)`, `computeIfAbsent(k, ttl, f)` | `entry(&k)[.ttl(d)].await?` → `hash_map::Entry` | uses `GET`, `PUT`, `FAST_PUT_WITH_LIMITS`, `FAST_REMOVE` | 5 |
| `expireEntry`, `expireEntryIfNotSet`, `updateEntryExpiration` (deprecated) | `expire_entry(&k)[.ttl][.max_idle][.if_not_set()]` → `bool` | `EXPIRE_ENTRY` (1514), `EXPIRE_ENTRY_IF_NOT_SET` (1593) | 6 |
| `expireEntries`, `expireEntriesIfNotSet` | `expire_entries(&[&Q])[…]` → `usize` | `EXPIRE_ENTRIES` (1768), `EXPIRE_ENTRIES_IF_NOT_SET` (1672) | 6 |
| `getWithTTLOnly`, `getAllWithTTLOnly` | `peek(&k)`, `peek_many(&[&k])` | `GET_WITH_TTL_ONLY` (2017), `GET_ALL_WITH_TTL_ONLY` (2089) | 6 |
| `getWithLease` | `lease(&k, time)` → `Lease<'_, K, V, C>` | `LEASE_GET` (125) | 7 |
| `putWithLease(..[,ttl[,idle]], token)` | `LeaseGuard::insert(&v)[.ttl][.max_idle]`; across processes `set(..).lease(token)` | `FAST_PUT_WITH_LIMITS` (1371) with the token | 7 |
| `removeWithLease` | `LeaseGuard::remove()`, and `invalidate(&k)` for callers without a lease | `LEASE_REMOVE` (213) | 7 |
| — (old redissun data) | `migrate_from_0_23()[.batch_size(n)]` → `usize`, called until `0` | `MIGRATE_FROM_0_23` (redissun's own) | 9 |

**Not ported, my call. Each one gets a line in the PR, and the user-visible ones also go in `limitations.mdx`:**
- `putIfAbsentOperationAsync(k,v)` (1085, without limits): on an expired entry that has not been evicted yet, it writes the value but leaves the old score in the timeout set, so the new value stays invisible. `try_insert` therefore always calls the `PUT_IF_ABSENT_WITH_LIMITS` script, with `0`s when there are no limits. That is Redisson's own Lua, picked on the Rust side, and it keeps the existing test `try_insert_only_inserts_over_a_missing_or_expired_entry` green.
- `FAST_PUT` (2518), `PUT_ALL` (3027), `PUT_ALL_TTL` (3125): they keep a TTL or idle score on a plain write, which breaks "same name ⇒ same behaviour". See "Write semantics".
- `fastPutIfAbsent*` (2666, 2813), `fastReplace` (2949), `putIfExists`/`fastPutIfExists` (995, 2605): they only differ from the ported methods in what they return (naming rule: merge `fast*`). `putIfExists` also resets the idle time. `replace` keeps it. Documented.
- `mapReduce`, `TrackingListener`, `MapLoader`/`MapWriter` (0c), `destroy()` (the eviction task ends with the client), `sizeInMemory` (redissun has no such method on any object), `expireAt`, and `expire` with a condition (`Object` has neither).
- `evict_expired()` is removed (owner decision 1; Redisson has no such method).

## Global Constraints

- `#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::todo, clippy::unimplemented)]`: no unwrap/expect in library code. Tests may unwrap.
- Lua: each Redisson script is copied verbatim from the Redisson line range in the table above. The only edits are `redisson__expiretag` → `redissun__expiretag` and `temp_val__redisson` → `redissun__temp_val`. Each `static` gets a `/// RedissonMapCache.<method>` comment. KEYS and ARGV are passed in Redisson's order. The one redissun-only script is `MIGRATE_FROM_0_23` (Task 9).
- Key/channel prefix `redissun_` instead of `redisson_`. Names are built only with `prefix_name`/`suffix_name` (Redisson's rules: a name that contains `{` is used as is).
- `ARGV[1]` (`now`) is the client's `SystemTime` in ms. The deadlines are `now + millis(d)`. A zero `Duration` passed to `.ttl()`/`.max_idle()` means "no limit", as in Redisson and as today's test `a_zero_ttl_means_no_time_limit` pins (owner decision 12). A duration that overflows `i64` ms is `Error::Config` (existing test `a_huge_ttl_is_a_config_error`).
- Every public method name matches `HashMap`'s for the same operation, behaves the same way, and has `#[doc(alias = "<Redisson name>")]`. The only extra names are the RMapCache-only methods listed above. The one name with a different meaning is `events()` (entry events here, keyspace events on `HashMap`); it is documented on both types.
- Optional settings are builder methods (`.ttl()`, `.max_idle()`, `.if_not_set()`, `.batch_size()`, `.lease()`), never `bool` or sentinel parameters; no public method takes a lease token except the cross-process `set(..).lease(token)`.
- The builders live in `pub mod hash_map_cache` in `lib.rs` (the same pattern as `hash_map`, `bucket`). `HashMapCache`, `EntryEvent`, `EntryEventKind`, `EntryEvents`, `EvictionMode`, `Lease` and `LeaseGuard` are at the crate root (guards are at the root, like `LockGuard`).
- Private traits must not appear in public bounds (`private_bounds` is denied by `-D warnings`). Shared behaviour across the two maps goes through a private enum in a private field or a `macro_rules!`, not through a `pub(crate)` trait bound on a public type.
- One cargo build or test run at a time, with `--test-threads 4`. No git worktrees.
- Docs go in the task that changes the behaviour: `website/content/docs/objects/hash-map-cache.mdx` (method table), `guides/redisson-migration.mdx` (the `## HashMapCache` table), `guides/limitations.mdx`, and for stored-data changes `guides/upgrading.mdx` (Task 9).

## Review Focus

1. **Clocks differ between clients.** After this change, expiry follows the clock of the client that runs the script, as in Redisson. A reasonable user expects an entry whose deadline has passed by *this* client's clock to be hidden, and one in the future to be visible, whatever the server clock says. Pinned in Task 1 (`expiry_follows_the_client_clock`).
2. **Unmigrated data under a name that keeps its key** (`{t}:cache`, which was not re-tagged). A get must fail with `Error::Redis(_)` (the Lua `struct.unpack` error). It must never return a wrong or partial value or panic. Pinned in Task 1 (`an_old_layout_value_is_an_error_not_a_wrong_value`).
3. **A plain write clears the limits, for every write method.** `insert`, `set`, `entry` and `extend` without `.ttl()`/`.max_idle()` on an entry that has a TTL must all leave `entry_ttl == None`, and `set` must report exactly what `insert` reports (`true` ⇔ `None`). This deviates from Redisson's `fastPut`/`compute`/`putAll`. Pinned in Task 3 (`set_and_insert_both_clear_the_limits`, `set_reports_what_insert_reports`, `extend_without_limits_clears_them`) and Task 5 (`entry_without_ttl_clears_the_ttl`).
4. **Event payloads with arbitrary bytes.** This covers a value that contains `:`, NUL or bytes that look like a length, an empty value, and a server that reports `os:Windows`. The parser must split by the length prefix only and return `Error::Codec` on truncation, never panic. Pinned in Task 1 (`parse_entry_event_*` unit tests).
5. **Names with a hash tag, and Redis Cluster.** `{t}:cache` must give `{t}:cache`, `redissun__timeout__set:{t}:cache` and `{t}:cache:redissun_options`. Every script must run on a 3-master cluster without `CROSSSLOT`. That includes the lease key, which the Lua code builds from `KEYS[n] .. field`, and the migration script, which touches old and new keys in one call. Pinned in Task 1 (`names_with_a_hash_tag_follow_redissons_rules`), Task 8 (`every_operation_runs_on_a_cluster`) and Task 9 (`migration_runs_on_a_cluster`).
6. **A lease guard that outlives its lease.** `LeaseGuard::insert` after the lease time must store nothing and return `false`, never overwrite a value someone else stored. Pinned in Task 7 (`an_expired_guard_stores_nothing`).
7. **The migration never loses or corrupts a live entry, survives being split into batches, and is safe to repeat.** A call returns `0` only when nothing old is left. Pinned in Task 9.

---

### Task 1: Redisson's layout, names, clock and event format for the existing methods

**Files:**
- Move: `src/collections/hash_map_cache.rs` → `src/collections/hash_map_cache/mod.rs` (the type, `HasKey`, public methods)
- Create:
  - `src/collections/hash_map_cache/scripts.rs` (every Lua `static`; this task adds `GET`, `CONTAINS_KEY`, `PUT`, `PUT_WITH_LIMITS`, `PUT_IF_ABSENT_WITH_LIMITS`, `REMOVE`, `REMAIN_TTL`, `SCAN_ENTRIES`, `SCAN_KEYS`, `TOUCH_IDLE`, `EVICT`, `EXPIRE`, `CLEAR_EXPIRE`, `TRY_SET_MAX_SIZE`)
  - `src/collections/hash_map_cache/names.rs` (key and channel names)
  - `src/collections/hash_map_cache/entry_events.rs` (payload parser, OS detection, the `Events` type moved here)
- Modify:
  - `src/object.rs`: move `suffix_name` here from `src/coordination/rate_limiter.rs` and add `prefix_name`. Add `pub(crate) fn now_millis() -> i64`, and make `locks/fair.rs::now_millis` call it. Add `HasKey::expire_args(&self) -> Vec<Bytes>` (default empty), which `Key::expire_with` appends after the ttl.
  - `src/collections/map/key_hash.rs`, `src/coordination/rate_limiter.rs`: import `suffix_name` from `object`.
  - `src/eviction.rs`: `schedule(core, task, script, keys, args: fn(Option<Duration>) -> Vec<Bytes>)`. `HashSetCache` passes `eviction::arguments`, so its behaviour does not change.
  - `src/collections/hash_set_cache.rs` (the new `schedule` argument only)
  - `src/client/mod.rs`: `hash_map_cache` uses `Key::new(core, name)` without `tagged`.
- Test:
  - `tests/hash_map_cache.rs` (update `hash_key` to return the raw name and the companion-name literals; replace the `evict_expired` tests, see Step 2)
  - unit tests in `names.rs`, `entry_events.rs`, `object.rs`

**Interfaces:**
- Produces:
  - `pub(crate) fn prefix_name(prefix: &str, name: &str) -> String`: `name.contains('{')` → `"{prefix}:{name}"`, otherwise `"{prefix}:{{{name}}}"`.
  - In `names.rs`: `timeout(name)`, `idle(name)`, `last_access(name)`, `options(name)`, `lease(name)` (with the trailing `:`), `channel(kind: EventKind, name)` (renamed `EntryEventKind` in Task 2), `latch(name)`, all `-> String`. Also the old names for Task 9: `old_main(name)` (= `object::tagged(name)`), `old_last_access(name)`, `old_options(name)`.
  - On `HashMapCache` (private): `fn keys(&self, set: &[Name]) -> Vec<String>`, where `enum Name { Main, Timeout, Idle, Created, Updated, Removed, Expired, LastAccess, Options, Lease }`. Each call site lists the KEYS in its Redisson order, so nobody hand-orders `vec![]`s. Also `fn limits(ttl: Option<Duration>, max_idle: Option<Duration>) -> Result<Limits>` with `struct Limits { now: i64, ttl_timeout: i64, idle_timeout: i64, idle_delta: i64 }` (each `0` when unset or zero, as in Java; private, never public), and `Limits::any(&self) -> bool`.
  - `entry_events.rs`: `pub(crate) enum LengthPrefix { U64Le, U32Le, U64Be }`, `pub(crate) fn length_prefix(info_server: &str) -> LengthPrefix` (no `os` field or `Windows` → `U32Le`, `NONSTOP` → `U64Be`, otherwise `U64Le`, exactly `addListenerAsync`'s rule), `LengthPrefix::value_header(self) -> usize` (`8 + 8`, or `8 + 4` for `U32Le`: the `dLc0` header size; used by `value_len` in Task 4), and `pub(crate) fn split_event(payload: &[u8], prefix: LengthPrefix) -> Option<(&[u8], &[u8], Option<&[u8]>)>` (key, value, old value when bytes remain).
  - `TryInsert` resolves to `Result<Option<V>>`: `None` means it stored the entry, `Some(v)` is the live value it left alone, like `HashMap::try_insert`'s `Err(OccupiedError)` carrying the value.
  - `PUT`/`PUT_WITH_LIMITS`/`FAST_PUT_WITH_LIMITS`/`ADD_AND_GET`/`REMOVE_IF` and the other writers take `publishCommand` = `"PUBLISH"`. The lease token argument is `""` until Task 7.

- [ ] **Step 1: Write failing unit tests.**
  - `object.rs`: `prefix_name("p", "m") == "p:{m}"`, `prefix_name("p", "{t}:m") == "p:{t}:m"`, `suffix_name("m", "redissun_options") == "{m}:redissun_options"`.
  - `names.rs` `names_match_redisson`: for `"m"`:
    - `timeout` = `redissun__timeout__set:{m}`, `idle` = `redissun__idle__set:{m}`;
    - `last_access` = `redissun__map_cache__last_access__set:{m}`, `options` = `{m}:redissun_options`;
    - `lease` = `redissun__map__lease:{m}:`, `latch` = `redissun__execute_task_once_latch:{m}`;
    - `channel(Created)` = `redissun_map_cache_created:{m}`;
    - `old_main` = `{m}`, `old_last_access` = `redissun__map_cache_last_access__set:{m}`, `old_options` = `redissun__map_cache_options:{m}`.
    - Repeat for `"{t}:m"`: `redissun__timeout__set:{t}:m`, `{t}:m:redissun_options`, `old_main` = `{t}:m`.
  - `entry_events.rs`:
    - `parse_entry_event_reads_two_parts`: `[1,0,0,0,0,0,0,0,b'k', 2,0,0,0,0,0,0,0,b'v',b'1']` with `U64Le` gives `(b"k", b"v1", None)`.
    - `parse_entry_event_reads_the_old_value`: a third part gives `Some(b"old")`.
    - `parse_entry_event_keeps_colons_and_nuls`: value `b"a:\0:1"`.
    - `parse_entry_event_allows_an_empty_value`: length 0 gives `b""`.
    - `parse_entry_event_uses_four_bytes_on_windows`: `[1,0,0,0,b'k',1,0,0,0,b'v']` with `U32Le`.
    - `parse_entry_event_rejects_truncation`: a length larger than the rest gives `None`, and an empty payload gives `None`.
    - `length_prefix_follows_the_os`: `"os:Linux 6.1 x86_64"` gives `U64Le`, `"os:Windows"` gives `U32Le`, `""` gives `U32Le`; `value_header` is 16 for `U64Le` and 12 for `U32Le`.
- [ ] **Step 2: Write failing integration tests in `tests/hash_map_cache.rs`.**
  - `stores_values_in_redissons_layout`: `insert("a", &1i64).max_idle(30s)`. Then `raw_command(["EVAL", "local t, v = struct.unpack('dLc0', redis.call('hget', KEYS[1], ARGV[1])) return {tostring(t), v}", "1", &name, "\"a\""])` returns `30000` and `1`. `HSTRLEN name "a"` is `:17` (16-byte header + `1`). `ZSCORE redissun__idle__set:{name} "a"` is within ±2000 of the client's `now + 30000`.
  - `reads_an_entry_written_by_redissons_script`: write with Redisson's exact `fastPut`-with-limits lines through `EVAL`: `hset` of `struct.pack('dLc0', ARGV[3], string.len(ARGV[2]), ARGV[2])`, plus `zadd` on the timeout and idle sets with `now+60000` / `now+400` and idle delta `400`. Then `get` gives the value, `entry_ttl` is ≤ 400 ms, `keys()` contains it, and after a 600 ms sleep with no reads `get` gives `None`.
  - `names_with_a_hash_tag_follow_redissons_rules`: on a cache named `{t}:<unique>`, do `insert(..).ttl(60s)`, `set_max_size(10, Lru)` and `events()`. `EXISTS` is `:1` for `{t}:<u>`, `redissun__timeout__set:{t}:<u>`, `redissun__map_cache__last_access__set:{t}:<u>` and `{t}:<u>:redissun_options`, and `subscribed_channels("redissun_map_cache_created:{t}:<u>")` is 1.
  - `expiry_follows_the_client_clock`: `insert("a","x").ttl(60s)`, then `ZADD redissun__timeout__set:{name} <client_now - 1> "a"`, then `get` is `None`. `ZADD … <client_now + 400>` gives `Some`, and after a 600 ms sleep `None`.
  - `an_old_layout_value_is_an_error_not_a_wrong_value`: `HSET {t}:<u> "\"k\"" "0:\"v\""`. Then `get("k")` is `Err(Error::Redis(_))`.
  - `try_insert_returns_the_live_value`: the first call gives `None`. The second gives `Some(first)`. After the first entry's TTL passes, the call gives `None` again and the new value is stored.
  - `insert_clears_the_limits_of_a_live_entry`: `insert("a",1).ttl(60s).max_idle(60s)`, then `insert("a",2)` gives `Some(1)`, `entry_ttl == None`, and `ZSCORE` on both sets is nil.
  - Update the existing tests:
    - Delete `a_value_without_the_idle_prefix_is_read_as_having_no_idle_time` (layout gone; Task 9 covers reading that form).
    - Change the `try_insert` assertions from `bool` to `Option` (`a_zero_ttl_means_no_time_limit`: `.is_none()`).
    - Replace `cache.evict_expired()` with a client built by `connect_with(|b| b.eviction_interval(Duration::from_millis(100)))` and `eventually("evicted", 5s, …)` on `HLEN`.
    - Delete `evict_expired_deletes_the_entries_from_redis` and fold its assertion into `the_background_task_deletes_expired_entries_nobody_touches`.
    - Rewrite `evict_expired_also_removes_orphans_from_the_sets` the same way, as `the_background_task_also_removes_orphans_from_the_sets`.
- [ ] **Step 3: Run** `cargo test --lib hash_map_cache -- --test-threads 4`, then `cargo test --lib object -- --test-threads 4`. Expected: FAIL (compile errors: `prefix_name`, `names`, `split_event` missing).
- [ ] **Step 4: Implement.**
  - Port the 14 scripts verbatim with their KEYS/ARGV order.
  - `insert` without limits → `PUT`, with limits → `PUT_WITH_LIMITS` ("Write semantics").
  - `iter()`/`keys()` call `SCAN_ENTRIES`/`SCAN_KEYS` with `[now, cursor, count]` (no pattern until Task 4). They decode `{cursor, flat pairs, idleKeys}`. When `idleKeys` is non-empty, they send `TOUCH_IDLE` (KEYS `[name, idle]`, ARGV `[now, k…]`), awaited before the page is yielded. That is my call: Redisson fires it and does not wait.
  - The eviction arguments are `[now, KEYS_LIMIT, max(1, latch.as_secs()), "1", "PUBLISH"]`, with KEYS `[name, timeout, idle, expired channel, last access, latch]`.
  - `HasKey`: `companions` = `[timeout, idle, last_access, options]` and `renamed_key` = the raw new name. `expire_args` = `[""]`.
  - `Events::recv` takes the event kind from the source it read (sources become `Vec<(EventKind, Subscription, Receiver)>`) and parses with `split_event`. `events_of` runs `INFO server` once to pick the `LengthPrefix` (cached on the cache's core for `value_len`), then sets `has-listeners`.
  - Remove `evict_expired` and `BASE`/`PRELUDE`.
  - Update `hash-map-cache.mdx`:
    - "How expiry works": the client clock, as Redisson; a plain write replaces the entry and clears its limits, like `HashMap::insert`.
    - "How it is stored": the new names and the `dLc0` layout (the "after 0b" column of "Old vs new data format").
    - Delete the `evict_expired` row.
    - Change the `try_insert` row to "returns `Some(live value)` and stores nothing, like `HashMap::try_insert`".
  - In the migration guide, remove "redissun takes them from the Redis server", change the `try_insert` row, and add a note on the data and event-format break with a pointer to `migrate_from_0_23()` (Task 9).
- [ ] **Step 5: Run** `cargo test --lib hash_map_cache -- --test-threads 4`, then `cargo test --test hash_map_cache -- --test-threads 4`, then `cargo test --test hash_set_cache -- --test-threads 4`. Expected: `test result: ok.` for each.
- [ ] **Step 6: Commit** `refactor(hash_map_cache)!: Redisson's scripts, value layout, key names, client clock and event format`

### Task 2: Event API names and RMap's keyspace listeners

**Files:**
- Modify:
  - `src/collections/hash_map_cache/entry_events.rs` (renames)
  - `src/collections/map/events.rs` (`subscribe` gets `skip_field: Option<&'static [u8]>`; `HashMap` passes `None`)
  - `src/collections/hash_map_cache/mod.rs`, `src/lib.rs`, `src/collections/hash_map.rs` (doc note on `events()`)
- Test: `tests/hash_map_cache.rs`

**Interfaces:**
- Produces:
  - `EntryEvent<K, V>` (variants unchanged: `Created { key, value }`, `Updated { key, value, previous }`, `Removed { key, value }`, `Expired { key, value }`), `EntryEventKind`, `EntryEvents<K, V, C>`. Renamed from `Event`/`EventKind`/`Events`, with `#[doc(alias = "EntryEvent")]` on the enum and aliases `MapEntryListener`, `EntryCreatedListener`, … on `events`.
  - `pub async fn keyspace_events(&self) -> Result<MapEvents<K, C>>` and `pub async fn keyspace_events_of(&self, kinds: &[MapEventKind]) -> Result<MapEvents<K, C>>`. They call `events::subscribe(&self.key, codec, kinds, Some(b"redissun__temp_val"))`. A subkey event whose field equals `skip_field` is dropped before decoding (`ADD_AND_GET` writes and deletes that field). Aliases `MapPutListener`, `MapIncrListener`, `MapRemoveListener`, `MapDeletedListener`, `MapExpiredListener`.
- Naming decision (owner decision 6): `events()` stays the entry listener. It is the one RMapCache users use, it carries the values and needs no server config. The keyspace listener, which `HashMap::events()` returns, is `keyspace_events()` here. This is the one documented case where a name means something else than on `HashMap`; the rustdoc of `HashMapCache::events`, `HashMapCache::keyspace_events` and `HashMap::events` each says so in one sentence, and `hash-map-cache.mdx` repeats it.

- [ ] **Step 1: Write failing tests.**
  - `keyspace_events_report_put_and_remove`: an own Redis 8.8 container with `notify-keyspace-events ETh` (reuse 0a's `events_client` pattern). `insert("a")` gives `MapEvent::Put { key: Some("a") }` and `remove("a")` gives `Removed { key: Some("a") }`.
  - `keyspace_events_skip_the_temporary_incr_field`: the same server. `incr_by("n", 1)` (Task 3; until then use a raw `EVAL` that `hset`s and `hdel`s `redissun__temp_val`). The next event is `Put { key: Some("n") }`, and no `Err` item arrives within 300 ms.
  - `entry_events_keep_working_after_the_rename`: compile-level, the existing event tests use `EntryEvent`/`EntryEventKind`.
- [ ] **Step 2: Run** `cargo test --test hash_map_cache keyspace_events -- --test-threads 4`. Expected: FAIL (no `keyspace_events`).
- [ ] **Step 3: Implement.** Rename the types and update their imports in the tests. Add a "Keyspace events" section to `hash-map-cache.mdx`, with the `events()` naming note. Add `addListener(MapPutListener …)` → `keyspace_events()` to the migration table.
- [ ] **Step 4: Run** the same command, then `cargo test --test hash_map_cache -- --test-threads 4`, then `cargo test --test it hash_map::events -- --test-threads 4`. Expected: `ok`.
- [ ] **Step 5: Commit** `feat(hash_map_cache)!: EntryEvent names and keyspace_events for RMap's keyspace listeners`

### Task 3: The RMap write family

**Files:**
- Modify:
  - `src/collections/hash_map_cache/scripts.rs` (add `FAST_PUT_WITH_LIMITS`, `REPLACE`, `REPLACE_IF`, `REMOVE_IF`, `FAST_REMOVE`, `ADD_AND_GET`, `PUT_ALL_TTL_IDLE`)
  - `src/collections/hash_map_cache/mod.rs`
  - Create `src/collections/hash_map_cache/builders.rs` (move `Insert`/`TryInsert` here; add `Set`, `Extend`)
  - `src/lib.rs`: `pub mod hash_map_cache { pub use …::{Insert, TryInsert, Set, Extend}; }` and remove `Insert`/`TryInsert` from the root
- Test: `tests/hash_map_cache.rs`

**Interfaces:**
- Produces (all on `HashMapCache<K, V, C>`, bounds as on today's `insert`):
  - `pub fn set<'a, Q, W>(&'a self, k: &'a Q, v: &'a W) -> Set<'a, K, V, C, Q, W>`. `Set` has `.ttl(Duration)` and `.max_idle(Duration)` (`.lease(&str)` comes in Task 7) and resolves to `Result<bool>`: `true` when there was no live entry, exactly when `insert` would return `None`. Without limits it runs `PUT` and maps the reply (`nil` → `true`); with limits `FAST_PUT_WITH_LIMITS` with an empty token. Its Redis effects are those of `insert` (see "Write semantics"). Aliases `fastPut`, `putWithLease`.
  - `pub async fn replace<Q, W>(&self, k: &Q, v: &W) -> Result<Option<V>>`. It keeps the entry's limits (it only touches an existing entry, so nothing is "replaced" in the `insert` sense). Aliases `putIfExists`, `fastPutIfExists`, `fastReplace`.
  - `pub async fn compare_and_set<Q, W>(&self, k: &Q, old: &W, new: &W) -> Result<bool>`, `pub async fn remove_if_eq<Q, W>(&self, k: &Q, v: &W) -> Result<bool>`, and `pub async fn remove_many<Q>(&self, ks: &[&Q]) -> Result<usize>` (empty input gives `Ok(0)` without a call; alias `fastRemove`).
  - `pub async fn incr_by<Q>(&self, k: &Q, delta: i64) -> Result<i64>` and `incr_by_float<Q>(&self, k: &Q, delta: f64) -> Result<f64>`. Both use `ADD_AND_GET` with the delta formatted by `Display` (never exponent notation) and parse the returned string. A non-finite `delta` is `Error::Config`. Alias `addAndGet`.
  - `pub fn extend<'a, Q, W>(&self, entries: impl IntoIterator<Item = (&'a Q, &'a W)>) -> Extend<'_, K, V, C>` with `.batch_size(n)`, `.ttl(d)` and `.max_idle(d)`, resolving to `Result<()>`. Entries are encoded eagerly, like `HashMap::extend`. It always runs `PUT_ALL_TTL_IDLE`, with zeros for unset limits, so each entry ends up as `insert` would leave it (Java's `PUT_ALL`/`PUT_ALL_TTL` keep stale scores; deviation recorded). One `EVAL` per batch, in order, each with a fresh `now`. Empty input sends nothing. Alias `putAll`.

- [ ] **Step 1: Write failing tests.**
  - `set_reports_a_new_key`: the first call gives `true`, the second `false`. After the TTL passes, `true` again.
  - `set_reports_what_insert_reports`: on a missing, a live and an expired-but-not-evicted key, `set(..)` gives `true`/`false`/`true` where `insert(..)` gives `None`/`Some`/`None`, with and without `.ttl(60s)`.
  - `set_and_insert_both_clear_the_limits` (Review Focus 3): `insert("a",1).ttl(60s).max_idle(60s)`, then `set("a",2)`: `entry_ttl == None`, `ZSCORE` on the timeout and idle sets is nil, and `get` is `2`. The same sequence with `insert("a",2)` gives the same Redis state (compare `HGET`, both `ZSCORE`s).
  - `set_with_limits_sends_created_and_updated_events`: `events_of(&[Created, Updated])` receives `Created` then `Updated { previous }`.
  - `replace_keeps_the_idle_time`: `insert(..).max_idle(400ms)`, then `replace` gives `Some(old)`. `get` after 200 ms is `Some`, and after 600 ms with no reads it is `None`. `replace` on a missing key gives `None` and the key stays missing.
  - `compare_and_set_and_remove_if_eq_follow_the_value`: a wrong old value gives `false` and leaves the value. The right one gives `true`. An expired entry gives `false`.
  - `remove_many_counts_and_publishes`: 3 keys, `remove_many(&[a, b, missing]) == 2`, and two `Removed` events.
  - `incr_by_creates_and_adds`: a missing key gives `5`, then `7`. `incr_by_float(&k, 0.5)` gives `7.5`. `get` returns `7.5`.
  - `extend_with_ttl_and_batches`: 250 entries with `.batch_size(100).ttl(300ms)`. `len() == 250`, and after 500 ms every `get` is `None`.
  - `extend_with_max_idle_only`: `.max_idle(300ms)`, then `entry_ttl` is ≤ 300 ms.
  - `extend_without_limits_clears_them` (Review Focus 3): `insert("a",1).ttl(60s).max_idle(60s)`, then `extend([("a",2)])`: `entry_ttl == None` and both `ZSCORE`s are nil.
  - `extend_respects_the_size_limit`: with `set_max_size(10, Lru)`, extending by 25 entries gives `len() == 10`.
- [ ] **Step 2: Run** `cargo test --test hash_map_cache set_ replace_ compare_and_set remove_many incr_by extend_ -- --test-threads 4`. Expected: FAIL (compile).
- [ ] **Step 3: Implement.** Docs: add rows to the method table in `hash-map-cache.mdx` and a short "Writes" paragraph: every plain write (`insert`, `set`, `entry`, `extend`) replaces the entry and clears its limits; `set` differs from `insert` only in returning `bool`; `replace` keeps the limits. Add rows to the migration table for `fastPut` (with the note "clears the TTL, unlike Redisson's `fastPut`"), `replace`, `putIfExists`, `addAndGet` and `putAll(map[, ttl[, idle]])` (note: "clears the TTL without `.ttl()`, unlike Redisson's `putAll(map)`").
- [ ] **Step 4: Run** the same command, then the whole `cargo test --test hash_map_cache -- --test-threads 4`. Expected: `ok`.
- [ ] **Step 5: Commit** `feat(hash_map_cache): set, replace, compare_and_set, remove_if_eq, remove_many, incr_by and extend with limits`

### Task 4: The RMap read family

**Files:**
- Modify:
  - `src/collections/hash_map_cache/scripts.rs` (add `GET_ALL`, `CONTAINS_VALUE`, `RANDOM_KEYS`, `RANDOM_ENTRIES`, `READ_ALL`)
  - `src/collections/hash_map_cache/mod.rs`
  - `src/collections/map/scan.rs`: `pub(crate) fn pattern(&self) -> Option<&str>` and `pub(crate) fn page(&self) -> usize` (0 → 100) on `MapScan`
- Test: `tests/hash_map_cache.rs`

**Interfaces:**
- Produces:
  - `get_many<Q>(&self, ks: &[&Q]) -> Result<Vec<Option<V>>>` (in key order; empty input gives an empty `Vec` without a call; alias `getAll`)
  - `contains_value<W>(&self, v: &W) -> Result<bool>` (alias `containsValue`)
  - `value_len<Q>(&self, k: &Q) -> Result<usize>`: the length of the encoded value, as `HashMap::value_len` returns it. It sends `HSTRLEN` and subtracts the `dLc0` header (`LengthPrefix::value_header()`: 16, or 12 on a Windows server; `INFO server` is read once per client and cached) with `saturating_sub`; a missing key gives `0`. Redisson's `valueSize` returns the raw field length including the header; this Rust-side difference is recorded in the PR and `limitations.mdx`. Like Redisson, it does not check expiry. Alias `valueSize`.
  - `random_keys(&self, count: usize) -> Result<Vec<K>>`
  - `random_entries(&self, count: usize) -> Result<std::collections::HashMap<K, V>>`
  - `to_hash_map(&self) -> Result<std::collections::HashMap<K, V>>` (aliases `readAllMap`, `readAllEntrySet`, `readAllKeySet`, `readAllValues`)
  - `iter_with(&self, scan: MapScan)`, `keys_with(MapScan)`, `values_with(MapScan)`, with `iter/keys/values` = `*_with(MapScan::new())`; the names are 0a's `HashMap` names, kept for "same name". `keys*` use `SCAN_KEYS`, the others `SCAN_ENTRIES`, with the pattern as `ARGV[3]` when set (Redisson's `#ARGV == 4` branch).
  - The map-returning methods need `K: Eq + Hash` (a separate impl block, as on `HashMap`). The replies are decoded with `map::scan::decode_pairs`.

- [ ] **Step 1: Write failing tests.**
  - `get_many_skips_expired_and_keeps_order`: `[a (live), b (ttl passed), missing]` gives `[Some, None, None]`.
  - `get_many_refreshes_the_idle_time`: a 400 ms idle entry read by `get_many` every 200 ms is still `Some` at 800 ms.
  - `contains_value_ignores_expired_entries`.
  - `value_len_counts_the_encoded_value`: `insert("a", &"xy")` (JSON `"xy"`, 4 bytes) gives `value_len == 4`, the same number a `HashMap` gives for the same value, while `HSTRLEN` is `20`. A missing key gives `0`.
  - `random_keys_and_entries_skip_expired`: 5 live and 5 expired entries; `random_keys(10)` and `random_entries(10)` only return live ones (length 5).
  - `to_hash_map_returns_live_entries_only`.
  - `scan_with_pattern_reads_every_page`: the same as 0a's for `HashMap` (1000 keys, pattern `"\"k1*"`, page 10 gives 111).
  - `keys_with_uses_the_pattern_and_skips_expired`.
  - Unit test in `mod.rs`: `value_len_never_underflows` (a raw length below the header gives `0`).
- [ ] **Step 2: Run** `cargo test --test hash_map_cache get_many contains_value value_len random_ to_hash_map scan_with keys_with -- --test-threads 4`. Expected: FAIL.
- [ ] **Step 3: Implement.** Docs: method rows, plus migration rows for `getAll`, `readAllMap`, `keySet(pattern, count)` and `valueSize` (note: "without Redisson's 16-byte header").
- [ ] **Step 4: Run** the same command and the whole file. Expected: `ok`.
- [ ] **Step 5: Commit** `feat(hash_map_cache): get_many, contains_value, value_len, random keys and entries, to_hash_map and pattern scans`

### Task 5: Shared per-key objects and `entry()` with TTL

**Files:**
- Modify:
  - `src/collections/map/key_hash.rs`: `macro_rules! per_key_objects` with the six methods and their docs. It expects fields `key: Key` and `codec: C` and generates `fn per_key` plus `lock`, `fair_lock`, `rw_lock`, `semaphore`, `count_down_latch` and `permit_expirable_semaphore`.
  - `src/collections/hash_map.rs`: replace the six hand-written methods with `per_key_objects!();`.
  - `src/collections/map/entry.rs`: generic over the target.
  - `src/collections/hash_map_cache/mod.rs`, `builders.rs` (`EntryRequest`), `src/lib.rs` (export `EntryRequest` in `hash_map_cache`)
- Test: `tests/hash_map_cache.rs`; the existing `tests/it/hash_map.rs` entry tests must stay green unchanged

**Interfaces:**
- Consumes: `KeyLock::acquire(&Key, &[u8])` (`map/ops.rs`, unchanged; the cache's `Key` name is now the raw name, so the lock is `{m}:<hash>:lock`, Redisson's `getLock(key)` for `RMapCache`).
- Produces:
  - In `entry.rs`:
    - `pub(crate) enum Target<'m, K, V, C: Codec> { Map(&'m HashMap<K, V, C>), Cache(&'m HashMapCache<K, V, C>, Option<Duration>) }`, with `fn key(&self) -> &Key`, `async fn read(&self, k: &Bytes) -> Result<Option<V>>`, `async fn write(&self, k: &Bytes, v: &V) -> Result<()>` and `async fn delete(&self, k: &Bytes) -> Result<()>`.
    - `Held { target: Target<'m, K, V, C>, key, lock }`.
    - `Entry::acquire(target: Target<'m, K, V, C>, key: Bytes)`.
    - Public `Entry<'m, K, V, C>`, `OccupiedEntry` and `VacantEntry` keep their exact signatures. `Target` lives only in a private field, so no private type reaches a public bound. A `pub(crate) trait` type parameter would either trip `private_bounds` or change 0a's public `Entry` type.
  - On `HashMapCache` (`pub(crate)`):
    - `read_encoded` (the `GET` script);
    - `write_encoded(k, v, ttl)`: `PUT` when `ttl` is `None`/zero, the same write as `insert` (owner decision 3); else `FAST_PUT_WITH_LIMITS` with the TTL only (Redisson's `compute(k, ttl, f)` → `fastPut(k, v, ttl)`). Redisson's `compute(k, f)` uses `fastPut(k, v)`, which keeps the TTL; that difference is recorded in the PR;
    - `delete_encoded` (`FAST_REMOVE` with one key).
  - `pub fn entry<Q>(&self, k: &Q) -> EntryRequest<'_, K, V, C>`, with `EntryRequest::ttl(self, Duration) -> Self` and `impl IntoFuture<Output = Result<Entry<'a, K, V, C>>>`. The key is encoded at once, and an error comes back on `.await`. A write through a plain `entry(&k)` replaces the entry like `insert` (clears the TTL and max idle); `entry(&k).ttl(d)` writes with that TTL. Aliases `compute`, `computeIfAbsent`, `computeIfPresent`, `merge`. Redisson's compute takes no max idle, so there is no `.max_idle()` (parity: don't add what Redisson lacks).
  - The six per-key methods on `HashMapCache`, with the same signatures as on `HashMap`.

- [ ] **Step 1: Write failing tests.**
  - `per_key_lock_lives_under_the_redisson_name`: the `HashMap` test from 0a, run on a cache. `EXISTS {<name>}:IKm9EBQhlzpVIBxUXDHBWw:lock` is `:1` while it is held.
  - `entry_with_ttl_inserts_an_expiring_value`: `entry("a").ttl(300ms).await?.or_insert(1)` gives `1`. `entry_ttl` is ≤ 300 ms, and after 500 ms `get` is `None`.
  - `entry_without_ttl_clears_the_ttl` (Review Focus 3): `insert("a",1).ttl(60s)`, then `entry("a").await?.and_modify(|v| *v += 1).or_insert(0)` gives `2`, and `entry_ttl == None`, as after `insert("a", 2)`.
  - `entry_with_ttl_replaces_the_ttl`: `insert("a",1).ttl(60s)`, then `entry("a").ttl(300ms)` with `and_modify(+1)`: `entry_ttl` ≤ 300 ms.
  - `entry_sees_an_expired_entry_as_vacant`: after the TTL passes, `entry` gives `Entry::Vacant`.
  - `entry_remove_publishes_removed`: `Occupied.remove()` sends a `Removed` event.
  - `entry_is_atomic_across_tasks`: 20 tasks run `and_modify(+1).or_insert(1)`, and the final value is `20`.
  - `a_dropped_cache_entry_releases_the_lock`: the same as 0a's.
- [ ] **Step 2: Run** `cargo test --test hash_map_cache entry per_key -- --test-threads 4`. Expected: FAIL.
- [ ] **Step 3: Implement.** Docs: an "Entries" section in `hash-map-cache.mdx`: `entry(&k).ttl(d)` writes with that TTL; a plain `entry(&k)` writes like `insert` and clears the entry's TTL and max idle (unlike Redisson's `compute`, which keeps it). Migration rows for `compute(k, f)` → `entry(&k)` (with that note), `compute(k, ttl, f)` / `computeIfAbsent(k, ttl, f)` → `entry(&k).ttl(d)` and `getLock(k)` → `lock(&k)`.
- [ ] **Step 4: Run** the same command, then `cargo test --test it hash_map:: -- --test-threads 4` (the 0a entry and per-key tests, unchanged). Expected: `ok`.
- [ ] **Step 5: Commit** `feat(hash_map_cache): per-key objects and entry() with ttl, sharing HashMap's Entry`

### Task 6: Entry expiration management and `peek`

**Files:**
- Modify: `scripts.rs` (add `EXPIRE_ENTRY`, `EXPIRE_ENTRY_IF_NOT_SET`, `EXPIRE_ENTRIES`, `EXPIRE_ENTRIES_IF_NOT_SET`, `GET_WITH_TTL_ONLY`, `GET_ALL_WITH_TTL_ONLY`), `builders.rs` (`ExpireEntry`, `ExpireEntries`), `mod.rs`, `src/lib.rs`
- Test: `tests/hash_map_cache.rs`

**Interfaces:**
- Produces:
  - `pub fn expire_entry<'a, Q>(&'a self, k: &'a Q) -> ExpireEntry<'a, K, V, C, Q>`. `ExpireEntry` has `.ttl(Duration)`, `.max_idle(Duration)` and `.if_not_set()`, and resolves to `Result<bool>` (whether a live entry was changed). Without `.ttl`/`.max_idle`, both limits are cleared (Redisson's `expireEntry(k, ZERO, ZERO)`). Aliases `expireEntry`, `expireEntryIfNotSet`, `updateEntryExpiration`.
  - `pub fn expire_entries<'a, Q>(&'a self, ks: &'a [&'a Q]) -> ExpireEntries<'a, K, V, C, Q>` with the same three options, resolving to `Result<usize>`. Empty `ks` gives `Ok(0)` without a call. Aliases `expireEntries`, `expireEntriesIfNotSet`.
  - `pub async fn peek<Q>(&self, k: &Q) -> Result<Option<V>>`: like `lru::LruCache::peek`, it reads without counting as a use. It honours the TTL, ignores max idle, does not refresh the idle deadline and does not touch the LRU/LFU score. Alias `getWithTTLOnly`.
  - `pub async fn peek_many<Q>(&self, ks: &[&Q]) -> Result<Vec<Option<V>>>` (in key order; empty input gives an empty `Vec` without a call; alias `getAllWithTTLOnly`).
- Why a builder instead of Redisson's four methods: each call has two optional durations and one condition. A builder names them (`.ttl().max_idle().if_not_set()`), matches `insert(..).ttl().max_idle()`, and follows ruling 7 (conditions as `.if_*()` on the builder). Four methods with two positional `Duration`s, where `ZERO` means "none", would be easy to misuse.

- [ ] **Step 1: Write failing tests.**
  - `expire_entry_sets_and_replaces_limits`: a plain insert, then `expire_entry("a").ttl(300ms)` gives `true` and `entry_ttl` ≤ 300 ms. After 500 ms, `get` is `None`.
  - `expire_entry_without_limits_clears_them`: `insert(..).ttl(60s)`, then `expire_entry("a").await` gives `true` and `entry_ttl == None`.
  - `expire_entry_on_missing_or_expired_is_false`.
  - `expire_entry_if_not_set_only_touches_unlimited_entries`: on an entry with a TTL it gives `false` (TTL unchanged). On a plain entry it gives `true`.
  - `expire_entry_max_idle_restarts_on_read`.
  - `expire_entries_counts_live_keys`: `[a, b, missing]` gives `2`, and `.if_not_set()` on `[a (has ttl), c (plain)]` gives `1`.
  - `peek_ignores_max_idle_and_does_not_refresh_it`: `insert(..).max_idle(200ms)`, `peek` every 100 ms for 400 ms gives `Some` each time, and then `get` gives `None` (the peeks did not refresh the idle deadline). An entry whose TTL has passed gives `None` from `peek`.
  - `peek_does_not_count_as_a_use`: `set_max_size(2, Lru)`, insert `a`, `b`, `peek(a)`, insert `c`: `a` is the one evicted (with `get(a)` instead of `peek`, `b` would be).
  - `peek_many_keeps_order`.
- [ ] **Step 2: Run** `cargo test --test hash_map_cache expire_entr peek -- --test-threads 4`. Expected: FAIL.
- [ ] **Step 3: Implement.** Docs: method rows, plus migration rows for `expireEntry`, `expireEntries`, `*IfNotSet`, `updateEntryExpiration`, `getWithTTLOnly` → `peek` and `getAllWithTTLOnly` → `peek_many`.
- [ ] **Step 4: Run** the same command and the whole file. Expected: `ok`.
- [ ] **Step 5: Commit** `feat(hash_map_cache): expire_entry and expire_entries builders, peek and peek_many`

### Task 7: Leases (`RLeasedMap`)

**Files:**
- Modify: `scripts.rs` (add `LEASE_GET`, `LEASE_REMOVE`), create `src/collections/hash_map_cache/lease.rs` (`Lease`, `LeaseGuard`, `LeaseInsert`), `builders.rs` (`Set::lease`), `mod.rs`, `src/lib.rs` (export `Lease`, `LeaseGuard` at the root, `LeaseInsert` in `hash_map_cache`)
- Test: `tests/hash_map_cache.rs`

**Interfaces:**
- Produces:
  - ```rust
    #[non_exhaustive]
    pub enum Lease<'m, K, V, C: Codec> {
        /// The key had a live value (`status == 1`).
        Hit(V),
        /// This call took the lease (`leaseAcquired == 1`). Load the value and call `insert`.
        Acquired(LeaseGuard<'m, K, V, C>),
        /// Someone else holds the lease. Retry later or read through.
        Held,
    }
    ```
    `Held` does not carry the other holder's token, which Redisson returns: with it, any caller could finish someone else's lease.
  - `pub async fn lease<Q>(&self, k: &Q, time: Duration) -> Result<Lease<'_, K, V, C>>`. KEYS `[name, timeout, idle, last_access, options, lease]`, ARGV `[now, k, token, millis(time)]`. The token is a fresh random id (the same generator `Lock` uses for its owner id). A zero `time` is `Error::Config` (Redisson: "should be positive"). A reply with no value and no token is `Error::Redis("malformed lease reply")`. Alias `getWithLease`.
  - `pub struct LeaseGuard<'m, K, V, C: Codec>`: private fields `cache: &'m HashMapCache<K, V, C>`, `key: Bytes` (encoded), `token: String`. `#[must_use]`. Methods:
    - `pub fn insert<W>(self, value: &'m W) -> LeaseInsert<'m, K, V, C, W>` (bounds as on `HashMapCache::insert`; it takes `&W` like every other write in the crate). `LeaseInsert` has `.ttl(Duration)` and `.max_idle(Duration)` and resolves to `Result<bool>`. It runs `FAST_PUT_WITH_LIMITS` with this token in `ARGV[8]` (zeros when there are no limits), Redisson's `putWithLease`. `true` = stored and the lease is freed. `false` = the lease had expired or was taken by someone else, nothing was stored; also, verbatim from Redisson, `false` when the token matched but a live entry was already there (the value was stored over it; Redisson's script returns `0` for an update). Documented as Redisson's behaviour (owner decision 10); no extra `GET`. Alias `putWithLease`.
    - `pub async fn remove(self) -> Result<bool>`: `LEASE_REMOVE` (Redisson's `removeWithLease`): deletes the entry and the lease key, `true` if an entry existed. KEYS `[name, timeout, idle, removed channel, last_access, options, lease]`, ARGV `["PUBLISH", k]`. Alias `removeWithLease`.
    - `pub fn token(&self) -> &str`: for handing the lease to another process, which finishes it with `set(&k, &v).lease(token)`.
    - Dropping the guard does nothing: the lease key expires after `time` by itself (no `Drop` impl, no background call). Documented on the type and in the "Leases" section.
  - `pub async fn invalidate<Q>(&self, k: &Q) -> Result<bool>`: the same `LEASE_REMOVE` for a caller without a lease (Redisson's `removeWithLease` is not token-bound; it is the "the source changed, drop the cached value and any pending load" call). Alias `removeWithLease`.
  - `Set::lease(self, token: &'a str) -> Self`: the cross-process escape hatch. It switches to `FAST_PUT_WITH_LIMITS` with the token and has the same `bool` meaning as `LeaseGuard::insert`. Alias `putWithLease`. It is the only public API that takes a token.

- [ ] **Step 1: Write failing tests.**
  - `the_first_miss_takes_the_lease`: `lease("a", 1s)` gives `Acquired(g)`. A second call gives `Held`.
  - `a_hit_needs_no_lease`: after `insert`, the call gives `Hit(v)`.
  - `inserting_through_the_guard_stores_and_frees_the_lease`: `g.insert(&v).await` gives `true`. `get` gives `v`, and `EXISTS redissun__map__lease:{name}:"a"` is `:0`.
  - `the_guard_insert_takes_limits`: `g.insert(&v).ttl(300ms)` gives `true`, and after 500 ms `get` is `None`.
  - `an_expired_guard_stores_nothing` (Review Focus 6): a 200 ms lease; after 400 ms another caller's `lease` gives `Acquired(g2)`; `g.insert(&v1)` gives `false`, `g2.insert(&v2)` gives `true`, and `get` gives `v2`.
  - `a_dropped_guard_leaves_the_lease_until_it_expires`: drop `g`; `lease` gives `Held` until the lease time passes, then `Acquired`.
  - `another_process_finishes_the_lease_with_the_token`: a second client runs `set("a", &v).lease(g.token())` → `true`; `set(..).lease("wrong")` → `false` and `get` stays `None`.
  - `guard_remove_deletes_entry_and_lease` and `invalidate_deletes_entry_and_lease`: each gives `true` on an existing entry, and both keys are gone; after `invalidate`, an outstanding guard's `insert` gives `false`.
  - `a_plain_insert_frees_the_lease`: `insert` (no limits, `PUT`) deletes the lease key (`PUT` deletes `KEYS[9] .. field`), and so does `set` (same script); the guard's `insert` then gives `false`.
  - `a_zero_lease_is_a_config_error`.
- [ ] **Step 2: Run** `cargo test --test hash_map_cache lease guard invalidate -- --test-threads 4`. Expected: FAIL.
- [ ] **Step 3: Implement.** Docs: a "Leases" section in `hash-map-cache.mdx` (the `match cache.lease(&k, d).await? { Hit(v) => …, Acquired(g) => g.insert(&load()).await?, Held => … }` pattern, what `false` means, that dropping the guard does nothing, `token()` + `set(..).lease(token)` for other processes, `invalidate`), plus migration rows for `getWithLease` → `lease`, `putWithLease` → `LeaseGuard::insert` / `set(..).lease(token)` and `removeWithLease` → `LeaseGuard::remove` / `invalidate`.
- [ ] **Step 4: Run** the same command and the whole file. Expected: `ok`.
- [ ] **Step 5: Commit** `feat(hash_map_cache): leases as a guard (Redisson's getWithLease, putWithLease and removeWithLease)`

### Task 8: Ported Redisson tests, Cluster, limitations, final check

**Files:**
- Test: `tests/hash_map_cache.rs`
- Modify:
  - `website/content/docs/guides/limitations.mdx`
  - `README.md` (the HashMapCache row)
  - `src/collections/hash_map_cache/mod.rs` (any missing doc aliases)

- [ ] **Step 1: Fetch the Redisson tests for this tag.** Get `RedissonMapCacheTest.java` from `https://raw.githubusercontent.com/redisson/redisson/redisson-4.8.0/redisson/src/test/java/org/redisson/RedissonMapCacheTest.java` into the scratchpad. `BaseMapTest.java` is already in `scratchpad/rtests/`; `RedissonMapCacheTest` runs it against a map cache. Port every test that covers Tasks 1–7 and is not already in the file, named `redisson_<snake_case>`. Run the `BaseMapTest` cases on a `HashMapCache` too, with the names 0a used plus `_on_cache`. Skip the loader/writer tests (0c) and list them in the commit message. A ported test that relies on `fastPut`/`compute`/`putAll` keeping a TTL is adapted to the recorded deviation (owner decision 3) and marked with a `// redissun: plain writes clear the TTL` comment.
- [ ] **Step 2: Write** `every_operation_runs_on_a_cluster` (Review Focus 5). Follow `an_entry_expires_and_is_reported_on_a_cluster`. On a 3-master cluster, call each method family once: `insert`, `set`, `try_insert`, `replace`, `extend(..).ttl`, `incr_by`, `expire_entry`, `get_many`, `peek`, `iter_with`, `entry`, `lease` / `LeaseGuard::insert` / `set(..).lease` / `invalidate`, `events` with one `Created`, `del`. Each returns `Ok`. Use names with and without a hash tag.
- [ ] **Step 3: Run** `cargo test --test hash_map_cache -- --test-threads 4`. Expected: `ok`. Fix the implementation, not the ported assertion, unless the behaviour is one of the recorded deviations (the write-script choice, the `try_insert` script choice, the awaited `TOUCH_IDLE`, `value_len` without the header).
- [ ] **Step 4: Update `limitations.mdx`.** Replace the HashMapCache line with these points:
  - The clean-up runs on the runtime that first created the cache.
  - Expiry uses each client's clock, as in Redisson, so keep the clocks in sync.
  - Events are always sent with `PUBLISH` (no sharded pub/sub).
  - `set`, `entry` and `extend` without `.ttl()` clear an entry's TTL, like `insert`; Redisson's `fastPut`, `compute` and `putAll` keep it.
  - `incr_by` on an expired entry that has not been evicted yet keeps the old TTL score and so stays invisible, as in Redisson.
  - `value_len` excludes Redisson's 16-byte value header (Redisson's `valueSize` includes it).
  - A write with limits into a full cache that overwrites a live key also evicts one other entry (Redisson's `>=` check).
  - `peek` and the scans read from the master (Redisson may read a replica).
  - The ones not ported: `mapReduce`, `TrackingListener`, `destroy`, `sizeInMemory`, `expireAt`, conditional `expire`.
- [ ] **Step 5: Run the full verification, one command at a time:**
  - `cargo fmt --check`;
  - `cargo clippy --all-targets -- -D warnings`;
  - `cargo test -- --test-threads 4`;
  - `cargo doc --no-deps` (no broken intra-doc links to the renamed `Event*`);
  - `cargo bench --bench compare`, HashMapCache rows compared with `main`. `get` and `insert` must not be slower than noise: one `EVAL` either way, and the `TIME` call is gone. `set` without limits now returns the old value's bytes; note its number in the PR.

  Expected: all green.
- [ ] **Step 6: Commit** `test(hash_map_cache): port Redisson RMapCache tests; docs for RMapCache parity`

### Task 9: `migrate_from_0_23()`, the old-data helper, and the "Upgrading redissun" guide

**Files:**
- Modify: `src/collections/hash_map_cache/scripts.rs` (add `MIGRATE_FROM_0_23`, redissun's own script, with a `/// redissun only: moves 0.23 data into Redisson's layout` comment), `builders.rs` (`Migrate`), `mod.rs`, `names.rs` (old names from Task 1), `src/lib.rs` (export `Migrate` in `hash_map_cache`)
- Create: `website/content/docs/guides/upgrading.mdx` ("Upgrading redissun")
- Modify docs: `website/content/docs/guides/meta.json` (add `"upgrading"` to `pages`, after `"redisson-migration"`), `hash-map-cache.mdx` (a short "Upgrading from 0.23" note linking to the page), `guides/redisson-migration.mdx` (pointer)
- Test: `tests/hash_map_cache.rs`

**Interfaces:**
- Produces: `pub fn migrate_from_0_23(&self) -> Migrate<'_, K, V, C>`, with `Migrate::batch_size(self, n: usize) -> Self` (default 1000; `0` is `Error::Config` on `.await`) and `impl IntoFuture<Output = Result<usize>>`. No doc alias (Redisson has no such method). One `.await` is one `EVAL`, atomic for up to `n` old entries. It returns how many old entries it handled: moved into the new layout, or dropped because they had expired (counted too, so that a batch of only expired entries does not look like "done"). `0` means nothing old is left. Usage:
  ```rust
  while cache.migrate_from_0_23().batch_size(1000).await? > 0 {}
  ```
- The script:
  - KEYS `[old main, new main, timeout, idle, old last access, new last access, old options, new options]` (all in one slot, see "Old vs new data format"; for a name with a tag, old main == new main). ARGV `[now, batch_size]` (client ms).
  - Progress: the `HSCAN` cursor lives in the field `migrate-cursor` of the **old** options hash (`redissun__map_cache_options:<tag>`, a key only 0.23 uses, so no new key name is added; it is created for that field if the cache had no options). The script continues `HSCAN old_main cursor COUNT batch_size` until it has handled `batch_size` old entries or the cursor is back at `0`, and stores the cursor.
  - An old value is one that does not unpack as `dLc0` with `8 + L + len == #raw` (checked with `pcall(struct.unpack, …)`), so values already in the new layout (tagged name, in-place case) are skipped and not counted. Parse an old value with 0.23's `unwrap` exactly (`<idleMs>:<v>`, or `<v>` with idle 0), so the helper reads what 0.23 read.
  - Live check with 0.23's rule (`min(timeout score, idle score if idle ≠ 0) > now`; a missing score means no limit). A live entry is written to the new main hash as `struct.pack('dLc0', idleMs, #v, v)`, and `HDEL`ed from the old main when old main ≠ new main; its timeout and idle scores stay as they are (same keys, absolute ms deadlines). An expired entry is dropped: `HDEL` from the old main and `ZREM` from the timeout, idle and old last-access sets.
  - When the cursor is back at `0` (the last batch), the script finishes the cache in the same call: the old last-access set is moved to the new name (`RENAME` when the new one does not exist; otherwise `ZUNIONSTORE` into it with `AGGREGATE MAX`, then `DEL`), keeping LRU timestamps and LFU counts; the old options fields `max-size`, `mode`, `has-listeners` are copied into the new options hash with `HSETNX` (a value set by a new client wins); when old main ≠ new main, a key-level `PTTL` on the old main (from `Object::expire`) is copied to the new main with `PEXPIRE` and the old main is deleted; the old options hash (with the cursor) is deleted.
  - The `redissun__expiretag` members are left in place (same meaning in both layouts).
- Rules, in the rustdoc and on the upgrading page: stop the 0.23 clients, then run the loop once per cache, from one client, until it returns `0`, before any 0.24 client uses that cache. Each call is atomic, the whole migration is not: until the loop has returned `0`, part of the data is in each layout, so old and new clients must not mix and new clients should not write to that cache (a 0.23 client cannot read the new layout, and the event formats differ). A smaller `batch_size` blocks Redis for less time per call. Running the loop again later is harmless: on an untagged name it returns `0` at once; on a tagged name it scans the hash once more in batches and moves nothing. It does nothing for data that Redisson's Java client wrote (that is already in the new layout).

- [ ] **Step 1: Write failing tests.** A fixture helper `write_0_23_layout(name, extra: usize)` writes with raw commands: `HSET {name} "\"a\"" "0:\"x\"" "\"b\"" "300000:\"y\"" "\"c\"" "\"z\"" "\"gone\"" "0:\"old\""` plus `extra` plain entries `"\"k<i>\"" "0:<i>"`, `ZADD redissun__timeout__set:{name} <now+60000> "\"a\"" <now-1> "\"gone\""`, `ZADD redissun__idle__set:{name} <now+300000> "\"b\""`, `ZADD redissun__map_cache_last_access__set:{name} <now> "\"a\"" …`, `HSET redissun__map_cache_options:{name} max-size 5000 mode LRU`. A helper `migrate_all(cache, n) -> (calls, total)` loops until `0`.
  - `migrate_moves_live_entries_into_redissons_layout`: fixture with `extra = 0`; one call with the default batch gives `4` (3 moved + 1 expired dropped), the next `0`. `get` gives `x`, `y`, `z`; `get("gone")` gives `None`; `entry_ttl("a")` is about 60 s; `b`'s idle header unpacks to `300000`; `HSTRLEN` follows the new layout; `EXISTS {name}` and both old companion keys are `:0`.
  - `migrate_works_in_batches` (Review Focus 7): fixture with `extra = 2500`, `migrate_all(cache, 1000)` takes at least 3 non-zero calls and `total == 2504`, each call returns at most 1000; afterwards `len() == 2503`, every `k<i>` reads back `i`, and the old keys are gone. Run for an untagged name and for `{t}:<u>` (in-place case).
  - `migrate_keeps_the_size_limit`: after the loop, `HGET {name}:redissun_options max-size` is `5000` and mode `LRU`; with `set_max_size(10, Lru)` on a small fixture, inserting 15 entries leaves `len() == 10`.
  - `a_second_full_run_moves_nothing`: after the loop has returned `0`, a second `migrate_all(cache, 1000)` gives `total == 0` and changes nothing (compare `DUMP` of every key). Untagged and tagged names.
  - `a_zero_batch_size_is_a_config_error`.
  - `migrate_on_an_empty_or_new_cache_gives_zero`.
  - `migration_runs_on_a_cluster` (Review Focus 5): the fixture (`extra = 50`, batch 20) and the loop on a 3-master cluster, tagged and untagged names, no `CROSSSLOT`.
- [ ] **Step 2: Run** `cargo test --test hash_map_cache migrat -- --test-threads 4`. Expected: FAIL.
- [ ] **Step 3: Implement**, then write the docs:
  - `guides/upgrading.mdx`, titled "Upgrading redissun", organised by version, newest first. The intro says that every release that changes stored data gets a section here, with what changed per object and the helper or step to run. First section, `## 0.23 → 0.24`:
    - "Stored data", per object: `HashMapCache` (the "Old vs new data format" table of this plan, shortened to the keys and value bytes that change); every other object: unchanged.
    - "What to run": stop 0.23 clients; for each cache run `while cache.migrate_from_0_23().batch_size(1000).await? > 0 {}` until it returns `0`, before any 0.24 client uses that cache; then start 0.24 clients. Say that each call is atomic but the whole migration is not, that old and new clients must not mix until the loop has returned `0`, and how `batch_size` trades call length against number of calls. Event listeners of 0.23 and 0.24 do not understand each other's payloads.
    - "API changes": one line pointing to the changelog and to the `## HashMapCache` table in `redisson-migration.mdx` for the renames (`try_insert`, `EntryEvent*`, `peek`, `lease`, `keyspace_events`, removed `evict_expired`).
  - `guides/meta.json`: add `"upgrading"` after `"redisson-migration"`.
  - `hash-map-cache.mdx`: a two-line "Upgrading from 0.23" note linking to the page; `redisson-migration.mdx`: the same pointer.
- [ ] **Step 4: Run** the same command, then the whole file. Expected: `ok`.
- [ ] **Step 5: Commit** `feat(hash_map_cache): migrate_from_0_23 in batches and the Upgrading redissun guide`

### Task 10: PR and changelog text

- [ ] **Step 1: Open the PR** `feat!: HashMapCache reaches RMapCache parity (0b)` with this body. During the PR, verify on the GitHub CI logs, not by running locally.

```markdown
## Summary
HashMapCache now runs Redisson's RMapCache scripts verbatim (client clock, `struct.pack('dLc0')` values, Redisson's key, channel and event formats) and has every RMapCache, RMap and RLeasedMap method under HashMap's Rust names, with HashMap's behaviour for the same name.

## Changelog
### Added
- `HashMapCache`: `set` (`.ttl`, `.max_idle`, `.lease`), `replace`, `compare_and_set`, `remove_if_eq`, `remove_many`, `incr_by`, `incr_by_float`, `extend(..)` with `.batch_size`, `.ttl` and `.max_idle`.
- `HashMapCache`: `get_many`, `contains_value`, `value_len`, `random_keys`, `random_entries`, `to_hash_map`, `iter_with`/`keys_with`/`values_with(MapScan)`.
- `HashMapCache::entry(&k)` with `.ttl(d)`, the same `Entry` as `HashMap` (Redisson's `compute`/`computeIfAbsent` with a TTL), and the per-key `lock`, `fair_lock`, `rw_lock`, `semaphore`, `count_down_latch`, `permit_expirable_semaphore`.
- `HashMapCache::expire_entry(&k)` / `expire_entries(&ks)` with `.ttl`, `.max_idle` and `.if_not_set()`; `peek` and `peek_many` (Redisson's `getWithTTLOnly`, `getAllWithTTLOnly`).
- Leases (Redisson 4.8's `RLeasedMap`): `lease(&k, time)` → `Lease::{Hit, Acquired(LeaseGuard), Held}`; `LeaseGuard::insert(&v)` (with `.ttl`/`.max_idle`), `LeaseGuard::remove()`, `LeaseGuard::token()`; `set(..).lease(token)` for another process; `invalidate(&k)`.
- `HashMapCache::keyspace_events()` / `keyspace_events_of(..)`: RMap's keyspace listeners.
- `HashMapCache::migrate_from_0_23()` with `.batch_size(n)`: moves a cache written by redissun 0.23 or earlier into the new layout, up to `n` entries per atomic call; loop until it returns `0`.
- Docs: a new "Upgrading redissun" guide (`guides/upgrading`), organised by version, starting with 0.23 → 0.24; later data-format changes get a section there.

### Changed (breaking)
- **Data layout.** Values are stored as Redisson stores them (`struct.pack('dLc0', maxIdle, len, value)`), and the hash lives at `name` instead of `{name}`. The options hash moved to `{name}:redissun_options` and the last-access set to `redissun__map_cache__last_access__set:{name}`. Caches written by earlier versions are not readable until moved: stop the old clients, then run `while cache.migrate_from_0_23().batch_size(1000).await? > 0 {}` for each cache before any new client uses it. Each call is atomic, the whole migration is not, so old and new clients must not mix until the loop has returned `0`. The steps are on the new docs page "Upgrading redissun" (`guides/upgrading`, section 0.23 → 0.24). Without it, old entries are invisible, and under names that already contain `{` a read fails with a script error.
- **Event format.** Events on `redissun_map_cache_*` channels use Redisson's binary payload. Do not run old and new redissun versions against the same cache.
- **Clock.** Expiry deadlines come from the client clock, as in Redisson, not from the Redis server's `TIME`.
- `try_insert` returns `Option<V>` (the live value it kept) instead of `bool`, like `HashMap::try_insert`.
- `Event`, `EventKind`, `Events` are renamed `EntryEvent`, `EntryEventKind`, `EntryEvents`. `Insert` and `TryInsert` moved to `redissun::hash_map_cache`.
- `evict_expired()` is removed; Redisson has no such method. The background task does the clean-up.
- `Object::name()` of a `HashMapCache` returns the name as given, without the added `{}`.

### Behaviour that differs from Redisson on purpose
- A plain write clears the entry's TTL and max idle for every write method (`insert`, `set`, `entry`, `extend`), like `HashMap::insert`. Redisson's `fastPut(k,v)`, `compute(k,f)` and `putAll(map)` keep it. Done by picking Redisson's scripts on the Rust side: `set` uses `put`'s script (`PUT`) and `fastPut`-with-limits, plain `entry` uses `PUT`, `extend` always uses `putAll`-with-ttl-and-idle; Redisson's `fastPut(k,v)`, `putAll(map)` and `putAll(map, ttl)` scripts are not used.
- `value_len` is the encoded value length; Redisson's `valueSize` adds its 16-byte header.
- `Lease::Held` does not expose the other holder's token.

### Redis-side deviations from Redisson (recorded per CLAUDE.md)
- `try_insert` without limits calls Redisson's `putIfAbsent`-with-limits script with zero limits, because the no-limit `putIfAbsent` script leaves a stale timeout score on expired entries.
- Scans await the idle-time refresh (`TOUCH_IDLE`) before yielding a page; Redisson fires it without waiting.
- The temporary field of `addAndGet` is `redissun__temp_val`.
- `publishCommand` is always `PUBLISH`.
- `migrate_from_0_23` is a redissun-only script; it only reads and writes redissun's own keys.
```

- [ ] **Step 2:** Check that the changelog's data-layout entry and the "Upgrading redissun" page say the same steps. No commit: the commits come from Tasks 1–9, and this task only writes the PR body.
