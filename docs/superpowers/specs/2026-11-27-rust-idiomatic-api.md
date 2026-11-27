# Rust-idiomatic API across the crate

**Rule (CLAUDE.md):** no 1:1 Java interfaces; Rust-idiomatic methods close to std types. Lua scripts, Redis commands, key names and data layouts stay exactly as in Redisson. Every renamed method keeps the Redisson name as `#[doc(alias = "...")]`.

HashMap is done (commit 8b74dd8): `entry()`, `set`, `try_insert`, `replace`, `compare_and_set`, `remove_if_eq`, `remove_many`, `value_len`, `to_hash_map`, streams instead of `read_all_*`.

## Rulings on the open questions

1. Caches' `try_insert` returns `bool` (their Lua reports only success); documented. No Lua change.
2. `init_*` for every Redisson `trySetX`/`tryInit` (BloomFilter, IdGenerator, Semaphore, PermitExpirableSemaphore, CountDownLatch, RateLimiter, HashMapCache max size).
3. `retain(&[&Q])` on sets and `retain_values`/`remove_values` on lists stay.
4. Semaphore gets `acquire()` + `acquire_many(n)`, `try_acquire()` + `try_acquire_many(n)`, like tokio and PermitExpirableSemaphore. RateLimiter keeps `acquire(n)`.
5. AtomicI64/F64 keep `get`/`set`; only `swap`, `fetch_add`, `add`, `take` replace the Java pairs.
6. Bucket uses builders; `BatchBucket::set_ex` becomes `set_with_ttl` (batch views cannot be builders).
7. SortedSet single-value conditions stay named methods; `extend` gets the `.if_*()` builder.
8. Geo: `hash` → `geohash`, `pos`/`dist` → `position`/`positions`/`distance`, the thin `radius*`/`within_box` wrappers are dropped in favour of `search`.
9. `ClientBuilder::keep_alive` → `tcp_keep_alive` (alias `keepAlive`).

## Table (from the audit)

## Naming rules used everywhere

Most of the tables follow from these, so it's worth agreeing on them first.

1. **Insert only if missing (NX):** `try_insert` (or `try_set` for single values). It returns `Option<V>` where a script can give back the old value, and `bool` where the existing Lua or command only reports success.
2. **Write only if present (XX):** `replace` when it returns the old value (as HashMap and `Option::replace` do), `update` when it returns `bool`.
3. **"Set once" config (Redisson `trySetX`):** `init_*`, by analogy with `OnceLock`. In Rust, `try_` usually means "fallible", and these return `Result<bool>`.
4. **Read-then-write pairs:** `get_and_set` → `swap`, `get_and_add` → `fetch_add`, `add_and_get` → `add`, `get_and_delete`/`get_del` → `take`.
5. **One-shot reads:** `read_all` → `to_vec()`.
6. **Queue verbs:** `poll` / `offer` / `peek` → `pop_front` / `push_back` / `front`.
7. **Local listener handles:** `listener_count` → `local_subscriber_count`, `remove_all_listeners` → `unsubscribe_all`.
8. **Builders:** use `IntoFuture` builders for optional TTL, expiry and conditions, as `HashMapCache::insert` already does. The exception is batch views. `BatchFuture` queues its command when the method is called, so a builder can't be added on afterwards.

Every renamed or merged method gets the `#[doc(alias)]` shown in its row. No row needs a Lua, command or key change. Where a nicer shape would need one, I chose the Rust-side shape or left the method alone.

---

### src/collections/sorted_set.rs (n≈10)
| current | new | alias | note |
|---|---|---|---|
| `insert_if_exists(&v, s)->bool` | `update(&v, s)->bool` | `addIfExists` | XX CH; rule 2 |
| `extend_if_absent / _if_exists / _if_greater / _if_less` | `extend(entries)` builder with `.if_absent()` / `.if_exists()` / `.if_greater()` / `.if_less()` → `usize` | `addAll`, `addAllIfAbsent`, `addAllIfExist`, `addAllIfGreater`, `addAllIfLess` | All four already return `usize`. Plain `extend` keeps "how many new"; with a condition it is "how many changed" (CH), as today |
| `range(Range<usize>)`, `rev_range`, `remove_range` | take `impl RangeBounds<usize>` | – | Matches `Vec::range`; the conversion stays in Rust |

I kept `try_insert`, `insert_if_greater` and `insert_if_less` as named methods. A builder on single `insert` would quietly change what the `bool` means ("new" vs "changed").

### src/collections/hash_set.rs
No changes. `retain(&[&Q])` takes a list of values, not a predicate (see Ambiguities). `try_extend` is all-or-nothing, which is a different meaning from SortedSet's per-value NX, so it stays.

### src/collections/hash_set_cache.rs (n≈11)
| current | new | alias | note |
|---|---|---|---|
| `insert_nx(&v)` (`SetInsert` builder) | `try_insert(&v)` builder → `bool` | `tryAdd` | Keeps `.ttl()` |

### src/collections/hash_map_cache.rs (n≈50)
| current | new | alias | note |
|---|---|---|---|
| `insert_nx(&k,&v)` (`InsertNx`) | `try_insert(&k,&v)` builder → `bool` | `fastPutIfAbsent`, `putIfAbsent` | `PUT_IF_ABSENT*` returns 0/1, so it can't return `Option<V>` the way `HashMap::try_insert` does without a Lua change. Document the difference |
| `try_set_max_size(max, mode)` | `init_max_size(max, mode)->bool` | `trySetMaxSize` | rule 3 |

`entry_ttl` stays: it's clearer than `remainTimeToLive`.

### src/collections/local_cached_map.rs
| current | new | alias | note |
|---|---|---|---|
| `cached_entries()` | `local_entries()` | `getCachedMap` | Pairs with `local_len` / `clear_local`. Optional |

Nothing else; it already uses HashMap's names.

### src/collections/vec.rs (n≈25)
| current | new | alias | note |
|---|---|---|---|
| `set(i,&v)->V` | `replace(i,&v)->V` | `set` | Like `mem::replace` |
| `fast_set(i,&v)->()` | `set(i,&v)->()` | `fastSet` | Callers that use the result of today's `set` get a compile error, which is safe |
| `fast_remove(i)->()` | drop → use `remove(i)` | `fastRemove` (on `remove`) | Same Lua, only the return differs |
| `insert_all(i, values)` | `insert_many(i, values)` | `addAll` | |
| `read_all()` | `to_vec()` | `readAll` | |
| `remove_all(&v)->usize` | `remove_value_all(&v)` | `removeAll` | "remove_all" reads like `clear`. Family becomes `remove_value` / `remove_value_n` / `remove_value_all` |

`remove_values` and `retain_values` keep their names (see Ambiguities).

### src/collections/vec_deque.rs (n≈12)
| current | new | alias | note |
|---|---|---|---|
| `remove_first_occurrence(&v)` | `remove_value(&v)` | `removeFirstOccurrence` | Same as `Vec::remove_value` |
| `remove_last_occurrence(&v)` | `remove_last_value(&v)` | `removeLastOccurrence` | |
| `drain_up_to(max)` | drop → use `pop_front_many(max)` | `drainTo` (on `pop_front_many`) | Same result; `DRAIN_UP_TO` becomes unused. Keep `drain()` |
| `read_all()` | `to_vec()` | `readAll` | |
| `extend_back_if_exists` / `extend_front_if_exists` | `extend_back(..).if_exists()` / `extend_front(..).if_exists()` builder | `addLastIfExists`, `addFirstIfExists` | Optional; RPUSHX/LPUSHX unchanged |

### src/values/atomic_i64.rs and atomic_f64.rs (n≈40)
I recommend keeping `get` / `set`. `load` / `store` suggest memory orderings that don't exist here, and `get` / `set` match Redis and `Cell`. I'd only change the Java read-then-write pairs:

| current | new | alias | note |
|---|---|---|---|
| `get_and_set(v)` | `swap(v)` | `getAndSet` | std name |
| `get_and_add(d)` | `fetch_add(d)` | `getAndAdd` | std name |
| `add_and_get(d)` | `add(d)` → new value | `addAndGet` | |
| `get_and_incr()` / `get_and_decr()` | drop → use `fetch_add(1)` / `fetch_add(-1)` | `getAndIncrement`, `getAndDecrement` | `incr` / `decr` stay |
| `get_and_delete()` | `take()` | `getAndDelete` | Like `Option::take` (0 when missing) |

`compare_and_set` stays. std's `compare_exchange` returns `Result<old, current>`, and the script only returns a `bool`. `set_if_less`, `set_if_greater` and `compare_and_delete` keep their names (they say what they do).

### src/values/bucket.rs (n≈45)
| current | new | alias | note |
|---|---|---|---|
| `set`, `set_ex`, `set_keep_ttl` | `set(&v)` builder with `.ttl(d)` / `.keep_ttl()` → `()` | `set`, `setex`, `setAndKeepTTL` | |
| `set_nx`, `set_nx_ex` | `try_set(&v)` builder with `.ttl(d)` → `bool` | `setIfAbsent`, `trySet` | rule 1 |
| `set_xx`, `set_xx_ex` | `update(&v)` builder with `.ttl(d)` → `bool` | `setIfExists` | rule 2 |
| `get_set`, `get_set_ex` | `replace(&v)` builder with `.ttl(d)` → `Option<V>` | `getAndSet` | Like `Option::replace` |
| `get_del()` | `take()` | `getAndDelete` | Like `Option::take` |
| `get_and_expire(ttl)` | `get_expire(ttl)` | `getAndExpire` | Parallels `Object::expire` |
| `get_and_clear_expire()` | `get_persist()` | `getAndClearExpire` | |
| `size()` (bytes) | `len()` | `size` | `String::len` is bytes too |

### src/values/buckets.rs (n≈12)
| current | new | alias | note |
|---|---|---|---|
| `try_set(entries)` and `set_if_all_absent(entries, expiry)` | `try_set(entries)` builder with `.expiry(Expiry)` | `trySet`, `setIfAllKeysAbsent` | Without expiry: MSETNX. With expiry: MSETEX NX (8.4) |
| `set_if_all_exist(entries, expiry)` | `update(entries)` builder with `.expiry(Expiry)` | `setIfAllKeysExist` | MSETEX XX, as today |

### src/values/bit_set.rs (n≈30)
| current | new | alias | note |
|---|---|---|---|
| `count()` | `count_ones()` | `cardinality` | std integer name |
| `length()` (highest set bit + 1) | `last_set()->Option<u64>` | `length` | Rust-side `checked_sub(1)`; pairs with `first_set` |
| `size()` (bits in the string) | `len()` | `size` | Like `BitVec::len` |
| `increment_and_get_signed` / `_unsigned` | `add_signed` / `add_unsigned` → new value | `incrementAndGetSigned`, `incrementAndGetUnsigned` | Same convention as `AtomicI64::add` |

### src/values/bloom_filter.rs (n≈50)
| current | new | alias | note |
|---|---|---|---|
| `try_init(n, p)` | `init(n, p)->bool` | `tryInit` | rule 3 |
| `insert_all(&[&Q])->u64` | `extend(impl IntoIterator)->u64` | `add(Collection)` | Same as `HashSet::extend` |
| `contains_all(&[&Q])->u64` | `count_contained(..)->u64` | `contains(Collection)` | `HashSet::contains_all` returns `bool`, so the current name is misleading |
| `contains_each(..)->Vec<bool>` | `contains_many` | – | Same as `HashSet::contains_many` |

### src/values/id_generator.rs (n≈6)
| current | new | alias | note |
|---|---|---|---|
| `try_init` | `init` | `tryInit` | rule 3 |

### src/values/geo.rs (n≈60)
| current | new | alias | note |
|---|---|---|---|
| `add(&v, at)` | `insert(&v, at)` | `add` | Same as SortedSet / HashSet |
| `try_add` | `try_insert` | `tryAdd` | |
| `add_if_exists` | `update(&v, at)->bool` | `addIfExists` | rule 2 |
| `hash` / `hash_many` | `geohash` / `geohash_many` | `hash` | `hash` collides with what `Hash::hash` means |
| `pos` / `pos_many`, `dist` | `position` / `positions`, `distance` | `pos`, `dist` | Optional, low value |
| `read_all()` | `to_vec()` | `readAll` | |
| `store_search_to` / `store_search_from_to` | `store_search` / `store_search_from` | `storeSearchTo` | Same as SortedSet `store_*` |
| `radius`, `radius_of`, `within_box` | drop → use `search(c, &GeoSearch::radius(..).order(Asc).count(n))` | `radius`, `searchWithDistance` (on `search`) | Thin wrappers around `search` |

`hyper_log_log.rs`: no changes.

### src/coordination/semaphore.rs (n≈60)
| current | new | alias | note |
|---|---|---|---|
| `acquire(n)` | `acquire()` + `acquire_many(n)` | `acquire` | Same as tokio and `PermitExpirableSemaphore` (about 15 call sites) |
| `try_acquire(n)` | `try_acquire()` + `try_acquire_many(n)` | `tryAcquire` | |
| `try_set_permits` and `try_set_permits_with_ttl` | `init_permits(n)` builder with `.ttl(d)` → `bool` | `trySetPermits` | |

### src/coordination/permit_semaphore.rs (n≈22)
| current | new | alias | note |
|---|---|---|---|
| `try_set_permits` | `init_permits` | `trySetPermits` | rule 3 |

### src/coordination/rate_limiter.rs (n≈75)
| current | new | alias | note |
|---|---|---|---|
| `try_set_rate`, `try_set_rate_with(args)` | `init_rate(type, rate, interval)` builder with `.keep_alive(d)` → `bool` | `trySetRate` | |
| `set_rate`, `set_rate_with(args)` | `set_rate(type, rate, interval)` builder with `.keep_alive(d)` / `.keep_state(b)` → `()` | `setRate` | |
| `update_rate(args)` | `update_rate(type, rate, interval)` builder (same options) → `bool` | `updateRate` | `RateLimiterArgs` stops being public |

### src/coordination/latch.rs (n≈25)
| current | new | alias | note |
|---|---|---|---|
| `try_set_count` | `init_count` | `trySetCount` | |

`adder.rs`: no changes.

### src/locks/*
No changes. They already follow tokio: `lock()` builder, `try_lock`, guards, `read` / `write`, `try_read` / `try_write`.

### src/messaging/delayed_queue.rs (n≈20)
| current | new | alias | note |
|---|---|---|---|
| `peek` | `front` | `peek` | |
| `poll` | `pop_front` | `poll` | |
| `poll_many(n)` | `pop_front_many(n)` | `poll(int)` | |
| `poll_last_and_offer_first_to(t)` | `pop_back_push_front(t)` | `pollLastAndOfferFirstTo` | Same as VecDeque |
| `remove_all(values)->bool` | `remove_values` | `removeAll` | Same as Vec |
| `retain_all(values)->bool` | `retain_values` | `retainAll` | Same as Vec |
| `values()->Vec` | `to_vec()` | `readAll` | Elsewhere `values()` is a Stream |

### src/messaging/topic.rs, sharded_topic.rs, reliable_topic.rs (n≈35)
| current | new | alias | note |
|---|---|---|---|
| `listener_count()` (all three) | `local_subscriber_count()` | `countListeners` | `subscriber_count()` (server side) stays |
| `remove_all_listeners()` (all three, plus PatternTopic) | `unsubscribe_all()` | `removeAllListeners` | |
| `ReliableTopic::size()` | `len()` | `size` | Counts messages, not bytes |

### src/messaging/stream/mod.rs (n≈3)
| current | new | alias | note |
|---|---|---|---|
| `update_group_message_id(g, id)` | `set_group_id(g, id)` | `updateGroupMessageId` | XGROUP SETID |
| `fast_claim` | `claim_ids` | `fastClaim` | |
| `fast_auto_claim` | `auto_claim_ids` | `fastAutoClaim` | |

### src/keys.rs (n≈3)
| current | new | alias | note |
|---|---|---|---|
| `iter_matching(p)` | drop → use `iter_with(KeysScan::new().pattern(p))` | `getKeysByPattern` (on `iter_with`) | Same as HashMap's `*_with(MapScan)`. To keep Redisson's page size of 10, make `KeysScan::pattern` default `chunk_size` to 10 |
| `rename_nx` | `try_rename` | `renamenx` | rule 1 |

### src/script.rs (n≈22)
| current | new | alias | note |
|---|---|---|---|
| `script_load` / `script_exists` / `script_flush` / `script_kill` | `load` / `exists` / `flush` / `kill` | `scriptLoad`, `scriptExists`, `scriptFlush`, `scriptKill` | `script.script_load` repeats itself; `Function` already uses these short names |

`Function::load_and_replace` stays (it's clear).

### src/nodes.rs (n≈5)
| current | new | alias | note |
|---|---|---|---|
| `RedisNode::size()` (DBSIZE) | `db_size()` | `size` | `size` usually means bytes. Check the Redisson name before adding the alias |
| `SentinelNode::master_addr` | `master_address` | `getMasterAddr` | |

`ping_within` stays (it's clear).

### src/batch/views.rs
| current | new | alias | note |
|---|---|---|---|
| `BatchAtomicI64::add_and_get` | `add` | `addAndGet` | Matches AtomicI64 |
| `BatchBucket::set_ex(&v, ttl)` | `set_with_ttl(&v, ttl)` | `setex` | Can't be a builder (rule 8). The only name that doesn't match its object |

The other views (HashMap, HashSet, Vec, VecDeque, SortedSet, Topic) only use names that don't change.

### src/client/mod.rs and config.rs
Factory names stay. The only change I'd consider, and it's optional: `ClientBuilder::keep_alive(bool)` → `tcp_keep_alive(bool)`, so it groups with `tcp_keep_alive_idle` / `tcp_keep_alive_interval` (alias `keepAlive`). The other setting names are fine.

---

