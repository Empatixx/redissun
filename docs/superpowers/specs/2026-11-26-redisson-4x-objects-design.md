# Redisson 4.x objects: design

## Why

Release 0.23.0 brought `Keys`, `Buckets`, `PermitExpirableSemaphore`, `ReliableTopic` and `ShardedTopic`. Compared with `RedissonClient` in Redisson 4.8.0, the most valuable objects still missing are: `RMapCacheNative`, the multimaps, `RTimeSeries`/`RTimeSeriesNative`, `RLocalCachedMapCache`, `RGcra`, `RReliableQueue` and `RReliablePubSubTopic`. The user wants all of them, with an API that is as idiomatic in Rust as possible, not Java with Rust names.

## Goals

- Every object listed below, with the full functionality of its Redisson interface (checked with `javap` on `redisson-4.8.0.jar` and in `redisson-4.8.0-sources.jar`).
- A Rust-first API shape (see "API conventions"). It goes further than the existing objects: builders with `IntoFuture`, ranges, ownership, streams and enums replace Java overloads and argument objects.
- Objects that Redisson implements in open source keep Redisson's data layout and Lua scripts, so Java and Rust programs share the data. The `redisson` prefix in key and channel names becomes `redissun`, as everywhere else in the crate.
- Every new object is **experimental** in the README table, in its docs page and in its rustdoc.
- Many tests: Redisson's own tests, ported, where they exist (`<Name>Test.java` from the `redisson-4.8.0` tag), plus our own tests for every method, edge cases, Cluster and failure tests for the queue and the topic.

## Non-goals

- Batch views (`RBatch.getMapCacheNative`, `getListMultimap*`, `getSetMultimap*`, `getTimeSeriesNative`). The existing `Batch` also lacks views that Redisson has (`getMapCache`, `getSetCache`, …). Batch coverage gets its own round.
- Reactive and Rx variants: the async API is the only API, as for every other object.
- `RReliableFanout`: the interface (`org.redisson.api.fanout`) is in the 4.8.0 jar, but `RedissonClient` has no getter for it, so Redisson users cannot reach it either. My call: left out; the queue core must not be shaped around it.
- Message groups (`groupId`): `ConsumerConfig.groupIdClaimTimeout` exists, but 4.8.0 has no way to give a message a `groupId` (`MessageArgs` has no setter), so the setting does nothing in Redisson. My call: no groups and no `group_id_claim_timeout`; the queue core keeps one leading version byte in its metadata so groups can be added later.
- The other missing objects (Bloom/Cuckoo/CountMin/TopK/TDigest/Json/Search/VectorSet, `NonReentrantLock`, `SpinLock`, `RingBuffer`, the queue variants). They get later rounds.

## Anti-goals

- A PRO object that only looks like a queue: losing a message after `poll` without `ack`, delivering the same message to two consumers at the same time within the visibility window, or a `clear`/`del` that leaves keys behind.
- An API that copies Java: `fooAsync`, `Duration`/`Instant` overload pairs, `Args.defaults().x().y()` objects, `-1`/`-2` as missing TTLs, `String...` varargs.
- `unwrap`/`expect`/`panic` in library code (`#![deny]` in `lib.rs`).

## Constraints

- Redis or Valkey 6.2+ stays the crate's requirement. Each object documents its own minimum, checked when the object is first used, with `Error::Unsupported`-style errors (use the crate's existing error for unsupported commands): `MapCacheNative` and `*MultimapCacheNative` need 7.4 (`HPEXPIRE`), `TimeSeriesNative` needs Redis 8 (or the RedisTimeSeries module), `Gcra` needs nothing beyond Lua.
- Measured on Docker images on 2026-11-26: `redis:8.8-alpine` (8.8.3), `8.10-alpine` (8.10.3) and `8.12-alpine` (8.12.0) have `HSETEX`, `HGETEX`, `HTTL` and the `timeseries` module, and **none of them knows `GCRA`**, although Redisson's `RGcra` says "Requires Redis 8.8.0 or higher". Valkey 9.2.1 does not have it either.
- Tests run on `redis_url()` (7.4) and `redis8_url()` (8.8-alpine) from `tests/common/mod.rs`; Cluster and Sentinel through `tests/common/topology.rs`. One `cargo test` at a time, `--test-threads 4`.
- Cluster: all keys of one object share a hash tag `{name}`, like the existing objects. A dead-letter queue or topic can live in another slot (see ReliableQueue).
- `rust-best-practices` applies: no `unwrap` outside tests, borrowed arguments (`&Q where K: Borrow<Q>`), `Result<T>` from `crate::Error`.

## Approaches

### API style

1. **As before:** every Redisson method with a Rust name, as `HashMap` does today. Consistent, but Java shapes remain (overload pairs, argument objects, string ids).
2. **Rust-first (chosen by the user):** the same functionality, in Rust shapes. Builders with `IntoFuture` for optional arguments (`HashMapCache::insert(..).ttl(..)` already works this way), `RangeBounds` for ranges, `Expiry`/condition enums instead of overloads, message handles that are consumed by `ack`, `futures::Stream` for consumers and events, handles for a multimap's values.

### Backend of the PRO objects (no Redisson source exists)

`RLocalCachedMapCache`, `RReliableQueue` and `RReliablePubSubTopic` throw `UnsupportedOperationException` in open-source Redisson. Their interfaces are public (`org.redisson.api.queue.*`, `org.redisson.api.pubsub.*`), their implementation is not. The user decided: implement them after the interfaces, with our own data layout, optimised for Redis, tested by our own suite, experimental.

- `LocalCachedMapCache`: compose `LocalCachedMap`'s local cache and invalidation with `HashMapCache`'s data layout (Redisson's open-source `RMapCache` layout). The data stays compatible with Redisson's `RMapCache`. No alternative is worth it.
- `ReliableQueue`: **Lua over sorted sets and hashes** (chosen) or **Streams with consumer groups**. Streams give visibility through `XAUTOCLAIM`, but delays, priorities, per-message TTL, remove/move by id and deduplication are hard on them. Lua keeps each operation atomic and does not depend on stream ids.
- `ReliablePubSubTopic`: in the conversation I proposed **Streams with consumer groups**. Reading `MessageArgs`, which topic messages share with queue messages, changed that: topic messages also have a priority, a delay, a per-message TTL, a delivery limit and deduplication, and `negativeAcknowledge` has a delay. Streams have no priority and no per-message redelivery delay. The design therefore uses **the queue's core per subscription**: a subscription is a queue of message ids, and the payload is stored once for the topic. Publish costs one `ZADD` per subscription in one script (subscriptions are few), and every queue feature works for subscriptions too. This is my call. Streams stay the fallback if the core turns out too slow in the benchmark (see Risks).

## API conventions (all new objects)

- Factory methods on `Client` in snake case: `client.map_cache_native::<K, V>("x")`, `client.list_multimap`, `client.set_multimap`, `client.list_multimap_cache`, `client.set_multimap_cache`, `client.list_multimap_cache_native`, `client.set_multimap_cache_native`, `client.time_series::<V, L>("x")`, `client.time_series_native("x")`, `client.time_series_natives()`, `client.local_cached_map_cache(..)` (builder like `local_cached_map`), `client.gcra("x")`, `client.reliable_queue::<V>("x")`, `client.reliable_pubsub_topic::<V>("x")`. Every object works with `with_codec` and implements `Object` (`name`, `del`, `exists`, `rename`, `expire`, `ttl`, `persist`) over all its keys.
- Optional arguments go in a builder that implements `IntoFuture`: `map.insert(&k, &v).ttl(d).await`, `queue.poll().visibility(d).timeout(d).await`.
- A time either as a duration or as a moment: `Expiry::{After(Duration), At(SystemTime)}` (the enum in `values/buckets.rs`, moved to a shared place when needed), or two builder methods `.ttl(d)` / `.expire_at(t)`, not two overloads.
- Conditional expiry: `ExpireCondition::{Always, IfNotSet, IfGreater, IfLess}` mapped to `NX`/`GT`/`LT`, instead of `expireEntryIfNotSet`, `…IfGreater`, `…IfLess`.
- Missing TTL is `None` (no `-1`/`-2`); counts are `usize`/`u64`.
- Many ids are `&[Id]`/`impl IntoIterator`, never varargs. Message ids are a newtype `MessageId(String)` with `Display`/`FromStr`.
- Events and push consumers are `Stream`s (`events()` → `Events` with `recv()` and `into_stream()`, as in `HashMapCache`). `removeListener` is dropping the stream.
- Names follow the existing objects: `insert`, `fast_insert`, `get`, `remove`, `len`, `iter`, `keys`, `values`, `read_all_*`, `contains_key`.

## The objects

Each object below is one pull request in this order. Each pull request has code, tests, its docs page (`website/content/docs/objects/<name>.mdx`), a README row marked experimental, rows in the migration guide and a `## Changelog` section. Release 0.24.0 comes after the last one.

### 0. Full map parity first (decided by the user on 2026-11-26: "úplně všechno")

"Full functionality" is meant literally: the new map objects get every method of the Redisson interfaces they extend. The code is shared, not copied, so the existing maps are brought up to Redisson first, in three pull requests before #1. They change stable objects, so their existing behaviour and tests stay as they are and only new methods and options are added.

- **0a. `HashMap` → all of `RMap`** (48 methods in `javap org.redisson.api.RMap`), in a shared hash-operations core that `MapCacheNative`, `HashMapCache`, `LocalCachedMap` and `LocalCachedMapCache` build on: `compute`, `compute_if_absent`, `compute_if_present`, `merge` (async closures under Redisson's per-key lock, as `RedissonMap` does), `extend`/`putAll` with batch size, `get_many`, `fast_insert_if_absent`, `fast_replace`, `value_size`, `random_entries`, `key_set`/`values`/`entry_set` with a pattern and a page size, `read_all_map`, `load_all`, and the rest the jar lists.
- **0b. `HashMapCache` → all of `RMapCache`** (27 methods of its own on top of `RMap`): `insert`/`fast_insert`/`insert_if_absent`/`fast_insert_if_absent`/`insert_if_exists` with ttl and max idle, `extend` with ttl, `update_entry_expiration`, `expire_entry`/`expire_entries`, `get_with_ttl`, `remain_time_to_live`, `set_max_size`/`try_set_max_size` (exist), `listeners`, and the rest the jar lists. Redisson's scripts from `RedissonMapCache.java`.
- **0c. Loader, writer and store mode for every map** (Redisson `MapOptions`, `LocalCachedMapOptions`, `LocalCachedMapCacheOptions`): a `MapLoader` trait (read-through on a miss, `load_all`), a `MapWriter` trait (`write`/`delete`), `WriteMode::{WriteThrough, WriteBehind { batch_size, delay }}` (`writeBehindBatchSize` default 50, `writeBehindDelay` default 1000 ms like Redisson), retries of the writer (`writerRetryAttempts`, `writerRetryInterval`), and for the local-cached maps `StoreMode::{LocalRedis, Local}`, `CacheProvider` (the local cache implementation) and `use_key_events_pattern`. Traits are async (`async fn` in traits, `Arc<dyn …>` stored in the object). Applies to `HashMap`, `HashMapCache`, `LocalCachedMap`, and to `MapCacheNative` and `LocalCachedMapCache` when they arrive.
- The same reading applies to the multimaps (#3): every method of `RMultimap`, `RListMultimap`, `RSetMultimap` and the cache interfaces, including `putAll(Multimap)` and the `keySet`/`entries`/`values` views.

### 1. `MapCacheNative` (Redisson `RMapCacheNative`, open source)

- Data: a plain Redis hash with per-field TTL (`HPEXPIRE`, `HPEXPIREAT`, `HPTTL`, `HPERSIST`). The scripts are Redisson's (`RedissonMapCacheNative.java`).
- API: everything `HashMap` has (shared code, not copied: a private module of hash operations that both use, or `MapCacheNative` wraps a `HashMap`. Builder's choice). On top of that:
  - `insert(&k,&v)`, `fast_insert`, `insert_if_absent`, `insert_if_exists`, `insert_nx` with `.ttl(d)` / `.expire_at(t)`;
  - `entry_ttl(&k) -> Option<Duration>`, `entry_ttls(&keys) -> HashMap<K, Option<Duration>>` (Redisson `remainTimeToLive(Set)`);
  - `expire_entry(&k, Expiry)` with `.condition(ExpireCondition)`, `expire_entries(&keys, Expiry)` with `.condition(..)` → number changed;
  - `persist_entry(&k)`, `persist_entries(&keys)` (Redisson `clearExpire`);
  - `extend_with(entries).ttl(d)` (Redisson `putAll(map, ttl)` and `putAll(PutArgs)`), `insert_if_all_absent(entries, Expiry)`, `insert_if_all_exist(entries, Expiry)` (`HSETEX FNX/FXX`, like Redisson);
  - `get_or_insert_with(&k, Expiry, async fn)` and `compute(&k, Expiry, async fn)` for Redisson's `computeIfAbsent`/`compute` with TTL, under the same per-key lock Redisson takes;
  - `events()` for Redisson's `addListener(ObjectListener)` (keyspace events `hexpired`, `hset`, `hdel`, …).
- Optimisation: use the single native command where Redisson does (`HSETEX`, `HPTTL`); measure the Lua `put`+`hpexpire` against `HSETEX ... KEEPTTL`-style alternatives only where the reply semantics are equal.
- Tests: port `RedissonMapCacheNativeTest.java`; run on `redis_url()` (7.4) and the `HSETEX` paths on `redis8_url()`; Cluster.

### 2. `Gcra` (Redisson `RGcra`, open source, needs a command no Redis has)

- Config: Redisson's layout, the hash `{name}:config` with `maxBurst`, `rate`, `period` (seconds), via `try_set_rate(GcraConfig)` / `set_rate(GcraConfig)` / `config() -> Option<GcraConfig>`.
- Acquire: `try_acquire()` and `try_acquire_n(tokens)` → `GcraResult { limited, max_tokens, available_tokens, retry_after: Duration, full_burst_after: Duration }`, the five values of Redisson's decoder (they are the reply of redis-cell's `CL.THROTTLE`).
- Because no released Redis has `GCRA`, the algorithm runs in Lua (theoretical arrival time in ms in the key `{name}`, server time from `TIME`), with the reply shape of the native command. My call: when `COMMAND INFO GCRA` says the server has the command, call it like Redisson. Otherwise use the Lua script. Documented: on one server, do not mix Redisson's native command with redissun's Lua on the same key, because the state formats differ.
- Redisson's deprecated `tryAcquire(maxBurst, rate, period[, tokens])` is not ported (deprecated in 4.8.0).
- Tests: port `RedissonGcraTest.java` where it does not depend on the server's command; property tests of the algorithm against a reference implementation in Rust (burst, refill, `tokens > 1`, `tokens > burst`).

### 3. Multimaps (Redisson `RListMultimap`, `RSetMultimap` and their `Cache`/`CacheNative` variants, open source)

- Six objects: `ListMultimap`, `SetMultimap`, `ListMultimapCache`, `SetMultimapCache`, `ListMultimapCacheNative`, `SetMultimapCacheNative`. Data and scripts are Redisson's (`RedissonMultimap`, `RedissonListMultimap`, `RedissonSetMultimap`, `RedissonMultimapCache`, `RedissonMultimapCacheNative`, the `*Values` classes): a hash from the encoded key to a hash of that key, and one list or set per key at `{name}:<hash>`. The cache variants keep Redisson's TTL set. The native ones use `HPEXPIRE` on the field and `PEXPIRE` on the values key.
- API, shared by a generic core with a `List`/`Set` flavour:
  - `insert(&k, &v) -> bool`, `extend(&k, values) -> bool`, `get(&k)` → a values handle (`ListMultimapValues`, `SetMultimapValues`) with the methods of `Vec` or `HashSet`. It is lazy, like Redisson's `get` view; the values-key name is computed from the key's hash on the client;
  - `get_all(&k) -> Vec<V>` (Redisson `getAll`), `remove(&k, &v) -> bool`, `remove_all(&k) -> Vec<V>`, `replace_values(&k, values) -> Vec<V>`, `fast_remove(&keys) -> usize`;
  - `contains_key`, `contains_value`, `contains_entry`, `len()` (all entries, Redisson `size`), `key_len()` (Redisson `keySize`), `is_empty`;
  - `keys()`, `values()`, `iter()` as `Stream`s, `read_all_keys() -> HashSet<K>`;
  - cache variants: `expire_key(&k, Duration) -> bool` (Redisson `expireKey`), and, in the native variants, what Redisson adds on top.
- Tests: port `RedissonListMultimapTest`, `RedissonSetMultimapTest`, `RedissonListMultimapCacheTest`, `RedissonSetMultimapCacheTest`, and the native cache tests when they exist in the tag. Cluster.

### 4. `LocalCachedMapCache` (Redisson `RLocalCachedMapCache`, PRO: interface only)

- `RLocalCachedMapCache<K,V> extends RMapCache<K,V>, RLocalCachedMap<K,V>` plus `preloadCache()`. Options: `LocalCachedMapCacheOptions` (read its fields from the jar).
- Data: `HashMapCache`'s layout (Redisson's `RMapCache`), so Java's `RMapCache` reads the same entries. Local cache, invalidation channel, `SyncStrategy`, `ReconnectionStrategy`, `EvictionPolicy` from `LocalCachedMap`. A local entry also keeps the remote entry's expiry, so it never outlives its TTL or max idle on the server.
- API: `HashMapCache`'s methods (insert with `.ttl`/`.max_idle`, `entry_ttl`, `set_max_size`, `events`, …) plus `LocalCachedMap`'s local ones (`local_len`, `cached_entries`, `clear_local`, `clear_local_everywhere`) plus `preload_cache()`, plus every option of `LocalCachedMapCacheOptions` (loader, writer, write mode, store mode, cache provider, key events pattern, from #0c).
- Implementation: share the code of the two existing objects, do not copy it.
- Tests: our own. Two clients see each other's writes and invalidations; local entries expire with the TTL and max idle; eviction from the server (max size, the clean-up task) invalidates the local copies; reconnection strategies; Cluster.

### 5. `TimeSeries`, `TimeSeriesNative`, `TimeSeriesNatives` (open source)

- `TimeSeries<V, L>`: Redisson's `RedissonTimeSeries` layout and scripts (a sorted set by timestamp, a hash of entries, `redissun__ts_ttl` and `redissun__ts_seq`). Timestamps are milliseconds as Redisson's `long`. The API takes `impl Into<Timestamp>` (from `u64` ms and from `SystemTime`).
  - `insert(ts, &v)` with `.label(&l)` and `.ttl(d)`; `extend(entries)`; `get(ts)`, `entry(ts) -> Option<TimeSeriesEntry<V, L>>`, `remove(ts)`;
  - `range(r)` / `entries_range(r)` with `RangeBounds`, `.rev()` and `.limit(n)` (Redisson `range`, `rangeReversed`, `entryRange`, …);
  - `first(n)`, `last(n)`, `first_timestamp`, `last_timestamp`, `pop_first(n)`, `pop_last(n)` (`pollFirst`/`pollLast`), `remove_range(r)`, `len`, `iter()`;
  - `events()` for Redisson's listeners.
- `TimeSeriesNative`: Redisson's `RedissonTimeSeriesNative` on `TS.*`: `create` (builder: retention, encoding, chunk size, duplicate policy, labels), `alter`, `add`/`add_many` (`TS.MADD`), `incr_by`/`decr_by`, `get`, `range`/`rev_range` with filters and `Aggregation` (enum of `TS` aggregators with bucket duration, alignment, empty buckets), `remove_range`, `info`, `create_rule`/`delete_rule`. Values are `f64`.
- `TimeSeriesNatives`: `TS.MGET`, `TS.MRANGE`, `TS.MREVRANGE`, `TS.QUERYINDEX` with a filter builder (`label = value`, `!=`, `=(a,b)`, …).
- Tests: port `RedissonTimeSeriesTest.java` and `RedissonTimeSeriesNativeTest.java` (and `…NativesTest` if it exists); `TimeSeriesNative*` on `redis8_url()`.

### 6. `ReliableQueue` (Redisson `RReliableQueue`, PRO: interface only)

Functionality from `RReliableQueueAsync` and `org.redisson.api.queue.*`:

- Config (`set_config`, `set_config_if_absent`, `QueueConfig`): `delivery_limit`, `visibility`, `ttl`, `dead_letter_queue`, `max_message_size`, `delay`, `max_size`, `processing_mode: ProcessingMode::{Sequential, Parallel}`.
- Add: `add(&v)` and `add_with(MessageArgs)`/`add_many(..)`. Per message: `priority` (0–9), `delay`, `ttl`, `delivery_limit`, `deduplication: Dedup::{ByHash(Duration), ById(String, Duration)}`, headers (`header(name, &value)`). The add builder has `.timeout(d)` to wait while the queue is full (`max_size`) and `.sync(..)` for Redisson's `SyncArgs`. Returns `Message<V>` (or `None` when deduplicated).
- Poll: `poll()` builder with `.visibility(d)`, `.timeout(d)` (wait for a message), `.ack_mode(AckMode::{Auto, Manual})`; `poll_many(n)`. Returns `Option<Delivery<V>>`: the message plus the queue handle. `delivery.ack()` and `delivery.nack(Nack::{Failed { delay }, Rejected})` consume it. Bulk `ack(&ids)` / `nack(&ids, ..)` stay for ids kept elsewhere. A `Delivery` dropped without ack is redelivered after its visibility, as in Redisson. `consumer()` builder → `Stream<Item = Result<Delivery<V>>>`, a loop of blocking polls (the shape Rust users expect for a consumer; it adds no feature).
- Inspect: `len`, `delayed_len`, `unacked_len`, `contains(&id)`, `contains_many(&ids)`, `get(&id)`, `get_many(&ids)`, `list_all()`, `remove(&ids)`, `move_to(dest, QueueMove::{Ids(..), Count(n)})` (Redisson `move` with `QueueMoveDestination`), `dead_letter_sources()`, `clear()`.
- Operations: `disable(QueueOperation)`/`enable(..)`; a disabled operation fails with a new `Error::OperationDisabled`.
- Events: `events()` → `QueueEvent::{Added, Polled, Acknowledged, NegativelyAcknowledged(status), Removed, Full, Config, Disabled(op), Enabled(op)}` (from `org.redisson.api.queue.event`).
- `Message<V> { id: MessageId, payload: V, headers }`. Headers are encoded with the codec. `message.header::<T>(name) -> Result<Option<T>>`.

Data (hash tag `{name}`, all in one slot):

| Key | Type | Holds |
|---|---|---|
| `{name}` | sorted set | ready ids, score = `(9 - priority) · 2^44 + seq` so higher priority first, then FIFO |
| `{name}:delayed` | sorted set | id → ready-at (ms, server time) |
| `{name}:unacked` | sorted set | id → visibility deadline (ms) |
| `{name}:expiry` | sorted set | id → expire-at (ms), for per-message and queue TTL |
| `{name}:payloads` | hash | id → payload |
| `{name}:meta` | hash | id → packed metadata: headers, priority, delivery count, delivery limit, dedup key |
| `{name}:dedup` | hash + `{name}:dedup:exp` sorted set | dedup key → id, expiry of the dedup window |
| `{name}:config`, `{name}:disabled`, `{name}:seq` | hash, set, string | config, disabled operations, id counter |
| `{name}:sources` | set | names of queues that use this one as dead-letter queue |

- Ids: `<ms>-<seq>` from the server's `TIME` and `{name}:seq` (sortable, unique per queue).
- Housekeeping runs at the start of every script that reads the queue, in bounded steps (at most 100 ids per call): delayed ids whose time has come become ready; unacked ids past their deadline become ready again with delivery count + 1, or go to the dead letter when the count reaches the limit; expired ids are deleted. No background task is needed for correctness.
- Waiting: a blocking `poll` subscribes to `redissun_rq:{name}` (published by add and by housekeeping when something became ready) and also wakes at the earliest delayed or unacked deadline. It uses the crate's existing pub/sub waiting (`wait.rs`), as locks and semaphores do.
- `ProcessingMode::Sequential`: a poll returns nothing while the queue has an unacked message.
- Dead-letter queue in another slot: the script moves the id to `{name}:dead` (a list) and returns it. The client then adds the message to the dead-letter queue with the same id and removes it from `{name}:dead`. After a crash between the two steps, the next housekeeping run retries it, so delivery is at least once.

### 7. `ReliablePubSubTopic` (Redisson `RReliablePubSubTopic`, PRO: interface only)

Functionality from `RReliablePubSubTopicAsync` and `org.redisson.api.pubsub.*`:

- Topic config (`TopicConfig`): `visibility`, `ttl`, `max_message_size`, `delay`, `max_size`, `delivery_limit`, `retention: Retention::{RequiredDeleteProcessed, RequiredRetainAll, OptionalRetainAll}`.
- `publish(&v)`, `publish_with(MessageArgs)`, `publish_many`; `len`, `contains`, `contains_many`, `get`, `get_many`, `list_all`, `clear`, `dead_letter_sources`, `disable`/`enable(PubSubOperation)`, `events()`, `statistics() -> TopicStatistics`.
- Subscriptions: `create_subscription()` builder (`name` or generated, `dead_letter_topic`, `delivery_limit`, `visibility`, `position: Position::{Latest, Earliest, Id(id, inclusive), Time(SystemTime, inclusive)}`, `retain_after_ack`), `subscription(name)`, `has_subscription`, `remove_subscription`, `subscriptions()`.
- `Subscription`: `pull_consumer()` / `push_consumer()` builders (`name` or generated), `consumer_names`, `has_consumer`, `remove_consumer`, `seek(Position)`, `statistics() -> SubscriptionStatistics`.
- `PullConsumer`: `pull()` builder (`.visibility`, `.timeout`, `.ack_mode`), `pull_many(n)` → `Delivery<V>` with `ack()`/`nack(..)` as in the queue. `PushConsumer`: `into_stream()` instead of `registerListener(MessageListenerArgs)`.

Data:

- `{name}:log` (sorted set, id → publish seq), `{name}:payloads` and `{name}:meta` (hashes) hold each message once, with a reference count of the subscriptions that still hold it.
- Each subscription `s` has the queue's keys under `{name}:sub:{s}:…` (ready, delayed, unacked), plus `{name}:subs` (hash, subscription → config) and `{name}:sub:{s}:consumers` (consumer → last seen).
- Publish adds the id to the log and to every subscription's ready (or delayed) set in one script.
- Retention: with `RequiredDeleteProcessed`, a message is deleted when its reference count falls to 0. With the `RetainAll` modes it stays until the topic's TTL or `max_size` removes it. `Optional…` accepts publishes without subscriptions; `Required…` rejects them (the topic's `Full`/error semantics from the interface).
- `Position` and `seek` rebuild a subscription's ready set from the log (only messages the topic still keeps).
- Delayed redelivery after `nack` and all housekeeping are the queue's scripts, run per subscription.

The queue and the topic share one private core (`messaging/reliable/`): scripts, housekeeping, `Delivery`, `Message`, `MessageArgs`, events.

## Testing

- Ported Redisson tests, one file per object (`tests/map_cache_native.rs`, …), as the other objects do.
- Our own tests for every public method, the experimental PRO objects included: visibility expiry and redelivery, delivery limit and the dead letter in the same and in another slot, priorities and FIFO within a priority, delays, per-message TTL, deduplication by hash and by id, `max_size` with a waiting add, sequential mode, disabled operations, every event, statistics, `Position` and `seek`, all three retention modes, a consumer stream that keeps running across a reconnect.
- Concurrency tests: N consumers on one queue get every message exactly once while they ack within the visibility, and at least once while some of them crash (dropped `Delivery`).
- Fault injection in `tests/chaos.rs` for the queue and the topic (connection dropped between poll and ack, between the dead-letter steps).
- Cluster tests for every object; Redis 8 tests for the `HSETEX` paths and `TimeSeriesNative*`.
- Benchmarks: a queue add + poll + ack and a topic publish to 1 and 10 subscriptions in `benches/`, to see what the Lua core costs.

## Risks

- **`GCRA` does not exist yet.** The Lua version is the real implementation for now. If Redis adds the command with another state format, keys written by the Lua version will not be read by it (documented, experimental).
- **Topic cost with many subscriptions.** Publish is O(subscriptions) inside one script. If the benchmark shows that 10 subscriptions cost more than 3× one, the topic moves to Streams with consumer groups and loses per-message priority (decided then, with the user).
- **Long scripts.** Housekeeping is bounded per call. `list_all`, `clear` and `seek` on large queues walk in pages, not in one script.
- **Server time.** Delays and visibility use the server's `TIME`, so clients with skewed clocks agree.
- **Stable maps change in #0.** New methods and options only; every existing test must pass unchanged, and the benchmark of `HashMap` must not get slower.
- **Size.** About 13 public objects and three parity pull requests on existing maps (#0a–#0c). If one of them grows past its pull request, it is split, and the order above stays.

## Left to the builder

- Module layout inside `src/` and how `MapCacheNative` and `LocalCachedMapCache` share code with existing objects (no copies).
- Exact type names of builders and events, as long as they follow "API conventions".
- The packing format of queue metadata (codec-independent, versioned with one leading byte).
- Which Redisson test cases cannot be ported (with the reason in a comment at the top of the test file).
