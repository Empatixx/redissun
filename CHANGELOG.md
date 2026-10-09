# Changelog

## [Unreleased]

### Added

- `StringCodec` (plain UTF-8 text, like Redisson's `StringCodec`) and `BytesCodec` (raw bytes, like Redisson's `ByteArrayCodec`).
- `Client::with_codec(codec)`: a client with another codec that shares the connections, like Redisson's `getStream(name, codec)`. For example `client.with_codec(StringCodec).stream("s")` writes `XADD s * payload <json>` without JSON quotes.
- Redisson's connection settings on `ClientBuilder`: `timeout` (3 s), `retry_attempts` (4), `retry_delay` and `reconnection_delay` with `DelayStrategy` (`Constant`, `EqualJitter`, `FullJitter`, `DecorrelatedJitter`), `ping_connection_interval` (30 s), `keep_alive` with `tcp_keep_alive_idle` and `tcp_keep_alive_interval`, `tcp_no_delay` (true), `client_name` and `database`.
- `Batch::retry_delay`. A batch now takes its `response_timeout`, `retry_attempts` and retry delay from the client settings.

### Changed

- Commands now time out after 3 seconds by default, like Redisson's `timeout`. Before, a command waited forever. Blocking pops and reads are not limited. Set `.timeout(Duration::ZERO)` for the old behaviour.
- A command whose connection failed is now sent again up to 4 times (`retry_attempts`, Redisson's default) instead of 2. Commands that Redisson sends once are still sent once.
- Reconnection pauses follow Redisson's `EqualJitterDelay(100 ms, 10 s)` instead of growing from 100 ms to 30 s.
- `TCP_NODELAY` is on by default, as in Redisson.
- Every connection is checked with `PING` every 30 seconds and reconnected when it does not answer, like Redisson's `pingConnectionInterval`.
- The pub/sub connection is opened when an object first needs it (a lock or semaphore that waits, a `Topic` subscriber, cache events, `LocalCachedMap`, `DelayedQueue`), like Redisson's on-demand pub/sub connections. A client that never waits or subscribes keeps only its pool connections.

## [0.16.0] - 2026-10-09

The business logic of every object was compared with Redisson's source and aligned with it, and Redisson's own tests were ported, one Rust test file per type. Many APIs changed to match Redisson's behaviour.

### Changed

- Retries follow Redisson. Commands that Redisson sends without a retry are sent once: taking and releasing locks and semaphore permits, `Vec::insert`, pushes and pops, `DelayedQueue::push`, `SETNX`-like writes, `HINCRBY`, `ZINCRBY`, `GEOADD`, `XADD`. Other commands are retried up to 3 times, so a non-idempotent write such as `incr` can still be applied twice when a connection drops.
- Locks wait for a replica like Redisson's `syncedEval`: on Sentinel and Cluster the lock script is followed by `WAIT` on the same connection, and `Error::NoSyncedReplicas` is raised and retried when no replica acknowledged it. Settings: `check_lock_synced_replicas` (default true), `replicas_sync_timeout` (1 s), `fair_lock_wait_timeout` (300 s).
- Unlock uses Redisson's unlock latch, which is deleted after a successful unlock.
- `FencedLock` increments its token on every acquisition, reentrant ones included, and returns it from the acquire script.
- `FairLock` and `DelayedQueue` use the client clock and Redisson's scripts.
- `MultiLock` uses Redisson's algorithm again. Owners that list the same locks in different orders can block each other until their timeout, as in Redisson.
- `RwLock`: a write unlock publishes only when the lock is gone. Added read and write leases, hold counts and separate force unlocks.
- `Semaphore`: 0 permits is a no-op; added `try_set_permits_with_ttl` and `release_if_exists`; `add_permits` takes `i64`.
- `CountDownLatch`: `try_set_count(0)` creates an open latch, and deleting a latch wakes waiters.
- `RateLimiter`: Redisson's scripts and keys with the client clock, `release`, keep-alive time, `update_rate` and `config`. `available_permits` is no longer capped.
- `AtomicI64`: added `compare_and_delete`, `set_if_less`, `set_if_greater`, `get_and_incr`, `get_and_decr`.
- `BloomFilter`: HighwayHash128 with Redisson's key, so bit positions match Redisson for the same bytes; added `insert_all`, `contains_all`, `contains_each`.
- `Bucket::compare_and_set` takes `Option` values with Redisson's null rules. `Vec::set` returns the old value and `Vec::remove` returns `Result<V>`. `BitSet::len` is replaced by `size()` and `length()`. `SortedSet` score methods take ranges and `iter` uses `ZSCAN`. Blocking pops go straight to the blocking command with whole-second timeouts.
- `Object::expire`, `persist` and `rename` change all keys of an object in one script.
- `HashMapCache`: Redisson's eviction order, idle and LRU/LFU refresh on `contains_key` and iteration, one channel per event kind with `events_of`, and Redisson's clean-up task (one client per run, pause growing ×1.5 up to 30 minutes).
- `HashSetCache`: Redisson's scores and an `expired()` listener.
- `LocalCachedMap`: messages only when something changed; `ReconnectionStrategy` (None by default, Clear, Load), `EvictionPolicy` (None by default, Lru, Lfu), `max_idle`, `store_cache_miss`. Set `.eviction_policy(EvictionPolicy::Lru)` for a bounded cache.
- `Batch`: per-node `WAIT` and `MULTI`/`EXEC` in a cluster, Redisson's retry defaults, and `execute` fails with the first command error.
- Added `PatternTopic`.

### Fixed

- Pub/sub subscriptions are restored after a reconnect, retried every second until they succeed, and waiters are woken. Before, a dropped pub/sub connection silently stopped `Topic` subscribers, cache events and `LocalCachedMap` invalidation.
- Topic subscribers get `Error::Lagged` when messages were dropped.
- `HashMapCache` events reach listeners in Redis Cluster.
- `FencedLock` no longer leaves its watchdog running when the acquisition fails half way.
- Sentinel test topologies use ports below the Linux ephemeral range, so a replica's outgoing connection can no longer take a sentinel's port.
- The test containers are removed together with their volumes.

### Added

- Redisson's tests, ported per type (about 1100 tests in all).
- Sentinel and Cluster tests with a master failover, and fault-injection tests in the style of Jepsen (`tests/chaos.rs`).
- Git tags and GitHub releases for every published version.

## [0.15.0] - 2026-10-09

### Added

- A crab logo that eats the letter R, on the docs site and in the README.
- A new look for the docs site: Rust-orange accents, a landing page with badges, a code example and every object, a logo, and a GitHub link with the star count.
- The docs site serves `llms.txt`, `llms-full.txt` and every page as Markdown (`/llms.mdx/docs/<page>/content.md`) for language models and coding agents.
- `Batch` (`client.batch()`): queues commands of several objects and sends them in one round trip, as in Redisson's `RBatch`. Each queued call returns a `BatchFuture` with the typed reply. Modes: pipelined by default (`IN_MEMORY`), `.atomic()` for `MULTI`/`EXEC` (`IN_MEMORY_ATOMIC`), `.stored_in_redis()` to keep the commands in a `MULTI` transaction in Redis (`REDIS_WRITE_ATOMIC`). Options: `.skip_result()`, `.response_timeout()`, `.retry_attempts()`, `.retry_interval()`, `.sync()` and `.sync_aof()`. `execute()` returns a `BatchResult`, and `discard()` throws the commands away. Batch views exist for `HashMap`, `Bucket`, `HashSet`, `AtomicI64`, `Vec`, `VecDeque`, `SortedSet` and `Topic`, plus `del` and `expire` for any key.

### Fixed

- `FairLock` gives a waiter one more second before it drops it from the queue. Before, a waiter could lose its place when another waiter woke up at the very moment its time ended.
- `Geo` searches with a count of zero return an empty list instead of a Redis error.

## [0.14.0] - 2026-10-09

### Added

- `Geo`: members with a position on Earth, as in Redisson's `RGeo`. It has `add`, `remove`, `pos`, `hash`, `dist`, `len`, `is_empty`, `clear`, and the searches `radius`, `radius_of` and `within_box`, with `GeoPoint`, `GeoUnit` and `GeoMatch`. Needs Redis 6.2 or newer.

## [0.13.0] - 2026-10-09

### Added

- `Stream`: Redis Streams with consumer groups, as in Redisson's `RStream`. Entries: `add` (with `.max_len(n)`), `len`, `range`, `rev_range`, `remove`, `trim`, `read`, `read_wait`. Groups: `create_group`, `destroy_group`, `read_group`, `read_group_wait`, `ack`, `pending`, `pending_entries`, `claim`, `auto_claim`. Also `StreamId`, `StreamEntry`, `PendingSummary` and `PendingEntry`.

## [0.12.0] - 2026-10-09

### Fixed

- The background cleanup of `HashMapCache` and `HashSetCache` is no longer postponed by steady inserts with a TTL. Before, it could wait until the inserts paused.
- `LocalCachedMap` no longer caches a stale value when a change message arrives while a read or write is in progress. It also clears its cache when the pub/sub client falls behind.

### Added

- `LocalCachedMap`: a `HashMap` with a cache inside the program (`get`, `insert`, `remove`, `contains_key`, `len`, `clear`, `local_len`, `clear_local`, `iter`). Options: `cache_size` (LRU), `ttl` and `SyncStrategy::{Invalidate, Update, None}`. Writes and their messages run in one Lua script. The local cache is cleared when the pub/sub connection is restored.

## [0.11.0] - 2026-10-09

### Added

- `HashSetCache`: a set in which every value can expire (`insert(..).ttl(d)`, `insert_nx`, `remove`, `contains`, `entry_ttl`, `len`, `evict_expired`, `iter`), stored like Redisson's `RedissonSetCache` with a background task that deletes expired values.
- `LockGuard::unlock` now finishes the release even when the caller cancels it.

### Fixed

- `DelayedQueue` no longer sleeps for up to an hour when its pub/sub subscription failed. Its idle sleep is now one minute.

## [0.10.0] - 2026-10-09

### Added

- `MultiLock`: takes several locks (`Lock`, `FairLock`, `FencedLock`) as one, with `lock`, `try_lock` and `MultiLockGuard`. It follows Redisson's `RedissonMultiLock`: rounds that release everything and start again, so opposite lock orders do not deadlock.
- `DelayedQueue`: values that reach a destination `VecDeque` after a delay, with `push`, `remove`, `len`, `is_empty`, `clear` and `values`. It uses the same Lua scripts and keys as Redisson's `RedissonDelayedQueue`, and a background task moves due values.

## [0.9.0] - 2026-10-09

### Added

- `BitSet`: `get`, `set`, `count`, `first_set`, `len`.
- `HyperLogLog`: `insert`, `extend`, `count`, `count_with`, `merge_from`.
- `BloomFilter`: `try_init`, `insert`, `contains`, `count`, `expected_insertions`, `false_probability`, `size_bits`, `hash_iterations`. The settings are stored in Redis like in Redisson's `RBloomFilter`.

## [0.8.0] - 2026-10-09

### Added

- `FencedLock`: a `Lock` whose every new acquisition gets a higher fencing token, read with `LockGuard::fencing_token`. It also has `current_token`.
- `SortedSet::pop_first_wait` and `pop_last_wait` block until a value arrives (`BZPOPMIN`, `BZPOPMAX`), so a `SortedSet` works as a priority queue. Add `.timeout(duration)` to limit the wait.

## [0.7.0] - 2026-10-09

### Added

- `FairLock`: a reentrant lock that hands itself to waiters in arrival order, taken from Redisson's `RedissonFairLock`. It has `lock`, `try_lock`, `is_locked`, `is_held_by_current`, `hold_count`, `queue_len` and `force_unlock`. A waiter that crashes loses its place after a few seconds.
- `SortedSet`: values with a score, stored in a Redis sorted set. It has `insert`, `add_score`, `score`, `remove`, `contains`, `rank`, `rev_rank`, `len`, `is_empty`, `clear`, `first`, `last`, `pop_first`, `pop_last`, `range`, `rev_range`, `range_by_score`, `count_by_score`, `remove_by_score` and `iter`.

## [0.6.0] - 2026-10-09

### Added

- `HashMapCache::set_max_size` and `try_set_max_size` limit the number of entries, with `EvictionMode::{Lru, Lfu}`.
- `HashMapCache::events` returns `Events`, which receives `Event::{Created, Updated, Removed, Expired}` for the changes of every client.

## [0.5.0] - 2026-10-09

### Added

- `HashMapCache` entries take `.max_idle(duration)` and a background task now deletes expired entries. `ClientBuilder::eviction_interval` sets how often it wakes up.
- `Pending` and `PendingTimeout`: the values that waiting calls return.

### Changed

- A zero `.timeout` on a `VecDeque` pop now pops once without waiting, like the other waiting calls. It used to be an error.
- A duration that is too long for Redis is now `Error::Config` and no longer wraps around.
- `HashMapCache` stores its data like Redisson's `RMapCache`: a hash plus a timeout set and an idle set.
- Breaking: every waiting call is now one method that you can `await` directly or limit with `.timeout(duration)`. This replaces `Lock::lock_for`, `Lock::lock_with`, `LockOptions`, `RwLock::read_for`, `RwLock::write_for`, `Semaphore::acquire_for`, `RateLimiter::try_acquire_for`, `CountDownLatch::wait_for`, `VecDeque::pop_front_for` and `VecDeque::pop_back_for`. A call with `.timeout` resolves to `Option`. `Lock::lock` also takes `.lease(duration)`.

## [0.4.0] - 2026-10-09

### Added

- `Topic` with `Subscriber`: `publish`, `subscribe`, `subscriber_count`, `recv`, `into_stream`.
- `VecDeque` blocking pops: `pop_front_for`, `pop_back_for`, `pop_front_wait`, `pop_back_wait`.
- `RwLock`: `read`, `try_read`, `read_for`, `write`, `try_write`, `write_for`, `is_write_locked`, `force_unlock`.
- `HashMapCache`: `insert` and `insert_nx` (both with an optional `.ttl(duration)`), `get`, `remove`, `contains_key`, `entry_ttl`, `len`, `is_empty`, `clear`, `evict_expired`, and `iter`, `keys`, `values` as streams.
- `Error::Lagged`.

### Changed

- The license is now Apache-2.0. It was MIT.
- `Object::del`, `expire` and `persist` now cover every key of an object that owns more than one, and `rename` returns `Error::Unsupported` for such objects.

### Fixed

- `subscribe` now waits until Redis has registered the subscription. Before, a message published right after could be missed.

## [0.3.0] - 2026-10-09

### Added

- `Vec`: `push`, `extend`, `pop`, `get`, `set`, `insert`, `remove`, `range`, `trim`, `position`, `contains`, `remove_value`, `remove_all`, `len`, `is_empty`, `clear`, `iter`.
- `VecDeque`: `push_back`, `push_front`, `pop_back`, `pop_front`, `front`, `back`, `len`, `is_empty`, `clear`, `iter`.
- `HashSet`: `insert`, `extend`, `remove`, `contains`, `contains_many`, `len`, `is_empty`, `clear`, `pop`, `random`, `move_to`, `union`, `intersection`, `difference`, `iter`.
- `Error::OutOfRange`.

### Changed

- Breaking: `Map` is now `HashMap` (`client.hash_map`) and `AtomicLong` is now `AtomicI64` (`client.atomic_i64`), so the names follow Rust.

## [0.2.0] - 2026-10-09

### Added

- `AtomicLong`: `get`, `set`, `get_and_set`, `get_and_delete`, `compare_and_set`, `add_and_get`, `get_and_add`, `incr`, `decr`.
- `Semaphore` with `Permits` guards: `try_set_permits`, `add_permits`, `available_permits`, `drain_permits`, `acquire`, `try_acquire`, `acquire_for`, `release`.
- `CountDownLatch`: `try_set_count`, `count_down`, `count`, `wait`, `wait_for`.
- `RateLimiter` with a sliding window and `RateType::{Overall, PerClient}`: `try_set_rate`, `set_rate`, `try_acquire`, `try_acquire_for`, `acquire`, `available_permits`.
- `Error::Unsupported`.
- `Debug` for all public handle types. It never prints the connection URL.

### Changed

- `Map::insert`, `Map::insert_nx` and `Map::extend` take borrowed keys and values (`users.insert("jirka", &user)`), like the lookups already did.
- `Bucket` methods take the value as a borrowed form, so a `Bucket<String>` accepts `&str` (`bucket.set("hello")`) as well as `&String`.

### Fixed

- Cancelling `Lock::lock` or `Semaphore::acquire` at any moment no longer leaves the lock held or the permits lost.
- `RateLimiter` no longer drops the expiry of its value key when it releases old permits.
- `RateLimiter` keys of a name like `a{}b` now get a hash tag.
- `CountDownLatch::try_set_count(0)` is an error instead of leaving a key that blocks the latch forever.
- Pub/sub subscriptions are now released when the last waiter on a channel leaves. Before, a process that waited on many different lock names kept every subscription until it exited.


## [0.1.0] - 2026-10-08

### Added

- `Client` and `ClientBuilder` on top of `fred`, with a connection pool, automatic reconnection, `lock_lease` and `connect_timeout` settings; connections close when the last clone is dropped.
- `Codec` trait and `JsonCodec`.
- `Bucket`: `set`, `get`, `set_ex`, `set_nx`, `get_set`, `get_del`, `compare_and_set`.
- `Map`: `insert`, `get`, `remove`, `contains_key`, `len`, `is_empty`, `clear`, `extend`, `get_many`, `insert_nx`, `incr_by`, `incr_by_float`, and `iter`, `keys`, `values` as streams.
- `Lock`: reentrant lock with watchdog and pub/sub wake-ups; `lock`, `try_lock`, `lock_for`, `lock_with`, `is_locked`, `is_held_by_current`, `hold_count`, `force_unlock`.
- `Object` trait: `name`, `del`, `exists`, `rename`, `expire`, `ttl`, `persist`.
