# Changelog

## [0.12.0] - unreleased

### Added

- `LocalCachedMap`: a `HashMap` with a cache inside the program (`get`, `insert`, `remove`, `contains_key`, `len`, `clear`, `local_len`, `clear_local`, `iter`). Options: `cache_size` (LRU), `ttl` and `SyncStrategy::{Invalidate, Update, None}`. Writes and their messages run in one Lua script. The local cache is cleared when the pub/sub connection is restored.

## [0.11.0] - unreleased

### Added

- `HashSetCache`: a set in which every value can expire (`insert(..).ttl(d)`, `insert_nx`, `remove`, `contains`, `entry_ttl`, `len`, `evict_expired`, `iter`), stored like Redisson's `RedissonSetCache` with a background task that deletes expired values.
- `LockGuard::unlock` now finishes the release even when the caller cancels it.

### Fixed

- `DelayedQueue` no longer sleeps for up to an hour when its pub/sub subscription failed. Its idle sleep is now one minute.

## [0.10.0] - unreleased

### Added

- `MultiLock`: takes several locks (`Lock`, `FairLock`, `FencedLock`) as one, with `lock`, `try_lock` and `MultiLockGuard`. It follows Redisson's `RedissonMultiLock`: rounds that release everything and start again, so opposite lock orders do not deadlock.
- `DelayedQueue`: values that reach a destination `VecDeque` after a delay, with `push`, `remove`, `len`, `is_empty`, `clear` and `values`. It uses the same Lua scripts and keys as Redisson's `RedissonDelayedQueue`, and a background task moves due values.

## [0.9.0] - unreleased

### Added

- `BitSet`: `get`, `set`, `count`, `first_set`, `len`.
- `HyperLogLog`: `insert`, `extend`, `count`, `count_with`, `merge_from`.
- `BloomFilter`: `try_init`, `insert`, `contains`, `count`, `expected_insertions`, `false_probability`, `size_bits`, `hash_iterations`. The settings are stored in Redis like in Redisson's `RBloomFilter`.

## [0.8.0] - unreleased

### Added

- `FencedLock`: a `Lock` whose every new acquisition gets a higher fencing token, read with `LockGuard::fencing_token`. It also has `current_token`.
- `SortedSet::pop_first_wait` and `pop_last_wait` block until a value arrives (`BZPOPMIN`, `BZPOPMAX`), so a `SortedSet` works as a priority queue. Add `.timeout(duration)` to limit the wait.

## [0.7.0] - unreleased

### Added

- `FairLock`: a reentrant lock that hands itself to waiters in arrival order, taken from Redisson's `RedissonFairLock`. It has `lock`, `try_lock`, `is_locked`, `is_held_by_current`, `hold_count`, `queue_len` and `force_unlock`. A waiter that crashes loses its place after a few seconds.
- `SortedSet`: values with a score, stored in a Redis sorted set. It has `insert`, `add_score`, `score`, `remove`, `contains`, `rank`, `rev_rank`, `len`, `is_empty`, `clear`, `first`, `last`, `pop_first`, `pop_last`, `range`, `rev_range`, `range_by_score`, `count_by_score`, `remove_by_score` and `iter`.

## [0.6.0] - unreleased

### Added

- `HashMapCache::set_max_size` and `try_set_max_size` limit the number of entries, with `EvictionMode::{Lru, Lfu}`.
- `HashMapCache::events` returns `Events`, which receives `Event::{Created, Updated, Removed, Expired}` for the changes of every client.

## [0.5.0] - unreleased

### Added

- `HashMapCache` entries take `.max_idle(duration)` and a background task now deletes expired entries. `ClientBuilder::eviction_interval` sets how often it wakes up.
- `Pending` and `PendingTimeout`: the values that waiting calls return.

### Changed

- A zero `.timeout` on a `VecDeque` pop now pops once without waiting, like the other waiting calls. It used to be an error.
- A duration that is too long for Redis is now `Error::Config` and no longer wraps around.
- `HashMapCache` stores its data like Redisson's `RMapCache`: a hash plus a timeout set and an idle set.
- Breaking: every waiting call is now one method that you can `await` directly or limit with `.timeout(duration)`. This replaces `Lock::lock_for`, `Lock::lock_with`, `LockOptions`, `RwLock::read_for`, `RwLock::write_for`, `Semaphore::acquire_for`, `RateLimiter::try_acquire_for`, `CountDownLatch::wait_for`, `VecDeque::pop_front_for` and `VecDeque::pop_back_for`. A call with `.timeout` resolves to `Option`. `Lock::lock` also takes `.lease(duration)`.

## [0.4.0] - unreleased

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

## [0.3.0] - unreleased

### Added

- `Vec`: `push`, `extend`, `pop`, `get`, `set`, `insert`, `remove`, `range`, `trim`, `position`, `contains`, `remove_value`, `remove_all`, `len`, `is_empty`, `clear`, `iter`.
- `VecDeque`: `push_back`, `push_front`, `pop_back`, `pop_front`, `front`, `back`, `len`, `is_empty`, `clear`, `iter`.
- `HashSet`: `insert`, `extend`, `remove`, `contains`, `contains_many`, `len`, `is_empty`, `clear`, `pop`, `random`, `move_to`, `union`, `intersection`, `difference`, `iter`.
- `Error::OutOfRange`.

### Changed

- Breaking: `Map` is now `HashMap` (`client.hash_map`) and `AtomicLong` is now `AtomicI64` (`client.atomic_i64`), so the names follow Rust.

## [0.2.0] - unreleased

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


## [0.1.0] - unreleased

### Added

- `Client` and `ClientBuilder` on top of `fred`, with a connection pool, automatic reconnection, `lock_lease` and `connect_timeout` settings; connections close when the last clone is dropped.
- `Codec` trait and `JsonCodec`.
- `Bucket`: `set`, `get`, `set_ex`, `set_nx`, `get_set`, `get_del`, `compare_and_set`.
- `Map`: `insert`, `get`, `remove`, `contains_key`, `len`, `is_empty`, `clear`, `extend`, `get_many`, `insert_nx`, `incr_by`, `incr_by_float`, and `iter`, `keys`, `values` as streams.
- `Lock`: reentrant lock with watchdog and pub/sub wake-ups; `lock`, `try_lock`, `lock_for`, `lock_with`, `is_locked`, `is_held_by_current`, `hold_count`, `force_unlock`.
- `Object` trait: `name`, `del`, `exists`, `rename`, `expire`, `ttl`, `persist`.
