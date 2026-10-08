# Changelog

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
