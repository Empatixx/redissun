# Changelog

## [0.1.0] - unreleased

### Added

- `Client` and `ClientBuilder` on top of `fred`, with a connection pool and `lock_lease` setting.
- `Codec` trait and `JsonCodec`.
- `Bucket`: `set`, `get`, `set_ex`, `set_nx`, `get_set`, `get_del`, `compare_and_set`.
- `Map`: `insert`, `get`, `remove`, `contains_key`, `len`, `is_empty`, `clear`, `extend`, `get_many`, `insert_nx`, `incr_by`, `incr_by_float`, and `iter`, `keys`, `values` as streams.
- `Lock`: reentrant lock with watchdog and pub/sub wake-ups; `lock`, `try_lock`, `lock_for`, `lock_with`, `is_locked`, `is_held_by_current`, `hold_count`, `force_unlock`.
- `Object` trait: `name`, `del`, `exists`, `rename`, `expire`, `ttl`, `persist`.
