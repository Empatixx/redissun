//! Redisson 4.8.0's `RedissonMapCacheNative` scripts, verbatim, with `KEYS` and `ARGV` in Redisson's order.

use crate::lua::Lua;
use std::sync::LazyLock;

/// RedissonMapCacheNative.putOperationAsync: `ARGV` = `[ms, key, value, expire command]`; returns the old value.
pub(super) static PUT: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local currValue = redis.call('hget', KEYS[1], ARGV[2]); \
         redis.call('hset', KEYS[1], ARGV[2], ARGV[3]); \
         redis.call(ARGV[4], KEYS[1], ARGV[1], 'fields', 1, ARGV[2]); \
         return currValue; ",
    )
});

/// RedissonMapCacheNative.fastPutOperationAsync: `ARGV` = `[ms, key, value, expire command]`; returns `hset`'s reply.
pub(super) static FAST_PUT: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local added = redis.call('hset', KEYS[1], ARGV[2], ARGV[3]); \
         redis.call(ARGV[4], KEYS[1], ARGV[1], 'fields', 1, ARGV[2]); \
         return added;",
    )
});

/// RedissonMapCacheNative.putIfAbsentOperationAsync: `ARGV` = `[ms, key, value, expire command]`; returns the present value, or nil when it stored.
pub(super) static PUT_IF_ABSENT: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local currValue = redis.call('hget', KEYS[1], ARGV[2]); \
         if currValue ~= false then \
         return currValue;\
         end;\
         redis.call('hset', KEYS[1], ARGV[2], ARGV[3]); \
         redis.call(ARGV[4], KEYS[1], ARGV[1], 'fields', 1, ARGV[2]); \
         return nil; ",
    )
});

/// RedissonMapCacheNative.putIfExistOperationAsync: `ARGV` = `[ms, key, value, expire command]`; returns the old value, or nil when the key is missing.
pub(super) static PUT_IF_EXIST: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local currValue = redis.call('hget', KEYS[1], ARGV[2]); \
         if currValue == false then \
         return nil;\
         end;\
         redis.call('hset', KEYS[1], ARGV[2], ARGV[3]); \
         redis.call(ARGV[4], KEYS[1], ARGV[1], 'fields', 1, ARGV[2]); \
         return currValue; ",
    )
});

/// RedissonMapCacheNative.putAllOperationAsync(map, ms): `ARGV` = `[ms, key1, value1, key2, value2, …]`; returns nothing.
pub(super) static PUT_ALL_TTL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "for i = 2, #ARGV, 2 do \
         redis.call('hset', KEYS[1], ARGV[i], ARGV[i + 1]); \
         redis.call('hpexpire', KEYS[1], ARGV[1], 'fields', 1, ARGV[i]); \
         end; ",
    )
});

/// RedissonMap.replaceOperationAsync(key, value), the script of `HashMap::replace`: `ARGV` = `[key, value]`; returns the old value, or nil when the key is missing.
///
/// Deviation from Redisson: it reads the field's `HPTTL` before the `hset` and puts it back with `HPEXPIRE` after, so `replace` keeps the entry's time to live, as `HashMapCache::replace` does. Redisson's `replace` clears it.
pub(super) static REPLACE_KEEP_TTL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then
            local value = redis.call('HGET', KEYS[1], ARGV[1])
            local ttl = redis.call('HPTTL', KEYS[1], 'FIELDS', 1, ARGV[1])[1]
            redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
            if ttl > 0 then
                redis.call('HPEXPIRE', KEYS[1], ttl, 'FIELDS', 1, ARGV[1])
            end
            return value
        end
        return nil",
    )
});

/// RedissonMap.replaceOperationAsync(key, oldValue, newValue), the script of `HashMap::compare_and_set`: `ARGV` = `[key, old, new]`; returns `1` when it replaced.
///
/// Deviation from Redisson: it reads the field's `HPTTL` before the `HSET` and puts it back with `HPEXPIRE` after, so `compare_and_set` keeps the entry's time to live, as `HashMapCache::compare_and_set` does. Redisson's `replace(key, old, new)` clears it.
pub(super) static COMPARE_AND_SET_KEEP_TTL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then
            local ttl = redis.call('HPTTL', KEYS[1], 'FIELDS', 1, ARGV[1])[1]
            redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
            if ttl > 0 then
                redis.call('HPEXPIRE', KEYS[1], ttl, 'FIELDS', 1, ARGV[1])
            end
            return 1
        end
        return 0",
    )
});

/// RedissonMapCacheNative.expireEntryAsyncInternal: `ARGV` = `[ms, key, expire command]`; returns `1` when it set the time.
pub(super) static EXPIRE_ENTRY: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local expireSet = redis.call(ARGV[3], KEYS[1], ARGV[1], 'fields', 1, ARGV[2]); \
         if #expireSet > 0 and expireSet[1] >= 1 then \
         return 1;\
         end; \
         return 0; ",
    )
});

/// RedissonMapCacheNative.expireEntryAsync(param, …): `ARGV` = `[ms, key, NX|GT|LT, expire command]`; returns `1` when it set the time.
pub(super) static EXPIRE_ENTRY_IF: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local expireSet = redis.call(ARGV[4], KEYS[1], ARGV[1], ARGV[3], 'fields', 1, ARGV[2]); \
         if #expireSet > 0 and expireSet[1] >= 1 then \
         return 1;\
         end; \
         return 0; ",
    )
});

/// RedissonMapCacheNative.expireEntriesAsyncInternal(keys, ms, …): `ARGV` = `[ms, expire command, key…]`; returns how many it changed.
pub(super) static EXPIRE_ENTRIES: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local result = 0;\
         for j = 3, #ARGV, 1 do \
         local expireSet = redis.call(ARGV[2], KEYS[1], ARGV[1], 'fields', 1, ARGV[j]); \
         if #expireSet > 0 and expireSet[1] >= 1 then \
         result = result + 1;\
         end; \
         end; \
         return result; ",
    )
});

/// RedissonMapCacheNative.expireEntriesAsyncInternal(param, keys, ms, …): `ARGV` = `[NX|GT|LT, ms, expire command, key…]`; returns how many it changed.
pub(super) static EXPIRE_ENTRIES_IF: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local result = 0;\
         for j = 4, #ARGV, 1 do \
         local expireSet = redis.call(ARGV[3], KEYS[1], ARGV[2], ARGV[1], 'fields', 1, ARGV[j]); \
         if #expireSet > 0 and expireSet[1] >= 1 then \
         result = result + 1;\
         end; \
         end; \
         return result; ",
    )
});
