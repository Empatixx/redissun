//! Redisson's multimap Lua scripts, copied verbatim (4.8.0). Each `static` names the method it comes from. Edits are only the recorded deviations, each marked `Deviation from Redisson`.

use crate::lua::Lua;
use std::sync::LazyLock;

/// `RedissonListMultimap.putAsync`. KEYS: map, values. ARGV: key, key hash, value.
pub(super) static LIST_PUT: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "redis.call('hsetnx', KEYS[1], ARGV[1], ARGV[2]); ",
        "redis.call('rpush', KEYS[2], ARGV[3]); ",
        "return 1; ",
    ))
});

/// `RedissonSetMultimap.putAsync`. KEYS: map, values. ARGV: key, key hash, value.
pub(super) static SET_PUT: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "redis.call('hsetnx', KEYS[1], ARGV[1], ARGV[2]); ",
        "return redis.call('sadd', KEYS[2], ARGV[3]); ",
    ))
});

/// `RedissonListMultimap.sizeAsync`. KEYS: map. ARGV: prefix.
pub(super) static LIST_LEN: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local keys = redis.call('hgetall', KEYS[1]); ",
        "local size = 0; ",
        "for i, v in ipairs(keys) do ",
        "if i % 2 == 0 then ",
        "local name = ARGV[1] .. v; ",
        "size = size + redis.call('llen', name); ",
        "end;",
        "end; ",
        "return size; ",
    ))
});

/// `RedissonSetMultimap.sizeAsync`. KEYS: map. ARGV: prefix.
pub(super) static SET_LEN: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local keys = redis.call('hgetall', KEYS[1]); ",
        "local size = 0; ",
        "for i, v in ipairs(keys) do ",
        "if i % 2 == 0 then ",
        "local name = ARGV[1] .. v; ",
        "size = size + redis.call('scard', name); ",
        "end;",
        "end; ",
        "return size; ",
    ))
});

/// `RedissonListMultimap.putAllAsync`. KEYS: map, values. ARGV: key, key hash, values….
pub(super) static LIST_PUT_ALL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "redis.call('hset', KEYS[1], ARGV[1], ARGV[2]); ",
        "local n = 0; ",
        "for i=3, #ARGV, 5000 do ",
        "n = n + redis.call('rpush', KEYS[2], unpack(ARGV, i, math.min(i+4999, table.getn(ARGV)))) ",
        "end; ",
        "return n; ",
    ))
});

/// `RedissonSetMultimap.putAllAsync`. KEYS: map, values. ARGV: key, key hash, values….
pub(super) static SET_PUT_ALL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "redis.call('hset', KEYS[1], ARGV[1], ARGV[2]); ",
        "local n = 0; ",
        "for i=3, #ARGV, 5000 do ",
        "n = n + redis.call('sadd', KEYS[2], unpack(ARGV, i, math.min(i+4999, table.getn(ARGV)))) ",
        "end; ",
        "",
        "return n; ",
    ))
});

/// `RedissonListMultimap.removeAsync`. KEYS: map, values. ARGV: key, value.
pub(super) static LIST_REMOVE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local res = redis.call('lrem', KEYS[2], 1, ARGV[2]); ",
        "if res == 1 and redis.call('llen', KEYS[2]) == 0 then ",
        "redis.call('hdel', KEYS[1], ARGV[1]); ",
        "end; ",
        "return res; ",
    ))
});

/// `RedissonSetMultimap.removeAsync`. KEYS: map, values. ARGV: key, value.
pub(super) static SET_REMOVE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local res = redis.call('srem', KEYS[2], ARGV[2]); ",
        "if res == 1 and redis.call('scard', KEYS[2]) == 0 then ",
        "redis.call('hdel', KEYS[1], ARGV[1]); ",
        "end; ",
        "return res; ",
    ))
});

/// `RedissonListMultimap.removeAllAsync`. KEYS: map, values. ARGV: key.
pub(super) static LIST_REMOVE_ALL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "redis.call('hdel', KEYS[1], ARGV[1]); ",
        "local members = redis.call('lrange', KEYS[2], 0, -1); ",
        "redis.call('del', KEYS[2]); ",
        "return members; ",
    ))
});

/// `RedissonSetMultimap.removeAllAsync`. KEYS: map, values. ARGV: key.
pub(super) static SET_REMOVE_ALL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "redis.call('hdel', KEYS[1], ARGV[1]); ",
        "local members = redis.call('smembers', KEYS[2]); ",
        "redis.call('del', KEYS[2]); ",
        "return members; ",
    ))
});

/// `RedissonListMultimap.replaceValuesAsync`. KEYS: map, values. ARGV: key, key hash, values….
pub(super) static LIST_REPLACE_VALUES: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "redis.call('hset', KEYS[1], ARGV[1], ARGV[2]); ",
        "local members = redis.call('lrange', KEYS[2], 0, -1); ",
        "redis.call('del', KEYS[2]); ",
        "if #ARGV > 2 then ",
        "for i=3, #ARGV, 5000 do ",
        "redis.call('rpush', KEYS[2], unpack(ARGV, i, math.min(i+4999, table.getn(ARGV)))) ",
        "end; ",
        "end; ",
        "return members; ",
    ))
});

/// `RedissonSetMultimap.replaceValuesAsync`. KEYS: map, values. ARGV: key, key hash, values….
pub(super) static SET_REPLACE_VALUES: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "redis.call('hset', KEYS[1], ARGV[1], ARGV[2]); ",
        "local members = redis.call('smembers', KEYS[2]); ",
        "redis.call('del', KEYS[2]); ",
        "if #ARGV > 2 then ",
        "for i=3, #ARGV, 5000 do ",
        "redis.call('sadd', KEYS[2], unpack(ARGV, i, math.min(i+4999, table.getn(ARGV)))) ",
        "end; ",
        "end; ",
        "return members; ",
    ))
});

/// `RedissonListMultimap.fastReplaceValuesAsync`. KEYS: map, values. ARGV: key, key hash, values….
pub(super) static LIST_FAST_REPLACE_VALUES: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "redis.call('hset', KEYS[1], ARGV[1], ARGV[2]); ",
        "redis.call('del', KEYS[2]); ",
        "if #ARGV > 2 then ",
        "for i=3, #ARGV, 5000 do ",
        "redis.call('rpush', KEYS[2], unpack(ARGV, i, math.min(i+4999, table.getn(ARGV)))) ",
        "end; ",
        "end; ",
    ))
});

/// `RedissonSetMultimap.fastReplaceValuesAsync`. KEYS: map, values. ARGV: key, key hash, values….
pub(super) static SET_FAST_REPLACE_VALUES: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "redis.call('hset', KEYS[1], ARGV[1], ARGV[2]); ",
        "redis.call('del', KEYS[2]); ",
        "if #ARGV > 2 then ",
        "for i=3, #ARGV, 5000 do ",
        "redis.call('sadd', KEYS[2], unpack(ARGV, i, math.min(i+4999, table.getn(ARGV)))) ",
        "end; ",
        "end; ",
    ))
});

/// `RedissonMultimap.fastRemoveAsync`. KEYS: map, the values key of each key…. ARGV: keys….
pub(super) static FAST_REMOVE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local res = redis.call('hdel', KEYS[1], unpack(ARGV)); ",
        "if res > 0 then ",
        "redis.call('del', unpack(KEYS, 2, #KEYS)); ",
        "end; ",
        "return res; ",
    ))
});

/// `RedissonListMultimap.fastRemoveValueAsync`. KEYS: map. ARGV: prefix, values….
pub(super) static LIST_FAST_REMOVE_VALUE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local keys = redis.call('hgetall', KEYS[1]); ",
        "local size = 0; ",
        "for i, v in ipairs(keys) do ",
        "if i % 2 == 0 then ",
        "local name = ARGV[1] .. v; ",
        "for j = 2, #ARGV, 1 do ",
        "size = size + redis.call('lrem', name, 1, ARGV[j]); ",
        "end; ",
        "end;",
        "end; ",
        "return size; ",
    ))
});

/// `RedissonSetMultimap.fastRemoveValueAsync`. KEYS: map. ARGV: prefix, values….
pub(super) static SET_FAST_REMOVE_VALUE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local keys = redis.call('hgetall', KEYS[1]); ",
        "local size = 0; ",
        "for i, v in ipairs(keys) do ",
        "if i % 2 == 0 then ",
        "local name = ARGV[1] .. v; ",
        "for j = 2, #ARGV, 1 do ",
        "size = size + redis.call('srem', name, ARGV[j]); ",
        "end; ",
        "if redis.call('exists', name) == 0 then ",
        "redis.call('hdel', KEYS[1], keys[i-1]); ",
        "end; ",
        "end;",
        "end; ",
        "return size; ",
    ))
});

/// `RedissonListMultimap.containsValueAsync`. KEYS: map. ARGV: value, prefix.
pub(super) static LIST_CONTAINS_VALUE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local keys = redis.call('hgetall', KEYS[1]); ",
        "for i, v in ipairs(keys) do ",
        "if i % 2 == 0 then ",
        "local name = ARGV[2] .. v; ",
        "",
        "local items = redis.call('lrange', name, 0, -1) ",
        "for i=1,#items do ",
        "if items[i] == ARGV[1] then ",
        "return 1; ",
        "end; ",
        "end; ",
        "end;",
        "end; ",
        "return 0; ",
    ))
});

/// `RedissonSetMultimap.containsValueAsync`. KEYS: map. ARGV: value, prefix.
pub(super) static SET_CONTAINS_VALUE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local keys = redis.call('hgetall', KEYS[1]); ",
        "for i, v in ipairs(keys) do ",
        "if i % 2 == 0 then ",
        "local name = ARGV[2] .. v; ",
        "if redis.call('sismember', name, ARGV[1]) == 1 then ",
        "return 1; ",
        "end;",
        "end;",
        "end; ",
        "return 0; ",
    ))
});

/// `RedissonListMultimap.containsEntryAsync`. KEYS: values. ARGV: value.
pub(super) static LIST_CONTAINS_ENTRY: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local items = redis.call('lrange', KEYS[1], 0, -1) ",
        "for i=1,#items do ",
        "if items[i] == ARGV[1] then ",
        "return 1; ",
        "end; ",
        "end; ",
        "return 0; ",
    ))
});

/// `RedissonMultimap.deleteAsync`. KEYS: map. ARGV: prefix.
pub(super) static DELETE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local entries = redis.call('hgetall', KEYS[1]); ",
        "local keys = {KEYS[1]}; ",
        "for i, v in ipairs(entries) do ",
        "if i % 2 == 0 then ",
        "local name = ARGV[1] .. v; ",
        "table.insert(keys, name); ",
        "end;",
        "end; ",
        "",
        "local n = 0 ",
        "for i=1, #keys,5000 do ",
        "n = n + redis.call('del', unpack(keys, i, math.min(i+4999, table.getn(keys)))) ",
        "end; ",
        "return n;",
    ))
});

/// `RedissonMultimap.renameAsync`. KEYS: map. ARGV: prefix, new prefix, new name.
pub(super) static RENAME: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local entries = redis.call('hgetall', KEYS[1]); ",
        "local keys = {}; ",
        "for i, v in ipairs(entries) do ",
        "if i % 2 == 0 then ",
        "table.insert(keys, v); ",
        "end;",
        "end; ",
        "",
        "redis.call('rename', KEYS[1], ARGV[3]); ",
        "for i=1, #keys, 1 do ",
        "redis.call('rename', ARGV[1] .. keys[i], ARGV[2] .. keys[i]); ",
        "end; ",
    ))
});

/// `RedissonMultimap.expireAsync`. KEYS: map. ARGV: milliseconds, prefix, `PEXPIRE` option (`''` for none).
pub(super) static EXPIRE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local entries = redis.call('hgetall', KEYS[1]); ",
        "for i, v in ipairs(entries) do ",
        "if i % 2 == 0 then ",
        "local name = ARGV[2] .. v; ",
        "if ARGV[3] ~= '' then ",
        "redis.call('pexpire', name, ARGV[1], ARGV[3]); ",
        "else ",
        "redis.call('pexpire', name, ARGV[1]); ",
        "end; ",
        "end;",
        "end; ",
        "if ARGV[3] ~= '' then ",
        "return redis.call('pexpire', KEYS[1], ARGV[1], ARGV[3]); ",
        "end; ",
        "return redis.call('pexpire', KEYS[1], ARGV[1]); ",
    ))
});

/// `RedissonMultimap.clearExpireAsync`. KEYS: map. ARGV: prefix.
pub(super) static CLEAR_EXPIRE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local entries = redis.call('hgetall', KEYS[1]); ",
        "for i, v in ipairs(entries) do ",
        "if i % 2 == 0 then ",
        "local name = ARGV[1] .. v; ",
        "redis.call('persist', name); ",
        "end;",
        "end; ",
        "return redis.call('persist', KEYS[1]); ",
    ))
});

/// `RedissonListMultimap.InnerList.removeAllAsync`. KEYS: map, values. ARGV: key, values….
///
/// Deviation from Redisson (D10): `lrem(...) == 1` → `lrem(...) > 0`. `LREM` with count 0 returns how many it removed, so Redisson reports `false` for a value that occurred more than once, and when that empties the list it keeps the key in the hash with no values.
pub(super) static LIST_VALUES_REMOVE_ALL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local v = 0 ",
        "for i = 2, #ARGV, 1 do ",
        "if redis.call('lrem', KEYS[2], 0, ARGV[i]) > 0 then ",
        "v = 1; ",
        "end ",
        "end ",
        "if v == 1 and redis.call('exists', KEYS[2]) == 0 then ",
        "redis.call('hdel', KEYS[1], ARGV[1]); ",
        "end ",
        "return v",
    ))
});

/// `RedissonSetMultimap.InnerSet.removeAllAsync`. KEYS: map, values. ARGV: key, values….
pub(super) static SET_VALUES_REMOVE_ALL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local count = 0;",
        "for i=2, #ARGV, 5000 do ",
        "count = count + redis.call('srem', KEYS[2], unpack(ARGV, i, math.min(i+4999, table.getn(ARGV)))) ",
        "end; ",
        "",
        "if count > 0 then ",
        "if redis.call('scard', KEYS[2]) == 0 then ",
        "redis.call('hdel', KEYS[1], ARGV[1]); ",
        "end; ",
        "return 1;",
        "end;",
        "return 0; ",
    ))
});

/// `RedissonListMultimap.putAllAsync`, for `ListMultimapCacheNative`. KEYS: map, values. ARGV: key, key hash, values….
///
/// Deviation from Redisson (D2): `hset` → `hsetnx`. `HSET` clears the field's TTL while the values key keeps its own; the field always holds the same hash, so `hsetnx` writes the same data.
pub(super) static NATIVE_LIST_PUT_ALL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "redis.call('hsetnx', KEYS[1], ARGV[1], ARGV[2]); ",
        "local n = 0; ",
        "for i=3, #ARGV, 5000 do ",
        "n = n + redis.call('rpush', KEYS[2], unpack(ARGV, i, math.min(i+4999, table.getn(ARGV)))) ",
        "end; ",
        "return n; ",
    ))
});

/// `RedissonSetMultimap.putAllAsync`, for `SetMultimapCacheNative`. KEYS: map, values. ARGV: key, key hash, values….
///
/// Deviation from Redisson (D2): `hset` → `hsetnx`. `HSET` clears the field's TTL while the values key keeps its own; the field always holds the same hash, so `hsetnx` writes the same data.
pub(super) static NATIVE_SET_PUT_ALL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "redis.call('hsetnx', KEYS[1], ARGV[1], ARGV[2]); ",
        "local n = 0; ",
        "for i=3, #ARGV, 5000 do ",
        "n = n + redis.call('sadd', KEYS[2], unpack(ARGV, i, math.min(i+4999, table.getn(ARGV)))) ",
        "end; ",
        "",
        "return n; ",
    ))
});

/// `RedissonMultimapCacheNative.expireKeyAsync`. KEYS: map, values. ARGV: milliseconds, key.
pub(super) static NATIVE_EXPIRE_KEY: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local res = redis.call('hpexpire', KEYS[1], ARGV[1], 'fields', 1, ARGV[2])",
        "if res[1] == 1 then ",
        "redis.call('pexpire', KEYS[2], ARGV[1]); ",
        "return 1;",
        "end; ",
        "return 0; ",
    ))
});

/// New, for `expire_key(&k, Duration::ZERO)` on the native multimaps. KEYS: map, values. ARGV: key.
///
/// Deviation from Redisson (D3): Redisson sends `hpexpire 0`, which deletes the field and leaves the values key behind; a zero TTL means "no TTL" crate-wide, so this clears the TTL of the field (`hpersist`) and of the values key (`persist`). Returns 1 when the key exists.
pub(super) static NATIVE_PERSIST_KEY: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local res = redis.call('hpersist', KEYS[1], 'fields', 1, ARGV[1]); ",
        "if res[1] == -2 then ",
        "return 0; ",
        "end; ",
        "redis.call('persist', KEYS[2]); ",
        "return 1; ",
    ))
});

/// `RedissonMultimapCache.expireKeyAsync`. KEYS: map, ttl zset. ARGV: deadline in Unix ms (`0` clears it), key.
pub(super) static CACHE_EXPIRE_KEY: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "if redis.call('hexists', KEYS[1], ARGV[2]) == 1 then ",
        "if tonumber(ARGV[1]) > 0 then ",
        "redis.call('zadd', KEYS[2], ARGV[1], ARGV[2]); ",
        "else ",
        "redis.call('zrem', KEYS[2], ARGV[2]); ",
        "end; ",
        "return 1; ",
        "else ",
        "return 0; ",
        "end",
    ))
});

/// `RedissonMultimapCache.deleteAsync`. KEYS: map, ttl zset. ARGV: prefix.
pub(super) static CACHE_DELETE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local entries = redis.call('hgetall', KEYS[1]); ",
        "local keys = {KEYS[1], KEYS[2]}; ",
        "for i, v in ipairs(entries) do ",
        "if i % 2 == 0 then ",
        "local name = ARGV[1] .. v; ",
        "table.insert(keys, name); ",
        "end;",
        "end; ",
        "local n = 0 ",
        "for i=1, #keys,5000 do ",
        "n = n + redis.call('del', unpack(keys, i, math.min(i+4999, table.getn(keys)))) ",
        "end; ",
        "return n;",
    ))
});

/// `RedissonMultimapCache.expireAsync`. KEYS: map, ttl zset. ARGV: milliseconds, prefix, `PEXPIRE` option (`''` for none).
pub(super) static CACHE_EXPIRE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "redis.call('zadd', KEYS[2], 92233720368547758, 'redissun__expiretag'); ",
        "local entries = redis.call('hgetall', KEYS[1]); ",
        "for i, v in ipairs(entries) do ",
        "if i % 2 == 0 then ",
        "local name = ARGV[2] .. v; ",
        "if ARGV[3] ~= '' then ",
        "redis.call('pexpire', name, ARGV[1], ARGV[3]); ",
        "else ",
        "redis.call('pexpire', name, ARGV[1]); ",
        "end; ",
        "end;",
        "end; ",
        "if ARGV[3] ~= '' then ",
        "redis.call('pexpire', KEYS[2], ARGV[1], ARGV[3]); ",
        "return redis.call('pexpire', KEYS[1], ARGV[1], ARGV[3]); ",
        "end; ",
        "redis.call('pexpire', KEYS[2], ARGV[1]); ",
        "return redis.call('pexpire', KEYS[1], ARGV[1]); ",
    ))
});

/// `RedissonMultimapCache.clearExpireAsync`. KEYS: map, ttl zset. ARGV: prefix.
pub(super) static CACHE_CLEAR_EXPIRE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "redis.call('zrem', KEYS[2], 'redissun__expiretag'); ",
        "local entries = redis.call('hgetall', KEYS[1]); ",
        "for i, v in ipairs(entries) do ",
        "if i % 2 == 0 then ",
        "local name = ARGV[1] .. v; ",
        "redis.call('persist', name); ",
        "end;",
        "end; ",
        "redis.call('persist', KEYS[2]); ",
        "return redis.call('persist', KEYS[1]); ",
    ))
});

/// `RedissonMultimap.renameAsync`, for the cache variants. KEYS: map, ttl zset. ARGV: prefix, new prefix, new name, new ttl zset.
///
/// Deviation from Redisson (D4): after Redisson's script, the ttl zset is renamed too when it exists. Redisson's cache does not override `rename`, so it drops every key TTL and leaks the old zset.
pub(super) static CACHE_RENAME: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local entries = redis.call('hgetall', KEYS[1]); ",
        "local keys = {}; ",
        "for i, v in ipairs(entries) do ",
        "if i % 2 == 0 then ",
        "table.insert(keys, v); ",
        "end;",
        "end; ",
        "",
        "redis.call('rename', KEYS[1], ARGV[3]); ",
        "for i=1, #keys, 1 do ",
        "redis.call('rename', ARGV[1] .. keys[i], ARGV[2] .. keys[i]); ",
        "end; ",
        "if redis.call('exists', KEYS[2]) == 1 then ",
        "redis.call('rename', KEYS[2], ARGV[4]); ",
        "end; ",
    ))
});

/// `RedissonListMultimapCache.containsKeyAsync`. KEYS: map, ttl zset. ARGV: now, key, values name.
pub(super) static CACHE_LIST_CONTAINS_KEY: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local value = redis.call('hget', KEYS[1], ARGV[2]); ",
        "if value ~= false then ",
        "local expireDate = 92233720368547758; ",
        "local expireDateScore = redis.call('zscore', KEYS[2], ARGV[2]); ",
        "if expireDateScore ~= false then ",
        "expireDate = tonumber(expireDateScore) ",
        "end; ",
        "if expireDate <= tonumber(ARGV[1]) then ",
        "return 0;",
        "end; ",
        "return redis.call('llen', ARGV[3]) > 0 and 1 or 0;",
        "end;",
        "return 0; ",
    ))
});

/// `RedissonSetMultimapCache.containsKeyAsync`. KEYS: map, ttl zset. ARGV: now, key, values name.
pub(super) static CACHE_SET_CONTAINS_KEY: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local value = redis.call('hget', KEYS[1], ARGV[2]); ",
        "if value ~= false then ",
        "local expireDate = 92233720368547758; ",
        "local expireDateScore = redis.call('zscore', KEYS[2], ARGV[2]); ",
        "if expireDateScore ~= false then ",
        "expireDate = tonumber(expireDateScore) ",
        "end; ",
        "if expireDate <= tonumber(ARGV[1]) then ",
        "return 0;",
        "end; ",
        "return redis.call('scard', ARGV[3]) > 0 and 1 or 0;",
        "end;",
        "return 0; ",
    ))
});

/// `RedissonListMultimapCache.containsValueAsync`. KEYS: map, ttl zset. ARGV: value, now, prefix.
pub(super) static CACHE_LIST_CONTAINS_VALUE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local keys = redis.call('hgetall', KEYS[1]); ",
        "for i, v in ipairs(keys) do ",
        "if i % 2 == 0 then ",
        "local expireDate = 92233720368547758; ",
        "local expireDateScore = redis.call('zscore', KEYS[2], keys[i-1]); ",
        "if expireDateScore ~= false then ",
        "expireDate = tonumber(expireDateScore) ",
        "end; ",
        "if expireDate > tonumber(ARGV[2]) then ",
        "local name = ARGV[3] .. v; ",
        "local items = redis.call('lrange', name, 0, -1) ",
        "for i=1,#items do ",
        "if items[i] == ARGV[1] then ",
        "return 1; ",
        "end; ",
        "end; ",
        "end; ",
        "end;",
        "end; ",
        "return 0; ",
    ))
});

/// `RedissonSetMultimapCache.containsValueAsync`. KEYS: map, ttl zset. ARGV: value, now, prefix.
pub(super) static CACHE_SET_CONTAINS_VALUE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local keys = redis.call('hgetall', KEYS[1]); ",
        "for i, v in ipairs(keys) do ",
        "if i % 2 == 0 then ",
        "local expireDate = 92233720368547758; ",
        "local expireDateScore = redis.call('zscore', KEYS[2], keys[i-1]); ",
        "if expireDateScore ~= false then ",
        "expireDate = tonumber(expireDateScore) ",
        "end; ",
        "if expireDate > tonumber(ARGV[2]) then ",
        "local name = ARGV[3] .. v; ",
        "if redis.call('sismember', name, ARGV[1]) == 1 then ",
        "return 1; ",
        "end;",
        "end; ",
        "end;",
        "end; ",
        "return 0; ",
    ))
});

/// `RedissonListMultimapCache.containsEntryAsync`. KEYS: values, ttl zset. ARGV: now, key, value.
pub(super) static CACHE_LIST_CONTAINS_ENTRY: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local expireDate = 92233720368547758; ",
        "local expireDateScore = redis.call('zscore', KEYS[2], ARGV[2]); ",
        "if expireDateScore ~= false then ",
        "expireDate = tonumber(expireDateScore) ",
        "end; ",
        "if expireDate > tonumber(ARGV[1]) then ",
        "local items = redis.call('lrange', KEYS[1], 0, -1); ",
        "for i = 1, #items do ",
        "if items[i] == ARGV[3] then ",
        "return 1; ",
        "end; ",
        "end; ",
        "end; ",
        "return 0; ",
    ))
});

/// `RedissonSetMultimapCache.containsEntryAsync`. KEYS: values, ttl zset. ARGV: now, key, value.
pub(super) static CACHE_SET_CONTAINS_ENTRY: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local expireDate = 92233720368547758; ",
        "local expireDateScore = redis.call('zscore', KEYS[2], ARGV[2]); ",
        "if expireDateScore ~= false then ",
        "expireDate = tonumber(expireDateScore) ",
        "end; ",
        "if expireDate > tonumber(ARGV[1]) then ",
        "if redis.call('sismember', KEYS[1], ARGV[3]) == 1 then ",
        "return 1; ",
        "end;",
        "end; ",
        "return 0; ",
    ))
});

/// `RedissonListMultimapCache.getAllAsync`. KEYS: values, ttl zset. ARGV: now, key.
pub(super) static CACHE_LIST_GET_ALL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local expireDate = 92233720368547758; ",
        "local expireDateScore = redis.call('zscore', KEYS[2], ARGV[2]); ",
        "if expireDateScore ~= false then ",
        "expireDate = tonumber(expireDateScore) ",
        "end; ",
        "if expireDate > tonumber(ARGV[1]) then ",
        "return redis.call('lrange', KEYS[1], 0, -1); ",
        "end; ",
        "return {}; ",
    ))
});

/// `RedissonSetMultimapCache.getAllAsync`. KEYS: values, ttl zset. ARGV: now, key.
pub(super) static CACHE_SET_GET_ALL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local expireDate = 92233720368547758; ",
        "local expireDateScore = redis.call('zscore', KEYS[2], ARGV[2]); ",
        "if expireDateScore ~= false then ",
        "expireDate = tonumber(expireDateScore) ",
        "end; ",
        "if expireDate > tonumber(ARGV[1]) then ",
        "return redis.call('smembers', KEYS[1]); ",
        "end; ",
        "return {}; ",
    ))
});

/// `RedissonListMultimapCache.removeAllAsync`. KEYS: map, values, ttl zset. ARGV: key.
pub(super) static CACHE_LIST_REMOVE_ALL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "redis.call('hdel', KEYS[1], ARGV[1]); ",
        "local members = redis.call('lrange', KEYS[2], 0, -1); ",
        "redis.call('del', KEYS[2]); ",
        "redis.call('zrem', KEYS[3], ARGV[1]); ",
        "return members; ",
    ))
});

/// `RedissonSetMultimapCache.removeAllAsync`. KEYS: map, values, ttl zset. ARGV: key.
pub(super) static CACHE_SET_REMOVE_ALL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "redis.call('hdel', KEYS[1], ARGV[1]); ",
        "local members = redis.call('smembers', KEYS[2]); ",
        "redis.call('del', KEYS[2]); ",
        "redis.call('zrem', KEYS[3], ARGV[1]); ",
        "return members; ",
    ))
});

/// `RedissonListMultimap.putAsync`, for `ListMultimapCache`. KEYS: map, values, ttl zset. ARGV: key, key hash, value.
///
/// Deviation from Redisson (D5): when `hsetnx` adds the key, its score is dropped from the ttl zset. `remove`, `remove_many` and `remove_from_all_keys` leave the score behind, so a key inserted again would read as expired and the clean-up would delete its new values.
pub(super) static CACHE_LIST_PUT: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "if redis.call('hsetnx', KEYS[1], ARGV[1], ARGV[2]) == 1 then redis.call('zrem', KEYS[3], ARGV[1]); end; ",
        "redis.call('rpush', KEYS[2], ARGV[3]); ",
        "return 1; ",
    ))
});

/// `RedissonSetMultimap.putAsync`, for `SetMultimapCache`. KEYS: map, values, ttl zset. ARGV: key, key hash, value.
///
/// Deviation from Redisson (D5): when `hsetnx` adds the key, its score is dropped from the ttl zset (see `CACHE_LIST_PUT`).
pub(super) static CACHE_SET_PUT: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "if redis.call('hsetnx', KEYS[1], ARGV[1], ARGV[2]) == 1 then redis.call('zrem', KEYS[3], ARGV[1]); end; ",
        "return redis.call('sadd', KEYS[2], ARGV[3]); ",
    ))
});

/// `RedissonListMultimap.putAllAsync`, for `ListMultimapCache`. KEYS: map, values, ttl zset. ARGV: key, key hash, values….
///
/// Deviation from Redisson (D5): when `hset` adds the key, its score is dropped from the ttl zset (see `CACHE_LIST_PUT`).
pub(super) static CACHE_LIST_PUT_ALL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "if redis.call('hset', KEYS[1], ARGV[1], ARGV[2]) == 1 then redis.call('zrem', KEYS[3], ARGV[1]); end; ",
        "local n = 0; ",
        "for i=3, #ARGV, 5000 do ",
        "n = n + redis.call('rpush', KEYS[2], unpack(ARGV, i, math.min(i+4999, table.getn(ARGV)))) ",
        "end; ",
        "return n; ",
    ))
});

/// `RedissonSetMultimap.putAllAsync`, for `SetMultimapCache`. KEYS: map, values, ttl zset. ARGV: key, key hash, values….
///
/// Deviation from Redisson (D5): when `hset` adds the key, its score is dropped from the ttl zset (see `CACHE_LIST_PUT`).
pub(super) static CACHE_SET_PUT_ALL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "if redis.call('hset', KEYS[1], ARGV[1], ARGV[2]) == 1 then redis.call('zrem', KEYS[3], ARGV[1]); end; ",
        "local n = 0; ",
        "for i=3, #ARGV, 5000 do ",
        "n = n + redis.call('sadd', KEYS[2], unpack(ARGV, i, math.min(i+4999, table.getn(ARGV)))) ",
        "end; ",
        "",
        "return n; ",
    ))
});

/// `RedissonListMultimap.replaceValuesAsync`, for `ListMultimapCache`. KEYS: map, values, ttl zset. ARGV: key, key hash, values….
///
/// Deviation from Redisson (D5): when `hset` adds the key, its score is dropped from the ttl zset (see `CACHE_LIST_PUT`).
pub(super) static CACHE_LIST_REPLACE_VALUES: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "if redis.call('hset', KEYS[1], ARGV[1], ARGV[2]) == 1 then redis.call('zrem', KEYS[3], ARGV[1]); end; ",
        "local members = redis.call('lrange', KEYS[2], 0, -1); ",
        "redis.call('del', KEYS[2]); ",
        "if #ARGV > 2 then ",
        "for i=3, #ARGV, 5000 do ",
        "redis.call('rpush', KEYS[2], unpack(ARGV, i, math.min(i+4999, table.getn(ARGV)))) ",
        "end; ",
        "end; ",
        "return members; ",
    ))
});

/// `RedissonSetMultimap.replaceValuesAsync`, for `SetMultimapCache`. KEYS: map, values, ttl zset. ARGV: key, key hash, values….
///
/// Deviation from Redisson (D5): when `hset` adds the key, its score is dropped from the ttl zset (see `CACHE_LIST_PUT`).
pub(super) static CACHE_SET_REPLACE_VALUES: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "if redis.call('hset', KEYS[1], ARGV[1], ARGV[2]) == 1 then redis.call('zrem', KEYS[3], ARGV[1]); end; ",
        "local members = redis.call('smembers', KEYS[2]); ",
        "redis.call('del', KEYS[2]); ",
        "if #ARGV > 2 then ",
        "for i=3, #ARGV, 5000 do ",
        "redis.call('sadd', KEYS[2], unpack(ARGV, i, math.min(i+4999, table.getn(ARGV)))) ",
        "end; ",
        "end; ",
        "return members; ",
    ))
});

/// `RedissonListMultimap.fastReplaceValuesAsync`, for `ListMultimapCache`. KEYS: map, values, ttl zset. ARGV: key, key hash, values….
///
/// Deviation from Redisson (D5): when `hset` adds the key, its score is dropped from the ttl zset (see `CACHE_LIST_PUT`).
pub(super) static CACHE_LIST_FAST_REPLACE_VALUES: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "if redis.call('hset', KEYS[1], ARGV[1], ARGV[2]) == 1 then redis.call('zrem', KEYS[3], ARGV[1]); end; ",
        "redis.call('del', KEYS[2]); ",
        "if #ARGV > 2 then ",
        "for i=3, #ARGV, 5000 do ",
        "redis.call('rpush', KEYS[2], unpack(ARGV, i, math.min(i+4999, table.getn(ARGV)))) ",
        "end; ",
        "end; ",
    ))
});

/// `RedissonSetMultimap.fastReplaceValuesAsync`, for `SetMultimapCache`. KEYS: map, values, ttl zset. ARGV: key, key hash, values….
///
/// Deviation from Redisson (D5): when `hset` adds the key, its score is dropped from the ttl zset (see `CACHE_LIST_PUT`).
pub(super) static CACHE_SET_FAST_REPLACE_VALUES: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "if redis.call('hset', KEYS[1], ARGV[1], ARGV[2]) == 1 then redis.call('zrem', KEYS[3], ARGV[1]); end; ",
        "redis.call('del', KEYS[2]); ",
        "if #ARGV > 2 then ",
        "for i=3, #ARGV, 5000 do ",
        "redis.call('sadd', KEYS[2], unpack(ARGV, i, math.min(i+4999, table.getn(ARGV)))) ",
        "end; ",
        "end; ",
    ))
});

/// `MultimapEvictionTask.execute`. KEYS: map, ttl zset. ARGV: now, the most keys to delete.
///
/// Deviation from Redisson (D1): the values names start with `suffix_name(KEYS[1], '')`, computed in Lua, instead of `'{' .. KEYS[1] .. '}:'`. Redisson's prefix is wrong for names that contain `{`: it leaks their values keys, and on a cluster the script fails with a non-local key.
pub(super) static CACHE_EVICT: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local expiredKeys = redis.call('zrangebyscore', KEYS[2], 0, ARGV[1], 'limit', 0, ARGV[2]); ",
        "if #expiredKeys > 0 then ",
        "redis.call('zrem', KEYS[2], unpack(expiredKeys)); ",
        "local values = redis.call('hmget', KEYS[1], unpack(expiredKeys)); ",
        "local keys = {}; ",
        "local prefix = string.find(KEYS[1], '{', 1, true) and (KEYS[1] .. ':') or ('{' .. KEYS[1] .. '}:'); ",
        "for i, v in ipairs(values) do ",
        "if v ~= false then ",
        "local name = prefix .. v; ",
        "table.insert(keys, name); ",
        "end;",
        "end; ",
        "if #keys > 0 then ",
        "redis.call('del', unpack(keys)); ",
        "end; ",
        "redis.call('hdel', KEYS[1], unpack(expiredKeys)); ",
        "end; ",
        "return #expiredKeys;",
    ))
});
