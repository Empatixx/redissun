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
pub(super) static LIST_VALUES_REMOVE_ALL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local v = 0 ",
        "for i = 2, #ARGV, 1 do ",
        "if redis.call('lrem', KEYS[2], 0, ARGV[i]) == 1 then ",
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
