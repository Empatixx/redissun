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
