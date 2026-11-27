//! The scripts of a [`HashMapCache`](super::HashMapCache) with a local cache, the one a [`LocalCachedMapCache`](crate::LocalCachedMapCache) wraps.
//!
//! Each twin is Redisson's script text, unchanged, inside a redissun wrapper: a write twin publishes the sync messages (and writes the updates log) in the same script, a read twin also returns the entry's deadlines. The data a twin writes is byte for byte what the plain script writes.

use super::scripts;
use crate::lua::Lua;
use std::sync::LazyLock;

/// Wraps a write script. KEYS are the script's KEYS, then the updates log. ARGV are the script's ARGV, then `channel, mode, when, now, keep`, then one `notice, log entry` pair per key, then the count of these tail items. `when` says when the change is announced: `a` always, `n` when the reply is nil, `s` when it is not, `1` when it is `1`, `k` per key when the reply's element is `1`.
pub(super) fn wrap_write(body: &str) -> String {
    format!(
        "local n = #ARGV
local c = tonumber(ARGV[n])
local tail = {{}}
for i = 1, c do tail[i] = ARGV[n - c - 1 + i] end
local args = {{}}
for i = 1, n - c - 1 do args[i] = ARGV[i] end
local keys = {{}}
for i = 1, #KEYS - 1 do keys[i] = KEYS[i] end
local result = (function(KEYS, ARGV)
{body}
end)(keys, args)
local channel, mode, when, now, keep = tail[1], tail[2], tail[3], tail[4], tail[5]
if mode ~= '0' then
    for i = 6, c, 2 do
        local j = (i - 4) / 2
        if when == 'a' or (when == 'n' and not result) or (when == 's' and result)
            or (when == '1' and result == 1) or (when == 'k' and result[j] == 1) then
            if mode == '2' then
                redis.call('zadd', KEYS[#KEYS], now, tail[i + 1])
                redis.call('zremrangebyscore', KEYS[#KEYS], '-inf', '(' .. (tonumber(now) - tonumber(keep)))
            end
            redis.call('publish', channel, tail[i])
        end
    end
end
return result
"
    )
}

/// Wraps a read script whose KEYS start with main and timeout and whose ARGV are `now, key…`. A live value becomes `{value, ttl deadline, idle delta}`, `0` meaning no limit; a missing one stays nil. With `per_key` the script replies one value per key, else one value.
pub(super) fn wrap_read(body: &str, per_key: bool) -> String {
    let reply = if per_key {
        "local out = {}
for i = 1, #result do
    if result[i] == false then
        out[i] = false
    else
        out[i] = expiry(ARGV[i + 1], result[i])
    end
end
return out"
    } else {
        "if not result then
    return false
end
return expiry(ARGV[2], result)"
    };
    format!(
        "local args = {{}}
for i = 1, #ARGV do args[i] = ARGV[i] end
local result = (function(KEYS, ARGV)
{body}
end)(KEYS, args)
local function expiry(key, value)
    local score = redis.call('zscore', KEYS[2], key)
    local packed = redis.call('hget', KEYS[1], key)
    local idle = 0
    if packed ~= false then
        idle = struct.unpack('dLc0', packed)
    end
    return {{value, tonumber(score) or 0, idle}}
end
{reply}
"
    )
}

fn write_twin(body: &Lua) -> Lua {
    Lua::from_lua(wrap_write(body.source()))
}

/// redissun: PUT announced for LocalCachedMapCache
pub(super) static PUT: LazyLock<Lua> = LazyLock::new(|| write_twin(&scripts::PUT));

/// redissun: PUT_WITH_LIMITS announced for LocalCachedMapCache
pub(super) static PUT_WITH_LIMITS: LazyLock<Lua> =
    LazyLock::new(|| write_twin(&scripts::PUT_WITH_LIMITS));

/// redissun: REMOVE announced for LocalCachedMapCache
pub(super) static REMOVE: LazyLock<Lua> = LazyLock::new(|| write_twin(&scripts::REMOVE));

/// redissun: PUT_IF_ABSENT_WITH_LIMITS announced for LocalCachedMapCache
pub(super) static PUT_IF_ABSENT_WITH_LIMITS: LazyLock<Lua> =
    LazyLock::new(|| write_twin(&scripts::PUT_IF_ABSENT_WITH_LIMITS));

/// redissun: FAST_PUT_WITH_LIMITS announced for LocalCachedMapCache
pub(super) static FAST_PUT_WITH_LIMITS: LazyLock<Lua> =
    LazyLock::new(|| write_twin(&scripts::FAST_PUT_WITH_LIMITS));

/// redissun: REMOVE_IF announced for LocalCachedMapCache
pub(super) static REMOVE_IF: LazyLock<Lua> = LazyLock::new(|| write_twin(&scripts::REMOVE_IF));

/// redissun: FAST_REMOVE_BATCH announced for LocalCachedMapCache
pub(super) static FAST_REMOVE_BATCH: LazyLock<Lua> =
    LazyLock::new(|| write_twin(&scripts::FAST_REMOVE_BATCH));

/// redissun: REPLACE announced for LocalCachedMapCache
pub(super) static REPLACE: LazyLock<Lua> = LazyLock::new(|| write_twin(&scripts::REPLACE));

/// redissun: REPLACE_IF announced for LocalCachedMapCache
pub(super) static REPLACE_IF: LazyLock<Lua> = LazyLock::new(|| write_twin(&scripts::REPLACE_IF));

/// redissun: ADD_AND_GET announced for LocalCachedMapCache
pub(super) static ADD_AND_GET: LazyLock<Lua> = LazyLock::new(|| write_twin(&scripts::ADD_AND_GET));

/// redissun: PUT_ALL_TTL_IDLE announced for LocalCachedMapCache
pub(super) static PUT_ALL_TTL_IDLE: LazyLock<Lua> =
    LazyLock::new(|| write_twin(&scripts::PUT_ALL_TTL_IDLE));

/// redissun: EXPIRE_ENTRY announced for LocalCachedMapCache
pub(super) static EXPIRE_ENTRY: LazyLock<Lua> =
    LazyLock::new(|| write_twin(&scripts::EXPIRE_ENTRY));

/// redissun: EXPIRE_ENTRY_IF_NOT_SET announced for LocalCachedMapCache
pub(super) static EXPIRE_ENTRY_IF_NOT_SET: LazyLock<Lua> =
    LazyLock::new(|| write_twin(&scripts::EXPIRE_ENTRY_IF_NOT_SET));

/// redissun: EXPIRE_ENTRIES announced for LocalCachedMapCache
pub(super) static EXPIRE_ENTRIES: LazyLock<Lua> =
    LazyLock::new(|| write_twin(&scripts::EXPIRE_ENTRIES));

/// redissun: EXPIRE_ENTRIES_IF_NOT_SET announced for LocalCachedMapCache
pub(super) static EXPIRE_ENTRIES_IF_NOT_SET: LazyLock<Lua> =
    LazyLock::new(|| write_twin(&scripts::EXPIRE_ENTRIES_IF_NOT_SET));

/// redissun: LEASE_REMOVE announced for LocalCachedMapCache
pub(super) static LEASE_REMOVE: LazyLock<Lua> =
    LazyLock::new(|| write_twin(&scripts::LEASE_REMOVE));

/// redissun: GET read with its deadlines for LocalCachedMapCache. Replies nil or `{value, ttl deadline, idle delta}`.
pub(super) static GET_EXPIRY: LazyLock<Lua> =
    LazyLock::new(|| Lua::from_lua(wrap_read(scripts::GET.source(), false)));

/// redissun only, for LocalCachedMapCache's `clear`. KEYS: main, timeout, idle, last access, updates log. ARGV: channel, mode, clear message. Deletes them and publishes the message when something was deleted and the mode announces; replies `1` when something was deleted.
pub(super) static CLEAR_ANNOUNCED: LazyLock<Lua> =
    LazyLock::new(|| Lua::from_lua(DELETE_ANNOUNCED));

/// redissun only, for LocalCachedMapCache's `Object::del`: [`CLEAR_ANNOUNCED`] with the KEYS main, timeout, idle, last access, options, updates log.
pub(super) static DEL_ANNOUNCED: LazyLock<Lua> = LazyLock::new(|| Lua::from_lua(DELETE_ANNOUNCED));

const DELETE_ANNOUNCED: &str = "if redis.call('DEL', unpack(KEYS)) > 0 then
    if ARGV[2] ~= '0' then
        redis.call('PUBLISH', ARGV[1], ARGV[3])
    end
    return 1
end
return 0";

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs() -> std::vec::Vec<(&'static Lua, &'static Lua)> {
        vec![
            (&PUT, &scripts::PUT),
            (&PUT_WITH_LIMITS, &scripts::PUT_WITH_LIMITS),
            (&REMOVE, &scripts::REMOVE),
            (&GET_EXPIRY, &scripts::GET),
            (
                &PUT_IF_ABSENT_WITH_LIMITS,
                &scripts::PUT_IF_ABSENT_WITH_LIMITS,
            ),
            (&FAST_PUT_WITH_LIMITS, &scripts::FAST_PUT_WITH_LIMITS),
            (&REMOVE_IF, &scripts::REMOVE_IF),
            (&FAST_REMOVE_BATCH, &scripts::FAST_REMOVE_BATCH),
            (&REPLACE, &scripts::REPLACE),
            (&REPLACE_IF, &scripts::REPLACE_IF),
            (&ADD_AND_GET, &scripts::ADD_AND_GET),
            (&PUT_ALL_TTL_IDLE, &scripts::PUT_ALL_TTL_IDLE),
            (&EXPIRE_ENTRY, &scripts::EXPIRE_ENTRY),
            (&EXPIRE_ENTRY_IF_NOT_SET, &scripts::EXPIRE_ENTRY_IF_NOT_SET),
            (&EXPIRE_ENTRIES, &scripts::EXPIRE_ENTRIES),
            (
                &EXPIRE_ENTRIES_IF_NOT_SET,
                &scripts::EXPIRE_ENTRIES_IF_NOT_SET,
            ),
            (&LEASE_REMOVE, &scripts::LEASE_REMOVE),
        ]
    }

    #[test]
    fn every_twin_holds_redissons_body() {
        for (twin, body) in pairs() {
            assert!(twin.source().contains(body.source()));
        }
    }

    #[test]
    fn twins_differ_from_their_body() {
        for (twin, body) in pairs() {
            assert_ne!(twin.sha1(), body.sha1());
        }
    }
}
