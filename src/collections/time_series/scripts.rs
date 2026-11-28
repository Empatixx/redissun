//! The Lua scripts of Redisson 4.8.0's `RedissonTimeSeries` and `TimeSeriesEvictionTask`, copied verbatim.
//!
//! Each fragment keeps Redisson's name and is joined in the order of Redisson's `+`. The scripts take KEYS and ARGV in Redisson's order.
//! The file is generated from the Redisson sources: do not edit a script by hand.

use crate::lua::Lua;
use std::sync::LazyLock;

/// RedissonTimeSeries.NEXT_ID (line 375)
const NEXT_ID: &str = concat!(
    "local sequenceWidth = tonumber(ARGV[1]); ",
    "local previous; ",
    "local function nextId() ",
    "if previous == nil then ",
    "previous = redis.call('hget', KEYS[3], 'id'); ",
    "if previous == false then ",
    "previous = '0'; ",
    "end; ",
    "end; ",
    "local sequence = redis.call('hincrbyfloat', KEYS[3], 'id', 1); ",
    "if sequence == previous or #sequence > sequenceWidth then ",
    "error('RTimeSeries sequence overflow'); ",
    "end; ",
    "previous = sequence; ",
    "return string.rep('0', sequenceWidth - #sequence) .. sequence; ",
    "end; ",
);

/// RedissonTimeSeries.REMOVE_MEMBERS (line 471)
const REMOVE_MEMBERS: &str = concat!(
    "local function removeMembers(members) ",
    "local from = 1; ",
    "while from <= #members do ",
    "local to = math.min(from + 499, #members); ",
    "redis.call('zrem', KEYS[1], unpack(members, from, to)); ",
    "redis.call('zrem', KEYS[2], unpack(members, from, to)); ",
    "from = to + 1; ",
    "end; ",
    "end; ",
);

/// RedissonTimeSeries.ENTRY_ARGS (line 398)
const ENTRY_ARGS: &str = concat!(
    "local timestamp = ARGV[4]; ",
    "local labelFlag = ARGV[5]; ",
    "local entryValue = ARGV[6]; ",
    "local label = ARGV[7]; ",
    "local expiration = ARGV[8]; ",
);

/// RedissonTimeSeries.STORE_ENTRY (line 412)
const STORE_ENTRY: &str = concat!(
    "local function storeEntry(id, timestamp, value) ",
    "local field = string.char(labelFlag) .. label; ",
    "local packed = struct.pack('BBc0Lc0Lc0', 4, ",
    "string.len(id), id, ",
    "string.len(value), value, ",
    "string.len(field), field); ",
    "redis.call('zadd', KEYS[1], timestamp, packed); ",
    "redis.call('zadd', KEYS[2], expiration, packed); ",
    "end; ",
);

/// RedissonTimeSeries.RETENTION_CUTOFF (line 433)
const RETENTION_CUTOFF: &str = concat!(
    "local cutoff; ",
    "local retention = tonumber(ARGV[2]); ",
    "if retention > 0 then ",
    "local highest = tonumber(timestamp); ",
    "local probes = 0; ",
    "while probes < 100 do ",
    "local top = redis.call('zrevrange', KEYS[1], 0, 0, 'withscores'); ",
    "if #top == 0 then ",
    "break; ",
    "end; ",
    "local expirationDate = redis.call('zscore', KEYS[2], top[1]); ",
    "if expirationDate == false or tonumber(expirationDate) > tonumber(ARGV[3]) then ",
    "local top1 = tonumber(top[2]); ",
    "if top1 > highest then ",
    "highest = top1; ",
    "end; ",
    "break; ",
    "end; ",
    "redis.call('zrem', KEYS[1], top[1]); ",
    "redis.call('zrem', KEYS[2], top[1]); ",
    "probes = probes + 1; ",
    "end; ",
    "cutoff = highest - retention; ",
    "end; ",
);

/// RedissonTimeSeries.WITHIN_RETENTION (line 463)
const WITHIN_RETENTION: &str = concat!("if cutoff == nil or tonumber(timestamp) >= cutoff then ",);

/// RedissonTimeSeries.RETENTION_TRIM (line 490)
const RETENTION_TRIM: &str = concat!(
    "if cutoff ~= nil then ",
    "removeMembers(redis.call('zrangebyscore', KEYS[1], '-inf', ",
    "'(' .. string.format('%.0f', cutoff), 'limit', 0, 500)); ",
    "end; ",
);

/// RedissonTimeSeries.ADD_ENTRY (line 510)
pub(super) static ADD_ENTRY: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            NEXT_ID,
            REMOVE_MEMBERS,
            ENTRY_ARGS,
            STORE_ENTRY,
            RETENTION_CUTOFF,
            concat!("local added = 0; ",),
            WITHIN_RETENTION,
            concat!(
                "storeEntry(nextId(), timestamp, entryValue); ",
                "added = 1; ",
                "end; ",
            ),
            RETENTION_TRIM,
            concat!("return added;",),
        ]
        .concat(),
    )
});

/// RedissonTimeSeries.ADD_IF_ABSENT (line 520)
pub(super) static ADD_IF_ABSENT: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            NEXT_ID,
            REMOVE_MEMBERS,
            ENTRY_ARGS,
            STORE_ENTRY,
            RETENTION_CUTOFF,
            concat!("local now = tonumber(ARGV[3]); ", "local added = 0; ",),
            WITHIN_RETENTION,
            concat!(
                "local occupied = false; ",
                "local stale; ",
                "local existing = redis.call('zrangebyscore', KEYS[1], timestamp, timestamp); ",
                "for j = 1, #existing do ",
                "local expirationDate = redis.call('zscore', KEYS[2], existing[j]); ",
                "if expirationDate == false or tonumber(expirationDate) > now then ",
                "occupied = true; ",
                "else ",
                "if stale == nil then ",
                "stale = {}; ",
                "end; ",
                "table.insert(stale, existing[j]); ",
                "end; ",
                "end; ",
                "if stale ~= nil then ",
                "removeMembers(stale); ",
                "end; ",
                "if occupied == false then ",
                "storeEntry(nextId(), timestamp, entryValue); ",
                "added = 1; ",
                "end; ",
                "end; ",
            ),
            RETENTION_TRIM,
            concat!("return added;",),
        ]
        .concat(),
    )
});

/// RedissonTimeSeries.ADD_OR_REPLACE (line 551)
pub(super) static ADD_OR_REPLACE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            NEXT_ID,
            REMOVE_MEMBERS,
            ENTRY_ARGS,
            STORE_ENTRY,
            RETENTION_CUTOFF,
            concat!("local now = tonumber(ARGV[3]); ", "local created = 0; ",),
            WITHIN_RETENTION,
            concat!(
                "local replaced = false; ",
                "local existing = redis.call('zrangebyscore', KEYS[1], timestamp, timestamp); ",
                "for j = 1, #existing do ",
                "local expirationDate = redis.call('zscore', KEYS[2], existing[j]); ",
                "if expirationDate == false or tonumber(expirationDate) > now then ",
                "replaced = true; ",
                "end; ",
                "end; ",
                "if #existing > 0 then ",
                "removeMembers(existing); ",
                "end; ",
                "storeEntry(nextId(), timestamp, entryValue); ",
                "if replaced == false then ",
                "created = 1; ",
                "end; ",
                "end; ",
            ),
            RETENTION_TRIM,
            concat!("return created;",),
        ]
        .concat(),
    )
});

/// RedissonTimeSeries.sizeAsync (line 1165)
pub(super) static SIZE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local expired = redis.call('zcount', KEYS[2], '-inf', ARGV[1]);",
        "return redis.call('zcard', KEYS[1]) - expired;",
    ))
});

/// RedissonTimeSeries.getAsync (line 1179)
pub(super) static GET: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local values = redis.call('zrangebyscore', KEYS[1], ARGV[2], ARGV[2]);",
        "for i = 1, #values do ",
        "local expirationDate = redis.call('zscore', KEYS[2], values[i]); ",
        "if expirationDate == false or tonumber(expirationDate) > tonumber(ARGV[1]) then ",
        "local n, t, val, label = struct.unpack('BBc0Lc0Lc0', values[i]); ",
        "return val;",
        "end;",
        "end;",
        "return nil;",
    ))
});

/// RedissonTimeSeries.DECODE_LABEL (line 306)
const DECODE_LABEL: &str = concat!(
    "if n == 4 then ",
    "local flag = string.byte(label, 1); ",
    "if flag == 2 then ",
    "label = 0; ",
    "else ",
    "label = string.sub(label, 2); ",
    "end; ",
    "n = flag; ",
    "elseif n == 2 then ",
    "label = 0; ",
    "end; ",
);

/// RedissonTimeSeries.getEntryAsync (line 1200)
pub(super) static GET_ENTRY: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            concat!(
                "local values = redis.call('zrangebyscore', KEYS[1], ARGV[2], ARGV[2]);",
                "for i = 1, #values do ",
                "local expirationDate = redis.call('zscore', KEYS[2], values[i]); ",
                "if expirationDate == false or tonumber(expirationDate) > tonumber(ARGV[1]) then ",
                "local n, t, val, label = struct.unpack('BBc0Lc0Lc0', values[i]); ",
            ),
            DECODE_LABEL,
            concat!(
                "if n == 2 then ",
                "return {n, ARGV[2], val};",
                "end;",
                "return {n, ARGV[2], val, label};",
                "end;",
                "end;",
                "return nil;",
            ),
        ]
        .concat(),
    )
});

/// Redisson's TimeSeriesEvictionTask.execute
pub(super) static EVICT: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local expiredKeys = redis.call('zrangebyscore', KEYS[2], 0, ARGV[1], 'limit', 0, ARGV[2]); ",
        "if #expiredKeys > 0 then ",
        "redis.call('zrem', KEYS[2], unpack(expiredKeys)); ",
        "redis.call('zrem', KEYS[1], unpack(expiredKeys)); ",
        "end; ",
        "if redis.call('zcard', KEYS[1]) == 0 then ",
        "redis.call('del', KEYS[3]); ",
        "end; ",
        "return #expiredKeys;",
    ))
});
#[cfg(test)]
mod tests {
    use super::*;

    // The SHA-1 of ADD_ENTRY as Redisson 4.8.0 concatenates it (`NEXT_ID + REMOVE_MEMBERS + ENTRY_ARGS + STORE_ENTRY + RETENTION_CUTOFF + …`), computed from RedissonTimeSeries.java.
    const REDISSON_ADD_ENTRY_SHA1: &str = "660beca6c96fcff1712d9516922cf8d9f296f24e";

    #[test]
    fn add_entry_is_redissons_concatenation() {
        assert!(ADD_ENTRY
            .source()
            .starts_with("local sequenceWidth = tonumber(ARGV[1]); "));
        assert!(ADD_ENTRY.source().ends_with("return added;"));
        assert_eq!(ADD_ENTRY.source().len(), 2064);
        assert_eq!(ADD_ENTRY.sha1(), REDISSON_ADD_ENTRY_SHA1);
    }

    #[test]
    fn the_eviction_script_is_redissons() {
        assert!(EVICT
            .source()
            .starts_with("local expiredKeys = redis.call('zrangebyscore', KEYS[2], 0, ARGV[1]"));
        assert!(EVICT.source().ends_with("return #expiredKeys;"));
    }
}
