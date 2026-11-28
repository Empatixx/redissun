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
const WITHIN_RETENTION: &str = "if cutoff == nil or tonumber(timestamp) >= cutoff then ";

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
            "local added = 0; ",
            WITHIN_RETENTION,
            concat!(
                "storeEntry(nextId(), timestamp, entryValue); ",
                "added = 1; ",
                "end; ",
            ),
            RETENTION_TRIM,
            "return added;",
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
            "return added;",
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
            "return created;",
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

/// RedissonTimeSeries.LIVE_AT_TIMESTAMP (line 1224)
const LIVE_AT_TIMESTAMP: &str = concat!(
    "local result = {}; ",
    "local doomed = {}; ",
    "local values = redis.call('zrangebyscore', KEYS[1], ARGV[2], ARGV[2]); ",
    "for i = 1, #values do ",
    "local expirationDate = redis.call('zscore', KEYS[2], values[i]); ",
    "if expirationDate == false or tonumber(expirationDate) > tonumber(ARGV[1]) then ",
);

/// RedissonTimeSeries.getAllAsync (line 1240)
pub(super) static GET_ALL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            LIVE_AT_TIMESTAMP,
            concat!(
                "local n, t, val, label = struct.unpack('BBc0Lc0Lc0', values[i]); ",
                "table.insert(result, val); ",
                "end; ",
                "end; ",
                "return result;",
            ),
        ]
        .concat(),
    )
});

/// RedissonTimeSeries.getAllEntriesAsync (line 1258)
pub(super) static GET_ALL_ENTRIES: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            LIVE_AT_TIMESTAMP,
            "local n, t, val, label = struct.unpack('BBc0Lc0Lc0', values[i]); ",
            DECODE_LABEL,
            concat!(
                "table.insert(result, val); ",
                "table.insert(result, label); ",
                "table.insert(result, n); ",
                "table.insert(result, ARGV[2]); ",
                "end; ",
                "end; ",
                "return result;",
            ),
        ]
        .concat(),
    )
});

/// RedissonTimeSeries.getAndRemoveAsync (line 1369)
pub(super) static GET_AND_REMOVE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local values = redis.call('zrangebyscore', KEYS[1], ARGV[2], ARGV[2]);",
        "for i = 1, #values do ",
        "local expirationDate = redis.call('zscore', KEYS[2], values[i]); ",
        "if expirationDate == false or tonumber(expirationDate) > tonumber(ARGV[1]) then ",
        "redis.call('zrem', KEYS[2], values[i]); ",
        "redis.call('zrem', KEYS[1], values[i]); ",
        "local n, t, val, label = struct.unpack('BBc0Lc0Lc0', values[i]); ",
        "return val;",
        "end;",
        "end;",
        "return nil;",
    ))
});

/// RedissonTimeSeries.getAndRemoveEntryAsync (line 1392)
pub(super) static GET_AND_REMOVE_ENTRY: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            concat!(
                "local values = redis.call('zrangebyscore', KEYS[1], ARGV[2], ARGV[2]);",
                "for i = 1, #values do ",
                "local expirationDate = redis.call('zscore', KEYS[2], values[i]); ",
                "if expirationDate == false or tonumber(expirationDate) > tonumber(ARGV[1]) then ",
                "redis.call('zrem', KEYS[2], values[i]); ",
                "redis.call('zrem', KEYS[1], values[i]); ",
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

/// RedissonTimeSeries.getAndRemoveAllAsync (line 1303)
pub(super) static GET_AND_REMOVE_ALL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            REMOVE_MEMBERS,
            LIVE_AT_TIMESTAMP,
            concat!(
                "local n, t, val, label = struct.unpack('BBc0Lc0Lc0', values[i]); ",
                "table.insert(result, val); ",
                "table.insert(doomed, values[i]); ",
                "end; ",
                "end; ",
                "removeMembers(doomed); ",
                "return result;",
            ),
        ]
        .concat(),
    )
});

/// RedissonTimeSeries.getAndRemoveAllEntriesAsync (line 1323)
pub(super) static GET_AND_REMOVE_ALL_ENTRIES: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            REMOVE_MEMBERS,
            LIVE_AT_TIMESTAMP,
            "local n, t, val, label = struct.unpack('BBc0Lc0Lc0', values[i]); ",
            DECODE_LABEL,
            concat!(
                "table.insert(result, val); ",
                "table.insert(result, label); ",
                "table.insert(result, n); ",
                "table.insert(result, ARGV[2]); ",
                "table.insert(doomed, values[i]); ",
                "end; ",
                "end; ",
                "removeMembers(doomed); ",
                "return result;",
            ),
        ]
        .concat(),
    )
});

/// RedissonTimeSeries.FORMAT_TIMESTAMP (line 1539)
const FORMAT_TIMESTAMP: &str = concat!(
    "local function formatTimestamp(score) ",
    "if score >= 9223372036854775808 then ",
    "return '9223372036854775807'; ",
    "end; ",
    "if score <= -9223372036854775808 then ",
    "return '-9223372036854775808'; ",
    "end; ",
    "return string.format('%.0f', score); ",
    "end; ",
);

/// RedissonTimeSeries.KEEP_MEMBERS (line 1574)
static KEEP_MEMBERS: LazyLock<String> = LazyLock::new(|| {
    [
        FORMAT_TIMESTAMP,
        concat!(
            "local result = {}; ",
            "local members = {}; ",
            "local scores = {}; ",
            "local keepLimit = tonumber(ARGV[4]); ",
            "local wholeTimestamps = ARGV[8] == '1'; ",
            "local function keep(member, score) ",
            "if keepLimit > 0 and #members >= keepLimit then ",
            "if not wholeTimestamps then ",
            "return false; ",
            "end; ",
            "if score ~= scores[#members] then ",
            "return false; ",
            "end; ",
            "end; ",
            "table.insert(members, member); ",
            "table.insert(scores, score); ",
            "return true; ",
            "end; ",
        ),
    ]
    .concat()
});

/// RedissonTimeSeries.MATCHES_LABEL (line 339)
const MATCHES_LABEL: &str = concat!(
    "local matches = true; ",
    "if labelMode == '1' then ",
    "matches = label == labelValue; ",
    "elseif labelMode == '2' then ",
    "matches = label == 0; ",
    "end; ",
);

/// RedissonTimeSeries.COLLECT_RANGE (line 1598)
static COLLECT_RANGE: LazyLock<String> = LazyLock::new(|| {
    [
        concat!(
            "local labelMode = ARGV[6]; ",
            "local labelValue = ARGV[7]; ",
            "local function collect() ",
            "local limit = tonumber(ARGV[4]); ",
            "if limit < 0 then ",
            "return; ",
            "end; ",
            "local pageSize = limit; ",
            "if pageSize <= 0 or pageSize > 500 then ",
            "pageSize = 500; ",
            "elseif pageSize < 100 then ",
            "pageSize = 100; ",
            "end; ",
            "local anyExpired = redis.call('zcount', KEYS[2], '-inf', ARGV[1]) > 0; ",
            "local low = tonumber(ARGV[2]); ",
            "local high = tonumber(ARGV[3]); ",
            "local from, to = ARGV[2], ARGV[3]; ",
            "local seek, page, rank = 'zrangebyscore', 'zrange', 'zrank'; ",
            "if ARGV[5] ~= '0' then ",
            "from, to = ARGV[3], ARGV[2]; ",
            "seek, page, rank = 'zrevrangebyscore', 'zrevrange', 'zrevrank'; ",
            "end; ",
            "local head = redis.call(seek, KEYS[1], from, to, 'limit', 0, 1); ",
            "if #head == 0 then ",
            "return; ",
            "end; ",
            "local index = redis.call(rank, KEYS[1], head[1]); ",
            "while true do ",
            "local values = redis.call(page, KEYS[1], index, index + pageSize - 1, 'withscores'); ",
            "if #values == 0 then ",
            "return; ",
            "end; ",
            "for i = 1, #values, 2 do ",
            "local score = tonumber(values[i+1]); ",
            "if score < low or score > high then ",
            "return; ",
            "end; ",
            "local live = true; ",
            "if anyExpired then ",
            "local expirationDate = redis.call('zscore', KEYS[2], values[i]); ",
            "live = expirationDate == false ",
            "or tonumber(expirationDate) > tonumber(ARGV[1]); ",
            "end; ",
            "if live then ",
            "local label = 0; ",
            "if labelMode == '1' or labelMode == '2' then ",
            "local n, t, val; ",
            "n, t, val, label = struct.unpack('BBc0Lc0Lc0', values[i]); ",
        ),
        DECODE_LABEL,
        "end; ",
        MATCHES_LABEL,
        concat!(
            "if matches then ",
            "if not keep(values[i], values[i+1]) then ",
            "return; ",
            "end; ",
            "end; ",
            "end; ",
            "end; ",
            "index = index + #values/2; ",
            "end; ",
            "end; ",
            "collect(); ",
        ),
    ]
    .concat()
});

/// RedissonTimeSeries.UNPACK_VALUES (line 1737)
const UNPACK_VALUES: &str = concat!(
    "for i = 1, #members do ",
    "local n, t, val = struct.unpack('BBc0Lc0Lc0', members[i]); ",
    "table.insert(result, val); ",
    "end; ",
    "return result;",
);

/// RedissonTimeSeries.COLLECT_VALUES (line 1763)
pub(super) static COLLECT_VALUES: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua([KEEP_MEMBERS.as_str(), COLLECT_RANGE.as_str(), UNPACK_VALUES].concat())
});

/// RedissonTimeSeries.UNPACK_ENTRIES (line 1744)
static UNPACK_ENTRIES: LazyLock<String> = LazyLock::new(|| {
    [
        concat!(
            "for i = 1, #members do ",
            "local n, t, val, label = struct.unpack('BBc0Lc0Lc0', members[i]); ",
        ),
        DECODE_LABEL,
        concat!(
            "table.insert(result, val); ",
            "table.insert(result, label); ",
            "table.insert(result, n); ",
            "table.insert(result, formatTimestamp(tonumber(scores[i]))); ",
            "end; ",
            "return result;",
        ),
    ]
    .concat()
});

/// RedissonTimeSeries.COLLECT_ENTRIES (line 1764)
pub(super) static COLLECT_ENTRIES: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            KEEP_MEMBERS.as_str(),
            COLLECT_RANGE.as_str(),
            UNPACK_ENTRIES.as_str(),
        ]
        .concat(),
    )
});

/// RedissonTimeSeries.UNPACK_TIMESTAMPS (line 1755)
const UNPACK_TIMESTAMPS: &str = concat!(
    "for i = 1, #scores do ",
    "table.insert(result, formatTimestamp(tonumber(scores[i]))); ",
    "end; ",
    "return result;",
);

/// RedissonTimeSeries.COLLECT_TIMESTAMPS (line 1765)
pub(super) static COLLECT_TIMESTAMPS: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            KEEP_MEMBERS.as_str(),
            COLLECT_RANGE.as_str(),
            UNPACK_TIMESTAMPS,
        ]
        .concat(),
    )
});

/// RedissonTimeSeries.REMOVE_COLLECTED (line 1761)
const REMOVE_COLLECTED: &str = "removeMembers(members); ";

/// RedissonTimeSeries.POLL_VALUES (line 1766)
pub(super) static POLL_VALUES: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            REMOVE_MEMBERS,
            KEEP_MEMBERS.as_str(),
            COLLECT_RANGE.as_str(),
            REMOVE_COLLECTED,
            UNPACK_VALUES,
        ]
        .concat(),
    )
});

/// RedissonTimeSeries.POLL_ENTRIES (line 1768)
pub(super) static POLL_ENTRIES: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            REMOVE_MEMBERS,
            KEEP_MEMBERS.as_str(),
            COLLECT_RANGE.as_str(),
            REMOVE_COLLECTED,
            UNPACK_ENTRIES.as_str(),
        ]
        .concat(),
    )
});

/// RedissonTimeSeries.removeRangeAsync (line 1954)
pub(super) static REMOVE_RANGE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            concat!(
                "local labelMode = ARGV[4]; ",
                "local labelValue = ARGV[5]; ",
            ),
            REMOVE_MEMBERS,
            concat!(
                "local values = redis.call('zrangebyscore', KEYS[1], ARGV[2], ARGV[3]);",
                "local doomed = {}; ",
                "for i, v in ipairs(values) do ",
                "local expirationDate = redis.call('zscore', KEYS[2], v); ",
                "if expirationDate == false or tonumber(expirationDate) > tonumber(ARGV[1]) then ",
                "local label = 0; ",
                "if labelMode == '1' or labelMode == '2' then ",
                "local n, t, val; ",
                "n, t, val, label = struct.unpack('BBc0Lc0Lc0', v); ",
            ),
            DECODE_LABEL,
            "end; ",
            MATCHES_LABEL,
            concat!(
                "if matches then ",
                "table.insert(doomed, v); ",
                "end; ",
                "end;",
                "end;",
                "removeMembers(doomed); ",
                "return #doomed;",
            ),
        ]
        .concat(),
    )
});

/// RedissonTimeSeries.COLLECT_LABELS (line 2094)
pub(super) static COLLECT_LABELS: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            concat!(
        "local result = {}; ",
        "local seen = {}; ",
        "local low = tonumber(ARGV[2]); ",
        "local high = tonumber(ARGV[3]); ",
        "local head = redis.call('zrangebyscore', KEYS[1], ARGV[2], ARGV[3], 'limit', 0, 1); ",
        "if #head == 0 then ",
        "return result; ",
        "end; ",
        "local index = redis.call('zrank', KEYS[1], head[1]); ",
        "while true do ",
        "local values = redis.call('zrange', KEYS[1], index, index + 499, 'withscores'); ",
        "if #values == 0 then ",
        "return result; ",
        "end; ",
        "for i = 1, #values, 2 do ",
        "local score = tonumber(values[i+1]); ",
        "if score < low or score > high then ",
        "return result; ",
        "end; ",
        "local expirationDate = redis.call('zscore', KEYS[2], values[i]); ",
        "if expirationDate == false or tonumber(expirationDate) > tonumber(ARGV[1]) then ",
        "local n, t, val, label = struct.unpack('BBc0Lc0Lc0', values[i]); ",
    ),
            DECODE_LABEL,
            concat!(
                "if label ~= 0 and seen[label] == nil then ",
                "seen[label] = true; ",
                "table.insert(result, label); ",
                "end; ",
                "end; ",
                "end; ",
                "index = index + #values/2; ",
                "end;",
            ),
        ]
        .concat(),
    )
});

/// RedissonTimeSeries.scanIteratorAsync (line 2376)
pub(super) static SCAN: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(concat!(
        "local result = {}; ",
        "local res = redis.call('zrange', KEYS[1], ARGV[1], tonumber(ARGV[1]) + tonumber(ARGV[3]) - 1); ",
        "for i, value in ipairs(res) do ",
        "local expirationDate = redis.call('zscore', KEYS[2], value); ",
        "if expirationDate == false or tonumber(expirationDate) > tonumber(ARGV[2]) then ",
        "local n, t, val, label = struct.unpack('BBc0Lc0Lc0', value); ",
        "table.insert(result, val);",
        "end;",
        "end;",
        "local nextPos = tonumber(ARGV[1]) + tonumber(ARGV[3]); ",
        "if #res < tonumber(ARGV[3]) then ",
        "nextPos = 0;",
        "end;",
        "return {tostring(nextPos), result};",
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
