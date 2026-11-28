//! The Lua scripts of [`ReliableQueue`](crate::ReliableQueue). Redisson's `RReliableQueue` is a PRO feature without published scripts, so this backend is redissun's own (see the spec's "Data" table).
//!
//! Every script takes the same KEYS, all in the slot of `{name}`:
//! 1 ready (sorted set, score `(9 - priority) * 2^44 + seq`), 2 delayed (id → ready at), 3 unacked (id → visibility deadline), 4 expiry (id → expire at), 5 payloads (hash), 6 meta (hash, `<id>:p|d|l|k|h`), 7 dedup (hash, key → id), 8 dedup expiry (sorted set), 9 config (hash), 10 disabled operations (set), 11 id counter, 12 dead letters waiting to move (list).
//! ARGV[1] is the wake channel and ARGV[2] the event channel; the script's own arguments follow. Times are the server's (`TIME`).

use crate::lua::Lua;
use std::sync::LazyLock;

/// The shared prelude: the clock, the helpers and the housekeeping, which runs in bounded steps (100 ids of each kind).
const PRELUDE: &str = r#"
local wake, events = ARGV[1], ARGV[2]
local t = redis.call('time')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local function cfg(field) return redis.call('hget', KEYS[9], field) end
local function num(value, default) if value then return tonumber(value) end return default end
local function score(id, priority)
  local seq = tonumber(string.match(id, '%-(%d+)$'))
  return (9 - priority) * 17592186044416 + seq
end
local function priority(id) return num(redis.call('hget', KEYS[6], id .. ':p'), 4) end
local function limit(id) return num(redis.call('hget', KEYS[6], id .. ':l'), num(cfg('limit'), 10)) end
local function event(kind, arg, ids)
  if #ids > 0 or kind == 'full' or kind == 'config' or kind == 'disabled' or kind == 'enabled' then
    redis.call('publish', events, kind .. '\n' .. arg .. '\n' .. table.concat(ids, ' '))
  end
end
local function forget(id)
  redis.call('zrem', KEYS[1], id)
  redis.call('zrem', KEYS[2], id)
  redis.call('zrem', KEYS[3], id)
  redis.call('zrem', KEYS[4], id)
  redis.call('hdel', KEYS[5], id)
  local key = redis.call('hget', KEYS[6], id .. ':k')
  if key and redis.call('hget', KEYS[7], key) == id then
    redis.call('hdel', KEYS[7], key)
    redis.call('zrem', KEYS[8], key)
  end
  redis.call('hdel', KEYS[6], id .. ':p', id .. ':d', id .. ':l', id .. ':k', id .. ':h')
end
local function dead(id)
  redis.call('zrem', KEYS[1], id)
  redis.call('zrem', KEYS[2], id)
  redis.call('zrem', KEYS[3], id)
  if cfg('dlq') then
    redis.call('rpush', KEYS[12], id)
  else
    forget(id)
  end
end
local woke = false
local function ready(id)
  redis.call('zadd', KEYS[1], score(id, priority(id)), id)
  woke = true
end
local function housekeep()
  for _, id in ipairs(redis.call('zrangebyscore', KEYS[2], '-inf', now, 'limit', 0, 100)) do
    redis.call('zrem', KEYS[2], id)
    ready(id)
  end
  for _, id in ipairs(redis.call('zrangebyscore', KEYS[3], '-inf', now, 'limit', 0, 100)) do
    redis.call('zrem', KEYS[3], id)
    local deliveries = num(redis.call('hget', KEYS[6], id .. ':d'), 0)
    local max = limit(id)
    if max > 0 and deliveries >= max then dead(id) else ready(id) end
  end
  for _, id in ipairs(redis.call('zrangebyscore', KEYS[4], '-inf', now, 'limit', 0, 100)) do
    forget(id)
  end
  for _, key in ipairs(redis.call('zrangebyscore', KEYS[8], '-inf', now, 'limit', 0, 100)) do
    redis.call('hdel', KEYS[7], key)
    redis.call('zrem', KEYS[8], key)
  end
end
local function finish()
  if woke then redis.call('publish', wake, '1') end
end
local function next_due()
  local due = -1
  for _, k in ipairs({KEYS[2], KEYS[3]}) do
    local first = redis.call('zrange', k, 0, 0, 'withscores')
    if #first > 0 then
      local at = tonumber(first[2])
      if due < 0 or at < due then due = at end
    end
  end
  if due < 0 then return -1 end
  return math.max(0, due - now)
end
local function size() return redis.call('zcard', KEYS[1]) + redis.call('zcard', KEYS[2]) + redis.call('zcard', KEYS[3]) end
"#;

/// Adds messages. ARGV[3] the number of messages, then per message: priority, delay ms (`-1` = the queue's), ttl ms (`-1` = the queue's), delivery limit (`0` = the queue's), dedup mode (`0` none, `1` by hash, `2` by id), dedup id, dedup ms, payload, headers.
/// Replies `{'disabled'}`, `{'full'}`, `{'size', max}` for a payload over `max-size-bytes`, or `{'ok', id or '', …}` with `''` for a deduplicated message.
pub(crate) static ADD: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            PRELUDE,
            r#"
if redis.call('sismember', KEYS[10], 'add') == 1 then return {'disabled'} end
housekeep()
local n = tonumber(ARGV[3])
local max = num(cfg('max-size'), 0)
if max > 0 and size() + n > max then
  finish()
  event('full', '', {})
  return {'full'}
end
local bytes = num(cfg('max-message-size'), 0)
local result = {'ok'}
local added = {}
for i = 0, n - 1 do
  local b = 4 + i * 9
  local payload = ARGV[b + 7]
  if bytes > 0 and #payload > bytes then return {'size', tostring(bytes)} end
end
for i = 0, n - 1 do
  local b = 4 + i * 9
  local p, delay, ttl, lim = tonumber(ARGV[b]), tonumber(ARGV[b + 1]), tonumber(ARGV[b + 2]), tonumber(ARGV[b + 3])
  local mode, dedup_id, dedup_ms = ARGV[b + 4], ARGV[b + 5], tonumber(ARGV[b + 6])
  local payload, headers = ARGV[b + 7], ARGV[b + 8]
  local key = false
  if mode == '1' then key = 'h:' .. redis.sha1hex(payload) elseif mode == '2' then key = 'i:' .. dedup_id end
  local existing = key and redis.call('hget', KEYS[7], key)
  if existing and redis.call('hexists', KEYS[5], existing) == 1 then
    table.insert(result, '')
  else
    local id = tostring(now) .. '-' .. tostring(redis.call('incr', KEYS[11]))
    redis.call('hset', KEYS[5], id, payload)
    redis.call('hset', KEYS[6], id .. ':p', p, id .. ':d', 0)
    if lim > 0 then redis.call('hset', KEYS[6], id .. ':l', lim) end
    if #headers > 0 then redis.call('hset', KEYS[6], id .. ':h', headers) end
    if key then
      redis.call('hset', KEYS[6], id .. ':k', key)
      redis.call('hset', KEYS[7], key, id)
      redis.call('zadd', KEYS[8], now + dedup_ms, key)
    end
    if delay < 0 then delay = num(cfg('delay'), 0) end
    if ttl < 0 then ttl = num(cfg('ttl'), 0) end
    if ttl > 0 then redis.call('zadd', KEYS[4], now + ttl, id) end
    if delay > 0 then
      redis.call('zadd', KEYS[2], now + delay, id)
    else
      redis.call('zadd', KEYS[1], score(id, p), id)
      woke = true
    end
    table.insert(result, id)
    table.insert(added, id)
  end
end
finish()
event('added', '', added)
return result
"#,
        ]
        .concat(),
    )
});

/// Polls. ARGV[3] count, ARGV[4] visibility ms (`0` = the queue's), ARGV[5] `1` for automatic acknowledgement.
/// Replies `{'disabled'}`, or `{'ok', ms until the next delayed or unacked deadline or -1, dead letters waiting, (id, payload, headers, deliveries)…}`.
pub(crate) static POLL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            PRELUDE,
            r#"
if redis.call('sismember', KEYS[10], 'poll') == 1 then return {'disabled'} end
housekeep()
local count = tonumber(ARGV[3])
local visibility = tonumber(ARGV[4])
if visibility <= 0 then visibility = num(cfg('visibility'), 30000) end
local auto = ARGV[5] == '1'
local result = {'ok', '0', tostring(redis.call('llen', KEYS[12]))}
if cfg('mode') == 'sequential' then
  if redis.call('zcard', KEYS[3]) > 0 then
    result[2] = tostring(next_due())
    finish()
    return result
  end
  count = 1
end
local polled = {}
for _, id in ipairs(redis.call('zrange', KEYS[1], 0, count - 1)) do
  redis.call('zrem', KEYS[1], id)
  local deliveries = redis.call('hincrby', KEYS[6], id .. ':d', 1)
  table.insert(result, id)
  table.insert(result, redis.call('hget', KEYS[5], id) or '')
  table.insert(result, redis.call('hget', KEYS[6], id .. ':h') or '')
  table.insert(result, tostring(deliveries))
  table.insert(polled, id)
  if auto then forget(id) else redis.call('zadd', KEYS[3], now + visibility, id) end
end
result[2] = tostring(next_due())
finish()
event('polled', '', polled)
return result
"#,
        ]
        .concat(),
    )
});

/// Acknowledges. ARGV[3…] the ids. Replies how many were unacknowledged.
pub(crate) static ACK: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            PRELUDE,
            r#"
local done = {}
for i = 3, #ARGV do
  local id = ARGV[i]
  if redis.call('zscore', KEYS[3], id) then
    forget(id)
    table.insert(done, id)
  end
end
event('acknowledged', '', done)
return #done
"#,
        ]
        .concat(),
    )
});

/// Negatively acknowledges. ARGV[3] `failed` or `rejected`, ARGV[4] delay ms for a failed message, ARGV[5…] the ids. A failed message goes back (after the delay) unless its delivery limit is reached; a rejected one goes to the dead letters (or is deleted without a dead-letter queue). Replies `{count, dead letters waiting}`.
pub(crate) static NACK: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            PRELUDE,
            r#"
local status, delay = ARGV[3], tonumber(ARGV[4])
local done = {}
for i = 5, #ARGV do
  local id = ARGV[i]
  if redis.call('zscore', KEYS[3], id) then
    redis.call('zrem', KEYS[3], id)
    local deliveries = num(redis.call('hget', KEYS[6], id .. ':d'), 0)
    local max = limit(id)
    if status == 'rejected' or (max > 0 and deliveries >= max) then
      dead(id)
    elseif delay > 0 then
      redis.call('zadd', KEYS[2], now + delay, id)
    else
      ready(id)
    end
    table.insert(done, id)
  end
end
finish()
event('nacked', status, done)
return {#done, redis.call('llen', KEYS[12])}
"#,
        ]
        .concat(),
    )
});

/// Reads messages without changing them. ARGV[3…] the ids. Replies per id `(id, payload, headers, deliveries)`, `''` payload for a missing id.
pub(crate) static GET: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            PRELUDE,
            r#"
housekeep()
local result = {}
for i = 3, #ARGV do
  local id = ARGV[i]
  local payload = redis.call('hget', KEYS[5], id)
  table.insert(result, id)
  if payload then
    table.insert(result, payload)
    table.insert(result, redis.call('hget', KEYS[6], id .. ':h') or '')
    table.insert(result, redis.call('hget', KEYS[6], id .. ':d') or '0')
  else
    table.insert(result, false)
    table.insert(result, '')
    table.insert(result, '0')
  end
end
finish()
return result
"#,
        ]
        .concat(),
    )
});

/// Removes messages in any state. ARGV[3…] the ids. Replies how many existed.
pub(crate) static REMOVE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            PRELUDE,
            r#"
local done = {}
for i = 3, #ARGV do
  local id = ARGV[i]
  if redis.call('hexists', KEYS[5], id) == 1 then
    forget(id)
    table.insert(done, id)
  end
end
event('removed', '', done)
return #done
"#,
        ]
        .concat(),
    )
});

/// The counts after housekeeping: `{ready, delayed, unacked}`.
pub(crate) static COUNTS: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            PRELUDE,
            r#"
housekeep()
finish()
return {redis.call('zcard', KEYS[1]), redis.call('zcard', KEYS[2]), redis.call('zcard', KEYS[3])}
"#,
        ]
        .concat(),
    )
});

/// Writes the configuration. ARGV[3] `1` only when absent, then field/value pairs (an empty value deletes the field). Replies `1` when written.
pub(crate) static SET_CONFIG: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            PRELUDE,
            r#"
if ARGV[3] == '1' and redis.call('exists', KEYS[9]) == 1 then return 0 end
redis.call('del', KEYS[9])
redis.call('hset', KEYS[9], 'set', '1')
for i = 4, #ARGV, 2 do
  if ARGV[i + 1] ~= '' then redis.call('hset', KEYS[9], ARGV[i], ARGV[i + 1]) end
end
event('config', '', {})
return 1
"#,
        ]
        .concat(),
    )
});

/// Disables (`ARGV[3] == '1'`) or enables the operation ARGV[4].
pub(crate) static OPERATION: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            PRELUDE,
            r#"
if ARGV[3] == '1' then
  redis.call('sadd', KEYS[10], ARGV[4])
  event('disabled', ARGV[4], {})
else
  redis.call('srem', KEYS[10], ARGV[4])
  event('enabled', ARGV[4], {})
end
return 1
"#,
        ]
        .concat(),
    )
});

/// Deletes every message, keeping the configuration and the disabled operations. Replies `1` when there was anything.
pub(crate) static CLEAR: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            PRELUDE,
            r#"
local any = redis.call('hlen', KEYS[5]) > 0
local removed = redis.call('hkeys', KEYS[5])
redis.call('del', KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[7], KEYS[8], KEYS[12])
event('removed', '', removed)
if any then return 1 end
return 0
"#,
        ]
        .concat(),
    )
});

/// Takes up to 100 dead letters waiting to move: `{(id, payload, headers, priority)…}`; they stay until [`DEAD_DONE`] confirms the move.
pub(crate) static DEAD_PEEK: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            PRELUDE,
            r#"
local result = {}
for _, id in ipairs(redis.call('lrange', KEYS[12], 0, 99)) do
  local payload = redis.call('hget', KEYS[5], id)
  if payload then
    table.insert(result, id)
    table.insert(result, payload)
    table.insert(result, redis.call('hget', KEYS[6], id .. ':h') or '')
    table.insert(result, tostring(priority(id)))
  else
    redis.call('lrem', KEYS[12], 0, id)
  end
end
return result
"#,
        ]
        .concat(),
    )
});

/// Confirms moved dead letters ARGV[3…]: deleted here.
pub(crate) static DEAD_DONE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            PRELUDE,
            r#"
for i = 3, #ARGV do
  redis.call('lrem', KEYS[12], 0, ARGV[i])
  forget(ARGV[i])
end
return 1
"#,
        ]
        .concat(),
    )
});

/// Adds messages moved from another queue with their ids kept. ARGV[3…] groups of (id, payload, headers, priority). Replies how many it added (an id already here is skipped).
pub(crate) static RECEIVE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            PRELUDE,
            r#"
local added = {}
for i = 3, #ARGV, 4 do
  local id, payload, headers, p = ARGV[i], ARGV[i + 1], ARGV[i + 2], tonumber(ARGV[i + 3])
  if redis.call('hexists', KEYS[5], id) == 0 then
    redis.call('hset', KEYS[5], id, payload)
    redis.call('hset', KEYS[6], id .. ':p', p, id .. ':d', 0)
    if #headers > 0 then redis.call('hset', KEYS[6], id .. ':h', headers) end
    redis.call('zadd', KEYS[1], score(id, p), id)
    woke = true
    table.insert(added, id)
  end
end
finish()
event('added', '', added)
return #added
"#,
        ]
        .concat(),
    )
});

/// A page of ids: ARGV[3] the cursor; replies `{next cursor, ids…}` from `HSCAN` of the payloads.
pub(crate) static IDS: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            PRELUDE,
            r#"
housekeep()
finish()
local page = redis.call('hscan', KEYS[5], ARGV[3], 'count', 100)
local result = {page[1]}
for i = 1, #page[2], 2 do table.insert(result, page[2][i]) end
return result
"#,
        ]
        .concat(),
    )
});

/// The ready ids, at most ARGV[3], in delivery order: for `move_to(.., Count(n))`.
pub(crate) static FIRST_READY: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        [
            PRELUDE,
            r#"
housekeep()
finish()
return redis.call('zrange', KEYS[1], 0, tonumber(ARGV[3]) - 1)
"#,
        ]
        .concat(),
    )
});
