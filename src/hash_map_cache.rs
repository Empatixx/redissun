use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::eviction::{self, KEYS_LIMIT};
use crate::object::{millis, HasKey, Key};
use crate::pubsub::Subscription;
use bytes::Bytes;
use fred::interfaces::HashesInterface;
use fred::types::scripts::Script;
use futures::future::select_all;
use futures::{stream, Stream, TryStreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::sync::broadcast::{self, error::RecvError, error::TryRecvError};

const SCAN_PAGE: u32 = 100;

const BASE: &str = "local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local NEVER = 92233720368547758
local function unwrap(raw)
    local sep = string.find(raw, ':', 1, true)
    if sep then
        return string.sub(raw, sep + 1), tonumber(string.sub(raw, 1, sep - 1)) or 0
    end
    return raw, 0
end
local function message(kind, field, value, previous)
    return kind .. #field .. ':' .. #value .. ':' .. field .. value .. (previous or '')
end
";

const PRELUDE: &str =
    "local MAIN, TIMEOUT, IDLE, ACCESS, OPTIONS = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local CREATED, UPDATED, REMOVED = KEYS[6], KEYS[7], KEYS[8]
local maxSize = tonumber(redis.call('HGET', OPTIONS, 'max-size'))
local limited = maxSize ~= nil and maxSize ~= 0
local mode = redis.call('HGET', OPTIONS, 'mode')
local lru = mode == false or mode == 'LRU'
local hasListeners = redis.call('HGET', OPTIONS, 'has-listeners') ~= false
local function publish(channel, kind, field, value, previous)
    if hasListeners then
        redis.call('PUBLISH', channel, message(kind, field, value, previous))
    end
end
local function expiry(field, idle)
    local expires = NEVER
    local score = redis.call('ZSCORE', TIMEOUT, field)
    if score then
        expires = tonumber(score)
    end
    if idle ~= 0 then
        local idleScore = redis.call('ZSCORE', IDLE, field)
        if idleScore then
            expires = math.min(expires, tonumber(idleScore))
        end
    end
    return expires
end
local function refresh(field, idle)
    if idle ~= 0 then
        local idleScore = redis.call('ZSCORE', IDLE, field)
        if idleScore and tonumber(idleScore) > now then
            redis.call('ZADD', IDLE, idle + now, field)
        end
    end
end
local function touch(field)
    if limited then
        if lru then
            redis.call('ZADD', ACCESS, now, field)
        else
            redis.call('ZINCRBY', ACCESS, 1, field)
        end
    end
end
local function evict(field, last)
    for _, item in ipairs(redis.call('ZRANGE', ACCESS, 0, last)) do
        if item ~= field then
            local raw = redis.call('HGET', MAIN, item)
            redis.call('HDEL', MAIN, item)
            redis.call('ZREM', TIMEOUT, item)
            redis.call('ZREM', IDLE, item)
            redis.call('ZREM', ACCESS, item)
            if raw then
                publish(REMOVED, 'R', item, (unwrap(raw)))
            end
        end
    end
end
local function limits(field, ttl, idle)
    if ttl > 0 then
        redis.call('ZADD', TIMEOUT, now + ttl, field)
    else
        redis.call('ZREM', TIMEOUT, field)
    end
    if idle > 0 then
        redis.call('ZADD', IDLE, now + idle, field)
    else
        redis.call('ZREM', IDLE, field)
    end
end
local function makeRoom(field)
    if limited then
        if lru then
            redis.call('ZADD', ACCESS, now, field)
        end
        local size = redis.call('HLEN', MAIN)
        if size >= maxSize then
            evict(field, size - maxSize)
        end
        if not lru then
            redis.call('ZINCRBY', ACCESS, 1, field)
        end
    end
end
local function trim(field)
    if limited then
        if lru then
            redis.call('ZADD', ACCESS, now, field)
        end
        local size = redis.call('HLEN', MAIN)
        if size > maxSize then
            evict(field, size - maxSize - 1)
        end
        if not lru then
            redis.call('ZINCRBY', ACCESS, 1, field)
        end
    end
end
local field = ARGV[1]
";

fn script(body: &str) -> Script {
    Script::from_lua(format!("{BASE}{PRELUDE}{body}"))
}

static GET: LazyLock<Script> = LazyLock::new(|| {
    script(
        "local raw = redis.call('HGET', MAIN, field)
        if raw == false then
            return false
        end
        local value, idle = unwrap(raw)
        local expires = expiry(field, idle)
        refresh(field, idle)
        if expires <= now then
            return false
        end
        touch(field)
        return value",
    )
});

static CONTAINS: LazyLock<Script> = LazyLock::new(|| {
    script(
        "local raw = redis.call('HGET', MAIN, field)
        if raw == false then
            return 0
        end
        touch(field)
        local value, idle = unwrap(raw)
        local expires = expiry(field, idle)
        refresh(field, idle)
        if expires <= now then
            return 0
        end
        return 1",
    )
});

static PUT: LazyLock<Script> = LazyLock::new(|| {
    script(
        "local raw = redis.call('HGET', MAIN, field)
        local exists = false
        if raw then
            local _, idle = unwrap(raw)
            exists = expiry(field, idle) > now
        end
        redis.call('ZREM', TIMEOUT, field)
        redis.call('ZREM', IDLE, field)
        redis.call('HSET', MAIN, field, '0:' .. ARGV[2])
        if not exists then
            trim(field)
            publish(CREATED, 'C', field, ARGV[2])
            return false
        end
        touch(field)
        local previous = unwrap(raw)
        publish(UPDATED, 'U', field, ARGV[2], previous)
        return previous",
    )
});

static PUT_WITH_LIMITS: LazyLock<Script> = LazyLock::new(|| {
    script(
        "local ttl, idle = tonumber(ARGV[3]), tonumber(ARGV[4])
        local raw = redis.call('HGET', MAIN, field)
        local insertable = true
        if raw then
            local _, oldIdle = unwrap(raw)
            insertable = expiry(field, oldIdle) <= now
        end
        limits(field, ttl, idle)
        makeRoom(field)
        redis.call('HSET', MAIN, field, idle .. ':' .. ARGV[2])
        if insertable then
            publish(CREATED, 'C', field, ARGV[2])
            return false
        end
        local previous = unwrap(raw)
        publish(UPDATED, 'U', field, ARGV[2], previous)
        return previous",
    )
});

static PUT_IF_ABSENT: LazyLock<Script> = LazyLock::new(|| {
    script(
        "local raw = redis.call('HGET', MAIN, field)
        if raw == false then
            redis.call('HSET', MAIN, field, '0:' .. ARGV[2])
            publish(CREATED, 'C', field, ARGV[2])
            trim(field)
            return 1
        end
        touch(field)
        local _, idle = unwrap(raw)
        local expires = expiry(field, idle)
        refresh(field, idle)
        if expires > now then
            return 0
        end
        redis.call('ZREM', TIMEOUT, field)
        redis.call('ZREM', IDLE, field)
        redis.call('HSET', MAIN, field, '0:' .. ARGV[2])
        publish(CREATED, 'C', field, ARGV[2])
        return 1",
    )
});

static PUT_IF_ABSENT_WITH_LIMITS: LazyLock<Script> = LazyLock::new(|| {
    script(
        "local ttl, idle = tonumber(ARGV[3]), tonumber(ARGV[4])
        local raw = redis.call('HGET', MAIN, field)
        if raw then
            local _, oldIdle = unwrap(raw)
            if expiry(field, oldIdle) > now then
                return 0
            end
        end
        limits(field, ttl, idle)
        makeRoom(field)
        redis.call('HSET', MAIN, field, idle .. ':' .. ARGV[2])
        publish(CREATED, 'C', field, ARGV[2])
        return 1",
    )
});

static REMOVE: LazyLock<Script> = LazyLock::new(|| {
    script(
        "local raw = redis.call('HGET', MAIN, field)
        if raw == false then
            return false
        end
        local value, idle = unwrap(raw)
        if expiry(field, idle) <= now then
            return false
        end
        redis.call('ZREM', TIMEOUT, field)
        redis.call('ZREM', IDLE, field)
        redis.call('ZREM', ACCESS, field)
        redis.call('HDEL', MAIN, field)
        publish(REMOVED, 'R', field, value)
        return value",
    )
});

static ENTRY_TTL: LazyLock<Script> = LazyLock::new(|| {
    script(
        "local raw = redis.call('HGET', MAIN, field)
        if raw == false then
            return -2
        end
        local _, idle = unwrap(raw)
        local expires = expiry(field, idle)
        if expires == NEVER then
            return -1
        end
        if expires > now then
            return expires - now
        end
        return -2",
    )
});

static SCAN: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(format!(
        "{BASE}local res = redis.call('HSCAN', KEYS[1], ARGV[1], 'COUNT', ARGV[2])
        local result = {{}}
        for i = 1, #res[2], 2 do
            local field = res[2][i]
            local value, idle = unwrap(res[2][i + 1])
            local expires = NEVER
            local score = redis.call('ZSCORE', KEYS[2], field)
            if score then
                expires = tonumber(score)
            end
            if idle ~= 0 then
                local idleScore = redis.call('ZSCORE', KEYS[3], field)
                if idleScore then
                    if tonumber(idleScore) > now and expires > now then
                        redis.call('ZADD', KEYS[3], idle + now, field)
                    end
                    expires = math.min(expires, tonumber(idleScore))
                end
            end
            if expires > now then
                table.insert(result, field)
                table.insert(result, value)
            end
        end
        return {{res[1], result}}"
    ))
});

static EVICT: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(format!(
        "{BASE}if ARGV[3] == '1' and not redis.call('SET', KEYS[6], 1, 'NX', 'PX', ARGV[2]) then
            return -1
        end
        local function sweep(set)
            local expired = redis.call('ZRANGEBYSCORE', set, 0, now, 'LIMIT', 0, ARGV[1])
            for _, field in ipairs(expired) do
                local raw = redis.call('HGET', KEYS[1], field)
                if raw then
                    if redis.call('PUBLISH', KEYS[5], message('E', field, (unwrap(raw)))) == 0 then
                        break
                    end
                end
            end
            for i = 1, #expired, 5000 do
                local last = math.min(i + 4999, #expired)
                redis.call('ZREM', KEYS[4], unpack(expired, i, last))
                redis.call('ZREM', KEYS[3], unpack(expired, i, last))
                redis.call('ZREM', KEYS[2], unpack(expired, i, last))
                redis.call('HDEL', KEYS[1], unpack(expired, i, last))
            end
            return #expired
        end
        return sweep(KEYS[2]) + sweep(KEYS[3])"
    ))
});

static TRY_SET_MAX_SIZE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "redis.call('HSETNX', KEYS[1], 'max-size', ARGV[1])
        return redis.call('HSETNX', KEYS[1], 'mode', ARGV[2])",
    )
});

/// Which entry [`HashMapCache::set_max_size`] drops when the cache is full.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EvictionMode {
    /// The entry that was used longest ago.
    #[default]
    Lru,
    /// The entry that was used least often.
    Lfu,
}

impl EvictionMode {
    fn as_str(self) -> &'static str {
        match self {
            EvictionMode::Lru => "LRU",
            EvictionMode::Lfu => "LFU",
        }
    }
}

/// A change of a [`HashMapCache`], received through [`Events`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Event<K, V> {
    /// A new entry was added, or an expired one was written again.
    Created {
        /// The key of the entry.
        key: K,
        /// The new value.
        value: V,
    },
    /// The value of a live entry was replaced.
    Updated {
        /// The key of the entry.
        key: K,
        /// The new value.
        value: V,
        /// The value that was replaced.
        previous: V,
    },
    /// An entry was removed by [`HashMapCache::remove`] or dropped because the cache was full.
    Removed {
        /// The key of the entry.
        key: K,
        /// The value it had.
        value: V,
    },
    /// The background clean-up or [`HashMapCache::evict_expired`] deleted an expired entry.
    Expired {
        /// The key of the entry.
        key: K,
        /// The value it had.
        value: V,
    },
}

/// The kinds of [`Event`], each published on its own channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EventKind {
    /// [`Event::Created`].
    Created,
    /// [`Event::Updated`].
    Updated,
    /// [`Event::Removed`].
    Removed,
    /// [`Event::Expired`].
    Expired,
}

impl EventKind {
    const ALL: [EventKind; 4] = [
        EventKind::Removed,
        EventKind::Created,
        EventKind::Updated,
        EventKind::Expired,
    ];

    fn channel_prefix(self) -> &'static str {
        match self {
            EventKind::Created => "redissun_map_cache_created",
            EventKind::Updated => "redissun_map_cache_updated",
            EventKind::Removed => "redissun_map_cache_removed",
            EventKind::Expired => "redissun_map_cache_expired",
        }
    }
}

/// Receives the [`Event`]s of a [`HashMapCache`]. Dropping the last listener of a channel in a client unsubscribes from Redis.
pub struct Events<K, V, C: Codec> {
    sources: std::vec::Vec<(Subscription, broadcast::Receiver<Bytes>)>,
    codec: C,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V, C: Codec> fmt::Debug for Events<K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Events").finish_non_exhaustive()
    }
}

struct RawEvent<'a> {
    kind: u8,
    key: &'a [u8],
    value: &'a [u8],
    previous: &'a [u8],
}

fn split_payload(payload: &[u8]) -> Option<RawEvent<'_>> {
    let (&kind, rest) = payload.split_first()?;
    let first = rest.iter().position(|byte| *byte == b':')?;
    let key_length: usize = std::str::from_utf8(&rest[..first]).ok()?.parse().ok()?;
    let rest = &rest[first + 1..];
    let second = rest.iter().position(|byte| *byte == b':')?;
    let value_length: usize = std::str::from_utf8(&rest[..second]).ok()?.parse().ok()?;
    let rest = &rest[second + 1..];
    let key = rest.get(..key_length)?;
    let end = key_length.checked_add(value_length)?;
    let value = rest.get(key_length..end)?;
    let previous = rest.get(end..)?;
    Some(RawEvent {
        kind,
        key,
        value,
        previous,
    })
}

fn received(outcome: std::result::Result<Bytes, RecvError>) -> Result<Bytes> {
    match outcome {
        Ok(payload) => Ok(payload),
        Err(RecvError::Lagged(missed)) => Err(Error::Lagged(missed as usize)),
        Err(RecvError::Closed) => Err(Error::Redis("the subscription was closed".into())),
    }
}

impl<K, V, C> Events<K, V, C>
where
    K: DeserializeOwned + Send,
    V: DeserializeOwned + Send,
    C: Codec,
{
    fn parse(&self, payload: &[u8]) -> Result<Event<K, V>> {
        let malformed = || Error::Codec("malformed cache event".into());
        let raw = split_payload(payload).ok_or_else(malformed)?;
        let key = self.codec.decode(raw.key)?;
        let value = self.codec.decode(raw.value)?;
        match raw.kind {
            b'C' => Ok(Event::Created { key, value }),
            b'U' => Ok(Event::Updated {
                key,
                value,
                previous: self.codec.decode(raw.previous)?,
            }),
            b'R' => Ok(Event::Removed { key, value }),
            b'E' => Ok(Event::Expired { key, value }),
            _ => Err(malformed()),
        }
    }

    fn buffered(&mut self) -> Option<Result<Bytes>> {
        for (_, receiver) in self.sources.iter_mut() {
            match receiver.try_recv() {
                Ok(payload) => return Some(Ok(payload)),
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Lagged(missed)) => {
                    return Some(Err(Error::Lagged(missed as usize)))
                }
                Err(TryRecvError::Closed) => {
                    return Some(Err(Error::Redis("the subscription was closed".into())))
                }
            }
        }
        None
    }

    /// Waits for the next event. Events of one kind arrive in order; events of different kinds come from different channels, and when several are waiting, removals are returned first, then creations, updates and expirations. A listener that falls more than 256 events behind on a channel gets [`Error::Lagged`] once and then continues with the newest events.
    pub async fn recv(&mut self) -> Result<Event<K, V>> {
        let payload = match self.buffered() {
            Some(payload) => payload?,
            None => {
                let waiting = self
                    .sources
                    .iter_mut()
                    .map(|(_, receiver)| Box::pin(receiver.recv()));
                let (outcome, _, _) = select_all(waiting).await;
                received(outcome)?
            }
        };
        self.parse(&payload)
    }

    /// Turns the listener into a stream of events. The stream never ends.
    pub fn into_stream(self) -> impl Stream<Item = Result<Event<K, V>>> {
        stream::unfold(self, |mut events| async move {
            Some((events.recv().await, events))
        })
    }
}

/// A pending [`HashMapCache::insert`]. Await it, optionally after setting [`Insert::ttl`] or [`Insert::max_idle`].
#[must_use = "an insert does nothing until it is awaited"]
pub struct Insert<'a, K, V, C: Codec, Q: ?Sized, W: ?Sized> {
    cache: &'a HashMapCache<K, V, C>,
    k: &'a Q,
    v: &'a W,
    ttl: Option<Duration>,
    max_idle: Option<Duration>,
}

impl<'a, K, V, C: Codec, Q: ?Sized, W: ?Sized> Insert<'a, K, V, C, Q, W> {
    /// Makes the entry expire after `ttl`, whether it is read or not. Zero means no time to live.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Makes the entry expire when nobody has read it for `max_idle`. Every read starts the time again. Zero means no idle limit.
    pub fn max_idle(mut self, max_idle: Duration) -> Self {
        self.max_idle = Some(max_idle);
        self
    }
}

impl<'a, K, V, C, Q, W> IntoFuture for Insert<'a, K, V, C, Q, W>
where
    K: Serialize + DeserializeOwned + Send + Sync + Borrow<Q> + 'a,
    V: Serialize + DeserializeOwned + Send + Sync + Borrow<W> + 'a,
    C: Codec,
    Q: Serialize + ?Sized + Sync + 'a,
    W: Serialize + ?Sized + Sync + 'a,
{
    type Output = Result<Option<V>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let cache = self.cache;
            let (arguments, limited) =
                cache.insert_arguments(self.k, self.v, self.ttl, self.max_idle)?;
            let script = if limited { &PUT_WITH_LIMITS } else { &PUT };
            let previous: Option<Bytes> = cache
                .key
                .core
                .eval(script, cache.redis_keys(), arguments)
                .await?;
            cache.decode_value(previous)
        })
    }
}

/// A pending [`HashMapCache::insert_nx`]. Await it, optionally after setting [`InsertNx::ttl`] or [`InsertNx::max_idle`].
#[must_use = "an insert does nothing until it is awaited"]
pub struct InsertNx<'a, K, V, C: Codec, Q: ?Sized, W: ?Sized> {
    cache: &'a HashMapCache<K, V, C>,
    k: &'a Q,
    v: &'a W,
    ttl: Option<Duration>,
    max_idle: Option<Duration>,
}

impl<'a, K, V, C: Codec, Q: ?Sized, W: ?Sized> InsertNx<'a, K, V, C, Q, W> {
    /// Makes the entry expire after `ttl`, whether it is read or not. Zero means no time to live.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Makes the entry expire when nobody has read it for `max_idle`. Every read starts the time again. Zero means no idle limit.
    pub fn max_idle(mut self, max_idle: Duration) -> Self {
        self.max_idle = Some(max_idle);
        self
    }
}

impl<'a, K, V, C, Q, W> IntoFuture for InsertNx<'a, K, V, C, Q, W>
where
    K: Serialize + DeserializeOwned + Send + Sync + Borrow<Q> + 'a,
    V: Serialize + DeserializeOwned + Send + Sync + Borrow<W> + 'a,
    C: Codec,
    Q: Serialize + ?Sized + Sync + 'a,
    W: Serialize + ?Sized + Sync + 'a,
{
    type Output = Result<bool>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let cache = self.cache;
            let (arguments, limited) =
                cache.insert_arguments(self.k, self.v, self.ttl, self.max_idle)?;
            let script = if limited {
                &PUT_IF_ABSENT_WITH_LIMITS
            } else {
                &PUT_IF_ABSENT
            };
            let inserted: i64 = cache
                .key
                .core
                .eval(script, cache.redis_keys(), arguments)
                .await?;
            Ok(inserted == 1)
        })
    }
}

/// A distributed map where each entry can expire, modelled on Redisson's `RMapCache`.
///
/// An entry can have a time to live ([`Insert::ttl`]) and a maximum idle time ([`Insert::max_idle`]). Time is taken from the Redis server. Reads skip expired entries at once but leave them in Redis; a background task deletes them, as Redisson's eviction task does. It first runs [`ClientBuilder::eviction_interval`](crate::ClientBuilder::eviction_interval) after the cache is first used in this client, deletes up to 100 entries per run, and adapts its pause between that interval and 30 minutes. A short-lived lock in Redis lets only one client clean a cache per run. [`HashMapCache::evict_expired`] cleans at once.
///
/// [`HashMapCache::set_max_size`] limits the number of entries. The data lives in the hash `{name}`, the expiry times in the sorted sets `redissun__timeout__set:{name}` and `redissun__idle__set:{name}`, the use counts in `redissun__map_cache_last_access__set:{name}` and the settings in `redissun__map_cache_options:{name}`. The name gets a hash tag, so all keys are in one slot in Redis Cluster.
pub struct HashMapCache<K, V, C: Codec> {
    key: Key,
    codec: C,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V, C: Codec> Clone for HashMapCache<K, V, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
            _marker: PhantomData,
        }
    }
}

impl<K, V, C: Codec> fmt::Debug for HashMapCache<K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "HashMapCache")
    }
}

impl<K, V, C: Codec> HasKey for HashMapCache<K, V, C> {
    fn key(&self) -> &Key {
        &self.key
    }

    fn companions(&self) -> std::vec::Vec<String> {
        vec![
            self.timeout_key(),
            self.idle_key(),
            self.access_key(),
            self.options_key(),
        ]
    }
}

impl<K, V, C: Codec> HashMapCache<K, V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        let cache = Self {
            key,
            codec,
            _marker: PhantomData,
        };
        eviction::schedule(
            &cache.key.core,
            format!("redissun__map_cache_eviction:{}", cache.redis_key()),
            &EVICT,
            cache.eviction_keys(),
        );
        cache
    }

    fn timeout_key(&self) -> String {
        format!("redissun__timeout__set:{}", self.key.redis_key())
    }

    fn idle_key(&self) -> String {
        format!("redissun__idle__set:{}", self.key.redis_key())
    }

    fn access_key(&self) -> String {
        format!(
            "redissun__map_cache_last_access__set:{}",
            self.key.redis_key()
        )
    }

    fn options_key(&self) -> String {
        format!("redissun__map_cache_options:{}", self.key.redis_key())
    }

    fn channel(&self, kind: EventKind) -> String {
        format!("{}:{}", kind.channel_prefix(), self.key.redis_key())
    }

    fn redis_key(&self) -> String {
        self.key.redis_key()
    }

    fn redis_keys(&self) -> std::vec::Vec<String> {
        vec![
            self.key.redis_key(),
            self.timeout_key(),
            self.idle_key(),
            self.access_key(),
            self.options_key(),
            self.channel(EventKind::Created),
            self.channel(EventKind::Updated),
            self.channel(EventKind::Removed),
        ]
    }

    fn eviction_keys(&self) -> std::vec::Vec<String> {
        vec![
            self.key.redis_key(),
            self.timeout_key(),
            self.idle_key(),
            self.access_key(),
            self.channel(EventKind::Expired),
            eviction::latch_key(&self.key.redis_key()),
        ]
    }
}

impl<K, V, C> HashMapCache<K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    fn decode_value(&self, raw: Option<Bytes>) -> Result<Option<V>> {
        raw.map(|bytes| self.codec.decode(&bytes)).transpose()
    }

    fn insert_arguments<Q, W>(
        &self,
        k: &Q,
        v: &W,
        ttl: Option<Duration>,
        max_idle: Option<Duration>,
    ) -> Result<(std::vec::Vec<Bytes>, bool)>
    where
        Q: Serialize + ?Sized,
        W: Serialize + ?Sized,
    {
        let limit = |duration: Option<Duration>| match duration {
            Some(duration) if !duration.is_zero() => millis(duration),
            _ => Ok(0),
        };
        let ttl = limit(ttl)?;
        let max_idle = limit(max_idle)?;
        Ok((
            vec![
                self.codec.encode(k)?,
                self.codec.encode(v)?,
                Bytes::from(ttl.to_string()),
                Bytes::from(max_idle.to_string()),
            ],
            ttl > 0 || max_idle > 0,
        ))
    }

    /// Inserts an entry and returns the live value it replaced. Without [`Insert::ttl`] and [`Insert::max_idle`] the entry never expires, and a limit that the old entry had is removed.
    ///
    /// With a size limit, an insert without a time limit adds the entry and then drops the least used entries above the limit; an insert with a time limit first drops entries until there is room, even when it replaces a live entry, as Redisson does.
    ///
    /// ```ignore
    /// cache.insert("a", &value).await?;
    /// cache.insert("b", &value).ttl(Duration::from_secs(60)).await?;
    /// cache.insert("c", &value).max_idle(Duration::from_secs(30)).await?;
    /// ```
    pub fn insert<'a, Q, W>(&'a self, k: &'a Q, v: &'a W) -> Insert<'a, K, V, C, Q, W>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        Insert {
            cache: self,
            k,
            v,
            ttl: None,
            max_idle: None,
        }
    }

    /// Inserts the entry only when there is no live entry for the key, like Redisson's `fastPutIfAbsent`. Resolves to whether it was inserted. It takes `.ttl(..)` and `.max_idle(..)` too.
    pub fn insert_nx<'a, Q, W>(&'a self, k: &'a Q, v: &'a W) -> InsertNx<'a, K, V, C, Q, W>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        InsertNx {
            cache: self,
            k,
            v,
            ttl: None,
            max_idle: None,
        }
    }

    /// Limits the cache to `max` entries. When it is full, an insert drops the least recently (`Lru`) or least often (`Lfu`) used entry. `0` removes the limit. The limit is stored in Redis and applies to every client. Only entries written or read while a limit is set are tracked, so older entries are never dropped for space.
    pub async fn set_max_size(&self, max: usize, mode: EvictionMode) -> Result<()> {
        let _: i64 = self
            .key
            .core
            .redis()
            .hset(
                self.options_key(),
                vec![
                    ("max-size", max.to_string()),
                    ("mode", mode.as_str().to_string()),
                ],
            )
            .await?;
        Ok(())
    }

    /// Sets the size limit only when none was set before; returns whether it did.
    pub async fn try_set_max_size(&self, max: usize, mode: EvictionMode) -> Result<bool> {
        let set: i64 = self
            .key
            .core
            .eval_no_retry(
                &TRY_SET_MAX_SIZE,
                vec![self.options_key()],
                vec![Bytes::from(max.to_string()), Bytes::from(mode.as_str())],
            )
            .await?;
        Ok(set == 1)
    }

    /// Starts listening to every kind of change of the cache, made by any client. See [`HashMapCache::events_of`].
    pub async fn events(&self) -> Result<Events<K, V, C>> {
        self.events_of(&EventKind::ALL).await
    }

    /// Starts listening to the given kinds of change, made by any client.
    ///
    /// As in Redisson, the first listener marks the cache in Redis, and from then on every write publishes its changes until the cache is deleted with [`Object::del`](crate::Object::del). Expired entries are always published by the clean-up.
    pub async fn events_of(&self, kinds: &[EventKind]) -> Result<Events<K, V, C>> {
        let _: i64 = self
            .key
            .core
            .redis()
            .hset(self.options_key(), ("has-listeners", 1))
            .await?;
        let mut sources = std::vec::Vec::new();
        for kind in EventKind::ALL {
            if kinds.contains(&kind) {
                sources.push(
                    self.key
                        .core
                        .pubsub
                        .subscribe_with_messages(&self.channel(kind))
                        .await?,
                );
            }
        }
        if sources.is_empty() {
            return Err(Error::Config("listen to at least one kind of event".into()));
        }
        Ok(Events {
            sources,
            codec: self.codec.clone(),
            _marker: PhantomData,
        })
    }

    /// Returns the value for the key, or `None` when it is missing or expired. A read starts the idle time of the entry again and counts as a use for the size limit.
    pub async fn get<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let raw: Option<Bytes> = self
            .key
            .core
            .eval(&GET, self.redis_keys(), vec![self.codec.encode(k)?])
            .await?;
        self.decode_value(raw)
    }

    /// Removes the live entry and returns its value. An expired entry is left for the clean-up and `None` is returned.
    pub async fn remove<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let raw: Option<Bytes> = self
            .key
            .core
            .eval(&REMOVE, self.redis_keys(), vec![self.codec.encode(k)?])
            .await?;
        self.decode_value(raw)
    }

    /// Returns whether a live entry exists for the key. Like a read, it starts the idle time again and counts as a use for the size limit.
    pub async fn contains_key<Q>(&self, k: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let found: i64 = self
            .key
            .core
            .eval(&CONTAINS, self.redis_keys(), vec![self.codec.encode(k)?])
            .await?;
        Ok(found == 1)
    }

    /// Time left until the entry expires, by its time to live or its idle time, whichever comes first. `None` means the entry has no limit or does not exist.
    pub async fn entry_ttl<Q>(&self, k: &Q) -> Result<Option<Duration>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let millis: i64 = self
            .key
            .core
            .eval(&ENTRY_TTL, self.redis_keys(), vec![self.codec.encode(k)?])
            .await?;
        Ok((millis >= 0).then(|| Duration::from_millis(millis as u64)))
    }

    /// Number of entries in Redis. As in Redisson, expired entries count until the clean-up deletes them.
    pub async fn len(&self) -> Result<usize> {
        let len: usize = self.key.core.redis().hlen(self.key.redis_key()).await?;
        Ok(len)
    }

    /// Returns whether Redis holds no entry.
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// Deletes every entry. The size limit and the listener mark stay.
    pub async fn clear(&self) -> Result<()> {
        self.key
            .del_all(vec![self.timeout_key(), self.idle_key(), self.access_key()])
            .await?;
        Ok(())
    }

    /// Deletes the expired entries now, publishes them as [`Event::Expired`] and returns how many there were.
    pub async fn evict_expired(&self) -> Result<usize> {
        let mut total = 0;
        loop {
            let evicted: i64 = self
                .key
                .core
                .eval(&EVICT, self.eviction_keys(), eviction::arguments(None))
                .await?;
            total += evicted.max(0) as usize;
            if evicted < KEYS_LIMIT {
                return Ok(total);
            }
        }
    }

    /// Streams the live entries, 100 at a time, in no particular order. Expired entries are skipped page by page, and an entry with an idle limit that is returned starts its idle time again. An entry that is present the whole time is returned at least once and can be returned twice.
    pub fn iter(&self) -> impl Stream<Item = Result<(K, V)>> + '_ {
        stream::try_unfold(Some("0".to_string()), move |cursor| async move {
            let Some(cursor) = cursor else {
                return Ok::<_, Error>(None);
            };
            let (next, raw): (String, std::vec::Vec<Bytes>) = self
                .key
                .core
                .eval(
                    &SCAN,
                    vec![self.key.redis_key(), self.timeout_key(), self.idle_key()],
                    vec![Bytes::from(cursor), Bytes::from(SCAN_PAGE.to_string())],
                )
                .await?;
            let items = raw
                .chunks(2)
                .filter(|pair| pair.len() == 2)
                .map(|pair| Ok((self.codec.decode(&pair[0])?, self.codec.decode(&pair[1])?)))
                .collect::<Result<std::vec::Vec<(K, V)>>>()?;
            let next = (next != "0").then_some(next);
            Ok(Some((stream::iter(items.into_iter().map(Ok)), next)))
        })
        .try_flatten()
    }

    /// Streams the live keys.
    pub fn keys(&self) -> impl Stream<Item = Result<K>> + '_ {
        self.iter().map_ok(|(k, _)| k)
    }

    /// Streams the live values.
    pub fn values(&self) -> impl Stream<Item = Result<V>> + '_ {
        self.iter().map_ok(|(_, v)| v)
    }
}
