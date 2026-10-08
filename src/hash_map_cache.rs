use crate::codec::Codec;
use crate::core::{Core, Evictor};
use crate::error::Result;
use crate::object::{millis, HasKey, Key};
use bytes::Bytes;
use fred::types::scan::Scanner;
use fred::types::scripts::Script;
use futures::{stream, Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Weak};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::Notify;

const EVICT_BATCH: usize = 100;
const MAX_EVICT_PAUSE: Duration = Duration::from_secs(2 * 60 * 60);
const SCAN_PAGE: u32 = 100;

const PRELUDE: &str = "local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local MAIN, TIMEOUT, IDLE = KEYS[1], KEYS[2], KEYS[3]
local function drop(field)
    redis.call('HDEL', MAIN, field)
    redis.call('ZREM', TIMEOUT, field)
    redis.call('ZREM', IDLE, field)
end
local function fetch(field)
    local raw = redis.call('HGET', MAIN, field)
    if raw == false then
        return false
    end
    local sep = string.find(raw, ':', 1, true)
    local idle = 0
    local value = raw
    if sep then
        idle = tonumber(string.sub(raw, 1, sep - 1)) or 0
        value = string.sub(raw, sep + 1)
    end
    local expires = math.huge
    local timeoutScore = redis.call('ZSCORE', TIMEOUT, field)
    if timeoutScore then
        expires = tonumber(timeoutScore)
    end
    if idle > 0 then
        local idleScore = redis.call('ZSCORE', IDLE, field)
        if idleScore then
            expires = math.min(expires, tonumber(idleScore))
        end
    end
    if expires <= now then
        drop(field)
        return false
    end
    return value, idle, expires
end
local function store(field, value, ttl, idle)
    redis.call('HSET', MAIN, field, idle .. ':' .. value)
    if tonumber(ttl) > 0 then
        redis.call('ZADD', TIMEOUT, now + tonumber(ttl), field)
    else
        redis.call('ZREM', TIMEOUT, field)
    end
    if tonumber(idle) > 0 then
        redis.call('ZADD', IDLE, now + tonumber(idle), field)
    else
        redis.call('ZREM', IDLE, field)
    end
end
";

fn script(body: &str) -> Script {
    Script::from_lua(format!("{PRELUDE}{body}"))
}

static GET: LazyLock<Script> = LazyLock::new(|| {
    script(
        "local value, idle = fetch(ARGV[1])
        if not value then
            return false
        end
        if idle > 0 then
            redis.call('ZADD', IDLE, now + idle, ARGV[1])
        end
        return value",
    )
});

static INSERT: LazyLock<Script> = LazyLock::new(|| {
    script(
        "local previous = fetch(ARGV[1])
        store(ARGV[1], ARGV[2], ARGV[3], ARGV[4])
        return previous",
    )
});

static INSERT_NX: LazyLock<Script> = LazyLock::new(|| {
    script(
        "if fetch(ARGV[1]) then
            return 0
        end
        store(ARGV[1], ARGV[2], ARGV[3], ARGV[4])
        return 1",
    )
});

static REMOVE: LazyLock<Script> = LazyLock::new(|| {
    script(
        "local previous = fetch(ARGV[1])
        drop(ARGV[1])
        return previous",
    )
});

static CONTAINS: LazyLock<Script> = LazyLock::new(|| {
    script(
        "if fetch(ARGV[1]) then
            return 1
        end
        return 0",
    )
});

static ENTRY_TTL: LazyLock<Script> = LazyLock::new(|| {
    script(
        "local value, idle, expires = fetch(ARGV[1])
        if not value then
            return -2
        end
        if expires == math.huge then
            return -1
        end
        return expires - now",
    )
});

static LEN: LazyLock<Script> = LazyLock::new(|| {
    script(
        "local expired = {}
        local count = 0
        for _, set in ipairs({TIMEOUT, IDLE}) do
            for _, field in ipairs(redis.call('ZRANGEBYSCORE', set, '-inf', now)) do
                if not expired[field] then
                    expired[field] = true
                    if redis.call('HEXISTS', MAIN, field) == 1 then
                        count = count + 1
                    end
                end
            end
        end
        return redis.call('HLEN', MAIN) - count",
    )
});

static EVICT: LazyLock<Script> = LazyLock::new(|| {
    script(
        "local removed = 0
        for _, set in ipairs({TIMEOUT, IDLE}) do
            for _, field in ipairs(redis.call('ZRANGEBYSCORE', set, '-inf', now, 'LIMIT', 0, ARGV[1])) do
                local dropped = redis.call('HDEL', MAIN, field)
                dropped = dropped + redis.call('ZREM', TIMEOUT, field)
                dropped = dropped + redis.call('ZREM', IDLE, field)
                if dropped > 0 then
                    removed = removed + 1
                end
            end
        end
        return removed",
    )
});

fn strip_idle(raw: &[u8]) -> &[u8] {
    match raw.iter().position(|byte| *byte == b':') {
        Some(separator) => &raw[separator + 1..],
        None => raw,
    }
}

async fn evict(core: &Core, keys: Vec<String>) -> Result<usize> {
    core.eval(&EVICT, keys, vec![Bytes::from(EVICT_BATCH.to_string())])
        .await
}

async fn evict_loop(core: Weak<Core>, keys: Vec<String>, minimum: Duration, wake: Arc<Notify>) {
    let mut pause = minimum;
    loop {
        tokio::select! {
            _ = tokio::time::sleep(pause) => {}
            _ = wake.notified() => {
                pause = minimum;
                continue;
            }
        }
        let Some(core) = core.upgrade() else {
            return;
        };
        let evicted = evict(&core, keys.clone()).await.unwrap_or(0);
        pause = if evicted >= EVICT_BATCH {
            (pause / 4).max(minimum)
        } else if evicted == 0 {
            (pause * 2).min(MAX_EVICT_PAUSE)
        } else {
            pause
        };
    }
}

fn start_evictor(core: &Arc<Core>, keys: Vec<String>) {
    let Ok(runtime) = Handle::try_current() else {
        return;
    };
    let mut evictors = core.evictors.lock().unwrap_or_else(|e| e.into_inner());
    if evictors
        .get(&keys[0])
        .is_some_and(|evictor| !evictor.handle.is_finished())
    {
        return;
    }
    let name = keys[0].clone();
    let wake = Arc::new(Notify::new());
    let handle = runtime.spawn(evict_loop(
        Arc::downgrade(core),
        keys,
        core.eviction_interval,
        wake.clone(),
    ));
    evictors.insert(name, Evictor { handle, wake });
}

fn wake_evictor(core: &Core, name: &str) {
    let evictors = core.evictors.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(evictor) = evictors.get(name) {
        evictor.wake.notify_one();
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
    /// Makes the entry expire after `ttl`, whether it is read or not.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Makes the entry expire when nobody has read it for `max_idle`. Every read starts the time again.
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
            let previous: Option<Bytes> = cache
                .key
                .core
                .eval(
                    &INSERT,
                    cache.redis_keys(),
                    cache.insert_arguments(self.k, self.v, self.ttl, self.max_idle)?,
                )
                .await?;
            if self.ttl.is_some() || self.max_idle.is_some() {
                wake_evictor(&cache.key.core, &cache.redis_key());
            }
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
    /// Makes the entry expire after `ttl`, whether it is read or not.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Makes the entry expire when nobody has read it for `max_idle`. Every read starts the time again.
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
            let inserted: i64 = cache
                .key
                .core
                .eval(
                    &INSERT_NX,
                    cache.redis_keys(),
                    cache.insert_arguments(self.k, self.v, self.ttl, self.max_idle)?,
                )
                .await?;
            if inserted == 1 && (self.ttl.is_some() || self.max_idle.is_some()) {
                wake_evictor(&cache.key.core, &cache.redis_key());
            }
            Ok(inserted == 1)
        })
    }
}

/// A distributed map where each entry can expire, as in Redisson's `RMapCache`.
///
/// An entry can have a time to live ([`Insert::ttl`]) and a maximum idle time ([`Insert::max_idle`]). Time is taken from the Redis server. Expired entries are hidden at once, and a background task deletes them: it wakes up every [`ClientBuilder::eviction_interval`](crate::ClientBuilder::eviction_interval) when there is work and less often when there is none. [`HashMapCache::evict_expired`] does the same immediately.
///
/// The data lives in the hash `{name}`. The expiry times live in the sorted sets `redissun__timeout__set:{name}` and `redissun__idle__set:{name}`. The name gets a hash tag, so all keys are in one slot in Redis Cluster.
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

    fn companions(&self) -> Vec<String> {
        vec![self.timeout_key(), self.idle_key()]
    }
}

impl<K, V, C: Codec> HashMapCache<K, V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        let cache = Self {
            key,
            codec,
            _marker: PhantomData,
        };
        start_evictor(&cache.key.core, cache.redis_keys());
        cache
    }

    fn timeout_key(&self) -> String {
        format!("redissun__timeout__set:{}", self.key.redis_key())
    }

    fn idle_key(&self) -> String {
        format!("redissun__idle__set:{}", self.key.redis_key())
    }

    fn redis_key(&self) -> String {
        self.key.redis_key()
    }

    fn redis_keys(&self) -> Vec<String> {
        vec![self.key.redis_key(), self.timeout_key(), self.idle_key()]
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
    ) -> Result<Vec<Bytes>>
    where
        Q: Serialize + ?Sized,
        W: Serialize + ?Sized,
    {
        let ttl = ttl.map(millis).transpose()?.unwrap_or(0);
        let max_idle = max_idle.map(millis).transpose()?.unwrap_or(0);
        Ok(vec![
            self.codec.encode(k)?,
            self.codec.encode(v)?,
            Bytes::from(ttl.to_string()),
            Bytes::from(max_idle.to_string()),
        ])
    }

    /// Inserts an entry and returns the live value it replaced. Without [`Insert::ttl`] and [`Insert::max_idle`] the entry never expires, and a limit that the old entry had is removed.
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

    /// Inserts the entry only when there is no live entry for the key. Resolves to whether it was inserted. It takes `.ttl(..)` and `.max_idle(..)` too.
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

    /// Returns the value for the key, or `None` when it is missing or expired. A read starts the idle time of the entry again.
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

    /// Removes the entry and returns its live value.
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

    /// Returns whether a live entry exists for the key. It does not start the idle time again.
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

    /// Number of live entries. Expired entries that are still waiting for the clean-up are not counted.
    pub async fn len(&self) -> Result<usize> {
        let len: i64 = self
            .key
            .core
            .eval(&LEN, self.redis_keys(), Vec::new())
            .await?;
        Ok(len.max(0) as usize)
    }

    /// Returns whether there are no live entries.
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// Deletes every entry.
    pub async fn clear(&self) -> Result<()> {
        self.key
            .del_all(vec![self.timeout_key(), self.idle_key()])
            .await?;
        Ok(())
    }

    /// Deletes the expired entries now and returns how many there were.
    pub async fn evict_expired(&self) -> Result<usize> {
        let mut total = 0;
        loop {
            let evicted = evict(&self.key.core, self.redis_keys()).await?;
            total += evicted;
            if evicted < EVICT_BATCH {
                return Ok(total);
            }
        }
    }

    /// Streams all live entries, 100 at a time. It first deletes the expired entries.
    pub async fn iter(&self) -> Result<impl Stream<Item = Result<(K, V)>> + '_> {
        self.evict_expired().await?;
        let pages = Box::pin(self.key.core.redis().hscan(
            self.key.redis_key(),
            "*",
            Some(SCAN_PAGE),
        ));
        Ok(pages.flat_map(move |page| {
            let items: Vec<Result<(K, V)>> = match page {
                Ok(mut page) => {
                    let results = page.take_results();
                    page.next();
                    results
                        .map(|map| {
                            map.inner()
                                .iter()
                                .map(|(field, value)| {
                                    let value: Bytes = value.clone().convert()?;
                                    Ok((
                                        self.codec.decode(field.as_bytes())?,
                                        self.codec.decode(strip_idle(&value))?,
                                    ))
                                })
                                .collect()
                        })
                        .unwrap_or_default()
                }
                Err(error) => vec![Err(error.into())],
            };
            stream::iter(items)
        }))
    }

    /// Streams all live keys.
    pub async fn keys(&self) -> Result<impl Stream<Item = Result<K>> + '_> {
        Ok(self.iter().await?.map(|entry| entry.map(|(k, _)| k)))
    }

    /// Streams all live values.
    pub async fn values(&self) -> Result<impl Stream<Item = Result<V>> + '_> {
        Ok(self.iter().await?.map(|entry| entry.map(|(_, v)| v)))
    }
}
