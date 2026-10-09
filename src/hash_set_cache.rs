use crate::codec::Codec;
use crate::core::{Core, Evictor};
use crate::error::Result;
use crate::object::{millis as to_millis, HasKey, Key};
use bytes::Bytes;
use fred::types::scripts::Script;
use futures::{stream, Stream, TryStreamExt};
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

const NO_EXPIRY: &str = "92233720368547758";
const EVICT_BATCH: usize = 100;
const SCAN_PAGE: u32 = 100;
const MAX_EVICT_PAUSE: Duration = Duration::from_secs(2 * 60 * 60);

const NOW: &str = "local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
";

fn script(body: &str) -> Script {
    Script::from_lua(format!("{NOW}{body}"))
}

static INSERT: LazyLock<Script> = LazyLock::new(|| {
    script(
        "local score = ARGV[1]
        if score ~= ARGV[3] then
            score = now + tonumber(ARGV[1])
        end
        local old = redis.call('ZSCORE', KEYS[1], ARGV[2])
        local live = old ~= false and tonumber(old) > now
        if ARGV[4] == '1' and live then
            return 0
        end
        redis.call('ZADD', KEYS[1], score, ARGV[2])
        if live then
            return 0
        end
        return 1",
    )
});

static CONTAINS: LazyLock<Script> = LazyLock::new(|| {
    script(
        "local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
        if score ~= false and tonumber(score) > now then
            return 1
        end
        return 0",
    )
});

static REMOVE: LazyLock<Script> = LazyLock::new(|| {
    script(
        "local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
        if score == false then
            return 0
        end
        redis.call('ZREM', KEYS[1], ARGV[1])
        if tonumber(score) > now then
            return 1
        end
        return 0",
    )
});

static LEN: LazyLock<Script> =
    LazyLock::new(|| script("return redis.call('ZCOUNT', KEYS[1], '(' .. now, '+inf')"));

static ENTRY_TTL: LazyLock<Script> = LazyLock::new(|| {
    script(
        "local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
        if score == false or tonumber(score) <= now then
            return -2
        end
        if tonumber(score) > now + 100000000000000 then
            return -1
        end
        return math.floor(tonumber(score) - now)",
    )
});

static EVICT: LazyLock<Script> = LazyLock::new(|| {
    script(
        "local expired = redis.call('ZRANGEBYSCORE', KEYS[1], 0, now, 'LIMIT', 0, ARGV[1])
        if #expired > 0 then
            redis.call('ZREM', KEYS[1], unpack(expired))
        end
        return #expired",
    )
});

static SCAN: LazyLock<Script> = LazyLock::new(|| {
    script(
        "local res = redis.call('ZSCAN', KEYS[1], ARGV[1], 'COUNT', ARGV[2])
        local result = {}
        for i = 2, #res[2], 2 do
            if tonumber(res[2][i]) > now then
                table.insert(result, res[2][i - 1])
            end
        end
        return {res[1], result}",
    )
});

/// A pending [`HashSetCache::insert`] or [`HashSetCache::insert_nx`]. Await it, optionally after setting [`SetInsert::ttl`].
#[must_use = "an insert does nothing until it is awaited"]
pub struct SetInsert<'a, V, C: Codec, Q: ?Sized> {
    set: &'a HashSetCache<V, C>,
    v: &'a Q,
    ttl: Option<Duration>,
    only_if_absent: bool,
}

impl<'a, V, C: Codec, Q: ?Sized> SetInsert<'a, V, C, Q> {
    /// Makes the value expire after `ttl`.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }
}

impl<'a, V, C, Q> IntoFuture for SetInsert<'a, V, C, Q>
where
    V: Serialize + DeserializeOwned + Send + Sync + Borrow<Q> + 'a,
    C: Codec,
    Q: Serialize + ?Sized + Sync + 'a,
{
    type Output = Result<bool>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let set = self.set;
            let ttl = self.ttl.map(to_millis).transpose()?;
            let added: i64 = set
                .key
                .core
                .eval(
                    &INSERT,
                    vec![set.key.redis_key()],
                    vec![
                        Bytes::from(ttl.map_or(NO_EXPIRY.to_string(), |ttl| ttl.to_string())),
                        set.codec.encode(self.v)?,
                        Bytes::from(NO_EXPIRY),
                        Bytes::from(if self.only_if_absent { "1" } else { "0" }),
                    ],
                )
                .await?;
            if ttl.is_some() {
                wake_evictor(&set.key.core, &set.key.redis_key());
            }
            Ok(added == 1)
        })
    }
}

/// A distributed set in which every value can expire, stored in a Redis sorted set whose score is the expiry time, as in Redisson's `RedissonSetCache`. Values are equal when their encoded bytes are equal.
///
/// Time is taken from the Redis server. Expired values are hidden at once, and a background task deletes them: it wakes up every [`ClientBuilder::eviction_interval`](crate::ClientBuilder::eviction_interval) when there is work and less often when there is none. [`HashSetCache::evict_expired`] does the same immediately.
pub struct HashSetCache<V, C: Codec> {
    key: Key,
    codec: C,
    _marker: PhantomData<fn() -> V>,
}

impl<V, C: Codec> Clone for HashSetCache<V, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
            _marker: PhantomData,
        }
    }
}

impl<V, C: Codec> fmt::Debug for HashSetCache<V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "HashSetCache")
    }
}

impl<V, C: Codec> HasKey for HashSetCache<V, C> {
    fn key(&self) -> &Key {
        &self.key
    }
}

impl<V, C: Codec> HashSetCache<V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        start_evictor(&key.core, key.redis_key());
        Self {
            key,
            codec,
            _marker: PhantomData,
        }
    }
}

impl<V, C> HashSetCache<V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    /// Adds a value, or replaces its time limit; returns whether it was new. Without [`SetInsert::ttl`] the value never expires. The value is borrowed: `set.insert("a").ttl(ttl)`.
    pub fn insert<'a, Q>(&'a self, v: &'a Q) -> SetInsert<'a, V, C, Q>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        SetInsert {
            set: self,
            v,
            ttl: None,
            only_if_absent: false,
        }
    }

    /// Adds a value only when there is no live one, and keeps the time limit of a live one; returns whether it was added.
    pub fn insert_nx<'a, Q>(&'a self, v: &'a Q) -> SetInsert<'a, V, C, Q>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        SetInsert {
            set: self,
            v,
            ttl: None,
            only_if_absent: true,
        }
    }

    /// Removes a value; returns whether a live one was removed.
    pub async fn remove<Q>(&self, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let removed: i64 = self
            .key
            .core
            .eval(
                &REMOVE,
                vec![self.key.redis_key()],
                vec![self.codec.encode(v)?],
            )
            .await?;
        Ok(removed == 1)
    }

    /// Returns whether the set holds a live value.
    pub async fn contains<Q>(&self, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let found: i64 = self
            .key
            .core
            .eval(
                &CONTAINS,
                vec![self.key.redis_key()],
                vec![self.codec.encode(v)?],
            )
            .await?;
        Ok(found == 1)
    }

    /// Time left of a value. `None` means the value has no limit or is not in the set.
    pub async fn entry_ttl<Q>(&self, v: &Q) -> Result<Option<Duration>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let left: i64 = self
            .key
            .core
            .eval(
                &ENTRY_TTL,
                vec![self.key.redis_key()],
                vec![self.codec.encode(v)?],
            )
            .await?;
        Ok(u64::try_from(left).ok().map(Duration::from_millis))
    }

    /// Number of live values.
    pub async fn len(&self) -> Result<usize> {
        let length: usize = self
            .key
            .core
            .eval(&LEN, vec![self.key.redis_key()], Vec::new())
            .await?;
        Ok(length)
    }

    /// Returns whether the set has no live value.
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// Removes every value.
    pub async fn clear(&self) -> Result<()> {
        self.key.del_all(Vec::new()).await?;
        Ok(())
    }

    /// Deletes the expired values now and returns how many.
    pub async fn evict_expired(&self) -> Result<usize> {
        let mut total = 0;
        loop {
            let evicted = evict(&self.key.core, self.key.redis_key()).await?;
            total += evicted;
            if evicted < EVICT_BATCH {
                return Ok(total);
            }
        }
    }

    /// Streams the live values one page at a time, in no particular order. A value that is present the whole time is returned at least once and can be returned twice.
    pub fn iter(&self) -> impl Stream<Item = Result<V>> + '_ {
        stream::try_unfold(Some("0".to_string()), move |cursor| async move {
            let Some(cursor) = cursor else {
                return Ok::<_, crate::error::Error>(None);
            };
            let (next, raw): (String, Vec<Bytes>) = self
                .key
                .core
                .eval(
                    &SCAN,
                    vec![self.key.redis_key()],
                    vec![Bytes::from(cursor), Bytes::from(SCAN_PAGE.to_string())],
                )
                .await?;
            let items = raw
                .iter()
                .map(|bytes| self.codec.decode(bytes))
                .collect::<Result<Vec<V>>>()?;
            let next = (next != "0").then_some(next);
            Ok(Some((stream::iter(items.into_iter().map(Ok)), next)))
        })
        .try_flatten()
    }
}

async fn evict(core: &Core, key: String) -> Result<usize> {
    core.eval(
        &EVICT,
        vec![key],
        vec![Bytes::from(EVICT_BATCH.to_string())],
    )
    .await
}

async fn evict_loop(core: Weak<Core>, key: String, minimum: Duration, wake: Arc<Notify>) {
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
        let evicted = evict(&core, key.clone()).await.unwrap_or(0);
        pause = if evicted >= EVICT_BATCH {
            (pause / 4).max(minimum)
        } else if evicted == 0 {
            (pause * 2).min(MAX_EVICT_PAUSE)
        } else {
            pause
        };
    }
}

fn registry_name(key: &str) -> String {
    format!("redissun__set_cache_evictor:{key}")
}

fn start_evictor(core: &Arc<Core>, key: String) {
    let Ok(runtime) = Handle::try_current() else {
        return;
    };
    let name = registry_name(&key);
    let mut evictors = core.evictors.lock().unwrap_or_else(|e| e.into_inner());
    if evictors
        .get(&name)
        .is_some_and(|evictor| !evictor.handle.is_finished())
    {
        return;
    }
    let wake = Arc::new(Notify::new());
    let handle = runtime.spawn(evict_loop(
        Arc::downgrade(core),
        key,
        core.eviction_interval,
        wake.clone(),
    ));
    evictors.insert(name, Evictor { handle, wake });
}

fn wake_evictor(core: &Core, key: &str) {
    let evictors = core.evictors.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(evictor) = evictors.get(&registry_name(key)) {
        evictor.wake.notify_one();
    }
}
