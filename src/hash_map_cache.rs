use crate::codec::Codec;
use crate::error::Result;
use crate::hash_map::HashMap;
use crate::object::{millis, HasKey, Key};
use bytes::Bytes;
use fred::types::scripts::Script;
use futures::Stream;
use futures::StreamExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;
use std::sync::LazyLock;
use std::time::Duration;

const EVICT_BATCH: usize = 1000;

const PRELUDE: &str = "local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local function live(field)
    local score = redis.call('ZSCORE', KEYS[2], field)
    if score and tonumber(score) <= now then
        redis.call('HDEL', KEYS[1], field)
        redis.call('ZREM', KEYS[2], field)
        return false
    end
    return true
end
";

fn script(body: &str) -> Script {
    Script::from_lua(format!("{PRELUDE}{body}"))
}

static GET: LazyLock<Script> = LazyLock::new(|| {
    script(
        "if not live(ARGV[1]) then
            return false
        end
        return redis.call('HGET', KEYS[1], ARGV[1])",
    )
});

static INSERT: LazyLock<Script> = LazyLock::new(|| {
    script(
        "local previous = false
        if live(ARGV[1]) then
            previous = redis.call('HGET', KEYS[1], ARGV[1])
        end
        redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
        local ttl = tonumber(ARGV[3])
        if ttl > 0 then
            redis.call('ZADD', KEYS[2], now + ttl, ARGV[1])
        else
            redis.call('ZREM', KEYS[2], ARGV[1])
        end
        return previous",
    )
});

static INSERT_NX: LazyLock<Script> = LazyLock::new(|| {
    script(
        "if live(ARGV[1]) and redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then
            return 0
        end
        redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
        local ttl = tonumber(ARGV[3])
        if ttl > 0 then
            redis.call('ZADD', KEYS[2], now + ttl, ARGV[1])
        else
            redis.call('ZREM', KEYS[2], ARGV[1])
        end
        return 1",
    )
});

static REMOVE: LazyLock<Script> = LazyLock::new(|| {
    script(
        "local previous = false
        if live(ARGV[1]) then
            previous = redis.call('HGET', KEYS[1], ARGV[1])
        end
        redis.call('HDEL', KEYS[1], ARGV[1])
        redis.call('ZREM', KEYS[2], ARGV[1])
        return previous",
    )
});

static CONTAINS: LazyLock<Script> = LazyLock::new(|| {
    script(
        "if not live(ARGV[1]) then
            return 0
        end
        return redis.call('HEXISTS', KEYS[1], ARGV[1])",
    )
});

static ENTRY_TTL: LazyLock<Script> = LazyLock::new(|| {
    script(
        "if not live(ARGV[1]) or redis.call('HEXISTS', KEYS[1], ARGV[1]) == 0 then
            return -2
        end
        local score = redis.call('ZSCORE', KEYS[2], ARGV[1])
        if not score then
            return -1
        end
        return tonumber(score) - now",
    )
});

static LEN: LazyLock<Script> = LazyLock::new(|| {
    script("return redis.call('HLEN', KEYS[1]) - redis.call('ZCOUNT', KEYS[2], '-inf', now)")
});

static EVICT: LazyLock<Script> = LazyLock::new(|| {
    script(
        "local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, ARGV[1])
        for _, field in ipairs(expired) do
            redis.call('HDEL', KEYS[1], field)
            redis.call('ZREM', KEYS[2], field)
        end
        return #expired",
    )
});

/// A distributed map where each entry can have its own time to live, as in Redisson's `RMapCache`.
///
/// Time is taken from the Redis server. Expiry is lazy: every operation hides and deletes the expired entries it touches, but an entry nobody touches stays in Redis until [`HashMapCache::evict_expired`], [`HashMapCache::iter`] or [`HashMapCache::clear`] runs. [`HashMapCache::len`] counts only live entries but does not delete the expired ones.
///
/// The data lives in the hash `{name}` and the expiry times in the sorted set `{name}:expires`. The name gets a hash tag, so both keys are in one slot in Redis Cluster.
pub struct HashMapCache<K, V, C: Codec> {
    key: Key,
    codec: C,
    map: HashMap<K, V, C>,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V, C: Codec> Clone for HashMapCache<K, V, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
            map: self.map.clone(),
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
        vec![self.expires_key()]
    }
}

impl<K, V, C: Codec> HashMapCache<K, V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self {
            map: HashMap::new(key.clone(), codec.clone()),
            key,
            codec,
            _marker: PhantomData,
        }
    }

    fn expires_key(&self) -> String {
        format!("{}:expires", self.key.redis_key())
    }

    fn redis_keys(&self) -> Vec<String> {
        vec![self.key.redis_key(), self.expires_key()]
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

    async fn insert_for<Q, W>(&self, k: &Q, v: &W, ttl_millis: i64) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        let previous: Option<Bytes> = self
            .key
            .core
            .eval(
                &INSERT,
                self.redis_keys(),
                vec![
                    self.codec.encode(k)?,
                    self.codec.encode(v)?,
                    Bytes::from(ttl_millis.to_string()),
                ],
            )
            .await?;
        self.decode_value(previous)
    }

    /// Inserts an entry without a time limit and returns the live value it replaced. A time limit the old entry had is removed.
    pub async fn insert<Q, W>(&self, k: &Q, v: &W) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        self.insert_for(k, v, 0).await
    }

    /// Inserts an entry that expires after `ttl` and returns the live value it replaced.
    pub async fn insert_with_ttl<Q, W>(&self, k: &Q, v: &W, ttl: Duration) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        self.insert_for(k, v, millis(ttl)?).await
    }

    /// Inserts the entry only when there is no live entry for the key; returns whether it was inserted.
    pub async fn insert_nx<Q, W>(&self, k: &Q, v: &W) -> Result<bool>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        let inserted: i64 = self
            .key
            .core
            .eval(
                &INSERT_NX,
                self.redis_keys(),
                vec![
                    self.codec.encode(k)?,
                    self.codec.encode(v)?,
                    Bytes::from("0"),
                ],
            )
            .await?;
        Ok(inserted == 1)
    }

    /// Returns the value for the key, or `None` when it is missing or expired.
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

    /// Returns whether a live entry exists for the key.
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

    /// Time left of one entry. `None` means the entry has no time limit or does not exist.
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

    /// Number of live entries.
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
        self.key.del_all(vec![self.expires_key()]).await?;
        Ok(())
    }

    /// Deletes the expired entries now and returns how many there were.
    pub async fn evict_expired(&self) -> Result<usize> {
        let mut total = 0;
        loop {
            let evicted: usize = self
                .key
                .core
                .eval(
                    &EVICT,
                    self.redis_keys(),
                    vec![Bytes::from(EVICT_BATCH.to_string())],
                )
                .await?;
            total += evicted;
            if evicted < EVICT_BATCH {
                return Ok(total);
            }
        }
    }

    /// Streams all live entries, 100 at a time. It first deletes the expired entries.
    pub async fn iter(&self) -> Result<impl Stream<Item = Result<(K, V)>> + '_> {
        self.evict_expired().await?;
        Ok(self.map.iter())
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
