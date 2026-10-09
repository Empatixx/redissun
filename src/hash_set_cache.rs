use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::eviction::{self, KEYS_LIMIT};
use crate::object::{millis as to_millis, HasKey, Key};
use crate::pubsub::Subscription;
use bytes::Bytes;
use fred::interfaces::SortedSetsInterface;
use fred::types::scripts::Script;
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
use tokio::sync::broadcast::{self, error::RecvError};

const SCAN_PAGE: u32 = 100;

const NOW: &str = "local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local NEVER = 92233720368547758
";

fn script(body: &str) -> Script {
    Script::from_lua(format!("{NOW}{body}"))
}

static INSERT: LazyLock<Script> = LazyLock::new(|| {
    script(
        "local score = NEVER - now
        if tonumber(ARGV[1]) > 0 then
            score = now + tonumber(ARGV[1])
        end
        local old = redis.call('ZSCORE', KEYS[1], ARGV[2])
        local live = old ~= false and tonumber(old) > now
        if ARGV[3] == '1' and live then
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

static LEN: LazyLock<Script> =
    LazyLock::new(|| script("return redis.call('ZCOUNT', KEYS[1], now, NEVER)"));

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
        "if ARGV[3] == '1' and not redis.call('SET', KEYS[3], 1, 'NX', 'PX', ARGV[2]) then
            return -1
        end
        local expired = redis.call('ZRANGEBYSCORE', KEYS[1], 0, now, 'LIMIT', 0, ARGV[1])
        for _, value in ipairs(expired) do
            if redis.call('PUBLISH', KEYS[2], value) == 0 then
                break
            end
        end
        for i = 1, #expired, 5000 do
            redis.call('ZREM', KEYS[1], unpack(expired, i, math.min(i + 4999, #expired)))
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
    /// Makes the value expire after `ttl`. Zero means it never expires.
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
            let ttl = match self.ttl {
                Some(ttl) if !ttl.is_zero() => to_millis(ttl)?,
                _ => 0,
            };
            let added: i64 = set
                .key
                .core
                .eval(
                    &INSERT,
                    vec![set.key.redis_key()],
                    vec![
                        Bytes::from(ttl.to_string()),
                        set.codec.encode(self.v)?,
                        Bytes::from(if self.only_if_absent { "1" } else { "0" }),
                    ],
                )
                .await?;
            Ok(added == 1)
        })
    }
}

/// Receives the values that the clean-up of a [`HashSetCache`] deleted, from [`HashSetCache::expired`].
pub struct Expirations<V, C: Codec> {
    _subscription: Subscription,
    receiver: broadcast::Receiver<Bytes>,
    codec: C,
    _marker: PhantomData<fn() -> V>,
}

impl<V, C: Codec> fmt::Debug for Expirations<V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Expirations").finish_non_exhaustive()
    }
}

impl<V, C> Expirations<V, C>
where
    V: DeserializeOwned + Send,
    C: Codec,
{
    /// Waits for the next expired value. A listener that falls more than 256 values behind gets [`Error::Lagged`] once and then continues with the newest ones.
    pub async fn recv(&mut self) -> Result<V> {
        match self.receiver.recv().await {
            Ok(payload) => self.codec.decode(&payload),
            Err(RecvError::Lagged(missed)) => Err(Error::Lagged(missed as usize)),
            Err(RecvError::Closed) => Err(Error::Redis("the subscription was closed".into())),
        }
    }

    /// Turns the listener into a stream of expired values. The stream never ends.
    pub fn into_stream(self) -> impl Stream<Item = Result<V>> {
        stream::unfold(self, |mut expirations| async move {
            Some((expirations.recv().await, expirations))
        })
    }
}

/// A distributed set in which every value can expire, modelled on Redisson's `RSetCache`. It is a Redis sorted set whose score is the expiry time; a value without a time limit gets the score `92233720368547758 - now`, as in Redisson. Values are equal when their encoded bytes are equal.
///
/// Time is taken from the Redis server. Reads skip expired values at once but leave them in Redis; a background task deletes them, as Redisson's eviction task does. It first runs [`ClientBuilder::eviction_interval`](crate::ClientBuilder::eviction_interval) after the set is first used in this client, deletes up to 100 values per run, and adapts its pause between that interval and 30 minutes. A short-lived lock in Redis lets only one client clean a set per run. [`HashSetCache::evict_expired`] cleans at once.
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
        let set = Self {
            key,
            codec,
            _marker: PhantomData,
        };
        eviction::schedule(
            &set.key.core,
            format!("redissun__set_cache_eviction:{}", set.key.redis_key()),
            &EVICT,
            set.eviction_keys(),
        );
        set
    }

    fn expired_channel(&self) -> String {
        format!("redissun_set_cache_expired:{}", self.key.redis_key())
    }

    fn eviction_keys(&self) -> std::vec::Vec<String> {
        vec![
            self.key.redis_key(),
            self.expired_channel(),
            eviction::latch_key(&self.key.redis_key()),
        ]
    }
}

impl<V, C> HashSetCache<V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    /// Adds a value, or replaces its time limit; returns whether it was new or expired. Without [`SetInsert::ttl`] the value never expires. The value is borrowed: `set.insert("a").ttl(ttl)`.
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

    /// Adds a value only when there is no live one, and keeps the time limit of a live one, like Redisson's `tryAdd`; returns whether it was added.
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

    /// Removes a value; returns whether it was stored, even if it had already expired, as Redisson's `remove` does.
    pub async fn remove<Q>(&self, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let removed: i64 = self
            .key
            .core
            .redis()
            .zrem(self.key.redis_key(), self.codec.encode(v)?)
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

    /// Starts listening to the values that the clean-up deletes because they expired, from any client, as Redisson's `SetExpiredListener`.
    pub async fn expired(&self) -> Result<Expirations<V, C>> {
        let (subscription, receiver) = self
            .key
            .core
            .pubsub()
            .await?
            .subscribe_with_messages(&self.expired_channel())
            .await?;
        Ok(Expirations {
            _subscription: subscription,
            receiver,
            codec: self.codec.clone(),
            _marker: PhantomData,
        })
    }

    /// Deletes the expired values now, publishes them to [`HashSetCache::expired`] listeners and returns how many there were.
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

    /// Streams the live values one page at a time, in no particular order. A value that is present the whole time is returned at least once and can be returned twice.
    pub fn iter(&self) -> impl Stream<Item = Result<V>> + '_ {
        stream::try_unfold(Some("0".to_string()), move |cursor| async move {
            let Some(cursor) = cursor else {
                return Ok::<_, Error>(None);
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
