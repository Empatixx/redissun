mod entry_events;
mod names;
mod scripts;

use crate::client::request::Request;
use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::eviction::{self, KEYS_LIMIT};
use crate::lua::Lua;
use crate::object::{millis, now_millis, HasKey, Key};
use bytes::Bytes;
use futures::{stream, Stream, TryStreamExt};
use scripts::{
    CLEAR_EXPIRE, CONTAINS_KEY, EVICT, EXPIRE, GET, PUT, PUT_IF_ABSENT_WITH_LIMITS,
    PUT_WITH_LIMITS, REMAIN_TTL, REMOVE, SCAN_ENTRIES, SCAN_KEYS, TOUCH_IDLE, TRY_SET_MAX_SIZE,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;

pub(crate) use entry_events::LengthPrefix;
pub use entry_events::{Event, EventKind, Events};

const SCAN_PAGE: u32 = 100;

/// The publish command the scripts take as an argument; Redisson sends `SPUBLISH` when the server supports sharded pub/sub.
const PUBLISH: &str = "PUBLISH";

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
            let limits = limits(self.ttl, self.max_idle)?;
            let key = cache.codec.encode(self.k)?;
            let value = cache.codec.encode(self.v)?;
            let previous: Option<Bytes> = if limits.any() {
                cache
                    .key
                    .core
                    .eval(
                        &PUT_WITH_LIMITS,
                        cache.redis_keys(&[
                            Name::Main,
                            Name::Timeout,
                            Name::Idle,
                            Name::Created,
                            Name::Updated,
                            Name::LastAccess,
                            Name::Removed,
                            Name::Options,
                        ]),
                        limits.arguments(vec![key, value, Bytes::from(PUBLISH)]),
                    )
                    .await?
            } else {
                cache
                    .key
                    .core
                    .eval(
                        &PUT,
                        cache.redis_keys(&[
                            Name::Main,
                            Name::Timeout,
                            Name::Idle,
                            Name::Created,
                            Name::Updated,
                            Name::LastAccess,
                            Name::Removed,
                            Name::Options,
                            Name::Lease,
                        ]),
                        vec![limits.now(), key, value, Bytes::from(PUBLISH)],
                    )
                    .await?
            };
            cache.decode_value(previous)
        })
    }
}

/// A pending [`HashMapCache::try_insert`]. Await it, optionally after setting [`TryInsert::ttl`] or [`TryInsert::max_idle`].
#[must_use = "an insert does nothing until it is awaited"]
pub struct TryInsert<'a, K, V, C: Codec, Q: ?Sized, W: ?Sized> {
    cache: &'a HashMapCache<K, V, C>,
    k: &'a Q,
    v: &'a W,
    ttl: Option<Duration>,
    max_idle: Option<Duration>,
}

impl<'a, K, V, C: Codec, Q: ?Sized, W: ?Sized> TryInsert<'a, K, V, C, Q, W> {
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

impl<'a, K, V, C, Q, W> IntoFuture for TryInsert<'a, K, V, C, Q, W>
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
            let limits = limits(self.ttl, self.max_idle)?;
            let key = cache.codec.encode(self.k)?;
            let value = cache.codec.encode(self.v)?;
            // Always the script with limits, with zeros when there are none: Redisson's script without limits leaves the old deadline of an expired entry in place.
            let live: Option<Bytes> = cache
                .key
                .core
                .eval(
                    &PUT_IF_ABSENT_WITH_LIMITS,
                    cache.redis_keys(&[
                        Name::Main,
                        Name::Timeout,
                        Name::Idle,
                        Name::Created,
                        Name::LastAccess,
                        Name::Removed,
                        Name::Options,
                        Name::Lease,
                    ]),
                    limits.arguments(vec![key, value, Bytes::from(PUBLISH)]),
                )
                .await?;
            cache.decode_value(live)
        })
    }
}

/// The deadlines of a write, computed on the client as Redisson does: each is `0` when its limit is unset or zero.
struct Limits {
    now: i64,
    ttl_timeout: i64,
    idle_timeout: i64,
    idle_delta: i64,
}

impl Limits {
    fn any(&self) -> bool {
        self.ttl_timeout > 0 || self.idle_timeout > 0
    }

    fn now(&self) -> Bytes {
        Bytes::from(self.now.to_string())
    }

    /// `[now, ttl timeout, idle timeout, idle delta, rest…]`, the ARGV of Redisson's scripts with limits.
    fn arguments(&self, rest: std::vec::Vec<Bytes>) -> std::vec::Vec<Bytes> {
        [
            self.now,
            self.ttl_timeout,
            self.idle_timeout,
            self.idle_delta,
        ]
        .into_iter()
        .map(|n| Bytes::from(n.to_string()))
        .chain(rest)
        .collect()
    }
}

fn limits(ttl: Option<Duration>, max_idle: Option<Duration>) -> Result<Limits> {
    let delta = |duration: Option<Duration>| match duration {
        Some(duration) if !duration.is_zero() => millis(duration),
        _ => Ok(0),
    };
    let ttl = delta(ttl)?;
    let idle_delta = delta(max_idle)?;
    let now = now_millis();
    let deadline = |delta: i64| {
        if delta == 0 {
            return Ok(0);
        }
        now.checked_add(delta)
            .ok_or_else(|| Error::Config("duration is too long".into()))
    };
    Ok(Limits {
        now,
        ttl_timeout: deadline(ttl)?,
        idle_timeout: deadline(idle_delta)?,
        idle_delta,
    })
}

/// The arguments of [`EVICT`] for one run: Redisson's `MapCacheEvictionTask` takes the latch time in whole seconds, at least one.
fn eviction_arguments(latch: Option<Duration>) -> std::vec::Vec<Bytes> {
    let latch_seconds = latch.map_or(1, |latch| latch.as_secs().max(1));
    vec![
        Bytes::from(now_millis().to_string()),
        Bytes::from(KEYS_LIMIT.to_string()),
        Bytes::from(latch_seconds.to_string()),
        Bytes::from("1"),
        Bytes::from(PUBLISH),
    ]
}

/// The keys a script takes, listed at each call site in Redisson's order.
#[derive(Clone, Copy)]
enum Name {
    Main,
    Timeout,
    Idle,
    Created,
    Updated,
    Removed,
    Expired,
    LastAccess,
    Options,
    Lease,
}

/// A distributed map where each entry can expire, modelled on Redisson's `RMapCache`, and sharing its data with Redisson.
///
/// An entry can have a time to live ([`Insert::ttl`]) and a maximum idle time ([`Insert::max_idle`]). As in Redisson, the deadlines come from the clock of the client that writes or reads: a client whose clock is ahead sees entries expire earlier. A plain write replaces the entry and clears its limits, like [`std::collections::HashMap::insert`]. Reads skip expired entries at once but leave them in Redis; a background task deletes them, as Redisson's eviction task does. It first runs [`ClientBuilder::eviction_interval`](crate::ClientBuilder::eviction_interval) after the cache is first used in this client, deletes up to 100 entries per run, and adapts its pause between that interval and 30 minutes. A latch in Redis, held for that pause (between 1 and 30 seconds), lets only one client clean a cache per run.
///
/// [`HashMapCache::set_max_size`] limits the number of entries. The data lives in the hash `name`, each value stored as Redisson's `struct.pack('dLc0', maxIdleMs, #value, value)`. The deadlines are in the sorted sets `redissun__timeout__set:{name}` and `redissun__idle__set:{name}`, the uses in `redissun__map_cache__last_access__set:{name}` and the settings in `{name}:redissun_options`. A name that contains `{` is used as it is in every key, as in Redisson, so all keys are in one slot in Redis Cluster.
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
        companions(self.key.name())
    }

    fn renamed_key(&self, new_name: &str) -> String {
        new_name.to_string()
    }

    fn renamed_companions(&self, new_name: &str) -> Option<std::vec::Vec<String>> {
        Some(companions(new_name))
    }

    fn expire_script(&self) -> Option<&'static Lua> {
        Some(&EXPIRE)
    }

    fn expire_args(&self) -> std::vec::Vec<Bytes> {
        vec![Bytes::new()]
    }

    fn persist_script(&self) -> Option<&'static Lua> {
        Some(&CLEAR_EXPIRE)
    }
}

/// The keys besides the main hash, in the order of Redisson's `expire`, `clearExpire` and `delete`.
fn companions(name: &str) -> std::vec::Vec<String> {
    vec![
        names::timeout(name),
        names::idle(name),
        names::last_access(name),
        names::options(name),
    ]
}

impl<K, V, C: Codec> HashMapCache<K, V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        let cache = Self {
            key,
            codec,
            _marker: PhantomData,
        };
        let mut keys = cache.redis_keys(&[
            Name::Main,
            Name::Timeout,
            Name::Idle,
            Name::Expired,
            Name::LastAccess,
        ]);
        keys.push(names::latch(cache.key.name()));
        eviction::schedule(
            &cache.key.core,
            format!("redissun__map_cache_eviction:{}", cache.key.name()),
            &EVICT,
            keys,
            eviction_arguments,
        );
        cache
    }

    fn redis_keys(&self, set: &[Name]) -> std::vec::Vec<String> {
        let name = self.key.name();
        set.iter()
            .map(|which| match which {
                Name::Main => name.to_string(),
                Name::Timeout => names::timeout(name),
                Name::Idle => names::idle(name),
                Name::Created => names::channel(EventKind::Created, name),
                Name::Updated => names::channel(EventKind::Updated, name),
                Name::Removed => names::channel(EventKind::Removed, name),
                Name::Expired => names::channel(EventKind::Expired, name),
                Name::LastAccess => names::last_access(name),
                Name::Options => names::options(name),
                Name::Lease => names::lease(name),
            })
            .collect()
    }

    /// Sends `cmd`, routed by the options key, which is in the slot of the cache.
    async fn on_options<T: redis::FromRedisValue>(&self, cmd: redis::Cmd) -> Result<T> {
        let options = names::options(self.key.name());
        self.key
            .core
            .query(Request::new(cmd).key(options.as_bytes()))
            .await
    }

    /// How the server writes the lengths in event payloads, read once per client with `INFO server`, as Redisson does when the first listener is added.
    async fn length_prefix(&self) -> Result<LengthPrefix> {
        let prefix = self
            .key
            .core
            .map_cache_length_prefix
            .get_or_try_init(|| async {
                let mut cmd = redis::cmd("INFO");
                cmd.arg("server");
                let info: String = self.key.query(cmd).await?;
                Ok::<_, Error>(entry_events::length_prefix(&info))
            })
            .await?;
        Ok(*prefix)
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

    /// Inserts an entry and returns the live value it replaced. Without [`Insert::ttl`] and [`Insert::max_idle`] the entry never expires, and a limit that the old entry had is removed, like [`std::collections::HashMap::insert`] replaces the value.
    ///
    /// With a size limit, an insert without a time limit adds the entry and then drops the least used entries above the limit; an insert with a time limit first drops entries until there is room, even when it replaces a live entry, as Redisson does. A plain insert also frees a lease on the key, an insert with a time limit does not, as in Redisson.
    ///
    /// ```ignore
    /// cache.insert("a", &value).await?;
    /// cache.insert("b", &value).ttl(Duration::from_secs(60)).await?;
    /// cache.insert("c", &value).max_idle(Duration::from_secs(30)).await?;
    /// ```
    #[doc(alias = "put")]
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

    /// Inserts the entry only when there is no live entry for the key, like [`HashMap::try_insert`](std::collections::HashMap::try_insert). Resolves to `None` when it stored the entry, or to `Some` with the live value it left alone. It takes `.ttl(..)` and `.max_idle(..)` too.
    #[doc(alias = "putIfAbsent", alias = "fastPutIfAbsent")]
    pub fn try_insert<'a, Q, W>(&'a self, k: &'a Q, v: &'a W) -> TryInsert<'a, K, V, C, Q, W>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        TryInsert {
            cache: self,
            k,
            v,
            ttl: None,
            max_idle: None,
        }
    }

    /// Limits the cache to `max` entries. When it is full, an insert drops the least recently (`Lru`) or least often (`Lfu`) used entry. `0` removes the limit. The limit is stored in Redis and applies to every client. Only entries written or read while a limit is set are tracked, so older entries are never dropped for space.
    #[doc(alias = "setMaxSize")]
    pub async fn set_max_size(&self, max: usize, mode: EvictionMode) -> Result<()> {
        let mut cmd = redis::cmd("HSET");
        cmd.arg(names::options(self.key.name())).arg(vec![
            ("max-size", max.to_string()),
            ("mode", mode.as_str().to_string()),
        ]);
        let _: i64 = self.on_options(cmd).await?;
        Ok(())
    }

    /// Sets the size limit only when none was set before, like `OnceLock::set`; returns whether it did. Redisson's `trySetMaxSize`.
    #[doc(alias = "trySetMaxSize")]
    pub async fn init_max_size(&self, max: usize, mode: EvictionMode) -> Result<bool> {
        let set: i64 = self
            .key
            .core
            .eval_once(
                &TRY_SET_MAX_SIZE,
                self.redis_keys(&[Name::Options]),
                vec![Bytes::from(max.to_string()), Bytes::from(mode.as_str())],
            )
            .await?;
        Ok(set == 1)
    }

    /// Starts listening to every kind of change of the cache, made by any client. See [`HashMapCache::events_of`].
    #[doc(alias = "addListener")]
    pub async fn events(&self) -> Result<Events<K, V, C>> {
        self.events_of(&EventKind::ALL).await
    }

    /// Starts listening to the given kinds of change, made by any client.
    ///
    /// As in Redisson, the first listener marks the cache in Redis, and from then on every write publishes its changes until the cache is deleted with [`Object::del`](crate::Object::del). Expired entries are always published by the clean-up. The payload is Redisson's, so listeners see the changes Redisson clients make too.
    pub async fn events_of(&self, kinds: &[EventKind]) -> Result<Events<K, V, C>> {
        if !EventKind::ALL.iter().any(|kind| kinds.contains(kind)) {
            return Err(Error::Config("listen to at least one kind of event".into()));
        }
        let prefix = self.length_prefix().await?;
        let mut cmd = redis::cmd("HSET");
        cmd.arg(names::options(self.key.name()))
            .arg(("has-listeners", 1));
        let _: i64 = self.on_options(cmd).await?;
        let mut sources = std::vec::Vec::new();
        for kind in EventKind::ALL {
            if kinds.contains(&kind) {
                let (subscription, receiver) = self
                    .key
                    .core
                    .pubsub()
                    .await?
                    .subscribe_with_messages(&names::channel(kind, self.key.name()))
                    .await?;
                sources.push((kind, subscription, receiver));
            }
        }
        Ok(Events::new(sources, prefix, self.codec.clone()))
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
            .eval(
                &GET,
                self.redis_keys(&[
                    Name::Main,
                    Name::Timeout,
                    Name::Idle,
                    Name::LastAccess,
                    Name::Options,
                ]),
                vec![now(), self.codec.encode(k)?],
            )
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
            .eval(
                &REMOVE,
                self.redis_keys(&[
                    Name::Main,
                    Name::Timeout,
                    Name::Idle,
                    Name::Removed,
                    Name::LastAccess,
                    Name::Options,
                ]),
                vec![now(), self.codec.encode(k)?, Bytes::from(PUBLISH)],
            )
            .await?;
        self.decode_value(raw)
    }

    /// Returns whether a live entry exists for the key. Like a read, it starts the idle time again and counts as a use for the size limit.
    #[doc(alias = "containsKey")]
    pub async fn contains_key<Q>(&self, k: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let found: i64 = self
            .key
            .core
            .eval(
                &CONTAINS_KEY,
                self.redis_keys(&[
                    Name::Main,
                    Name::Timeout,
                    Name::Idle,
                    Name::LastAccess,
                    Name::Options,
                ]),
                vec![now(), self.codec.encode(k)?],
            )
            .await?;
        Ok(found == 1)
    }

    /// Time left until the entry expires, by its time to live or its idle time, whichever comes first. `None` means the entry has no limit or does not exist.
    #[doc(alias = "remainTimeToLive")]
    pub async fn entry_ttl<Q>(&self, k: &Q) -> Result<Option<Duration>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let millis: i64 = self
            .key
            .core
            .eval(
                &REMAIN_TTL,
                self.redis_keys(&[Name::Main, Name::Timeout, Name::Idle]),
                vec![now(), self.codec.encode(k)?],
            )
            .await?;
        Ok((millis >= 0).then(|| Duration::from_millis(millis as u64)))
    }

    /// Number of entries in Redis. As in Redisson, expired entries count until the clean-up deletes them.
    pub async fn len(&self) -> Result<usize> {
        let len: usize = self
            .key
            .query({
                let mut cmd = redis::cmd("HLEN");
                cmd.arg(self.key.redis_key());
                cmd
            })
            .await?;
        Ok(len)
    }

    /// Returns whether Redis holds no entry.
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// Deletes every entry. The size limit and the listener mark stay.
    pub async fn clear(&self) -> Result<()> {
        self.key
            .del_all(vec![
                names::timeout(self.key.name()),
                names::idle(self.key.name()),
                names::last_access(self.key.name()),
            ])
            .await?;
        Ok(())
    }

    /// One page of [`SCAN_ENTRIES`] or [`SCAN_KEYS`], then the idle refresh of the entries on it, which Redisson sends without waiting and redissun awaits before the page is returned.
    async fn scan_page(
        &self,
        script: &Lua,
        cursor: String,
    ) -> Result<(String, std::vec::Vec<Bytes>)> {
        let (next, items, idle): (String, std::vec::Vec<Bytes>, std::vec::Vec<Bytes>) = self
            .key
            .core
            .eval(
                script,
                self.redis_keys(&[Name::Main, Name::Timeout, Name::Idle]),
                vec![
                    now(),
                    Bytes::from(cursor),
                    Bytes::from(SCAN_PAGE.to_string()),
                ],
            )
            .await?;
        if !idle.is_empty() {
            let _: redis::Value = self
                .key
                .core
                .eval(
                    &TOUCH_IDLE,
                    self.redis_keys(&[Name::Main, Name::Idle]),
                    std::iter::once(now()).chain(idle).collect(),
                )
                .await?;
        }
        Ok((next, items))
    }

    /// Streams the live entries, 100 at a time, in no particular order. Expired entries are skipped page by page, and an entry with an idle limit that is returned starts its idle time again. An entry that is present the whole time is returned at least once and can be returned twice.
    pub fn iter(&self) -> impl Stream<Item = Result<(K, V)>> + '_ {
        stream::try_unfold(Some("0".to_string()), move |cursor| async move {
            let Some(cursor) = cursor else {
                return Ok::<_, Error>(None);
            };
            let (next, raw) = self.scan_page(&SCAN_ENTRIES, cursor).await?;
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

    /// Streams the live keys, like [`HashMapCache::iter`].
    pub fn keys(&self) -> impl Stream<Item = Result<K>> + '_ {
        stream::try_unfold(Some("0".to_string()), move |cursor| async move {
            let Some(cursor) = cursor else {
                return Ok::<_, Error>(None);
            };
            let (next, raw) = self.scan_page(&SCAN_KEYS, cursor).await?;
            let items = raw
                .iter()
                .map(|key| self.codec.decode(key))
                .collect::<Result<std::vec::Vec<K>>>()?;
            let next = (next != "0").then_some(next);
            Ok(Some((stream::iter(items.into_iter().map(Ok)), next)))
        })
        .try_flatten()
    }

    /// Streams the live values.
    pub fn values(&self) -> impl Stream<Item = Result<V>> + '_ {
        self.iter().map_ok(|(_, v)| v)
    }
}

/// The client's clock as a script argument.
fn now() -> Bytes {
    Bytes::from(now_millis().to_string())
}
