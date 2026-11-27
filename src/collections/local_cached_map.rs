use crate::client::core::Core;
use crate::client::request::Request;
use crate::codec::Codec;
use crate::collections::hash_map::HashMap;
use crate::collections::map::entry::Target;
use crate::collections::map::load::{load_value, LoadAll};
use crate::collections::map::store::{store_options, Store as MapStore, StoreOptions};
use crate::error::Result;
use crate::lua::Lua;
use crate::object::{tagged, HasKey, Key};
use crate::pubsub::{PatternMessage, Subscription};
use bytes::Bytes;
use futures::future::Either;
use futures::stream;
use futures::Stream;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap as StdHashMap};
use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use uuid::Uuid;

const ID_LEN: usize = 16;
const LOG_ID_LEN: usize = 8;
const INVALIDATE: u8 = b'I';
const UPDATE: u8 = b'U';
const CLEAR: u8 = b'C';
const UPDATES_LOG_TIME: Duration = Duration::from_secs(10 * 60);
const UPDATES_LOG_KEEP: Duration = Duration::from_secs(11 * 60);
const EXPIRED_EVENTS: &str = "__keyevent@*:expired";

static PUT: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local previous = redis.call('HGET', KEYS[1], ARGV[1])
        redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
        if ARGV[4] == '1' then
            redis.call('PUBLISH', ARGV[3], ARGV[5])
        end
        if ARGV[4] == '2' then
            redis.call('ZADD', KEYS[2], ARGV[6], ARGV[7])
            redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', '(' .. (tonumber(ARGV[6]) - tonumber(ARGV[8])))
            redis.call('PUBLISH', ARGV[3], ARGV[5])
        end
        return previous",
    )
});

static REMOVE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local previous = redis.call('HGET', KEYS[1], ARGV[1])
        if redis.call('HDEL', KEYS[1], ARGV[1]) == 1 then
            if ARGV[4] == '1' then
                redis.call('PUBLISH', ARGV[3], ARGV[5])
            end
            if ARGV[4] == '2' then
                redis.call('ZADD', KEYS[2], ARGV[6], ARGV[7])
                redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', '(' .. (tonumber(ARGV[6]) - tonumber(ARGV[8])))
                redis.call('PUBLISH', ARGV[3], ARGV[5])
            end
        end
        return previous",
    )
});

/// `del` in [`StoreMode::Local`]: Redis holds nothing to delete, so it only publishes `CLEAR` and returns whether the local cache held anything.
static CLEAR_LOCAL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "if ARGV[2] ~= '0' then
            redis.call('PUBLISH', ARGV[1], ARGV[3])
        end
        return tonumber(ARGV[4])",
    )
});

static CLEAR_ALL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "if redis.call('DEL', KEYS[1], KEYS[2]) > 0 and ARGV[2] ~= '0' then
            redis.call('PUBLISH', ARGV[1], ARGV[3])
            return 1
        end
        return 0",
    )
});

/// How a [`LocalCachedMap`] tells the other instances about a change, as Redisson's `SyncStrategy`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SyncStrategy {
    /// Other instances drop the changed entry and read it again when needed. The default.
    #[default]
    Invalidate,
    /// Other instances receive the new value and keep it.
    Update,
    /// Nothing is sent. Instances only learn about changes when an entry expires locally.
    None,
}

/// What a [`LocalCachedMap`] does with its local cache when its pub/sub connection comes back, as Redisson's `ReconnectionStrategy`. Messages sent while it was away are lost.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReconnectionStrategy {
    /// Nothing; entries changed meanwhile stay stale until they are changed again or expire locally. The default.
    #[default]
    None,
    /// The whole local cache is cleared.
    Clear,
    /// Every write also records the changed key in the sorted set `redissun__cache_updates_log:{name}` for 10 minutes. After a reconnect the keys changed since the last message are dropped, or the whole local cache when the instance was away longer than that or the map is gone.
    Load,
}

/// How a [`LocalCachedMap`] learns that its map expired in Redis, so it clears its local cache, Redisson's `ExpirationEventPolicy` (the deprecated `useKeyEventsPattern`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ExpirationEventPolicy {
    /// `PSUBSCRIBE __keyevent@*:expired`, shared by every map of the client; the server needs `notify-keyspace-events` with `Ex`. The default.
    #[default]
    #[doc(alias = "SUBSCRIBE_WITH_KEYEVENT_PATTERN")]
    KeyeventPattern,
    /// `SUBSCRIBE __keyspace@<database>__:<name>`, one channel for this map; the server needs `notify-keyspace-events` with `Kx`.
    #[doc(alias = "SUBSCRIBE_WITH_KEYSPACE_CHANNEL")]
    KeyspaceChannel,
    /// No subscription: the local cache is not cleared when the map expires.
    #[doc(alias = "DONT_SUBSCRIBE")]
    None,
}

/// The expiry notifications the listener of a [`LocalCachedMap`] reads, by [`ExpirationEventPolicy`]. The subscriptions are held for their `Drop`, which unsubscribes.
#[allow(dead_code)]
enum ExpiryFeed {
    Pattern(
        Subscription<PatternMessage>,
        broadcast::Receiver<PatternMessage>,
    ),
    Channel(Subscription, broadcast::Receiver<Bytes>),
    None,
}

/// What one expiry notification means for the local cache.
enum Expiry {
    /// The map expired, or messages were lost: clear the cache.
    Clear,
    /// Another key or event.
    Ignore,
    /// The feed ended.
    Closed,
}

impl ExpiryFeed {
    /// The next notification; never returns once the feed is [`ExpiryFeed::None`].
    async fn next(&mut self, key: &[u8]) -> Expiry {
        let received = match self {
            ExpiryFeed::Pattern(_, messages) => messages.recv().await.map(|(_, name)| name == key),
            ExpiryFeed::Channel(_, messages) => {
                messages.recv().await.map(|event| &event[..] == b"expired")
            }
            ExpiryFeed::None => std::future::pending().await,
        };
        match received {
            Ok(true) | Err(RecvError::Lagged(_)) => Expiry::Clear,
            Ok(false) => Expiry::Ignore,
            Err(RecvError::Closed) => Expiry::Closed,
        }
    }
}

/// Which cache holds the local entries of a [`LocalCachedMap`], Redisson's `CacheProvider`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum CacheProvider {
    /// redissun's own cache, with every [`EvictionPolicy`]. The default.
    #[default]
    #[doc(alias = "REDISSON")]
    Builtin,
    /// A [`moka`](https://docs.rs/moka) cache, as Redisson's Caffeine one: [`ttl`](LocalCachedMapBuilder::ttl) is moka's time to live, [`max_idle`](LocalCachedMapBuilder::max_idle) its time to idle, and a [`cache_size`](LocalCachedMapBuilder::cache_size) above zero its capacity, whatever the [`EvictionPolicy`]. Moka evicts in the background, so the cache may hold more entries for a short time. Needs the `moka` feature.
    #[cfg(feature = "moka")]
    #[cfg_attr(docsrs, doc(cfg(feature = "moka")))]
    #[doc(alias = "CAFFEINE")]
    Moka,
}

/// Where a [`LocalCachedMap`] keeps its entries, Redisson's `StoreMode`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StoreMode {
    /// In Redis and in the local cache. The default.
    #[default]
    #[doc(alias = "LOCALCACHE_REDIS")]
    LocalRedis,
    /// In the local cache only. Redis carries the sync messages, and the per-key lock when a loader is asked, but holds no entry. With [`SyncStrategy::Invalidate`], a change on one instance drops the only copy the others have.
    #[doc(alias = "LOCALCACHE")]
    Local,
}

/// Which local entry a [`LocalCachedMap`] drops when its cache is full, as Redisson's `EvictionPolicy`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EvictionPolicy {
    /// No limit: [`LocalCachedMapBuilder::cache_size`] is ignored. The default.
    #[default]
    None,
    /// The entry that was read or written longest ago.
    Lru,
    /// The entry that was read least often.
    Lfu,
}

struct Slot {
    value: Option<Bytes>,
    stored: Instant,
    touched: Instant,
    rank: (u64, u64),
}

struct Store {
    slots: StdHashMap<Bytes, Slot>,
    order: BTreeMap<(u64, u64), Bytes>,
    policy: EvictionPolicy,
    capacity: usize,
    ttl: Option<Duration>,
    max_idle: Option<Duration>,
    sequence: u64,
    baseline: u64,
}

impl Store {
    fn new(
        policy: EvictionPolicy,
        capacity: usize,
        ttl: Option<Duration>,
        max_idle: Option<Duration>,
    ) -> Self {
        Self {
            slots: StdHashMap::new(),
            order: BTreeMap::new(),
            policy,
            capacity: if policy == EvictionPolicy::None {
                0
            } else {
                capacity
            },
            ttl,
            max_idle,
            sequence: 0,
            baseline: 0,
        }
    }

    fn next_sequence(&mut self) -> u64 {
        self.sequence += 1;
        self.sequence
    }

    fn expired(&self, slot: &Slot, now: Instant) -> bool {
        self.ttl
            .is_some_and(|ttl| now.duration_since(slot.stored) >= ttl)
            || self
                .max_idle
                .is_some_and(|idle| now.duration_since(slot.touched) >= idle)
    }

    fn get(&mut self, key: &Bytes) -> Option<Option<Bytes>> {
        let now = Instant::now();
        let expired = self.expired(self.slots.get(key)?, now);
        if expired {
            self.remove(key);
            return None;
        }
        let sequence = self.next_sequence();
        let slot = self.slots.get_mut(key)?;
        slot.touched = now;
        let old = slot.rank;
        slot.rank = match self.policy {
            EvictionPolicy::Lfu => (old.0 + 1, old.1),
            _ => (0, sequence),
        };
        let rank = slot.rank;
        let value = slot.value.clone();
        self.order.remove(&old);
        self.order.insert(rank, key.clone());
        Some(value)
    }

    fn put(&mut self, key: Bytes, value: Option<Bytes>) {
        if self.capacity > 0
            && !self.slots.contains_key(&key)
            && self.slots.len() >= self.capacity
            && !self.remove_expired()
        {
            if let Some((rank, victim)) = self.order.pop_first() {
                self.slots.remove(&victim);
                if self.policy == EvictionPolicy::Lfu {
                    self.baseline = self.baseline.max(rank.0);
                }
            }
        }
        let sequence = self.next_sequence();
        let rank = match self.policy {
            EvictionPolicy::Lfu => (self.baseline, sequence),
            _ => (0, sequence),
        };
        let now = Instant::now();
        if let Some(old) = self.slots.insert(
            key.clone(),
            Slot {
                value,
                stored: now,
                touched: now,
                rank,
            },
        ) {
            self.order.remove(&old.rank);
        }
        self.order.insert(rank, key);
    }

    fn remove_expired(&mut self) -> bool {
        let now = Instant::now();
        let expired: std::vec::Vec<Bytes> = self
            .slots
            .iter()
            .filter(|(_, slot)| self.expired(slot, now))
            .map(|(key, _)| key.clone())
            .collect();
        expired.iter().for_each(|key| self.remove(key));
        !expired.is_empty()
    }

    fn remove(&mut self, key: &Bytes) {
        if let Some(slot) = self.slots.remove(key) {
            self.order.remove(&slot.rank);
        }
    }

    fn clear(&mut self) {
        self.slots.clear();
        self.order.clear();
    }

    fn entries(&mut self) -> std::vec::Vec<(Bytes, Bytes)> {
        self.remove_expired();
        self.slots
            .iter()
            .filter_map(|(key, slot)| Some((key.clone(), slot.value.clone()?)))
            .collect()
    }
}

/// The local cache of a [`LocalCachedMap`], by [`CacheProvider`]. A value of `None` is a cached miss.
enum LocalStore {
    Builtin(Store),
    #[cfg(feature = "moka")]
    Moka(moka::sync::Cache<Bytes, Option<Bytes>>),
}

impl LocalStore {
    fn new(
        provider: CacheProvider,
        policy: EvictionPolicy,
        capacity: usize,
        ttl: Option<Duration>,
        max_idle: Option<Duration>,
    ) -> Self {
        match provider {
            CacheProvider::Builtin => {
                LocalStore::Builtin(Store::new(policy, capacity, ttl, max_idle))
            }
            // Redisson's Caffeine branch: the eviction policy does not matter, the size limits the cache.
            #[cfg(feature = "moka")]
            CacheProvider::Moka => {
                let mut builder = moka::sync::Cache::builder();
                if let Some(ttl) = ttl {
                    builder = builder.time_to_live(ttl);
                }
                if let Some(idle) = max_idle {
                    builder = builder.time_to_idle(idle);
                }
                if capacity > 0 {
                    builder = builder.max_capacity(capacity as u64);
                }
                LocalStore::Moka(builder.build())
            }
        }
    }

    fn get(&mut self, key: &Bytes) -> Option<Option<Bytes>> {
        match self {
            LocalStore::Builtin(store) => store.get(key),
            #[cfg(feature = "moka")]
            LocalStore::Moka(cache) => cache.get(key),
        }
    }

    fn put(&mut self, key: Bytes, value: Option<Bytes>) {
        match self {
            LocalStore::Builtin(store) => store.put(key, value),
            #[cfg(feature = "moka")]
            LocalStore::Moka(cache) => cache.insert(key, value),
        }
    }

    fn remove(&mut self, key: &Bytes) {
        match self {
            LocalStore::Builtin(store) => store.remove(key),
            #[cfg(feature = "moka")]
            LocalStore::Moka(cache) => cache.invalidate(key),
        }
    }

    fn clear(&mut self) {
        match self {
            LocalStore::Builtin(store) => store.clear(),
            #[cfg(feature = "moka")]
            LocalStore::Moka(cache) => cache.invalidate_all(),
        }
    }

    /// Every slot, cached misses included.
    fn len(&mut self) -> usize {
        match self {
            LocalStore::Builtin(store) => store.slots.len(),
            #[cfg(feature = "moka")]
            LocalStore::Moka(cache) => {
                cache.run_pending_tasks();
                usize::try_from(cache.entry_count()).unwrap_or(usize::MAX)
            }
        }
    }

    /// The slots that hold a value.
    fn entries(&mut self) -> std::vec::Vec<(Bytes, Bytes)> {
        match self {
            LocalStore::Builtin(store) => store.entries(),
            #[cfg(feature = "moka")]
            LocalStore::Moka(cache) => {
                cache.run_pending_tasks();
                cache
                    .iter()
                    .filter_map(|(key, value)| Some(((*key).clone(), value?)))
                    .collect()
            }
        }
    }
}

/// The local cache and the reads from Redis in flight, under one lock: a change that arrives while a key is read marks the read, so its stale result is not cached.
struct Cache {
    store: LocalStore,
    /// Per key: the reads in flight, and whether a change arrived meanwhile.
    reads: StdHashMap<Bytes, (usize, bool)>,
}

impl Cache {
    fn invalidate(&mut self, key: &Bytes) {
        self.store.remove(key);
        if let Some(read) = self.reads.get_mut(key) {
            read.1 = true;
        }
    }

    fn clear(&mut self) {
        self.store.clear();
        self.reads.values_mut().for_each(|read| read.1 = true);
    }

    fn begin_read(&mut self, key: &Bytes) {
        self.reads.entry(key.clone()).or_insert((0, false)).0 += 1;
    }

    fn finish_read(&mut self, key: Bytes, value: Option<Option<Bytes>>) {
        let Some(read) = self.reads.get_mut(&key) else {
            return;
        };
        read.0 -= 1;
        let dirty = read.1;
        if read.0 == 0 {
            self.reads.remove(&key);
        }
        if let (false, Some(value)) = (dirty, value) {
            self.store.put(key, value);
        }
    }
}

/// A read of one key from Redis in flight; see [`Local::begin_read`].
struct ReadGuard<'a> {
    local: &'a Local,
    /// `None` once the read is finished.
    key: Option<Bytes>,
}

impl ReadGuard<'_> {
    /// Ends the read; `Some(value)` caches `value` unless the key changed meanwhile.
    fn finish(mut self, value: Option<Option<Bytes>>) {
        if let Some(key) = self.key.take() {
            self.local.cache().finish_read(key, value);
        }
    }
}

impl Drop for ReadGuard<'_> {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.local.cache().finish_read(key, None);
        }
    }
}

struct Local {
    cache: Mutex<Cache>,
    id: [u8; ID_LEN],
    reconnection: ReconnectionStrategy,
    last_invalidate: AtomicI64,
    listener: Mutex<Option<JoinHandle<()>>>,
}

impl Local {
    fn cache(&self) -> std::sync::MutexGuard<'_, Cache> {
        self.cache.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn lookup(&self, key: &Bytes) -> Option<Option<Bytes>> {
        self.cache().store.get(key)
    }

    fn write(&self, key: Bytes, value: Option<Bytes>) {
        let mut cache = self.cache();
        cache.invalidate(&key);
        cache.store.put(key, value);
    }

    /// Opens a read of `key` from Redis. The guard ends it: [`ReadGuard::finish`] with what was read, or its drop, caching nothing, when the read is cancelled or fails.
    fn begin_read(&self, key: &Bytes) -> ReadGuard<'_> {
        self.cache().begin_read(key);
        ReadGuard {
            local: self,
            key: Some(key.clone()),
        }
    }

    fn forget(&self, key: &Bytes) {
        self.cache().invalidate(key);
    }

    fn forget_all(&self) {
        self.cache().clear();
    }

    /// Every local slot, cached misses included.
    fn len(&self) -> usize {
        self.cache().store.len()
    }

    /// The local slots that hold a value, encoded.
    fn entries(&self) -> std::vec::Vec<(Bytes, Bytes)> {
        self.cache().store.entries()
    }

    fn apply(&self, message: &[u8]) {
        if message.len() < ID_LEN + 1 {
            return;
        }
        if message[..ID_LEN] != self.id {
            let body = &message[ID_LEN + 1..];
            match message[ID_LEN] {
                INVALIDATE => self.forget(&Bytes::copy_from_slice(body)),
                UPDATE if body.len() >= 4 => {
                    let key_len = u32::from_be_bytes([body[0], body[1], body[2], body[3]]) as usize;
                    if body.len() >= 4 + key_len {
                        let key = Bytes::copy_from_slice(&body[4..4 + key_len]);
                        let value = Bytes::copy_from_slice(&body[4 + key_len..]);
                        self.write(key, Some(value));
                    }
                }
                CLEAR => self.forget_all(),
                _ => {}
            }
        }
        if self.reconnection == ReconnectionStrategy::Load {
            self.last_invalidate.store(now_millis(), Ordering::Release);
        }
    }
}

impl Drop for Local {
    fn drop(&mut self) {
        let listener = self.listener.get_mut().unwrap_or_else(|e| e.into_inner());
        if let Some(listener) = listener.take() {
            listener.abort();
        }
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as i64)
}

fn message(id: &[u8; ID_LEN], kind: u8, body: &[&[u8]]) -> Bytes {
    let mut payload =
        std::vec::Vec::with_capacity(ID_LEN + 1 + body.iter().map(|p| p.len()).sum::<usize>());
    payload.extend_from_slice(id);
    payload.push(kind);
    body.iter().for_each(|part| payload.extend_from_slice(part));
    Bytes::from(payload)
}

fn channel(key: &Key) -> String {
    format!("redissun__local_cache:{}", key.redis_key())
}

fn updates_log(key: &Key) -> String {
    format!("redissun__cache_updates_log:{}", tagged(key.name()))
}

/// Sets up a [`LocalCachedMap`]. Get one from [`Client::local_cached_map`](crate::Client::local_cached_map). The defaults are Redisson's: no size limit, no local expiry, [`SyncStrategy::Invalidate`], [`ReconnectionStrategy::None`], [`EvictionPolicy::None`], no cached misses, and no loader or writer.
#[must_use = "a builder does nothing until build() is awaited"]
pub struct LocalCachedMapBuilder<K, V, C: Codec> {
    key: Key,
    codec: C,
    cache_size: usize,
    ttl: Option<Duration>,
    max_idle: Option<Duration>,
    sync: SyncStrategy,
    reconnection: ReconnectionStrategy,
    eviction: EvictionPolicy,
    store_cache_miss: bool,
    store_mode: StoreMode,
    expiration: ExpirationEventPolicy,
    provider: CacheProvider,
    options: StoreOptions<K, V>,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V, C: Codec> LocalCachedMapBuilder<K, V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self {
            key,
            codec,
            cache_size: 0,
            ttl: None,
            max_idle: None,
            sync: SyncStrategy::default(),
            reconnection: ReconnectionStrategy::default(),
            eviction: EvictionPolicy::default(),
            store_cache_miss: false,
            store_mode: StoreMode::default(),
            expiration: ExpirationEventPolicy::default(),
            provider: CacheProvider::default(),
            options: StoreOptions::default(),
            _marker: PhantomData,
        }
    }

    /// Keeps at most `size` entries locally; the [`EvictionPolicy`] picks the one to drop. Zero, the default, means no limit. It has no effect with [`EvictionPolicy::None`].
    pub fn cache_size(mut self, size: usize) -> Self {
        self.cache_size = size;
        self
    }

    /// Chooses which local entry is dropped when the cache is full. The default is [`EvictionPolicy::None`].
    pub fn eviction_policy(mut self, policy: EvictionPolicy) -> Self {
        self.eviction = policy;
        self
    }

    /// Drops a local entry this long after it was stored, whether it is read or not. By default entries stay until they are changed or pushed out.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = (!ttl.is_zero()).then_some(ttl);
        self
    }

    /// Drops a local entry that was not read for this long. By default there is no idle limit.
    pub fn max_idle(mut self, max_idle: Duration) -> Self {
        self.max_idle = (!max_idle.is_zero()).then_some(max_idle);
        self
    }

    /// Chooses how changes reach the other instances. The default is [`SyncStrategy::Invalidate`].
    pub fn sync_strategy(mut self, sync: SyncStrategy) -> Self {
        self.sync = sync;
        self
    }

    /// Chooses what happens to the local cache after the pub/sub connection is restored. The default is [`ReconnectionStrategy::None`].
    pub fn reconnection_strategy(mut self, reconnection: ReconnectionStrategy) -> Self {
        self.reconnection = reconnection;
        self
    }

    /// Also caches that a key is missing, so the next read of it costs no round trip. Off by default. With a loader, a key the loader does not have is cached as missing too.
    pub fn store_cache_miss(mut self, store: bool) -> Self {
        self.store_cache_miss = store;
        self
    }

    /// Chooses where the entries live: [`StoreMode::LocalRedis`], the default, or [`StoreMode::Local`].
    #[doc(alias = "storeMode")]
    pub fn store_mode(mut self, mode: StoreMode) -> Self {
        self.store_mode = mode;
        self
    }

    /// Chooses how the map learns that it expired in Redis. The default is [`ExpirationEventPolicy::KeyeventPattern`].
    #[doc(alias = "expirationEventPolicy", alias = "useKeyEventsPattern")]
    pub fn expiration_event_policy(mut self, policy: ExpirationEventPolicy) -> Self {
        self.expiration = policy;
        self
    }

    /// Chooses the cache that holds the local entries. The default is [`CacheProvider::Builtin`].
    #[doc(alias = "cacheProvider")]
    pub fn cache_provider(mut self, provider: CacheProvider) -> Self {
        self.provider = provider;
        self
    }
}

impl<K, V, C> LocalCachedMapBuilder<K, V, C>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    C: Codec,
{
    store_options!();
}

impl<K, V, C> LocalCachedMapBuilder<K, V, C>
where
    K: DeserializeOwned + Send + Sync + 'static,
    V: DeserializeOwned + Send + Sync + 'static,
    C: Codec,
{
    /// Checks the options, subscribes to the changes of the other instances and returns the map. Invalid store options are an [`Error::Config`](crate::Error::Config).
    pub async fn build(self) -> Result<LocalCachedMap<K, V, C>> {
        let store = self.options.into_store(&self.key, self.codec.clone())?;
        let local = Arc::new(Local {
            cache: Mutex::new(Cache {
                store: LocalStore::new(
                    self.provider,
                    self.eviction,
                    self.cache_size,
                    self.ttl,
                    self.max_idle,
                ),
                reads: StdHashMap::new(),
            }),
            id: *Uuid::new_v4().as_bytes(),
            reconnection: self.reconnection,
            last_invalidate: AtomicI64::new(0),
            listener: Mutex::new(None),
        });
        let pubsub = self.key.core.pubsub().await?.clone();
        let synced =
            self.sync != SyncStrategy::None || self.reconnection != ReconnectionStrategy::None;
        let (subscription, messages) = if synced {
            let (subscription, messages) =
                pubsub.subscribe_with_messages(&channel(&self.key)).await?;
            (Some(subscription), Some(messages))
        } else {
            (None, None)
        };
        let expiry = match self.expiration {
            ExpirationEventPolicy::KeyeventPattern => {
                let (subscription, messages) =
                    pubsub.psubscribe_with_messages(EXPIRED_EVENTS).await?;
                ExpiryFeed::Pattern(subscription, messages)
            }
            ExpirationEventPolicy::KeyspaceChannel => {
                let name = format!(
                    "__keyspace@{}__:{}",
                    self.key.core.database,
                    self.key.redis_key()
                );
                let (subscription, messages) = pubsub.subscribe_with_messages(&name).await?;
                ExpiryFeed::Channel(subscription, messages)
            }
            ExpirationEventPolicy::None => ExpiryFeed::None,
        };
        let reconnects = pubsub.reconnects();
        let handle = tokio::spawn(listen(
            Arc::downgrade(&local),
            Arc::downgrade(&self.key.core),
            vec![self.key.redis_key(), updates_log(&self.key)],
            subscription,
            messages,
            expiry,
            reconnects,
        ));
        *local.listener.lock().unwrap_or_else(|e| e.into_inner()) = Some(handle);
        Ok(LocalCachedMap {
            map: HashMap::new(self.key.clone(), self.codec.clone()),
            key: self.key,
            codec: self.codec,
            local,
            sync: self.sync,
            reconnection: self.reconnection,
            store_cache_miss: self.store_cache_miss,
            store_mode: self.store_mode,
            store,
            _marker: PhantomData,
        })
    }
}

async fn load_after_reconnection(local: &Local, core: Weak<Core>, keys: &[String]) {
    let since = local.last_invalidate.load(Ordering::Acquire);
    if since == 0 {
        return;
    }
    if now_millis() - since > UPDATES_LOG_TIME.as_millis() as i64 {
        local.forget_all();
        return;
    }
    let Some(core) = core.upgrade() else {
        return;
    };
    let mut cmd = redis::cmd("EXISTS");
    cmd.arg(&keys[0]);
    let exists: Result<i64> = core.query(Request::new(cmd).key(keys[0].as_bytes())).await;
    match exists {
        Ok(0) => local.forget_all(),
        Ok(_) => {
            let mut cmd = redis::cmd("ZRANGEBYSCORE");
            cmd.arg(&keys[1]).arg(since).arg("+inf");
            let changed: Result<std::vec::Vec<Bytes>> =
                core.query(Request::new(cmd).key(keys[1].as_bytes())).await;
            match changed {
                Ok(changed) => changed
                    .iter()
                    .filter(|entry| entry.len() >= LOG_ID_LEN)
                    .for_each(|entry| local.forget(&entry.slice(LOG_ID_LEN..))),
                Err(_) => local.forget_all(),
            }
        }
        Err(_) => local.forget_all(),
    }
}

async fn next<T: Clone>(
    receiver: &mut Option<broadcast::Receiver<T>>,
) -> std::result::Result<T, RecvError> {
    match receiver {
        Some(receiver) => receiver.recv().await,
        None => std::future::pending().await,
    }
}

async fn listen(
    local: Weak<Local>,
    core: Weak<Core>,
    keys: std::vec::Vec<String>,
    _subscription: Option<Subscription>,
    mut messages: Option<broadcast::Receiver<Bytes>>,
    mut expiry: ExpiryFeed,
    mut reconnects: broadcast::Receiver<()>,
) {
    loop {
        tokio::select! {
            received = next(&mut messages) => {
                let Some(local) = local.upgrade() else { return };
                match received {
                    Ok(payload) => local.apply(&payload),
                    Err(RecvError::Lagged(_)) => local.forget_all(),
                    Err(RecvError::Closed) => return,
                }
            }
            received = expiry.next(keys[0].as_bytes()) => {
                let Some(local) = local.upgrade() else { return };
                match received {
                    Expiry::Clear => local.forget_all(),
                    Expiry::Ignore => {}
                    Expiry::Closed => expiry = ExpiryFeed::None,
                }
            }
            reconnected = reconnects.recv() => {
                let Some(local) = local.upgrade() else { return };
                if matches!(reconnected, Err(RecvError::Closed)) {
                    return;
                }
                match local.reconnection {
                    ReconnectionStrategy::None => {}
                    ReconnectionStrategy::Clear => local.forget_all(),
                    ReconnectionStrategy::Load => {
                        load_after_reconnection(&local, core.clone(), &keys).await
                    }
                }
            }
        }
    }
}

/// A [`HashMap`] with a cache inside this program, modelled on Redisson's `RLocalCachedMap`. Reads that hit the cache cost no network round trip.
///
/// Every write goes to Redis and, when something changed, publishes a message in the same Lua script, so the other instances of the same map can drop or update their copy (see [`SyncStrategy`]). An instance never reacts to its own messages, and its own writes update its cache. What happens after a lost pub/sub connection is set by [`ReconnectionStrategy`]; when this client falls behind on the messages, the whole local cache is cleared.
///
/// Only changes made through a `LocalCachedMap` are announced. Changes made through a plain [`HashMap`] of the same name are not, so other instances see them only when their entry expires or is pushed out. [`LocalCachedMap::clear`] and [`Object::del`](crate::Object::del) on a `LocalCachedMap` empty the map for everybody.
///
/// As in Redisson, every instance also listens for `__keyevent@*:expired` by default and clears its local cache when the map's key expires; [`ExpirationEventPolicy`] chooses another channel or none. Redis sends these events only when `notify-keyspace-events` contains `Ex` (`Kx` for [`ExpirationEventPolicy::KeyspaceChannel`]); neither Redisson nor this crate sets it.
///
/// The data in Redis is a normal hash, so a [`HashMap`] with the same name and codec reads the same entries.
///
/// A map built with [`loader`](LocalCachedMapBuilder::loader) or [`writer`](LocalCachedMapBuilder::writer) reads through and writes through (or behind), as Redisson's `LocalCachedMapOptions`; see the "Loaders and writers" guide.
pub struct LocalCachedMap<K, V, C: Codec> {
    /// Redis-side reads; it has no store, so the writer is called once, by this map.
    map: HashMap<K, V, C>,
    key: Key,
    codec: C,
    local: Arc<Local>,
    sync: SyncStrategy,
    reconnection: ReconnectionStrategy,
    store_cache_miss: bool,
    store_mode: StoreMode,
    /// The loader and writer; `None` keeps every call as it is without them.
    store: Option<Arc<MapStore<K, V, C>>>,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V, C: Codec> fmt::Debug for LocalCachedMap<K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "LocalCachedMap")
    }
}

impl<K, V, C: Codec> HasKey for LocalCachedMap<K, V, C> {
    fn key(&self) -> &Key {
        &self.key
    }

    fn del_script(&self) -> Option<(&'static Lua, std::vec::Vec<String>, std::vec::Vec<Bytes>)> {
        if self.store_mode == StoreMode::Local {
            let held = self.local.len() > 0;
            self.local.forget_all();
            let mut arguments = self.clear_arguments();
            arguments.push(Bytes::from(if held { "1" } else { "0" }));
            return Some((&CLEAR_LOCAL, self.clear_keys(), arguments));
        }
        self.local.forget_all();
        Some((&CLEAR_ALL, self.clear_keys(), self.clear_arguments()))
    }
}

impl<K, V, C: Codec> LocalCachedMap<K, V, C> {
    fn mode(&self) -> Bytes {
        Bytes::from(if self.reconnection == ReconnectionStrategy::Load {
            "2"
        } else if self.sync != SyncStrategy::None {
            "1"
        } else {
            "0"
        })
    }

    /// Whether a change is announced: the scripts' `mode != "0"`.
    fn announces(&self) -> bool {
        self.sync != SyncStrategy::None || self.reconnection == ReconnectionStrategy::Load
    }

    /// Whether the entries live in the local cache only.
    fn local_only(&self) -> bool {
        self.store_mode == StoreMode::Local
    }

    /// Publishes a sync message on the map's channel, routed by the channel's slot, as fred did.
    async fn publish(&self, payload: Bytes) -> Result<()> {
        let channel = channel(&self.key);
        let mut cmd = redis::cmd("PUBLISH");
        cmd.arg(&channel).arg(&payload[..]);
        let _: i64 = self
            .key
            .core
            .query(Request::new(cmd).key(channel.as_bytes()))
            .await?;
        Ok(())
    }

    fn clear_keys(&self) -> std::vec::Vec<String> {
        vec![self.key.redis_key(), updates_log(&self.key)]
    }

    fn clear_arguments(&self) -> std::vec::Vec<Bytes> {
        vec![
            Bytes::from(channel(&self.key)),
            self.mode(),
            message(&self.local.id, CLEAR, &[]),
        ]
    }
}

impl<K, V, C> LocalCachedMap<K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    /// Whether a miss asks the loader.
    fn loads(&self) -> bool {
        self.store.as_ref().is_some_and(|store| store.has_loader())
    }

    /// The store when it has a writer; `None` skips every writer call.
    fn writer(&self) -> Option<&MapStore<K, V, C>> {
        self.store.as_deref().filter(|store| store.has_writer())
    }

    pub(crate) fn store(&self) -> Option<&MapStore<K, V, C>> {
        self.store.as_deref()
    }

    /// Asks the loader for a key that missed, for a read that [`Local::begin_read`] opened. A loaded value is cached by [`put_encoded`](Self::put_encoded); a value another caller stored meanwhile is cached here, and a key the loader lacks only with `store_cache_miss`, as Redisson's `getAsync`.
    async fn load_missing(&self, field: Bytes, read: ReadGuard<'_>) -> Result<Option<V>> {
        let loaded = load_value(&Target::Local(self), &field, false).await;
        let cached = match &loaded {
            Ok(Some(value)) => self.codec.encode(value).ok().map(Some),
            Ok(None) if self.store_cache_miss => Some(None),
            _ => None,
        };
        read.finish(cached);
        loaded
    }

    /// Reads an encoded key: the local cache, then the master unless the entries live locally only.
    pub(crate) async fn read_encoded(&self, k: &Bytes) -> Result<Option<V>> {
        if let Some(Some(value)) = self.local.lookup(k) {
            return Ok(Some(self.codec.decode(&value)?));
        }
        if self.local_only() {
            return Ok(None);
        }
        let mut cmd = self.key.key_cmd("HGET");
        cmd.arg(&k[..]);
        let raw: Option<Bytes> = self.key.query(cmd).await?;
        raw.map(|raw| self.codec.decode(&raw)).transpose()
    }

    /// Stores a value the way [`insert`](Self::insert) does, local cache and announcement included, without the writer.
    pub(crate) async fn put_encoded(&self, k: &Bytes, v: &V) -> Result<()> {
        self.put_raw(k.clone(), self.codec.encode(v)?)
            .await
            .map(drop)
    }

    /// Removes a key the way [`remove`](Self::remove) does, without the writer.
    pub(crate) async fn delete_encoded(&self, k: &Bytes) -> Result<()> {
        self.remove_raw(k.clone()).await.map(drop)
    }

    /// The local write and the `PUT` script; returns the previous encoded value. Locally only, the sync message is published by itself and the previous value is the local one.
    async fn put_raw(&self, field: Bytes, value: Bytes) -> Result<Option<Bytes>> {
        let notice = match self.sync {
            SyncStrategy::Update => message(
                &self.local.id,
                UPDATE,
                &[&(field.len() as u32).to_be_bytes(), &field, &value],
            ),
            _ => message(&self.local.id, INVALIDATE, &[&field]),
        };
        if self.local_only() {
            let previous = self.local.lookup(&field).flatten();
            self.local.write(field, Some(value));
            if self.announces() {
                self.publish(notice).await?;
            }
            return Ok(previous);
        }
        self.local.write(field.clone(), Some(value.clone()));
        let [now, entry, keep] = self.log_arguments(&field);
        let written: Result<Option<Bytes>> = self
            .key
            .core
            .eval(
                &PUT,
                self.clear_keys(),
                vec![
                    field.clone(),
                    value,
                    Bytes::from(channel(&self.key)),
                    self.mode(),
                    notice,
                    now,
                    entry,
                    keep,
                ],
            )
            .await;
        written.inspect_err(|_| self.local.forget(&field))
    }

    /// The local forget and the `REMOVE` script; returns the previous encoded value. Locally only, the invalidation is published by itself and the previous value is the local one.
    async fn remove_raw(&self, field: Bytes) -> Result<Option<Bytes>> {
        let notice = message(&self.local.id, INVALIDATE, &[&field]);
        if self.local_only() {
            let previous = self.local.lookup(&field).flatten();
            self.local.forget(&field);
            if self.announces() {
                self.publish(notice).await?;
            }
            return Ok(previous);
        }
        self.local.forget(&field);
        let [now, entry, keep] = self.log_arguments(&field);
        self.key
            .core
            .eval(
                &REMOVE,
                self.clear_keys(),
                vec![
                    field.clone(),
                    Bytes::new(),
                    Bytes::from(channel(&self.key)),
                    self.mode(),
                    notice,
                    now,
                    entry,
                    keep,
                ],
            )
            .await
    }

    fn log_arguments(&self, field: &Bytes) -> [Bytes; 3] {
        let mut entry = Uuid::new_v4().as_bytes()[..LOG_ID_LEN].to_vec();
        entry.extend_from_slice(field);
        [
            Bytes::from(now_millis().to_string()),
            Bytes::from(entry),
            Bytes::from(UPDATES_LOG_KEEP.as_millis().to_string()),
        ]
    }

    /// Returns the value for a key, from the local cache when it is there. A value read from Redis is cached. The key is borrowed: `map.get("k")`. With a loader, a miss asks the loader, and the value it returns is stored in Redis and cached.
    pub async fn get<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let field = self.codec.encode(k)?;
        match self.local.lookup(&field) {
            Some(Some(value)) => return Ok(Some(self.codec.decode(&value)?)),
            Some(None) if self.store_cache_miss => return Ok(None),
            _ => {}
        }
        if self.local_only() {
            if !self.loads() {
                return Ok(None);
            }
            let read = self.local.begin_read(&field);
            return self.load_missing(field, read).await;
        }
        let read = self.local.begin_read(&field);
        let mut cmd = self.key.key_cmd("HGET");
        cmd.arg(&field[..]);
        // An error or a cancelled call drops `read`, which caches nothing.
        let raw: Option<Bytes> = self.key.query(cmd).await?;
        if raw.is_none() && self.loads() {
            return self.load_missing(field, read).await;
        }
        let cached = (raw.is_some() || self.store_cache_miss).then(|| raw.clone());
        read.finish(cached);
        raw.map(|raw| self.codec.decode(&raw)).transpose()
    }

    /// Writes a value and returns the previous one. The local cache of this instance holds the new value, and the other instances are told. Both arguments are borrowed. With a writer, the entry is written after Redis stored it.
    pub async fn insert<Q, W>(&self, k: &Q, v: &W) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        let field = self.codec.encode(k)?;
        let value = self.codec.encode(v)?;
        let previous = self.put_raw(field.clone(), value.clone()).await?;
        if let Some(store) = self.writer() {
            store.written(&[(field, value)]).await?;
        }
        previous.map(|bytes| self.codec.decode(&bytes)).transpose()
    }

    /// Removes a key and returns its value. The other instances are told when the key existed. With a writer, the key is deleted from the store also when the map did not have it, as in Redisson.
    pub async fn remove<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let field = self.codec.encode(k)?;
        let previous = self.remove_raw(field.clone()).await?;
        if let Some(store) = self.writer() {
            store.deleted(&[field]).await?;
        }
        previous.map(|bytes| self.codec.decode(&bytes)).transpose()
    }

    /// Returns whether the key exists. A cached key answers without a round trip. With a loader, a miss asks the loader, and a loaded value counts as present.
    pub async fn contains_key<Q>(&self, k: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let field = self.codec.encode(k)?;
        if let Some(cached) = self.local.lookup(&field) {
            return Ok(cached.is_some());
        }
        if self.local_only() {
            if !self.loads() {
                return Ok(false);
            }
            let read = self.local.begin_read(&field);
            return Ok(self.load_missing(field, read).await?.is_some());
        }
        let read = self.local.begin_read(&field);
        // From the master, as `get`: a miss read from a replica could be cached after the write that ends it.
        let mut cmd = self.key.key_cmd("HEXISTS");
        cmd.arg(&field[..]);
        let found: Result<bool> = self.key.query(cmd).await;
        if matches!(found, Ok(false)) && self.loads() {
            return Ok(self.load_missing(field, read).await?.is_some());
        }
        let miss = matches!(found, Ok(false)) && self.store_cache_miss;
        read.finish(miss.then_some(None));
        found
    }

    /// Number of entries in Redis. In [`StoreMode::Local`], the local entries that hold a value.
    pub async fn len(&self) -> Result<usize> {
        if self.local_only() {
            return Ok(self.local.entries().len());
        }
        self.map.len().await
    }

    /// Returns whether Redis holds no entry. In [`StoreMode::Local`], whether no local entry holds a value.
    pub async fn is_empty(&self) -> Result<bool> {
        if self.local_only() {
            return Ok(self.local.entries().is_empty());
        }
        self.map.is_empty().await
    }

    /// Removes every entry for everybody; returns whether there was anything to remove. The other instances empty their local cache only when something was removed. In [`StoreMode::Local`], it empties the local cache, tells the others, and returns whether the local cache held anything.
    pub async fn clear(&self) -> Result<bool> {
        if self.local_only() {
            let held = self.local.len() > 0;
            self.local.forget_all();
            if self.announces() {
                self.publish(message(&self.local.id, CLEAR, &[])).await?;
            }
            return Ok(held);
        }
        self.local.forget_all();
        self.key
            .del_with(&CLEAR_ALL, self.clear_keys(), self.clear_arguments())
            .await
    }

    /// Number of entries in the local cache of this instance, including cached misses.
    pub fn local_len(&self) -> usize {
        self.local.len()
    }

    /// Returns the entries in the local cache of this instance. Redisson's `getCachedMap`.
    #[doc(alias = "getCachedMap")]
    pub fn local_entries(&self) -> Result<std::vec::Vec<(K, V)>> {
        let entries = self.local.entries();
        entries
            .iter()
            .map(|(key, value)| Ok((self.codec.decode(key)?, self.codec.decode(value)?)))
            .collect()
    }

    /// Empties the local cache of this instance only.
    pub fn clear_local(&self) {
        self.local.forget_all();
    }

    /// Empties the local cache of this instance and tells every other instance to empty theirs, like Redisson's `clearLocalCache`. Redis is not changed. It returns once the message is published, without waiting for the others.
    pub async fn clear_local_everywhere(&self) -> Result<()> {
        self.local.forget_all();
        self.publish(message(&self.local.id, CLEAR, &[])).await
    }

    /// Loads every key of the loader into the map, Redisson's `loadAll(replaceExistingValues, parallelism)`. A loaded value is cached here and announced like an [`insert`](LocalCachedMap::insert), without the writer. Without a loader it is an [`Error::Config`](crate::Error::Config).
    #[doc(alias = "loadAll")]
    pub fn load_all(&self) -> LoadAll<'_, K, V, C> {
        LoadAll::new(Target::Local(self), None)
    }

    /// Loads the given keys into the map, Redisson's `loadAll(keys, replaceExistingValues, parallelism)`.
    #[doc(alias = "loadAll")]
    pub fn load_many<Q>(&self, ks: &[&Q]) -> LoadAll<'_, K, V, C>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let keys = ks.iter().map(|k| self.codec.encode(*k)).collect();
        LoadAll::new(Target::Local(self), Some(keys))
    }

    /// Hands every write-behind change still queued to the writer now and returns the first writer error, as [`HashMap::flush`]. Without write-behind it does nothing.
    pub async fn flush(&self) -> Result<()> {
        match &self.store {
            Some(store) => store.drain().await,
            None => Ok(()),
        }
    }

    /// The encoded local entries that hold a value, for the streams in [`StoreMode::Local`].
    fn snapshot(&self) -> std::vec::IntoIter<(Bytes, Bytes)> {
        self.local.entries().into_iter()
    }

    /// Streams every entry from Redis. It does not use or fill the local cache. In [`StoreMode::Local`], it streams a snapshot of the local entries.
    pub fn iter(&self) -> impl Stream<Item = Result<(K, V)>> + '_ {
        if self.local_only() {
            return Either::Right(stream::iter(
                self.snapshot()
                    .map(|(k, v)| Ok((self.codec.decode(&k)?, self.codec.decode(&v)?))),
            ));
        }
        Either::Left(self.map.iter())
    }

    /// Streams every key from Redis. In [`StoreMode::Local`], the keys of the local entries.
    pub fn keys(&self) -> impl Stream<Item = Result<K>> + '_ {
        if self.local_only() {
            return Either::Right(stream::iter(
                self.snapshot().map(|(k, _)| self.codec.decode(&k)),
            ));
        }
        Either::Left(self.map.keys())
    }

    /// Streams every value from Redis. In [`StoreMode::Local`], the values of the local entries.
    pub fn values(&self) -> impl Stream<Item = Result<V>> + '_ {
        if self.local_only() {
            return Either::Right(stream::iter(
                self.snapshot().map(|(_, v)| self.codec.decode(&v)),
            ));
        }
        Either::Left(self.map.values())
    }
}
