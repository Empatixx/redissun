mod announced;
mod builders;
pub(crate) mod entry_events;
mod lease;
mod local;
mod local_mode;
mod names;
mod scripts;

use crate::client::request::Request;
use crate::codec::Codec;
use crate::collections::local_cache::messages::{message, updates_log_name, CLEAR};
use crate::collections::local_cache::{EvictionFeed, LocalCache};
use crate::collections::map::entry::Target;
use crate::collections::map::events::{self as map_events, MapEventKind, MapEvents};
use crate::collections::map::key_hash::per_key_objects;
use crate::collections::map::load::{load_value, LoadAll};
use crate::collections::map::scan::{decode_pairs, MapScan};
use crate::collections::map::store::{store_options, Store, StoreOptions};
use crate::error::{Error, Result};
use crate::eviction::{self, KEYS_LIMIT};
use crate::lua::Lua;
use crate::object::{millis, now_millis, HasKey, Key};
use bytes::Bytes;
use futures::{stream, Stream, StreamExt, TryStreamExt};
use scripts::{
    ADD_AND_GET, CLEAR_EXPIRE, CONTAINS_KEY, CONTAINS_VALUE, EVICT, EXPIRE, FAST_PUT_WITH_LIMITS,
    FAST_REMOVE, FAST_REMOVE_BATCH, GET, GET_ALL, GET_ALL_WITH_TTL_ONLY, GET_WITH_TTL_ONLY, PUT,
    RANDOM_ENTRIES, RANDOM_KEYS, READ_ALL, REMAIN_TTL, REMOVE, REMOVE_IF, REPLACE, REPLACE_IF,
    SCAN_ENTRIES, SCAN_KEYS, TOUCH_IDLE, TRY_SET_MAX_SIZE,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

pub use builders::{
    EntryRequest, ExpireEntries, ExpireEntry, Extend, Insert, Migrate, Set, TryInsert,
};
pub(crate) use entry_events::LengthPrefix;
pub use lease::{Lease, LeaseGuard, LeaseInsert};
pub(crate) use local::Op;

/// The field `ADD_AND_GET` writes and deletes, Redisson's `temp_val__redisson`.
const TEMP_VAL_FIELD: &[u8] = b"redissun__temp_val";
pub use entry_events::{EntryEvent, EntryEventKind, EntryEvents};

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
/// A cache from [`Client::hash_map_cache_builder`](crate::Client::hash_map_cache_builder) can have a [`MapLoader`](crate::MapLoader) and a [`MapWriter`](crate::MapWriter); see the "Loaders and writers" guide.
///
/// [`HashMapCache::set_max_size`] limits the number of entries. The data lives in the hash `name`, each value stored as Redisson's `struct.pack('dLc0', maxIdleMs, #value, value)`. The deadlines are in the sorted sets `redissun__timeout__set:{name}` and `redissun__idle__set:{name}`, the uses in `redissun__map_cache__last_access__set:{name}` and the settings in `{name}:redissun_options`. A name that contains `{` is used as it is in every key, as in Redisson, so all keys are in one slot in Redis Cluster.
pub struct HashMapCache<K, V, C: Codec> {
    key: Key,
    codec: C,
    /// The loader and writer; `None` keeps every call as it is without them.
    store: Option<Arc<Store<K, V, C>>>,
    /// The local cache of a [`LocalCachedMapCache`](crate::LocalCachedMapCache); `None` keeps every call as Redisson's `RMapCache` sends it.
    local: Option<Arc<LocalCache>>,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V, C: Codec> Clone for HashMapCache<K, V, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
            store: self.store.clone(),
            local: self.local.clone(),
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
        let mut keys = companions(self.key.name());
        if self.local.is_some() {
            keys.push(updates_log_name(self.key.name()));
        }
        keys
    }

    fn renamed_key(&self, new_name: &str) -> String {
        new_name.to_string()
    }

    fn renamed_companions(&self, new_name: &str) -> Option<std::vec::Vec<String>> {
        let mut keys = companions(new_name);
        if self.local.is_some() {
            keys.push(updates_log_name(new_name));
        }
        Some(keys)
    }

    fn del_script(&self) -> Option<(&'static Lua, std::vec::Vec<String>, std::vec::Vec<Bytes>)> {
        let local = self.local.as_ref()?;
        if local.local_only() {
            // Redis holds nothing: only the clear is published, and the reply says whether the local cache held a value.
            let held = !local.local.entries().is_empty();
            local.local.forget_all();
            let mut arguments = clear_arguments(local);
            arguments.push(Bytes::from(if held { "1" } else { "0" }));
            return Some((
                &crate::collections::local_cached_map::CLEAR_LOCAL,
                vec![self.key.redis_key()],
                arguments,
            ));
        }
        local.local.forget_all();
        let mut keys = self.redis_keys(&[
            Name::Main,
            Name::Timeout,
            Name::Idle,
            Name::LastAccess,
            Name::Options,
        ]);
        keys.push(local.updates_log.clone());
        Some((&announced::DEL_ANNOUNCED, keys, clear_arguments(local)))
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

/// [`HashMapCache::length_prefix`] for the cache under `key`.
pub(crate) async fn length_prefix_of(key: &Key) -> Result<LengthPrefix> {
    let prefix = key
        .core
        .map_cache_length_prefix
        .get_or_try_init(|| async {
            let mut cmd = redis::cmd("INFO");
            cmd.arg("server");
            let info: String = key.query(cmd).await?;
            Ok::<_, Error>(entry_events::length_prefix(&info))
        })
        .await?;
    Ok(*prefix)
}

/// Subscribes to the removed and expired entry events of the cache under `key`, for a local cache to forget the entries the server drops.
pub(crate) async fn eviction_feed(key: &Key) -> Result<EvictionFeed> {
    let prefix = length_prefix_of(key).await?;
    let pubsub = key.core.pubsub().await?;
    let (mut subscriptions, mut receivers) = (std::vec::Vec::new(), std::vec::Vec::new());
    for kind in [EntryEventKind::Removed, EntryEventKind::Expired] {
        let (subscription, receiver) = pubsub
            .subscribe_with_messages(&names::channel(kind, key.name()))
            .await?;
        subscriptions.push(subscription);
        receivers.push(receiver);
    }
    Ok(EvictionFeed::new(subscriptions, receivers, prefix))
}

/// Marks the cache `key` in Redis so every client's writes publish their entry events, as Redisson's `addListener` does.
pub(crate) async fn mark_listeners(key: &Key) -> Result<()> {
    let options = names::options(key.name());
    let mut cmd = redis::cmd("HSET");
    cmd.arg(&options).arg(("has-listeners", 1));
    let _: i64 = key
        .core
        .query(Request::new(cmd).key(options.as_bytes()))
        .await?;
    Ok(())
}

/// `channel, mode, clear message`, the ARGV of [`announced::CLEAR_ANNOUNCED`] and [`announced::DEL_ANNOUNCED`].
fn clear_arguments(local: &LocalCache) -> std::vec::Vec<Bytes> {
    vec![
        Bytes::from(local.channel.clone()),
        local.mode(),
        message(&local.local.id, CLEAR, &[]),
    ]
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

/// The keys whose existence shows that `name` still holds redissun 0.23 data, all in the slot of the cache: the old options hash and last-access set and, for a name without `{`, the old main hash. The clean-up waits while one exists, as it would drop the deadlines of entries still in the old layout and the migration would then move them as live.
fn old_layout_keys(name: &str) -> std::vec::Vec<String> {
    let mut keys = vec![names::old_options(name), names::old_last_access(name)];
    if !name.contains('{') {
        keys.push(names::old_main(name));
    }
    keys
}

impl<K, V, C: Codec> HashMapCache<K, V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self::with_store(key, codec, None)
    }

    /// A cache with the loader and writer of `store`; like [`new`](HashMapCache::new), it schedules the clean-up.
    pub(crate) fn with_store(key: Key, codec: C, store: Option<Arc<Store<K, V, C>>>) -> Self {
        Self::with_local(key, codec, store, None)
    }

    /// A cache with the loader and writer of `store` and, for a [`LocalCachedMapCache`](crate::LocalCachedMapCache), a local cache; like [`new`](HashMapCache::new), it schedules the clean-up.
    pub(crate) fn with_local(
        key: Key,
        codec: C,
        store: Option<Arc<Store<K, V, C>>>,
        local: Option<Arc<LocalCache>>,
    ) -> Self {
        let cache = Self {
            key,
            codec,
            store,
            local,
            _marker: PhantomData,
        };
        if cache.local_only().is_some() {
            // Redis holds no entries to clean up.
            return cache;
        }
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
            old_layout_keys(cache.key.name()),
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
                Name::Created => names::channel(EntryEventKind::Created, name),
                Name::Updated => names::channel(EntryEventKind::Updated, name),
                Name::Removed => names::channel(EntryEventKind::Removed, name),
                Name::Expired => names::channel(EntryEventKind::Expired, name),
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
    pub(crate) async fn length_prefix(&self) -> Result<LengthPrefix> {
        length_prefix_of(&self.key).await
    }

    pub(crate) fn store(&self) -> Option<&Store<K, V, C>> {
        self.store.as_deref()
    }

    /// Whether a miss asks the loader.
    fn loads(&self) -> bool {
        self.store.as_ref().is_some_and(|store| store.has_loader())
    }

    /// The store when it has a writer; `None` skips every writer call.
    pub(super) fn writer(&self) -> Option<&Store<K, V, C>> {
        self.store.as_deref().filter(|store| store.has_writer())
    }
}

/// Sets up a [`HashMapCache`] with a [`MapLoader`](crate::MapLoader) or a [`MapWriter`](crate::MapWriter), Redisson's `getMapCache(name, MapCacheOptions)`. Get one from [`Client::hash_map_cache_builder`](crate::Client::hash_map_cache_builder), set its options, then `.build().await`.
#[must_use = "a builder does nothing until build() is awaited"]
pub struct HashMapCacheBuilder<K, V, C: Codec> {
    key: Key,
    codec: C,
    options: StoreOptions<K, V>,
}

impl<K, V, C: Codec> fmt::Debug for HashMapCacheBuilder<K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HashMapCacheBuilder")
            .field("name", &self.key.name())
            .field("options", &self.options)
            .finish()
    }
}

impl<K, V, C: Codec> HashMapCacheBuilder<K, V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self {
            key,
            codec,
            options: StoreOptions::default(),
        }
    }
}

impl<K, V, C> HashMapCacheBuilder<K, V, C>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    C: Codec,
{
    store_options!();
}

impl<K, V, C> HashMapCacheBuilder<K, V, C>
where
    K: DeserializeOwned + Send + Sync + 'static,
    V: DeserializeOwned + Send + Sync + 'static,
    C: Codec,
{
    /// Checks the options and returns the cache. Invalid options are an [`Error::Config`](crate::Error::Config).
    pub async fn build(self) -> Result<HashMapCache<K, V, C>> {
        let store = self.options.into_store(&self.key, self.codec.clone())?;
        Ok(HashMapCache::with_store(self.key, self.codec, store))
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

    /// Hands stored entries to the writer, if there is one.
    pub(super) async fn written(&self, entries: &[(Bytes, Bytes)]) -> Result<()> {
        match self.writer() {
            Some(store) => store.written(entries).await,
            None => Ok(()),
        }
    }

    /// Hands a removed key to the writer, if there is one.
    async fn deleted(&self, k: Bytes) -> Result<()> {
        match self.writer() {
            Some(store) => store.deleted(&[k]).await,
            None => Ok(()),
        }
    }

    /// Asks the loader for the missing values, one key at a time, in the order of the keys.
    async fn load_missing(&self, values: &mut [Option<V>], keys: &[Bytes]) -> Result<()> {
        if self.loads() {
            for (value, k) in values.iter_mut().zip(keys) {
                if value.is_none() {
                    *value = load_value(&Target::Cache(self, None), k, false).await?;
                }
            }
        }
        Ok(())
    }

    /// Hands every write-behind change still queued to the writer now and returns the first writer error, like [`HashMap::flush`](crate::HashMap::flush). Without write-behind it does nothing.
    ///
    /// It plays the part Redisson's `destroy()` plays for pending writes; dropping the cache's last handle drains the queue once more in the background.
    pub async fn flush(&self) -> Result<()> {
        match &self.store {
            Some(store) => store.drain().await,
            None => Ok(()),
        }
    }

    /// Loads every key of the loader into the cache, Redisson's `loadAll(replaceExistingValues, parallelism)`. A loaded value is stored without limits, like `insert`. Live keys keep their value unless [`replace_existing`](LoadAll::replace_existing). Without a loader it is an [`Error::Config`](crate::Error::Config).
    #[doc(alias = "loadAll")]
    pub fn load_all(&self) -> LoadAll<'_, K, V, C> {
        LoadAll::new(Target::Cache(self, None), None)
    }

    /// Loads the given keys into the cache, Redisson's `loadAll(keys, replaceExistingValues, parallelism)`.
    #[doc(alias = "loadAll")]
    pub fn load_many<Q>(&self, ks: &[&Q]) -> LoadAll<'_, K, V, C>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let keys = ks.iter().map(|k| self.codec.encode(*k)).collect();
        LoadAll::new(Target::Cache(self, None), Some(keys))
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
        Insert::new(self, k, v)
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
        TryInsert::new(self, k, v)
    }

    /// Inserts an entry like [`HashMapCache::insert`] and returns whether there was no live entry for the key: `true` exactly when `insert` would return `None`. The old value is not decoded.
    ///
    /// It replaces the entry and clears its limits like `insert`, unlike Redisson's `fastPut`, which keeps a live entry's time to live. It takes `.ttl(..)` and `.max_idle(..)` too.
    #[doc(alias = "fastPut", alias = "putWithLease")]
    pub fn set<'a, Q, W>(&'a self, k: &'a Q, v: &'a W) -> Set<'a, K, V, C, Q, W>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        Set::new(self, k, v)
    }

    /// Replaces the value of a live entry and returns the old one; a missing or expired entry is left alone and `None` is returned. The entry keeps its time to live and its idle time.
    #[doc(
        alias = "putIfExists",
        alias = "fastPutIfExists",
        alias = "fastReplace"
    )]
    pub async fn replace<Q, W>(&self, k: &Q, v: &W) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        if let Some(local) = self.local_only() {
            let k = self.codec.encode(k)?;
            let Some(previous) = Self::local_value(local, &k) else {
                return Ok(None);
            };
            let kept = Self::kept_limits(local, &k);
            let entry = [(k, self.codec.encode(v)?)];
            self.local_store(local, &entry, &kept).await?;
            self.written(&entry).await?;
            return self.decode_value(Some(previous));
        }
        let (k, v) = (self.codec.encode(k)?, self.codec.encode(v)?);
        let raw: Option<Bytes> = self
            .eval_write(
                &REPLACE,
                &announced::REPLACE,
                self.redis_keys(&[
                    Name::Main,
                    Name::Timeout,
                    Name::Idle,
                    Name::Updated,
                    Name::Options,
                ]),
                vec![now(), k.clone(), v.clone(), Bytes::from(PUBLISH)],
                Op::IfSome(&k),
                false,
            )
            .await?;
        if raw.is_some() {
            self.written(&[(k, v)]).await?;
        }
        self.decode_value(raw)
    }

    /// Sets the value to `new` only when the live entry holds `old`, and returns whether it did. The entry keeps its limits. Values are compared by their encoded bytes.
    #[doc(alias = "replace")]
    pub async fn compare_and_set<Q, W>(&self, k: &Q, old: &W, new: &W) -> Result<bool>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        if let Some(local) = self.local_only() {
            let k = self.codec.encode(k)?;
            if Self::local_value(local, &k) != Some(self.codec.encode(old)?) {
                return Ok(false);
            }
            let kept = Self::kept_limits(local, &k);
            let entry = [(k, self.codec.encode(new)?)];
            self.local_store(local, &entry, &kept).await?;
            self.written(&entry).await?;
            return Ok(true);
        }
        let (k, new) = (self.codec.encode(k)?, self.codec.encode(new)?);
        let keys = self.redis_keys(&[
            Name::Main,
            Name::Timeout,
            Name::Idle,
            Name::Updated,
            Name::Options,
        ]);
        let args = vec![
            now(),
            k.clone(),
            self.codec.encode(old)?,
            new.clone(),
            Bytes::from(PUBLISH),
        ];
        // With a writer, at most once: run again after a lost reply, the script would answer `0` and the writer would miss the change.
        let once = self.writer().is_some();
        let replaced: i64 = self
            .eval_write(
                &REPLACE_IF,
                &announced::REPLACE_IF,
                keys,
                args,
                Op::IfOne(&k),
                once,
            )
            .await?;
        if replaced == 1 {
            self.written(&[(k, new)]).await?;
        }
        Ok(replaced == 1)
    }

    /// Removes the live entry only when it holds `v`, and returns whether it did. Values are compared by their encoded bytes.
    #[doc(alias = "remove")]
    pub async fn remove_if_eq<Q, W>(&self, k: &Q, v: &W) -> Result<bool>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        if let Some(local) = self.local_only() {
            let k = self.codec.encode(k)?;
            if Self::local_value(local, &k) != Some(self.codec.encode(v)?) {
                return Ok(false);
            }
            self.local_forget(local, std::slice::from_ref(&k)).await?;
            if let Some(store) = self.writer() {
                store.deleted(&[k]).await?;
            }
            return Ok(true);
        }
        let k = self.codec.encode(k)?;
        let keys = self.redis_keys(&[
            Name::Main,
            Name::Timeout,
            Name::Idle,
            Name::Removed,
            Name::LastAccess,
            Name::Options,
        ]);
        let args = vec![
            now(),
            k.clone(),
            self.codec.encode(v)?,
            Bytes::from(PUBLISH),
        ];
        // With a writer, at most once, as in `compare_and_set`.
        let once = self.writer().is_some();
        let removed: i64 = self
            .eval_write(
                &REMOVE_IF,
                &announced::REMOVE_IF,
                keys,
                args,
                Op::IfOne(&k),
                once,
            )
            .await?;
        if removed == 1 {
            self.deleted(k).await?;
        }
        Ok(removed == 1)
    }

    /// Removes several entries in one call and returns how many were in Redis. As in Redisson, an expired entry that the clean-up has not deleted yet counts too. With a writer, the removed keys are deleted from the store in one call, and none removed calls nothing.
    #[doc(alias = "fastRemove")]
    pub async fn remove_many<Q>(&self, ks: &[&Q]) -> Result<usize>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if let Some(local) = self.local_only() {
            let fields = ks
                .iter()
                .map(|k| self.codec.encode(*k))
                .collect::<Result<std::vec::Vec<_>>>()?;
            let removed = self.local_remove(local, &fields).await?;
            if let Some(store) = self.writer() {
                let keys: std::vec::Vec<Bytes> = removed.iter().map(|(k, _)| k.clone()).collect();
                store.deleted(&keys).await?;
            }
            return Ok(removed.len());
        }
        if ks.is_empty() {
            return Ok(0);
        }
        let mut args = std::vec::Vec::with_capacity(ks.len() + 1);
        args.push(Bytes::from(PUBLISH));
        for k in ks {
            args.push(self.codec.encode(*k)?);
        }
        let keys = self.redis_keys(&[
            Name::Main,
            Name::Timeout,
            Name::Idle,
            Name::Removed,
            Name::LastAccess,
            Name::Options,
        ]);
        if self.writer().is_some() || self.local.is_some() {
            let fields = args.get(1..).unwrap_or_default().to_vec();
            let flags: std::vec::Vec<i64> = self
                // At most once: run again after a lost reply, it would report nothing removed and the writer would miss the deletes.
                .eval_write(
                    &FAST_REMOVE_BATCH,
                    &announced::FAST_REMOVE_BATCH,
                    keys,
                    args,
                    Op::PerKey(&fields),
                    true,
                )
                .await?;
            let removed: std::vec::Vec<Bytes> = fields
                .into_iter()
                .zip(flags)
                .filter_map(|(k, flag)| (flag == 1).then_some(k))
                .collect();
            if let Some(store) = self.writer() {
                store.deleted(&removed).await?;
            }
            return Ok(removed.len());
        }
        let removed: usize = self.key.core.eval(&FAST_REMOVE, keys, args).await?;
        Ok(removed)
    }

    /// Adds `delta` to the number stored under the key, creating it at `delta`, and returns the result. The entry keeps its limits.
    #[doc(alias = "addAndGet")]
    pub async fn incr_by<Q>(&self, k: &Q, delta: i64) -> Result<i64>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.redis_store("incr_by")?;
        let result = self.add_and_get(k, delta.to_string()).await?;
        result
            .parse()
            .map_err(|_| Error::Codec(format!("not an integer: {result}")))
    }

    /// Adds `delta` to the number stored under the key, creating it at `delta`, and returns the result. A `delta` that is not finite is [`Error::Config`].
    #[doc(alias = "addAndGet")]
    pub async fn incr_by_float<Q>(&self, k: &Q, delta: f64) -> Result<f64>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.redis_store("incr_by_float")?;
        if !delta.is_finite() {
            return Err(Error::Config(format!("cannot add {delta}")));
        }
        // `Display` of an `f64` never uses an exponent, like Java's `BigDecimal.toPlainString`.
        let result = self.add_and_get(k, delta.to_string()).await?;
        result
            .parse()
            .map_err(|_| Error::Codec(format!("not a number: {result}")))
    }

    async fn add_and_get<Q>(&self, k: &Q, delta: String) -> Result<String>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let k = self.codec.encode(k)?;
        // Not idempotent: a retried call would add twice.
        let fields = [k.clone()];
        let outcome: Result<String> = self
            .eval_write(
                &ADD_AND_GET,
                &announced::ADD_AND_GET,
                self.redis_keys(&[
                    Name::Main,
                    Name::Timeout,
                    Name::Idle,
                    Name::Created,
                    Name::Updated,
                    Name::LastAccess,
                    Name::Removed,
                    Name::Options,
                ]),
                vec![now(), k.clone(), Bytes::from(delta), Bytes::from(PUBLISH)],
                Op::Touch(&fields),
                true,
            )
            .await;
        if let Err(Error::Redis(_)) = &outcome {
            // `HINCRBYFLOAT` fails on a stored value that is not a number after the script wrote the temporary field, and a script error rolls nothing back. Best effort: the original error is what the caller needs.
            let mut cmd = redis::cmd("HDEL");
            cmd.arg(self.key.name()).arg(TEMP_VAL_FIELD);
            let _: Result<i64> = self
                .key
                .core
                .query(Request::new(cmd).key(self.key.name().as_bytes()))
                .await;
        }
        let result = outcome?;
        // The writer gets the new number as the reply's text, the bytes `get` would decode, Redisson's `addAndGet` result.
        self.written(&[(k, Bytes::from(result.clone()))]).await?;
        Ok(result)
    }

    /// Inserts all entries, as [`HashMapCache::insert`] would one by one: each replaces its entry and clears its limits unless [`Extend::ttl`] or [`Extend::max_idle`] sets new ones (Redisson's `putAll(map)` keeps them). Entries are `(&key, &value)` pairs and are encoded at once; nothing is sent for none.
    #[doc(alias = "putAll")]
    pub fn extend<'a, Q, W>(
        &self,
        entries: impl IntoIterator<Item = (&'a Q, &'a W)>,
    ) -> Extend<'_, K, V, C>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync + 'a,
        W: Serialize + ?Sized + Sync + 'a,
    {
        let encoded = entries
            .into_iter()
            .map(|(k, v)| Ok((self.codec.encode(k)?, self.codec.encode(v)?)))
            .collect::<Result<std::vec::Vec<(Bytes, Bytes)>>>();
        Extend::new(self, encoded)
    }

    /// Limits the cache to `max` entries. When it is full, an insert drops the least recently (`Lru`) or least often (`Lfu`) used entry. `0` removes the limit. The limit is stored in Redis and applies to every client. Only entries written or read while a limit is set are tracked, so older entries are never dropped for space.
    #[doc(alias = "setMaxSize")]
    pub async fn set_max_size(&self, max: usize, mode: EvictionMode) -> Result<()> {
        self.redis_store("set_max_size")?;
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
        self.redis_store("init_max_size")?;
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
    ///
    /// Unlike [`HashMap::events`](crate::HashMap::events), which returns keyspace notifications, this returns the entry events of Redisson's `MapEntryListener`; the keyspace notifications of a cache come from [`HashMapCache::keyspace_events`].
    #[doc(
        alias = "addListener",
        alias = "MapEntryListener",
        alias = "EntryCreatedListener",
        alias = "EntryUpdatedListener",
        alias = "EntryRemovedListener",
        alias = "EntryExpiredListener"
    )]
    pub async fn events(&self) -> Result<EntryEvents<K, V, C>> {
        self.events_of(&EntryEventKind::ALL).await
    }

    /// Starts listening to the given kinds of change, made by any client.
    ///
    /// As in Redisson, the first listener marks the cache in Redis, and from then on every write publishes its changes until the cache is deleted with [`Object::del`](crate::Object::del). Expired entries are always published by the clean-up. The payload is Redisson's, so listeners see the changes Redisson clients make too.
    pub async fn events_of(&self, kinds: &[EntryEventKind]) -> Result<EntryEvents<K, V, C>> {
        self.redis_store("events")?;
        if !EntryEventKind::ALL.iter().any(|kind| kinds.contains(kind)) {
            return Err(Error::Config("listen to at least one kind of event".into()));
        }
        let prefix = self.length_prefix().await?;
        mark_listeners(&self.key).await?;
        let mut sources = std::vec::Vec::new();
        for kind in EntryEventKind::ALL {
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
        Ok(EntryEvents::new(sources, prefix, self.codec.clone()))
    }

    /// Starts listening to the keyspace notifications of the cache's hash, Redisson's `addListener` with the map and object listeners. See [`MapEvents`] for the server setting it needs.
    ///
    /// This is what [`HashMap::events`](crate::HashMap::events) returns; on a cache, [`HashMapCache::events`] returns the entry events instead. The temporary field an increment writes and deletes is left out.
    #[doc(
        alias = "addListener",
        alias = "MapPutListener",
        alias = "MapIncrListener",
        alias = "MapRemoveListener",
        alias = "MapDeletedListener",
        alias = "MapExpiredListener"
    )]
    pub async fn keyspace_events(&self) -> Result<MapEvents<K, C>> {
        self.keyspace_events_of(&MapEventKind::ALL).await
    }

    /// Starts listening to the given kinds of keyspace notification. See [`HashMapCache::keyspace_events`].
    pub async fn keyspace_events_of(&self, kinds: &[MapEventKind]) -> Result<MapEvents<K, C>> {
        self.redis_store("keyspace_events")?;
        map_events::subscribe(
            &self.key,
            self.codec.clone(),
            kinds,
            Some(TEMP_VAL_FIELD),
            false,
        )
        .await
    }

    /// Returns the value for the key, or `None` when it is missing or expired. A read starts the idle time of the entry again and counts as a use for the size limit. With a loader, a miss asks the loader and stores what it returns without limits.
    pub async fn get<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let encoded = self.codec.encode(k)?;
        if let Some(local) = &self.local {
            return self.read_cached(local, &encoded).await;
        }
        match self.read_encoded(&encoded).await? {
            None if self.loads() => load_value(&Target::Cache(self, None), &encoded, false).await,
            value => Ok(value),
        }
    }

    /// Returns the values for several keys, in the order of the keys: `None` for a key that is missing or expired. Like [`HashMapCache::get`], it starts the idle time of each live entry again and counts as a use for the size limit. With a loader, the missing keys are loaded one at a time, in the order of the keys. Nothing is sent for no keys.
    #[doc(alias = "getAll")]
    pub async fn get_many<Q>(&self, ks: &[&Q]) -> Result<std::vec::Vec<Option<V>>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if ks.is_empty() {
            return Ok(std::vec::Vec::new());
        }
        let mut args = std::vec::Vec::with_capacity(ks.len() + 1);
        args.push(now());
        for k in ks {
            args.push(self.codec.encode(*k)?);
        }
        if let Some(local) = &self.local {
            return self
                .get_many_cached(local, args.get(1..).unwrap_or_default())
                .await;
        }
        let raw: std::vec::Vec<Option<Bytes>> = self
            .key
            .core
            .eval(
                &GET_ALL,
                self.redis_keys(&[
                    Name::Main,
                    Name::Timeout,
                    Name::Idle,
                    Name::LastAccess,
                    Name::Options,
                ]),
                args.clone(),
            )
            .await?;
        let mut values = raw
            .into_iter()
            .map(|raw| self.decode_value(raw))
            .collect::<Result<std::vec::Vec<Option<V>>>>()?;
        self.load_missing(&mut values, args.get(1..).unwrap_or_default())
            .await?;
        Ok(values)
    }

    /// Returns the value for the key without counting it as a read, like `lru::LruCache::peek`: `None` when it is missing or its time to live has passed. It ignores the idle limit, does not start the idle time again and is not a use for the size limit. With a loader, a miss asks the loader, as Redisson's `getWithTTLOnly` does.
    #[doc(alias = "getWithTTLOnly")]
    pub async fn peek<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let encoded = self.codec.encode(k)?;
        if let Some(local) = &self.local {
            match local.local.lookup(&encoded) {
                Some(Some(value)) => return Ok(Some(self.codec.decode(&value)?)),
                Some(None) if local.store_cache_miss => return Ok(None),
                _ => {}
            }
            if local.local_only() {
                return match self.loads() {
                    true => load_value(&Target::Cache(self, None), &encoded, false).await,
                    false => Ok(None),
                };
            }
        }
        let raw: Option<Bytes> = self
            .key
            .core
            .eval(
                &GET_WITH_TTL_ONLY,
                self.redis_keys(&[Name::Main, Name::Timeout]),
                vec![now(), encoded.clone()],
            )
            .await?;
        match self.decode_value(raw)? {
            None if self.loads() => load_value(&Target::Cache(self, None), &encoded, false).await,
            value => Ok(value),
        }
    }

    /// Returns the values for several keys like [`HashMapCache::peek`], in the order of the keys: `None` for a key that is missing or whose time to live has passed. With a loader, the missing keys are loaded one at a time. Nothing is sent for no keys.
    #[doc(alias = "getAllWithTTLOnly")]
    pub async fn peek_many<Q>(&self, ks: &[&Q]) -> Result<std::vec::Vec<Option<V>>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if ks.is_empty() {
            return Ok(std::vec::Vec::new());
        }
        let mut args = std::vec::Vec::with_capacity(ks.len() + 1);
        args.push(now());
        for k in ks {
            args.push(self.codec.encode(*k)?);
        }
        if let Some(local) = &self.local {
            return self.peek_many_cached(local, args).await;
        }
        let raw: std::vec::Vec<Option<Bytes>> = self
            .key
            .core
            .eval(
                &GET_ALL_WITH_TTL_ONLY,
                self.redis_keys(&[Name::Main, Name::Timeout]),
                args.clone(),
            )
            .await?;
        let mut values = raw
            .into_iter()
            .map(|raw| self.decode_value(raw))
            .collect::<Result<std::vec::Vec<Option<V>>>>()?;
        self.load_missing(&mut values, args.get(1..).unwrap_or_default())
            .await?;
        Ok(values)
    }

    /// Sets new limits on a live entry and returns whether there was one. Without [`ExpireEntry::ttl`] and [`ExpireEntry::max_idle`] both limits are removed; [`ExpireEntry::if_not_set`] changes only an entry that has no limit. The deadlines start now, and the call counts as a use for the size limit.
    ///
    /// ```ignore
    /// cache.expire_entry("a").ttl(Duration::from_secs(60)).await?;
    /// cache.expire_entry("b").max_idle(Duration::from_secs(30)).if_not_set().await?;
    /// cache.expire_entry("c").await?; // never expires
    /// ```
    #[doc(
        alias = "expireEntry",
        alias = "expireEntryIfNotSet",
        alias = "updateEntryExpiration"
    )]
    pub fn expire_entry<'a, Q>(&'a self, k: &'a Q) -> ExpireEntry<'a, K, V, C, Q>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        ExpireEntry::new(self, k)
    }

    /// Sets new limits on several entries like [`HashMapCache::expire_entry`] and returns how many live entries it changed. Nothing is sent for no keys.
    #[doc(alias = "expireEntries", alias = "expireEntriesIfNotSet")]
    pub fn expire_entries<'a, Q>(&'a self, ks: &'a [&'a Q]) -> ExpireEntries<'a, K, V, C, Q>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        ExpireEntries::new(self, ks)
    }

    /// Returns whether a live entry holds `v`, compared by its encoded bytes. It reads every value on the server. As in Redisson, only the first entry that holds `v` is looked at: when that one has expired, the answer is `false`.
    #[doc(alias = "containsValue")]
    pub async fn contains_value<W>(&self, v: &W) -> Result<bool>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        self.redis_store("contains_value")?;
        let found: i64 = self
            .key
            .core
            .eval(
                &CONTAINS_VALUE,
                self.redis_keys(&[
                    Name::Main,
                    Name::Timeout,
                    Name::Idle,
                    Name::LastAccess,
                    Name::Options,
                ]),
                vec![now(), self.codec.encode(v)?],
            )
            .await?;
        Ok(found == 1)
    }

    /// Length in bytes of the encoded value stored under the key, the number [`HashMap::value_len`](crate::HashMap::value_len) gives for the same value; `0` when the key is missing. Redisson's `valueSize` counts the 16-byte header of the stored value too; this does not. Like Redisson, it does not check whether the entry has expired.
    #[doc(alias = "valueSize")]
    pub async fn value_len<Q>(&self, k: &Q) -> Result<usize>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.redis_store("value_len")?;
        let raw: usize = self
            .key
            .query({
                let mut cmd = redis::cmd("HSTRLEN");
                cmd.arg(self.key.redis_key());
                cmd.arg(&self.codec.encode(k)?[..]);
                cmd
            })
            .await?;
        Ok(encoded_value_len(raw, self.length_prefix().await?))
    }

    /// Returns up to `count` distinct random live keys. Redis picks `count` fields and the expired ones are left out, so fewer can come back while expired entries wait for the clean-up.
    #[doc(alias = "randomKeys")]
    pub async fn random_keys(&self, count: usize) -> Result<std::vec::Vec<K>> {
        self.redis_store("random_keys")?;
        let raw: std::vec::Vec<Bytes> = self.random(&RANDOM_KEYS, count).await?;
        raw.iter().map(|bytes| self.codec.decode(bytes)).collect()
    }

    async fn random<T: redis::FromRedisValue>(&self, script: &Lua, count: usize) -> Result<T> {
        let count = i64::try_from(count).unwrap_or(i64::MAX);
        self.key
            .core
            .eval(
                script,
                self.redis_keys(&[
                    Name::Main,
                    Name::Timeout,
                    Name::Idle,
                    Name::LastAccess,
                    Name::Options,
                ]),
                vec![now(), Bytes::from(count.to_string())],
            )
            .await
    }

    /// Removes the live entry and returns its value. An expired entry is left for the clean-up and `None` is returned. With a writer, the key is deleted from the store also when the cache did not have it, as in Redisson.
    pub async fn remove<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if let Some(local) = self.local_only() {
            let field = self.codec.encode(k)?;
            let removed = self
                .local_remove(local, std::slice::from_ref(&field))
                .await?;
            if let Some(store) = self.writer() {
                if !removed.is_empty() {
                    store.deleted(&[field]).await?;
                }
            }
            return self.decode_value(removed.into_iter().next().map(|(_, v)| v));
        }
        let k = self.codec.encode(k)?;
        let raw: Option<Bytes> = self
            .eval_write(
                &REMOVE,
                &announced::REMOVE,
                self.redis_keys(&[
                    Name::Main,
                    Name::Timeout,
                    Name::Idle,
                    Name::Removed,
                    Name::LastAccess,
                    Name::Options,
                ]),
                vec![now(), k.clone(), Bytes::from(PUBLISH)],
                Op::IfSome(&k),
                false,
            )
            .await?;
        self.deleted(k).await?;
        self.decode_value(raw)
    }

    /// Returns whether a live entry exists for the key. Like a read, it starts the idle time again and counts as a use for the size limit. With a loader, a miss asks the loader, and a loaded value counts as present.
    #[doc(alias = "containsKey")]
    pub async fn contains_key<Q>(&self, k: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let encoded = self.codec.encode(k)?;
        if let Some(local) = &self.local {
            return self.contains_cached(local, &encoded).await;
        }
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
                vec![now(), encoded.clone()],
            )
            .await?;
        if found != 1 && self.loads() {
            return Ok(load_value(&Target::Cache(self, None), &encoded, false)
                .await?
                .is_some());
        }
        Ok(found == 1)
    }

    /// Time left until the entry expires, by its time to live or its idle time, whichever comes first. `None` means the entry has no limit or does not exist.
    #[doc(alias = "remainTimeToLive")]
    pub async fn entry_ttl<Q>(&self, k: &Q) -> Result<Option<Duration>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if let Some(local) = self.local_only() {
            return Ok(Self::local_ttl(local, &self.codec.encode(k)?).flatten());
        }
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
        if let Some(local) = self.local_only() {
            return Ok(Self::local_snapshot(local).len());
        }
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
        if let Some(local) = self.local_only() {
            self.local_clear(local).await?;
            return Ok(());
        }
        if let Some(local) = &self.local {
            local.local.forget_all();
            let mut keys =
                self.redis_keys(&[Name::Main, Name::Timeout, Name::Idle, Name::LastAccess]);
            keys.push(local.updates_log.clone());
            let _: i64 = self
                .key
                .core
                .eval(&announced::CLEAR_ANNOUNCED, keys, clear_arguments(local))
                .await?;
            return Ok(());
        }
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
    pub(crate) async fn scan_page(
        &self,
        script: &Lua,
        cursor: String,
        pattern: Option<&str>,
        page: usize,
    ) -> Result<(String, std::vec::Vec<Bytes>)> {
        let mut args = vec![now(), Bytes::from(cursor)];
        if let Some(pattern) = pattern {
            args.push(Bytes::copy_from_slice(pattern.as_bytes()));
        }
        args.push(Bytes::from(page.to_string()));
        let (next, items, idle): (String, std::vec::Vec<Bytes>, std::vec::Vec<Bytes>) = self
            .key
            .core
            .eval(
                script,
                self.redis_keys(&[Name::Main, Name::Timeout, Name::Idle]),
                args,
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
        match self.local_only() {
            Some(local) => stream::iter(
                Self::local_snapshot(local)
                    .into_iter()
                    .map(|(k, v)| Ok((self.codec.decode(&k)?, self.codec.decode(&v)?))),
            )
            .left_stream(),
            None => self.iter_with(MapScan::new()).right_stream(),
        }
    }

    /// Streams the live keys, like [`HashMapCache::iter`].
    pub fn keys(&self) -> impl Stream<Item = Result<K>> + '_ {
        match self.local_only() {
            Some(_) => self.iter().map_ok(|(k, _)| k).left_stream(),
            None => self.keys_with(MapScan::new()).right_stream(),
        }
    }

    /// Streams the live values, like [`HashMapCache::iter`].
    pub fn values(&self) -> impl Stream<Item = Result<V>> + '_ {
        self.iter().map_ok(|(_, v)| v)
    }

    /// Streams the live entries `scan` selects, like [`HashMapCache::iter`]. Redisson's `entrySet(pattern, count)`.
    #[doc(alias = "entrySet")]
    pub fn iter_with(&self, scan: MapScan) -> impl Stream<Item = Result<(K, V)>> + '_ {
        if let Err(error) = self.redis_store("iter_with") {
            return stream::once(async move { Err(error) }).left_stream();
        }
        let page = scan.page();
        let pattern = scan.match_pattern().map(str::to_string);
        stream::try_unfold(
            (Some("0".to_string()), pattern),
            move |(cursor, pattern)| async move {
                let Some(cursor) = cursor else {
                    return Ok::<_, Error>(None);
                };
                let (next, raw) = self
                    .scan_page(&SCAN_ENTRIES, cursor, pattern.as_deref(), page)
                    .await?;
                let items = raw
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|[k, v]| Ok((self.codec.decode(k)?, self.codec.decode(v)?)))
                    .collect::<Result<std::vec::Vec<(K, V)>>>()?;
                let next = (next != "0").then_some(next);
                Ok(Some((
                    stream::iter(items.into_iter().map(Ok)),
                    (next, pattern),
                )))
            },
        )
        .try_flatten()
        .right_stream()
    }

    /// Streams the live keys `scan` selects, like [`HashMapCache::iter`]. Redisson's `keySet(pattern, count)`.
    #[doc(alias = "keySet")]
    pub fn keys_with(&self, scan: MapScan) -> impl Stream<Item = Result<K>> + '_ {
        if let Err(error) = self.redis_store("keys_with") {
            return stream::once(async move { Err(error) }).left_stream();
        }
        let page = scan.page();
        let pattern = scan.match_pattern().map(str::to_string);
        stream::try_unfold(
            (Some("0".to_string()), pattern),
            move |(cursor, pattern)| async move {
                let Some(cursor) = cursor else {
                    return Ok::<_, Error>(None);
                };
                let (next, raw) = self
                    .scan_page(&SCAN_KEYS, cursor, pattern.as_deref(), page)
                    .await?;
                let items = raw
                    .iter()
                    .map(|key| self.codec.decode(key))
                    .collect::<Result<std::vec::Vec<K>>>()?;
                let next = (next != "0").then_some(next);
                Ok(Some((
                    stream::iter(items.into_iter().map(Ok)),
                    (next, pattern),
                )))
            },
        )
        .try_flatten()
        .right_stream()
    }

    /// Streams the live values of the entries `scan` selects, like [`HashMapCache::iter`]. Redisson's `values(pattern, count)`.
    pub fn values_with(&self, scan: MapScan) -> impl Stream<Item = Result<V>> + '_ {
        self.iter_with(scan).map_ok(|(_, v)| v)
    }
}

impl<K, V, C> HashMapCache<K, V, C>
where
    K: Serialize + DeserializeOwned + Eq + Hash + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    /// Up to `count` distinct random live entries, picked like [`HashMapCache::random_keys`]. Redisson's `randomEntries`.
    #[doc(alias = "randomEntries")]
    pub async fn random_entries(&self, count: usize) -> Result<std::collections::HashMap<K, V>> {
        self.redis_store("random_entries")?;
        let raw: redis::Value = self.random(&RANDOM_ENTRIES, count).await?;
        Ok(decode_pairs(&self.codec, raw)?.into_iter().collect())
    }

    /// Every live entry in one call, a snapshot of one moment; [`iter`](HashMapCache::iter) reads it in pages instead. Like a read, it starts the idle time of each entry with an idle limit again.
    #[doc(
        alias = "readAllMap",
        alias = "readAllEntrySet",
        alias = "readAllKeySet",
        alias = "readAllValues"
    )]
    pub async fn to_hash_map(&self) -> Result<std::collections::HashMap<K, V>> {
        if let Some(local) = self.local_only() {
            return Self::local_snapshot(local)
                .into_iter()
                .map(|(k, v)| Ok((self.codec.decode(&k)?, self.codec.decode(&v)?)))
                .collect();
        }
        let raw: redis::Value = self
            .key
            .core
            .eval(
                &READ_ALL,
                self.redis_keys(&[
                    Name::Main,
                    Name::Timeout,
                    Name::Idle,
                    Name::LastAccess,
                    Name::Options,
                ]),
                vec![now()],
            )
            .await?;
        Ok(decode_pairs(&self.codec, raw)?.into_iter().collect())
    }
}

impl<K, V, C> HashMapCache<K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    /// The value of `k`, taken under the key's lock to read and change it, like [`std::collections::hash_map::Entry`]: Redisson's `compute`, `computeIfAbsent`, `computeIfPresent` and `merge`. Await it, optionally after [`EntryRequest::ttl`].
    ///
    /// An expired entry is vacant. A write through a plain `entry(&k)` replaces the entry like [`insert`](HashMapCache::insert) and clears its time to live and max idle, unlike Redisson's `compute(key, f)`, which keeps them; `entry(&k).ttl(d)` writes with that time to live, Redisson's `compute(key, ttl, f)`. The lock is the one [`lock`](HashMapCache::lock) returns, the lock Redisson's compute family takes. Calls for the same key from this client wait for each other, also futures joined in one task: while you hold an entry, taking another one for the same key in the same task waits forever.
    ///
    /// ```no_run
    /// # async fn run(client: redissun::Client) -> redissun::Result<()> {
    /// # use std::time::Duration;
    /// let visits = client.hash_map_cache::<String, i64>("visits");
    /// visits
    ///     .entry("home")
    ///     .ttl(Duration::from_secs(60))
    ///     .await?
    ///     .and_modify(|n| *n += 1)
    ///     .or_insert(1)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[doc(
        alias = "compute",
        alias = "computeIfAbsent",
        alias = "computeIfPresent",
        alias = "merge"
    )]
    pub fn entry<Q>(&self, k: &Q) -> EntryRequest<'_, K, V, C>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        EntryRequest::new(self, self.codec.encode(k))
    }

    per_key_objects!();

    /// The `GET` script for an encoded key: the live value, as [`get`](HashMapCache::get) reads it.
    pub(crate) async fn read_encoded(&self, k: &Bytes) -> Result<Option<V>> {
        if let Some(local) = self.local_only() {
            return self.decode_value(Self::local_value(local, k));
        }
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
                vec![now(), k.clone()],
            )
            .await?;
        self.decode_value(raw)
    }

    /// Writes an encoded key for an entry: like `insert` (`PUT`) without a time to live or with a zero one, else `FAST_PUT_WITH_LIMITS` with the time to live only, Redisson's `compute(key, ttl, f)` → `fastPut(key, value, ttl)`.
    pub(crate) async fn write_encoded(
        &self,
        k: &Bytes,
        v: &V,
        ttl: Option<Duration>,
    ) -> Result<()> {
        let limits = limits(ttl, None)?;
        let value = self.codec.encode(v)?;
        if let Some(local) = self.local_only() {
            return self
                .local_store(local, &[(k.clone(), value)], &limits)
                .await;
        }
        if limits.any() {
            self.fast_put_with_limits(&limits, k.clone(), value, Bytes::new())
                .await?;
        } else {
            self.put(&limits, k.clone(), value).await?;
        }
        Ok(())
    }

    /// Removes an encoded key with `FAST_REMOVE`, publishing `Removed` like [`remove_many`](HashMapCache::remove_many).
    pub(crate) async fn delete_encoded(&self, k: &Bytes) -> Result<()> {
        let fields = [k.clone()];
        if let Some(local) = self.local_only() {
            return self.local_forget(local, &fields).await;
        }
        // With a local cache, the announced twin of the batch variant, which replies per key, so only a removal is announced.
        let _: redis::Value = self
            .eval_write(
                &FAST_REMOVE,
                &announced::FAST_REMOVE_BATCH,
                self.redis_keys(&[
                    Name::Main,
                    Name::Timeout,
                    Name::Idle,
                    Name::Removed,
                    Name::LastAccess,
                    Name::Options,
                ]),
                vec![Bytes::from(PUBLISH), k.clone()],
                Op::PerKey(&fields),
                false,
            )
            .await?;
        Ok(())
    }
}

impl<K, V, C: Codec> HashMapCache<K, V, C> {
    /// Moves data that redissun 0.23 or earlier wrote for this cache into the layout this version and Redisson use, at most [`Migrate::batch_size`] old entries (1000 by default) per call. Resolves to how many old entries the call handled: moved, or dropped because they had expired. `0` means nothing old is left, so call it until it returns `0`:
    ///
    /// ```ignore
    /// while cache.migrate_from_0_23().batch_size(1000).await? > 0 {}
    /// ```
    ///
    /// Stop the 0.23 clients first, then run the loop once per cache, from one client, before any client of this version uses that cache. Each script is atomic; the whole migration is not: until the loop has returned `0`, part of the data is in each layout, so old and new clients must not mix and new clients should not write to the cache (a 0.23 client cannot read the new layout, and the event formats differ). A live entry keeps its time to live, max idle time and use score; the old last-access set and the size limit move to their new keys in the last call. One script scans at most ten times `batch_size` fields, and a call runs scripts until it has handled `batch_size` old entries or the scan is complete. Deleting expired entries meanwhile would drop the deadlines of entries still in the old layout and the migration would move them as live, so the clean-up waits: every call holds the clean-up latch of the cache for 30 seconds and the last call frees it, and before that the clean-up of every client of this version skips its runs while the old options hash, the old last-access set or (for a name without `{`) the old main hash exists, until one check finds none. A name with a hash tag whose 0.23 cache had no size limit keeps its old data in the main hash only, which nothing tells apart from the new layout: for such a cache, start the loop right after creating the handle. Do not pause for more than 30 seconds between calls. Running the loop again changes nothing: on a name without `{` it returns `0` at once; on a name with a hash tag it scans the whole hash once more and moves nothing. It leaves data that Redisson wrote alone, as that is already in the new layout. See the "Upgrading redissun" guide.
    pub fn migrate_from_0_23(&self) -> Migrate<'_, K, V, C> {
        Migrate::new(self)
    }

    /// Redisson's `PUT`: replaces the entry, clears its limits and frees a lease on the key. Returns the encoded live value it replaced.
    async fn put(&self, limits: &Limits, k: Bytes, v: Bytes) -> Result<Option<Bytes>> {
        let entries = [(k.clone(), v.clone())];
        self.eval_write(
            &PUT,
            &announced::PUT,
            self.redis_keys(&[
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
            vec![limits.now(), k, v, Bytes::from(PUBLISH)],
            Op::Put {
                entries: &entries,
                ttl_timeout: 0,
                idle_delta: 0,
            },
            false,
        )
        .await
    }

    /// Redisson's `FAST_PUT_WITH_LIMITS` with the lease `token`, empty for none. Returns whether there was no live entry.
    async fn fast_put_with_limits(
        &self,
        limits: &Limits,
        k: Bytes,
        v: Bytes,
        token: Bytes,
    ) -> Result<bool> {
        let keys = self.redis_keys(&[
            Name::Main,
            Name::Timeout,
            Name::Idle,
            Name::Created,
            Name::Updated,
            Name::LastAccess,
            Name::Removed,
            Name::Options,
            Name::Lease,
        ]);
        // With a token the script frees the lease, so a second run would find no lease and report `false` for a stored value.
        let leased = !token.is_empty();
        let entries = [(k.clone(), v.clone())];
        let fields = [k.clone()];
        // With a lease the reply is `0` both for "stored over a live entry" and for "wrong lease", so the client does not know whether it stored: only an invalidation.
        let op = if leased {
            Op::Touch(&fields)
        } else {
            Op::Put {
                entries: &entries,
                ttl_timeout: limits.ttl_timeout,
                idle_delta: limits.idle_delta,
            }
        };
        let args = limits.arguments(vec![k, v, Bytes::from(PUBLISH), token]);
        let inserted: i64 = self
            .eval_write(
                &FAST_PUT_WITH_LIMITS,
                &announced::FAST_PUT_WITH_LIMITS,
                keys,
                args,
                op,
                leased,
            )
            .await?;
        Ok(inserted == 1)
    }
}

/// The length of an encoded value whose stored field is `raw` bytes long: the field minus the `dLc0` header, never below zero.
fn encoded_value_len(raw: usize, prefix: LengthPrefix) -> usize {
    raw.saturating_sub(prefix.value_header())
}

/// The client's clock as a script argument.
fn now() -> Bytes {
    Bytes::from(now_millis().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_len_never_underflows() {
        assert_eq!(encoded_value_len(20, LengthPrefix::U64Le), 4);
        assert_eq!(encoded_value_len(16, LengthPrefix::U64Le), 0);
        assert_eq!(encoded_value_len(5, LengthPrefix::U64Le), 0);
        assert_eq!(encoded_value_len(0, LengthPrefix::U64Be), 0);
        assert_eq!(encoded_value_len(16, LengthPrefix::U32Le), 4);
        assert_eq!(encoded_value_len(3, LengthPrefix::U32Le), 0);
    }
}
