use crate::codec::Codec;
use crate::collections::hash_map_cache::{
    mark_listeners, EntryEventKind, EntryEvents, EntryRequest, EvictionMode, ExpireEntries,
    ExpireEntry, Extend, HashMapCache, Insert, Lease, Set, TryInsert,
};
use crate::collections::local_cache::messages::{message, CLEAR};
use crate::collections::local_cache::{local_options, start, LocalCache, LocalOptions, StoreMode};
use crate::collections::map::events::{MapEventKind, MapEvents};
use crate::collections::map::key_hash::per_key_objects;
use crate::collections::map::load::LoadAll;
use crate::collections::map::scan::MapScan;
use crate::collections::map::store::{store_options, StoreOptions};
use crate::error::Result;
use crate::lua::Lua;
use crate::object::{HasKey, Key};
use bytes::Bytes;
use futures::Stream;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::hash::Hash;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

/// Redisson's `entrySet()` page, the default page of [`PreloadCache`].
const PRELOAD_PAGE: usize = 10;

/// A pending [`LocalCachedMapCache::preload_cache`]. Await it, optionally after [`PreloadCache::page_size`]; it resolves to the number of entries it cached.
#[must_use = "a preload does nothing until it is awaited"]
pub struct PreloadCache<'a, K, V, C: Codec> {
    cache: &'a LocalCachedMapCache<K, V, C>,
    page: usize,
}

impl<K, V, C: Codec> fmt::Debug for PreloadCache<'_, K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreloadCache")
            .field("page", &self.page)
            .finish_non_exhaustive()
    }
}

impl<K, V, C: Codec> PreloadCache<'_, K, V, C> {
    /// Reads this many keys per page, Redisson's `preloadCache(count)`. Zero means the default, 10.
    pub fn page_size(mut self, n: usize) -> Self {
        self.page = if n == 0 { PRELOAD_PAGE } else { n };
        self
    }
}

impl<'a, K, V, C> IntoFuture for PreloadCache<'a, K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync + 'a,
    V: Serialize + DeserializeOwned + Send + Sync + 'a,
    C: Codec,
{
    type Output = Result<usize>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let cache = self.cache;
            cache.cache.preload(&cache.local, self.page).await
        })
    }
}

/// Sets up a [`LocalCachedMapCache`]. Get one from [`Client::local_cached_map_cache`](crate::Client::local_cached_map_cache). The local cache options are [`LocalCachedMapBuilder`](crate::LocalCachedMapBuilder)'s, with the same defaults; the loader and writer options are [`HashMapCacheBuilder`](crate::HashMapCacheBuilder)'s.
#[must_use = "a builder does nothing until build() is awaited"]
pub struct LocalCachedMapCacheBuilder<K, V, C: Codec> {
    key: Key,
    codec: C,
    local: LocalOptions,
    options: StoreOptions<K, V>,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V, C: Codec> fmt::Debug for LocalCachedMapCacheBuilder<K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalCachedMapCacheBuilder")
            .field("name", &self.key.name())
            .field("local", &self.local)
            .field("options", &self.options)
            .finish()
    }
}

impl<K, V, C: Codec> LocalCachedMapCacheBuilder<K, V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self {
            key,
            codec,
            local: LocalOptions::default(),
            options: StoreOptions::default(),
            _marker: PhantomData,
        }
    }

    local_options!();
}

impl<K, V, C> LocalCachedMapCacheBuilder<K, V, C>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    C: Codec,
{
    store_options!();
}

impl<K, V, C> LocalCachedMapCacheBuilder<K, V, C>
where
    K: DeserializeOwned + Send + Sync + 'static,
    V: DeserializeOwned + Send + Sync + 'static,
    C: Codec,
{
    /// Checks the options, marks the cache for entry events, subscribes to the changes of the other instances and returns the cache. Invalid store options are an [`Error::Config`](crate::Error::Config).
    pub async fn build(self) -> Result<LocalCachedMapCache<K, V, C>> {
        let store = self.options.into_store(&self.key, self.codec.clone())?;
        if self.local.store_mode != StoreMode::Local {
            mark_listeners(&self.key).await?;
        }
        let local = start(&self.key, self.key.name().to_string(), &self.local).await?;
        let cache = HashMapCache::with_local(
            self.key.clone(),
            self.codec.clone(),
            store,
            Some(local.clone()),
        );
        Ok(LocalCachedMapCache {
            cache,
            key: self.key,
            codec: self.codec,
            local,
        })
    }
}

/// **Experimental.** A [`HashMapCache`] with a cache inside this program, modelled on Redisson's `RLocalCachedMapCache`. Reads that hit the local cache cost no network round trip.
///
/// The data in Redis is [`HashMapCache`]'s, which is Redisson's `RMapCache` layout, so a `HashMapCache` or a Redisson `RMapCache` of the same name and codec reads and writes the same entries. Every method it shares with `HashMapCache` takes the same arguments, returns the same builders and changes Redis the same way. The local cache, its options and the messages between instances are [`LocalCachedMap`](crate::LocalCachedMap)'s.
///
/// A local copy never outlives its entry: it is dropped at the entry's time-to-live deadline, and the copy of an entry with a max idle time lives half that time after it was read from Redis, so a key read all the time is read from Redis, which restarts its idle time there, before it can expire. Writes through this type tell the other instances (see [`SyncStrategy`](crate::SyncStrategy)); writes through a plain `HashMapCache` or Redisson are not announced, and other instances see them when their copy's lifetime ends.
///
/// Not `Clone`: one handle owns the listener of the local cache.
pub struct LocalCachedMapCache<K, V, C: Codec> {
    cache: HashMapCache<K, V, C>,
    key: Key,
    codec: C,
    local: Arc<LocalCache>,
}

impl<K, V, C: Codec> fmt::Debug for LocalCachedMapCache<K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "LocalCachedMapCache")
    }
}

impl<K, V, C: Codec> HasKey for LocalCachedMapCache<K, V, C> {
    fn key(&self) -> &Key {
        self.cache.key()
    }

    fn companions(&self) -> std::vec::Vec<String> {
        self.cache.companions()
    }

    fn renamed_companions(&self, new_name: &str) -> Option<std::vec::Vec<String>> {
        self.cache.renamed_companions(new_name)
    }

    fn renamed_key(&self, new_name: &str) -> String {
        self.cache.renamed_key(new_name)
    }

    fn del_script(&self) -> Option<(&'static Lua, std::vec::Vec<String>, std::vec::Vec<Bytes>)> {
        self.cache.del_script()
    }

    fn expire_script(&self) -> Option<&'static Lua> {
        self.cache.expire_script()
    }

    fn expire_args(&self) -> std::vec::Vec<Bytes> {
        self.cache.expire_args()
    }

    fn persist_script(&self) -> Option<&'static Lua> {
        self.cache.persist_script()
    }

    fn persist_args(&self) -> std::vec::Vec<Bytes> {
        self.cache.persist_args()
    }

    fn rename_script(
        &self,
        new_name: &str,
    ) -> Option<(&'static Lua, std::vec::Vec<String>, std::vec::Vec<Bytes>)> {
        self.cache.rename_script(new_name)
    }
}

impl<K, V, C> LocalCachedMapCache<K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    /// Returns the value for the key, from the local cache when it is there, else as [`HashMapCache::get`] does, caching what it read for as long as the entry lives. A missing key is cached only with [`store_cache_miss`](LocalCachedMapCacheBuilder::store_cache_miss).
    pub async fn get<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.cache.get(k).await
    }

    /// Returns whether a live entry exists for the key; a cached key answers without a round trip. Else as [`HashMapCache::contains_key`].
    #[doc(alias = "containsKey")]
    pub async fn contains_key<Q>(&self, k: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.cache.contains_key(k).await
    }

    /// Same as [`HashMapCache::insert`]. This instance caches the new value with its limits, and the other instances are told.
    #[doc(alias = "put")]
    pub fn insert<'a, Q, W>(&'a self, k: &'a Q, v: &'a W) -> Insert<'a, K, V, C, Q, W>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        self.cache.insert(k, v)
    }

    /// Same as [`HashMapCache::remove`]. The other instances are told when a live entry was removed.
    pub async fn remove<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.cache.remove(k).await
    }

    /// Same as [`HashMapCache::clear`]. Every local cache is emptied when something was deleted.
    pub async fn clear(&self) -> Result<()> {
        self.cache.clear().await
    }

    /// Same as [`HashMapCache::len`]: the entries in Redis, expired ones included until the clean-up deletes them.
    pub async fn len(&self) -> Result<usize> {
        self.cache.len().await
    }

    /// Same as [`HashMapCache::is_empty`].
    pub async fn is_empty(&self) -> Result<bool> {
        self.cache.is_empty().await
    }

    /// Same as [`HashMapCache::try_insert`]. A stored entry is cached here and announced; when a live entry was there, nothing is announced.
    #[doc(alias = "putIfAbsent", alias = "fastPutIfAbsent")]
    pub fn try_insert<'a, Q, W>(&'a self, k: &'a Q, v: &'a W) -> TryInsert<'a, K, V, C, Q, W>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        self.cache.try_insert(k, v)
    }

    /// Same as [`HashMapCache::set`]. With [`Set::lease`] the result does not tell whether the value was stored, so the key is only invalidated everywhere.
    #[doc(alias = "fastPut", alias = "putWithLease")]
    pub fn set<'a, Q, W>(&'a self, k: &'a Q, v: &'a W) -> Set<'a, K, V, C, Q, W>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        self.cache.set(k, v)
    }

    /// Same as [`HashMapCache::replace`]. The entry keeps its limits in Redis, which the client does not know, so the key is invalidated everywhere and read again when needed.
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
        self.cache.replace(k, v).await
    }

    /// Same as [`HashMapCache::compare_and_set`]. The key is invalidated everywhere only when the value changed.
    #[doc(alias = "replace")]
    pub async fn compare_and_set<Q, W>(&self, k: &Q, old: &W, new: &W) -> Result<bool>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        self.cache.compare_and_set(k, old, new).await
    }

    /// Same as [`HashMapCache::remove_if_eq`]. The others are told only when the entry was removed.
    #[doc(alias = "remove")]
    pub async fn remove_if_eq<Q, W>(&self, k: &Q, v: &W) -> Result<bool>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        self.cache.remove_if_eq(k, v).await
    }

    /// Same as [`HashMapCache::remove_many`]. Each removed key is invalidated everywhere.
    #[doc(alias = "fastRemove")]
    pub async fn remove_many<Q>(&self, ks: &[&Q]) -> Result<usize>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.cache.remove_many(ks).await
    }

    /// Same as [`HashMapCache::incr_by`]. The key is invalidated everywhere.
    #[doc(alias = "addAndGet")]
    pub async fn incr_by<Q>(&self, k: &Q, delta: i64) -> Result<i64>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.cache.incr_by(k, delta).await
    }

    /// Same as [`HashMapCache::incr_by_float`]. The key is invalidated everywhere.
    #[doc(alias = "addAndGet")]
    pub async fn incr_by_float<Q>(&self, k: &Q, delta: f64) -> Result<f64>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.cache.incr_by_float(k, delta).await
    }

    /// Same as [`HashMapCache::extend`]. Each entry is cached here with its limits and announced.
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
        self.cache.extend(entries)
    }

    /// Same as [`HashMapCache::expire_entry`]. A changed entry is invalidated everywhere, so the next read caches it with its new limits.
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
        self.cache.expire_entry(k)
    }

    /// Same as [`HashMapCache::expire_entries`]. Every key is invalidated everywhere.
    #[doc(alias = "expireEntries", alias = "expireEntriesIfNotSet")]
    pub fn expire_entries<'a, Q>(&'a self, ks: &'a [&'a Q]) -> ExpireEntries<'a, K, V, C, Q>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.cache.expire_entries(ks)
    }

    /// Same as [`HashMapCache::entry`]. Under the key's lock it reads Redis, not the local copy, so a late invalidation cannot feed a stale value to the change; its write is cached and announced like [`insert`](LocalCachedMapCache::insert).
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
        self.cache.entry(k)
    }

    /// Same as [`HashMapCache::lease`]. It reads Redis, not the local cache; the guard's writes invalidate the key everywhere.
    #[doc(alias = "getWithLease")]
    pub async fn lease<Q>(&self, k: &Q, time: std::time::Duration) -> Result<Lease<'_, K, V, C>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.cache.lease(k, time).await
    }

    /// Same as [`HashMapCache::invalidate`]. The key is invalidated everywhere when an entry was removed.
    #[doc(alias = "removeWithLease")]
    pub async fn invalidate<Q>(&self, k: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.cache.invalidate(k).await
    }

    /// Same as [`HashMapCache::load_all`]. A loaded value is cached here and announced like an insert.
    #[doc(alias = "loadAll")]
    pub fn load_all(&self) -> LoadAll<'_, K, V, C> {
        self.cache.load_all()
    }

    /// Same as [`HashMapCache::load_many`].
    #[doc(alias = "loadAll")]
    pub fn load_many<Q>(&self, ks: &[&Q]) -> LoadAll<'_, K, V, C>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.cache.load_many(ks)
    }

    /// Same as [`HashMapCache::flush`]. It plays the part of Redisson's `destroy()` for pending writes; the listener stops with the last handle.
    #[doc(alias = "destroy")]
    pub async fn flush(&self) -> Result<()> {
        self.cache.flush().await
    }

    /// Returns the values for several keys: the local hits, then one read from Redis for the misses, each cached for as long as its entry lives. Else as [`HashMapCache::get_many`].
    #[doc(alias = "getAll")]
    pub async fn get_many<Q>(&self, ks: &[&Q]) -> Result<std::vec::Vec<Option<V>>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.cache.get_many(ks).await
    }

    /// Same as [`HashMapCache::peek`]; a local copy answers, and a value read from Redis is not cached.
    #[doc(alias = "getWithTTLOnly")]
    pub async fn peek<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.cache.peek(k).await
    }

    /// Same as [`HashMapCache::peek_many`]; local copies answer, and values read from Redis are not cached.
    #[doc(alias = "getAllWithTTLOnly")]
    pub async fn peek_many<Q>(&self, ks: &[&Q]) -> Result<std::vec::Vec<Option<V>>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.cache.peek_many(ks).await
    }

    /// Reads every entry from Redis, a page at a time, and caches it locally for as long as it lives, Redisson's `preloadCache()` and `preloadCache(count)`. Resolves to the number of entries it cached.
    #[doc(alias = "preloadCache")]
    pub fn preload_cache(&self) -> PreloadCache<'_, K, V, C> {
        PreloadCache {
            cache: self,
            page: PRELOAD_PAGE,
        }
    }

    /// Same as [`HashMapCache::entry_ttl`].
    #[doc(alias = "remainTimeToLive")]
    pub async fn entry_ttl<Q>(&self, k: &Q) -> Result<Option<Duration>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.cache.entry_ttl(k).await
    }

    /// Same as [`HashMapCache::contains_value`].
    #[doc(alias = "containsValue")]
    pub async fn contains_value<W>(&self, v: &W) -> Result<bool>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        self.cache.contains_value(v).await
    }

    /// Same as [`HashMapCache::value_len`].
    #[doc(alias = "valueSize")]
    pub async fn value_len<Q>(&self, k: &Q) -> Result<usize>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.cache.value_len(k).await
    }

    /// Same as [`HashMapCache::random_keys`].
    #[doc(alias = "randomKeys")]
    pub async fn random_keys(&self, count: usize) -> Result<std::vec::Vec<K>> {
        self.cache.random_keys(count).await
    }

    /// Same as [`HashMapCache::iter`]; it neither uses nor fills the local cache.
    pub fn iter(&self) -> impl Stream<Item = Result<(K, V)>> + '_ {
        self.cache.iter()
    }

    /// Same as [`HashMapCache::keys`].
    pub fn keys(&self) -> impl Stream<Item = Result<K>> + '_ {
        self.cache.keys()
    }

    /// Same as [`HashMapCache::values`].
    pub fn values(&self) -> impl Stream<Item = Result<V>> + '_ {
        self.cache.values()
    }

    /// Same as [`HashMapCache::iter_with`].
    #[doc(alias = "entrySet")]
    pub fn iter_with(&self, scan: MapScan) -> impl Stream<Item = Result<(K, V)>> + '_ {
        self.cache.iter_with(scan)
    }

    /// Same as [`HashMapCache::keys_with`].
    #[doc(alias = "keySet")]
    pub fn keys_with(&self, scan: MapScan) -> impl Stream<Item = Result<K>> + '_ {
        self.cache.keys_with(scan)
    }

    /// Same as [`HashMapCache::values_with`].
    pub fn values_with(&self, scan: MapScan) -> impl Stream<Item = Result<V>> + '_ {
        self.cache.values_with(scan)
    }

    /// Same as [`HashMapCache::set_max_size`]. It limits the entries in Redis, not the local copies ([`cache_size`](LocalCachedMapCacheBuilder::cache_size) does that). Local hits do not reach Redis, so they do not count as uses there.
    #[doc(alias = "setMaxSize")]
    pub async fn set_max_size(&self, max: usize, mode: EvictionMode) -> Result<()> {
        self.cache.set_max_size(max, mode).await
    }

    /// Same as [`HashMapCache::init_max_size`].
    #[doc(alias = "trySetMaxSize")]
    pub async fn init_max_size(&self, max: usize, mode: EvictionMode) -> Result<bool> {
        self.cache.init_max_size(max, mode).await
    }

    /// Same as [`HashMapCache::events`].
    #[doc(
        alias = "addListener",
        alias = "MapEntryListener",
        alias = "EntryCreatedListener",
        alias = "EntryUpdatedListener",
        alias = "EntryRemovedListener",
        alias = "EntryExpiredListener"
    )]
    pub async fn events(&self) -> Result<EntryEvents<K, V, C>> {
        self.cache.events().await
    }

    /// Same as [`HashMapCache::events_of`].
    pub async fn events_of(&self, kinds: &[EntryEventKind]) -> Result<EntryEvents<K, V, C>> {
        self.cache.events_of(kinds).await
    }

    /// Same as [`HashMapCache::keyspace_events`].
    #[doc(
        alias = "addListener",
        alias = "MapPutListener",
        alias = "MapIncrListener",
        alias = "MapRemoveListener",
        alias = "MapDeletedListener",
        alias = "MapExpiredListener"
    )]
    pub async fn keyspace_events(&self) -> Result<MapEvents<K, C>> {
        self.cache.keyspace_events().await
    }

    /// Same as [`HashMapCache::keyspace_events_of`].
    pub async fn keyspace_events_of(&self, kinds: &[MapEventKind]) -> Result<MapEvents<K, C>> {
        self.cache.keyspace_events_of(kinds).await
    }

    per_key_objects!();

    /// Number of entries in the local cache of this instance, including cached misses.
    pub fn local_len(&self) -> usize {
        self.local.local.len()
    }

    /// Returns the entries in the local cache of this instance. Redisson's `getCachedMap`, `cachedEntrySet`, `cachedKeySet` and `cachedValues`.
    #[doc(
        alias = "getCachedMap",
        alias = "cachedEntrySet",
        alias = "cachedKeySet",
        alias = "cachedValues"
    )]
    pub fn local_entries(&self) -> Result<std::vec::Vec<(K, V)>> {
        self.local
            .local
            .entries()
            .iter()
            .map(|(key, value)| Ok((self.codec.decode(key)?, self.codec.decode(value)?)))
            .collect()
    }

    /// Empties the local cache of this instance only.
    pub fn clear_local(&self) {
        self.local.local.forget_all();
    }

    /// Empties the local cache of this instance and tells every other instance to empty theirs, like Redisson's `clearLocalCache`. Redis is not changed. It returns once the message is published, without waiting for the others.
    #[doc(alias = "clearLocalCache")]
    pub async fn clear_local_everywhere(&self) -> Result<()> {
        self.local.local.forget_all();
        self.local
            .publish(&self.key, message(&self.local.local.id, CLEAR, &[]))
            .await
    }
}

impl<K, V, C> LocalCachedMapCache<K, V, C>
where
    K: Serialize + DeserializeOwned + Eq + Hash + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    /// Same as [`HashMapCache::random_entries`].
    #[doc(alias = "randomEntries")]
    pub async fn random_entries(&self, count: usize) -> Result<std::collections::HashMap<K, V>> {
        self.cache.random_entries(count).await
    }

    /// Same as [`HashMapCache::to_hash_map`]; it neither uses nor fills the local cache.
    #[doc(
        alias = "readAllMap",
        alias = "readAllEntrySet",
        alias = "readAllKeySet",
        alias = "readAllValues"
    )]
    pub async fn to_hash_map(&self) -> Result<std::collections::HashMap<K, V>> {
        self.cache.to_hash_map().await
    }
}
