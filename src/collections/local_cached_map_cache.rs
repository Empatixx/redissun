use crate::codec::Codec;
use crate::collections::hash_map_cache::{mark_listeners, HashMapCache, Insert};
use crate::collections::local_cache::messages::{message, CLEAR};
use crate::collections::local_cache::{local_options, start, LocalCache, LocalOptions, StoreMode};
use crate::collections::map::store::{store_options, StoreOptions};
use crate::error::Result;
use crate::lua::Lua;
use crate::object::{HasKey, Key};
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

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
