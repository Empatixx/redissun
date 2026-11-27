//! Redisson's `RMapCacheNative`: a hash whose fields expire on the server (Redis 7.4 hash field expiration).

mod support;

use crate::codec::Codec;
use crate::collections::hash_map::HashMap;
use crate::collections::map::key_hash::per_key_objects;
use crate::collections::map::load::LoadAll;
use crate::collections::map::scan::MapScan;
use crate::collections::map::store::{store_options, StoreOptions};
use crate::error::Result;
use crate::object::{HasKey, Key};
use futures::Stream;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::hash::Hash;
use std::time::Duration;
use support::{require, Needs};

pub(crate) use support::HashFieldSupport;

/// **Experimental.** A distributed map in one Redis hash whose entries can expire on their own, Redisson's `RMapCacheNative`.
///
/// It needs Redis 7.4 or newer, or Valkey 9, for the methods that set or read a time to live (hash field expiration), and Redis 8.0 for the ones that send `HSETEX`; such a method returns [`Error::Unsupported`](crate::Error::Unsupported) on an older server and writes nothing. Every method it shares with [`HashMap`](crate::HashMap) works the same way, also on older servers, and the hash is the one a `HashMap` of the same name and a Redisson `RMapCacheNative` read.
///
/// Unlike [`HashMapCache`](crate::HashMapCache), the server deletes expired entries itself, so there is no clean-up task and no extra keys; it has no size limit, no idle limit, no entry events and no leases, as in Redisson.
pub struct MapCacheNative<K, V, C: Codec> {
    key: Key,
    codec: C,
    map: HashMap<K, V, C>,
}

impl<K, V, C: Codec> Clone for MapCacheNative<K, V, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
            map: self.map.clone(),
        }
    }
}

impl<K, V, C: Codec> fmt::Debug for MapCacheNative<K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "MapCacheNative")
    }
}

impl<K, V, C: Codec> HasKey for MapCacheNative<K, V, C> {
    fn key(&self) -> &Key {
        &self.key
    }
}

impl<K, V, C: Codec> MapCacheNative<K, V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        let map = HashMap::new(key.clone(), codec.clone());
        Self { key, codec, map }
    }

    fn from_map(map: HashMap<K, V, C>, key: Key, codec: C) -> Self {
        Self { key, codec, map }
    }
}

/// Sets up a [`MapCacheNative`] with a [`MapLoader`](crate::MapLoader) or a [`MapWriter`](crate::MapWriter), Redisson's `getMapCacheNative(MapOptions)`. Get one from [`Client::map_cache_native_builder`](crate::Client::map_cache_native_builder), set its options, then `.build().await`.
#[must_use = "a builder does nothing until build() is awaited"]
pub struct MapCacheNativeBuilder<K, V, C: Codec> {
    key: Key,
    codec: C,
    options: StoreOptions<K, V>,
}

impl<K, V, C: Codec> fmt::Debug for MapCacheNativeBuilder<K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MapCacheNativeBuilder")
            .field("name", &self.key.name())
            .field("options", &self.options)
            .finish()
    }
}

impl<K, V, C: Codec> MapCacheNativeBuilder<K, V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self {
            key,
            codec,
            options: StoreOptions::default(),
        }
    }
}

impl<K, V, C> MapCacheNativeBuilder<K, V, C>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    C: Codec,
{
    store_options!();
}

impl<K, V, C> MapCacheNativeBuilder<K, V, C>
where
    K: DeserializeOwned + Send + Sync + 'static,
    V: DeserializeOwned + Send + Sync + 'static,
    C: Codec,
{
    /// Checks the options and returns the map. Invalid options are an [`Error::Config`](crate::Error::Config).
    pub async fn build(self) -> Result<MapCacheNative<K, V, C>> {
        let store = self.options.into_store(&self.key, self.codec.clone())?;
        let map = HashMap::with_store(self.key.clone(), self.codec.clone(), store);
        Ok(MapCacheNative::from_map(map, self.key, self.codec))
    }
}

impl<K, V, C> MapCacheNative<K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    per_key_objects!();

    /// The time the entry has left, read with `HPTTL`, Redisson's `remainTimeToLive(key)`. `None` when the entry has no time to live or is missing. Needs Redis 7.4.
    #[doc(alias = "remainTimeToLive")]
    pub async fn entry_ttl<Q>(&self, k: &Q) -> Result<Option<Duration>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        require(&self.key, Needs::FieldTtl).await?;
        let mut cmd = self.key.key_cmd("HPTTL");
        cmd.arg("FIELDS").arg(1).arg(&self.codec.encode(k)?[..]);
        let millis: Vec<i64> = self.key.read(cmd).await?;
        Ok(millis.first().and_then(|&ms| ttl_of(ms)))
    }

    /// Same as [`HashMap::get`](crate::HashMap::get). An expired entry is missing.
    pub async fn get<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.map.get(k).await
    }

    /// Same as [`HashMap::remove`](crate::HashMap::remove).
    pub async fn remove<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.map.remove(k).await
    }

    /// Same as [`HashMap::contains_key`](crate::HashMap::contains_key).
    pub async fn contains_key<Q>(&self, k: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.map.contains_key(k).await
    }

    /// Same as [`HashMap::len`](crate::HashMap::len). Expired entries are gone, the server deletes them.
    pub async fn len(&self) -> Result<usize> {
        self.map.len().await
    }

    /// Same as [`HashMap::is_empty`](crate::HashMap::is_empty).
    pub async fn is_empty(&self) -> Result<bool> {
        self.map.is_empty().await
    }

    /// Same as [`HashMap::clear`](crate::HashMap::clear).
    pub async fn clear(&self) -> Result<()> {
        self.map.clear().await
    }

    /// Same as [`HashMap::get_many`](crate::HashMap::get_many).
    pub async fn get_many<Q>(&self, ks: &[&Q]) -> Result<Vec<Option<V>>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.map.get_many(ks).await
    }

    /// Same as [`HashMap::load_all`](crate::HashMap::load_all). Loaded values have no time to live.
    #[doc(alias = "loadAll")]
    pub fn load_all(&self) -> LoadAll<'_, K, V, C> {
        self.map.load_all()
    }

    /// Same as [`HashMap::load_many`](crate::HashMap::load_many). Loaded values have no time to live.
    #[doc(alias = "loadAll")]
    pub fn load_many<Q>(&self, ks: &[&Q]) -> LoadAll<'_, K, V, C>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.map.load_many(ks)
    }

    /// Same as [`HashMap::incr_by`](crate::HashMap::incr_by). Keeps the entry's time to live (`HINCRBY`).
    #[doc(alias = "addAndGet")]
    pub async fn incr_by<Q>(&self, k: &Q, delta: i64) -> Result<i64>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.map.incr_by(k, delta).await
    }

    /// Same as [`HashMap::incr_by_float`](crate::HashMap::incr_by_float). Keeps the entry's time to live (`HINCRBYFLOAT`).
    #[doc(alias = "addAndGet")]
    pub async fn incr_by_float<Q>(&self, k: &Q, delta: f64) -> Result<f64>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.map.incr_by_float(k, delta).await
    }

    /// Same as [`HashMap::compare_and_set`](crate::HashMap::compare_and_set), Redisson's `replace(key, old, new)`.
    #[doc(alias = "replace")]
    pub async fn compare_and_set<Q, W>(&self, k: &Q, old: &W, new: &W) -> Result<bool>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        self.map.compare_and_set(k, old, new).await
    }

    /// Same as [`HashMap::remove_if_eq`](crate::HashMap::remove_if_eq), Redisson's `remove(key, value)`.
    pub async fn remove_if_eq<Q, W>(&self, k: &Q, v: &W) -> Result<bool>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        self.map.remove_if_eq(k, v).await
    }

    /// Same as [`HashMap::remove_many`](crate::HashMap::remove_many).
    #[doc(alias = "fastRemove")]
    pub async fn remove_many<Q>(&self, ks: &[&Q]) -> Result<usize>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.map.remove_many(ks).await
    }

    /// Same as [`HashMap::flush`](crate::HashMap::flush).
    pub async fn flush(&self) -> Result<()> {
        self.map.flush().await
    }

    /// Same as [`HashMap::contains_value`](crate::HashMap::contains_value).
    pub async fn contains_value<W>(&self, v: &W) -> Result<bool>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        self.map.contains_value(v).await
    }

    /// Same as [`HashMap::value_len`](crate::HashMap::value_len).
    #[doc(alias = "valueSize")]
    pub async fn value_len<Q>(&self, k: &Q) -> Result<usize>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.map.value_len(k).await
    }

    /// Same as [`HashMap::random_keys`](crate::HashMap::random_keys).
    #[doc(alias = "randomKeys")]
    pub async fn random_keys(&self, count: usize) -> Result<Vec<K>> {
        self.map.random_keys(count).await
    }

    /// Same as [`HashMap::iter`](crate::HashMap::iter).
    pub fn iter(&self) -> impl Stream<Item = Result<(K, V)>> + '_ {
        self.map.iter()
    }

    /// Same as [`HashMap::keys`](crate::HashMap::keys).
    pub fn keys(&self) -> impl Stream<Item = Result<K>> + '_ {
        self.map.keys()
    }

    /// Same as [`HashMap::values`](crate::HashMap::values).
    pub fn values(&self) -> impl Stream<Item = Result<V>> + '_ {
        self.map.values()
    }

    /// Same as [`HashMap::iter_with`](crate::HashMap::iter_with), Redisson's `entrySet(pattern, count)`.
    #[doc(alias = "entrySet")]
    pub fn iter_with(&self, scan: MapScan) -> impl Stream<Item = Result<(K, V)>> + '_ {
        self.map.iter_with(scan)
    }

    /// Same as [`HashMap::keys_with`](crate::HashMap::keys_with), Redisson's `keySet(pattern, count)`.
    #[doc(alias = "keySet")]
    pub fn keys_with(&self, scan: MapScan) -> impl Stream<Item = Result<K>> + '_ {
        self.map.keys_with(scan)
    }

    /// Same as [`HashMap::values_with`](crate::HashMap::values_with), Redisson's `values(pattern, count)`.
    pub fn values_with(&self, scan: MapScan) -> impl Stream<Item = Result<V>> + '_ {
        self.map.values_with(scan)
    }
}

impl<K, V, C> MapCacheNative<K, V, C>
where
    K: Serialize + DeserializeOwned + Eq + Hash + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    /// Same as [`HashMap::to_hash_map`](crate::HashMap::to_hash_map).
    #[doc(
        alias = "readAllMap",
        alias = "readAllEntrySet",
        alias = "readAllKeySet",
        alias = "readAllValues"
    )]
    pub async fn to_hash_map(&self) -> Result<std::collections::HashMap<K, V>> {
        self.map.to_hash_map().await
    }

    /// Same as [`HashMap::random_entries`](crate::HashMap::random_entries).
    #[doc(alias = "randomEntries")]
    pub async fn random_entries(&self, count: usize) -> Result<std::collections::HashMap<K, V>> {
        self.map.random_entries(count).await
    }
}

/// One `HPTTL` reply item: `-2` (missing) and `-1` (no TTL) are `None`.
fn ttl_of(millis: i64) -> Option<Duration> {
    u64::try_from(millis).ok().map(Duration::from_millis)
}
