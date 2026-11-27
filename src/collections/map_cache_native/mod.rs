//! Redisson's `RMapCacheNative`: a hash whose fields expire on the server (Redis 7.4 hash field expiration).

mod builders;
mod scripts;
mod support;
mod ttl;

use crate::codec::Codec;
use crate::collections::hash_map::HashMap;
use crate::collections::map::key_hash::per_key_objects;
use crate::collections::map::load::LoadAll;
use crate::collections::map::scan::MapScan;
use crate::collections::map::store::{store_options, StoreOptions};
use crate::error::Result;
use crate::object::{HasKey, Key};
use bytes::Bytes;
use futures::Stream;
use scripts::{COMPARE_AND_SET_KEEP_TTL, REPLACE_KEEP_TTL};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::hash::Hash;
use std::time::Duration;
use support::{has_field_ttl, require, Needs};

pub use builders::{
    ExpireEntries, ExpireEntry, Extend, Insert, Replace, Set, TryExtend, TryInsert, UpdateMany,
};
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

    /// Inserts an entry and returns the value it replaced, like [`HashMap::insert`](crate::HashMap::insert). Await it as it is, or set a time first: `map.insert("a", &v).ttl(d).await`.
    ///
    /// Without a time, the entry is stored without one and loses any TTL it had (`HSET`). With [`ttl`](Insert::ttl) or [`expire_at`](Insert::expire_at), it gets that time, replacing the old one (Redis 7.4). With a writer, the entry is written after Redis stored it.
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

    /// Stores the entry without reading the old value and returns whether the key was new, like [`HashMap::set`](crate::HashMap::set). A time works as on [`insert`](MapCacheNative::insert).
    #[doc(alias = "fastPut")]
    pub fn set<'a, Q, W>(&'a self, k: &'a Q, v: &'a W) -> Set<'a, K, V, C, Q, W>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        Set::new(self, k, v)
    }

    /// Stores the entry only when the key is missing, like [`HashMap::try_insert`](crate::HashMap::try_insert). Returns `None` when it stored it, or the present value, which keeps its value and its TTL. A time applies only to an entry it stores.
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

    /// Replaces the value of a present key and returns the value it replaced, like [`HashMap::replace`](crate::HashMap::replace); a missing key stays missing and gives `None`.
    ///
    /// Without a time, the entry keeps its TTL, like [`HashMapCache::replace`](crate::HashMapCache::replace); Redisson's `replace` clears it. With [`ttl`](Replace::ttl) or [`expire_at`](Replace::expire_at), it gets the new time, and `.ttl(Duration::ZERO)` leaves it without one, as Redisson's `putIfExists(k, v)`.
    #[doc(
        alias = "putIfExist",
        alias = "putIfExists",
        alias = "fastPutIfExists",
        alias = "fastReplace"
    )]
    pub fn replace<'a, Q, W>(&'a self, k: &'a Q, v: &'a W) -> Replace<'a, K, V, C, Q, W>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        Replace::new(self, k, v)
    }

    /// Inserts all entries, in one call unless [`batch_size`](Extend::batch_size) splits them, like [`HashMap::extend`](crate::HashMap::extend), Redisson's `putAll`. Entries are `(&key, &value)` pairs. An empty list sends nothing.
    ///
    /// Without a time, the entries are stored without one and lose any TTL they had (`HSET`). With [`ttl`](Extend::ttl) each entry gets that time (Redisson's `putAll(map, ttl)` script, Redis 7.4); [`expire_at`](Extend::expire_at) and [`keep_ttl`](Extend::keep_ttl) send `HSETEX`, which needs Redis 8.0 (Redisson's `putAll(PutArgs)`). With a writer, each batch is written after Redis stored it.
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
        Extend::new(self, self.encode_entries(entries))
    }

    /// Stores all entries only when none of the keys is present, in one `HSETEX … FNX`, Redisson's `putIfAllKeysAbsent(PutArgs)`. Returns whether it stored them; when one key is present, nothing changes. An empty list gives `false` without a call. A time is set with [`ttl`](TryExtend::ttl), [`expire_at`](TryExtend::expire_at) or [`keep_ttl`](TryExtend::keep_ttl). With a writer, every entry is written when they were stored. Needs Redis 8.0.
    #[doc(alias = "putIfAllKeysAbsent")]
    pub fn try_extend<'a, Q, W>(
        &self,
        entries: impl IntoIterator<Item = (&'a Q, &'a W)>,
    ) -> TryExtend<'_, K, V, C>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync + 'a,
        W: Serialize + ?Sized + Sync + 'a,
    {
        TryExtend::new(self, self.encode_entries(entries))
    }

    /// Overwrites all entries only when every key is present, in one `HSETEX … FXX`, Redisson's `putIfAllKeysExist(PutArgs)`. Returns whether it stored them; when one key is missing, nothing changes. An empty list gives `false` without a call. Without a time the entries lose their TTL; [`keep_ttl`](UpdateMany::keep_ttl) keeps it. With a writer, every entry is written when they were stored. Needs Redis 8.0.
    #[doc(alias = "putIfAllKeysExist")]
    pub fn update_many<'a, Q, W>(
        &self,
        entries: impl IntoIterator<Item = (&'a Q, &'a W)>,
    ) -> UpdateMany<'_, K, V, C>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync + 'a,
        W: Serialize + ?Sized + Sync + 'a,
    {
        UpdateMany::new(self, self.encode_entries(entries))
    }

    /// Encodes `(&key, &value)` pairs at once; an error comes back when the builder is awaited.
    fn encode_entries<'a, Q, W>(
        &self,
        entries: impl IntoIterator<Item = (&'a Q, &'a W)>,
    ) -> Result<Vec<(Bytes, Bytes)>>
    where
        Q: Serialize + ?Sized + Sync + 'a,
        W: Serialize + ?Sized + Sync + 'a,
    {
        entries
            .into_iter()
            .map(|(k, v)| Ok((self.codec.encode(k)?, self.codec.encode(v)?)))
            .collect()
    }

    /// The time each entry has left, in the order of the keys, Redisson's `remainTimeToLive(keys)`. `None` for an entry without a TTL or a missing one. A key listed twice gives two items. Needs Redis 7.4.
    #[doc(alias = "remainTimeToLive")]
    pub async fn entry_ttls<Q>(&self, ks: &[&Q]) -> Result<Vec<Option<Duration>>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if ks.is_empty() {
            return Ok(Vec::new());
        }
        require(&self.key, Needs::FieldTtl).await?;
        let fields = ks
            .iter()
            .map(|k| self.codec.encode(*k))
            .collect::<Result<Vec<Bytes>>>()?;
        let mut cmd = self.key.key_cmd("HPTTL");
        cmd.arg("FIELDS")
            .arg(fields.len())
            .arg(crate::client::request::binary(&fields));
        let millis: Vec<i64> = self.key.read(cmd).await?;
        Ok(millis.into_iter().map(ttl_of).collect())
    }

    /// Sets a new time to live on a present entry, Redisson's `expireEntry` and its `IfNotSet`/`IfGreater`/`IfLess` forms. Await it after [`ttl`](ExpireEntry::ttl) or [`expire_at`](ExpireEntry::expire_at), optionally with [`if_not_set`](ExpireEntry::if_not_set), [`if_greater`](ExpireEntry::if_greater) or [`if_less`](ExpireEntry::if_less). Returns whether it changed the entry; a missing entry gives `false`. No writer is called. Needs Redis 7.4.
    ///
    /// Without a time, or with `.ttl(Duration::ZERO)`, it removes the TTL and returns whether the entry exists, as [`HashMapCache::expire_entry`](crate::HashMapCache::expire_entry) does; Redisson's `expireEntry(k, ZERO)` deletes the entry instead. A condition without a time is an [`Error::Config`](crate::Error::Config).
    ///
    /// ```no_run
    /// # async fn run(map: redissun::MapCacheNative<String, String, redissun::JsonCodec>) -> redissun::Result<()> {
    /// use std::time::Duration;
    /// map.expire_entry("a").ttl(Duration::from_secs(60)).if_greater().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[doc(
        alias = "expireEntry",
        alias = "expireEntryIfNotSet",
        alias = "expireEntryIfGreater",
        alias = "expireEntryIfLess"
    )]
    pub fn expire_entry<'a, Q>(&'a self, k: &'a Q) -> ExpireEntry<'a, K, V, C, Q>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        ExpireEntry::new(self, k)
    }

    /// [`expire_entry`](MapCacheNative::expire_entry) for several entries in one script, Redisson's `expireEntries` and its `IfNotSet`/`IfGreater`/`IfLess` forms. Returns how many entries it changed; a key listed twice is counted twice. Without a time it removes the TTLs and counts the entries that exist. An empty list gives `0` without a call.
    #[doc(
        alias = "expireEntries",
        alias = "expireEntriesIfNotSet",
        alias = "expireEntriesIfGreater",
        alias = "expireEntriesIfLess"
    )]
    pub fn expire_entries<'a, Q>(&'a self, ks: &'a [&'a Q]) -> ExpireEntries<'a, K, V, C, Q>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        ExpireEntries::new(self, ks)
    }

    /// Removes the entry's time to live (`HPERSIST`), Redisson's `clearExpire(key)`. Returns `true` only when there was one to remove; an entry without a TTL or a missing one gives `false`. No writer is called. Needs Redis 7.4.
    #[doc(alias = "clearExpire")]
    pub async fn persist_entry<Q>(&self, k: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        require(&self.key, Needs::FieldTtl).await?;
        let replies = self.persist_fields(&[self.codec.encode(k)?]).await?;
        Ok(replies.first() == Some(&1))
    }

    /// [`persist_entry`](MapCacheNative::persist_entry) for several entries in one `HPERSIST`, Redisson's `clearExpire(keys)`: one flag per key, in key order. An empty list gives `[]` without a call.
    #[doc(alias = "clearExpire")]
    pub async fn persist_entries<Q>(&self, ks: &[&Q]) -> Result<Vec<bool>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if ks.is_empty() {
            return Ok(Vec::new());
        }
        require(&self.key, Needs::FieldTtl).await?;
        let fields = ks
            .iter()
            .map(|k| self.codec.encode(*k))
            .collect::<Result<Vec<Bytes>>>()?;
        let replies = self.persist_fields(&fields).await?;
        Ok(replies.into_iter().map(|reply| reply == 1).collect())
    }

    /// `HPERSIST key FIELDS n …`: per field `1` (TTL removed), `-1` (no TTL) or `-2` (missing).
    async fn persist_fields(&self, fields: &[Bytes]) -> Result<Vec<i64>> {
        let mut cmd = self.key.key_cmd("HPERSIST");
        cmd.arg("FIELDS")
            .arg(fields.len())
            .arg(crate::client::request::binary(fields));
        self.key.query(cmd).await
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

    /// Same as [`HashMap::compare_and_set`](crate::HashMap::compare_and_set), Redisson's `replace(key, old, new)`, and keeps the entry's time to live, like [`HashMapCache::compare_and_set`](crate::HashMapCache::compare_and_set). Redisson's `replace(key, old, new)` clears it.
    #[doc(alias = "replace")]
    pub async fn compare_and_set<Q, W>(&self, k: &Q, old: &W, new: &W) -> Result<bool>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        // A server without field TTLs has no TTL to keep, and its script would fail at HPTTL.
        if !has_field_ttl(&self.key).await? {
            return self.map.compare_and_set(k, old, new).await;
        }
        let (k, new) = (self.codec.encode(k)?, self.codec.encode(new)?);
        let args = vec![k.clone(), self.codec.encode(old)?, new.clone()];
        let keys = vec![self.key.redis_key()];
        let core = &self.key.core;
        // At most once when a writer waits for the change, as HashMap::compare_and_set: a second run would answer 0.
        let flag: i64 = if self.map.writer().is_some() {
            core.eval_once(&COMPARE_AND_SET_KEEP_TTL, keys, args)
                .await?
        } else {
            core.eval(&COMPARE_AND_SET_KEEP_TTL, keys, args).await?
        };
        if flag == 1 {
            self.map.written(k, new).await?;
        }
        Ok(flag == 1)
    }

    /// `replace` without a time: Redisson's `replace(k, v)` script, keeping the entry's TTL where the server has field TTLs.
    async fn replace_keeping_ttl<Q, W>(&self, k: &Q, v: &W) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        if !has_field_ttl(&self.key).await? {
            return self.map.replace(k, v).await;
        }
        let (k, v) = (self.codec.encode(k)?, self.codec.encode(v)?);
        // At most once: run again after a lost reply, it would return this call's own value as the old one.
        let previous: Option<Bytes> = self
            .key
            .core
            .eval_once(
                &REPLACE_KEEP_TTL,
                vec![self.key.redis_key()],
                vec![k.clone(), v.clone()],
            )
            .await?;
        if previous.is_some() {
            self.map.written(k, v).await?;
        }
        self.map.decode_value(previous)
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
