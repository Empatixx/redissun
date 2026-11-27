use crate::codec::Codec;
use crate::collections::hash_map::HashMap;
use crate::collections::local_cache::local::{Local, ReadGuard};
use crate::collections::local_cache::messages::{
    log_arguments, message, CLEAR, INVALIDATE, UPDATE,
};
use crate::collections::local_cache::store::Lifetime;
use crate::collections::local_cache::{local_options, start, LocalCache, LocalOptions};
use crate::collections::map::entry::Target;
use crate::collections::map::load::{load_value, LoadAll};
use crate::collections::map::store::{store_options, Store as MapStore, StoreOptions};
use crate::error::Result;
use crate::lua::Lua;
use crate::object::{HasKey, Key};
use crate::SyncStrategy;
use bytes::Bytes;
use futures::future::Either;
use futures::stream;
use futures::Stream;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;
use std::sync::{Arc, LazyLock};

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

/// `del` in [`StoreMode::Local`](crate::StoreMode::Local): Redis holds nothing to delete, so it only publishes `CLEAR` and returns whether the local cache held anything.
pub(crate) static CLEAR_LOCAL: LazyLock<Lua> = LazyLock::new(|| {
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

/// Sets up a [`LocalCachedMap`]. Get one from [`Client::local_cached_map`](crate::Client::local_cached_map). The defaults are Redisson's: no size limit, no local expiry, [`SyncStrategy::Invalidate`], [`ReconnectionStrategy::None`](crate::ReconnectionStrategy::None), [`EvictionPolicy::None`](crate::EvictionPolicy::None), no cached misses, and no loader or writer.
#[must_use = "a builder does nothing until build() is awaited"]
pub struct LocalCachedMapBuilder<K, V, C: Codec> {
    key: Key,
    codec: C,
    local: LocalOptions,
    options: StoreOptions<K, V>,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V, C: Codec> LocalCachedMapBuilder<K, V, C> {
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
        let local = start(&self.key, self.key.redis_key(), &self.local, None).await?;
        Ok(LocalCachedMap {
            map: HashMap::new(self.key.clone(), self.codec.clone()),
            key: self.key,
            codec: self.codec,
            local,
            store,
            _marker: PhantomData,
        })
    }
}

/// A [`HashMap`] with a cache inside this program, modelled on Redisson's `RLocalCachedMap`. Reads that hit the cache cost no network round trip.
///
/// Every write goes to Redis and, when something changed, publishes a message in the same Lua script, so the other instances of the same map can drop or update their copy (see [`SyncStrategy`]). An instance never reacts to its own messages, and its own writes update its cache. What happens after a lost pub/sub connection is set by [`ReconnectionStrategy`](crate::ReconnectionStrategy); when this client falls behind on the messages, the whole local cache is cleared.
///
/// Only changes made through a `LocalCachedMap` are announced. Changes made through a plain [`HashMap`] of the same name are not, so other instances see them only when their entry expires or is pushed out. [`LocalCachedMap::clear`] and [`Object::del`](crate::Object::del) on a `LocalCachedMap` empty the map for everybody.
///
/// As in Redisson, every instance also listens for `__keyevent@*:expired` by default and clears its local cache when the map's key expires; [`ExpirationEventPolicy`](crate::ExpirationEventPolicy) chooses another channel or none. Redis sends these events only when `notify-keyspace-events` contains `Ex` (`Kx` for [`ExpirationEventPolicy::KeyspaceChannel`](crate::ExpirationEventPolicy::KeyspaceChannel)); neither Redisson nor this crate sets it.
///
/// The data in Redis is a normal hash, so a [`HashMap`] with the same name and codec reads the same entries.
///
/// A map built with [`loader`](LocalCachedMapBuilder::loader) or [`writer`](LocalCachedMapBuilder::writer) reads through and writes through (or behind), as Redisson's `LocalCachedMapOptions`; see the "Loaders and writers" guide.
pub struct LocalCachedMap<K, V, C: Codec> {
    /// Redis-side reads; it has no store, so the writer is called once, by this map.
    map: HashMap<K, V, C>,
    key: Key,
    codec: C,
    local: Arc<LocalCache>,
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
        if self.local.local_only() {
            let held = self.cache().len() > 0;
            self.cache().forget_all();
            let mut arguments = self.clear_arguments();
            arguments.push(Bytes::from(if held { "1" } else { "0" }));
            return Some((&CLEAR_LOCAL, self.clear_keys(), arguments));
        }
        self.cache().forget_all();
        Some((&CLEAR_ALL, self.clear_keys(), self.clear_arguments()))
    }
}

impl<K, V, C: Codec> LocalCachedMap<K, V, C> {
    /// The local entries and the reads in flight.
    fn cache(&self) -> &Local {
        &self.local.local
    }

    fn clear_keys(&self) -> std::vec::Vec<String> {
        vec![self.key.redis_key(), self.local.updates_log.clone()]
    }

    fn clear_arguments(&self) -> std::vec::Vec<Bytes> {
        vec![
            Bytes::from(self.local.channel.clone()),
            self.local.mode(),
            message(&self.cache().id, CLEAR, &[]),
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
            Ok(None) if self.local.store_cache_miss => Some(None),
            _ => None,
        };
        read.finish(cached.map(|value| (value, Lifetime::default())));
        loaded
    }

    /// Reads an encoded key: the local cache, then the master unless the entries live locally only.
    pub(crate) async fn read_encoded(&self, k: &Bytes) -> Result<Option<V>> {
        if let Some(Some(value)) = self.cache().lookup(k) {
            return Ok(Some(self.codec.decode(&value)?));
        }
        if self.local.local_only() {
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
        let notice = match self.local.sync {
            SyncStrategy::Update => message(
                &self.cache().id,
                UPDATE,
                &[&(field.len() as u32).to_be_bytes(), &field, &value],
            ),
            _ => message(&self.cache().id, INVALIDATE, &[&field]),
        };
        if self.local.local_only() {
            let previous = self.cache().lookup(&field).flatten();
            self.cache().write(field, Some(value), Lifetime::default());
            if self.local.announces() {
                self.local.publish(&self.key, notice).await?;
            }
            return Ok(previous);
        }
        self.cache()
            .write(field.clone(), Some(value.clone()), Lifetime::default());
        let [now, entry, keep] = log_arguments(&field);
        let written: Result<Option<Bytes>> = self
            .key
            .core
            .eval(
                &PUT,
                self.clear_keys(),
                vec![
                    field.clone(),
                    value,
                    Bytes::from(self.local.channel.clone()),
                    self.local.mode(),
                    notice,
                    now,
                    entry,
                    keep,
                ],
            )
            .await;
        written.inspect_err(|_| self.cache().forget(&field))
    }

    /// The local forget and the `REMOVE` script; returns the previous encoded value. Locally only, the invalidation is published by itself and the previous value is the local one.
    async fn remove_raw(&self, field: Bytes) -> Result<Option<Bytes>> {
        let notice = message(&self.cache().id, INVALIDATE, &[&field]);
        if self.local.local_only() {
            let previous = self.cache().lookup(&field).flatten();
            self.cache().forget(&field);
            if self.local.announces() {
                self.local.publish(&self.key, notice).await?;
            }
            return Ok(previous);
        }
        self.cache().forget(&field);
        let [now, entry, keep] = log_arguments(&field);
        self.key
            .core
            .eval(
                &REMOVE,
                self.clear_keys(),
                vec![
                    field.clone(),
                    Bytes::new(),
                    Bytes::from(self.local.channel.clone()),
                    self.local.mode(),
                    notice,
                    now,
                    entry,
                    keep,
                ],
            )
            .await
    }

    /// Returns the value for a key, from the local cache when it is there. A value read from Redis is cached. The key is borrowed: `map.get("k")`. With a loader, a miss asks the loader, and the value it returns is stored in Redis and cached.
    pub async fn get<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let field = self.codec.encode(k)?;
        match self.cache().lookup(&field) {
            Some(Some(value)) => return Ok(Some(self.codec.decode(&value)?)),
            Some(None) if self.local.store_cache_miss => return Ok(None),
            _ => {}
        }
        if self.local.local_only() {
            if !self.loads() {
                return Ok(None);
            }
            let read = self.cache().begin_read(&field);
            return self.load_missing(field, read).await;
        }
        let read = self.cache().begin_read(&field);
        let mut cmd = self.key.key_cmd("HGET");
        cmd.arg(&field[..]);
        // An error or a cancelled call drops `read`, which caches nothing.
        let raw: Option<Bytes> = self.key.query(cmd).await?;
        if raw.is_none() && self.loads() {
            return self.load_missing(field, read).await;
        }
        let cached = (raw.is_some() || self.local.store_cache_miss).then(|| raw.clone());
        read.finish(cached.map(|value| (value, Lifetime::default())));
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
        if let Some(cached) = self.cache().lookup(&field) {
            return Ok(cached.is_some());
        }
        if self.local.local_only() {
            if !self.loads() {
                return Ok(false);
            }
            let read = self.cache().begin_read(&field);
            return Ok(self.load_missing(field, read).await?.is_some());
        }
        let read = self.cache().begin_read(&field);
        // From the master, as `get`: a miss read from a replica could be cached after the write that ends it.
        let mut cmd = self.key.key_cmd("HEXISTS");
        cmd.arg(&field[..]);
        let found: Result<bool> = self.key.query(cmd).await;
        if matches!(found, Ok(false)) && self.loads() {
            return Ok(self.load_missing(field, read).await?.is_some());
        }
        let miss = matches!(found, Ok(false)) && self.local.store_cache_miss;
        read.finish(miss.then_some((None, Lifetime::default())));
        found
    }

    /// Number of entries in Redis. In [`StoreMode::Local`](crate::StoreMode::Local), the local entries that hold a value.
    pub async fn len(&self) -> Result<usize> {
        if self.local.local_only() {
            return Ok(self.cache().entries().len());
        }
        self.map.len().await
    }

    /// Returns whether Redis holds no entry. In [`StoreMode::Local`](crate::StoreMode::Local), whether no local entry holds a value.
    pub async fn is_empty(&self) -> Result<bool> {
        if self.local.local_only() {
            return Ok(self.cache().entries().is_empty());
        }
        self.map.is_empty().await
    }

    /// Removes every entry for everybody; returns whether there was anything to remove. The other instances empty their local cache only when something was removed. In [`StoreMode::Local`](crate::StoreMode::Local), it empties the local cache, tells the others, and returns whether the local cache held anything.
    pub async fn clear(&self) -> Result<bool> {
        if self.local.local_only() {
            let held = self.cache().len() > 0;
            self.cache().forget_all();
            if self.local.announces() {
                self.local
                    .publish(&self.key, message(&self.cache().id, CLEAR, &[]))
                    .await?;
            }
            return Ok(held);
        }
        self.cache().forget_all();
        self.key
            .del_with(&CLEAR_ALL, self.clear_keys(), self.clear_arguments())
            .await
    }

    /// Number of entries in the local cache of this instance, including cached misses.
    pub fn local_len(&self) -> usize {
        self.cache().len()
    }

    /// Returns the entries in the local cache of this instance. Redisson's `getCachedMap`.
    #[doc(alias = "getCachedMap")]
    pub fn local_entries(&self) -> Result<std::vec::Vec<(K, V)>> {
        let entries = self.cache().entries();
        entries
            .iter()
            .map(|(key, value)| Ok((self.codec.decode(key)?, self.codec.decode(value)?)))
            .collect()
    }

    /// Empties the local cache of this instance only.
    pub fn clear_local(&self) {
        self.cache().forget_all();
    }

    /// Empties the local cache of this instance and tells every other instance to empty theirs, like Redisson's `clearLocalCache`. Redis is not changed. It returns once the message is published, without waiting for the others.
    pub async fn clear_local_everywhere(&self) -> Result<()> {
        self.cache().forget_all();
        self.local
            .publish(&self.key, message(&self.cache().id, CLEAR, &[]))
            .await
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

    /// The encoded local entries that hold a value, for the streams in [`StoreMode::Local`](crate::StoreMode::Local).
    fn snapshot(&self) -> std::vec::IntoIter<(Bytes, Bytes)> {
        self.cache().entries().into_iter()
    }

    /// Streams every entry from Redis. It does not use or fill the local cache. In [`StoreMode::Local`](crate::StoreMode::Local), it streams a snapshot of the local entries.
    pub fn iter(&self) -> impl Stream<Item = Result<(K, V)>> + '_ {
        if self.local.local_only() {
            return Either::Right(stream::iter(
                self.snapshot()
                    .map(|(k, v)| Ok((self.codec.decode(&k)?, self.codec.decode(&v)?))),
            ));
        }
        Either::Left(self.map.iter())
    }

    /// Streams every key from Redis. In [`StoreMode::Local`](crate::StoreMode::Local), the keys of the local entries.
    pub fn keys(&self) -> impl Stream<Item = Result<K>> + '_ {
        if self.local.local_only() {
            return Either::Right(stream::iter(
                self.snapshot().map(|(k, _)| self.codec.decode(&k)),
            ));
        }
        Either::Left(self.map.keys())
    }

    /// Streams every value from Redis. In [`StoreMode::Local`](crate::StoreMode::Local), the values of the local entries.
    pub fn values(&self) -> impl Stream<Item = Result<V>> + '_ {
        if self.local.local_only() {
            return Either::Right(stream::iter(
                self.snapshot().map(|(_, v)| self.codec.decode(&v)),
            ));
        }
        Either::Left(self.map.values())
    }
}
