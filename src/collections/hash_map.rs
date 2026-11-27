use crate::codec::Codec;
use crate::collections::map::entry::Target;
pub use crate::collections::map::entry::{Entry, OccupiedEntry, VacantEntry};
use crate::collections::map::events::{self, MapEventKind, MapEvents};
use crate::collections::map::key_hash::per_key_objects;
use crate::collections::map::load::load_value;
pub use crate::collections::map::load::LoadAll;
use crate::collections::map::scan::{decode_pairs, hscan, MapScan};
use crate::collections::map::store::{store_options, Store, StoreOptions};
use crate::error::Result;
use crate::lua::Lua;
use crate::object::{HasKey, Key};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::hash::Hash;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};

pub(crate) static INSERT: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local previous = redis.call('HGET', KEYS[1], ARGV[1])
        redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
        return previous",
    )
});

pub(crate) static REMOVE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local previous = redis.call('HGET', KEYS[1], ARGV[1])
        redis.call('HDEL', KEYS[1], ARGV[1])
        return previous",
    )
});

static PUT_IF_ABSENT: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "if redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2]) == 1 then
            return nil
        end
        return redis.call('HGET', KEYS[1], ARGV[1])",
    )
});

static REPLACE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then
            local value = redis.call('HGET', KEYS[1], ARGV[1])
            redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
            return value
        end
        return nil",
    )
});

static REPLACE_IF: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then
            redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
            return 1
        end
        return 0",
    )
});

static REMOVE_IF: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then
            return redis.call('HDEL', KEYS[1], ARGV[1])
        end
        return 0",
    )
});

/// RedissonMap.fastRemoveOperationBatchAsync: one `0`/`1` per key, for the writer to get the removed keys.
static FAST_REMOVE_BATCH: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local result = {}; \
        for i = 1, #ARGV, 1 do \
        local val = redis.call('hdel', KEYS[1], ARGV[i]); \
        table.insert(result, val); \
        end;\
        return result;",
    )
});

static CONTAINS_VALUE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local values = redis.call('HVALS', KEYS[1])
        for i = 1, #values, 1 do
            if ARGV[1] == values[i] then
                return 1
            end
        end
        return 0",
    )
});

/// A distributed map stored in a Redis hash.
///
/// A map from [`Client::hash_map_builder`](crate::Client::hash_map_builder) can have a [`MapLoader`](crate::MapLoader) and a [`MapWriter`](crate::MapWriter); see the "Loaders and writers" guide.
pub struct HashMap<K, V, C: Codec> {
    key: Key,
    codec: C,
    /// The loader and writer; `None` keeps every call as it is without them.
    store: Option<Arc<Store<K, V, C>>>,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V, C: Codec> Clone for HashMap<K, V, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
            store: self.store.clone(),
            _marker: PhantomData,
        }
    }
}

impl<K, V, C: Codec> fmt::Debug for HashMap<K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "HashMap")
    }
}

impl<K, V, C: Codec> HasKey for HashMap<K, V, C> {
    fn key(&self) -> &Key {
        &self.key
    }
}

impl<K, V, C: Codec> HashMap<K, V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self::with_store(key, codec, None)
    }

    pub(crate) fn with_store(key: Key, codec: C, store: Option<Arc<Store<K, V, C>>>) -> Self {
        Self {
            key,
            codec,
            store,
            _marker: PhantomData,
        }
    }

    pub(crate) fn store(&self) -> Option<&Store<K, V, C>> {
        self.store.as_deref()
    }

    /// Whether a miss asks the loader.
    fn loads(&self) -> bool {
        self.store.as_ref().is_some_and(|store| store.has_loader())
    }

    /// The store when it has a writer; `None` skips every writer call.
    fn writer(&self) -> Option<&Store<K, V, C>> {
        self.store.as_deref().filter(|store| store.has_writer())
    }
}

/// Sets up a [`HashMap`] with a [`MapLoader`](crate::MapLoader) or a [`MapWriter`](crate::MapWriter), Redisson's `getMap(name, MapOptions)`. Get one from [`Client::hash_map_builder`](crate::Client::hash_map_builder), set its options, then `.build().await`.
#[must_use = "a builder does nothing until build() is awaited"]
pub struct HashMapBuilder<K, V, C: Codec> {
    key: Key,
    codec: C,
    options: StoreOptions<K, V>,
}

impl<K, V, C: Codec> fmt::Debug for HashMapBuilder<K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HashMapBuilder")
            .field("name", &self.key.name())
            .field("options", &self.options)
            .finish()
    }
}

impl<K, V, C: Codec> HashMapBuilder<K, V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self {
            key,
            codec,
            options: StoreOptions::default(),
        }
    }
}

impl<K, V, C> HashMapBuilder<K, V, C>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    C: Codec,
{
    store_options!();
}

impl<K, V, C> HashMapBuilder<K, V, C>
where
    K: DeserializeOwned + Send + Sync + 'static,
    V: DeserializeOwned + Send + Sync + 'static,
    C: Codec,
{
    /// Checks the options and returns the map. Invalid options are an [`Error::Config`](crate::Error::Config).
    pub async fn build(self) -> Result<HashMap<K, V, C>> {
        let store = self.options.into_store(&self.key, self.codec.clone())?;
        Ok(HashMap::with_store(self.key, self.codec, store))
    }
}

impl<K, V, C> HashMap<K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    fn decode_value(&self, raw: Option<Bytes>) -> Result<Option<V>> {
        raw.map(|bytes| self.codec.decode(&bytes)).transpose()
    }

    /// Hands a stored entry to the writer, if there is one.
    async fn written(&self, k: Bytes, v: Bytes) -> Result<()> {
        match self.writer() {
            Some(store) => store.written(&[(k, v)]).await,
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

    /// The value of `k`, taken under the key's lock to read and change it, like [`std::collections::hash_map::Entry`]: Redisson's `compute`, `computeIfAbsent`, `computeIfPresent` and `merge`.
    ///
    /// The lock is the one [`lock`](HashMap::lock) returns, the lock Redisson's compute family takes, so programs do not change the key at once. Calls for the same key from this client wait for each other, also futures joined in one task: while you hold an entry, taking another one for the same key in the same task waits forever.
    ///
    /// ```no_run
    /// # async fn run(client: redissun::Client) -> redissun::Result<()> {
    /// let visits = client.hash_map::<String, i64>("visits");
    /// visits.entry("home").await?.and_modify(|n| *n += 1).or_insert(1).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[doc(
        alias = "compute",
        alias = "computeIfAbsent",
        alias = "computeIfPresent",
        alias = "merge"
    )]
    pub async fn entry<Q>(&self, k: &Q) -> Result<Entry<'_, K, V, C>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Entry::acquire(Target::Map(self), self.codec.encode(k)?).await
    }

    /// Starts listening to every kind of change of the map, Redisson's `addListener` with the map and object listeners. See [`MapEvents`] for the server setting it needs.
    ///
    /// On a [`HashMapCache`](crate::HashMapCache), these keyspace notifications come from [`keyspace_events`](crate::HashMapCache::keyspace_events), and its `events()` returns the cache's entry events.
    #[doc(alias = "addListener")]
    pub async fn events(&self) -> Result<MapEvents<K, C>> {
        self.events_of(&MapEventKind::ALL).await
    }

    /// Starts listening to the given kinds of change.
    pub async fn events_of(&self, kinds: &[MapEventKind]) -> Result<MapEvents<K, C>> {
        events::subscribe(&self.key, self.codec.clone(), kinds, None).await
    }

    per_key_objects!();

    /// Inserts an entry and returns the value it replaced. The key and value are borrowed: `map.insert("a", &value)`. With a writer, the entry is written after Redis stored it.
    pub async fn insert<Q, W>(&self, k: &Q, v: &W) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        let (k, v) = (self.codec.encode(k)?, self.codec.encode(v)?);
        let previous: Option<Bytes> = self
            .key
            .core
            .eval(
                &INSERT,
                vec![self.key.redis_key()],
                vec![k.clone(), v.clone()],
            )
            .await?;
        self.written(k, v).await?;
        self.decode_value(previous)
    }

    /// Returns the value for the key, or `None` when it is missing. With a loader, a miss asks the loader and stores what it returns.
    pub async fn get<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let encoded = self.codec.encode(k)?;
        let raw: Option<Bytes> = self
            .key
            .read({
                let mut cmd = redis::cmd("HGET");
                cmd.arg(self.key.redis_key());
                cmd.arg(&encoded[..]);
                cmd
            })
            .await?;
        match self.decode_value(raw)? {
            None if self.loads() => load_value(&Target::Map(self), &encoded, false).await,
            value => Ok(value),
        }
    }

    /// Removes the entry and returns its value. With a writer, the key is deleted from the store also when the map did not have it, as in Redisson.
    pub async fn remove<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let k = self.codec.encode(k)?;
        let previous: Option<Bytes> = self
            .key
            .core
            .eval(&REMOVE, vec![self.key.redis_key()], vec![k.clone()])
            .await?;
        self.deleted(k).await?;
        self.decode_value(previous)
    }

    /// Returns whether the key is present. With a loader, a miss asks the loader, and a loaded value counts as present.
    pub async fn contains_key<Q>(&self, k: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let encoded = self.codec.encode(k)?;
        let found: bool = self
            .key
            .read({
                let mut cmd = redis::cmd("HEXISTS");
                cmd.arg(self.key.redis_key());
                cmd.arg(&encoded[..]);
                cmd
            })
            .await?;
        if !found && self.loads() {
            return Ok(load_value(&Target::Map(self), &encoded, false)
                .await?
                .is_some());
        }
        Ok(found)
    }

    /// Number of entries.
    pub async fn len(&self) -> Result<usize> {
        let len: usize = self
            .key
            .read({
                let mut cmd = redis::cmd("HLEN");
                cmd.arg(self.key.redis_key());
                cmd
            })
            .await?;
        Ok(len)
    }

    /// Returns whether the map has no entries.
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// Deletes every entry.
    pub async fn clear(&self) -> Result<()> {
        self.key
            .query::<()>({
                let mut cmd = redis::cmd("DEL");
                cmd.arg(self.key.redis_key());
                cmd
            })
            .await?;
        Ok(())
    }

    /// Inserts all entries, in one round trip unless [`batch_size`](Extend::batch_size) splits them, Redisson's `putAll`. Entries are `(&key, &value)` pairs, for example `map.extend(other.iter()).await`.
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
            .collect::<Result<Vec<(Bytes, Bytes)>>>();
        Extend {
            map: self,
            encoded,
            batch_size: 0,
        }
    }

    /// Returns the values for several keys, in the order of the keys. With a loader, the missing keys are loaded one at a time, in the order of the keys.
    pub async fn get_many<Q>(&self, ks: &[&Q]) -> Result<Vec<Option<V>>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if ks.is_empty() {
            return Ok(Vec::new());
        }
        let fields = ks
            .iter()
            .map(|k| self.codec.encode(*k))
            .collect::<Result<Vec<Bytes>>>()?;
        let raw: Vec<Option<Bytes>> = self
            .key
            .read({
                let mut cmd = redis::cmd("HMGET");
                cmd.arg(self.key.redis_key());
                cmd.arg(crate::client::request::binary(&fields));
                cmd
            })
            .await?;
        let mut values = raw
            .into_iter()
            .map(|r| self.decode_value(r))
            .collect::<Result<Vec<Option<V>>>>()?;
        if self.loads() {
            for (value, field) in values.iter_mut().zip(&fields) {
                if value.is_none() {
                    *value = load_value(&Target::Map(self), field, false).await?;
                }
            }
        }
        Ok(values)
    }

    /// Loads every key of the loader into the map, Redisson's `loadAll(replaceExistingValues, parallelism)`. Keys the map holds keep their value unless [`replace_existing`](LoadAll::replace_existing). Without a loader it is an [`Error::Config`](crate::Error::Config).
    #[doc(alias = "loadAll")]
    pub fn load_all(&self) -> LoadAll<'_, K, V, C> {
        LoadAll::new(Target::Map(self), None)
    }

    /// Loads the given keys into the map, Redisson's `loadAll(keys, replaceExistingValues, parallelism)`.
    #[doc(alias = "loadAll")]
    pub fn load_many<Q>(&self, ks: &[&Q]) -> LoadAll<'_, K, V, C>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let keys = ks.iter().map(|k| self.codec.encode(*k)).collect();
        LoadAll::new(Target::Map(self), Some(keys))
    }

    /// Adds `delta` to the integer stored under the key, creating it at zero, and returns the result.
    pub async fn incr_by<Q>(&self, k: &Q, delta: i64) -> Result<i64>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let k = self.codec.encode(k)?;
        let value: i64 = self
            .key
            .query_once({
                let mut cmd = redis::cmd("HINCRBY");
                cmd.arg(self.key.redis_key());
                cmd.arg(&k[..]);
                cmd.arg(delta);
                cmd
            })
            .await?;
        // The writer gets the new number as `get` would return it.
        if self.writer().is_some() {
            self.written(k, Bytes::from(value.to_string())).await?;
        }
        Ok(value)
    }

    /// Adds `delta` to the float stored under the key, creating it at zero, and returns the result.
    pub async fn incr_by_float<Q>(&self, k: &Q, delta: f64) -> Result<f64>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let k = self.codec.encode(k)?;
        let mut cmd = redis::cmd("HINCRBYFLOAT");
        cmd.arg(self.key.redis_key());
        cmd.arg(&k[..]);
        cmd.arg(delta);
        if self.writer().is_none() {
            let value: f64 = self.key.query_once(cmd).await?;
            return Ok(value);
        }
        // The writer gets the reply's text, the bytes `get` would decode.
        let reply: Bytes = self.key.query_once(cmd).await?;
        let value = std::str::from_utf8(&reply)
            .ok()
            .and_then(|text| text.parse::<f64>().ok())
            .ok_or_else(|| {
                crate::error::Error::Redis("HINCRBYFLOAT did not return a number".into())
            })?;
        self.written(k, reply).await?;
        Ok(value)
    }

    async fn eval_value(&self, script: &Lua, args: Vec<Bytes>) -> Result<Option<V>> {
        let raw: Option<Bytes> = self
            .key
            .core
            .eval(script, vec![self.key.redis_key()], args)
            .await?;
        self.decode_value(raw)
    }

    /// Runs a script that answers `0`/`1`. With `once` it is sent at most once ([`Core::eval_once`](crate::client::core::Core::eval_once)): a script that changed the map and lost its reply would answer `0` when run again, and the writer would miss the change.
    async fn eval_flag(&self, script: &Lua, args: Vec<Bytes>, once: bool) -> Result<bool> {
        let core = &self.key.core;
        let keys = vec![self.key.redis_key()];
        let flag: i64 = if once {
            core.eval_once(script, keys, args).await?
        } else {
            core.eval(script, keys, args).await?
        };
        Ok(flag == 1)
    }

    /// Stores the entry without reading the old value (`HSET`); returns whether the key was new.
    #[doc(alias = "fastPut")]
    pub async fn set<Q, W>(&self, k: &Q, v: &W) -> Result<bool>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        let (k, v) = (self.codec.encode(k)?, self.codec.encode(v)?);
        let added: i64 = self
            .key
            .query({
                let mut cmd = redis::cmd("HSET");
                cmd.arg(self.key.redis_key());
                cmd.arg((&k[..], &v[..]));
                cmd
            })
            .await?;
        self.written(k, v).await?;
        Ok(added == 1)
    }

    /// Stores the entry only when the key is missing. Returns `None` when it stored it, or the present value, which it left as it is.
    #[doc(alias = "putIfAbsent", alias = "fastPutIfAbsent")]
    pub async fn try_insert<Q, W>(&self, k: &Q, v: &W) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        let (k, v) = (self.codec.encode(k)?, self.codec.encode(v)?);
        let raw: Option<Bytes> = self
            .key
            .core
            .eval_once(
                &PUT_IF_ABSENT,
                vec![self.key.redis_key()],
                vec![k.clone(), v.clone()],
            )
            .await?;
        if raw.is_none() {
            self.written(k, v).await?;
        }
        self.decode_value(raw)
    }

    /// Replaces the value of a present key and returns the value it replaced; a missing key stays missing and gives `None`.
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
        let (k, v) = (self.codec.encode(k)?, self.codec.encode(v)?);
        let previous = self
            .eval_value(&REPLACE, vec![k.clone(), v.clone()])
            .await?;
        if previous.is_some() {
            self.written(k, v).await?;
        }
        Ok(previous)
    }

    /// Replaces the value only when it currently equals `old`; returns whether it did. Redisson's `replace(key, old, new)`.
    pub async fn compare_and_set<Q, W>(&self, k: &Q, old: &W, new: &W) -> Result<bool>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        let (k, new) = (self.codec.encode(k)?, self.codec.encode(new)?);
        let replaced = self
            .eval_flag(
                &REPLACE_IF,
                vec![k.clone(), self.codec.encode(old)?, new.clone()],
                self.writer().is_some(),
            )
            .await?;
        if replaced {
            self.written(k, new).await?;
        }
        Ok(replaced)
    }

    /// Removes the entry only when its value equals `v`; returns whether it did. Redisson's `remove(key, value)`.
    pub async fn remove_if_eq<Q, W>(&self, k: &Q, v: &W) -> Result<bool>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        let k = self.codec.encode(k)?;
        let removed = self
            .eval_flag(
                &REMOVE_IF,
                vec![k.clone(), self.codec.encode(v)?],
                self.writer().is_some(),
            )
            .await?;
        if removed {
            self.deleted(k).await?;
        }
        Ok(removed)
    }

    /// Removes the keys and returns how many were present (`HDEL`). With a writer, the removed keys are deleted from the store in one call, and none removed calls nothing.
    #[doc(alias = "fastRemove")]
    pub async fn remove_many<Q>(&self, ks: &[&Q]) -> Result<usize>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if ks.is_empty() {
            return Ok(0);
        }
        let fields = ks
            .iter()
            .map(|k| self.codec.encode(*k))
            .collect::<Result<Vec<Bytes>>>()?;
        if let Some(store) = self.writer() {
            let flags: Vec<i64> = self
                .key
                .core
                // At most once: run again after a lost reply, it would report nothing removed and the writer would miss the deletes.
                .eval_once(
                    &FAST_REMOVE_BATCH,
                    vec![self.key.redis_key()],
                    fields.clone(),
                )
                .await?;
            let removed: Vec<Bytes> = fields
                .into_iter()
                .zip(flags)
                .filter_map(|(k, flag)| (flag == 1).then_some(k))
                .collect();
            store.deleted(&removed).await?;
            return Ok(removed.len());
        }
        let removed: usize = self
            .key
            .query({
                let mut cmd = redis::cmd("HDEL");
                cmd.arg(self.key.redis_key());
                cmd.arg(crate::client::request::binary(&fields));
                cmd
            })
            .await?;
        Ok(removed)
    }

    /// Hands every write-behind change still queued to the writer now and returns the first writer error. A batch the writer rejected after its retries is dropped, as Redisson drops it. Without write-behind it does nothing.
    ///
    /// It plays the part Redisson's `destroy()` plays for pending writes; dropping the map's last handle drains the queue once more in the background.
    pub async fn flush(&self) -> Result<()> {
        match &self.store {
            Some(store) => store.drain().await,
            None => Ok(()),
        }
    }

    /// Returns whether any entry has a value equal to `v`. It reads every value on the server.
    pub async fn contains_value<W>(&self, v: &W) -> Result<bool>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        self.eval_flag(&CONTAINS_VALUE, vec![self.codec.encode(v)?], false)
            .await
    }

    /// Length in bytes of the encoded value stored under the key; 0 when missing (`HSTRLEN`).
    #[doc(alias = "valueSize")]
    pub async fn value_len<Q>(&self, k: &Q) -> Result<usize>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let size: usize = self
            .key
            .read({
                let mut cmd = redis::cmd("HSTRLEN");
                cmd.arg(self.key.redis_key());
                cmd.arg(&self.codec.encode(k)?[..]);
                cmd
            })
            .await?;
        Ok(size)
    }

    /// Returns up to `count` distinct random keys (`HRANDFIELD`).
    pub async fn random_keys(&self, count: usize) -> Result<Vec<K>> {
        let count = i64::try_from(count).unwrap_or(i64::MAX);
        let mut cmd = self.key.key_cmd("HRANDFIELD");
        cmd.arg(count);
        let raw: Vec<Bytes> = self.key.read(cmd).await?;
        raw.iter().map(|bytes| self.codec.decode(bytes)).collect()
    }

    /// Streams all entries, reading the hash in pages of 100 with `HSCAN`.
    pub fn iter(&self) -> impl Stream<Item = Result<(K, V)>> + '_ {
        self.iter_with(MapScan::new())
    }

    /// Streams all keys.
    pub fn keys(&self) -> impl Stream<Item = Result<K>> + '_ {
        self.keys_with(MapScan::new())
    }

    /// Streams all values.
    pub fn values(&self) -> impl Stream<Item = Result<V>> + '_ {
        self.values_with(MapScan::new())
    }

    /// Streams the entries `scan` selects, Redisson's `entrySet(pattern, count)`.
    #[doc(alias = "entrySet")]
    pub fn iter_with(&self, scan: MapScan) -> impl Stream<Item = Result<(K, V)>> + '_ {
        hscan(&self.key, &self.codec, scan)
    }

    /// Streams the keys of the entries `scan` selects, Redisson's `keySet(pattern, count)`.
    #[doc(alias = "keySet")]
    pub fn keys_with(&self, scan: MapScan) -> impl Stream<Item = Result<K>> + '_ {
        self.iter_with(scan).map(|entry| entry.map(|(k, _)| k))
    }

    /// Streams the values of the entries `scan` selects, Redisson's `values(pattern, count)`.
    pub fn values_with(&self, scan: MapScan) -> impl Stream<Item = Result<V>> + '_ {
        self.iter_with(scan).map(|entry| entry.map(|(_, v)| v))
    }
}

impl<K, V, C> HashMap<K, V, C>
where
    K: Serialize + DeserializeOwned + Eq + Hash + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    /// The whole map in one `HGETALL`, a snapshot of one moment. [`iter`](HashMap::iter) reads it in pages instead.
    #[doc(
        alias = "readAllMap",
        alias = "readAllEntrySet",
        alias = "readAllKeySet",
        alias = "readAllValues"
    )]
    pub async fn to_hash_map(&self) -> Result<std::collections::HashMap<K, V>> {
        let cmd = self.key.key_cmd("HGETALL");
        let raw: redis::Value = self.key.read(cmd).await?;
        Ok(decode_pairs(&self.codec, raw)?.into_iter().collect())
    }

    /// Up to `count` distinct random entries (`HRANDFIELD … WITHVALUES`), Redisson's `randomEntries`.
    #[doc(alias = "randomEntries")]
    pub async fn random_entries(&self, count: usize) -> Result<std::collections::HashMap<K, V>> {
        let count = i64::try_from(count).unwrap_or(i64::MAX);
        let mut cmd = self.key.key_cmd("HRANDFIELD");
        cmd.arg(count).arg("WITHVALUES");
        let raw: redis::Value = self.key.read(cmd).await?;
        Ok(decode_pairs(&self.codec, raw)?.into_iter().collect())
    }
}

impl<K, V, C> HashMap<K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    pub(crate) fn redis_key(&self) -> &Key {
        &self.key
    }

    /// Reads an encoded key from the master: an entry writes what it read, and a lagging replica would lose updates.
    pub(crate) async fn read_encoded(&self, k: &Bytes) -> Result<Option<V>> {
        let mut cmd = self.key.key_cmd("HGET");
        cmd.arg(&k[..]);
        let raw: Option<Bytes> = self.key.query(cmd).await?;
        self.decode_value(raw)
    }

    pub(crate) async fn write_encoded(&self, k: &Bytes, v: &V) -> Result<()> {
        let mut cmd = self.key.key_cmd("HSET");
        cmd.arg((&k[..], &self.codec.encode(v)?[..]));
        let _: i64 = self.key.query(cmd).await?;
        Ok(())
    }

    pub(crate) async fn delete_encoded(&self, k: &Bytes) -> Result<()> {
        let mut cmd = self.key.key_cmd("HDEL");
        cmd.arg(&k[..]);
        let _: i64 = self.key.query(cmd).await?;
        Ok(())
    }
}

/// The insert of [`HashMap::extend`]; await it to run.
#[must_use = "an extend does nothing until it is awaited"]
pub struct Extend<'m, K, V, C: Codec> {
    map: &'m HashMap<K, V, C>,
    encoded: Result<Vec<(Bytes, Bytes)>>,
    batch_size: usize,
}

impl<K, V, C: Codec> fmt::Debug for Extend<'_, K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Extend")
            .field("batch_size", &self.batch_size)
            .finish_non_exhaustive()
    }
}

impl<K, V, C: Codec> Extend<'_, K, V, C> {
    /// Sends one `HSET` per `batch_size` entries, in order, Redisson's `putAll(map, batchSize)`. Zero, the default, sends everything at once.
    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

impl<'m, K, V, C> IntoFuture for Extend<'m, K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync + 'm,
    V: Serialize + DeserializeOwned + Send + Sync + 'm,
    C: Codec,
{
    type Output = Result<()>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'm>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let encoded = self.encoded?;
            let size = if self.batch_size == 0 {
                encoded.len().max(1)
            } else {
                self.batch_size
            };
            for batch in encoded.chunks(size) {
                let mut cmd = self.map.key.key_cmd("HSET");
                cmd.arg(crate::client::request::binary_pairs(batch));
                self.map.key.query::<()>(cmd).await?;
                if let Some(store) = self.map.writer() {
                    store.written(batch).await?;
                }
            }
            Ok(())
        })
    }
}
