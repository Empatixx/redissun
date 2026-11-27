use crate::codec::Codec;
pub use crate::collections::map::entry::{Entry, OccupiedEntry, VacantEntry};
use crate::collections::map::events::{self, MapEventKind, MapEvents};
use crate::collections::map::key_hash::map_key_name;
use crate::collections::map::scan::{decode_pairs, hscan, MapScan};
use crate::coordination::latch::CountDownLatch;
use crate::coordination::permit_semaphore::PermitExpirableSemaphore;
use crate::coordination::semaphore::Semaphore;
use crate::error::Result;
use crate::locks::fair_lock::FairLock;
use crate::locks::rw_lock::RwLock;
use crate::locks::Lock;
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
use std::sync::LazyLock;

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
pub struct HashMap<K, V, C: Codec> {
    key: Key,
    codec: C,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V, C: Codec> Clone for HashMap<K, V, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
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
        Self {
            key,
            codec,
            _marker: PhantomData,
        }
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
        Entry::acquire(self, self.codec.encode(k)?).await
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

    /// The key of the object Redisson keeps for map key `k` under `suffix`.
    fn per_key<Q>(&self, k: &Q, suffix: &str) -> Result<Key>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let encoded = self.codec.encode(k)?;
        Ok(Key::new(
            self.key.core.clone(),
            map_key_name(self.key.name(), &encoded, suffix),
        ))
    }

    /// The [`Lock`] of one key of the map, Redisson's `getLock(key)`. A Redisson client locking the same key of the same map takes the same lock.
    #[doc(alias = "getLock")]
    pub fn lock<Q>(&self, k: &Q) -> Result<Lock>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        Ok(Lock::new(self.per_key(k, "lock")?))
    }

    /// The [`FairLock`] of one key of the map, Redisson's `getFairLock(key)`.
    #[doc(alias = "getFairLock")]
    pub fn fair_lock<Q>(&self, k: &Q) -> Result<FairLock>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        Ok(FairLock::new(self.per_key(k, "fairlock")?))
    }

    /// The [`RwLock`] of one key of the map, Redisson's `getReadWriteLock(key)`.
    #[doc(alias = "getReadWriteLock")]
    pub fn rw_lock<Q>(&self, k: &Q) -> Result<RwLock>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        Ok(RwLock::new(self.per_key(k, "rw_lock")?))
    }

    /// The [`Semaphore`] of one key of the map, Redisson's `getSemaphore(key)`.
    #[doc(alias = "getSemaphore")]
    pub fn semaphore<Q>(&self, k: &Q) -> Result<Semaphore>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        Ok(Semaphore::new(self.per_key(k, "semaphore")?))
    }

    /// The [`CountDownLatch`] of one key of the map, Redisson's `getCountDownLatch(key)`.
    #[doc(alias = "getCountDownLatch")]
    pub fn count_down_latch<Q>(&self, k: &Q) -> Result<CountDownLatch>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        Ok(CountDownLatch::new(self.per_key(k, "countdownlatch")?))
    }

    /// The [`PermitExpirableSemaphore`] of one key of the map, Redisson's `getPermitExpirableSemaphore(key)`.
    #[doc(alias = "getPermitExpirableSemaphore")]
    pub fn permit_expirable_semaphore<Q>(&self, k: &Q) -> Result<PermitExpirableSemaphore>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        Ok(PermitExpirableSemaphore::new(
            self.per_key(k, "permitexpirablesemaphore")?,
        ))
    }

    /// Inserts an entry and returns the value it replaced. The key and value are borrowed: `map.insert("a", &value)`.
    pub async fn insert<Q, W>(&self, k: &Q, v: &W) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        let previous: Option<Bytes> = self
            .key
            .core
            .eval(
                &INSERT,
                vec![self.key.redis_key()],
                vec![self.codec.encode(k)?, self.codec.encode(v)?],
            )
            .await?;
        self.decode_value(previous)
    }

    /// Returns the value for the key, or `None` when it is missing.
    pub async fn get<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let raw: Option<Bytes> = self
            .key
            .read({
                let mut cmd = redis::cmd("HGET");
                cmd.arg(self.key.redis_key());
                cmd.arg(&self.codec.encode(k)?[..]);
                cmd
            })
            .await?;
        self.decode_value(raw)
    }

    /// Removes the entry and returns its value.
    pub async fn remove<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let previous: Option<Bytes> = self
            .key
            .core
            .eval(
                &REMOVE,
                vec![self.key.redis_key()],
                vec![self.codec.encode(k)?],
            )
            .await?;
        self.decode_value(previous)
    }

    /// Returns whether the key is present.
    pub async fn contains_key<Q>(&self, k: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let found: bool = self
            .key
            .read({
                let mut cmd = redis::cmd("HEXISTS");
                cmd.arg(self.key.redis_key());
                cmd.arg(&self.codec.encode(k)?[..]);
                cmd
            })
            .await?;
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

    /// Returns the values for several keys, in the order of the keys.
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
        raw.into_iter().map(|r| self.decode_value(r)).collect()
    }

    /// Adds `delta` to the integer stored under the key, creating it at zero, and returns the result.
    pub async fn incr_by<Q>(&self, k: &Q, delta: i64) -> Result<i64>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let value: i64 = self
            .key
            .query_once({
                let mut cmd = redis::cmd("HINCRBY");
                cmd.arg(self.key.redis_key());
                cmd.arg(&self.codec.encode(k)?[..]);
                cmd.arg(delta);
                cmd
            })
            .await?;
        Ok(value)
    }

    /// Adds `delta` to the float stored under the key, creating it at zero, and returns the result.
    pub async fn incr_by_float<Q>(&self, k: &Q, delta: f64) -> Result<f64>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let value: f64 = self
            .key
            .query_once({
                let mut cmd = redis::cmd("HINCRBYFLOAT");
                cmd.arg(self.key.redis_key());
                cmd.arg(&self.codec.encode(k)?[..]);
                cmd.arg(delta);
                cmd
            })
            .await?;
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

    async fn eval_flag(&self, script: &Lua, args: Vec<Bytes>) -> Result<bool> {
        let flag: i64 = self
            .key
            .core
            .eval(script, vec![self.key.redis_key()], args)
            .await?;
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
        let added: i64 = self
            .key
            .query({
                let mut cmd = redis::cmd("HSET");
                cmd.arg(self.key.redis_key());
                cmd.arg((&self.codec.encode(k)?[..], &self.codec.encode(v)?[..]));
                cmd
            })
            .await?;
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
        let raw: Option<Bytes> = self
            .key
            .core
            .eval_once(
                &PUT_IF_ABSENT,
                vec![self.key.redis_key()],
                vec![self.codec.encode(k)?, self.codec.encode(v)?],
            )
            .await?;
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
        self.eval_value(&REPLACE, vec![self.codec.encode(k)?, self.codec.encode(v)?])
            .await
    }

    /// Replaces the value only when it currently equals `old`; returns whether it did. Redisson's `replace(key, old, new)`.
    pub async fn compare_and_set<Q, W>(&self, k: &Q, old: &W, new: &W) -> Result<bool>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        self.eval_flag(
            &REPLACE_IF,
            vec![
                self.codec.encode(k)?,
                self.codec.encode(old)?,
                self.codec.encode(new)?,
            ],
        )
        .await
    }

    /// Removes the entry only when its value equals `v`; returns whether it did. Redisson's `remove(key, value)`.
    pub async fn remove_if_eq<Q, W>(&self, k: &Q, v: &W) -> Result<bool>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        self.eval_flag(
            &REMOVE_IF,
            vec![self.codec.encode(k)?, self.codec.encode(v)?],
        )
        .await
    }

    /// Removes the keys and returns how many were present (`HDEL`).
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

    /// Returns whether any entry has a value equal to `v`. It reads every value on the server.
    pub async fn contains_value<W>(&self, v: &W) -> Result<bool>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        self.eval_flag(&CONTAINS_VALUE, vec![self.codec.encode(v)?])
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
    K: Send + Sync + 'm,
    V: Send + Sync + 'm,
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
            }
            Ok(())
        })
    }
}
