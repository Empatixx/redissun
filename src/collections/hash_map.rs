use crate::codec::Codec;
use crate::collections::map::events::{self, MapEventKind, MapEvents};
use crate::collections::map::key_hash::map_key_name;
use crate::collections::map::ops::{self, AtomicMap};
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
use futures::{Stream, StreamExt, TryStreamExt};
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

static PUT_IF_EXISTS: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local value = redis.call('HGET', KEYS[1], ARGV[1])
        if value ~= false then
            redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
            return value
        end
        return nil",
    )
});

static FAST_PUT_IF_EXISTS: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local value = redis.call('HGET', KEYS[1], ARGV[1])
        if value ~= false then
            redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
            return 1
        end
        return 0",
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

static FAST_REPLACE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then
            redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
            return 1
        end
        return 0",
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

    /// Redisson's `compute`: under the key's [`lock`](HashMap::lock), the closure gets the current value; `Some` stores what it returns, `None` removes the entry. Returns the new value.
    pub async fn compute<Q, F, Fut>(&self, k: &Q, f: F) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
        F: FnOnce(Option<V>) -> Fut + Send,
        Fut: Future<Output = Option<V>> + Send,
    {
        ops::compute(self, self.codec.encode(k)?, f).await
    }

    /// Redisson's `computeIfAbsent`: returns the present value without calling the closure; for a missing key it stores and returns what the closure gives, under the key's lock. `None` stores nothing.
    pub async fn compute_if_absent<Q, F, Fut>(&self, k: &Q, f: F) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Option<V>> + Send,
    {
        ops::compute_if_absent(self, self.codec.encode(k)?, f).await
    }

    /// Redisson's `computeIfPresent`: under the key's lock, the closure gets the present value; `Some` replaces it, `None` removes it. A missing key returns `None` without calling the closure.
    pub async fn compute_if_present<Q, F, Fut>(&self, k: &Q, f: F) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
        F: FnOnce(V) -> Fut + Send,
        Fut: Future<Output = Option<V>> + Send,
    {
        ops::compute_if_present(self, self.codec.encode(k)?, f).await
    }

    /// Redisson's `merge`: under the key's lock, stores `value` for a missing key, or what the closure makes of the old value and `value`; `None` removes the entry. Returns the new value.
    pub async fn merge<Q, F, Fut>(&self, k: &Q, value: V, f: F) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
        F: FnOnce(V, V) -> Fut + Send,
        Fut: Future<Output = Option<V>> + Send,
    {
        ops::merge(self, self.codec.encode(k)?, value, f).await
    }

    /// Starts listening to every kind of change of the map, Redisson's `addListener` with the map and object listeners. See [`MapEvents`] for the server setting it needs.
    #[doc(alias = "addListener")]
    pub async fn events(&self) -> Result<MapEvents<K, C>> {
        self.events_of(&MapEventKind::ALL).await
    }

    /// Starts listening to the given kinds of change.
    pub async fn events_of(&self, kinds: &[MapEventKind]) -> Result<MapEvents<K, C>> {
        events::subscribe(&self.key, self.codec.clone(), kinds).await
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

    /// Inserts the entry only when the key is missing (`HSETNX`); returns whether it was inserted. Like Redisson `fastPutIfAbsent`.
    pub async fn insert_nx<Q, W>(&self, k: &Q, v: &W) -> Result<bool>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        let inserted: bool = self
            .key
            .query_once({
                let mut cmd = redis::cmd("HSETNX");
                cmd.arg(self.key.redis_key());
                cmd.arg(&self.codec.encode(k)?[..]);
                cmd.arg(&self.codec.encode(v)?[..]);
                cmd
            })
            .await?;
        Ok(inserted)
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

    /// Inserts an entry and returns whether the key was new (`HSET`). Like Redisson `fastPut`.
    pub async fn fast_insert<Q, W>(&self, k: &Q, v: &W) -> Result<bool>
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

    /// Inserts the entry only when the key is missing and returns the current value otherwise. Like Redisson `putIfAbsent`.
    pub async fn insert_if_absent<Q, W>(&self, k: &Q, v: &W) -> Result<Option<V>>
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

    /// Replaces the value only when the key is present and returns the value it replaced. Like Redisson `putIfExists`.
    pub async fn insert_if_exists<Q, W>(&self, k: &Q, v: &W) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        self.eval_value(
            &PUT_IF_EXISTS,
            vec![self.codec.encode(k)?, self.codec.encode(v)?],
        )
        .await
    }

    /// Replaces the value only when the key is present; returns whether it did. Like Redisson `fastPutIfExists`.
    pub async fn fast_insert_if_exists<Q, W>(&self, k: &Q, v: &W) -> Result<bool>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        self.eval_flag(
            &FAST_PUT_IF_EXISTS,
            vec![self.codec.encode(k)?, self.codec.encode(v)?],
        )
        .await
    }

    /// Replaces the value of a present key and returns the value it replaced. Like Redisson `replace(key, value)`.
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

    /// Replaces the value of a present key; returns whether it did. Like Redisson `fastReplace`.
    pub async fn fast_replace<Q, W>(&self, k: &Q, v: &W) -> Result<bool>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        self.eval_flag(
            &FAST_REPLACE,
            vec![self.codec.encode(k)?, self.codec.encode(v)?],
        )
        .await
    }

    /// Replaces the value only when it currently equals `old`; returns whether it did. Like Redisson `replace(key, old, new)`.
    pub async fn replace_if<Q, W>(&self, k: &Q, old: &W, new: &W) -> Result<bool>
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

    /// Removes the entry only when its value equals `v`; returns whether it did. Like Redisson `remove(key, value)`.
    pub async fn remove_if<Q, W>(&self, k: &Q, v: &W) -> Result<bool>
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

    /// Removes the keys and returns how many were present (`HDEL`). Like Redisson `fastRemove`.
    pub async fn fast_remove<Q>(&self, ks: &[&Q]) -> Result<usize>
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
    pub async fn value_size<Q>(&self, k: &Q) -> Result<usize>
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

    /// Returns every key in one call (`HKEYS`).
    pub async fn read_all_keys(&self) -> Result<Vec<K>> {
        let raw: Vec<Bytes> = self
            .key
            .read({
                let mut cmd = redis::cmd("HKEYS");
                cmd.arg(self.key.redis_key());
                cmd
            })
            .await?;
        raw.iter().map(|bytes| self.codec.decode(bytes)).collect()
    }

    /// Returns every value in one call (`HVALS`).
    pub async fn read_all_values(&self) -> Result<Vec<V>> {
        let raw: Vec<Bytes> = self
            .key
            .read({
                let mut cmd = redis::cmd("HVALS");
                cmd.arg(self.key.redis_key());
                cmd
            })
            .await?;
        raw.iter().map(|bytes| self.codec.decode(bytes)).collect()
    }

    /// Returns every entry in one call (`HGETALL`).
    pub async fn read_all_entries(&self) -> Result<Vec<(K, V)>> {
        let mut cmd = redis::cmd("HGETALL");
        cmd.arg(self.key.redis_key());
        let raw: redis::Value = self.key.read(cmd).await?;
        self.decode_pairs(raw)
    }

    fn decode_pairs(&self, raw: redis::Value) -> Result<Vec<(K, V)>> {
        decode_pairs(&self.codec, raw)
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

    /// Every entry whose encoded key matches `pattern`, Redisson's `readAllEntrySet(pattern)`. See [`MapScan`] for how patterns match.
    ///
    /// Unlike Redisson, which reads only the first `HSCAN` page, this scans the whole hash.
    pub async fn read_all_entries_matching(&self, pattern: &str) -> Result<Vec<(K, V)>> {
        self.iter_with(MapScan::new().pattern(pattern))
            .try_collect()
            .await
    }

    /// Every key that matches `pattern`, Redisson's `readAllKeySet(pattern)`, scanning the whole hash.
    pub async fn read_all_keys_matching(&self, pattern: &str) -> Result<Vec<K>> {
        self.keys_with(MapScan::new().pattern(pattern))
            .try_collect()
            .await
    }

    /// The values of the keys that match `pattern`, Redisson's `readAllValues(pattern)`, scanning the whole hash.
    pub async fn read_all_values_matching(&self, pattern: &str) -> Result<Vec<V>> {
        self.values_with(MapScan::new().pattern(pattern))
            .try_collect()
            .await
    }
}

impl<K, V, C> HashMap<K, V, C>
where
    K: Serialize + DeserializeOwned + Eq + Hash + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    /// The whole map in one call (`HGETALL`), Redisson's `readAllMap`.
    pub async fn read_all_map(&self) -> Result<std::collections::HashMap<K, V>> {
        Ok(self.read_all_entries().await?.into_iter().collect())
    }

    /// The entries of the given keys that exist, in one `HMGET`, Redisson's `getAll`.
    #[doc(alias = "getAll")]
    pub async fn get_all<Q>(&self, ks: &[&Q]) -> Result<std::collections::HashMap<K, V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if ks.is_empty() {
            return Ok(std::collections::HashMap::new());
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
        fields
            .iter()
            .zip(raw)
            .filter_map(|(field, value)| value.map(|value| (field, value)))
            .map(|(field, value)| Ok((self.codec.decode(field)?, self.codec.decode(&value)?)))
            .collect()
    }

    /// Up to `count` distinct random entries (`HRANDFIELD … WITHVALUES`), Redisson's `randomEntries`.
    pub async fn random_entries(&self, count: usize) -> Result<std::collections::HashMap<K, V>> {
        let count = i64::try_from(count).unwrap_or(i64::MAX);
        let mut cmd = self.key.key_cmd("HRANDFIELD");
        cmd.arg(count).arg("WITHVALUES");
        let raw: redis::Value = self.key.read(cmd).await?;
        Ok(decode_pairs(&self.codec, raw)?.into_iter().collect())
    }
}

impl<K, V, C> AtomicMap<V> for HashMap<K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    fn map_key(&self) -> &Key {
        &self.key
    }

    async fn get_encoded(&self, k: &Bytes) -> Result<Option<V>> {
        let mut cmd = self.key.key_cmd("HGET");
        cmd.arg(&k[..]);
        let raw: Option<Bytes> = self.key.read(cmd).await?;
        self.decode_value(raw)
    }

    async fn fast_insert_encoded(&self, k: &Bytes, v: &V) -> Result<()> {
        let mut cmd = self.key.key_cmd("HSET");
        cmd.arg((&k[..], &self.codec.encode(v)?[..]));
        let _: i64 = self.key.query(cmd).await?;
        Ok(())
    }

    async fn insert_if_absent_encoded(&self, k: &Bytes, v: &V) -> Result<Option<V>> {
        let raw: Option<Bytes> = self
            .key
            .core
            .eval_once(
                &PUT_IF_ABSENT,
                vec![self.key.redis_key()],
                vec![k.clone(), self.codec.encode(v)?],
            )
            .await?;
        self.decode_value(raw)
    }

    async fn fast_insert_if_exists_encoded(&self, k: &Bytes, v: &V) -> Result<bool> {
        self.eval_flag(&FAST_PUT_IF_EXISTS, vec![k.clone(), self.codec.encode(v)?])
            .await
    }

    fn encode_value(&self, v: &V) -> Result<Bytes> {
        self.codec.encode(v)
    }

    async fn remove_if_encoded(&self, k: &Bytes, v: &Bytes) -> Result<bool> {
        self.eval_flag(&REMOVE_IF, vec![k.clone(), v.clone()]).await
    }

    async fn fast_remove_encoded(&self, k: &Bytes) -> Result<()> {
        let mut cmd = self.key.key_cmd("HDEL");
        cmd.arg(&k[..]);
        let _: i64 = self.key.query(cmd).await?;
        Ok(())
    }
}

/// The insert of [`HashMap::extend`]; await it to run. Exported as `HashMapExtend`.
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
