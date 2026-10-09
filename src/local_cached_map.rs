use crate::codec::Codec;
use crate::core::Core;
use crate::error::Result;
use crate::hash_map::HashMap;
use crate::object::{tagged, HasKey, Key};
use crate::pubsub::Subscription;
use bytes::Bytes;
use fred::interfaces::{HashesInterface, KeysInterface, PubsubInterface, SortedSetsInterface};
use fred::types::scripts::Script;
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

static PUT: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
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

static REMOVE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
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

static CLEAR_ALL: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
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
    reads: StdHashMap<Bytes, (usize, bool)>,
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
            reads: StdHashMap::new(),
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

    fn invalidate(&mut self, key: &Bytes) {
        self.remove(key);
        if let Some(read) = self.reads.get_mut(key) {
            read.1 = true;
        }
    }

    fn clear(&mut self) {
        self.slots.clear();
        self.order.clear();
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
            self.put(key, value);
        }
    }

    fn entries(&mut self) -> std::vec::Vec<(Bytes, Bytes)> {
        self.remove_expired();
        self.slots
            .iter()
            .filter_map(|(key, slot)| Some((key.clone(), slot.value.clone()?)))
            .collect()
    }
}

struct Local {
    store: Mutex<Store>,
    id: [u8; ID_LEN],
    reconnection: ReconnectionStrategy,
    last_invalidate: AtomicI64,
    listener: Mutex<Option<JoinHandle<()>>>,
}

impl Local {
    fn store(&self) -> std::sync::MutexGuard<'_, Store> {
        self.store.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn lookup(&self, key: &Bytes) -> Option<Option<Bytes>> {
        self.store().get(key)
    }

    fn write(&self, key: Bytes, value: Option<Bytes>) {
        let mut store = self.store();
        store.invalidate(&key);
        store.put(key, value);
    }

    fn begin_read(&self, key: &Bytes) {
        self.store().begin_read(key);
    }

    fn finish_read(&self, key: Bytes, value: Option<Option<Bytes>>) {
        self.store().finish_read(key, value);
    }

    fn forget(&self, key: &Bytes) {
        self.store().invalidate(key);
    }

    fn forget_all(&self) {
        self.store().clear();
    }

    fn len(&self) -> usize {
        self.store().slots.len()
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

/// Sets up a [`LocalCachedMap`]. Get one from [`Client::local_cached_map`](crate::Client::local_cached_map). The defaults are Redisson's: no size limit, no local expiry, [`SyncStrategy::Invalidate`], [`ReconnectionStrategy::None`], [`EvictionPolicy::None`] and no cached misses.
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

    /// Also caches that a key is missing, so the next read of it costs no round trip. Off by default.
    pub fn store_cache_miss(mut self, store: bool) -> Self {
        self.store_cache_miss = store;
        self
    }

    /// Subscribes to the changes of the other instances and returns the map.
    pub async fn build(self) -> Result<LocalCachedMap<K, V, C>> {
        let local = Arc::new(Local {
            store: Mutex::new(Store::new(
                self.eviction,
                self.cache_size,
                self.ttl,
                self.max_idle,
            )),
            id: *Uuid::new_v4().as_bytes(),
            reconnection: self.reconnection,
            last_invalidate: AtomicI64::new(0),
            listener: Mutex::new(None),
        });
        if self.sync != SyncStrategy::None || self.reconnection != ReconnectionStrategy::None {
            let pubsub = self.key.core.pubsub.clone();
            let (subscription, messages) =
                pubsub.subscribe_with_messages(&channel(&self.key)).await?;
            let reconnects = pubsub.reconnects();
            let handle = tokio::spawn(listen(
                Arc::downgrade(&local),
                Arc::downgrade(&self.key.core),
                vec![self.key.redis_key(), updates_log(&self.key)],
                subscription,
                messages,
                reconnects,
            ));
            *local.listener.lock().unwrap_or_else(|e| e.into_inner()) = Some(handle);
        }
        Ok(LocalCachedMap {
            map: HashMap::new(self.key.clone(), self.codec.clone()),
            key: self.key,
            codec: self.codec,
            local,
            sync: self.sync,
            reconnection: self.reconnection,
            store_cache_miss: self.store_cache_miss,
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
    let exists: Result<i64> = core
        .redis()
        .exists(keys[0].clone())
        .await
        .map_err(Into::into);
    match exists {
        Ok(0) => local.forget_all(),
        Ok(_) => {
            let changed: Result<std::vec::Vec<Bytes>> = core
                .redis()
                .zrangebyscore(keys[1].clone(), since as f64, "+inf", false, None)
                .await
                .map_err(Into::into);
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

async fn listen(
    local: Weak<Local>,
    core: Weak<Core>,
    keys: std::vec::Vec<String>,
    _subscription: Subscription,
    mut messages: broadcast::Receiver<Bytes>,
    mut reconnects: broadcast::Receiver<()>,
) {
    loop {
        tokio::select! {
            received = messages.recv() => {
                let Some(local) = local.upgrade() else { return };
                match received {
                    Ok(payload) => local.apply(&payload),
                    Err(RecvError::Lagged(_)) => local.forget_all(),
                    Err(RecvError::Closed) => return,
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
/// Only changes made through a `LocalCachedMap` are announced. Changes made through a plain [`HashMap`] or [`Object::del`](crate::Object::del) of the same name are not, so other instances see them only when their entry expires or is pushed out. Use [`LocalCachedMap::clear`] to empty the map for everybody.
///
/// The data in Redis is a normal hash, so a [`HashMap`] with the same name and codec reads the same entries.
pub struct LocalCachedMap<K, V, C: Codec> {
    map: HashMap<K, V, C>,
    key: Key,
    codec: C,
    local: Arc<Local>,
    sync: SyncStrategy,
    reconnection: ReconnectionStrategy,
    store_cache_miss: bool,
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
}

impl<K, V, C> LocalCachedMap<K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    fn mode(&self) -> Bytes {
        Bytes::from(if self.reconnection == ReconnectionStrategy::Load {
            "2"
        } else if self.sync != SyncStrategy::None {
            "1"
        } else {
            "0"
        })
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

    fn redis_keys(&self) -> std::vec::Vec<String> {
        vec![self.key.redis_key(), updates_log(&self.key)]
    }

    /// Returns the value for a key, from the local cache when it is there. A value read from Redis is cached. The key is borrowed: `map.get("k")`.
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
        self.local.begin_read(&field);
        let read: Result<Option<Bytes>> = self
            .key
            .core
            .redis()
            .hget(self.key.redis_key(), field.clone())
            .await
            .map_err(Into::into);
        let raw = match read {
            Ok(raw) => raw,
            Err(error) => {
                self.local.finish_read(field, None);
                return Err(error);
            }
        };
        let cached = (raw.is_some() || self.store_cache_miss).then(|| raw.clone());
        self.local.finish_read(field, cached);
        raw.map(|raw| self.codec.decode(&raw)).transpose()
    }

    /// Writes a value and returns the previous one. The local cache of this instance holds the new value, and the other instances are told. Both arguments are borrowed.
    pub async fn insert<Q, W>(&self, k: &Q, v: &W) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        let field = self.codec.encode(k)?;
        let value = self.codec.encode(v)?;
        let notice = match self.sync {
            SyncStrategy::Update => message(
                &self.local.id,
                UPDATE,
                &[&(field.len() as u32).to_be_bytes(), &field, &value],
            ),
            _ => message(&self.local.id, INVALIDATE, &[&field]),
        };
        self.local.write(field.clone(), Some(value.clone()));
        let [now, entry, keep] = self.log_arguments(&field);
        let written: Result<Option<Bytes>> = self
            .key
            .core
            .eval(
                &PUT,
                self.redis_keys(),
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
        let previous = written.inspect_err(|_| self.local.forget(&field))?;
        previous.map(|bytes| self.codec.decode(&bytes)).transpose()
    }

    /// Removes a key and returns its value. The other instances are told when the key existed.
    pub async fn remove<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let field = self.codec.encode(k)?;
        let notice = message(&self.local.id, INVALIDATE, &[&field]);
        self.local.forget(&field);
        let [now, entry, keep] = self.log_arguments(&field);
        let previous: Option<Bytes> = self
            .key
            .core
            .eval(
                &REMOVE,
                self.redis_keys(),
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
            .await?;
        previous.map(|bytes| self.codec.decode(&bytes)).transpose()
    }

    /// Returns whether the key exists. A cached key answers without a round trip.
    pub async fn contains_key<Q>(&self, k: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let field = self.codec.encode(k)?;
        if let Some(cached) = self.local.lookup(&field) {
            return Ok(cached.is_some());
        }
        self.local.begin_read(&field);
        let found = self.map.contains_key(k).await;
        let miss = matches!(found, Ok(false)) && self.store_cache_miss;
        self.local.finish_read(field, miss.then_some(None));
        found
    }

    /// Number of entries in Redis.
    pub async fn len(&self) -> Result<usize> {
        self.map.len().await
    }

    /// Returns whether Redis holds no entry.
    pub async fn is_empty(&self) -> Result<bool> {
        self.map.is_empty().await
    }

    /// Removes every entry for everybody; returns whether there was anything to remove. The other instances empty their local cache only when something was removed.
    pub async fn clear(&self) -> Result<bool> {
        self.local.forget_all();
        let notice = message(&self.local.id, CLEAR, &[]);
        let removed: i64 = self
            .key
            .core
            .eval(
                &CLEAR_ALL,
                self.redis_keys(),
                vec![Bytes::from(channel(&self.key)), self.mode(), notice],
            )
            .await?;
        Ok(removed == 1)
    }

    /// Number of entries in the local cache of this instance, including cached misses.
    pub fn local_len(&self) -> usize {
        self.local.len()
    }

    /// Returns the entries in the local cache of this instance.
    pub fn cached_entries(&self) -> Result<std::vec::Vec<(K, V)>> {
        let entries = self.local.store().entries();
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
        let _: i64 = self
            .key
            .core
            .redis()
            .publish(channel(&self.key), message(&self.local.id, CLEAR, &[]))
            .await?;
        Ok(())
    }

    /// Streams every entry from Redis. It does not use or fill the local cache.
    pub fn iter(&self) -> impl Stream<Item = Result<(K, V)>> + '_ {
        self.map.iter()
    }

    /// Streams every key from Redis.
    pub fn keys(&self) -> impl Stream<Item = Result<K>> + '_ {
        self.map.keys()
    }

    /// Streams every value from Redis.
    pub fn values(&self) -> impl Stream<Item = Result<V>> + '_ {
        self.map.values()
    }
}
