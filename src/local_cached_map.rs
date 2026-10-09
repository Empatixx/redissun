use crate::codec::Codec;
use crate::error::Result;
use crate::hash_map::HashMap;
use crate::object::{HasKey, Key};
use bytes::Bytes;
use fred::interfaces::HashesInterface;
use fred::types::scripts::Script;
use futures::Stream;
use lru::LruCache;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::Duration;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use uuid::Uuid;

const ID_LEN: usize = 16;
const INVALIDATE: u8 = b'I';
const UPDATE: u8 = b'U';
const CLEAR: u8 = b'C';

static PUT: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local previous = redis.call('HGET', KEYS[1], ARGV[1])
        redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
        if ARGV[4] == '1' then
            redis.call('PUBLISH', ARGV[3], ARGV[5])
        end
        return previous",
    )
});

static REMOVE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local previous = redis.call('HGET', KEYS[1], ARGV[1])
        redis.call('HDEL', KEYS[1], ARGV[1])
        if ARGV[3] == '1' then
            redis.call('PUBLISH', ARGV[2], ARGV[4])
        end
        return previous",
    )
});

static CLEAR_ALL: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "redis.call('DEL', KEYS[1])
        if ARGV[2] == '1' then
            redis.call('PUBLISH', ARGV[1], ARGV[3])
        end
        return 1",
    )
});

/// How a [`LocalCachedMap`] tells the other instances about a change.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SyncStrategy {
    /// Other instances drop the changed entry and read it again when needed.
    #[default]
    Invalidate,
    /// Other instances receive the new value and keep it.
    Update,
    /// Nothing is sent. Instances only learn about changes when an entry expires locally.
    None,
}

struct Local {
    cache: Mutex<LruCache<Bytes, (Bytes, Instant)>>,
    epoch: AtomicU64,
    ttl: Option<Duration>,
    id: [u8; ID_LEN],
    listener: Mutex<Option<JoinHandle<()>>>,
}

impl Local {
    fn lookup(&self, key: &Bytes) -> Option<Bytes> {
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        let (value, stored) = cache.get(key)?;
        if self.ttl.is_some_and(|ttl| stored.elapsed() >= ttl) {
            cache.pop(key);
            return None;
        }
        Some(value.clone())
    }

    fn store(&self, key: Bytes, value: Bytes) {
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        cache.put(key, (value, Instant::now()));
    }

    fn forget(&self, key: &Bytes) {
        self.epoch.fetch_add(1, Ordering::AcqRel);
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        cache.pop(key);
    }

    fn forget_all(&self) {
        self.epoch.fetch_add(1, Ordering::AcqRel);
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        cache.clear();
    }

    fn len(&self) -> usize {
        self.cache.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    fn apply(&self, message: &[u8]) {
        if message.len() <= ID_LEN || message[..ID_LEN] == self.id {
            return;
        }
        let body = &message[ID_LEN + 1..];
        match message[ID_LEN] {
            INVALIDATE => self.forget(&Bytes::copy_from_slice(body)),
            UPDATE if body.len() >= 4 => {
                let key_len = u32::from_be_bytes([body[0], body[1], body[2], body[3]]) as usize;
                if body.len() >= 4 + key_len {
                    let key = Bytes::copy_from_slice(&body[4..4 + key_len]);
                    let value = Bytes::copy_from_slice(&body[4 + key_len..]);
                    self.forget(&key);
                    self.store(key, value);
                }
            }
            CLEAR => self.forget_all(),
            _ => {}
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

fn message(id: &[u8; ID_LEN], kind: u8, body: &[&[u8]]) -> Bytes {
    let mut payload = Vec::with_capacity(ID_LEN + 1 + body.iter().map(|p| p.len()).sum::<usize>());
    payload.extend_from_slice(id);
    payload.push(kind);
    body.iter().for_each(|part| payload.extend_from_slice(part));
    Bytes::from(payload)
}

fn channel(key: &Key) -> String {
    format!("redissun__local_cache:{}", key.redis_key())
}

/// Sets up a [`LocalCachedMap`]. Get one from [`Client::local_cached_map`](crate::Client::local_cached_map).
#[must_use = "a builder does nothing until build() is awaited"]
pub struct LocalCachedMapBuilder<K, V, C: Codec> {
    key: Key,
    codec: C,
    cache_size: usize,
    ttl: Option<Duration>,
    sync: SyncStrategy,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V, C: Codec> LocalCachedMapBuilder<K, V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self {
            key,
            codec,
            cache_size: 0,
            ttl: None,
            sync: SyncStrategy::default(),
            _marker: PhantomData,
        }
    }

    /// Keeps at most `size` entries locally and drops the least recently used one beyond that. Zero, the default, means no limit.
    pub fn cache_size(mut self, size: usize) -> Self {
        self.cache_size = size;
        self
    }

    /// Drops a local entry this long after it was stored, whether it is read or not. By default entries stay until they are changed or pushed out.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Chooses how changes reach the other instances. The default is [`SyncStrategy::Invalidate`].
    pub fn sync_strategy(mut self, sync: SyncStrategy) -> Self {
        self.sync = sync;
        self
    }

    /// Subscribes to the changes of the other instances and returns the map.
    pub async fn build(self) -> Result<LocalCachedMap<K, V, C>> {
        let cache = match NonZeroUsize::new(self.cache_size) {
            Some(size) => LruCache::new(size),
            None => LruCache::unbounded(),
        };
        let local = Arc::new(Local {
            cache: Mutex::new(cache),
            epoch: AtomicU64::new(0),
            ttl: self.ttl,
            id: *Uuid::new_v4().as_bytes(),
            listener: Mutex::new(None),
        });
        if self.sync != SyncStrategy::None {
            let pubsub = self.key.core.pubsub.clone();
            let (subscription, messages) =
                pubsub.subscribe_with_messages(&channel(&self.key)).await?;
            let reconnects = pubsub.reconnects();
            let handle = tokio::spawn(listen(
                Arc::downgrade(&local),
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
            _marker: PhantomData,
        })
    }
}

async fn listen(
    local: Weak<Local>,
    _subscription: crate::pubsub::Subscription,
    mut messages: tokio::sync::broadcast::Receiver<Bytes>,
    mut reconnects: tokio::sync::broadcast::Receiver<()>,
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
                match reconnected {
                    Ok(()) | Err(RecvError::Lagged(_)) => local.forget_all(),
                    Err(RecvError::Closed) => return,
                }
            }
        }
    }
}

/// A [`HashMap`] with a cache inside this program, as in Redisson's `RLocalCachedMap`. Reads that hit the cache cost no network round trip.
///
/// Every write goes to Redis and publishes a message in the same Lua script, so the other instances of the same map can drop or update their copy (see [`SyncStrategy`]). An instance never reacts to its own messages, and its own writes update its cache. When the pub/sub connection is lost and restored, or messages are missed, the whole local cache is cleared, because changes may have been lost.
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
    fn publish_flag(&self) -> Bytes {
        Bytes::from(if self.sync == SyncStrategy::None {
            "0"
        } else {
            "1"
        })
    }

    /// Returns the value for a key, from the local cache when it is there. A value read from Redis is cached. The key is borrowed: `map.get("k")`.
    pub async fn get<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let field = self.codec.encode(k)?;
        if let Some(value) = self.local.lookup(&field) {
            return Ok(Some(self.codec.decode(&value)?));
        }
        let epoch = self.local.epoch.load(Ordering::Acquire);
        let raw: Option<Bytes> = self
            .key
            .core
            .redis()
            .hget(self.key.redis_key(), field.clone())
            .await?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let value = self.codec.decode(&raw)?;
        if self.local.epoch.load(Ordering::Acquire) == epoch {
            self.local.store(field, raw);
        }
        Ok(Some(value))
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
        let previous: Option<Bytes> = self
            .key
            .core
            .eval(
                &PUT,
                vec![self.key.redis_key()],
                vec![
                    field.clone(),
                    value.clone(),
                    Bytes::from(channel(&self.key)),
                    self.publish_flag(),
                    notice,
                ],
            )
            .await?;
        self.local.forget(&field);
        self.local.store(field, value);
        previous.map(|bytes| self.codec.decode(&bytes)).transpose()
    }

    /// Removes a key and returns its value. The other instances are told.
    pub async fn remove<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let field = self.codec.encode(k)?;
        let notice = message(&self.local.id, INVALIDATE, &[&field]);
        let previous: Option<Bytes> = self
            .key
            .core
            .eval(
                &REMOVE,
                vec![self.key.redis_key()],
                vec![
                    field.clone(),
                    Bytes::from(channel(&self.key)),
                    self.publish_flag(),
                    notice,
                ],
            )
            .await?;
        self.local.forget(&field);
        previous.map(|bytes| self.codec.decode(&bytes)).transpose()
    }

    /// Returns whether the key exists. A cached key answers without a round trip.
    pub async fn contains_key<Q>(&self, k: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let field = self.codec.encode(k)?;
        if self.local.lookup(&field).is_some() {
            return Ok(true);
        }
        self.map.contains_key(k).await
    }

    /// Number of entries in Redis.
    pub async fn len(&self) -> Result<usize> {
        self.map.len().await
    }

    /// Returns whether Redis holds no entry.
    pub async fn is_empty(&self) -> Result<bool> {
        self.map.is_empty().await
    }

    /// Removes every entry for everybody and empties the local cache of the other instances.
    pub async fn clear(&self) -> Result<()> {
        let notice = message(&self.local.id, CLEAR, &[]);
        let _: i64 = self
            .key
            .core
            .eval(
                &CLEAR_ALL,
                vec![self.key.redis_key()],
                vec![Bytes::from(channel(&self.key)), self.publish_flag(), notice],
            )
            .await?;
        self.local.forget_all();
        Ok(())
    }

    /// Number of entries in the local cache of this instance.
    pub fn local_len(&self) -> usize {
        self.local.len()
    }

    /// Empties the local cache of this instance only.
    pub fn clear_local(&self) {
        self.local.forget_all();
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
