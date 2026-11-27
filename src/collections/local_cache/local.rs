//! The local cache with the reads in flight, and the instance that applies the sync messages.

use super::messages::{now_millis, CLEAR, EXPIRING_UPDATE, ID_LEN, INVALIDATE, UPDATE};
use super::options::ReconnectionStrategy;
use super::store::{copy_lifetime, Lifetime, LocalStore};
use bytes::Bytes;
use std::collections::HashMap as StdHashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

/// The local cache and the reads from Redis in flight, under one lock: a change that arrives while a key is read marks the read, so its stale result is not cached.
pub(crate) struct Cache {
    pub(crate) store: LocalStore,
    /// Per key: the reads in flight, and whether a change arrived meanwhile.
    reads: StdHashMap<Bytes, (usize, bool)>,
}

impl Cache {
    fn invalidate(&mut self, key: &Bytes) {
        self.store.remove(key);
        if let Some(read) = self.reads.get_mut(key) {
            read.1 = true;
        }
    }

    fn clear(&mut self) {
        self.store.clear();
        self.reads.values_mut().for_each(|read| read.1 = true);
    }

    fn begin_read(&mut self, key: &Bytes) {
        self.reads.entry(key.clone()).or_insert((0, false)).0 += 1;
    }

    fn finish_read(&mut self, key: Bytes, value: Option<(Option<Bytes>, Lifetime)>) {
        let Some(read) = self.reads.get_mut(&key) else {
            return;
        };
        read.0 -= 1;
        let dirty = read.1;
        if read.0 == 0 {
            self.reads.remove(&key);
        }
        if let (false, Some((value, lifetime))) = (dirty, value) {
            self.store.put(key, value, lifetime);
        }
    }
}

/// A read of one key from Redis in flight; see [`Local::begin_read`].
pub(crate) struct ReadGuard<'a> {
    local: &'a Local,
    /// `None` once the read is finished.
    key: Option<Bytes>,
}

impl ReadGuard<'_> {
    /// Ends the read; `Some((value, lifetime))` caches `value` for `lifetime` unless the key changed meanwhile.
    pub(crate) fn finish(mut self, value: Option<(Option<Bytes>, Lifetime)>) {
        if let Some(key) = self.key.take() {
            self.local.cache().finish_read(key, value);
        }
    }
}

impl Drop for ReadGuard<'_> {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.local.cache().finish_read(key, None);
        }
    }
}

pub(crate) struct Local {
    cache: Mutex<Cache>,
    pub(crate) id: [u8; ID_LEN],
    pub(crate) reconnection: ReconnectionStrategy,
    pub(crate) last_invalidate: AtomicI64,
    pub(crate) listener: Mutex<Option<JoinHandle<()>>>,
}

impl Local {
    pub(crate) fn new(store: LocalStore, reconnection: ReconnectionStrategy) -> Self {
        Self {
            cache: Mutex::new(Cache {
                store,
                reads: StdHashMap::new(),
            }),
            id: *Uuid::new_v4().as_bytes(),
            reconnection,
            last_invalidate: AtomicI64::new(0),
            listener: Mutex::new(None),
        }
    }

    fn cache(&self) -> std::sync::MutexGuard<'_, Cache> {
        self.cache.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn lookup(&self, key: &Bytes) -> Option<Option<Bytes>> {
        self.cache().store.get(key)
    }

    pub(crate) fn write(&self, key: Bytes, value: Option<Bytes>, lifetime: Lifetime) {
        let mut cache = self.cache();
        cache.invalidate(&key);
        cache.store.put(key, value, lifetime);
    }

    /// Opens a read of `key` from Redis. The guard ends it: [`ReadGuard::finish`] with what was read, or its drop, caching nothing, when the read is cancelled or fails.
    pub(crate) fn begin_read(&self, key: &Bytes) -> ReadGuard<'_> {
        self.cache().begin_read(key);
        ReadGuard {
            local: self,
            key: Some(key.clone()),
        }
    }

    pub(crate) fn forget(&self, key: &Bytes) {
        self.cache().invalidate(key);
    }

    pub(crate) fn forget_all(&self) {
        self.cache().clear();
    }

    /// Every local slot, cached misses included.
    pub(crate) fn len(&self) -> usize {
        self.cache().store.len()
    }

    /// The local slots that hold a value, encoded.
    pub(crate) fn entries(&self) -> std::vec::Vec<(Bytes, Bytes)> {
        self.cache().store.entries()
    }

    pub(crate) fn apply(&self, message: &[u8]) {
        if message.len() < ID_LEN + 1 {
            return;
        }
        if message[..ID_LEN] != self.id {
            let body = &message[ID_LEN + 1..];
            match message[ID_LEN] {
                INVALIDATE => self.forget(&Bytes::copy_from_slice(body)),
                UPDATE => {
                    if let Some((key, value)) = key_and_value(body) {
                        self.write(key, Some(value), Lifetime::default());
                    }
                }
                EXPIRING_UPDATE => self.apply_expiring(body),
                CLEAR => self.forget_all(),
                _ => {}
            }
        }
        if self.reconnection == ReconnectionStrategy::Load {
            self.last_invalidate.store(now_millis(), Ordering::Release);
        }
    }

    /// An `E` body: the key is forgotten, then cached with the entry's lifetime unless that has already ended.
    fn apply_expiring(&self, body: &[u8]) {
        let (Some(ttl_timeout), Some(idle_delta)) = (be_i64(body, 0), be_i64(body, 8)) else {
            return;
        };
        let Some((key, value)) = body.get(16..).and_then(key_and_value) else {
            return;
        };
        self.forget(&key);
        if let Some(lifetime) = copy_lifetime(ttl_timeout, idle_delta, now_millis()).lifetime() {
            self.write(key, Some(value), lifetime);
        }
    }
}

/// `[key_len: u32 BE][key][value]`.
fn key_and_value(body: &[u8]) -> Option<(Bytes, Bytes)> {
    let len: [u8; 4] = body.get(..4)?.try_into().ok()?;
    let key_len = u32::from_be_bytes(len) as usize;
    let key = body.get(4..4usize.checked_add(key_len)?)?;
    let value = &body[4 + key_len..];
    Some((Bytes::copy_from_slice(key), Bytes::copy_from_slice(value)))
}

fn be_i64(body: &[u8], at: usize) -> Option<i64> {
    let bytes: [u8; 8] = body.get(at..at + 8)?.try_into().ok()?;
    Some(i64::from_be_bytes(bytes))
}

impl Drop for Local {
    fn drop(&mut self) {
        let listener = self.listener.get_mut().unwrap_or_else(|e| e.into_inner());
        if let Some(listener) = listener.take() {
            listener.abort();
        }
    }
}
