//! [`HashMap::entry`] and [`HashMapCache::entry`]: one key's value, read and changed under the key's lock, like `std::collections::hash_map::Entry`.

use crate::codec::Codec;
use crate::collections::hash_map::HashMap;
use crate::collections::hash_map_cache::HashMapCache;
use crate::collections::local_cached_map::LocalCachedMap;
use crate::collections::map::load::load_value_locked;
use crate::collections::map::ops::KeyLock;
use crate::collections::map::store::Store;
use crate::error::Result;
use crate::object::{HasKey, Key};
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt;
use std::time::Duration;

/// One key of a [`HashMap`] or a [`HashMapCache`], held under the key's lock until the entry is consumed or dropped. See [`HashMap::entry`] and [`HashMapCache::entry`].
///
/// Dropping an entry without consuming it changes nothing and releases the lock in the background.
#[must_use = "an entry holds the key's lock until it is consumed or dropped"]
pub enum Entry<'m, K, V, C: Codec> {
    /// The key has a value.
    Occupied(OccupiedEntry<'m, K, V, C>),
    /// The key has no value.
    Vacant(VacantEntry<'m, K, V, C>),
}

/// An [`Entry`] whose key has a value.
#[must_use = "an entry holds the key's lock until it is consumed or dropped"]
pub struct OccupiedEntry<'m, K, V, C: Codec> {
    held: Held<'m, K, V, C>,
    value: V,
    modified: bool,
}

/// An [`Entry`] whose key has no value.
#[must_use = "an entry holds the key's lock until it is consumed or dropped"]
pub struct VacantEntry<'m, K, V, C: Codec> {
    held: Held<'m, K, V, C>,
}

/// The map an [`Entry`] reads and writes. It stays in a private field, so the public entry types do not depend on it.
pub(crate) enum Target<'m, K, V, C: Codec> {
    Map(&'m HashMap<K, V, C>),
    /// A cache and the time to live its writes get; `None` writes like `insert`, which clears the entry's limits.
    Cache(&'m HashMapCache<K, V, C>, Option<Duration>),
    /// A local cached map, for loading only: reads look at the local cache first, and writes go through its plain put.
    Local(&'m LocalCachedMap<K, V, C>),
}

impl<K, V, C> Target<'_, K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    pub(crate) fn key(&self) -> &Key {
        match self {
            Target::Map(map) => map.redis_key(),
            Target::Cache(cache, _) => HasKey::key(*cache),
            Target::Local(map) => HasKey::key(*map),
        }
    }

    /// Reads an encoded key from the master.
    pub(crate) async fn read(&self, k: &Bytes) -> Result<Option<V>> {
        match self {
            Target::Map(map) => map.read_encoded(k).await,
            Target::Cache(cache, _) => cache.read_encoded(k).await,
            Target::Local(map) => map.read_encoded(k).await,
        }
    }

    /// Stores the value with the map's plain put, without the writer.
    pub(crate) async fn write(&self, k: &Bytes, v: &V) -> Result<()> {
        match self {
            Target::Map(map) => map.write_encoded(k, v).await,
            Target::Cache(cache, ttl) => cache.write_encoded(k, v, *ttl).await,
            Target::Local(map) => map.put_encoded(k, v).await,
        }
    }

    /// Removes the key, without the writer.
    pub(crate) async fn delete(&self, k: &Bytes) -> Result<()> {
        match self {
            Target::Map(map) => map.delete_encoded(k).await,
            Target::Cache(cache, _) => cache.delete_encoded(k).await,
            Target::Local(map) => map.delete_encoded(k).await,
        }
    }

    /// The map's loader and writer; `None` when it has neither.
    pub(crate) fn store(&self) -> Option<&Store<K, V, C>> {
        match self {
            Target::Map(map) => map.store(),
            Target::Cache(cache, _) => cache.store(),
            Target::Local(map) => map.store(),
        }
    }

    /// Stores a loaded value with the map's plain put, Redisson's `loadValue`: `HSET` on a map, `PUT` without limits on a cache, also inside an entry with a time to live.
    pub(crate) async fn write_loaded(&self, k: &Bytes, v: &V) -> Result<()> {
        match self {
            Target::Map(map) => map.write_encoded(k, v).await,
            Target::Cache(cache, _) => cache.write_encoded(k, v, None).await,
            Target::Local(map) => map.put_encoded(k, v).await,
        }
    }
}

struct Held<'m, K, V, C: Codec> {
    target: Target<'m, K, V, C>,
    key: Bytes,
    lock: KeyLock,
}

impl<K, V, C> Held<'_, K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    /// Writes the value, hands it to the writer, then releases the lock.
    async fn write(self, value: &V) -> Result<()> {
        let body = async {
            self.target.write(&self.key, value).await?;
            match self.target.store() {
                Some(store) => store.written_value(&self.key, value).await,
                None => Ok(()),
            }
        }
        .await;
        self.lock.release(body).await
    }
}

impl<'m, K, V, C> Entry<'m, K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    pub(crate) async fn acquire(target: Target<'m, K, V, C>, key: Bytes) -> Result<Self> {
        let lock = KeyLock::acquire(target.key(), &key).await?;
        let value = match target.read(&key).await {
            // A miss asks the loader under the lock this entry already holds; taking it again would wait forever.
            Ok(None) if target.store().is_some_and(Store::has_loader) => {
                load_value_locked(&target, &key, true).await
            }
            other => other,
        };
        let value = match value {
            Ok(value) => value,
            Err(e) => return lock.release(Err(e)).await,
        };
        let held = Held { target, key, lock };
        Ok(match value {
            Some(value) => Entry::Occupied(OccupiedEntry {
                held,
                value,
                modified: false,
            }),
            None => Entry::Vacant(VacantEntry { held }),
        })
    }

    /// Changes the present value in place; the change is written by the `or_insert` that follows. A vacant entry is left as it is.
    pub fn and_modify(self, f: impl FnOnce(&mut V)) -> Self {
        match self {
            Entry::Occupied(mut entry) => {
                f(&mut entry.value);
                entry.modified = true;
                Entry::Occupied(entry)
            }
            vacant => vacant,
        }
    }

    /// Stores `default` for a vacant key, or writes the present value when [`and_modify`](Entry::and_modify) changed it, and returns the value the key now has. Releases the lock.
    pub async fn or_insert(self, default: V) -> Result<V> {
        self.or_insert_with(|| default).await
    }

    /// Like [`or_insert`](Entry::or_insert), with the default made by `default` only for a vacant key.
    pub async fn or_insert_with(self, default: impl FnOnce() -> V) -> Result<V> {
        match self {
            Entry::Occupied(entry) => {
                if entry.modified {
                    entry.held.write(&entry.value).await?;
                } else {
                    entry.held.lock.release(Ok(())).await?;
                }
                Ok(entry.value)
            }
            Entry::Vacant(entry) => entry.insert(default()).await,
        }
    }

    /// Like [`or_insert`](Entry::or_insert) with `V::default()`.
    pub async fn or_default(self) -> Result<V>
    where
        V: Default,
    {
        self.or_insert_with(V::default).await
    }
}

impl<K, V, C> OccupiedEntry<'_, K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    /// The value the key had when the entry was taken, with any [`Entry::and_modify`] change.
    pub fn get(&self) -> &V {
        &self.value
    }

    /// Stores `value` and returns the value it replaced. Releases the lock.
    pub async fn insert(self, value: V) -> Result<V> {
        self.held.write(&value).await?;
        Ok(self.value)
    }

    /// Removes the entry and returns its value. Releases the lock.
    pub async fn remove(self) -> Result<V> {
        let held = &self.held;
        let body = async {
            held.target.delete(&held.key).await?;
            match held.target.store() {
                Some(store) => store.deleted(std::slice::from_ref(&held.key)).await,
                None => Ok(()),
            }
        }
        .await;
        self.held.lock.release(body).await?;
        Ok(self.value)
    }

    /// Releases the lock and returns the value, without writing anything.
    pub async fn into_value(self) -> Result<V> {
        self.held.lock.release(Ok(())).await?;
        Ok(self.value)
    }
}

impl<K, V, C> VacantEntry<'_, K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    /// Stores `value` and returns it. Releases the lock.
    pub async fn insert(self, value: V) -> Result<V> {
        self.held.write(&value).await?;
        Ok(value)
    }
}

impl<K, V, C: Codec> fmt::Debug for Entry<'_, K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Entry::Occupied(entry) => f.debug_tuple("Entry").field(entry).finish(),
            Entry::Vacant(entry) => f.debug_tuple("Entry").field(entry).finish(),
        }
    }
}

impl<K, V, C: Codec> fmt::Debug for OccupiedEntry<'_, K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OccupiedEntry")
            .field("modified", &self.modified)
            .finish_non_exhaustive()
    }
}

impl<K, V, C: Codec> fmt::Debug for VacantEntry<'_, K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VacantEntry").finish_non_exhaustive()
    }
}
