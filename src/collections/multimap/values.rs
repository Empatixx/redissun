//! The values of one multimap key: `ListMultimapValues` (Redisson's `InnerList`) and `SetMultimapValues` (`InnerSet`).
//!
//! A handle is lazy: building it sends nothing. Writes that can add or remove the key go through the multimap scripts, so the key index (the hash) stays right. Reads and index writes go to the existing [`crate::Vec`] or [`crate::HashSet`] on the values key.

use super::{scripts, Ops, Shape, Slot};
use crate::codec::Codec;
use crate::collections::{hash_set::HashSet, list, vec::Vec};
use crate::error::{Error, Result};
use crate::lua::Lua;
use crate::object::{now_millis, Key};
use bytes::Bytes;
use futures::{stream, Stream, StreamExt, TryStreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::ops::RangeBounds;

/// The shared parts of both handles.
fn ops<'a, C: Codec>(slot: &'a Slot, codec: &'a C, shape: Shape) -> Ops<'a, C> {
    Ops {
        key: &slot.map,
        codec,
        shape,
        mode: &slot.mode,
    }
}

/// The page size of the cache list handle's `iter`, as `Vec::iter`.
const LIST_PAGE: i64 = 100;
/// The `SSCAN` count of the cache set handle's `iter`, Redisson's `iterator()` (10).
const SET_PAGE: usize = 10;

/// ARGV that the cache handle scripts start with: the client clock, Redisson's `System.currentTimeMillis()`, and the encoded key.
fn now_and_key(slot: &Slot) -> std::vec::Vec<Bytes> {
    vec![
        Bytes::from(now_millis().to_string()),
        slot.encoded_key.clone(),
    ]
}

/// Runs a script of `RedissonListMultimapValues`/`RedissonSetMultimapValues`: KEYS are the ttl zset and the values key.
async fn cache_eval<R: redis::FromRedisValue>(
    slot: &Slot,
    timeout: &str,
    script: &'static Lua,
    args: std::vec::Vec<Bytes>,
) -> Result<R> {
    slot.map
        .core
        .eval(
            script,
            vec![timeout.to_string(), slot.values.redis_key()],
            args,
        )
        .await
}

/// A cache handle script whose ARGV are the clock, the key and `values`.
async fn cache_eval_values<R: redis::FromRedisValue>(
    slot: &Slot,
    timeout: &str,
    script: &'static Lua,
    values: std::vec::Vec<Bytes>,
) -> Result<R> {
    let args = now_and_key(slot).into_iter().chain(values).collect();
    cache_eval(slot, timeout, script, args).await
}

/// `removeAll(values)` of `InnerList`/`InnerSet`: whether any value was removed; the key goes when its values do. No values send nothing.
async fn remove_values(slot: &Slot, shape: Shape, values: std::vec::Vec<Bytes>) -> Result<bool> {
    if values.is_empty() {
        return Ok(false);
    }
    if let Some(timeout) = slot.mode.timeout() {
        let script = match shape {
            Shape::List => &scripts::LV_REMOVE_ALL,
            Shape::Set => &scripts::SV_REMOVE_ALL,
        };
        let removed: i64 = cache_eval_values(slot, timeout, script, values).await?;
        return Ok(removed == 1);
    }
    let script = match shape {
        Shape::List => &scripts::LIST_VALUES_REMOVE_ALL,
        Shape::Set => &scripts::SET_VALUES_REMOVE_ALL,
    };
    let args = std::iter::once(slot.encoded_key.clone())
        .chain(values)
        .collect();
    let removed: i64 = slot
        .map
        .core
        .eval(
            script,
            vec![slot.map.redis_key(), slot.values.redis_key()],
            args,
        )
        .await?;
    Ok(removed == 1)
}

/// `LV_RANGE`: the values from `start` to `stop` (inclusive, `-1` for the end); nothing for an expired key.
async fn cache_range(
    slot: &Slot,
    timeout: &str,
    start: i64,
    stop: i64,
) -> Result<std::vec::Vec<Bytes>> {
    let args = now_and_key(slot)
        .into_iter()
        .chain([
            Bytes::from(start.to_string()),
            Bytes::from(stop.to_string()),
        ])
        .collect();
    cache_eval(slot, timeout, &scripts::LV_RANGE, args).await
}

/// The handle's `clear`: the multimap's `remove_all` of the key in cache mode (D9), which also drops its score, and `remove_many` otherwise.
async fn clear<C: Codec>(ops: &Ops<'_, C>, slot: &Slot) -> Result<()> {
    if slot.mode.timeout().is_some() {
        ops.remove_all(slot).await?;
    } else {
        ops.remove_many(std::slice::from_ref(slot)).await?;
    }
    Ok(())
}

/// **Experimental.** The values of one key of a [`ListMultimap`](crate::ListMultimap), [`ListMultimapCache`](crate::ListMultimapCache) or [`ListMultimapCacheNative`](crate::ListMultimapCacheNative), Redisson's `RList` from `get(key)`. Get one with `get(&k)`.
///
/// The handle is lazy: building it sends nothing. [`push`](Self::push), [`extend`](Self::extend), [`remove_value`](Self::remove_value), [`remove_values`](Self::remove_values) and [`clear`](Self::clear) go through the multimap, so a new key joins the key index and a key whose last value goes leaves it. Reads and index writes run on the list itself. An index write that empties the list ([`pop`](Self::pop), [`remove`](Self::remove), [`trim`](Self::trim), [`retain_values`](Self::retain_values)) keeps the key in the index, as in Redisson: [`contains_key`](crate::ListMultimap::contains_key) is `false`, but `key_len` and `keys` count it until `remove_all` or `remove_many` removes it.
///
/// On a [`ListMultimapCache`](crate::ListMultimapCache) the reads, [`remove_value`](Self::remove_value), [`remove_value_n`](Self::remove_value_n), [`remove_value_all`](Self::remove_value_all), [`remove_values`](Self::remove_values) and [`retain_values`](Self::retain_values) run Redisson's TTL-aware scripts: an expired key reads as empty and these writes leave it alone. Those removals do not take the key out of the index when they empty the list, as in Redisson. [`push`](Self::push) and [`extend`](Self::extend) go through the multimap, so a new key joins the index (Redisson's cache handle writes the list only), and [`clear`](Self::clear) is the multimap's `remove_all`, which also drops the key's TTL. The index writes (`set`, `insert`, `remove`, `pop`, `trim`, …) do not check the TTL, as in Redisson.
pub struct ListMultimapValues<V, C: Codec> {
    slot: Slot,
    list: Vec<V, C>,
    codec: C,
}

impl<V, C: Codec> Clone for ListMultimapValues<V, C> {
    fn clone(&self) -> Self {
        Self {
            slot: self.slot.clone(),
            list: self.list.clone(),
            codec: self.codec.clone(),
        }
    }
}

impl<V, C: Codec> fmt::Debug for ListMultimapValues<V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.slot.values.describe(f, "ListMultimapValues")
    }
}

impl<V, C: Codec> ListMultimapValues<V, C> {
    pub(crate) fn new(slot: Slot, codec: C) -> Self {
        let list = Vec::new(slot.values.clone(), codec.clone());
        Self { slot, list, codec }
    }

    fn ops(&self) -> Ops<'_, C> {
        ops(&self.slot, &self.codec, Shape::List)
    }

    /// The name of the list that holds the values, `{name}:<key hash>`.
    pub fn name(&self) -> &str {
        self.slot.values.name()
    }

    /// The raw [`crate::Vec`] on the values key, for the `RList` methods the handle does not have (`sort`, `move`, …).
    ///
    /// Writes through it do not add the key to the multimap or check its TTL, as with Redisson's inherited `RList` methods.
    pub fn as_vec(&self) -> &Vec<V, C> {
        &self.list
    }
}

impl<V, C> ListMultimapValues<V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    /// Appends a value through the multimap's `insert`, Redisson's `add`.
    #[doc(alias = "add")]
    pub async fn push<W>(&self, v: &W) -> Result<()>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        let v = self.codec.encode(v)?;
        self.ops().insert(&self.slot, v).await?;
        Ok(())
    }

    /// Appends values in one round trip through the multimap's `extend`, Redisson's `addAll`. No values send nothing.
    #[doc(alias = "addAll")]
    pub async fn extend<'a, W>(&self, values: impl IntoIterator<Item = &'a W>) -> Result<()>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync + 'a,
    {
        let ops = self.ops();
        let values = ops.encode_values(values)?;
        ops.extend(&self.slot, values).await?;
        Ok(())
    }

    /// The value at the index, or `None` past the end.
    pub async fn get(&self, index: usize) -> Result<Option<V>> {
        let Some(timeout) = self.slot.mode.timeout() else {
            return self.list.get(index).await;
        };
        let Ok(index) = i64::try_from(index) else {
            return Ok(None);
        };
        let args = vec![
            Bytes::from(now_millis().to_string()),
            Bytes::from(index.to_string()),
            self.slot.encoded_key.clone(),
        ];
        let raw: Option<Bytes> = cache_eval(&self.slot, timeout, &scripts::LV_GET, args).await?;
        raw.map(|bytes| self.codec.decode(&bytes)).transpose()
    }

    /// The values in the range, as [`Vec::range`](crate::Vec::range). Redisson's inclusive `range(1, 3)` is `range(1..=3)`, and `range(2)` is `range(..=2)`.
    pub async fn range(&self, range: impl RangeBounds<usize>) -> Result<std::vec::Vec<V>> {
        let Some(timeout) = self.slot.mode.timeout() else {
            return self.list.range(range).await;
        };
        let Some((start, stop)) = list::bounds(range) else {
            return Ok(std::vec::Vec::new());
        };
        let raw = cache_range(&self.slot, timeout, start, stop).await?;
        self.ops().decode_all(raw)
    }

    /// Every value in order, Redisson's `readAll`.
    #[doc(alias = "readAll")]
    pub async fn to_vec(&self) -> Result<std::vec::Vec<V>> {
        match self.slot.mode.timeout() {
            Some(_) => self.range(..).await,
            None => self.list.to_vec().await,
        }
    }

    /// The number of values, Redisson's `size`.
    #[doc(alias = "size")]
    pub async fn len(&self) -> Result<usize> {
        match self.slot.mode.timeout() {
            Some(timeout) => {
                cache_eval(
                    &self.slot,
                    timeout,
                    &scripts::LV_SIZE,
                    now_and_key(&self.slot),
                )
                .await
            }
            None => self.list.len().await,
        }
    }

    /// Whether the key has no values.
    #[doc(alias = "isEmpty")]
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// Whether an equal value is there.
    pub async fn contains<W>(&self, v: &W) -> Result<bool>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        Ok(self.position(v).await?.is_some())
    }

    /// The index of the first equal value, Redisson's `indexOf`.
    #[doc(alias = "indexOf")]
    pub async fn position<W>(&self, v: &W) -> Result<Option<usize>>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        match self.slot.mode.timeout() {
            Some(timeout) => self.cache_index_of(timeout, &scripts::LV_INDEX_OF, v).await,
            None => self.list.position(v).await,
        }
    }

    /// The index of the last equal value, Redisson's `lastIndexOf`.
    #[doc(alias = "lastIndexOf")]
    pub async fn rposition<W>(&self, v: &W) -> Result<Option<usize>>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        match self.slot.mode.timeout() {
            Some(timeout) => {
                self.cache_index_of(timeout, &scripts::LV_LAST_INDEX_OF, v)
                    .await
            }
            None => self.list.rposition(v).await,
        }
    }

    /// Whether every one of the values is there, Redisson's `containsAll`. It is `true` for no values.
    #[doc(alias = "containsAll")]
    pub async fn contains_all<'a, W>(&self, values: impl IntoIterator<Item = &'a W>) -> Result<bool>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync + 'a,
    {
        let Some(timeout) = self.slot.mode.timeout() else {
            return self.list.contains_all(values).await;
        };
        let values = self.ops().encode_values(values)?;
        if values.is_empty() {
            return Ok(true);
        }
        let found: i64 =
            cache_eval_values(&self.slot, timeout, &scripts::LV_CONTAINS_ALL, values).await?;
        Ok(found == 1)
    }

    /// Removes the first equal value through the multimap's `remove`, Redisson's `remove(value)`. The key goes with its last value.
    #[doc(alias = "remove")]
    pub async fn remove_value<W>(&self, v: &W) -> Result<bool>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        let v = self.codec.encode(v)?;
        match self.slot.mode.timeout() {
            Some(timeout) => Ok(self.cache_remove(timeout, v, 1).await? > 0),
            None => self.ops().remove(&self.slot, v).await,
        }
    }

    /// Removes up to `count` equal values from the front and returns how many went, Redisson's `remove(value, count)`. It does not remove the key from the index.
    pub async fn remove_value_n<W>(&self, v: &W, count: usize) -> Result<usize>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        let Some(timeout) = self.slot.mode.timeout() else {
            return self.list.remove_value_n(v, count).await;
        };
        if count == 0 {
            return Ok(0);
        }
        let v = self.codec.encode(v)?;
        self.cache_remove(timeout, v, i64::try_from(count).unwrap_or(i64::MAX))
            .await
    }

    /// Removes every equal value and returns how many went. It does not remove the key from the index.
    pub async fn remove_value_all<W>(&self, v: &W) -> Result<usize>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        let Some(timeout) = self.slot.mode.timeout() else {
            return self.list.remove_value_all(v).await;
        };
        let v = self.codec.encode(v)?;
        self.cache_remove(timeout, v, 0).await
    }

    /// Removes every occurrence of each value and returns whether any was removed, Redisson's `removeAll`. The key goes when its list is emptied. No values send nothing.
    ///
    /// A value that occurred several times counts as removed too. Redisson's script counts it only when it occurred exactly once (deviation D10).
    #[doc(alias = "removeAll")]
    pub async fn remove_values<'a, W>(
        &self,
        values: impl IntoIterator<Item = &'a W>,
    ) -> Result<bool>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync + 'a,
    {
        let values = self.ops().encode_values(values)?;
        remove_values(&self.slot, Shape::List, values).await
    }

    /// Keeps only the values equal to one of `values`, Redisson's `retainAll`; returns whether any was removed. It does not remove the key from the index.
    #[doc(alias = "retainAll")]
    pub async fn retain_values<'a, W>(
        &self,
        values: impl IntoIterator<Item = &'a W>,
    ) -> Result<bool>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync + 'a,
    {
        let Some(timeout) = self.slot.mode.timeout() else {
            return self.list.retain_values(values).await;
        };
        let values = self.ops().encode_values(values)?;
        let changed: i64 =
            cache_eval_values(&self.slot, timeout, &scripts::LV_RETAIN_ALL, values).await?;
        Ok(changed == 1)
    }

    /// Replaces the value at the index without reading the old one, as [`Vec::set`](crate::Vec::set).
    #[doc(alias = "fastSet")]
    pub async fn set<W>(&self, index: usize, v: &W) -> Result<()>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        self.list.set(index, v).await
    }

    /// Replaces the value at the index and returns the old one, as [`Vec::replace`](crate::Vec::replace).
    pub async fn replace<W>(&self, index: usize, v: &W) -> Result<V>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        self.list.replace(index, v).await
    }

    /// Inserts a value before the index, as [`Vec::insert`](crate::Vec::insert). It writes the list only, so on a key not yet in the multimap use [`push`](Self::push).
    pub async fn insert<W>(&self, index: usize, v: &W) -> Result<()>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        self.list.insert(index, v).await
    }

    /// Inserts values before the index, as [`Vec::insert_many`](crate::Vec::insert_many). It writes the list only.
    pub async fn insert_many<'a, W>(
        &self,
        index: usize,
        values: impl IntoIterator<Item = &'a W>,
    ) -> Result<()>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync + 'a,
    {
        self.list.insert_many(index, values).await
    }

    /// Inserts a value before the first one equal to `pivot`, as [`Vec::insert_before`](crate::Vec::insert_before).
    pub async fn insert_before<W>(&self, pivot: &W, v: &W) -> Result<Option<usize>>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        self.list.insert_before(pivot, v).await
    }

    /// Inserts a value after the first one equal to `pivot`, as [`Vec::insert_after`](crate::Vec::insert_after).
    pub async fn insert_after<W>(&self, pivot: &W, v: &W) -> Result<Option<usize>>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        self.list.insert_after(pivot, v).await
    }

    /// Removes and returns the value at the index, as [`Vec::remove`](crate::Vec::remove). It does not remove the key from the index.
    #[doc(alias = "fastRemove")]
    pub async fn remove(&self, index: usize) -> Result<V> {
        self.list.remove(index).await
    }

    /// Removes and returns the last value, as [`Vec::pop`](crate::Vec::pop). It does not remove the key from the index.
    pub async fn pop(&self) -> Result<Option<V>> {
        self.list.pop().await
    }

    /// Keeps only the values in the range, as [`Vec::trim`](crate::Vec::trim). It does not remove the key from the index.
    pub async fn trim(&self, range: impl RangeBounds<usize>) -> Result<()> {
        self.list.trim(range).await
    }

    /// Removes the key with all its values, Redisson's `delete` on the handle.
    #[doc(alias = "delete")]
    pub async fn clear(&self) -> Result<()> {
        clear(&self.ops(), &self.slot).await
    }

    /// Streams the values in order, in pages of 100, Redisson's `iterator`.
    #[doc(alias = "iterator")]
    pub fn iter(&self) -> impl Stream<Item = Result<V>> + '_ {
        match self.slot.mode.timeout() {
            Some(timeout) => self.cache_pages(timeout).left_stream(),
            None => self.list.iter().right_stream(),
        }
    }

    /// `indexOf`/`lastIndexOf` of the cache handle: `-1` for none or an expired key.
    async fn cache_index_of<W>(
        &self,
        timeout: &str,
        script: &'static Lua,
        v: &W,
    ) -> Result<Option<usize>>
    where
        W: Serialize + ?Sized + Sync,
    {
        let v = self.codec.encode(v)?;
        let index: i64 = cache_eval_values(&self.slot, timeout, script, vec![v]).await?;
        Ok(usize::try_from(index).ok())
    }

    /// `remove(value, count)` of the cache handle: how many went (D7); `0` for an expired key.
    async fn cache_remove(&self, timeout: &str, v: Bytes, count: i64) -> Result<usize> {
        let args = vec![
            Bytes::from(now_millis().to_string()),
            Bytes::from(count.to_string()),
            self.slot.encoded_key.clone(),
            v,
        ];
        cache_eval(&self.slot, timeout, &scripts::LV_REMOVE, args).await
    }

    /// The values in pages of 100 over `LV_RANGE`; nothing for an expired key.
    fn cache_pages<'a>(&'a self, timeout: &'a str) -> impl Stream<Item = Result<V>> + 'a {
        stream::try_unfold(Some(0i64), move |offset| async move {
            let Some(offset) = offset else {
                return Ok::<_, Error>(None);
            };
            let raw = cache_range(&self.slot, timeout, offset, offset + LIST_PAGE - 1).await?;
            if raw.is_empty() {
                return Ok(None);
            }
            let next = (raw.len() as i64 == LIST_PAGE).then_some(offset + LIST_PAGE);
            let values = self.ops().decode_all::<V>(raw)?;
            Ok(Some((stream::iter(values.into_iter().map(Ok)), next)))
        })
        .try_flatten()
    }
}

/// **Experimental.** The values of one key of a [`SetMultimap`](crate::SetMultimap), [`SetMultimapCache`](crate::SetMultimapCache) or [`SetMultimapCacheNative`](crate::SetMultimapCacheNative), Redisson's `RSet` from `get(key)`. Get one with `get(&k)`.
///
/// The handle is lazy: building it sends nothing. [`insert`](Self::insert), [`extend`](Self::extend), [`remove`](Self::remove), [`remove_values`](Self::remove_values) and [`clear`](Self::clear) go through the multimap, so a new key joins the key index and a key whose last value goes leaves it. Reads and [`retain`](Self::retain) run on the set itself; a `retain` that empties the set keeps the key in the index, as in Redisson.
///
/// On a [`SetMultimapCache`](crate::SetMultimapCache) the reads, [`remove`](Self::remove), [`remove_values`](Self::remove_values) and [`retain`](Self::retain) run Redisson's TTL-aware scripts: an expired key reads as empty and these writes leave it alone. Those removals do not take the key out of the index when they empty the set, as in Redisson. [`insert`](Self::insert) and [`extend`](Self::extend) go through the multimap, so a new key joins the index (Redisson's cache handle writes the set only), and [`clear`](Self::clear) is the multimap's `remove_all`, which also drops the key's TTL. Writes through [`as_set`](Self::as_set) do not check the TTL, as in Redisson.
pub struct SetMultimapValues<V, C: Codec> {
    slot: Slot,
    set: HashSet<V, C>,
    codec: C,
}

impl<V, C: Codec> Clone for SetMultimapValues<V, C> {
    fn clone(&self) -> Self {
        Self {
            slot: self.slot.clone(),
            set: self.set.clone(),
            codec: self.codec.clone(),
        }
    }
}

impl<V, C: Codec> fmt::Debug for SetMultimapValues<V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.slot.values.describe(f, "SetMultimapValues")
    }
}

impl<V, C: Codec> SetMultimapValues<V, C> {
    pub(crate) fn new(slot: Slot, codec: C) -> Self {
        let set = HashSet::new(slot.values.clone(), codec.clone());
        Self { slot, set, codec }
    }

    fn ops(&self) -> Ops<'_, C> {
        ops(&self.slot, &self.codec, Shape::Set)
    }

    /// The name of the set that holds the values, `{name}:<key hash>`.
    pub fn name(&self) -> &str {
        self.slot.values.name()
    }

    /// The raw [`crate::HashSet`] on the values key, for the `RSet` methods the handle does not have (set algebra, `pop`, `random`, `move_to`, …).
    ///
    /// Writes through it do not add the key to the multimap or check its TTL, as with Redisson's inherited `RSet` methods.
    pub fn as_set(&self) -> &HashSet<V, C> {
        &self.set
    }

    fn values_key(&self) -> &Key {
        &self.slot.values
    }
}

impl<V, C> SetMultimapValues<V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    /// Adds a value through the multimap's `insert`, Redisson's `add`; returns whether it was new.
    #[doc(alias = "add")]
    pub async fn insert<W>(&self, v: &W) -> Result<bool>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        let v = self.codec.encode(v)?;
        self.ops().insert(&self.slot, v).await
    }

    /// Adds values in one round trip through the multimap's `extend`, Redisson's `addAll`; returns how many were new. No values send nothing.
    #[doc(alias = "addAll")]
    pub async fn extend<'a, W>(&self, values: impl IntoIterator<Item = &'a W>) -> Result<usize>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync + 'a,
    {
        let ops = self.ops();
        let values = ops.encode_values(values)?;
        ops.extend_count(&self.slot, values).await
    }

    /// Removes a value through the multimap's `remove`; returns whether it was there. The key goes with its last value.
    pub async fn remove<W>(&self, v: &W) -> Result<bool>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        let v = self.codec.encode(v)?;
        match self.slot.mode.timeout() {
            Some(timeout) => {
                let removed: i64 =
                    cache_eval_values(&self.slot, timeout, &scripts::SV_REMOVE, vec![v]).await?;
                Ok(removed == 1)
            }
            None => self.ops().remove(&self.slot, v).await,
        }
    }

    /// Removes the values and returns whether any was there, Redisson's `removeAll`. The key goes with its last value. No values send nothing.
    #[doc(alias = "removeAll")]
    pub async fn remove_values<W>(&self, values: &[&W]) -> Result<bool>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        let values = self.ops().encode_values(values.iter().copied())?;
        remove_values(&self.slot, Shape::Set, values).await
    }

    /// Keeps only the given values, Redisson's `retainAll`; returns whether the set changed. It does not remove the key from the index.
    #[doc(alias = "retainAll")]
    pub async fn retain<W>(&self, values: &[&W]) -> Result<bool>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        let Some(timeout) = self.slot.mode.timeout() else {
            return self.set.retain(values).await;
        };
        let values = self.ops().encode_values(values.iter().copied())?;
        let changed: i64 =
            cache_eval_values(&self.slot, timeout, &scripts::SV_RETAIN_ALL, values).await?;
        Ok(changed == 1)
    }

    /// Whether the value is there.
    pub async fn contains<W>(&self, v: &W) -> Result<bool>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        let Some(timeout) = self.slot.mode.timeout() else {
            return self.set.contains(v).await;
        };
        let v = self.codec.encode(v)?;
        let found: i64 =
            cache_eval_values(&self.slot, timeout, &scripts::SV_CONTAINS, vec![v]).await?;
        Ok(found == 1)
    }

    /// Whether every one of the values is there, Redisson's `containsAll`. It is `true` for no values.
    #[doc(alias = "containsAll")]
    pub async fn contains_all<W>(&self, values: &[&W]) -> Result<bool>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        let Some(timeout) = self.slot.mode.timeout() else {
            return self.set.contains_all(values).await;
        };
        let values = self.ops().encode_values(values.iter().copied())?;
        if values.is_empty() {
            return Ok(true);
        }
        let found: i64 =
            cache_eval_values(&self.slot, timeout, &scripts::SV_CONTAINS_ALL, values).await?;
        Ok(found == 1)
    }

    /// The number of values, Redisson's `size`.
    #[doc(alias = "size")]
    pub async fn len(&self) -> Result<usize> {
        match self.slot.mode.timeout() {
            Some(timeout) => {
                cache_eval(
                    &self.slot,
                    timeout,
                    &scripts::SV_SIZE,
                    now_and_key(&self.slot),
                )
                .await
            }
            None => self.set.len().await,
        }
    }

    /// Whether the key has no values.
    #[doc(alias = "isEmpty")]
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// Every value, in no particular order, Redisson's `readAll` (`SMEMBERS`).
    #[doc(alias = "readAll")]
    pub async fn to_vec(&self) -> Result<std::vec::Vec<V>> {
        let raw: std::vec::Vec<Bytes> = match self.slot.mode.timeout() {
            Some(timeout) => {
                cache_eval(
                    &self.slot,
                    timeout,
                    &scripts::SV_READ_ALL,
                    now_and_key(&self.slot),
                )
                .await?
            }
            None => {
                self.values_key()
                    .read(self.values_key().key_cmd("SMEMBERS"))
                    .await?
            }
        };
        self.ops().decode_all(raw)
    }

    /// Removes the key with all its values, Redisson's `delete` on the handle.
    #[doc(alias = "delete")]
    pub async fn clear(&self) -> Result<()> {
        clear(&self.ops(), &self.slot).await
    }

    /// Streams the values with `SSCAN`, Redisson's `iterator`.
    #[doc(alias = "iterator")]
    #[doc(alias = "iteratorAsync")]
    pub fn iter(&self) -> impl Stream<Item = Result<V>> + '_ {
        match self.slot.mode.timeout() {
            Some(timeout) => self.cache_scan(timeout).left_stream(),
            None => self.set.iter().right_stream(),
        }
    }

    /// The values with `SV_SCAN`, `SSCAN … COUNT 10` that yields nothing for an expired key.
    fn cache_scan<'a>(&'a self, timeout: &'a str) -> impl Stream<Item = Result<V>> + 'a {
        stream::try_unfold(Some("0".to_string()), move |cursor| async move {
            let Some(cursor) = cursor else {
                return Ok::<_, Error>(None);
            };
            let args = vec![
                Bytes::from(now_millis().to_string()),
                Bytes::from(cursor),
                self.slot.encoded_key.clone(),
                Bytes::from(SET_PAGE.to_string()),
            ];
            let (next, raw): (String, std::vec::Vec<Bytes>) =
                cache_eval(&self.slot, timeout, &scripts::SV_SCAN, args).await?;
            let next = (next != "0").then_some(next);
            let values = self.ops().decode_all::<V>(raw)?;
            Ok(Some((stream::iter(values.into_iter().map(Ok)), next)))
        })
        .try_flatten()
    }
}
