//! The values of one multimap key: `ListMultimapValues` (Redisson's `InnerList`) and `SetMultimapValues` (`InnerSet`).
//!
//! A handle is lazy: building it sends nothing. Writes that can add or remove the key go through the multimap scripts, so the key index (the hash) stays right. Reads and index writes go to the existing [`crate::Vec`] or [`crate::HashSet`] on the values key.

use super::{scripts, Mode, Ops, Shape, Slot};
use crate::codec::Codec;
use crate::collections::{hash_set::HashSet, vec::Vec};
use crate::error::Result;
use crate::object::Key;
use bytes::Bytes;
use futures::Stream;
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

/// `removeAll(values)` of `InnerList`/`InnerSet`: whether any value was removed; the key goes when its values do. No values send nothing.
async fn remove_values(slot: &Slot, shape: Shape, values: std::vec::Vec<Bytes>) -> Result<bool> {
    if values.is_empty() {
        return Ok(false);
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

/// **Experimental.** The values of one key of a [`ListMultimap`](crate::ListMultimap), Redisson's `RList` from `get(key)`. Get one with `get(&k)`.
///
/// The handle is lazy: building it sends nothing. [`push`](Self::push), [`extend`](Self::extend), [`remove_value`](Self::remove_value), [`remove_values`](Self::remove_values) and [`clear`](Self::clear) go through the multimap, so a new key joins the key index and a key whose last value goes leaves it. Reads and index writes run on the list itself. An index write that empties the list ([`pop`](Self::pop), [`remove`](Self::remove), [`trim`](Self::trim), [`retain_values`](Self::retain_values)) keeps the key in the index, as in Redisson: [`contains_key`](crate::ListMultimap::contains_key) is `false`, but `key_len` and `keys` count it until `remove_all` or `remove_many` removes it.
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
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => {
                self.ops().insert(&self.slot, v).await?;
            }
        }
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
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => {
                ops.extend(&self.slot, values).await?;
            }
        }
        Ok(())
    }

    /// The value at the index, or `None` past the end.
    pub async fn get(&self, index: usize) -> Result<Option<V>> {
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => self.list.get(index).await,
        }
    }

    /// The values in the range, as [`Vec::range`](crate::Vec::range). Redisson's inclusive `range(1, 3)` is `range(1..=3)`, and `range(2)` is `range(..=2)`.
    pub async fn range(&self, range: impl RangeBounds<usize>) -> Result<std::vec::Vec<V>> {
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => self.list.range(range).await,
        }
    }

    /// Every value in order, Redisson's `readAll`.
    #[doc(alias = "readAll")]
    pub async fn to_vec(&self) -> Result<std::vec::Vec<V>> {
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => self.list.to_vec().await,
        }
    }

    /// The number of values, Redisson's `size`.
    #[doc(alias = "size")]
    pub async fn len(&self) -> Result<usize> {
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => self.list.len().await,
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
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => self.list.position(v).await,
        }
    }

    /// The index of the last equal value, Redisson's `lastIndexOf`.
    #[doc(alias = "lastIndexOf")]
    pub async fn rposition<W>(&self, v: &W) -> Result<Option<usize>>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => self.list.rposition(v).await,
        }
    }

    /// Whether every one of the values is there, Redisson's `containsAll`. It is `true` for no values.
    #[doc(alias = "containsAll")]
    pub async fn contains_all<'a, W>(&self, values: impl IntoIterator<Item = &'a W>) -> Result<bool>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync + 'a,
    {
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => self.list.contains_all(values).await,
        }
    }

    /// Removes the first equal value through the multimap's `remove`, Redisson's `remove(value)`. The key goes with its last value.
    #[doc(alias = "remove")]
    pub async fn remove_value<W>(&self, v: &W) -> Result<bool>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        let v = self.codec.encode(v)?;
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => {
                self.ops().remove(&self.slot, v).await
            }
        }
    }

    /// Removes up to `count` equal values from the front and returns how many went, Redisson's `remove(value, count)`. It does not remove the key from the index.
    pub async fn remove_value_n<W>(&self, v: &W, count: usize) -> Result<usize>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => {
                self.list.remove_value_n(v, count).await
            }
        }
    }

    /// Removes every equal value and returns how many went. It does not remove the key from the index.
    pub async fn remove_value_all<W>(&self, v: &W) -> Result<usize>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => self.list.remove_value_all(v).await,
        }
    }

    /// Removes every occurrence of each value and returns whether any was removed, Redisson's `removeAll`. The key goes when its list is emptied. No values send nothing.
    ///
    /// As in Redisson's script, a value counts as removed only when it occurred exactly once.
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
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => {
                remove_values(&self.slot, Shape::List, values).await
            }
        }
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
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => {
                self.list.retain_values(values).await
            }
        }
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
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => {
                self.ops()
                    .remove_many(std::slice::from_ref(&self.slot))
                    .await?;
            }
        }
        Ok(())
    }

    /// Streams the values in order, in pages of 100, Redisson's `iterator`.
    #[doc(alias = "iterator")]
    pub fn iter(&self) -> impl Stream<Item = Result<V>> + '_ {
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => self.list.iter(),
        }
    }
}

/// **Experimental.** The values of one key of a [`SetMultimap`](crate::SetMultimap), Redisson's `RSet` from `get(key)`. Get one with `get(&k)`.
///
/// The handle is lazy: building it sends nothing. [`insert`](Self::insert), [`extend`](Self::extend), [`remove`](Self::remove), [`remove_values`](Self::remove_values) and [`clear`](Self::clear) go through the multimap, so a new key joins the key index and a key whose last value goes leaves it. Reads and [`retain`](Self::retain) run on the set itself; a `retain` that empties the set keeps the key in the index, as in Redisson.
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
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => {
                self.ops().insert(&self.slot, v).await
            }
        }
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
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => {
                ops.extend_count(&self.slot, values).await
            }
        }
    }

    /// Removes a value through the multimap's `remove`; returns whether it was there. The key goes with its last value.
    pub async fn remove<W>(&self, v: &W) -> Result<bool>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        let v = self.codec.encode(v)?;
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => {
                self.ops().remove(&self.slot, v).await
            }
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
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => {
                remove_values(&self.slot, Shape::Set, values).await
            }
        }
    }

    /// Keeps only the given values, Redisson's `retainAll`; returns whether the set changed. It does not remove the key from the index.
    #[doc(alias = "retainAll")]
    pub async fn retain<W>(&self, values: &[&W]) -> Result<bool>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => self.set.retain(values).await,
        }
    }

    /// Whether the value is there.
    pub async fn contains<W>(&self, v: &W) -> Result<bool>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => self.set.contains(v).await,
        }
    }

    /// Whether every one of the values is there, Redisson's `containsAll`. It is `true` for no values.
    #[doc(alias = "containsAll")]
    pub async fn contains_all<W>(&self, values: &[&W]) -> Result<bool>
    where
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync,
    {
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => self.set.contains_all(values).await,
        }
    }

    /// The number of values, Redisson's `size`.
    #[doc(alias = "size")]
    pub async fn len(&self) -> Result<usize> {
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => self.set.len().await,
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
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => {
                let raw: std::vec::Vec<Bytes> = self
                    .values_key()
                    .read(self.values_key().key_cmd("SMEMBERS"))
                    .await?;
                self.ops().decode_all(raw)
            }
        }
    }

    /// Removes the key with all its values, Redisson's `delete` on the handle.
    #[doc(alias = "delete")]
    pub async fn clear(&self) -> Result<()> {
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => {
                self.ops()
                    .remove_many(std::slice::from_ref(&self.slot))
                    .await?;
            }
        }
        Ok(())
    }

    /// Streams the values with `SSCAN`, Redisson's `iterator`.
    #[doc(alias = "iterator")]
    #[doc(alias = "iteratorAsync")]
    pub fn iter(&self) -> impl Stream<Item = Result<V>> + '_ {
        match self.slot.mode {
            Mode::Plain | Mode::Native | Mode::Cache { .. } => self.set.iter(),
        }
    }
}
