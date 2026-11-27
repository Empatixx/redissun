//! The multimaps, Redisson's `RListMultimap` and `RSetMultimap` and their variants.
//!
//! A multimap is a hash at the raw name that maps each codec-encoded key to Redisson's `Hash.hash128toBase64` of it, stored as a plain string. The values of a key live in a list or set at `suffix_name(name, hash)`. [`Shape`] picks the values type and [`Mode`] the TTL scheme; [`Ops`] runs Redisson's script or command for each pair, and [`multimap_type!`] generates the public types as thin calls into it.

pub(crate) mod scripts;

use crate::codec::Codec;
use crate::collections::map::key_hash::hash128_base64;
use crate::error::Result;
use crate::object::{suffix_name, Key};
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;

/// Which Redis type holds the values of a key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Shape {
    /// A list, Redisson's `RListMultimap`.
    List,
    /// A set, Redisson's `RSetMultimap`.
    Set,
}

/// How the keys of a multimap expire.
#[derive(Clone, Debug)]
#[expect(
    dead_code,
    reason = "the native variants (Task 7) and the cache variants (PR 3b) build the other modes"
)]
pub(crate) enum Mode {
    /// No key TTLs, Redisson's `RedissonListMultimap`/`RedissonSetMultimap`.
    Plain,
    /// Key TTLs in the zset `timeout`, Redisson's `…MultimapCache`.
    Cache { timeout: Arc<str> },
    /// Key TTLs as hash field TTLs (Redis 7.4), Redisson's `…MultimapCacheNative`.
    Native,
}

/// Redisson's `Hash.hash128toBase64` of an encoded key: the hash field value and the suffix of the values key.
pub(crate) fn key_hash(encoded_key: &[u8]) -> String {
    hash128_base64(encoded_key)
}

/// Redisson's `RedissonMultimap.prefix`: what the values names of the multimap `name` start with.
pub(crate) fn prefix(name: &str) -> String {
    suffix_name(name, "")
}

/// Redisson's `RedissonMultimap.getValuesName`: the list or set that holds the values of the key with hash `key_hash`.
pub(crate) fn values_name(name: &str, key_hash: &str) -> String {
    suffix_name(name, key_hash)
}

/// One key of a multimap: the hash, the values key and the key's encoding.
pub(crate) struct Slot {
    pub(crate) map: Key,
    pub(crate) values: Key,
    pub(crate) encoded_key: Bytes,
    pub(crate) key_hash: String,
    #[expect(dead_code, reason = "the value handles (Task 5) read the mode")]
    pub(crate) mode: Mode,
}

/// The [`Slot`] of the encoded key `encoded_key` of the multimap `key`.
pub(crate) fn slot(key: &Key, encoded_key: Bytes, mode: &Mode) -> Slot {
    let key_hash = key_hash(&encoded_key);
    Slot {
        map: key.clone(),
        values: Key::new(key.core.clone(), values_name(key.name(), &key_hash)),
        encoded_key,
        key_hash,
        mode: mode.clone(),
    }
}

/// The operations of a multimap, on encoded keys and values. The public types decode what they return.
pub(crate) struct Ops<'a, C> {
    pub(crate) key: &'a Key,
    pub(crate) codec: &'a C,
    pub(crate) shape: Shape,
    pub(crate) mode: &'a Mode,
}

impl<C: Codec> Ops<'_, C> {
    /// The slot of the key `k`, encoded with the codec.
    pub(crate) fn slot<Q: Serialize + ?Sized>(&self, k: &Q) -> Result<Slot> {
        Ok(slot(self.key, self.codec.encode(k)?, self.mode))
    }

    /// Decodes the values of a reply.
    pub(crate) fn decode_all<V: DeserializeOwned>(&self, raw: Vec<Bytes>) -> Result<Vec<V>> {
        raw.iter().map(|bytes| self.codec.decode(bytes)).collect()
    }

    fn slot_keys(slot: &Slot) -> Vec<String> {
        vec![slot.map.redis_key(), slot.values.redis_key()]
    }

    /// `put`: list always `true`, set `true` when the value is new.
    pub(crate) async fn insert(&self, slot: &Slot, value: Bytes) -> Result<bool> {
        let args = vec![
            slot.encoded_key.clone(),
            Bytes::from(slot.key_hash.clone()),
            value,
        ];
        let core = &self.key.core;
        let added: i64 = match self.shape {
            Shape::List => {
                core.eval_once(&scripts::LIST_PUT, Self::slot_keys(slot), args)
                    .await?
            }
            Shape::Set => {
                core.eval(&scripts::SET_PUT, Self::slot_keys(slot), args)
                    .await?
            }
        };
        Ok(added > 0)
    }

    /// `getAll`: `LRANGE v 0 -1` or `SMEMBERS v`.
    pub(crate) async fn get_all(&self, slot: &Slot) -> Result<Vec<Bytes>> {
        let cmd = match self.shape {
            Shape::List => {
                let mut cmd = slot.values.key_cmd("LRANGE");
                cmd.arg(0).arg(-1);
                cmd
            }
            Shape::Set => slot.values.key_cmd("SMEMBERS"),
        };
        slot.values.read(cmd).await
    }

    /// `containsKey`: the values key holds anything (`LLEN`/`SCARD` > 0).
    pub(crate) async fn contains_key(&self, slot: &Slot) -> Result<bool> {
        let cmd = match self.shape {
            Shape::List => slot.values.key_cmd("LLEN"),
            Shape::Set => slot.values.key_cmd("SCARD"),
        };
        let len: i64 = slot.values.read(cmd).await?;
        Ok(len > 0)
    }

    /// `size`: the values of every key.
    pub(crate) async fn len(&self) -> Result<usize> {
        let script = match self.shape {
            Shape::List => &scripts::LIST_LEN,
            Shape::Set => &scripts::SET_LEN,
        };
        let len: usize = self
            .key
            .core
            .eval(
                script,
                vec![self.key.redis_key()],
                vec![Bytes::from(prefix(self.key.name()))],
            )
            .await?;
        Ok(len)
    }

    /// `keySize`: `HLEN`.
    pub(crate) async fn key_len(&self) -> Result<usize> {
        self.key.read(self.key.key_cmd("HLEN")).await
    }

    /// Encodes values with the codec, before any request is sent.
    pub(crate) fn encode_values<'v, W>(
        &self,
        values: impl IntoIterator<Item = &'v W>,
    ) -> Result<Vec<Bytes>>
    where
        W: Serialize + ?Sized + 'v,
    {
        values.into_iter().map(|v| self.codec.encode(v)).collect()
    }

    fn prefix_arg(&self) -> Bytes {
        Bytes::from(prefix(self.key.name()))
    }

    /// ARGV of the scripts that write a key: the key, its hash, then the values.
    fn key_and_values(slot: &Slot, values: Vec<Bytes>) -> Vec<Bytes> {
        [slot.encoded_key.clone(), Bytes::from(slot.key_hash.clone())]
            .into_iter()
            .chain(values)
            .collect()
    }

    /// `putAll(K, Iterable)`: whether any value was added. No values send nothing, so no key is left without values.
    pub(crate) async fn extend(&self, slot: &Slot, values: Vec<Bytes>) -> Result<bool> {
        if values.is_empty() {
            return Ok(false);
        }
        let core = &self.key.core;
        let keys = Self::slot_keys(slot);
        let args = Self::key_and_values(slot, values);
        let added: i64 = match self.shape {
            Shape::List => core.eval_once(&scripts::LIST_PUT_ALL, keys, args).await?,
            Shape::Set => core.eval(&scripts::SET_PUT_ALL, keys, args).await?,
        };
        Ok(added > 0)
    }

    /// `remove(K, V)`: whether the value was there; the key goes with its last value.
    pub(crate) async fn remove(&self, slot: &Slot, value: Bytes) -> Result<bool> {
        let script = match self.shape {
            Shape::List => &scripts::LIST_REMOVE,
            Shape::Set => &scripts::SET_REMOVE,
        };
        let removed: i64 = self
            .key
            .core
            .eval(
                script,
                Self::slot_keys(slot),
                vec![slot.encoded_key.clone(), value],
            )
            .await?;
        Ok(removed > 0)
    }

    /// `removeAll(K)`: the values the key held.
    pub(crate) async fn remove_all(&self, slot: &Slot) -> Result<Vec<Bytes>> {
        let script = match self.shape {
            Shape::List => &scripts::LIST_REMOVE_ALL,
            Shape::Set => &scripts::SET_REMOVE_ALL,
        };
        self.key
            .core
            .eval(
                script,
                Self::slot_keys(slot),
                vec![slot.encoded_key.clone()],
            )
            .await
    }

    /// `replaceValues`: the old values. No new values remove the key, as `remove_all`.
    pub(crate) async fn replace_values(
        &self,
        slot: &Slot,
        values: Vec<Bytes>,
    ) -> Result<Vec<Bytes>> {
        if values.is_empty() {
            return self.remove_all(slot).await;
        }
        let core = &self.key.core;
        let keys = Self::slot_keys(slot);
        let args = Self::key_and_values(slot, values);
        match self.shape {
            Shape::List => {
                core.eval_once(&scripts::LIST_REPLACE_VALUES, keys, args)
                    .await
            }
            Shape::Set => core.eval(&scripts::SET_REPLACE_VALUES, keys, args).await,
        }
    }

    /// `fastReplaceValues`. No new values remove the key, as `remove_many`.
    pub(crate) async fn set_values(&self, slot: &Slot, values: Vec<Bytes>) -> Result<()> {
        if values.is_empty() {
            self.remove_many(std::slice::from_ref(slot)).await?;
            return Ok(());
        }
        let core = &self.key.core;
        let keys = Self::slot_keys(slot);
        let args = Self::key_and_values(slot, values);
        let () = match self.shape {
            Shape::List => {
                core.eval_once(&scripts::LIST_FAST_REPLACE_VALUES, keys, args)
                    .await?
            }
            Shape::Set => {
                core.eval(&scripts::SET_FAST_REPLACE_VALUES, keys, args)
                    .await?
            }
        };
        Ok(())
    }

    /// `fastRemove(K...)`: how many of the keys were there. No keys send nothing.
    pub(crate) async fn remove_many(&self, slots: &[Slot]) -> Result<usize> {
        if slots.is_empty() {
            return Ok(0);
        }
        let keys = std::iter::once(self.key.redis_key())
            .chain(slots.iter().map(|slot| slot.values.redis_key()))
            .collect();
        let args = slots.iter().map(|slot| slot.encoded_key.clone()).collect();
        self.key.core.eval(&scripts::FAST_REMOVE, keys, args).await
    }

    /// `fastRemoveValue(V...)`: how many values were removed over all keys. No values send nothing.
    pub(crate) async fn remove_from_all_keys(&self, values: Vec<Bytes>) -> Result<usize> {
        if values.is_empty() {
            return Ok(0);
        }
        let script = match self.shape {
            Shape::List => &scripts::LIST_FAST_REMOVE_VALUE,
            Shape::Set => &scripts::SET_FAST_REMOVE_VALUE,
        };
        let args = std::iter::once(self.prefix_arg()).chain(values).collect();
        self.key
            .core
            .eval(script, vec![self.key.redis_key()], args)
            .await
    }

    /// `containsValue`: whether any key holds the value.
    pub(crate) async fn contains_value(&self, value: Bytes) -> Result<bool> {
        let script = match self.shape {
            Shape::List => &scripts::LIST_CONTAINS_VALUE,
            Shape::Set => &scripts::SET_CONTAINS_VALUE,
        };
        let found: i64 = self
            .key
            .core
            .eval(
                script,
                vec![self.key.redis_key()],
                vec![value, self.prefix_arg()],
            )
            .await?;
        Ok(found == 1)
    }

    /// `containsEntry`: the list script, or `SISMEMBER` on the values key.
    pub(crate) async fn contains_entry(&self, slot: &Slot, value: Bytes) -> Result<bool> {
        let found: i64 = match self.shape {
            Shape::List => {
                self.key
                    .core
                    .eval(
                        &scripts::LIST_CONTAINS_ENTRY,
                        vec![slot.values.redis_key()],
                        vec![value],
                    )
                    .await?
            }
            Shape::Set => {
                let mut cmd = slot.values.key_cmd("SISMEMBER");
                cmd.arg(&value[..]);
                slot.values.read(cmd).await?
            }
        };
        Ok(found == 1)
    }

    /// `readAllKeySet`: `HKEYS`.
    pub(crate) async fn keys_to_vec(&self) -> Result<Vec<Bytes>> {
        self.key.read(self.key.key_cmd("HKEYS")).await
    }

    /// `delete`: the hash and every values key; whether any key was deleted.
    pub(crate) async fn delete(&self) -> Result<bool> {
        let deleted: i64 = self
            .key
            .core
            .eval(
                &scripts::DELETE,
                vec![self.key.redis_key()],
                vec![self.prefix_arg()],
            )
            .await?;
        Ok(deleted > 0)
    }
}

/// Generates a public multimap type: the struct, `Clone`, `Debug` and its methods as calls into [`Ops`].
macro_rules! multimap_type {
    ($(#[$meta:meta])* $name:ident, $values:ident, $shape:expr, $redisson_getter:literal) => {
        $(#[$meta])*
        pub struct $name<K, V, C: $crate::codec::Codec> {
            key: $crate::object::Key,
            codec: C,
            mode: $crate::collections::multimap::Mode,
            _marker: ::std::marker::PhantomData<fn() -> (K, V)>,
        }

        impl<K, V, C: $crate::codec::Codec> ::std::clone::Clone for $name<K, V, C> {
            fn clone(&self) -> Self {
                Self {
                    key: self.key.clone(),
                    codec: self.codec.clone(),
                    mode: self.mode.clone(),
                    _marker: ::std::marker::PhantomData,
                }
            }
        }

        impl<K, V, C: $crate::codec::Codec> ::std::fmt::Debug for $name<K, V, C> {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                self.key.describe(f, stringify!($name))
            }
        }

        impl<K, V, C: $crate::codec::Codec> $name<K, V, C> {
            pub(crate) fn new(
                key: $crate::object::Key,
                codec: C,
                mode: $crate::collections::multimap::Mode,
            ) -> Self {
                Self {
                    key,
                    codec,
                    mode,
                    _marker: ::std::marker::PhantomData,
                }
            }

            fn ops(&self) -> $crate::collections::multimap::Ops<'_, C> {
                $crate::collections::multimap::Ops {
                    key: &self.key,
                    codec: &self.codec,
                    shape: $shape,
                    mode: &self.mode,
                }
            }
        }

        impl<K, V, C> $name<K, V, C>
        where
            K: ::serde::Serialize + ::serde::de::DeserializeOwned + Send + Sync,
            V: ::serde::Serialize + ::serde::de::DeserializeOwned + Send + Sync,
            C: $crate::codec::Codec,
        {
            /// Adds the value `v` to the key `k`, Redisson's `put`. A `ListMultimap` appends it and returns `true`; a `SetMultimap` returns whether the value was new for the key.
            #[doc(alias = "put")]
            pub async fn insert<Q, W>(&self, k: &Q, v: &W) -> $crate::error::Result<bool>
            where
                K: ::std::borrow::Borrow<Q>,
                V: ::std::borrow::Borrow<W>,
                Q: ::serde::Serialize + ?Sized + Sync,
                W: ::serde::Serialize + ?Sized + Sync,
            {
                let ops = self.ops();
                let slot = ops.slot(k)?;
                let v = self.codec.encode(v)?;
                ops.insert(&slot, v).await
            }

            /// The values of the key `k`, empty when it has none, Redisson's `getAll`. A `ListMultimap` returns them in insertion order, a `SetMultimap` in no particular order.
            #[doc(alias = "getAll")]
            pub async fn get_all<Q>(&self, k: &Q) -> $crate::error::Result<::std::vec::Vec<V>>
            where
                K: ::std::borrow::Borrow<Q>,
                Q: ::serde::Serialize + ?Sized + Sync,
            {
                let ops = self.ops();
                let slot = ops.slot(k)?;
                let raw = ops.get_all(&slot).await?;
                ops.decode_all(raw)
            }

            /// Whether the key `k` has any values, Redisson's `containsKey`.
            #[doc(alias = "containsKey")]
            pub async fn contains_key<Q>(&self, k: &Q) -> $crate::error::Result<bool>
            where
                K: ::std::borrow::Borrow<Q>,
                Q: ::serde::Serialize + ?Sized + Sync,
            {
                let ops = self.ops();
                let slot = ops.slot(k)?;
                ops.contains_key(&slot).await
            }

            /// The number of values under all keys, Redisson's `size`.
            #[doc(alias = "size")]
            pub async fn len(&self) -> $crate::error::Result<usize> {
                self.ops().len().await
            }

            /// The number of keys, Redisson's `keySize` (`HLEN`).
            #[doc(alias = "keySize")]
            pub async fn key_len(&self) -> $crate::error::Result<usize> {
                self.ops().key_len().await
            }

            /// Whether the multimap holds no values, Redisson's `isEmpty` (`len() == 0`).
            #[doc(alias = "isEmpty")]
            pub async fn is_empty(&self) -> $crate::error::Result<bool> {
                Ok(self.len().await? == 0)
            }

            /// Adds every value to the key `k` in one round trip, Redisson's `putAll(key, values)`. Returns whether any value was added: always `true` on a `ListMultimap`, and on a `SetMultimap` when a value was new for the key. No values send nothing and return `false`.
            #[doc(alias = "putAll")]
            pub async fn extend<'a, Q, W>(
                &self,
                k: &Q,
                values: impl IntoIterator<Item = &'a W>,
            ) -> $crate::error::Result<bool>
            where
                K: ::std::borrow::Borrow<Q>,
                V: ::std::borrow::Borrow<W>,
                Q: ::serde::Serialize + ?Sized + Sync,
                W: ::serde::Serialize + ?Sized + Sync + 'a,
            {
                let ops = self.ops();
                let slot = ops.slot(k)?;
                let values = ops.encode_values(values)?;
                ops.extend(&slot, values).await
            }

            /// Removes one occurrence of the value `v` from the key `k`, Redisson's `remove(key, value)`. Returns whether it was there. The key goes when its last value does.
            pub async fn remove<Q, W>(&self, k: &Q, v: &W) -> $crate::error::Result<bool>
            where
                K: ::std::borrow::Borrow<Q>,
                V: ::std::borrow::Borrow<W>,
                Q: ::serde::Serialize + ?Sized + Sync,
                W: ::serde::Serialize + ?Sized + Sync,
            {
                let ops = self.ops();
                let slot = ops.slot(k)?;
                let v = self.codec.encode(v)?;
                ops.remove(&slot, v).await
            }

            /// Removes the key `k` and returns the values it held, Redisson's `removeAll(key)`.
            #[doc(alias = "removeAll")]
            pub async fn remove_all<Q>(&self, k: &Q) -> $crate::error::Result<::std::vec::Vec<V>>
            where
                K: ::std::borrow::Borrow<Q>,
                Q: ::serde::Serialize + ?Sized + Sync,
            {
                let ops = self.ops();
                let slot = ops.slot(k)?;
                let raw = ops.remove_all(&slot).await?;
                ops.decode_all(raw)
            }

            /// Replaces the values of the key `k` and returns the old ones, Redisson's `replaceValues`. No new values remove the key, as [`remove_all`](Self::remove_all) does.
            #[doc(alias = "replaceValues")]
            pub async fn replace_values<'a, Q, W>(
                &self,
                k: &Q,
                values: impl IntoIterator<Item = &'a W>,
            ) -> $crate::error::Result<::std::vec::Vec<V>>
            where
                K: ::std::borrow::Borrow<Q>,
                V: ::std::borrow::Borrow<W>,
                Q: ::serde::Serialize + ?Sized + Sync,
                W: ::serde::Serialize + ?Sized + Sync + 'a,
            {
                let ops = self.ops();
                let slot = ops.slot(k)?;
                let values = ops.encode_values(values)?;
                let raw = ops.replace_values(&slot, values).await?;
                ops.decode_all(raw)
            }

            /// Replaces the values of the key `k` without reading the old ones, Redisson's `fastReplaceValues`. No new values remove the key.
            #[doc(alias = "fastReplaceValues")]
            pub async fn set_values<'a, Q, W>(
                &self,
                k: &Q,
                values: impl IntoIterator<Item = &'a W>,
            ) -> $crate::error::Result<()>
            where
                K: ::std::borrow::Borrow<Q>,
                V: ::std::borrow::Borrow<W>,
                Q: ::serde::Serialize + ?Sized + Sync,
                W: ::serde::Serialize + ?Sized + Sync + 'a,
            {
                let ops = self.ops();
                let slot = ops.slot(k)?;
                let values = ops.encode_values(values)?;
                ops.set_values(&slot, values).await
            }

            /// Removes the keys with all their values and returns how many of them were there, Redisson's `fastRemove(keys...)`. No keys send nothing.
            #[doc(alias = "fastRemove")]
            pub async fn remove_many<Q>(&self, ks: &[&Q]) -> $crate::error::Result<usize>
            where
                K: ::std::borrow::Borrow<Q>,
                Q: ::serde::Serialize + ?Sized + Sync,
            {
                let ops = self.ops();
                let slots = ks
                    .iter()
                    .map(|k| ops.slot(*k))
                    .collect::<$crate::error::Result<::std::vec::Vec<_>>>()?;
                ops.remove_many(&slots).await
            }

            /// Removes each value from every key and returns how many values were removed, Redisson's `fastRemoveValue(values...)`. A `ListMultimap` removes one occurrence per key and keeps a key whose list it emptied in the hash, as Redisson does; a `SetMultimap` removes such a key. No values send nothing.
            #[doc(alias = "fastRemoveValue")]
            pub async fn remove_from_all_keys<W>(&self, values: &[&W]) -> $crate::error::Result<usize>
            where
                V: ::std::borrow::Borrow<W>,
                W: ::serde::Serialize + ?Sized + Sync,
            {
                let ops = self.ops();
                let values = ops.encode_values(values.iter().copied())?;
                ops.remove_from_all_keys(values).await
            }

            /// Whether any key holds the value `v`, Redisson's `containsValue`.
            #[doc(alias = "containsValue")]
            pub async fn contains_value<W>(&self, v: &W) -> $crate::error::Result<bool>
            where
                V: ::std::borrow::Borrow<W>,
                W: ::serde::Serialize + ?Sized + Sync,
            {
                let v = self.codec.encode(v)?;
                self.ops().contains_value(v).await
            }

            /// Whether the key `k` holds the value `v`, Redisson's `containsEntry`.
            #[doc(alias = "containsEntry")]
            pub async fn contains_entry<Q, W>(&self, k: &Q, v: &W) -> $crate::error::Result<bool>
            where
                K: ::std::borrow::Borrow<Q>,
                V: ::std::borrow::Borrow<W>,
                Q: ::serde::Serialize + ?Sized + Sync,
                W: ::serde::Serialize + ?Sized + Sync,
            {
                let ops = self.ops();
                let slot = ops.slot(k)?;
                let v = self.codec.encode(v)?;
                ops.contains_entry(&slot, v).await
            }

            /// Every key, Redisson's `readAllKeySet` (`HKEYS`).
            #[doc(alias = "readAllKeySet")]
            pub async fn keys_to_vec(&self) -> $crate::error::Result<::std::vec::Vec<K>> {
                let ops = self.ops();
                let raw = ops.keys_to_vec().await?;
                ops.decode_all(raw)
            }

            /// Deletes the hash and the values of every key, Redisson's `clear`.
            pub async fn clear(&self) -> $crate::error::Result<()> {
                self.ops().delete().await?;
                Ok(())
            }

            $crate::collections::map::key_hash::per_key_objects!();
        }
    };
}

multimap_type!(
    /// **Experimental.** A distributed multimap whose keys each hold a list of values, Redisson's `RListMultimap`. Values keep their insertion order, and a key can hold the same value more than once.
    ///
    /// It uses Redisson's layout, so Redisson clients read and write the same data: a hash at the name maps each encoded key to a hash of it, and the values of a key live in the list `{name}:<hash>`. Get one with [`Client::list_multimap`](crate::Client::list_multimap).
    ListMultimap,
    ListMultimapValues,
    Shape::List,
    "getListMultimap"
);

multimap_type!(
    /// **Experimental.** A distributed multimap whose keys each hold a set of values, Redisson's `RSetMultimap`. A key holds each value at most once; values are equal when their encoded bytes are equal.
    ///
    /// It uses Redisson's layout, so Redisson clients read and write the same data: a hash at the name maps each encoded key to a hash of it, and the values of a key live in the set `{name}:<hash>`. Get one with [`Client::set_multimap`](crate::Client::set_multimap).
    SetMultimap,
    SetMultimapValues,
    Shape::Set,
    "getSetMultimap"
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_name_follows_redisson() {
        let hash = key_hash(b"\"key\"");
        assert_eq!(hash, "IKm9EBQhlzpVIBxUXDHBWw");
        assert_eq!(values_name("m", &hash), "{m}:IKm9EBQhlzpVIBxUXDHBWw");
        assert_eq!(values_name("{t}:m", &hash), "{t}:m:IKm9EBQhlzpVIBxUXDHBWw");
        assert_eq!(prefix("m"), "{m}:");
        assert_eq!(prefix("{t}:m"), "{t}:m:");
    }
}
