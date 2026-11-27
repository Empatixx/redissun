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
