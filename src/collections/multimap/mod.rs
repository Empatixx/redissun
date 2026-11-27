//! The multimaps, Redisson's `RListMultimap` and `RSetMultimap` and their variants.
//!
//! A multimap is a hash at the raw name that maps each codec-encoded key to Redisson's `Hash.hash128toBase64` of it, stored as a plain string. The values of a key live in a list or set at `suffix_name(name, hash)`. [`Shape`] picks the values type and [`Mode`] the TTL scheme; [`Ops`] runs Redisson's script or command for each pair, and [`multimap_type!`] generates the public types as thin calls into it.

pub(crate) mod events;
pub(crate) mod scripts;
pub(crate) mod stream;
pub(crate) mod values;

use crate::codec::Codec;
use crate::collections::map::key_hash::hash128_base64;
use crate::collections::map_cache_native::support::{self, Needs};
use crate::error::{Error, Result};
use crate::eviction::{self, KEYS_LIMIT};
use crate::lua::Lua;
use crate::object::{millis, now_millis, suffix_name, Key};
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;

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
pub(crate) enum Mode {
    /// No key TTLs, Redisson's `RedissonListMultimap`/`RedissonSetMultimap`.
    Plain,
    /// Key TTLs in the zset `timeout`, Redisson's `…MultimapCache`.
    Cache { timeout: Arc<str> },
    /// Key TTLs as hash field TTLs (Redis 7.4), Redisson's `…MultimapCacheNative`.
    Native,
}

impl Mode {
    /// The cache mode of the multimap `name`: its ttl zset is Redisson's `getTimeoutSetName`.
    pub(crate) fn cache(name: &str, shape: Shape) -> Self {
        Self::Cache {
            timeout: ttl_name(name, shape).into(),
        }
    }

    /// The ttl zset in cache mode.
    pub(crate) fn timeout(&self) -> Option<&str> {
        match self {
            Self::Cache { timeout } => Some(timeout),
            Self::Plain | Self::Native => None,
        }
    }
}

/// Redisson's `getTimeoutSetName` of the cache variants: `suffix_name(name, "redissun_list_multimap_ttl")` or `"redissun_set_multimap_ttl"`.
pub(crate) fn ttl_name(name: &str, shape: Shape) -> String {
    let suffix = match shape {
        Shape::List => "redissun_list_multimap_ttl",
        Shape::Set => "redissun_set_multimap_ttl",
    };
    suffix_name(name, suffix)
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
#[derive(Clone)]
pub(crate) struct Slot {
    pub(crate) map: Key,
    pub(crate) values: Key,
    pub(crate) encoded_key: Bytes,
    pub(crate) key_hash: String,
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

type ScriptCall = (&'static Lua, Vec<String>, Vec<Bytes>);

/// The arguments of one clean-up run: the client clock and the most keys to delete, as Redisson's `MultimapEvictionTask` (which has no latch).
fn eviction_arguments(_: Option<Duration>) -> Vec<Bytes> {
    vec![
        Bytes::from(now_millis().to_string()),
        Bytes::from(KEYS_LIMIT.to_string()),
    ]
}

/// Schedules the clean-up task of a cache multimap, Redisson's `scheduleCleanMultimap`, unless it already runs in this client. Other modes have none.
pub(crate) fn schedule_clean_up(key: &Key, mode: &Mode) {
    if let Some(timeout) = mode.timeout() {
        eviction::schedule(
            &key.core,
            format!("redissun__multimap_eviction:{}", key.name()),
            &scripts::CACHE_EVICT,
            vec![key.redis_key(), timeout.to_string()],
            eviction_arguments,
            Vec::new(),
        );
    }
}

/// `Object::del`: Redisson's `deleteAsync` over the hash and every values key, and the ttl zset in cache mode.
pub(crate) fn del_script(key: &Key, mode: &Mode) -> ScriptCall {
    let args = vec![Bytes::from(prefix(key.name()))];
    match mode.timeout() {
        Some(timeout) => (
            &scripts::CACHE_DELETE,
            vec![key.redis_key(), timeout.to_string()],
            args,
        ),
        None => (&scripts::DELETE, vec![key.redis_key()], args),
    }
}

/// The keys `Object::expire` and `persist` cover besides the hash and the values keys: the ttl zset in cache mode.
pub(crate) fn companions(mode: &Mode) -> Vec<String> {
    mode.timeout().map(str::to_string).into_iter().collect()
}

/// `Object::expire`: Redisson's `expireAsync`, of `RedissonMultimapCache` in cache mode.
pub(crate) fn expire_script(mode: &Mode) -> &'static Lua {
    match mode {
        Mode::Cache { .. } => &scripts::CACHE_EXPIRE,
        Mode::Plain | Mode::Native => &scripts::EXPIRE,
    }
}

/// `Object::persist`: Redisson's `clearExpireAsync`, of `RedissonMultimapCache` in cache mode.
pub(crate) fn persist_script(mode: &Mode) -> &'static Lua {
    match mode {
        Mode::Cache { .. } => &scripts::CACHE_CLEAR_EXPIRE,
        Mode::Plain | Mode::Native => &scripts::CLEAR_EXPIRE,
    }
}

/// The arguments of `EXPIRE` after the milliseconds: the prefix and no `PEXPIRE` option.
pub(crate) fn expire_args(key: &Key) -> Vec<Bytes> {
    vec![Bytes::from(prefix(key.name())), Bytes::new()]
}

/// The arguments of `CLEAR_EXPIRE`: the prefix.
pub(crate) fn persist_args(key: &Key) -> Vec<Bytes> {
    vec![Bytes::from(prefix(key.name()))]
}

/// `Object::rename`: Redisson's `renameAsync`, which renames the hash and every values key, and in cache mode the ttl zset too (D4).
pub(crate) fn rename_script(key: &Key, mode: &Mode, shape: Shape, new_name: &str) -> ScriptCall {
    let args = vec![
        Bytes::from(prefix(key.name())),
        Bytes::from(prefix(new_name)),
        Bytes::from(new_name.to_string()),
    ];
    match mode.timeout() {
        Some(timeout) => (
            &scripts::CACHE_RENAME,
            vec![key.redis_key(), timeout.to_string()],
            args.into_iter()
                .chain(std::iter::once(Bytes::from(ttl_name(new_name, shape))))
                .collect(),
        ),
        None => (&scripts::RENAME, vec![key.redis_key()], args),
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

    /// The hash, the values key and, in cache mode, the ttl zset: the KEYS of the write scripts.
    fn write_keys(&self, slot: &Slot) -> Vec<String> {
        let mut keys = Self::slot_keys(slot);
        keys.extend(self.mode.timeout().map(str::to_string));
        keys
    }

    /// The client clock, Redisson's `System.currentTimeMillis()`, as a script argument.
    fn now_arg() -> Bytes {
        Bytes::from(now_millis().to_string())
    }

    /// `put`: list always `true`, set `true` when the value is new.
    pub(crate) async fn insert(&self, slot: &Slot, value: Bytes) -> Result<bool> {
        let args = vec![
            slot.encoded_key.clone(),
            Bytes::from(slot.key_hash.clone()),
            value,
        ];
        let core = &self.key.core;
        let keys = self.write_keys(slot);
        let cache = self.mode.timeout().is_some();
        let added: i64 = match (self.shape, cache) {
            (Shape::List, false) => core.eval_once(&scripts::LIST_PUT, keys, args).await?,
            (Shape::List, true) => core.eval_once(&scripts::CACHE_LIST_PUT, keys, args).await?,
            (Shape::Set, false) => core.eval(&scripts::SET_PUT, keys, args).await?,
            (Shape::Set, true) => core.eval(&scripts::CACHE_SET_PUT, keys, args).await?,
        };
        Ok(added > 0)
    }

    /// `getAll`: `LRANGE v 0 -1` or `SMEMBERS v`; in cache mode Redisson's script, empty for an expired key.
    pub(crate) async fn get_all(&self, slot: &Slot) -> Result<Vec<Bytes>> {
        if let Some(timeout) = self.mode.timeout() {
            let script = match self.shape {
                Shape::List => &scripts::CACHE_LIST_GET_ALL,
                Shape::Set => &scripts::CACHE_SET_GET_ALL,
            };
            return self
                .key
                .core
                .eval(
                    script,
                    vec![slot.values.redis_key(), timeout.to_string()],
                    vec![Self::now_arg(), slot.encoded_key.clone()],
                )
                .await;
        }
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

    /// `containsKey`: the values key holds anything (`LLEN`/`SCARD` > 0); in cache mode also that the key has not expired.
    pub(crate) async fn contains_key(&self, slot: &Slot) -> Result<bool> {
        if let Some(timeout) = self.mode.timeout() {
            let script = match self.shape {
                Shape::List => &scripts::CACHE_LIST_CONTAINS_KEY,
                Shape::Set => &scripts::CACHE_SET_CONTAINS_KEY,
            };
            let found: i64 = self
                .key
                .core
                .eval(
                    script,
                    vec![self.key.redis_key(), timeout.to_string()],
                    vec![
                        Self::now_arg(),
                        slot.encoded_key.clone(),
                        Bytes::from(slot.values.redis_key()),
                    ],
                )
                .await?;
            return Ok(found == 1);
        }
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
        Ok(self.extend_count(slot, values).await? > 0)
    }

    /// `putAll(K, Iterable)` with the script's count: the list length after `RPUSH`, or the values `SADD` added. No values send nothing.
    pub(crate) async fn extend_count(&self, slot: &Slot, values: Vec<Bytes>) -> Result<usize> {
        if values.is_empty() {
            return Ok(0);
        }
        let core = &self.key.core;
        let keys = self.write_keys(slot);
        let args = Self::key_and_values(slot, values);
        match (self.shape, self.mode) {
            (Shape::List, Mode::Plain) => core.eval_once(&scripts::LIST_PUT_ALL, keys, args).await,
            (Shape::List, Mode::Native) => {
                core.eval_once(&scripts::NATIVE_LIST_PUT_ALL, keys, args)
                    .await
            }
            (Shape::List, Mode::Cache { .. }) => {
                core.eval_once(&scripts::CACHE_LIST_PUT_ALL, keys, args)
                    .await
            }
            (Shape::Set, Mode::Plain) => core.eval(&scripts::SET_PUT_ALL, keys, args).await,
            (Shape::Set, Mode::Native) => core.eval(&scripts::NATIVE_SET_PUT_ALL, keys, args).await,
            (Shape::Set, Mode::Cache { .. }) => {
                core.eval(&scripts::CACHE_SET_PUT_ALL, keys, args).await
            }
        }
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

    /// `removeAll(K)`: the values the key held; in cache mode its score goes too.
    pub(crate) async fn remove_all(&self, slot: &Slot) -> Result<Vec<Bytes>> {
        let cache = self.mode.timeout().is_some();
        let script = match (self.shape, cache) {
            (Shape::List, false) => &scripts::LIST_REMOVE_ALL,
            (Shape::List, true) => &scripts::CACHE_LIST_REMOVE_ALL,
            (Shape::Set, false) => &scripts::SET_REMOVE_ALL,
            (Shape::Set, true) => &scripts::CACHE_SET_REMOVE_ALL,
        };
        self.key
            .core
            .eval(
                script,
                self.write_keys(slot),
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
        let keys = self.write_keys(slot);
        let args = Self::key_and_values(slot, values);
        let cache = self.mode.timeout().is_some();
        match (self.shape, cache) {
            (Shape::List, false) => {
                core.eval_once(&scripts::LIST_REPLACE_VALUES, keys, args)
                    .await
            }
            (Shape::List, true) => {
                core.eval_once(&scripts::CACHE_LIST_REPLACE_VALUES, keys, args)
                    .await
            }
            (Shape::Set, false) => core.eval(&scripts::SET_REPLACE_VALUES, keys, args).await,
            (Shape::Set, true) => {
                core.eval(&scripts::CACHE_SET_REPLACE_VALUES, keys, args)
                    .await
            }
        }
    }

    /// `fastReplaceValues`. No new values remove the key, as `remove_many`.
    pub(crate) async fn set_values(&self, slot: &Slot, values: Vec<Bytes>) -> Result<()> {
        if values.is_empty() {
            self.remove_many(std::slice::from_ref(slot)).await?;
            return Ok(());
        }
        let core = &self.key.core;
        let keys = self.write_keys(slot);
        let args = Self::key_and_values(slot, values);
        let cache = self.mode.timeout().is_some();
        let () = match (self.shape, cache) {
            (Shape::List, false) => {
                core.eval_once(&scripts::LIST_FAST_REPLACE_VALUES, keys, args)
                    .await?
            }
            (Shape::List, true) => {
                core.eval_once(&scripts::CACHE_LIST_FAST_REPLACE_VALUES, keys, args)
                    .await?
            }
            (Shape::Set, false) => {
                core.eval(&scripts::SET_FAST_REPLACE_VALUES, keys, args)
                    .await?
            }
            (Shape::Set, true) => {
                core.eval(&scripts::CACHE_SET_FAST_REPLACE_VALUES, keys, args)
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

    /// `containsValue`: whether any key holds the value; in cache mode expired keys are skipped.
    pub(crate) async fn contains_value(&self, value: Bytes) -> Result<bool> {
        if let Some(timeout) = self.mode.timeout() {
            let script = match self.shape {
                Shape::List => &scripts::CACHE_LIST_CONTAINS_VALUE,
                Shape::Set => &scripts::CACHE_SET_CONTAINS_VALUE,
            };
            let found: i64 = self
                .key
                .core
                .eval(
                    script,
                    vec![self.key.redis_key(), timeout.to_string()],
                    vec![value, Self::now_arg(), self.prefix_arg()],
                )
                .await?;
            return Ok(found == 1);
        }
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

    /// `containsEntry`: the list script, or `SISMEMBER` on the values key; in cache mode Redisson's script, `false` for an expired key.
    pub(crate) async fn contains_entry(&self, slot: &Slot, value: Bytes) -> Result<bool> {
        if let Some(timeout) = self.mode.timeout() {
            let script = match self.shape {
                Shape::List => &scripts::CACHE_LIST_CONTAINS_ENTRY,
                Shape::Set => &scripts::CACHE_SET_CONTAINS_ENTRY,
            };
            let found: i64 = self
                .key
                .core
                .eval(
                    script,
                    vec![slot.values.redis_key(), timeout.to_string()],
                    vec![Self::now_arg(), slot.encoded_key.clone(), value],
                )
                .await?;
            return Ok(found == 1);
        }
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

    /// `expireKey`. Whether the key exists.
    ///
    /// In cache mode it records the deadline `now + ttl` (client clock) as the key's score in the ttl zset, or drops the score for a zero TTL. In native mode it sets the TTL of the key's field and values key (`HPEXPIRE` + `PEXPIRE`), or clears both for a zero TTL; that needs Redis 7.4, checked before the write, and `type_name` names the type in the error.
    pub(crate) async fn expire_key(
        &self,
        slot: &Slot,
        ttl: Duration,
        type_name: &str,
    ) -> Result<bool> {
        if let Some(timeout) = self.mode.timeout() {
            let deadline = if ttl.is_zero() {
                0
            } else {
                now_millis().saturating_add(millis(ttl)?)
            };
            let done: i64 = self
                .key
                .core
                .eval(
                    &scripts::CACHE_EXPIRE_KEY,
                    vec![self.key.redis_key(), timeout.to_string()],
                    vec![Bytes::from(deadline.to_string()), slot.encoded_key.clone()],
                )
                .await?;
            return Ok(done == 1);
        }
        support::require(self.key, Needs::FieldTtl)
            .await
            .map_err(|error| match error {
                Error::Unsupported(_) => {
                    Error::Unsupported(format!("{type_name} needs Redis 7.4 or newer (HPEXPIRE)"))
                }
                other => other,
            })?;
        let keys = Self::slot_keys(slot);
        let core = &self.key.core;
        let done: i64 = if ttl.is_zero() {
            core.eval(
                &scripts::NATIVE_PERSIST_KEY,
                keys,
                vec![slot.encoded_key.clone()],
            )
            .await?
        } else {
            let ms = Bytes::from(millis(ttl)?.to_string());
            core.eval(
                &scripts::NATIVE_EXPIRE_KEY,
                keys,
                vec![ms, slot.encoded_key.clone()],
            )
            .await?
        };
        Ok(done == 1)
    }

    /// `delete`: the hash and every values key (and the ttl zset in cache mode); whether any key was deleted.
    pub(crate) async fn delete(&self) -> Result<bool> {
        let (script, keys, args) = del_script(self.key, self.mode);
        let deleted: i64 = self.key.core.eval(script, keys, args).await?;
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

        impl<K, V, C: $crate::codec::Codec> $crate::object::HasKey for $name<K, V, C> {
            fn key(&self) -> &$crate::object::Key {
                &self.key
            }

            fn companions(&self) -> ::std::vec::Vec<String> {
                $crate::collections::multimap::companions(&self.mode)
            }

            fn del_script(
                &self,
            ) -> Option<(
                &'static $crate::lua::Lua,
                ::std::vec::Vec<String>,
                ::std::vec::Vec<::bytes::Bytes>,
            )> {
                Some($crate::collections::multimap::del_script(
                    &self.key, &self.mode,
                ))
            }

            fn expire_script(&self) -> Option<&'static $crate::lua::Lua> {
                Some($crate::collections::multimap::expire_script(&self.mode))
            }

            fn expire_args(&self) -> ::std::vec::Vec<::bytes::Bytes> {
                $crate::collections::multimap::expire_args(&self.key)
            }

            fn persist_script(&self) -> Option<&'static $crate::lua::Lua> {
                Some($crate::collections::multimap::persist_script(&self.mode))
            }

            fn persist_args(&self) -> ::std::vec::Vec<::bytes::Bytes> {
                $crate::collections::multimap::persist_args(&self.key)
            }

            fn rename_script(
                &self,
                new_name: &str,
            ) -> Option<(
                &'static $crate::lua::Lua,
                ::std::vec::Vec<String>,
                ::std::vec::Vec<::bytes::Bytes>,
            )> {
                Some($crate::collections::multimap::rename_script(
                    &self.key, &self.mode, $shape, new_name,
                ))
            }
        }

        impl<K, V, C: $crate::codec::Codec> $name<K, V, C> {
            pub(crate) fn new(
                key: $crate::object::Key,
                codec: C,
                mode: $crate::collections::multimap::Mode,
            ) -> Self {
                $crate::collections::multimap::schedule_clean_up(&key, &mode);
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

            /// The values of the key `k` as a lazy handle, Redisson's `get(key)`. Building it sends nothing; it fails only when the key cannot be encoded.
            pub fn get<Q>(
                &self,
                k: &Q,
            ) -> $crate::error::Result<$crate::collections::multimap::values::$values<V, C>>
            where
                K: ::std::borrow::Borrow<Q>,
                Q: ::serde::Serialize + ?Sized + Sync,
            {
                let slot = self.ops().slot(k)?;
                Ok($crate::collections::multimap::values::$values::new(
                    slot,
                    self.codec.clone(),
                ))
            }

            $crate::collections::map::key_hash::per_key_objects!();

            /// Starts listening to every kind of change, Redisson's `addListener` with each multimap listener. Dropping the returned [`MultimapEvents`](crate::multimap::MultimapEvents) is `removeListener`.
            #[doc(alias = "addListener")]
            pub async fn events(
                &self,
            ) -> $crate::error::Result<$crate::multimap::MultimapEvents<K, C>> {
                self.events_of(&$crate::MultimapEventKind::ALL).await
            }

            /// Starts listening to the given kinds of change. No kinds are an [`Error::Config`]($crate::Error::Config).
            pub async fn events_of(
                &self,
                kinds: &[$crate::MultimapEventKind],
            ) -> $crate::error::Result<$crate::multimap::MultimapEvents<K, C>> {
                $crate::collections::multimap::events::subscribe(
                    &self.key,
                    self.codec.clone(),
                    $shape,
                    kinds,
                )
                .await
            }

            /// Streams every `(key, value)` pair, Redisson's `entries()`, in pages of 10 keys. Keys without values yield nothing. Changes made while the stream runs may be missed or seen twice, as with any scan.
            #[doc(alias = "entries")]
            pub fn iter(
                &self,
            ) -> impl ::futures::Stream<Item = $crate::error::Result<(K, V)>> + '_ {
                self.iter_paged(0)
            }

            /// Like [`iter`](Self::iter), with `HSCAN … COUNT page_size` over the keys (and `SSCAN … COUNT page_size` over a set's values), Redisson's `entries(count)`. Zero means 10.
            #[doc(alias = "entries")]
            pub fn iter_paged(
                &self,
                page_size: usize,
            ) -> impl ::futures::Stream<Item = $crate::error::Result<(K, V)>> + '_ {
                $crate::collections::multimap::stream::entries(
                    &self.key,
                    &self.codec,
                    $shape,
                    page_size,
                )
            }

            /// Streams every key, Redisson's `keySet()`, in pages of 10. A `ListMultimap` key whose list [`remove_from_all_keys`](Self::remove_from_all_keys) emptied still shows here until it is removed, as in Redisson.
            #[doc(alias = "keySet")]
            pub fn keys(&self) -> impl ::futures::Stream<Item = $crate::error::Result<K>> + '_ {
                self.keys_paged(0)
            }

            /// Like [`keys`](Self::keys), with `HSCAN … COUNT page_size`, Redisson's `keySet(count)`. Zero means 10.
            #[doc(alias = "keySet")]
            pub fn keys_paged(
                &self,
                page_size: usize,
            ) -> impl ::futures::Stream<Item = $crate::error::Result<K>> + '_ {
                $crate::collections::multimap::stream::keys(&self.key, &self.codec, page_size)
            }

            /// Streams every value of every key, Redisson's `values()`, in pages of 10 keys. A value held by several keys comes once per key.
            pub fn values(&self) -> impl ::futures::Stream<Item = $crate::error::Result<V>> + '_ {
                self.values_paged(0)
            }

            /// Like [`values`](Self::values), with the page size of [`iter_paged`](Self::iter_paged), Redisson's `values(count)`. Zero means 10.
            pub fn values_paged(
                &self,
                page_size: usize,
            ) -> impl ::futures::Stream<Item = $crate::error::Result<V>> + '_ {
                $crate::collections::multimap::stream::values(
                    &self.key,
                    &self.codec,
                    $shape,
                    page_size,
                )
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

/// Adds `expire_key` over field expiry to a native multimap type.
macro_rules! native_expire_key {
    ($name:ident) => {
        impl<K, V, C> $name<K, V, C>
        where
            K: ::serde::Serialize + ::serde::de::DeserializeOwned + Send + Sync,
            V: ::serde::Serialize + ::serde::de::DeserializeOwned + Send + Sync,
            C: $crate::codec::Codec,
        {
            /// Sets the time to live of the key `k` and its values, Redisson's `expireKey`: `HPEXPIRE` on the key's hash field and `PEXPIRE` on its values, so both go together. [`Duration::ZERO`](::std::time::Duration::ZERO) clears both TTLs instead. Returns whether the key exists.
            ///
            /// It needs Redis 7.4 or newer (or Valkey 9) and fails with [`Error::Unsupported`]($crate::Error::Unsupported) before writing anything on an older server; every other method runs on 6.2. [`extend`](Self::extend) and the handle's writes keep the TTL; [`replace_values`](Self::replace_values) and [`set_values`](Self::set_values) make the key permanent, as in Redisson.
            #[doc(alias = "expireKey")]
            pub async fn expire_key<Q>(
                &self,
                k: &Q,
                ttl: ::std::time::Duration,
            ) -> $crate::error::Result<bool>
            where
                K: ::std::borrow::Borrow<Q>,
                Q: ::serde::Serialize + ?Sized + Sync,
            {
                let ops = self.ops();
                let slot = ops.slot(k)?;
                ops.expire_key(&slot, ttl, stringify!($name)).await
            }
        }
    };
}

multimap_type!(
    /// **Experimental.** A [`ListMultimap`] whose keys can expire one by one, Redisson's `RListMultimapCacheNative`. [`expire_key`](Self::expire_key) sets a key's TTL with Redis 7.4 hash field expiry; everything else is a plain `ListMultimap` and runs on Redis 6.2.
    ///
    /// It uses Redisson's layout, so Redisson clients read and write the same data. Get one with [`Client::list_multimap_cache_native`](crate::Client::list_multimap_cache_native).
    ListMultimapCacheNative,
    ListMultimapValues,
    Shape::List,
    "getListMultimapCacheNative"
);
native_expire_key!(ListMultimapCacheNative);

multimap_type!(
    /// **Experimental.** A [`SetMultimap`] whose keys can expire one by one, Redisson's `RSetMultimapCacheNative`. [`expire_key`](Self::expire_key) sets a key's TTL with Redis 7.4 hash field expiry; everything else is a plain `SetMultimap` and runs on Redis 6.2.
    ///
    /// It uses Redisson's layout, so Redisson clients read and write the same data. Get one with [`Client::set_multimap_cache_native`](crate::Client::set_multimap_cache_native).
    SetMultimapCacheNative,
    SetMultimapValues,
    Shape::Set,
    "getSetMultimapCacheNative"
);
native_expire_key!(SetMultimapCacheNative);

/// Adds `expire_key` over Redisson's ttl zset to a cache multimap type.
macro_rules! cache_expire_key {
    ($name:ident) => {
        impl<K, V, C> $name<K, V, C>
        where
            K: ::serde::Serialize + ::serde::de::DeserializeOwned + Send + Sync,
            V: ::serde::Serialize + ::serde::de::DeserializeOwned + Send + Sync,
            C: $crate::codec::Codec,
        {
            /// Sets the time to live of the key `k`, Redisson's `expireKey`: it records the deadline, now plus `ttl` on the client's clock (as in Redisson), as the key's score in the multimap's ttl zset. [`Duration::ZERO`](::std::time::Duration::ZERO) removes the key's TTL instead. Returns whether the key exists.
            ///
            /// Reads of one key ([`get_all`](Self::get_all), [`contains_key`](Self::contains_key), [`contains_entry`](Self::contains_entry), the handle from [`get`](Self::get)) and [`contains_value`](Self::contains_value) hide an expired key at once. [`len`](Self::len), [`key_len`](Self::key_len), [`keys_to_vec`](Self::keys_to_vec), [`remove_from_all_keys`](Self::remove_from_all_keys) and the streams count it until the clean-up task deletes it, as in Redisson. Writes to a live key keep its TTL; a key that is new to the multimap starts without one.
            #[doc(alias = "expireKey")]
            pub async fn expire_key<Q>(
                &self,
                k: &Q,
                ttl: ::std::time::Duration,
            ) -> $crate::error::Result<bool>
            where
                K: ::std::borrow::Borrow<Q>,
                Q: ::serde::Serialize + ?Sized + Sync,
            {
                let ops = self.ops();
                let slot = ops.slot(k)?;
                ops.expire_key(&slot, ttl, stringify!($name)).await
            }
        }
    };
}

multimap_type!(
    /// **Experimental.** A [`ListMultimap`] whose keys can expire one by one, Redisson's `RListMultimapCache`. [`expire_key`](Self::expire_key) gives a key a deadline, kept in Redisson's ttl zset `{name}:redissun_list_multimap_ttl`; a background task deletes expired keys. It runs on Redis 6.2.
    ///
    /// It uses Redisson's layout, so Redisson clients read and write the same data. Get one with [`Client::list_multimap_cache`](crate::Client::list_multimap_cache).
    ListMultimapCache,
    ListMultimapValues,
    Shape::List,
    "getListMultimapCache"
);
cache_expire_key!(ListMultimapCache);

multimap_type!(
    /// **Experimental.** A [`SetMultimap`] whose keys can expire one by one, Redisson's `RSetMultimapCache`. [`expire_key`](Self::expire_key) gives a key a deadline, kept in Redisson's ttl zset `{name}:redissun_set_multimap_ttl`; a background task deletes expired keys. It runs on Redis 6.2.
    ///
    /// It uses Redisson's layout, so Redisson clients read and write the same data. Get one with [`Client::set_multimap_cache`](crate::Client::set_multimap_cache).
    SetMultimapCache,
    SetMultimapValues,
    Shape::Set,
    "getSetMultimapCache"
);
cache_expire_key!(SetMultimapCache);

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
