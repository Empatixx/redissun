//! The builders [`MapCacheNative`] write methods return: await them, optionally after setting a time with `.ttl(d)` or `.expire_at(t)`.

use super::scripts::{
    EXPIRE_ENTRIES, EXPIRE_ENTRIES_IF, EXPIRE_ENTRY, EXPIRE_ENTRY_IF, FAST_PUT, PUT, PUT_ALL_TTL,
    PUT_IF_ABSENT, PUT_IF_EXIST,
};
use super::support::{require, Needs};
use super::ttl::FieldTtl;
use super::MapCacheNative;
use crate::codec::Codec;
use crate::collections::hash_map::Entry;
use crate::collections::map::entry::Target;
use crate::error::{Error, Result};
use crate::expiry::{expiry_args, Expiry};
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::time::{Duration, SystemTime};

/// A single-key write builder: the struct, `new`, `Debug`, `.ttl(d)` and `.expire_at(t)`.
macro_rules! single_key_write {
    ($(#[$doc:meta])* $name:ident, $what:literal) => {
        $(#[$doc])*
        #[must_use = concat!("a ", $what, " does nothing until it is awaited")]
        pub struct $name<'a, K, V, C: Codec, Q: ?Sized, W: ?Sized> {
            map: &'a MapCacheNative<K, V, C>,
            k: &'a Q,
            v: &'a W,
            ttl: Option<FieldTtl>,
        }

        impl<'a, K, V, C: Codec, Q: ?Sized, W: ?Sized> $name<'a, K, V, C, Q, W> {
            pub(super) fn new(map: &'a MapCacheNative<K, V, C>, k: &'a Q, v: &'a W) -> Self {
                Self {
                    map,
                    k,
                    v,
                    ttl: None,
                }
            }

            /// Makes the entry expire `ttl` after the write, on the server's clock (`HPEXPIRE`). Zero means no time to live. The last of `ttl` and `expire_at` wins.
            pub fn ttl(mut self, ttl: Duration) -> Self {
                self.ttl = Some(FieldTtl::After(ttl));
                self
            }

            /// Makes the entry expire at `time`, on the caller's clock (`HPEXPIREAT`). A time in the past deletes the entry at once; a time before 1970 is an [`Error::Config`](crate::Error::Config). The last of `ttl` and `expire_at` wins.
            pub fn expire_at(mut self, time: SystemTime) -> Self {
                self.ttl = Some(FieldTtl::At(time));
                self
            }
        }

        impl<K, V, C: Codec, Q: ?Sized, W: ?Sized> fmt::Debug for $name<'_, K, V, C, Q, W> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct(stringify!($name))
                    .field("map", &self.map)
                    .field("ttl", &self.ttl)
                    .finish_non_exhaustive()
            }
        }
    };
}

single_key_write!(
    /// A pending [`MapCacheNative::insert`]. Await it, optionally after [`Insert::ttl`] or [`Insert::expire_at`].
    Insert,
    "insert"
);
single_key_write!(
    /// A pending [`MapCacheNative::set`]. Await it, optionally after [`Set::ttl`] or [`Set::expire_at`].
    Set,
    "set"
);
single_key_write!(
    /// A pending [`MapCacheNative::try_insert`]. Await it, optionally after [`TryInsert::ttl`] or [`TryInsert::expire_at`].
    TryInsert,
    "try_insert"
);
single_key_write!(
    /// A pending [`MapCacheNative::replace`]. Await it, optionally after [`Replace::ttl`] or [`Replace::expire_at`].
    Replace,
    "replace"
);

/// The script arguments of a write with a time, after the server was checked; `None` takes the path without a time.
async fn timed<K, V, C: Codec>(
    map: &MapCacheNative<K, V, C>,
    ttl: Option<FieldTtl>,
) -> Result<Option<(i64, &'static str)>> {
    let Some(args) = ttl.map(FieldTtl::script_args).transpose()?.flatten() else {
        return Ok(None);
    };
    require(&map.key, Needs::FieldTtl).await?;
    Ok(Some(args))
}

/// Redisson's `ARGV` for the single-key scripts: `[ms, key, value, expire command]`.
fn script_argv(ms: i64, command: &str, k: &Bytes, v: &Bytes) -> Vec<Bytes> {
    vec![
        Bytes::from(ms.to_string()),
        k.clone(),
        v.clone(),
        Bytes::copy_from_slice(command.as_bytes()),
    ]
}

impl<'a, K, V, C, Q, W> IntoFuture for Insert<'a, K, V, C, Q, W>
where
    K: Serialize + DeserializeOwned + Send + Sync + Borrow<Q> + 'a,
    V: Serialize + DeserializeOwned + Send + Sync + Borrow<W> + 'a,
    C: Codec,
    Q: Serialize + ?Sized + Sync + 'a,
    W: Serialize + ?Sized + Sync + 'a,
{
    type Output = Result<Option<V>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let map = self.map;
            let Some((ms, command)) = timed(map, self.ttl).await? else {
                return map.map.insert(self.k, self.v).await;
            };
            let (k, v) = (map.codec.encode(self.k)?, map.codec.encode(self.v)?);
            let previous: Option<Bytes> = map
                .key
                .core
                .eval(
                    &PUT,
                    vec![map.key.redis_key()],
                    script_argv(ms, command, &k, &v),
                )
                .await?;
            map.map.written(k, v).await?;
            map.map.decode_value(previous)
        })
    }
}

impl<'a, K, V, C, Q, W> IntoFuture for Set<'a, K, V, C, Q, W>
where
    K: Serialize + DeserializeOwned + Send + Sync + Borrow<Q> + 'a,
    V: Serialize + DeserializeOwned + Send + Sync + Borrow<W> + 'a,
    C: Codec,
    Q: Serialize + ?Sized + Sync + 'a,
    W: Serialize + ?Sized + Sync + 'a,
{
    type Output = Result<bool>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let map = self.map;
            let Some((ms, command)) = timed(map, self.ttl).await? else {
                return map.map.set(self.k, self.v).await;
            };
            let (k, v) = (map.codec.encode(self.k)?, map.codec.encode(self.v)?);
            let added: i64 = map
                .key
                .core
                .eval(
                    &FAST_PUT,
                    vec![map.key.redis_key()],
                    script_argv(ms, command, &k, &v),
                )
                .await?;
            map.map.written(k, v).await?;
            Ok(added == 1)
        })
    }
}

impl<'a, K, V, C, Q, W> IntoFuture for TryInsert<'a, K, V, C, Q, W>
where
    K: Serialize + DeserializeOwned + Send + Sync + Borrow<Q> + 'a,
    V: Serialize + DeserializeOwned + Send + Sync + Borrow<W> + 'a,
    C: Codec,
    Q: Serialize + ?Sized + Sync + 'a,
    W: Serialize + ?Sized + Sync + 'a,
{
    type Output = Result<Option<V>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let map = self.map;
            let Some((ms, command)) = timed(map, self.ttl).await? else {
                return map.map.try_insert(self.k, self.v).await;
            };
            let (k, v) = (map.codec.encode(self.k)?, map.codec.encode(self.v)?);
            // At most once: run again after a lost reply, it would find this call's own value and return it as the present one.
            let present: Option<Bytes> = map
                .key
                .core
                .eval_once(
                    &PUT_IF_ABSENT,
                    vec![map.key.redis_key()],
                    script_argv(ms, command, &k, &v),
                )
                .await?;
            if present.is_none() {
                map.map.written(k, v).await?;
            }
            map.map.decode_value(present)
        })
    }
}

impl<'a, K, V, C, Q, W> IntoFuture for Replace<'a, K, V, C, Q, W>
where
    K: Serialize + DeserializeOwned + Send + Sync + Borrow<Q> + 'a,
    V: Serialize + DeserializeOwned + Send + Sync + Borrow<W> + 'a,
    C: Codec,
    Q: Serialize + ?Sized + Sync + 'a,
    W: Serialize + ?Sized + Sync + 'a,
{
    type Output = Result<Option<V>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let map = self.map;
            let Some(ttl) = self.ttl else {
                return map.replace_keeping_ttl(self.k, self.v).await;
            };
            // A zero TTL is Redisson's `putIfExists(k, v)`: the plain `HSET`, which leaves the entry without a TTL.
            let Some((ms, command)) = timed(map, Some(ttl)).await? else {
                return map.map.replace(self.k, self.v).await;
            };
            let (k, v) = (map.codec.encode(self.k)?, map.codec.encode(self.v)?);
            // At most once: run again after a lost reply, it would return this call's own value as the old one.
            let previous: Option<Bytes> = map
                .key
                .core
                .eval_once(
                    &PUT_IF_EXIST,
                    vec![map.key.redis_key()],
                    script_argv(ms, command, &k, &v),
                )
                .await?;
            if previous.is_some() {
                map.map.written(k, v).await?;
            }
            map.map.decode_value(previous)
        })
    }
}

/// Which entries [`ExpireEntry`] and [`ExpireEntries`] change: Redis's `NX`, `GT` and `LT`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Condition {
    Always,
    IfNotSet,
    IfGreater,
    IfLess,
}

impl Condition {
    fn param(self) -> Option<&'static str> {
        match self {
            Condition::Always => None,
            Condition::IfNotSet => Some("NX"),
            Condition::IfGreater => Some("GT"),
            Condition::IfLess => Some("LT"),
        }
    }
}

/// What an expire builder sends, checked before the server is asked.
enum ExpirePlan {
    /// `HPERSIST`: no time, or a zero one.
    Persist,
    /// A script with the time and maybe a condition.
    Set {
        ms: i64,
        command: &'static str,
        param: Option<&'static str>,
    },
}

fn expire_plan(ttl: Option<FieldTtl>, condition: Condition) -> Result<ExpirePlan> {
    let args = ttl.map(FieldTtl::script_args).transpose()?.flatten();
    match (args, condition.param()) {
        (None, None) => Ok(ExpirePlan::Persist),
        (None, Some(_)) => Err(Error::Config(
            "expire_entry with a condition needs ttl or expire_at".into(),
        )),
        (Some((ms, command)), param) => Ok(ExpirePlan::Set { ms, command, param }),
    }
}

/// The options both expire builders share: the struct's `ttl`, `expire_at` and the three conditions.
macro_rules! expire_options {
    () => {
        /// Gives the entry `ttl` from now, on the server's clock (`HPEXPIRE`). Zero, like no time, removes the TTL. The last of `ttl` and `expire_at` wins.
        pub fn ttl(mut self, ttl: Duration) -> Self {
            self.ttl = Some(FieldTtl::After(ttl));
            self
        }

        /// Makes the entry expire at `time`, on the caller's clock (`HPEXPIREAT`). A time in the past deletes it at once. The last of `ttl` and `expire_at` wins.
        pub fn expire_at(mut self, time: SystemTime) -> Self {
            self.ttl = Some(FieldTtl::At(time));
            self
        }

        /// Changes only an entry without a TTL (`NX`), Redisson's `expireEntryIfNotSet`. Needs a time. The last condition wins.
        #[doc(alias = "expireEntryIfNotSet", alias = "expireEntriesIfNotSet")]
        pub fn if_not_set(mut self) -> Self {
            self.condition = Condition::IfNotSet;
            self
        }

        /// Changes only an entry whose new time is later than its current one (`GT`); an entry without a TTL counts as never expiring, so it is not changed. Needs a time. The last condition wins.
        #[doc(alias = "expireEntryIfGreater", alias = "expireEntriesIfGreater")]
        pub fn if_greater(mut self) -> Self {
            self.condition = Condition::IfGreater;
            self
        }

        /// Changes only an entry whose new time is earlier than its current one (`LT`); an entry without a TTL counts as never expiring, so it is changed. Needs a time. The last condition wins.
        #[doc(alias = "expireEntryIfLess", alias = "expireEntriesIfLess")]
        pub fn if_less(mut self) -> Self {
            self.condition = Condition::IfLess;
            self
        }
    };
}

/// A pending [`MapCacheNative::expire_entry`]. Await it after [`ExpireEntry::ttl`] or [`ExpireEntry::expire_at`], and optionally a condition; without a time it removes the TTL.
#[must_use = "an expire does nothing until it is awaited"]
pub struct ExpireEntry<'a, K, V, C: Codec, Q: ?Sized> {
    map: &'a MapCacheNative<K, V, C>,
    k: &'a Q,
    ttl: Option<FieldTtl>,
    condition: Condition,
}

impl<'a, K, V, C: Codec, Q: ?Sized> ExpireEntry<'a, K, V, C, Q> {
    pub(super) fn new(map: &'a MapCacheNative<K, V, C>, k: &'a Q) -> Self {
        Self {
            map,
            k,
            ttl: None,
            condition: Condition::Always,
        }
    }

    expire_options!();
}

impl<K, V, C: Codec, Q: ?Sized> fmt::Debug for ExpireEntry<'_, K, V, C, Q> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExpireEntry")
            .field("map", &self.map)
            .field("ttl", &self.ttl)
            .field("condition", &self.condition)
            .finish_non_exhaustive()
    }
}

impl<'a, K, V, C, Q> IntoFuture for ExpireEntry<'a, K, V, C, Q>
where
    K: Serialize + DeserializeOwned + Send + Sync + Borrow<Q> + 'a,
    V: Serialize + DeserializeOwned + Send + Sync + 'a,
    C: Codec,
    Q: Serialize + ?Sized + Sync + 'a,
{
    type Output = Result<bool>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let map = self.map;
            let plan = expire_plan(self.ttl, self.condition)?;
            require(&map.key, Needs::FieldTtl).await?;
            let k = map.codec.encode(self.k)?;
            let (ms, command, param) = match plan {
                ExpirePlan::Persist => {
                    let replies = map.persist_fields(std::slice::from_ref(&k)).await?;
                    // 1: the TTL was removed; -1: the entry has none. Either way it is there and has no TTL now.
                    return Ok(matches!(replies.first(), Some(1 | -1)));
                }
                ExpirePlan::Set { ms, command, param } => (ms, command, param),
            };
            let ms = Bytes::from(ms.to_string());
            let command = Bytes::copy_from_slice(command.as_bytes());
            let (script, args) = match param {
                None => (&*EXPIRE_ENTRY, vec![ms, k, command]),
                Some(param) => (
                    &*EXPIRE_ENTRY_IF,
                    vec![ms, k, Bytes::copy_from_slice(param.as_bytes()), command],
                ),
            };
            let flag: i64 = map
                .key
                .core
                .eval(script, vec![map.key.redis_key()], args)
                .await?;
            Ok(flag == 1)
        })
    }
}

/// A pending [`MapCacheNative::expire_entries`]. Await it after [`ExpireEntries::ttl`] or [`ExpireEntries::expire_at`], and optionally a condition; without a time it removes the TTLs.
#[must_use = "an expire does nothing until it is awaited"]
pub struct ExpireEntries<'a, K, V, C: Codec, Q: ?Sized> {
    map: &'a MapCacheNative<K, V, C>,
    ks: &'a [&'a Q],
    ttl: Option<FieldTtl>,
    condition: Condition,
}

impl<'a, K, V, C: Codec, Q: ?Sized> ExpireEntries<'a, K, V, C, Q> {
    pub(super) fn new(map: &'a MapCacheNative<K, V, C>, ks: &'a [&'a Q]) -> Self {
        Self {
            map,
            ks,
            ttl: None,
            condition: Condition::Always,
        }
    }

    expire_options!();
}

impl<K, V, C: Codec, Q: ?Sized> fmt::Debug for ExpireEntries<'_, K, V, C, Q> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExpireEntries")
            .field("map", &self.map)
            .field("keys", &self.ks.len())
            .field("ttl", &self.ttl)
            .field("condition", &self.condition)
            .finish_non_exhaustive()
    }
}

impl<'a, K, V, C, Q> IntoFuture for ExpireEntries<'a, K, V, C, Q>
where
    K: Serialize + DeserializeOwned + Send + Sync + Borrow<Q> + 'a,
    V: Serialize + DeserializeOwned + Send + Sync + 'a,
    C: Codec,
    Q: Serialize + ?Sized + Sync + 'a,
{
    type Output = Result<usize>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let map = self.map;
            let plan = expire_plan(self.ttl, self.condition)?;
            if self.ks.is_empty() {
                return Ok(0);
            }
            require(&map.key, Needs::FieldTtl).await?;
            let fields = self
                .ks
                .iter()
                .map(|k| map.codec.encode(*k))
                .collect::<Result<Vec<Bytes>>>()?;
            let (ms, command, param) = match plan {
                ExpirePlan::Persist => {
                    let replies = map.persist_fields(&fields).await?;
                    return Ok(replies.iter().filter(|&&r| r == 1 || r == -1).count());
                }
                ExpirePlan::Set { ms, command, param } => (ms, command, param),
            };
            let ms = Bytes::from(ms.to_string());
            let command = Bytes::copy_from_slice(command.as_bytes());
            let (script, mut args) = match param {
                None => (&*EXPIRE_ENTRIES, vec![ms, command]),
                Some(param) => (
                    &*EXPIRE_ENTRIES_IF,
                    vec![Bytes::copy_from_slice(param.as_bytes()), ms, command],
                ),
            };
            args.extend(fields);
            let changed: usize = map
                .key
                .core
                .eval(script, vec![map.key.redis_key()], args)
                .await?;
            Ok(changed)
        })
    }
}

/// The time options of the builders for many entries: `ttl`, `expire_at` and `keep_ttl`, the last one set wins.
macro_rules! many_expiry_options {
    () => {
        /// Makes the entries expire `ttl` after the write, on the server's clock. Zero means no time to live. The last of `ttl`, `expire_at` and `keep_ttl` wins.
        #[doc(alias = "timeToLive")]
        pub fn ttl(mut self, ttl: Duration) -> Self {
            self.expiry = if ttl.is_zero() {
                Expiry::None
            } else {
                Expiry::After(ttl)
            };
            self
        }

        /// Makes the entries expire at `time`, on the caller's clock (`HSETEX … PXAT`, Redis 8.0). A time in the past deletes them at once; a time before 1970 is an [`Error::Config`](crate::Error::Config). The last of `ttl`, `expire_at` and `keep_ttl` wins.
        #[doc(alias = "expireAt")]
        pub fn expire_at(mut self, time: SystemTime) -> Self {
            self.expiry = Expiry::At(time);
            self
        }

        /// Keeps the time to live of the entries that are overwritten (`HSETEX … KEEPTTL`, Redis 8.0); new entries get none. The last of `ttl`, `expire_at` and `keep_ttl` wins.
        #[doc(alias = "keepTTL")]
        pub fn keep_ttl(mut self) -> Self {
            self.expiry = Expiry::KeepTtl;
            self
        }
    };
}

/// `HSETEX key [FNX|FXX] [PX ms|PXAT ms|KEEPTTL] FIELDS n k v …`, Redisson's `putAllOperationAsync(PutParams)` and `putAllKeysOperationAsync`.
fn hsetex<K, V, C: Codec>(
    map: &MapCacheNative<K, V, C>,
    condition: Option<&str>,
    expiry: Option<(&'static str, Option<i64>)>,
    entries: &[(Bytes, Bytes)],
) -> redis::Cmd {
    let mut cmd = map.key.key_cmd("HSETEX");
    if let Some(condition) = condition {
        cmd.arg(condition);
    }
    if let Some((option, value)) = expiry {
        cmd.arg(option);
        if let Some(value) = value {
            cmd.arg(value);
        }
    }
    cmd.arg("FIELDS")
        .arg(entries.len())
        .arg(crate::client::request::binary_pairs(entries));
    cmd
}

/// A pending [`MapCacheNative::extend`]. Await it, optionally after [`Extend::batch_size`] and a time.
#[must_use = "an extend does nothing until it is awaited"]
pub struct Extend<'a, K, V, C: Codec> {
    map: &'a MapCacheNative<K, V, C>,
    encoded: Result<Vec<(Bytes, Bytes)>>,
    batch_size: usize,
    expiry: Expiry,
}

impl<'a, K, V, C: Codec> Extend<'a, K, V, C> {
    pub(super) fn new(
        map: &'a MapCacheNative<K, V, C>,
        encoded: Result<Vec<(Bytes, Bytes)>>,
    ) -> Self {
        Self {
            map,
            encoded,
            batch_size: 0,
            expiry: Expiry::None,
        }
    }

    /// Sends one call per `batch_size` entries, in order, Redisson's `putAll(map, batchSize)`. Zero, the default, sends everything at once. The writer gets each batch after Redis stored it.
    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    many_expiry_options!();
}

impl<K, V, C: Codec> fmt::Debug for Extend<'_, K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Extend")
            .field("map", &self.map)
            .field("batch_size", &self.batch_size)
            .field("expiry", &self.expiry)
            .finish_non_exhaustive()
    }
}

impl<'a, K, V, C> IntoFuture for Extend<'a, K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync + 'a,
    V: Serialize + DeserializeOwned + Send + Sync + 'a,
    C: Codec,
{
    type Output = Result<()>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let map = self.map;
            let entries = self.encoded?;
            let option = expiry_args(self.expiry)?;
            if entries.is_empty() {
                return Ok(());
            }
            match self.expiry {
                Expiry::None => {}
                Expiry::After(_) => require(&map.key, Needs::FieldTtl).await?,
                Expiry::At(_) | Expiry::KeepTtl => require(&map.key, Needs::Hsetex).await?,
            }
            let size = if self.batch_size == 0 {
                entries.len()
            } else {
                self.batch_size
            };
            for batch in entries.chunks(size) {
                match (self.expiry, option) {
                    (Expiry::None, _) => {
                        let mut cmd = map.key.key_cmd("HSET");
                        cmd.arg(crate::client::request::binary_pairs(batch));
                        map.key.query::<()>(cmd).await?;
                    }
                    (Expiry::After(_), Some((_, Some(ms)))) => {
                        let mut args = Vec::with_capacity(batch.len() * 2 + 1);
                        args.push(Bytes::from(ms.to_string()));
                        for (k, v) in batch {
                            args.push(k.clone());
                            args.push(v.clone());
                        }
                        map.key
                            .core
                            .eval::<()>(&PUT_ALL_TTL, vec![map.key.redis_key()], args)
                            .await?;
                    }
                    (_, option) => {
                        map.key
                            .query::<()>(hsetex(map, None, option, batch))
                            .await?;
                    }
                }
                if let Some(store) = map.map.writer() {
                    store.written(batch).await?;
                }
            }
            Ok(())
        })
    }
}

/// `HSETEX key FNX|FXX …` for all entries in one call; the writer gets them all when Redis stored them.
async fn set_all<K, V, C>(
    map: &MapCacheNative<K, V, C>,
    condition: &str,
    encoded: Result<Vec<(Bytes, Bytes)>>,
    expiry: Expiry,
) -> Result<bool>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    let entries = encoded?;
    let option = expiry_args(expiry)?;
    if entries.is_empty() {
        return Ok(false);
    }
    require(&map.key, Needs::Hsetex).await?;
    let cmd = hsetex(map, Some(condition), option, &entries);
    let Some(store) = map.map.writer() else {
        let stored: i64 = map.key.query(cmd).await?;
        return Ok(stored == 1);
    };
    // At most once when a writer waits for the entries: sent again after a lost reply, it would answer 0 and the writer would miss them.
    let stored: i64 = map.key.query_once(cmd).await?;
    if stored == 1 {
        store.written(&entries).await?;
    }
    Ok(stored == 1)
}

/// The all-or-nothing builders for many entries: the struct, `new`, `Debug`, the time options and `IntoFuture` over `HSETEX` with `$condition`.
macro_rules! all_or_nothing {
    ($(#[$doc:meta])* $name:ident, $what:literal, $condition:literal) => {
        $(#[$doc])*
        #[must_use = concat!("a ", $what, " does nothing until it is awaited")]
        pub struct $name<'a, K, V, C: Codec> {
            map: &'a MapCacheNative<K, V, C>,
            encoded: Result<Vec<(Bytes, Bytes)>>,
            expiry: Expiry,
        }

        impl<'a, K, V, C: Codec> $name<'a, K, V, C> {
            pub(super) fn new(
                map: &'a MapCacheNative<K, V, C>,
                encoded: Result<Vec<(Bytes, Bytes)>>,
            ) -> Self {
                Self {
                    map,
                    encoded,
                    expiry: Expiry::None,
                }
            }

            many_expiry_options!();
        }

        impl<K, V, C: Codec> fmt::Debug for $name<'_, K, V, C> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct(stringify!($name))
                    .field("map", &self.map)
                    .field("expiry", &self.expiry)
                    .finish_non_exhaustive()
            }
        }

        impl<'a, K, V, C> IntoFuture for $name<'a, K, V, C>
        where
            K: Serialize + DeserializeOwned + Send + Sync + 'a,
            V: Serialize + DeserializeOwned + Send + Sync + 'a,
            C: Codec,
        {
            type Output = Result<bool>;
            type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

            fn into_future(self) -> Self::IntoFuture {
                Box::pin(set_all(self.map, $condition, self.encoded, self.expiry))
            }
        }
    };
}

all_or_nothing!(
    /// A pending [`MapCacheNative::try_extend`]. Await it, optionally after [`TryExtend::ttl`], [`TryExtend::expire_at`] or [`TryExtend::keep_ttl`].
    TryExtend,
    "try_extend",
    "FNX"
);
all_or_nothing!(
    /// A pending [`MapCacheNative::update_many`]. Await it, optionally after [`UpdateMany::ttl`], [`UpdateMany::expire_at`] or [`UpdateMany::keep_ttl`].
    UpdateMany,
    "update_many",
    "FXX"
);

/// A pending [`MapCacheNative::entry`]. Await it, optionally after [`EntryRequest::ttl`] or [`EntryRequest::expire_at`], to take the key's lock and read its value.
#[must_use = "an entry request does nothing until it is awaited"]
pub struct EntryRequest<'a, K, V, C: Codec> {
    map: &'a MapCacheNative<K, V, C>,
    key: Result<Bytes>,
    ttl: Option<FieldTtl>,
}

impl<K, V, C: Codec> fmt::Debug for EntryRequest<'_, K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EntryRequest")
            .field("map", &self.map)
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl<'a, K, V, C: Codec> EntryRequest<'a, K, V, C> {
    pub(super) fn new(map: &'a MapCacheNative<K, V, C>, key: Result<Bytes>) -> Self {
        Self {
            map,
            key,
            ttl: None,
        }
    }

    /// Makes a write through the entry expire `ttl` after it, on the server's clock, Redisson's `compute(key, ttl, f)`. Zero means no time to live: the write clears the TTL, as without this call. The last of `ttl` and `expire_at` wins.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(FieldTtl::After(ttl));
        self
    }

    /// Makes a write through the entry expire at `time`, on the caller's clock. A time before 1970 is an [`Error::Config`](crate::Error::Config), returned before the lock is taken. The last of `ttl` and `expire_at` wins.
    pub fn expire_at(mut self, time: SystemTime) -> Self {
        self.ttl = Some(FieldTtl::At(time));
        self
    }
}

impl<'a, K, V, C> IntoFuture for EntryRequest<'a, K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync + 'a,
    V: Serialize + DeserializeOwned + Send + Sync + 'a,
    C: Codec,
{
    type Output = Result<Entry<'a, K, V, C>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let key = self.key?;
            // Checked before the lock is taken, so a bad time or an old server fails at once.
            let ttl = match timed(self.map, self.ttl).await? {
                Some(_) => self.ttl,
                None => None,
            };
            Entry::acquire(Target::Native(self.map, ttl), key).await
        })
    }
}
