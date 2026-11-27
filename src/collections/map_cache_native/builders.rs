//! The builders [`MapCacheNative`] write methods return: await them, optionally after setting a time with `.ttl(d)` or `.expire_at(t)`.

use super::scripts::{FAST_PUT, PUT, PUT_IF_ABSENT, PUT_IF_EXIST};
use super::support::{require, Needs};
use super::ttl::FieldTtl;
use super::MapCacheNative;
use crate::codec::Codec;
use crate::error::Result;
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
