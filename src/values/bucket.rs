use crate::client::request::{command, Arg};
use crate::codec::Codec;
use crate::error::Result;
use crate::lua::Lua;
use crate::object::{millis, HasKey, Key};
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::LazyLock;
use std::time::Duration;

static COMPARE_AND_SET: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "if redis.call('GET', KEYS[1]) == ARGV[1] then
            redis.call('SET', KEYS[1], ARGV[2])
            return 1
        end
        return 0",
    )
});

static COMPARE_AND_DELETE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "if redis.call('GET', KEYS[1]) == ARGV[1] then
            redis.call('DEL', KEYS[1])
            return 1
        end
        return 0",
    )
});

static GET_AND_DELETE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local value = redis.call('GET', KEYS[1])
        redis.call('DEL', KEYS[1])
        return value",
    )
});

static GET_AND_SET_EX: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local value = redis.call('GET', KEYS[1])
        redis.call('PSETEX', KEYS[1], ARGV[2], ARGV[1])
        return value",
    )
});

/// A single value stored in a Redis string.
pub struct Bucket<V, C: Codec> {
    key: Key,
    codec: C,
    _marker: PhantomData<fn() -> V>,
}

impl<V, C: Codec> Clone for Bucket<V, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
            _marker: PhantomData,
        }
    }
}

impl<V, C: Codec> fmt::Debug for Bucket<V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "Bucket")
    }
}

impl<V, C: Codec> HasKey for Bucket<V, C> {
    fn key(&self) -> &Key {
        &self.key
    }
}

impl<V, C: Codec> Bucket<V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self {
            key,
            codec,
            _marker: PhantomData,
        }
    }
}

impl<V, C> Bucket<V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    fn decode(&self, raw: Option<Bytes>) -> Result<Option<V>> {
        raw.map(|bytes| self.codec.decode(&bytes)).transpose()
    }

    /// `SET key bytes` followed by `options` such as `PX <ms>`, `KEEPTTL`, `NX` or `XX`.
    fn set_command(&self, bytes: &[u8], options: Vec<Arg>) -> redis::Cmd {
        let mut cmd = redis::cmd("SET");
        cmd.arg(self.key.redis_key()).arg(bytes);
        for option in options {
            cmd.arg(option);
        }
        cmd
    }

    /// A conditional `SET`, sent once as Redisson does; returns whether the value was stored.
    async fn set_with(&self, bytes: Bytes, options: Vec<Arg>) -> Result<bool> {
        let stored: Option<String> = self
            .key
            .query_once(self.set_command(&bytes, options))
            .await?;
        Ok(stored.is_some())
    }

    async fn get_ex(&self, args: Vec<Arg>) -> Result<Option<V>> {
        let args: Vec<Arg> = std::iter::once(Arg::from(self.key.redis_key()))
            .chain(args)
            .collect();
        let raw: Option<Bytes> = self.key.query(command("GETEX", args)).await?;
        self.decode(raw)
    }

    /// Stores the value, replacing any existing one, Redisson's `set`. The time to live is cleared unless the builder says otherwise:
    /// `.ttl(d)` stores it with a time to live (`SET PX`, Redisson's `set(v, ttl)` / `setex`) and `.keep_ttl()` keeps the current one (`SET KEEPTTL`, Redisson's `setAndKeepTTL`).
    ///
    /// ```no_run
    /// # async fn run(client: redissun::Client) -> redissun::Result<()> {
    /// # use std::time::Duration;
    /// let bucket = client.bucket::<String>("greeting");
    /// bucket.set("hello").await?;
    /// bucket.set("hello").ttl(Duration::from_secs(60)).await?;
    /// bucket.set("hi").keep_ttl().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[doc(alias = "setex", alias = "setAndKeepTTL")]
    pub fn set<'a, Q>(&'a self, value: &Q) -> Set<'a, V, C>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Set {
            bucket: self,
            value: self.codec.encode(value),
            ttl: SetTtl::Clear,
        }
    }

    /// Returns the value, or `None` when the key is missing.
    pub async fn get(&self) -> Result<Option<V>> {
        let mut cmd = redis::cmd("GET");
        cmd.arg(self.key.redis_key());
        let raw: Option<Bytes> = self.key.read(cmd).await?;
        self.decode(raw)
    }

    /// Stores the value only when the key is missing; resolves to whether it was stored. Redisson's `setIfAbsent` and `trySet`.
    /// `.ttl(d)` stores it with a time to live (`SET PX NX`); without it the command is `SETNX`.
    #[doc(alias = "setIfAbsent", alias = "trySet")]
    pub fn try_set<'a, Q>(&'a self, value: &Q) -> TrySet<'a, V, C>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        TrySet {
            bucket: self,
            value: self.codec.encode(value),
            ttl: None,
        }
    }

    /// Replaces the value only when the key exists (`SET XX`); resolves to whether it was stored. Redisson's `setIfExists`.
    /// `.ttl(d)` also sets a time to live (`SET PX XX`); without it the time to live is cleared.
    #[doc(alias = "setIfExists")]
    pub fn update<'a, Q>(&'a self, value: &Q) -> Update<'a, V, C>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Update {
            bucket: self,
            value: self.codec.encode(value),
            ttl: None,
        }
    }

    /// Stores the value and resolves to the previous one, like `Option::replace`. Redisson's `getAndSet`.
    /// Without `.ttl(d)` it is `GETSET` and the time to live is cleared; with it the value is stored with that time to live.
    #[doc(alias = "getAndSet", alias = "getset")]
    pub fn replace<'a, Q>(&'a self, value: &Q) -> Replace<'a, V, C>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Replace {
            bucket: self,
            value: self.codec.encode(value),
            ttl: None,
        }
    }

    /// Returns the value and deletes the key, like `Option::take`. Redisson's `getAndDelete`.
    #[doc(alias = "getAndDelete", alias = "getdel")]
    pub async fn take(&self) -> Result<Option<V>> {
        let previous: Option<Bytes> = self
            .key
            .core
            .eval(&GET_AND_DELETE, vec![self.key.redis_key()], Vec::new())
            .await?;
        self.decode(previous)
    }

    /// Returns the value and sets a time to live on the key (`GETEX PX`), Redisson's `getAndExpire`. Needs Redis 6.2 or newer.
    #[doc(alias = "getAndExpire")]
    pub async fn get_expire(&self, ttl: Duration) -> Result<Option<V>> {
        self.get_ex(vec![Arg::from("PX"), Arg::from(millis(ttl)?)])
            .await
    }

    /// Returns the value and clears the time to live of the key (`GETEX PERSIST`), Redisson's `getAndClearExpire`. Needs Redis 6.2 or newer.
    #[doc(alias = "getAndClearExpire")]
    pub async fn get_persist(&self) -> Result<Option<V>> {
        self.get_ex(vec![Arg::from("PERSIST")]).await
    }

    /// Length of the encoded value in bytes (`STRLEN`), like `String::len`; 0 when the key is missing. Redisson's `size`.
    #[doc(alias = "size")]
    pub async fn len(&self) -> Result<usize> {
        let mut cmd = redis::cmd("STRLEN");
        cmd.arg(self.key.redis_key());
        let size: usize = self.key.read(cmd).await?;
        Ok(size)
    }

    /// Atomically replaces the value when it currently equals `expected`; returns whether it did.
    ///
    /// Like Redisson `compareAndSet`: `expected = None` stores `new` only when the key is missing,
    /// `new = None` deletes the key when it holds `expected`, and both `None` returns whether the key is missing.
    pub async fn compare_and_set<Q>(&self, expected: Option<&Q>, new: Option<&Q>) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        match (expected, new) {
            (None, None) => Ok(!self.key.exists().await?),
            (None, Some(new)) => self.try_set(new).await,
            (Some(expected), None) => self.compare_and_delete(expected).await,
            (Some(expected), Some(new)) => {
                let swapped: i64 = self
                    .key
                    .core
                    .eval(
                        &COMPARE_AND_SET,
                        vec![self.key.redis_key()],
                        vec![self.codec.encode(expected)?, self.codec.encode(new)?],
                    )
                    .await?;
                Ok(swapped == 1)
            }
        }
    }

    /// Atomically deletes the key when it holds `expected`; returns whether it did.
    pub async fn compare_and_delete<Q>(&self, expected: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let deleted: i64 = self
            .key
            .core
            .eval(
                &COMPARE_AND_DELETE,
                vec![self.key.redis_key()],
                vec![self.codec.encode(expected)?],
            )
            .await?;
        Ok(deleted == 1)
    }
}

/// How [`Set`] treats the time to live of the key.
#[derive(Clone, Copy, Debug)]
enum SetTtl {
    Clear,
    After(Duration),
    Keep,
}

/// A pending [`Bucket::set`]. Await it, optionally after [`Set::ttl`] or [`Set::keep_ttl`].
#[must_use = "a set does nothing until it is awaited"]
pub struct Set<'a, V, C: Codec> {
    bucket: &'a Bucket<V, C>,
    value: Result<Bytes>,
    ttl: SetTtl,
}

impl<V, C: Codec> fmt::Debug for Set<'_, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Set")
            .field("bucket", self.bucket)
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl<V, C: Codec> Set<'_, V, C> {
    /// Stores the value with a time to live (`SET PX`), Redisson's `set(value, ttl)`.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = SetTtl::After(ttl);
        self
    }

    /// Keeps the current time to live of the key (`SET KEEPTTL`, Redis 6.0 or newer), Redisson's `setAndKeepTTL`.
    pub fn keep_ttl(mut self) -> Self {
        self.ttl = SetTtl::Keep;
        self
    }
}

impl<'a, V, C> IntoFuture for Set<'a, V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'a,
    C: Codec,
{
    type Output = Result<()>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let bytes = self.value?;
            let options = match self.ttl {
                SetTtl::Clear => Vec::new(),
                SetTtl::After(ttl) => vec![Arg::from("PX"), Arg::from(millis(ttl)?)],
                SetTtl::Keep => vec![Arg::from("KEEPTTL")],
            };
            let bucket = self.bucket;
            let () = bucket
                .key
                .query(bucket.set_command(&bytes, options))
                .await?;
            Ok(())
        })
    }
}

/// A pending [`Bucket::try_set`]. Await it, optionally after [`TrySet::ttl`].
#[must_use = "a set does nothing until it is awaited"]
pub struct TrySet<'a, V, C: Codec> {
    bucket: &'a Bucket<V, C>,
    value: Result<Bytes>,
    ttl: Option<Duration>,
}

impl<V, C: Codec> fmt::Debug for TrySet<'_, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TrySet")
            .field("bucket", self.bucket)
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl<V, C: Codec> TrySet<'_, V, C> {
    /// Stores the value with a time to live (`SET PX NX`), Redisson's `setIfAbsent(value, ttl)`.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }
}

impl<'a, V, C> IntoFuture for TrySet<'a, V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'a,
    C: Codec,
{
    type Output = Result<bool>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let bytes = self.value?;
            let bucket = self.bucket;
            match self.ttl {
                None => {
                    let mut cmd = redis::cmd("SETNX");
                    cmd.arg(bucket.key.redis_key()).arg(&bytes[..]);
                    let stored: bool = bucket.key.query_once(cmd).await?;
                    Ok(stored)
                }
                Some(ttl) => {
                    let options = vec![Arg::from("PX"), Arg::from(millis(ttl)?), Arg::from("NX")];
                    bucket.set_with(bytes, options).await
                }
            }
        })
    }
}

/// A pending [`Bucket::update`]. Await it, optionally after [`Update::ttl`].
#[must_use = "an update does nothing until it is awaited"]
pub struct Update<'a, V, C: Codec> {
    bucket: &'a Bucket<V, C>,
    value: Result<Bytes>,
    ttl: Option<Duration>,
}

impl<V, C: Codec> fmt::Debug for Update<'_, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Update")
            .field("bucket", self.bucket)
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl<V, C: Codec> Update<'_, V, C> {
    /// Also sets a time to live (`SET PX XX`), Redisson's `setIfExists(value, ttl)`.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }
}

impl<'a, V, C> IntoFuture for Update<'a, V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'a,
    C: Codec,
{
    type Output = Result<bool>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let bytes = self.value?;
            let options = match self.ttl {
                None => vec![Arg::from("XX")],
                Some(ttl) => vec![Arg::from("PX"), Arg::from(millis(ttl)?), Arg::from("XX")],
            };
            self.bucket.set_with(bytes, options).await
        })
    }
}

/// A pending [`Bucket::replace`]. Await it, optionally after [`Replace::ttl`].
#[must_use = "a replace does nothing until it is awaited"]
pub struct Replace<'a, V, C: Codec> {
    bucket: &'a Bucket<V, C>,
    value: Result<Bytes>,
    ttl: Option<Duration>,
}

impl<V, C: Codec> fmt::Debug for Replace<'_, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Replace")
            .field("bucket", self.bucket)
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl<V, C: Codec> Replace<'_, V, C> {
    /// Stores the new value with a time to live, Redisson's `getAndSet(value, ttl)`.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }
}

impl<'a, V, C> IntoFuture for Replace<'a, V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'a,
    C: Codec,
{
    type Output = Result<Option<V>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let bytes = self.value?;
            let bucket = self.bucket;
            let previous: Option<Bytes> = match self.ttl {
                None => {
                    let mut cmd = redis::cmd("GETSET");
                    cmd.arg(bucket.key.redis_key()).arg(&bytes[..]);
                    bucket.key.query(cmd).await?
                }
                Some(ttl) => {
                    bucket
                        .key
                        .core
                        .eval(
                            &GET_AND_SET_EX,
                            vec![bucket.key.redis_key()],
                            vec![bytes, Bytes::from(millis(ttl)?.to_string())],
                        )
                        .await?
                }
            };
            bucket.decode(previous)
        })
    }
}
