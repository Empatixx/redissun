use crate::client::core::Core;
use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::object::millis;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// When the keys written by [`Buckets::update`] and [`Buckets::try_set`] expire, Redisson's `SetArgs`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expiry {
    /// The keys get no expiry; keys that are overwritten lose theirs.
    None,
    /// The keys expire after this time (`PX`), Redisson's `timeToLive`.
    After(Duration),
    /// The keys expire at this time (`PXAT`), Redisson's `expireAt`.
    At(SystemTime),
    /// Keys that are overwritten keep their time to live (`KEEPTTL`), Redisson's `keepTTL`.
    KeepTtl,
}

/// Many [`Bucket`](crate::Bucket)s read and written together, from [`Client::buckets`](crate::Client::buckets): Redisson's `RBuckets`.
///
/// Each call sends one command for all the keys, or in a cluster one for each hash slot, as Redisson does. So in a cluster a call over keys of several slots is not atomic: use a hash tag such as `{user:1}` to keep keys in one slot.
///
/// ```no_run
/// # async fn run(client: redissun::Client) -> redissun::Result<()> {
/// let buckets = client.buckets::<u32>();
/// buckets.set([("{user:1}:age", &30), ("{user:1}:score", &7)]).await?;
/// let values = buckets.get(&["{user:1}:age", "{user:1}:score", "{user:1}:missing"]).await?;
/// assert_eq!(values.len(), 2);
/// # Ok(())
/// # }
/// ```
pub struct Buckets<V, C: Codec> {
    core: Arc<Core>,
    codec: C,
    _marker: PhantomData<fn() -> V>,
}

impl<V, C: Codec> Clone for Buckets<V, C> {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
            codec: self.codec.clone(),
            _marker: PhantomData,
        }
    }
}

impl<V, C: Codec> fmt::Debug for Buckets<V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Buckets").finish_non_exhaustive()
    }
}

impl<V, C: Codec> Buckets<V, C> {
    pub(crate) fn new(core: Arc<Core>, codec: C) -> Self {
        Self {
            core,
            codec,
            _marker: PhantomData,
        }
    }
}

impl<V, C> Buckets<V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    /// The values of the keys that exist, by key (`MGET`), Redisson's `get`. Missing keys are left out.
    pub async fn get<K: AsRef<str>>(&self, names: &[K]) -> Result<HashMap<String, V>> {
        if names.is_empty() {
            return Ok(HashMap::new());
        }
        let keys: Vec<&[u8]> = names.iter().map(|name| name.as_ref().as_bytes()).collect();
        let replies = self
            .core
            .query_by_slot(&keys, true, |group| {
                let mut cmd = redis::cmd("MGET");
                for &index in group {
                    cmd.arg(keys[index]);
                }
                cmd
            })
            .await?;
        let mut found = HashMap::new();
        for (group, reply) in replies {
            let values: Vec<Option<Bytes>> = redis::from_redis_value(reply)?;
            for (index, value) in group.into_iter().zip(values) {
                if let Some(bytes) = value {
                    found.insert(
                        names[index].as_ref().to_string(),
                        self.codec.decode(&bytes)?,
                    );
                }
            }
        }
        Ok(found)
    }

    fn encode<'a, K, W>(
        &self,
        entries: impl IntoIterator<Item = (K, &'a W)>,
    ) -> Result<Vec<(String, Bytes)>>
    where
        K: AsRef<str>,
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync + 'a,
    {
        entries
            .into_iter()
            .map(|(name, value)| Ok((name.as_ref().to_string(), self.codec.encode(value)?)))
            .collect()
    }

    /// Sends `build(entries of the group)` for the entries of each group and returns each reply.
    async fn write(
        &self,
        entries: &[(String, Bytes)],
        build: impl Fn(&[usize]) -> redis::Cmd,
    ) -> Result<Vec<redis::Value>> {
        let keys: Vec<&[u8]> = entries.iter().map(|(name, _)| name.as_bytes()).collect();
        let replies = self.core.query_by_slot(&keys, false, build).await?;
        Ok(replies.into_iter().map(|(_, reply)| reply).collect())
    }

    fn pairs(cmd: &mut redis::Cmd, entries: &[(String, Bytes)], group: &[usize]) {
        for &index in group {
            let (name, value) = &entries[index];
            cmd.arg(name).arg(&value[..]);
        }
    }

    /// Stores every value under its key (`MSET`), Redisson's `set`.
    pub async fn set<'a, K, W>(&self, entries: impl IntoIterator<Item = (K, &'a W)>) -> Result<()>
    where
        K: AsRef<str>,
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync + 'a,
    {
        let entries = self.encode(entries)?;
        if entries.is_empty() {
            return Ok(());
        }
        self.write(&entries, |group| {
            let mut cmd = redis::cmd("MSET");
            Self::pairs(&mut cmd, &entries, group);
            cmd
        })
        .await?;
        Ok(())
    }

    /// Stores the values only when none of the keys exists; resolves to whether they were stored. Redisson's `trySet`, or `setIfAllKeysAbsent` with `.expiry(..)`.
    ///
    /// Without [`expiry`](TrySet::expiry) the command is `MSETNX`; with it, `MSETEX … NX` (Redis 8.4 or newer).
    /// In a cluster this holds for each hash slot on its own, and `true` means that the values of at least one slot were stored, as in Redisson.
    #[doc(alias = "trySet", alias = "setIfAllKeysAbsent")]
    pub fn try_set<'a, K, W>(
        &self,
        entries: impl IntoIterator<Item = (K, &'a W)>,
    ) -> TrySet<'_, V, C>
    where
        K: AsRef<str>,
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync + 'a,
    {
        TrySet {
            buckets: self,
            entries: self.encode(entries),
            expiry: None,
        }
    }

    /// Stores the values only when every key exists (`MSETEX … XX`, Redis 8.4 or newer); resolves to whether they were stored, Redisson's `setIfAllKeysExist`.
    /// The keys get no expiry unless [`expiry`](Update::expiry) says otherwise.
    /// In a cluster this holds for each hash slot on its own, as for [`try_set`](Buckets::try_set).
    #[doc(alias = "setIfAllKeysExist")]
    pub fn update<'a, K, W>(
        &self,
        entries: impl IntoIterator<Item = (K, &'a W)>,
    ) -> Update<'_, V, C>
    where
        K: AsRef<str>,
        V: Borrow<W>,
        W: Serialize + ?Sized + Sync + 'a,
    {
        Update {
            buckets: self,
            entries: self.encode(entries),
            expiry: Expiry::None,
        }
    }

    async fn set_nx(&self, entries: Vec<(String, Bytes)>) -> Result<bool> {
        if entries.is_empty() {
            return Ok(false);
        }
        let replies = self
            .write(&entries, |group| {
                let mut cmd = redis::cmd("MSETNX");
                Self::pairs(&mut cmd, &entries, group);
                cmd
            })
            .await?;
        any_stored(replies)
    }

    async fn set_ex(
        &self,
        condition: &str,
        entries: Vec<(String, Bytes)>,
        expiry: Expiry,
    ) -> Result<bool> {
        if entries.is_empty() {
            return Ok(false);
        }
        let expiry = expiry_args(expiry)?;
        let replies = self
            .write(&entries, |group| {
                let mut cmd = redis::cmd("MSETEX");
                cmd.arg(group.len());
                Self::pairs(&mut cmd, &entries, group);
                cmd.arg(condition);
                if let Some((option, value)) = &expiry {
                    cmd.arg(*option);
                    if let Some(value) = value {
                        cmd.arg(*value);
                    }
                }
                cmd
            })
            .await?;
        any_stored(replies)
    }
}

/// The option and argument of `expiry` in `MSETEX`.
fn expiry_args(expiry: Expiry) -> Result<Option<(&'static str, Option<i64>)>> {
    Ok(match expiry {
        Expiry::None => None,
        Expiry::After(ttl) => Some(("PX", Some(millis(ttl)?))),
        Expiry::At(time) => {
            let since = time
                .duration_since(UNIX_EPOCH)
                .map_err(|_| Error::Config("time is before 1970".into()))?;
            let at = i64::try_from(since.as_millis())
                .map_err(|_| Error::Config("time is too far away".into()))?;
            Some(("PXAT", Some(at)))
        }
        Expiry::KeepTtl => Some(("KEEPTTL", None)),
    })
}

/// Whether any command stored its values, Redisson's `BooleanSlotCallback`.
fn any_stored(replies: Vec<redis::Value>) -> Result<bool> {
    let mut stored = false;
    for reply in replies {
        stored |= redis::from_redis_value::<bool>(reply)?;
    }
    Ok(stored)
}

/// A pending [`Buckets::try_set`]. Await it, optionally after [`TrySet::expiry`].
#[must_use = "a set does nothing until it is awaited"]
pub struct TrySet<'b, V, C: Codec> {
    buckets: &'b Buckets<V, C>,
    entries: Result<Vec<(String, Bytes)>>,
    expiry: Option<Expiry>,
}

impl<V, C: Codec> fmt::Debug for TrySet<'_, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TrySet")
            .field("expiry", &self.expiry)
            .finish_non_exhaustive()
    }
}

impl<V, C: Codec> TrySet<'_, V, C> {
    /// Sends `MSETEX … NX` (Redis 8.4 or newer) with this expiry instead of `MSETNX`, Redisson's `setIfAllKeysAbsent`.
    pub fn expiry(mut self, expiry: Expiry) -> Self {
        self.expiry = Some(expiry);
        self
    }
}

impl<'b, V, C> IntoFuture for TrySet<'b, V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'b,
    C: Codec,
{
    type Output = Result<bool>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'b>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let entries = self.entries?;
            match self.expiry {
                None => self.buckets.set_nx(entries).await,
                Some(expiry) => self.buckets.set_ex("NX", entries, expiry).await,
            }
        })
    }
}

/// A pending [`Buckets::update`]. Await it, optionally after [`Update::expiry`].
#[must_use = "an update does nothing until it is awaited"]
pub struct Update<'b, V, C: Codec> {
    buckets: &'b Buckets<V, C>,
    entries: Result<Vec<(String, Bytes)>>,
    expiry: Expiry,
}

impl<V, C: Codec> fmt::Debug for Update<'_, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Update")
            .field("expiry", &self.expiry)
            .finish_non_exhaustive()
    }
}

impl<V, C: Codec> Update<'_, V, C> {
    /// When the written keys expire; [`Expiry::None`], the default, clears their expiry.
    pub fn expiry(mut self, expiry: Expiry) -> Self {
        self.expiry = expiry;
        self
    }
}

impl<'b, V, C> IntoFuture for Update<'b, V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'b,
    C: Codec,
{
    type Output = Result<bool>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'b>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let entries = self.entries?;
            self.buckets.set_ex("XX", entries, self.expiry).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{expiry_args, Expiry};
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn expiry_becomes_the_option_redisson_sends() {
        assert_eq!(expiry_args(Expiry::None).unwrap(), None);
        assert_eq!(
            expiry_args(Expiry::After(Duration::from_millis(1500))).unwrap(),
            Some(("PX", Some(1500)))
        );
        assert_eq!(
            expiry_args(Expiry::At(UNIX_EPOCH + Duration::from_secs(2))).unwrap(),
            Some(("PXAT", Some(2000)))
        );
        assert_eq!(
            expiry_args(Expiry::KeepTtl).unwrap(),
            Some(("KEEPTTL", None))
        );
    }
}
