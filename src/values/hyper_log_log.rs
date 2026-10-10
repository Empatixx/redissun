use crate::codec::Codec;
use crate::error::Result;
use crate::object::{HasKey, Key};
use crate::reply::number;
use fred::types::Value;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;

/// A distributed estimate of how many different values were added, stored in a Redis HyperLogLog. It uses about 12 kB however many values there are, and the estimate is off by less than 1%. Values are told apart by their encoded bytes.
pub struct HyperLogLog<V, C: Codec> {
    key: Key,
    codec: C,
    _marker: PhantomData<fn() -> V>,
}

impl<V, C: Codec> Clone for HyperLogLog<V, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
            _marker: PhantomData,
        }
    }
}

impl<V, C: Codec> fmt::Debug for HyperLogLog<V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "HyperLogLog")
    }
}

impl<V, C: Codec> HasKey for HyperLogLog<V, C> {
    fn key(&self) -> &Key {
        &self.key
    }
}

impl<V, C: Codec> HyperLogLog<V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self {
            key,
            codec,
            _marker: PhantomData,
        }
    }
}

impl<V, C> HyperLogLog<V, C>
where
    V: Serialize + Send + Sync,
    C: Codec,
{
    /// Adds a value (`PFADD`); returns whether the estimate changed. The value is borrowed: `log.insert("a")`.
    pub async fn insert<Q>(&self, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.add(vec![Value::Bytes(self.codec.encode(v)?)]).await
    }

    /// Adds many values with one `PFADD`; returns whether the estimate changed, or the key was created.
    pub async fn extend<'a, Q>(&self, values: impl IntoIterator<Item = &'a Q>) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync + 'a,
    {
        let encoded = values
            .into_iter()
            .map(|v| Ok(Value::Bytes(self.codec.encode(v)?)))
            .collect::<Result<Vec<Value>>>()?;
        self.add(encoded).await
    }

    async fn add(&self, values: Vec<Value>) -> Result<bool> {
        let args = std::iter::once(Value::from(self.key.redis_key()))
            .chain(values)
            .collect();
        let reply = self.key.command("PFADD", args, 0).await?;
        Ok(number(&reply) == Some(1))
    }

    /// Estimated number of different values (`PFCOUNT`).
    pub async fn count(&self) -> Result<u64> {
        self.count_with(&[]).await
    }

    /// Estimated number of different values in this log and the logs named in `others` together, without changing any of them (`PFCOUNT`). In Redis Cluster all names must share a hash slot.
    pub async fn count_with(&self, others: &[&str]) -> Result<u64> {
        let reply = self.key.command("PFCOUNT", self.names(others), 0).await?;
        Ok(number(&reply).unwrap_or(0) as u64)
    }

    /// Merges the logs named in `others` into this one (`PFMERGE`). In Redis Cluster all names must share a hash slot.
    pub async fn merge_from(&self, others: &[&str]) -> Result<()> {
        self.key.command("PFMERGE", self.names(others), 0).await?;
        Ok(())
    }

    fn names(&self, others: &[&str]) -> Vec<Value> {
        std::iter::once(self.key.redis_key())
            .chain(others.iter().map(|name| name.to_string()))
            .map(Value::from)
            .collect()
    }
}
