use crate::codec::Codec;
use crate::error::Result;
use crate::object::{HasKey, Key};
use bytes::Bytes;
use fred::types::scripts::Script;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;
use std::sync::LazyLock;

static ADD: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local changed = 0
        local first = 1
        while first <= #ARGV do
            local last = math.min(first + 999, #ARGV)
            if redis.call('PFADD', KEYS[1], unpack(ARGV, first, last)) == 1 then
                changed = 1
            end
            first = last + 1
        end
        return changed",
    )
});

static COUNT: LazyLock<Script> =
    LazyLock::new(|| Script::from_lua("return redis.call('PFCOUNT', unpack(KEYS))"));

static MERGE: LazyLock<Script> =
    LazyLock::new(|| Script::from_lua("redis.call('PFMERGE', KEYS[1], unpack(KEYS)) return 1"));

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
    /// Adds a value; returns whether the estimate changed. The value is borrowed: `log.insert("a")`.
    pub async fn insert<Q>(&self, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let changed: i64 = self
            .key
            .core
            .eval(
                &ADD,
                vec![self.key.redis_key()],
                vec![self.codec.encode(v)?],
            )
            .await?;
        Ok(changed == 1)
    }

    /// Adds many values in one round trip; returns whether the estimate changed.
    pub async fn extend<'a, Q>(&self, values: impl IntoIterator<Item = &'a Q>) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync + 'a,
    {
        let encoded = values
            .into_iter()
            .map(|v| self.codec.encode(v))
            .collect::<Result<Vec<Bytes>>>()?;
        if encoded.is_empty() {
            return Ok(false);
        }
        let changed: i64 = self
            .key
            .core
            .eval(&ADD, vec![self.key.redis_key()], encoded)
            .await?;
        Ok(changed == 1)
    }

    /// Estimated number of different values.
    pub async fn count(&self) -> Result<u64> {
        self.count_with(&[]).await
    }

    /// Estimated number of different values in this log and the logs named in `others`, without changing any of them. In Redis Cluster all names must share a hash slot.
    pub async fn count_with(&self, others: &[&str]) -> Result<u64> {
        let count: u64 = self
            .key
            .core
            .eval(&COUNT, self.names(others), Vec::new())
            .await?;
        Ok(count)
    }

    /// Adds the values of the logs named in `others` to this one. In Redis Cluster all names must share a hash slot.
    pub async fn merge_from(&self, others: &[&str]) -> Result<()> {
        let _: i64 = self
            .key
            .core
            .eval(&MERGE, self.names(others), Vec::new())
            .await?;
        Ok(())
    }

    fn names(&self, others: &[&str]) -> Vec<String> {
        std::iter::once(self.key.redis_key())
            .chain(others.iter().map(|name| name.to_string()))
            .collect()
    }
}
