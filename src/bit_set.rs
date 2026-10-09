use crate::error::{Error, Result};
use crate::object::{HasKey, Key};
use bytes::Bytes;
use fred::types::scripts::Script;
use std::fmt;
use std::sync::LazyLock;

const MAX_INDEX: u64 = (1 << 32) - 1;

static GET: LazyLock<Script> =
    LazyLock::new(|| Script::from_lua("return redis.call('GETBIT', KEYS[1], ARGV[1])"));

static SET: LazyLock<Script> =
    LazyLock::new(|| Script::from_lua("return redis.call('SETBIT', KEYS[1], ARGV[1], ARGV[2])"));

pub(crate) static COUNT: LazyLock<Script> =
    LazyLock::new(|| Script::from_lua("return redis.call('BITCOUNT', KEYS[1])"));

static FIRST_SET: LazyLock<Script> =
    LazyLock::new(|| Script::from_lua("return redis.call('BITPOS', KEYS[1], 1)"));

static LEN: LazyLock<Script> =
    LazyLock::new(|| Script::from_lua("return redis.call('STRLEN', KEYS[1]) * 8"));

fn index_arg(index: u64) -> Result<Bytes> {
    if index > MAX_INDEX {
        return Err(Error::Config(
            "bit index is too large, the maximum is 2^32 - 1".into(),
        ));
    }
    Ok(Bytes::from(index.to_string()))
}

/// A distributed array of bits stored in a Redis string. A missing bit reads as `false`. The largest index is 2^32 - 1.
#[derive(Clone)]
pub struct BitSet {
    key: Key,
}

impl fmt::Debug for BitSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "BitSet")
    }
}

impl HasKey for BitSet {
    fn key(&self) -> &Key {
        &self.key
    }
}

impl BitSet {
    pub(crate) fn new(key: Key) -> Self {
        Self { key }
    }

    /// Returns the bit at `index`.
    pub async fn get(&self, index: u64) -> Result<bool> {
        let bit: i64 = self
            .key
            .core
            .eval(&GET, vec![self.key.redis_key()], vec![index_arg(index)?])
            .await?;
        Ok(bit == 1)
    }

    /// Sets the bit at `index` and returns its previous value. The string grows when needed.
    pub async fn set(&self, index: u64, value: bool) -> Result<bool> {
        let previous: i64 = self
            .key
            .core
            .eval(
                &SET,
                vec![self.key.redis_key()],
                vec![
                    index_arg(index)?,
                    Bytes::from(if value { "1" } else { "0" }),
                ],
            )
            .await?;
        Ok(previous == 1)
    }

    /// Number of bits that are set.
    pub async fn count(&self) -> Result<u64> {
        let count: u64 = self
            .key
            .core
            .eval(&COUNT, vec![self.key.redis_key()], Vec::new())
            .await?;
        Ok(count)
    }

    /// Index of the lowest set bit, or `None` when no bit is set.
    pub async fn first_set(&self) -> Result<Option<u64>> {
        let position: i64 = self
            .key
            .core
            .eval(&FIRST_SET, vec![self.key.redis_key()], Vec::new())
            .await?;
        Ok(u64::try_from(position).ok())
    }

    /// Size of the underlying string in bits, which is always a multiple of 8.
    pub async fn len(&self) -> Result<u64> {
        let bits: u64 = self
            .key
            .core
            .eval(&LEN, vec![self.key.redis_key()], Vec::new())
            .await?;
        Ok(bits)
    }
}
