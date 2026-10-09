use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::object::{Key, Object};
use crate::rate_limiter::{suffix_name, EXPIRE_ANY, PERSIST_ANY};
use bytes::Bytes;
use fred::interfaces::{HashesInterface, KeysInterface};
use fred::types::scripts::Script;
use highway::{HighwayHash, HighwayHasher};
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

const MAX_SIZE: u64 = 2 * i32::MAX as u64;

const HASH_KEY: highway::Key = highway::Key([
    0x9e37_79b9_7f4a_7c15,
    0xf39c_c060_5ced_c834,
    0x1082_276b_f3a2_7251,
    0xf86c_6a11_d0c1_8e95,
]);

static INIT: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('EXISTS', KEYS[1]) == 1 then
            return 0
        end
        redis.call('HSET', KEYS[1], 'size', ARGV[1], 'hashIterations', ARGV[2], 'expectedInsertions', ARGV[3], 'falseProbability', ARGV[4])
        return 1",
    )
});

static INSERT: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('HGET', KEYS[1], 'size') ~= ARGV[1] or redis.call('HGET', KEYS[1], 'hashIterations') ~= ARGV[2] then
            return redis.error_reply('Bloom filter config has been changed')
        end
        local k = 0
        local c = 0
        for i = 4, #ARGV do
            if redis.call('SETBIT', KEYS[2], ARGV[i], 1) == 0 then
                k = k + 1
            end
            if ((i - 4) + 1) % ARGV[3] == 0 then
                if k > 0 then
                    c = c + 1
                end
                k = 0
            end
        end
        return c",
    )
});

static CONTAINS: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('HGET', KEYS[1], 'size') ~= ARGV[1] or redis.call('HGET', KEYS[1], 'hashIterations') ~= ARGV[2] then
            return false
        end
        local result = {}
        local k = 0
        local cc = (#ARGV - 3) / ARGV[3]
        for i = 4, #ARGV do
            if redis.call('GETBIT', KEYS[2], ARGV[i]) == 0 then
                k = k + 1
            end
            if ((i - 4) + 1) % cc == 0 then
                if k == 0 then
                    table.insert(result, 1)
                else
                    table.insert(result, 0)
                end
                k = 0
            end
        end
        return result",
    )
});

static RENAME: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local half = #KEYS / 2
        for j = 1, half do
            if redis.call('EXISTS', KEYS[j]) == 1 then
                redis.call('RENAME', KEYS[j], KEYS[half + j])
            else
                redis.call('DEL', KEYS[half + j])
            end
        end
        return 1",
    )
});

#[derive(Clone, Copy)]
struct Config {
    size: u64,
    hash_iterations: u64,
}

fn indexes(bytes: &[u8], config: Config) -> impl Iterator<Item = Bytes> {
    let [first, second] = HighwayHasher::new(HASH_KEY).hash128(bytes);
    let mut hash = first;
    (0..config.hash_iterations).map(move |round| {
        let index = (hash & i64::MAX as u64) % config.size;
        hash = hash.wrapping_add(if round % 2 == 0 { second } else { first });
        Bytes::from(index.to_string())
    })
}

fn not_initialised() -> Error {
    Error::Config("the bloom filter is not initialised; call try_init first".into())
}

fn changed(error: Error) -> Error {
    match error {
        Error::Redis(message) if message.contains("Bloom filter config has been changed") => {
            Error::Config(
                "the bloom filter was deleted or created again with other settings".into(),
            )
        }
        other => other,
    }
}

/// A distributed Bloom filter, modelled on Redisson's `RBloomFilter`: it remembers values in a small, fixed amount of memory and answers "possibly seen" or "never seen". It never forgets a value, and it is wrong in the "possibly" direction at most as often as the rate you asked for.
///
/// Call [`try_init`](BloomFilter::try_init) once before use; the settings are stored in Redis and shared by every client. Values are told apart by their encoded bytes, hashed with HighwayHash under Redisson's key, so the same bytes set the same bits as in Redisson. The bits live in the string `name` and the settings in the hash `{name}:config`.
pub struct BloomFilter<V, C: Codec> {
    key: Key,
    codec: C,
    config: Arc<Mutex<Option<Config>>>,
    _marker: PhantomData<fn() -> V>,
}

impl<V, C: Codec> Clone for BloomFilter<V, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
            config: self.config.clone(),
            _marker: PhantomData,
        }
    }
}

impl<V, C: Codec> fmt::Debug for BloomFilter<V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "BloomFilter")
    }
}

impl<V, C: Codec> BloomFilter<V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self {
            key,
            codec,
            config: Arc::new(Mutex::new(None)),
            _marker: PhantomData,
        }
    }

    fn config_key(&self) -> String {
        suffix_name(self.key.name(), "config")
    }

    fn keys(&self) -> Vec<String> {
        vec![self.key.redis_key(), self.config_key()]
    }

    fn keys_for_scripts(&self) -> Vec<String> {
        vec![self.config_key(), self.key.redis_key()]
    }

    fn cached(&self) -> Option<Config> {
        *self.config.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn remember(&self, config: Option<Config>) {
        *self.config.lock().unwrap_or_else(|e| e.into_inner()) = config;
    }

    async fn read_config(&self) -> Result<Config> {
        let fields: Vec<Option<u64>> = self
            .key
            .core
            .redis()
            .hmget(self.config_key(), vec!["size", "hashIterations"])
            .await?;
        let [Some(size), Some(hash_iterations)] = fields.as_slice() else {
            return Err(not_initialised());
        };
        let config = Config {
            size: *size,
            hash_iterations: *hash_iterations,
        };
        self.remember(Some(config));
        Ok(config)
    }

    async fn config(&self) -> Result<Config> {
        match self.cached() {
            Some(config) => Ok(config),
            None => self.read_config().await,
        }
    }

    async fn setting<T: fred::prelude::FromValue>(&self, field: &str) -> Result<T> {
        let value: Option<T> = self.key.core.redis().hget(self.config_key(), field).await?;
        value.ok_or_else(not_initialised)
    }

    /// Sets up the filter for `expected_insertions` values and a false positive rate of `false_probability` (for example 0.01 for 1%). Returns `false` and changes nothing when the filter was already set up.
    ///
    /// The number of bits is `floor(-n ln p / ln² 2)` and must be between 1 and 2 × (2³¹ − 1); the number of hashes is `round(bits / n × ln 2)`, at least 1.
    pub async fn try_init(&self, expected_insertions: u64, false_probability: f64) -> Result<bool> {
        if false_probability > 1.0 {
            return Err(Error::Config(
                "false probability can't be greater than 1".into(),
            ));
        }
        if false_probability < 0.0 || false_probability.is_nan() {
            return Err(Error::Config("false probability can't be negative".into()));
        }
        let probability = if false_probability == 0.0 {
            f64::from_bits(1)
        } else {
            false_probability
        };
        let n = expected_insertions as f64;
        let bits = -n * probability.ln() / (std::f64::consts::LN_2 * std::f64::consts::LN_2);
        if bits < 1.0 || bits.is_nan() {
            return Err(Error::Config(
                "the calculated size of the filter is 0".into(),
            ));
        }
        if bits > MAX_SIZE as f64 {
            return Err(Error::Config(format!(
                "the filter can't have more than {MAX_SIZE} bits, but would need {}",
                bits as u64
            )));
        }
        let size = bits as u64;
        let hashes = ((size as f64 / n) * std::f64::consts::LN_2)
            .round()
            .max(1.0) as u64;
        let created: i64 = self
            .key
            .core
            .eval(
                &INIT,
                vec![self.config_key()],
                vec![
                    Bytes::from(size.to_string()),
                    Bytes::from(hashes.to_string()),
                    Bytes::from(expected_insertions.to_string()),
                    Bytes::from(false_probability.to_string()),
                ],
            )
            .await?;
        self.remember(None);
        Ok(created == 1)
    }

    /// The `expected_insertions` the filter was set up with.
    pub async fn expected_insertions(&self) -> Result<u64> {
        self.setting("expectedInsertions").await
    }

    /// The false positive rate the filter was set up with.
    pub async fn false_probability(&self) -> Result<f64> {
        self.setting("falseProbability").await
    }

    /// Number of bits the filter uses.
    pub async fn size_bits(&self) -> Result<u64> {
        self.setting("size").await
    }

    /// Number of bits that each value sets.
    pub async fn hash_iterations(&self) -> Result<u64> {
        self.setting("hashIterations").await
    }

    async fn delete_all(&self) -> Result<bool> {
        let removed: i64 = self.key.core.redis().del(self.keys()).await?;
        Ok(removed > 0)
    }

    async fn exists_any(&self) -> Result<bool> {
        let found: i64 = self.key.core.redis().exists(self.keys()).await?;
        Ok(found > 0)
    }

    async fn rename_all(&self, new_name: &str) -> Result<()> {
        let mut keys = self.keys();
        keys.push(new_name.to_string());
        keys.push(suffix_name(new_name, "config"));
        let _: i64 = self.key.core.eval(&RENAME, keys, Vec::new()).await?;
        Ok(())
    }

    async fn expire_all(&self, ttl: Duration) -> Result<bool> {
        let applied: i64 = self
            .key
            .core
            .eval(
                &EXPIRE_ANY,
                self.keys(),
                vec![Bytes::from(crate::object::millis(ttl)?.to_string())],
            )
            .await?;
        Ok(applied == 1)
    }

    async fn persist_all(&self) -> Result<bool> {
        let applied: i64 = self
            .key
            .core
            .eval(&PERSIST_ANY, self.keys(), Vec::new())
            .await?;
        Ok(applied == 1)
    }
}

impl<V, C> BloomFilter<V, C>
where
    V: Serialize + Send + Sync,
    C: Codec,
{
    fn arguments<Q>(&self, values: &[&Q], config: Config, per_value: u64) -> Result<Vec<Bytes>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let mut args = vec![
            Bytes::from(config.size.to_string()),
            Bytes::from(config.hash_iterations.to_string()),
            Bytes::from(per_value.to_string()),
        ];
        for value in values {
            args.extend(indexes(&self.codec.encode(*value)?, config));
        }
        Ok(args)
    }

    /// Adds a value; returns whether it changed the filter, which means the value was certainly new. `false` means it was possibly there already. The value is borrowed: `filter.insert("a")`.
    pub async fn insert<Q>(&self, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Ok(self.insert_all(&[v]).await? > 0)
    }

    /// Adds several values; returns how many of them changed the filter, which means they were certainly new.
    pub async fn insert_all<Q>(&self, values: &[&Q]) -> Result<u64>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if values.is_empty() {
            return Ok(0);
        }
        let config = self.config().await?;
        let args = self.arguments(values, config, config.hash_iterations)?;
        let outcome = self
            .key
            .core
            .eval(&INSERT, self.keys_for_scripts(), args)
            .await
            .map_err(changed);
        if outcome.is_err() {
            self.remember(None);
        }
        outcome
    }

    /// Returns `false` when the value was certainly never added, and `true` when it was possibly added.
    pub async fn contains<Q>(&self, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Ok(self.contains_each(&[v]).await?[0])
    }

    /// Returns how many of `values` were possibly added.
    pub async fn contains_all<Q>(&self, values: &[&Q]) -> Result<u64>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let found = self.contains_each(values).await?;
        Ok(found.into_iter().filter(|found| *found).count() as u64)
    }

    /// Answers [`contains`](BloomFilter::contains) for each of `values`, in order.
    ///
    /// A filter that is not set up, or was set up again with other settings since this handle last looked, contains nothing.
    pub async fn contains_each<Q>(&self, values: &[&Q]) -> Result<Vec<bool>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if values.is_empty() {
            return Ok(Vec::new());
        }
        let config = match self.config().await {
            Ok(config) => config,
            Err(Error::Config(_)) => return Ok(vec![false; values.len()]),
            Err(error) => return Err(error),
        };
        let args = self.arguments(values, config, values.len() as u64)?;
        let found: Option<Vec<i64>> = self
            .key
            .core
            .eval(&CONTAINS, self.keys_for_scripts(), args)
            .await?;
        match found {
            Some(found) => Ok(found.into_iter().map(|bit| bit == 1).collect()),
            None => {
                self.remember(None);
                Ok(vec![false; values.len()])
            }
        }
    }

    /// Estimated number of values added so far, `round(-m / k × ln(1 - x / m))` for `m` bits, `k` hashes and `x` bits set.
    pub async fn count(&self) -> Result<u64> {
        let config = self.read_config().await?;
        let set: u64 = self
            .key
            .core
            .eval(
                &crate::bit_set::COUNT,
                vec![self.key.redis_key()],
                Vec::new(),
            )
            .await?;
        let (m, k, x) = (
            config.size as f64,
            config.hash_iterations as f64,
            set as f64,
        );
        let estimate = (-m / k * (1.0 - x / m).ln()).round();
        Ok(estimate.min(i64::MAX as f64) as u64)
    }
}

impl<V, C> Object for BloomFilter<V, C>
where
    V: Send + Sync,
    C: Codec,
{
    fn name(&self) -> &str {
        self.key.name()
    }

    fn del(&self) -> impl Future<Output = Result<bool>> + Send {
        self.delete_all()
    }

    fn exists(&self) -> impl Future<Output = Result<bool>> + Send {
        self.exists_any()
    }

    fn rename(&self, new_name: &str) -> impl Future<Output = Result<()>> + Send {
        self.rename_all(new_name)
    }

    fn expire(&self, ttl: Duration) -> impl Future<Output = Result<bool>> + Send {
        self.expire_all(ttl)
    }

    fn ttl(&self) -> impl Future<Output = Result<Option<Duration>>> + Send {
        self.key.ttl()
    }

    fn persist(&self) -> impl Future<Output = Result<bool>> + Send {
        self.persist_all()
    }
}

#[cfg(test)]
mod tests {
    use super::{indexes, Config, HighwayHash, HighwayHasher, HASH_KEY};

    #[test]
    fn hashes_match_redisson() {
        let cases: [(&[u8], [u64; 2]); 4] = [
            (b"", [13234629882879058795, 12886175836324478118]),
            (b"\"123\"", [14648179893508101146, 3380708970600393408]),
            (
                b"0123456789abcdef0123456789abcdef",
                [15260565878964644349, 14961286810610642008],
            ),
            (
                b"0123456789abcdef0123456789abcdef0123456789abcdefXYZ",
                [8917617231722318317, 2702148571249333176],
            ),
        ];
        for (bytes, expected) in cases {
            assert_eq!(HighwayHasher::new(HASH_KEY).hash128(bytes), expected);
        }
    }

    #[test]
    fn bit_positions_match_redisson() {
        let config = Config {
            size: 958,
            hash_iterations: 7,
        };
        let positions: Vec<_> = indexes(b"\"123\"", config).collect();
        assert_eq!(positions, ["136", "940", "156", "2", "176", "22", "196"]);
    }
}
