use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::object::{HasKey, Key};
use bytes::Bytes;
use fred::interfaces::HashesInterface;
use fred::types::scripts::Script;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;
use std::sync::{Arc, LazyLock, Mutex};

const MAX_BITS: f64 = 4_294_967_296.0;

static INIT: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('EXISTS', KEYS[1]) == 1 then
            return 0
        end
        redis.call('HSET', KEYS[1], 'size', ARGV[1], 'hashIterations', ARGV[2], 'expectedInsertions', ARGV[3], 'falseProbability', ARGV[4])
        redis.call('SETBIT', KEYS[2], 0, 0)
        return 1",
    )
});

static INSERT: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('HGET', KEYS[2], 'size') ~= ARGV[1] or redis.call('HGET', KEYS[2], 'hashIterations') ~= ARGV[2] then
            return redis.error_reply('bloom filter config changed')
        end
        local changed = 0
        for i = 3, #ARGV do
            if redis.call('SETBIT', KEYS[1], ARGV[i], 1) == 0 then
                changed = 1
            end
        end
        return changed",
    )
});

static CONTAINS: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('HGET', KEYS[2], 'size') ~= ARGV[1] or redis.call('HGET', KEYS[2], 'hashIterations') ~= ARGV[2] then
            return redis.error_reply('bloom filter config changed')
        end
        for i = 3, #ARGV do
            if redis.call('GETBIT', KEYS[1], ARGV[i]) == 0 then
                return 0
            end
        end
        return 1",
    )
});

#[derive(Clone, Copy)]
struct Config {
    size: u64,
    hash_iterations: u64,
    expected_insertions: u64,
    false_probability: f64,
}

fn fnv1a(bytes: &[u8], basis: u64) -> u64 {
    let mut hash = basis;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn indexes(bytes: &[u8], config: Config) -> Vec<Bytes> {
    let first = mix(fnv1a(bytes, 0xcbf2_9ce4_8422_2325));
    let second = mix(fnv1a(bytes, 0x8422_2325_cbf2_9ce4)) | 1;
    (0..config.hash_iterations)
        .map(|round| {
            let index = first.wrapping_add(round.wrapping_mul(second)) % config.size;
            Bytes::from(index.to_string())
        })
        .collect()
}

fn not_initialised() -> Error {
    Error::Config("the bloom filter is not initialised; call try_init first".into())
}

fn changed(error: Error) -> Error {
    match error {
        Error::Redis(message) if message.contains("bloom filter config changed") => Error::Config(
            "the bloom filter was deleted or created again with other settings".into(),
        ),
        other => other,
    }
}

/// A distributed Bloom filter: it remembers values in a small, fixed amount of memory and answers "possibly seen" or "never seen". It never forgets a value, and it is wrong in the "possibly" direction at most as often as the rate you asked for.
///
/// Call [`try_init`](BloomFilter::try_init) once before use; the settings are stored in Redis and shared by every client. Values are told apart by their encoded bytes. The bits live in the string `{name}` and the settings in the hash `{name}:config`.
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

impl<V, C: Codec> HasKey for BloomFilter<V, C> {
    fn key(&self) -> &Key {
        &self.key
    }

    fn companions(&self) -> Vec<String> {
        vec![self.config_key()]
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
        format!("{}:config", self.key.redis_key())
    }

    fn cached(&self) -> Option<Config> {
        *self.config.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn remember(&self, config: Option<Config>) {
        *self.config.lock().unwrap_or_else(|e| e.into_inner()) = config;
    }

    async fn config(&self) -> Result<Config> {
        if let Some(config) = self.cached() {
            return Ok(config);
        }
        let fields: Vec<Option<String>> = self
            .key
            .core
            .redis()
            .hmget(
                self.config_key(),
                vec![
                    "size",
                    "hashIterations",
                    "expectedInsertions",
                    "falseProbability",
                ],
            )
            .await?;
        let [Some(size), Some(hashes), Some(expected), Some(probability)] = fields.as_slice()
        else {
            return Err(not_initialised());
        };
        let parse = |text: &String| text.parse::<u64>().map_err(|_| not_initialised());
        let config = Config {
            size: parse(size)?,
            hash_iterations: parse(hashes)?,
            expected_insertions: parse(expected)?,
            false_probability: probability.parse().map_err(|_| not_initialised())?,
        };
        self.remember(Some(config));
        Ok(config)
    }

    /// Sets up the filter for `expected_insertions` values and a false positive rate of `false_probability` (for example 0.01 for 1%). Returns `false` and changes nothing when the filter was already set up. The filter needs at most 512 MB.
    pub async fn try_init(&self, expected_insertions: u64, false_probability: f64) -> Result<bool> {
        if expected_insertions == 0 {
            return Err(Error::Config(
                "expected insertions must be at least 1".into(),
            ));
        }
        if !(false_probability > 0.0 && false_probability < 1.0) {
            return Err(Error::Config(
                "false probability must be between 0 and 1".into(),
            ));
        }
        let n = expected_insertions as f64;
        let size = (-n * false_probability.ln() / std::f64::consts::LN_2.powi(2)).ceil();
        if size > MAX_BITS {
            return Err(Error::Config(
                "the filter would need more than 2^32 bits; lower the expected insertions or raise the false probability".into(),
            ));
        }
        let size = size.max(1.0) as u64;
        let hashes = ((size as f64 / n) * std::f64::consts::LN_2)
            .round()
            .max(1.0) as u64;
        let created: i64 = self
            .key
            .core
            .eval(
                &INIT,
                vec![self.config_key(), self.key.redis_key()],
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
        Ok(self.config().await?.expected_insertions)
    }

    /// The false positive rate the filter was set up with.
    pub async fn false_probability(&self) -> Result<f64> {
        Ok(self.config().await?.false_probability)
    }

    /// Number of bits the filter uses.
    pub async fn size_bits(&self) -> Result<u64> {
        Ok(self.config().await?.size)
    }

    /// Number of bits that each value sets.
    pub async fn hash_iterations(&self) -> Result<u64> {
        Ok(self.config().await?.hash_iterations)
    }
}

impl<V, C> BloomFilter<V, C>
where
    V: Serialize + Send + Sync,
    C: Codec,
{
    async fn run<Q>(&self, script: &Script, v: &Q) -> Result<i64>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let config = self.config().await?;
        let mut args = vec![
            Bytes::from(config.size.to_string()),
            Bytes::from(config.hash_iterations.to_string()),
        ];
        args.extend(indexes(&self.codec.encode(v)?, config));
        let outcome = self
            .key
            .core
            .eval(script, vec![self.key.redis_key(), self.config_key()], args)
            .await
            .map_err(changed);
        if outcome.is_err() {
            self.remember(None);
        }
        outcome
    }

    /// Adds a value; returns whether it changed the filter, which means the value was certainly new. `false` means it was possibly there already. The value is borrowed: `filter.insert("a")`.
    pub async fn insert<Q>(&self, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Ok(self.run(&INSERT, v).await? == 1)
    }

    /// Returns `false` when the value was certainly never added, and `true` when it was possibly added.
    pub async fn contains<Q>(&self, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        Ok(self.run(&CONTAINS, v).await? == 1)
    }

    /// Estimated number of values added so far.
    pub async fn count(&self) -> Result<u64> {
        let config = self.config().await?;
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
        if x >= m {
            return Ok(config.expected_insertions);
        }
        Ok((-(m / k) * (1.0 - x / m).ln()).round() as u64)
    }
}
