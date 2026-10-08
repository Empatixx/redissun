use crate::error::Result;
use crate::object::{HasKey, Key};
use bytes::Bytes;
use fred::interfaces::KeysInterface;
use fred::types::scripts::Script;
use std::fmt;
use std::sync::LazyLock;

static COMPARE_AND_SET: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local current = redis.call('GET', KEYS[1])
        if current == ARGV[1] or (tonumber(ARGV[1]) == 0 and current == false) then
            redis.call('SET', KEYS[1], ARGV[2])
            return 1
        end
        return 0",
    )
});

/// A shared 64-bit counter stored as a plain integer in a Redis string. A missing key reads as 0.
#[derive(Clone)]
pub struct AtomicLong {
    key: Key,
}

impl fmt::Debug for AtomicLong {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "AtomicLong")
    }
}

impl HasKey for AtomicLong {
    fn key(&self) -> &Key {
        &self.key
    }
}

impl AtomicLong {
    pub(crate) fn new(key: Key) -> Self {
        Self { key }
    }

    /// Returns the current value, or 0 when the counter does not exist.
    pub async fn get(&self) -> Result<i64> {
        self.key.get_i64_or_zero().await
    }

    /// Sets the value.
    pub async fn set(&self, value: i64) -> Result<()> {
        self.key
            .core
            .redis()
            .set::<(), _, _>(self.key.redis_key(), value, None, None, false)
            .await?;
        Ok(())
    }

    /// Sets the value and returns the previous one (0 when there was none). Needs Redis 6.2 or newer.
    pub async fn get_and_set(&self, value: i64) -> Result<i64> {
        let previous: Option<i64> = self
            .key
            .core
            .redis()
            .set(self.key.redis_key(), value, None, None, true)
            .await?;
        Ok(previous.unwrap_or(0))
    }

    /// Returns the value and deletes the counter (0 when there was none). Needs Redis 6.2 or newer.
    pub async fn get_and_delete(&self) -> Result<i64> {
        let previous: Option<i64> = self.key.core.redis().getdel(self.key.redis_key()).await?;
        Ok(previous.unwrap_or(0))
    }

    /// Sets the value to `update` only when it currently equals `expected`; a missing counter equals 0. Returns whether it did.
    pub async fn compare_and_set(&self, expected: i64, update: i64) -> Result<bool> {
        let swapped: i64 = self
            .key
            .core
            .eval(
                &COMPARE_AND_SET,
                vec![self.key.redis_key()],
                vec![
                    Bytes::from(expected.to_string()),
                    Bytes::from(update.to_string()),
                ],
            )
            .await?;
        Ok(swapped == 1)
    }

    /// Adds `delta` and returns the new value.
    pub async fn add_and_get(&self, delta: i64) -> Result<i64> {
        let value: i64 = self
            .key
            .core
            .redis()
            .incr_by(self.key.redis_key(), delta)
            .await?;
        Ok(value)
    }

    /// Adds `delta` and returns the value from before the addition.
    pub async fn get_and_add(&self, delta: i64) -> Result<i64> {
        Ok(self.add_and_get(delta).await?.wrapping_sub(delta))
    }

    /// Adds 1 and returns the new value.
    pub async fn incr(&self) -> Result<i64> {
        let value: i64 = self.key.core.redis().incr(self.key.redis_key()).await?;
        Ok(value)
    }

    /// Subtracts 1 and returns the new value.
    pub async fn decr(&self) -> Result<i64> {
        let value: i64 = self.key.core.redis().decr(self.key.redis_key()).await?;
        Ok(value)
    }
}
