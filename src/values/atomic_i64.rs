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

static COMPARE_AND_DELETE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local current = redis.call('GET', KEYS[1])
        if current == false then
            return 0
        end
        current = tonumber(current)
        local threshold = tonumber(ARGV[1])
        local op = ARGV[2]
        local match = false
        if op == '<' then match = current < threshold
        elseif op == '<=' then match = current <= threshold
        elseif op == '>' then match = current > threshold
        elseif op == '>=' then match = current >= threshold
        elseif op == '==' then match = current == threshold
        elseif op == '~=' then match = current ~= threshold
        end
        if match then
            redis.call('DEL', KEYS[1])
            return 1
        end
        return 0",
    )
});

static GET_AND_DELETE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local current = redis.call('GET', KEYS[1])
        redis.call('DEL', KEYS[1])
        return current",
    )
});

static SET_IF_LESS: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local current = redis.call('GET', KEYS[1])
        current = current == false and 0 or tonumber(current)
        if current < tonumber(ARGV[1]) then
            redis.call('SET', KEYS[1], ARGV[2])
            return 1
        end
        return 0",
    )
});

static SET_IF_GREATER: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local current = redis.call('GET', KEYS[1])
        current = current == false and 0 or tonumber(current)
        if current > tonumber(ARGV[1]) then
            redis.call('SET', KEYS[1], ARGV[2])
            return 1
        end
        return 0",
    )
});

/// How [`AtomicI64::compare_and_delete`] compares the stored value with the threshold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Comparison {
    /// The value is less than the threshold.
    Less,
    /// The value is less than or equal to the threshold.
    LessOrEqual,
    /// The value is greater than the threshold.
    Greater,
    /// The value is greater than or equal to the threshold.
    GreaterOrEqual,
    /// The value equals the threshold.
    Equal,
    /// The value differs from the threshold.
    NotEqual,
}

impl Comparison {
    fn operator(self) -> &'static str {
        match self {
            Comparison::Less => "<",
            Comparison::LessOrEqual => "<=",
            Comparison::Greater => ">",
            Comparison::GreaterOrEqual => ">=",
            Comparison::Equal => "==",
            Comparison::NotEqual => "~=",
        }
    }
}

/// A shared 64-bit counter stored as a plain integer in a Redis string, like Redisson's `RAtomicLong`. A missing key reads as 0.
#[derive(Clone)]
pub struct AtomicI64 {
    key: Key,
}

impl fmt::Debug for AtomicI64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "AtomicI64")
    }
}

impl HasKey for AtomicI64 {
    fn key(&self) -> &Key {
        &self.key
    }
}

impl AtomicI64 {
    pub(crate) fn new(key: Key) -> Self {
        Self { key }
    }

    async fn flag(&self, script: &Script, args: Vec<Bytes>) -> Result<bool> {
        let done: i64 = self
            .key
            .core
            .eval(script, vec![self.key.redis_key()], args)
            .await?;
        Ok(done == 1)
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

    /// Sets the value and returns the previous one (0 when there was none).
    pub async fn get_and_set(&self, value: i64) -> Result<i64> {
        let previous: Option<i64> = self
            .key
            .core
            .redis()
            .getset(self.key.redis_key(), value)
            .await?;
        Ok(previous.unwrap_or(0))
    }

    /// Returns the value and deletes the counter (0 when there was none).
    pub async fn get_and_delete(&self) -> Result<i64> {
        let previous: Option<i64> = self
            .key
            .core
            .eval(&GET_AND_DELETE, vec![self.key.redis_key()], Vec::new())
            .await?;
        Ok(previous.unwrap_or(0))
    }

    /// Sets the value to `update` only when it currently equals `expected`; a missing counter equals 0. Returns whether it did.
    pub async fn compare_and_set(&self, expected: i64, update: i64) -> Result<bool> {
        self.flag(
            &COMPARE_AND_SET,
            vec![
                Bytes::from(expected.to_string()),
                Bytes::from(update.to_string()),
            ],
        )
        .await
    }

    /// Deletes the counter when its value compares to `threshold` as `comparison` says. A missing counter is never deleted. Returns whether it was deleted.
    pub async fn compare_and_delete(&self, comparison: Comparison, threshold: i64) -> Result<bool> {
        self.flag(
            &COMPARE_AND_DELETE,
            vec![
                Bytes::from(threshold.to_string()),
                Bytes::from(comparison.operator()),
            ],
        )
        .await
    }

    /// Sets the value to `value` when the current value (0 when missing) is less than `less`. Returns whether it did.
    pub async fn set_if_less(&self, less: i64, value: i64) -> Result<bool> {
        self.flag(
            &SET_IF_LESS,
            vec![
                Bytes::from(less.to_string()),
                Bytes::from(value.to_string()),
            ],
        )
        .await
    }

    /// Sets the value to `value` when the current value (0 when missing) is greater than `greater`. Returns whether it did.
    pub async fn set_if_greater(&self, greater: i64, value: i64) -> Result<bool> {
        self.flag(
            &SET_IF_GREATER,
            vec![
                Bytes::from(greater.to_string()),
                Bytes::from(value.to_string()),
            ],
        )
        .await
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

    /// Adds 1 and returns the value from before.
    pub async fn get_and_incr(&self) -> Result<i64> {
        self.get_and_add(1).await
    }

    /// Subtracts 1 and returns the value from before.
    pub async fn get_and_decr(&self) -> Result<i64> {
        self.get_and_add(-1).await
    }
}
