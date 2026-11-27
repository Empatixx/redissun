use crate::error::Result;
use crate::lua::Lua;
use crate::object::{HasKey, Key};
use crate::values::atomic_i64::{
    Comparison, COMPARE_AND_DELETE, GET_AND_DELETE, SET_IF_GREATER, SET_IF_LESS,
};
use bytes::Bytes;
use std::fmt;
use std::sync::LazyLock;

static COMPARE_AND_SET: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local value = redis.call('GET', KEYS[1])
        if (value == false and tonumber(ARGV[1]) == 0) or (tonumber(value) == tonumber(ARGV[1])) then
            redis.call('SET', KEYS[1], ARGV[2])
            return 1
        end
        return 0",
    )
});

/// A shared `f64` stored as a plain number in a Redis string, like Redisson's `RAtomicDouble`. A missing key reads as 0.
#[derive(Clone)]
pub struct AtomicF64 {
    key: Key,
}

impl fmt::Debug for AtomicF64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "AtomicF64")
    }
}

impl HasKey for AtomicF64 {
    fn key(&self) -> &Key {
        &self.key
    }
}

impl AtomicF64 {
    pub(crate) fn new(key: Key) -> Self {
        Self { key }
    }

    async fn flag(&self, script: &Lua, args: [String; 2]) -> Result<bool> {
        let done: i64 = self
            .key
            .core
            .eval(
                script,
                vec![self.key.redis_key()],
                args.into_iter().map(Bytes::from).collect(),
            )
            .await?;
        Ok(done == 1)
    }

    /// Returns the current value, or 0 when the number does not exist.
    pub async fn get(&self) -> Result<f64> {
        let value: Option<f64> = self.key.query(self.key.key_cmd("GET")).await?;
        Ok(value.unwrap_or(0.0))
    }

    /// Sets the value.
    pub async fn set(&self, value: f64) -> Result<()> {
        self.key
            .query::<()>({
                let mut cmd = self.key.key_cmd("SET");
                cmd.arg(value.to_string());
                cmd
            })
            .await?;
        Ok(())
    }

    /// Sets the value and returns the previous one (0 when there was none).
    #[doc(alias = "getAndSet")]
    pub async fn swap(&self, value: f64) -> Result<f64> {
        let previous: Option<f64> = self
            .key
            .query({
                let mut cmd = self.key.key_cmd("GETSET");
                cmd.arg(value.to_string());
                cmd
            })
            .await?;
        Ok(previous.unwrap_or(0.0))
    }

    /// Returns the value and deletes the number (0 when there was none).
    #[doc(alias = "getAndDelete")]
    pub async fn take(&self) -> Result<f64> {
        let previous: Option<f64> = self
            .key
            .core
            .eval(&GET_AND_DELETE, vec![self.key.redis_key()], Vec::new())
            .await?;
        Ok(previous.unwrap_or(0.0))
    }

    /// Sets the value to `update` only when it currently equals `expected`; a missing number equals 0. Returns whether it did.
    pub async fn compare_and_set(&self, expected: f64, update: f64) -> Result<bool> {
        self.flag(&COMPARE_AND_SET, [expected.to_string(), update.to_string()])
            .await
    }

    /// Deletes the number when its value compares to `threshold` as `comparison` says. A missing number is never deleted. Returns whether it was deleted.
    pub async fn compare_and_delete(&self, comparison: Comparison, threshold: f64) -> Result<bool> {
        self.flag(
            &COMPARE_AND_DELETE,
            [threshold.to_string(), comparison.operator().to_string()],
        )
        .await
    }

    /// Sets the value to `value` when the current value (0 when missing) is less than `less`. Returns whether it did.
    pub async fn set_if_less(&self, less: f64, value: f64) -> Result<bool> {
        self.flag(&SET_IF_LESS, [less.to_string(), value.to_string()])
            .await
    }

    /// Sets the value to `value` when the current value (0 when missing) is greater than `greater`. Returns whether it did.
    pub async fn set_if_greater(&self, greater: f64, value: f64) -> Result<bool> {
        self.flag(&SET_IF_GREATER, [greater.to_string(), value.to_string()])
            .await
    }

    /// Adds `delta` and returns the new value.
    #[doc(alias = "addAndGet")]
    pub async fn add(&self, delta: f64) -> Result<f64> {
        let value: f64 = self
            .key
            .query({
                let mut cmd = self.key.key_cmd("INCRBYFLOAT");
                cmd.arg(delta.to_string());
                cmd
            })
            .await?;
        Ok(value)
    }

    /// Adds `delta` and returns the value from before the addition. Use `fetch_add(1.0)` and `fetch_add(-1.0)` for Redisson's `getAndIncrement` and `getAndDecrement`.
    #[doc(
        alias = "getAndAdd",
        alias = "getAndIncrement",
        alias = "getAndDecrement"
    )]
    pub async fn fetch_add(&self, delta: f64) -> Result<f64> {
        Ok(self.add(delta).await? - delta)
    }

    /// Adds 1 and returns the new value.
    pub async fn incr(&self) -> Result<f64> {
        self.add(1.0).await
    }

    /// Subtracts 1 and returns the new value.
    pub async fn decr(&self) -> Result<f64> {
        self.add(-1.0).await
    }
}
