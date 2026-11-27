use crate::error::{Error, Result};
use crate::lua::Lua;
use crate::object::suffix_name;
use crate::object::{HasKey, Key};
use bytes::Bytes;
use std::fmt;
use std::ops::Range;
use std::sync::{Arc, LazyLock};
use tokio::sync::Mutex;

static TRY_INIT: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "redis.call('SETNX', KEYS[1], ARGV[1])
        return redis.call('SETNX', KEYS[2], ARGV[2])",
    )
});

static ALLOCATE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local allocationSize = redis.call('GET', KEYS[2])
        if allocationSize == false then
            allocationSize = 5000
            redis.call('SET', KEYS[2], allocationSize)
        end
        local value = redis.call('GET', KEYS[1])
        if value == false then
            redis.call('INCR', KEYS[1])
            value = 1
        end
        redis.call('INCRBY', KEYS[1], allocationSize)
        return {tonumber(value), tonumber(allocationSize)}",
    )
});

/// Hands out unique, growing `i64` ids, like Redisson's `RIdGenerator`.
///
/// The generator takes a block of ids from Redis at a time and hands them out from memory, so most calls need no round trip. Ids are unique across all programs but not in one unbroken sequence: a program that stops throws away the rest of its block. The next free id lives in the string `name` and the block size in `{name}:allocation`. Clones share one block.
#[derive(Clone)]
pub struct IdGenerator {
    key: Key,
    block: Arc<Mutex<Range<i64>>>,
}

impl fmt::Debug for IdGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "IdGenerator")
    }
}

impl HasKey for IdGenerator {
    fn key(&self) -> &Key {
        &self.key
    }

    fn companions(&self) -> Vec<String> {
        vec![suffix_name(self.key.name(), "allocation")]
    }

    fn renamed_companions(&self, new_name: &str) -> Option<Vec<String>> {
        Some(vec![suffix_name(new_name, "allocation")])
    }
}

impl IdGenerator {
    pub(crate) fn new(key: Key) -> Self {
        Self {
            key,
            block: Arc::default(),
        }
    }

    fn keys(&self) -> Vec<String> {
        std::iter::once(self.key.redis_key())
            .chain(self.companions())
            .collect()
    }

    /// Sets the first id and how many ids one block holds, but only when they are not set yet. Returns whether the block size was set. Without it the ids start at 1 and a block holds 5000.
    #[doc(alias = "tryInit")]
    pub async fn init(&self, first: i64, allocation_size: i64) -> Result<bool> {
        if allocation_size <= 0 {
            return Err(Error::Config("allocation size must be positive".into()));
        }
        let set: i64 = self
            .key
            .core
            .eval_once(
                &TRY_INIT,
                self.keys(),
                [first, allocation_size]
                    .map(|number| Bytes::from(number.to_string()))
                    .to_vec(),
            )
            .await?;
        Ok(set == 1)
    }

    /// Returns the next id. It asks Redis only when the current block is used up.
    pub async fn next_id(&self) -> Result<i64> {
        let mut block = self.block.lock().await;
        if let Some(id) = block.next() {
            return Ok(id);
        }
        let (first, size): (i64, i64) = self
            .key
            .core
            .eval(&ALLOCATE, self.keys(), Vec::new())
            .await?;
        *block = first..first.saturating_add(size);
        block
            .next()
            .ok_or_else(|| Error::Redis("the id generator has an empty block".into()))
    }
}
