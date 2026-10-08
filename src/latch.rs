use crate::error::{Error, Result};
use crate::object::{HasKey, Key};
use crate::pending::Pending;
use crate::wait::{wait_on, SET_IF_ABSENT_AND_PUBLISH};
use bytes::Bytes;
use fred::types::scripts::Script;
use std::fmt;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::time::Instant;

const ZERO_COUNT_MESSAGE: &str = "0";
const NEW_COUNT_MESSAGE: &str = "1";

static COUNT_DOWN: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local value = redis.call('DECR', KEYS[1])
        if value <= 0 then
            redis.call('DEL', KEYS[1])
        end
        if value == 0 then
            redis.call('PUBLISH', ARGV[2], ARGV[1])
        end
        return value",
    )
});

fn channel(name: &str) -> String {
    format!("redissun_countdownlatch__channel__{name}")
}

/// A shared count-down latch, as in Redisson's `RCountDownLatch`.
///
/// Set a count with [`CountDownLatch::try_set_count`], call [`CountDownLatch::count_down`] from any process, and let any number of processes [`CountDownLatch::wait`] until the count reaches zero. A latch that does not exist counts as open.
#[derive(Clone)]
pub struct CountDownLatch {
    key: Key,
}

impl fmt::Debug for CountDownLatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "CountDownLatch")
    }
}

impl HasKey for CountDownLatch {
    fn key(&self) -> &Key {
        &self.key
    }
}

impl CountDownLatch {
    pub(crate) fn new(key: Key) -> Self {
        Self { key }
    }

    /// Sets the count, but only when the latch does not exist. Returns whether it was set.
    pub async fn try_set_count(&self, count: u64) -> Result<bool> {
        if count == 0 {
            return Err(Error::Config("count must be positive".into()));
        }
        let set: i64 = self
            .key
            .core
            .eval(
                &SET_IF_ABSENT_AND_PUBLISH,
                vec![self.key.redis_key()],
                vec![
                    Bytes::from(count.to_string()),
                    Bytes::from(NEW_COUNT_MESSAGE),
                    Bytes::from(channel(self.key.name())),
                ],
            )
            .await?;
        Ok(set == 1)
    }

    /// Lowers the count by one. At zero the latch is removed and every waiter is released.
    pub async fn count_down(&self) -> Result<()> {
        let _: i64 = self
            .key
            .core
            .eval(
                &COUNT_DOWN,
                vec![self.key.redis_key()],
                vec![
                    Bytes::from(ZERO_COUNT_MESSAGE),
                    Bytes::from(channel(self.key.name())),
                ],
            )
            .await?;
        Ok(())
    }

    /// The current count. A missing latch has 0.
    pub async fn count(&self) -> Result<i64> {
        self.key.get_i64_or_zero().await
    }

    /// Waits until the count is zero. Add `.timeout(duration)` to wait at most that long.
    pub fn wait(&self) -> Pending<'_, ()> {
        Pending::new(
            move |timeout| async move { Ok(self.wait_inner(timeout).await?.then_some(())) },
        )
    }

    async fn wait_inner(&self, timeout: Option<Duration>) -> Result<bool> {
        if self.count().await? == 0 {
            return Ok(true);
        }
        if timeout.is_some_and(|timeout| timeout.is_zero()) {
            return Ok(false);
        }

        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        let opened = wait_on(
            &self.key.core,
            &channel(self.key.name()),
            deadline,
            || async { Ok((self.count().await? == 0).then_some(())) },
        )
        .await?;
        Ok(opened.is_some())
    }
}
