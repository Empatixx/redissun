use crate::error::Result;
use crate::object::{Key, Object};
use crate::pending::Pending;
use crate::wait::wait_on;
use bytes::Bytes;
use fred::types::scripts::Script;
use std::fmt;
use std::future::Future;
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
            redis.call('PUBLISH', KEYS[2], ARGV[1])
        end
        return value",
    )
});

static TRY_SET_COUNT: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('EXISTS', KEYS[1]) == 0 then
            redis.call('SET', KEYS[1], ARGV[2])
            redis.call('PUBLISH', KEYS[2], ARGV[1])
            return 1
        end
        return 0",
    )
});

static DELETE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('DEL', KEYS[1]) == 1 then
            redis.call('PUBLISH', KEYS[2], ARGV[1])
            return 1
        end
        return 0",
    )
});

fn channel(name: &str) -> String {
    format!("redissun_countdownlatch__channel__{{{name}}}")
}

/// A shared count-down latch, modelled on Redisson's `RCountDownLatch`.
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

impl CountDownLatch {
    pub(crate) fn new(key: Key) -> Self {
        Self { key }
    }

    fn keys(&self) -> Vec<String> {
        vec![self.key.redis_key(), channel(self.key.name())]
    }

    /// Sets the count, but only when the latch does not exist. Returns whether it was set. A count of 0 creates a latch that is already open.
    pub async fn try_set_count(&self, count: u64) -> Result<bool> {
        let set: i64 = self
            .key
            .core
            .eval(
                &TRY_SET_COUNT,
                self.keys(),
                vec![
                    Bytes::from(NEW_COUNT_MESSAGE),
                    Bytes::from(count.to_string()),
                ],
            )
            .await?;
        Ok(set == 1)
    }

    /// Lowers the count by one. At zero the latch is removed and every waiter is released. The command is sent once and never retried, so a timeout cannot count down twice.
    pub async fn count_down(&self) -> Result<()> {
        let _: i64 = self
            .key
            .core
            .eval_no_retry(
                &COUNT_DOWN,
                self.keys(),
                vec![Bytes::from(ZERO_COUNT_MESSAGE)],
            )
            .await?;
        Ok(())
    }

    async fn delete(&self) -> Result<bool> {
        let deleted: i64 = self
            .key
            .core
            .eval(&DELETE, self.keys(), vec![Bytes::from(NEW_COUNT_MESSAGE)])
            .await?;
        Ok(deleted == 1)
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

impl Object for CountDownLatch {
    fn name(&self) -> &str {
        self.key.name()
    }

    fn del(&self) -> impl Future<Output = Result<bool>> + Send {
        self.delete()
    }

    fn exists(&self) -> impl Future<Output = Result<bool>> + Send {
        self.key.exists()
    }

    fn rename(&self, new_name: &str) -> impl Future<Output = Result<()>> + Send {
        self.key.rename(new_name)
    }

    fn expire(&self, ttl: Duration) -> impl Future<Output = Result<bool>> + Send {
        self.key.expire(ttl)
    }

    fn ttl(&self) -> impl Future<Output = Result<Option<Duration>>> + Send {
        self.key.ttl()
    }

    fn persist(&self) -> impl Future<Output = Result<bool>> + Send {
        self.key.persist()
    }
}
