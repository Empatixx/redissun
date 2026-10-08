use crate::error::Result;
use crate::object::{HasKey, Key};
use bytes::Bytes;
use fred::interfaces::KeysInterface;
use fred::types::scripts::Script;
use std::fmt;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;

const FALLBACK_POLL: Duration = Duration::from_secs(2);
const ZERO_COUNT_MESSAGE: &str = "0";
const NEW_COUNT_MESSAGE: &str = "1";

static TRY_SET_COUNT: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('EXISTS', KEYS[1]) == 0 then
            redis.call('SET', KEYS[1], ARGV[1])
            redis.call('PUBLISH', ARGV[3], ARGV[2])
            return 1
        end
        return 0",
    )
});

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
        f.debug_struct("CountDownLatch")
            .field("name", &self.key.name())
            .finish()
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
        let set: i64 = self
            .key
            .core
            .eval(
                &TRY_SET_COUNT,
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
        let value: Option<i64> = self.key.core.redis().get(self.key.redis_key()).await?;
        Ok(value.unwrap_or(0))
    }

    /// Waits until the count is zero.
    pub async fn wait(&self) -> Result<()> {
        self.wait_inner(None).await?;
        Ok(())
    }

    /// Waits up to `timeout` for the count to reach zero; returns whether it did.
    pub async fn wait_for(&self, timeout: Duration) -> Result<bool> {
        self.wait_inner(Some(timeout)).await
    }

    async fn wait_inner(&self, timeout: Option<Duration>) -> Result<bool> {
        if self.count().await? == 0 {
            return Ok(true);
        }
        if timeout.is_some_and(|timeout| timeout.is_zero()) {
            return Ok(false);
        }

        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        let subscription = self
            .key
            .core
            .pubsub
            .subscribe(&channel(self.key.name()))
            .await?;
        let notify: &Notify = subscription.notify();
        loop {
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if self.count().await? == 0 {
                return Ok(true);
            }

            let pause = match deadline {
                Some(deadline) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Ok(false);
                    }
                    remaining.min(FALLBACK_POLL)
                }
                None => FALLBACK_POLL,
            };
            let _ = tokio::time::timeout(pause, notified).await;
        }
    }
}
