use crate::error::{Error, Result};
use crate::object::{HasKey, Key};
use crate::shield::shielded;
use crate::wait::{wait_on, SET_IF_ABSENT_AND_PUBLISH};
use bytes::Bytes;
use fred::types::scripts::Script;
use std::fmt;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::time::Instant;

static TRY_ACQUIRE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local value = redis.call('GET', KEYS[1])
        if value ~= false and tonumber(value) >= tonumber(ARGV[1]) then
            redis.call('DECRBY', KEYS[1], ARGV[1])
            return 1
        end
        return 0",
    )
});

static RELEASE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local value = redis.call('INCRBY', KEYS[1], ARGV[1])
        redis.call('PUBLISH', ARGV[2], value)
        return value",
    )
});

static ADD_PERMITS: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local value = redis.call('GET', KEYS[1])
        if value == false then
            value = 0
        end
        local total = tonumber(value) + tonumber(ARGV[1])
        redis.call('SET', KEYS[1], total)
        redis.call('PUBLISH', ARGV[2], total)
        return total",
    )
});

static DRAIN: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local value = redis.call('GET', KEYS[1])
        if value == false then
            return 0
        end
        redis.call('SET', KEYS[1], 0)
        return tonumber(value)",
    )
});

fn channel(name: &str) -> String {
    format!("redissun_sc:{name}")
}

fn positive(permits: u64) -> Result<Bytes> {
    if permits == 0 {
        return Err(Error::Config("permits must be positive".into()));
    }
    Ok(Bytes::from(permits.to_string()))
}

/// A shared counting semaphore, as in Redisson's `RSemaphore`.
///
/// The permits live in one Redis string. They have no owner, so permits held by a process that crashes are lost until someone releases or resets them.
#[derive(Clone)]
pub struct Semaphore {
    key: Key,
}

impl fmt::Debug for Semaphore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "Semaphore")
    }
}

impl HasKey for Semaphore {
    fn key(&self) -> &Key {
        &self.key
    }
}

async fn release_permits(key: &Key, permits: Bytes) -> Result<i64> {
    key.core
        .eval(
            &RELEASE,
            vec![key.redis_key()],
            vec![permits, Bytes::from(channel(key.name()))],
        )
        .await
}

impl Semaphore {
    pub(crate) fn new(key: Key) -> Self {
        Self { key }
    }

    /// Sets the number of permits, but only when the semaphore does not exist yet. Returns whether it was set.
    pub async fn try_set_permits(&self, permits: u64) -> Result<bool> {
        let set: i64 = self
            .key
            .core
            .eval(
                &SET_IF_ABSENT_AND_PUBLISH,
                vec![self.key.redis_key()],
                vec![
                    Bytes::from(permits.to_string()),
                    Bytes::from(permits.to_string()),
                    Bytes::from(channel(self.key.name())),
                ],
            )
            .await?;
        Ok(set == 1)
    }

    /// Adds permits, creating the semaphore when needed, and wakes waiters.
    pub async fn add_permits(&self, permits: u64) -> Result<()> {
        let _: i64 = self
            .key
            .core
            .eval(
                &ADD_PERMITS,
                vec![self.key.redis_key()],
                vec![positive(permits)?, Bytes::from(channel(self.key.name()))],
            )
            .await?;
        Ok(())
    }

    /// Number of permits that can be acquired right now. A missing semaphore has 0.
    pub async fn available_permits(&self) -> Result<i64> {
        self.key.get_i64_or_zero().await
    }

    /// Takes every available permit and returns how many there were.
    pub async fn drain_permits(&self) -> Result<i64> {
        self.key
            .core
            .eval(&DRAIN, vec![self.key.redis_key()], Vec::new())
            .await
    }

    /// Returns `permits` to the semaphore by hand and wakes waiters. Use it for permits that were `forget`-ed.
    pub async fn release(&self, permits: u64) -> Result<()> {
        release_permits(&self.key, positive(permits)?).await?;
        Ok(())
    }

    /// Waits until `permits` are available and takes them.
    pub async fn acquire(&self, permits: u64) -> Result<Permits> {
        self.acquire_inner(permits, None)
            .await?
            .ok_or(Error::Timeout)
    }

    /// Takes `permits` if they are available right now; returns `None` otherwise.
    pub async fn try_acquire(&self, permits: u64) -> Result<Option<Permits>> {
        self.acquire_inner(permits, Some(Duration::ZERO)).await
    }

    /// Waits up to `wait` for `permits`; returns `None` when the time runs out.
    pub async fn acquire_for(&self, permits: u64, wait: Duration) -> Result<Option<Permits>> {
        self.acquire_inner(permits, Some(wait)).await
    }

    async fn try_take(&self, permits: u64, argument: &Bytes) -> Result<Option<Permits>> {
        let key = self.key.clone();
        let argument = argument.clone();
        shielded(async move {
            let taken: i64 = key
                .core
                .eval(&TRY_ACQUIRE, vec![key.redis_key()], vec![argument])
                .await?;
            Ok((taken == 1).then(|| Permits::new(key, permits)))
        })
        .await
    }

    async fn acquire_inner(&self, permits: u64, wait: Option<Duration>) -> Result<Option<Permits>> {
        let argument = positive(permits)?;
        if let Some(taken) = self.try_take(permits, &argument).await? {
            return Ok(Some(taken));
        }
        if wait.is_some_and(|wait| wait.is_zero()) {
            return Ok(None);
        }

        let deadline = wait.map(|wait| Instant::now() + wait);
        wait_on(&self.key.core, &channel(self.key.name()), deadline, || {
            self.try_take(permits, &argument)
        })
        .await
    }
}

struct Held {
    key: Key,
    count: u64,
    runtime: Option<Handle>,
}

/// Permits taken from a [`Semaphore`]. Dropping the value returns them in the background; call [`Permits::release`] to return them and see errors, or [`Permits::forget`] to keep them taken.
#[must_use = "dropping the permits returns them at once; call forget() to keep them taken"]
pub struct Permits {
    held: Option<Held>,
}

impl fmt::Debug for Permits {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("Permits");
        if let Some(held) = &self.held {
            debug
                .field("name", &held.key.name())
                .field("count", &held.count);
        }
        debug.finish()
    }
}

impl Permits {
    fn new(key: Key, count: u64) -> Self {
        Self {
            held: Some(Held {
                key,
                count,
                runtime: Handle::try_current().ok(),
            }),
        }
    }

    /// How many permits this value holds.
    pub fn count(&self) -> u64 {
        self.held.as_ref().map_or(0, |held| held.count)
    }

    /// Returns the permits to the semaphore and wakes waiters.
    pub async fn release(mut self) -> Result<()> {
        let Some(held) = self.held.take() else {
            return Ok(());
        };
        release_permits(&held.key, Bytes::from(held.count.to_string())).await?;
        Ok(())
    }

    /// Keeps the permits taken: nothing is returned now or on drop. Return them later with [`Semaphore::release`].
    pub fn forget(mut self) {
        self.held = None;
    }
}

impl Drop for Permits {
    fn drop(&mut self) {
        let Some(held) = self.held.take() else {
            return;
        };
        if let Some(runtime) = held.runtime {
            runtime.spawn(async move {
                let _ = release_permits(&held.key, Bytes::from(held.count.to_string())).await;
            });
        }
    }
}
