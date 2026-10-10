use crate::error::Result;
use crate::object::{millis, HasKey, Key};
use crate::pending::Pending;
use crate::shield::shielded;
use crate::wait::wait_on;
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
        redis.call('PUBLISH', KEYS[2], value)",
    )
});

static RELEASE_IF_EXISTS: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('EXISTS', KEYS[1]) == 0 then
            return 0
        end
        local value = redis.call('INCRBY', KEYS[1], ARGV[1])
        redis.call('PUBLISH', KEYS[2], value)
        return 1",
    )
});

static TRY_SET_PERMITS: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local value = redis.call('GET', KEYS[1])
        if value == false then
            if ARGV[2] ~= nil then
                redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
            else
                redis.call('SET', KEYS[1], ARGV[1])
            end
            redis.call('PUBLISH', KEYS[2], ARGV[1])
            return 1
        end
        return 0",
    )
});

static ADD_PERMITS: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local value = redis.call('GET', KEYS[1])
        if value == false then
            value = 0
        end
        redis.call('SET', KEYS[1], value + ARGV[1])
        redis.call('PUBLISH', KEYS[2], value + ARGV[1])",
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
    if name.contains('{') {
        format!("redissun_sc:{name}")
    } else {
        format!("redissun_sc:{{{name}}}")
    }
}

fn keys(key: &Key) -> Vec<String> {
    vec![key.redis_key(), channel(key.name())]
}

/// A shared counting semaphore, modelled on Redisson's `RSemaphore`.
///
/// The permits live in one Redis string. They have no owner, so permits held by a process that crashes are lost until someone releases or resets them. Acquiring and releasing are sent once and never retried, so a command that times out is not applied twice; waiters are woken through the channel `redissun_sc:{name}`.
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

async fn release_permits(key: &Key, permits: u64) -> Result<()> {
    if permits == 0 {
        return Ok(());
    }
    key.core
        .eval_no_retry::<()>(&RELEASE, keys(key), vec![Bytes::from(permits.to_string())])
        .await
}

impl Semaphore {
    pub(crate) fn new(key: Key) -> Self {
        Self { key }
    }

    async fn set_permits(&self, permits: i64, ttl: Option<Duration>) -> Result<bool> {
        let mut args = vec![Bytes::from(permits.to_string())];
        if let Some(ttl) = ttl {
            args.push(Bytes::from(millis(ttl)?.to_string()));
        }
        let set: i64 = self
            .key
            .core
            .eval(&TRY_SET_PERMITS, keys(&self.key), args)
            .await?;
        Ok(set == 1)
    }

    /// Sets the number of permits, but only when the semaphore does not exist yet. Returns whether it was set.
    pub async fn try_set_permits(&self, permits: i64) -> Result<bool> {
        self.set_permits(permits, None).await
    }

    /// Like [`Semaphore::try_set_permits`], and the semaphore is removed after `ttl`.
    pub async fn try_set_permits_with_ttl(&self, permits: i64, ttl: Duration) -> Result<bool> {
        self.set_permits(permits, Some(ttl)).await
    }

    /// Adds permits, creating the semaphore when needed, and wakes waiters. A negative number takes permits away, possibly below zero.
    pub async fn add_permits(&self, permits: i64) -> Result<()> {
        self.key
            .core
            .eval::<()>(
                &ADD_PERMITS,
                keys(&self.key),
                vec![Bytes::from(permits.to_string())],
            )
            .await
    }

    /// Number of permits that can be acquired right now. A missing semaphore has 0; the number is negative after permits were taken away with [`Semaphore::add_permits`].
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

    /// Returns `permits` to the semaphore by hand, creating it when needed, and wakes waiters. Use it for permits that were `forget`-ed. Releasing 0 permits does nothing.
    pub async fn release(&self, permits: u64) -> Result<()> {
        release_permits(&self.key, permits).await
    }

    /// Returns `permits` only when the semaphore exists, and wakes waiters. Returns whether it did; releasing 0 permits returns `false`.
    pub async fn release_if_exists(&self, permits: u64) -> Result<bool> {
        if permits == 0 {
            return Ok(false);
        }
        let released: i64 = self
            .key
            .core
            .eval_no_retry(
                &RELEASE_IF_EXISTS,
                keys(&self.key),
                vec![Bytes::from(permits.to_string())],
            )
            .await?;
        Ok(released == 1)
    }

    /// Waits until `permits` are available and takes them. Add `.timeout(duration)` to wait at most that long; it then resolves to `None` when the time runs out. Acquiring 0 permits succeeds at once.
    pub fn acquire(&self, permits: u64) -> Pending<'_, Permits> {
        Pending::new(move |wait| self.acquire_inner(permits, wait))
    }

    /// Takes `permits` if they are available right now; returns `None` otherwise. Taking 0 permits always succeeds.
    pub async fn try_acquire(&self, permits: u64) -> Result<Option<Permits>> {
        self.acquire_inner(permits, Some(Duration::ZERO)).await
    }

    async fn try_take(&self, permits: u64) -> Result<Option<Permits>> {
        let key = self.key.clone();
        shielded(async move {
            let taken: i64 = key
                .core
                .eval_no_retry(
                    &TRY_ACQUIRE,
                    vec![key.redis_key()],
                    vec![Bytes::from(permits.to_string())],
                )
                .await?;
            Ok((taken == 1).then(|| Permits::new(key, permits)))
        })
        .await
    }

    async fn acquire_inner(&self, permits: u64, wait: Option<Duration>) -> Result<Option<Permits>> {
        if permits == 0 {
            return Ok(Some(Permits::new(self.key.clone(), 0)));
        }
        if let Some(taken) = self.try_take(permits).await? {
            return Ok(Some(taken));
        }
        if wait.is_some_and(|wait| wait.is_zero()) {
            return Ok(None);
        }

        let deadline = wait.map(|wait| Instant::now() + wait);
        wait_on(&self.key.core, &channel(self.key.name()), deadline, || {
            self.try_take(permits)
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
        release_permits(&held.key, held.count).await
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
                let _ = release_permits(&held.key, held.count).await;
            });
        }
    }
}
