use crate::codec::Codec;
use crate::core::{Core, Evictor};
use crate::error::Result;
use crate::object::{millis as to_millis, tagged, HasKey, Key};
use crate::vec_deque::VecDeque;
use bytes::Bytes;
use fred::interfaces::ListInterface;
use fred::types::scripts::Script;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;
use std::sync::{Arc, LazyLock, Weak};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::Notify;
use uuid::Uuid;

const TRANSFER_BATCH: u32 = 100;
const IDLE_PAUSE: Duration = Duration::from_secs(60);
const UNSUBSCRIBED_PAUSE: Duration = Duration::from_secs(1);

static PUSH: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local t = redis.call('TIME')
        local timeout = t[1] * 1000 + math.floor(t[2] / 1000) + tonumber(ARGV[1])
        local value = struct.pack('Bc0Lc0', string.len(ARGV[2]), ARGV[2], string.len(ARGV[3]), ARGV[3])
        redis.call('ZADD', KEYS[2], timeout, value)
        redis.call('RPUSH', KEYS[3], value)
        local head = redis.call('ZRANGE', KEYS[2], 0, 0)
        if head[1] == value then
            redis.call('PUBLISH', KEYS[4], timeout)
        end
        return 1",
    )
});

static TRANSFER: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local t = redis.call('TIME')
        local now = t[1] * 1000 + math.floor(t[2] / 1000)
        local due = redis.call('ZRANGEBYSCORE', KEYS[2], 0, now, 'LIMIT', 0, ARGV[1])
        if #due > 0 then
            for i, v in ipairs(due) do
                local randomId, value = struct.unpack('Bc0Lc0', v)
                redis.call('RPUSH', KEYS[1], value)
                redis.call('LREM', KEYS[3], 1, v)
            end
            redis.call('ZREM', KEYS[2], unpack(due))
        end
        local head = redis.call('ZRANGE', KEYS[2], 0, 0, 'WITHSCORES')
        if head[1] ~= nil then
            return math.max(0, tonumber(head[2]) - now)
        end
        return -1",
    )
});

static REMOVE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local size = redis.call('LLEN', KEYS[1])
        for i = 0, size - 1 do
            local v = redis.call('LINDEX', KEYS[1], i)
            local randomId, value = struct.unpack('Bc0Lc0', v)
            if value == ARGV[1] then
                redis.call('ZREM', KEYS[2], v)
                redis.call('LREM', KEYS[1], 1, v)
                return 1
            end
        end
        return 0",
    )
});

static VALUES: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local result = {}
        for i, v in ipairs(redis.call('LRANGE', KEYS[1], 0, -1)) do
            local randomId, value = struct.unpack('Bc0Lc0', v)
            table.insert(result, value)
        end
        return result",
    )
});

fn staging(destination: &Key) -> String {
    format!("redissun__delay_queue:{}", tagged(destination.name()))
}

fn timeouts(destination: &Key) -> String {
    format!("redissun__delay_timeout:{}", tagged(destination.name()))
}

fn channel(destination: &Key) -> String {
    format!("redissun__delay_channel:{}", tagged(destination.name()))
}

/// A queue whose values become available to the consumers of a destination [`VecDeque`] only after a delay.
///
/// Waiting values are kept apart from the destination, in a list and a sorted set by due time, as in Redisson's `RedissonDelayedQueue`. Each client that holds a `DelayedQueue` for a destination runs a background task that moves due values to the end of the destination. A new value that is due before all others wakes the tasks of all clients through pub/sub, and an idle task sleeps until then. Time comes from the Redis server.
///
/// Consume the values from the destination, for example with [`VecDeque::pop_front_wait`]. The task stops when the client is dropped, so keep a client alive in at least one program for the values to move. Equal values are kept as separate entries.
pub struct DelayedQueue<V, C: Codec> {
    key: Key,
    destination: Key,
    codec: C,
    _marker: PhantomData<fn() -> V>,
}

impl<V, C: Codec> Clone for DelayedQueue<V, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            destination: self.destination.clone(),
            codec: self.codec.clone(),
            _marker: PhantomData,
        }
    }
}

impl<V, C: Codec> fmt::Debug for DelayedQueue<V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.destination.describe(f, "DelayedQueue")
    }
}

impl<V, C: Codec> HasKey for DelayedQueue<V, C> {
    fn key(&self) -> &Key {
        &self.key
    }

    fn companions(&self) -> Vec<String> {
        vec![timeouts(&self.destination)]
    }
}

impl<V, C: Codec> DelayedQueue<V, C> {
    pub(crate) fn new<D: Codec>(destination: &VecDeque<V, D>, codec: C) -> Self {
        let destination = HasKey::key(destination).clone();
        let key = Key::new(destination.core.clone(), staging(&destination));
        start_transfer(&destination);
        Self {
            key,
            destination,
            codec,
            _marker: PhantomData,
        }
    }
}

impl<V, C> DelayedQueue<V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    /// Adds a value that reaches the destination after `delay`. The value is borrowed: `delayed.push("job", delay)`.
    pub async fn push<Q>(&self, v: &Q, delay: Duration) -> Result<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let delay = if delay.is_zero() {
            0
        } else {
            to_millis(delay)?
        };
        let id = Uuid::new_v4().as_bytes()[..8].to_vec();
        let _: i64 = self
            .key
            .core
            .eval(
                &PUSH,
                vec![
                    self.destination.redis_key(),
                    timeouts(&self.destination),
                    self.key.redis_key(),
                    channel(&self.destination),
                ],
                vec![
                    Bytes::from(delay.to_string()),
                    Bytes::from(id),
                    self.codec.encode(v)?,
                ],
            )
            .await?;
        Ok(())
    }

    /// Takes the first pending value equal to `v` out; returns whether there was one.
    pub async fn remove<Q>(&self, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let removed: i64 = self
            .key
            .core
            .eval(
                &REMOVE,
                vec![self.key.redis_key(), timeouts(&self.destination)],
                vec![self.codec.encode(v)?],
            )
            .await?;
        Ok(removed == 1)
    }

    /// Number of values that wait for their delay.
    pub async fn len(&self) -> Result<usize> {
        let length: usize = self.key.core.redis().llen(self.key.redis_key()).await?;
        Ok(length)
    }

    /// Returns whether no value waits.
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// Drops every waiting value. Values already moved to the destination stay there.
    pub async fn clear(&self) -> Result<()> {
        self.key.del_all(vec![timeouts(&self.destination)]).await?;
        Ok(())
    }

    /// Returns the waiting values in the order they were pushed.
    pub async fn values(&self) -> Result<Vec<V>> {
        let raw: Vec<Bytes> = self
            .key
            .core
            .eval(&VALUES, vec![self.key.redis_key()], Vec::new())
            .await?;
        raw.iter().map(|bytes| self.codec.decode(bytes)).collect()
    }
}

fn start_transfer(destination: &Key) {
    let Ok(runtime) = Handle::try_current() else {
        return;
    };
    let core = &destination.core;
    let name = format!("redissun__delay_transfer:{}", destination.redis_key());
    let mut tasks = core.evictors.lock().unwrap_or_else(|e| e.into_inner());
    if tasks
        .get(&name)
        .is_some_and(|task| !task.handle.is_finished())
    {
        return;
    }
    let keys = vec![
        destination.redis_key(),
        timeouts(destination),
        staging(destination),
    ];
    let handle = runtime.spawn(transfer_loop(
        Arc::downgrade(core),
        channel(destination),
        keys,
    ));
    tasks.insert(
        name,
        Evictor {
            handle,
            wake: Arc::new(Notify::new()),
        },
    );
}

async fn transfer_loop(core: Weak<Core>, channel: String, keys: Vec<String>) {
    let mut subscription = None;
    loop {
        let Some(strong) = core.upgrade() else {
            return;
        };
        if subscription.is_none() {
            subscription = strong.pubsub.subscribe(&channel).await.ok();
        }
        let local = Notify::new();
        let notify: &Notify = subscription.as_ref().map_or(&local, |s| s.notify());
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let outcome: Result<i64> = strong
            .eval(
                &TRANSFER,
                keys.clone(),
                vec![Bytes::from(TRANSFER_BATCH.to_string())],
            )
            .await;
        drop(strong);
        let pause = match outcome {
            Ok(remaining) if remaining < 0 => IDLE_PAUSE,
            Ok(0) => continue,
            Ok(remaining) => Duration::from_millis(remaining as u64),
            Err(_) => Duration::from_secs(1),
        };
        let pause = if subscription.is_some() {
            pause
        } else {
            pause.min(UNSUBSCRIBED_PAUSE)
        };
        let _ = tokio::time::timeout(pause, notified).await;
    }
}
