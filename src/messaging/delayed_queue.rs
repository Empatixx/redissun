use crate::client::core::{Core, Evictor};
use crate::codec::Codec;
use crate::collections::vec_deque::VecDeque;
use crate::error::{Error, Result};
use crate::object::{millis as to_millis, tagged, HasKey, Key};
use crate::pubsub::Subscription;
use bytes::Bytes;
use fred::interfaces::ListInterface;
use fred::types::scripts::Script;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;
use std::sync::{Arc, LazyLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::runtime::Handle;
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::sync::Notify;
use uuid::Uuid;

const TRANSFER_BATCH: u32 = 100;
const IDLE_PAUSE: Duration = Duration::from_secs(60);
const RETRY_PAUSE: Duration = Duration::from_secs(5);
const UNSUBSCRIBED_PAUSE: Duration = Duration::from_secs(1);
const IMMEDIATE: Duration = Duration::from_millis(10);

static OFFER: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local value = struct.pack('Bc0Lc0', string.len(ARGV[2]), ARGV[2], string.len(ARGV[3]), ARGV[3])
        redis.call('ZADD', KEYS[2], ARGV[1], value)
        redis.call('RPUSH', KEYS[3], value)
        local head = redis.call('ZRANGE', KEYS[2], 0, 0)
        if head[1] == value then
            redis.call('PUBLISH', KEYS[4], ARGV[1])
        end
        return 1",
    )
});

static TRANSFER: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local due = redis.call('ZRANGEBYSCORE', KEYS[2], 0, ARGV[1], 'LIMIT', 0, ARGV[2])
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
            return tonumber(head[2])
        end
        return false",
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

static REMOVE_ALL: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local result = 0
        local s = redis.call('LLEN', KEYS[1])
        local i = 0
        while i < s do
            local v = redis.call('LINDEX', KEYS[1], i)
            local randomId, value = struct.unpack('Bc0Lc0', v)
            for j = 1, #ARGV, 1 do
                if value == ARGV[j] then
                    result = 1
                    i = i - 1
                    s = s - 1
                    redis.call('ZREM', KEYS[2], v)
                    redis.call('LREM', KEYS[1], 0, v)
                    break
                end
            end
            i = i + 1
        end
        return result",
    )
});

static RETAIN_ALL: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local changed = 0
        for _, v in ipairs(redis.call('LRANGE', KEYS[1], 0, -1)) do
            local randomId, value = struct.unpack('Bc0Lc0', v)
            local keep = false
            for j = 1, #ARGV, 1 do
                if ARGV[j] == value then
                    keep = true
                    break
                end
            end
            if not keep then
                redis.call('LREM', KEYS[1], 0, v)
                redis.call('ZREM', KEYS[2], v)
                changed = 1
            end
        end
        return changed",
    )
});

static CONTAINS: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local size = redis.call('LLEN', KEYS[1])
        for i = 0, size - 1 do
            local v = redis.call('LINDEX', KEYS[1], i)
            local randomId, value = struct.unpack('Bc0Lc0', v)
            if value == ARGV[1] then
                return 1
            end
        end
        return 0",
    )
});

static CONTAINS_ALL: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local size = redis.call('LLEN', KEYS[1])
        for i = 0, size - 1 do
            local v = redis.call('LINDEX', KEYS[1], i)
            local randomId, value = struct.unpack('Bc0Lc0', v)
            for j = #ARGV, 1, -1 do
                if value == ARGV[j] then
                    table.remove(ARGV, j)
                end
            end
        end
        return #ARGV == 0 and 1 or 0",
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

static PEEK: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local v = redis.call('LINDEX', KEYS[1], 0)
        if v ~= false then
            local randomId, value = struct.unpack('Bc0Lc0', v)
            return value
        end
        return false",
    )
});

static POLL: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local result = {}
        for i = 1, tonumber(ARGV[1]), 1 do
            local v = redis.call('LPOP', KEYS[1])
            if v == false then
                return result
            end
            redis.call('ZREM', KEYS[2], v)
            local randomId, value = struct.unpack('Bc0Lc0', v)
            table.insert(result, value)
        end
        return result",
    )
});

static POLL_LAST_AND_OFFER_FIRST_TO: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local v = redis.call('RPOP', KEYS[1])
        if v ~= false then
            redis.call('ZREM', KEYS[2], v)
            local randomId, value = struct.unpack('Bc0Lc0', v)
            redis.call('LPUSH', KEYS[3], value)
            return value
        end
        return false",
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

fn task_name(destination: &Key) -> String {
    format!("redissun__delay_transfer:{}", destination.redis_key())
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as i64)
}

/// A queue whose values reach a destination [`VecDeque`] only after a delay, modelled on Redisson's `RDelayedQueue`.
///
/// Waiting values are kept apart from the destination, in a list in the order they were offered and in a sorted set scored by their due time, with the same value layout as Redisson. Due times come from the clock of the client that offers the value, as in Redisson. Every client that holds a `DelayedQueue` for a destination runs one background task that moves due values to the end of the destination: a value that becomes the earliest one is announced through pub/sub with its due time, the task sleeps until then, and it also looks again after it subscribes, after a reconnect and, as a fallback, every minute. The task stops when the last `DelayedQueue` for the destination in the client is dropped.
///
/// Consume the values from the destination, for example with [`VecDeque::pop_front_wait`]. Keep a `DelayedQueue` alive in at least one program for the values to move. Equal values are kept as separate entries.
pub struct DelayedQueue<V, C: Codec> {
    key: Key,
    destination: Key,
    codec: C,
    usage: Option<Arc<Notify>>,
    _marker: PhantomData<fn() -> V>,
}

impl<V, C: Codec> Clone for DelayedQueue<V, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            destination: self.destination.clone(),
            codec: self.codec.clone(),
            usage: self.usage.clone(),
            _marker: PhantomData,
        }
    }
}

impl<V, C: Codec> Drop for DelayedQueue<V, C> {
    fn drop(&mut self) {
        let Some(usage) = self.usage.take() else {
            return;
        };
        let name = task_name(&self.destination);
        let mut tasks = self
            .destination
            .core
            .evictors
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let last = Arc::strong_count(&usage) == 2
            && tasks
                .get(&name)
                .is_some_and(|task| Arc::ptr_eq(&task.wake, &usage));
        if last {
            if let Some(task) = tasks.remove(&name) {
                task.handle.abort();
            }
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

    fn companions(&self) -> std::vec::Vec<String> {
        vec![timeouts(&self.destination)]
    }
}

impl<V, C: Codec> DelayedQueue<V, C> {
    pub(crate) fn new<D: Codec>(destination: &VecDeque<V, D>, codec: C) -> Self {
        let destination = HasKey::key(destination).clone();
        let key = Key::new(destination.core.clone(), staging(&destination));
        let usage = start_transfer(&destination);
        Self {
            key,
            destination,
            codec,
            usage,
            _marker: PhantomData,
        }
    }

    fn keys(&self) -> std::vec::Vec<String> {
        vec![self.key.redis_key(), timeouts(&self.destination)]
    }
}

impl<V, C> DelayedQueue<V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    fn decode_all(&self, raw: std::vec::Vec<Bytes>) -> Result<std::vec::Vec<V>> {
        raw.iter().map(|bytes| self.codec.decode(bytes)).collect()
    }

    fn encode_all<Q>(&self, values: &[&Q]) -> Result<std::vec::Vec<Bytes>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        values.iter().map(|v| self.codec.encode(*v)).collect()
    }

    /// Adds a value that reaches the destination after `delay`, like Redisson's `offer`. It is sent once and not retried, so a lost reply never leaves a second copy behind. The value is borrowed: `delayed.push("job", delay)`.
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
        let due = now_millis()
            .checked_add(delay)
            .ok_or_else(|| Error::Config("delay is too long".into()))?;
        let id = Uuid::new_v4().as_bytes()[..8].to_vec();
        let _: i64 = self
            .key
            .core
            .eval_no_retry(
                &OFFER,
                vec![
                    self.destination.redis_key(),
                    timeouts(&self.destination),
                    self.key.redis_key(),
                    channel(&self.destination),
                ],
                vec![
                    Bytes::from(due.to_string()),
                    Bytes::from(id),
                    self.codec.encode(v)?,
                ],
            )
            .await?;
        Ok(())
    }

    /// Takes the first waiting value equal to `v` out; returns whether there was one.
    pub async fn remove<Q>(&self, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let removed: i64 = self
            .key
            .core
            .eval(&REMOVE, self.keys(), vec![self.codec.encode(v)?])
            .await?;
        Ok(removed == 1)
    }

    /// Takes every waiting value equal to one of `values` out; returns whether any was removed.
    pub async fn remove_all<Q>(&self, values: &[&Q]) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if values.is_empty() {
            return Ok(false);
        }
        let removed: i64 = self
            .key
            .core
            .eval(&REMOVE_ALL, self.keys(), self.encode_all(values)?)
            .await?;
        Ok(removed == 1)
    }

    /// Keeps only the waiting values equal to one of `values`; returns whether any was removed. An empty list drops every waiting value.
    pub async fn retain_all<Q>(&self, values: &[&Q]) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if values.is_empty() {
            return self.key.del_all(vec![timeouts(&self.destination)]).await;
        }
        let changed: i64 = self
            .key
            .core
            .eval(&RETAIN_ALL, self.keys(), self.encode_all(values)?)
            .await?;
        Ok(changed == 1)
    }

    /// Returns whether a value equal to `v` is waiting.
    pub async fn contains<Q>(&self, v: &Q) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let found: i64 = self
            .key
            .core
            .eval(
                &CONTAINS,
                vec![self.key.redis_key()],
                vec![self.codec.encode(v)?],
            )
            .await?;
        Ok(found == 1)
    }

    /// Returns whether every one of `values` is waiting. An empty list is always contained.
    pub async fn contains_all<Q>(&self, values: &[&Q]) -> Result<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if values.is_empty() {
            return Ok(true);
        }
        let found: i64 = self
            .key
            .core
            .eval(
                &CONTAINS_ALL,
                vec![self.key.redis_key()],
                self.encode_all(values)?,
            )
            .await?;
        Ok(found == 1)
    }

    /// Returns the waiting value that was offered first, without taking it out.
    pub async fn peek(&self) -> Result<Option<V>> {
        let raw: Option<Bytes> = self
            .key
            .core
            .eval(&PEEK, vec![self.key.redis_key()], Vec::new())
            .await?;
        raw.map(|bytes| self.codec.decode(&bytes)).transpose()
    }

    /// Takes the waiting value that was offered first out, before its delay ends. It is sent once and not retried.
    pub async fn poll(&self) -> Result<Option<V>> {
        Ok(self.poll_many(1).await?.into_iter().next())
    }

    /// Takes up to `limit` waiting values out, in the order they were offered. It is sent once and not retried.
    pub async fn poll_many(&self, limit: usize) -> Result<std::vec::Vec<V>> {
        let raw: std::vec::Vec<Bytes> = self
            .key
            .core
            .eval_no_retry(&POLL, self.keys(), vec![Bytes::from(limit.to_string())])
            .await?;
        self.decode_all(raw)
    }

    /// Takes the waiting value that was offered last out and pushes it to the front of `target`. It is sent once and not retried.
    pub async fn poll_last_and_offer_first_to<D: Codec>(
        &self,
        target: &VecDeque<V, D>,
    ) -> Result<Option<V>> {
        let raw: Option<Bytes> = self
            .key
            .core
            .eval_no_retry(
                &POLL_LAST_AND_OFFER_FIRST_TO,
                vec![
                    self.key.redis_key(),
                    timeouts(&self.destination),
                    HasKey::key(target).redis_key(),
                ],
                Vec::new(),
            )
            .await?;
        raw.map(|bytes| self.codec.decode(&bytes)).transpose()
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

    /// Returns the waiting values in the order they were offered.
    pub async fn values(&self) -> Result<std::vec::Vec<V>> {
        let raw: std::vec::Vec<Bytes> = self
            .key
            .core
            .eval(&VALUES, vec![self.key.redis_key()], Vec::new())
            .await?;
        self.decode_all(raw)
    }
}

fn start_transfer(destination: &Key) -> Option<Arc<Notify>> {
    let runtime = Handle::try_current().ok()?;
    let core = &destination.core;
    let name = task_name(destination);
    let mut tasks = core.evictors.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(task) = tasks.get(&name) {
        if !task.handle.is_finished() {
            return Some(task.wake.clone());
        }
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
    let usage = Arc::new(Notify::new());
    tasks.insert(
        name,
        Evictor {
            handle,
            wake: usage.clone(),
        },
    );
    Some(usage)
}

enum Wake {
    Due,
    Announced(Option<i64>),
    Resubscribed,
    Lost,
}

async fn wait(
    pause: Duration,
    subscription: &mut Option<(Subscription, broadcast::Receiver<Bytes>)>,
    reconnects: &mut Option<broadcast::Receiver<()>>,
) -> Wake {
    let (Some((_, messages)), Some(reconnects)) = (subscription.as_mut(), reconnects.as_mut())
    else {
        tokio::time::sleep(pause.min(UNSUBSCRIBED_PAUSE)).await;
        return Wake::Due;
    };
    tokio::select! {
        _ = tokio::time::sleep(pause) => Wake::Due,
        received = messages.recv() => match received {
            Ok(payload) => Wake::Announced(
                std::str::from_utf8(&payload).ok().and_then(|text| text.trim().parse().ok()),
            ),
            Err(RecvError::Lagged(_)) => Wake::Resubscribed,
            Err(RecvError::Closed) => Wake::Lost,
        },
        reconnected = reconnects.recv() => match reconnected {
            Err(RecvError::Closed) => Wake::Lost,
            _ => Wake::Resubscribed,
        },
    }
}

async fn transfer_loop(core: Weak<Core>, channel: String, keys: std::vec::Vec<String>) {
    let mut subscription = None;
    let mut reconnects = None;
    let mut due: Option<i64> = None;
    let mut push = true;
    loop {
        let Some(strong) = core.upgrade() else {
            return;
        };
        if subscription.is_none() {
            if let Ok(pubsub) = strong.pubsub().await {
                reconnects.get_or_insert_with(|| pubsub.reconnects());
                subscription = pubsub.subscribe_with_messages(&channel).await.ok();
            }
            push = push || subscription.is_some();
        }
        if push {
            push = false;
            let outcome: Result<Option<i64>> = strong
                .eval(
                    &TRANSFER,
                    keys.clone(),
                    vec![
                        Bytes::from(now_millis().to_string()),
                        Bytes::from(TRANSFER_BATCH.to_string()),
                    ],
                )
                .await;
            due = match outcome {
                Ok(head) => head,
                Err(_) => Some(now_millis() + RETRY_PAUSE.as_millis() as i64),
            };
        }
        drop(strong);
        let pause = match due {
            Some(start) => Duration::from_millis((start - now_millis()).max(0) as u64),
            None => IDLE_PAUSE,
        };
        if pause <= IMMEDIATE {
            push = true;
            continue;
        }
        match wait(pause, &mut subscription, &mut reconnects).await {
            Wake::Due | Wake::Resubscribed => push = true,
            Wake::Announced(Some(start)) => due = Some(start),
            Wake::Announced(None) => push = true,
            Wake::Lost => {
                subscription = None;
                push = true;
            }
        }
    }
}
