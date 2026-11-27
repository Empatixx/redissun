use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::lua::Lua;
use crate::object::{HasKey, Key};
use bytes::Bytes;
use futures::{stream, Stream};
use redis::Value;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt;
use std::marker::PhantomData;
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// How many messages a subscriber reads at a time, as in Redisson.
const BATCH: usize = 100;

/// How many messages wait for a [`ReliableSubscriber`] that is slower than the others before the topic stops reading for it.
const BUFFER: usize = 256;

/// The pause after a failed read, as in Redisson.
const RETRY_PAUSE: Duration = Duration::from_secs(1);

// The scripts are Redisson's. KEYS: the stream, and for most the subscribers by expiry.
static PUBLISH: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "redis.call('xadd', KEYS[1], '*', 'm', ARGV[1])
        local v = redis.call('xinfo', 'groups', KEYS[1])
        return #v",
    )
});

static ADD_SUBSCRIBER: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "redis.call('zadd', KEYS[2], ARGV[3], ARGV[2])
        redis.call('xgroup', 'create', KEYS[1], ARGV[2], ARGV[1], 'MKSTREAM')",
    )
});

static PENDING: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local pendingData = redis.call('xpending', KEYS[1], ARGV[1], '-', '+', ARGV[2])
        local result = {}
        for i = 1, #pendingData, 1 do
            local value = redis.call('xrange', KEYS[1], pendingData[i][1], pendingData[i][1])
            table.insert(result, value[1])
        end
        return result",
    )
});

static UPDATE: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local expired = redis.call('zrangebyscore', KEYS[2], 0, tonumber(ARGV[2]) - 1)
        for i, v in ipairs(expired) do
            redis.call('xgroup', 'destroy', KEYS[1], v)
            redis.call('zrem', KEYS[2], v)
        end
        local r = redis.call('zscore', KEYS[2], ARGV[1])
        local score = 92233720368547758
        local groups = redis.call('xinfo', 'groups', KEYS[1])
        for i, v in ipairs(groups) do
            local id1, id2 = string.match(v[8], '(.*)%-(.*)')
            score = math.min(tonumber(id1), score)
        end
        score = tostring(score) .. '-0'
        local range = redis.call('xrange', KEYS[1], score, '+')
        if #range == 0 or (#range == 1 and range[1][1] == score) then
            redis.call('xtrim', KEYS[1], 'maxlen', 0)
        else
            redis.call('xtrim', KEYS[1], 'maxlen', #range)
        end
        return r ~= false",
    )
});

static REMOVE_SUBSCRIBER: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "redis.call('xgroup', 'destroy', KEYS[1], ARGV[1])
        redis.call('zrem', KEYS[2], ARGV[1])",
    )
});

static RENEW: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "if redis.call('zscore', KEYS[1], ARGV[2]) == false then
            return 0
        end
        redis.call('zadd', KEYS[1], ARGV[1], ARGV[2])
        return 1",
    )
});

static COUNT_SUBSCRIBERS: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local v = redis.call('xinfo', 'groups', KEYS[1])
        return #v",
    )
});

/// The sorted set of the subscribers by the time they expire, Redisson's `suffixName(name, "timeout")`, in the slot of the stream.
fn timeout_name(name: &str) -> String {
    if name.contains('{') {
        format!("{name}:timeout")
    } else {
        format!("{{{name}}}:timeout")
    }
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis())
}

/// The listeners of one topic handle and the tasks that read for them.
struct Shared {
    /// The consumer group of this handle, Redisson's `subscriberId`.
    subscriber_id: String,
    state: Mutex<State>,
    /// Held while the subscriber is registered or its last reading task ends, so a new registration never races the removal of the old one.
    transition: tokio::sync::Mutex<()>,
}

#[derive(Default)]
struct State {
    next_listener: u64,
    listeners: Vec<(u64, mpsc::Sender<Bytes>)>,
    /// Whether the subscriber is registered or being registered, Redisson's `subscribed`.
    subscribed: bool,
    running: Option<Running>,
    /// The reading task of a stopped subscriber, which removes it as it ends.
    stopping: Option<JoinHandle<()>>,
}

/// The tasks of a registered subscriber.
struct Running {
    /// Asks the reading task to finish the messages it has, then remove the subscriber.
    stop: CancellationToken,
    poll: JoinHandle<()>,
    renew: JoinHandle<()>,
}

impl State {
    /// Stops the subscriber: its reading task removes it once the messages it has are acknowledged.
    fn stop(&mut self) {
        self.subscribed = false;
        if let Some(running) = self.running.take() {
            running.renew.abort();
            running.stop.cancel();
            self.stopping = Some(running.poll);
        }
    }
}

/// A topic whose messages are kept in a Redis stream until every subscriber has read them, Redisson's `RReliableTopic`.
///
/// Unlike [`Topic`](crate::Topic), a subscriber that loses its connection or falls behind gets every message once it reads again. Each topic handle from [`Client::reliable_topic`](crate::Client::reliable_topic) is one subscriber, a consumer group of the stream, and all the [`ReliableSubscriber`]s of the handle and its clones share it. A new subscriber reads the messages still in the stream, then the new ones. A subscriber renews itself every third of the client's `reliable_topic_watchdog_timeout`; one that stops renewing, because its program ended, is removed after that time, and its unread messages with it.
///
/// The messages live in the stream named like the topic, under the field `m`, and the subscribers in the sorted set `{name}:timeout`.
///
/// ```no_run
/// # async fn run(client: redissun::Client) -> redissun::Result<()> {
/// let events = client.reliable_topic::<String>("events");
/// let mut subscriber = events.subscribe().await?;
/// events.publish("deployed").await?;
/// assert_eq!(subscriber.recv().await?, "deployed");
/// # Ok(())
/// # }
/// ```
pub struct ReliableTopic<M, C: Codec> {
    key: Key,
    codec: C,
    shared: Arc<Shared>,
    _marker: PhantomData<fn() -> M>,
}

impl<M, C: Codec> Clone for ReliableTopic<M, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
            shared: self.shared.clone(),
            _marker: PhantomData,
        }
    }
}

impl<M, C: Codec> fmt::Debug for ReliableTopic<M, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "ReliableTopic")
    }
}

impl<M, C: Codec> HasKey for ReliableTopic<M, C> {
    fn key(&self) -> &Key {
        &self.key
    }

    fn companions(&self) -> Vec<String> {
        vec![timeout_name(self.key.name())]
    }

    fn renamed_companions(&self, new_name: &str) -> Option<Vec<String>> {
        Some(vec![timeout_name(new_name)])
    }
}

impl<M, C: Codec> ReliableTopic<M, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self {
            key,
            codec,
            shared: Arc::new(Shared {
                subscriber_id: uuid::Uuid::new_v4().simple().to_string(),
                state: Mutex::new(State::default()),
                transition: tokio::sync::Mutex::new(()),
            }),
            _marker: PhantomData,
        }
    }

    fn keys(&self) -> Vec<String> {
        vec![self.key.redis_key(), timeout_name(self.key.name())]
    }

    /// How many messages the stream holds, read or not, Redisson's `size`.
    #[doc(alias = "size")]
    pub async fn len(&self) -> Result<u64> {
        self.key.read(self.key.key_cmd("XLEN")).await
    }

    /// How many [`ReliableSubscriber`]s this handle and its clones have, Redisson's `countListeners`.
    #[doc(alias = "countListeners")]
    pub async fn local_subscriber_count(&self) -> usize {
        lock(&self.shared.state).listeners.len()
    }

    /// How many subscribers the topic has in all clients, the consumer groups of the stream, Redisson's `countSubscribers`. 0 when the stream does not exist.
    pub async fn subscriber_count(&self) -> Result<usize> {
        let count: std::result::Result<usize, Error> = self
            .key
            .core
            .eval(&COUNT_SUBSCRIBERS, vec![self.key.redis_key()], Vec::new())
            .await;
        match count {
            Err(Error::Redis(message)) if message.contains("no such key") => Ok(0),
            other => other,
        }
    }

    /// Ends every [`ReliableSubscriber`] of this handle and removes the subscriber, Redisson's `removeAllListeners`. Its unread messages are dropped.
    #[doc(alias = "removeAllListeners")]
    pub async fn unsubscribe_all(&self) {
        let _transition = self.shared.transition.lock().await;
        let stopping = {
            let mut state = lock(&self.shared.state);
            state.listeners.clear();
            state.stop();
            state.stopping.take()
        };
        if let Some(stopping) = stopping {
            // The task removes the subscriber as it ends.
            let _ = stopping.await;
        }
    }
}

fn lock(state: &Mutex<State>) -> std::sync::MutexGuard<'_, State> {
    state.lock().unwrap_or_else(|e| e.into_inner())
}

async fn remove_subscriber(key: &Key, subscriber_id: &str) -> Result<()> {
    let removed: Result<()> = key
        .core
        .eval(
            &REMOVE_SUBSCRIBER,
            vec![key.redis_key(), timeout_name(key.name())],
            vec![Bytes::from(subscriber_id.to_string())],
        )
        .await;
    match removed {
        // The stream was deleted meanwhile, and the group with it.
        Err(Error::Redis(message)) if message.contains("no such key") => Ok(()),
        other => other,
    }
}

impl<M, C> ReliableTopic<M, C>
where
    M: Serialize + DeserializeOwned + Send,
    C: Codec,
{
    /// Adds a message to the stream; returns how many subscribers the topic has, Redisson's `publish`.
    pub async fn publish<Q>(&self, message: &Q) -> Result<usize>
    where
        M: std::borrow::Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let encoded = self.codec.encode(message)?;
        self.key
            .core
            .eval(&PUBLISH, vec![self.key.redis_key()], vec![encoded])
            .await
    }

    /// Starts receiving messages, Redisson's `addListener`. The first subscriber of this handle creates its consumer group, which reads the messages still in the stream first. Drop the [`ReliableSubscriber`] to stop, Redisson's `removeListener`.
    pub async fn subscribe(&self) -> Result<ReliableSubscriber<M, C>> {
        let (sender, receiver) = mpsc::channel(BUFFER);
        let (id, first) = {
            let mut state = lock(&self.shared.state);
            let id = state.next_listener;
            state.next_listener += 1;
            state.listeners.push((id, sender));
            (id, !std::mem::replace(&mut state.subscribed, true))
        };
        let subscriber = ReliableSubscriber {
            receiver,
            id,
            shared: self.shared.clone(),
            codec: self.codec.clone(),
            _marker: PhantomData,
        };
        if first {
            self.start().await?;
        }
        Ok(subscriber)
    }

    async fn start(&self) -> Result<()> {
        let _transition = self.shared.transition.lock().await;
        let stopping = lock(&self.shared.state).stopping.take();
        if let Some(stopping) = stopping {
            // The subscriber of this handle that was stopped before is removed first.
            let _ = stopping.await;
        }
        let watchdog = self.key.core.reliable_topic_watchdog_timeout;
        let expires = now_millis() + watchdog.as_millis();
        self.key
            .core
            .eval_once::<()>(
                &ADD_SUBSCRIBER,
                self.keys(),
                vec![
                    Bytes::from_static(b"0"),
                    Bytes::from(self.shared.subscriber_id.clone()),
                    Bytes::from(expires.to_string()),
                ],
            )
            .await?;
        let stop = CancellationToken::new();
        let poll = tokio::spawn(poll(
            self.key.clone(),
            Arc::downgrade(&self.shared),
            stop.clone(),
        ));
        let renew = tokio::spawn(renew(
            self.key.clone(),
            Arc::downgrade(&self.shared),
            watchdog,
        ));
        let stopped = {
            let mut state = lock(&self.shared.state);
            if state.subscribed {
                state.running = Some(Running { stop, poll, renew });
                false
            } else {
                poll.abort();
                renew.abort();
                true
            }
        };
        if stopped {
            // Every subscriber was dropped while the group was created, so it goes again.
            remove_subscriber(&self.key, &self.shared.subscriber_id).await?;
        }
        Ok(())
    }
}

/// An entry of the stream: its id and the encoded message.
type Entry = (String, Bytes);

fn entries(value: &Value) -> Vec<Entry> {
    let items = match value {
        Value::Array(items) => items.as_slice(),
        _ => &[],
    };
    items
        .iter()
        .filter_map(|entry| {
            let Value::Array(parts) = entry else {
                return None;
            };
            let id = crate::reply::text(parts.first()?)?;
            let fields = match parts.get(1)? {
                Value::Array(fields) => fields
                    .chunks(2)
                    .map(|pair| (pair.first(), pair.get(1)))
                    .collect::<Vec<_>>(),
                Value::Map(fields) => fields.iter().map(|(k, v)| (Some(k), Some(v))).collect(),
                _ => return None,
            };
            let message = fields.into_iter().find_map(|(field, value)| {
                (crate::reply::text(field?)? == "m")
                    .then(|| value.and_then(crate::reply::bytes))
                    .flatten()
            })?;
            Some((id, message))
        })
        .collect()
}

/// The entries of an `XREADGROUP` reply, `[[stream, entries]]` in RESP2 and `{stream: entries}` in RESP3.
fn read_entries(value: &Value) -> Vec<Entry> {
    match value {
        Value::Array(streams) => streams
            .iter()
            .filter_map(|stream| match stream {
                Value::Array(parts) => parts.get(1).map(entries),
                _ => None,
            })
            .flatten()
            .collect(),
        Value::Map(streams) => streams
            .iter()
            .flat_map(|(_, value)| entries(value))
            .collect(),
        _ => Vec::new(),
    }
}

fn read_group(key: &Key, group: &str, block: bool) -> redis::Cmd {
    let mut cmd = redis::cmd("XREADGROUP");
    cmd.arg("GROUP").arg(group).arg("consumer");
    if block {
        cmd.arg("BLOCK").arg(0);
    } else {
        cmd.arg("COUNT").arg(BATCH);
    }
    cmd.arg("STREAMS").arg(key.redis_key()).arg(">");
    cmd
}

/// What one read for a subscriber came to.
enum Read {
    Entries(Vec<Entry>),
    /// The group is gone: the subscriber was removed, or the stream deleted.
    Gone,
}

async fn read(key: &Key, group: &str) -> Result<Read> {
    let gone =
        |error: &Error| matches!(error, Error::Redis(message) if message.contains("NOGROUP"));
    let pending: Value = match key
        .core
        .eval(
            &PENDING,
            vec![key.redis_key()],
            vec![
                Bytes::from(group.to_string()),
                Bytes::from(BATCH.to_string()),
            ],
        )
        .await
    {
        Err(error) if gone(&error) => return Ok(Read::Gone),
        other => other?,
    };
    let pending = entries(&pending);
    if !pending.is_empty() {
        return Ok(Read::Entries(pending));
    }
    // A read that finds messages needs no connection of its own; only an empty one waits on a blocking read.
    let unread: Value = match key.query(read_group(key, group, false)).await {
        Err(error) if gone(&error) => return Ok(Read::Gone),
        other => other?,
    };
    let unread = read_entries(&unread);
    if !unread.is_empty() {
        return Ok(Read::Entries(unread));
    }
    let blocked: Value = match key
        .core
        .blocking(key.redis_key().as_bytes(), read_group(key, group, true))
        .await
    {
        Err(error) if gone(&error) => return Ok(Read::Gone),
        other => other?,
    };
    Ok(Read::Entries(read_entries(&blocked)))
}

/// Reads for a subscriber until it is removed, like Redisson's `poll`: the messages it holds first, then new ones; each one goes to every listener and is acknowledged, and after each batch the stream is trimmed to what some subscriber still has to read.
///
/// When `stop` is cancelled it stops waiting for messages, but a batch it has is acknowledged and trimmed first; then it removes the subscriber, Redisson's `removeSubscriber`.
async fn poll(key: Key, shared: Weak<Shared>, stop: CancellationToken) {
    let Some(group) = shared.upgrade().map(|shared| shared.subscriber_id.clone()) else {
        return;
    };
    if read_until_stopped(&key, &shared, &group, &stop).await {
        if let Err(error) = remove_subscriber(&key, &group).await {
            tracing::error!(
                "Unable to remove the subscriber {group} of {}: {error}",
                key.name()
            );
        }
    }
}

/// Reads until the subscriber is stopped, which gives `true`, or removed by Redis, which gives `false`.
async fn read_until_stopped(
    key: &Key,
    shared: &Weak<Shared>,
    group: &str,
    stop: &CancellationToken,
) -> bool {
    loop {
        let read = tokio::select! {
            () = stop.cancelled() => return true,
            read = read(key, group) => read,
        };
        let entries = match read {
            Ok(Read::Entries(entries)) => entries,
            Ok(Read::Gone) => return false,
            Err(error) => {
                tracing::error!("Unable to poll a new element. Subscriber id: {group}: {error}");
                tokio::select! {
                    () = stop.cancelled() => return true,
                    () = tokio::time::sleep(RETRY_PAUSE) => continue,
                }
            }
        };
        for (id, message) in entries {
            let listeners = shared
                .upgrade()
                .map(|shared| {
                    lock(&shared.state)
                        .listeners
                        .iter()
                        .map(|(_, sender)| sender.clone())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            for listener in listeners {
                // A listener that was dropped meanwhile misses it.
                let _ = listener.send(message.clone()).await;
            }
            let mut ack = key.key_cmd("XACK");
            ack.arg(group).arg(&id);
            if let Err(error) = key.query::<i64>(ack).await {
                tracing::error!("Unable to process a message. Subscriber id: {group}: {error}");
            }
        }
        let still_subscribed: Result<bool> = key
            .core
            .eval(
                &UPDATE,
                vec![key.redis_key(), timeout_name(key.name())],
                vec![
                    Bytes::from(group.to_string()),
                    Bytes::from(now_millis().to_string()),
                ],
            )
            .await;
        match still_subscribed {
            Ok(true) if stop.is_cancelled() => return true,
            Ok(true) => {}
            Ok(false) => return false,
            Err(error) => {
                tracing::error!("Unable to update subscriber status: {error}");
                return stop.is_cancelled();
            }
        }
    }
}

/// Keeps the subscriber registered, like Redisson's `renewExpiration`: every third of `watchdog` its expiry moves `watchdog` ahead, until it is no longer registered.
async fn renew(key: Key, shared: Weak<Shared>, watchdog: Duration) {
    let Some(group) = shared.upgrade().map(|shared| shared.subscriber_id.clone()) else {
        return;
    };
    loop {
        tokio::time::sleep(watchdog / 3).await;
        if shared.strong_count() == 0 {
            return;
        }
        let expires = now_millis() + watchdog.as_millis();
        let renewed: Result<i64> = key
            .core
            .eval(
                &RENEW,
                vec![timeout_name(key.name())],
                vec![Bytes::from(expires.to_string()), Bytes::from(group.clone())],
            )
            .await;
        match renewed {
            Ok(1) => {}
            Ok(_) => return,
            Err(error) => {
                tracing::error!(
                    "Can't update reliable topic {} expiration time: {error}",
                    key.name()
                );
                return;
            }
        }
    }
}

/// Receives the messages of a [`ReliableTopic`]. Dropping the last subscriber of a topic handle removes its consumer group, Redisson's `removeListener`.
pub struct ReliableSubscriber<M, C: Codec> {
    receiver: mpsc::Receiver<Bytes>,
    id: u64,
    shared: Arc<Shared>,
    codec: C,
    _marker: PhantomData<fn() -> M>,
}

impl<M, C: Codec> fmt::Debug for ReliableSubscriber<M, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReliableSubscriber").finish_non_exhaustive()
    }
}

impl<M, C> ReliableSubscriber<M, C>
where
    M: DeserializeOwned + Send,
    C: Codec,
{
    /// Waits for the next message. Fails with [`Error::Unsupported`] after [`ReliableTopic::unsubscribe_all`] ended the subscriber.
    pub async fn recv(&mut self) -> Result<M> {
        match self.receiver.recv().await {
            Some(payload) => self.codec.decode(&payload),
            None => Err(Error::Unsupported(
                "the reliable topic subscriber was removed".into(),
            )),
        }
    }

    /// Turns the subscriber into a stream of messages. The stream ends when the subscriber is removed.
    pub fn into_stream(self) -> impl Stream<Item = Result<M>> {
        stream::unfold(self, |mut subscriber| async move {
            match subscriber.recv().await {
                Err(Error::Unsupported(_)) => None,
                other => Some((other, subscriber)),
            }
        })
    }
}

impl<M, C: Codec> Drop for ReliableSubscriber<M, C> {
    fn drop(&mut self) {
        let mut state = lock(&self.shared.state);
        state.listeners.retain(|(id, _)| *id != self.id);
        if state.listeners.is_empty() {
            // The reading task removes the subscriber once its messages are acknowledged.
            state.stop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{entries, read_entries, timeout_name};
    use redis::Value;

    fn bulk(text: &str) -> Value {
        Value::BulkString(text.as_bytes().to_vec())
    }

    #[test]
    fn entries_give_the_id_and_the_message_field() {
        let entry = Value::Array(vec![
            bulk("1-0"),
            Value::Array(vec![bulk("m"), bulk("\"hi\"")]),
        ]);
        let reply = Value::Array(vec![Value::Array(vec![
            bulk("topic"),
            Value::Array(vec![entry.clone()]),
        ])]);
        assert_eq!(
            read_entries(&reply),
            vec![("1-0".to_string(), bytes::Bytes::from_static(b"\"hi\""))]
        );
        // A pending entry the stream no longer has comes back as nothing.
        assert_eq!(entries(&Value::Array(vec![entry, Value::Nil])).len(), 1);
        assert!(read_entries(&Value::Nil).is_empty());
    }

    #[test]
    fn the_subscribers_live_in_the_slot_of_the_stream() {
        assert_eq!(timeout_name("events"), "{events}:timeout");
    }
}
