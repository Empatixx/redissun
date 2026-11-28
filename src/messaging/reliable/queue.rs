//! [`ReliableQueue`], the functionality of Redisson PRO's `RReliableQueue` on redissun's own Lua backend.

use super::message::{
    pack_headers, unpack_headers, AckMode, Dedup, Message, MessageArgs, Nack, ProcessingMode,
    QueueEvent, QueueOperation,
};
use super::scripts;
use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::lua::Lua;
use crate::object::{tagged, HasKey, Key};
use crate::pubsub::Subscription;
use bytes::Bytes;
use futures::{stream, Stream};
use redis::Value;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::time::Instant;

/// How long a waiting call sleeps at most between two checks when no notification arrives.
const FALLBACK: Duration = Duration::from_secs(2);
/// How often an add waits for room in a full queue.
const FULL_RETRY: Duration = Duration::from_millis(100);

fn millis(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

/// The configuration of a [`ReliableQueue`], Redisson's `QueueConfig`. [`set_config`](ReliableQueue::set_config) replaces the whole configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
#[must_use]
pub struct QueueConfig {
    delivery_limit: u32,
    visibility: Duration,
    ttl: Duration,
    dead_letter_queue: Option<String>,
    max_message_size: usize,
    delay: Duration,
    max_size: usize,
    processing_mode: ProcessingMode,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            delivery_limit: 10,
            visibility: Duration::from_secs(30),
            ttl: Duration::ZERO,
            dead_letter_queue: None,
            max_message_size: 0,
            delay: Duration::ZERO,
            max_size: 0,
            processing_mode: ProcessingMode::Parallel,
        }
    }
}

impl QueueConfig {
    /// Redisson's defaults: delivery limit 10, visibility 30 s, no TTL, no delay, no size limits, parallel processing.
    pub fn new() -> Self {
        Self::default()
    }

    /// The deliveries after which a message goes to the dead-letter queue (or is deleted). `0` means no limit.
    #[doc(alias = "deliveryLimit")]
    pub fn delivery_limit(mut self, limit: u32) -> Self {
        self.delivery_limit = limit;
        self
    }

    /// How long a polled message stays invisible before it is delivered again.
    pub fn visibility(mut self, visibility: Duration) -> Self {
        self.visibility = visibility;
        self
    }

    /// How long a message lives; zero for ever.
    #[doc(alias = "timeToLive")]
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// The queue that receives the messages over their delivery limit and the rejected ones.
    #[doc(alias = "deadLetterQueueName")]
    pub fn dead_letter_queue(mut self, name: impl Into<String>) -> Self {
        self.dead_letter_queue = Some(name.into());
        self
    }

    /// The largest payload in bytes; zero for no limit.
    #[doc(alias = "maxMessageSize")]
    pub fn max_message_size(mut self, bytes: usize) -> Self {
        self.max_message_size = bytes;
        self
    }

    /// The delay of every message without its own.
    pub fn delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    /// The most messages the queue holds (ready, delayed and unacknowledged); zero for no limit.
    #[doc(alias = "maxSize")]
    pub fn max_size(mut self, size: usize) -> Self {
        self.max_size = size;
        self
    }

    /// Parallel or sequential processing.
    #[doc(alias = "processingMode")]
    pub fn processing_mode(mut self, mode: ProcessingMode) -> Self {
        self.processing_mode = mode;
        self
    }

    fn fields(&self) -> Vec<(&'static str, String)> {
        let zero_empty = |n: i64| if n == 0 { String::new() } else { n.to_string() };
        vec![
            ("limit", self.delivery_limit.to_string()),
            ("visibility", millis(self.visibility).to_string()),
            ("ttl", zero_empty(millis(self.ttl))),
            ("dlq", self.dead_letter_queue.clone().unwrap_or_default()),
            (
                "max-message-size",
                zero_empty(i64::try_from(self.max_message_size).unwrap_or(i64::MAX)),
            ),
            ("delay", zero_empty(millis(self.delay))),
            (
                "max-size",
                zero_empty(i64::try_from(self.max_size).unwrap_or(i64::MAX)),
            ),
            (
                "mode",
                match self.processing_mode {
                    ProcessingMode::Parallel => String::new(),
                    ProcessingMode::Sequential => "sequential".into(),
                },
            ),
        ]
    }
}

/// Where [`move_to`](ReliableQueue::move_to) takes messages from, Redisson's `QueueMoveArgs`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum QueueMove {
    /// These ids.
    Ids(Vec<String>),
    /// The first `n` ready messages, in delivery order.
    Count(usize),
}

/// **Experimental.** A queue with acknowledgements, visibility timeouts, redelivery, priorities, delays, TTL, deduplication and a dead-letter queue: the functionality of Redisson PRO's `RReliableQueue`.
///
/// Redisson's implementation is not open source, so the data layout and the scripts are redissun's own, and a Redisson PRO client cannot share the queue. Every key lives in the slot of `{name}`. Times (delays, visibility, TTL) are the server's, so clients with skewed clocks agree. Expired, delayed and timed-out messages are handled at the start of every script that reads the queue, in bounded steps; no background task is needed.
///
/// ```no_run
/// # async fn run(client: redissun::Client) -> redissun::Result<()> {
/// use std::time::Duration;
///
/// let jobs = client.reliable_queue::<String>("jobs");
/// jobs.add("resize photo.jpg").priority(7).await?;
/// if let Some(job) = jobs.poll().timeout(Duration::from_secs(5)).await? {
///     println!("{}", job.payload);
///     job.ack().await?;
/// }
/// # Ok(())
/// # }
/// ```
pub struct ReliableQueue<V, C: Codec> {
    key: Key,
    codec: C,
    _marker: PhantomData<fn() -> V>,
}

impl<V, C: Codec> Clone for ReliableQueue<V, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
            _marker: PhantomData,
        }
    }
}

impl<V, C: Codec> fmt::Debug for ReliableQueue<V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "ReliableQueue")
    }
}

/// The key suffixes of a queue, in the scripts' KEYS order after the ready set.
const SUFFIXES: [&str; 11] = [
    ":delayed",
    ":unacked",
    ":expiry",
    ":payloads",
    ":meta",
    ":dedup",
    ":dedup:exp",
    ":config",
    ":disabled",
    ":seq",
    ":dead",
];

fn keys_of(base: &str) -> Vec<String> {
    std::iter::once(base.to_string())
        .chain(SUFFIXES.iter().map(|s| format!("{base}{s}")))
        .collect()
}

impl<V, C: Codec> HasKey for ReliableQueue<V, C> {
    fn key(&self) -> &Key {
        &self.key
    }

    fn companions(&self) -> Vec<String> {
        keys_of(self.key.name()).split_off(1)
    }

    fn renamed_companions(&self, new_name: &str) -> Option<Vec<String>> {
        Some(keys_of(&tagged(new_name)).split_off(1))
    }

    fn renamed_key(&self, new_name: &str) -> String {
        tagged(new_name)
    }
}

/// One parsed message row: id, payload, headers, deliveries.
type Row = (String, Option<Bytes>, Bytes, u32);

fn bytes_of(value: &Value) -> Option<Bytes> {
    crate::reply::bytes(value)
}

fn rows(values: &[Value]) -> Result<Vec<Row>> {
    values
        .chunks(4)
        .map(|chunk| match chunk {
            [id, payload, headers, deliveries] => Ok((
                crate::reply::text(id).ok_or_else(crate::reply::malformed)?,
                bytes_of(payload),
                bytes_of(headers).unwrap_or_default(),
                crate::reply::number(deliveries)
                    .and_then(|d| u32::try_from(d).ok())
                    .unwrap_or(0),
            )),
            _ => Err(crate::reply::malformed()),
        })
        .collect()
}

impl<V, C: Codec> ReliableQueue<V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self {
            key,
            codec,
            _marker: PhantomData,
        }
    }

    fn base(&self) -> &str {
        self.key.name()
    }

    fn wake_channel(&self) -> String {
        format!("redissun_rq:{}", self.base())
    }

    fn event_channel(&self) -> String {
        format!("redissun_rq_events:{}", self.base())
    }

    async fn run<T: redis::FromRedisValue>(
        &self,
        script: &Lua,
        args: Vec<Bytes>,
        once: bool,
    ) -> Result<T> {
        let mut argv = vec![
            Bytes::from(self.wake_channel()),
            Bytes::from(self.event_channel()),
        ];
        argv.extend(args);
        let keys = keys_of(self.base());
        if once {
            self.key.core.eval_once(script, keys, argv).await
        } else {
            self.key.core.eval(script, keys, argv).await
        }
    }

    /// Replaces the configuration, Redisson's `setConfig`. A dead-letter queue learns this queue as a source.
    #[doc(alias = "setConfig")]
    pub async fn set_config(&self, config: &QueueConfig) -> Result<()> {
        self.write_config(config, false).await.map(drop)
    }

    /// Sets the configuration only when the queue has none, Redisson's `setConfigIfAbsent`. Resolves to whether it did.
    #[doc(alias = "setConfigIfAbsent")]
    pub async fn set_config_if_absent(&self, config: &QueueConfig) -> Result<bool> {
        self.write_config(config, true).await
    }

    async fn write_config(&self, config: &QueueConfig, if_absent: bool) -> Result<bool> {
        let mut args = vec![Bytes::from_static(if if_absent { b"1" } else { b"0" })];
        for (field, value) in config.fields() {
            args.push(Bytes::from_static(field.as_bytes()));
            args.push(Bytes::from(value));
        }
        let written: i64 = self.run(&scripts::SET_CONFIG, args, false).await?;
        if written == 1 {
            if let Some(dlq) = &config.dead_letter_queue {
                let mut cmd = redis::cmd("SADD");
                cmd.arg(format!("{}:sources", tagged(dlq))).arg(self.base());
                let sources = format!("{}:sources", tagged(dlq));
                let _: i64 = self
                    .key
                    .core
                    .query(crate::client::request::Request::new(cmd).key(sources.as_bytes()))
                    .await?;
            }
        }
        Ok(written == 1)
    }

    /// The queues that use this one as their dead-letter queue, Redisson's `getDeadLetterQueueSources`.
    #[doc(alias = "getDeadLetterQueueSources")]
    pub async fn dead_letter_sources(&self) -> Result<Vec<String>> {
        let mut cmd = redis::cmd("SMEMBERS");
        let key = format!("{}:sources", self.base());
        cmd.arg(&key);
        let mut sources: Vec<String> = self
            .key
            .core
            .query(crate::client::request::Request::new(cmd).key(key.as_bytes()))
            .await?;
        sources.sort();
        Ok(sources)
    }

    async fn counts(&self) -> Result<(usize, usize, usize)> {
        let (ready, delayed, unacked): (usize, usize, usize) =
            self.run(&scripts::COUNTS, Vec::new(), false).await?;
        Ok((ready, delayed, unacked))
    }

    /// The ready messages, Redisson's `size`.
    #[doc(alias = "size")]
    pub async fn len(&self) -> Result<usize> {
        Ok(self.counts().await?.0)
    }

    /// Whether no message is ready.
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// The delayed messages, Redisson's `countDelayedMessages`.
    #[doc(alias = "countDelayedMessages")]
    pub async fn delayed_len(&self) -> Result<usize> {
        Ok(self.counts().await?.1)
    }

    /// The polled messages not yet acknowledged, Redisson's `countUnacknowledgedMessages`.
    #[doc(alias = "countUnacknowledgedMessages")]
    pub async fn unacked_len(&self) -> Result<usize> {
        Ok(self.counts().await?.2)
    }

    /// Deletes every message, Redisson's `clear`. The configuration stays. Resolves to whether there was any.
    pub async fn clear(&self) -> Result<bool> {
        let cleared: i64 = self.run(&scripts::CLEAR, Vec::new(), false).await?;
        Ok(cleared == 1)
    }

    /// Whether a message with `id` is in the queue, in any state.
    pub async fn contains(&self, id: &str) -> Result<bool> {
        Ok(self.contains_many(&[id]).await? == 1)
    }

    /// How many of `ids` are in the queue, Redisson's `containsMany`.
    #[doc(alias = "containsMany")]
    pub async fn contains_many(&self, ids: &[&str]) -> Result<usize> {
        if ids.is_empty() {
            return Ok(0);
        }
        let mut cmd = redis::cmd("HMGET");
        cmd.arg(format!("{}:payloads", self.base()));
        for id in ids {
            cmd.arg(*id);
        }
        let found: Vec<Option<Bytes>> = self.key.query(cmd).await?;
        Ok(found.iter().filter(|f| f.is_some()).count())
    }

    /// Removes the messages `ids` in any state, Redisson's `remove` and `removeMany`. Resolves to how many there were.
    #[doc(alias = "removeMany")]
    pub async fn remove(&self, ids: &[&str]) -> Result<usize> {
        if ids.is_empty() {
            return Ok(0);
        }
        let args = ids
            .iter()
            .map(|id| Bytes::copy_from_slice(id.as_bytes()))
            .collect();
        let removed: i64 = self.run(&scripts::REMOVE, args, false).await?;
        Ok(usize::try_from(removed).unwrap_or(0))
    }

    /// Acknowledges the messages `ids`, Redisson's `acknowledge`: they are removed. Resolves to how many were unacknowledged. [`Delivery::ack`] does this for one delivery.
    #[doc(alias = "acknowledge")]
    pub async fn ack(&self, ids: &[&str]) -> Result<usize> {
        if ids.is_empty() {
            return Ok(0);
        }
        let args = ids
            .iter()
            .map(|id| Bytes::copy_from_slice(id.as_bytes()))
            .collect();
        let done: i64 = self.run(&scripts::ACK, args, false).await?;
        Ok(usize::try_from(done).unwrap_or(0))
    }

    /// Negatively acknowledges the messages `ids`, Redisson's `negativeAcknowledge`. Resolves to how many were unacknowledged.
    #[doc(alias = "negativeAcknowledge")]
    pub async fn nack(&self, ids: &[&str], nack: Nack) -> Result<usize> {
        if ids.is_empty() {
            return Ok(0);
        }
        let (status, delay) = match nack {
            Nack::Failed { delay } => ("failed", millis(delay)),
            Nack::Rejected => ("rejected", 0),
        };
        let mut args = vec![
            Bytes::from_static(status.as_bytes()),
            Bytes::from(delay.to_string()),
        ];
        args.extend(ids.iter().map(|id| Bytes::copy_from_slice(id.as_bytes())));
        let (done, dead): (i64, i64) = self.run(&scripts::NACK, args, false).await?;
        if dead > 0 {
            self.move_dead_letters().await?;
        }
        Ok(usize::try_from(done).unwrap_or(0))
    }

    /// Turns `operation` off; while it is off it fails with [`Error::OperationDisabled`], Redisson's `disableOperation`.
    #[doc(alias = "disableOperation")]
    pub async fn disable(&self, operation: QueueOperation) -> Result<()> {
        self.operation(operation, true).await
    }

    /// Turns `operation` back on, Redisson's `enableOperation`.
    #[doc(alias = "enableOperation")]
    pub async fn enable(&self, operation: QueueOperation) -> Result<()> {
        self.operation(operation, false).await
    }

    async fn operation(&self, operation: QueueOperation, disable: bool) -> Result<()> {
        let args = vec![
            Bytes::from_static(if disable { b"1" } else { b"0" }),
            Bytes::from_static(operation.name().as_bytes()),
        ];
        let _: i64 = self.run(&scripts::OPERATION, args, false).await?;
        Ok(())
    }

    /// Starts listening to the queue's changes, Redisson's `addListener`. Dropping it stops, Redisson's `removeListener`.
    #[doc(alias = "addListener")]
    pub async fn events(&self) -> Result<QueueEvents> {
        let (subscription, receiver) = self
            .key
            .core
            .pubsub()
            .await?
            .subscribe_with_messages(&self.event_channel())
            .await?;
        Ok(QueueEvents {
            _subscription: subscription,
            receiver,
        })
    }

    /// Moves the dead letters waiting in this queue to its dead-letter queue: added there with the same ids, then deleted here. A crash between the two steps repeats the move later; the target skips ids it has.
    async fn move_dead_letters(&self) -> Result<()> {
        let mut cmd = redis::cmd("HGET");
        cmd.arg(format!("{}:config", self.base())).arg("dlq");
        let dlq: Option<String> = self.key.query(cmd).await?;
        let Some(dlq) = dlq else {
            return Ok(());
        };
        let target = ReliableQueue::<V, C>::new(
            Key::new(self.key.core.clone(), tagged(&dlq)),
            self.codec.clone(),
        );
        loop {
            let peeked: Vec<Value> = self.run(&scripts::DEAD_PEEK, Vec::new(), false).await?;
            if peeked.is_empty() {
                return Ok(());
            }
            let mut args = Vec::new();
            let mut ids = Vec::new();
            for group in peeked.chunks(4) {
                for value in group {
                    args.push(bytes_of(value).unwrap_or_default());
                }
                if let Some(id) = group.first().and_then(bytes_of) {
                    ids.push(id);
                }
            }
            let _: i64 = target.run(&scripts::RECEIVE, args, false).await?;
            let _: i64 = self.run(&scripts::DEAD_DONE, ids, false).await?;
        }
    }
}

impl<V, C> ReliableQueue<V, C>
where
    V: DeserializeOwned,
    C: Codec,
{
    fn message(&self, (id, payload, headers, deliveries): Row) -> Result<Option<Message<V, C>>> {
        let Some(payload) = payload else {
            return Ok(None);
        };
        Ok(Some(Message {
            id,
            payload: self.codec.decode(&payload)?,
            deliveries,
            headers: unpack_headers(&headers)?,
            codec: self.codec.clone(),
        }))
    }

    /// The message `id` without changing it, Redisson's `get`.
    pub async fn get(&self, id: &str) -> Result<Option<Message<V, C>>> {
        Ok(self.get_many(&[id]).await?.into_iter().next().flatten())
    }

    /// The messages `ids`, in order, `None` for a missing one, Redisson's `getAll`.
    #[doc(alias = "getAll")]
    pub async fn get_many(&self, ids: &[&str]) -> Result<Vec<Option<Message<V, C>>>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let args = ids
            .iter()
            .map(|id| Bytes::copy_from_slice(id.as_bytes()))
            .collect();
        let reply: Vec<Value> = self.run(&scripts::GET, args, false).await?;
        rows(&reply)?
            .into_iter()
            .map(|row| self.message(row))
            .collect()
    }

    /// Every message in any state, read in pages, Redisson's `listAll`. Order is not defined.
    #[doc(alias = "listAll")]
    pub async fn list_all(&self) -> Result<Vec<Message<V, C>>> {
        let mut cursor = "0".to_string();
        let mut all = Vec::new();
        loop {
            let page: Vec<String> = self
                .run(&scripts::IDS, vec![Bytes::from(cursor.clone())], false)
                .await?;
            let Some((next, ids)) = page.split_first() else {
                return Ok(all);
            };
            let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
            all.extend(self.get_many(&ids).await?.into_iter().flatten());
            if next == "0" {
                return Ok(all);
            }
            cursor = next.clone();
        }
    }

    /// Adds one message, Redisson's `add`. Options: [`Add::priority`], [`Add::delay`], [`Add::ttl`], [`Add::delivery_limit`], [`Add::dedup_by_hash`], [`Add::dedup_by_id`], [`Add::header`], [`Add::timeout`]. Resolves to the message, `None` when it was deduplicated.
    pub fn add<W: Serialize + ?Sized>(&self, payload: &W) -> Add<'_, V, C>
    where
        V: std::borrow::Borrow<W>,
    {
        Add {
            queue: self,
            args: MessageArgs::new(self.codec.encode(payload)),
            timeout: None,
        }
    }

    /// Adds several messages in one script, all or none, Redisson's `addMany`. Resolves to one entry per message, `None` for a deduplicated one. With [`AddMany::timeout`] it waits for room in a full queue.
    #[doc(alias = "addMany")]
    pub fn add_many(&self, messages: impl IntoIterator<Item = MessageArgs<V>>) -> AddMany<'_, V, C>
    where
        V: Serialize,
    {
        let encoded = messages
            .into_iter()
            .map(|m| {
                let payload = self.codec.encode(&m.payload);
                MessageArgs {
                    payload,
                    priority: m.priority,
                    delay: m.delay,
                    ttl: m.ttl,
                    delivery_limit: m.delivery_limit,
                    dedup: m.dedup,
                    headers: m.headers,
                }
            })
            .collect();
        AddMany {
            queue: self,
            messages: encoded,
            timeout: None,
        }
    }

    async fn add_encoded(
        &self,
        messages: Vec<MessageArgs<Result<Bytes>>>,
        timeout: Option<Duration>,
    ) -> Result<Vec<Option<Message<V, C>>>> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }
        let mut args = vec![Bytes::from(messages.len().to_string())];
        let mut kept = Vec::with_capacity(messages.len());
        for m in messages {
            let payload = m.payload?;
            let headers: Vec<(String, Bytes)> = m
                .headers
                .into_iter()
                .map(|(name, value)| Ok((name, self.codec.encode(&value?)?)))
                .collect::<Result<_>>()?;
            let (mode, id, window) = match &m.dedup {
                None => ("0", String::new(), 0),
                Some(Dedup::ByHash(w)) => ("1", String::new(), millis(*w)),
                Some(Dedup::ById(id, w)) => ("2", id.clone(), millis(*w)),
            };
            args.extend([
                Bytes::from(m.priority.to_string()),
                Bytes::from(m.delay.map_or(-1, millis).to_string()),
                Bytes::from(m.ttl.map_or(-1, millis).to_string()),
                Bytes::from(m.delivery_limit.to_string()),
                Bytes::from_static(mode.as_bytes()),
                Bytes::from(id),
                Bytes::from(window.to_string()),
                payload.clone(),
                pack_headers(&headers),
            ]);
            kept.push((payload, headers));
        }
        let deadline = timeout.map(|t| Instant::now() + t);
        loop {
            let reply: Vec<Value> = self.run(&scripts::ADD, args.clone(), true).await?;
            let status = reply
                .first()
                .and_then(crate::reply::text)
                .unwrap_or_default();
            match status.as_str() {
                "ok" => {
                    return reply[1..]
                        .iter()
                        .zip(kept)
                        .map(|(id, (payload, headers))| {
                            let id = crate::reply::text(id).unwrap_or_default();
                            if id.is_empty() {
                                return Ok(None);
                            }
                            Ok(Some(Message {
                                id,
                                payload: self.codec.decode(&payload)?,
                                deliveries: 0,
                                headers,
                                codec: self.codec.clone(),
                            }))
                        })
                        .collect();
                }
                "disabled" => return Err(Error::OperationDisabled("add".into())),
                "size" => {
                    let max = reply
                        .get(1)
                        .and_then(crate::reply::text)
                        .unwrap_or_default();
                    return Err(Error::Config(format!(
                        "the payload is larger than the queue's max message size of {max} bytes"
                    )));
                }
                "full" => match deadline {
                    Some(deadline) if Instant::now() < deadline => {
                        tokio::time::sleep(FULL_RETRY.min(deadline - Instant::now())).await;
                    }
                    _ => return Err(Error::Redis("the queue is full".into())),
                },
                _ => return Err(crate::reply::malformed()),
            }
        }
    }

    /// Takes the next ready message, Redisson's `poll`. Options: [`Poll::visibility`], [`Poll::timeout`] (wait for one), [`Poll::ack_mode`]. Resolves to `None` when no message is ready (in time).
    pub fn poll(&self) -> Poll<'_, V, C> {
        Poll {
            queue: self,
            count: 1,
            visibility: None,
            timeout: None,
            ack_mode: AckMode::Manual,
        }
    }

    /// Takes up to `count` ready messages, Redisson's `pollMany`, with the options of [`poll`](ReliableQueue::poll). With a timeout it waits until at least one is ready.
    #[doc(alias = "pollMany")]
    pub fn poll_many(&self, count: usize) -> PollMany<'_, V, C> {
        PollMany(Poll {
            queue: self,
            count: count.max(1),
            visibility: None,
            timeout: None,
            ack_mode: AckMode::Manual,
        })
    }

    async fn poll_once(
        &self,
        count: usize,
        visibility: i64,
        auto: bool,
    ) -> Result<(Vec<Delivery<'_, V, C>>, i64)> {
        let args = vec![
            Bytes::from(count.to_string()),
            Bytes::from(visibility.to_string()),
            Bytes::from_static(if auto { b"1" } else { b"0" }),
        ];
        let reply: Vec<Value> = self.run(&scripts::POLL, args, true).await?;
        let status = reply
            .first()
            .and_then(crate::reply::text)
            .unwrap_or_default();
        if status == "disabled" {
            return Err(Error::OperationDisabled("poll".into()));
        }
        let due = reply.get(1).and_then(crate::reply::number).unwrap_or(-1);
        let dead = reply.get(2).and_then(crate::reply::number).unwrap_or(0);
        if dead > 0 {
            self.move_dead_letters().await?;
        }
        let deliveries = rows(reply.get(3..).unwrap_or_default())?
            .into_iter()
            .filter_map(|row| self.message(row).transpose())
            .map(|message| {
                message.map(|message| Delivery {
                    queue: self,
                    message,
                    acked: auto,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((deliveries, due))
    }

    async fn poll_wait(
        &self,
        count: usize,
        visibility: Option<Duration>,
        timeout: Option<Duration>,
        ack_mode: AckMode,
    ) -> Result<Vec<Delivery<'_, V, C>>> {
        let visibility = visibility.map_or(0, millis);
        let auto = ack_mode == AckMode::Auto;
        let Some(timeout) = timeout else {
            return Ok(self.poll_once(count, visibility, auto).await?.0);
        };
        let deadline = Instant::now() + timeout;
        let subscription: Subscription = self
            .key
            .core
            .pubsub()
            .await?
            .subscribe(&self.wake_channel())
            .await?;
        let notify = subscription.notify();
        loop {
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let (found, due) = self.poll_once(count, visibility, auto).await?;
            if !found.is_empty() {
                return Ok(found);
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Ok(Vec::new());
            }
            let mut pause = left.min(FALLBACK);
            if due >= 0 {
                pause = pause.min(Duration::from_millis(u64::try_from(due).unwrap_or(0) + 1));
            }
            let _ = tokio::time::timeout(pause, notified).await;
        }
    }

    /// A stream of deliveries: a loop of polls that waits up to `wait` each time, for a consumer task. It never ends; drop it to stop.
    pub fn consumer(&self, wait: Duration) -> impl Stream<Item = Result<Delivery<'_, V, C>>> + '_ {
        stream::unfold(
            Vec::<Delivery<'_, V, C>>::new(),
            move |mut buffered| async move {
                loop {
                    if let Some(next) = buffered.pop() {
                        return Some((Ok(next), buffered));
                    }
                    match self.poll_wait(1, None, Some(wait), AckMode::Manual).await {
                        Ok(found) => buffered = found,
                        Err(e) => return Some((Err(e), buffered)),
                    }
                }
            },
        )
    }

    /// Moves messages to the queue `destination`, Redisson's `move`: added there with the same ids, then removed here. Not atomic across the two queues; resolves to how many moved.
    #[doc(alias = "move")]
    pub async fn move_to(&self, destination: &str, which: QueueMove) -> Result<usize> {
        let ids: Vec<String> = match which {
            QueueMove::Ids(ids) => ids,
            QueueMove::Count(0) => return Ok(0),
            QueueMove::Count(n) => {
                self.run(
                    &scripts::FIRST_READY,
                    vec![Bytes::from(n.to_string())],
                    false,
                )
                .await?
            }
        };
        if ids.is_empty() {
            return Ok(0);
        }
        let args: Vec<Bytes> = ids.iter().map(|id| Bytes::from(id.clone())).collect();
        let reply: Vec<Value> = self.run(&scripts::GET, args, false).await?;
        let mut moved = Vec::new();
        let mut receive = Vec::new();
        for (id, payload, headers, _) in rows(&reply)? {
            let Some(payload) = payload else { continue };
            let mut cmd = redis::cmd("HGET");
            cmd.arg(format!("{}:meta", self.base()))
                .arg(format!("{id}:p"));
            let priority: Option<String> = self.key.query(cmd).await?;
            receive.extend([
                Bytes::from(id.clone()),
                payload,
                headers,
                Bytes::from(priority.unwrap_or_else(|| "4".into())),
            ]);
            moved.push(id);
        }
        if moved.is_empty() {
            return Ok(0);
        }
        let target = ReliableQueue::<V, C>::new(
            Key::new(self.key.core.clone(), tagged(destination)),
            self.codec.clone(),
        );
        let _: i64 = target.run(&scripts::RECEIVE, receive, false).await?;
        let ids: Vec<&str> = moved.iter().map(String::as_str).collect();
        self.remove(&ids).await
    }
}

/// A pending [`ReliableQueue::add`].
#[must_use = "an add does nothing until it is awaited"]
pub struct Add<'a, V, C: Codec> {
    queue: &'a ReliableQueue<V, C>,
    args: MessageArgs<Result<Bytes>>,
    timeout: Option<Duration>,
}

impl<V, C: Codec> Add<'_, V, C> {
    /// See [`MessageArgs::priority`].
    pub fn priority(mut self, priority: u8) -> Self {
        self.args = self.args.priority(priority);
        self
    }

    /// See [`MessageArgs::delay`].
    pub fn delay(mut self, delay: Duration) -> Self {
        self.args = self.args.delay(delay);
        self
    }

    /// See [`MessageArgs::ttl`].
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.args = self.args.ttl(ttl);
        self
    }

    /// See [`MessageArgs::delivery_limit`].
    pub fn delivery_limit(mut self, limit: u32) -> Self {
        self.args = self.args.delivery_limit(limit);
        self
    }

    /// See [`MessageArgs::dedup_by_hash`].
    pub fn dedup_by_hash(mut self, window: Duration) -> Self {
        self.args = self.args.dedup_by_hash(window);
        self
    }

    /// See [`MessageArgs::dedup_by_id`].
    pub fn dedup_by_id(mut self, id: impl Into<String>, window: Duration) -> Self {
        self.args = self.args.dedup_by_id(id, window);
        self
    }

    /// See [`MessageArgs::header`].
    pub fn header<T: Serialize + ?Sized>(mut self, name: impl Into<String>, value: &T) -> Self {
        self.args = self.args.header(name, value);
        self
    }

    /// Waits up to `timeout` for room when the queue is full, instead of failing at once.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

impl<'a, V, C> IntoFuture for Add<'a, V, C>
where
    V: DeserializeOwned + Send + Sync + 'a,
    C: Codec,
{
    type Output = Result<Option<Message<V, C>>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            Ok(self
                .queue
                .add_encoded(vec![self.args], self.timeout)
                .await?
                .into_iter()
                .next()
                .flatten())
        })
    }
}

/// A pending [`ReliableQueue::add_many`].
#[must_use = "an add does nothing until it is awaited"]
pub struct AddMany<'a, V, C: Codec> {
    queue: &'a ReliableQueue<V, C>,
    messages: Vec<MessageArgs<Result<Bytes>>>,
    timeout: Option<Duration>,
}

impl<V, C: Codec> AddMany<'_, V, C> {
    /// Waits up to `timeout` for room when the queue is full.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

impl<'a, V, C> IntoFuture for AddMany<'a, V, C>
where
    V: DeserializeOwned + Send + Sync + 'a,
    C: Codec,
{
    type Output = Result<Vec<Option<Message<V, C>>>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.queue.add_encoded(self.messages, self.timeout).await })
    }
}

/// A pending [`ReliableQueue::poll`].
#[must_use = "a poll does nothing until it is awaited"]
pub struct Poll<'a, V, C: Codec> {
    queue: &'a ReliableQueue<V, C>,
    count: usize,
    visibility: Option<Duration>,
    timeout: Option<Duration>,
    ack_mode: AckMode,
}

impl<V, C: Codec> Poll<'_, V, C> {
    /// How long the message stays invisible before it is delivered again, instead of the queue's visibility.
    pub fn visibility(mut self, visibility: Duration) -> Self {
        self.visibility = Some(visibility);
        self
    }

    /// Waits up to `timeout` for a message.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Manual (the default) or automatic acknowledgement, Redisson's `acknowledgeMode`.
    #[doc(alias = "acknowledgeMode")]
    pub fn ack_mode(mut self, mode: AckMode) -> Self {
        self.ack_mode = mode;
        self
    }
}

impl<'a, V, C> IntoFuture for Poll<'a, V, C>
where
    V: DeserializeOwned + Send + Sync + 'a,
    C: Codec,
{
    type Output = Result<Option<Delivery<'a, V, C>>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            Ok(self
                .queue
                .poll_wait(self.count, self.visibility, self.timeout, self.ack_mode)
                .await?
                .into_iter()
                .next())
        })
    }
}

/// A pending [`ReliableQueue::poll_many`]; the options of [`Poll`].
#[must_use = "a poll does nothing until it is awaited"]
pub struct PollMany<'a, V, C: Codec>(Poll<'a, V, C>);

impl<V, C: Codec> PollMany<'_, V, C> {
    /// See [`Poll::visibility`].
    pub fn visibility(self, visibility: Duration) -> Self {
        Self(self.0.visibility(visibility))
    }

    /// See [`Poll::timeout`].
    pub fn timeout(self, timeout: Duration) -> Self {
        Self(self.0.timeout(timeout))
    }

    /// See [`Poll::ack_mode`].
    pub fn ack_mode(self, mode: AckMode) -> Self {
        Self(self.0.ack_mode(mode))
    }
}

impl<'a, V, C> IntoFuture for PollMany<'a, V, C>
where
    V: DeserializeOwned + Send + Sync + 'a,
    C: Codec,
{
    type Output = Result<Vec<Delivery<'a, V, C>>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        let p = self.0;
        Box::pin(async move {
            p.queue
                .poll_wait(p.count, p.visibility, p.timeout, p.ack_mode)
                .await
        })
    }
}

/// A polled message with its queue: [`ack`](Delivery::ack) or [`nack`](Delivery::nack) it. Dropped without either, it is delivered again after its visibility, as in Redisson.
pub struct Delivery<'a, V, C: Codec> {
    queue: &'a ReliableQueue<V, C>,
    message: Message<V, C>,
    acked: bool,
}

impl<V: fmt::Debug, C: Codec> fmt::Debug for Delivery<'_, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Delivery")
            .field("message", &self.message)
            .finish_non_exhaustive()
    }
}

impl<V, C: Codec> std::ops::Deref for Delivery<'_, V, C> {
    type Target = Message<V, C>;

    fn deref(&self) -> &Self::Target {
        &self.message
    }
}

impl<V, C: Codec> Delivery<'_, V, C> {
    /// Acknowledges the message: it is removed. Resolves to `false` when it was no longer unacknowledged (its visibility ran out, or it was polled with [`AckMode::Auto`]).
    pub async fn ack(self) -> Result<bool> {
        if self.acked {
            return Ok(false);
        }
        Ok(self.queue.ack(&[self.message.id.as_str()]).await? == 1)
    }

    /// Negatively acknowledges the message, see [`Nack`].
    pub async fn nack(self, nack: Nack) -> Result<bool> {
        if self.acked {
            return Ok(false);
        }
        Ok(self.queue.nack(&[self.message.id.as_str()], nack).await? == 1)
    }

    /// The message, taken out of the delivery; it stays unacknowledged.
    pub fn into_message(self) -> Message<V, C> {
        self.message
    }
}

/// The changes of a [`ReliableQueue`]. Dropping it stops listening.
pub struct QueueEvents {
    _subscription: Subscription,
    receiver: broadcast::Receiver<Bytes>,
}

impl fmt::Debug for QueueEvents {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueueEvents").finish_non_exhaustive()
    }
}

impl QueueEvents {
    /// Waits for the next event. A listener more than 256 messages behind gets [`Error::Lagged`] once.
    pub async fn recv(&mut self) -> Result<QueueEvent> {
        loop {
            match self.receiver.recv().await {
                Ok(payload) => {
                    if let Some(event) = QueueEvent::parse(&payload) {
                        return Ok(event);
                    }
                }
                Err(RecvError::Lagged(n)) => return Err(Error::Lagged(n as usize)),
                Err(RecvError::Closed) => {
                    return Err(Error::Redis("the subscription was closed".into()))
                }
            }
        }
    }

    /// Turns the listener into a stream of events. It never ends.
    pub fn into_stream(self) -> impl Stream<Item = Result<QueueEvent>> {
        stream::unfold(self, |mut events| async move {
            Some((events.recv().await, events))
        })
    }
}
