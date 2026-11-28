//! [`ReliablePubSubTopic`], the functionality of Redisson PRO's `RReliablePubSubTopic`: every subscription is a [`ReliableQueue`] in the topic's slot, fed by one publish script, and the topic keeps a log for [`Position`] and [`Subscription::seek`].

use super::message::{pack_headers, AckMode, Message, MessageArgs};
use super::queue::{keys_of, millis, Delivery, QueueConfig, ReliableQueue};
use super::scripts;
use crate::client::request::Request;
use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::object::Key;
use crate::pubsub::{PatternMessage, Subscription as PubSubscription};
use bytes::Bytes;
use futures::future::select;
use futures::{stream, Stream};
use redis::Value;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast::{self, error::RecvError};

/// What the topic keeps, Redisson's `RetentionMode`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Retention {
    /// A publish needs a subscription; a message leaves the log once no subscription holds it. Redisson's `SUBSCRIPTION_REQUIRED_DELETE_PROCESSED`, the default.
    #[default]
    RequiredDeleteProcessed,
    /// A publish needs a subscription; the log keeps every message until the topic's TTL or max size. `SUBSCRIPTION_REQUIRED_RETAIN_ALL`.
    RequiredRetainAll,
    /// A publish without subscriptions is kept in the log; the log keeps every message until the TTL or max size. `SUBSCRIPTION_OPTIONAL_RETAIN_ALL`.
    OptionalRetainAll,
}

impl Retention {
    fn field(self) -> &'static str {
        match self {
            Retention::RequiredDeleteProcessed => "delete-processed",
            Retention::RequiredRetainAll => "retain-all",
            Retention::OptionalRetainAll => "optional",
        }
    }
}

/// An operation that [`ReliablePubSubTopic::disable`] can turn off, Redisson's `PubSubOperation`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum PubSubOperation {
    /// Publishing.
    Publish,
    /// Pulling, on every subscription.
    Pull,
}

/// Where a subscription starts, Redisson's `Position`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Position {
    /// Only messages published from now on. The default.
    Latest,
    /// Every message the topic still keeps.
    Earliest,
    /// From the message `id`; `inclusive` includes it.
    Id {
        /// The message id.
        id: String,
        /// Whether the message itself is included.
        inclusive: bool,
    },
    /// From a publish time; `inclusive` includes messages published at exactly that millisecond.
    Time {
        /// The time.
        at: SystemTime,
        /// Whether messages at exactly that time are included.
        inclusive: bool,
    },
}

/// The configuration of a [`ReliablePubSubTopic`], Redisson's `TopicConfig`. Visibility and delivery limit are the defaults of new subscriptions.
#[derive(Clone, Debug, PartialEq, Eq)]
#[must_use]
pub struct TopicConfig {
    visibility: Duration,
    ttl: Duration,
    max_message_size: usize,
    delay: Duration,
    max_size: usize,
    delivery_limit: u32,
    retention: Retention,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            visibility: Duration::from_secs(30),
            ttl: Duration::ZERO,
            max_message_size: 0,
            delay: Duration::ZERO,
            max_size: 0,
            delivery_limit: 10,
            retention: Retention::default(),
        }
    }
}

impl TopicConfig {
    /// The defaults: visibility 30 s, delivery limit 10, no TTL, delay or size limits, [`Retention::RequiredDeleteProcessed`].
    pub fn new() -> Self {
        Self::default()
    }

    /// The visibility of new subscriptions.
    pub fn visibility(mut self, visibility: Duration) -> Self {
        self.visibility = visibility;
        self
    }

    /// How long a message lives, in the log and in every subscription; zero for ever.
    #[doc(alias = "timeToLive")]
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
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

    /// The most messages the log holds; a publish over it fails. Zero for no limit.
    #[doc(alias = "maxSize")]
    pub fn max_size(mut self, size: usize) -> Self {
        self.max_size = size;
        self
    }

    /// The delivery limit of new subscriptions.
    #[doc(alias = "deliveryLimit")]
    pub fn delivery_limit(mut self, limit: u32) -> Self {
        self.delivery_limit = limit;
        self
    }

    /// What the topic keeps.
    #[doc(alias = "retentionMode")]
    pub fn retention(mut self, retention: Retention) -> Self {
        self.retention = retention;
        self
    }
}

/// The counters of a topic, Redisson's `TopicStatistics`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct TopicStatistics {
    /// The topic.
    pub name: String,
    /// Messages published, ever.
    pub published: u64,
    /// The subscriptions.
    pub subscriptions: u64,
    /// Delayed messages over every subscription.
    pub delayed: u64,
}

/// The counters of a subscription, Redisson's `SubscriptionStatistics` (the ones this backend keeps).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct SubscriptionStatistics {
    /// The subscription.
    pub name: String,
    /// Its consumers.
    pub consumers: u64,
    /// Messages ready.
    pub ready: u64,
    /// Messages delayed.
    pub delayed: u64,
    /// Messages pulled and not acknowledged.
    pub unacked: u64,
}

/// A change of a topic or one of its subscriptions, Redisson's `PubSubEventListener`s.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TopicEvent {
    /// Messages were published, Redisson's `PublishedEventListener`.
    Published(Vec<String>),
    /// Messages were pulled from `subscription`, Redisson's `PulledEventListener`.
    Pulled {
        /// The subscription.
        subscription: String,
        /// The ids.
        ids: Vec<String>,
    },
    /// Messages were acknowledged in `subscription`.
    Acknowledged {
        /// The subscription.
        subscription: String,
        /// The ids.
        ids: Vec<String>,
    },
    /// Messages were negatively acknowledged in `subscription`.
    NegativelyAcknowledged {
        /// The subscription.
        subscription: String,
        /// The ids.
        ids: Vec<String>,
        /// Whether they were rejected rather than failed.
        rejected: bool,
    },
    /// A publish found the topic full, Redisson's `TopicFullEventListener`.
    Full,
    /// The configuration changed, Redisson's `TopicConfigEventListener`.
    Config,
    /// An operation was disabled.
    Disabled(PubSubOperation),
    /// An operation was enabled.
    Enabled(PubSubOperation),
}

/// **Experimental.** A topic whose subscriptions each get every message with acknowledgements, redelivery, a delivery limit and a dead-letter queue: the functionality of Redisson PRO's `RReliablePubSubTopic`.
///
/// Redisson's implementation is not open source, so the data and scripts are redissun's own. Each subscription is a [`ReliableQueue`] named `{topic}:sub:<name>`, in the topic's slot, and one script publishes to the topic's log and to every subscription. The log serves [`Position`] and [`Subscription::seek`] and is kept as [`Retention`] says.
///
/// ```no_run
/// # async fn run(client: redissun::Client) -> redissun::Result<()> {
/// use std::time::Duration;
///
/// let orders = client.reliable_pubsub_topic::<String>("orders");
/// let billing = orders.create_subscription().name("billing").await?;
/// orders.publish("order 1").await?;
/// let consumer = billing.pull_consumer().await?;
/// if let Some(m) = consumer.pull().timeout(Duration::from_secs(1)).await? {
///     m.ack().await?;
/// }
/// # Ok(())
/// # }
/// ```
pub struct ReliablePubSubTopic<V, C: Codec> {
    key: Key,
    codec: C,
    _marker: std::marker::PhantomData<fn() -> V>,
}

impl<V, C: Codec> Clone for ReliablePubSubTopic<V, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<V, C: Codec> fmt::Debug for ReliablePubSubTopic<V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "ReliablePubSubTopic")
    }
}

const TOPIC_SUFFIXES: [&str; 8] = [
    ":log",
    ":payloads",
    ":meta",
    ":subs",
    ":seq",
    ":config",
    ":disabled",
    ":stats",
];

impl<V, C: Codec> ReliablePubSubTopic<V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self {
            key,
            codec,
            _marker: std::marker::PhantomData,
        }
    }

    fn base(&self) -> &str {
        self.key.name()
    }

    fn topic_keys(&self) -> Vec<String> {
        TOPIC_SUFFIXES
            .iter()
            .map(|s| format!("{}{s}", self.base()))
            .collect()
    }

    fn event_channel(&self) -> String {
        format!("redissun_rt_events:{}", self.base())
    }

    fn queue(&self, subscription: &str) -> ReliableQueue<V, C> {
        ReliableQueue::new(
            Key::new(
                self.key.core.clone(),
                format!("{}:sub:{subscription}", self.base()),
            ),
            self.codec.clone(),
        )
    }

    async fn query<T: redis::FromRedisValue>(&self, cmd: redis::Cmd) -> Result<T> {
        let base = self.base().to_string();
        self.key
            .core
            .query(Request::new(cmd).key(base.as_bytes()))
            .await
    }

    async fn hget(&self, suffix: &str, field: &str) -> Result<Option<String>> {
        let mut cmd = redis::cmd("HGET");
        cmd.arg(format!("{}{suffix}", self.base())).arg(field);
        self.query(cmd).await
    }

    /// Replaces the configuration, Redisson's `setConfig`.
    #[doc(alias = "setConfig")]
    pub async fn set_config(&self, config: &TopicConfig) -> Result<()> {
        self.write_config(config, false).await.map(drop)
    }

    /// Sets the configuration only when the topic has none, Redisson's `setConfigIfAbsent`.
    #[doc(alias = "setConfigIfAbsent")]
    pub async fn set_config_if_absent(&self, config: &TopicConfig) -> Result<bool> {
        self.write_config(config, true).await
    }

    async fn write_config(&self, config: &TopicConfig, if_absent: bool) -> Result<bool> {
        let key = format!("{}:config", self.base());
        if if_absent {
            let mut cmd = redis::cmd("EXISTS");
            cmd.arg(&key);
            let exists: i64 = self.query(cmd).await?;
            if exists == 1 {
                return Ok(false);
            }
        }
        let zero = |n: i64| if n == 0 { String::new() } else { n.to_string() };
        let fields = [
            ("visibility", millis(config.visibility).to_string()),
            ("ttl", zero(millis(config.ttl))),
            (
                "max-message-size",
                zero(i64::try_from(config.max_message_size).unwrap_or(i64::MAX)),
            ),
            ("delay", zero(millis(config.delay))),
            (
                "max-size",
                zero(i64::try_from(config.max_size).unwrap_or(i64::MAX)),
            ),
            ("limit", config.delivery_limit.to_string()),
            ("retention", config.retention.field().to_string()),
        ];
        let mut del = redis::cmd("DEL");
        del.arg(&key);
        let _: i64 = self.query(del).await?;
        let mut cmd = redis::cmd("HSET");
        cmd.arg(&key);
        for (f, v) in fields.iter().filter(|(_, v)| !v.is_empty()) {
            cmd.arg(*f).arg(v);
        }
        let _: i64 = self.query(cmd).await?;
        let mut publish = redis::cmd("PUBLISH");
        publish.arg(self.event_channel()).arg("config\n\n");
        let _: i64 = self.query(publish).await?;
        Ok(true)
    }

    /// The subscription names, sorted, Redisson's `getSubscriptions`.
    #[doc(alias = "getSubscriptions")]
    pub async fn subscriptions(&self) -> Result<Vec<String>> {
        let mut cmd = redis::cmd("HKEYS");
        cmd.arg(format!("{}:subs", self.base()));
        let mut names: Vec<String> = self.query(cmd).await?;
        names.sort();
        Ok(names)
    }

    /// Whether the subscription `name` exists, Redisson's `hasSubscription`.
    #[doc(alias = "hasSubscription")]
    pub async fn has_subscription(&self, name: &str) -> Result<bool> {
        Ok(self.hget(":subs", name).await?.is_some())
    }

    /// The subscription `name`, `None` when it does not exist, Redisson's `getSubscription`.
    #[doc(alias = "getSubscription")]
    pub async fn subscription(&self, name: &str) -> Result<Option<Subscription<V, C>>> {
        if !self.has_subscription(name).await? {
            return Ok(None);
        }
        Ok(Some(Subscription {
            topic: self.clone(),
            name: name.to_string(),
            queue: self.queue(name),
        }))
    }

    /// Creates a subscription, Redisson's `createSubscription`. Options: [`CreateSubscription::name`] (generated without it), [`CreateSubscription::dead_letter_queue`], [`CreateSubscription::delivery_limit`], [`CreateSubscription::visibility`], [`CreateSubscription::position`]. An existing name is an [`Error::Config`].
    #[doc(alias = "createSubscription")]
    pub fn create_subscription(&self) -> CreateSubscription<'_, V, C> {
        CreateSubscription {
            topic: self,
            name: None,
            dead_letter: None,
            delivery_limit: None,
            visibility: None,
            position: Position::Latest,
        }
    }

    /// Removes the subscription and its messages, Redisson's `removeSubscription`. Resolves to whether it existed.
    #[doc(alias = "removeSubscription")]
    pub async fn remove_subscription(&self, name: &str) -> Result<bool> {
        let mut cmd = redis::cmd("HDEL");
        cmd.arg(format!("{}:subs", self.base())).arg(name);
        let removed: i64 = self.query(cmd).await?;
        let mut del = redis::cmd("DEL");
        for key in keys_of(&format!("{}:sub:{name}", self.base())) {
            del.arg(key);
        }
        del.arg(format!("{}:sub:{name}:consumers", self.base()));
        let _: i64 = self.query(del).await?;
        Ok(removed == 1)
    }

    /// The messages in the log, Redisson's `size`.
    #[doc(alias = "size")]
    pub async fn len(&self) -> Result<usize> {
        let mut cmd = redis::cmd("ZCARD");
        cmd.arg(format!("{}:log", self.base()));
        self.query(cmd).await
    }

    /// Whether the log is empty.
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// Whether the log holds `id`.
    pub async fn contains(&self, id: &str) -> Result<bool> {
        Ok(self.contains_many(&[id]).await? == 1)
    }

    /// How many of `ids` the log holds, Redisson's `containsMany`.
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
        let found: Vec<Option<Bytes>> = self.query(cmd).await?;
        Ok(found.iter().filter(|f| f.is_some()).count())
    }

    /// Deletes the log and every subscription's messages; the subscriptions stay. Redisson's `clear`. Resolves to whether there was anything.
    pub async fn clear(&self) -> Result<bool> {
        let had = !self.is_empty().await?;
        let mut del = redis::cmd("DEL");
        for suffix in [":log", ":payloads", ":meta"] {
            del.arg(format!("{}{suffix}", self.base()));
        }
        let _: i64 = self.query(del).await?;
        for name in self.subscriptions().await? {
            self.queue(&name).clear().await?;
        }
        Ok(had)
    }

    /// Deletes the topic: its log, configuration and every subscription.
    pub async fn delete(&self) -> Result<bool> {
        let subs = self.subscriptions().await?;
        for name in &subs {
            self.remove_subscription(name).await?;
        }
        let mut del = redis::cmd("DEL");
        for key in self.topic_keys() {
            del.arg(key);
        }
        let removed: i64 = self.query(del).await?;
        Ok(removed > 0 || !subs.is_empty())
    }

    /// The topics and queues whose subscriptions use this topic's name as their dead-letter queue, Redisson's `getDeadLetterTopicSources`.
    #[doc(alias = "getDeadLetterTopicSources")]
    pub async fn dead_letter_sources(&self) -> Result<Vec<String>> {
        let mut cmd = redis::cmd("SMEMBERS");
        cmd.arg(format!("{}:sources", self.base()));
        let mut sources: Vec<String> = self.query(cmd).await?;
        sources.sort();
        Ok(sources)
    }

    /// Turns `operation` off, Redisson's `disableOperation`: a disabled publish or pull fails with [`Error::OperationDisabled`].
    #[doc(alias = "disableOperation")]
    pub async fn disable(&self, operation: PubSubOperation) -> Result<()> {
        self.operation(operation, true).await
    }

    /// Turns `operation` back on, Redisson's `enableOperation`.
    #[doc(alias = "enableOperation")]
    pub async fn enable(&self, operation: PubSubOperation) -> Result<()> {
        self.operation(operation, false).await
    }

    async fn operation(&self, operation: PubSubOperation, disable: bool) -> Result<()> {
        let name = match operation {
            PubSubOperation::Publish => "publish",
            PubSubOperation::Pull => "pull",
        };
        let mut cmd = redis::cmd(if disable { "SADD" } else { "SREM" });
        cmd.arg(format!("{}:disabled", self.base())).arg(name);
        let _: i64 = self.query(cmd).await?;
        if operation == PubSubOperation::Pull {
            for sub in self.subscriptions().await? {
                let queue = self.queue(&sub);
                if disable {
                    queue.disable(super::message::QueueOperation::Poll).await?;
                } else {
                    queue.enable(super::message::QueueOperation::Poll).await?;
                }
            }
        }
        let mut publish = redis::cmd("PUBLISH");
        publish.arg(self.event_channel()).arg(format!(
            "{}\n{name}\n",
            if disable { "disabled" } else { "enabled" }
        ));
        let _: i64 = self.query(publish).await?;
        Ok(())
    }

    /// The topic's counters, Redisson's `getStatistics`.
    #[doc(alias = "getStatistics")]
    pub async fn statistics(&self) -> Result<TopicStatistics> {
        let published = self
            .hget(":stats", "published")
            .await?
            .and_then(|p| p.parse().ok())
            .unwrap_or(0);
        let subs = self.subscriptions().await?;
        let mut delayed = 0u64;
        for s in &subs {
            delayed += self.queue(s).delayed_len().await? as u64;
        }
        Ok(TopicStatistics {
            name: self.base().to_string(),
            published,
            subscriptions: subs.len() as u64,
            delayed,
        })
    }

    /// Starts listening to the changes of the topic and its subscriptions, Redisson's `addListener`. Drop it to stop.
    #[doc(alias = "addListener")]
    pub async fn events(&self) -> Result<TopicEvents> {
        let pubsub = self.key.core.pubsub().await?.clone();
        let (topic_sub, topic) = pubsub
            .subscribe_with_messages(&self.event_channel())
            .await?;
        let prefix = format!("redissun_rq_events:{}:sub:", self.base());
        let (subs_sub, subs) = pubsub
            .psubscribe_with_messages(&format!("{prefix}*"))
            .await?;
        Ok(TopicEvents {
            _topic_sub: topic_sub,
            _subs_sub: subs_sub,
            topic,
            subs,
            prefix,
        })
    }
}

impl<V, C> ReliablePubSubTopic<V, C>
where
    V: DeserializeOwned,
    C: Codec,
{
    /// Publishes one message to every subscription, Redisson's `publish`. Options as [`MessageArgs`] (deduplication is not supported on a topic). Resolves to the message.
    pub fn publish<W: Serialize + ?Sized>(&self, payload: &W) -> Publish<'_, V, C>
    where
        V: std::borrow::Borrow<W>,
    {
        Publish {
            topic: self,
            messages: vec![MessageArgs::new(self.codec.encode(payload))],
            timeout: None,
        }
    }

    /// Publishes several messages in one script, Redisson's `publishMany`.
    #[doc(alias = "publishMany")]
    pub fn publish_many(
        &self,
        messages: impl IntoIterator<Item = MessageArgs<V>>,
    ) -> PublishMany<'_, V, C>
    where
        V: Serialize,
    {
        let messages = messages
            .into_iter()
            .map(|m| MessageArgs {
                payload: self.codec.encode(&m.payload),
                priority: m.priority,
                delay: m.delay,
                ttl: m.ttl,
                delivery_limit: m.delivery_limit,
                dedup: m.dedup,
                headers: m.headers,
            })
            .collect();
        PublishMany(Publish {
            topic: self,
            messages,
            timeout: None,
        })
    }

    async fn publish_encoded(
        &self,
        messages: Vec<MessageArgs<Result<Bytes>>>,
        timeout: Option<Duration>,
    ) -> Result<Vec<Message<V, C>>> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }
        let mut kept = Vec::with_capacity(messages.len());
        let mut message_args = vec![Bytes::from(messages.len().to_string())];
        for m in messages {
            if m.dedup.is_some() {
                return Err(Error::Config(
                    "deduplication is not supported on a reliable topic".into(),
                ));
            }
            let payload = m.payload?;
            let headers: Vec<(String, Bytes)> = m
                .headers
                .into_iter()
                .map(|(name, value)| Ok((name, self.codec.encode(&value?)?)))
                .collect::<Result<_>>()?;
            message_args.extend([
                Bytes::from(m.priority.to_string()),
                Bytes::from(m.delay.map_or(-1, millis).to_string()),
                Bytes::from(m.ttl.map_or(-1, millis).to_string()),
                Bytes::from(m.delivery_limit.to_string()),
                payload.clone(),
                pack_headers(&headers),
            ]);
            kept.push((payload, headers));
        }
        let deadline = timeout.map(|t| tokio::time::Instant::now() + t);
        loop {
            let subs = self.subscriptions().await?;
            let mut keys = self.topic_keys();
            let mut args = vec![
                Bytes::from(self.event_channel()),
                Bytes::from(subs.len().to_string()),
            ];
            for s in &subs {
                let queue = self.queue(s);
                keys.extend(keys_of(queue.base()));
                args.push(Bytes::from(queue.wake_channel()));
            }
            args.extend(message_args.iter().cloned());
            let reply: Vec<Value> = self
                .key
                .core
                .eval_once(&scripts::PUBLISH, keys, args)
                .await?;
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
                            Ok(Message {
                                id: crate::reply::text(id).ok_or_else(crate::reply::malformed)?,
                                payload: self.codec.decode(&payload)?,
                                deliveries: 0,
                                headers,
                                codec: self.codec.clone(),
                            })
                        })
                        .collect();
                }
                "disabled" => return Err(Error::OperationDisabled("publish".into())),
                "nosub" => {
                    return Err(Error::Redis(
                        "the topic has no subscription and its retention requires one".into(),
                    ))
                }
                "size" => {
                    return Err(Error::Config(
                        "the payload is larger than the topic's max message size".into(),
                    ))
                }
                "full" => match deadline {
                    Some(d) if tokio::time::Instant::now() < d => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    _ => return Err(Error::Redis("the topic is full".into())),
                },
                _ => return Err(crate::reply::malformed()),
            }
        }
    }

    /// The message `id` from the log, Redisson's `get`.
    pub async fn get(&self, id: &str) -> Result<Option<Message<V, C>>> {
        Ok(self.get_many(&[id]).await?.into_iter().next().flatten())
    }

    /// The messages `ids` from the log, in order, Redisson's `getAll`.
    #[doc(alias = "getAll")]
    pub async fn get_many(&self, ids: &[&str]) -> Result<Vec<Option<Message<V, C>>>> {
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            out.push(self.log_entry(id).await?.map(|(m, _)| m));
        }
        Ok(out)
    }

    /// The log's messages, oldest first, Redisson's `listAll`.
    #[doc(alias = "listAll")]
    pub async fn list_all(&self) -> Result<Vec<Message<V, C>>> {
        let mut cmd = redis::cmd("ZRANGE");
        cmd.arg(format!("{}:log", self.base())).arg(0).arg(-1);
        let ids: Vec<String> = self.query(cmd).await?;
        let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        Ok(self.get_many(&refs).await?.into_iter().flatten().collect())
    }

    /// A log entry with its priority.
    async fn log_entry(&self, id: &str) -> Result<Option<(Message<V, C>, String)>> {
        let mut cmd = redis::cmd("HGET");
        cmd.arg(format!("{}:payloads", self.base())).arg(id);
        let payload: Option<Bytes> = self.query(cmd).await?;
        let Some(payload) = payload else {
            return Ok(None);
        };
        let mut meta = redis::cmd("HMGET");
        meta.arg(format!("{}:meta", self.base()))
            .arg(format!("{id}:h"))
            .arg(format!("{id}:p"));
        let (headers, priority): (Option<Bytes>, Option<String>) = self.query(meta).await?;
        Ok(Some((
            Message {
                id: id.to_string(),
                payload: self.codec.decode(&payload)?,
                deliveries: 0,
                headers: super::message::unpack_headers(&headers.unwrap_or_default())?,
                codec: self.codec.clone(),
            },
            priority.unwrap_or_else(|| "4".into()),
        )))
    }

    /// Fills the subscription `name` from the log, from `position`.
    async fn fill(&self, name: &str, position: &Position) -> Result<()> {
        let log = format!("{}:log", self.base());
        let ids: Vec<String> = match position {
            Position::Latest => return Ok(()),
            Position::Earliest => {
                let mut cmd = redis::cmd("ZRANGE");
                cmd.arg(&log).arg(0).arg(-1);
                self.query(cmd).await?
            }
            Position::Time { at, inclusive } => {
                let ms = at.duration_since(UNIX_EPOCH).map(millis).unwrap_or(0);
                let mut cmd = redis::cmd("ZRANGEBYSCORE");
                cmd.arg(&log)
                    .arg(if *inclusive {
                        ms.to_string()
                    } else {
                        format!("({ms}")
                    })
                    .arg("+inf");
                self.query(cmd).await?
            }
            Position::Id { id, inclusive } => {
                let mut rank = redis::cmd("ZRANK");
                rank.arg(&log).arg(id);
                let rank: Option<i64> = self.query(rank).await?;
                let Some(rank) = rank else {
                    return Ok(());
                };
                let mut cmd = redis::cmd("ZRANGE");
                cmd.arg(&log)
                    .arg(if *inclusive { rank } else { rank + 1 })
                    .arg(-1);
                self.query(cmd).await?
            }
        };
        let queue = self.queue(name);
        for chunk in ids.chunks(100) {
            let mut args = Vec::new();
            for id in chunk {
                let mut raw = redis::cmd("HGET");
                raw.arg(format!("{}:payloads", self.base())).arg(id);
                let payload: Option<Bytes> = self.query(raw).await?;
                let Some(payload) = payload else { continue };
                let mut meta = redis::cmd("HMGET");
                meta.arg(format!("{}:meta", self.base()))
                    .arg(format!("{id}:h"))
                    .arg(format!("{id}:p"));
                let (headers, priority): (Option<Bytes>, Option<String>) = self.query(meta).await?;
                args.extend([
                    Bytes::from(id.clone()),
                    payload,
                    headers.unwrap_or_default(),
                    Bytes::from(priority.unwrap_or_else(|| "4".into())),
                ]);
            }
            if !args.is_empty() {
                let _: i64 = queue.run(&scripts::RECEIVE, args, false).await?;
            }
        }
        Ok(())
    }
}

/// A pending [`ReliablePubSubTopic::create_subscription`].
#[must_use = "a subscription is not created until it is awaited"]
pub struct CreateSubscription<'a, V, C: Codec> {
    topic: &'a ReliablePubSubTopic<V, C>,
    name: Option<String>,
    dead_letter: Option<String>,
    delivery_limit: Option<u32>,
    visibility: Option<Duration>,
    position: Position,
}

impl<V, C: Codec> CreateSubscription<'_, V, C> {
    /// The name; without it one is generated.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// The [`ReliableQueue`] that receives the messages over the delivery limit and the rejected ones, Redisson's `deadLetterTopicName`.
    #[doc(alias = "deadLetterTopicName")]
    pub fn dead_letter_queue(mut self, name: impl Into<String>) -> Self {
        self.dead_letter = Some(name.into());
        self
    }

    /// The delivery limit, instead of the topic's.
    #[doc(alias = "deliveryLimit")]
    pub fn delivery_limit(mut self, limit: u32) -> Self {
        self.delivery_limit = Some(limit);
        self
    }

    /// The visibility, instead of the topic's.
    pub fn visibility(mut self, visibility: Duration) -> Self {
        self.visibility = Some(visibility);
        self
    }

    /// Where it starts; [`Position::Latest`] by default.
    pub fn position(mut self, position: Position) -> Self {
        self.position = position;
        self
    }
}

impl<'a, V, C> IntoFuture for CreateSubscription<'a, V, C>
where
    V: DeserializeOwned + Send + Sync + 'a,
    C: Codec,
{
    type Output = Result<Subscription<V, C>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let topic = self.topic;
            let name = self
                .name
                .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());
            let mut cmd = redis::cmd("HSETNX");
            cmd.arg(format!("{}:subs", topic.base())).arg(&name).arg(1);
            let created: i64 = topic.query(cmd).await?;
            if created == 0 {
                return Err(Error::Config(format!("subscription {name} already exists")));
            }
            let visibility = match self.visibility {
                Some(v) => v,
                None => topic
                    .hget(":config", "visibility")
                    .await?
                    .and_then(|v| v.parse().ok())
                    .map_or(Duration::from_secs(30), Duration::from_millis),
            };
            let limit = match self.delivery_limit {
                Some(l) => l,
                None => topic
                    .hget(":config", "limit")
                    .await?
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(10),
            };
            let mut config = QueueConfig::new()
                .visibility(visibility)
                .delivery_limit(limit);
            if let Some(dlq) = &self.dead_letter {
                config = config.dead_letter_queue(dlq.clone());
            }
            let queue = topic.queue(&name);
            queue.set_config(&config).await?;
            let mut disabled = redis::cmd("SISMEMBER");
            disabled
                .arg(format!("{}:disabled", topic.base()))
                .arg("pull");
            let pull_off: i64 = topic.query(disabled).await?;
            if pull_off == 1 {
                queue.disable(super::message::QueueOperation::Poll).await?;
            }
            topic.fill(&name, &self.position).await?;
            Ok(Subscription {
                topic: topic.clone(),
                name,
                queue,
            })
        })
    }
}

/// A subscription of a [`ReliablePubSubTopic`]: it gets every message published after it was created (or from its [`Position`]), Redisson's `Subscription`.
pub struct Subscription<V, C: Codec> {
    topic: ReliablePubSubTopic<V, C>,
    name: String,
    queue: ReliableQueue<V, C>,
}

impl<V, C: Codec> fmt::Debug for Subscription<V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscription")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl<V, C: Codec> Subscription<V, C> {
    /// Its name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The queue behind it, for the queue's inspection methods.
    pub fn queue(&self) -> &ReliableQueue<V, C> {
        &self.queue
    }

    fn consumers_key(&self) -> String {
        format!("{}:sub:{}:consumers", self.topic.base(), self.name)
    }

    async fn register(&self, consumer: &str) -> Result<()> {
        let mut cmd = redis::cmd("HSET");
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(millis)
            .unwrap_or(0);
        cmd.arg(self.consumers_key()).arg(consumer).arg(now);
        let _: i64 = self.topic.query(cmd).await?;
        Ok(())
    }

    /// A consumer that pulls, Redisson's `createPullConsumer`; the name is generated unless given with [`CreateConsumer::name`].
    #[doc(alias = "createPullConsumer")]
    pub fn pull_consumer(&self) -> CreateConsumer<'_, V, C> {
        CreateConsumer {
            subscription: self,
            name: None,
        }
    }

    /// The consumer names, sorted, Redisson's `getConsumerNames`.
    #[doc(alias = "getConsumerNames")]
    pub async fn consumer_names(&self) -> Result<Vec<String>> {
        let mut cmd = redis::cmd("HKEYS");
        cmd.arg(self.consumers_key());
        let mut names: Vec<String> = self.topic.query(cmd).await?;
        names.sort();
        Ok(names)
    }

    /// Whether the consumer `name` exists, Redisson's `hasConsumer`.
    #[doc(alias = "hasConsumer")]
    pub async fn has_consumer(&self, name: &str) -> Result<bool> {
        let mut cmd = redis::cmd("HEXISTS");
        cmd.arg(self.consumers_key()).arg(name);
        let exists: i64 = self.topic.query(cmd).await?;
        Ok(exists == 1)
    }

    /// Removes the consumer `name`, Redisson's `removeConsumer`.
    #[doc(alias = "removeConsumer")]
    pub async fn remove_consumer(&self, name: &str) -> Result<bool> {
        let mut cmd = redis::cmd("HDEL");
        cmd.arg(self.consumers_key()).arg(name);
        let removed: i64 = self.topic.query(cmd).await?;
        Ok(removed == 1)
    }

    /// The subscription's counters, Redisson's `getStatistics`.
    #[doc(alias = "getStatistics")]
    pub async fn statistics(&self) -> Result<SubscriptionStatistics> {
        Ok(SubscriptionStatistics {
            name: self.name.clone(),
            consumers: self.consumer_names().await?.len() as u64,
            ready: self.queue.len().await? as u64,
            delayed: self.queue.delayed_len().await? as u64,
            unacked: self.queue.unacked_len().await? as u64,
        })
    }
}

impl<V, C> Subscription<V, C>
where
    V: DeserializeOwned,
    C: Codec,
{
    /// Drops the subscription's messages and refills it from the topic's log, from `position`, Redisson's `seek`.
    pub async fn seek(&self, position: Position) -> Result<()> {
        self.queue.clear().await?;
        self.topic.fill(&self.name, &position).await
    }
}

/// A pending [`Subscription::pull_consumer`].
#[must_use = "a consumer is not created until it is awaited"]
pub struct CreateConsumer<'a, V, C: Codec> {
    subscription: &'a Subscription<V, C>,
    name: Option<String>,
}

impl<V, C: Codec> CreateConsumer<'_, V, C> {
    /// The consumer's name.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}

impl<'a, V, C> IntoFuture for CreateConsumer<'a, V, C>
where
    V: Send + Sync + 'a,
    C: Codec,
{
    type Output = Result<PullConsumer<'a, V, C>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let name = self
                .name
                .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());
            self.subscription.register(&name).await?;
            Ok(PullConsumer {
                subscription: self.subscription,
                name,
            })
        })
    }
}

/// A consumer of a [`Subscription`] that pulls messages, Redisson's `PullConsumer`.
pub struct PullConsumer<'a, V, C: Codec> {
    subscription: &'a Subscription<V, C>,
    name: String,
}

impl<V, C: Codec> fmt::Debug for PullConsumer<'_, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PullConsumer")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl<'a, V, C> PullConsumer<'a, V, C>
where
    V: DeserializeOwned + Send + Sync + 'a,
    C: Codec,
{
    /// Its name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Takes the next message of the subscription, Redisson's `pull`; the options of [`ReliableQueue::poll`].
    pub fn pull(&self) -> super::queue::Poll<'a, V, C> {
        self.subscription.queue.poll()
    }

    /// Takes up to `count` messages, Redisson's `pullMany`.
    #[doc(alias = "pullMany")]
    pub fn pull_many(&self, count: usize) -> super::queue::PollMany<'a, V, C> {
        self.subscription.queue.poll_many(count)
    }

    /// The messages as a stream, a loop of pulls waiting up to `wait` each: the push consumer of Redisson (`createPushConsumer` with a listener).
    #[doc(alias = "createPushConsumer")]
    pub fn into_stream(
        self,
        wait: Duration,
    ) -> impl Stream<Item = Result<Delivery<'a, V, C>>> + 'a {
        let queue = &self.subscription.queue;
        stream::unfold(
            Vec::<Delivery<'a, V, C>>::new(),
            move |mut buffered| async move {
                loop {
                    if let Some(next) = buffered.pop() {
                        return Some((Ok(next), buffered));
                    }
                    match queue.poll_wait(1, None, Some(wait), AckMode::Manual).await {
                        Ok(found) => buffered = found,
                        Err(e) => return Some((Err(e), buffered)),
                    }
                }
            },
        )
    }
}

/// A pending [`ReliablePubSubTopic::publish`]. Await it, optionally after the message options and [`Publish::timeout`].
#[must_use = "a publish does nothing until it is awaited"]
pub struct Publish<'a, V, C: Codec> {
    topic: &'a ReliablePubSubTopic<V, C>,
    messages: Vec<MessageArgs<Result<Bytes>>>,
    timeout: Option<Duration>,
}

impl<V, C: Codec> Publish<'_, V, C> {
    fn first(
        mut self,
        f: impl FnOnce(MessageArgs<Result<Bytes>>) -> MessageArgs<Result<Bytes>>,
    ) -> Self {
        if let Some(m) = self.messages.pop() {
            self.messages.push(f(m));
        }
        self
    }

    /// See [`MessageArgs::priority`].
    pub fn priority(self, priority: u8) -> Self {
        self.first(|m| m.priority(priority))
    }

    /// See [`MessageArgs::delay`].
    pub fn delay(self, delay: Duration) -> Self {
        self.first(|m| m.delay(delay))
    }

    /// See [`MessageArgs::ttl`].
    pub fn ttl(self, ttl: Duration) -> Self {
        self.first(|m| m.ttl(ttl))
    }

    /// See [`MessageArgs::delivery_limit`].
    pub fn delivery_limit(self, limit: u32) -> Self {
        self.first(|m| m.delivery_limit(limit))
    }

    /// See [`MessageArgs::header`].
    pub fn header<T: Serialize + ?Sized>(self, name: impl Into<String>, value: &T) -> Self {
        let name = name.into();
        let json = serde_json::to_value(value).map_err(|e| Error::Codec(e.to_string()));
        self.first(move |mut m| {
            m.headers.push((name, json));
            m
        })
    }

    /// Waits up to `timeout` for room when the topic is full.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

impl<'a, V, C> IntoFuture for Publish<'a, V, C>
where
    V: DeserializeOwned + Send + Sync + 'a,
    C: Codec,
{
    type Output = Result<Message<V, C>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            self.topic
                .publish_encoded(self.messages, self.timeout)
                .await?
                .into_iter()
                .next()
                .ok_or_else(crate::reply::malformed)
        })
    }
}

/// A pending [`ReliablePubSubTopic::publish_many`].
#[must_use = "a publish does nothing until it is awaited"]
pub struct PublishMany<'a, V, C: Codec>(Publish<'a, V, C>);

impl<V, C: Codec> PublishMany<'_, V, C> {
    /// Waits up to `timeout` for room when the topic is full.
    pub fn timeout(self, timeout: Duration) -> Self {
        Self(self.0.timeout(timeout))
    }
}

impl<'a, V, C> IntoFuture for PublishMany<'a, V, C>
where
    V: DeserializeOwned + Send + Sync + 'a,
    C: Codec,
{
    type Output = Result<Vec<Message<V, C>>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        let p = self.0;
        Box::pin(async move { p.topic.publish_encoded(p.messages, p.timeout).await })
    }
}

/// The changes of a [`ReliablePubSubTopic`] and its subscriptions. Drop it to stop.
pub struct TopicEvents {
    _topic_sub: PubSubscription,
    _subs_sub: PubSubscription<PatternMessage>,
    topic: broadcast::Receiver<Bytes>,
    subs: broadcast::Receiver<PatternMessage>,
    prefix: String,
}

impl fmt::Debug for TopicEvents {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TopicEvents").finish_non_exhaustive()
    }
}

fn received<T>(outcome: std::result::Result<T, RecvError>) -> Result<T> {
    match outcome {
        Ok(v) => Ok(v),
        Err(RecvError::Lagged(n)) => Err(Error::Lagged(n as usize)),
        Err(RecvError::Closed) => Err(Error::Redis("the subscription was closed".into())),
    }
}

impl TopicEvents {
    fn topic_event(payload: &[u8]) -> Option<TopicEvent> {
        let text = std::str::from_utf8(payload).ok()?;
        let mut parts = text.splitn(3, '\n');
        let kind = parts.next()?;
        let arg = parts.next().unwrap_or("");
        let ids: Vec<String> = parts
            .next()
            .unwrap_or("")
            .split(' ')
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        let op = |a: &str| match a {
            "publish" => Some(PubSubOperation::Publish),
            "pull" => Some(PubSubOperation::Pull),
            _ => None,
        };
        Some(match kind {
            "published" => TopicEvent::Published(ids),
            "full" => TopicEvent::Full,
            "config" => TopicEvent::Config,
            "disabled" => TopicEvent::Disabled(op(arg)?),
            "enabled" => TopicEvent::Enabled(op(arg)?),
            _ => return None,
        })
    }

    fn sub_event(prefix: &str, (channel, payload): PatternMessage) -> Option<TopicEvent> {
        let subscription = channel.strip_prefix(prefix)?.to_string();
        match super::message::QueueEvent::parse(&payload)? {
            super::message::QueueEvent::Polled(ids) => {
                Some(TopicEvent::Pulled { subscription, ids })
            }
            super::message::QueueEvent::Acknowledged(ids) => {
                Some(TopicEvent::Acknowledged { subscription, ids })
            }
            super::message::QueueEvent::NegativelyAcknowledged { ids, rejected } => {
                Some(TopicEvent::NegativelyAcknowledged {
                    subscription,
                    ids,
                    rejected,
                })
            }
            _ => None,
        }
    }

    /// Waits for the next event.
    pub async fn recv(&mut self) -> Result<TopicEvent> {
        loop {
            let topic = Box::pin(self.topic.recv());
            let subs = Box::pin(self.subs.recv());
            match select(topic, subs).await {
                futures::future::Either::Left((outcome, _)) => {
                    if let Some(e) = Self::topic_event(&received(outcome)?) {
                        return Ok(e);
                    }
                }
                futures::future::Either::Right((outcome, _)) => {
                    let message = received(outcome)?;
                    if let Some(e) = Self::sub_event(&self.prefix, message) {
                        return Ok(e);
                    }
                }
            }
        }
    }

    /// Turns the listener into a stream of events. It never ends.
    pub fn into_stream(self) -> impl Stream<Item = Result<TopicEvent>> {
        stream::unfold(self, |mut events| async move {
            Some((events.recv().await, events))
        })
    }
}
