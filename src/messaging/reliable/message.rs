//! The types [`ReliableQueue`](crate::ReliableQueue) and [`ReliablePubSubTopic`](crate::ReliablePubSubTopic) share: messages, their options, acknowledgements and events.

use crate::codec::Codec;
use crate::error::{Error, Result};
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt;
use std::time::Duration;

/// A message of a reliable queue or topic, Redisson's `Message`.
#[derive(Clone)]
pub struct Message<V, C: Codec> {
    /// The id, `<server ms>-<sequence>`, unique per queue.
    pub id: String,
    /// The payload.
    pub payload: V,
    /// How many times it was delivered, this delivery included.
    pub deliveries: u32,
    pub(crate) headers: Vec<(String, Bytes)>,
    pub(crate) codec: C,
}

impl<V: fmt::Debug, C: Codec> fmt::Debug for Message<V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Message")
            .field("id", &self.id)
            .field("payload", &self.payload)
            .field("deliveries", &self.deliveries)
            .field(
                "headers",
                &self.headers.iter().map(|(n, _)| n).collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl<V: PartialEq, C: Codec> PartialEq for Message<V, C> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.payload == other.payload && self.headers == other.headers
    }
}

impl<V, C: Codec> Message<V, C> {
    /// The header `name` decoded with the queue's codec, `None` when the message has none.
    pub fn header<T: DeserializeOwned>(&self, name: &str) -> Result<Option<T>> {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| self.codec.decode(v))
            .transpose()
    }

    /// The header names, in the order they were set.
    pub fn header_names(&self) -> impl Iterator<Item = &str> {
        self.headers.iter().map(|(n, _)| n.as_str())
    }
}

/// How a poll acknowledges, Redisson's `AcknowledgeMode`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum AckMode {
    /// The message stays unacknowledged until [`Delivery::ack`](crate::reliable_queue::Delivery::ack) or `nack`, and comes back after its visibility. Redisson's default.
    #[default]
    Manual,
    /// The message is removed as it is polled.
    Auto,
}

/// A negative acknowledgement, Redisson's `QueueNegativeAckArgs`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Nack {
    /// The processing failed: the message is delivered again after `delay`, until its delivery limit; then it goes to the dead-letter queue.
    Failed {
        /// When it becomes ready again.
        delay: Duration,
    },
    /// The message is rejected: it goes to the dead-letter queue (or is deleted without one) at once.
    Rejected,
}

/// An operation that [`disable`](crate::ReliableQueue::disable) can turn off, Redisson's `QueueOperation`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum QueueOperation {
    /// Adding messages.
    Add,
    /// Polling messages.
    Poll,
}

impl QueueOperation {
    pub(crate) fn name(self) -> &'static str {
        match self {
            QueueOperation::Add => "add",
            QueueOperation::Poll => "poll",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        match text {
            "add" => Some(QueueOperation::Add),
            "poll" => Some(QueueOperation::Poll),
            _ => None,
        }
    }
}

/// Whether messages are handed out one at a time, Redisson's `ProcessingMode`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ProcessingMode {
    /// Any number of messages can be unacknowledged at once. The default.
    #[default]
    Parallel,
    /// A poll returns nothing while a message is unacknowledged.
    Sequential,
}

/// How a message is deduplicated, Redisson's `deduplicationByHash` and `deduplicationById`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Dedup {
    /// A message with the same payload bytes added within the window is dropped.
    ByHash(Duration),
    /// A message with the same id added within the window is dropped.
    ById(String, Duration),
}

/// A change of a reliable queue, from [`events`](crate::ReliableQueue::events), Redisson's `QueueEventListener`s.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum QueueEvent {
    /// Messages were added, Redisson's `AddedEventListener`.
    Added(Vec<String>),
    /// Messages were polled, Redisson's `PolledEventListener`.
    Polled(Vec<String>),
    /// Messages were acknowledged, Redisson's `AcknowledgedEventListener`.
    Acknowledged(Vec<String>),
    /// Messages were negatively acknowledged, Redisson's `NegativelyAcknowledgedEventListener`; `rejected` is `false` for [`Nack::Failed`].
    NegativelyAcknowledged {
        /// The ids.
        ids: Vec<String>,
        /// Whether they were rejected rather than failed.
        rejected: bool,
    },
    /// Messages were removed, Redisson's `RemovedEventListener`.
    Removed(Vec<String>),
    /// An add found the queue full, Redisson's `FullEventListener`.
    Full,
    /// The configuration changed, Redisson's `ConfigEventListener`.
    Config,
    /// An operation was disabled, Redisson's `DisabledOperationEventListener`.
    Disabled(QueueOperation),
    /// An operation was enabled, Redisson's `EnabledOperationEventListener`.
    Enabled(QueueOperation),
}

impl QueueEvent {
    /// Parses `kind\narg\nid id…`; `None` for an unknown kind.
    pub(crate) fn parse(payload: &[u8]) -> Option<Self> {
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
        Some(match kind {
            "added" => QueueEvent::Added(ids),
            "polled" => QueueEvent::Polled(ids),
            "acknowledged" => QueueEvent::Acknowledged(ids),
            "nacked" => QueueEvent::NegativelyAcknowledged {
                ids,
                rejected: arg == "rejected",
            },
            "removed" => QueueEvent::Removed(ids),
            "full" => QueueEvent::Full,
            "config" => QueueEvent::Config,
            "disabled" => QueueEvent::Disabled(QueueOperation::parse(arg)?),
            "enabled" => QueueEvent::Enabled(QueueOperation::parse(arg)?),
            _ => return None,
        })
    }
}

/// One message to add with its options, Redisson's `MessageArgs`.
#[must_use]
pub struct MessageArgs<V> {
    pub(crate) payload: V,
    pub(crate) priority: u8,
    pub(crate) delay: Option<Duration>,
    pub(crate) ttl: Option<Duration>,
    pub(crate) delivery_limit: u32,
    pub(crate) dedup: Option<Dedup>,
    /// Header values kept as JSON values until the queue's codec encodes them.
    pub(crate) headers: Vec<(String, Result<serde_json::Value>)>,
}

impl<V> MessageArgs<V> {
    /// A message with `payload` and the defaults: priority 4, the queue's delay, TTL and delivery limit, no deduplication, no headers.
    pub fn new(payload: V) -> Self {
        Self {
            payload,
            priority: 4,
            delay: None,
            ttl: None,
            delivery_limit: 0,
            dedup: None,
            headers: Vec::new(),
        }
    }

    /// The priority, 0 (lowest) to 9 (highest); higher is delivered first, FIFO within one. Values above 9 count as 9.
    pub fn priority(mut self, priority: u8) -> Self {
        self.priority = priority.min(9);
        self
    }

    /// Makes the message ready only after `delay`, instead of the queue's delay.
    pub fn delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    /// Deletes the message after `ttl` if nobody acknowledged it, instead of the queue's TTL.
    #[doc(alias = "timeToLive")]
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// The deliveries after which the message goes to the dead-letter queue, instead of the queue's limit.
    #[doc(alias = "deliveryLimit")]
    pub fn delivery_limit(mut self, limit: u32) -> Self {
        self.delivery_limit = limit;
        self
    }

    /// Drops the message when the same payload was added within `window`, Redisson's `deduplicationByHash`.
    #[doc(alias = "deduplicationByHash")]
    pub fn dedup_by_hash(mut self, window: Duration) -> Self {
        self.dedup = Some(Dedup::ByHash(window));
        self
    }

    /// Drops the message when one with `id` was added within `window`, Redisson's `deduplicationById`.
    #[doc(alias = "deduplicationById")]
    pub fn dedup_by_id(mut self, id: impl Into<String>, window: Duration) -> Self {
        self.dedup = Some(Dedup::ById(id.into(), window));
        self
    }
}

impl<V> MessageArgs<V> {
    /// Adds the header `name`, encoded with the queue's codec.
    pub fn header<T: Serialize + ?Sized>(mut self, name: impl Into<String>, value: &T) -> Self {
        let json = serde_json::to_value(value).map_err(|e| Error::Codec(e.to_string()));
        self.headers.push((name.into(), json));
        self
    }
}

/// `[u32 BE name len][name][u32 BE value len][value]…`, the headers as the scripts store them.
pub(crate) fn pack_headers(headers: &[(String, Bytes)]) -> Bytes {
    let mut out = Vec::new();
    for (name, value) in headers {
        out.extend_from_slice(&(name.len() as u32).to_be_bytes());
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(&(value.len() as u32).to_be_bytes());
        out.extend_from_slice(value);
    }
    Bytes::from(out)
}

pub(crate) fn unpack_headers(mut raw: &[u8]) -> Result<Vec<(String, Bytes)>> {
    let mut headers = Vec::new();
    let take = |raw: &mut &[u8]| -> Result<Vec<u8>> {
        let len: [u8; 4] = raw
            .get(..4)
            .and_then(|b| b.try_into().ok())
            .ok_or_else(crate::reply::malformed)?;
        let len = u32::from_be_bytes(len) as usize;
        let body = raw
            .get(4..4 + len)
            .ok_or_else(crate::reply::malformed)?
            .to_vec();
        *raw = &raw[4 + len..];
        Ok(body)
    };
    while !raw.is_empty() {
        let name = String::from_utf8(take(&mut raw)?).map_err(|_| crate::reply::malformed())?;
        let value = Bytes::from(take(&mut raw)?);
        headers.push((name, value));
    }
    Ok(headers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn headers_round_trip() {
        let headers = vec![
            ("a".to_string(), Bytes::from_static(b"1")),
            ("bb".to_string(), Bytes::from_static(b"")),
        ];
        assert_eq!(unpack_headers(&pack_headers(&headers)).unwrap(), headers);
        assert!(unpack_headers(&[0, 0, 0, 9, 1]).is_err());
    }

    #[test]
    fn events_parse() {
        assert_eq!(
            QueueEvent::parse(b"added\n\n1-1 1-2"),
            Some(QueueEvent::Added(vec!["1-1".into(), "1-2".into()]))
        );
        assert_eq!(
            QueueEvent::parse(b"nacked\nrejected\n1-1"),
            Some(QueueEvent::NegativelyAcknowledged {
                ids: vec!["1-1".into()],
                rejected: true
            })
        );
        assert_eq!(
            QueueEvent::parse(b"disabled\npoll\n"),
            Some(QueueEvent::Disabled(QueueOperation::Poll))
        );
        assert_eq!(QueueEvent::parse(b"full\n\n"), Some(QueueEvent::Full));
        assert_eq!(QueueEvent::parse(b"what\n\n"), None);
    }
}
