use super::*;
use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::reply::{array, bytes, int, malformed, number, text};
use bytes::Bytes;
use fred::types::Value;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::time::Duration;

/// A pending [`Stream::add`]. Await it, optionally after setting an explicit [`id`](StreamAdd::id) or a trim: [`max_len`](StreamAdd::max_len) or [`min_id`](StreamAdd::min_id), with [`non_strict`](StreamAdd::non_strict) and [`limit`](StreamAdd::limit).
#[must_use = "an add does nothing until it is awaited"]
pub struct StreamAdd<'a, K, V, C: Codec> {
    pub(super) stream: &'a Stream<K, V, C>,
    pub(super) fields: Result<Vec<Bytes>>,
    pub(super) id: Option<StreamId>,
    pub(super) trim: Option<Trim>,
    pub(super) strict: bool,
    pub(super) limit: Option<u64>,
}

impl<'a, K, V, C: Codec> StreamAdd<'a, K, V, C> {
    /// Adds the entry under this id instead of a generated one. It must be greater than every id in the stream; [`StreamId::auto_sequence`] lets Redis pick the sequence number.
    pub fn id(mut self, id: StreamId) -> Self {
        self.id = Some(id);
        self
    }

    /// Trims the stream to `max_len` entries after the add, dropping the oldest (`MAXLEN`).
    pub fn max_len(mut self, max_len: u64) -> Self {
        self.trim = Some(Trim::MaxLen(max_len));
        self
    }

    /// Drops the entries with an id below `min_id` after the add (`MINID`).
    pub fn min_id(mut self, min_id: StreamId) -> Self {
        self.trim = Some(Trim::MinId(min_id));
        self
    }

    /// Trims only whole internal nodes (`~`): faster, but may keep a few more entries than asked.
    pub fn non_strict(mut self) -> Self {
        self.strict = false;
        self
    }

    /// Drops at most `limit` entries per trim (`LIMIT`). Needs [`non_strict`](StreamAdd::non_strict).
    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }
}

impl<'a, K, V, C> IntoFuture for StreamAdd<'a, K, V, C>
where
    K: Send + Sync + 'a,
    V: Send + Sync + 'a,
    C: Codec,
{
    type Output = Result<StreamId>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let fields = self.fields?;
            if fields.is_empty() {
                return Err(Error::Config(
                    "a stream entry needs at least one field".into(),
                ));
            }
            let mut args = vec![Value::from(self.stream.key.redis_key())];
            if let Some(trim) = &self.trim {
                args.extend(trim_args(trim, self.strict, self.limit));
            }
            args.push(Value::from(
                self.id.map_or_else(|| "*".to_string(), |id| id.0),
            ));
            args.extend(fields.into_iter().map(Value::Bytes));
            let reply = self.stream.run("XADD", args, 0, Mode::Once).await?;
            id_of(&reply)
        })
    }
}

/// A pending [`Stream::trim`] or [`Stream::trim_min_id`]. Await it, optionally after [`non_strict`](StreamTrim::non_strict) or [`limit`](StreamTrim::limit).
#[must_use = "a trim does nothing until it is awaited"]
pub struct StreamTrim<'a, K, V, C: Codec> {
    pub(super) stream: &'a Stream<K, V, C>,
    pub(super) trim: Trim,
    pub(super) strict: bool,
    pub(super) limit: Option<u64>,
}

impl<K, V, C: Codec> StreamTrim<'_, K, V, C> {
    /// Trims only whole internal nodes (`~`): faster, but may keep a few more entries than asked.
    pub fn non_strict(mut self) -> Self {
        self.strict = false;
        self
    }

    /// Drops at most `limit` entries (`LIMIT`). Needs [`non_strict`](StreamTrim::non_strict).
    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }
}

impl<'a, K, V, C> IntoFuture for StreamTrim<'a, K, V, C>
where
    K: Send + Sync + 'a,
    V: Send + Sync + 'a,
    C: Codec,
{
    type Output = Result<usize>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let mut args = vec![Value::from(self.stream.key.redis_key())];
            args.extend(trim_args(&self.trim, self.strict, self.limit));
            let reply = self.stream.run("XTRIM", args, 0, Mode::Retry).await?;
            Ok(number(&reply).unwrap_or(0) as usize)
        })
    }
}

/// A pending [`Stream::read_group`]. Await it, optionally after [`after`](ReadGroup::after) or [`no_ack`](ReadGroup::no_ack).
#[must_use = "a read does nothing until it is awaited"]
pub struct ReadGroup<'a, K, V, C: Codec> {
    pub(super) stream: &'a Stream<K, V, C>,
    pub(super) group: &'a str,
    pub(super) consumer: &'a str,
    pub(super) count: Option<usize>,
    pub(super) after: Option<StreamId>,
    pub(super) no_ack: bool,
}

impl<K, V, C: Codec> ReadGroup<'_, K, V, C> {
    /// Re-reads the entries already delivered to this consumer and still pending, with an id after `after`, instead of new ones.
    pub fn after(mut self, after: StreamId) -> Self {
        self.after = Some(after);
        self
    }

    /// Does not keep the entries pending: they count as acknowledged on delivery (`NOACK`).
    pub fn no_ack(mut self) -> Self {
        self.no_ack = true;
        self
    }
}

impl<'a, K, V, C> IntoFuture for ReadGroup<'a, K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync + 'a,
    V: Serialize + DeserializeOwned + Send + Sync + 'a,
    C: Codec,
{
    type Output = Result<Vec<StreamEntry<K, V>>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let (args, offset) = self.stream.group_args(
                self.group,
                self.consumer,
                self.count,
                None,
                self.no_ack,
                self.after.as_ref(),
            );
            let reply = self
                .stream
                .run("XREADGROUP", args, offset, Mode::Retry)
                .await?;
            self.stream.read_reply(&reply)
        })
    }
}

/// A pending [`Stream::pending_entries`]. Await it for the [`PendingEntry`] list, or call [`messages`](PendingRange::messages) for the entries themselves.
#[must_use = "a query does nothing until it is awaited"]
pub struct PendingRange<'a, K, V, C: Codec> {
    pub(super) stream: &'a Stream<K, V, C>,
    pub(super) group: &'a str,
    pub(super) count: usize,
    pub(super) from: StreamId,
    pub(super) to: StreamId,
    pub(super) consumer: Option<&'a str>,
    pub(super) min_idle: Option<Duration>,
}

impl<'a, K, V, C> PendingRange<'a, K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    /// Only ids from `from` to `to`, both included. The default is every id.
    pub fn range(mut self, from: &StreamId, to: &StreamId) -> Self {
        self.from = from.clone();
        self.to = to.clone();
        self
    }

    /// Only the entries held by `consumer`.
    pub fn consumer(mut self, consumer: &'a str) -> Self {
        self.consumer = Some(consumer);
        self
    }

    /// Only the entries idle for at least `min_idle` (`IDLE`).
    pub fn min_idle(mut self, min_idle: Duration) -> Self {
        self.min_idle = Some(min_idle);
        self
    }

    fn args(&self) -> Vec<Value> {
        let mut args = vec![Value::from(self.group.to_string())];
        if let Some(min_idle) = self.min_idle {
            args.extend([Value::from("IDLE"), int(min_idle.as_millis())]);
        }
        args.extend([
            Value::from(self.from.0.clone()),
            Value::from(self.to.0.clone()),
            int(self.count),
        ]);
        if let Some(consumer) = self.consumer {
            args.push(Value::from(consumer.to_string()));
        }
        args
    }

    /// Returns the pending entries with their fields, read in one atomic step. Entries deleted from the stream are left out.
    pub async fn messages(self) -> Result<Vec<StreamEntry<K, V>>> {
        let args = self
            .args()
            .iter()
            .map(|value| bytes(value).ok_or_else(malformed))
            .collect::<Result<Vec<Bytes>>>()?;
        let reply: Value = self
            .stream
            .key
            .core
            .eval(&PENDING_RANGE, vec![self.stream.key.redis_key()], args)
            .await?;
        self.stream.entries(&reply)
    }
}

impl<'a, K, V, C> IntoFuture for PendingRange<'a, K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync + 'a,
    V: Serialize + DeserializeOwned + Send + Sync + 'a,
    C: Codec,
{
    type Output = Result<Vec<PendingEntry>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let mut args = vec![Value::from(self.stream.key.redis_key())];
            args.extend(self.args());
            let reply = self.stream.run("XPENDING", args, 0, Mode::Retry).await?;
            array(&reply)
                .iter()
                .map(|raw| match array(raw) {
                    [id, consumer, idle, deliveries] => Ok(PendingEntry {
                        id: id_of(id)?,
                        consumer: text(consumer).ok_or_else(malformed)?,
                        idle: Duration::from_millis(number(idle).unwrap_or(0) as u64),
                        deliveries: number(deliveries).unwrap_or(0) as u64,
                    }),
                    _ => Err(malformed()),
                })
                .collect()
        })
    }
}
