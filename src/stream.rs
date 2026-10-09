use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::object::{millis as to_millis, HasKey, Key};
use crate::pending::Pending;
use bytes::Bytes;
use fred::interfaces::ClientLike;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::str::FromStr;
use std::time::Duration;

/// The id of a stream entry: a millisecond time and a sequence number, written `1700000000000-0`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StreamId(String);

impl StreamId {
    /// The smallest possible id, as an open lower end of a range (`-`).
    pub fn min() -> Self {
        Self("-".into())
    }

    /// The largest possible id, as an open upper end of a range (`+`).
    pub fn max() -> Self {
        Self("+".into())
    }

    /// The id `0-0`, which is before every entry. Reading after it returns everything.
    pub fn zero() -> Self {
        Self("0-0".into())
    }

    /// The id of the newest entry at the time of the call (`$`). Use it to read only entries added later.
    pub fn latest() -> Self {
        Self("$".into())
    }

    /// The id as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for StreamId {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self> {
        let valid = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
        let well_formed = match text.split_once('-') {
            Some((time, sequence)) => valid(time) && (valid(sequence) || sequence == "*"),
            None => valid(text),
        };
        if well_formed || matches!(text, "-" | "+" | "$") {
            Ok(Self(text.to_string()))
        } else {
            Err(Error::Config(format!("{text:?} is not a stream id")))
        }
    }
}

/// One entry of a [`Stream`]: its id and its fields in the order they were added.
#[derive(Clone, Debug, PartialEq)]
pub struct StreamEntry<K, V> {
    /// The id of the entry.
    pub id: StreamId,
    /// The fields of the entry.
    pub fields: Vec<(K, V)>,
}

/// Summary of the entries a group has delivered and not yet acknowledged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingSummary {
    /// Number of pending entries.
    pub count: u64,
    /// The smallest pending id.
    pub first: Option<StreamId>,
    /// The largest pending id.
    pub last: Option<StreamId>,
    /// Each consumer with its number of pending entries.
    pub consumers: Vec<(String, u64)>,
}

/// One entry that was delivered to a consumer and not yet acknowledged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingEntry {
    /// The id of the entry.
    pub id: StreamId,
    /// The consumer that holds it.
    pub consumer: String,
    /// Time since it was last delivered.
    pub idle: Duration,
    /// How many times it was delivered.
    pub deliveries: u64,
}

fn text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.to_string()),
        Value::Bytes(bytes) => String::from_utf8(bytes.to_vec()).ok(),
        Value::Integer(number) => Some(number.to_string()),
        _ => None,
    }
}

fn bytes(value: &Value) -> Option<Bytes> {
    match value {
        Value::Bytes(bytes) => Some(bytes.clone()),
        Value::String(text) => Some(Bytes::copy_from_slice(text.as_bytes())),
        Value::Integer(number) => Some(Bytes::from(number.to_string())),
        _ => None,
    }
}

fn number(value: &Value) -> Option<i64> {
    match value {
        Value::Integer(number) => Some(*number),
        other => text(other)?.parse().ok(),
    }
}

fn array(value: &Value) -> &[Value] {
    match value {
        Value::Array(items) => items,
        _ => &[],
    }
}

fn malformed() -> Error {
    Error::Redis("unexpected reply to a stream command".into())
}

fn id_of(value: &Value) -> Result<StreamId> {
    text(value).map(StreamId).ok_or_else(malformed)
}

fn ids(ids: &[StreamId]) -> Vec<Value> {
    ids.iter().map(|id| Value::from(id.0.clone())).collect()
}

/// A pending [`Stream::add`]. Await it, optionally after setting [`StreamAdd::max_len`].
#[must_use = "an add does nothing until it is awaited"]
pub struct StreamAdd<'a, K, V, C: Codec> {
    stream: &'a Stream<K, V, C>,
    fields: Result<Vec<Bytes>>,
    max_len: Option<u64>,
}

impl<'a, K, V, C: Codec> StreamAdd<'a, K, V, C> {
    /// Trims the stream to exactly `max_len` entries after the add, dropping the oldest.
    pub fn max_len(mut self, max_len: u64) -> Self {
        self.max_len = Some(max_len);
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
            if let Some(max_len) = self.max_len {
                args.extend([Value::from("MAXLEN"), Value::Integer(max_len as i64)]);
            }
            args.push(Value::from("*"));
            args.extend(fields.into_iter().map(Value::Bytes));
            let reply = self.stream.run("XADD", args, 0, false).await?;
            id_of(&reply)
        })
    }
}

/// A distributed append-only log of entries stored in a Redis Stream, with consumer groups, as in Redisson's `RStream`. Each entry has an id and fields.
///
/// A group delivers every entry to one consumer, keeps it pending until it is acknowledged, and lets other consumers claim entries that a crashed consumer left. Needs Redis 6.2 or newer for [`auto_claim`](Stream::auto_claim).
pub struct Stream<K, V, C: Codec> {
    key: Key,
    codec: C,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V, C: Codec> Clone for Stream<K, V, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
            _marker: PhantomData,
        }
    }
}

impl<K, V, C: Codec> fmt::Debug for Stream<K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "Stream")
    }
}

impl<K, V, C: Codec> HasKey for Stream<K, V, C> {
    fn key(&self) -> &Key {
        &self.key
    }
}

impl<K, V, C: Codec> Stream<K, V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self {
            key,
            codec,
            _marker: PhantomData,
        }
    }

    async fn run(
        &self,
        name: &'static str,
        args: Vec<Value>,
        key_offset: usize,
        blocking: bool,
    ) -> Result<Value> {
        let command = CustomCommand::new_static(name, ClusterHash::Offset(key_offset), false);
        if blocking {
            let connection = self.key.core.blocking_client().await?;
            Ok(connection.custom(command, args).await?)
        } else {
            Ok(self.key.core.redis().custom(command, args).await?)
        }
    }
}

impl<K, V, C> Stream<K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    fn entries(&self, value: &Value) -> Result<Vec<StreamEntry<K, V>>> {
        let mut entries = Vec::new();
        for raw in array(value) {
            let [id, fields] = array(raw) else {
                continue;
            };
            if matches!(fields, Value::Null) {
                continue;
            }
            let parts = array(fields);
            let mut decoded = Vec::with_capacity(parts.len() / 2);
            for pair in parts.as_chunks::<2>().0 {
                let key = bytes(&pair[0]).ok_or_else(malformed)?;
                let value = bytes(&pair[1]).ok_or_else(malformed)?;
                decoded.push((self.codec.decode(&key)?, self.codec.decode(&value)?));
            }
            entries.push(StreamEntry {
                id: id_of(id)?,
                fields: decoded,
            });
        }
        Ok(entries)
    }

    fn read_reply(&self, reply: &Value) -> Result<Vec<StreamEntry<K, V>>> {
        match array(reply).first() {
            Some(stream) => match array(stream) {
                [_, entries] => self.entries(entries),
                _ => Err(malformed()),
            },
            None => Ok(Vec::new()),
        }
    }

    /// Appends an entry made of these fields and returns its id. The fields are borrowed: `log.add([("temp", "21")])`. An entry needs at least one field. Add `.max_len(n)` to keep the stream short.
    pub fn add<'a, Q, W>(
        &'a self,
        fields: impl IntoIterator<Item = (&'a Q, &'a W)>,
    ) -> StreamAdd<'a, K, V, C>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync + 'a,
        W: Serialize + ?Sized + Sync + 'a,
    {
        let encoded = fields
            .into_iter()
            .try_fold(Vec::new(), |mut all, (key, value)| {
                all.push(self.codec.encode(key)?);
                all.push(self.codec.encode(value)?);
                Ok(all)
            });
        StreamAdd {
            stream: self,
            fields: encoded,
            max_len: None,
        }
    }

    /// Number of entries.
    pub async fn len(&self) -> Result<usize> {
        let reply = self
            .run("XLEN", vec![Value::from(self.key.redis_key())], 0, false)
            .await?;
        Ok(number(&reply).unwrap_or(0) as usize)
    }

    /// Returns whether the stream has no entries.
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    async fn ranged(
        &self,
        name: &'static str,
        first: &StreamId,
        second: &StreamId,
        count: Option<usize>,
    ) -> Result<Vec<StreamEntry<K, V>>> {
        let mut args = vec![
            Value::from(self.key.redis_key()),
            Value::from(first.0.clone()),
            Value::from(second.0.clone()),
        ];
        if let Some(count) = count {
            args.extend([Value::from("COUNT"), Value::Integer(count as i64)]);
        }
        let reply = self.run(name, args, 0, false).await?;
        self.entries(&reply)
    }

    /// Returns the entries from `from` to `to`, both included, oldest first. Use [`StreamId::min`] and [`StreamId::max`] for open ends.
    pub async fn range(
        &self,
        from: &StreamId,
        to: &StreamId,
        count: Option<usize>,
    ) -> Result<Vec<StreamEntry<K, V>>> {
        self.ranged("XRANGE", from, to, count).await
    }

    /// Returns the entries from `from` down to `to`, newest first. `from` is the larger id.
    pub async fn rev_range(
        &self,
        from: &StreamId,
        to: &StreamId,
        count: Option<usize>,
    ) -> Result<Vec<StreamEntry<K, V>>> {
        self.ranged("XREVRANGE", from, to, count).await
    }

    /// Deletes these entries; returns how many existed.
    pub async fn remove(&self, entries: &[StreamId]) -> Result<usize> {
        if entries.is_empty() {
            return Ok(0);
        }
        let mut args = vec![Value::from(self.key.redis_key())];
        args.extend(ids(entries));
        let reply = self.run("XDEL", args, 0, false).await?;
        Ok(number(&reply).unwrap_or(0) as usize)
    }

    /// Cuts the stream down to exactly `max_len` entries, dropping the oldest; returns how many were dropped.
    pub async fn trim(&self, max_len: u64) -> Result<usize> {
        let args = vec![
            Value::from(self.key.redis_key()),
            Value::from("MAXLEN"),
            Value::Integer(max_len as i64),
        ];
        let reply = self.run("XTRIM", args, 0, false).await?;
        Ok(number(&reply).unwrap_or(0) as usize)
    }

    fn read_args(
        &self,
        after: &StreamId,
        count: Option<usize>,
        block: Option<u64>,
    ) -> (Vec<Value>, usize) {
        let mut args = Vec::new();
        if let Some(count) = count {
            args.extend([Value::from("COUNT"), Value::Integer(count as i64)]);
        }
        if let Some(block) = block {
            args.extend([Value::from("BLOCK"), Value::Integer(block as i64)]);
        }
        args.push(Value::from("STREAMS"));
        let offset = args.len();
        args.push(Value::from(self.key.redis_key()));
        args.push(Value::from(after.0.clone()));
        (args, offset)
    }

    /// Returns the entries added after `after`, oldest first, without waiting. [`StreamId::zero`] returns everything.
    pub async fn read(
        &self,
        after: &StreamId,
        count: Option<usize>,
    ) -> Result<Vec<StreamEntry<K, V>>> {
        let (args, offset) = self.read_args(after, count, None);
        let reply = self.run("XREAD", args, offset, false).await?;
        self.read_reply(&reply)
    }

    /// Waits until there are entries after `after` and returns them. Add `.timeout(duration)` to wait at most that long; the call then resolves to `None`.
    ///
    /// Use `.timeout` for a time limit and not `tokio::time::timeout`. A waiting call uses its own short-lived connection.
    pub fn read_wait<'a>(
        &'a self,
        after: &'a StreamId,
        count: Option<usize>,
    ) -> Pending<'a, Vec<StreamEntry<K, V>>> {
        Pending::new(move |wait| async move {
            let immediate = self.read(after, count).await?;
            if !immediate.is_empty() || wait.is_some_and(|wait| wait.is_zero()) {
                return Ok((!immediate.is_empty()).then_some(immediate));
            }
            let block = wait.map_or(Ok(0), to_millis)? as u64;
            let (args, offset) = self.read_args(after, count, Some(block));
            let reply = self.run("XREAD", args, offset, true).await?;
            let entries = self.read_reply(&reply)?;
            Ok((!entries.is_empty()).then_some(entries))
        })
    }

    /// Creates a consumer group that starts after `start` ([`StreamId::zero`] for all entries, [`StreamId::latest`] for new ones only), and creates the stream when it does not exist. Returns `false` when the group already existed.
    pub async fn create_group(&self, group: &str, start: &StreamId) -> Result<bool> {
        let args = vec![
            Value::from("CREATE"),
            Value::from(self.key.redis_key()),
            Value::from(group.to_string()),
            Value::from(start.0.clone()),
            Value::from("MKSTREAM"),
        ];
        match self.run("XGROUP", args, 1, false).await {
            Ok(_) => Ok(true),
            Err(Error::Redis(message)) if message.contains("BUSYGROUP") => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Deletes a consumer group; returns whether it existed.
    pub async fn destroy_group(&self, group: &str) -> Result<bool> {
        let args = vec![
            Value::from("DESTROY"),
            Value::from(self.key.redis_key()),
            Value::from(group.to_string()),
        ];
        let reply = self.run("XGROUP", args, 1, false).await?;
        Ok(number(&reply) == Some(1))
    }

    fn group_args(
        &self,
        group: &str,
        consumer: &str,
        count: Option<usize>,
        block: Option<u64>,
    ) -> (Vec<Value>, usize) {
        let mut args = vec![
            Value::from("GROUP"),
            Value::from(group.to_string()),
            Value::from(consumer.to_string()),
        ];
        if let Some(count) = count {
            args.extend([Value::from("COUNT"), Value::Integer(count as i64)]);
        }
        if let Some(block) = block {
            args.extend([Value::from("BLOCK"), Value::Integer(block as i64)]);
        }
        args.push(Value::from("STREAMS"));
        let offset = args.len();
        args.push(Value::from(self.key.redis_key()));
        args.push(Value::from(">"));
        (args, offset)
    }

    /// Takes the entries that no consumer of the group has received yet, without waiting. Each stays pending for `consumer` until it is acknowledged.
    pub async fn read_group(
        &self,
        group: &str,
        consumer: &str,
        count: Option<usize>,
    ) -> Result<Vec<StreamEntry<K, V>>> {
        let (args, offset) = self.group_args(group, consumer, count, None);
        let reply = self.run("XREADGROUP", args, offset, false).await?;
        self.read_reply(&reply)
    }

    /// Waits until the group has new entries and takes them. Add `.timeout(duration)` to wait at most that long; the call then resolves to `None`.
    pub fn read_group_wait<'a>(
        &'a self,
        group: &'a str,
        consumer: &'a str,
        count: Option<usize>,
    ) -> Pending<'a, Vec<StreamEntry<K, V>>> {
        Pending::new(move |wait| async move {
            let immediate = self.read_group(group, consumer, count).await?;
            if !immediate.is_empty() || wait.is_some_and(|wait| wait.is_zero()) {
                return Ok((!immediate.is_empty()).then_some(immediate));
            }
            let block = wait.map_or(Ok(0), to_millis)? as u64;
            let (args, offset) = self.group_args(group, consumer, count, Some(block));
            let reply = self.run("XREADGROUP", args, offset, true).await?;
            let entries = self.read_reply(&reply)?;
            Ok((!entries.is_empty()).then_some(entries))
        })
    }

    /// Acknowledges entries, which removes them from the pending list of the group; returns how many were pending.
    pub async fn ack(&self, group: &str, entries: &[StreamId]) -> Result<usize> {
        if entries.is_empty() {
            return Ok(0);
        }
        let mut args = vec![
            Value::from(self.key.redis_key()),
            Value::from(group.to_string()),
        ];
        args.extend(ids(entries));
        let reply = self.run("XACK", args, 0, false).await?;
        Ok(number(&reply).unwrap_or(0) as usize)
    }

    /// Summary of the pending entries of a group.
    pub async fn pending(&self, group: &str) -> Result<PendingSummary> {
        let args = vec![
            Value::from(self.key.redis_key()),
            Value::from(group.to_string()),
        ];
        let reply = self.run("XPENDING", args, 0, false).await?;
        let [count, first, last, consumers] = array(&reply) else {
            return Err(malformed());
        };
        let consumers = array(consumers)
            .iter()
            .filter_map(|pair| match array(pair) {
                [name, held] => Some((text(name)?, number(held)? as u64)),
                _ => None,
            })
            .collect();
        Ok(PendingSummary {
            count: number(count).unwrap_or(0) as u64,
            first: text(first).map(StreamId),
            last: text(last).map(StreamId),
            consumers,
        })
    }

    /// Lists up to `count` pending entries of a group with their consumer, idle time and delivery count.
    pub async fn pending_entries(&self, group: &str, count: usize) -> Result<Vec<PendingEntry>> {
        let args = vec![
            Value::from(self.key.redis_key()),
            Value::from(group.to_string()),
            Value::from("-"),
            Value::from("+"),
            Value::Integer(count as i64),
        ];
        let reply = self.run("XPENDING", args, 0, false).await?;
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
    }

    /// Moves these pending entries to `consumer` when they were idle for at least `min_idle`, and returns them.
    pub async fn claim(
        &self,
        group: &str,
        consumer: &str,
        min_idle: Duration,
        entries: &[StreamId],
    ) -> Result<Vec<StreamEntry<K, V>>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let mut args = vec![
            Value::from(self.key.redis_key()),
            Value::from(group.to_string()),
            Value::from(consumer.to_string()),
            Value::Integer(min_idle.as_millis() as i64),
        ];
        args.extend(ids(entries));
        let reply = self.run("XCLAIM", args, 0, false).await?;
        self.entries(&reply)
    }

    /// Moves up to `count` pending entries that were idle for at least `min_idle` to `consumer`, scanning from `start` ([`StreamId::zero`] to begin). Returns the id to continue from, which is `0-0` when the scan is complete, and the entries.
    pub async fn auto_claim(
        &self,
        group: &str,
        consumer: &str,
        min_idle: Duration,
        start: &StreamId,
        count: usize,
    ) -> Result<(StreamId, Vec<StreamEntry<K, V>>)> {
        let args = vec![
            Value::from(self.key.redis_key()),
            Value::from(group.to_string()),
            Value::from(consumer.to_string()),
            Value::Integer(min_idle.as_millis() as i64),
            Value::from(start.0.clone()),
            Value::from("COUNT"),
            Value::Integer(count as i64),
        ];
        let reply = self.run("XAUTOCLAIM", args, 0, false).await?;
        let parts = array(&reply);
        let (Some(next), Some(entries)) = (parts.first(), parts.get(1)) else {
            return Err(malformed());
        };
        Ok((id_of(next)?, self.entries(entries)?))
    }
}
