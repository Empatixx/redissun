use crate::codec::Codec;
use crate::core::no_retry;
use crate::error::{Error, Result};
use crate::object::{HasKey, Key};
use crate::pending::Pending;
use crate::reply::{array, bytes, int, malformed, number, text};
use bytes::Bytes;
use fred::interfaces::ClientLike;
use fred::types::scripts::Script;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::LazyLock;
use std::time::Duration;

static PENDING_RANGE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local pending = redis.call('XPENDING', KEYS[1], unpack(ARGV))
        local result = {}
        for i = 1, #pending, 1 do
            local value = redis.call('XRANGE', KEYS[1], pending[i][1], pending[i][1])
            table.insert(result, value[1])
        end
        return result",
    )
});

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

    /// The id `millis-sequence`.
    pub fn new(millis: u64, sequence: u64) -> Self {
        Self(format!("{millis}-{sequence}"))
    }

    /// The id `millis-*` for [`StreamAdd::id`]: Redis picks the next sequence number for that time.
    pub fn auto_sequence(millis: u64) -> Self {
        Self(format!("{millis}-*"))
    }

    /// This id as an exclusive end of a [`Stream::range`] (`(id`). [`StreamId::min`] and [`StreamId::max`] stay as they are.
    pub fn exclusive(&self) -> Self {
        match self.0.as_str() {
            "-" | "+" => self.clone(),
            id => Self(format!("({id}")),
        }
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
        let number = |part: &str| {
            !part.is_empty()
                && part.bytes().all(|byte| byte.is_ascii_digit())
                && part.parse::<u64>().is_ok()
        };
        let well_formed = match text.split_once('-') {
            Some((time, sequence)) => number(time) && number(sequence),
            None => number(text),
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

/// The result of [`Stream::auto_claim`].
#[derive(Clone, Debug, PartialEq)]
pub struct AutoClaim<K, V> {
    /// The id to continue the scan from; `0-0` when the scan is complete.
    pub next: StreamId,
    /// The entries now held by the claiming consumer.
    pub entries: Vec<StreamEntry<K, V>>,
    /// Pending ids whose entries no longer exist; Redis dropped them from the pending list.
    pub deleted: Vec<StreamId>,
}

/// Information about a stream, from `XINFO STREAM`.
#[derive(Clone, Debug, PartialEq)]
pub struct StreamInfo<K, V> {
    /// Number of entries.
    pub length: u64,
    /// Number of keys in the underlying radix tree.
    pub radix_tree_keys: u64,
    /// Number of nodes in the underlying radix tree.
    pub radix_tree_nodes: u64,
    /// Number of consumer groups.
    pub groups: u64,
    /// The id of the newest entry ever added.
    pub last_generated_id: StreamId,
    /// The oldest entry, if any.
    pub first_entry: Option<StreamEntry<K, V>>,
    /// The newest entry, if any.
    pub last_entry: Option<StreamEntry<K, V>>,
}

/// A consumer group of a stream, from `XINFO GROUPS`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamGroup {
    /// The group name.
    pub name: String,
    /// Number of consumers.
    pub consumers: u64,
    /// Number of delivered and not yet acknowledged entries.
    pub pending: u64,
    /// The id of the last entry delivered to the group.
    pub last_delivered_id: StreamId,
}

/// A consumer of a group, from `XINFO CONSUMERS`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamConsumer {
    /// The consumer name.
    pub name: String,
    /// Number of entries it holds unacknowledged.
    pub pending: u64,
    /// Time since it last interacted with the group.
    pub idle: Duration,
}

#[derive(Clone)]
enum Trim {
    MaxLen(u64),
    MinId(StreamId),
}

fn trim_args(trim: &Trim, strict: bool, limit: Option<u64>) -> Vec<Value> {
    let mut args = match trim {
        Trim::MaxLen(_) => vec![Value::from("MAXLEN")],
        Trim::MinId(_) => vec![Value::from("MINID")],
    };
    if !strict {
        args.push(Value::from("~"));
    }
    args.push(match trim {
        Trim::MaxLen(max_len) => int(*max_len),
        Trim::MinId(id) => Value::from(id.0.clone()),
    });
    if let Some(limit) = limit {
        args.extend([Value::from("LIMIT"), int(limit)]);
    }
    args
}

fn block_millis(wait: Duration) -> u64 {
    u64::try_from(wait.as_millis()).unwrap_or(u64::MAX).max(1)
}

fn fields(value: &Value) -> Vec<(String, Value)> {
    match value {
        Value::Array(items) => items
            .chunks(2)
            .filter_map(|pair| Some((text(&pair[0])?, pair.get(1)?.clone())))
            .collect(),
        Value::Map(map) => map
            .clone()
            .inner()
            .into_iter()
            .filter_map(|(key, value)| Some((key.as_str()?.to_string(), value)))
            .collect(),
        _ => Vec::new(),
    }
}

fn field<'v>(fields: &'v [(String, Value)], name: &str) -> &'v Value {
    fields
        .iter()
        .find(|(key, _)| key == name)
        .map_or(&Value::Null, |(_, value)| value)
}

fn count_of(fields: &[(String, Value)], name: &str) -> u64 {
    number(field(fields, name)).unwrap_or(0).max(0) as u64
}

fn id_list(value: &Value) -> Result<Vec<StreamId>> {
    array(value).iter().map(id_of).collect()
}

enum Mode {
    Retry,
    Once,
    Block,
}

fn id_of(value: &Value) -> Result<StreamId> {
    text(value).map(StreamId).ok_or_else(malformed)
}

fn ids(ids: &[StreamId]) -> Vec<Value> {
    ids.iter().map(|id| Value::from(id.0.clone())).collect()
}

/// A pending [`Stream::add`]. Await it, optionally after setting an explicit [`id`](StreamAdd::id) or a trim: [`max_len`](StreamAdd::max_len) or [`min_id`](StreamAdd::min_id), with [`non_strict`](StreamAdd::non_strict) and [`limit`](StreamAdd::limit).
#[must_use = "an add does nothing until it is awaited"]
pub struct StreamAdd<'a, K, V, C: Codec> {
    stream: &'a Stream<K, V, C>,
    fields: Result<Vec<Bytes>>,
    id: Option<StreamId>,
    trim: Option<Trim>,
    strict: bool,
    limit: Option<u64>,
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
    stream: &'a Stream<K, V, C>,
    trim: Trim,
    strict: bool,
    limit: Option<u64>,
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
    stream: &'a Stream<K, V, C>,
    group: &'a str,
    consumer: &'a str,
    count: Option<usize>,
    after: Option<StreamId>,
    no_ack: bool,
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
    stream: &'a Stream<K, V, C>,
    group: &'a str,
    count: usize,
    from: StreamId,
    to: StreamId,
    consumer: Option<&'a str>,
    min_idle: Option<Duration>,
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

/// A distributed append-only log of entries stored in a Redis Stream, with consumer groups, as in Redisson's `RStream`. Each entry has an id and fields.
///
/// Field names and values go through the codec, so with the JSON codec a field `temp` is stored as `"temp"` including the quotes. Other programs that read the stream must expect that.
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
        mode: Mode,
    ) -> Result<Value> {
        let command = CustomCommand::new_static(name, ClusterHash::Offset(key_offset), false);
        match mode {
            Mode::Retry => self.key.command(name, args, key_offset).await,
            Mode::Once => Ok(self.key.core.redis_no_retry().custom(command, args).await?),
            Mode::Block => {
                let connection = self.key.core.blocking_client().await?;
                Ok(no_retry(&*connection).custom(command, args).await?)
            }
        }
    }
}

impl<K, V, C> Stream<K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    fn entry(&self, raw: &Value) -> Result<Option<StreamEntry<K, V>>> {
        let [id, fields] = array(raw) else {
            return Ok(None);
        };
        if matches!(fields, Value::Null) {
            return Ok(None);
        }
        let parts = array(fields);
        let mut decoded = Vec::with_capacity(parts.len() / 2);
        if !parts.len().is_multiple_of(2) {
            return Err(malformed());
        }
        for pair in parts.as_chunks::<2>().0 {
            let key = bytes(&pair[0]).ok_or_else(malformed)?;
            let value = bytes(&pair[1]).ok_or_else(malformed)?;
            decoded.push((self.codec.decode(&key)?, self.codec.decode(&value)?));
        }
        Ok(Some(StreamEntry {
            id: id_of(id)?,
            fields: decoded,
        }))
    }

    fn entries(&self, value: &Value) -> Result<Vec<StreamEntry<K, V>>> {
        let mut entries = Vec::new();
        for raw in array(value) {
            entries.extend(self.entry(raw)?);
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

    /// Appends an entry made of these fields and returns its id. The fields are borrowed: `log.add([("temp", "21")])`. An entry needs at least one field. Add `.max_len(n)` to keep the stream short. It is sent once and never retried, like Redisson's `add`.
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
            id: None,
            trim: None,
            strict: true,
            limit: None,
        }
    }

    /// Number of entries.
    pub async fn len(&self) -> Result<usize> {
        let reply = self
            .run(
                "XLEN",
                vec![Value::from(self.key.redis_key())],
                0,
                Mode::Retry,
            )
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
            args.extend([Value::from("COUNT"), int(count)]);
        }
        let reply = self.run(name, args, 0, Mode::Retry).await?;
        self.entries(&reply)
    }

    /// Returns the entries from `from` to `to`, both included unless made [`exclusive`](StreamId::exclusive), oldest first. Use [`StreamId::min`] and [`StreamId::max`] for open ends.
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
        let reply = self.run("XDEL", args, 0, Mode::Retry).await?;
        Ok(number(&reply).unwrap_or(0) as usize)
    }

    /// Cuts the stream down to `max_len` entries, dropping the oldest; resolves to how many were dropped. Add `.non_strict()` for `MAXLEN ~`.
    pub fn trim(&self, max_len: u64) -> StreamTrim<'_, K, V, C> {
        StreamTrim {
            stream: self,
            trim: Trim::MaxLen(max_len),
            strict: true,
            limit: None,
        }
    }

    /// Drops the entries with an id below `min_id`; resolves to how many were dropped.
    pub fn trim_min_id(&self, min_id: &StreamId) -> StreamTrim<'_, K, V, C> {
        StreamTrim {
            stream: self,
            trim: Trim::MinId(min_id.clone()),
            strict: true,
            limit: None,
        }
    }

    fn read_args(
        &self,
        after: &StreamId,
        count: Option<usize>,
        block: Option<u64>,
    ) -> (Vec<Value>, usize) {
        let mut args = Vec::new();
        if let Some(count) = count {
            args.extend([Value::from("COUNT"), int(count)]);
        }
        if let Some(block) = block {
            args.extend([Value::from("BLOCK"), int(block)]);
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
        let reply = self.run("XREAD", args, offset, Mode::Retry).await?;
        self.read_reply(&reply)
    }

    /// Waits until there are entries after `after` and returns them (`XREAD BLOCK`). With [`StreamId::latest`] (`$`) it waits for entries added after the call. Add `.timeout(duration)` to wait at most that long, in whole milliseconds; the call then resolves to `None`. A zero timeout reads once without waiting.
    ///
    /// Use `.timeout` for a time limit and not `tokio::time::timeout`. A waiting call uses its own short-lived connection and is never retried.
    pub fn read_wait<'a>(
        &'a self,
        after: &'a StreamId,
        count: Option<usize>,
    ) -> Pending<'a, Vec<StreamEntry<K, V>>> {
        Pending::new(move |wait| async move {
            let entries = match wait {
                Some(wait) if wait.is_zero() => self.read(after, count).await?,
                _ => {
                    let block = wait.map_or(0, block_millis);
                    let (args, offset) = self.read_args(after, count, Some(block));
                    let reply = self.run("XREAD", args, offset, Mode::Block).await?;
                    self.read_reply(&reply)?
                }
            };
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
        match self.run("XGROUP", args, 1, Mode::Retry).await {
            Ok(_) => Ok(true),
            Err(Error::Redis(message)) if message.contains("BUSYGROUP") => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Deletes a consumer group (`XGROUP DESTROY`); returns whether it existed.
    pub async fn destroy_group(&self, group: &str) -> Result<bool> {
        let args = vec![
            Value::from("DESTROY"),
            Value::from(self.key.redis_key()),
            Value::from(group.to_string()),
        ];
        let reply = self.run("XGROUP", args, 1, Mode::Retry).await?;
        Ok(number(&reply) == Some(1))
    }

    /// Creates a consumer in a group (`XGROUP CREATECONSUMER`); returns `false` when it already existed.
    pub async fn create_consumer(&self, group: &str, consumer: &str) -> Result<bool> {
        let args = vec![
            Value::from("CREATECONSUMER"),
            Value::from(self.key.redis_key()),
            Value::from(group.to_string()),
            Value::from(consumer.to_string()),
        ];
        let reply = self.run("XGROUP", args, 1, Mode::Retry).await?;
        Ok(number(&reply) == Some(1))
    }

    /// Deletes a consumer from a group (`XGROUP DELCONSUMER`) and returns how many entries it still held pending; they are dropped.
    pub async fn remove_consumer(&self, group: &str, consumer: &str) -> Result<u64> {
        let args = vec![
            Value::from("DELCONSUMER"),
            Value::from(self.key.redis_key()),
            Value::from(group.to_string()),
            Value::from(consumer.to_string()),
        ];
        let reply = self.run("XGROUP", args, 1, Mode::Retry).await?;
        Ok(number(&reply).unwrap_or(0).max(0) as u64)
    }

    /// Moves the last delivered id of a group to `id` (`XGROUP SETID`), so entries after it are delivered again.
    pub async fn update_group_message_id(&self, group: &str, id: &StreamId) -> Result<()> {
        let args = vec![
            Value::from("SETID"),
            Value::from(self.key.redis_key()),
            Value::from(group.to_string()),
            Value::from(id.0.clone()),
        ];
        self.run("XGROUP", args, 1, Mode::Retry).await?;
        Ok(())
    }

    fn group_args(
        &self,
        group: &str,
        consumer: &str,
        count: Option<usize>,
        block: Option<u64>,
        no_ack: bool,
        after: Option<&StreamId>,
    ) -> (Vec<Value>, usize) {
        let mut args = vec![
            Value::from("GROUP"),
            Value::from(group.to_string()),
            Value::from(consumer.to_string()),
        ];
        if let Some(count) = count {
            args.extend([Value::from("COUNT"), int(count)]);
        }
        if let Some(block) = block {
            args.extend([Value::from("BLOCK"), int(block)]);
        }
        if no_ack {
            args.push(Value::from("NOACK"));
        }
        args.push(Value::from("STREAMS"));
        let offset = args.len();
        args.push(Value::from(self.key.redis_key()));
        args.push(Value::from(after.map_or(">", |id| id.as_str()).to_string()));
        (args, offset)
    }

    /// Takes the entries that no consumer of the group has received yet, without waiting. Each stays pending for `consumer` until it is acknowledged. Add `.no_ack()` to skip the pending list, or `.after(id)` to re-read this consumer's pending entries instead.
    pub fn read_group<'a>(
        &'a self,
        group: &'a str,
        consumer: &'a str,
        count: Option<usize>,
    ) -> ReadGroup<'a, K, V, C> {
        ReadGroup {
            stream: self,
            group,
            consumer,
            count,
            after: None,
            no_ack: false,
        }
    }

    /// Waits until the group has new entries and takes them (`XREADGROUP BLOCK`). If the call is dropped just as Redis hands entries over, they stay pending for `consumer`; [`Stream::auto_claim`] recovers them. Add `.timeout(duration)` to wait at most that long, in whole milliseconds; the call then resolves to `None`. A zero timeout reads once without waiting.
    pub fn read_group_wait<'a>(
        &'a self,
        group: &'a str,
        consumer: &'a str,
        count: Option<usize>,
    ) -> Pending<'a, Vec<StreamEntry<K, V>>> {
        Pending::new(move |wait| async move {
            let entries = match wait {
                Some(wait) if wait.is_zero() => self.read_group(group, consumer, count).await?,
                _ => {
                    let block = wait.map_or(0, block_millis);
                    let (args, offset) =
                        self.group_args(group, consumer, count, Some(block), false, None);
                    let reply = self.run("XREADGROUP", args, offset, Mode::Block).await?;
                    self.read_reply(&reply)?
                }
            };
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
        let reply = self.run("XACK", args, 0, Mode::Retry).await?;
        Ok(number(&reply).unwrap_or(0) as usize)
    }

    /// Summary of the pending entries of a group.
    pub async fn pending(&self, group: &str) -> Result<PendingSummary> {
        let args = vec![
            Value::from(self.key.redis_key()),
            Value::from(group.to_string()),
        ];
        let reply = self.run("XPENDING", args, 0, Mode::Retry).await?;
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

    /// Lists up to `count` pending entries of a group with their consumer, idle time and delivery count. Narrow it with `.range(from, to)`, `.consumer(name)` and `.min_idle(duration)`; `.messages()` returns the entries themselves.
    pub fn pending_entries<'a>(
        &'a self,
        group: &'a str,
        count: usize,
    ) -> PendingRange<'a, K, V, C> {
        PendingRange {
            stream: self,
            group,
            count,
            from: StreamId::min(),
            to: StreamId::max(),
            consumer: None,
            min_idle: None,
        }
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
            int(min_idle.as_millis()),
        ];
        args.extend(ids(entries));
        let reply = self.run("XCLAIM", args, 0, Mode::Retry).await?;
        self.entries(&reply)
    }

    /// Like [`claim`](Stream::claim) but returns only the ids (`JUSTID`), and does not count as a delivery.
    pub async fn fast_claim(
        &self,
        group: &str,
        consumer: &str,
        min_idle: Duration,
        entries: &[StreamId],
    ) -> Result<Vec<StreamId>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let mut args = vec![
            Value::from(self.key.redis_key()),
            Value::from(group.to_string()),
            Value::from(consumer.to_string()),
            int(min_idle.as_millis()),
        ];
        args.extend(ids(entries));
        args.push(Value::from("JUSTID"));
        let reply = self.run("XCLAIM", args, 0, Mode::Retry).await?;
        id_list(&reply)
    }

    fn auto_claim_args(
        &self,
        group: &str,
        consumer: &str,
        min_idle: Duration,
        start: &StreamId,
        count: usize,
    ) -> Vec<Value> {
        vec![
            Value::from(self.key.redis_key()),
            Value::from(group.to_string()),
            Value::from(consumer.to_string()),
            int(min_idle.as_millis()),
            Value::from(start.0.clone()),
            Value::from("COUNT"),
            int(count),
        ]
    }

    /// Moves up to `count` pending entries that were idle for at least `min_idle` to `consumer`, scanning from `start` ([`StreamId::zero`] to begin). Returns the id to continue from, which is `0-0` when the scan is complete, the entries, and the pending ids whose entries were deleted. Needs Redis 7 for `deleted`.
    pub async fn auto_claim(
        &self,
        group: &str,
        consumer: &str,
        min_idle: Duration,
        start: &StreamId,
        count: usize,
    ) -> Result<AutoClaim<K, V>> {
        let args = self.auto_claim_args(group, consumer, min_idle, start, count);
        let reply = self.run("XAUTOCLAIM", args, 0, Mode::Retry).await?;
        let parts = array(&reply);
        let (Some(next), Some(entries)) = (parts.first(), parts.get(1)) else {
            return Err(malformed());
        };
        Ok(AutoClaim {
            next: id_of(next)?,
            entries: self.entries(entries)?,
            deleted: parts.get(2).map_or(Ok(Vec::new()), id_list)?,
        })
    }

    /// Like [`auto_claim`](Stream::auto_claim) but returns only the next id and the claimed ids (`JUSTID`).
    pub async fn fast_auto_claim(
        &self,
        group: &str,
        consumer: &str,
        min_idle: Duration,
        start: &StreamId,
        count: usize,
    ) -> Result<(StreamId, Vec<StreamId>)> {
        let mut args = self.auto_claim_args(group, consumer, min_idle, start, count);
        args.push(Value::from("JUSTID"));
        let reply = self.run("XAUTOCLAIM", args, 0, Mode::Retry).await?;
        let parts = array(&reply);
        let (Some(next), Some(claimed)) = (parts.first(), parts.get(1)) else {
            return Err(malformed());
        };
        Ok((id_of(next)?, id_list(claimed)?))
    }

    /// Returns length, radix tree size, group count, last generated id and the first and last entry (`XINFO STREAM`). Fails when the stream does not exist.
    pub async fn info(&self) -> Result<StreamInfo<K, V>> {
        let args = vec![Value::from("STREAM"), Value::from(self.key.redis_key())];
        let reply = self.run("XINFO", args, 1, Mode::Retry).await?;
        let info = fields(&reply);
        Ok(StreamInfo {
            length: count_of(&info, "length"),
            radix_tree_keys: count_of(&info, "radix-tree-keys"),
            radix_tree_nodes: count_of(&info, "radix-tree-nodes"),
            groups: count_of(&info, "groups"),
            last_generated_id: id_of(field(&info, "last-generated-id"))?,
            first_entry: self.entry(field(&info, "first-entry"))?,
            last_entry: self.entry(field(&info, "last-entry"))?,
        })
    }

    /// Lists the consumer groups (`XINFO GROUPS`).
    pub async fn groups(&self) -> Result<Vec<StreamGroup>> {
        let args = vec![Value::from("GROUPS"), Value::from(self.key.redis_key())];
        let reply = self.run("XINFO", args, 1, Mode::Retry).await?;
        array(&reply)
            .iter()
            .map(|raw| {
                let group = fields(raw);
                Ok(StreamGroup {
                    name: text(field(&group, "name")).ok_or_else(malformed)?,
                    consumers: count_of(&group, "consumers"),
                    pending: count_of(&group, "pending"),
                    last_delivered_id: id_of(field(&group, "last-delivered-id"))?,
                })
            })
            .collect()
    }

    /// Lists the consumers of a group (`XINFO CONSUMERS`).
    pub async fn consumers(&self, group: &str) -> Result<Vec<StreamConsumer>> {
        let args = vec![
            Value::from("CONSUMERS"),
            Value::from(self.key.redis_key()),
            Value::from(group.to_string()),
        ];
        let reply = self.run("XINFO", args, 1, Mode::Retry).await?;
        array(&reply)
            .iter()
            .map(|raw| {
                let consumer = fields(raw);
                Ok(StreamConsumer {
                    name: text(field(&consumer, "name")).ok_or_else(malformed)?,
                    pending: count_of(&consumer, "pending"),
                    idle: Duration::from_millis(count_of(&consumer, "idle")),
                })
            })
            .collect()
    }
}
