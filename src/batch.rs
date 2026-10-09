use crate::codec::Codec;
use crate::core::{BlockingClient, Core};
use crate::error::{Error, Result};
use crate::object::millis as to_millis;
use crate::reply::{bytes, int, malformed, number, text};
use bytes::Bytes;
use fred::clients::Client as RedisClient;
use fred::interfaces::ClientLike;
use fred::types::{ClusterHash, CustomCommand, Resp3Frame, Value};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

type Complete = Box<dyn FnOnce(Result<Value>) + Send>;
type FredResult<T> = std::result::Result<T, fred::error::Error>;
type Finished = std::result::Result<(Vec<Result<Value>>, RedisClient, BlockingClient), Failure>;
type Sending = Pin<Box<dyn Future<Output = FredResult<Resp3Frame>> + Send>>;

struct Op {
    name: &'static str,
    args: Vec<Value>,
    key_offset: usize,
    complete: Complete,
}

/// The reply to one command of a [`Batch`]. Await it after [`Batch::execute`]. Awaiting it earlier waits for an execution that has not started.
///
/// If the batch is dropped, discarded or skips its results, the handle resolves to `Error::Config`.
#[must_use = "the reply is only available through this handle"]
pub struct BatchFuture<T> {
    receiver: oneshot::Receiver<Result<T>>,
}

impl<T> Future for BatchFuture<T> {
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(cx) {
            Poll::Ready(Ok(outcome)) => Poll::Ready(outcome),
            Poll::Ready(Err(_)) => Poll::Ready(Err(Error::Config(
                "the batch was dropped, discarded or skips its results".into(),
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// What a [`Batch`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchResult {
    /// Number of commands that were sent, not counting the ones the batch adds itself.
    pub commands: usize,
    /// How many replicas had the writes when [`Batch::sync`] or [`Batch::sync_aof`] was set; otherwise 0.
    pub synced_slaves: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Pipeline,
    Atomic,
    Stored,
}

#[derive(Clone, Copy)]
struct Replication {
    slaves: u64,
    timeout: Duration,
    local_aof: Option<u64>,
}

fn frame_value(frame: Resp3Frame) -> Result<Value> {
    Ok(match frame {
        Resp3Frame::Null => Value::Null,
        Resp3Frame::SimpleString { data, .. }
        | Resp3Frame::BlobString { data, .. }
        | Resp3Frame::VerbatimString { data, .. }
        | Resp3Frame::BigNumber { data, .. } => Value::Bytes(data),
        Resp3Frame::Number { data, .. } => Value::Integer(data),
        Resp3Frame::Double { data, .. } => Value::Double(data),
        Resp3Frame::Boolean { data, .. } => Value::Boolean(data),
        Resp3Frame::SimpleError { data, .. } => return Err(Error::Redis(data.to_string())),
        Resp3Frame::BlobError { data, .. } => {
            return Err(Error::Redis(String::from_utf8_lossy(&data).into_owned()))
        }
        Resp3Frame::Array { data, .. } => Value::Array(
            data.into_iter()
                .map(frame_value)
                .collect::<Result<Vec<_>>>()?,
        ),
        Resp3Frame::Set { data, .. } => Value::Array(
            data.into_iter()
                .map(frame_value)
                .collect::<Result<Vec<_>>>()?,
        ),
        _ => return Err(malformed()),
    })
}

enum Failure {
    Fred(fred::error::Error),
    Timeout,
    Other(Error),
}

impl Failure {
    fn retryable(&self) -> bool {
        use fred::error::ErrorKind;
        match self {
            Failure::Fred(error) => matches!(
                error.kind(),
                ErrorKind::IO | ErrorKind::Timeout | ErrorKind::Canceled
            ),
            Failure::Timeout => true,
            Failure::Other(_) => false,
        }
    }

    fn into_error(self) -> Error {
        match self {
            Failure::Fred(error) => error.into(),
            Failure::Timeout => Error::Timeout,
            Failure::Other(error) => error,
        }
    }
}

impl From<fred::error::Error> for Failure {
    fn from(error: fred::error::Error) -> Self {
        Failure::Fred(error)
    }
}

impl From<Error> for Failure {
    fn from(error: Error) -> Self {
        Failure::Other(error)
    }
}

/// A dedicated connection that holds a `MULTI` transaction. Each command is sent as it is added and the answers are read at the end, so the transaction costs one round trip however long it is.
struct Session {
    client: RedisClient,
    _connection: BlockingClient,
    sending: Vec<Sending>,
    early: Vec<Option<FredResult<Resp3Frame>>>,
}

impl Session {
    async fn begin(core: &Core) -> Result<Self> {
        let connection = core.blocking_client().await?;
        let mut session = Self {
            client: (*connection).clone(),
            _connection: connection,
            sending: Vec::new(),
            early: Vec::new(),
        };
        session.send("MULTI", Vec::new(), 0).await;
        Ok(session)
    }

    async fn send(&mut self, name: &'static str, args: Vec<Value>, key_offset: usize) {
        let client = self.client.clone();
        let command = CustomCommand::new_static(name, ClusterHash::Offset(key_offset), false);
        let mut sending: Sending = Box::pin(async move { client.custom_raw(command, args).await });
        match futures::poll!(sending.as_mut()) {
            Poll::Ready(outcome) => self.early.push(Some(outcome)),
            Poll::Pending => self.early.push(None),
        }
        self.sending.push(sending);
    }

    async fn finish(
        mut self,
        count: usize,
    ) -> std::result::Result<(Vec<Result<Value>>, RedisClient, BlockingClient), Failure> {
        self.send("EXEC", Vec::new(), 0).await;
        let mut frames = Vec::with_capacity(self.sending.len());
        for (sending, early) in self.sending.into_iter().zip(self.early) {
            frames.push(match early {
                Some(outcome) => outcome,
                None => sending.await,
            });
        }
        let exec = frames.pop().ok_or_else(|| Failure::Other(malformed()))??;
        for queued in frames.into_iter().skip(1) {
            queued?;
        }
        let replies = match exec {
            Resp3Frame::Array { data, .. } => data,
            Resp3Frame::SimpleError { data, .. } => {
                return Err(Failure::Other(Error::Redis(data.to_string())))
            }
            _ => return Err(Failure::Other(malformed())),
        };
        let mut replies = replies.into_iter().map(frame_value);
        let outcomes = (0..count)
            .map(|_| replies.next().unwrap_or_else(|| Err(malformed())))
            .collect();
        Ok((outcomes, self.client, self._connection))
    }
}

enum Command {
    Op {
        name: &'static str,
        args: Vec<Value>,
        key_offset: usize,
    },
    Finish {
        count: usize,
        reply: oneshot::Sender<Finished>,
    },
}

struct Stored {
    sender: mpsc::UnboundedSender<Command>,
}

impl Stored {
    fn start(core: Arc<Core>) -> Option<Self> {
        let runtime = tokio::runtime::Handle::try_current().ok()?;
        let (sender, mut receiver) = mpsc::unbounded_channel();
        runtime.spawn(async move {
            let mut session = match Session::begin(&core).await {
                Ok(session) => session,
                Err(error) => {
                    while let Some(command) = receiver.recv().await {
                        if let Command::Finish { reply, .. } = command {
                            let _ =
                                reply.send(Err(Failure::Other(Error::Redis(error.to_string()))));
                            return;
                        }
                    }
                    return;
                }
            };
            while let Some(command) = receiver.recv().await {
                match command {
                    Command::Op {
                        name,
                        args,
                        key_offset,
                    } => {
                        session.send(name, args, key_offset).await;
                    }
                    Command::Finish { count, reply } => {
                        let _ = reply.send(session.finish(count).await);
                        return;
                    }
                }
            }
        });
        Some(Self { sender })
    }
}

/// Several commands that are sent to Redis together, as in Redisson's `RBatch`.
///
/// Get the objects from the batch (`batch.hash_map(..)`, `batch.bucket(..)`, ...). Their methods do not wait for Redis: they queue a command and return a [`BatchFuture`]. [`execute`](Batch::execute) sends everything in one round trip and fills the handles. Commands run in the order they were queued.
///
/// | Mode | Redisson | What it does |
/// |---|---|---|
/// | default | `IN_MEMORY` | the commands are held in memory and pipelined. Other clients' commands can run between them, and a failing command fails only its own handle. |
/// | [`atomic`](Batch::atomic) | `IN_MEMORY_ATOMIC` | held in memory and run in one `MULTI`/`EXEC` |
/// | [`stored_in_redis`](Batch::stored_in_redis) | `REDIS_WRITE_ATOMIC` | each command is sent to Redis at once and waits in a `MULTI` transaction on a dedicated connection until `execute` runs `EXEC`. Nothing is held in memory, and nothing is applied if the batch is dropped. |
///
/// Options: [`skip_result`](Batch::skip_result), [`response_timeout`](Batch::response_timeout), [`retry_attempts`](Batch::retry_attempts) with [`retry_interval`](Batch::retry_interval), and [`sync`](Batch::sync) or [`sync_aof`](Batch::sync_aof) to wait for replicas.
///
/// Only methods that make sense without waiting are available on the batch objects. Operations that wait, lock or need several steps are not.
#[must_use = "a batch does nothing until execute() is awaited"]
pub struct Batch<C: Codec> {
    core: Arc<Core>,
    codec: C,
    ops: Mutex<Vec<Op>>,
    stored: Mutex<Option<Stored>>,
    mode: Mode,
    skip_result: bool,
    response_timeout: Option<Duration>,
    retry_attempts: u32,
    retry_interval: Duration,
    replication: Option<Replication>,
}

impl<C: Codec> fmt::Debug for Batch<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Batch")
            .field("queued", &self.len())
            .finish_non_exhaustive()
    }
}

impl<C: Codec> Batch<C> {
    pub(crate) fn new(core: Arc<Core>, codec: C) -> Self {
        Self {
            core,
            codec,
            ops: Mutex::new(Vec::new()),
            stored: Mutex::new(None),
            mode: Mode::Pipeline,
            skip_result: false,
            response_timeout: None,
            retry_attempts: 0,
            retry_interval: Duration::from_millis(1500),
            replication: None,
        }
    }

    /// Runs the commands in one `MULTI`/`EXEC` transaction, so no other command runs between them. Redis does not roll back: a command that fails at run time fails only its own handle.
    pub fn atomic(mut self) -> Self {
        self.mode = Mode::Atomic;
        self
    }

    /// Sends every command to Redis when it is queued and keeps it in a `MULTI` transaction on a dedicated connection. [`execute`](Batch::execute) then runs `EXEC`. Use it for batches that are too big to hold in memory. It needs a tokio runtime when you queue, it cannot be retried, and in Redis Cluster all keys must share a hash slot.
    pub fn stored_in_redis(mut self) -> Self {
        self.mode = Mode::Stored;
        self
    }

    /// Does not hand out the replies: the handles resolve to `Error::Config`. If any command fails, `execute` still returns that error.
    pub fn skip_result(mut self) -> Self {
        self.skip_result = true;
        self
    }

    /// Fails `execute` with `Error::Timeout` when Redis does not answer in time.
    pub fn response_timeout(mut self, timeout: Duration) -> Self {
        self.response_timeout = Some(timeout);
        self
    }

    /// Sends the batch again up to this many times when the connection fails or the response times out. A retried write can be applied twice. The default is 0. Not available with [`stored_in_redis`](Batch::stored_in_redis).
    pub fn retry_attempts(mut self, attempts: u32) -> Self {
        self.retry_attempts = attempts;
        self
    }

    /// The pause between retries. The default is 1.5 seconds.
    pub fn retry_interval(mut self, interval: Duration) -> Self {
        self.retry_interval = interval;
        self
    }

    /// After the commands, waits until `slaves` replicas have them or `timeout` runs out (`WAIT`). [`BatchResult::synced_slaves`] says how many did.
    pub fn sync(mut self, slaves: u64, timeout: Duration) -> Self {
        self.replication = Some(Replication {
            slaves,
            timeout,
            local_aof: None,
        });
        self
    }

    /// Like [`sync`](Batch::sync) but also waits until `local` (0 or 1) local and `slaves` replica servers have fsynced the writes (`WAITAOF`, Redis 7.2 or newer).
    pub fn sync_aof(mut self, local: u64, slaves: u64, timeout: Duration) -> Self {
        self.replication = Some(Replication {
            slaves,
            timeout,
            local_aof: Some(local),
        });
        self
    }

    /// Number of queued commands.
    pub fn len(&self) -> usize {
        self.ops.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Returns whether nothing is queued.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn queue<T: Send + 'static>(
        &self,
        name: &'static str,
        args: Result<Vec<Value>>,
        key_offset: usize,
        decode: impl FnOnce(&Value) -> Result<T> + Send + 'static,
    ) -> BatchFuture<T> {
        let (sender, receiver) = oneshot::channel();
        let args = match args {
            Ok(args) => args,
            Err(error) => {
                let _ = sender.send(Err(error));
                return BatchFuture { receiver };
            }
        };
        if self.mode == Mode::Stored {
            let mut stored = self.stored.lock().unwrap_or_else(|e| e.into_inner());
            if stored.is_none() {
                *stored = Stored::start(self.core.clone());
            }
            let Some(stored) = stored.as_ref() else {
                let _ = sender.send(Err(Error::Config(
                    "a batch stored in Redis needs a tokio runtime".into(),
                )));
                return BatchFuture { receiver };
            };
            let _ = stored.sender.send(Command::Op {
                name,
                args: args.clone(),
                key_offset,
            });
        }
        let complete: Complete = Box::new(move |reply| {
            let _ = sender.send(reply.and_then(|value| decode(&value)));
        });
        self.ops.lock().unwrap_or_else(|e| e.into_inner()).push(Op {
            name,
            args,
            key_offset,
            complete,
        });
        BatchFuture { receiver }
    }

    /// Throws the queued commands away. Their handles resolve to `Error::Config`, and nothing is applied.
    pub async fn discard(self) -> Result<()> {
        self.ops.lock().unwrap_or_else(|e| e.into_inner()).clear();
        Ok(())
    }

    /// Sends every queued command and fills the handles.
    ///
    /// A command that Redis rejects fails only its own handle. The call itself fails when the connection fails or times out, and then every handle fails too.
    pub async fn execute(self) -> Result<BatchResult> {
        let ops = std::mem::take(&mut *self.ops.lock().unwrap_or_else(|e| e.into_inner()));
        let count = ops.len();
        if count == 0 {
            return Ok(BatchResult {
                commands: 0,
                synced_slaves: 0,
            });
        }
        let outcome = match self.mode {
            Mode::Stored => self.finish_stored(count).await,
            _ => self.run_with_retries(&ops).await,
        };
        match outcome {
            Ok((replies, synced_slaves)) => {
                let mut first_error = None;
                let mut replies = replies.into_iter();
                for op in ops {
                    let reply = replies.next().unwrap_or_else(|| Err(malformed()));
                    if self.skip_result {
                        if let Err(error) = reply {
                            first_error.get_or_insert(error);
                        }
                        drop(op.complete);
                    } else {
                        (op.complete)(reply);
                    }
                }
                match first_error {
                    Some(error) => Err(error),
                    None => Ok(BatchResult {
                        commands: count,
                        synced_slaves,
                    }),
                }
            }
            Err(failure) => {
                let error = failure.into_error();
                for op in ops {
                    (op.complete)(Err(match &error {
                        Error::Timeout => Error::Timeout,
                        other => Error::Redis(other.to_string()),
                    }));
                }
                Err(error)
            }
        }
    }

    async fn finish_stored(
        &self,
        count: usize,
    ) -> std::result::Result<(Vec<Result<Value>>, usize), Failure> {
        let sender = self
            .stored
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .map(|stored| stored.sender)
            .ok_or_else(|| Failure::Other(Error::Config("nothing was stored".into())))?;
        let (reply, receiver) = oneshot::channel();
        sender
            .send(Command::Finish { count, reply })
            .map_err(|_| Failure::Other(malformed()))?;
        let received = self
            .with_timeout(async { receiver.await.map_err(|_| Failure::Other(malformed()))? })
            .await?;
        let (replies, client, _connection) = received;
        let synced = self.wait_for_replicas(&client).await?;
        Ok((replies, synced))
    }

    async fn with_timeout<T>(
        &self,
        work: impl Future<Output = std::result::Result<T, Failure>>,
    ) -> std::result::Result<T, Failure> {
        match self.response_timeout {
            Some(limit) => tokio::time::timeout(limit, work)
                .await
                .unwrap_or(Err(Failure::Timeout)),
            None => work.await,
        }
    }

    async fn run_with_retries(
        &self,
        ops: &[Op],
    ) -> std::result::Result<(Vec<Result<Value>>, usize), Failure> {
        let mut attempt = 0;
        loop {
            let outcome = self.with_timeout(self.run_once(ops)).await;
            match outcome {
                Err(failure) if failure.retryable() && attempt < self.retry_attempts => {
                    attempt += 1;
                    tokio::time::sleep(self.retry_interval).await;
                }
                other => return other,
            }
        }
    }

    async fn run_once(
        &self,
        ops: &[Op],
    ) -> std::result::Result<(Vec<Result<Value>>, usize), Failure> {
        match self.mode {
            Mode::Pipeline => self.run_pipeline(ops).await,
            _ => {
                let mut session = Session::begin(&self.core).await?;
                for op in ops {
                    session.send(op.name, op.args.clone(), op.key_offset).await;
                }
                let (replies, client, _connection) = session.finish(ops.len()).await?;
                let synced = self.wait_for_replicas(&client).await?;
                Ok((replies, synced))
            }
        }
    }

    async fn run_pipeline(
        &self,
        ops: &[Op],
    ) -> std::result::Result<(Vec<Result<Value>>, usize), Failure> {
        let pipeline = self.core.redis().pipeline();
        for op in ops {
            let command =
                CustomCommand::new_static(op.name, ClusterHash::Offset(op.key_offset), false);
            let _: Value = pipeline.custom(command, op.args.clone()).await?;
        }
        if let Some((name, args)) = self.replication_command() {
            let command = CustomCommand::new_static(name, ClusterHash::Random, false);
            let _: Value = pipeline.custom(command, args).await?;
        }
        let mut replies: Vec<Result<Value>> = pipeline
            .try_all::<Value>()
            .await
            .into_iter()
            .map(|reply| reply.map_err(Error::from))
            .collect();
        let synced = if self.replication.is_some() {
            replies.pop().map_or(0, |reply| synced_from(reply.ok()))
        } else {
            0
        };
        Ok((replies, synced))
    }

    fn replication_command(&self) -> Option<(&'static str, Vec<Value>)> {
        let replication = self.replication?;
        let timeout = Value::Integer(
            replication
                .timeout
                .as_millis()
                .try_into()
                .unwrap_or(i64::MAX),
        );
        Some(match replication.local_aof {
            Some(local) => (
                "WAITAOF",
                vec![int(local), int(replication.slaves), timeout],
            ),
            None => ("WAIT", vec![int(replication.slaves), timeout]),
        })
    }

    async fn wait_for_replicas(&self, client: &RedisClient) -> std::result::Result<usize, Failure> {
        let Some((name, args)) = self.replication_command() else {
            return Ok(0);
        };
        let command = CustomCommand::new_static(name, ClusterHash::Random, false);
        let reply: Value = client.custom(command, args).await?;
        Ok(synced_from(Some(reply)))
    }

    /// Queues `DEL` for a key; the handle says whether it existed.
    pub fn del(&self, name: impl Into<String>) -> BatchFuture<bool> {
        let args = Ok(vec![Value::from(name.into())]);
        self.queue("DEL", args, 0, |reply| Ok(number(reply) == Some(1)))
    }

    /// Queues `PEXPIRE` for a key; the handle says whether the key existed.
    pub fn expire(&self, name: impl Into<String>, ttl: Duration) -> BatchFuture<bool> {
        let args = to_millis(ttl).map(|ttl| vec![Value::from(name.into()), Value::Integer(ttl)]);
        self.queue("PEXPIRE", args, 0, |reply| Ok(number(reply) == Some(1)))
    }

    /// Returns a [`BatchHashMap`] over the map `name`.
    pub fn hash_map<K, V>(&self, name: impl Into<String>) -> BatchHashMap<'_, K, V, C> {
        BatchHashMap::new(self, name.into())
    }

    /// Returns a [`BatchBucket`] over the bucket `name`.
    pub fn bucket<V>(&self, name: impl Into<String>) -> BatchBucket<'_, V, C> {
        BatchBucket::new(self, name.into())
    }

    /// Returns a [`BatchHashSet`] over the set `name`.
    pub fn hash_set<V>(&self, name: impl Into<String>) -> BatchHashSet<'_, V, C> {
        BatchHashSet::new(self, name.into())
    }

    /// Returns a [`BatchAtomicI64`] over the counter `name`.
    pub fn atomic_i64(&self, name: impl Into<String>) -> BatchAtomicI64<'_, C> {
        BatchAtomicI64 {
            batch: self,
            name: name.into(),
        }
    }

    /// Returns a [`BatchVec`] over the list `name`.
    pub fn vec<V>(&self, name: impl Into<String>) -> BatchVec<'_, V, C> {
        BatchVec::new(self, name.into())
    }

    /// Returns a [`BatchVecDeque`] over the list `name`.
    pub fn vec_deque<V>(&self, name: impl Into<String>) -> BatchVecDeque<'_, V, C> {
        BatchVecDeque::new(self, name.into())
    }

    /// Returns a [`BatchSortedSet`] over the sorted set `name`.
    pub fn sorted_set<V>(&self, name: impl Into<String>) -> BatchSortedSet<'_, V, C> {
        BatchSortedSet::new(self, name.into())
    }

    /// Returns a [`BatchTopic`] for the topic `name`.
    pub fn topic<M>(&self, name: impl Into<String>) -> BatchTopic<'_, M, C> {
        BatchTopic::new(self, name.into())
    }
}

fn optional(reply: &Value) -> Option<Bytes> {
    bytes(reply)
}

fn decode_optional<V: DeserializeOwned, C: Codec>(codec: &C, reply: &Value) -> Result<Option<V>> {
    optional(reply).map(|raw| codec.decode(&raw)).transpose()
}

fn whole(reply: &Value) -> usize {
    number(reply).unwrap_or(0).max(0) as usize
}

macro_rules! typed_view {
    ($name:ident<$($generic:ident),+>, $doc:literal) => {
        #[doc = $doc]
        pub struct $name<'a, $($generic,)+ C: Codec> {
            batch: &'a Batch<C>,
            name: String,
            _marker: PhantomData<fn() -> ($($generic,)+)>,
        }

        impl<'a, $($generic,)+ C: Codec> $name<'a, $($generic,)+ C> {
            fn new(batch: &'a Batch<C>, name: String) -> Self {
                Self { batch, name, _marker: PhantomData }
            }

            fn key(&self) -> Value {
                Value::from(self.name.clone())
            }
        }
    };
}

fn lua(script: &fred::types::scripts::Script) -> Value {
    Value::from(
        script
            .lua()
            .map(|source| source.to_string())
            .unwrap_or_default(),
    )
}

typed_view!(BatchHashMap<K, V>, "A [`HashMap`](crate::HashMap) whose methods queue commands in a [`Batch`].");

impl<'a, K, V, C> BatchHashMap<'a, K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
    C: Codec,
{
    /// Queues an insert; the handle gives the replaced value.
    pub fn insert<Q, W>(&self, k: &Q, v: &W) -> BatchFuture<Option<V>>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        let args = (|| {
            Ok(vec![
                lua(&crate::hash_map::INSERT),
                Value::Integer(1),
                self.key(),
                Value::Bytes(self.batch.codec.encode(k)?),
                Value::Bytes(self.batch.codec.encode(v)?),
            ])
        })();
        let codec = self.batch.codec.clone();
        self.batch
            .queue("EVAL", args, 2, move |reply| decode_optional(&codec, reply))
    }

    /// Queues a read; the handle gives the value.
    pub fn get<Q>(&self, k: &Q) -> BatchFuture<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let args = self
            .batch
            .codec
            .encode(k)
            .map(|field| vec![self.key(), Value::Bytes(field)]);
        let codec = self.batch.codec.clone();
        self.batch
            .queue("HGET", args, 0, move |reply| decode_optional(&codec, reply))
    }

    /// Queues a removal; the handle gives the removed value.
    pub fn remove<Q>(&self, k: &Q) -> BatchFuture<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let args = self.batch.codec.encode(k).map(|field| {
            vec![
                lua(&crate::hash_map::REMOVE),
                Value::Integer(1),
                self.key(),
                Value::Bytes(field),
            ]
        });
        let codec = self.batch.codec.clone();
        self.batch
            .queue("EVAL", args, 2, move |reply| decode_optional(&codec, reply))
    }

    /// Queues a key check.
    pub fn contains_key<Q>(&self, k: &Q) -> BatchFuture<bool>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let args = self
            .batch
            .codec
            .encode(k)
            .map(|field| vec![self.key(), Value::Bytes(field)]);
        self.batch
            .queue("HEXISTS", args, 0, |reply| Ok(number(reply) == Some(1)))
    }

    /// Queues a length query.
    pub fn len(&self) -> BatchFuture<usize> {
        self.batch
            .queue("HLEN", Ok(vec![self.key()]), 0, |reply| Ok(whole(reply)))
    }
}

typed_view!(
    BatchBucket<V>,
    "A [`Bucket`](crate::Bucket) whose methods queue commands in a [`Batch`]."
);

impl<'a, V, C> BatchBucket<'a, V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
    C: Codec,
{
    /// Queues a write.
    pub fn set<Q>(&self, value: &Q) -> BatchFuture<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let args = self
            .batch
            .codec
            .encode(value)
            .map(|raw| vec![self.key(), Value::Bytes(raw)]);
        self.batch.queue("SET", args, 0, |_| Ok(()))
    }

    /// Queues a write that expires after `ttl`.
    pub fn set_ex<Q>(&self, value: &Q, ttl: Duration) -> BatchFuture<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let args = (|| {
            Ok(vec![
                self.key(),
                Value::Bytes(self.batch.codec.encode(value)?),
                Value::from("PX"),
                Value::Integer(to_millis(ttl)?),
            ])
        })();
        self.batch.queue("SET", args, 0, |_| Ok(()))
    }

    /// Queues a read.
    pub fn get(&self) -> BatchFuture<Option<V>> {
        let codec = self.batch.codec.clone();
        self.batch
            .queue("GET", Ok(vec![self.key()]), 0, move |reply| {
                decode_optional(&codec, reply)
            })
    }
}

typed_view!(
    BatchHashSet<V>,
    "A [`HashSet`](crate::HashSet) whose methods queue commands in a [`Batch`]."
);

impl<'a, V, C> BatchHashSet<'a, V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    fn member_command<Q>(&self, name: &'static str, v: &Q) -> BatchFuture<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let args = self
            .batch
            .codec
            .encode(v)
            .map(|raw| vec![self.key(), Value::Bytes(raw)]);
        self.batch
            .queue(name, args, 0, |reply| Ok(number(reply) == Some(1)))
    }

    /// Queues an insert; the handle says whether the value was new.
    pub fn insert<Q>(&self, v: &Q) -> BatchFuture<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.member_command("SADD", v)
    }

    /// Queues a removal; the handle says whether the value was there.
    pub fn remove<Q>(&self, v: &Q) -> BatchFuture<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.member_command("SREM", v)
    }

    /// Queues a membership check.
    pub fn contains<Q>(&self, v: &Q) -> BatchFuture<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.member_command("SISMEMBER", v)
    }

    /// Queues a length query.
    pub fn len(&self) -> BatchFuture<usize> {
        self.batch
            .queue("SCARD", Ok(vec![self.key()]), 0, |reply| Ok(whole(reply)))
    }
}

/// An [`AtomicI64`](crate::AtomicI64) whose methods queue commands in a [`Batch`].
pub struct BatchAtomicI64<'a, C: Codec> {
    batch: &'a Batch<C>,
    name: String,
}

impl<C: Codec> BatchAtomicI64<'_, C> {
    fn key(&self) -> Value {
        Value::from(self.name.clone())
    }

    fn integer(&self, name: &'static str, extra: Option<i64>) -> BatchFuture<i64> {
        let mut args = vec![self.key()];
        args.extend(extra.map(Value::Integer));
        self.batch
            .queue(name, Ok(args), 0, |reply| Ok(number(reply).unwrap_or(0)))
    }

    /// Queues a read; a missing counter reads as 0.
    pub fn get(&self) -> BatchFuture<i64> {
        self.batch.queue("GET", Ok(vec![self.key()]), 0, |reply| {
            Ok(number(reply).unwrap_or(0))
        })
    }

    /// Queues a write.
    pub fn set(&self, value: i64) -> BatchFuture<()> {
        let args = Ok(vec![self.key(), Value::Integer(value)]);
        self.batch.queue("SET", args, 0, |_| Ok(()))
    }

    /// Queues adding 1; the handle gives the new value.
    pub fn incr(&self) -> BatchFuture<i64> {
        self.integer("INCR", None)
    }

    /// Queues subtracting 1; the handle gives the new value.
    pub fn decr(&self) -> BatchFuture<i64> {
        self.integer("DECR", None)
    }

    /// Queues adding `delta`; the handle gives the new value.
    pub fn add_and_get(&self, delta: i64) -> BatchFuture<i64> {
        self.integer("INCRBY", Some(delta))
    }
}

typed_view!(
    BatchVec<V>,
    "A [`Vec`](crate::Vec) whose methods queue commands in a [`Batch`]."
);

impl<'a, V, C> BatchVec<'a, V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
    C: Codec,
{
    /// Queues adding a value at the end.
    pub fn push<Q>(&self, v: &Q) -> BatchFuture<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let args = self
            .batch
            .codec
            .encode(v)
            .map(|raw| vec![self.key(), Value::Bytes(raw)]);
        self.batch.queue("RPUSH", args, 0, |_| Ok(()))
    }

    /// Queues a read of the value at `index`.
    pub fn get(&self, index: usize) -> BatchFuture<Option<V>> {
        let args = Ok(vec![self.key(), int(index)]);
        let codec = self.batch.codec.clone();
        self.batch.queue("LINDEX", args, 0, move |reply| {
            decode_optional(&codec, reply)
        })
    }

    /// Queues a length query.
    pub fn len(&self) -> BatchFuture<usize> {
        self.batch
            .queue("LLEN", Ok(vec![self.key()]), 0, |reply| Ok(whole(reply)))
    }
}

typed_view!(
    BatchVecDeque<V>,
    "A [`VecDeque`](crate::VecDeque) whose methods queue commands in a [`Batch`]."
);

impl<'a, V, C> BatchVecDeque<'a, V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
    C: Codec,
{
    fn push_command<Q>(&self, name: &'static str, v: &Q) -> BatchFuture<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let args = self
            .batch
            .codec
            .encode(v)
            .map(|raw| vec![self.key(), Value::Bytes(raw)]);
        self.batch.queue(name, args, 0, |_| Ok(()))
    }

    fn pop_command(&self, name: &'static str) -> BatchFuture<Option<V>> {
        let codec = self.batch.codec.clone();
        self.batch
            .queue(name, Ok(vec![self.key()]), 0, move |reply| {
                decode_optional(&codec, reply)
            })
    }

    /// Queues adding a value at the back.
    pub fn push_back<Q>(&self, v: &Q) -> BatchFuture<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.push_command("RPUSH", v)
    }

    /// Queues adding a value at the front.
    pub fn push_front<Q>(&self, v: &Q) -> BatchFuture<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.push_command("LPUSH", v)
    }

    /// Queues removing the front value; the handle gives it.
    pub fn pop_front(&self) -> BatchFuture<Option<V>> {
        self.pop_command("LPOP")
    }

    /// Queues removing the back value; the handle gives it.
    pub fn pop_back(&self) -> BatchFuture<Option<V>> {
        self.pop_command("RPOP")
    }

    /// Queues a length query.
    pub fn len(&self) -> BatchFuture<usize> {
        self.batch
            .queue("LLEN", Ok(vec![self.key()]), 0, |reply| Ok(whole(reply)))
    }
}

typed_view!(
    BatchSortedSet<V>,
    "A [`SortedSet`](crate::SortedSet) whose methods queue commands in a [`Batch`]."
);

impl<'a, V, C> BatchSortedSet<'a, V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    fn member_command<T: Send + 'static>(
        &self,
        name: &'static str,
        v: &impl Serialize,
        extra: Option<f64>,
        decode: impl FnOnce(&Value) -> Result<T> + Send + 'static,
    ) -> BatchFuture<T> {
        let args = self.batch.codec.encode(v).map(|raw| {
            let mut args = vec![self.key()];
            args.extend(extra.map(|score| Value::from(score.to_string())));
            args.push(Value::Bytes(raw));
            args
        });
        self.batch.queue(name, args, 0, decode)
    }

    /// Queues adding a value with a score; the handle says whether the value was new.
    pub fn insert<Q>(&self, v: &Q, score: f64) -> BatchFuture<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if score.is_nan() {
            return self.batch.queue(
                "ZADD",
                Err(Error::Config("a score cannot be NaN".into())),
                0,
                |_| Ok(false),
            );
        }
        self.member_command(
            "ZADD",
            &v,
            Some(score),
            |reply| Ok(number(reply) == Some(1)),
        )
    }

    /// Queues adding `delta` to a score; the handle gives the new score.
    pub fn add_score<Q>(&self, v: &Q, delta: f64) -> BatchFuture<f64>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if delta.is_nan() {
            return self.batch.queue(
                "ZINCRBY",
                Err(Error::Config("a score cannot be NaN".into())),
                0,
                |_| Ok(0.0),
            );
        }
        if delta.is_nan() {
            return self.batch.queue(
                "ZINCRBY",
                Err(Error::Config("a score cannot be NaN".into())),
                0,
                |_| Ok(0.0),
            );
        }
        let args = self.batch.codec.encode(v).map(|raw| {
            vec![
                self.key(),
                Value::from(delta.to_string()),
                Value::Bytes(raw),
            ]
        });
        self.batch.queue("ZINCRBY", args, 0, |reply| {
            text(reply)
                .and_then(|score| score.parse().ok())
                .ok_or_else(malformed)
        })
    }

    /// Queues a score query.
    pub fn score<Q>(&self, v: &Q) -> BatchFuture<Option<f64>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.member_command("ZSCORE", &v, None, |reply| {
            Ok(text(reply).and_then(|score| score.parse().ok()))
        })
    }

    /// Queues a removal; the handle says whether the value was there.
    pub fn remove<Q>(&self, v: &Q) -> BatchFuture<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.member_command("ZREM", &v, None, |reply| Ok(number(reply) == Some(1)))
    }

    /// Queues a length query.
    pub fn len(&self) -> BatchFuture<usize> {
        self.batch
            .queue("ZCARD", Ok(vec![self.key()]), 0, |reply| Ok(whole(reply)))
    }
}

typed_view!(
    BatchTopic<M>,
    "A [`Topic`](crate::Topic) whose publish queues a command in a [`Batch`]."
);

impl<'a, M, C> BatchTopic<'a, M, C>
where
    M: Serialize + Send + Sync,
    C: Codec,
{
    /// Queues a publish; the handle gives the number of receivers.
    pub fn publish<Q>(&self, message: &Q) -> BatchFuture<usize>
    where
        M: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let args = self
            .batch
            .codec
            .encode(message)
            .map(|raw| vec![self.key(), Value::Bytes(raw)]);
        self.batch
            .queue("PUBLISH", args, 0, |reply| Ok(whole(reply)))
    }
}

fn synced_from(reply: Option<Value>) -> usize {
    match reply {
        Some(Value::Array(items)) => items.get(1).and_then(number).unwrap_or(0).max(0) as usize,
        Some(other) => number(&other).unwrap_or(0).max(0) as usize,
        None => 0,
    }
}
