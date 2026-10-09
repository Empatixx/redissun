use crate::codec::Codec;
use crate::core::Core;
use crate::error::{Error, Result};
use crate::object::millis as to_millis;
use crate::reply::{bytes, int, malformed, number, text};
use crate::retry::DelayStrategy;
use bytes::Bytes;
use fred::clients::{Client as RedisClient, Pipeline};
use fred::interfaces::{ClientLike, ClusterInterface};
use fred::prelude::Options;
use fred::types::cluster::ClusterRouting;
use fred::types::config::Server;
use fred::types::{ClusterHash, CustomCommand, Resp3Frame, Value};
use futures::FutureExt;
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
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};

type Complete = Box<dyn FnOnce(Result<Value>) + Send>;
type FredResult<T> = std::result::Result<T, fred::error::Error>;
type Replies = (Vec<Result<Value>>, usize);
type Finished = std::result::Result<(Vec<Result<Value>>, usize, Lease), Failure>;
type Sending = Pin<Box<dyn Future<Output = FredResult<Resp3Frame>> + Send>>;
type Replication = Option<(&'static str, Vec<Value>)>;

const IDLE_CONNECTIONS: usize = 8;

struct Op {
    name: &'static str,
    args: Vec<Value>,
    key_offset: usize,
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
    /// How many replicas had the writes when [`Batch::sync`] or [`Batch::sync_aof`] was set, summed over the nodes the batch wrote to; otherwise 0.
    pub synced_slaves: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Pipeline,
    Atomic,
    Stored,
}

#[derive(Clone, Copy)]
struct Replicas {
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

fn exec_items(frame: Resp3Frame) -> Result<Vec<Result<Value>>> {
    match frame {
        Resp3Frame::Array { data, .. } => Ok(data.into_iter().map(frame_value).collect()),
        Resp3Frame::Null => Err(Error::Redis("the transaction was aborted".into())),
        other => Err(frame_value(other).err().unwrap_or_else(malformed)),
    }
}

fn duplicate(error: &Error) -> Error {
    match error {
        Error::Timeout => Error::Timeout,
        Error::Codec(message) => Error::Codec(message.clone()),
        Error::Config(message) => Error::Config(message.clone()),
        Error::Redis(message) => Error::Redis(message.clone()),
        other => Error::Redis(other.to_string()),
    }
}

fn transport(error: &fred::error::Error) -> bool {
    use fred::error::ErrorKind;
    matches!(
        error.kind(),
        ErrorKind::IO | ErrorKind::Timeout | ErrorKind::Canceled
    )
}

enum Failure {
    Fred(fred::error::Error),
    Timeout,
    Other(Error),
}

impl Failure {
    fn retryable(&self) -> bool {
        match self {
            Failure::Fred(error) => transport(error),
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

fn routing(client: &RedisClient) -> Option<ClusterRouting> {
    if client.is_clustered() {
        client.cached_cluster_state()
    } else {
        None
    }
}

fn locate(
    routing: Option<&ClusterRouting>,
    args: &[Value],
    key_offset: usize,
) -> (Option<Server>, Option<u16>) {
    let Some(routing) = routing else {
        return (None, None);
    };
    let slot = args
        .get(key_offset)
        .and_then(bytes)
        .map(|key| fred::util::redis_keyslot(&key));
    (
        slot.and_then(|slot| routing.get_server(slot).cloned()),
        slot,
    )
}

fn hashed(slot: Option<u16>, fallback: ClusterHash) -> ClusterHash {
    slot.map_or(fallback, ClusterHash::Custom)
}

/// A connection that only one batch uses at a time, like a connection Redisson takes from its pool for a transaction. It goes back to the client's idle list when the batch finished cleanly and is closed otherwise.
struct Lease {
    core: Arc<Core>,
    client: RedisClient,
    reusable: bool,
}

impl Lease {
    async fn take(core: &Arc<Core>) -> std::result::Result<Self, Failure> {
        loop {
            let idle = core
                .exclusive
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .pop();
            match idle {
                Some(client) if client.is_connected() => {
                    return Ok(Self {
                        core: core.clone(),
                        client,
                        reusable: false,
                    })
                }
                Some(client) => {
                    tokio::spawn(async move {
                        let _ = client.quit().await;
                    });
                }
                None => break,
            }
        }
        let client = core.redis().clone_new();
        if let Err(error) = client.init().await {
            let _ = client.quit().await;
            return Err(error.into());
        }
        Ok(Self {
            core: core.clone(),
            client,
            reusable: false,
        })
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        if self.reusable && self.client.is_connected() {
            let mut idle = self
                .core
                .exclusive
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if idle.len() < IDLE_CONNECTIONS {
                idle.push(self.client.clone());
                return;
            }
        }
        if let Ok(runtime) = Handle::try_current() {
            let client = self.client.clone();
            runtime.spawn(async move {
                let _ = client.quit().await;
            });
        }
    }
}

enum Role {
    Multi,
    Op(usize),
    Exec(usize),
    Wait,
}

struct Group {
    server: Option<Server>,
    slot: Option<u16>,
    ops: Vec<usize>,
}

/// `MULTI`/`EXEC` transactions on a leased connection, one per node, as in Redisson's atomic modes. Each command is handed to the connection as it is added, in order, and the answers are read at the end, so the transactions cost one round trip however long they are.
struct Session {
    lease: Lease,
    routing: Option<ClusterRouting>,
    groups: Vec<Group>,
    pending: Vec<(Role, Sending, Option<FredResult<Resp3Frame>>)>,
    ops: usize,
}

impl Session {
    async fn begin(core: &Arc<Core>) -> std::result::Result<Self, Failure> {
        let lease = Lease::take(core).await?;
        let routing = routing(&lease.client);
        Ok(Self {
            lease,
            routing,
            groups: Vec::new(),
            pending: Vec::new(),
            ops: 0,
        })
    }

    fn enqueue(&mut self, role: Role, name: &'static str, args: Vec<Value>, hash: ClusterHash) {
        let client = self.lease.client.with_options(&Options {
            max_attempts: Some(1),
            timeout: Some(Duration::ZERO),
            ..Default::default()
        });
        let command = CustomCommand::new_static(name, hash, false);
        let mut sending: Sending = Box::pin(async move { client.custom_raw(command, args).await });
        let early = sending.as_mut().now_or_never();
        self.pending.push((role, sending, early));
    }

    fn send(&mut self, name: &'static str, args: Vec<Value>, key_offset: usize) {
        let (server, slot) = locate(self.routing.as_ref(), &args, key_offset);
        let group = match self.groups.iter().position(|group| group.server == server) {
            Some(group) => group,
            None => {
                self.groups.push(Group {
                    server,
                    slot,
                    ops: Vec::new(),
                });
                self.enqueue(
                    Role::Multi,
                    "MULTI",
                    Vec::new(),
                    hashed(slot, ClusterHash::Random),
                );
                self.groups.len() - 1
            }
        };
        let op = self.ops;
        self.ops += 1;
        self.groups[group].ops.push(op);
        self.enqueue(
            Role::Op(op),
            name,
            args,
            hashed(slot, ClusterHash::Offset(key_offset)),
        );
    }

    async fn finish(mut self, replication: Replication) -> Finished {
        for group in 0..self.groups.len() {
            let hash = hashed(self.groups[group].slot, ClusterHash::Random);
            self.enqueue(Role::Exec(group), "EXEC", Vec::new(), hash.clone());
            if let Some((name, args)) = &replication {
                self.enqueue(Role::Wait, name, args.clone(), hash);
            }
        }
        let mut outcomes: Vec<Option<Result<Value>>> = (0..self.ops).map(|_| None).collect();
        let mut synced = 0;
        for (role, sending, early) in std::mem::take(&mut self.pending) {
            let frame = match early {
                Some(outcome) => outcome,
                None => sending.await,
            };
            let frame = match frame {
                Ok(frame) => Ok(frame),
                Err(error) if transport(&error) => return Err(Failure::Fred(error)),
                Err(error) => Err(Error::from(error)),
            };
            match role {
                Role::Multi => {
                    frame.and_then(frame_value)?;
                }
                Role::Op(op) => {
                    if let Err(error) = frame.and_then(frame_value) {
                        outcomes[op] = Some(Err(error));
                    }
                }
                Role::Exec(group) => match frame.and_then(exec_items) {
                    Ok(items) => {
                        let mut items = items.into_iter();
                        for &op in &self.groups[group].ops {
                            let item = items.next().unwrap_or_else(|| Err(malformed()));
                            outcomes[op].get_or_insert(item);
                        }
                    }
                    Err(error) => {
                        for &op in &self.groups[group].ops {
                            outcomes[op].get_or_insert_with(|| Err(duplicate(&error)));
                        }
                    }
                },
                Role::Wait => synced += synced_from(frame.and_then(frame_value).ok()),
            }
        }
        let outcomes = outcomes
            .into_iter()
            .map(|outcome| outcome.unwrap_or_else(|| Err(malformed())))
            .collect();
        self.lease.reusable = true;
        Ok((outcomes, synced, self.lease))
    }
}

enum Command {
    Op {
        name: &'static str,
        args: Vec<Value>,
        key_offset: usize,
    },
    Finish {
        replication: Replication,
        reply: oneshot::Sender<Finished>,
    },
}

struct Stored {
    sender: mpsc::UnboundedSender<Command>,
}

impl Stored {
    fn start(core: Arc<Core>) -> Option<Self> {
        let runtime = Handle::try_current().ok()?;
        let (sender, mut receiver) = mpsc::unbounded_channel();
        runtime.spawn(async move {
            let mut session = match Session::begin(&core).await {
                Ok(session) => session,
                Err(failure) => {
                    while let Some(command) = receiver.recv().await {
                        if let Command::Finish { reply, .. } = command {
                            let _ = reply.send(Err(failure));
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
                    } => session.send(name, args, key_offset),
                    Command::Finish { replication, reply } => {
                        let _ = reply.send(session.finish(replication).await);
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
/// | default | `IN_MEMORY` | the commands are held in memory and pipelined. Other clients' commands can run between them. |
/// | [`atomic`](Batch::atomic) | `IN_MEMORY_ATOMIC` | held in memory and run in one `MULTI`/`EXEC` per node on a connection that only this batch uses |
/// | [`stored_in_redis`](Batch::stored_in_redis) | `REDIS_WRITE_ATOMIC` | each command is sent to Redis at once and waits in a `MULTI` transaction per node on a connection that only this batch uses, until `execute` runs `EXEC`. Nothing is held in memory, and nothing is applied if the batch is dropped. |
///
/// In every mode a command that Redis rejects fails its own handle and makes `execute` return that error, while the other handles get their replies. Redis does not roll back, so the other commands are applied, also in the atomic modes.
///
/// Options: [`skip_result`](Batch::skip_result), [`response_timeout`](Batch::response_timeout), [`retry_attempts`](Batch::retry_attempts) with [`retry_interval`](Batch::retry_interval), and [`sync`](Batch::sync) or [`sync_aof`](Batch::sync_aof) to wait for replicas.
///
/// Only methods that make sense without waiting are available on the batch objects. Operations that wait, lock or need several steps are not.
#[must_use = "a batch does nothing until execute() is awaited"]
pub struct Batch<C: Codec> {
    core: Arc<Core>,
    codec: C,
    ops: Mutex<Vec<(Op, Complete)>>,
    stored: Mutex<Option<Stored>>,
    mode: Mode,
    skip_result: bool,
    response_timeout: Duration,
    retry_attempts: u32,
    retry_delay: DelayStrategy,
    sync: Option<Replicas>,
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
        let retry = core.retry;
        Self {
            core,
            codec,
            ops: Mutex::new(Vec::new()),
            stored: Mutex::new(None),
            mode: Mode::Pipeline,
            skip_result: false,
            response_timeout: retry.timeout,
            retry_attempts: retry.attempts,
            retry_delay: retry.delay,
            sync: None,
        }
    }

    /// Runs the commands in a `MULTI`/`EXEC` transaction, one per Redis node, so no other command runs between the commands of a node. Redis does not roll back: a command that fails at run time does not undo the others. In Redis Cluster the keys on one node must share a hash slot, because Redis refuses a transaction that spans slots.
    pub fn atomic(mut self) -> Self {
        self.mode = Mode::Atomic;
        self
    }

    /// Sends every command to Redis when it is queued and keeps it in a `MULTI` transaction per node on a connection that only this batch uses. [`execute`](Batch::execute) then runs `EXEC`. Use it for batches that are too big to hold in memory. It needs a tokio runtime when you queue and it is not retried. In Redis Cluster the keys on one node must share a hash slot.
    pub fn stored_in_redis(mut self) -> Self {
        self.mode = Mode::Stored;
        self
    }

    /// Does not hand out the replies: the handles resolve to `Error::Config`. If any command fails, `execute` still returns that error.
    pub fn skip_result(mut self) -> Self {
        self.skip_result = true;
        self
    }

    /// How long one attempt may wait for Redis before it fails with `Error::Timeout` or is retried. With [`sync`](Batch::sync) the sync timeout is added. The default is the client's [`timeout`](crate::ClientBuilder::timeout), 3 seconds unless changed, as in Redisson; zero waits forever.
    pub fn response_timeout(mut self, timeout: Duration) -> Self {
        self.response_timeout = timeout;
        self
    }

    /// Sends the batch again up to this many times when the connection fails or the response times out. A retried write can be applied twice. The default is the client's [`retry_attempts`](crate::ClientBuilder::retry_attempts), 4 unless changed, as in Redisson. Not used by [`stored_in_redis`](Batch::stored_in_redis).
    pub fn retry_attempts(mut self, attempts: u32) -> Self {
        self.retry_attempts = attempts;
        self
    }

    /// A fixed pause between retries, the same as `retry_delay(DelayStrategy::Constant(interval))`.
    pub fn retry_interval(mut self, interval: Duration) -> Self {
        self.retry_delay = DelayStrategy::Constant(interval);
        self
    }

    /// The pause between retries, like Redisson's `retryDelay`. The default is the client's [`retry_delay`](crate::ClientBuilder::retry_delay): unless changed, it grows from about 0.5 to 2 seconds with random jitter, like Redisson's `EqualJitterDelay(1s, 2s)`.
    pub fn retry_delay(mut self, delay: DelayStrategy) -> Self {
        self.retry_delay = delay;
        self
    }

    /// After the commands, waits on every node the batch wrote to until `slaves` replicas have the writes or `timeout` runs out (`WAIT`). [`BatchResult::synced_slaves`] gives the sum over the nodes.
    pub fn sync(mut self, slaves: u64, timeout: Duration) -> Self {
        self.sync = Some(Replicas {
            slaves,
            timeout,
            local_aof: None,
        });
        self
    }

    /// Like [`sync`](Batch::sync) but also waits until `local` (0 or 1) local and `slaves` replica servers have fsynced the writes (`WAITAOF`, Redis 7.2 or newer).
    pub fn sync_aof(mut self, local: u64, slaves: u64, timeout: Duration) -> Self {
        self.sync = Some(Replicas {
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
        self.ops.lock().unwrap_or_else(|e| e.into_inner()).push((
            Op {
                name,
                args,
                key_offset,
            },
            complete,
        ));
        BatchFuture { receiver }
    }

    /// Throws the queued commands away. Their handles resolve to `Error::Config`, and nothing is applied.
    pub async fn discard(self) -> Result<()> {
        self.ops.lock().unwrap_or_else(|e| e.into_inner()).clear();
        Ok(())
    }

    /// Sends every queued command and fills the handles.
    ///
    /// As in Redisson, a command that Redis rejects fails its own handle and the call returns the first such error; the other handles still get their replies. When the connection fails or times out after the retries, the call and every handle fail.
    pub async fn execute(self) -> Result<BatchResult> {
        let queued = std::mem::take(&mut *self.ops.lock().unwrap_or_else(|e| e.into_inner()));
        let (ops, completes): (Vec<Op>, Vec<Complete>) = queued.into_iter().unzip();
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
                for complete in completes {
                    let reply = replies.next().unwrap_or_else(|| Err(malformed()));
                    if let Err(error) = &reply {
                        first_error.get_or_insert_with(|| duplicate(error));
                    }
                    if self.skip_result {
                        drop(complete);
                    } else {
                        complete(reply);
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
                for complete in completes {
                    complete(Err(duplicate(&error)));
                }
                Err(error)
            }
        }
    }

    async fn finish_stored(&self, count: usize) -> std::result::Result<Replies, Failure> {
        let sender = self
            .stored
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .map(|stored| stored.sender)
            .ok_or_else(|| Failure::Other(Error::Config("nothing was stored".into())))?;
        let (reply, receiver) = oneshot::channel();
        sender
            .send(Command::Finish {
                replication: self.replication_command(),
                reply,
            })
            .map_err(|_| Failure::Other(malformed()))?;
        let (replies, synced, _lease) = self
            .with_timeout(async { receiver.await.map_err(|_| Failure::Other(malformed()))? })
            .await?;
        if replies.len() != count {
            return Err(Failure::Other(malformed()));
        }
        Ok((replies, synced))
    }

    fn attempt_timeout(&self) -> Duration {
        self.response_timeout + self.sync.map_or(Duration::ZERO, |sync| sync.timeout)
    }

    async fn with_timeout<T>(
        &self,
        work: impl Future<Output = std::result::Result<T, Failure>>,
    ) -> std::result::Result<T, Failure> {
        if self.response_timeout.is_zero() {
            return work.await;
        }
        tokio::time::timeout(self.attempt_timeout(), work)
            .await
            .unwrap_or(Err(Failure::Timeout))
    }

    async fn run_with_retries(&self, ops: &[Op]) -> std::result::Result<Replies, Failure> {
        let mut attempt = 0;
        let mut pause = Duration::ZERO;
        loop {
            let outcome = self.with_timeout(self.run_once(ops)).await;
            match outcome {
                Err(failure) if failure.retryable() && attempt < self.retry_attempts => {
                    pause = self.retry_delay.delay(attempt, pause);
                    tokio::time::sleep(pause).await;
                    attempt += 1;
                }
                other => return other,
            }
        }
    }

    async fn run_once(&self, ops: &[Op]) -> std::result::Result<Replies, Failure> {
        match self.mode {
            Mode::Pipeline => self.run_pipeline(ops).await,
            _ => {
                let mut session = Session::begin(&self.core).await?;
                for op in ops {
                    session.send(op.name, op.args.clone(), op.key_offset);
                }
                let (replies, synced, _lease) = session.finish(self.replication_command()).await?;
                Ok((replies, synced))
            }
        }
    }

    async fn run_pipeline(&self, ops: &[Op]) -> std::result::Result<Replies, Failure> {
        let client = self.core.redis();
        let routing = routing(client);
        let pipeline = Pipeline::from(client.with_options(&Options {
            timeout: Some(Duration::ZERO),
            ..Default::default()
        }));
        let mut nodes: Vec<(Option<Server>, Option<u16>)> = Vec::new();
        for op in ops {
            let (server, slot) = locate(routing.as_ref(), &op.args, op.key_offset);
            if !nodes.iter().any(|(known, _)| *known == server) {
                nodes.push((server, slot));
            }
            let command =
                CustomCommand::new_static(op.name, ClusterHash::Offset(op.key_offset), false);
            let _: Value = pipeline.custom(command, op.args.clone()).await?;
        }
        let waits = match self.replication_command() {
            Some((name, args)) => {
                for (_, slot) in &nodes {
                    let command =
                        CustomCommand::new_static(name, hashed(*slot, ClusterHash::Random), false);
                    let _: Value = pipeline.custom(command, args.clone()).await?;
                }
                nodes.len()
            }
            None => 0,
        };
        let replies = pipeline.try_all::<Value>().await;
        if replies.len() != ops.len() + waits
            || replies
                .iter()
                .any(|reply| matches!(reply, Err(error) if transport(error)))
        {
            return Err(match replies.into_iter().find_map(|reply| reply.err()) {
                Some(error) => Failure::Fred(error),
                None => Failure::Other(malformed()),
            });
        }
        let mut replies: Vec<Result<Value>> = replies
            .into_iter()
            .map(|reply| reply.map_err(Error::from))
            .collect();
        let synced = replies
            .split_off(ops.len())
            .into_iter()
            .map(|reply| synced_from(reply.ok()))
            .sum();
        Ok((replies, synced))
    }

    fn replication_command(&self) -> Replication {
        let sync = self.sync?;
        let timeout = Value::Integer(sync.timeout.as_millis().try_into().unwrap_or(i64::MAX));
        Some(match sync.local_aof {
            Some(local) => ("WAITAOF", vec![int(local), int(sync.slaves), timeout]),
            None => ("WAIT", vec![int(sync.slaves), timeout]),
        })
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
