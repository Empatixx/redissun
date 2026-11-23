use super::synced_from;
use crate::client::core::Core;
use crate::error::{Error, Result};
use crate::reply::malformed;
use bytes::Bytes;
use fred::clients::Client as RedisClient;
use fred::error::ErrorKind;
use fred::interfaces::{ClientLike, ClusterInterface, MetricsInterface, TransactionInterface};
use fred::prelude::Options;
use fred::types::cluster::ClusterRouting;
use fred::types::config::Server;
use fred::types::{ClusterHash, CustomCommand, Resp3Frame, Value};
use fred::util::redis_keyslot;
use futures::FutureExt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit};

pub(super) type Complete = Box<dyn FnOnce(Result<Value>) + Send>;
/// What the engine returns: a [`Failure`] says whether the batch is tried again.
pub(super) type Outcome<T> = std::result::Result<T, Failure>;
pub(super) type Replication = Option<(&'static str, Vec<Value>)>;
type Answer<T> = std::result::Result<T, fred::error::Error>;
type Sending = Pin<Box<dyn Future<Output = Answer<Resp3Frame>> + Send>>;
type Pending = Vec<(Role, Sending, Option<Answer<Resp3Frame>>)>;

const IDLE_CONNECTIONS: usize = 8;
/// How often the client is asked whether it has written a batch. A response timeout too short for this is checked at every turn of the runtime instead.
const POLL: Duration = Duration::from_millis(1);
/// How many commands a batch hands to its connection before it lets other tasks run.
pub(super) const HANDED_AT_ONCE: usize = 1024;

/// How long a batch may wait, as in Redisson: `wait` for a connection, `write` while the connection writes nothing of the batch, and `response` for the answers once the batch is written.
#[derive(Clone, Copy)]
pub(super) struct Limits {
    pub(super) wait: Duration,
    pub(super) write: Duration,
    pub(super) response: Duration,
}

impl Limits {
    pub(super) const fn all(limit: Duration) -> Self {
        Self {
            wait: limit,
            write: limit,
            response: limit,
        }
    }
}

/// The limits of a batch that has none.
pub(super) const FOREVER: Limits = Limits::all(Duration::from_secs(1 << 30));

#[derive(Clone)]
pub(super) struct Op {
    pub(super) name: &'static str,
    pub(super) args: Vec<Value>,
    pub(super) key_offset: usize,
}

impl Op {
    fn slot(&self) -> Option<u16> {
        let key = match self.args.get(self.key_offset)? {
            Value::Bytes(bytes) => bytes.clone(),
            Value::String(text) => Bytes::copy_from_slice(text.as_bytes()),
            _ => return None,
        };
        Some(redis_keyslot(&key))
    }
}

/// The reply to one command of a [`Batch`]. Await it after [`Batch::execute`]. Awaiting it earlier waits for an execution that has not started.
///
/// If the batch is dropped, discarded or skips its results, the handle resolves to `Error::Config`.
#[must_use = "the reply is only available through this handle"]
pub struct BatchFuture<T> {
    pub(super) receiver: oneshot::Receiver<Result<T>>,
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
pub(super) enum Mode {
    Pipeline,
    Atomic,
    Stored,
}

#[derive(Clone, Copy)]
pub(super) struct Replicas {
    pub(super) slaves: u64,
    pub(super) timeout: Duration,
    pub(super) local_aof: Option<u64>,
}

pub(super) fn frame_value(frame: Resp3Frame) -> Result<Value> {
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

pub(super) fn exec_items(frame: Resp3Frame) -> Result<Vec<Result<Value>>> {
    match frame {
        Resp3Frame::Array { data, .. } => Ok(data.into_iter().map(frame_value).collect()),
        Resp3Frame::Null => Err(Error::Redis("the transaction was aborted".into())),
        other => Err(frame_value(other).err().unwrap_or_else(malformed)),
    }
}

pub(super) fn duplicate(error: &Error) -> Error {
    match error {
        Error::Timeout => Error::Timeout,
        Error::Codec(message) => Error::Codec(message.clone()),
        Error::Config(message) => Error::Config(message.clone()),
        Error::Redis(message) => Error::Redis(message.clone()),
        other => Error::Redis(other.to_string()),
    }
}

pub(super) enum Failure {
    /// The connection failed or could not be opened. The batch is tried again.
    Failed(Error),
    /// No connection was free in time. As in Redisson, the wait takes the place of the pause before the next attempt.
    Busy,
    Timeout,
    Other(Error),
}

impl Failure {
    pub(super) fn into_error(self) -> Error {
        match self {
            Failure::Failed(error) | Failure::Other(error) => error,
            Failure::Busy | Failure::Timeout => Error::Timeout,
        }
    }
}

impl From<fred::error::Error> for Failure {
    /// A command failed with its connection when fred says so; fred 10 answers a command that has no connection to its node with the last of these.
    fn from(error: fred::error::Error) -> Self {
        let lost = match error.kind() {
            ErrorKind::IO | ErrorKind::Timeout | ErrorKind::Canceled => true,
            ErrorKind::Unknown => error.details() == "Failed to route command.",
            _ => false,
        };
        match lost {
            true => Failure::Failed(error.into()),
            false => Failure::Other(error.into()),
        }
    }
}

impl From<Error> for Failure {
    fn from(error: Error) -> Self {
        Failure::Other(error)
    }
}

/// Splits what fred returned for a command into the answer of Redis and the failure of the connection.
fn answer<T>(answer: Answer<T>) -> Outcome<Result<T>> {
    match answer.map_err(Failure::from) {
        Ok(value) => Ok(Ok(value)),
        Err(Failure::Other(error)) => Ok(Err(error)),
        Err(failure) => Err(failure),
    }
}

/// Waits until the client has written everything it was given. Fails when the client writes nothing for `limit`.
async fn written(client: &RedisClient, precise: bool, limit: Duration) -> Outcome<()> {
    let (mut last, mut progress) = (usize::MAX, std::time::Instant::now());
    loop {
        let queued = client.command_queue_len();
        if queued == 0 {
            return Ok(());
        }
        if queued < last {
            progress = std::time::Instant::now();
        } else if progress.elapsed() >= limit {
            return Err(Failure::Timeout);
        }
        last = queued;
        match precise {
            true => tokio::task::yield_now().await,
            false => tokio::time::sleep(POLL).await,
        }
    }
}

/// A connection that only one batch uses at a time, like a connection Redisson takes from its pool. It goes back to the client's idle list when the batch finished cleanly and is closed otherwise.
struct Lease {
    core: Arc<Core>,
    client: RedisClient,
    reusable: bool,
    _permit: Option<OwnedSemaphorePermit>,
}

impl Lease {
    /// Takes one of the `pool_size` connections that batches share, as a Redisson batch takes a connection from the pool: it waits while other batches use all of them and gives up after `limits.wait`.
    async fn take_pooled(core: &Arc<Core>, limits: Limits) -> Outcome<Self> {
        // The wait for a free place and the opening of the connection have separate limits: a pause of a few milliseconds between attempts must not fail a connect, and a server that does not answer must not hold the batch longer than a write may take.
        let waiting = core.batch_connections.clone().acquire_owned();
        let permit = tokio::time::timeout(limits.wait, waiting).await;
        let permit = permit
            .map_err(|_| Failure::Busy)?
            .map_err(|_| malformed())?;
        let lease = tokio::time::timeout(limits.write, Self::take(core)).await;
        let mut lease = lease.map_err(|_| Failure::Busy)??;
        lease._permit = Some(permit);
        Ok(lease)
    }

    async fn take(core: &Arc<Core>) -> Outcome<Self> {
        let lease = |client| Self {
            core: core.clone(),
            client,
            reusable: false,
            _permit: None,
        };
        let idle = || {
            core.exclusive
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .pop()
        };
        while let Some(client) = idle() {
            if client.is_connected() {
                return Ok(lease(client));
            }
            tokio::spawn(async move { client.quit().await });
        }
        // Dropped unfinished, the lease closes the connection.
        let lease = lease(core.own_connection()?);
        let opened = lease.client.init().await;
        opened.map_err(|error| Failure::Failed(error.into()))?;
        core.name(&lease.client).await?;
        Ok(lease)
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut idle = self
            .core
            .exclusive
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if self.reusable && self.client.is_connected() && idle.len() < IDLE_CONNECTIONS {
            idle.push(self.client.clone());
        } else if let Ok(runtime) = Handle::try_current() {
            let client = self.client.clone();
            runtime.spawn(async move { client.quit().await });
        }
    }
}

enum Role {
    Multi,
    Op {
        op: usize,
        group: usize,
    },
    /// `EXEC`, or `DISCARD` when the batch is discarded.
    End(usize),
    Wait,
}

struct Group {
    server: Option<Server>,
    slot: Option<u16>,
    ops: Vec<usize>,
}

/// What a [`Session`] got back.
pub(super) struct Finish {
    pub(super) replies: Vec<Result<Value>>,
    pub(super) synced: usize,
    /// The commands of the transactions that Redis refused with `MOVED` or `ASK`. It applied nothing of them.
    moved: Vec<usize>,
    /// Whether fred sent every command. It may have closed a connection on which it could not.
    clean: bool,
}

/// The commands of a batch on a leased connection: as they are, or for every node in one `MULTI`/`EXEC` when the session is `atomic`. Each command is handed to the connection as it is added, in order, and the answers are read at the end, so the batch is one pipeline on the wire.
pub(super) struct Session {
    lease: Lease,
    atomic: bool,
    routing: Option<ClusterRouting>,
    groups: Vec<Group>,
    pending: Pending,
    ops: usize,
}

impl Session {
    /// Starts a session on one of the `pool_size` connections of batches, or on a connection outside that limit when `limits` is `None`, as the transactions of a stored batch are.
    pub(super) async fn begin(
        core: &Arc<Core>,
        atomic: bool,
        limits: Option<Limits>,
    ) -> Outcome<Self> {
        let lease = match limits {
            Some(limits) => Lease::take_pooled(core, limits).await?,
            None => Lease::take(core).await?,
        };
        let client = &lease.client;
        let routing = client.is_clustered().then(|| client.cached_cluster_state());
        Ok(Self {
            routing: routing.flatten(),
            lease,
            atomic,
            groups: Vec::new(),
            pending: Vec::new(),
            ops: 0,
        })
    }

    /// Hands a command to the connection at once, so commands go out in the order they are handed over.
    fn enqueue(&mut self, role: Role, name: &'static str, args: Vec<Value>, slot: Option<u16>) {
        // A command of a transaction is sent once: when the connection fails the whole batch is retried, and after `MOVED` or `ASK` the whole transaction is sent again by [`follow`]. In a cluster a command outside a transaction may be sent a second time, which is how fred follows `MOVED` for it.
        let redirected = !self.atomic && self.lease.client.is_clustered();
        let client = self.lease.client.with_options(&Options {
            max_attempts: Some(if redirected { 2 } else { 1 }),
            timeout: Some(Duration::ZERO),
            ..Default::default()
        });
        // fred routes a custom command by its key only when it is given the slot.
        let hash = slot.map_or(ClusterHash::Random, ClusterHash::Custom);
        let command = CustomCommand::new_static(name, hash, false);
        let mut sending: Sending = Box::pin(async move { client.custom_raw(command, args).await });
        let early = sending.as_mut().now_or_never();
        self.pending.push((role, sending, early));
    }

    pub(super) fn send(&mut self, op: Op) {
        let slot = op.slot();
        let routing = self.routing.as_ref();
        let server = slot.and_then(|slot| routing?.get_server(slot).cloned());
        let group = match self.groups.iter().position(|group| group.server == server) {
            Some(group) => group,
            None => {
                let ops = Vec::new();
                self.groups.push(Group { server, slot, ops });
                if self.atomic {
                    self.enqueue(Role::Multi, "MULTI", Vec::new(), slot);
                }
                self.groups.len() - 1
            }
        };
        self.groups[group].ops.push(self.ops);
        self.enqueue(
            Role::Op {
                op: self.ops,
                group,
            },
            op.name,
            op.args,
            slot,
        );
        self.ops += 1;
    }

    /// Ends the transactions with `EXEC`, or with `DISCARD` as Redisson does for a batch that is discarded, and reads all the answers. As in Redisson, the response timeout starts when the batch is written, so a big batch is not timed while it is being sent.
    pub(super) async fn end(
        mut self,
        discard: bool,
        replication: Replication,
        limits: Limits,
    ) -> Outcome<Finish> {
        for group in 0..self.groups.len() {
            let slot = self.groups[group].slot;
            if self.atomic {
                let name = if discard { "DISCARD" } else { "EXEC" };
                self.enqueue(Role::End(group), name, Vec::new(), slot);
            }
            if let Some((name, args)) = replication.as_ref().filter(|_| !discard) {
                self.enqueue(Role::Wait, name, args.clone(), slot);
            }
        }
        let pending = std::mem::take(&mut self.pending);
        let finish = {
            let collecting = collect(pending, &self.groups, self.ops, self.atomic, discard);
            tokio::pin!(collecting);
            let precise = limits.response < POLL * 32;
            tokio::select! {
                biased;
                finish = &mut collecting => finish,
                sent = written(&self.lease.client, precise, limits.write) => match sent {
                    Ok(()) => tokio::time::timeout(limits.response, collecting).await,
                    Err(failure) => Ok(Err(failure)),
                }
                .unwrap_or(Err(Failure::Timeout)),
            }
        }?;
        self.lease.reusable = finish.clean;
        Ok(finish)
    }
}

async fn collect(
    pending: Pending,
    groups: &[Group],
    ops: usize,
    atomic: bool,
    discard: bool,
) -> Outcome<Finish> {
    let mut outcomes: Vec<Option<Result<Value>>> = (0..ops).map(|_| None).collect();
    let mut redirected = vec![false; groups.len()];
    let (mut moved, mut synced, mut clean) = (Vec::new(), 0, true);
    for (role, sending, early) in pending {
        let frame = match early {
            Some(outcome) => outcome,
            None => sending.await,
        };
        // Redis's own errors come as answers, so an error here is fred's: it could not send the command, or would have had to send it again after `MOVED` or `ASK`.
        let reply = answer(frame)?;
        let unsent = reply.is_err();
        clean &= !unsent;
        match role {
            Role::Multi => drop(reply.and_then(frame_value)?),
            Role::Op { op, group } => {
                let reply = reply.and_then(frame_value);
                redirected[group] |= unsent;
                if !atomic || reply.is_err() {
                    outcomes[op] = Some(reply);
                }
            }
            Role::End(_) if discard => drop(reply.and_then(frame_value)?),
            Role::End(group) => {
                let ops = &groups[group].ops;
                match reply.and_then(exec_items) {
                    Ok(items) => {
                        for (&op, item) in ops.iter().zip(items) {
                            outcomes[op].get_or_insert(item);
                        }
                    }
                    Err(error) => {
                        if redirected[group] || unsent {
                            moved.extend_from_slice(ops);
                        }
                        for &op in ops {
                            outcomes[op].get_or_insert_with(|| Err(duplicate(&error)));
                        }
                    }
                }
            }
            Role::Wait => synced += synced_from(reply.and_then(frame_value).ok()),
        }
    }
    let replies = outcomes
        .into_iter()
        .map(|outcome| outcome.unwrap_or_else(|| Err(malformed())));
    Ok(Finish {
        replies: replies.collect(),
        synced,
        moved,
        clean,
    })
}

/// Sends the transactions again that Redis refused with `MOVED` or `ASK`, as fred's own transactions, which follow both: after `ASKING` when Redis said `ASK`. Redis applied nothing of a transaction it refused, so nothing runs twice.
pub(super) async fn follow(core: &Core, ops: &[Op], finish: &mut Finish) -> Outcome<()> {
    let mut moved = std::mem::take(&mut finish.moved);
    // The keys of a transaction share a slot in a cluster, so each slot is a transaction of its own.
    moved.sort_by_key(|&at| ops[at].slot());
    for slot in moved.chunk_by(|&a, &b| ops[a].slot() == ops[b].slot()) {
        let transaction = core.redis().multi();
        for op in slot.iter().map(|&at| &ops[at]) {
            let hash = op.slot().map_or(ClusterHash::Random, ClusterHash::Custom);
            let command = CustomCommand::new_static(op.name, hash, false);
            let _: Value = transaction.custom(command, op.args.clone()).await?;
        }
        // fred returns the answers of a transaction only when all its commands succeeded.
        let executed = answer(transaction.exec::<Vec<Value>>(true).await)?;
        for (at, &op) in slot.iter().enumerate() {
            let value = executed.as_ref().map(|values| values[at].clone());
            finish.replies[op] = value.map_err(duplicate);
        }
    }
    Ok(())
}

pub(super) enum Command {
    Op(Op),
    End {
        discard: bool,
        replication: Replication,
        limits: Limits,
        reply: oneshot::Sender<Outcome<Finish>>,
    },
}

/// Starts the transactions of a batch whose commands wait in Redis, as in Redisson's `REDIS_WRITE_ATOMIC`: every command is sent when it is queued, after a `MULTI` on a connection that the batch keeps until it ends.
pub(super) fn store_in_redis(core: Arc<Core>) -> Option<mpsc::UnboundedSender<Command>> {
    let runtime = Handle::try_current().ok()?;
    let (sender, mut receiver) = mpsc::unbounded_channel();
    runtime.spawn(async move {
        let mut session = Session::begin(&core, true, None).await;
        while let Some(command) = receiver.recv().await {
            match command {
                Command::Op(op) => {
                    if let Ok(session) = &mut session {
                        session.send(op);
                    }
                }
                Command::End {
                    discard,
                    replication,
                    limits,
                    mut reply,
                } => {
                    let ending = async { session?.end(discard, replication, limits).await };
                    // A caller that gave up drops the session, which closes its connection.
                    tokio::select! {
                        ended = ending => drop(reply.send(ended)),
                        _ = reply.closed() => {}
                    }
                    return;
                }
            }
        }
    });
    Some(sender)
}
