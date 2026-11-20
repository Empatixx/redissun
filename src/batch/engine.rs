use super::synced_from;
use super::wire::{encode, Frame, Wire};
use crate::client::core::Core;
use crate::client::retry::DelayStrategy;
use crate::error::{Error, Result};
use crate::reply::{bytes, malformed};
use fred::interfaces::{ClientLike, ClusterInterface};
use fred::types::cluster::ClusterRouting;
use fred::types::config::{Server, ServerConfig};
use fred::types::Value;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit};

pub(super) type Complete = Box<dyn FnOnce(Result<Value>) + Send>;
/// The answers of commands by their place in the batch, and the replicas that had the writes.
pub(super) type Answers = (Vec<(usize, Result<Value>)>, usize);
pub(super) type Replication = Option<(&'static str, Vec<Value>)>;

/// How many idle connections of batches a client keeps for each node.
const IDLE_CONNECTIONS: usize = 8;
/// How often commands are sent again to the node that Redis names in `MOVED` or `ASK`.
const MAX_REDIRECTIONS: usize = 5;

/// How long a batch may wait, as in Redisson: `wait` for a connection, `write` until the batch is written, and `response` for the answers once it is written.
#[derive(Clone, Copy)]
pub(super) struct Limits {
    pub(super) wait: Duration,
    pub(super) write: Duration,
    pub(super) response: Duration,
}

pub(super) struct Op {
    pub(super) name: &'static str,
    pub(super) args: Vec<Value>,
    pub(super) key_offset: usize,
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
    /// No connection could be opened.
    Connect(String),
    /// The connection failed while the batch used it.
    Io(String),
    /// No connection was free in time. As in Redisson, the wait takes the place of the pause before the next attempt.
    Busy,
    Timeout,
    /// Redis refused a whole transaction for now, as with `TRYAGAIN` while a slot is moved. Redisson retries these too.
    Again(String),
    Other(Error),
}

impl Failure {
    pub(super) fn retryable(&self) -> bool {
        !matches!(self, Failure::Other(_))
    }

    pub(super) fn into_error(self) -> Error {
        match self {
            Failure::Connect(message) | Failure::Io(message) | Failure::Again(message) => {
                Error::Redis(message)
            }
            Failure::Busy | Failure::Timeout => Error::Timeout,
            Failure::Other(error) => error,
        }
    }
}

impl From<Error> for Failure {
    fn from(error: Error) -> Self {
        Failure::Other(error)
    }
}

/// How the commands of a node are sent: as they are, or in one `MULTI`/`EXEC`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    Pipeline,
    Transaction,
}

/// The commands of a batch that go to one node, by their place in the batch. `asking` is set when Redis sent them here with `ASK`: they are then sent after `ASKING`, as Redisson does.
pub(super) struct Group {
    pub(super) server: Server,
    pub(super) asking: bool,
    pub(super) ops: Vec<usize>,
}

fn routing(core: &Core) -> Option<ClusterRouting> {
    let client = core.redis();
    if client.is_clustered() {
        client.cached_cluster_state()
    } else {
        None
    }
}

/// The node that a command goes to: the owner of its slot in a cluster, the master otherwise.
fn locate(
    core: &Core,
    routing: Option<&ClusterRouting>,
    op: &Op,
) -> std::result::Result<Server, Failure> {
    let unknown = || Failure::Connect("no Redis node is known for the command".into());
    let client = core.redis();
    if client.is_clustered() {
        let routing = routing.ok_or_else(unknown)?;
        let slot = op.args.get(op.key_offset).and_then(bytes);
        let slot = slot.map(|key| fred::util::redis_keyslot(&key));
        let owner = slot.and_then(|slot| routing.get_server(slot).cloned());
        return owner
            .or_else(|| routing.unique_primary_nodes().into_iter().next())
            .ok_or_else(unknown);
    }
    if let Some(master) = client.active_connections().into_iter().next() {
        return Ok(master);
    }
    match client.client_config().server {
        ServerConfig::Centralized { server } => Ok(server),
        _ => Err(unknown()),
    }
}

/// Sorts the commands `todo` by the node they go to, in the order of the batch.
fn plan(core: &Core, ops: &[Op], todo: &[usize]) -> std::result::Result<Vec<Group>, Failure> {
    let routing = routing(core);
    let mut groups: Vec<Group> = Vec::new();
    for &at in todo {
        let server = locate(core, routing.as_ref(), &ops[at])?;
        match groups.iter_mut().find(|group| group.server == server) {
            Some(group) => group.ops.push(at),
            None => groups.push(Group {
                server,
                asking: false,
                ops: vec![at],
            }),
        }
    }
    Ok(groups)
}

fn gate_name(server: &Server) -> String {
    format!("{}:{}", server.host, server.port)
}

/// A connection of a node while one batch uses it, like a connection a Redisson batch takes from the pool of that node: at most `pool_size` batches use a node at once.
struct Place {
    wire: Wire,
    _permit: OwnedSemaphorePermit,
}

impl Place {
    /// Waits for a free place at `server` for `limits.wait` and opens the connection within `limits.write` when no idle one is left.
    async fn take(
        core: &Arc<Core>,
        server: &Server,
        limits: Option<Limits>,
    ) -> std::result::Result<Self, Failure> {
        let waiting = core.batch_gate(&gate_name(server)).acquire_owned();
        let permit = match limits {
            Some(limits) => match tokio::time::timeout(limits.wait, waiting).await {
                Ok(permit) => permit,
                Err(_) => return Err(Failure::Busy),
            },
            None => waiting.await,
        };
        let permit = permit.map_err(|_| Failure::Other(malformed()))?;
        let wire = connect(core, server, limits).await?;
        Ok(Self {
            wire,
            _permit: permit,
        })
    }
}

/// Takes an idle connection to `server` or opens one. A server that does not answer must not hold the batch longer than a write may take.
async fn connect(
    core: &Arc<Core>,
    server: &Server,
    limits: Option<Limits>,
) -> std::result::Result<Wire, Failure> {
    let idle = {
        let mut idle = core.exclusive.lock().unwrap_or_else(|e| e.into_inner());
        let found = idle.iter().rposition(|wire| wire.server == *server);
        found.map(|at| idle.swap_remove(at))
    };
    if let Some(wire) = idle {
        return Ok(wire);
    }
    match limits {
        Some(limits) => match tokio::time::timeout(limits.write, Wire::open(core, server)).await {
            Ok(wire) => wire,
            Err(_) => Err(Failure::Busy),
        },
        None => Wire::open(core, server).await,
    }
}

/// Gives a connection back that was read to the end of its answers.
fn release(core: &Core, wire: Wire) {
    let mut idle = core.exclusive.lock().unwrap_or_else(|e| e.into_inner());
    let same = idle.iter().filter(|other| other.server == wire.server);
    if same.count() < IDLE_CONNECTIONS {
        idle.push(wire);
    }
}

/// Closes the idle connections to a node whose connection has just failed: they are most likely as dead.
fn forget(core: &Core, server: &Server) {
    let mut idle = core.exclusive.lock().unwrap_or_else(|e| e.into_inner());
    idle.retain(|wire| wire.server != *server);
}

/// Where Redis sends a command with `MOVED` or `ASK`.
fn redirect(message: &str, from: &Server) -> Option<(Server, bool)> {
    let mut parts = message.split(' ');
    let asking = match parts.next()? {
        "MOVED" => false,
        "ASK" => true,
        _ => return None,
    };
    let (host, port) = parts.nth(1)?.rsplit_once(':')?;
    let port = port.parse().ok()?;
    if host.is_empty() || host == &*from.host {
        let mut server = from.clone();
        server.port = port;
        return Some((server, asking));
    }
    Some((Server::new(host, port), asking))
}

/// What a node answered to a [`Group`].
struct Answered {
    replies: Vec<(usize, Result<Value>)>,
    synced: usize,
    /// The commands that Redis sent to another node, and the error it sent them with.
    redirected: Vec<(usize, Server, bool, String)>,
    /// The error of a transaction that Redis refused for now and applied nothing of.
    again: Option<String>,
}

fn request(
    kind: Kind,
    ops: &[Op],
    group: &Group,
    replication: &Replication,
) -> Result<(Vec<u8>, usize)> {
    let mut out = Vec::with_capacity(group.ops.len() * 48);
    let mut expected = group.ops.len();
    let ask = |out: &mut Vec<u8>, expected: &mut usize| {
        if group.asking {
            *expected += 1;
            encode(out, "ASKING", &[])
        } else {
            Ok(())
        }
    };
    if kind == Kind::Transaction {
        ask(&mut out, &mut expected)?;
        encode(&mut out, "MULTI", &[])?;
        expected += 2;
    }
    for &at in &group.ops {
        if kind == Kind::Pipeline {
            ask(&mut out, &mut expected)?;
        }
        encode(&mut out, ops[at].name, &ops[at].args)?;
    }
    if kind == Kind::Transaction {
        encode(&mut out, "EXEC", &[])?;
    }
    if let Some((name, args)) = replication {
        encode(&mut out, name, args)?;
        expected += 1;
    }
    Ok((out, expected))
}

/// Reads the answers to a transaction: `MULTI`, one for each command, `EXEC`. When Redis refused the transaction with `MOVED` or `ASK`, all its commands go to the node it names; Redis applied none of them.
fn transaction(frames: &mut impl Iterator<Item = Frame>, group: &Group) -> Answered {
    let mut answered = Answered {
        replies: Vec::with_capacity(group.ops.len()),
        synced: 0,
        redirected: Vec::new(),
        again: None,
    };
    let opened = frames.next().map(Frame::value);
    let queued: Vec<Option<String>> = group
        .ops
        .iter()
        .map(|_| match frames.next() {
            Some(Frame::Error(message)) => Some(message),
            _ => None,
        })
        .collect();
    let refused = match (opened, frames.next()) {
        (Some(Ok(_)), Some(Frame::Array(items))) if items.len() == group.ops.len() => {
            let items = items.into_iter().map(Frame::value);
            answered
                .replies
                .extend(group.ops.iter().copied().zip(items));
            return answered;
        }
        (Some(Err(Error::Redis(message))), _) | (_, Some(Frame::Error(message))) => message,
        (_, Some(Frame::Null)) => "the transaction was aborted".to_string(),
        _ => malformed().to_string(),
    };
    let again = std::iter::once(&refused)
        .chain(queued.iter().flatten())
        .find(|message| temporary(message));
    if let Some(message) = again {
        answered.again = Some(message.clone());
        return answered;
    }
    let moved = std::iter::once(&refused)
        .chain(queued.iter().flatten())
        .find_map(|message| Some((redirect(message, &group.server)?, message)));
    for (&at, queued) in group.ops.iter().zip(&queued) {
        match &moved {
            Some(((server, asking), message)) => {
                let moved = (at, server.clone(), *asking, message.to_string());
                answered.redirected.push(moved);
            }
            None => {
                let message = queued.clone().unwrap_or_else(|| refused.clone());
                answered.replies.push((at, Err(Error::Redis(message))));
            }
        }
    }
    answered
}

/// Whether Redis refuses commands only for now: while a slot is moved, a node loads its data, the cluster is down or a master has just become a replica. Redisson tries again after these.
fn temporary(message: &str) -> bool {
    ["TRYAGAIN", "CLUSTERDOWN", "LOADING", "READONLY"]
        .iter()
        .any(|kind| message.starts_with(kind))
}

fn pipeline(frames: &mut impl Iterator<Item = Frame>, group: &Group) -> Answered {
    let mut answered = Answered {
        replies: Vec::with_capacity(group.ops.len()),
        synced: 0,
        redirected: Vec::new(),
        again: None,
    };
    for &at in &group.ops {
        if group.asking {
            frames.next();
        }
        match frames.next() {
            Some(Frame::Error(message)) => match redirect(&message, &group.server) {
                Some((server, asking)) => answered.redirected.push((at, server, asking, message)),
                None => {
                    if temporary(&message) && answered.again.is_none() {
                        answered.again = Some(message.clone());
                    }
                    answered.replies.push((at, Err(Error::Redis(message))))
                }
            },
            Some(frame) => answered.replies.push((at, frame.value())),
            None => answered.replies.push((at, Err(malformed()))),
        }
    }
    answered
}

fn read(kind: Kind, frames: Vec<Frame>, group: &Group, replication: &Replication) -> Answered {
    let mut frames = frames.into_iter();
    let mut answered = match kind {
        Kind::Transaction => {
            if group.asking {
                frames.next();
            }
            transaction(&mut frames, group)
        }
        Kind::Pipeline => pipeline(&mut frames, group),
    };
    if replication.is_some() {
        answered.synced = synced_from(frames.next().and_then(|frame| frame.value().ok()));
    }
    answered
}

/// What became of the commands of a [`Group`].
pub(super) struct Settled {
    replies: Vec<(usize, Result<Value>)>,
    synced: usize,
}

/// Sends a group to its node as one pipeline on a connection of its own and follows `MOVED` and `ASK` as Redisson does: the commands that Redis sent elsewhere go to the node it named, after `ASKING` when it said `ASK`.
async fn settle(
    core: &Arc<Core>,
    kind: Kind,
    ops: &[Op],
    group: Group,
    replication: &Replication,
    limits: Option<Limits>,
) -> std::result::Result<Settled, Failure> {
    let mut settled = Settled {
        replies: Vec::with_capacity(group.ops.len()),
        synced: 0,
    };
    let mut pending = vec![group];
    for round in 0..=MAX_REDIRECTIONS {
        let mut next: Vec<Group> = Vec::new();
        for group in pending {
            let (out, expected) = request(kind, ops, &group, replication)?;
            let mut place = Place::take(core, &group.server, limits).await?;
            let frames = match place.wire.exchange(&out, expected, limits).await {
                Ok(frames) => frames,
                Err(failure) => {
                    forget(core, &group.server);
                    return Err(failure);
                }
            };
            release(core, place.wire);
            let answered = read(kind, frames, &group, replication);
            if let Some(message) = answered.again {
                forget(core, &group.server);
                return Err(Failure::Again(message));
            }
            settled.replies.extend(answered.replies);
            settled.synced += answered.synced;
            for (at, server, asking, message) in answered.redirected {
                if round == MAX_REDIRECTIONS {
                    settled.replies.push((at, Err(Error::Redis(message))));
                    continue;
                }
                if !asking {
                    core.refresh_cluster();
                }
                let known = next
                    .iter_mut()
                    .find(|group| group.server == server && group.asking == asking);
                match known {
                    Some(group) => group.ops.push(at),
                    None => next.push(Group {
                        server,
                        asking,
                        ops: vec![at],
                    }),
                }
            }
        }
        if next.is_empty() {
            break;
        }
        pending = next;
    }
    Ok(settled)
}

/// How a batch is retried.
#[derive(Clone, Copy)]
pub(super) struct Retries {
    pub(super) attempts: u32,
    pub(super) delay: DelayStrategy,
}

/// Runs the commands `first` of a batch. As in Redisson, every node gets its commands on a connection of its own, at the same time as the other nodes, and only the nodes that failed are tried again.
pub(super) async fn run(
    core: &Arc<Core>,
    kind: Kind,
    ops: &[Op],
    first: Vec<Group>,
    replication: &Replication,
    retries: Retries,
    limits: impl Fn(Duration) -> Option<Limits>,
) -> std::result::Result<Answers, Failure> {
    let mut replies = Vec::new();
    let mut synced = 0;
    let mut groups = first;
    let mut attempt = 0;
    let mut pause = Duration::ZERO;
    loop {
        pause = retries.delay.delay(attempt, pause);
        let limits = limits(pause);
        let todo: Vec<Vec<usize>> = groups.iter().map(|group| group.ops.clone()).collect();
        let settling = groups
            .into_iter()
            .map(|group| settle(core, kind, ops, group, replication, limits));
        let outcomes = futures::future::join_all(settling).await;
        let mut again = Vec::new();
        let mut failure = None;
        for (todo, outcome) in todo.into_iter().zip(outcomes) {
            match outcome {
                Ok(settled) => {
                    replies.extend(settled.replies);
                    synced += settled.synced;
                }
                Err(failed) if failed.retryable() && attempt < retries.attempts => {
                    again.extend(todo);
                    if !matches!(failure, Some(Failure::Connect(_) | Failure::Io(_))) {
                        failure = Some(failed);
                    }
                }
                Err(failed) => return Err(failed),
            }
        }
        match failure {
            None => return Ok((replies, synced)),
            Some(Failure::Busy) => {}
            Some(_) => tokio::time::sleep(pause).await,
        }
        core.refresh_cluster();
        again.sort_unstable();
        // A cluster that has no node for a slot yet, as during a failover, is asked once more after the pause.
        groups = match plan(core, ops, &again) {
            Ok(groups) => groups,
            Err(_) => {
                tokio::time::sleep(pause).await;
                plan(core, ops, &again)?
            }
        };
        attempt += 1;
    }
}

/// The first plan of a batch that was not sent yet.
pub(super) fn first(core: &Core, ops: &[Op]) -> std::result::Result<Vec<Group>, Failure> {
    let all: Vec<usize> = (0..ops.len()).collect();
    plan(core, ops, &all)
}

/// Puts the answers in the order of the batch.
pub(super) fn ordered(count: usize, replies: Vec<(usize, Result<Value>)>) -> Vec<Result<Value>> {
    let mut ordered: Vec<Option<Result<Value>>> = (0..count).map(|_| None).collect();
    for (at, reply) in replies {
        ordered[at] = Some(reply);
    }
    ordered
        .into_iter()
        .map(|reply| reply.unwrap_or_else(|| Err(malformed())))
        .collect()
}

/// A transaction that a stored batch keeps open on a node.
struct Open {
    wire: Wire,
    group: Group,
}

/// What the stored transactions answered. The commands in `redirected` were refused with `MOVED` or `ASK` and applied nowhere; they are sent again by [`run`].
pub(super) struct Finished {
    pub(super) replies: Vec<(usize, Result<Value>)>,
    pub(super) synced: usize,
    pub(super) redirected: Vec<Group>,
}

pub(super) enum Command {
    Op(Op),
    Finish {
        replication: Replication,
        limits: Option<Limits>,
        reply: oneshot::Sender<std::result::Result<Finished, Failure>>,
    },
    Discard {
        limits: Option<Limits>,
        reply: oneshot::Sender<std::result::Result<(), Failure>>,
    },
}

/// The transactions of a batch whose commands wait in Redis, as in Redisson's `REDIS_WRITE_ATOMIC`: every command is written when it is queued, after a `MULTI` on a connection that the batch keeps for that node.
pub(super) struct Stored {
    pub(super) sender: mpsc::UnboundedSender<Command>,
}

impl Stored {
    pub(super) fn start(core: Arc<Core>) -> Option<Self> {
        let runtime = Handle::try_current().ok()?;
        let (sender, mut receiver) = mpsc::unbounded_channel();
        runtime.spawn(async move {
            let mut open: Vec<Open> = Vec::new();
            let mut stored = 0;
            let mut failure = None;
            while let Some(command) = receiver.recv().await {
                match command {
                    Command::Op(op) => {
                        if failure.is_none() {
                            failure = store(&core, &mut open, &op, stored).await.err();
                        }
                        stored += 1;
                    }
                    Command::Finish {
                        replication,
                        limits,
                        reply,
                    } => {
                        let finished = match failure {
                            Some(failure) => Err(failure),
                            None => finish(&core, open, &replication, limits).await,
                        };
                        let _ = reply.send(finished);
                        return;
                    }
                    Command::Discard { limits, reply } => {
                        let discarded = match failure {
                            Some(failure) => Err(failure),
                            None => discard(&core, open, limits).await,
                        };
                        let _ = reply.send(discarded);
                        return;
                    }
                }
            }
        });
        Some(Self { sender })
    }
}

async fn store(
    core: &Arc<Core>,
    open: &mut Vec<Open>,
    op: &Op,
    at: usize,
) -> std::result::Result<(), Failure> {
    let server = locate(core, routing(core).as_ref(), op)?;
    let mut out = Vec::new();
    let known = open.iter().position(|open| open.group.server == server);
    let known = match known {
        Some(known) => known,
        None => {
            let wire = connect(core, &server, None).await?;
            encode(&mut out, "MULTI", &[])?;
            open.push(Open {
                wire,
                group: Group {
                    server,
                    asking: false,
                    ops: Vec::new(),
                },
            });
            open.len() - 1
        }
    };
    encode(&mut out, op.name, &op.args)?;
    open[known].group.ops.push(at);
    open[known].wire.send(&out).await
}

async fn finish(
    core: &Arc<Core>,
    open: Vec<Open>,
    replication: &Replication,
    limits: Option<Limits>,
) -> std::result::Result<Finished, Failure> {
    let mut finished = Finished {
        replies: Vec::new(),
        synced: 0,
        redirected: Vec::new(),
    };
    let finishing = open.into_iter().map(|mut open| async move {
        let mut out = Vec::new();
        encode(&mut out, "EXEC", &[])?;
        let mut expected = open.group.ops.len() + 2;
        if let Some((name, args)) = replication {
            encode(&mut out, name, args)?;
            expected += 1;
        }
        let frames = open.wire.exchange(&out, expected, limits).await?;
        release(core, open.wire);
        let answered = read(Kind::Transaction, frames, &open.group, replication);
        Ok::<_, Failure>((answered, open.group))
    });
    for answered in futures::future::join_all(finishing).await {
        let (mut answered, group) = answered?;
        if answered.again.take().is_some() {
            // Nothing of the transaction was applied, so it is sent again like one that Redis sent elsewhere.
            finished.redirected.push(group);
            continue;
        }
        finished.replies.extend(answered.replies);
        finished.synced += answered.synced;
        for (at, server, asking, _) in answered.redirected {
            if !asking {
                core.refresh_cluster();
            }
            let known = finished
                .redirected
                .iter_mut()
                .find(|group| group.server == server && group.asking == asking);
            match known {
                Some(group) => group.ops.push(at),
                None => finished.redirected.push(Group {
                    server,
                    asking,
                    ops: vec![at],
                }),
            }
        }
    }
    Ok(finished)
}

/// Ends every open transaction with `DISCARD`, as Redisson does for a batch whose commands wait in Redis. A connection is used again only when its `DISCARD` was answered.
async fn discard(
    core: &Arc<Core>,
    open: Vec<Open>,
    limits: Option<Limits>,
) -> std::result::Result<(), Failure> {
    let discarding = open.into_iter().map(|mut open| async move {
        let mut out = Vec::new();
        encode(&mut out, "DISCARD", &[])?;
        let expected = open.group.ops.len() + 2;
        let frames = open.wire.exchange(&out, expected, limits).await?;
        match frames.into_iter().next_back().map(Frame::value) {
            Some(Ok(_)) => {
                release(core, open.wire);
                Ok(())
            }
            Some(Err(error)) => Err(Failure::Other(error)),
            None => Err(Failure::Other(malformed())),
        }
    });
    for discarded in futures::future::join_all(discarding).await {
        discarded?;
    }
    Ok(())
}
