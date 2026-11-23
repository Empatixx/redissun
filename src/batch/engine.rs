use super::synced_from;
use crate::client::core::Core;
use crate::client::request::{command, Arg, Request};
use crate::client::slot::keyslot;
use crate::error::{Error, Result};
use crate::net::node::{server_failure, Failure as NodeFailure, Node};
use crate::net::stream::Address;
use crate::reply::malformed;
use redis::Value;
use std::collections::HashMap;
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
pub(super) type Replication = Option<(&'static str, Vec<Arg>)>;

/// How many idle connections of batches are kept for each node.
const IDLE_CONNECTIONS: usize = 8;
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
    pub(super) args: Vec<Arg>,
    pub(super) key_offset: usize,
}

impl Op {
    fn key(&self) -> Option<&[u8]> {
        match self.args.get(self.key_offset)? {
            Arg::Bytes(bytes) => Some(bytes),
            _ => None,
        }
    }

    fn slot(&self) -> Option<u16> {
        self.key().map(keyslot)
    }

    fn cmd(&self) -> redis::Cmd {
        command(self.name, self.args.clone())
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

impl From<NodeFailure> for Failure {
    fn from(failure: NodeFailure) -> Self {
        match failure {
            NodeFailure::Connection(message) | NodeFailure::Unsent(message) => {
                Failure::Failed(Error::Redis(message))
            }
            other => Failure::Other(other.into()),
        }
    }
}

impl From<Error> for Failure {
    fn from(error: Error) -> Self {
        Failure::Other(error)
    }
}

/// Where Redis Cluster sent a command instead: after `MOVED` (`ask: false`) or `ASK`.
#[derive(Clone)]
struct Redirect {
    ask: bool,
    address: Address,
}

/// One reply of a pipeline: an error reply is an error, and `MOVED` or `ASK` says where to go.
fn reply_of(value: Value) -> (Result<Value>, Option<Redirect>) {
    match value {
        Value::ServerError(error) => match server_failure(error) {
            NodeFailure::Redirect { ask, address } => (
                Err(Error::Redis(format!(
                    "{} {address}",
                    if ask { "ASK" } else { "MOVED" }
                ))),
                Some(Redirect { ask, address }),
            ),
            failure => (Err(failure.into()), None),
        },
        value => (Ok(value), None),
    }
}

fn exec_items(value: Value) -> Result<Vec<Result<Value>>> {
    match value {
        Value::Array(items) => Ok(items.into_iter().map(|item| reply_of(item).0).collect()),
        Value::Nil => Err(Error::Redis("the transaction was aborted".into())),
        other => Err(reply_of(other).0.err().unwrap_or_else(malformed)),
    }
}

/// Connections that only one batch uses at a time, like connections Redisson takes from its pool, one for each node the batch writes to. They go back to the client's idle list when the batch finished cleanly and are closed otherwise.
struct Lease {
    core: Arc<Core>,
    nodes: HashMap<Address, Arc<Node>>,
    reusable: bool,
    _permit: Option<OwnedSemaphorePermit>,
}

impl Lease {
    /// Takes one of the `pool_size` places of batches, as a Redisson batch takes a connection from the pool: it waits while other batches use all of them and gives up after `limits.wait`.
    async fn take_pooled(core: &Arc<Core>, limits: Limits) -> Outcome<Self> {
        let waiting = core.batch_connections.clone().acquire_owned();
        let permit = tokio::time::timeout(limits.wait, waiting)
            .await
            .map_err(|_| Failure::Busy)?
            .map_err(|_| malformed())?;
        let mut lease = Self::take(core);
        lease._permit = Some(permit);
        Ok(lease)
    }

    fn take(core: &Arc<Core>) -> Self {
        Self {
            core: core.clone(),
            nodes: HashMap::new(),
            reusable: false,
            _permit: None,
        }
    }

    /// The batch's connection to `address`: an idle one of the client, or a new one.
    fn node(&mut self, address: &Address) -> Arc<Node> {
        if let Some(node) = self.nodes.get(address) {
            return node.clone();
        }
        let idle = self
            .core
            .exclusive
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(address)
            .and_then(Vec::pop);
        let node = idle.unwrap_or_else(|| {
            Arc::new(Node::new(
                address.clone(),
                self.core.topology().settings().clone(),
            ))
        });
        self.nodes.insert(address.clone(), node.clone());
        node
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let nodes = std::mem::take(&mut self.nodes);
        if self.reusable {
            let mut idle = self
                .core
                .exclusive
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            for (address, node) in nodes {
                let list = idle.entry(address).or_default();
                if list.len() < IDLE_CONNECTIONS {
                    list.push(node);
                }
            }
        } else if let Ok(runtime) = Handle::try_current() {
            runtime.spawn(async move {
                for node in nodes.into_values() {
                    node.reset().await;
                }
            });
        }
    }
}

#[derive(Clone, Copy)]
enum Role {
    Multi,
    Op(usize),
    /// `EXEC`, or `DISCARD` when the batch is discarded.
    End,
    Wait,
}

/// The commands of a batch for one node.
struct Group {
    address: Address,
    ops: Vec<usize>,
    pipe: redis::Pipeline,
    roles: Vec<Role>,
    /// The replies of commands already sent, by a stored batch, in the order of `roles`.
    sent: Vec<Value>,
}

/// A transaction that Redis refused with `MOVED` or `ASK`, sent again by [`follow`].
struct Moved {
    ops: Vec<usize>,
    redirect: Redirect,
}

/// What a [`Session`] got back.
pub(super) struct Finish {
    pub(super) replies: Vec<Result<Value>>,
    pub(super) synced: usize,
    /// The transactions that Redis refused with `MOVED` or `ASK`. It applied nothing of them.
    moved: Vec<Moved>,
    /// The commands outside a transaction that Redis sent elsewhere with `MOVED` or `ASK`.
    resend: Vec<usize>,
}

/// The commands of a batch on leased connections: as they are, or for every node in one `MULTI`/`EXEC` when the session is `atomic`. A stored session sends each command as it is added; the others send each node's commands as one pipeline when the session ends.
pub(super) struct Session {
    lease: Lease,
    atomic: bool,
    stored: bool,
    map: Option<crate::net::topology::SlotMap>,
    default: Address,
    groups: Vec<Group>,
    ops: usize,
}

impl Session {
    /// Starts a session on one of the `pool_size` places of batches, or outside that limit when `limits` is `None`, as the transactions of a stored batch are, which send each command when it is added.
    pub(super) async fn begin(
        core: &Arc<Core>,
        atomic: bool,
        limits: Option<Limits>,
    ) -> Outcome<Self> {
        let stored = limits.is_none();
        let lease = match limits {
            Some(limits) => Lease::take_pooled(core, limits).await?,
            None => Lease::take(core),
        };
        let topology = core.topology();
        let default = topology.pool_for(None).await?.address().clone();
        Ok(Self {
            map: topology.slot_map(),
            default,
            lease,
            atomic,
            stored,
            groups: Vec::new(),
            ops: 0,
        })
    }

    fn address_of(&self, slot: Option<u16>) -> Address {
        slot.and_then(|slot| self.map.as_ref()?.master(slot).cloned())
            .unwrap_or_else(|| self.default.clone())
    }

    /// Adds `cmd` to its group; a stored session sends it at once.
    async fn add(&mut self, group: usize, role: Role, cmd: redis::Cmd) -> Outcome<()> {
        if self.stored {
            let node = self.lease.node(&self.groups[group].address);
            let mut pipe = redis::pipe();
            pipe.add_command(cmd);
            let mut replies = node.send_pipeline(&pipe, 0, 1).await?;
            let reply = replies.pop().unwrap_or(Value::Nil);
            self.groups[group].sent.push(reply);
        } else {
            self.groups[group].pipe.add_command(cmd);
        }
        self.groups[group].roles.push(role);
        Ok(())
    }

    pub(super) async fn send(&mut self, op: Op) -> Outcome<()> {
        let address = self.address_of(op.slot());
        let group = match self
            .groups
            .iter()
            .position(|group| group.address == address)
        {
            Some(group) => group,
            None => {
                self.groups.push(Group {
                    address,
                    ops: Vec::new(),
                    pipe: redis::pipe(),
                    roles: Vec::new(),
                    sent: Vec::new(),
                });
                let group = self.groups.len() - 1;
                if self.atomic {
                    self.add(group, Role::Multi, redis::cmd("MULTI")).await?;
                }
                group
            }
        };
        let at = self.ops;
        self.groups[group].ops.push(at);
        self.ops += 1;
        self.add(group, Role::Op(at), op.cmd()).await
    }

    /// Ends the transactions with `EXEC`, or with `DISCARD` as Redisson does for a batch that is discarded, and reads all the answers. Each node's commands go out as one pipeline, and the answers have `limits.response` to arrive.
    pub(super) async fn end(
        mut self,
        discard: bool,
        replication: Replication,
        limits: Limits,
    ) -> Outcome<Finish> {
        for group in 0..self.groups.len() {
            if self.atomic {
                let end = redis::cmd(if discard { "DISCARD" } else { "EXEC" });
                self.add(group, Role::End, end).await?;
            }
            if let Some((name, args)) = replication.as_ref().filter(|_| !discard) {
                self.add(group, Role::Wait, command(name, args.clone()))
                    .await?;
            }
        }
        // A stored session sent everything already; the others send each node's pipeline now.
        let addresses: Vec<Address> = self
            .groups
            .iter()
            .map(|group| group.address.clone())
            .collect();
        let nodes: Vec<Option<Arc<Node>>> = addresses
            .iter()
            .map(|address| (!self.stored).then(|| self.lease.node(address)))
            .collect();
        let sending = futures::future::join_all(self.groups.iter().zip(nodes).map(
            |(group, node)| async move {
                match node {
                    None => Ok(group.sent.clone()),
                    Some(node) => node.send_pipeline(&group.pipe, 0, group.roles.len()).await,
                }
            },
        ));
        let replies = match tokio::time::timeout(limits.response, sending).await {
            Ok(replies) => replies,
            Err(_) => return Err(Failure::Timeout),
        };
        let mut groups_replies = Vec::with_capacity(replies.len());
        for reply in replies {
            match reply {
                Ok(values) => groups_replies.push(values),
                Err(failure) => {
                    // A lost connection after a failover: the next attempt goes to the node that serves the slots now.
                    if crate::net::node::is_connection_failure(&failure) {
                        self.lease.core.topology().refresh().await;
                    }
                    return Err(failure.into());
                }
            }
        }
        let finish = collect(&self.groups, groups_replies, self.ops, self.atomic, discard)?;
        self.lease.reusable = true;
        Ok(finish)
    }
}

fn collect(
    groups: &[Group],
    replies: Vec<Vec<Value>>,
    ops: usize,
    atomic: bool,
    discard: bool,
) -> Outcome<Finish> {
    let mut outcomes: Vec<Option<Result<Value>>> = (0..ops).map(|_| None).collect();
    let (mut moved, mut resend, mut synced) = (Vec::new(), Vec::new(), 0);
    for (group, values) in groups.iter().zip(replies) {
        let mut redirect: Option<Redirect> = None;
        for (role, value) in group.roles.iter().zip(values) {
            match *role {
                Role::Multi => drop(reply_of(value).0?),
                Role::Op(op) => {
                    let (reply, redirected) = reply_of(value);
                    if let Some(redirected) = redirected {
                        if !atomic {
                            resend.push(op);
                        }
                        redirect.get_or_insert(redirected);
                    }
                    if !atomic || reply.is_err() {
                        outcomes[op] = Some(reply);
                    }
                }
                Role::End if discard => drop(reply_of(value).0?),
                Role::End => {
                    // A transaction queued before its slot moved hears MOVED or ASK from EXEC itself.
                    let exec_redirect = match &value {
                        Value::ServerError(_) => reply_of(value.clone()).1,
                        _ => None,
                    };
                    let redirect = redirect.clone().or(exec_redirect);
                    match exec_items(value) {
                        Ok(items) => {
                            for (&op, item) in group.ops.iter().zip(items) {
                                outcomes[op].get_or_insert(item);
                            }
                        }
                        Err(error) => {
                            if let Some(redirect) = redirect {
                                moved.push(Moved {
                                    ops: group.ops.clone(),
                                    redirect,
                                });
                            }
                            for &op in &group.ops {
                                outcomes[op].get_or_insert_with(|| Err(duplicate(&error)));
                            }
                        }
                    }
                }
                Role::Wait => synced += synced_from(reply_of(value).0.ok()),
            }
        }
    }
    let replies = outcomes
        .into_iter()
        .map(|outcome| outcome.unwrap_or_else(|| Err(malformed())));
    Ok(Finish {
        replies: replies.collect(),
        synced,
        moved,
        resend,
    })
}

/// Sends again what Redis Cluster sent elsewhere: a command outside a transaction through the client, which follows `MOVED` and `ASK`; a refused transaction as a whole to the node Redis named, after `ASKING` when it said `ASK`. Redis applied nothing of a refused transaction, so nothing runs twice.
pub(super) async fn follow(core: &Core, ops: &[Op], finish: &mut Finish) -> Outcome<()> {
    for at in std::mem::take(&mut finish.resend) {
        let op = &ops[at];
        let request = match op.key() {
            Some(key) => Request::new(op.cmd()).key(key),
            None => Request::new(op.cmd()),
        };
        finish.replies[at] = core.query_value(request).await;
    }
    for moved in std::mem::take(&mut finish.moved) {
        let node = Node::new(
            moved.redirect.address.clone(),
            core.topology().settings().clone(),
        );
        let mut pipe = redis::pipe();
        if moved.redirect.ask {
            pipe.cmd("ASKING");
        }
        pipe.cmd("MULTI");
        for &at in &moved.ops {
            pipe.add_command(ops[at].cmd());
        }
        pipe.cmd("EXEC");
        let count = pipe.len();
        let values = node.send_pipeline(&pipe, 0, count).await?;
        node.reset().await;
        let executed = values
            .into_iter()
            .last()
            .ok_or_else(malformed)
            .and_then(exec_items);
        match executed {
            Ok(items) => {
                for (&at, item) in moved.ops.iter().zip(items) {
                    finish.replies[at] = item;
                }
            }
            Err(error) => {
                for &at in &moved.ops {
                    finish.replies[at] = Err(duplicate(&error));
                }
            }
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
                    if let Ok(open) = &mut session {
                        if let Err(failure) = open.send(op).await {
                            session = Err(failure);
                        }
                    }
                }
                Command::End {
                    discard,
                    replication,
                    limits,
                    mut reply,
                } => {
                    let ending = async { session?.end(discard, replication, limits).await };
                    // A caller that gave up drops the session, which closes its connections.
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
