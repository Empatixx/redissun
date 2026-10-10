use super::synced_from;
use crate::client::core::Core;
use crate::error::{Error, Result};
use crate::reply::{bytes, malformed};
use fred::clients::Client as RedisClient;
use fred::interfaces::{ClientLike, ClusterInterface};
use fred::prelude::Options;
use fred::types::cluster::ClusterRouting;
use fred::types::config::Server;
use fred::types::{ClusterHash, CustomCommand, Resp3Frame, Value};
use futures::FutureExt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};

pub(super) type Complete = Box<dyn FnOnce(Result<Value>) + Send>;
pub(super) type FredResult<T> = std::result::Result<T, fred::error::Error>;
pub(super) type Replies = (Vec<Result<Value>>, usize);
pub(super) type Finished = std::result::Result<(Vec<Result<Value>>, usize, Lease), Failure>;
pub(super) type Sending = Pin<Box<dyn Future<Output = FredResult<Resp3Frame>> + Send>>;
pub(super) type Replication = Option<(&'static str, Vec<Value>)>;

pub(super) const IDLE_CONNECTIONS: usize = 8;

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

pub(super) fn transport(error: &fred::error::Error) -> bool {
    use fred::error::ErrorKind;
    matches!(
        error.kind(),
        ErrorKind::IO | ErrorKind::Timeout | ErrorKind::Canceled
    )
}

pub(super) enum Failure {
    Fred(fred::error::Error),
    Timeout,
    Other(Error),
}

impl Failure {
    pub(super) fn retryable(&self) -> bool {
        match self {
            Failure::Fred(error) => transport(error),
            Failure::Timeout => true,
            Failure::Other(_) => false,
        }
    }

    pub(super) fn into_error(self) -> Error {
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

pub(super) fn routing(client: &RedisClient) -> Option<ClusterRouting> {
    if client.is_clustered() {
        client.cached_cluster_state()
    } else {
        None
    }
}

pub(super) fn locate(
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

pub(super) fn hashed(slot: Option<u16>, fallback: ClusterHash) -> ClusterHash {
    slot.map_or(fallback, ClusterHash::Custom)
}

/// A connection that only one batch uses at a time, like a connection Redisson takes from its pool for a transaction. It goes back to the client's idle list when the batch finished cleanly and is closed otherwise.
pub(super) struct Lease {
    pub(super) core: Arc<Core>,
    pub(super) client: RedisClient,
    pub(super) reusable: bool,
}

impl Lease {
    pub(super) async fn take(core: &Arc<Core>) -> std::result::Result<Self, Failure> {
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
        if let Err(error) = core.name(&client).await {
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

pub(super) enum Role {
    Multi,
    Op(usize),
    Exec(usize),
    Wait,
}

pub(super) struct Group {
    pub(super) server: Option<Server>,
    pub(super) slot: Option<u16>,
    pub(super) ops: Vec<usize>,
}

/// `MULTI`/`EXEC` transactions on a leased connection, one per node, as in Redisson's atomic modes. Each command is handed to the connection as it is added, in order, and the answers are read at the end, so the transactions cost one round trip however long they are.
pub(super) struct Session {
    pub(super) lease: Lease,
    pub(super) routing: Option<ClusterRouting>,
    pub(super) groups: Vec<Group>,
    pub(super) pending: Vec<(Role, Sending, Option<FredResult<Resp3Frame>>)>,
    pub(super) ops: usize,
}

impl Session {
    pub(super) async fn begin(core: &Arc<Core>) -> std::result::Result<Self, Failure> {
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

    pub(super) fn enqueue(
        &mut self,
        role: Role,
        name: &'static str,
        args: Vec<Value>,
        hash: ClusterHash,
    ) {
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

    pub(super) fn send(&mut self, name: &'static str, args: Vec<Value>, key_offset: usize) {
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

    pub(super) async fn finish(mut self, replication: Replication) -> Finished {
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

pub(super) enum Command {
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

pub(super) struct Stored {
    pub(super) sender: mpsc::UnboundedSender<Command>,
}

impl Stored {
    pub(super) fn start(core: Arc<Core>) -> Option<Self> {
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
