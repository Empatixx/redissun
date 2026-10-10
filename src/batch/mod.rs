mod engine;
mod views;

pub use engine::{BatchFuture, BatchResult};
pub use views::{
    BatchAtomicI64, BatchBucket, BatchHashMap, BatchHashSet, BatchSortedSet, BatchTopic, BatchVec,
    BatchVecDeque,
};

use crate::client::core::Core;
use crate::client::retry::DelayStrategy;
use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::object::millis as to_millis;
use crate::reply::{bytes, int, malformed, number};
use bytes::Bytes;
use engine::*;
use fred::clients::Pipeline;
use fred::interfaces::ClientLike;
use fred::prelude::Options;
use fred::types::config::Server;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::de::DeserializeOwned;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

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

fn synced_from(reply: Option<Value>) -> usize {
    match reply {
        Some(Value::Array(items)) => items.get(1).and_then(number).unwrap_or(0).max(0) as usize,
        Some(other) => number(&other).unwrap_or(0).max(0) as usize,
        None => 0,
    }
}
