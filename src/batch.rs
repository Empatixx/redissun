use crate::codec::Codec;
use crate::core::Core;
use crate::error::{Error, Result};
use crate::object::{millis as to_millis, tagged};
use crate::reply::{array, bytes, int, malformed, number, text};
use bytes::Bytes;
use fred::interfaces::{ClientLike, TransactionInterface};
use fred::types::{ClusterHash, CustomCommand, Value};
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
use tokio::sync::oneshot;

type Complete = Box<dyn FnOnce(Result<Value>) + Send>;

struct Op {
    name: &'static str,
    args: Vec<Value>,
    key_offset: usize,
    complete: Complete,
}

/// The reply to one command of a [`Batch`]. Await it after [`Batch::execute`]. Awaiting it earlier waits for the execution that you have not started.
///
/// If the batch is dropped without being executed, or if it skips results, the handle resolves to `Error::Config`.
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
                "the batch was dropped or skips its results".into(),
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}

enum Mode {
    Pipeline,
    Atomic,
}

/// Several commands that are sent to Redis together, as in Redisson's `RBatch`.
///
/// Get the objects from the batch (`batch.hash_map(..)`, `batch.bucket(..)`, ...). Their methods do not talk to Redis: they queue a command and return a [`BatchFuture`]. [`execute`](Batch::execute) then sends everything in one round trip and fills the handles. Commands run in the order they were queued.
///
/// By default the commands are pipelined, so other clients' commands can run between them and one failing command does not stop the rest. [`atomic`](Batch::atomic) wraps them in `MULTI`/`EXEC` instead. [`skip_result`](Batch::skip_result) drops the replies, which saves bandwidth for writes.
///
/// Only the methods that make sense without waiting are available on the batch objects. Operations that wait, lock or need several steps are not.
#[must_use = "a batch does nothing until execute() is awaited"]
pub struct Batch<C: Codec> {
    core: Arc<Core>,
    codec: C,
    ops: Mutex<Vec<Op>>,
    mode: Mode,
    skip_result: bool,
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
            mode: Mode::Pipeline,
            skip_result: false,
        }
    }

    /// Runs the commands in one `MULTI`/`EXEC` transaction, so no other command runs between them.
    pub fn atomic(mut self) -> Self {
        self.mode = Mode::Atomic;
        self
    }

    /// Does not return the replies. The handles then resolve to `Error::Config`.
    pub fn skip_result(mut self) -> Self {
        self.skip_result = true;
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
        match args {
            Ok(args) => {
                let complete: Complete = Box::new(move |reply| {
                    let _ = sender.send(reply.and_then(|value| decode(&value)));
                });
                self.ops.lock().unwrap_or_else(|e| e.into_inner()).push(Op {
                    name,
                    args,
                    key_offset,
                    complete,
                });
            }
            Err(error) => {
                let _ = sender.send(Err(error));
            }
        }
        BatchFuture { receiver }
    }

    /// Sends every queued command and fills the handles. Returns how many commands were sent.
    ///
    /// A command that Redis rejects fails only its own handle. The call itself fails when the connection fails, and then the handles fail too.
    pub async fn execute(self) -> Result<usize> {
        let ops = std::mem::take(&mut *self.ops.lock().unwrap_or_else(|e| e.into_inner()));
        let count = ops.len();
        if count == 0 {
            return Ok(0);
        }
        let commands: Vec<(CustomCommand, Vec<Value>)> = ops
            .iter()
            .map(|op| {
                (
                    CustomCommand::new_static(op.name, ClusterHash::Offset(op.key_offset), false),
                    op.args.clone(),
                )
            })
            .collect();
        let replies = match self.mode {
            Mode::Pipeline => self.run_pipeline(commands).await,
            Mode::Atomic => self.run_transaction(commands).await,
        };
        match replies {
            Ok(replies) => {
                let mut replies = replies.into_iter();
                for op in ops {
                    let reply = replies.next().unwrap_or_else(|| Err(malformed()));
                    if self.skip_result {
                        drop(op.complete);
                    } else {
                        (op.complete)(reply);
                    }
                }
                Ok(count)
            }
            Err(error) => {
                let message = error.to_string();
                for op in ops {
                    (op.complete)(Err(Error::Redis(message.clone())));
                }
                Err(error)
            }
        }
    }

    async fn run_pipeline(
        &self,
        commands: Vec<(CustomCommand, Vec<Value>)>,
    ) -> Result<Vec<Result<Value>>> {
        let pipeline = self.core.redis().pipeline();
        for (command, args) in commands {
            let _: Value = pipeline.custom(command, args).await?;
        }
        Ok(pipeline
            .try_all::<Value>()
            .await
            .into_iter()
            .map(|reply| reply.map_err(Error::from))
            .collect())
    }

    async fn run_transaction(
        &self,
        commands: Vec<(CustomCommand, Vec<Value>)>,
    ) -> Result<Vec<Result<Value>>> {
        let transaction = self.core.redis().multi();
        for (command, args) in commands {
            let _: Value = transaction.custom(command, args).await?;
        }
        let reply: Value = transaction.exec(false).await?;
        Ok(array(&reply)
            .iter()
            .map(|value| Ok(value.clone()))
            .collect())
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

#[allow(dead_code)]
fn unused(_: &str) -> String {
    tagged("")
}
