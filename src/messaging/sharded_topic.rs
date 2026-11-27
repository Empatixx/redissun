use crate::client::request::Request;
use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::net::node::{Failure, Node, NodeSettings};
use crate::net::topology::Topology;
use crate::object::Key;
use bytes::Bytes;
use futures::{stream, Stream};
use redis::{PushInfo, PushKind, Value};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

const MESSAGE_BUFFER: usize = 256;
const RESUBSCRIBE_PAUSE: Duration = Duration::from_secs(1);

/// The shard channels a client listens to, by name, so that the subscribers of one channel share its connection.
#[derive(Default)]
pub(crate) struct Registry {
    channels: Mutex<HashMap<String, Weak<Channel>>>,
}

/// One shard channel: its own RESP3 connection to the master that serves the channel's slot, and the task that reads it.
///
/// redis-rs sends `SSUBSCRIBE` as a plain command, whose confirmation is its reply, but cannot match the confirmation of a plain `SUNSUBSCRIBE` to its request. So a channel has a connection of its own, and it unsubscribes by closing it.
struct Channel {
    name: String,
    /// The messages; `None` ends the subscribers, after `unsubscribe_all`.
    messages: broadcast::Sender<Option<Bytes>>,
    task: JoinHandle<()>,
    node: Arc<Mutex<Option<Arc<Node>>>>,
    core: Weak<Core>,
}

type Core = crate::client::core::Core;

impl Drop for Channel {
    fn drop(&mut self) {
        self.task.abort();
        if let Some(core) = self.core.upgrade() {
            let mut channels = lock(&core.sharded.channels);
            if channels
                .get(&self.name)
                .is_some_and(|channel| channel.strong_count() == 0)
            {
                channels.remove(&self.name);
            }
        }
        let node = self.node.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let (Some(node), Ok(runtime)) = (node, tokio::runtime::Handle::try_current()) {
            runtime.spawn(async move { node.reset().await });
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// A connection to the master that serves `channel`, subscribed to it. A `MOVED` reply reads the slot map again and tries the node it names.
async fn open(
    topology: &Topology,
    channel: &str,
) -> Result<(Arc<Node>, mpsc::UnboundedReceiver<PushInfo>)> {
    let mut attempts = 0;
    loop {
        let pool = topology
            .pool_for(Some(channel.as_bytes()))
            .await
            .map_err(Error::from)?;
        let (push, pushes) = mpsc::unbounded_channel();
        let mut settings = NodeSettings::clone(topology.settings());
        settings.resp3 = true;
        settings.push = Some(push);
        let node = Arc::new(Node::new(pool.address().clone(), Arc::new(settings)));
        let mut cmd = redis::cmd("SSUBSCRIBE");
        cmd.arg(channel);
        match node.send(&cmd).await {
            Ok(_) => return Ok((node, pushes)),
            Err(Failure::Redirect { .. }) if attempts < 3 => {
                attempts += 1;
                node.reset().await;
                topology.refresh().await;
            }
            Err(failure) => {
                node.reset().await;
                return Err(Error::from(failure));
            }
        }
    }
}

/// Reads the channel's connection and opens a new one when it is lost or the channel's slot moves, which Redis tells with `sunsubscribe`, as Redisson resubscribes a shard channel.
async fn run(
    topology: Arc<Topology>,
    name: String,
    messages: broadcast::Sender<Option<Bytes>>,
    node: Arc<Mutex<Option<Arc<Node>>>>,
    ready: oneshot::Sender<Result<()>>,
) {
    let mut ready = Some(ready);
    loop {
        let (connection, mut pushes) = match open(&topology, &name).await {
            Ok(opened) => opened,
            Err(error) => match ready.take() {
                Some(ready) => {
                    let _ = ready.send(Err(error));
                    return;
                }
                None => {
                    tracing::warn!("Unable to resubscribe to '{name}' shard channel: {error}");
                    tokio::time::sleep(RESUBSCRIBE_PAUSE).await;
                    topology.refresh().await;
                    continue;
                }
            },
        };
        let address = connection.address().to_string();
        let previous = lock(&node).replace(connection);
        if let Some(previous) = previous {
            previous.reset().await;
        }
        match ready.take() {
            Some(ready) => {
                if ready.send(Ok(())).is_err() {
                    return;
                }
            }
            None => tracing::info!(
                "listeners of '{name}' shard channel have been resubscribed to '{address}'"
            ),
        }
        while let Some(push) = pushes.recv().await {
            match push.kind {
                PushKind::SMessage => {
                    let channel = push.data.first().and_then(crate::reply::text);
                    let payload = push.data.get(1).and_then(crate::reply::bytes);
                    if let (Some(channel), Some(payload)) = (channel, payload) {
                        if channel == name {
                            let _ = messages.send(Some(payload));
                        }
                    }
                }
                PushKind::Disconnection | PushKind::SUnsubscribe => break,
                _ => {}
            }
        }
        topology.refresh().await;
    }
}

impl Registry {
    async fn join(&self, core: &Arc<Core>, name: &str) -> Result<Arc<Channel>> {
        if let Some(channel) = lock(&self.channels).get(name).and_then(Weak::upgrade) {
            return Ok(channel);
        }
        let (ready, started) = oneshot::channel();
        let (messages, _) = broadcast::channel(MESSAGE_BUFFER);
        let node = Arc::new(Mutex::new(None));
        let task = tokio::spawn(run(
            core.topology().clone(),
            name.to_string(),
            messages.clone(),
            node.clone(),
            ready,
        ));
        let channel = Arc::new(Channel {
            name: name.to_string(),
            messages,
            task,
            node,
            core: Arc::downgrade(core),
        });
        started
            .await
            .map_err(|_| Error::Redis("the subscription was abandoned".into()))??;
        let mut channels = lock(&self.channels);
        // Another subscriber may have opened the channel meanwhile; the first one stays and this connection closes.
        if let Some(existing) = channels.get(name).and_then(Weak::upgrade) {
            return Ok(existing);
        }
        channels.insert(name.to_string(), Arc::downgrade(&channel));
        Ok(channel)
    }

    fn listeners(&self, name: &str) -> usize {
        lock(&self.channels)
            .get(name)
            .map_or(0, |channel| channel.strong_count())
    }
}

/// A shard channel of Redis 7 (`SPUBLISH` and `SSUBSCRIBE`), Redisson's `RShardedTopic`.
///
/// In Redis Cluster a message of a shard channel goes only to the master that serves the channel's slot and its replicas, not to every node as with [`Topic`](crate::Topic), so publishing scales with the cluster. Without a cluster it works like a [`Topic`](crate::Topic). Each channel that a client listens to has a connection of its own to that master; it moves to the new master after a failover or when the slot moves.
///
/// ```no_run
/// # async fn run(client: redissun::Client) -> redissun::Result<()> {
/// let orders = client.sharded_topic::<u64>("orders:eu");
/// let mut subscriber = orders.subscribe().await?;
/// orders.publish(&42).await?;
/// assert_eq!(subscriber.recv().await?, 42);
/// # Ok(())
/// # }
/// ```
pub struct ShardedTopic<M, C: Codec> {
    key: Key,
    codec: C,
    _marker: PhantomData<fn() -> M>,
}

impl<M, C: Codec> Clone for ShardedTopic<M, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
            _marker: PhantomData,
        }
    }
}

impl<M, C: Codec> fmt::Debug for ShardedTopic<M, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "ShardedTopic")
    }
}

impl<M, C: Codec> ShardedTopic<M, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self {
            key,
            codec,
            _marker: PhantomData,
        }
    }

    /// The name of the topic and of its shard channel.
    pub fn name(&self) -> &str {
        self.key.name()
    }

    /// Number of [`ShardedSubscriber`]s of this topic in this client, Redisson's `countListeners`.
    #[doc(alias = "countListeners")]
    pub async fn local_subscriber_count(&self) -> usize {
        self.key.core.sharded.listeners(self.key.name())
    }

    /// Ends every [`ShardedSubscriber`] of this topic in this client and unsubscribes, Redisson's `removeAllListeners`. Their `recv` fails once the messages already received are read.
    #[doc(alias = "removeAllListeners")]
    pub async fn unsubscribe_all(&self) {
        let channel = lock(&self.key.core.sharded.channels)
            .remove(self.key.name())
            .and_then(|channel| channel.upgrade());
        if let Some(channel) = channel {
            channel.task.abort();
            let node = lock(&channel.node).take();
            if let (Some(node), Ok(runtime)) = (node, tokio::runtime::Handle::try_current()) {
                runtime.spawn(async move { node.reset().await });
            }
            // The subscribers end once they read what they have.
            let _ = channel.messages.send(None);
        }
    }
}

impl<M, C> ShardedTopic<M, C>
where
    M: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    /// Sends a message (`SPUBLISH`); returns how many connections received it, Redisson's `publish`.
    pub async fn publish<Q>(&self, message: &Q) -> Result<usize>
    where
        M: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let channel = self.key.redis_key();
        let mut cmd = redis::cmd("SPUBLISH");
        cmd.arg(&channel).arg(&self.codec.encode(message)?[..]);
        self.key
            .core
            .query(Request::new(cmd).key(channel.as_bytes()))
            .await
    }

    /// Starts receiving (`SSUBSCRIBE`), Redisson's `addListener`. The subscriber gets the messages published after this call returns. Dropping the last subscriber of the topic in this client unsubscribes. Fails on a server without shard channels, before Redis 7.0.
    pub async fn subscribe(&self) -> Result<ShardedSubscriber<M, C>> {
        let channel = self
            .key
            .core
            .sharded
            .join(&self.key.core, self.key.name())
            .await?;
        Ok(ShardedSubscriber {
            receiver: channel.messages.subscribe(),
            _channel: channel,
            codec: self.codec.clone(),
            _marker: PhantomData,
        })
    }

    /// Number of connections subscribed to the channel across all programs (`PUBSUB SHARDNUMSUB`), Redisson's `countSubscribers`.
    pub async fn subscriber_count(&self) -> Result<usize> {
        let channel = self.key.redis_key();
        let mut cmd = redis::cmd("PUBSUB");
        cmd.arg("SHARDNUMSUB").arg(&channel);
        let reply: Value = self
            .key
            .core
            .query(Request::new(cmd).key(channel.as_bytes()))
            .await?;
        let counts: HashMap<String, usize> = redis::from_redis_value(reply)?;
        Ok(counts.get(&channel).copied().unwrap_or(0))
    }
}

/// Receives the messages of a [`ShardedTopic`].
pub struct ShardedSubscriber<M, C: Codec> {
    receiver: broadcast::Receiver<Option<Bytes>>,
    /// Keeps the channel subscribed while the subscriber lives.
    _channel: Arc<Channel>,
    codec: C,
    _marker: PhantomData<fn() -> M>,
}

impl<M, C: Codec> fmt::Debug for ShardedSubscriber<M, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardedSubscriber").finish_non_exhaustive()
    }
}

impl<M, C> ShardedSubscriber<M, C>
where
    M: DeserializeOwned + Send,
    C: Codec,
{
    /// Waits for the next message. A subscriber that falls more than 256 messages behind gets [`Error::Lagged`] once and then continues with the newest messages, as for a [`Subscriber`](crate::Subscriber).
    pub async fn recv(&mut self) -> Result<M> {
        match self.receiver.recv().await {
            Ok(Some(payload)) => self.codec.decode(&payload),
            Err(RecvError::Lagged(missed)) => {
                Err(Error::Lagged(usize::try_from(missed).unwrap_or(usize::MAX)))
            }
            Ok(None) | Err(RecvError::Closed) => {
                Err(Error::Redis("the subscription was closed".into()))
            }
        }
    }

    /// Turns the subscriber into a stream of messages. The stream never ends.
    pub fn into_stream(self) -> impl Stream<Item = Result<M>> {
        stream::unfold(self, |mut subscriber| async move {
            Some((subscriber.recv().await, subscriber))
        })
    }
}
