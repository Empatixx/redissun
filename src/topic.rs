use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::object::Key;
use crate::pubsub::{PatternMessage, Subscription};
use bytes::Bytes;
use fred::interfaces::PubsubInterface;
use futures::{stream, Stream};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use tokio::sync::broadcast::{self, error::RecvError};

/// A pub/sub channel, as in Redisson's `RTopic`. The Redis channel has the same name as the topic, so programs that do not use redissun can publish and subscribe too.
pub struct Topic<M, C: Codec> {
    key: Key,
    codec: C,
    _marker: PhantomData<fn() -> M>,
}

impl<M, C: Codec> Clone for Topic<M, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
            _marker: PhantomData,
        }
    }
}

impl<M, C: Codec> fmt::Debug for Topic<M, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "Topic")
    }
}

impl<M, C: Codec> Topic<M, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self {
            key,
            codec,
            _marker: PhantomData,
        }
    }

    /// The name of the topic and of its Redis channel.
    pub fn name(&self) -> &str {
        self.key.name()
    }

    /// Number of [`Subscriber`]s of this topic in this client, like Redisson's `countListeners`.
    pub async fn listener_count(&self) -> usize {
        match self.key.core.pubsub_started() {
            Some(pubsub) => pubsub.listeners::<Bytes>(&self.key.redis_key()).await,
            None => 0,
        }
    }

    /// Ends every [`Subscriber`] of this topic in this client and unsubscribes from Redis, like Redisson's `removeAllListeners`. Their `recv` fails once the messages already received are read.
    pub async fn remove_all_listeners(&self) {
        if let Some(pubsub) = self.key.core.pubsub_started() {
            pubsub.close::<Bytes>(&self.key.redis_key()).await;
        }
    }
}

impl<M, C> Topic<M, C>
where
    M: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    /// Sends a message. The message is borrowed: `topic.publish("hello")`.
    /// Returns how many Redis connections received it. All subscribers of one client share one connection.
    pub async fn publish<Q>(&self, message: &Q) -> Result<usize>
    where
        M: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let receivers: usize = self
            .key
            .core
            .redis()
            .publish(self.key.redis_key(), self.codec.encode(message)?)
            .await?;
        Ok(receivers)
    }

    /// Starts receiving, like adding a listener in Redisson. The subscriber gets the messages published after this call returns. Dropping it removes the listener.
    pub async fn subscribe(&self) -> Result<Subscriber<M, C>> {
        let (subscription, receiver) = self
            .key
            .core
            .pubsub()
            .await?
            .subscribe_with_messages(&self.key.redis_key())
            .await?;
        Ok(Subscriber {
            lost: subscription.lost(),
            _subscription: subscription,
            receiver,
            codec: self.codec.clone(),
            _marker: PhantomData,
        })
    }

    /// Number of Redis connections subscribed to the topic, across all programs, like Redisson's `countSubscribers`.
    pub async fn subscriber_count(&self) -> Result<usize> {
        let channel = self.key.redis_key();
        let counts: HashMap<String, usize> =
            self.key.core.redis().pubsub_numsub(channel.clone()).await?;
        Ok(counts.get(&channel).copied().unwrap_or(0))
    }
}

async fn next_payload<T: Clone>(
    receiver: &mut broadcast::Receiver<T>,
    lost: &mut broadcast::Receiver<usize>,
) -> Result<T> {
    loop {
        tokio::select! {
            biased;
            missed = lost.recv() => match missed {
                Ok(missed) => return Err(Error::Lagged(missed)),
                Err(RecvError::Lagged(missed)) => {
                    return Err(Error::Lagged(usize::try_from(missed).unwrap_or(usize::MAX)))
                }
                Err(RecvError::Closed) => continue,
            },
            received = receiver.recv() => return match received {
                Ok(payload) => Ok(payload),
                Err(RecvError::Lagged(missed)) => {
                    Err(Error::Lagged(usize::try_from(missed).unwrap_or(usize::MAX)))
                }
                Err(RecvError::Closed) => Err(Error::Redis("the subscription was closed".into())),
            },
        }
    }
}

/// Receives the messages of a [`Topic`]. Dropping the last subscriber of a topic in a client unsubscribes from Redis.
pub struct Subscriber<M, C: Codec> {
    _subscription: Subscription,
    receiver: broadcast::Receiver<Bytes>,
    lost: broadcast::Receiver<usize>,
    codec: C,
    _marker: PhantomData<fn() -> M>,
}

impl<M, C: Codec> fmt::Debug for Subscriber<M, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscriber").finish_non_exhaustive()
    }
}

impl<M, C> Subscriber<M, C>
where
    M: DeserializeOwned + Send,
    C: Codec,
{
    /// Waits for the next message. A subscriber that falls more than 256 messages behind, or whose connection dropped messages, gets [`Error::Lagged`] once and then continues with the newest messages.
    pub async fn recv(&mut self) -> Result<M> {
        let payload = next_payload(&mut self.receiver, &mut self.lost).await?;
        self.codec.decode(&payload)
    }

    /// Turns the subscriber into a stream of messages. The stream never ends.
    pub fn into_stream(self) -> impl Stream<Item = Result<M>> {
        stream::unfold(self, |mut subscriber| async move {
            Some((subscriber.recv().await, subscriber))
        })
    }
}

/// A subscription to every channel whose name matches a glob pattern (`news.*`, `h?llo`, `h[ae]llo`), as in Redisson's `RPatternTopic`.
pub struct PatternTopic<M, C: Codec> {
    key: Key,
    codec: C,
    _marker: PhantomData<fn() -> M>,
}

impl<M, C: Codec> Clone for PatternTopic<M, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
            _marker: PhantomData,
        }
    }
}

impl<M, C: Codec> fmt::Debug for PatternTopic<M, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "PatternTopic")
    }
}

impl<M, C: Codec> PatternTopic<M, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self {
            key,
            codec,
            _marker: PhantomData,
        }
    }

    /// The channel pattern.
    pub fn pattern(&self) -> &str {
        self.key.name()
    }

    /// Number of [`PatternSubscriber`]s of this pattern in this client.
    pub async fn listener_count(&self) -> usize {
        match self.key.core.pubsub_started() {
            Some(pubsub) => {
                pubsub
                    .listeners::<PatternMessage>(&self.key.redis_key())
                    .await
            }
            None => 0,
        }
    }

    /// Ends every [`PatternSubscriber`] of this pattern in this client and unsubscribes from Redis, like Redisson's `removeAllListeners`.
    pub async fn remove_all_listeners(&self) {
        if let Some(pubsub) = self.key.core.pubsub_started() {
            pubsub.close::<PatternMessage>(&self.key.redis_key()).await;
        }
    }
}

impl<M, C> PatternTopic<M, C>
where
    M: DeserializeOwned + Send,
    C: Codec,
{
    /// Starts receiving from every matching channel. The subscriber gets the messages published after this call returns.
    pub async fn subscribe(&self) -> Result<PatternSubscriber<M, C>> {
        let (subscription, receiver) = self
            .key
            .core
            .pubsub()
            .await?
            .psubscribe_with_messages(&self.key.redis_key())
            .await?;
        Ok(PatternSubscriber {
            lost: subscription.lost(),
            _subscription: subscription,
            receiver,
            codec: self.codec.clone(),
            _marker: PhantomData,
        })
    }
}

/// Receives the messages of a [`PatternTopic`] together with the channel each one was published to. Dropping the last subscriber of a pattern in a client unsubscribes from Redis.
pub struct PatternSubscriber<M, C: Codec> {
    _subscription: Subscription<PatternMessage>,
    receiver: broadcast::Receiver<PatternMessage>,
    lost: broadcast::Receiver<usize>,
    codec: C,
    _marker: PhantomData<fn() -> M>,
}

impl<M, C: Codec> fmt::Debug for PatternSubscriber<M, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PatternSubscriber").finish_non_exhaustive()
    }
}

impl<M, C> PatternSubscriber<M, C>
where
    M: DeserializeOwned + Send,
    C: Codec,
{
    /// Waits for the next message and returns the channel it was published to with it. Falling behind gives [`Error::Lagged`] as for [`Subscriber::recv`].
    pub async fn recv(&mut self) -> Result<(String, M)> {
        let (channel, payload) = next_payload(&mut self.receiver, &mut self.lost).await?;
        Ok((channel, self.codec.decode(&payload)?))
    }

    /// Turns the subscriber into a stream of `(channel, message)` pairs. The stream never ends.
    pub fn into_stream(self) -> impl Stream<Item = Result<(String, M)>> {
        stream::unfold(self, |mut subscriber| async move {
            Some((subscriber.recv().await, subscriber))
        })
    }
}
