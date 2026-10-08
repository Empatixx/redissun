use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::object::Key;
use crate::pubsub::Subscription;
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

/// A pub/sub channel. The Redis channel has the same name as the topic, so programs that do not use redissun can publish and subscribe too.
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

    /// Starts receiving. The subscriber gets the messages published after this call returns.
    pub async fn subscribe(&self) -> Result<Subscriber<M, C>> {
        let (subscription, receiver) = self
            .key
            .core
            .pubsub
            .subscribe_with_messages(&self.key.redis_key())
            .await?;
        Ok(Subscriber {
            _subscription: subscription,
            receiver,
            codec: self.codec.clone(),
            _marker: PhantomData,
        })
    }

    /// Number of Redis connections subscribed to the topic, across all programs.
    pub async fn subscriber_count(&self) -> Result<usize> {
        let counts: HashMap<String, usize> = self
            .key
            .core
            .redis()
            .pubsub_numsub(self.key.redis_key())
            .await?;
        Ok(counts.get(self.key.name()).copied().unwrap_or(0))
    }
}

/// Receives the messages of a [`Topic`]. Dropping the last subscriber of a topic in a client unsubscribes from Redis.
pub struct Subscriber<M, C: Codec> {
    _subscription: Subscription,
    receiver: broadcast::Receiver<Bytes>,
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
    /// Waits for the next message. A subscriber that falls more than 256 messages behind gets [`Error::Lagged`] once and then continues with the newest messages.
    pub async fn recv(&mut self) -> Result<M> {
        match self.receiver.recv().await {
            Ok(payload) => self.codec.decode(&payload),
            Err(RecvError::Lagged(missed)) => Err(Error::Lagged(missed as usize)),
            Err(RecvError::Closed) => Err(Error::Redis("the subscription was closed".into())),
        }
    }

    /// Turns the subscriber into a stream of messages. The stream never ends.
    pub fn into_stream(self) -> impl Stream<Item = Result<M>> {
        stream::unfold(self, |mut subscriber| async move {
            Some((subscriber.recv().await, subscriber))
        })
    }
}
