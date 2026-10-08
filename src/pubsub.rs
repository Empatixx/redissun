use crate::error::Result;
use fred::clients::Client as RedisClient;
use fred::interfaces::{ClientLike, EventInterface, PubsubInterface};
use fred::types::Message;
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, Mutex, Notify};

pub(crate) struct PubSub {
    client: RedisClient,
    channels: Mutex<HashMap<String, Arc<Notify>>>,
}

impl PubSub {
    pub(crate) async fn start(base: &RedisClient) -> Result<Arc<Self>> {
        let client = base.clone_new();
        client.init().await?;
        let receiver = client.message_rx();
        let pubsub = Arc::new(Self {
            client,
            channels: Mutex::new(HashMap::new()),
        });
        tokio::spawn(dispatch(Arc::downgrade(&pubsub), receiver));
        Ok(pubsub)
    }

    pub(crate) async fn quit(&self) {
        let _ = self.client.quit().await;
    }

    pub(crate) async fn subscribe(&self, channel: &str) -> Result<Arc<Notify>> {
        let mut channels = self.channels.lock().await;
        if let Some(notify) = channels.get(channel) {
            return Ok(notify.clone());
        }
        self.client.subscribe(channel).await?;
        let notify = Arc::new(Notify::new());
        channels.insert(channel.to_string(), notify.clone());
        Ok(notify)
    }
}

async fn dispatch(pubsub: Weak<PubSub>, mut receiver: broadcast::Receiver<Message>) {
    loop {
        let message = receiver.recv().await;
        let Some(pubsub) = pubsub.upgrade() else {
            break;
        };
        let channels = pubsub.channels.lock().await;
        match message {
            Ok(message) => {
                if let Some(notify) = channels.get(&message.channel.to_string()) {
                    notify.notify_waiters();
                }
            }
            Err(RecvError::Lagged(_)) => channels.values().for_each(|n| n.notify_waiters()),
            Err(RecvError::Closed) => break,
        }
    }
}
