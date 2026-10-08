use crate::error::Result;
use fred::clients::Client as RedisClient;
use fred::interfaces::{ClientLike, EventInterface, PubsubInterface};
use fred::types::Message;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use tokio::runtime::Handle;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, Mutex, Notify};

struct Entry {
    notify: Notify,
    holders: AtomicUsize,
}

pub(crate) struct PubSub {
    client: RedisClient,
    channels: Mutex<HashMap<String, Arc<Entry>>>,
}

pub(crate) struct Subscription {
    pubsub: Arc<PubSub>,
    channel: String,
    entry: Arc<Entry>,
}

impl Subscription {
    pub(crate) fn notify(&self) -> &Notify {
        &self.entry.notify
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if self.entry.holders.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        let Ok(runtime) = Handle::try_current() else {
            return;
        };
        let pubsub = self.pubsub.clone();
        let channel = std::mem::take(&mut self.channel);
        runtime.spawn(async move { pubsub.release(channel).await });
    }
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

    pub(crate) async fn subscribe(self: &Arc<Self>, channel: &str) -> Result<Subscription> {
        let mut channels = self.channels.lock().await;
        let entry = match channels.get(channel) {
            Some(entry) => {
                entry.holders.fetch_add(1, Ordering::AcqRel);
                entry.clone()
            }
            None => {
                self.client.subscribe(channel).await?;
                let entry = Arc::new(Entry {
                    notify: Notify::new(),
                    holders: AtomicUsize::new(1),
                });
                channels.insert(channel.to_string(), entry.clone());
                entry
            }
        };
        Ok(Subscription {
            pubsub: self.clone(),
            channel: channel.to_string(),
            entry,
        })
    }

    async fn release(&self, channel: String) {
        let mut channels = self.channels.lock().await;
        let unused = channels
            .get(&channel)
            .is_some_and(|entry| entry.holders.load(Ordering::Acquire) == 0);
        if unused {
            channels.remove(&channel);
            let _ = self.client.unsubscribe(channel).await;
        }
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
                if let Some(entry) = channels.get(&*message.channel) {
                    entry.notify.notify_waiters();
                }
            }
            Err(RecvError::Lagged(_)) => channels
                .values()
                .for_each(|entry| entry.notify.notify_waiters()),
            Err(RecvError::Closed) => break,
        }
    }
}
