//! The background task of a local cache: the sync messages, the expiry notifications and the reconnections.

use super::local::Local;
use super::messages::{now_millis, LOG_ID_LEN, UPDATES_LOG_TIME};
use super::options::ReconnectionStrategy;
use crate::client::core::Core;
use crate::client::request::Request;
use crate::collections::hash_map_cache::entry_events::{split_event, LengthPrefix};
use crate::error::Result;
use crate::pubsub::{PatternMessage, Subscription};
use bytes::Bytes;
use std::sync::atomic::Ordering;
use std::sync::Weak;
use tokio::sync::broadcast::{self, error::RecvError};

pub(crate) const EXPIRED_EVENTS: &str = "__keyevent@*:expired";

/// The expiry notifications the listener reads, by [`ExpirationEventPolicy`](crate::ExpirationEventPolicy). The subscriptions are held for their `Drop`, which unsubscribes.
#[allow(dead_code)]
pub(crate) enum ExpiryFeed {
    Pattern(
        Subscription<PatternMessage>,
        broadcast::Receiver<PatternMessage>,
    ),
    Channel(Subscription, broadcast::Receiver<Bytes>),
    None,
}

/// What one expiry notification means for the local cache.
enum Expiry {
    /// The map expired, or messages were lost: clear the cache.
    Clear,
    /// Another key or event.
    Ignore,
    /// The feed ended.
    Closed,
}

impl ExpiryFeed {
    /// The next notification; never returns once the feed is [`ExpiryFeed::None`].
    async fn next(&mut self, key: &[u8]) -> Expiry {
        let received = match self {
            ExpiryFeed::Pattern(_, messages) => messages.recv().await.map(|(_, name)| name == key),
            ExpiryFeed::Channel(_, messages) => {
                messages.recv().await.map(|event| &event[..] == b"expired")
            }
            ExpiryFeed::None => std::future::pending().await,
        };
        match received {
            Ok(true) | Err(RecvError::Lagged(_)) => Expiry::Clear,
            Ok(false) => Expiry::Ignore,
            Err(RecvError::Closed) => Expiry::Closed,
        }
    }
}

/// The entry events of a `HashMapCache` the server publishes when it drops entries (the removed and expired channels): each forgets the key it names, whatever the sync strategy, because the server's truth wins. The subscriptions are held for their `Drop`, which unsubscribes.
pub(crate) struct EvictionFeed {
    #[allow(dead_code)]
    subscriptions: std::vec::Vec<Subscription>,
    receivers: std::vec::Vec<broadcast::Receiver<Bytes>>,
    prefix: LengthPrefix,
}

/// What one eviction message means for the local cache.
pub(crate) enum Eviction {
    /// Forget this encoded key.
    Key(Bytes),
    /// A message did not parse or messages were lost: clear the cache.
    Clear,
    /// The feed ended.
    Closed,
}

impl EvictionFeed {
    pub(crate) fn new(
        subscriptions: std::vec::Vec<Subscription>,
        receivers: std::vec::Vec<broadcast::Receiver<Bytes>>,
        prefix: LengthPrefix,
    ) -> Self {
        Self {
            subscriptions,
            receivers,
            prefix,
        }
    }

    /// The next message of any channel; never returns without channels.
    pub(crate) async fn next(&mut self) -> Eviction {
        if self.receivers.is_empty() {
            return std::future::pending().await;
        }
        let received =
            futures::future::select_all(self.receivers.iter_mut().map(|r| Box::pin(r.recv())))
                .await
                .0;
        match received {
            Ok(payload) => match split_event(&payload, self.prefix) {
                Some((key, _, _)) => Eviction::Key(Bytes::copy_from_slice(key)),
                None => Eviction::Clear,
            },
            Err(RecvError::Lagged(_)) => Eviction::Clear,
            Err(RecvError::Closed) => Eviction::Closed,
        }
    }
}

async fn next_eviction(feed: &mut Option<EvictionFeed>) -> Eviction {
    match feed {
        Some(feed) => feed.next().await,
        None => std::future::pending().await,
    }
}

/// After a reconnect with [`ReconnectionStrategy::Load`]: forgets the keys the updates log `keys[1]` records since the last message, or everything when that is too long ago or the watched key `keys[0]` is gone.
pub(crate) async fn load_after_reconnection(local: &Local, core: Weak<Core>, keys: &[String]) {
    let since = local.last_invalidate.load(Ordering::Acquire);
    if since == 0 {
        return;
    }
    if now_millis() - since > UPDATES_LOG_TIME.as_millis() as i64 {
        local.forget_all();
        return;
    }
    let Some(core) = core.upgrade() else {
        return;
    };
    let mut cmd = redis::cmd("EXISTS");
    cmd.arg(&keys[0]);
    let exists: Result<i64> = core.query(Request::new(cmd).key(keys[0].as_bytes())).await;
    match exists {
        Ok(0) => local.forget_all(),
        Ok(_) => {
            let mut cmd = redis::cmd("ZRANGEBYSCORE");
            cmd.arg(&keys[1]).arg(since).arg("+inf");
            let changed: Result<std::vec::Vec<Bytes>> =
                core.query(Request::new(cmd).key(keys[1].as_bytes())).await;
            match changed {
                Ok(changed) => changed
                    .iter()
                    .filter(|entry| entry.len() >= LOG_ID_LEN)
                    .for_each(|entry| local.forget(&entry.slice(LOG_ID_LEN..))),
                Err(_) => local.forget_all(),
            }
        }
        Err(_) => local.forget_all(),
    }
}

async fn next<T: Clone>(
    receiver: &mut Option<broadcast::Receiver<T>>,
) -> std::result::Result<T, RecvError> {
    match receiver {
        Some(receiver) => receiver.recv().await,
        None => std::future::pending().await,
    }
}

/// Runs until the last handle of `local` is gone. `keys` are the watched key and the updates log.
pub(crate) async fn listen(
    local: Weak<Local>,
    core: Weak<Core>,
    keys: std::vec::Vec<String>,
    sync: (Option<Subscription>, Option<broadcast::Receiver<Bytes>>),
    mut expiry: ExpiryFeed,
    mut evictions: Option<EvictionFeed>,
    mut reconnects: broadcast::Receiver<()>,
) {
    // The subscription is held for its `Drop`, which unsubscribes.
    let (_subscription, mut messages) = sync;
    loop {
        tokio::select! {
            received = next(&mut messages) => {
                let Some(local) = local.upgrade() else { return };
                match received {
                    Ok(payload) => local.apply(&payload),
                    Err(RecvError::Lagged(_)) => local.forget_all(),
                    Err(RecvError::Closed) => return,
                }
            }
            received = expiry.next(keys[0].as_bytes()) => {
                let Some(local) = local.upgrade() else { return };
                match received {
                    Expiry::Clear => local.forget_all(),
                    Expiry::Ignore => {}
                    Expiry::Closed => expiry = ExpiryFeed::None,
                }
            }
            evicted = next_eviction(&mut evictions) => {
                let Some(local) = local.upgrade() else { return };
                match evicted {
                    Eviction::Key(key) => local.forget(&key),
                    Eviction::Clear => local.forget_all(),
                    Eviction::Closed => evictions = None,
                }
            }
            reconnected = reconnects.recv() => {
                let Some(local) = local.upgrade() else { return };
                if matches!(reconnected, Err(RecvError::Closed)) {
                    return;
                }
                // Entries the server dropped meanwhile are not in the updates log, so a cache that follows server-side drops starts over.
                if evictions.is_some() {
                    local.forget_all();
                    continue;
                }
                match local.reconnection {
                    ReconnectionStrategy::None => {}
                    ReconnectionStrategy::Clear => local.forget_all(),
                    ReconnectionStrategy::Load => {
                        load_after_reconnection(&local, core.clone(), &keys).await
                    }
                }
            }
        }
    }
}
