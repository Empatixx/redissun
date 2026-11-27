//! The local cache that [`LocalCachedMap`](crate::LocalCachedMap) keeps in this program: the store, the sync messages between instances, the listener and the reconnection handling.

mod listener;
pub(crate) mod local;
pub(crate) mod messages;
pub(crate) mod options;
pub(crate) mod store;

use crate::error::Result;
use crate::object::Key;
use bytes::Bytes;
pub(crate) use listener::EvictionFeed;
use listener::{listen, ExpiryFeed, EXPIRED_EVENTS};
use local::Local;
use messages::{channel, updates_log};
pub(crate) use options::{local_options, LocalOptions};
pub use options::{
    CacheProvider, EvictionPolicy, ExpirationEventPolicy, ReconnectionStrategy, StoreMode,
    SyncStrategy,
};
use std::sync::Arc;
use store::LocalStore;

/// The local cache of one object handle, with the settings that decide what its writes announce.
pub(crate) struct LocalCache {
    pub(crate) local: Arc<Local>,
    pub(crate) sync: SyncStrategy,
    pub(crate) reconnection: ReconnectionStrategy,
    pub(crate) store_cache_miss: bool,
    pub(crate) store_mode: StoreMode,
    /// The channel of the sync messages.
    pub(crate) channel: String,
    /// The sorted set of [`ReconnectionStrategy::Load`].
    pub(crate) updates_log: String,
}

impl LocalCache {
    /// The scripts' `mode`: `0` announces nothing, `1` publishes, `2` also writes the updates log.
    pub(crate) fn mode(&self) -> Bytes {
        Bytes::from(if self.reconnection == ReconnectionStrategy::Load {
            "2"
        } else if self.sync != SyncStrategy::None {
            "1"
        } else {
            "0"
        })
    }

    /// Whether a change is announced: the scripts' `mode != "0"`.
    pub(crate) fn announces(&self) -> bool {
        self.sync != SyncStrategy::None || self.reconnection == ReconnectionStrategy::Load
    }

    /// Whether the entries live in the local cache only.
    pub(crate) fn local_only(&self) -> bool {
        self.store_mode == StoreMode::Local
    }

    /// Publishes a sync message on the channel, routed by the channel's slot, as fred did.
    pub(crate) async fn publish(&self, key: &Key, payload: Bytes) -> Result<()> {
        let mut cmd = redis::cmd("PUBLISH");
        cmd.arg(&self.channel).arg(&payload[..]);
        let _: i64 = key
            .core
            .query(crate::client::request::Request::new(cmd).key(self.channel.as_bytes()))
            .await?;
        Ok(())
    }
}

/// Creates the local cache of `key` and spawns its listener: the sync messages (when anything is announced or [`ReconnectionStrategy::Load`] needs them), the expiry notifications of the Redis key `watched`, which clear the cache, and the reconnections.
pub(crate) async fn start(
    key: &Key,
    watched: String,
    options: &LocalOptions,
    evictions: Option<EvictionFeed>,
) -> Result<Arc<LocalCache>> {
    let local = Arc::new(Local::new(
        LocalStore::new(
            options.provider,
            options.eviction,
            options.cache_size,
            options.ttl,
            options.max_idle,
        ),
        options.reconnection,
        options.store_mode == StoreMode::Local,
    ));
    let channel = channel(key);
    let updates_log = updates_log(key);
    let pubsub = key.core.pubsub().await?.clone();
    let synced =
        options.sync != SyncStrategy::None || options.reconnection != ReconnectionStrategy::None;
    let (subscription, messages) = if synced {
        let (subscription, messages) = pubsub.subscribe_with_messages(&channel).await?;
        (Some(subscription), Some(messages))
    } else {
        (None, None)
    };
    let expiry = match options.expiration {
        ExpirationEventPolicy::KeyeventPattern => {
            let (subscription, messages) = pubsub.psubscribe_with_messages(EXPIRED_EVENTS).await?;
            ExpiryFeed::Pattern(subscription, messages)
        }
        ExpirationEventPolicy::KeyspaceChannel => {
            let name = format!("__keyspace@{}__:{}", key.core.database, watched);
            let (subscription, messages) = pubsub.subscribe_with_messages(&name).await?;
            ExpiryFeed::Channel(subscription, messages)
        }
        ExpirationEventPolicy::None => ExpiryFeed::None,
    };
    let reconnects = pubsub.reconnects();
    let handle = tokio::spawn(listen(
        Arc::downgrade(&local),
        Arc::downgrade(&key.core),
        vec![watched, updates_log.clone()],
        (subscription, messages),
        expiry,
        evictions,
        reconnects,
    ));
    *local.listener.lock().unwrap_or_else(|e| e.into_inner()) = Some(handle);
    Ok(Arc::new(LocalCache {
        local,
        sync: options.sync,
        reconnection: options.reconnection,
        store_cache_miss: options.store_cache_miss,
        store_mode: options.store_mode,
        channel,
        updates_log,
    }))
}
