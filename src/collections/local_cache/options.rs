//! The public options of a local cache and the builder setters that set them.

use std::time::Duration;

/// How a [`LocalCachedMap`](crate::LocalCachedMap) tells the other instances about a change, as Redisson's `SyncStrategy`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SyncStrategy {
    /// Other instances drop the changed entry and read it again when needed. The default.
    #[default]
    Invalidate,
    /// Other instances receive the new value and keep it.
    Update,
    /// Nothing is sent. Instances only learn about changes when an entry expires locally.
    None,
}

/// What a [`LocalCachedMap`](crate::LocalCachedMap) does with its local cache when its pub/sub connection comes back, as Redisson's `ReconnectionStrategy`. Messages sent while it was away are lost.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReconnectionStrategy {
    /// Nothing; entries changed meanwhile stay stale until they are changed again or expire locally. The default.
    #[default]
    None,
    /// The whole local cache is cleared.
    Clear,
    /// Every write also records the changed key in the sorted set `redissun__cache_updates_log:{name}` for 10 minutes. After a reconnect the keys changed since the last message are dropped, or the whole local cache when the instance was away longer than that or the map is gone.
    Load,
}

/// How a [`LocalCachedMap`](crate::LocalCachedMap) learns that its map expired in Redis, so it clears its local cache, Redisson's `ExpirationEventPolicy` (the deprecated `useKeyEventsPattern`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ExpirationEventPolicy {
    /// `PSUBSCRIBE __keyevent@*:expired`, shared by every map of the client; the server needs `notify-keyspace-events` with `Ex`. The default.
    #[default]
    #[doc(alias = "SUBSCRIBE_WITH_KEYEVENT_PATTERN")]
    KeyeventPattern,
    /// `SUBSCRIBE __keyspace@<database>__:<name>`, one channel for this map; the server needs `notify-keyspace-events` with `Kx`.
    #[doc(alias = "SUBSCRIBE_WITH_KEYSPACE_CHANNEL")]
    KeyspaceChannel,
    /// No subscription: the local cache is not cleared when the map expires.
    #[doc(alias = "DONT_SUBSCRIBE")]
    None,
}

/// Which cache holds the local entries of a [`LocalCachedMap`](crate::LocalCachedMap), Redisson's `CacheProvider`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum CacheProvider {
    /// redissun's own cache, with every [`EvictionPolicy`]. The default.
    #[default]
    #[doc(alias = "REDISSON")]
    Builtin,
    /// A [`moka`](https://docs.rs/moka) cache, as Redisson's Caffeine one: [`ttl`](crate::LocalCachedMapBuilder::ttl) is moka's time to live, [`max_idle`](crate::LocalCachedMapBuilder::max_idle) its time to idle, and a [`cache_size`](crate::LocalCachedMapBuilder::cache_size) above zero its capacity, whatever the [`EvictionPolicy`]. Moka evicts in the background, so the cache may hold more entries for a short time. Needs the `moka` feature.
    #[cfg(feature = "moka")]
    #[cfg_attr(docsrs, doc(cfg(feature = "moka")))]
    #[doc(alias = "CAFFEINE")]
    Moka,
}

/// Where a [`LocalCachedMap`](crate::LocalCachedMap) keeps its entries, Redisson's `StoreMode`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StoreMode {
    /// In Redis and in the local cache. The default.
    #[default]
    #[doc(alias = "LOCALCACHE_REDIS")]
    LocalRedis,
    /// In the local cache only. Redis carries the sync messages, and the per-key lock when a loader is asked, but holds no entry. With [`SyncStrategy::Invalidate`], a change on one instance drops the only copy the others have.
    #[doc(alias = "LOCALCACHE")]
    Local,
}

/// Which local entry a [`LocalCachedMap`](crate::LocalCachedMap) drops when its cache is full, as Redisson's `EvictionPolicy`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EvictionPolicy {
    /// No limit: [`LocalCachedMapBuilder::cache_size`](crate::LocalCachedMapBuilder::cache_size) is ignored. The default.
    #[default]
    None,
    /// The entry that was read or written longest ago.
    Lru,
    /// The entry that was read least often.
    Lfu,
}

/// The local cache settings of a builder; [`Default`] is Redisson's defaults.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct LocalOptions {
    pub(crate) cache_size: usize,
    pub(crate) ttl: Option<Duration>,
    pub(crate) max_idle: Option<Duration>,
    pub(crate) sync: SyncStrategy,
    pub(crate) reconnection: ReconnectionStrategy,
    pub(crate) eviction: EvictionPolicy,
    pub(crate) store_cache_miss: bool,
    pub(crate) store_mode: StoreMode,
    pub(crate) expiration: ExpirationEventPolicy,
    pub(crate) provider: CacheProvider,
}

/// The local cache setters of a builder with a field `local: LocalOptions`: `cache_size`, `eviction_policy`, `ttl`, `max_idle`, `sync_strategy`, `reconnection_strategy`, `store_cache_miss`, `store_mode`, `expiration_event_policy` and `cache_provider`.
macro_rules! local_options {
    () => {
        /// Keeps at most `size` entries locally; the [`EvictionPolicy`](crate::EvictionPolicy) picks the one to drop. Zero, the default, means no limit. It has no effect with [`EvictionPolicy::None`](crate::EvictionPolicy::None).
        pub fn cache_size(mut self, size: usize) -> Self {
            self.local.cache_size = size;
            self
        }

        /// Chooses which local entry is dropped when the cache is full. The default is [`EvictionPolicy::None`](crate::EvictionPolicy::None).
        pub fn eviction_policy(mut self, policy: $crate::EvictionPolicy) -> Self {
            self.local.eviction = policy;
            self
        }

        /// Drops a local entry this long after it was stored, whether it is read or not. By default entries stay until they are changed or pushed out.
        pub fn ttl(mut self, ttl: std::time::Duration) -> Self {
            self.local.ttl = (!ttl.is_zero()).then_some(ttl);
            self
        }

        /// Drops a local entry that was not read for this long. By default there is no idle limit.
        pub fn max_idle(mut self, max_idle: std::time::Duration) -> Self {
            self.local.max_idle = (!max_idle.is_zero()).then_some(max_idle);
            self
        }

        /// Chooses how changes reach the other instances. The default is [`SyncStrategy::Invalidate`](crate::SyncStrategy::Invalidate).
        pub fn sync_strategy(mut self, sync: $crate::SyncStrategy) -> Self {
            self.local.sync = sync;
            self
        }

        /// Chooses what happens to the local cache after the pub/sub connection is restored. The default is [`ReconnectionStrategy::None`](crate::ReconnectionStrategy::None).
        pub fn reconnection_strategy(mut self, reconnection: $crate::ReconnectionStrategy) -> Self {
            self.local.reconnection = reconnection;
            self
        }

        /// Also caches that a key is missing, so the next read of it costs no round trip. Off by default. With a loader, a key the loader does not have is cached as missing too.
        pub fn store_cache_miss(mut self, store: bool) -> Self {
            self.local.store_cache_miss = store;
            self
        }

        /// Chooses where the entries live: [`StoreMode::LocalRedis`](crate::StoreMode::LocalRedis), the default, or [`StoreMode::Local`](crate::StoreMode::Local).
        #[doc(alias = "storeMode")]
        pub fn store_mode(mut self, mode: $crate::StoreMode) -> Self {
            self.local.store_mode = mode;
            self
        }

        /// Chooses how the map learns that it expired in Redis. The default is [`ExpirationEventPolicy::KeyeventPattern`](crate::ExpirationEventPolicy::KeyeventPattern).
        #[doc(alias = "expirationEventPolicy", alias = "useKeyEventsPattern")]
        pub fn expiration_event_policy(mut self, policy: $crate::ExpirationEventPolicy) -> Self {
            self.local.expiration = policy;
            self
        }

        /// Chooses the cache that holds the local entries. The default is [`CacheProvider::Builtin`](crate::CacheProvider::Builtin).
        #[doc(alias = "cacheProvider")]
        pub fn cache_provider(mut self, provider: $crate::CacheProvider) -> Self {
            self.local.provider = provider;
            self
        }
    };
}

pub(crate) use local_options;
