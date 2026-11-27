//! Redisson for Rust: a Redis library with std-style collections, distributed locks, caches and streams.
//!
//! ```no_run
//! use redissun::Client;
//!
//! # async fn run() -> redissun::Result<()> {
//! let client = Client::builder().url("redis://127.0.0.1:6379").build().await?;
//!
//! let users = client.hash_map::<String, String>("users");
//! users.insert("jirka", "Jirka").await?;
//!
//! let guard = client.lock("order:42").lock().await?;
//! guard.unlock().await?;
//! # Ok(())
//! # }
//! ```

#![deny(missing_docs)]
// The library returns errors instead of panicking. clippy.toml allows these calls in tests, where a panic is a failed test.
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    clippy::unimplemented
)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod batch;
mod client;
mod codec;
mod collections;
mod coordination;
mod error;
mod eviction;
mod expiry;
mod keys;
mod locks;
mod lua;
mod messaging;
mod net;
mod nodes;
mod object;
mod pending;
mod pubsub;
mod reply;
mod script;
mod shield;
mod values;
mod wait;

/// The types [`HashMap`] methods return, like `std::collections::hash_map`.
pub mod hash_map {
    pub use crate::collections::hash_map::{Entry, Extend, LoadAll, OccupiedEntry, VacantEntry};
}

/// The builders [`HashMapCache`] methods return.
pub mod hash_map_cache {
    pub use crate::collections::hash_map_cache::{
        EntryRequest, ExpireEntries, ExpireEntry, Extend, Insert, LeaseInsert, Migrate, Set,
        TryInsert,
    };
    pub use crate::collections::map::load::LoadAll;
}

/// The builder [`LocalCachedMapCache::preload_cache`] returns; its other methods return the builders of [`hash_map_cache`].
pub mod local_cached_map_cache {
    pub use crate::collections::local_cached_map_cache::PreloadCache;
}

/// The builders [`MapCacheNative`] methods return.
pub mod map_cache_native {
    pub use crate::collections::hash_map::{Entry, OccupiedEntry, VacantEntry};
    pub use crate::collections::map::load::LoadAll;
    pub use crate::collections::map_cache_native::{
        EntryRequest, ExpireEntries, ExpireEntry, Extend, Insert, Replace, Set, TryExtend,
        TryInsert, UpdateMany,
    };
}

/// The value handles and the event listener of the multimaps.
pub mod multimap {
    pub use crate::collections::multimap::events::MultimapEvents;
    pub use crate::collections::multimap::values::{ListMultimapValues, SetMultimapValues};
}

/// The builders [`SortedSet`] methods return.
pub mod sorted_set {
    pub use crate::collections::sorted_set::Extend;
}

/// The builders [`VecDeque`] methods return.
pub mod vec_deque {
    pub use crate::collections::vec_deque::Extend;
}

/// The builders [`Bucket`] methods return.
pub mod bucket {
    pub use crate::values::bucket::{Replace, Set, TrySet, Update};
}

/// The builders [`Semaphore`] methods return.
pub mod semaphore {
    pub use crate::coordination::semaphore::InitPermits;
}

/// The builders [`RateLimiter`] methods return.
pub mod rate_limiter {
    pub use crate::coordination::rate_limiter::{InitRate, SetRate, UpdateRate};
}

/// The builders [`Gcra`] methods return.
pub mod gcra {
    pub use crate::coordination::gcra::TryAcquire;
}

/// The builders [`Buckets`] methods return.
pub mod buckets {
    pub use crate::values::buckets::{TrySet, Update};
}

pub use batch::{
    Batch, BatchAtomicI64, BatchBucket, BatchFuture, BatchHashMap, BatchHashSet, BatchResult,
    BatchSortedSet, BatchTopic, BatchVec, BatchVecDeque,
};
pub use client::config::ClientBuilder;
pub use client::credentials::Credentials;
pub use client::events::{ConnectionListener, NodeType};
pub use client::read_mode::ReadMode;
pub use client::retry::DelayStrategy;
#[cfg(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc"))]
pub use client::tls::TlsVerification;
pub use client::Client;
pub use codec::{BytesCodec, Codec, JsonCodec, StringCodec};
pub use collections::hash_map::{HashMap, HashMapBuilder};
pub use collections::hash_map_cache::{
    EntryEvent, EntryEventKind, EntryEvents, EvictionMode, HashMapCache, HashMapCacheBuilder,
    Lease, LeaseGuard,
};
pub use collections::hash_set::HashSet;
pub use collections::hash_set_cache::{Expirations, HashSetCache, SetInsert};
pub use collections::local_cache::{
    CacheProvider, EvictionPolicy, ExpirationEventPolicy, ReconnectionStrategy, StoreMode,
    SyncStrategy,
};
pub use collections::local_cached_map::{LocalCachedMap, LocalCachedMapBuilder};
pub use collections::local_cached_map_cache::{LocalCachedMapCache, LocalCachedMapCacheBuilder};
pub use collections::map::events::{MapEvent, MapEventKind, MapEvents};
pub use collections::map::scan::MapScan;
pub use collections::map::store::{BoxError, MapLoader, MapWriter, WriteMode};
pub use collections::map_cache_native::{MapCacheNative, MapCacheNativeBuilder};
pub use collections::multimap::events::{MultimapEvent, MultimapEventKind};
pub use collections::multimap::{
    ListMultimap, ListMultimapCache, ListMultimapCacheNative, SetMultimap, SetMultimapCache,
    SetMultimapCacheNative,
};
pub use collections::sorted_set::{Aggregate, SortedSet};
pub use collections::vec::Vec;
pub use collections::vec_deque::VecDeque;
pub use coordination::adder::{Adder, Summable};
pub use coordination::gcra::{Gcra, GcraConfig, GcraResult};
pub use coordination::latch::CountDownLatch;
pub use coordination::permit_semaphore::{PermitExpirableSemaphore, PermitRequest};
pub use coordination::rate_limiter::{RateLimiter, RateLimiterConfig, RateType};
pub use coordination::semaphore::{Permits, Semaphore};
pub use error::{Error, Result};
pub use expiry::Expiry;
pub use keys::{KeyEvent, KeyEvents, KeyType, Keys, KeysScan, Migrate};
pub use locks::fair_lock::FairLock;
pub use locks::fenced_lock::FencedLock;
pub use locks::multi_lock::{LockTarget, MultiLock, MultiLockGuard, MultiLockRequest};
pub use locks::rw_lock::RwLock;
pub use locks::{Lock, LockGuard, LockRequest};
pub use messaging::delayed_queue::DelayedQueue;
pub use messaging::reliable_topic::{ReliableSubscriber, ReliableTopic};
pub use messaging::sharded_topic::{ShardedSubscriber, ShardedTopic};
pub use messaging::stream::{
    AutoClaim, PendingEntry, PendingRange, PendingSummary, ReadGroup, Stream, StreamAdd,
    StreamConsumer, StreamEntry, StreamGroup, StreamId, StreamInfo, StreamTrim,
};
pub use messaging::topic::{PatternSubscriber, PatternTopic, Subscriber, Topic};
pub use nodes::{InfoSection, Nodes, RedisNode, SentinelNode, SetSlot};
pub use object::Object;
pub use pending::{Pending, PendingTimeout};
pub use script::{Decoded, Function, LuaScript, Script, ScriptCall, ScriptMode, ScriptOutput};
pub use values::atomic_f64::AtomicF64;
pub use values::atomic_i64::{AtomicI64, Comparison};
pub use values::bit_set::BitSet;
pub use values::bloom_filter::BloomFilter;
pub use values::bucket::Bucket;
pub use values::buckets::Buckets;
pub use values::geo::{Geo, GeoMatch, GeoOrder, GeoPoint, GeoSearch, GeoUnit};
pub use values::hyper_log_log::HyperLogLog;
pub use values::id_generator::IdGenerator;
