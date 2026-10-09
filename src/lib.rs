//! Distributed objects on Redis, inspired by Redisson.
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

mod atomic_i64;
mod batch;
mod bit_set;
mod bloom_filter;
mod bucket;
mod client;
mod codec;
mod config;
mod core;
mod delayed_queue;
mod error;
mod eviction;
mod fair_lock;
mod fenced_lock;
mod geo;
mod hash_map;
mod hash_map_cache;
mod hash_set;
mod hash_set_cache;
mod hyper_log_log;
mod latch;
mod list;
mod local_cached_map;
mod lock;
mod multi_lock;
mod object;
mod pending;
mod pubsub;
mod rate_limiter;
mod reply;
mod rw_lock;
mod semaphore;
mod shield;
mod sorted_set;
mod stream;
mod topic;
mod vec;
mod vec_deque;
mod wait;

pub use atomic_i64::AtomicI64;
pub use batch::{
    Batch, BatchAtomicI64, BatchBucket, BatchFuture, BatchHashMap, BatchHashSet, BatchResult,
    BatchSortedSet, BatchTopic, BatchVec, BatchVecDeque,
};
pub use bit_set::BitSet;
pub use bloom_filter::BloomFilter;
pub use bucket::Bucket;
pub use client::Client;
pub use codec::{Codec, JsonCodec};
pub use config::ClientBuilder;
pub use delayed_queue::DelayedQueue;
pub use error::{Error, Result};
pub use fair_lock::FairLock;
pub use fenced_lock::FencedLock;
pub use geo::{Geo, GeoMatch, GeoPoint, GeoUnit};
pub use hash_map::HashMap;
pub use hash_map_cache::{Event, EventKind, Events, EvictionMode, HashMapCache, Insert, InsertNx};
pub use hash_set::HashSet;
pub use hash_set_cache::{Expirations, HashSetCache, SetInsert};
pub use hyper_log_log::HyperLogLog;
pub use latch::CountDownLatch;
pub use local_cached_map::{
    EvictionPolicy, LocalCachedMap, LocalCachedMapBuilder, ReconnectionStrategy, SyncStrategy,
};
pub use lock::{Lock, LockGuard, LockRequest};
pub use multi_lock::{LockTarget, MultiLock, MultiLockGuard};
pub use object::Object;
pub use pending::{Pending, PendingTimeout};
pub use rate_limiter::{RateLimiter, RateType};
pub use rw_lock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
pub use semaphore::{Permits, Semaphore};
pub use sorted_set::SortedSet;
pub use stream::{PendingEntry, PendingSummary, Stream, StreamAdd, StreamEntry, StreamId};
pub use topic::{Subscriber, Topic};
pub use vec::Vec;
pub use vec_deque::VecDeque;
