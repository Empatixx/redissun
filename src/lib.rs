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
#![cfg_attr(docsrs, feature(doc_cfg))]

mod batch;
mod client;
mod codec;
mod collections;
mod coordination;
mod error;
mod eviction;
mod locks;
mod messaging;
mod object;
mod pending;
mod pubsub;
mod reply;
mod script;
mod shield;
mod values;
mod wait;

pub use batch::{
    Batch, BatchAtomicI64, BatchBucket, BatchFuture, BatchHashMap, BatchHashSet, BatchResult,
    BatchSortedSet, BatchTopic, BatchVec, BatchVecDeque,
};
pub use client::config::ClientBuilder;
pub use client::credentials::Credentials;
pub use client::retry::DelayStrategy;
#[cfg(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc"))]
pub use client::tls::TlsVerification;
pub use client::Client;
pub use codec::{BytesCodec, Codec, JsonCodec, StringCodec};
pub use collections::hash_map::HashMap;
pub use collections::hash_map_cache::{
    Event, EventKind, Events, EvictionMode, HashMapCache, Insert, InsertNx,
};
pub use collections::hash_set::HashSet;
pub use collections::hash_set_cache::{Expirations, HashSetCache, SetInsert};
pub use collections::local_cached_map::{
    EvictionPolicy, LocalCachedMap, LocalCachedMapBuilder, ReconnectionStrategy, SyncStrategy,
};
pub use collections::sorted_set::{Aggregate, SortedSet};
pub use collections::vec::Vec;
pub use collections::vec_deque::VecDeque;
pub use coordination::latch::CountDownLatch;
pub use coordination::rate_limiter::{RateLimiter, RateLimiterArgs, RateLimiterConfig, RateType};
pub use coordination::semaphore::{Permits, Semaphore};
pub use error::{Error, Result};
pub use locks::fair_lock::FairLock;
pub use locks::fenced_lock::FencedLock;
pub use locks::multi_lock::{LockTarget, MultiLock, MultiLockGuard, MultiLockRequest};
pub use locks::rw_lock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
pub use locks::{Lock, LockGuard, LockRequest};
pub use messaging::delayed_queue::DelayedQueue;
pub use messaging::stream::{
    AutoClaim, PendingEntry, PendingRange, PendingSummary, ReadGroup, Stream, StreamAdd,
    StreamConsumer, StreamEntry, StreamGroup, StreamId, StreamInfo, StreamTrim,
};
pub use messaging::topic::{PatternSubscriber, PatternTopic, Subscriber, Topic};
pub use object::Object;
pub use pending::{Pending, PendingTimeout};
pub use script::{Decoded, Function, LuaScript, Script, ScriptCall, ScriptMode, ScriptOutput};
pub use values::atomic_i64::{AtomicI64, Comparison};
pub use values::bit_set::BitSet;
pub use values::bloom_filter::BloomFilter;
pub use values::bucket::Bucket;
pub use values::geo::{Geo, GeoMatch, GeoOrder, GeoPoint, GeoSearch, GeoUnit};
pub use values::hyper_log_log::HyperLogLog;
