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
mod bucket;
mod client;
mod codec;
mod config;
mod core;
mod error;
mod hash_map;
mod hash_map_cache;
mod hash_set;
mod latch;
mod list;
mod lock;
mod object;
mod pending;
mod pubsub;
mod rate_limiter;
mod rw_lock;
mod semaphore;
mod shield;
mod topic;
mod vec;
mod vec_deque;
mod wait;

pub use atomic_i64::AtomicI64;
pub use bucket::Bucket;
pub use client::Client;
pub use codec::{Codec, JsonCodec};
pub use config::ClientBuilder;
pub use error::{Error, Result};
pub use hash_map::HashMap;
pub use hash_map_cache::{Event, Events, EvictionMode, HashMapCache, Insert, InsertNx};
pub use hash_set::HashSet;
pub use latch::CountDownLatch;
pub use lock::{Lock, LockGuard, LockRequest};
pub use object::Object;
pub use pending::{Pending, PendingTimeout};
pub use rate_limiter::{RateLimiter, RateType};
pub use rw_lock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
pub use semaphore::{Permits, Semaphore};
pub use topic::{Subscriber, Topic};
pub use vec::Vec;
pub use vec_deque::VecDeque;
