//! Distributed objects on Redis, inspired by Redisson.
//!
//! ```no_run
//! use redissun::Client;
//!
//! # async fn run() -> redissun::Result<()> {
//! let client = Client::builder().url("redis://127.0.0.1:6379").build().await?;
//!
//! let users = client.map::<String, String>("users");
//! users.insert("jirka".into(), "Jirka".into()).await?;
//!
//! let guard = client.lock("order:42").lock().await?;
//! guard.unlock().await?;
//! # Ok(())
//! # }
//! ```

#![deny(missing_docs)]

mod atomic_long;
mod bucket;
mod client;
mod codec;
mod config;
mod core;
mod error;
mod latch;
mod lock;
mod map;
mod object;
mod pubsub;
mod rate_limiter;
mod semaphore;

pub use atomic_long::AtomicLong;
pub use bucket::Bucket;
pub use client::Client;
pub use codec::{Codec, JsonCodec};
pub use config::ClientBuilder;
pub use error::{Error, Result};
pub use latch::CountDownLatch;
pub use lock::{Lock, LockGuard, LockOptions};
pub use map::Map;
pub use object::Object;
pub use rate_limiter::{RateLimiter, RateType};
pub use semaphore::{Permits, Semaphore};
