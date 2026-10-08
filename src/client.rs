use crate::atomic_i64::AtomicI64;
use crate::bucket::Bucket;
use crate::codec::{Codec, JsonCodec};
use crate::config::ClientBuilder;
use crate::core::Core;
use crate::hash_map::HashMap;
use crate::latch::CountDownLatch;
use crate::lock::Lock;
use crate::object::Key;
use crate::rate_limiter::RateLimiter;
use crate::rw_lock::RwLock;
use crate::semaphore::Semaphore;
use crate::topic::Topic;
use std::fmt;
use std::sync::Arc;

/// Entry point that hands out distributed objects. Cheap to clone; clones share the connection pool.
#[derive(Clone)]
pub struct Client<C: Codec = JsonCodec> {
    core: Arc<Core>,
    codec: C,
}

impl<C: Codec> fmt::Debug for Client<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
    }
}

impl Client<JsonCodec> {
    /// Starts building a client with the JSON codec.
    pub fn builder() -> ClientBuilder<JsonCodec> {
        ClientBuilder::new()
    }
}

impl<C: Codec> Client<C> {
    pub(crate) fn from_parts(core: Arc<Core>, codec: C) -> Self {
        Self { core, codec }
    }

    /// Returns the [`AtomicI64`] stored under `name`.
    pub fn atomic_i64(&self, name: impl Into<Arc<str>>) -> AtomicI64 {
        AtomicI64::new(Key::new(self.core.clone(), name))
    }

    /// Returns the [`Bucket`] stored under `name`.
    pub fn bucket<V>(&self, name: impl Into<Arc<str>>) -> Bucket<V, C> {
        Bucket::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Returns the [`HashMap`] stored under `name`.
    pub fn hash_map<K, V>(&self, name: impl Into<Arc<str>>) -> HashMap<K, V, C> {
        HashMap::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Returns the [`Vec`](crate::Vec) stored under `name`.
    pub fn vec<V>(&self, name: impl Into<Arc<str>>) -> crate::vec::Vec<V, C> {
        crate::vec::Vec::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Returns the [`VecDeque`](crate::VecDeque) stored under `name`.
    pub fn vec_deque<V>(&self, name: impl Into<Arc<str>>) -> crate::vec_deque::VecDeque<V, C> {
        crate::vec_deque::VecDeque::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Returns the [`HashSet`](crate::HashSet) stored under `name`.
    pub fn hash_set<V>(&self, name: impl Into<Arc<str>>) -> crate::hash_set::HashSet<V, C> {
        crate::hash_set::HashSet::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Returns the [`Topic`] named `name`.
    pub fn topic<M>(&self, name: impl Into<Arc<str>>) -> Topic<M, C> {
        Topic::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Returns the [`Lock`] named `name`.
    pub fn lock(&self, name: impl Into<Arc<str>>) -> Lock {
        Lock::new(Key::new(self.core.clone(), name))
    }

    /// Returns the [`RwLock`] named `name`.
    pub fn rw_lock(&self, name: impl Into<Arc<str>>) -> RwLock {
        let name: Arc<str> = name.into();
        RwLock::new(Key::new(self.core.clone(), format!("{{{name}}}")))
    }

    /// Returns the [`Semaphore`] named `name`.
    pub fn semaphore(&self, name: impl Into<Arc<str>>) -> Semaphore {
        Semaphore::new(Key::new(self.core.clone(), name))
    }

    /// Returns the [`CountDownLatch`] named `name`.
    pub fn count_down_latch(&self, name: impl Into<Arc<str>>) -> CountDownLatch {
        CountDownLatch::new(Key::new(self.core.clone(), name))
    }

    /// Returns the [`RateLimiter`] named `name`.
    pub fn rate_limiter(&self, name: impl Into<Arc<str>>) -> RateLimiter {
        RateLimiter::new(Key::new(self.core.clone(), name))
    }
}
