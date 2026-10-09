use crate::atomic_i64::AtomicI64;
use crate::bit_set::BitSet;
use crate::bloom_filter::BloomFilter;
use crate::bucket::Bucket;
use crate::codec::{Codec, JsonCodec};
use crate::config::ClientBuilder;
use crate::core::Core;
use crate::delayed_queue::DelayedQueue;
use crate::error::Result;
use crate::fair_lock::FairLock;
use crate::fenced_lock::FencedLock;
use crate::hash_map::HashMap;
use crate::hash_map_cache::HashMapCache;
use crate::hash_set_cache::HashSetCache;
use crate::hyper_log_log::HyperLogLog;
use crate::latch::CountDownLatch;
use crate::lock::Lock;
use crate::multi_lock::{LockTarget, MultiLock};
use crate::object::{tagged, Key};
use crate::rate_limiter::RateLimiter;
use crate::rw_lock::RwLock;
use crate::semaphore::Semaphore;
use crate::sorted_set::SortedSet;
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

    /// Returns the [`BitSet`] stored under `name`.
    pub fn bit_set(&self, name: impl Into<Arc<str>>) -> BitSet {
        BitSet::new(Key::new(self.core.clone(), name))
    }

    /// Returns the [`HyperLogLog`] stored under `name`.
    pub fn hyper_log_log<V>(&self, name: impl Into<Arc<str>>) -> HyperLogLog<V, C> {
        HyperLogLog::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Returns the [`BloomFilter`] stored under `name`.
    pub fn bloom_filter<V>(&self, name: impl Into<Arc<str>>) -> BloomFilter<V, C> {
        let name: Arc<str> = name.into();
        BloomFilter::new(
            Key::new(self.core.clone(), tagged(&name)),
            self.codec.clone(),
        )
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

    /// Returns a [`DelayedQueue`] that delivers its values to `destination` after a delay.
    pub fn delayed_queue<V>(
        &self,
        destination: &crate::vec_deque::VecDeque<V, C>,
    ) -> DelayedQueue<V, C> {
        DelayedQueue::new(destination, self.codec.clone())
    }

    /// Returns the [`HashSet`](crate::HashSet) stored under `name`.
    pub fn hash_set<V>(&self, name: impl Into<Arc<str>>) -> crate::hash_set::HashSet<V, C> {
        crate::hash_set::HashSet::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Returns the [`SortedSet`] stored under `name`.
    pub fn sorted_set<V>(&self, name: impl Into<Arc<str>>) -> SortedSet<V, C> {
        SortedSet::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Returns the [`HashSetCache`] stored under `name`.
    pub fn hash_set_cache<V>(&self, name: impl Into<Arc<str>>) -> HashSetCache<V, C> {
        let name: Arc<str> = name.into();
        HashSetCache::new(
            Key::new(self.core.clone(), tagged(&name)),
            self.codec.clone(),
        )
    }

    /// Returns the [`Topic`] named `name`.
    pub fn topic<M>(&self, name: impl Into<Arc<str>>) -> Topic<M, C> {
        Topic::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Returns the [`HashMapCache`] stored under `name`.
    pub fn hash_map_cache<K, V>(&self, name: impl Into<Arc<str>>) -> HashMapCache<K, V, C> {
        let name: Arc<str> = name.into();
        HashMapCache::new(
            Key::new(self.core.clone(), tagged(&name)),
            self.codec.clone(),
        )
    }

    /// Returns the [`Lock`] named `name`.
    pub fn lock(&self, name: impl Into<Arc<str>>) -> Lock {
        Lock::new(Key::new(self.core.clone(), name))
    }

    /// Returns the [`FairLock`] named `name`.
    pub fn fair_lock(&self, name: impl Into<Arc<str>>) -> FairLock {
        let name: Arc<str> = name.into();
        FairLock::new(Key::new(self.core.clone(), tagged(&name)))
    }

    /// Returns the [`FencedLock`] named `name`.
    pub fn fenced_lock(&self, name: impl Into<Arc<str>>) -> FencedLock {
        let name: Arc<str> = name.into();
        FencedLock::new(Key::new(self.core.clone(), tagged(&name)))
    }

    /// Returns a [`MultiLock`] over these locks, which fails with `Error::Config` when there are none.
    pub fn multi_lock<L: Into<LockTarget>>(
        &self,
        locks: impl IntoIterator<Item = L>,
    ) -> Result<MultiLock> {
        MultiLock::new(locks.into_iter().map(Into::into).collect())
    }

    /// Returns the [`RwLock`] named `name`.
    pub fn rw_lock(&self, name: impl Into<Arc<str>>) -> RwLock {
        let name: Arc<str> = name.into();
        RwLock::new(Key::new(self.core.clone(), tagged(&name)))
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
