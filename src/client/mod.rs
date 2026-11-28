pub(crate) mod config;
pub(crate) mod core;
pub(crate) mod credentials;
pub(crate) mod events;
pub(crate) mod read_mode;
pub(crate) mod request;
pub(crate) mod retry;
pub(crate) mod slot;
#[cfg(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc"))]
pub(crate) mod tls;

use crate::batch::Batch;
use crate::client::config::ClientBuilder;
use crate::client::core::Core;
use crate::codec::{Codec, JsonCodec};
use crate::collections::hash_map::{HashMap, HashMapBuilder};
use crate::collections::hash_map_cache::{HashMapCache, HashMapCacheBuilder};
use crate::collections::hash_set_cache::HashSetCache;
use crate::collections::local_cached_map::LocalCachedMapBuilder;
use crate::collections::local_cached_map_cache::LocalCachedMapCacheBuilder;
use crate::collections::map_cache_native::{MapCacheNative, MapCacheNativeBuilder};
use crate::collections::multimap::{
    ListMultimap, ListMultimapCache, ListMultimapCacheNative, Mode as MultimapMode, SetMultimap,
    SetMultimapCache, SetMultimapCacheNative, Shape as MultimapShape,
};
use crate::collections::sorted_set::SortedSet;
use crate::collections::time_series::TimeSeries;
use crate::collections::time_series_native::{TimeSeriesNative, TimeSeriesNatives};
use crate::coordination::adder::{Adder, Summable};
use crate::coordination::gcra::Gcra;
use crate::coordination::latch::CountDownLatch;
use crate::coordination::rate_limiter::RateLimiter;
use crate::coordination::semaphore::Semaphore;
use crate::error::Result;
use crate::locks::fair_lock::FairLock;
use crate::locks::fenced_lock::FencedLock;
use crate::locks::multi_lock::{LockTarget, MultiLock};
use crate::locks::rw_lock::RwLock;
use crate::locks::Lock;
use crate::messaging::delayed_queue::DelayedQueue;
use crate::messaging::stream::Stream;
use crate::messaging::topic::{PatternTopic, Topic};
use crate::object::{tagged, Key};
use crate::script::{Function, Script};
use crate::values::atomic_f64::AtomicF64;
use crate::values::atomic_i64::AtomicI64;
use crate::values::bit_set::BitSet;
use crate::values::bloom_filter::BloomFilter;
use crate::values::bucket::Bucket;
use crate::values::geo::Geo;
use crate::values::hyper_log_log::HyperLogLog;
use crate::values::id_generator::IdGenerator;
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

    /// Returns a client that stores values with `codec` and shares this client's connections, like Redisson's `getXxx(name, codec)`.
    ///
    /// ```no_run
    /// # async fn run(client: redissun::Client) -> redissun::Result<()> {
    /// let events = client.with_codec(redissun::StringCodec).stream::<String, String>("events");
    /// events.add([("payload", r#"{"id":42}"#)]).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_codec<N: Codec>(&self, codec: N) -> Client<N> {
        Client::from_parts(self.core.clone(), codec)
    }

    /// The masters, replicas and sentinels of the client, with server commands for each, like Redisson's `getRedisNodes`.
    pub fn nodes(&self) -> crate::Nodes {
        crate::Nodes::new(self.core.clone())
    }

    /// Commands on keys by name across every master, like Redisson's `getKeys`.
    pub fn keys(&self) -> crate::Keys {
        crate::Keys::new(self.core.clone())
    }

    /// Returns a [`Script`] that runs Lua scripts, like Redisson's `getScript()`. Arguments and [`Decoded`](crate::Decoded) replies use this client's codec.
    pub fn script(&self) -> Script<C> {
        Script::new(self.core.clone(), self.codec.clone())
    }

    /// Returns a [`Function`] that loads and calls Redis functions, like Redisson's `getFunction()`.
    pub fn function(&self) -> Function<C> {
        Function::new(self.core.clone(), self.codec.clone())
    }

    /// Starts a [`Batch`] that sends several commands in one round trip.
    pub fn batch(&self) -> Batch<C> {
        Batch::new(self.core.clone(), self.codec.clone())
    }

    /// Returns the [`AtomicI64`] stored under `name`.
    pub fn atomic_i64(&self, name: impl Into<Arc<str>>) -> AtomicI64 {
        AtomicI64::new(Key::new(self.core.clone(), name))
    }

    /// Returns the [`AtomicF64`] stored under `name`.
    pub fn atomic_f64(&self, name: impl Into<Arc<str>>) -> AtomicF64 {
        AtomicF64::new(Key::new(self.core.clone(), name))
    }

    /// Returns the [`IdGenerator`] stored under `name`.
    pub fn id_generator(&self, name: impl Into<Arc<str>>) -> IdGenerator {
        IdGenerator::new(Key::new(self.core.clone(), name))
    }

    /// Joins the [`Adder`] named `name`: `client.adder::<i64>("hits").await?`. Every adder of one name in this client is the same counter.
    pub async fn adder<N: Summable>(&self, name: impl Into<Arc<str>>) -> Result<Adder<N>> {
        Adder::open(Key::new(self.core.clone(), name)).await
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
        BloomFilter::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Returns the [`Bucket`] stored under `name`.
    pub fn bucket<V>(&self, name: impl Into<Arc<str>>) -> Bucket<V, C> {
        Bucket::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Returns [`Buckets`](crate::Buckets) that read and write many buckets of `V` together, like Redisson's `getBuckets`.
    pub fn buckets<V>(&self) -> crate::Buckets<V, C> {
        crate::Buckets::new(self.core.clone(), self.codec.clone())
    }

    /// Returns the [`HashMap`] stored under `name`, the map [`hash_map_builder`](Client::hash_map_builder) builds without options.
    pub fn hash_map<K, V>(&self, name: impl Into<Arc<str>>) -> HashMap<K, V, C> {
        HashMap::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Starts a [`HashMap`] stored under `name` with a loader or a writer, Redisson's `getMap(name, MapOptions)`. Set its options, then `.build().await`.
    ///
    /// ```no_run
    /// # async fn run(client: redissun::Client, users: impl redissun::MapLoader<String, String>) -> redissun::Result<()> {
    /// let map = client
    ///     .hash_map_builder::<String, String>("users")
    ///     .loader(users)
    ///     .build()
    ///     .await?;
    /// map.load_all().parallelism(4).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[doc(alias = "getMap")]
    pub fn hash_map_builder<K, V>(&self, name: impl Into<Arc<str>>) -> HashMapBuilder<K, V, C> {
        HashMapBuilder::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Returns the [`Vec`](crate::Vec) stored under `name`.
    pub fn vec<V>(&self, name: impl Into<Arc<str>>) -> crate::collections::vec::Vec<V, C> {
        crate::collections::vec::Vec::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Returns the [`VecDeque`](crate::VecDeque) stored under `name`.
    pub fn vec_deque<V>(
        &self,
        name: impl Into<Arc<str>>,
    ) -> crate::collections::vec_deque::VecDeque<V, C> {
        crate::collections::vec_deque::VecDeque::new(
            Key::new(self.core.clone(), name),
            self.codec.clone(),
        )
    }

    /// Returns a [`DelayedQueue`] that delivers its values to `destination` after a delay.
    pub fn delayed_queue<V>(
        &self,
        destination: &crate::collections::vec_deque::VecDeque<V, C>,
    ) -> DelayedQueue<V, C> {
        DelayedQueue::new(destination, self.codec.clone())
    }

    /// Returns the [`HashSet`](crate::HashSet) stored under `name`.
    pub fn hash_set<V>(
        &self,
        name: impl Into<Arc<str>>,
    ) -> crate::collections::hash_set::HashSet<V, C> {
        crate::collections::hash_set::HashSet::new(
            Key::new(self.core.clone(), name),
            self.codec.clone(),
        )
    }

    /// Returns the [`SortedSet`] stored under `name`.
    pub fn sorted_set<V>(&self, name: impl Into<Arc<str>>) -> SortedSet<V, C> {
        SortedSet::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Returns the [`TimeSeries`] stored under `name`, Redisson's `getTimeSeries`. **Experimental.** Use `L = ()` for a series without labels. Like Redisson's, the handle schedules the clean-up task of expired entries.
    #[doc(alias = "getTimeSeries")]
    pub fn time_series<V, L>(&self, name: impl Into<Arc<str>>) -> TimeSeries<V, L, C> {
        TimeSeries::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Returns the Redis 8 time series stored under `name`, Redisson's `getTimeSeriesNative`. **Experimental.** Needs Redis 8.0 or the RedisTimeSeries module.
    #[doc(alias = "getTimeSeriesNative")]
    pub fn time_series_native(&self, name: impl Into<Arc<str>>) -> TimeSeriesNative {
        TimeSeriesNative::new(Key::new(self.core.clone(), name))
    }

    /// The `TS.*` commands over many series, Redisson's `getTimeSeriesNatives`. **Experimental.**
    #[doc(alias = "getTimeSeriesNatives")]
    pub fn time_series_natives(&self) -> TimeSeriesNatives {
        TimeSeriesNatives::new(self.core.clone())
    }

    /// Returns the reliable queue stored under `name`, with the functionality of Redisson PRO's `getReliableQueue`. **Experimental.** Every key is in the slot of `{name}`.
    #[doc(alias = "getReliableQueue")]
    pub fn reliable_queue<V>(
        &self,
        name: impl Into<Arc<str>>,
    ) -> crate::messaging::reliable::queue::ReliableQueue<V, C> {
        let name: Arc<str> = name.into();
        crate::messaging::reliable::queue::ReliableQueue::new(
            Key::new(self.core.clone(), tagged(&name)),
            self.codec.clone(),
        )
    }

    /// Returns the reliable publish/subscribe topic stored under `name`, with the functionality of Redisson PRO's `getReliablePubSubTopic`. **Experimental.**
    #[doc(alias = "getReliablePubSubTopic")]
    pub fn reliable_pubsub_topic<V>(
        &self,
        name: impl Into<Arc<str>>,
    ) -> crate::messaging::reliable::topic::ReliablePubSubTopic<V, C> {
        let name: Arc<str> = name.into();
        crate::messaging::reliable::topic::ReliablePubSubTopic::new(
            Key::new(self.core.clone(), tagged(&name)),
            self.codec.clone(),
        )
    }

    /// Returns the [`HashSetCache`] stored under `name`.
    pub fn hash_set_cache<V>(&self, name: impl Into<Arc<str>>) -> HashSetCache<V, C> {
        let name: Arc<str> = name.into();
        HashSetCache::new(
            Key::new(self.core.clone(), tagged(&name)),
            self.codec.clone(),
        )
    }

    /// Returns the [`ListMultimap`] stored under `name`, Redisson's `getListMultimap`. **Experimental.**
    #[doc(alias = "getListMultimap")]
    pub fn list_multimap<K, V>(&self, name: impl Into<Arc<str>>) -> ListMultimap<K, V, C> {
        ListMultimap::new(
            Key::new(self.core.clone(), name),
            self.codec.clone(),
            MultimapMode::Plain,
        )
    }

    /// Returns the [`SetMultimap`] stored under `name`, Redisson's `getSetMultimap`. **Experimental.**
    #[doc(alias = "getSetMultimap")]
    pub fn set_multimap<K, V>(&self, name: impl Into<Arc<str>>) -> SetMultimap<K, V, C> {
        SetMultimap::new(
            Key::new(self.core.clone(), name),
            self.codec.clone(),
            MultimapMode::Plain,
        )
    }

    /// Returns the [`ListMultimapCache`] stored under `name`, Redisson's `getListMultimapCache`: a `ListMultimap` whose keys expire one by one, with deadlines in a zset. **Experimental.**
    #[doc(alias = "getListMultimapCache")]
    pub fn list_multimap_cache<K, V>(
        &self,
        name: impl Into<Arc<str>>,
    ) -> ListMultimapCache<K, V, C> {
        let name: Arc<str> = name.into();
        let mode = MultimapMode::cache(&name, MultimapShape::List);
        ListMultimapCache::new(Key::new(self.core.clone(), name), self.codec.clone(), mode)
    }

    /// Returns the [`SetMultimapCache`] stored under `name`, Redisson's `getSetMultimapCache`: a `SetMultimap` whose keys expire one by one, with deadlines in a zset. **Experimental.**
    #[doc(alias = "getSetMultimapCache")]
    pub fn set_multimap_cache<K, V>(&self, name: impl Into<Arc<str>>) -> SetMultimapCache<K, V, C> {
        let name: Arc<str> = name.into();
        let mode = MultimapMode::cache(&name, MultimapShape::Set);
        SetMultimapCache::new(Key::new(self.core.clone(), name), self.codec.clone(), mode)
    }

    /// Returns the [`ListMultimapCacheNative`] stored under `name`, Redisson's `getListMultimapCacheNative`: a `ListMultimap` whose keys expire with Redis 7.4 field expiry. **Experimental.**
    #[doc(alias = "getListMultimapCacheNative")]
    pub fn list_multimap_cache_native<K, V>(
        &self,
        name: impl Into<Arc<str>>,
    ) -> ListMultimapCacheNative<K, V, C> {
        ListMultimapCacheNative::new(
            Key::new(self.core.clone(), name),
            self.codec.clone(),
            MultimapMode::Native,
        )
    }

    /// Returns the [`SetMultimapCacheNative`] stored under `name`, Redisson's `getSetMultimapCacheNative`: a `SetMultimap` whose keys expire with Redis 7.4 field expiry. **Experimental.**
    #[doc(alias = "getSetMultimapCacheNative")]
    pub fn set_multimap_cache_native<K, V>(
        &self,
        name: impl Into<Arc<str>>,
    ) -> SetMultimapCacheNative<K, V, C> {
        SetMultimapCacheNative::new(
            Key::new(self.core.clone(), name),
            self.codec.clone(),
            MultimapMode::Native,
        )
    }

    /// Starts a [`LocalCachedMap`](crate::LocalCachedMap) stored under `name`. Set its options, then `.build().await`.
    pub fn local_cached_map<K, V>(
        &self,
        name: impl Into<Arc<str>>,
    ) -> LocalCachedMapBuilder<K, V, C> {
        LocalCachedMapBuilder::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Starts a [`LocalCachedMapCache`](crate::LocalCachedMapCache) stored under `name`, Redisson's `getLocalCachedMapCache`. Set its options, then `.build().await`. Experimental.
    #[doc(alias = "getLocalCachedMapCache")]
    pub fn local_cached_map_cache<K, V>(
        &self,
        name: impl Into<Arc<str>>,
    ) -> LocalCachedMapCacheBuilder<K, V, C> {
        LocalCachedMapCacheBuilder::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Returns the [`Stream`] stored under `name`.
    pub fn stream<K, V>(&self, name: impl Into<Arc<str>>) -> Stream<K, V, C> {
        Stream::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Returns the [`Geo`] stored under `name`.
    pub fn geo<V>(&self, name: impl Into<Arc<str>>) -> Geo<V, C> {
        Geo::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Returns the [`Topic`] named `name`.
    pub fn topic<M>(&self, name: impl Into<Arc<str>>) -> Topic<M, C> {
        Topic::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Returns a [`ReliableTopic`](crate::ReliableTopic) named `name`, like Redisson's `getReliableTopic`. Each call is a subscriber of its own; clones of the returned handle share it.
    pub fn reliable_topic<M>(&self, name: impl Into<Arc<str>>) -> crate::ReliableTopic<M, C> {
        crate::ReliableTopic::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Returns the [`ShardedTopic`](crate::ShardedTopic) named `name`, like Redisson's `getShardedTopic`.
    pub fn sharded_topic<M>(&self, name: impl Into<Arc<str>>) -> crate::ShardedTopic<M, C> {
        crate::ShardedTopic::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Returns the [`PatternTopic`] for the channels matching `pattern`.
    pub fn pattern_topic<M>(&self, pattern: impl Into<Arc<str>>) -> PatternTopic<M, C> {
        PatternTopic::new(Key::new(self.core.clone(), pattern), self.codec.clone())
    }

    /// Returns the [`HashMapCache`] stored under `name`.
    pub fn hash_map_cache<K, V>(&self, name: impl Into<Arc<str>>) -> HashMapCache<K, V, C> {
        let name: Arc<str> = name.into();
        HashMapCache::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Starts a [`HashMapCache`] stored under `name` with a loader or a writer, Redisson's `getMapCache(name, MapCacheOptions)`. Set its options, then `.build().await`; like [`hash_map_cache`](Client::hash_map_cache), the cache schedules its clean-up.
    ///
    /// ```no_run
    /// # async fn run(client: redissun::Client, users: impl redissun::MapWriter<String, String>) -> redissun::Result<()> {
    /// use redissun::WriteMode;
    /// let cache = client
    ///     .hash_map_cache_builder::<String, String>("sessions")
    ///     .writer(users)
    ///     .write_mode(WriteMode::write_behind())
    ///     .build()
    ///     .await?;
    /// cache.flush().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[doc(alias = "getMapCache")]
    pub fn hash_map_cache_builder<K, V>(
        &self,
        name: impl Into<Arc<str>>,
    ) -> HashMapCacheBuilder<K, V, C> {
        HashMapCacheBuilder::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// **Experimental.** Returns the [`MapCacheNative`] stored under `name`, a hash whose entries expire on the server (Redis 7.4 or newer for the methods with a time). It is the map [`map_cache_native_builder`](Client::map_cache_native_builder) builds without options.
    #[doc(alias = "getMapCacheNative")]
    pub fn map_cache_native<K, V>(&self, name: impl Into<Arc<str>>) -> MapCacheNative<K, V, C> {
        MapCacheNative::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// **Experimental.** Starts a [`MapCacheNative`] stored under `name` with a loader or a writer, Redisson's `getMapCacheNative(MapOptions)`. Set its options, then `.build().await`.
    #[doc(alias = "getMapCacheNative")]
    pub fn map_cache_native_builder<K, V>(
        &self,
        name: impl Into<Arc<str>>,
    ) -> MapCacheNativeBuilder<K, V, C> {
        MapCacheNativeBuilder::new(Key::new(self.core.clone(), name), self.codec.clone())
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

    /// Returns the [`PermitExpirableSemaphore`](crate::PermitExpirableSemaphore) named `name`, like Redisson's `getPermitExpirableSemaphore`.
    pub fn permit_expirable_semaphore(
        &self,
        name: impl Into<Arc<str>>,
    ) -> crate::PermitExpirableSemaphore {
        crate::PermitExpirableSemaphore::new(Key::new(self.core.clone(), name))
    }

    /// Returns the [`CountDownLatch`] named `name`.
    pub fn count_down_latch(&self, name: impl Into<Arc<str>>) -> CountDownLatch {
        CountDownLatch::new(Key::new(self.core.clone(), name))
    }

    /// Returns the [`RateLimiter`] named `name`.
    pub fn rate_limiter(&self, name: impl Into<Arc<str>>) -> RateLimiter {
        RateLimiter::new(Key::new(self.core.clone(), name))
    }

    /// **Experimental.** A [`Gcra`] rate limiter stored under `name` (the state) and `{name}:config` (the settings), Redisson's `getGcra`.
    #[doc(alias = "getGcra")]
    pub fn gcra(&self, name: impl Into<Arc<str>>) -> Gcra {
        Gcra::new(Key::new(self.core.clone(), name))
    }
}
