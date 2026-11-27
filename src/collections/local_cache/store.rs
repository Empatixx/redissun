//! The local entries: redissun's own store and the moka one, each slot with its own lifetime.

use super::options::{CacheProvider, EvictionPolicy};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap as StdHashMap};
use std::time::Duration;
use tokio::time::Instant;

/// How long one local copy may live, besides the store's own `ttl` and `max_idle`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Lifetime {
    /// The copy is dropped at this moment, whether it is read or not.
    pub(crate) deadline: Option<Instant>,
    /// The copy is dropped once it was not read for this long; local reads restart it.
    pub(crate) idle: Option<Duration>,
}

/// How long a copy of an entry read from Redis may live locally; see [`copy_lifetime`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CopyLifetime {
    /// The entry has no limit.
    Forever,
    /// The copy lives this long.
    For(Duration),
    /// The entry is already expired, or about to: it is not cached.
    Skip,
}

impl CopyLifetime {
    /// The [`Lifetime`] of a copy stored now; `None` for [`CopyLifetime::Skip`].
    pub(crate) fn lifetime(self) -> Option<Lifetime> {
        match self {
            CopyLifetime::Forever => Some(Lifetime::default()),
            CopyLifetime::For(left) => Some(Lifetime {
                deadline: Instant::now().checked_add(left),
                idle: None,
            }),
            CopyLifetime::Skip => None,
        }
    }
}

/// The lifetime of a local copy of an entry with the time-to-live deadline `ttl_timeout_ms` (Unix ms) and the max idle `idle_delta_ms`, `0` meaning no limit: the earliest of the time left to the deadline and half the max idle, as local reads do not restart the idle time in Redis.
pub(crate) fn copy_lifetime(ttl_timeout_ms: i64, idle_delta_ms: i64, now_ms: i64) -> CopyLifetime {
    let ttl = (ttl_timeout_ms != 0).then(|| ttl_timeout_ms.saturating_sub(now_ms));
    let idle = (idle_delta_ms != 0).then_some(idle_delta_ms / 2);
    let left = match (ttl, idle) {
        (Some(ttl), Some(idle)) => ttl.min(idle),
        (Some(left), None) | (None, Some(left)) => left,
        (None, None) => return CopyLifetime::Forever,
    };
    if left <= 0 {
        CopyLifetime::Skip
    } else {
        CopyLifetime::For(Duration::from_millis(left.unsigned_abs()))
    }
}

pub(crate) struct Slot {
    value: Option<Bytes>,
    stored: Instant,
    touched: Instant,
    rank: (u64, u64),
    lifetime: Lifetime,
}

pub(crate) struct Store {
    pub(crate) slots: StdHashMap<Bytes, Slot>,
    order: BTreeMap<(u64, u64), Bytes>,
    policy: EvictionPolicy,
    capacity: usize,
    ttl: Option<Duration>,
    max_idle: Option<Duration>,
    sequence: u64,
    baseline: u64,
}

impl Store {
    pub(crate) fn new(
        policy: EvictionPolicy,
        capacity: usize,
        ttl: Option<Duration>,
        max_idle: Option<Duration>,
    ) -> Self {
        Self {
            slots: StdHashMap::new(),
            order: BTreeMap::new(),
            policy,
            capacity: if policy == EvictionPolicy::None {
                0
            } else {
                capacity
            },
            ttl,
            max_idle,
            sequence: 0,
            baseline: 0,
        }
    }

    fn next_sequence(&mut self) -> u64 {
        self.sequence += 1;
        self.sequence
    }

    fn expired(&self, slot: &Slot, now: Instant) -> bool {
        self.ttl
            .is_some_and(|ttl| now.duration_since(slot.stored) >= ttl)
            || self
                .max_idle
                .is_some_and(|idle| now.duration_since(slot.touched) >= idle)
            || slot
                .lifetime
                .deadline
                .is_some_and(|deadline| now >= deadline)
            || slot
                .lifetime
                .idle
                .is_some_and(|idle| now.duration_since(slot.touched) >= idle)
    }

    pub(crate) fn get(&mut self, key: &Bytes) -> Option<Option<Bytes>> {
        let now = Instant::now();
        let expired = self.expired(self.slots.get(key)?, now);
        if expired {
            self.remove(key);
            return None;
        }
        let sequence = self.next_sequence();
        let slot = self.slots.get_mut(key)?;
        slot.touched = now;
        let old = slot.rank;
        slot.rank = match self.policy {
            EvictionPolicy::Lfu => (old.0 + 1, old.1),
            _ => (0, sequence),
        };
        let rank = slot.rank;
        let value = slot.value.clone();
        self.order.remove(&old);
        self.order.insert(rank, key.clone());
        Some(value)
    }

    pub(crate) fn put(&mut self, key: Bytes, value: Option<Bytes>, lifetime: Lifetime) {
        if self.capacity > 0
            && !self.slots.contains_key(&key)
            && self.slots.len() >= self.capacity
            && !self.remove_expired()
        {
            if let Some((rank, victim)) = self.order.pop_first() {
                self.slots.remove(&victim);
                if self.policy == EvictionPolicy::Lfu {
                    self.baseline = self.baseline.max(rank.0);
                }
            }
        }
        let sequence = self.next_sequence();
        let rank = match self.policy {
            EvictionPolicy::Lfu => (self.baseline, sequence),
            _ => (0, sequence),
        };
        let now = Instant::now();
        if let Some(old) = self.slots.insert(
            key.clone(),
            Slot {
                value,
                stored: now,
                touched: now,
                rank,
                lifetime,
            },
        ) {
            self.order.remove(&old.rank);
        }
        self.order.insert(rank, key);
    }

    fn remove_expired(&mut self) -> bool {
        let now = Instant::now();
        let expired: std::vec::Vec<Bytes> = self
            .slots
            .iter()
            .filter(|(_, slot)| self.expired(slot, now))
            .map(|(key, _)| key.clone())
            .collect();
        expired.iter().for_each(|key| self.remove(key));
        !expired.is_empty()
    }

    pub(crate) fn remove(&mut self, key: &Bytes) {
        if let Some(slot) = self.slots.remove(key) {
            self.order.remove(&slot.rank);
        }
    }

    fn clear(&mut self) {
        self.slots.clear();
        self.order.clear();
    }

    fn entries(&mut self) -> std::vec::Vec<(Bytes, Bytes)> {
        self.remove_expired();
        self.slots
            .iter()
            .filter_map(|(key, slot)| Some((key.clone(), slot.value.clone()?)))
            .collect()
    }
}

/// A moka slot: the value (`None` is a cached miss) and its lifetime.
#[cfg(feature = "moka")]
#[derive(Clone)]
pub(crate) struct CachedCopy {
    value: Option<Bytes>,
    lifetime: Lifetime,
}

/// Gives each moka slot the time its [`Lifetime`] allows; moka drops it at the earliest of this, its time to live and its time to idle.
#[cfg(feature = "moka")]
struct CopyExpiry;

#[cfg(feature = "moka")]
impl CopyExpiry {
    /// The time left to the deadline or the idle limit, whichever is shorter; `None` without either.
    fn left(lifetime: &Lifetime) -> Option<Duration> {
        let deadline = lifetime
            .deadline
            .map(|deadline| deadline.saturating_duration_since(Instant::now()));
        match (deadline, lifetime.idle) {
            (Some(deadline), Some(idle)) => Some(deadline.min(idle)),
            (deadline, idle) => deadline.or(idle),
        }
    }
}

#[cfg(feature = "moka")]
impl moka::Expiry<Bytes, CachedCopy> for CopyExpiry {
    fn expire_after_create(
        &self,
        _key: &Bytes,
        value: &CachedCopy,
        _created_at: std::time::Instant,
    ) -> Option<Duration> {
        Self::left(&value.lifetime)
    }

    fn expire_after_read(
        &self,
        _key: &Bytes,
        value: &CachedCopy,
        _read_at: std::time::Instant,
        duration_until_expiry: Option<Duration>,
        _last_modified_at: std::time::Instant,
    ) -> Option<Duration> {
        match value.lifetime.idle {
            Some(_) => Self::left(&value.lifetime),
            None => duration_until_expiry,
        }
    }

    fn expire_after_update(
        &self,
        _key: &Bytes,
        value: &CachedCopy,
        _updated_at: std::time::Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        Self::left(&value.lifetime)
    }
}

/// The local cache, by [`CacheProvider`]. A value of `None` is a cached miss.
pub(crate) enum LocalStore {
    Builtin(Store),
    #[cfg(feature = "moka")]
    Moka(moka::sync::Cache<Bytes, CachedCopy>),
}

impl LocalStore {
    pub(crate) fn new(
        provider: CacheProvider,
        policy: EvictionPolicy,
        capacity: usize,
        ttl: Option<Duration>,
        max_idle: Option<Duration>,
    ) -> Self {
        match provider {
            CacheProvider::Builtin => {
                LocalStore::Builtin(Store::new(policy, capacity, ttl, max_idle))
            }
            // Redisson's Caffeine branch: the eviction policy does not matter, the size limits the cache.
            #[cfg(feature = "moka")]
            CacheProvider::Moka => {
                let mut builder = moka::sync::Cache::builder().expire_after(CopyExpiry);
                if let Some(ttl) = ttl {
                    builder = builder.time_to_live(ttl);
                }
                if let Some(idle) = max_idle {
                    builder = builder.time_to_idle(idle);
                }
                if capacity > 0 {
                    builder = builder.max_capacity(capacity as u64);
                }
                LocalStore::Moka(builder.build())
            }
        }
    }

    pub(crate) fn get(&mut self, key: &Bytes) -> Option<Option<Bytes>> {
        match self {
            LocalStore::Builtin(store) => store.get(key),
            #[cfg(feature = "moka")]
            LocalStore::Moka(cache) => cache.get(key).map(|copy| copy.value),
        }
    }

    pub(crate) fn put(&mut self, key: Bytes, value: Option<Bytes>, lifetime: Lifetime) {
        match self {
            LocalStore::Builtin(store) => store.put(key, value, lifetime),
            #[cfg(feature = "moka")]
            LocalStore::Moka(cache) => cache.insert(key, CachedCopy { value, lifetime }),
        }
    }

    pub(crate) fn remove(&mut self, key: &Bytes) {
        match self {
            LocalStore::Builtin(store) => store.remove(key),
            #[cfg(feature = "moka")]
            LocalStore::Moka(cache) => cache.invalidate(key),
        }
    }

    pub(crate) fn clear(&mut self) {
        match self {
            LocalStore::Builtin(store) => store.clear(),
            #[cfg(feature = "moka")]
            LocalStore::Moka(cache) => cache.invalidate_all(),
        }
    }

    /// Every live slot, cached misses included; expired slots are dropped first, as moka does.
    pub(crate) fn len(&mut self) -> usize {
        match self {
            LocalStore::Builtin(store) => {
                store.remove_expired();
                store.slots.len()
            }
            #[cfg(feature = "moka")]
            LocalStore::Moka(cache) => {
                cache.run_pending_tasks();
                usize::try_from(cache.entry_count()).unwrap_or(usize::MAX)
            }
        }
    }

    /// The slots that hold a value.
    pub(crate) fn entries(&mut self) -> std::vec::Vec<(Bytes, Bytes)> {
        match self {
            LocalStore::Builtin(store) => store.entries(),
            #[cfg(feature = "moka")]
            LocalStore::Moka(cache) => {
                cache.run_pending_tasks();
                cache
                    .iter()
                    .filter_map(|(key, copy)| Some(((*key).clone(), copy.value?)))
                    .collect()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    fn key() -> Bytes {
        Bytes::from_static(b"k")
    }

    fn value() -> Bytes {
        Bytes::from_static(b"v")
    }

    fn until(after: Duration) -> Lifetime {
        Lifetime {
            deadline: Some(Instant::now() + after),
            idle: None,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_slot_past_its_deadline_is_a_miss() {
        let mut store = Store::new(EvictionPolicy::None, 0, None, None);
        store.put(key(), Some(value()), until(50 * MS));
        assert_eq!(store.get(&key()), Some(Some(value())));
        tokio::time::advance(50 * MS).await;
        assert_eq!(store.get(&key()), None);
        assert_eq!(store.slots.len(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_slot_with_an_idle_limit_lives_while_read() {
        let mut store = Store::new(EvictionPolicy::None, 0, None, None);
        let lifetime = Lifetime {
            deadline: None,
            idle: Some(100 * MS),
        };
        store.put(key(), Some(value()), lifetime);
        tokio::time::advance(60 * MS).await;
        assert_eq!(store.get(&key()), Some(Some(value())));
        tokio::time::advance(60 * MS).await;
        assert_eq!(store.get(&key()), Some(Some(value())));
        tokio::time::advance(101 * MS).await;
        assert_eq!(store.get(&key()), None);
    }

    #[tokio::test(start_paused = true)]
    async fn the_store_ttl_and_the_slot_deadline_take_the_earlier() {
        let mut store = Store::new(EvictionPolicy::None, 0, Some(1000 * MS), None);
        store.put(key(), Some(value()), until(200 * MS));
        tokio::time::advance(199 * MS).await;
        assert_eq!(store.get(&key()), Some(Some(value())));
        tokio::time::advance(MS).await;
        assert_eq!(store.get(&key()), None);

        store.put(key(), Some(value()), until(5000 * MS));
        tokio::time::advance(999 * MS).await;
        assert_eq!(store.get(&key()), Some(Some(value())));
        tokio::time::advance(MS).await;
        assert_eq!(store.get(&key()), None);
    }

    #[test]
    fn copy_lifetime_takes_the_earliest_limit() {
        let now = 1_000_000;
        assert_eq!(
            copy_lifetime(now + 10_000, 0, now),
            CopyLifetime::For(10_000 * MS)
        );
        assert_eq!(copy_lifetime(0, 4_000, now), CopyLifetime::For(2_000 * MS));
        assert_eq!(
            copy_lifetime(now + 1_000, 10_000, now),
            CopyLifetime::For(1_000 * MS)
        );
        assert_eq!(copy_lifetime(0, 0, now), CopyLifetime::Forever);
        assert_eq!(copy_lifetime(now - 1, 0, now), CopyLifetime::Skip);
        assert_eq!(copy_lifetime(0, 1, now), CopyLifetime::Skip);
        assert_eq!(CopyLifetime::Skip.lifetime(), None);
        assert_eq!(CopyLifetime::Forever.lifetime(), Some(Lifetime::default()));
    }

    #[cfg(feature = "moka")]
    #[tokio::test]
    async fn moka_honours_the_slot_deadline() {
        let mut store = LocalStore::new(CacheProvider::Moka, EvictionPolicy::None, 0, None, None);
        store.put(key(), Some(value()), until(50 * MS));
        assert_eq!(store.get(&key()), Some(Some(value())));
        tokio::time::sleep(120 * MS).await;
        assert_eq!(store.get(&key()), None);
    }

    #[cfg(feature = "moka")]
    #[tokio::test]
    async fn moka_takes_the_earlier_of_ttl_and_deadline() {
        let mut store = LocalStore::new(
            CacheProvider::Moka,
            EvictionPolicy::None,
            0,
            Some(50 * MS),
            None,
        );
        store.put(key(), Some(value()), until(5000 * MS));
        assert_eq!(store.get(&key()), Some(Some(value())));
        tokio::time::sleep(120 * MS).await;
        assert_eq!(store.get(&key()), None);
    }
}
