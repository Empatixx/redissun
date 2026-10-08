use crate::atomic_long::AtomicLong;
use crate::bucket::Bucket;
use crate::codec::{Codec, JsonCodec};
use crate::config::ClientBuilder;
use crate::core::Core;
use crate::lock::Lock;
use crate::map::Map;
use crate::object::Key;
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

    /// Returns the [`AtomicLong`] stored under `name`.
    pub fn atomic_long(&self, name: impl Into<Arc<str>>) -> AtomicLong {
        AtomicLong::new(Key::new(self.core.clone(), name))
    }

    /// Returns the [`Bucket`] stored under `name`.
    pub fn bucket<V>(&self, name: impl Into<Arc<str>>) -> Bucket<V, C> {
        Bucket::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Returns the [`Map`] stored under `name`.
    pub fn map<K, V>(&self, name: impl Into<Arc<str>>) -> Map<K, V, C> {
        Map::new(Key::new(self.core.clone(), name), self.codec.clone())
    }

    /// Returns the [`Lock`] named `name`.
    pub fn lock(&self, name: impl Into<Arc<str>>) -> Lock {
        Lock::new(Key::new(self.core.clone(), name))
    }
}
