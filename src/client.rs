use crate::bucket::Bucket;
use crate::codec::{Codec, JsonCodec};
use crate::config::ClientBuilder;
use crate::core::Core;
use crate::object::Key;
use std::sync::Arc;

#[derive(Clone)]
pub struct Client<C: Codec = JsonCodec> {
    core: Arc<Core>,
    codec: C,
}

impl Client<JsonCodec> {
    pub fn builder() -> ClientBuilder<JsonCodec> {
        ClientBuilder::new()
    }
}

impl<C: Codec> Client<C> {
    pub(crate) fn from_parts(core: Arc<Core>, codec: C) -> Self {
        Self { core, codec }
    }

    pub fn bucket<V>(&self, name: impl Into<Arc<str>>) -> Bucket<V, C> {
        Bucket::new(Key::new(self.core.clone(), name), self.codec.clone())
    }
}
