mod json;

pub use json::JsonCodec;

use crate::error::Result;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;

/// Converts values to and from the bytes stored in Redis.
pub trait Codec: Clone + Send + Sync + 'static {
    /// Serializes a value into bytes.
    fn encode<T: Serialize + ?Sized>(&self, value: &T) -> Result<Bytes>;
    /// Deserializes a value from bytes.
    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T>;
}
