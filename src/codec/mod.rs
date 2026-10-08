mod json;

pub use json::JsonCodec;

use crate::error::Result;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait Codec: Clone + Send + Sync + 'static {
    fn encode<T: Serialize + ?Sized>(&self, value: &T) -> Result<Bytes>;
    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T>;
}
