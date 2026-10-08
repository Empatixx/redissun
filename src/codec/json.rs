use super::Codec;
use crate::error::{Error, Result};
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;

/// Codec that stores values as JSON through serde.
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonCodec;

impl Codec for JsonCodec {
    fn encode<T: Serialize + ?Sized>(&self, value: &T) -> Result<Bytes> {
        serde_json::to_vec(value)
            .map(Bytes::from)
            .map_err(|e| Error::Codec(e.to_string()))
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T> {
        serde_json::from_slice(bytes).map_err(|e| Error::Codec(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_values() {
        let codec = JsonCodec;
        let bytes = codec.encode(&vec![1, 2, 3]).unwrap();
        assert_eq!(&bytes[..], b"[1,2,3]");
        assert_eq!(codec.decode::<Vec<i32>>(&bytes).unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn str_and_string_encode_identically() {
        let codec = JsonCodec;
        assert_eq!(
            codec.encode("key").unwrap(),
            codec.encode(&"key".to_string()).unwrap()
        );
    }

    #[test]
    fn invalid_input_is_a_codec_error() {
        let result = JsonCodec.decode::<i64>(b"not json");
        assert!(matches!(result, Err(Error::Codec(_))));
    }
}
