use super::Codec;
use crate::error::{Error, Result};
use bytes::Bytes;
use serde::de::value::{SeqDeserializer, StringDeserializer};
use serde::de::{DeserializeOwned, IntoDeserializer, Visitor};
use serde::ser::{Impossible, SerializeSeq, SerializeTuple};
use serde::{Deserializer, Serialize, Serializer};
use std::fmt;

/// Codec that stores strings as plain UTF-8 text, like Redisson's `StringCodec`.
///
/// A string is stored as it is, without JSON quotes, and numbers, booleans and unit enum variants as their text. `Vec<u8>` and other byte sequences are stored as raw bytes. Structs, maps and lists of other values are an `Error::Codec`. Reading parses the text back into the requested type.
#[derive(Clone, Copy, Debug, Default)]
pub struct StringCodec;

/// Codec that stores bytes as they are, like Redisson's `ByteArrayCodec`.
///
/// It takes `Vec<u8>`, byte arrays and slices, `serde_bytes` types, and strings (stored as their UTF-8 bytes). Anything else is an `Error::Codec`.
#[derive(Clone, Copy, Debug, Default)]
pub struct BytesCodec;

impl Codec for StringCodec {
    fn encode<T: Serialize + ?Sized>(&self, value: &T) -> Result<Bytes> {
        value.serialize(Scalar::Text).map_err(Into::into)
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T> {
        T::deserialize(Stored { bytes, raw: false }).map_err(Into::into)
    }
}

impl Codec for BytesCodec {
    fn encode<T: Serialize + ?Sized>(&self, value: &T) -> Result<Bytes> {
        value.serialize(Scalar::Raw).map_err(Into::into)
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T> {
        T::deserialize(Stored { bytes, raw: true }).map_err(Into::into)
    }
}

#[derive(Debug)]
struct TextError(String);

impl fmt::Display for TextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TextError {}

impl serde::ser::Error for TextError {
    fn custom<M: fmt::Display>(message: M) -> Self {
        TextError(message.to_string())
    }
}

impl serde::de::Error for TextError {
    fn custom<M: fmt::Display>(message: M) -> Self {
        TextError(message.to_string())
    }
}

impl From<TextError> for Error {
    fn from(error: TextError) -> Self {
        Error::Codec(error.0)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scalar {
    Text,
    Raw,
    Byte,
}

impl Scalar {
    fn reject<T>(self, what: &str) -> std::result::Result<T, TextError> {
        Err(TextError(match self {
            Scalar::Text => format!("StringCodec cannot store {what}"),
            Scalar::Raw => format!("BytesCodec cannot store {what}"),
            Scalar::Byte => format!("a byte sequence cannot hold {what}"),
        }))
    }

    fn text(self, text: impl ToString, what: &str) -> std::result::Result<Bytes, TextError> {
        match self {
            Scalar::Text => Ok(Bytes::from(text.to_string())),
            _ => self.reject(what),
        }
    }
}

struct Collect {
    bytes: Vec<u8>,
}

impl Collect {
    fn push<T: Serialize + ?Sized>(&mut self, value: &T) -> std::result::Result<(), TextError> {
        let byte = value.serialize(Scalar::Byte)?;
        self.bytes.extend_from_slice(&byte);
        Ok(())
    }
}

impl SerializeSeq for Collect {
    type Ok = Bytes;
    type Error = TextError;

    fn serialize_element<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), TextError> {
        self.push(value)
    }

    fn end(self) -> std::result::Result<Bytes, TextError> {
        Ok(Bytes::from(self.bytes))
    }
}

impl SerializeTuple for Collect {
    type Ok = Bytes;
    type Error = TextError;

    fn serialize_element<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), TextError> {
        self.push(value)
    }

    fn end(self) -> std::result::Result<Bytes, TextError> {
        Ok(Bytes::from(self.bytes))
    }
}

impl Serializer for Scalar {
    type Ok = Bytes;
    type Error = TextError;
    type SerializeSeq = Collect;
    type SerializeTuple = Collect;
    type SerializeTupleStruct = Impossible<Bytes, TextError>;
    type SerializeTupleVariant = Impossible<Bytes, TextError>;
    type SerializeMap = Impossible<Bytes, TextError>;
    type SerializeStruct = Impossible<Bytes, TextError>;
    type SerializeStructVariant = Impossible<Bytes, TextError>;

    fn serialize_bool(self, v: bool) -> std::result::Result<Bytes, TextError> {
        self.text(v, "a bool")
    }

    fn serialize_i8(self, v: i8) -> std::result::Result<Bytes, TextError> {
        self.text(v, "a number")
    }

    fn serialize_i16(self, v: i16) -> std::result::Result<Bytes, TextError> {
        self.text(v, "a number")
    }

    fn serialize_i32(self, v: i32) -> std::result::Result<Bytes, TextError> {
        self.text(v, "a number")
    }

    fn serialize_i64(self, v: i64) -> std::result::Result<Bytes, TextError> {
        self.text(v, "a number")
    }

    fn serialize_i128(self, v: i128) -> std::result::Result<Bytes, TextError> {
        self.text(v, "a number")
    }

    fn serialize_u8(self, v: u8) -> std::result::Result<Bytes, TextError> {
        match self {
            Scalar::Byte => Ok(Bytes::copy_from_slice(&[v])),
            _ => self.text(v, "a number"),
        }
    }

    fn serialize_u16(self, v: u16) -> std::result::Result<Bytes, TextError> {
        self.text(v, "a number")
    }

    fn serialize_u32(self, v: u32) -> std::result::Result<Bytes, TextError> {
        self.text(v, "a number")
    }

    fn serialize_u64(self, v: u64) -> std::result::Result<Bytes, TextError> {
        self.text(v, "a number")
    }

    fn serialize_u128(self, v: u128) -> std::result::Result<Bytes, TextError> {
        self.text(v, "a number")
    }

    fn serialize_f32(self, v: f32) -> std::result::Result<Bytes, TextError> {
        self.text(v, "a number")
    }

    fn serialize_f64(self, v: f64) -> std::result::Result<Bytes, TextError> {
        self.text(v, "a number")
    }

    fn serialize_char(self, v: char) -> std::result::Result<Bytes, TextError> {
        self.serialize_str(v.encode_utf8(&mut [0; 4]))
    }

    fn serialize_str(self, v: &str) -> std::result::Result<Bytes, TextError> {
        match self {
            Scalar::Byte => self.reject("a string"),
            _ => Ok(Bytes::copy_from_slice(v.as_bytes())),
        }
    }

    fn serialize_bytes(self, v: &[u8]) -> std::result::Result<Bytes, TextError> {
        match self {
            Scalar::Byte => self.reject("bytes"),
            _ => Ok(Bytes::copy_from_slice(v)),
        }
    }

    fn serialize_none(self) -> std::result::Result<Bytes, TextError> {
        self.reject("None")
    }

    fn serialize_some<T: Serialize + ?Sized>(
        self,
        value: &T,
    ) -> std::result::Result<Bytes, TextError> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> std::result::Result<Bytes, TextError> {
        self.reject("()")
    }

    fn serialize_unit_struct(self, name: &'static str) -> std::result::Result<Bytes, TextError> {
        self.reject(name)
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
    ) -> std::result::Result<Bytes, TextError> {
        self.text(variant, "an enum")
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> std::result::Result<Bytes, TextError> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        _index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> std::result::Result<Bytes, TextError> {
        self.reject(name)
    }

    fn serialize_seq(self, len: Option<usize>) -> std::result::Result<Collect, TextError> {
        match self {
            Scalar::Byte => self.reject("a sequence"),
            _ => Ok(Collect {
                bytes: Vec::with_capacity(len.unwrap_or(0)),
            }),
        }
    }

    fn serialize_tuple(self, len: usize) -> std::result::Result<Collect, TextError> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        _len: usize,
    ) -> std::result::Result<Self::SerializeTupleStruct, TextError> {
        self.reject(name)
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> std::result::Result<Self::SerializeTupleVariant, TextError> {
        self.reject(name)
    }

    fn serialize_map(
        self,
        _len: Option<usize>,
    ) -> std::result::Result<Self::SerializeMap, TextError> {
        self.reject("a map")
    }

    fn serialize_struct(
        self,
        name: &'static str,
        _len: usize,
    ) -> std::result::Result<Self::SerializeStruct, TextError> {
        self.reject(name)
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> std::result::Result<Self::SerializeStructVariant, TextError> {
        self.reject(name)
    }
}

struct Stored<'a> {
    bytes: &'a [u8],
    raw: bool,
}

impl Stored<'_> {
    fn text(&self) -> std::result::Result<&str, TextError> {
        std::str::from_utf8(self.bytes).map_err(|_| TextError("the value is not UTF-8 text".into()))
    }

    fn parse<T: std::str::FromStr>(&self, what: &str) -> std::result::Result<T, TextError> {
        let text = self.text()?;
        text.parse()
            .map_err(|_| TextError(format!("{text:?} is not {what}")))
    }
}

macro_rules! parse_number {
    ($($method:ident => $visit:ident: $ty:ty),* $(,)?) => {
        $(
            fn $method<V: Visitor<'de>>(self, visitor: V) -> std::result::Result<V::Value, TextError> {
                visitor.$visit(self.parse::<$ty>("a number")?)
            }
        )*
    };
}

impl<'de> Deserializer<'de> for Stored<'_> {
    type Error = TextError;

    fn deserialize_any<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, TextError> {
        match std::str::from_utf8(self.bytes) {
            Ok(text) if !self.raw => visitor.visit_str(text),
            _ => visitor.visit_bytes(self.bytes),
        }
    }

    fn deserialize_bool<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, TextError> {
        visitor.visit_bool(self.parse("a bool")?)
    }

    parse_number! {
        deserialize_i8 => visit_i8: i8,
        deserialize_i16 => visit_i16: i16,
        deserialize_i32 => visit_i32: i32,
        deserialize_i64 => visit_i64: i64,
        deserialize_i128 => visit_i128: i128,
        deserialize_u8 => visit_u8: u8,
        deserialize_u16 => visit_u16: u16,
        deserialize_u32 => visit_u32: u32,
        deserialize_u64 => visit_u64: u64,
        deserialize_u128 => visit_u128: u128,
        deserialize_f32 => visit_f32: f32,
        deserialize_f64 => visit_f64: f64,
    }

    fn deserialize_char<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, TextError> {
        visitor.visit_char(self.parse("one character")?)
    }

    fn deserialize_str<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, TextError> {
        visitor.visit_str(self.text()?)
    }

    fn deserialize_string<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, TextError> {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, TextError> {
        visitor.visit_bytes(self.bytes)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, TextError> {
        visitor.visit_bytes(self.bytes)
    }

    fn deserialize_option<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, TextError> {
        visitor.visit_some(self)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> std::result::Result<V::Value, TextError> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, TextError> {
        visitor.visit_seq(SeqDeserializer::<_, TextError>::new(
            self.bytes.iter().copied(),
        ))
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        _len: usize,
        visitor: V,
    ) -> std::result::Result<V::Value, TextError> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, TextError> {
        let variant: StringDeserializer<TextError> = self.text()?.to_string().into_deserializer();
        visitor.visit_enum(variant)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, TextError> {
        visitor.visit_unit()
    }

    serde::forward_to_deserialize_any! {
        unit unit_struct tuple_struct map struct identifier
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strings_are_stored_without_quotes() {
        assert_eq!(&StringCodec.encode("payload").unwrap()[..], b"payload");
        assert_eq!(
            &StringCodec.encode(&"{\"a\":1}".to_string()).unwrap()[..],
            b"{\"a\":1}"
        );
        assert_eq!(StringCodec.decode::<String>(b"payload").unwrap(), "payload");
    }

    #[test]
    fn numbers_and_bools_round_trip_as_text() {
        assert_eq!(&StringCodec.encode(&42i64).unwrap()[..], b"42");
        assert_eq!(&StringCodec.encode(&1.5f64).unwrap()[..], b"1.5");
        assert_eq!(&StringCodec.encode(&true).unwrap()[..], b"true");
        assert_eq!(StringCodec.decode::<i64>(b"42").unwrap(), 42);
        assert_eq!(StringCodec.decode::<f64>(b"1.5").unwrap(), 1.5);
        assert!(StringCodec.decode::<bool>(b"true").unwrap());
        assert_eq!(StringCodec.decode::<String>(b"42").unwrap(), "42");
    }

    #[test]
    fn unit_enums_are_stored_by_name() {
        #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
        enum Color {
            Red,
        }
        assert_eq!(&StringCodec.encode(&Color::Red).unwrap()[..], b"Red");
        assert_eq!(StringCodec.decode::<Color>(b"Red").unwrap(), Color::Red);
    }

    #[test]
    fn structs_are_a_codec_error() {
        #[derive(serde::Serialize, serde::Deserialize, Debug)]
        struct User {
            name: String,
        }
        let user = User { name: "a".into() };
        assert!(matches!(StringCodec.encode(&user), Err(Error::Codec(_))));
        assert!(matches!(
            StringCodec.decode::<User>(b"a"),
            Err(Error::Codec(_))
        ));
        assert!(matches!(
            StringCodec.decode::<i64>(b"x"),
            Err(Error::Codec(_))
        ));
    }

    #[test]
    fn bytes_are_stored_as_they_are() {
        let raw = vec![0u8, 159, 146, 150];
        assert_eq!(&BytesCodec.encode(&raw).unwrap()[..], &raw[..]);
        assert_eq!(&BytesCodec.encode(&[1u8, 2, 3]).unwrap()[..], &[1, 2, 3]);
        assert_eq!(BytesCodec.decode::<Vec<u8>>(&raw).unwrap(), raw);
        assert_eq!(&BytesCodec.encode("text").unwrap()[..], b"text");
        assert_eq!(BytesCodec.decode::<String>(b"text").unwrap(), "text");
        assert_eq!(&StringCodec.encode(&raw).unwrap()[..], &raw[..]);
    }

    #[test]
    fn bytes_codec_refuses_numbers() {
        assert!(matches!(BytesCodec.encode(&42i64), Err(Error::Codec(_))));
        assert!(matches!(
            BytesCodec.encode(&vec![300u16]),
            Err(Error::Codec(_))
        ));
        assert!(matches!(
            BytesCodec.decode::<String>(&[0xff]),
            Err(Error::Codec(_))
        ));
    }
}
