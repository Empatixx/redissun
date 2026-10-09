use crate::error::Error;
use bytes::Bytes;
use fred::types::Value;

pub(crate) fn text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.to_string()),
        Value::Bytes(bytes) => String::from_utf8(bytes.to_vec()).ok(),
        Value::Integer(number) => Some(number.to_string()),
        _ => None,
    }
}

pub(crate) fn bytes(value: &Value) -> Option<Bytes> {
    match value {
        Value::Bytes(bytes) => Some(bytes.clone()),
        Value::String(text) => Some(Bytes::copy_from_slice(text.as_bytes())),
        Value::Integer(number) => Some(Bytes::from(number.to_string())),
        _ => None,
    }
}

pub(crate) fn number(value: &Value) -> Option<i64> {
    match value {
        Value::Integer(number) => Some(*number),
        other => text(other)?.parse().ok(),
    }
}

pub(crate) fn array(value: &Value) -> &[Value] {
    match value {
        Value::Array(items) => items,
        _ => &[],
    }
}

pub(crate) fn int(number: impl TryInto<i64>) -> Value {
    Value::Integer(number.try_into().unwrap_or(i64::MAX))
}

pub(crate) fn malformed() -> Error {
    Error::Redis("unexpected reply from Redis".into())
}
