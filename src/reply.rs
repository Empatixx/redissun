use crate::client::request::Arg;
use crate::error::Error;
use bytes::Bytes;
use redis::Value;

pub(crate) fn text(value: &Value) -> Option<String> {
    match value {
        Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
        Value::SimpleString(text) => Some(text.clone()),
        Value::Okay => Some("OK".into()),
        Value::Int(number) => Some(number.to_string()),
        _ => None,
    }
}

pub(crate) fn bytes(value: &Value) -> Option<Bytes> {
    match value {
        Value::BulkString(bytes) => Some(Bytes::copy_from_slice(bytes)),
        Value::SimpleString(text) => Some(Bytes::copy_from_slice(text.as_bytes())),
        Value::Int(number) => Some(Bytes::from(number.to_string())),
        _ => None,
    }
}

pub(crate) fn number(value: &Value) -> Option<i64> {
    match value {
        Value::Int(number) => Some(*number),
        other => text(other)?.parse().ok(),
    }
}

pub(crate) fn array(value: &Value) -> &[Value] {
    match value {
        Value::Array(items) | Value::Set(items) => items,
        _ => &[],
    }
}

pub(crate) fn int(number: impl TryInto<i64>) -> Arg {
    Arg::Int(number.try_into().unwrap_or(i64::MAX))
}

pub(crate) fn malformed() -> Error {
    Error::Redis("unexpected reply from Redis".into())
}
