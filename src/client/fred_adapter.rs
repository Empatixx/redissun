//! Runs a `redis::Cmd` on fred and reads fred's reply as a `redis::Value`. It lives only until the transport is redis-rs.

use crate::error::{Error, Result};
use bytes::Bytes;
use fred::types::Value as Fred;
use redis::Value;

/// The command name and the arguments of `cmd`, as fred sends a custom command.
pub(crate) fn to_fred(cmd: &redis::Cmd) -> Result<(String, Vec<Fred>)> {
    let mut parts = cmd.args_iter().map(|arg| match arg {
        redis::Arg::Simple(bytes) => Ok(bytes),
        _ => Err(Error::Redis("cursor arguments are not supported".into())),
    });
    let name = match parts.next() {
        Some(name) => String::from_utf8_lossy(name?).into_owned(),
        None => return Err(Error::Redis("empty command".into())),
    };
    let args = parts
        .map(|part| part.map(|bytes| Fred::Bytes(Bytes::copy_from_slice(bytes))))
        .collect::<Result<_>>()?;
    Ok((name, args))
}

/// fred's reply as redis-rs sees it. fred gives `String` for any UTF-8 bulk string, so every string becomes a bulk string.
pub(crate) fn from_fred(value: Fred) -> Value {
    match value {
        Fred::Null => Value::Nil,
        Fred::Integer(number) => Value::Int(number),
        Fred::Double(number) => Value::Double(number),
        Fred::Boolean(flag) => Value::Boolean(flag),
        Fred::String(text) => Value::BulkString(text.as_bytes().to_vec()),
        Fred::Bytes(bytes) => Value::BulkString(bytes.to_vec()),
        Fred::Queued => Value::SimpleString("QUEUED".into()),
        Fred::Array(items) => Value::Array(items.into_iter().map(from_fred).collect()),
        Fred::Map(map) => Value::Map(
            map.inner()
                .into_iter()
                .map(|(key, value)| (Value::BulkString(key.as_bytes().to_vec()), from_fred(value)))
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::{from_fred, to_fred};
    use bytes::Bytes;
    use fred::types::Value as Fred;
    use redis::{FromRedisValue, Value};

    #[test]
    fn fred_replies_become_redis_values() {
        let cases = [
            (Fred::Null, Value::Nil),
            (Fred::Integer(5), Value::Int(5)),
            (Fred::Double(1.5), Value::Double(1.5)),
            (Fred::Boolean(true), Value::Boolean(true)),
            (Fred::String("OK".into()), Value::BulkString(b"OK".to_vec())),
            (Fred::String("x".into()), Value::BulkString(b"x".to_vec())),
            (
                Fred::Bytes(Bytes::from_static(b"\xff")),
                Value::BulkString(vec![0xff]),
            ),
            (
                Fred::Array(vec![Fred::Integer(1), Fred::Null]),
                Value::Array(vec![Value::Int(1), Value::Nil]),
            ),
            (Fred::Queued, Value::SimpleString("QUEUED".into())),
        ];
        for (fred, redis) in cases {
            assert_eq!(from_fred(fred), redis);
        }
        let mut map = fred::types::Map::new();
        map.insert("a".into(), Fred::Integer(1));
        assert_eq!(
            from_fred(Fred::Map(map)),
            Value::Map(vec![(Value::BulkString(b"a".to_vec()), Value::Int(1))])
        );
    }

    #[test]
    fn binary_arguments_and_replies_pass_unchanged() {
        let mut cmd = redis::cmd("SET");
        cmd.arg("k").arg(&b"\xff\x00"[..]);
        let (name, args) = to_fred(&cmd).unwrap();
        assert_eq!(name, "SET");
        assert_eq!(
            args,
            vec![
                Fred::Bytes(Bytes::from_static(b"k")),
                Fred::Bytes(Bytes::from_static(b"\xff\x00"))
            ]
        );
        assert_eq!(
            from_fred(Fred::Bytes(Bytes::from_static(b"\xff\x00"))),
            Value::BulkString(vec![0xff, 0x00])
        );
    }

    #[test]
    fn typed_reads_match_what_fred_read() {
        assert!(bool::from_redis_value(from_fred(Fred::Integer(1))).unwrap());
        assert_eq!(
            Option::<String>::from_redis_value(from_fred(Fred::Null)).unwrap(),
            None
        );
        assert!(
            Vec::<String>::from_redis_value(from_fred(Fred::Array(vec![])))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            f64::from_redis_value(from_fred(Fred::Bytes(Bytes::from_static(b"2.5")))).unwrap(),
            2.5
        );
        <()>::from_redis_value(from_fred(Fred::String("OK".into()))).unwrap();
    }
}
