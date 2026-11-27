//! Reading a hash page by page with `HSCAN`, shared by the maps.

use crate::client::request::Arg;
use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::object::Key;
use bytes::Bytes;
use futures::{stream, Stream, TryStreamExt};
use serde::de::DeserializeOwned;

const DEFAULT_PAGE: usize = 100;

/// Which entries a map scan reads and how many it asks for per round trip, Redisson's `keySet(pattern, count)`, `values(pattern, count)` and `entrySet(pattern, count)`. By default every entry, in pages of 100.
///
/// The pattern is matched by Redis against the **encoded** key, as in Redisson: with [`JsonCodec`](crate::JsonCodec) a string key `abc` is stored as `"abc"`, so the pattern for keys starting with `a` is `"a*`.
#[derive(Clone, Debug, Default)]
pub struct MapScan {
    pattern: Option<String>,
    page_size: usize,
}

impl MapScan {
    /// Every entry, as [`Default`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Only the entries whose encoded key matches the glob-style `pattern` (`HSCAN … MATCH`).
    pub fn pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }

    /// How many entries each `HSCAN` asks for (`COUNT`); zero means the default of 100.
    pub fn page_size(mut self, page_size: usize) -> Self {
        self.page_size = page_size;
        self
    }

    /// The pattern set with [`MapScan::pattern`], if any.
    pub(crate) fn match_pattern(&self) -> Option<&str> {
        self.pattern.as_deref()
    }

    /// The page size, with zero read as the default of 100.
    pub(crate) fn page(&self) -> usize {
        if self.page_size == 0 {
            DEFAULT_PAGE
        } else {
            self.page_size
        }
    }
}

/// Streams the entries `scan` asks for, cursor after cursor until the server says the scan is complete.
pub(crate) fn hscan<'a, K, V, C>(
    key: &'a Key,
    codec: &'a C,
    scan: MapScan,
) -> impl Stream<Item = Result<(K, V)>> + 'a
where
    K: DeserializeOwned + 'a,
    V: DeserializeOwned + 'a,
    C: Codec,
{
    let page = i64::try_from(scan.page()).unwrap_or(i64::MAX);
    stream::try_unfold(
        (Some(Bytes::from_static(b"0")), scan.pattern),
        move |(cursor, pattern)| async move {
            let Some(cursor) = cursor else {
                return Ok::<_, Error>(None);
            };
            let mut args = vec![Arg::from(key.redis_key()), Arg::from(cursor)];
            if let Some(pattern) = &pattern {
                args.push(Arg::from("MATCH"));
                args.push(Arg::from(pattern.clone()));
            }
            args.push(Arg::from("COUNT"));
            args.push(Arg::from(page));
            let reply = key.command("HSCAN", args).await?;
            let (next, page): (Bytes, redis::Value) =
                redis::FromRedisValue::from_redis_value(reply)?;
            let next = (next.as_ref() != b"0").then_some(next);
            let items: std::vec::Vec<(K, V)> = decode_pairs(codec, page)?;
            Ok(Some((
                stream::iter(items.into_iter().map(Ok)),
                (next, pattern),
            )))
        },
    )
    .try_flatten()
}

/// The pairs of an `HGETALL`, `HSCAN` or `HRANDFIELD … WITHVALUES` reply: a map or an array of pairs in RESP3, a flat array of fields and values in RESP2.
pub(crate) fn decode_pairs<K, V, C>(codec: &C, raw: redis::Value) -> Result<std::vec::Vec<(K, V)>>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
    C: Codec,
{
    let flat: std::vec::Vec<Bytes> = match raw {
        redis::Value::Map(map) => map
            .into_iter()
            .flat_map(|(field, value)| [field, value])
            .map(redis::FromRedisValue::from_redis_value)
            .collect::<std::result::Result<_, _>>()?,
        redis::Value::Array(items)
            if items
                .iter()
                .all(|item| matches!(item, redis::Value::Array(_))) =>
        {
            let mut flat = std::vec::Vec::with_capacity(items.len() * 2);
            for item in items {
                let (field, value): (Bytes, Bytes) = redis::FromRedisValue::from_redis_value(item)?;
                flat.push(field);
                flat.push(value);
            }
            flat
        }
        other => redis::FromRedisValue::from_redis_value(other)?,
    };
    flat.as_chunks::<2>()
        .0
        .iter()
        .map(|[field, value]| Ok((codec.decode(field)?, codec.decode(value)?)))
        .collect()
}
