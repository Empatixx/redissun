//! The multimap streams, Redisson's `keySet`, `values` and `entries` iterators (`RedissonMultiMapIterator`).
//!
//! The hash is read with `HSCAN … COUNT n`. For the values, each key's list is read with `LRANGE` in pages of 100 and each key's set with `SSCAN … COUNT n`, from the values name built from the hash value. Keys whose values are empty yield nothing, as in Redisson.

use super::{values_name, Shape};
use crate::client::request::Arg;
use crate::codec::Codec;
use crate::collections::map::scan::raw_pairs;
use crate::error::{Error, Result};
use crate::object::Key;
use bytes::Bytes;
use futures::{stream, Stream, StreamExt, TryStreamExt};
use serde::de::DeserializeOwned;

/// Redisson's default page of the multimap views.
const DEFAULT_PAGE: usize = 10;

/// The page of `LRANGE` over one key's list, as `RedissonList.iterator()` reads it.
const LIST_PAGE: i64 = 100;

/// The `COUNT` of a page size, with zero read as the default of 10.
fn count(page_size: usize) -> i64 {
    let page = if page_size == 0 {
        DEFAULT_PAGE
    } else {
        page_size
    };
    i64::try_from(page).unwrap_or(i64::MAX)
}

/// The hash entries: each encoded key with the hash of it, as stored.
fn key_entries(key: &Key, count: i64) -> impl Stream<Item = Result<(Bytes, Bytes)>> + '_ {
    stream::try_unfold(Some(Bytes::from_static(b"0")), move |cursor| async move {
        let Some(cursor) = cursor else {
            return Ok::<_, Error>(None);
        };
        let reply = key
            .command(
                "HSCAN",
                vec![
                    Arg::from(key.redis_key()),
                    Arg::from(cursor),
                    Arg::from("COUNT"),
                    Arg::from(count),
                ],
            )
            .await?;
        let (next, page): (Bytes, redis::Value) = redis::FromRedisValue::from_redis_value(reply)?;
        let next = (next.as_ref() != b"0").then_some(next);
        let pairs = raw_pairs(page)?;
        Ok(Some((stream::iter(pairs.into_iter().map(Ok)), next)))
    })
    .try_flatten()
}

/// Where the read of one key's values continues.
enum Cursor {
    /// The next `LRANGE` start.
    Index(i64),
    /// The next `SSCAN` cursor.
    Scan(Bytes),
}

/// The encoded values of the key whose hash is `key_hash`.
fn values_of(
    map: &Key,
    shape: Shape,
    key_hash: &[u8],
    count: i64,
) -> impl Stream<Item = Result<Bytes>> + 'static {
    let values = Key::new(
        map.core.clone(),
        values_name(map.name(), &String::from_utf8_lossy(key_hash)),
    );
    let start = match shape {
        Shape::List => Cursor::Index(0),
        Shape::Set => Cursor::Scan(Bytes::from_static(b"0")),
    };
    stream::try_unfold((values, Some(start)), move |(values, cursor)| async move {
        let Some(cursor) = cursor else {
            return Ok::<_, Error>(None);
        };
        let (page, next): (Vec<Bytes>, Option<Cursor>) = match cursor {
            Cursor::Index(start) => {
                let page: Vec<Bytes> = values
                    .query({
                        let mut cmd = values.key_cmd("LRANGE");
                        cmd.arg(start).arg(start + LIST_PAGE - 1);
                        cmd
                    })
                    .await?;
                let full = i64::try_from(page.len()).unwrap_or(i64::MAX) == LIST_PAGE;
                (page, full.then_some(Cursor::Index(start + LIST_PAGE)))
            }
            Cursor::Scan(cursor) => {
                let reply = values
                    .command(
                        "SSCAN",
                        vec![
                            Arg::from(values.redis_key()),
                            Arg::from(cursor),
                            Arg::from("COUNT"),
                            Arg::from(count),
                        ],
                    )
                    .await?;
                let (next, page): (Bytes, Vec<Bytes>) =
                    redis::FromRedisValue::from_redis_value(reply)?;
                (page, (next.as_ref() != b"0").then_some(Cursor::Scan(next)))
            }
        };
        Ok(Some((
            stream::iter(page.into_iter().map(Ok)),
            (values, next),
        )))
    })
    .try_flatten()
}

/// Every encoded `(key, value)` pair.
fn raw_entries(
    map: &Key,
    shape: Shape,
    page_size: usize,
) -> impl Stream<Item = Result<(Bytes, Bytes)>> + '_ {
    let count = count(page_size);
    key_entries(map, count)
        .map_ok(move |(k, key_hash)| {
            values_of(map, shape, &key_hash, count).map_ok(move |v| (k.clone(), v))
        })
        .try_flatten()
}

/// Redisson's `keySet(count).iterator()`: every key of the hash.
pub(crate) fn keys<'a, K, C>(
    map: &'a Key,
    codec: &'a C,
    page_size: usize,
) -> impl Stream<Item = Result<K>> + 'a
where
    K: DeserializeOwned + 'a,
    C: Codec,
{
    key_entries(map, count(page_size)).map(move |entry| codec.decode(&entry?.0))
}

/// Redisson's `values(count).iterator()`: every value of every key.
pub(crate) fn values<'a, V, C>(
    map: &'a Key,
    codec: &'a C,
    shape: Shape,
    page_size: usize,
) -> impl Stream<Item = Result<V>> + 'a
where
    V: DeserializeOwned + 'a,
    C: Codec,
{
    raw_entries(map, shape, page_size).map(move |entry| codec.decode(&entry?.1))
}

/// Redisson's `entries(count).iterator()`: every `(key, value)` pair.
pub(crate) fn entries<'a, K, V, C>(
    map: &'a Key,
    codec: &'a C,
    shape: Shape,
    page_size: usize,
) -> impl Stream<Item = Result<(K, V)>> + 'a
where
    K: DeserializeOwned + 'a,
    V: DeserializeOwned + 'a,
    C: Codec,
{
    raw_entries(map, shape, page_size).map(move |entry| {
        let (k, v) = entry?;
        Ok((codec.decode(&k)?, codec.decode(&v)?))
    })
}
