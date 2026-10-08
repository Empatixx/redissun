use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::object::Key;
use bytes::Bytes;
use fred::interfaces::ListInterface;
use futures::{stream, Stream, TryStreamExt};
use serde::de::DeserializeOwned;

const PAGE: i64 = 100;

pub(crate) fn pages<'a, V, C>(key: &'a Key, codec: &'a C) -> impl Stream<Item = Result<V>> + 'a
where
    V: DeserializeOwned + 'a,
    C: Codec,
{
    stream::try_unfold(Some(0i64), move |state| async move {
        let Some(offset) = state else {
            return Ok::<_, Error>(None);
        };
        let raw: std::vec::Vec<Bytes> = key
            .core
            .redis()
            .lrange(key.redis_key(), offset, offset + PAGE - 1)
            .await?;
        if raw.is_empty() {
            return Ok(None);
        }
        let next = (raw.len() as i64 == PAGE).then_some(offset + PAGE);
        let items = raw
            .iter()
            .map(|bytes| codec.decode(bytes))
            .collect::<Result<std::vec::Vec<V>>>()?;
        Ok(Some((stream::iter(items.into_iter().map(Ok)), next)))
    })
    .try_flatten()
}
