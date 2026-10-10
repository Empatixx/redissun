use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::object::Key;
use bytes::Bytes;
use fred::interfaces::{KeysInterface, ListInterface};
use fred::types::scripts::Script;
use futures::{stream, Stream, TryStreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::LazyLock;

const PAGE: i64 = 100;

static POP_FRONT_MANY: LazyLock<Script> = LazyLock::new(|| pop_many_script("lpop"));

static POP_BACK_MANY: LazyLock<Script> = LazyLock::new(|| pop_many_script("rpop"));

static DRAIN: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local vals = redis.call('lrange', KEYS[1], 0, -1)
        redis.call('del', KEYS[1])
        return vals",
    )
});

static DRAIN_UP_TO: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local elemNum = math.min(ARGV[1], redis.call('llen', KEYS[1])) - 1
        local vals = redis.call('lrange', KEYS[1], 0, elemNum)
        redis.call('ltrim', KEYS[1], elemNum + 1, -1)
        return vals",
    )
});

fn pop_many_script(command: &str) -> Script {
    Script::from_lua(format!(
        "local result = {{}}
        for i = 1, ARGV[1], 1 do
            local value = redis.call('{command}', KEYS[1])
            if value ~= false then
                table.insert(result, value)
            else
                return result
            end
        end
        return result"
    ))
}

pub(crate) fn decode_all<V, C>(codec: &C, raw: &[Bytes]) -> Result<Vec<V>>
where
    V: DeserializeOwned,
    C: Codec,
{
    raw.iter().map(|bytes| codec.decode(bytes)).collect()
}

pub(crate) fn encode_all<'a, Q, C>(
    codec: &C,
    values: impl IntoIterator<Item = &'a Q>,
) -> Result<Vec<Bytes>>
where
    Q: Serialize + ?Sized + 'a,
    C: Codec,
{
    values.into_iter().map(|v| codec.encode(v)).collect()
}

pub(crate) async fn pop_many<V, C>(
    key: &Key,
    codec: &C,
    front: bool,
    limit: usize,
) -> Result<Vec<V>>
where
    V: DeserializeOwned,
    C: Codec,
{
    if limit == 0 {
        return Ok(Vec::new());
    }
    let script = if front {
        &POP_FRONT_MANY
    } else {
        &POP_BACK_MANY
    };
    let raw: Vec<Bytes> = key
        .core
        .eval_no_retry(
            script,
            vec![key.redis_key()],
            vec![Bytes::from(limit.to_string())],
        )
        .await?;
    decode_all(codec, &raw)
}

pub(crate) async fn drain<V, C>(key: &Key, codec: &C, max: Option<usize>) -> Result<Vec<V>>
where
    V: DeserializeOwned,
    C: Codec,
{
    let raw: Vec<Bytes> = match max {
        None => {
            key.core
                .eval(&DRAIN, vec![key.redis_key()], Vec::new())
                .await?
        }
        Some(0) => Vec::new(),
        Some(max) => {
            key.core
                .eval(
                    &DRAIN_UP_TO,
                    vec![key.redis_key()],
                    vec![Bytes::from(max.to_string())],
                )
                .await?
        }
    };
    decode_all(codec, &raw)
}

pub(crate) async fn read_all<V, C>(key: &Key, codec: &C) -> Result<Vec<V>>
where
    V: DeserializeOwned,
    C: Codec,
{
    let raw: Vec<Bytes> = key.core.redis().lrange(key.redis_key(), 0, -1).await?;
    decode_all(codec, &raw)
}

pub(crate) async fn position<Q, C>(key: &Key, codec: &C, v: &Q, last: bool) -> Result<Option<usize>>
where
    Q: Serialize + ?Sized,
    C: Codec,
{
    let index: Option<i64> = key
        .core
        .redis()
        .lpos(
            key.redis_key(),
            codec.encode(v)?,
            last.then_some(-1),
            None,
            None,
        )
        .await?;
    Ok(index.map(|index| index as usize))
}

pub(crate) async fn remove_matching<Q, C>(key: &Key, codec: &C, v: &Q, count: i64) -> Result<usize>
where
    Q: Serialize + ?Sized,
    C: Codec,
{
    let removed: usize = key
        .core
        .redis()
        .lrem(key.redis_key(), count, codec.encode(v)?)
        .await?;
    Ok(removed)
}

pub(crate) async fn len(key: &Key) -> Result<usize> {
    let len: usize = key.core.redis().llen(key.redis_key()).await?;
    Ok(len)
}

pub(crate) async fn clear(key: &Key) -> Result<()> {
    key.core.redis().del::<(), _>(key.redis_key()).await?;
    Ok(())
}

pub(crate) fn pages<'a, V, C>(key: &'a Key, codec: &'a C) -> impl Stream<Item = Result<V>> + 'a
where
    V: DeserializeOwned + 'a,
    C: Codec,
{
    paged(key, codec, false)
}

pub(crate) fn pages_rev<'a, V, C>(key: &'a Key, codec: &'a C) -> impl Stream<Item = Result<V>> + 'a
where
    V: DeserializeOwned + 'a,
    C: Codec,
{
    paged(key, codec, true)
}

fn paged<'a, V, C>(key: &'a Key, codec: &'a C, rev: bool) -> impl Stream<Item = Result<V>> + 'a
where
    V: DeserializeOwned + 'a,
    C: Codec,
{
    stream::try_unfold(Some(0i64), move |state| async move {
        let Some(offset) = state else {
            return Ok::<_, Error>(None);
        };
        let (start, stop) = if rev {
            (-(offset + PAGE), -(offset + 1))
        } else {
            (offset, offset + PAGE - 1)
        };
        let raw: Vec<Bytes> = key
            .core
            .redis()
            .lrange(key.redis_key(), start, stop)
            .await?;
        if raw.is_empty() {
            return Ok(None);
        }
        let next = (raw.len() as i64 == PAGE).then_some(offset + PAGE);
        let mut items = decode_all::<V, C>(codec, &raw)?;
        if rev {
            items.reverse();
        }
        Ok(Some((stream::iter(items.into_iter().map(Ok)), next)))
    })
    .try_flatten()
}
