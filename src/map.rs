use crate::codec::Codec;
use crate::error::Result;
use crate::object::{HasKey, Key};
use bytes::Bytes;
use fred::interfaces::{HashesInterface, KeysInterface};
use fred::types::scan::Scanner;
use fred::types::scripts::Script;
use futures::{stream, Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::marker::PhantomData;
use std::sync::LazyLock;

static INSERT: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local previous = redis.call('HGET', KEYS[1], ARGV[1])
        redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
        return previous",
    )
});

static REMOVE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local previous = redis.call('HGET', KEYS[1], ARGV[1])
        redis.call('HDEL', KEYS[1], ARGV[1])
        return previous",
    )
});

const SCAN_PAGE: u32 = 100;

pub struct Map<K, V, C: Codec> {
    key: Key,
    codec: C,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V, C: Codec> Clone for Map<K, V, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            codec: self.codec.clone(),
            _marker: PhantomData,
        }
    }
}

impl<K, V, C: Codec> HasKey for Map<K, V, C> {
    fn key(&self) -> &Key {
        &self.key
    }
}

impl<K, V, C: Codec> Map<K, V, C> {
    pub(crate) fn new(key: Key, codec: C) -> Self {
        Self {
            key,
            codec,
            _marker: PhantomData,
        }
    }
}

impl<K, V, C> Map<K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    fn decode_value(&self, raw: Option<Bytes>) -> Result<Option<V>> {
        raw.map(|bytes| self.codec.decode(&bytes)).transpose()
    }

    pub async fn insert(&self, k: K, v: V) -> Result<Option<V>> {
        let previous: Option<Bytes> = self
            .key
            .core
            .eval(
                &INSERT,
                vec![self.key.redis_key()],
                vec![self.codec.encode(&k)?, self.codec.encode(&v)?],
            )
            .await?;
        self.decode_value(previous)
    }

    pub async fn get<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let raw: Option<Bytes> = self
            .key
            .core
            .redis()
            .hget(self.key.redis_key(), self.codec.encode(k)?)
            .await?;
        self.decode_value(raw)
    }

    pub async fn remove<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let previous: Option<Bytes> = self
            .key
            .core
            .eval(
                &REMOVE,
                vec![self.key.redis_key()],
                vec![self.codec.encode(k)?],
            )
            .await?;
        self.decode_value(previous)
    }

    pub async fn contains_key<Q>(&self, k: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let found: bool = self
            .key
            .core
            .redis()
            .hexists(self.key.redis_key(), self.codec.encode(k)?)
            .await?;
        Ok(found)
    }

    pub async fn len(&self) -> Result<usize> {
        let len: usize = self.key.core.redis().hlen(self.key.redis_key()).await?;
        Ok(len)
    }

    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    pub async fn clear(&self) -> Result<()> {
        self.key
            .core
            .redis()
            .del::<(), _>(self.key.redis_key())
            .await?;
        Ok(())
    }

    pub async fn extend(&self, entries: impl IntoIterator<Item = (K, V)>) -> Result<()> {
        let encoded = entries
            .into_iter()
            .map(|(k, v)| Ok((self.codec.encode(&k)?, self.codec.encode(&v)?)))
            .collect::<Result<Vec<(Bytes, Bytes)>>>()?;
        if encoded.is_empty() {
            return Ok(());
        }
        self.key
            .core
            .redis()
            .hset::<(), _, _>(self.key.redis_key(), encoded)
            .await?;
        Ok(())
    }

    pub async fn get_many<Q>(&self, ks: &[&Q]) -> Result<Vec<Option<V>>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if ks.is_empty() {
            return Ok(Vec::new());
        }
        let fields = ks
            .iter()
            .map(|k| self.codec.encode(*k))
            .collect::<Result<Vec<Bytes>>>()?;
        let raw: Vec<Option<Bytes>> = self
            .key
            .core
            .redis()
            .hmget(self.key.redis_key(), fields)
            .await?;
        raw.into_iter().map(|r| self.decode_value(r)).collect()
    }

    pub async fn insert_nx(&self, k: K, v: V) -> Result<bool> {
        let inserted: bool = self
            .key
            .core
            .redis()
            .hsetnx(
                self.key.redis_key(),
                self.codec.encode(&k)?,
                self.codec.encode(&v)?,
            )
            .await?;
        Ok(inserted)
    }

    pub async fn incr_by<Q>(&self, k: &Q, delta: i64) -> Result<i64>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let value: i64 = self
            .key
            .core
            .redis()
            .hincrby(self.key.redis_key(), self.codec.encode(k)?, delta)
            .await?;
        Ok(value)
    }

    pub async fn incr_by_float<Q>(&self, k: &Q, delta: f64) -> Result<f64>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let value: f64 = self
            .key
            .core
            .redis()
            .hincrbyfloat(self.key.redis_key(), self.codec.encode(k)?, delta)
            .await?;
        Ok(value)
    }

    pub fn iter(&self) -> impl Stream<Item = Result<(K, V)>> + '_ {
        let pages = Box::pin(self.key.core.redis().hscan(
            self.key.redis_key(),
            "*",
            Some(SCAN_PAGE),
        ));
        pages.flat_map(move |page| {
            let items: Vec<Result<(K, V)>> = match page {
                Ok(mut page) => {
                    let results = page.take_results();
                    page.next();
                    results
                        .map(|map| {
                            map.inner()
                                .iter()
                                .map(|(field, value)| {
                                    let value: Bytes = value.clone().convert()?;
                                    Ok((
                                        self.codec.decode(field.as_bytes())?,
                                        self.codec.decode(&value)?,
                                    ))
                                })
                                .collect()
                        })
                        .unwrap_or_default()
                }
                Err(error) => vec![Err(error.into())],
            };
            stream::iter(items)
        })
    }

    pub fn keys(&self) -> impl Stream<Item = Result<K>> + '_ {
        self.iter().map(|entry| entry.map(|(k, _)| k))
    }

    pub fn values(&self) -> impl Stream<Item = Result<V>> + '_ {
        self.iter().map(|entry| entry.map(|(_, v)| v))
    }
}
