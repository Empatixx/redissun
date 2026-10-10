use super::*;
use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::object::millis as to_millis;
use crate::reply::{int, malformed, number, text};
use fred::types::Value;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use std::marker::PhantomData;
use std::time::Duration;

macro_rules! typed_view {
    ($name:ident<$($generic:ident),+>, $doc:literal) => {
        #[doc = $doc]
        pub struct $name<'a, $($generic,)+ C: Codec> {
            batch: &'a Batch<C>,
            name: String,
            _marker: PhantomData<fn() -> ($($generic,)+)>,
        }

        impl<'a, $($generic,)+ C: Codec> $name<'a, $($generic,)+ C> {
            pub(super) fn new(batch: &'a Batch<C>, name: String) -> Self {
                Self { batch, name, _marker: PhantomData }
            }

            fn key(&self) -> Value {
                Value::from(self.name.clone())
            }
        }
    };
}

fn lua(script: &fred::types::scripts::Script) -> Value {
    Value::from(
        script
            .lua()
            .map(|source| source.to_string())
            .unwrap_or_default(),
    )
}

typed_view!(BatchHashMap<K, V>, "A [`HashMap`](crate::HashMap) whose methods queue commands in a [`Batch`].");

impl<'a, K, V, C> BatchHashMap<'a, K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
    C: Codec,
{
    /// Queues an insert; the handle gives the replaced value.
    pub fn insert<Q, W>(&self, k: &Q, v: &W) -> BatchFuture<Option<V>>
    where
        K: Borrow<Q>,
        V: Borrow<W>,
        Q: Serialize + ?Sized + Sync,
        W: Serialize + ?Sized + Sync,
    {
        let args = (|| {
            Ok(vec![
                lua(&crate::collections::hash_map::INSERT),
                Value::Integer(1),
                self.key(),
                Value::Bytes(self.batch.codec.encode(k)?),
                Value::Bytes(self.batch.codec.encode(v)?),
            ])
        })();
        let codec = self.batch.codec.clone();
        self.batch
            .queue("EVAL", args, 2, move |reply| decode_optional(&codec, reply))
    }

    /// Queues a read; the handle gives the value.
    pub fn get<Q>(&self, k: &Q) -> BatchFuture<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let args = self
            .batch
            .codec
            .encode(k)
            .map(|field| vec![self.key(), Value::Bytes(field)]);
        let codec = self.batch.codec.clone();
        self.batch
            .queue("HGET", args, 0, move |reply| decode_optional(&codec, reply))
    }

    /// Queues a removal; the handle gives the removed value.
    pub fn remove<Q>(&self, k: &Q) -> BatchFuture<Option<V>>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let args = self.batch.codec.encode(k).map(|field| {
            vec![
                lua(&crate::collections::hash_map::REMOVE),
                Value::Integer(1),
                self.key(),
                Value::Bytes(field),
            ]
        });
        let codec = self.batch.codec.clone();
        self.batch
            .queue("EVAL", args, 2, move |reply| decode_optional(&codec, reply))
    }

    /// Queues a key check.
    pub fn contains_key<Q>(&self, k: &Q) -> BatchFuture<bool>
    where
        K: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let args = self
            .batch
            .codec
            .encode(k)
            .map(|field| vec![self.key(), Value::Bytes(field)]);
        self.batch
            .queue("HEXISTS", args, 0, |reply| Ok(number(reply) == Some(1)))
    }

    /// Queues a length query.
    pub fn len(&self) -> BatchFuture<usize> {
        self.batch
            .queue("HLEN", Ok(vec![self.key()]), 0, |reply| Ok(whole(reply)))
    }
}

typed_view!(
    BatchBucket<V>,
    "A [`Bucket`](crate::Bucket) whose methods queue commands in a [`Batch`]."
);

impl<'a, V, C> BatchBucket<'a, V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
    C: Codec,
{
    /// Queues a write.
    pub fn set<Q>(&self, value: &Q) -> BatchFuture<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let args = self
            .batch
            .codec
            .encode(value)
            .map(|raw| vec![self.key(), Value::Bytes(raw)]);
        self.batch.queue("SET", args, 0, |_| Ok(()))
    }

    /// Queues a write that expires after `ttl`.
    pub fn set_ex<Q>(&self, value: &Q, ttl: Duration) -> BatchFuture<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let args = (|| {
            Ok(vec![
                self.key(),
                Value::Bytes(self.batch.codec.encode(value)?),
                Value::from("PX"),
                Value::Integer(to_millis(ttl)?),
            ])
        })();
        self.batch.queue("SET", args, 0, |_| Ok(()))
    }

    /// Queues a read.
    pub fn get(&self) -> BatchFuture<Option<V>> {
        let codec = self.batch.codec.clone();
        self.batch
            .queue("GET", Ok(vec![self.key()]), 0, move |reply| {
                decode_optional(&codec, reply)
            })
    }
}

typed_view!(
    BatchHashSet<V>,
    "A [`HashSet`](crate::HashSet) whose methods queue commands in a [`Batch`]."
);

impl<'a, V, C> BatchHashSet<'a, V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    fn member_command<Q>(&self, name: &'static str, v: &Q) -> BatchFuture<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let args = self
            .batch
            .codec
            .encode(v)
            .map(|raw| vec![self.key(), Value::Bytes(raw)]);
        self.batch
            .queue(name, args, 0, |reply| Ok(number(reply) == Some(1)))
    }

    /// Queues an insert; the handle says whether the value was new.
    pub fn insert<Q>(&self, v: &Q) -> BatchFuture<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.member_command("SADD", v)
    }

    /// Queues a removal; the handle says whether the value was there.
    pub fn remove<Q>(&self, v: &Q) -> BatchFuture<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.member_command("SREM", v)
    }

    /// Queues a membership check.
    pub fn contains<Q>(&self, v: &Q) -> BatchFuture<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.member_command("SISMEMBER", v)
    }

    /// Queues a length query.
    pub fn len(&self) -> BatchFuture<usize> {
        self.batch
            .queue("SCARD", Ok(vec![self.key()]), 0, |reply| Ok(whole(reply)))
    }
}

/// An [`AtomicI64`](crate::AtomicI64) whose methods queue commands in a [`Batch`].
pub struct BatchAtomicI64<'a, C: Codec> {
    pub(super) batch: &'a Batch<C>,
    pub(super) name: String,
}

impl<C: Codec> BatchAtomicI64<'_, C> {
    fn key(&self) -> Value {
        Value::from(self.name.clone())
    }

    fn integer(&self, name: &'static str, extra: Option<i64>) -> BatchFuture<i64> {
        let mut args = vec![self.key()];
        args.extend(extra.map(Value::Integer));
        self.batch
            .queue(name, Ok(args), 0, |reply| Ok(number(reply).unwrap_or(0)))
    }

    /// Queues a read; a missing counter reads as 0.
    pub fn get(&self) -> BatchFuture<i64> {
        self.batch.queue("GET", Ok(vec![self.key()]), 0, |reply| {
            Ok(number(reply).unwrap_or(0))
        })
    }

    /// Queues a write.
    pub fn set(&self, value: i64) -> BatchFuture<()> {
        let args = Ok(vec![self.key(), Value::Integer(value)]);
        self.batch.queue("SET", args, 0, |_| Ok(()))
    }

    /// Queues adding 1; the handle gives the new value.
    pub fn incr(&self) -> BatchFuture<i64> {
        self.integer("INCR", None)
    }

    /// Queues subtracting 1; the handle gives the new value.
    pub fn decr(&self) -> BatchFuture<i64> {
        self.integer("DECR", None)
    }

    /// Queues adding `delta`; the handle gives the new value.
    pub fn add_and_get(&self, delta: i64) -> BatchFuture<i64> {
        self.integer("INCRBY", Some(delta))
    }
}

typed_view!(
    BatchVec<V>,
    "A [`Vec`](crate::Vec) whose methods queue commands in a [`Batch`]."
);

impl<'a, V, C> BatchVec<'a, V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
    C: Codec,
{
    /// Queues adding a value at the end.
    pub fn push<Q>(&self, v: &Q) -> BatchFuture<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let args = self
            .batch
            .codec
            .encode(v)
            .map(|raw| vec![self.key(), Value::Bytes(raw)]);
        self.batch.queue("RPUSH", args, 0, |_| Ok(()))
    }

    /// Queues a read of the value at `index`.
    pub fn get(&self, index: usize) -> BatchFuture<Option<V>> {
        let args = Ok(vec![self.key(), int(index)]);
        let codec = self.batch.codec.clone();
        self.batch.queue("LINDEX", args, 0, move |reply| {
            decode_optional(&codec, reply)
        })
    }

    /// Queues a length query.
    pub fn len(&self) -> BatchFuture<usize> {
        self.batch
            .queue("LLEN", Ok(vec![self.key()]), 0, |reply| Ok(whole(reply)))
    }
}

typed_view!(
    BatchVecDeque<V>,
    "A [`VecDeque`](crate::VecDeque) whose methods queue commands in a [`Batch`]."
);

impl<'a, V, C> BatchVecDeque<'a, V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
    C: Codec,
{
    fn push_command<Q>(&self, name: &'static str, v: &Q) -> BatchFuture<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let args = self
            .batch
            .codec
            .encode(v)
            .map(|raw| vec![self.key(), Value::Bytes(raw)]);
        self.batch.queue(name, args, 0, |_| Ok(()))
    }

    fn pop_command(&self, name: &'static str) -> BatchFuture<Option<V>> {
        let codec = self.batch.codec.clone();
        self.batch
            .queue(name, Ok(vec![self.key()]), 0, move |reply| {
                decode_optional(&codec, reply)
            })
    }

    /// Queues adding a value at the back.
    pub fn push_back<Q>(&self, v: &Q) -> BatchFuture<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.push_command("RPUSH", v)
    }

    /// Queues adding a value at the front.
    pub fn push_front<Q>(&self, v: &Q) -> BatchFuture<()>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.push_command("LPUSH", v)
    }

    /// Queues removing the front value; the handle gives it.
    pub fn pop_front(&self) -> BatchFuture<Option<V>> {
        self.pop_command("LPOP")
    }

    /// Queues removing the back value; the handle gives it.
    pub fn pop_back(&self) -> BatchFuture<Option<V>> {
        self.pop_command("RPOP")
    }

    /// Queues a length query.
    pub fn len(&self) -> BatchFuture<usize> {
        self.batch
            .queue("LLEN", Ok(vec![self.key()]), 0, |reply| Ok(whole(reply)))
    }
}

typed_view!(
    BatchSortedSet<V>,
    "A [`SortedSet`](crate::SortedSet) whose methods queue commands in a [`Batch`]."
);

impl<'a, V, C> BatchSortedSet<'a, V, C>
where
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    fn member_command<T: Send + 'static>(
        &self,
        name: &'static str,
        v: &impl Serialize,
        extra: Option<f64>,
        decode: impl FnOnce(&Value) -> Result<T> + Send + 'static,
    ) -> BatchFuture<T> {
        let args = self.batch.codec.encode(v).map(|raw| {
            let mut args = vec![self.key()];
            args.extend(extra.map(|score| Value::from(score.to_string())));
            args.push(Value::Bytes(raw));
            args
        });
        self.batch.queue(name, args, 0, decode)
    }

    /// Queues adding a value with a score; the handle says whether the value was new.
    pub fn insert<Q>(&self, v: &Q, score: f64) -> BatchFuture<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if score.is_nan() {
            return self.batch.queue(
                "ZADD",
                Err(Error::Config("a score cannot be NaN".into())),
                0,
                |_| Ok(false),
            );
        }
        self.member_command(
            "ZADD",
            &v,
            Some(score),
            |reply| Ok(number(reply) == Some(1)),
        )
    }

    /// Queues adding `delta` to a score; the handle gives the new score.
    pub fn add_score<Q>(&self, v: &Q, delta: f64) -> BatchFuture<f64>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        if delta.is_nan() {
            return self.batch.queue(
                "ZINCRBY",
                Err(Error::Config("a score cannot be NaN".into())),
                0,
                |_| Ok(0.0),
            );
        }
        let args = self.batch.codec.encode(v).map(|raw| {
            vec![
                self.key(),
                Value::from(delta.to_string()),
                Value::Bytes(raw),
            ]
        });
        self.batch.queue("ZINCRBY", args, 0, |reply| {
            text(reply)
                .and_then(|score| score.parse().ok())
                .ok_or_else(malformed)
        })
    }

    /// Queues a score query.
    pub fn score<Q>(&self, v: &Q) -> BatchFuture<Option<f64>>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.member_command("ZSCORE", &v, None, |reply| {
            Ok(text(reply).and_then(|score| score.parse().ok()))
        })
    }

    /// Queues a removal; the handle says whether the value was there.
    pub fn remove<Q>(&self, v: &Q) -> BatchFuture<bool>
    where
        V: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        self.member_command("ZREM", &v, None, |reply| Ok(number(reply) == Some(1)))
    }

    /// Queues a length query.
    pub fn len(&self) -> BatchFuture<usize> {
        self.batch
            .queue("ZCARD", Ok(vec![self.key()]), 0, |reply| Ok(whole(reply)))
    }
}

typed_view!(
    BatchTopic<M>,
    "A [`Topic`](crate::Topic) whose publish queues a command in a [`Batch`]."
);

impl<'a, M, C> BatchTopic<'a, M, C>
where
    M: Serialize + Send + Sync,
    C: Codec,
{
    /// Queues a publish; the handle gives the number of receivers.
    pub fn publish<Q>(&self, message: &Q) -> BatchFuture<usize>
    where
        M: Borrow<Q>,
        Q: Serialize + ?Sized + Sync,
    {
        let args = self
            .batch
            .codec
            .encode(message)
            .map(|raw| vec![self.key(), Value::Bytes(raw)]);
        self.batch
            .queue("PUBLISH", args, 0, |reply| Ok(whole(reply)))
    }
}
