//! Read-through: Redisson's `loadValue` and `loadAll`, for every map through [`Target`].

use crate::codec::Codec;
use crate::collections::map::entry::Target;
use crate::collections::map::ops::KeyLock;
use crate::collections::map::store::Store;
use crate::error::{Error, Result};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::pin::Pin;

/// Redisson's `loadValue`: takes the key's lock, returns a present value unless `replace`, else asks the loader and stores a `Some` value with the map's plain put, without the writer.
pub(crate) async fn load_value<K, V, C>(
    target: &Target<'_, K, V, C>,
    k: &Bytes,
    replace: bool,
) -> Result<Option<V>>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    let lock = KeyLock::acquire(target.key(), k).await?;
    let body = load_value_locked(target, k, replace).await;
    lock.release(body).await
}

/// [`load_value`] for a caller that already holds the key's lock, such as an entry.
pub(crate) async fn load_value_locked<K, V, C>(
    target: &Target<'_, K, V, C>,
    k: &Bytes,
    replace: bool,
) -> Result<Option<V>>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    if !replace {
        if let Some(present) = target.read(k).await? {
            return Ok(Some(present));
        }
    }
    let Some(store) = target.store() else {
        return Ok(None);
    };
    let loaded = store.load(k).await?;
    if let Some(value) = &loaded {
        target.write_loaded(k, value).await?;
    }
    Ok(loaded)
}

/// The load of [`HashMap::load_all`](crate::HashMap::load_all) and [`load_many`](crate::HashMap::load_many); await it to run. It stops at the first error.
#[must_use = "a load does nothing until it is awaited"]
pub struct LoadAll<'m, K, V, C: Codec> {
    target: Target<'m, K, V, C>,
    /// The encoded keys to load; `None` loads every key of the loader.
    keys: Option<Result<Vec<Bytes>>>,
    replace: bool,
    parallelism: usize,
}

impl<'m, K, V, C: Codec> LoadAll<'m, K, V, C> {
    pub(crate) fn new(target: Target<'m, K, V, C>, keys: Option<Result<Vec<Bytes>>>) -> Self {
        Self {
            target,
            keys,
            replace: false,
            parallelism: 1,
        }
    }

    /// Stores the loaded value also for keys the map holds already, Redisson's `replaceExistingValues = true`.
    #[doc(alias = "replaceExistingValues")]
    pub fn replace_existing(mut self) -> Self {
        self.replace = true;
        self
    }

    /// Loads up to `n` keys at once, 1 by default. Zero is a configuration error.
    pub fn parallelism(mut self, n: usize) -> Self {
        self.parallelism = n;
        self
    }
}

impl<K, V, C: Codec> fmt::Debug for LoadAll<'_, K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoadAll")
            .field("all", &self.keys.is_none())
            .field("replace", &self.replace)
            .field("parallelism", &self.parallelism)
            .finish_non_exhaustive()
    }
}

fn loader_of<'a, K, V, C>(target: &'a Target<'_, K, V, C>) -> Result<&'a Store<K, V, C>>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    target
        .store()
        .filter(|store| store.has_loader())
        .ok_or_else(|| Error::Config("this map has no loader".into()))
}

impl<'m, K, V, C> IntoFuture for LoadAll<'m, K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync + 'm,
    V: Serialize + DeserializeOwned + Send + Sync + 'm,
    C: Codec,
{
    type Output = Result<()>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'm>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            if self.parallelism == 0 {
                return Err(Error::Config("parallelism must be at least 1".into()));
            }
            let target = &self.target;
            let store = loader_of(target)?;
            let replace = self.replace;
            let load = |k: Bytes| async move { load_value(target, &k, replace).await.map(drop) };
            match self.keys {
                Some(keys) => {
                    futures::stream::iter(keys?.into_iter().map(Ok))
                        .try_for_each_concurrent(self.parallelism, load)
                        .await
                }
                None => {
                    store
                        .load_all_keys()?
                        .map(|key| key.and_then(|key| store.encode_key(&key)))
                        .try_for_each_concurrent(self.parallelism, load)
                        .await
                }
            }
        })
    }
}
