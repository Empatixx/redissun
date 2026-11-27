//! The loader and writer every map can take: Redisson's `MapOptions` store features.
//!
//! The public traits return `impl Future`/`impl Stream`, so they are not dyn-compatible. Each has a private, object-safe twin ([`ErasedLoader`], [`ErasedWriter`]) with a blanket impl, and the maps keep `Arc<dyn …>` of the twin.

use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::object::Key;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{BoxStream, Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// The error a [`MapLoader`] or a [`MapWriter`] returns: any error type, boxed.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Loads the values a map does not hold yet from another store, Redisson's `MapLoader` and `MapLoaderAsync` in one trait.
///
/// A map built with a loader asks it on a miss and stores the value it returns, without calling the map's writer. Implement it with plain `async fn`s; a blocking store calls [`tokio::task::spawn_blocking`] inside them.
///
/// ```
/// use futures::Stream;
/// use redissun::{BoxError, MapLoader};
/// use std::collections::HashMap;
///
/// struct Users(HashMap<String, String>);
///
/// impl MapLoader<String, String> for Users {
///     async fn load(&self, key: &String) -> Result<Option<String>, BoxError> {
///         Ok(self.0.get(key).cloned())
///     }
///
///     fn load_all_keys(&self) -> impl Stream<Item = Result<String, BoxError>> + Send {
///         futures::stream::iter(self.0.keys().cloned().map(Ok))
///     }
/// }
/// ```
#[doc(alias = "MapLoaderAsync")]
pub trait MapLoader<K, V>: Send + Sync + 'static {
    /// The value of `key`, or `None` when the store has none.
    fn load(
        &self,
        key: &K,
    ) -> impl Future<Output = std::result::Result<Option<V>, BoxError>> + Send;

    /// Every key the store holds, for [`HashMap::load_all`](crate::HashMap::load_all), Redisson's `loadAllKeys`.
    #[doc(alias = "loadAllKeys")]
    fn load_all_keys(&self) -> impl Stream<Item = std::result::Result<K, BoxError>> + Send;
}

/// Writes the changes of a map to another store, Redisson's `MapWriter` and `MapWriterAsync` in one trait.
///
/// The keys within one call are unique and keep the order of the map call. Implement it with plain `async fn`s.
#[doc(alias = "MapWriterAsync")]
pub trait MapWriter<K, V>: Send + Sync + 'static {
    /// Stores the entries.
    fn write(
        &self,
        entries: &[(K, V)],
    ) -> impl Future<Output = std::result::Result<(), BoxError>> + Send;

    /// Deletes the keys.
    fn delete(&self, keys: &[K]) -> impl Future<Output = std::result::Result<(), BoxError>> + Send;
}

/// When a map calls its [`MapWriter`], Redisson's `MapOptions.WriteMode`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[doc(alias = "WRITE_THROUGH", alias = "WRITE_BEHIND")]
pub enum WriteMode {
    /// Every write calls the writer before it returns, after Redis has changed.
    #[default]
    WriteThrough,
    /// Writes are queued in Redis and handed to the writer in the background, `batch_size` at a time, every `delay`.
    #[doc(alias = "writeBehindBatchSize", alias = "writeBehindDelay")]
    WriteBehind {
        /// The most entries one writer call gets, Redisson's `writeBehindBatchSize`.
        batch_size: usize,
        /// The time between two polls of the queue, Redisson's `writeBehindDelay`.
        delay: Duration,
    },
}

impl WriteMode {
    /// Write-behind with Redisson's defaults: batches of 50, every second.
    pub const fn write_behind() -> Self {
        WriteMode::WriteBehind {
            batch_size: 50,
            delay: Duration::from_millis(1000),
        }
    }
}

/// The object-safe twin of [`MapLoader`].
pub(crate) trait ErasedLoader<K, V>: Send + Sync {
    fn load<'a>(&'a self, key: &'a K) -> BoxFuture<'a, std::result::Result<Option<V>, BoxError>>;
    fn load_all_keys(&self) -> BoxStream<'_, std::result::Result<K, BoxError>>;
}

impl<K, V, T> ErasedLoader<K, V> for T
where
    K: Send + Sync + 'static,
    V: Send + 'static,
    T: MapLoader<K, V>,
{
    fn load<'a>(&'a self, key: &'a K) -> BoxFuture<'a, std::result::Result<Option<V>, BoxError>> {
        Box::pin(MapLoader::load(self, key))
    }

    fn load_all_keys(&self) -> BoxStream<'_, std::result::Result<K, BoxError>> {
        MapLoader::load_all_keys(self).boxed()
    }
}

/// The object-safe twin of [`MapWriter`].
pub(crate) trait ErasedWriter<K, V>: Send + Sync {
    fn write<'a>(
        &'a self,
        entries: &'a [(K, V)],
    ) -> BoxFuture<'a, std::result::Result<(), BoxError>>;
    fn delete<'a>(&'a self, keys: &'a [K]) -> BoxFuture<'a, std::result::Result<(), BoxError>>;
}

impl<K, V, T> ErasedWriter<K, V> for T
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    T: MapWriter<K, V>,
{
    fn write<'a>(
        &'a self,
        entries: &'a [(K, V)],
    ) -> BoxFuture<'a, std::result::Result<(), BoxError>> {
        Box::pin(MapWriter::write(self, entries))
    }

    fn delete<'a>(&'a self, keys: &'a [K]) -> BoxFuture<'a, std::result::Result<(), BoxError>> {
        Box::pin(MapWriter::delete(self, keys))
    }
}

/// The store settings of a map builder, before [`into_store`](StoreOptions::into_store) checks them.
pub(crate) struct StoreOptions<K, V> {
    pub(crate) loader: Option<Arc<dyn ErasedLoader<K, V>>>,
    pub(crate) writer: Option<Arc<dyn ErasedWriter<K, V>>>,
    pub(crate) mode: WriteMode,
    pub(crate) attempts: usize,
    pub(crate) interval: Duration,
}

impl<K, V> Default for StoreOptions<K, V> {
    fn default() -> Self {
        Self {
            loader: None,
            writer: None,
            mode: WriteMode::WriteThrough,
            attempts: 1,
            interval: Duration::from_millis(100),
        }
    }
}

impl<K, V> fmt::Debug for StoreOptions<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoreOptions")
            .field("loader", &self.loader.is_some())
            .field("writer", &self.writer.is_some())
            .field("mode", &self.mode)
            .field("attempts", &self.attempts)
            .field("interval", &self.interval)
            .finish()
    }
}

impl<K, V> StoreOptions<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    pub(crate) fn set_loader(&mut self, loader: impl MapLoader<K, V>) {
        self.loader = Some(Arc::new(loader));
    }

    pub(crate) fn set_writer(&mut self, writer: impl MapWriter<K, V>) {
        self.writer = Some(Arc::new(writer));
    }
}

impl<K, V> StoreOptions<K, V> {
    /// Rejects the settings Redisson would reject or that cannot work.
    fn check(&self) -> Result<()> {
        if self.attempts == 0 {
            return Err(Error::Config(
                "writer_retry_attempts must be at least 1".into(),
            ));
        }
        if let WriteMode::WriteBehind { batch_size, delay } = self.mode {
            if batch_size == 0 {
                return Err(Error::Config(
                    "the write-behind batch_size must be at least 1".into(),
                ));
            }
            if delay.is_zero() {
                return Err(Error::Config(
                    "the write-behind delay must not be zero".into(),
                ));
            }
            if self.writer.is_none() {
                return Err(Error::Config("write-behind needs a writer".into()));
            }
        }
        Ok(())
    }

    /// Checks the settings and builds the store of the map `key`; `None` when there is neither a loader nor a writer, so the map does what it did without one.
    pub(crate) fn into_store<C: Codec>(
        self,
        key: &Key,
        codec: C,
    ) -> Result<Option<Arc<Store<K, V, C>>>> {
        self.check()?;
        if self.loader.is_none() && self.writer.is_none() {
            return Ok(None);
        }
        Ok(Some(Arc::new(Store {
            key: key.clone(),
            codec,
            loader: self.loader,
            writer: self.writer,
            mode: self.mode,
            attempts: self.attempts,
            interval: self.interval,
        })))
    }
}

/// The loader and writer of one map. The map tells it what it did in encoded form, and it decodes the entries for the user's types.
pub(crate) struct Store<K, V, C: Codec> {
    key: Key,
    codec: C,
    loader: Option<Arc<dyn ErasedLoader<K, V>>>,
    writer: Option<Arc<dyn ErasedWriter<K, V>>>,
    mode: WriteMode,
    attempts: usize,
    interval: Duration,
}

#[allow(dead_code)] // The maps call these from the next commits on.
impl<K, V, C: Codec> Store<K, V, C> {
    pub(crate) fn has_loader(&self) -> bool {
        self.loader.is_some()
    }

    pub(crate) fn has_writer(&self) -> bool {
        self.writer.is_some()
    }
}

impl<K, V, C> Store<K, V, C>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    C: Codec,
{
    fn name(&self) -> &str {
        self.key.name()
    }

    /// Asks the loader for the encoded key `k`; `None` without a loader.
    pub(crate) async fn load(&self, k: &Bytes) -> Result<Option<V>> {
        let Some(loader) = &self.loader else {
            return Ok(None);
        };
        let key: K = self.codec.decode(k)?;
        loader.load(&key).await.map_err(Error::Loader)
    }

    /// The loader's keys; `Error::Config` without a loader.
    pub(crate) fn load_all_keys(&self) -> Result<BoxStream<'_, Result<K>>> {
        let Some(loader) = &self.loader else {
            return Err(Error::Config("this map has no loader".into()));
        };
        Ok(loader
            .load_all_keys()
            .map(|key| key.map_err(Error::Loader))
            .boxed())
    }

    pub(crate) fn encode_key(&self, k: &K) -> Result<Bytes> {
        self.codec.encode(k)
    }

    /// The map stored `v` under the encoded key `k`, from a value it holds decoded.
    pub(crate) async fn written_value(&self, k: &Bytes, v: &V) -> Result<()> {
        if self.writer.is_none() {
            return Ok(());
        }
        self.written(&[(k.clone(), self.codec.encode(v)?)]).await
    }

    /// The map stored these encoded entries.
    pub(crate) async fn written(&self, entries: &[(Bytes, Bytes)]) -> Result<()> {
        let Some(writer) = &self.writer else {
            return Ok(());
        };
        if entries.is_empty() {
            return Ok(());
        }
        let decoded = entries
            .iter()
            .map(|(k, v)| Ok((self.codec.decode::<K>(k)?, self.codec.decode::<V>(v)?)))
            .collect::<Result<Vec<(K, V)>>>()?;
        match self.mode {
            // Write-behind queues the entries from the write-behind commit on; until then they go through.
            WriteMode::WriteThrough | WriteMode::WriteBehind { .. } => {
                retry(self.attempts, self.interval, self.name(), || {
                    writer.write(&decoded)
                })
                .await
                .map_err(Error::Writer)
            }
        }
    }

    /// The map removed these encoded keys.
    pub(crate) async fn deleted(&self, keys: &[Bytes]) -> Result<()> {
        let Some(writer) = &self.writer else {
            return Ok(());
        };
        if keys.is_empty() {
            return Ok(());
        }
        let decoded = keys
            .iter()
            .map(|k| self.codec.decode::<K>(k))
            .collect::<Result<Vec<K>>>()?;
        match self.mode {
            WriteMode::WriteThrough | WriteMode::WriteBehind { .. } => {
                retry(self.attempts, self.interval, self.name(), || {
                    writer.delete(&decoded)
                })
                .await
                .map_err(Error::Writer)
            }
        }
    }
}

/// Calls `call` up to `attempts` times (at least once), `interval` apart, and returns the last error: Redisson's `RetryableMapWriterAsync`.
pub(crate) async fn retry<F, Fut>(
    attempts: usize,
    interval: Duration,
    name: &str,
    mut call: F,
) -> std::result::Result<(), BoxError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<(), BoxError>>,
{
    let attempts = attempts.max(1);
    let mut attempt = 1;
    loop {
        match call().await {
            Ok(()) => return Ok(()),
            Err(e) if attempt >= attempts => return Err(e),
            Err(_) => {
                tracing::warn!(map = name, attempt, attempts, "map writer failed, retrying");
                tokio::time::sleep(interval).await;
                attempt += 1;
            }
        }
    }
}

/// The store settings of a map builder with a field `options: StoreOptions<K, V>`: `loader`, `writer`, `write_mode`, `writer_retry_attempts` and `writer_retry_interval`. Invoke it inside an `impl` block whose `K` and `V` are `Send + Sync + 'static`.
macro_rules! store_options {
    () => {
        /// Loads missing values from another store on a read: read-through. See [`MapLoader`](crate::MapLoader).
        #[doc(alias = "loaderAsync")]
        pub fn loader(mut self, loader: impl $crate::MapLoader<K, V>) -> Self {
            self.options.set_loader(loader);
            self
        }

        /// Hands every change of the map to another store, when [`write_mode`](Self::write_mode) says. See [`MapWriter`](crate::MapWriter).
        #[doc(alias = "writerAsync")]
        pub fn writer(mut self, writer: impl $crate::MapWriter<K, V>) -> Self {
            self.options.set_writer(writer);
            self
        }

        /// When the writer is called: [`WriteMode::WriteThrough`](crate::WriteMode::WriteThrough), the default, or [`WriteMode::WriteBehind`](crate::WriteMode::WriteBehind).
        #[doc(alias = "writeMode")]
        pub fn write_mode(mut self, mode: $crate::WriteMode) -> Self {
            self.options.mode = mode;
            self
        }

        /// Calls the writer at most `attempts` times for one change, 1 by default. Zero is a configuration error at `build()`.
        #[doc(alias = "writerRetryAttempts", alias = "writeRetryAttempts")]
        pub fn writer_retry_attempts(mut self, attempts: usize) -> Self {
            self.options.attempts = attempts;
            self
        }

        /// The time between two writer calls for one change, 100 ms by default.
        #[doc(alias = "writerRetryInterval", alias = "writeRetryInterval")]
        pub fn writer_retry_interval(mut self, interval: ::std::time::Duration) -> Self {
            self.options.interval = interval;
            self
        }
    };
}
pub(crate) use store_options;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    #[test]
    fn write_mode_defaults_are_redissons() {
        assert_eq!(WriteMode::default(), WriteMode::WriteThrough);
        assert_eq!(
            WriteMode::write_behind(),
            WriteMode::WriteBehind {
                batch_size: 50,
                delay: Duration::from_millis(1000)
            }
        );
    }

    #[tokio::test]
    async fn retry_succeeds_on_the_last_attempt() {
        let calls = AtomicUsize::new(0);
        let result = retry(3, Duration::from_millis(10), "m", || {
            let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                if n < 3 {
                    Err::<(), BoxError>(format!("failure {n}").into())
                } else {
                    Ok(())
                }
            }
        })
        .await;
        assert!(result.is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_returns_the_last_error() {
        let calls = AtomicUsize::new(0);
        let started = Instant::now();
        let result = retry(2, Duration::from_millis(50), "m", || {
            let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
            async move { Err::<(), BoxError>(format!("failure {n}").into()) }
        })
        .await;
        assert_eq!(result.unwrap_err().to_string(), "failure 2");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(started.elapsed() >= Duration::from_millis(50));
    }

    #[tokio::test]
    async fn retry_calls_once_by_default() {
        let calls = AtomicUsize::new(0);
        let result = retry(1, Duration::from_millis(10), "m", || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), BoxError>("down".into()) }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    struct NoWriter;

    impl MapWriter<String, String> for NoWriter {
        async fn write(&self, _entries: &[(String, String)]) -> std::result::Result<(), BoxError> {
            Ok(())
        }

        async fn delete(&self, _keys: &[String]) -> std::result::Result<(), BoxError> {
            Ok(())
        }
    }

    fn is_config(result: Result<()>) -> bool {
        matches!(result, Err(Error::Config(_)))
    }

    #[test]
    fn options_are_checked() {
        let mut options = StoreOptions::<String, String>::default();
        assert!(options.check().is_ok());
        options.set_writer(NoWriter);
        assert!(options.check().is_ok());
        options.attempts = 0;
        assert!(is_config(options.check()));
        options.attempts = 1;
        options.mode = WriteMode::WriteBehind {
            batch_size: 0,
            delay: Duration::from_millis(1000),
        };
        assert!(is_config(options.check()));
        options.mode = WriteMode::WriteBehind {
            batch_size: 50,
            delay: Duration::ZERO,
        };
        assert!(is_config(options.check()));
        options.mode = WriteMode::write_behind();
        assert!(options.check().is_ok());
        let without_writer = StoreOptions::<String, String> {
            mode: WriteMode::write_behind(),
            ..StoreOptions::default()
        };
        assert!(is_config(without_writer.check()));
    }
}
