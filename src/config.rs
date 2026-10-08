use crate::client::Client;
use crate::codec::{Codec, JsonCodec};
use crate::core::Core;
use crate::error::{Error, Result};
use std::fmt;
use std::time::Duration;

/// Builder for [`Client`].
pub struct ClientBuilder<C: Codec = JsonCodec> {
    url: Option<String>,
    pool_size: usize,
    lock_lease: Duration,
    connect_timeout: Duration,
    eviction_interval: Duration,
    codec: C,
}

impl<C: Codec> fmt::Debug for ClientBuilder<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("url_set", &self.url.is_some())
            .field("pool_size", &self.pool_size)
            .field("lock_lease", &self.lock_lease)
            .field("connect_timeout", &self.connect_timeout)
            .field("eviction_interval", &self.eviction_interval)
            .finish()
    }
}

impl ClientBuilder<JsonCodec> {
    pub(crate) fn new() -> Self {
        Self {
            url: None,
            pool_size: 4,
            lock_lease: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            eviction_interval: Duration::from_secs(5),
            codec: JsonCodec,
        }
    }
}

impl<C: Codec> ClientBuilder<C> {
    /// Redis connection URL, for example `redis://127.0.0.1:6379`. Required.
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Number of pooled connections. Defaults to 4 and must be at least 1.
    pub fn pool_size(mut self, pool_size: usize) -> Self {
        self.pool_size = pool_size;
        self
    }

    /// Lease of locks acquired without an explicit lease; the watchdog renews it. Defaults to 30 seconds.
    pub fn lock_lease(mut self, lock_lease: Duration) -> Self {
        self.lock_lease = lock_lease;
        self
    }

    /// How long `build` waits for the first connection before failing with `Error::Timeout`. Defaults to 10 seconds.
    pub fn connect_timeout(mut self, connect_timeout: Duration) -> Self {
        self.connect_timeout = connect_timeout;
        self
    }

    /// The shortest pause between two background clean-ups of expired `HashMapCache` entries. The pause grows up to two hours while there is nothing to clean. Defaults to 5 seconds.
    pub fn eviction_interval(mut self, eviction_interval: Duration) -> Self {
        self.eviction_interval = eviction_interval;
        self
    }

    /// Replaces the codec used for values.
    pub fn codec<N: Codec>(self, codec: N) -> ClientBuilder<N> {
        ClientBuilder {
            url: self.url,
            pool_size: self.pool_size,
            lock_lease: self.lock_lease,
            connect_timeout: self.connect_timeout,
            eviction_interval: self.eviction_interval,
            codec,
        }
    }

    /// Connects and returns the client.
    pub async fn build(self) -> Result<Client<C>> {
        let url = self
            .url
            .ok_or_else(|| Error::Config("url is required".into()))?;
        if self.pool_size == 0 {
            return Err(Error::Config("pool_size must be at least 1".into()));
        }
        if self.lock_lease.is_zero() {
            return Err(Error::Config("lock_lease must be positive".into()));
        }
        if self.connect_timeout.is_zero() {
            return Err(Error::Config("connect_timeout must be positive".into()));
        }
        if self.eviction_interval.is_zero() {
            return Err(Error::Config("eviction_interval must be positive".into()));
        }
        let core = Core::connect(
            &url,
            self.pool_size,
            self.lock_lease,
            self.connect_timeout,
            self.eviction_interval,
        )
        .await?;
        Ok(Client::from_parts(core, self.codec))
    }
}
