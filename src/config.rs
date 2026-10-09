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
    check_lock_synced_replicas: bool,
    replicas_sync_timeout: Duration,
    fair_lock_wait_timeout: Duration,
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
            .field(
                "check_lock_synced_replicas",
                &self.check_lock_synced_replicas,
            )
            .field("replicas_sync_timeout", &self.replicas_sync_timeout)
            .field("fair_lock_wait_timeout", &self.fair_lock_wait_timeout)
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
            check_lock_synced_replicas: true,
            replicas_sync_timeout: Duration::from_secs(1),
            fair_lock_wait_timeout: Duration::from_secs(300),
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

    /// Whether a lock call fails with `Error::NoSyncedReplicas` when no replica confirmed the write in time while replicas are connected. Lock calls are retried before they fail. Ignored for a single server. Defaults to `true`, like Redisson's `checkLockSyncedSlaves`.
    pub fn check_lock_synced_replicas(mut self, check: bool) -> Self {
        self.check_lock_synced_replicas = check;
        self
    }

    /// How long a lock write waits for replicas (`WAIT`) in Sentinel and Cluster deployments. Defaults to 1 second, like Redisson's `slavesSyncTimeout`.
    pub fn replicas_sync_timeout(mut self, timeout: Duration) -> Self {
        self.replicas_sync_timeout = timeout;
        self
    }

    /// How long a [`FairLock`](crate::FairLock) waiter that waits without a timeout keeps its place after the waiter ahead of it is due. Defaults to 5 minutes, like Redisson's `fairLockWaitTimeout`.
    pub fn fair_lock_wait_timeout(mut self, timeout: Duration) -> Self {
        self.fair_lock_wait_timeout = timeout;
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
            check_lock_synced_replicas: self.check_lock_synced_replicas,
            replicas_sync_timeout: self.replicas_sync_timeout,
            fair_lock_wait_timeout: self.fair_lock_wait_timeout,
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
        if self.replicas_sync_timeout.is_zero() {
            return Err(Error::Config(
                "replicas_sync_timeout must be positive".into(),
            ));
        }
        if self.fair_lock_wait_timeout.is_zero() {
            return Err(Error::Config(
                "fair_lock_wait_timeout must be positive".into(),
            ));
        }
        let core = Core::connect(
            &url,
            self.pool_size,
            self.lock_lease,
            self.connect_timeout,
            self.eviction_interval,
            crate::lock::LockSettings::new(
                self.check_lock_synced_replicas,
                self.replicas_sync_timeout,
                self.fair_lock_wait_timeout,
            ),
        )
        .await?;
        Ok(Client::from_parts(core, self.codec))
    }
}
