use crate::client::Client;
use crate::codec::{Codec, JsonCodec};
use crate::core::{Core, Startup};
use crate::error::{Error, Result};
use crate::retry::{DelayStrategy, Retry};
use fred::prelude::{Builder, Config};
use fred::socket2::TcpKeepalive;
use std::fmt;
use std::time::Duration;

#[derive(Clone)]
struct Settings {
    url: Option<String>,
    pool_size: usize,
    lock_lease: Duration,
    connect_timeout: Duration,
    eviction_interval: Duration,
    check_lock_synced_replicas: bool,
    replicas_sync_timeout: Duration,
    fair_lock_wait_timeout: Duration,
    timeout: Duration,
    retry_attempts: u32,
    retry_delay: DelayStrategy,
    reconnection_delay: DelayStrategy,
    ping_connection_interval: Duration,
    keep_alive: bool,
    tcp_keep_alive_idle: Option<Duration>,
    tcp_keep_alive_interval: Option<Duration>,
    tcp_no_delay: bool,
    client_name: Option<String>,
    database: Option<u8>,
}

/// Builder for [`Client`].
pub struct ClientBuilder<C: Codec = JsonCodec> {
    settings: Settings,
    codec: C,
}

impl<C: Codec> fmt::Debug for ClientBuilder<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let settings = &self.settings;
        f.debug_struct("ClientBuilder")
            .field("url_set", &settings.url.is_some())
            .field("pool_size", &settings.pool_size)
            .field("lock_lease", &settings.lock_lease)
            .field("connect_timeout", &settings.connect_timeout)
            .field("eviction_interval", &settings.eviction_interval)
            .field(
                "check_lock_synced_replicas",
                &settings.check_lock_synced_replicas,
            )
            .field("replicas_sync_timeout", &settings.replicas_sync_timeout)
            .field("fair_lock_wait_timeout", &settings.fair_lock_wait_timeout)
            .field("timeout", &settings.timeout)
            .field("retry_attempts", &settings.retry_attempts)
            .field("retry_delay", &settings.retry_delay)
            .field("reconnection_delay", &settings.reconnection_delay)
            .field(
                "ping_connection_interval",
                &settings.ping_connection_interval,
            )
            .field("keep_alive", &settings.keep_alive)
            .field("tcp_no_delay", &settings.tcp_no_delay)
            .field("client_name", &settings.client_name)
            .field("database", &settings.database)
            .finish()
    }
}

impl ClientBuilder<JsonCodec> {
    pub(crate) fn new() -> Self {
        Self {
            settings: Settings {
                url: None,
                pool_size: 4,
                lock_lease: Duration::from_secs(30),
                connect_timeout: Duration::from_secs(10),
                eviction_interval: Duration::from_secs(5),
                check_lock_synced_replicas: true,
                replicas_sync_timeout: Duration::from_secs(1),
                fair_lock_wait_timeout: Duration::from_secs(300),
                timeout: Duration::from_secs(3),
                retry_attempts: 4,
                retry_delay: DelayStrategy::EqualJitter {
                    base: Duration::from_secs(1),
                    max: Duration::from_secs(2),
                },
                reconnection_delay: DelayStrategy::EqualJitter {
                    base: Duration::from_millis(100),
                    max: Duration::from_secs(10),
                },
                ping_connection_interval: Duration::from_secs(30),
                keep_alive: false,
                tcp_keep_alive_idle: None,
                tcp_keep_alive_interval: None,
                tcp_no_delay: true,
                client_name: None,
                database: None,
            },
            codec: JsonCodec,
        }
    }
}

impl<C: Codec> ClientBuilder<C> {
    /// Redis connection URL, for example `redis://127.0.0.1:6379`. Required.
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.settings.url = Some(url.into());
        self
    }

    /// Number of pooled connections. Defaults to 4 and must be at least 1.
    pub fn pool_size(mut self, pool_size: usize) -> Self {
        self.settings.pool_size = pool_size;
        self
    }

    /// Lease of locks acquired without an explicit lease; the watchdog renews it. Defaults to 30 seconds.
    pub fn lock_lease(mut self, lock_lease: Duration) -> Self {
        self.settings.lock_lease = lock_lease;
        self
    }

    /// How long `build` waits for the first connection before failing with `Error::Timeout`, and how long one connection attempt may take, like Redisson's `connectTimeout`. Defaults to 10 seconds.
    pub fn connect_timeout(mut self, connect_timeout: Duration) -> Self {
        self.settings.connect_timeout = connect_timeout;
        self
    }

    /// The first and shortest pause between two background clean-ups of expired `HashMapCache` and `HashSetCache` entries, like Redisson's `minCleanUpDelay`. As in Redisson, the pause grows by half, up to 30 minutes, after three runs in a row find nothing, and shrinks to a quarter after three full runs in a row. Defaults to 5 seconds.
    pub fn eviction_interval(mut self, eviction_interval: Duration) -> Self {
        self.settings.eviction_interval = eviction_interval;
        self
    }

    /// Whether a lock call fails with `Error::NoSyncedReplicas` when no replica confirmed the write in time while replicas are connected. Lock calls are retried before they fail. Ignored for a single server. Defaults to `true`, like Redisson's `checkLockSyncedSlaves`.
    pub fn check_lock_synced_replicas(mut self, check: bool) -> Self {
        self.settings.check_lock_synced_replicas = check;
        self
    }

    /// How long a lock write waits for replicas (`WAIT`) in Sentinel and Cluster deployments. Defaults to 1 second, like Redisson's `slavesSyncTimeout`.
    pub fn replicas_sync_timeout(mut self, timeout: Duration) -> Self {
        self.settings.replicas_sync_timeout = timeout;
        self
    }

    /// How long a [`FairLock`](crate::FairLock) waiter that waits without a timeout keeps its place after the waiter ahead of it is due. Defaults to 5 minutes, like Redisson's `fairLockWaitTimeout`.
    pub fn fair_lock_wait_timeout(mut self, timeout: Duration) -> Self {
        self.settings.fair_lock_wait_timeout = timeout;
        self
    }

    /// How long a command waits for its reply before it fails with an error, like Redisson's `timeout`. Blocking commands (`pop_front_wait`, `read_group_wait` and the like) are not limited by it, and a lock write that waits for replicas gets `replicas_sync_timeout` on top. It is also the default [`Batch::response_timeout`](crate::Batch::response_timeout). Defaults to 3 seconds; zero waits forever.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.settings.timeout = timeout;
        self
    }

    /// How many times a command is sent again after its connection failed, like Redisson's `retryAttempts`. Commands that Redisson never repeats, such as taking a lock, are still sent once. It is also the default for [`Batch::retry_attempts`](crate::Batch::retry_attempts) and the number of retries of a lock write that no replica confirmed. Defaults to 4.
    pub fn retry_attempts(mut self, attempts: u32) -> Self {
        self.settings.retry_attempts = attempts;
        self
    }

    /// The pause between two retries of a batch or of a lock write that no replica confirmed, like Redisson's `retryDelay`. A command whose connection failed is sent again as soon as the connection is back, so its pause is the [`reconnection_delay`](ClientBuilder::reconnection_delay). Defaults to `EqualJitter` from 1 to 2 seconds, as in Redisson.
    pub fn retry_delay(mut self, delay: DelayStrategy) -> Self {
        self.settings.retry_delay = delay;
        self
    }

    /// The pause between two attempts to reconnect a lost connection, like Redisson's `reconnectionDelay`. The client reconnects forever. Jitter is fixed by the underlying client, so `EqualJitter`, `FullJitter` and `DecorrelatedJitter` become an exponential pause from about the base to `max`. Defaults to `EqualJitter` from 100 ms to 10 seconds, as in Redisson.
    pub fn reconnection_delay(mut self, delay: DelayStrategy) -> Self {
        self.settings.reconnection_delay = delay;
        self
    }

    /// How often every pooled connection and the pub/sub connection is checked with `PING`, like Redisson's `pingConnectionInterval`. A connection whose `PING` gets no reply within [`timeout`](ClientBuilder::timeout) is closed and opened again. Defaults to 30 seconds; zero turns the check off.
    pub fn ping_connection_interval(mut self, interval: Duration) -> Self {
        self.settings.ping_connection_interval = interval;
        self
    }

    /// Turns on TCP keep-alive for every connection, like Redisson's `keepAlive`. Defaults to `false`.
    pub fn keep_alive(mut self, keep_alive: bool) -> Self {
        self.settings.keep_alive = keep_alive;
        self
    }

    /// How long a connection is idle before the first keep-alive probe, like Redisson's `tcpKeepAliveIdle`. Used with [`keep_alive`](ClientBuilder::keep_alive). Defaults to the system setting.
    pub fn tcp_keep_alive_idle(mut self, idle: Duration) -> Self {
        self.settings.tcp_keep_alive_idle = Some(idle);
        self
    }

    /// The pause between two keep-alive probes, like Redisson's `tcpKeepAliveInterval`. Used with [`keep_alive`](ClientBuilder::keep_alive) on Linux, macOS, Windows, Android, iOS and FreeBSD. Defaults to the system setting.
    pub fn tcp_keep_alive_interval(mut self, interval: Duration) -> Self {
        self.settings.tcp_keep_alive_interval = Some(interval);
        self
    }

    /// Sets `TCP_NODELAY` on every connection, like Redisson's `tcpNoDelay`. Defaults to `true`.
    pub fn tcp_no_delay(mut self, no_delay: bool) -> Self {
        self.settings.tcp_no_delay = no_delay;
        self
    }

    /// The name every connection of the client gets with `CLIENT SETNAME`, like Redisson's `clientName`. `CLIENT LIST` shows it. Not set by default.
    pub fn client_name(mut self, name: impl Into<String>) -> Self {
        self.settings.client_name = Some(name.into());
        self
    }

    /// The database number, like Redisson's `database`. It replaces a database given in the URL. Redis Cluster has only database 0. Defaults to the URL's database, or 0.
    pub fn database(mut self, database: u8) -> Self {
        self.settings.database = Some(database);
        self
    }

    /// Replaces the codec used for values.
    pub fn codec<N: Codec>(self, codec: N) -> ClientBuilder<N> {
        ClientBuilder {
            settings: self.settings,
            codec,
        }
    }

    fn validate(&self) -> Result<String> {
        let settings = &self.settings;
        let url = settings
            .url
            .clone()
            .ok_or_else(|| Error::Config("url is required".into()))?;
        if settings.pool_size == 0 {
            return Err(Error::Config("pool_size must be at least 1".into()));
        }
        let positive = [
            ("lock_lease", settings.lock_lease),
            ("connect_timeout", settings.connect_timeout),
            ("eviction_interval", settings.eviction_interval),
            ("replicas_sync_timeout", settings.replicas_sync_timeout),
            ("fair_lock_wait_timeout", settings.fair_lock_wait_timeout),
        ];
        if let Some((name, _)) = positive.iter().find(|(_, value)| value.is_zero()) {
            return Err(Error::Config(format!("{name} must be positive")));
        }
        Ok(url)
    }

    fn fred_builder(&self, url: &str) -> Result<Builder> {
        let settings = &self.settings;
        let mut config = Config::from_url(url).map_err(|e| Error::Config(e.to_string()))?;
        if settings.database.is_some() {
            config.database = settings.database;
        }
        let mut builder = Builder::from_config(config);
        builder.set_policy(settings.reconnection_delay.reconnect_policy());
        builder.with_performance_config(|performance| {
            performance.broadcast_channel_capacity = 4096;
            performance.default_command_timeout = settings.timeout;
        });
        builder.with_connection_config(|connection| {
            connection.connection_timeout = settings.connect_timeout;
            connection.max_command_attempts = settings.retry_attempts.saturating_add(1);
            connection.tcp.nodelay = Some(settings.tcp_no_delay);
            connection.tcp.keepalive = settings.keep_alive.then(|| self.keepalive());
        });
        Ok(builder)
    }

    fn keepalive(&self) -> TcpKeepalive {
        let mut keepalive = TcpKeepalive::new();
        if let Some(idle) = self.settings.tcp_keep_alive_idle {
            keepalive = keepalive.with_time(idle);
        }
        #[cfg(any(
            target_os = "linux",
            target_os = "android",
            target_os = "macos",
            target_os = "ios",
            target_os = "freebsd",
            target_os = "windows",
        ))]
        if let Some(interval) = self.settings.tcp_keep_alive_interval {
            keepalive = keepalive.with_interval(interval);
        }
        keepalive
    }

    /// Connects and returns the client.
    pub async fn build(self) -> Result<Client<C>> {
        let url = self.validate()?;
        let builder = self.fred_builder(&url)?;
        let settings = self.settings;
        let core = Core::connect(
            builder,
            Startup {
                pool_size: settings.pool_size,
                lock_lease: settings.lock_lease,
                connect_timeout: settings.connect_timeout,
                eviction_interval: settings.eviction_interval,
                lock_settings: crate::lock::LockSettings::new(
                    settings.check_lock_synced_replicas,
                    settings.replicas_sync_timeout,
                    settings.fair_lock_wait_timeout,
                ),
                retry: Retry {
                    attempts: settings.retry_attempts,
                    delay: settings.retry_delay,
                    timeout: settings.timeout,
                },
                ping_interval: settings.ping_connection_interval,
                client_name: settings.client_name,
            },
        )
        .await?;
        Ok(Client::from_parts(core, self.codec))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fred::prelude::ReconnectPolicy;

    fn fred(builder: ClientBuilder) -> Builder {
        let url = builder.validate().unwrap();
        builder.fred_builder(&url).unwrap()
    }

    #[test]
    fn defaults_follow_redisson() {
        let built = fred(ClientBuilder::new().url("redis://127.0.0.1:6379"));
        let connection = built.get_connection_config();
        assert_eq!(connection.max_command_attempts, 5);
        assert_eq!(connection.connection_timeout, Duration::from_secs(10));
        assert_eq!(connection.tcp.nodelay, Some(true));
        assert!(connection.tcp.keepalive.is_none());
        assert_eq!(
            built.get_performance_config().default_command_timeout,
            Duration::from_secs(3)
        );
        assert_eq!(
            built.get_policy(),
            Some(&ReconnectPolicy::Exponential {
                attempts: 0,
                max_attempts: 0,
                min_delay: 50,
                max_delay: 10_000,
                base: 2,
                jitter: 50,
            })
        );
        assert_eq!(built.get_config().unwrap().database, None);
    }

    #[test]
    fn settings_reach_the_redis_client() {
        let built = fred(
            ClientBuilder::new()
                .url("redis://127.0.0.1:6379/2")
                .retry_attempts(0)
                .timeout(Duration::from_millis(1500))
                .connect_timeout(Duration::from_secs(2))
                .reconnection_delay(DelayStrategy::Constant(Duration::from_millis(250)))
                .tcp_no_delay(false)
                .keep_alive(true)
                .tcp_keep_alive_idle(Duration::from_secs(60))
                .database(5),
        );
        let connection = built.get_connection_config();
        assert_eq!(connection.max_command_attempts, 1);
        assert_eq!(connection.connection_timeout, Duration::from_secs(2));
        assert_eq!(connection.tcp.nodelay, Some(false));
        assert!(connection.tcp.keepalive.is_some());
        assert_eq!(
            built.get_performance_config().default_command_timeout,
            Duration::from_millis(1500)
        );
        assert_eq!(
            built.get_policy(),
            Some(&ReconnectPolicy::Constant {
                attempts: 0,
                max_attempts: 0,
                delay: 250,
                jitter: 0,
            })
        );
        assert_eq!(built.get_config().unwrap().database, Some(5));
    }

    #[test]
    fn the_url_database_is_kept_without_an_override() {
        let built = fred(ClientBuilder::new().url("redis://127.0.0.1:6379/3"));
        assert_eq!(built.get_config().unwrap().database, Some(3));
    }

    #[test]
    fn debug_output_lists_the_new_settings() {
        let text = format!("{:?}", ClientBuilder::new().client_name("worker"));
        assert!(text.contains("retry_attempts: 4"));
        assert!(text.contains("worker"));
    }
}
