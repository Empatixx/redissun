use crate::client::core::{Core, Startup};
use crate::client::retry::{DelayStrategy, Retry};
use crate::client::Client;
use crate::codec::{Codec, JsonCodec};
use crate::error::{Error, Result};
use socket2::TcpKeepalive;
use std::fmt;
use std::time::Duration;

#[derive(Clone)]
struct Settings {
    url: Option<String>,
    pool_size: usize,
    lock_lease: Duration,
    connect_timeout: Duration,
    eviction_interval: Duration,
    reliable_topic_watchdog_timeout: Duration,
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
    check_slots_coverage: bool,
    read_mode: crate::ReadMode,
    replica_scan_interval: Option<Duration>,
    client_name: Option<String>,
    database: Option<u8>,
    username: Option<String>,
    password: Option<String>,
    resolver: Option<crate::client::credentials::Resolver>,
    credentials_refresh: Option<Duration>,
    listener: Option<std::sync::Arc<dyn crate::ConnectionListener>>,
    #[cfg(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc"))]
    tls: crate::client::tls::TlsSettings,
}

/// Builder for [`Client`].
pub struct ClientBuilder<C: Codec = JsonCodec> {
    settings: Settings,
    codec: C,
}

impl<C: Codec> Clone for ClientBuilder<C> {
    fn clone(&self) -> Self {
        Self {
            settings: self.settings.clone(),
            codec: self.codec.clone(),
        }
    }
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
                "reliable_topic_watchdog_timeout",
                &settings.reliable_topic_watchdog_timeout,
            )
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
            .field("check_slots_coverage", &settings.check_slots_coverage)
            .field("read_mode", &settings.read_mode)
            .field("replica_scan_interval", &settings.replica_scan_interval)
            .field("client_name", &settings.client_name)
            .field("database", &settings.database)
            .field("username", &settings.username)
            .field("password_set", &settings.password.is_some())
            .field("credentials_resolver", &settings.resolver)
            .field("connection_listener", &settings.listener.is_some())
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
                reliable_topic_watchdog_timeout: Duration::from_secs(600),
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
                check_slots_coverage: true,
                read_mode: crate::ReadMode::Master,
                replica_scan_interval: None,
                client_name: None,
                database: None,
                username: None,
                password: None,
                resolver: None,
                credentials_refresh: None,
                listener: None,
                #[cfg(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc"))]
                tls: Default::default(),
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

    /// How long a [`ReliableTopic`](crate::ReliableTopic) subscriber stays registered after its client stopped renewing it, like Redisson's `reliableTopicWatchdogTimeout`. A subscriber renews itself every third of it, and messages that only a vanished subscriber still holds are trimmed from the stream after it. Defaults to 10 minutes.
    pub fn reliable_topic_watchdog_timeout(mut self, timeout: Duration) -> Self {
        self.settings.reliable_topic_watchdog_timeout = timeout;
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

    /// Whether building a client for Redis Cluster fails while a slot has no master, like Redisson's `checkSlotsCoverage`. Defaults to `true`.
    pub fn check_slots_coverage(mut self, check: bool) -> Self {
        self.settings.check_slots_coverage = check;
        self
    }

    /// Where read commands go: the master, the replicas, or both in turn, like Redisson's `readMode`. See [`ReadMode`](crate::ReadMode) for which calls count as reads and where the replicas come from. Defaults to [`ReadMode::Master`](crate::ReadMode::Master), unlike Redisson, whose default reads from replicas: a read never misses a write made just before.
    pub fn read_mode(mut self, mode: crate::ReadMode) -> Self {
        self.settings.read_mode = mode;
        self
    }

    /// How often the client looks for replicas again while [`read_mode`](ClientBuilder::read_mode) is not `Master`, like Redisson's `scanInterval`: it reads `CLUSTER SLOTS` in Redis Cluster and asks Sentinel for the replicas, so a replica that was added, or one that comes back, starts getting reads. Defaults to Redisson's 5 seconds in a cluster and 1 second with Sentinel. Zero scans only when a read finds no replicas known yet.
    pub fn replica_scan_interval(mut self, interval: Duration) -> Self {
        self.settings.replica_scan_interval = Some(interval);
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

    /// How the server's TLS certificate is checked, like Redisson's `sslVerificationMode`. Defaults to [`TlsVerification::Strict`](crate::TlsVerification::Strict). Needs a `rediss://` URL.
    #[cfg(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc"))]
    pub fn tls_verification(mut self, verification: crate::TlsVerification) -> Self {
        self.settings.tls.verification = verification;
        self
    }

    /// Trusts only the CA certificates in this PEM data instead of the system's, like Redisson's `sslTruststore`. Needs a `rediss://` URL.
    #[cfg(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc"))]
    pub fn tls_ca_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.settings.tls.ca = Some(crate::client::tls::Pem::Bytes(pem.into()));
        self
    }

    /// Like [`tls_ca_pem`](ClientBuilder::tls_ca_pem), with the PEM data read from a file when the client is built.
    #[cfg(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc"))]
    pub fn tls_ca_file(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.settings.tls.ca = Some(crate::client::tls::Pem::File(path.into()));
        self
    }

    /// The client certificate chain and its private key, both as PEM data, for servers that check clients (mutual TLS, `tls-auth-clients yes`), like Redisson's `sslKeystore`. Needs a `rediss://` URL.
    #[cfg(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc"))]
    pub fn tls_client_auth_pem(
        mut self,
        certificate_chain: impl Into<Vec<u8>>,
        private_key: impl Into<Vec<u8>>,
    ) -> Self {
        self.settings.tls.client_auth = Some((
            crate::client::tls::Pem::Bytes(certificate_chain.into()),
            crate::client::tls::Pem::Bytes(private_key.into()),
        ));
        self
    }

    /// Like [`tls_client_auth_pem`](ClientBuilder::tls_client_auth_pem), with the PEM data read from files when the client is built.
    #[cfg(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc"))]
    pub fn tls_client_auth_files(
        mut self,
        certificate_chain: impl Into<std::path::PathBuf>,
        private_key: impl Into<std::path::PathBuf>,
    ) -> Self {
        self.settings.tls.client_auth = Some((
            crate::client::tls::Pem::File(certificate_chain.into()),
            crate::client::tls::Pem::File(private_key.into()),
        ));
        self
    }

    /// The ACL user for `AUTH`, like Redisson's `username`. It replaces a user given in the URL.
    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.settings.username = Some(username.into());
        self
    }

    /// The password for `AUTH`, like Redisson's `password`. It replaces a password given in the URL. `Debug` never prints it.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.settings.password = Some(password.into());
        self
    }

    /// Asks `resolve` for the credentials of every new connection, like Redisson's `credentialsResolver`. It gets the node's `host:port` and replaces [`username`](ClientBuilder::username) and [`password`](ClientBuilder::password). An error from it fails the connection.
    ///
    /// ```no_run
    /// # async fn run() -> redissun::Result<()> {
    /// use redissun::{Client, Credentials};
    ///
    /// let client = Client::builder()
    ///     .url("redis://127.0.0.1:6379")
    ///     .credentials_resolver(|_address| async {
    ///         Ok(Credentials::new("app", std::env::var("REDIS_TOKEN").unwrap_or_default()))
    ///     })
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn credentials_resolver<F, Fut>(mut self, resolve: F) -> Self
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<crate::Credentials>> + Send + 'static,
    {
        self.settings.resolver = Some(crate::client::credentials::Resolver::new(resolve));
        self
    }

    /// How often open connections ask the [`credentials_resolver`](ClientBuilder::credentials_resolver) again and send a new `AUTH`, like Redisson's `CredentialsResolver::nextRenewal`. Use it for passwords or tokens that expire. Not set by default.
    pub fn credentials_refresh_interval(mut self, interval: Duration) -> Self {
        self.settings.credentials_refresh = Some(interval);
        self
    }

    /// Tells `listener` when a node gets connected and when it loses its connections, like Redisson's `setConnectionListener`. Not set by default.
    pub fn connection_listener(mut self, listener: impl crate::ConnectionListener) -> Self {
        self.settings.listener = Some(std::sync::Arc::new(listener));
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
            (
                "reliable_topic_watchdog_timeout",
                settings.reliable_topic_watchdog_timeout,
            ),
            ("replicas_sync_timeout", settings.replicas_sync_timeout),
            ("fair_lock_wait_timeout", settings.fair_lock_wait_timeout),
        ];
        if let Some((name, _)) = positive.iter().find(|(_, value)| value.is_zero()) {
            return Err(Error::Config(format!("{name} must be positive")));
        }
        Ok(url)
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

    /// The servers behind the URL, and how every connection to them is opened.
    fn topology(&self, url: &str) -> Result<crate::net::topology::Topology> {
        use crate::net::node::{NodeCredentials, NodeSettings};
        use crate::net::topology::{Cluster, Pool, Sentinel, Topology};
        use crate::net::url::Kind;
        let target = crate::net::url::parse(url)?;
        let settings = &self.settings;
        #[cfg(not(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc")))]
        if target.tls {
            return Err(Error::Config(
                "rediss:// URLs need the tls-rustls or tls-rustls-aws-lc feature".into(),
            ));
        }
        #[cfg(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc"))]
        if !target.tls && !settings.tls.is_default() {
            return Err(Error::Config("TLS settings need a rediss:// URL".into()));
        }
        let credentials = match &settings.resolver {
            Some(resolver) => NodeCredentials::Resolver(resolver.clone()),
            None => {
                let username = settings
                    .username
                    .clone()
                    .or_else(|| target.username.clone());
                let password = settings
                    .password
                    .clone()
                    .or_else(|| target.password.clone());
                match (username, password) {
                    (None, None) => NodeCredentials::None,
                    (username, password) => {
                        NodeCredentials::Fixed(crate::Credentials { username, password })
                    }
                }
            }
        };
        let stream = crate::net::stream::StreamSettings {
            connect_timeout: settings.connect_timeout,
            tcp_no_delay: settings.tcp_no_delay,
            keep_alive: settings.keep_alive.then(|| self.keepalive()),
            #[cfg(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc"))]
            tls: if target.tls {
                Some(settings.tls.client_config()?)
            } else {
                None
            },
        };
        let events = settings
            .listener
            .clone()
            .map(|listener| std::sync::Arc::new(crate::client::events::Events::new(listener)));
        let node = std::sync::Arc::new(NodeSettings {
            stream: stream.clone(),
            db: settings.database.or(target.database).map(i64::from),
            credentials,
            client_name: settings.client_name.clone(),
            resp3: false,
            push: None,
            reconnection_delay: settings.reconnection_delay,
            readonly: false,
            kind: crate::NodeType::Master,
            events: events.clone(),
        });
        Ok(match target.kind {
            Kind::Single => {
                Topology::Single(Pool::new(&target.nodes[0], settings.pool_size, &node))
            }
            Kind::Sentinel { service } => {
                // Sentinels are asked without the master's database, credentials or name.
                let sentinels = std::sync::Arc::new(NodeSettings {
                    stream,
                    db: None,
                    credentials: NodeCredentials::None,
                    client_name: None,
                    resp3: false,
                    push: None,
                    reconnection_delay: settings.reconnection_delay,
                    readonly: false,
                    kind: crate::NodeType::Sentinel,
                    events,
                });
                Topology::Sentinel(Sentinel::new(
                    target.nodes,
                    service,
                    settings.pool_size,
                    node,
                    sentinels,
                    settings.read_mode,
                ))
            }
            Kind::Cluster => Topology::Cluster(Cluster::new(
                target.nodes,
                settings.pool_size,
                node,
                settings.read_mode,
            )),
        })
    }

    /// Connects and returns the client.
    pub async fn build(self) -> Result<Client<C>> {
        let url = self.validate()?;
        let topology = self.topology(&url)?;
        let check_slots_coverage = self.settings.check_slots_coverage;
        let settings = self.settings;
        let core = Core::connect(
            topology,
            check_slots_coverage,
            Startup {
                pool_size: settings.pool_size,
                lock_lease: settings.lock_lease,
                connect_timeout: settings.connect_timeout,
                eviction_interval: settings.eviction_interval,
                reliable_topic_watchdog_timeout: settings.reliable_topic_watchdog_timeout,
                lock_settings: crate::locks::LockSettings::new(
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
                credentials_refresh: settings.resolver.as_ref().and(settings.credentials_refresh),
                replica_scan_interval: settings.replica_scan_interval,
            },
        )
        .await?;
        Ok(Client::from_parts(core, self.codec))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::node::{NodeCredentials, NodeSettings};

    fn node(builder: ClientBuilder) -> std::sync::Arc<NodeSettings> {
        let url = builder.validate().unwrap();
        builder.topology(&url).unwrap().settings().clone()
    }

    #[test]
    fn reads_go_to_the_master_by_default() {
        let builder = ClientBuilder::new();
        assert_eq!(builder.settings.read_mode, crate::ReadMode::Master);
        assert_eq!(builder.settings.replica_scan_interval, None);
    }
    #[test]
    fn defaults_follow_redisson() {
        let builder = ClientBuilder::new().url("redis://127.0.0.1:6379");
        assert_eq!(builder.settings.retry_attempts, 4);
        assert_eq!(builder.settings.timeout, Duration::from_secs(3));
        let node = node(builder);
        assert_eq!(node.stream.connect_timeout, Duration::from_secs(10));
        assert!(node.stream.tcp_no_delay);
        assert!(node.stream.keep_alive.is_none());
        assert_eq!(
            node.reconnection_delay,
            DelayStrategy::EqualJitter {
                base: Duration::from_millis(100),
                max: Duration::from_secs(10),
            }
        );
        assert_eq!(node.db, None);
        assert!(matches!(node.credentials, NodeCredentials::None));
    }

    #[test]
    fn settings_reach_every_connection() {
        let node = node(
            ClientBuilder::new()
                .url("redis://127.0.0.1:6379/2")
                .connect_timeout(Duration::from_secs(2))
                .reconnection_delay(DelayStrategy::Constant(Duration::from_millis(250)))
                .tcp_no_delay(false)
                .keep_alive(true)
                .tcp_keep_alive_idle(Duration::from_secs(60))
                .client_name("worker")
                .database(5),
        );
        assert_eq!(node.stream.connect_timeout, Duration::from_secs(2));
        assert!(!node.stream.tcp_no_delay);
        assert!(node.stream.keep_alive.is_some());
        assert_eq!(
            node.reconnection_delay,
            DelayStrategy::Constant(Duration::from_millis(250))
        );
        assert_eq!(node.client_name.as_deref(), Some("worker"));
        assert_eq!(node.db, Some(5));
    }

    #[test]
    fn the_url_database_is_kept_without_an_override() {
        let node = node(ClientBuilder::new().url("redis://127.0.0.1:6379/3"));
        assert_eq!(node.db, Some(3));
    }

    #[test]
    fn builder_credentials_replace_the_url_credentials() {
        let node = node(
            ClientBuilder::new()
                .url("redis://old:secret@127.0.0.1:6379")
                .username("new")
                .password("better"),
        );
        let NodeCredentials::Fixed(credentials) = &node.credentials else {
            panic!("fixed credentials expected");
        };
        assert_eq!(credentials.username.as_deref(), Some("new"));
        assert_eq!(credentials.password.as_deref(), Some("better"));
    }

    #[test]
    fn the_resolver_is_asked_instead_of_fixed_credentials() {
        let node = node(
            ClientBuilder::new()
                .url("redis://user:secret@127.0.0.1:6379")
                .credentials_resolver(|_| async { Ok(crate::Credentials::default()) }),
        );
        assert!(matches!(node.credentials, NodeCredentials::Resolver(_)));
    }

    #[test]
    fn debug_output_never_shows_the_password() {
        let text = format!("{:?}", ClientBuilder::new().password("hunter2"));
        assert!(!text.contains("hunter2"));
        assert!(text.contains("password_set: true"));
    }

    #[test]
    fn debug_output_lists_the_new_settings() {
        let text = format!("{:?}", ClientBuilder::new().client_name("worker"));
        assert!(text.contains("retry_attempts: 4"));
        assert!(text.contains("worker"));
    }
}
