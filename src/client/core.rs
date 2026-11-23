use crate::client::fred_adapter;
use crate::client::request::Request;
use crate::client::retry::Retry;
use crate::client::slot::keyslot;
use crate::error::{Error, Result};
use crate::pubsub::PubSub;
use bytes::Bytes;
use fred::clients::{Client as RedisClient, Pool, WithOptions};
use fred::interfaces::{ClientInterface, ClientLike, ClusterInterface, EventInterface};
use fred::prelude::{Builder, FromValue, Options};
use fred::types::client::ClientKillFilter;
use fred::types::config::PerformanceConfig;
use fred::types::scripts::Script;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use uuid::Uuid;

pub(crate) struct Evictor {
    pub(crate) handle: tokio::task::JoinHandle<()>,
    pub(crate) wake: Arc<tokio::sync::Notify>,
}

pub(crate) struct Startup {
    pub(crate) pool_size: usize,
    pub(crate) lock_lease: Duration,
    pub(crate) connect_timeout: Duration,
    pub(crate) eviction_interval: Duration,
    pub(crate) lock_settings: crate::locks::LockSettings,
    pub(crate) retry: Retry,
    pub(crate) ping_interval: Duration,
    pub(crate) client_name: Option<String>,
}

pub(crate) struct Core {
    pool: Pool,
    /// Builds the connections that only redissun uses: a batch's, the pub/sub one and those of blocking calls. They have no `command_queue_limit`, because a batch and a resubscription hand many commands over at once and a limit would fail part of them. They have no `unresponsive_timeout` either, because a blocking read or a `WAIT` for replicas waits for its reply on purpose.
    own_connections: Builder,
    id: String,
    pub(crate) lock_lease: Duration,
    pub(crate) lock_settings: crate::locks::LockSettings,
    pub(crate) eviction_interval: Duration,
    pub(crate) retry: Retry,
    pub(crate) read_only_scripts: std::sync::atomic::AtomicBool,
    client_name: Option<String>,
    background: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    pubsub: tokio::sync::OnceCell<Arc<PubSub>>,
    pub(crate) evictors: std::sync::Mutex<std::collections::HashMap<String, Evictor>>,
    pub(crate) exclusive: std::sync::Mutex<Vec<RedisClient>>,
    /// The adders of this client by name, so that one name is one counter.
    pub(crate) adders: crate::coordination::adder::Registry,
    /// One permit for each connection that batches may use at the same time, `pool_size` in all, like the connections of Redisson's pool.
    pub(crate) batch_connections: Arc<tokio::sync::Semaphore>,
}

impl Core {
    pub(crate) async fn connect(builder: Builder, startup: Startup) -> Result<Arc<Self>> {
        let pool = builder
            .build_pool(startup.pool_size)
            .map_err(|e| Error::Config(e.to_string()))?;
        let mut own_connections = builder.clone();
        own_connections.with_connection_config(|connection| {
            connection.max_command_buffer_len = 0;
            connection.unresponsive.max_timeout = None;
        });
        match tokio::time::timeout(startup.connect_timeout, pool.init()).await {
            Ok(connected) => {
                connected?;
            }
            Err(_) => {
                let abandoned = pool.clone();
                tokio::spawn(async move {
                    let _ = abandoned.quit().await;
                });
                return Err(Error::Timeout);
            }
        }
        let core = Arc::new(Self {
            pool,
            own_connections,
            id: Uuid::new_v4().to_string(),
            lock_lease: startup.lock_lease,
            lock_settings: startup.lock_settings,
            eviction_interval: startup.eviction_interval,
            retry: startup.retry,
            read_only_scripts: std::sync::atomic::AtomicBool::new(true),
            client_name: startup.client_name,
            background: std::sync::Mutex::new(Vec::new()),
            pubsub: tokio::sync::OnceCell::new(),
            evictors: std::sync::Mutex::new(std::collections::HashMap::new()),
            exclusive: std::sync::Mutex::new(Vec::new()),
            adders: Default::default(),
            batch_connections: Arc::new(tokio::sync::Semaphore::new(startup.pool_size)),
        });
        for client in core.pool.clients() {
            core.keep_named(client).await?;
        }
        if !startup.ping_interval.is_zero() {
            let ping = tokio::spawn(ping_loop(Arc::downgrade(&core), startup.ping_interval));
            core.spawned(ping);
        }
        Ok(core)
    }

    fn spawned(&self, handle: tokio::task::JoinHandle<()>) {
        self.background
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(handle);
    }

    pub(crate) async fn name(&self, client: &RedisClient) -> Result<()> {
        if let Some(name) = &self.client_name {
            set_name(client, name, None).await?;
        }
        Ok(())
    }

    async fn keep_named(&self, client: &RedisClient) -> Result<()> {
        let Some(name) = self.client_name.clone() else {
            return Ok(());
        };
        set_name(client, &name, None).await?;
        let mut reconnected = client.reconnect_rx();
        let watched = client.clone();
        self.spawned(tokio::spawn(async move {
            loop {
                match reconnected.recv().await {
                    Ok(server) => {
                        let _ = set_name(&watched, &name, Some(server)).await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let _ = set_name(&watched, &name, None).await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        }));
        Ok(())
    }

    pub(crate) async fn pubsub(&self) -> Result<&Arc<PubSub>> {
        self.pubsub
            .get_or_try_init(|| async {
                let pubsub = PubSub::start(self.own_connection()?).await?;
                if let Err(error) = self.keep_named(pubsub.client()).await {
                    pubsub.quit().await;
                    return Err(error);
                }
                Ok(pubsub)
            })
            .await
    }

    pub(crate) fn pubsub_started(&self) -> Option<&Arc<PubSub>> {
        self.pubsub.get()
    }

    /// A new client, not connected yet, for a connection that only redissun uses. See `own_connections`.
    pub(crate) fn own_connection(&self) -> Result<RedisClient> {
        self.own_connections
            .build()
            .map_err(|e| Error::Config(e.to_string()))
    }

    /// Sends `request` and reads the reply as `T`. Every command outside the connection layer, pub/sub and the batch engine goes through here.
    pub(crate) async fn query<T: redis::FromRedisValue>(&self, request: Request) -> Result<T> {
        let value = self.query_value(request).await?;
        Ok(T::from_redis_value(value)?)
    }

    /// Like [`query`](Core::query), returning the reply as it came.
    pub(crate) async fn query_value(&self, request: Request) -> Result<redis::Value> {
        let (name, args) = fred_adapter::to_fred(request.cmd())?;
        let hash = request
            .routing_key()
            .map_or(fred::types::ClusterHash::Random, |key| {
                fred::types::ClusterHash::Custom(keyslot(key))
            });
        let command = fred::types::CustomCommand::new(name, hash, false);
        let client = self.redis();
        let reply: fred::types::Value = match request.is_once() {
            true => no_retry(client).custom(command, args).await?,
            false => client.custom(command, args).await?,
        };
        Ok(fred_adapter::from_fred(reply))
    }

    pub(crate) fn redis(&self) -> &RedisClient {
        self.pool.next()
    }

    pub(crate) async fn blocking_client(&self) -> Result<BlockingClient> {
        let client = self.own_connection()?;
        client.update_perf_config(PerformanceConfig {
            default_command_timeout: Duration::ZERO,
            ..client.perf_config()
        });
        client.init().await?;
        if let Err(error) = self.name(&client).await {
            let _ = client.quit().await;
            return Err(error);
        }
        let id: i64 = match client.client_id().await {
            Ok(id) => id,
            Err(error) => {
                let _ = client.quit().await;
                return Err(error.into());
            }
        };
        Ok(BlockingClient {
            client,
            admin: self.redis().clone(),
            id,
        })
    }

    pub(crate) fn redis_no_retry(&self) -> WithOptions<RedisClient> {
        no_retry(self.redis())
    }

    pub(crate) fn client_id(&self) -> &str {
        &self.id
    }

    pub(crate) fn owner(&self) -> String {
        match tokio::task::try_id() {
            Some(task) => format!("{}:{}", self.id, task),
            None => format!("{}:main", self.id),
        }
    }

    pub(crate) async fn eval<R: FromValue>(
        &self,
        script: &Script,
        keys: Vec<String>,
        args: Vec<Bytes>,
    ) -> Result<R> {
        Ok(script.evalsha_with_reload(self.redis(), keys, args).await?)
    }

    /// Runs a script that must reach Redis at most once. `EVALSHA` goes out as a custom command: fred's own `evalsha` sends the script again after a reconnection even when it may send it only once, so a script whose reply was lost ran twice.
    pub(crate) async fn eval_no_retry<R: FromValue>(
        &self,
        script: &Script,
        keys: Vec<String>,
        args: Vec<Bytes>,
    ) -> Result<R> {
        let client = self.redis();
        match evalsha_once(client, script, &keys, &args).await {
            Err(error) if error.details().starts_with("NOSCRIPT") => {
                script.load(client).await?;
                Ok(evalsha_once(client, script, &keys, &args).await?)
            }
            result => Ok(result?),
        }
    }
}

async fn evalsha_once<R: FromValue>(
    client: &RedisClient,
    script: &Script,
    keys: &[String],
    args: &[Bytes],
) -> std::result::Result<R, fred::error::Error> {
    // fred routes a custom command only by an explicit slot, not by a key offset.
    let hash = keys
        .first()
        .map_or(fred::types::ClusterHash::Random, |key| {
            fred::types::ClusterHash::Custom(fred::util::redis_keyslot(key.as_bytes()))
        });
    let command = fred::types::CustomCommand::new_static("EVALSHA", hash, false);
    let mut values: Vec<fred::types::Value> = Vec::with_capacity(2 + keys.len() + args.len());
    values.push(script.sha1().to_string().into());
    values.push((keys.len() as i64).into());
    values.extend(
        keys.iter()
            .map(|key| fred::types::Value::from(key.as_str())),
    );
    values.extend(args.iter().cloned().map(fred::types::Value::Bytes));
    no_retry(client).custom(command, values).await
}

async fn set_name(
    client: &RedisClient,
    name: &str,
    server: Option<fred::types::config::Server>,
) -> Result<()> {
    if !client.is_clustered() {
        return Ok(client.client_setname(name.to_string()).await?);
    }
    let servers = match server {
        Some(server) => vec![server],
        None => client
            .cached_cluster_state()
            .map(|state| state.unique_primary_nodes())
            .unwrap_or_default(),
    };
    for server in servers {
        client
            .with_cluster_node(server)
            .client_setname(name.to_string())
            .await?;
    }
    Ok(())
}

async fn ping_loop(core: std::sync::Weak<Core>, interval: Duration) {
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    loop {
        ticks.tick().await;
        let Some(strong) = core.upgrade() else {
            return;
        };
        let wait = if strong.retry.timeout.is_zero() {
            interval
        } else {
            strong.retry.timeout
        };
        let mut clients: Vec<RedisClient> = strong.pool.clients().to_vec();
        if let Some(pubsub) = strong.pubsub_started() {
            clients.push(pubsub.client().clone());
        }
        drop(strong);
        for client in clients {
            if !client.is_connected() {
                continue;
            }
            let answered = tokio::time::timeout(wait, client.ping::<()>(None)).await;
            let silent = match answered {
                Err(_) => true,
                Ok(Err(error)) => *error.kind() == fred::error::ErrorKind::Timeout,
                Ok(Ok(())) => false,
            };
            if silent {
                let _ = client.force_reconnection().await;
            }
        }
        // The connections that batches gave back are pinged too, as Redisson pings every connection. They are out of the idle list meanwhile, so a `PING` never lands in a transaction, and one that does not answer is closed.
        let Some(strong) = core.upgrade() else {
            return;
        };
        let idle = std::mem::take(&mut *strong.exclusive.lock().unwrap_or_else(|e| e.into_inner()));
        drop(strong);
        for client in idle {
            let answered = client.is_connected()
                && matches!(
                    tokio::time::timeout(wait, client.ping::<()>(None)).await,
                    Ok(Ok(()))
                );
            match core.upgrade() {
                Some(strong) if answered => strong
                    .exclusive
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(client),
                _ => {
                    tokio::spawn(async move {
                        let _ = client.quit().await;
                    });
                }
            }
        }
    }
}

pub(crate) fn no_retry<C: ClientLike>(client: &C) -> WithOptions<C> {
    client.with_options(&Options {
        max_attempts: Some(1),
        ..Default::default()
    })
}

pub(crate) struct BlockingClient {
    client: RedisClient,
    admin: RedisClient,
    id: i64,
}

impl std::ops::Deref for BlockingClient {
    type Target = RedisClient;

    fn deref(&self) -> &RedisClient {
        &self.client
    }
}

impl Drop for BlockingClient {
    fn drop(&mut self) {
        if let Ok(runtime) = Handle::try_current() {
            let client = self.client.clone();
            let admin = self.admin.clone();
            let id = self.id;
            runtime.spawn(async move {
                let _ = admin
                    .client_kill::<i64>(vec![ClientKillFilter::ID(id.to_string())])
                    .await;
                let _ = client.quit().await;
            });
        }
    }
}

impl Drop for Core {
    fn drop(&mut self) {
        let evictors = self.evictors.get_mut().unwrap_or_else(|e| e.into_inner());
        evictors.values().for_each(|evictor| evictor.handle.abort());
        let background = self.background.get_mut().unwrap_or_else(|e| e.into_inner());
        background.iter().for_each(|handle| handle.abort());
        let Ok(runtime) = Handle::try_current() else {
            return;
        };
        let pool = self.pool.clone();
        let pubsub = self.pubsub.take();
        let exclusive = std::mem::take(self.exclusive.get_mut().unwrap_or_else(|e| e.into_inner()));
        runtime.spawn(async move {
            let _ = pool.quit().await;
            if let Some(pubsub) = pubsub {
                pubsub.quit().await;
            }
            for client in exclusive {
                let _ = client.quit().await;
            }
        });
    }
}
