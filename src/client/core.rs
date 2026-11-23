use crate::client::request::Request;
use crate::client::retry::Retry;
use crate::error::{Error, Result};
use crate::lua::Lua;
use crate::net::node::{Failure, Node};
use crate::net::topology::Topology;
use crate::pubsub::PubSub;
use bytes::Bytes;
use fred::clients::{Client as RedisClient, Pool, WithOptions};
use fred::interfaces::{ClientInterface, ClientLike, ClusterInterface, EventInterface};
use fred::prelude::{Builder, Options};
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
    /// The servers and the connections to them; every command of `query` goes through it.
    topology: Arc<Topology>,
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
    /// The idle connections of batches, by node.
    pub(crate) exclusive:
        std::sync::Mutex<std::collections::HashMap<crate::net::stream::Address, Vec<Arc<Node>>>>,
    /// The adders of this client by name, so that one name is one counter.
    pub(crate) adders: crate::coordination::adder::Registry,
    /// One permit for each connection that batches may use at the same time, `pool_size` in all, like the connections of Redisson's pool.
    pub(crate) batch_connections: Arc<tokio::sync::Semaphore>,
}

impl Core {
    pub(crate) async fn connect(
        builder: Builder,
        topology: Topology,
        check_slots_coverage: bool,
        startup: Startup,
    ) -> Result<Arc<Self>> {
        let topology = Arc::new(topology);
        match tokio::time::timeout(
            startup.connect_timeout,
            open(&topology, check_slots_coverage),
        )
        .await
        {
            Ok(opened) => opened?,
            Err(_) => return Err(Error::Timeout),
        }
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
            topology,
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
            exclusive: std::sync::Mutex::new(std::collections::HashMap::new()),
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
            .get_or_try_init(|| async { PubSub::start(self.topology.clone()).await })
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
    ///
    /// A command whose connection failed is sent again up to `retry_attempts` times, unless the request is `once`; each attempt waits for the reconnection delay of its node. The `timeout` limits the whole call; zero waits forever.
    pub(crate) async fn query_value(&self, request: Request) -> Result<redis::Value> {
        let attempts = if request.is_once() {
            0
        } else {
            self.retry.attempts
        };
        let send = async {
            let mut attempt = 0;
            loop {
                match self
                    .topology
                    .send(request.cmd(), request.routing_key())
                    .await
                {
                    Err(Failure::Connection(_)) if attempt < attempts => attempt += 1,
                    result => return result.map_err(Error::from),
                }
            }
        };
        if self.retry.timeout.is_zero() {
            return send.await;
        }
        tokio::time::timeout(self.retry.timeout, send)
            .await
            .unwrap_or(Err(Error::Timeout))
    }

    pub(crate) fn topology(&self) -> &Arc<Topology> {
        &self.topology
    }

    pub(crate) fn redis(&self) -> &RedisClient {
        self.pool.next()
    }

    /// Sends a blocking command (`BLPOP`, `BZMPOP`, `XREAD BLOCK`…) once, without the command timeout, on a connection of its own to the master that serves `key`, as Redisson does. When the caller stops waiting, the connection is killed with `CLIENT KILL ID`, so the command takes nothing after that.
    pub(crate) async fn blocking<T: redis::FromRedisValue>(
        &self,
        key: &[u8],
        cmd: redis::Cmd,
    ) -> Result<T> {
        let pool = self
            .topology
            .pool_for(Some(key))
            .await
            .map_err(Error::from)?;
        let node = Arc::new(Node::new(
            pool.address().clone(),
            self.topology.settings().clone(),
        ));
        let mut guard = Unblock {
            node: node.clone(),
            admin: pool.next(),
            waiting: true,
        };
        let value = node.send(&cmd).await.map_err(Error::from);
        guard.waiting = false;
        Ok(T::from_redis_value(value?)?)
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

    /// Runs `lua` with `EVALSHA`, routed by its first key. When that node does not know the script (`NOSCRIPT`), it is loaded there and run once more.
    pub(crate) async fn eval<R: redis::FromRedisValue>(
        &self,
        lua: &Lua,
        keys: Vec<String>,
        args: Vec<Bytes>,
    ) -> Result<R> {
        self.run_lua(lua, &keys, &args, false).await
    }

    /// Like [`eval`](Core::eval) for a script that must reach Redis at most once: it is never sent again after its connection failed, so a script whose reply was lost does not run twice.
    pub(crate) async fn eval_once<R: redis::FromRedisValue>(
        &self,
        lua: &Lua,
        keys: Vec<String>,
        args: Vec<Bytes>,
    ) -> Result<R> {
        self.run_lua(lua, &keys, &args, true).await
    }

    async fn run_lua<R: redis::FromRedisValue>(
        &self,
        lua: &Lua,
        keys: &[String],
        args: &[Bytes],
        once: bool,
    ) -> Result<R> {
        let request = |cmd: redis::Cmd| {
            let request = match keys.first() {
                Some(key) => Request::new(cmd).key(key.as_bytes()),
                None => Request::new(cmd),
            };
            if once {
                request.once()
            } else {
                request
            }
        };
        let evalsha = || {
            let mut cmd = redis::cmd("EVALSHA");
            cmd.arg(lua.sha1()).arg(keys.len()).arg(keys);
            for arg in args {
                cmd.arg(&arg[..]);
            }
            cmd
        };
        match self.query(request(evalsha())).await {
            Err(Error::Redis(message)) if message.contains("NOSCRIPT") => {
                let mut load = redis::cmd("SCRIPT");
                load.arg("LOAD").arg(lua.source());
                let _: String = self.query(request(load)).await?;
                self.query(request(evalsha())).await
            }
            result => result,
        }
    }
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
        let clients: Vec<RedisClient> = strong.pool.clients().to_vec();
        let mut nodes = strong.topology.nodes();
        if let Some(pubsub) = strong.pubsub_started() {
            nodes.push(pubsub.node().await);
        }
        drop(strong);
        // A node whose PING gets no reply in time is closed; the next command opens a new connection.
        for node in nodes {
            let answered = tokio::time::timeout(wait, node.send(&redis::cmd("PING"))).await;
            if !matches!(answered, Ok(Ok(_))) {
                node.reset().await;
            }
        }
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
        for (address, nodes) in idle {
            for node in nodes {
                let answered = matches!(
                    tokio::time::timeout(wait, node.send(&redis::cmd("PING"))).await,
                    Ok(Ok(_))
                );
                match core.upgrade() {
                    Some(strong) if answered => strong
                        .exclusive
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .entry(address.clone())
                        .or_default()
                        .push(node),
                    _ => node.reset().await,
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

/// Closes the connection of a blocking command; one the caller stopped waiting on is killed first, so the server drops the command.
struct Unblock {
    node: Arc<Node>,
    admin: Arc<Node>,
    waiting: bool,
}

impl Drop for Unblock {
    fn drop(&mut self) {
        let Ok(runtime) = Handle::try_current() else {
            return;
        };
        let (node, admin, waiting) = (self.node.clone(), self.admin.clone(), self.waiting);
        runtime.spawn(async move {
            if waiting {
                if let Some(id) = node.client_id().await {
                    let mut kill = redis::cmd("CLIENT");
                    kill.arg("KILL").arg("ID").arg(id);
                    let _ = admin.send(&kill).await;
                }
            }
            node.reset().await;
        });
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
            for node in exclusive.into_values().flatten() {
                node.reset().await;
            }
        });
    }
}

/// Opens every pooled connection, after reading the slot map of a cluster, so that `build` fails on an unreachable server and every connection has its name from the start.
async fn open(topology: &Topology, check_slots_coverage: bool) -> Result<()> {
    if let Topology::Cluster(cluster) = topology {
        cluster.refresh().await?;
        let map = cluster.map();
        if check_slots_coverage && !map.covers_every_slot() {
            return Err(Error::Redis(
                "not every slot of the cluster has a master".into(),
            ));
        }
        for master in map.masters() {
            cluster.open_pool(&master);
        }
    }
    if let Topology::Sentinel(sentinel) = topology {
        sentinel.pool().await?;
    }
    let mut ping = redis::cmd("PING");
    ping.arg("redissun");
    let opened = futures::future::join_all(topology.nodes().into_iter().map(|node| {
        let ping = ping.clone();
        async move { node.send(&ping).await.map(|_| ()) }
    }))
    .await;
    opened
        .into_iter()
        .try_for_each(|opened| opened.map_err(Error::from))
}
