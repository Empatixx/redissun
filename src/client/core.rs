use crate::client::request::Request;
use crate::client::retry::Retry;
use crate::error::{Error, Result};
use crate::lua::Lua;
use crate::net::node::{Failure, Node};
use crate::net::topology::Topology;
use crate::pubsub::PubSub;
use bytes::Bytes;
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
    /// How often open connections log in again with what the credentials resolver gives now, Redisson's `nextRenewal`.
    pub(crate) credentials_refresh: Option<Duration>,
}

pub(crate) struct Core {
    /// The servers and the connections to them; every command of `query` goes through it.
    topology: Arc<Topology>,
    id: String,
    pub(crate) lock_lease: Duration,
    pub(crate) lock_settings: crate::locks::LockSettings,
    pub(crate) eviction_interval: Duration,
    pub(crate) retry: Retry,
    pub(crate) read_only_scripts: std::sync::atomic::AtomicBool,
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
        let core = Arc::new(Self {
            topology,
            id: Uuid::new_v4().to_string(),
            lock_lease: startup.lock_lease,
            lock_settings: startup.lock_settings,
            eviction_interval: startup.eviction_interval,
            retry: startup.retry,
            read_only_scripts: std::sync::atomic::AtomicBool::new(true),
            background: std::sync::Mutex::new(Vec::new()),
            pubsub: tokio::sync::OnceCell::new(),
            evictors: std::sync::Mutex::new(std::collections::HashMap::new()),
            exclusive: std::sync::Mutex::new(std::collections::HashMap::new()),
            adders: Default::default(),
            batch_connections: Arc::new(tokio::sync::Semaphore::new(startup.pool_size)),
        });
        if !startup.ping_interval.is_zero() {
            let ping = tokio::spawn(ping_loop(Arc::downgrade(&core), startup.ping_interval));
            core.spawned(ping);
        }
        if let Some(interval) = startup.credentials_refresh.filter(|i| !i.is_zero()) {
            let renewal = tokio::spawn(renewal_loop(Arc::downgrade(&core), interval));
            core.spawned(renewal);
        }
        Ok(core)
    }

    fn spawned(&self, handle: tokio::task::JoinHandle<()>) {
        self.background
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(handle);
    }

    pub(crate) async fn pubsub(&self) -> Result<&Arc<PubSub>> {
        self.pubsub
            .get_or_try_init(|| async { PubSub::start(self.topology.clone()).await })
            .await
    }

    pub(crate) fn pubsub_started(&self) -> Option<&Arc<PubSub>> {
        self.pubsub.get()
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
        let once = request.is_once();
        let attempts = self.retry.attempts;
        let send = async {
            let mut attempt = 0;
            loop {
                match self
                    .topology
                    .send(request.cmd(), request.routing_key())
                    .await
                {
                    // A command Redis never saw goes again, even one sent once; within `timeout` that costs no attempt, as the command waits for a connection (a node's reconnection delay paces it).
                    Err(Failure::Unsent(_)) if !self.retry.timeout.is_zero() => {}
                    Err(Failure::Unsent(_)) if attempt < attempts => attempt += 1,
                    Err(Failure::Connection(_)) if !once && attempt < attempts => attempt += 1,
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

/// Every `interval`, each open connection sends `AUTH` with what the credentials resolver gives for its node now; connections that a batch holds get theirs when they are opened again.
async fn renewal_loop(core: std::sync::Weak<Core>, interval: Duration) {
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    loop {
        ticks.tick().await;
        let Some(strong) = core.upgrade() else {
            return;
        };
        let mut nodes = strong.topology.nodes();
        if let Some(pubsub) = strong.pubsub_started() {
            nodes.push(pubsub.node().await);
        }
        nodes.extend(
            strong
                .exclusive
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .values()
                .flatten()
                .cloned(),
        );
        drop(strong);
        // A failed renewal keeps the login the connection has; the next one tries again.
        futures::future::join_all(nodes.iter().map(|node| node.renew_credentials())).await;
    }
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
        let nodes = self.topology.nodes();
        let pubsub = self.pubsub.take();
        let exclusive = std::mem::take(self.exclusive.get_mut().unwrap_or_else(|e| e.into_inner()));
        runtime.spawn(async move {
            for node in nodes {
                node.reset().await;
            }
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
