use crate::client::request::Request;
use crate::client::retry::Retry;
use crate::error::{Error, Result};
use crate::lua::Lua;
use crate::net::node::{Failure, Node};
use crate::net::stream::Address;
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
    pub(crate) reliable_topic_watchdog_timeout: Duration,
    pub(crate) lock_settings: crate::locks::LockSettings,
    pub(crate) retry: Retry,
    pub(crate) ping_interval: Duration,
    /// How often open connections log in again with what the credentials resolver gives now, Redisson's `nextRenewal`.
    pub(crate) credentials_refresh: Option<Duration>,
    /// How often to look for replicas while reads may go to them; `None` takes the default of the topology.
    pub(crate) replica_scan_interval: Option<Duration>,
    /// The database number the connections select, 0 by default.
    pub(crate) database: i64,
}

pub(crate) struct Core {
    /// The servers and the connections to them; every command of `query` goes through it.
    topology: Arc<Topology>,
    id: String,
    pub(crate) lock_lease: Duration,
    pub(crate) lock_settings: crate::locks::LockSettings,
    pub(crate) eviction_interval: Duration,
    pub(crate) reliable_topic_watchdog_timeout: Duration,
    pub(crate) retry: Retry,
    /// The database number the connections select, 0 by default; keyspace channels name it.
    pub(crate) database: i64,
    pub(crate) read_only_scripts: std::sync::atomic::AtomicBool,
    background: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    pubsub: tokio::sync::OnceCell<Arc<PubSub>>,
    pub(crate) evictors: std::sync::Mutex<std::collections::HashMap<String, Evictor>>,
    /// The idle connections of batches, by node.
    pub(crate) exclusive:
        std::sync::Mutex<std::collections::HashMap<crate::net::stream::Address, Vec<Arc<Node>>>>,
    /// The shard channels this client listens to.
    pub(crate) sharded: crate::messaging::sharded_topic::Registry,
    /// The adders of this client by name, so that one name is one counter.
    pub(crate) adders: crate::coordination::adder::Registry,
    /// The write-behind of each map name this client drains, so that one name has one drainer.
    pub(crate) write_behind: crate::collections::map::write_behind::Registry,
    /// One permit for each connection that batches may use at the same time, `pool_size` in all, like the connections of Redisson's pool.
    pub(crate) batch_connections: Arc<tokio::sync::Semaphore>,
    /// How the server writes lengths in `struct.pack` payloads, read with `INFO server` when the first map cache listener is added.
    pub(crate) map_cache_length_prefix:
        tokio::sync::OnceCell<crate::collections::hash_map_cache::LengthPrefix>,
    /// Which hash field commands the server has, probed with `HPTTL` and `HSETEX` when a `MapCacheNative` needs field TTLs; only a yes is kept.
    pub(crate) hash_field_support: crate::collections::map_cache_native::HashFieldSupport,
    /// Whether the server has the `GCRA` command, probed by the first `Gcra::try_acquire`. Both answers are kept, so one client never switches between the two state formats.
    pub(crate) gcra_command: tokio::sync::OnceCell<bool>,
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
            reliable_topic_watchdog_timeout: startup.reliable_topic_watchdog_timeout,
            retry: startup.retry,
            database: startup.database,
            read_only_scripts: std::sync::atomic::AtomicBool::new(true),
            background: std::sync::Mutex::new(Vec::new()),
            pubsub: tokio::sync::OnceCell::new(),
            evictors: std::sync::Mutex::new(std::collections::HashMap::new()),
            exclusive: std::sync::Mutex::new(std::collections::HashMap::new()),
            sharded: Default::default(),
            adders: Default::default(),
            write_behind: Default::default(),
            batch_connections: Arc::new(tokio::sync::Semaphore::new(startup.pool_size)),
            map_cache_length_prefix: tokio::sync::OnceCell::new(),
            hash_field_support: Default::default(),
            gcra_command: tokio::sync::OnceCell::new(),
        });
        if !startup.ping_interval.is_zero() {
            let ping = tokio::spawn(ping_loop(Arc::downgrade(&core), startup.ping_interval));
            core.spawned(ping);
        }
        if let Some(interval) = startup.credentials_refresh.filter(|i| !i.is_zero()) {
            let renewal = tokio::spawn(renewal_loop(Arc::downgrade(&core), interval));
            core.spawned(renewal);
        }
        if let Some((mode, default)) = core.topology.replica_scanning() {
            let interval = startup.replica_scan_interval.unwrap_or(default);
            if mode != crate::ReadMode::Master && !interval.is_zero() {
                let scan = tokio::spawn(replica_scan_loop(Arc::downgrade(&core), interval));
                core.spawned(scan);
            }
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
                let sent = if request.is_read() {
                    self.topology
                        .send_read(request.cmd(), request.routing_key())
                        .await
                } else {
                    self.topology
                        .send(request.cmd(), request.routing_key())
                        .await
                };
                match sent {
                    // A command Redis never saw goes again, even one sent once; within `timeout` that costs no attempt, as the command waits for a connection (a node's reconnection delay paces it).
                    Err(Failure::Unsent(_)) if !self.retry.timeout.is_zero() => {}
                    Err(Failure::Unsent(_)) if attempt < attempts => attempt += 1,
                    Err(Failure::Connection(_)) if !once && attempt < attempts => attempt += 1,
                    result => return result.map_err(Error::from),
                }
                tracing::debug!(
                    "attempt {attempt} for {}",
                    crate::net::node::command_name(request.cmd())
                );
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

    /// Whether the client talks to a Redis Cluster, where a command's keys must share one hash slot.
    pub(crate) fn is_cluster(&self) -> bool {
        matches!(*self.topology, Topology::Cluster(_))
    }

    /// The masters the client knows now, Redisson's `getEntrySet`: the one server, the master the sentinels name, or every master of a cluster.
    pub(crate) async fn masters(&self) -> Result<Vec<Address>> {
        let masters = self.topology.addresses(crate::NodeType::Master).await;
        if masters.is_empty() {
            return Err(Error::Redis("the client knows no master".into()));
        }
        Ok(masters)
    }

    /// Sends `cmd` to the master at `address`, limited by the client's `timeout`, as Redisson sends a command to one entry of `getEntrySet`.
    pub(crate) async fn query_master(
        &self,
        address: &Address,
        cmd: &redis::Cmd,
    ) -> Result<redis::Value> {
        let node = self
            .topology
            .node_at(address, crate::NodeType::Master)
            .await
            .ok_or_else(|| {
                Error::Redis(format!("{address} is no longer a master of the client"))
            })?;
        self.send_on(&node, cmd).await
    }

    /// Sends `cmd` to every master at once and returns their replies in the order of [`masters`](Core::masters), Redisson's `readAllAsync` and `writeAllAsync`.
    pub(crate) async fn query_masters(&self, cmd: &redis::Cmd) -> Result<Vec<redis::Value>> {
        let masters = self.masters().await?;
        futures::future::try_join_all(masters.iter().map(|master| self.query_master(master, cmd)))
            .await
    }

    /// Sends `cmd` on `node`, limited by the client's `timeout`.
    pub(crate) async fn send_on(&self, node: &Node, cmd: &redis::Cmd) -> Result<redis::Value> {
        let sent = node.send(cmd);
        if self.retry.timeout.is_zero() {
            return sent.await.map_err(Error::from);
        }
        match tokio::time::timeout(self.retry.timeout, sent).await {
            Ok(result) => result.map_err(Error::from),
            Err(_) => Err(Error::Timeout),
        }
    }

    /// Sends a command over many keys, built by `build` from the indexes of the keys it carries, as Redisson's `readBatchedAsync` and `writeBatchedAsync` do: one command for all of them, or in a cluster one for each hash slot, all at once. Returns each command's indexes with its reply.
    pub(crate) async fn query_by_slot<K: AsRef<[u8]>>(
        &self,
        keys: &[K],
        read: bool,
        build: impl Fn(&[usize]) -> redis::Cmd,
    ) -> Result<Vec<(Vec<usize>, redis::Value)>> {
        let groups = if self.is_cluster() {
            crate::client::slot::group_by_slot(keys.iter().map(AsRef::as_ref))
        } else {
            vec![(0..keys.len()).collect()]
        };
        let sends = groups
            .into_iter()
            .filter(|group| !group.is_empty())
            .map(|group| {
                let mut request = Request::new(build(&group)).key(keys[group[0]].as_ref());
                if read {
                    request = request.read();
                }
                async move { Ok::<_, Error>((group, self.query_value(request).await?)) }
            });
        futures::future::try_join_all(sends).await
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
/// Looks for replicas again every `interval`, like Redisson's `scanInterval`, so a replica that was added or came back gets reads, and one that left gets none.
async fn replica_scan_loop(core: std::sync::Weak<Core>, interval: Duration) {
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    loop {
        ticks.tick().await;
        let Some(strong) = core.upgrade() else {
            return;
        };
        let topology = strong.topology.clone();
        drop(strong);
        topology.scan_replicas().await;
    }
}

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
        let renewed =
            futures::future::join_all(nodes.iter().map(|node| node.renew_credentials())).await;
        for (node, result) in nodes.iter().zip(renewed) {
            if let Err(failure) = result {
                tracing::warn!(
                    "Renewal cycle failed, retrying: {}: {}",
                    node.address(),
                    Error::from(failure)
                );
            }
        }
    }
}

/// Whether `node` answers `PING` within `wait`; one that does not is logged like Redisson's `PingConnectionHandler` logs it.
async fn pinged(node: &Node, wait: Duration) -> bool {
    match tokio::time::timeout(wait, node.send(&redis::cmd("PING"))).await {
        Ok(answered) => answered.is_ok(),
        Err(_) => {
            tracing::debug!(
                "connection to {} closed due to PING response timeout set in {} ms",
                node.address(),
                wait.as_millis()
            );
            false
        }
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
            if !pinged(&node, wait).await {
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
                let answered = pinged(&node, wait).await;
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
        // The connections close from here on; Redisson reports nothing while it shuts down.
        if let Some(events) = &self.topology.settings().events {
            events.close();
        }
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
