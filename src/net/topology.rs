//! Which node a command goes to: one server, the master that Sentinel names, or the master of the key's slot in Redis Cluster.

use crate::client::read_mode::ReadMode;
use crate::client::slot::keyslot;
use crate::error::Error;
use crate::net::node::{is_connection_failure, Failure, Node, NodeSettings};
use crate::net::stream::Address;
use redis::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

const SLOTS: u32 = 16384;
/// How many `MOVED` or `ASK` replies one command follows, as Redisson follows them within its retries.
const REDIRECTS: usize = 5;

/// The slots `start..=end`, their master and its replicas.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SlotRange {
    pub(crate) start: u16,
    pub(crate) end: u16,
    pub(crate) master: Address,
    pub(crate) replicas: Vec<Address>,
}

fn node_address(value: &Value) -> Option<Address> {
    let Value::Array(parts) = value else {
        return None;
    };
    let host = match parts.first()? {
        Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok()?,
        Value::SimpleString(text) => text.clone(),
        Value::Nil => String::new(),
        _ => return None,
    };
    let port = match parts.get(1)? {
        Value::Int(port) => u16::try_from(*port).ok()?,
        _ => return None,
    };
    Some(Address::new(host, port))
}

/// The ranges of a `CLUSTER SLOTS` reply. An empty host stands for the node that answered; the caller fills it in.
pub(crate) fn parse_slots(reply: &Value) -> Option<Vec<SlotRange>> {
    let Value::Array(ranges) = reply else {
        return None;
    };
    ranges
        .iter()
        .map(|range| {
            let Value::Array(parts) = range else {
                return None;
            };
            let bound = |value: &Value| match value {
                Value::Int(slot) => u16::try_from(*slot).ok(),
                _ => None,
            };
            Some(SlotRange {
                start: bound(parts.first()?)?,
                end: bound(parts.get(1)?)?,
                master: node_address(parts.get(2)?)?,
                replicas: parts.iter().skip(3).filter_map(node_address).collect(),
            })
        })
        .collect()
}

/// Which master serves each slot.
#[derive(Clone, Debug, Default)]
pub(crate) struct SlotMap {
    ranges: Vec<SlotRange>,
}

impl SlotMap {
    pub(crate) fn new(mut ranges: Vec<SlotRange>) -> Self {
        ranges.sort_by_key(|range| range.start);
        Self { ranges }
    }

    pub(crate) fn range(&self, slot: u16) -> Option<&SlotRange> {
        let index = self.ranges.partition_point(|range| range.end < slot);
        self.ranges.get(index).filter(|range| range.start <= slot)
    }

    pub(crate) fn master(&self, slot: u16) -> Option<&Address> {
        self.range(slot).map(|range| &range.master)
    }

    pub(crate) fn masters(&self) -> Vec<Address> {
        let mut masters: Vec<Address> = Vec::new();
        for range in &self.ranges {
            if !masters.contains(&range.master) {
                masters.push(range.master.clone());
            }
        }
        masters
    }

    /// Every replica the map lists.
    pub(crate) fn replicas(&self) -> Vec<Address> {
        let mut replicas: Vec<Address> = Vec::new();
        for replica in self.ranges.iter().flat_map(|range| &range.replicas) {
            if !replicas.contains(replica) {
                replicas.push(replica.clone());
            }
        }
        replicas
    }

    /// The replicas of `master`.
    fn replicas_of(&self, master: &Address) -> Vec<Address> {
        let mut replicas: Vec<Address> = Vec::new();
        for range in self.ranges.iter().filter(|range| range.master == *master) {
            for replica in &range.replicas {
                if !replicas.contains(replica) {
                    replicas.push(replica.clone());
                }
            }
        }
        replicas
    }

    /// The slots of `master` as Redisson logs them: `[0-5460]`.
    fn slot_ranges(&self, master: &Address) -> String {
        let ranges: Vec<String> = self
            .ranges
            .iter()
            .filter(|range| range.master == *master)
            .map(|range| format!("[{}-{}]", range.start, range.end))
            .collect();
        ranges.join(", ")
    }

    /// Whether every one of the 16384 slots has a master, Redisson's `checkSlotsCoverage`.
    pub(crate) fn covers_every_slot(&self) -> bool {
        let covered: u32 = self
            .ranges
            .iter()
            .map(|range| u32::from(range.end) - u32::from(range.start) + 1)
            .sum();
        covered == SLOTS
    }
}

/// Logs the masters and replicas that `new` adds to or drops from `old`, as Redisson's `ClusterConnectionManager` does.
fn log_slot_changes(old: &SlotMap, new: &SlotMap) {
    let known = old.masters();
    for master in new.masters() {
        let slots = new.slot_ranges(&master);
        if !known.contains(&master) {
            tracing::info!("master: {master} added for slot ranges: {slots}");
        }
        let before = old.replicas_of(&master);
        let now = new.replicas_of(&master);
        for replica in now.iter().filter(|replica| !before.contains(replica)) {
            tracing::info!("replica: {replica} added for master {master} slot ranges: {slots}");
        }
        for replica in before.iter().filter(|replica| !now.contains(replica)) {
            tracing::info!("replica {replica} removed for master {master} slot ranges: {slots}");
        }
    }
}

/// Warns once for each master that has no replica to read from while reads should go to replicas, as Redisson's `MasterSlaveEntry` does; `warned` holds the masters already warned about.
fn warn_missing_replicas(
    mode: ReadMode,
    masters: impl IntoIterator<Item = (Address, bool)>,
    warned: &std::sync::Mutex<std::collections::HashSet<Address>>,
) {
    if mode == ReadMode::Master {
        return;
    }
    let mut warned = warned.lock().unwrap_or_else(|e| e.into_inner());
    for (master, has_replicas) in masters {
        if has_replicas {
            warned.remove(&master);
        } else if warned.insert(master.clone()) {
            tracing::warn!(
                "ReadMode = {mode:?}, but replica nodes are not found for master {master}!"
            );
        }
    }
}

/// `pool_size` connections to one node, used in turn.
pub(crate) struct Pool {
    nodes: Vec<Arc<Node>>,
    next: AtomicUsize,
}

impl Pool {
    pub(crate) fn new(address: &Address, size: usize, settings: &Arc<NodeSettings>) -> Self {
        Self {
            nodes: (0..size.max(1))
                .map(|_| Arc::new(Node::new(address.clone(), settings.clone())))
                .collect(),
            next: AtomicUsize::new(0),
        }
    }

    pub(crate) fn next(&self) -> Arc<Node> {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.nodes.len();
        self.nodes[index].clone()
    }

    pub(crate) fn address(&self) -> &Address {
        self.nodes[0].address()
    }
}

/// Picks the node a read goes to, as the client's [`ReadMode`] says.
pub(crate) struct ReadRouting {
    mode: ReadMode,
    next: AtomicUsize,
}

impl ReadRouting {
    pub(crate) fn new(mode: ReadMode) -> Self {
        Self {
            mode,
            next: AtomicUsize::new(0),
        }
    }

    pub(crate) fn mode(&self) -> ReadMode {
        self.mode
    }

    /// The replica the next read goes to, or `None` for the master. Like Redisson's round robin, `MasterAndReplica` counts the master as one more replica, and `Replica` reads from the master while there is no replica.
    fn pick<'a>(&self, replicas: &'a [Address]) -> Option<&'a Address> {
        let candidates = match self.mode {
            ReadMode::Master => return None,
            ReadMode::Replica => replicas.len(),
            ReadMode::MasterAndReplica => replicas.len() + 1,
        };
        if replicas.is_empty() {
            return None;
        }
        let turn = self.next.fetch_add(1, Ordering::Relaxed) % candidates;
        replicas.get(turn)
    }
}

/// Whether a read that failed on a replica goes to the master instead: the replica could not be reached, does not serve the slot any more, is loading its data or lost its master. An error about the data itself (`WRONGTYPE`) would be the same on the master.
fn replica_failed(failure: &Failure) -> bool {
    match failure {
        Failure::Connection(_) | Failure::Unsent(_) | Failure::Redirect { .. } => true,
        Failure::Other(Error::Redis(message)) => [
            "LOADING",
            "MASTERDOWN",
            "READONLY",
            "CLUSTERDOWN",
            "TRYAGAIN",
        ]
        .iter()
        .any(|code| message.starts_with(code)),
        Failure::Other(_) => false,
    }
}

/// Pools to replicas, opened on their first read and dropped when the replica is no longer listed.
struct ReplicaPools {
    size: usize,
    settings: Arc<NodeSettings>,
    pools: RwLock<HashMap<Address, Arc<Pool>>>,
}

impl ReplicaPools {
    fn new(size: usize, settings: Arc<NodeSettings>) -> Self {
        Self {
            size,
            settings,
            pools: RwLock::new(HashMap::new()),
        }
    }

    fn pool(&self, address: &Address) -> Arc<Pool> {
        if let Some(pool) = self
            .pools
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(address)
        {
            return pool.clone();
        }
        self.pools
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .entry(address.clone())
            .or_insert_with(|| Arc::new(Pool::new(address, self.size, &self.settings)))
            .clone()
    }

    fn keep(&self, listed: &[Address]) {
        self.pools
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|address, _| listed.contains(address));
    }

    fn nodes(&self) -> Vec<Arc<Node>> {
        self.pools
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .flat_map(|pool| pool.nodes.clone())
            .collect()
    }
}

/// The servers behind a client.
pub(crate) enum Topology {
    Single(Pool),
    Sentinel(Sentinel),
    Cluster(Cluster),
}

impl Topology {
    /// Sends `cmd` to the node that serves `key` (any node without a key) and follows `MOVED` and `ASK`.
    pub(crate) async fn send(
        &self,
        cmd: &redis::Cmd,
        key: Option<&[u8]>,
    ) -> Result<Value, Failure> {
        match self {
            Topology::Single(pool) => pool.next().send(cmd).await,
            Topology::Sentinel(sentinel) => sentinel.send(cmd).await,
            Topology::Cluster(cluster) => cluster.send(cmd, key).await,
        }
    }

    /// Sends a command that only reads where the client's [`ReadMode`] says: to a replica of the node that serves `key`, or to that node. A read a replica cannot answer goes to the master.
    pub(crate) async fn send_read(
        &self,
        cmd: &redis::Cmd,
        key: Option<&[u8]>,
    ) -> Result<Value, Failure> {
        match self {
            Topology::Single(pool) => pool.next().send(cmd).await,
            Topology::Sentinel(sentinel) => sentinel.send_read(cmd).await,
            Topology::Cluster(cluster) => cluster.send_read(cmd, key).await,
        }
    }

    /// Looks for replicas again, like Redisson's `scanInterval`: the slot map of a cluster, the replicas Sentinel reports. A failed scan keeps what was known.
    pub(crate) async fn scan_replicas(&self) {
        match self {
            Topology::Single(_) => {}
            Topology::Sentinel(sentinel) => sentinel.scan_replicas().await,
            Topology::Cluster(cluster) => drop(cluster.refresh().await),
        }
    }

    /// The read mode, and how often to scan for replicas by default: Redisson's `scanInterval` is 1 s with Sentinel and 5 s in a cluster. `None` for a single server, which has no replicas.
    pub(crate) fn replica_scanning(&self) -> Option<(ReadMode, std::time::Duration)> {
        match self {
            Topology::Single(_) => None,
            Topology::Sentinel(sentinel) => {
                Some((sentinel.reads.mode(), std::time::Duration::from_secs(1)))
            }
            Topology::Cluster(cluster) => {
                Some((cluster.reads.mode(), std::time::Duration::from_secs(5)))
            }
        }
    }

    /// The pool of the node that serves `key`, the master in Sentinel, any master in a cluster without a key.
    pub(crate) async fn pool_for(&self, key: Option<&[u8]>) -> Result<Arc<Pool>, Failure> {
        match self {
            Topology::Single(pool) => Ok(Arc::new(Pool {
                nodes: pool.nodes.clone(),
                next: AtomicUsize::new(pool.next.load(Ordering::Relaxed)),
            })),
            Topology::Sentinel(sentinel) => sentinel.pool().await,
            Topology::Cluster(cluster) => cluster.pool_for(key).await,
        }
    }

    /// Every pooled connection, for the `PING` check.
    pub(crate) fn nodes(&self) -> Vec<Arc<Node>> {
        match self {
            Topology::Single(pool) => pool.nodes.clone(),
            Topology::Sentinel(sentinel) => sentinel.nodes(),
            Topology::Cluster(cluster) => cluster.nodes(),
        }
    }

    /// Sends `cmd` to every master and returns their replies in order; the first failure ends it.
    pub(crate) async fn send_to_masters(&self, cmd: &redis::Cmd) -> Result<Vec<Value>, Failure> {
        let pools: Vec<Arc<Pool>> = match self {
            Topology::Single(_) | Topology::Sentinel(_) => vec![self.pool_for(None).await?],
            Topology::Cluster(cluster) => cluster
                .map()
                .masters()
                .iter()
                .map(|master| cluster.open_pool(master))
                .collect(),
        };
        let mut replies = Vec::with_capacity(pools.len());
        for pool in pools {
            replies.push(pool.next().send(cmd).await?);
        }
        Ok(replies)
    }

    /// The settings every node of this client connects with.
    pub(crate) fn settings(&self) -> &Arc<NodeSettings> {
        match self {
            Topology::Single(pool) => pool.nodes[0].settings_ref(),
            Topology::Sentinel(sentinel) => &sentinel.settings,
            Topology::Cluster(cluster) => &cluster.settings,
        }
    }

    /// Reads where the servers are again: the slot map of a cluster, the master that the sentinels name. A failed read keeps what was known.
    pub(crate) async fn refresh(&self) {
        match self {
            Topology::Single(_) => {}
            Topology::Sentinel(sentinel) => sentinel.refresh().await,
            Topology::Cluster(cluster) => drop(cluster.refresh().await),
        }
    }

    /// The slot map of a cluster, `None` otherwise.
    pub(crate) fn slot_map(&self) -> Option<SlotMap> {
        match self {
            Topology::Cluster(cluster) => Some(cluster.map()),
            _ => None,
        }
    }
}

/// Answers whether a failure says the master moved, after a Sentinel failover.
fn master_moved(failure: &Failure) -> bool {
    match failure {
        Failure::Connection(_) | Failure::Unsent(_) => true,
        Failure::Other(Error::Redis(message)) => message.contains("READONLY"),
        _ => false,
    }
}

/// The master that the sentinels name, and a pool to it that moves when they name another.
pub(crate) struct Sentinel {
    sentinels: Vec<Address>,
    service: String,
    size: usize,
    settings: Arc<NodeSettings>,
    sentinel_settings: Arc<NodeSettings>,
    master: tokio::sync::RwLock<Option<Arc<Pool>>>,
    reads: ReadRouting,
    /// The replicas Sentinel reported last, `None` before the first scan.
    replicas: RwLock<Option<Vec<Address>>>,
    replica_pools: ReplicaPools,
    /// The master that was last warned about having no replica.
    warned: std::sync::Mutex<std::collections::HashSet<Address>>,
}

/// The replicas of a `SENTINEL REPLICAS` reply that serve `master`, leaving out, as Redisson does, those marked `s_down` or `disconnected` and those whose link to the master is in error.
pub(crate) fn parse_sentinel_replicas(reply: &Value, master: &Address) -> Vec<Address> {
    let Value::Array(entries) = reply else {
        return Vec::new();
    };
    let text = |value: &Value| match value {
        Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
        Value::SimpleString(text) => Some(text.clone()),
        _ => None,
    };
    entries
        .iter()
        .filter_map(|entry| {
            let fields: HashMap<String, String> = match entry {
                Value::Array(flat) => flat
                    .chunks(2)
                    .filter_map(|pair| Some((text(pair.first()?)?, text(pair.get(1)?)?)))
                    .collect(),
                Value::Map(pairs) => pairs
                    .iter()
                    .filter_map(|(field, value)| Some((text(field)?, text(value)?)))
                    .collect(),
                _ => return None,
            };
            let field = |name: &str| fields.get(name).map(String::as_str).unwrap_or("");
            let flags = field("flags");
            if flags.contains("s_down")
                || flags.contains("disconnected")
                || field("master-link-status").contains("err")
            {
                return None;
            }
            let serves_master = field("master-host") == master.host
                && field("master-port").parse::<u16>().ok() == Some(master.port);
            if !serves_master {
                return None;
            }
            Some(Address::new(field("ip"), field("port").parse().ok()?))
        })
        .collect()
}

impl Sentinel {
    pub(crate) fn new(
        sentinels: Vec<Address>,
        service: String,
        size: usize,
        settings: Arc<NodeSettings>,
        sentinel_settings: Arc<NodeSettings>,
        read_mode: ReadMode,
    ) -> Self {
        Self {
            sentinels,
            service,
            size,
            replica_pools: ReplicaPools::new(size, settings.clone()),
            settings,
            sentinel_settings,
            master: tokio::sync::RwLock::new(None),
            reads: ReadRouting::new(read_mode),
            replicas: RwLock::new(None),
            warned: Default::default(),
        }
    }

    /// Asks the sentinels which replicas serve the current master. Nothing changes when none answers.
    async fn scan_replicas(&self) {
        let Ok(master) = self.pool().await else {
            return;
        };
        let master = master.address().clone();
        let mut cmd = redis::cmd("SENTINEL");
        cmd.arg("REPLICAS").arg(&self.service);
        for sentinel in &self.sentinels {
            let node = Node::new(sentinel.clone(), self.sentinel_settings.clone());
            if let Ok(reply) = node.send(&cmd).await {
                let replicas = parse_sentinel_replicas(&reply, &master);
                self.replica_pools.keep(&replicas);
                let before = self
                    .replicas
                    .write()
                    .unwrap_or_else(|e| e.into_inner())
                    .replace(replicas.clone())
                    .unwrap_or_default();
                // Redisson's `SentinelConnectionManager` logs each replica that comes and goes.
                for replica in replicas.iter().filter(|replica| !before.contains(replica)) {
                    tracing::info!("replica: {replica} added");
                }
                for replica in before.iter().filter(|replica| !replicas.contains(replica)) {
                    tracing::warn!("replica: {replica} is down");
                }
                warn_missing_replicas(
                    self.reads.mode(),
                    [(master.clone(), !replicas.is_empty())],
                    &self.warned,
                );
                return;
            }
        }
    }

    async fn send_read(&self, cmd: &redis::Cmd) -> Result<Value, Failure> {
        if self.reads.mode() == ReadMode::Master {
            return self.send(cmd).await;
        }
        let known = self
            .replicas
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let replicas = match known {
            Some(replicas) => replicas,
            None => {
                self.scan_replicas().await;
                self.replicas
                    .read()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone()
                    .unwrap_or_default()
            }
        };
        let Some(replica) = self.reads.pick(&replicas) else {
            return self.send(cmd).await;
        };
        match self.replica_pools.pool(replica).next().send(cmd).await {
            Err(failure) if replica_failed(&failure) => self.send(cmd).await,
            other => other,
        }
    }

    /// Asks each sentinel in turn for the master's address.
    async fn resolve(&self) -> Result<Address, Failure> {
        let mut last = Failure::Connection("no sentinel answered".into());
        for sentinel in &self.sentinels {
            let node = Node::new(sentinel.clone(), self.sentinel_settings.clone());
            let mut cmd = redis::cmd("SENTINEL");
            cmd.arg("GET-MASTER-ADDR-BY-NAME").arg(&self.service);
            match node.send(&cmd).await {
                Ok(Value::Array(parts)) => {
                    let text = |value: Option<&Value>| match value {
                        Some(Value::BulkString(bytes)) => String::from_utf8(bytes.clone()).ok(),
                        Some(Value::SimpleString(text)) => Some(text.clone()),
                        _ => None,
                    };
                    let host = text(parts.first());
                    let port = text(parts.get(1)).and_then(|port| port.parse().ok());
                    if let (Some(host), Some(port)) = (host, port) {
                        return Ok(Address::new(host, port));
                    }
                }
                Ok(_) => {
                    last = Failure::Other(Error::Redis(format!(
                        "sentinel knows no master {}",
                        self.service
                    )))
                }
                Err(failure) => last = failure,
            }
        }
        Err(last)
    }

    /// The pool to the current master, found through the sentinels when there is none.
    pub(crate) async fn pool(&self) -> Result<Arc<Pool>, Failure> {
        if let Some(pool) = self.master.read().await.as_ref() {
            return Ok(pool.clone());
        }
        let mut master = self.master.write().await;
        if let Some(pool) = master.as_ref() {
            return Ok(pool.clone());
        }
        let address = self.resolve().await?;
        let pool = Arc::new(Pool::new(&address, self.size, &self.settings));
        *master = Some(pool.clone());
        Ok(pool)
    }

    /// Asks the sentinels again; the pool moves when they name another master.
    async fn refresh(&self) {
        let address = match self.resolve().await {
            Ok(address) => address,
            Err(failure) => {
                tracing::error!(
                    "Can't update cluster state. A new attempt will be made. {}",
                    Error::from(failure)
                );
                return;
            }
        };
        let mut master = self.master.write().await;
        if master
            .as_ref()
            .is_some_and(|pool| *pool.address() == address)
        {
            return;
        }
        if let Some(old) = master.as_ref() {
            tracing::info!("master {} has been changed to {address}", old.address());
        }
        *master = Some(Arc::new(Pool::new(&address, self.size, &self.settings)));
        drop(master);
        // The replicas belong to the old master; the next read asks for those of the new one.
        *self.replicas.write().unwrap_or_else(|e| e.into_inner()) = None;
    }

    async fn send(&self, cmd: &redis::Cmd) -> Result<Value, Failure> {
        let result = self.pool().await?.next().send(cmd).await;
        if let Err(failure) = &result {
            if master_moved(failure) {
                self.refresh().await;
            }
        }
        result
    }

    fn nodes(&self) -> Vec<Arc<Node>> {
        let mut nodes: Vec<Arc<Node>> = self
            .master
            .try_read()
            .ok()
            .and_then(|master| master.as_ref().map(|pool| pool.nodes.clone()))
            .unwrap_or_default();
        nodes.extend(self.replica_pools.nodes());
        nodes
    }
}

/// The slot map of Redis Cluster and a pool to each master.
pub(crate) struct Cluster {
    seeds: Vec<Address>,
    size: usize,
    settings: Arc<NodeSettings>,
    map: RwLock<SlotMap>,
    pools: RwLock<HashMap<Address, Arc<Pool>>>,
    refreshing: tokio::sync::Mutex<()>,
    reads: ReadRouting,
    /// Connections to replicas send `READONLY`, so they serve reads of the slots of their master.
    replica_pools: ReplicaPools,
    /// The masters already warned about having no replica.
    warned: std::sync::Mutex<std::collections::HashSet<Address>>,
}

impl Cluster {
    pub(crate) fn new(
        seeds: Vec<Address>,
        size: usize,
        settings: Arc<NodeSettings>,
        read_mode: ReadMode,
    ) -> Self {
        let mut replica_settings = NodeSettings::clone(&settings);
        replica_settings.readonly = true;
        Self {
            seeds,
            size,
            settings,
            map: RwLock::new(SlotMap::default()),
            pools: RwLock::new(HashMap::new()),
            refreshing: tokio::sync::Mutex::new(()),
            reads: ReadRouting::new(read_mode),
            replica_pools: ReplicaPools::new(size, Arc::new(replica_settings)),
            warned: Default::default(),
        }
    }

    /// Sends a read to a replica of the master that serves `key`, or to the master as the read mode says. A read the replica cannot answer goes to the master.
    async fn send_read(&self, cmd: &redis::Cmd, key: Option<&[u8]>) -> Result<Value, Failure> {
        let replica = match key {
            Some(key) if self.reads.mode() != ReadMode::Master => {
                let map = self.map();
                map.range(keyslot(key))
                    .and_then(|range| self.reads.pick(&range.replicas).cloned())
            }
            _ => None,
        };
        let Some(replica) = replica else {
            return self.send(cmd, key).await;
        };
        match self.replica_pools.pool(&replica).next().send(cmd).await {
            Err(failure) if replica_failed(&failure) => {
                if !matches!(failure, Failure::Other(_)) {
                    let _ = self.refresh().await;
                }
                self.send(cmd, key).await
            }
            other => other,
        }
    }

    pub(crate) fn map(&self) -> SlotMap {
        self.map.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn pool(&self, address: &Address) -> Arc<Pool> {
        if let Some(pool) = self
            .pools
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(address)
        {
            return pool.clone();
        }
        let mut pools = self.pools.write().unwrap_or_else(|e| e.into_inner());
        pools
            .entry(address.clone())
            .or_insert_with(|| Arc::new(Pool::new(address, self.size, &self.settings)))
            .clone()
    }

    /// Reads `CLUSTER SLOTS` from a known master, or from a seed when none answers.
    pub(crate) async fn refresh(&self) -> Result<(), Failure> {
        let _turn = self.refreshing.lock().await;
        let mut candidates = self.map().masters();
        candidates.extend(self.seeds.iter().cloned());
        let tried: Vec<String> = candidates.iter().map(Address::to_string).collect();
        let mut last = Failure::Connection("no cluster node answered".into());
        let mut cmd = redis::cmd("CLUSTER");
        cmd.arg("SLOTS");
        for address in candidates {
            match self.pool(&address).next().send(&cmd).await {
                Ok(reply) => {
                    let Some(mut ranges) = parse_slots(&reply) else {
                        last = Failure::Other(Error::Redis("malformed CLUSTER SLOTS reply".into()));
                        continue;
                    };
                    for range in &mut ranges {
                        if range.master.host.is_empty() {
                            range.master.host = address.host.clone();
                        }
                        for replica in &mut range.replicas {
                            if replica.host.is_empty() {
                                replica.host = address.host.clone();
                            }
                        }
                    }
                    let map = SlotMap::new(ranges);
                    let masters = map.masters();
                    self.replica_pools.keep(&map.replicas());
                    log_slot_changes(&self.map(), &map);
                    warn_missing_replicas(
                        self.reads.mode(),
                        masters
                            .iter()
                            .map(|master| (master.clone(), !map.replicas_of(master).is_empty())),
                        &self.warned,
                    );
                    *self.map.write().unwrap_or_else(|e| e.into_inner()) = map;
                    self.pools
                        .write()
                        .unwrap_or_else(|e| e.into_inner())
                        .retain(|address, _| masters.contains(address));
                    return Ok(());
                }
                Err(failure) => last = failure,
            }
        }
        tracing::error!(
            "Can't update cluster state using nodes: {}. A new attempt will be made. {last:?}",
            tried.join(", ")
        );
        Err(last)
    }

    /// The pool to `address`, created when there is none yet.
    pub(crate) fn open_pool(&self, address: &Address) -> Arc<Pool> {
        self.pool(address)
    }

    async fn pool_for(&self, key: Option<&[u8]>) -> Result<Arc<Pool>, Failure> {
        let map = self.map();
        let address = match key {
            Some(key) => map.master(keyslot(key)).cloned(),
            None => map.masters().into_iter().next(),
        };
        match address {
            Some(address) => Ok(self.pool(&address)),
            None => {
                self.refresh().await?;
                let map = self.map();
                let address = match key {
                    Some(key) => map.master(keyslot(key)).cloned(),
                    None => map.masters().into_iter().next(),
                };
                address
                    .map(|address| self.pool(&address))
                    .ok_or_else(|| Failure::Connection("no master serves the slot".into()))
            }
        }
    }

    async fn send(&self, cmd: &redis::Cmd, key: Option<&[u8]>) -> Result<Value, Failure> {
        let mut pool = self.pool_for(key).await?;
        let mut asking = false;
        for _ in 0..=REDIRECTS {
            let node = pool.next();
            let result = if asking {
                let mut pipe = redis::pipe();
                pipe.cmd("ASKING").add_command(cmd.clone());
                node.send_pipeline(&pipe, 1, 1)
                    .await
                    .map(|mut replies| replies.pop().unwrap_or(Value::Nil))
            } else {
                node.send(cmd).await
            };
            match result {
                Err(Failure::Redirect { ask, address }) => {
                    if !ask {
                        let _ = self.refresh().await;
                    }
                    asking = ask;
                    pool = self.pool(&address);
                }
                Err(failure) if is_connection_failure(&failure) => {
                    let _ = self.refresh().await;
                    return Err(failure);
                }
                other => return other,
            }
        }
        Err(Failure::Other(Error::Redis(
            "too many cluster redirections".into(),
        )))
    }

    fn nodes(&self) -> Vec<Arc<Node>> {
        let mut nodes: Vec<Arc<Node>> = self
            .pools
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .flat_map(|pool| pool.nodes.clone())
            .collect();
        nodes.extend(self.replica_pools.nodes());
        nodes
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_sentinel_replicas, parse_slots, replica_failed, ReadRouting, SlotMap};
    use crate::client::read_mode::ReadMode;
    use crate::error::Error;
    use crate::net::node::Failure;
    use crate::net::stream::Address;
    use redis::Value;

    fn replicas() -> Vec<Address> {
        vec![
            Address::new("127.0.0.1", 7003),
            Address::new("127.0.0.1", 7004),
        ]
    }

    fn picks(mode: ReadMode, replicas: &[Address], reads: usize) -> Vec<Option<u16>> {
        let routing = ReadRouting::new(mode);
        (0..reads)
            .map(|_| routing.pick(replicas).map(|address| address.port))
            .collect()
    }

    #[test]
    fn master_mode_never_picks_a_replica() {
        assert_eq!(picks(ReadMode::Master, &replicas(), 3), [None, None, None]);
    }

    #[test]
    fn replica_mode_reads_from_each_replica_in_turn() {
        assert_eq!(
            picks(ReadMode::Replica, &replicas(), 4),
            [Some(7003), Some(7004), Some(7003), Some(7004)]
        );
    }

    #[test]
    fn master_and_replica_mode_counts_the_master_as_one_more_replica() {
        assert_eq!(
            picks(ReadMode::MasterAndReplica, &replicas(), 6),
            [Some(7003), Some(7004), None, Some(7003), Some(7004), None]
        );
    }

    #[test]
    fn without_replicas_every_mode_reads_from_the_master() {
        for mode in [ReadMode::Replica, ReadMode::MasterAndReplica] {
            assert_eq!(picks(mode, &[], 2), [None, None]);
        }
    }

    fn sentinel_replica(port: &str, flags: &str, link: &str, master_port: &str) -> Value {
        let pairs = [
            ("ip", "127.0.0.1"),
            ("port", port),
            ("flags", flags),
            ("master-link-status", link),
            ("master-host", "127.0.0.1"),
            ("master-port", master_port),
        ];
        Value::Array(
            pairs
                .iter()
                .flat_map(|(field, value)| {
                    [
                        Value::BulkString(field.as_bytes().to_vec()),
                        Value::BulkString(value.as_bytes().to_vec()),
                    ]
                })
                .collect(),
        )
    }

    #[test]
    fn sentinel_replicas_that_are_down_or_serve_another_master_get_no_reads() {
        let reply = Value::Array(vec![
            sentinel_replica("6380", "slave", "ok", "6379"),
            sentinel_replica("6381", "s_down,slave", "ok", "6379"),
            sentinel_replica("6382", "slave,disconnected", "ok", "6379"),
            sentinel_replica("6383", "slave", "err", "6379"),
            sentinel_replica("6384", "slave", "ok", "6390"),
            sentinel_replica("6385", "slave", "ok", "6379"),
        ]);
        let master = Address::new("127.0.0.1", 6379);
        let ports: Vec<u16> = parse_sentinel_replicas(&reply, &master)
            .iter()
            .map(|address| address.port)
            .collect();
        assert_eq!(ports, [6380, 6385]);
    }

    #[test]
    fn a_read_goes_to_the_master_when_the_replica_cannot_answer_it() {
        for failure in [
            Failure::Connection("broken pipe".into()),
            Failure::Unsent("connection refused".into()),
            Failure::Redirect {
                ask: false,
                address: Address::new("127.0.0.1", 7000),
            },
            Failure::Other(Error::Redis("LOADING Redis is loading".into())),
            Failure::Other(Error::Redis("MASTERDOWN Link with MASTER is down".into())),
        ] {
            assert!(replica_failed(&failure), "{failure:?}");
        }
    }

    #[test]
    fn an_error_about_the_data_is_the_answer_of_the_replica() {
        let failure = Failure::Other(Error::Redis("WRONGTYPE Operation against a key".into()));
        assert!(!replica_failed(&failure));
    }

    fn node(host: &str, port: i64) -> Value {
        Value::Array(vec![
            Value::BulkString(host.as_bytes().to_vec()),
            Value::Int(port),
            Value::BulkString(b"id".to_vec()),
        ])
    }

    fn reply() -> Value {
        Value::Array(vec![
            Value::Array(vec![
                Value::Int(0),
                Value::Int(5460),
                node("127.0.0.1", 7000),
                node("127.0.0.1", 7003),
            ]),
            Value::Array(vec![
                Value::Int(5461),
                Value::Int(16383),
                node("127.0.0.1", 7001),
            ]),
        ])
    }

    #[test]
    fn cluster_slots_become_ranges_with_their_master_and_replicas() {
        let ranges = parse_slots(&reply()).unwrap();
        assert_eq!(ranges.len(), 2);
        assert_eq!((ranges[0].start, ranges[0].end), (0, 5460));
        assert_eq!(ranges[0].master, Address::new("127.0.0.1", 7000));
        assert_eq!(ranges[0].replicas, vec![Address::new("127.0.0.1", 7003)]);
        assert!(ranges[1].replicas.is_empty());
    }

    #[test]
    fn a_slot_is_served_by_the_master_of_its_range() {
        let map = SlotMap::new(parse_slots(&reply()).unwrap());
        assert_eq!(map.master(0), Some(&Address::new("127.0.0.1", 7000)));
        assert_eq!(map.master(5460), Some(&Address::new("127.0.0.1", 7000)));
        assert_eq!(map.master(5461), Some(&Address::new("127.0.0.1", 7001)));
        assert_eq!(map.master(16383), Some(&Address::new("127.0.0.1", 7001)));
    }

    #[test]
    fn a_slot_without_a_master_has_none() {
        let partial = Value::Array(vec![Value::Array(vec![
            Value::Int(0),
            Value::Int(100),
            node("127.0.0.1", 7000),
        ])]);
        let map = SlotMap::new(parse_slots(&partial).unwrap());
        assert_eq!(map.master(101), None);
        assert!(!map.covers_every_slot());
    }

    #[test]
    fn an_empty_host_means_the_node_that_answered() {
        let reply = Value::Array(vec![Value::Array(vec![
            Value::Int(0),
            Value::Int(16383),
            node("", 7000),
        ])]);
        let ranges = parse_slots(&reply).unwrap();
        assert_eq!(ranges[0].master.port, 7000);
        assert!(ranges[0].master.host.is_empty());
    }
}
