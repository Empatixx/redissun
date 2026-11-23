//! Which node a command goes to: one server, the master that Sentinel names, or the master of the key's slot in Redis Cluster.

use crate::client::slot::keyslot;
use crate::error::Error;
use crate::net::node::{Failure, Node, NodeSettings};
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

    pub(crate) fn nodes(&self) -> &[Arc<Node>] {
        &self.nodes
    }

    pub(crate) fn address(&self) -> &Address {
        self.nodes[0].address()
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

    /// The address of every master: the one server, Sentinel's master, or every master of a cluster.
    pub(crate) async fn masters(&self) -> Result<Vec<Address>, Failure> {
        match self {
            Topology::Single(pool) => Ok(vec![pool.address().clone()]),
            Topology::Sentinel(sentinel) => Ok(vec![sentinel.pool().await?.address().clone()]),
            Topology::Cluster(cluster) => Ok(cluster.map().masters()),
        }
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

    pub(crate) fn is_cluster(&self) -> bool {
        matches!(self, Topology::Cluster(_))
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
        Failure::Connection(_) => true,
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
}

impl Sentinel {
    pub(crate) fn new(
        sentinels: Vec<Address>,
        service: String,
        size: usize,
        settings: Arc<NodeSettings>,
        sentinel_settings: Arc<NodeSettings>,
    ) -> Self {
        Self {
            sentinels,
            service,
            size,
            settings,
            sentinel_settings,
            master: tokio::sync::RwLock::new(None),
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
        let Ok(address) = self.resolve().await else {
            return;
        };
        let mut master = self.master.write().await;
        if master
            .as_ref()
            .is_some_and(|pool| *pool.address() == address)
        {
            return;
        }
        *master = Some(Arc::new(Pool::new(&address, self.size, &self.settings)));
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
        self.master
            .try_read()
            .ok()
            .and_then(|master| master.as_ref().map(|pool| pool.nodes.clone()))
            .unwrap_or_default()
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
}

impl Cluster {
    pub(crate) fn new(seeds: Vec<Address>, size: usize, settings: Arc<NodeSettings>) -> Self {
        Self {
            seeds,
            size,
            settings,
            map: RwLock::new(SlotMap::default()),
            pools: RwLock::new(HashMap::new()),
            refreshing: tokio::sync::Mutex::new(()),
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
                Err(Failure::Connection(message)) => {
                    let _ = self.refresh().await;
                    return Err(Failure::Connection(message));
                }
                other => return other,
            }
        }
        Err(Failure::Other(Error::Redis(
            "too many cluster redirections".into(),
        )))
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

#[cfg(test)]
mod tests {
    use super::{parse_slots, SlotMap};
    use crate::net::stream::Address;
    use redis::Value;

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
