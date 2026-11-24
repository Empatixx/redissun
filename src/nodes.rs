//! The masters, replicas and sentinels of a client, and server commands for each of them, like Redisson's `RedisNodes`.

use crate::client::core::Core;
use crate::error::{Error, Result};
use crate::net::node::{parse_address, Node};
use crate::net::stream::Address;
use crate::NodeType;
use redis::Value;
use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// How long [`ping`](RedisNode::ping) and [`ping_all`](Nodes::ping_all) wait, as in Redisson.
const PING_WAIT: Duration = Duration::from_secs(1);

/// A section of `INFO`, Redisson's `RedisNode.InfoSection`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum InfoSection {
    /// `INFO ALL`.
    All,
    /// `INFO DEFAULT`, what `INFO` gives without a section.
    Default,
    /// `INFO SERVER`.
    Server,
    /// `INFO CLIENTS`.
    Clients,
    /// `INFO MEMORY`.
    Memory,
    /// `INFO PERSISTENCE`.
    Persistence,
    /// `INFO STATS`.
    Stats,
    /// `INFO REPLICATION`.
    Replication,
    /// `INFO CPU`.
    Cpu,
    /// `INFO COMMANDSTATS`.
    CommandStats,
    /// `INFO CLUSTER`.
    Cluster,
    /// `INFO KEYSPACE`.
    Keyspace,
}

impl InfoSection {
    fn name(self) -> &'static str {
        match self {
            InfoSection::All => "ALL",
            InfoSection::Default => "DEFAULT",
            InfoSection::Server => "SERVER",
            InfoSection::Clients => "CLIENTS",
            InfoSection::Memory => "MEMORY",
            InfoSection::Persistence => "PERSISTENCE",
            InfoSection::Stats => "STATS",
            InfoSection::Replication => "REPLICATION",
            InfoSection::Cpu => "CPU",
            InfoSection::CommandStats => "COMMANDSTATS",
            InfoSection::Cluster => "CLUSTER",
            InfoSection::Keyspace => "KEYSPACE",
        }
    }
}

/// What `CLUSTER SETSLOT` sets, Redisson's `SetSlotCommand` with the node id it takes.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SetSlot {
    /// `MIGRATING`: the slot moves to the node with this id.
    Migrating(String),
    /// `IMPORTING`: the slot comes from the node with this id.
    Importing(String),
    /// `STABLE`: clears `MIGRATING` and `IMPORTING`.
    Stable,
    /// `NODE`: the slot belongs to the node with this id.
    Node(String),
}

/// The server commands of Redisson's `RedisNode`, for masters, replicas and sentinels alike; each type has `connection` and `run`. A sentinel answers only some of them (`INFO`, `PING`), as in Redisson.
macro_rules! server_commands {
    () => {
        /// Whether the node answers `PING` within a second, Redisson's `ping`.
        pub async fn ping(&self) -> bool {
            self.ping_within(PING_WAIT).await
        }

        /// Whether the node answers `PING` within `timeout`.
        pub async fn ping_within(&self, timeout: Duration) -> bool {
            let Some(node) = self.connection().await else {
                return false;
            };
            matches!(
                tokio::time::timeout(timeout, node.send(&redis::cmd("PING"))).await,
                Ok(Ok(_))
            )
        }

        /// `INFO section` as field and value, Redisson's `info`.
        pub async fn info(&self, section: InfoSection) -> Result<HashMap<String, String>> {
            let mut cmd = redis::cmd("INFO");
            cmd.arg(section.name());
            Ok(fields(&text(&self.run(cmd).await?)))
        }

        /// The server's clock, `TIME`.
        pub async fn time(&self) -> Result<SystemTime> {
            time(self.run(redis::cmd("TIME")).await?)
        }

        /// `MEMORY STATS`, with nested values written out, Redisson's `getMemoryStatistics`.
        pub async fn memory_statistics(&self) -> Result<HashMap<String, String>> {
            let mut cmd = redis::cmd("MEMORY");
            cmd.arg("STATS");
            Ok(pairs(&self.run(cmd).await?))
        }

        /// `CONFIG GET parameter`; a pattern like `maxmemory-*` gives every matching parameter.
        pub async fn config(&self, parameter: &str) -> Result<HashMap<String, String>> {
            let mut cmd = redis::cmd("CONFIG");
            cmd.arg("GET").arg(parameter);
            Ok(pairs(&self.run(cmd).await?))
        }

        /// `CONFIG SET parameter value`.
        pub async fn set_config(&self, parameter: &str, value: &str) -> Result<()> {
            let mut cmd = redis::cmd("CONFIG");
            cmd.arg("SET").arg(parameter).arg(value);
            self.run(cmd).await.map(drop)
        }

        /// `BGSAVE`.
        pub async fn bg_save(&self) -> Result<()> {
            self.run(redis::cmd("BGSAVE")).await.map(drop)
        }

        /// `BGSAVE SCHEDULE`: saves in the background once a running rewrite of the AOF is done.
        pub async fn schedule_bg_save(&self) -> Result<()> {
            let mut cmd = redis::cmd("BGSAVE");
            cmd.arg("SCHEDULE");
            self.run(cmd).await.map(drop)
        }

        /// `SAVE`, which blocks the server until the data is on disk.
        pub async fn save(&self) -> Result<()> {
            self.run(redis::cmd("SAVE")).await.map(drop)
        }

        /// When the data was last saved, `LASTSAVE`.
        pub async fn last_save_time(&self) -> Result<SystemTime> {
            let seconds: u64 = redis::from_redis_value(self.run(redis::cmd("LASTSAVE")).await?)?;
            Ok(UNIX_EPOCH + Duration::from_secs(seconds))
        }

        /// `BGREWRITEAOF`.
        pub async fn bg_rewrite_aof(&self) -> Result<()> {
            self.run(redis::cmd("BGREWRITEAOF")).await.map(drop)
        }

        /// The number of keys in the client's database, `DBSIZE`.
        pub async fn size(&self) -> Result<u64> {
            Ok(redis::from_redis_value(
                self.run(redis::cmd("DBSIZE")).await?,
            )?)
        }
    };
}

/// The nodes of a client, from [`Client::nodes`](crate::Client::nodes), like Redisson's `getRedisNodes`.
///
/// A single server is one master. With Sentinel, the master is the one the sentinels name now and the replicas are those they report. In a cluster, the masters and replicas are those of the slot map. Every list is what the client knows when it is asked.
///
/// ```no_run
/// # async fn run(client: redissun::Client) -> redissun::Result<()> {
/// use redissun::InfoSection;
///
/// let nodes = client.nodes();
/// assert!(nodes.ping_all().await);
/// for master in nodes.masters().await {
///     let server = master.info(InfoSection::Server).await?;
///     println!("{} runs Redis {:?}", master.address(), server.get("redis_version"));
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Nodes {
    core: Arc<Core>,
}

impl Nodes {
    pub(crate) fn new(core: Arc<Core>) -> Self {
        Self { core }
    }

    /// Whether every master and replica answers `PING` within a second, Redisson's `pingAll`.
    pub async fn ping_all(&self) -> bool {
        self.ping_all_within(PING_WAIT).await
    }

    /// Whether every master and replica answers `PING` within `timeout`.
    pub async fn ping_all_within(&self, timeout: Duration) -> bool {
        let mut nodes = self.masters().await;
        nodes.extend(self.replicas().await);
        let answers =
            futures::future::join_all(nodes.iter().map(|node| node.ping_within(timeout))).await;
        answers.into_iter().all(|answered| answered)
    }

    /// The masters, Redisson's `getMasters` (`getInstance` for a single server).
    pub async fn masters(&self) -> Vec<RedisNode> {
        self.of(NodeType::Master).await
    }

    /// The master at `address` (`host:port`), `None` when the client knows no such master.
    pub async fn master(&self, address: &str) -> Option<RedisNode> {
        self.find(NodeType::Master, address).await
    }

    /// The replicas, Redisson's `getSlaves`. They are known whatever the [`ReadMode`](crate::ReadMode), as Redisson knows them.
    pub async fn replicas(&self) -> Vec<RedisNode> {
        self.of(NodeType::Replica).await
    }

    /// The replica at `address` (`host:port`), `None` when the client knows no such replica.
    pub async fn replica(&self, address: &str) -> Option<RedisNode> {
        self.find(NodeType::Replica, address).await
    }

    /// The sentinels of the URL, empty without Sentinel, Redisson's `getSentinels`.
    pub fn sentinels(&self) -> Vec<SentinelNode> {
        self.core
            .topology()
            .sentinel_nodes()
            .into_iter()
            .map(|node| SentinelNode {
                core: self.core.clone(),
                address: node.address().to_string(),
                node,
            })
            .collect()
    }

    /// The sentinel at `address` (`host:port`), `None` when the URL names no such sentinel.
    pub fn sentinel(&self, address: &str) -> Option<SentinelNode> {
        self.sentinels()
            .into_iter()
            .find(|sentinel| sentinel.address == address)
    }

    async fn of(&self, kind: NodeType) -> Vec<RedisNode> {
        self.core
            .topology()
            .addresses(kind)
            .await
            .into_iter()
            .map(|address| RedisNode {
                core: self.core.clone(),
                text: address.to_string(),
                address,
                kind,
            })
            .collect()
    }

    async fn find(&self, kind: NodeType, address: &str) -> Option<RedisNode> {
        self.of(kind)
            .await
            .into_iter()
            .find(|node| node.text == address)
    }
}

/// A master or replica, Redisson's `RedisNode`, `RedisClusterNode` and their master and replica kinds.
///
/// Each call goes to the node over the client's connections to it, limited by the client's `timeout`. A node that is no longer part of the topology fails with [`Error::Redis`]. The `cluster_…` commands are for Redis Cluster; any other server answers them with an error.
#[derive(Clone)]
pub struct RedisNode {
    core: Arc<Core>,
    address: Address,
    text: String,
    kind: NodeType,
}

impl std::fmt::Debug for RedisNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisNode")
            .field("address", &self.text)
            .field("node_type", &self.kind)
            .finish()
    }
}

impl RedisNode {
    /// `host:port`, Redisson's `getAddr`.
    pub fn address(&self) -> &str {
        &self.text
    }

    /// [`NodeType::Master`] or [`NodeType::Replica`].
    pub fn node_type(&self) -> NodeType {
        self.kind
    }

    server_commands!();

    /// `CLUSTER INFO` as field and value.
    pub async fn cluster_info(&self) -> Result<HashMap<String, String>> {
        Ok(fields(&text(&self.run(cluster(&["INFO"])).await?)))
    }

    /// The node's id, `CLUSTER MYID`.
    pub async fn cluster_id(&self) -> Result<String> {
        Ok(text(&self.run(cluster(&["MYID"])).await?))
    }

    /// `CLUSTER ADDSLOTS`.
    pub async fn cluster_add_slots(&self, slots: &[u16]) -> Result<()> {
        let mut cmd = cluster(&["ADDSLOTS"]);
        cmd.arg(slots);
        self.run(cmd).await.map(drop)
    }

    /// `CLUSTER DELSLOTS`.
    pub async fn cluster_delete_slots(&self, slots: &[u16]) -> Result<()> {
        let mut cmd = cluster(&["DELSLOTS"]);
        cmd.arg(slots);
        self.run(cmd).await.map(drop)
    }

    /// `CLUSTER REPLICATE node_id`: the node becomes a replica of that node.
    pub async fn cluster_replicate(&self, node_id: &str) -> Result<()> {
        self.run(cluster(&["REPLICATE", node_id])).await.map(drop)
    }

    /// `CLUSTER FORGET node_id`.
    pub async fn cluster_forget(&self, node_id: &str) -> Result<()> {
        self.run(cluster(&["FORGET", node_id])).await.map(drop)
    }

    /// The number of keys in `slot`, `CLUSTER COUNTKEYSINSLOT`.
    pub async fn cluster_count_keys_in_slot(&self, slot: u16) -> Result<u64> {
        let mut cmd = cluster(&["COUNTKEYSINSLOT"]);
        cmd.arg(slot);
        Ok(redis::from_redis_value(self.run(cmd).await?)?)
    }

    /// Up to `count` keys of `slot`, `CLUSTER GETKEYSINSLOT`.
    pub async fn cluster_keys_in_slot(&self, slot: u16, count: usize) -> Result<Vec<String>> {
        let mut cmd = cluster(&["GETKEYSINSLOT"]);
        cmd.arg(slot).arg(count);
        Ok(redis::from_redis_value(self.run(cmd).await?)?)
    }

    /// `CLUSTER SETSLOT slot …`.
    pub async fn cluster_set_slot(&self, slot: u16, set: SetSlot) -> Result<()> {
        let mut cmd = cluster(&["SETSLOT"]);
        cmd.arg(slot);
        match set {
            SetSlot::Migrating(id) => cmd.arg("MIGRATING").arg(id),
            SetSlot::Importing(id) => cmd.arg("IMPORTING").arg(id),
            SetSlot::Stable => cmd.arg("STABLE"),
            SetSlot::Node(id) => cmd.arg("NODE").arg(id),
        };
        self.run(cmd).await.map(drop)
    }

    /// `CLUSTER MEET` with the node at `address` (`host:port`).
    pub async fn cluster_meet(&self, address: &str) -> Result<()> {
        let target = parse_address(address)
            .ok_or_else(|| Error::Config(format!("{address} is not host:port")))?;
        let mut cmd = cluster(&["MEET"]);
        cmd.arg(&target.host).arg(target.port);
        self.run(cmd).await.map(drop)
    }

    /// How many nodes report `node_id` as failing, `CLUSTER COUNT-FAILURE-REPORTS`.
    pub async fn cluster_count_failure_reports(&self, node_id: &str) -> Result<u64> {
        Ok(redis::from_redis_value(
            self.run(cluster(&["COUNT-FAILURE-REPORTS", node_id]))
                .await?,
        )?)
    }

    /// `CLUSTER FLUSHSLOTS`: the node gives up all its slots.
    pub async fn cluster_flush_slots(&self) -> Result<()> {
        self.run(cluster(&["FLUSHSLOTS"])).await.map(drop)
    }

    /// `CLUSTER SLOTS`: each range of slots and the `host:port` of its master and then its replicas.
    pub async fn cluster_slots(&self) -> Result<HashMap<RangeInclusive<u16>, Vec<String>>> {
        let reply = self.run(cluster(&["SLOTS"])).await?;
        let ranges = crate::net::topology::parse_slots(&reply)
            .ok_or_else(|| Error::Redis("malformed CLUSTER SLOTS reply".into()))?;
        let named = |address: &Address| {
            if address.host.is_empty() {
                Address::new(self.address.host.clone(), address.port).to_string()
            } else {
                address.to_string()
            }
        };
        Ok(ranges
            .into_iter()
            .map(|range| {
                let mut nodes = vec![named(&range.master)];
                nodes.extend(range.replicas.iter().map(named));
                (range.start..=range.end, nodes)
            })
            .collect())
    }

    async fn connection(&self) -> Option<Arc<Node>> {
        self.core.topology().node_at(&self.address, self.kind).await
    }

    async fn run(&self, cmd: redis::Cmd) -> Result<Value> {
        let node = self.connection().await.ok_or_else(|| {
            Error::Redis(format!("{} is no longer a node of the client", self.text))
        })?;
        send(&self.core, &node, &cmd).await
    }
}

/// A sentinel, Redisson's `RedisSentinel`. It has the server commands of [`RedisNode`] too, though a sentinel answers only some of them (`PING`, `INFO`), as in Redisson.
#[derive(Clone)]
pub struct SentinelNode {
    core: Arc<Core>,
    node: Arc<Node>,
    address: String,
}

impl std::fmt::Debug for SentinelNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SentinelNode")
            .field("address", &self.address)
            .finish()
    }
}

impl SentinelNode {
    /// `host:port`.
    pub fn address(&self) -> &str {
        &self.address
    }

    server_commands!();

    /// The `host:port` of the master the sentinel names for `master_name`, `None` when it knows no such master; Redisson's `getMasterAddr`.
    pub async fn master_addr(&self, master_name: &str) -> Result<Option<String>> {
        let reply = self
            .run(sentinel(&["GET-MASTER-ADDR-BY-NAME", master_name]))
            .await?;
        let parts: Option<Vec<String>> = redis::from_redis_value(reply)?;
        Ok(match parts.as_deref() {
            Some([host, port]) => Some(format!("{host}:{port}")),
            _ => None,
        })
    }

    /// The other sentinels that watch `master_name`, `SENTINEL SENTINELS`.
    pub async fn sentinels(&self, master_name: &str) -> Result<Vec<HashMap<String, String>>> {
        Ok(records(
            &self.run(sentinel(&["SENTINELS", master_name])).await?,
        ))
    }

    /// Every master the sentinel watches, `SENTINEL MASTERS`.
    pub async fn masters(&self) -> Result<Vec<HashMap<String, String>>> {
        Ok(records(&self.run(sentinel(&["MASTERS"])).await?))
    }

    /// The replicas of `master_name`, `SENTINEL REPLICAS`, Redisson's `getSlaves`.
    pub async fn replicas(&self, master_name: &str) -> Result<Vec<HashMap<String, String>>> {
        Ok(records(
            &self.run(sentinel(&["REPLICAS", master_name])).await?,
        ))
    }

    /// The state of `master_name`, `SENTINEL MASTER`.
    pub async fn master(&self, master_name: &str) -> Result<HashMap<String, String>> {
        Ok(pairs(&self.run(sentinel(&["MASTER", master_name])).await?))
    }

    /// Starts a failover of `master_name` without asking the other sentinels, `SENTINEL FAILOVER`.
    pub async fn failover(&self, master_name: &str) -> Result<()> {
        self.run(sentinel(&["FAILOVER", master_name]))
            .await
            .map(drop)
    }

    async fn connection(&self) -> Option<Arc<Node>> {
        Some(self.node.clone())
    }

    async fn run(&self, cmd: redis::Cmd) -> Result<Value> {
        send(&self.core, &self.node, &cmd).await
    }
}

fn cluster(args: &[&str]) -> redis::Cmd {
    let mut cmd = redis::cmd("CLUSTER");
    cmd.arg(args);
    cmd
}

fn sentinel(args: &[&str]) -> redis::Cmd {
    let mut cmd = redis::cmd("SENTINEL");
    cmd.arg(args);
    cmd
}

/// Sends `cmd` to `node` within the client's `timeout`, zero meaning no limit.
async fn send(core: &Core, node: &Node, cmd: &redis::Cmd) -> Result<Value> {
    let sent = node.send(cmd);
    let timeout = core.retry.timeout;
    if timeout.is_zero() {
        return sent.await.map_err(Error::from);
    }
    match tokio::time::timeout(timeout, sent).await {
        Ok(result) => result.map_err(Error::from),
        Err(_) => Err(Error::Timeout),
    }
}

/// A reply as text: strings as they are, numbers written out, nested lists joined with spaces.
fn text(value: &Value) -> String {
    match value {
        Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
        Value::SimpleString(text) => text.clone(),
        Value::VerbatimString { text, .. } => text.clone(),
        Value::Int(number) => number.to_string(),
        Value::Double(number) => number.to_string(),
        Value::Boolean(flag) => flag.to_string(),
        Value::Array(items) | Value::Set(items) => {
            items.iter().map(text).collect::<Vec<_>>().join(" ")
        }
        Value::Map(entries) => entries
            .iter()
            .map(|(key, value)| format!("{} {}", text(key), text(value)))
            .collect::<Vec<_>>()
            .join(" "),
        _ => String::new(),
    }
}

/// `field:value` lines, as `INFO` and `CLUSTER INFO` write them; `# Section` lines are left out.
fn fields(reply: &str) -> HashMap<String, String> {
    reply
        .lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| line.split_once(':'))
        .map(|(field, value)| (field.to_string(), value.trim().to_string()))
        .collect()
}

/// A flat list of field and value (RESP2) or a map (RESP3).
fn pairs(value: &Value) -> HashMap<String, String> {
    match value {
        Value::Array(items) => items
            .chunks(2)
            .filter_map(|pair| match pair {
                [field, value] => Some((text(field), text(value))),
                _ => None,
            })
            .collect(),
        Value::Map(entries) => entries
            .iter()
            .map(|(field, value)| (text(field), text(value)))
            .collect(),
        _ => HashMap::new(),
    }
}

/// A list of [`pairs`].
fn records(value: &Value) -> Vec<HashMap<String, String>> {
    match value {
        Value::Array(items) => items.iter().map(pairs).collect(),
        _ => Vec::new(),
    }
}

/// The reply of `TIME`: seconds and microseconds since the epoch.
fn time(value: Value) -> Result<SystemTime> {
    let (seconds, micros): (u64, u64) = redis::from_redis_value(value)?;
    Ok(UNIX_EPOCH + Duration::from_secs(seconds) + Duration::from_micros(micros))
}

#[cfg(test)]
mod tests {
    use super::{fields, pairs, time};
    use redis::Value;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn info_lines_become_fields_without_the_section_headers() {
        let parsed = fields("# Server\r\nredis_version:7.4.0\r\nos:Linux 6.1\r\n\r\n# Clients\r\nconnected_clients:3\r\n");
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed["redis_version"], "7.4.0");
        assert_eq!(parsed["os"], "Linux 6.1");
    }

    #[test]
    fn a_flat_reply_becomes_field_and_value() {
        let reply = Value::Array(vec![
            Value::BulkString(b"peak.allocated".to_vec()),
            Value::Int(1024),
            Value::BulkString(b"db.0".to_vec()),
            Value::Array(vec![
                Value::BulkString(b"overhead.hashtable.main".to_vec()),
                Value::Int(72),
            ]),
        ]);
        let parsed = pairs(&reply);
        assert_eq!(parsed["peak.allocated"], "1024");
        assert_eq!(parsed["db.0"], "overhead.hashtable.main 72");
    }

    #[test]
    fn time_is_seconds_and_microseconds() {
        let reply = Value::Array(vec![
            Value::BulkString(b"1700000000".to_vec()),
            Value::BulkString(b"250000".to_vec()),
        ]);
        assert_eq!(
            time(reply).unwrap(),
            UNIX_EPOCH + Duration::from_secs(1_700_000_000) + Duration::from_millis(250)
        );
    }
}
