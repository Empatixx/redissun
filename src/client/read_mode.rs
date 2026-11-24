/// Where read commands go, like Redisson's `readMode`.
///
/// Reads are the plain commands that only look at data: `get`, `len`, `contains`, ranges and the like on buckets, maps, sets, sorted sets, lists, bit sets, HyperLogLogs and geo sets, and `exists` and `ttl` on every object, as in Redisson. Writes, scans, scripts, batches, blocking calls, pub/sub, locks, semaphores and the other coordination objects always use the master, and so do the reads of a `LocalCachedMap`, which would otherwise cache a stale value.
///
/// Replicas are those Sentinel reports (`SENTINEL REPLICAS`, leaving out the ones it marks down) and those `CLUSTER SLOTS` lists in Redis Cluster. A single server has no replicas, as Redisson's single server mode has no `readMode`, so every read goes to it.
///
/// A replica copies the master with a delay, so a read from it can miss a write made just before.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReadMode {
    /// Every read goes to the master, like Redisson's `MASTER`. You always see your own writes. The default.
    #[default]
    Master,
    /// Reads go to the replicas in turn, like Redisson's `SLAVE`. While there is no replica, reads go to the master, and a read a replica cannot answer goes to the master instead.
    Replica,
    /// Reads go in turn to the master and each of its replicas, like Redisson's `MASTER_SLAVE`.
    MasterAndReplica,
}
