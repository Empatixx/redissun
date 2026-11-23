# redis-rs migration, phase 2: redis-rs underneath, fred removed

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** fred leaves `Cargo.toml`. A thin connection layer of redissun's own (`src/net/`) opens every connection itself and speaks Redis through redis-rs's `MultiplexedConnection`, so every feature Redisson has and redissun has today stays: client names after reconnects, a credentials resolver asked per node, `TlsVerification::CaOnly`, Sentinel failover, Redis Cluster with `MOVED`/`ASK`, pub/sub that resubscribes, blocking calls with `CLIENT UNBLOCK`, batches, scripts on every master, lock writes that `WAIT` for replicas.

**Architecture:** `net::stream` opens TCP (nodelay, keep-alive) and optional TLS (tokio-rustls with redissun's verifier). `net::node::Node` is one reconnecting multiplexed connection to one address: it connects, lets redis-rs send `HELLO`/`AUTH`/`SELECT` from a `RedisConnectionInfo` built with the resolver's answer for that address, sends `CLIENT SETNAME`, and reconnects after a failure, with the `reconnection_delay` strategy. `net::topology::Topology` is `Single`, `Sentinel` (resolves the master through the sentinels again after a failure) or `Cluster` (slot map from `CLUSTER SLOTS`, a `Node` pool per master, `MOVED` refreshes the map, `ASK` sends `ASKING` first). `Core::query_value` sends a `Request` through the topology with the per-command timeout and the retry rule. Pub/sub, blocking calls and batches take dedicated `Node`s.

**Tech Stack:** redis 1.8 (`aio`, `tokio-comp`, `script`, `streams`), tokio, tokio-rustls / rustls 0.23 with `ring` (`tls-rustls`) or `aws-lc-rs` (`tls-rustls-aws-lc`), rustls-native-certs, socket2.

**Spec:** `docs/superpowers/specs/2026-11-23-redis-rs-migration-design.md` (phase 2), with the maintainer's decisions of 2026-11-23: no feature Redisson has may be lost, and a setting Redisson does not have is removed (checked against `redisson-4.8.0.jar` with `javap`); the layer is redissun's own and thin.

## Global Constraints

- Every test in the suite passes after every task that switches a part over, unchanged; a test changes only when a task says why.
- The public API does not change, except that the fred-only settings of 0.20 that Redisson does not have are removed: `commands_per_flush`, `command_queue_limit`, `internal_command_timeout`, `unresponsive_timeout`, `reconnect_on_auth_error`, `max_redirections`, `cluster_cache_update_delay`. `cluster_health_check` becomes `check_slots_coverage`, Redisson's `checkSlotsCoverage`: building a cluster client fails while a slot has no master. This is a breaking change, released as 0.21.
- Commands keep their retry rule: a `Request` without `once` is sent again after a connection failure, up to `retry_attempts` times; with `once`, never.
- No `unwrap()`/`expect()`/`panic!` outside tests. `cargo clippy --all-targets --all-features --locked -- -D warnings` and `cargo fmt --check` pass after every task.
- Commits as the configured author, message ending with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Test names are sentences.

## Review Focus

1. **Client names survive a reconnect** on pooled, pub/sub, blocking and cluster node connections. Task 2 adds `the_client_name_is_set_again_after_a_reconnect`; Task 4 `every_cluster_node_connection_gets_the_client_name`.
2. **The resolver is asked with each node's address** in a cluster. Task 4 adds `the_resolver_is_asked_for_every_cluster_node`.
3. **`TlsVerification::CaOnly`** accepts a certificate from the trusted CA that names another host and rejects one from another CA. Task 8 adds `ca_only_verification_skips_the_host_name`.
4. **A command in flight when its connection drops** is sent again unless it is `once`, and a `once` command fails with an error. Covered by `tests/chaos.rs` and `tests/reconnect.rs`.
5. **Blocking calls are cancelled on the server** when the caller drops them (`CLIENT UNBLOCK`), so the connection is not left blocked. Covered by `tests/it/vec_deque_blocking.rs` (`aborted_waiting_pops_do_not_consume_later_elements`).

---

### Task 1: Streams

**Files:** Create `src/net/mod.rs`, `src/net/stream.rs`. Modify `Cargo.toml` (redis features `aio`, `tokio-comp`; `tokio-rustls`, `rustls`, `rustls-native-certs`, `socket2` behind the TLS features), `src/lib.rs`.

**Interfaces:** Produces `net::stream::connect(address: &Address, settings: &StreamSettings) -> Result<Stream>`, where `Address { host: String, port: u16 }`, `StreamSettings { connect_timeout, tcp_no_delay, keep_alive: Option<TcpKeepalive>, tls: Option<Arc<rustls::ClientConfig>> }`, and `Stream` is `AsyncRead + AsyncWrite + Unpin + Send` (plain TCP or TLS).

- [ ] Unit tests in `src/net/stream.rs`: `a_closed_port_fails_with_a_redis_error`, `the_connect_timeout_limits_connecting` (address `10.255.255.1:6379`, timeout 200 ms, fails within 1 s).
- [ ] Implement; verifier for `TlsVerification::{Strict, CaOnly, None}` lives in `src/client/tls.rs` and builds the `rustls::ClientConfig`.
- [ ] `cargo test --lib net::stream`. Commit `feat: net streams, TCP and TLS opened by redissun`.

### Task 2: Node, single server

**Files:** Create `src/net/node.rs`. Modify `src/client/core.rs` (`Core::query_value` and pooled connections on `Node`s for a single server; fred stays for the other topologies until their task), `src/client/config.rs`.

**Interfaces:** `Node::new(address, NodeSettings) -> Node`; `Node::send(&self, cmd: &redis::Cmd) -> Result<redis::Value>` (connects or reconnects first, with the reconnection delay); `Node::send_pipeline(&self, pipe: &redis::Pipeline, offset, count) -> Result<Vec<Value>>`; `Node::client_id(&self) -> Option<i64>`; `NodeSettings { stream, db, credentials (fixed or resolver), client_name, protocol, push: Option<push sender>, reconnection_delay }`.

- [ ] Integration test in `tests/settings.rs`: `the_client_name_is_set_again_after_a_reconnect` — `CLIENT KILL` every connection named N, then a command succeeds and `CLIENT LIST` shows connections named N again.
- [ ] Implement. `Core::query_value` keeps the retry rule (`retry_attempts`, never for `once`) and the per-command `timeout` (`tokio::time::timeout`, zero waits forever).
- [ ] `cargo test` for every single-server test binary (`it`, `client`, `settings`, `timeout`, `close`, `credentials`, `reconnect`, `lock`, …). Commit `feat: single-server connections on redissun's own node layer`.

### Task 3: Sentinel

**Files:** Create `src/net/sentinel.rs`. Modify `src/client/core.rs`, `src/client/config.rs` (URL `redis-sentinel://host:port?sentinelServiceName=…&node=…`).

- [ ] `tests/sentinel.rs` passes unchanged, including `reads_and_writes_continue_after_sentinel_failover`. Commit `feat: Sentinel on the node layer`.

### Task 4: Cluster

**Files:** Create `src/net/cluster.rs`. Modify `src/client/core.rs`, `src/client/config.rs` (URL `redis-cluster://host:port?node=…`).

**Interfaces:** `ClusterState::route(slot) -> Arc<NodePool>`, refresh from `CLUSTER SLOTS` on `MOVED` and on a connection failure; `ASK` sends `ASKING` then the command to the named node; a request without key goes to any master.

- [ ] Tests in `tests/cluster.rs`: `every_cluster_node_connection_gets_the_client_name`, and in `tests/credentials.rs` `the_resolver_is_asked_for_every_cluster_node` (every master's `host:port` is asked).
- [ ] `tests/cluster.rs`, `tests/batch.rs` cluster module, `tests/tls.rs` cluster test pass. Commit `feat: Redis Cluster on the node layer`.

### Task 5: Pub/sub

**Files:** Modify `src/pubsub.rs` (a dedicated RESP3 `Node` with a push sender; resubscribe every channel and pattern after a reconnect; keyspace notifications as pattern messages).

- [ ] `tests/reconnect.rs`, `tests/topic.rs`, `tests/local_cached_map.rs`, lock wake-ups, `tests/sentinel.rs` `topic_subscriber_receives_after_sentinel_failover` pass. Commit `feat: pub/sub on the node layer`.

### Task 6: Blocking calls

**Files:** Modify `src/client/core.rs` (`BlockingClient` on a dedicated `Node` to the key's master; `CLIENT UNBLOCK <id>` through a pooled connection when dropped), the blocking paths of `src/collections/{vec_deque,sorted_set}.rs` and `src/messaging/stream/mod.rs` (commands built as `redis::Cmd`).

- [ ] `tests/it/vec_deque_blocking.rs`, `tests/it/sorted_set_blocking.rs`, `tests/stream.rs`, `tests/settings.rs` blocking tests, `tests/sentinel.rs` `blocking_pop_survives_sentinel_failover` pass. Commit `feat: blocking calls on the node layer`.

### Task 7: Batches, scripts, lock replica sync

**Files:** Modify `src/batch/{engine,mod,views}.rs` (an exclusive `Node` per node group, `redis::Pipeline` with `MULTI`/`EXEC`, `MOVED`/`ASK` as now), `src/script.rs` (script and function commands on every master), `src/locks/sync.rs` (`EVALSHA` + `WAIT` pipeline on the slot's master).

- [ ] `tests/batch.rs`, `tests/it/script.rs`, `tests/cluster.rs` scripts test, lock tests, `tests/settings.rs` pass. Commit `feat: batches, scripts and lock sync on the node layer`.

### Task 8: TLS, credentials, settings; fred removed

**Files:** Modify `src/client/{tls,credentials,retry,config}.rs`, `src/error.rs`, delete `src/client/fred_adapter.rs`, `Cargo.toml` without fred, docs (`website/content/docs/guides/configuration.mdx`, `README.md`).

- [ ] Test in `tests/tls.rs`: `ca_only_verification_skips_the_host_name`.
- [ ] `grep -rn fred src Cargo.toml` finds nothing. Whole suite: `cargo test && cargo test --features tls-rustls --test tls && cargo test --features tls-rustls-aws-lc --test tls`. Benchmark against `main` within 10 %. Commit `feat: fred removed, redissun runs on redis-rs`.
