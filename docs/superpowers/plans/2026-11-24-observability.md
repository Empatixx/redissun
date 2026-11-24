# Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give redissun the observability that open-source Redisson 4.7.0 has: its log events, its `ConnectionListener`, and its `RedisNodes` API. Nothing that Redisson lacks (no metrics, no spans).

**Architecture:** Log events go through the `tracing` crate, which costs nothing without a subscriber. They sit where Redisson logs, at Redisson's level, with Redisson's wording adapted (`slave` → `replica`). Connection events are a `ConnectionListener` trait set on the config. A per-address status map fires `on_connect` and `on_disconnect` only when that status changes, like `ConnectionEventsHub`. `client.nodes()` gives handles to each node of the topology, with Redisson's `RedisNode` methods.

**Tech Stack:** Rust, tokio, redis-rs 1.8, `tracing` 0.1 (new dependency), `tracing-subscriber` (dev-dependency, to capture events in tests).

**Spec:** Redisson 4.7.0 sources (`~/.m2/repository/org/redisson/redisson/4.7.0/redisson-4.7.0-sources.jar`), classes `ConnectionEventsHub`, `ConnectionListener`, `NodeType`, `api/redisnode/*`, and the `log.*` calls in `connection/`, `cluster/`, `command/RedisExecutor`, `client/handler/*`, `pubsub/`, `renewal/RenewalTask`. The user ruled on 2026-11-24: "pokud nemá redisson tak nic" (what Redisson does not have, redissun does not get).

## Global Constraints

- No `unwrap`/`expect` outside tests.
- Every public item has docs; the docs site (`website/content/docs`) and `redisson-migration.mdx` are updated in the same PR.
- Never log keys, values, passwords or command arguments at `info`/`warn`/`error`/`debug`. Only `trace` may carry the command name (Redisson logs the whole message at trace; redissun logs the name only, as arguments may hold secrets — ruling).
- Docker tests run with `--test-threads 4`; one cargo run at a time.
- PR bodies carry a `## Changelog` section.

## Review Focus

- A disconnect while the node was never connected fires nothing (Redisson's `fireDisconnect` ignores an address with no status).
- A pool of several connections to one node fires one `on_connect`, not one per connection.
- The listener runs inside the connection path; a panicking or slow listener must not break commands (call it outside locks; catch nothing, document that it must be quick, as in Redisson).
- Shutdown (client dropped) fires no events, as Redisson skips them while shutting down.
- Logging must not happen on every command at `debug` or above, except retries (Redisson's `attempt {} for {} to {}` is debug).

---

### Task 1: `tracing` events at Redisson's log points (PR A)

**Files:**
- Modify: `Cargo.toml` (`tracing = "0.1"`, dev `tracing-subscriber`)
- Modify: `src/net/node.rs`, `src/net/topology.rs`, `src/client/core.rs`, `src/pubsub.rs`, `src/locks/watchdog.rs` (or wherever lock renewal fails)
- Test: unit tests with a capturing subscriber, `tests/observability.rs` for topology events

Events (target `redissun`, fields `address`, `replica`, `master`, `slots`, `attempt`):

| Where | Level | Message (Redisson's) |
|---|---|---|
| cluster refresh finds a new master | info | `master: {address} added for slot ranges: {slots}` |
| cluster refresh finds a new or gone replica | info | `replica: {address} added for master {master}` / `replica {address} removed for master {master}` |
| sentinel master changes | info | `master {old} has been changed to {new}` |
| sentinel scan: replica appears / goes | info / warn | `replica: {address} added` / `replica: {address} is down` |
| ReadMode not Master, but no replicas | warn | `ReadMode = {mode}, but replica nodes are not found!` (once per transition to empty) |
| a topology refresh where no node answers | error | `Can't update cluster state. A new attempt will be made.` |
| connection open fails with a timeout | warn | `Connection timeout to {address}` |
| reconnect after a failure | debug | `reconnecting to {address}` |
| PING gets no reply in time | debug | `connection to {address} closed due to PING response timeout set in {ms} ms` |
| AUTH error on connect | debug | `connection to {address} closed due to AUTH error response` |
| a command is sent again | debug | `attempt {attempt} for {command} to {address}` |
| credentials renewal fails | warn | `Renewal cycle failed, retrying` |
| lock lease renewal fails | error | `Can't update lock {name} expiration` |
| pub/sub restore fails | warn | `listeners of '{channel}' channel haven't been resubscribed` |
| pub/sub message cannot be decoded | error | `Unable to decode data. channel: {channel}` |
| every command written | trace | `command {name} to {address}` |

- [ ] Step 1: failing tests: a capturing subscriber sees `attempt 1 for GET` when a command is retried, `reconnecting to` after a broken connection, `Can't update cluster state` with an unreachable seed, `master: … added` on first cluster refresh (docker cluster test).
- [ ] Step 2: run them, expect FAIL (no events).
- [ ] Step 3: add the events.
- [ ] Step 4: run them, expect PASS; then the whole suite.
- [ ] Step 5: docs page `guides/logging.mdx` (how to install a subscriber, the event table), migration guide row `SLF4J logs → tracing`.
- [ ] Step 6: commit, PR, CI, merge.

### Task 2: `ConnectionListener` (PR B)

**Interfaces — Produces:**
- `pub enum NodeType { Master, Replica, Sentinel }` (`#[non_exhaustive]`)
- `pub trait ConnectionListener: Send + Sync + 'static { fn on_connect(&self, address: &str, node: NodeType) {} fn on_disconnect(&self, address: &str, node: NodeType) {} }`
- `ConfigBuilder::connection_listener(impl ConnectionListener)`

Behaviour copied from `ConnectionEventsHub`: one status per `host:port`; `on_connect` when it becomes connected (first time, or after a disconnect); `on_disconnect` when a connected address loses a connection; nothing after the client is dropped.

- [ ] Step 1: failing tests (docker, `tests/reconnect.rs` `Switch` proxy): a pool of 3 fires `on_connect` once; switching the proxy off fires `on_disconnect` once; on again fires `on_connect`; a replica read fires `on_connect(.., Replica)`; dropping the client fires nothing more.
- [ ] Step 2–4: RED, implement (hook in `Node::live`/`Node::broken` through `NodeSettings`, node type from the pool kind), GREEN, suite.
- [ ] Step 5: docs + migration row `setConnectionListener`.
- [ ] Step 6: commit, PR, CI, merge.

### Task 3: Nodes API (PR C)

**Interfaces — Produces:** `client.nodes() -> Nodes`; `Nodes::{ping_all(), ping_all_within(Duration), masters(), master(addr), replicas(), replica(addr), sentinels(), sentinel(addr)}`; `RedisNode::{address(), ping(), ping_within(Duration), info(InfoSection), time(), memory_statistics(), config(param), set_config(param, value), bg_save(), schedule_bg_save(), save(), last_save_time(), bg_rewrite_aof(), size()}`; cluster nodes add `cluster_info()`, `cluster_id()`, `cluster_slots()`, `cluster_count_keys_in_slot()`, `cluster_keys_in_slot()`, `cluster_count_failure_reports()` and the slot-admin commands Redisson has; `SentinelNode::{master_addr(name), sentinels(name), masters(), replicas(name), master(name), failover(name)}`.

Ruling: one `Nodes` type for all topologies instead of Redisson's `RedisNodes.CLUSTER`/`SENTINEL_MASTER_SLAVE`/`SINGLE` selector — Rust has no class tokens; a single server has one master and no replicas, so the same calls answer — cost if wrong: an API reshuffle before 1.0.

- [ ] Step 1: failing tests (single, sentinel and cluster docker topologies): `ping_all` is true; `masters()` lists every master; `info(Server)` has `redis_version`; `time()` is within a second of the local clock; sentinel `master_addr` names the master.
- [ ] Step 2–4: RED, implement in `src/nodes.rs`, GREEN, suite.
- [ ] Step 5: docs page section + migration rows for `getRedisNodes`.
- [ ] Step 6: commit, PR, CI, merge.

Out of scope (ruling): `FailedNodeDetector` — redissun already closes a connection whose PING fails and falls back from a failed replica read; a pluggable detector is a separate change if asked for. `NettyHook` has no Rust counterpart.
