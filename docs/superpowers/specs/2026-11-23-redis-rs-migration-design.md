# redissun on redis-rs: design

## Why

redissun talks to Redis through fred 10.1. fred has not had a release since 2025-02-27, and its `replicas` feature has a bug that blocks reading from replicas: when a command finds no connection, `create_replica_connection` adds one to `attempts_remaining` before it retries, so the command is retried forever and the router spins at 100 % CPU (`fred-10.1.0/src/router/commands.rs`, the `else` branch "connection does not exist"). It hits every client once the feature is compiled in, in every read mode, as soon as a command meets a closed connection (the batch tests reproduce it with a `QUIT` on a dropped lease).

redis-rs (crate `redis`) released 1.8.3 on 2026-11-23, has 2.0 in release candidates and about ten times fred's downloads. Version 1.8 has what redissun needs: `aio::ConnectionManager` with reconnection and automatic resubscription, an async cluster connection with `route_command`, pub/sub and resubscription, async Sentinel, `StreamingCredentialsProvider`, rustls, and pluggable read routing in a cluster (`read_from_replicas`, `ReadRoutingStrategy`).

## Goal

Replace fred with redis-rs inside redissun. The public API and its behaviour stay as they are, and every existing test passes unchanged. Reading from replicas (`ReadMode`, the work parked on the local branch `feat/read-mode`) is then built on redis-rs.

## Non-goals

- No change to the public API apart from the settings in "Settings without an equivalent".
- No second backend. fred and redis-rs coexist only inside phase 1, behind one private seam, and fred is gone at the end of phase 2.

## Phases, one pull request each

1. **One seam for commands.** Every command redissun sends is built as a `redis::Cmd` and every reply is read as a `redis::Value`, through `Core::query`. The seam still runs on fred: it turns the `Cmd` into a fred custom command routed to the slot redis-rs itself would pick (`RoutingInfo::for_routable`), and fred's reply into a `redis::Value`. Nothing outside `src/client/`, `src/pubsub.rs`, `src/batch/`, `src/script.rs` and `src/locks/sync.rs` names fred afterwards. The whole test suite proves the switch, because the code on the wire is unchanged.
2. **redis-rs underneath.** The seam runs on redis-rs, and the parts that phase 1 left on fred move too: connections (single server: a pool of `ConnectionManager`s; cluster: `ClusterConnection`; Sentinel: a manager that asks the sentinels for the master again after a failure), settings, TLS, credentials, per-command timeouts and the "sent once" retry rule, the `PING` check, client names, pub/sub with keyspace events and resubscription, blocking calls with `CLIENT UNBLOCK`, scripts and functions including the cluster-wide ones, the batch engine (a pipeline per node, `MULTI`/`EXEC`, `MOVED`/`ASK`), and the lock writes that `WAIT` for replicas. fred leaves `Cargo.toml`.
3. **Reading from replicas** on redis-rs: `ReadMode::{Master, Replica, MasterAndReplica}` and `replica_scan_interval`, with the tests from `feat/read-mode` (`tests/read_mode.rs` and the topology waits).
4. **Release 0.21.0**: the settings table below applied, documentation, the benchmark run again on redis-rs, `CHANGELOG` through the pull request notes.

## Settings without an equivalent (decision for the maintainer)

Version 0.20 added settings that pass straight to fred. Proposed mapping:

| Setting | redis-rs | Proposal |
|---|---|---|
| `pool_size`, `timeout`, `connect_timeout`, `retry_attempts`, `retry_delay`, `reconnection_delay`, `ping_connection_interval`, keep-alive, `tcp_no_delay`, `client_name`, `database`, credentials, TLS | equivalents exist | keep |
| `command_queue_limit` | `concurrency_limit` (waits instead of failing) | keep the name, document that a full queue now waits |
| `commands_per_flush` | `write_backpressure_boundary` (bytes, not commands) | deprecate, no effect |
| `internal_command_timeout` | covered by `connect_timeout` | deprecate, no effect |
| `unresponsive_timeout` | none; `timeout` fails a command, `ping_connection_interval` reconnects | deprecate, no effect |
| `reconnect_on_auth_error`, `max_redirections`, `cluster_cache_update_delay`, `cluster_health_check` | redis-rs handles redirections and slot refresh itself | deprecate, no effect |

Deprecated methods stay in 0.21 with `#[deprecated]` and a note, and go in the next breaking release.

## Risks

- **Retry semantics.** fred repeats a command whose connection failed; redis-rs's `ConnectionManager` reconnects but fails the command in flight. Phase 2 adds the retry around `Core::query`, keeping Redisson's rule that a lock, a pop, `SETNX` and the other non-idempotent commands are sent once. The retry and lock tests cover it.
- **Reply conversions.** `FromRedisValue` and fred's `FromValue` disagree on some types (booleans from integers, `Option` of an empty array, doubles from strings). Phase 1 meets these while still on fred, so a failing test points at a conversion, not at the transport.
- **Sentinel failover.** redis-rs gives a connection to the current master but no manager that follows a failover; phase 2 writes that manager, and `tests/sentinel.rs` and `tests/chaos.rs` cover it.
- **Batch on a cluster.** The batch engine groups commands by node and follows `MOVED`/`ASK` itself; `tests/batch.rs` (cluster module) covers it.
- **Speed.** The benchmark runs again at the end of phases 1 and 2; a regression over 10 % against today's numbers blocks the release.
