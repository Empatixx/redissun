# redis-rs migration, phase 1: one seam for commands

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every command redissun sends outside the connection layer, pub/sub, the batch engine, `script.rs` and the lock replica sync is built as a `redis::Cmd` and read as a `redis::Value`, through one private seam that still runs on fred.

**Architecture:** `Core::query` takes a `Request` (a `redis::Cmd`, the key it is routed by, and whether it may be sent again) and runs it as a fred custom command routed to the key's slot; a small adapter turns fred's reply into a `redis::Value` and `FromRedisValue` does the typing. Lua scripts become a crate type `Lua` run through the same seam. Phase 2 replaces the seam's body with redis-rs and touches none of the callers.

**Tech Stack:** Rust 2021, tokio, fred 10.1 (still the transport), redis 1.8 (types only: `Cmd`, `Value`, `FromRedisValue`, `ToRedisArgs`), `sha1_smol`.

**Spec:** `docs/superpowers/specs/2026-11-23-redis-rs-migration-design.md`

## Global Constraints

- The public API does not change; no public type or signature names redis-rs.
- No `unwrap()`/`expect()`/`panic!` outside tests (clippy denies them in `src/lib.rs`).
- `cargo clippy --all-targets --all-features --locked -- -D warnings` and `cargo fmt --check` pass after every task.
- Commits as the configured git author, message ending with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Test names are sentences about behaviour (no `test_` prefix).
- Wire behaviour stays the same: the same commands, the same keys, the same "sent once" rule. The existing test suite is the proof and is not edited, except where a task says so.
- Errors from Redis or from reading a reply stay `Error::Redis(message)`.

## Review Focus

1. **Reply typing.** `FromRedisValue` must read what fred's `FromValue` read: `bool` from `0`/`1`, `Option<T>` from nil, `Vec<T>` from an empty array, `f64` from a bulk string, `()` from `OK`. Task 1 pins the adapter mapping with unit tests; the object tests catch call sites.
2. **Binary values.** A value that is not UTF-8 must pass both ways unchanged. Task 1 adds `binary_arguments_and_replies_pass_unchanged`.
3. **Routing by a key that is not the first argument.** `BITOP` (destination second), `SINTERCARD` (after a count), `SMOVE`/`LMOVE` (two keys) and `EVALSHA` (keys after the count) must reach the slot of the key the call site names. `Request::key` makes it explicit; the cluster tests (`tests/cluster.rs`, `tests/batch.rs` cluster module) cover it.
4. **Scripts after `SCRIPT FLUSH` or a failover.** `EVALSHA` answered `NOSCRIPT` loads the script and runs it once more, for `eval` and `eval_once` alike. Task 3 adds `a_flushed_script_is_loaded_again`.
5. **Commands sent once.** A request marked `once` is never sent again after a connection failure. Task 1 keeps `once` mapped to fred's `max_attempts: 1`; `tests/timeout.rs` and the lock cancellation tests cover it.

---

### Task 1: The seam

**Files:**
- Modify: `Cargo.toml` (add `redis = { version = "1.8", default-features = false, features = ["bytes"] }` and `sha1_smol = "1"` to `[dependencies]`; the dev-dependency `redis` keeps its features)
- Create: `src/client/slot.rs`, `src/client/request.rs`, `src/client/fred_adapter.rs`
- Modify: `src/client/mod.rs`, `src/client/core.rs`, `src/error.rs`, `src/object.rs`

**Interfaces:**
- Produces: `crate::client::slot::keyslot(key: &[u8]) -> u16`: CRC16/XMODEM modulo 16384 of the key, or of the part between the first `{` and the next `}` when that part is not empty.
- Produces: `crate::client::request::Request` with `Request::new(cmd: redis::Cmd) -> Request`, `Request::key(self, key: &[u8]) -> Request` (route to that key's slot; without it the command goes to any node), `Request::once(self) -> Request` (never sent again after a connection failure).
- Produces: `Core::query<T: redis::FromRedisValue>(&self, request: Request) -> Result<T>` and `Core::query_value(&self, request: Request) -> Result<redis::Value>`.
- Produces: `Key::query<T: redis::FromRedisValue>(&self, cmd: redis::Cmd) -> Result<T>` and `Key::query_once<T>(&self, cmd: redis::Cmd) -> Result<T>`, both routed by `self.redis_key()`.
- Produces: `impl From<redis::RedisError> for Error`, giving `Error::Redis(error.to_string())`.
- Produces: `fred_adapter::from_fred(value: fred::types::Value) -> redis::Value` and `fred_adapter::to_fred(cmd: &redis::Cmd) -> Result<(String, Vec<fred::types::Value>)>`, `pub(crate)`, deleted in phase 2.

- [ ] **Step 1: Write the failing unit tests** in `src/client/slot.rs`, `src/client/fred_adapter.rs` and `src/client/request.rs`:
  - `keyslot_matches_fred_for_plain_and_tagged_keys`: for `"foo"`, `"bar"`, `"{user1000}.following"`, `"{}x"`, `"x{}"`, `"{a}{b}"`, `"a{b"`, 200 random byte strings: `keyslot(k) == fred::util::redis_keyslot(k)`; and `keyslot(b"foo") == 12182`.
  - `fred_replies_become_redis_values`: `Null → Nil`, `Integer(5) → Int(5)`, `Double(1.5) → Double(1.5)`, `Boolean(true) → Boolean(true)`, `String("OK") → BulkString(b"OK")` and `String("x") → BulkString(b"x")` (fred gives `String` for any UTF-8 bulk string too, so a stored value `"OK"` must not turn into `Okay`), `Bytes(b"\xff") → BulkString(vec![0xff])`, `Array([Integer(1), Null]) → Array([Int(1), Nil])`, `Map{a: 1} → Map([(BulkString(b"a"), Int(1))])`, `Queued → SimpleString("QUEUED")`.
  - `binary_arguments_and_replies_pass_unchanged`: `to_fred(redis::cmd("SET").arg("k").arg(&b"\xff\x00"[..]))` gives name `"SET"` and args `[Bytes(b"k"), Bytes(b"\xff\x00")]`.
  - `typed_reads_match_what_fred_read`: through `from_fred` then `FromRedisValue`: `bool` from `Integer(1)` is `true`, `Option<String>` from `Null` is `None`, `Vec<String>` from `Array([])` is empty, `f64` from `Bytes(b"2.5")` is `2.5`, `()` from `String("OK")` is `()`.

- [ ] **Step 2: Run them to see them fail**

  Run: `cargo test --lib client::`
  Expected: FAIL, the modules do not compile yet.

- [ ] **Step 3: Implement `keyslot`, `Request`, `to_fred`, `from_fred`, `Core::query`, `Key::query` and `From<redis::RedisError> for Error`.** `Core::query` sends `to_fred`'s name and arguments as `CustomCommand::new(name, ClusterHash::Custom(keyslot(key)) or Random, false)` on `self.redis()`, with fred options `max_attempts: Some(1)` when `once`, and reads the reply as `T::from_redis_value(from_fred(reply))`.

- [ ] **Step 4: Run the unit tests and the whole suite**

  Run: `cargo test --lib client:: && cargo test --test client --test settings`
  Expected: PASS.

- [ ] **Step 5: Commit** `refactor: one seam for commands, a redis::Cmd sent through fred`

### Task 2: Replies as `redis::Value`

**Files:**
- Modify: `src/reply.rs`, `src/object.rs` (`Key::command` and `Key::read` are removed; callers build a `redis::Cmd` and call `Key::query`), every caller of `crate::reply::*` and of `Key::command` (`grep -rn "reply::\|key.command(\|\.command(\"" src`)
- Modify: `src/batch/engine.rs`, `src/batch/views.rs`, `src/script.rs`: where they read a fred reply with a `reply::` helper, convert it with `fred_adapter::from_fred` first. Their own sending stays on fred until phase 2.

**Interfaces:**
- Consumes: `Key::query`, `fred_adapter::from_fred` (Task 1).
- Produces: `reply::{text, bytes, number, array, malformed}` taking `&redis::Value`; `reply::int(n) -> redis::Value` is removed, callers pass integers to `Cmd::arg`.

- [ ] **Step 1: Switch `src/reply.rs` to `redis::Value`.** `text` reads `BulkString` (UTF-8), `SimpleString`, `Okay` as `"OK"`, `Int`; `bytes` reads `BulkString`, `SimpleString`, `Int`; `number` reads `Int` or parses `text`; `array` reads `Array` and `Set`.
- [ ] **Step 2: Compile and follow the errors to every caller.** `Key::command("X", vec![...], offset)` becomes `self.key.query::<redis::Value>(redis::cmd("X").arg(...))`; a call routed by another key than the object's uses `Core::query(Request::new(cmd).key(other))`.
- [ ] **Step 3: Run the object tests**

  Run: `cargo test --test it && cargo test --test batch --test stream`
  Expected: PASS.

- [ ] **Step 4: Commit** `refactor: replies are redis::Value`

### Task 3: Lua scripts through the seam

**Files:**
- Create: `src/lua.rs`
- Modify: every `LazyLock<Script>` (121 of them, `grep -rln "LazyLock<Script>" src`), `src/client/core.rs` (`eval`, `eval_no_retry`, `evalsha_once`), `src/locks/sync.rs` (`synced_eval` keeps fred for its `WAIT` pipeline, but takes a `&Lua`), every caller of `core.eval`/`eval_no_retry`
- Test: `tests/script.rs` or `tests/it/script.rs`, whichever holds the script tests

**Interfaces:**
- Produces: `crate::lua::Lua` with `Lua::from_lua(source: impl Into<String>) -> Lua`, `Lua::sha1(&self) -> &str` (lowercase hex SHA-1 of the source) and `Lua::source(&self) -> &str`.
- Produces: `Core::eval<T: FromRedisValue>(&self, lua: &Lua, keys: Vec<String>, args: Vec<Bytes>) -> Result<T>` and `Core::eval_once<T>` (replaces `eval_no_retry`). Both send `EVALSHA`, routed by the first key; on a `NOSCRIPT` error they send `SCRIPT LOAD` to the same slot and `EVALSHA` once more.

- [ ] **Step 1: Write the failing tests**
  - unit, `src/lua.rs`: `the_sha_is_the_hex_sha1_of_the_source`: `Lua::from_lua("return 1").sha1() == "e0e1f9fabfc9d4800c877a703b823ac0578ff8db"`.
  - integration: `a_flushed_script_is_loaded_again`: an `AtomicF64::add_and_get(1.0)` (a script), then `SCRIPT FLUSH` through `common::raw_command`, then `add_and_get(1.0)` again returns `2.0`.
- [ ] **Step 2: Run them to see them fail**

  Run: `cargo test --lib lua && cargo test --test it a_flushed_script_is_loaded_again`
  Expected: FAIL, `crate::lua` does not exist.
- [ ] **Step 3: Implement `Lua`, `Core::eval`, `Core::eval_once`; replace `fred::types::scripts::Script` with `Lua` and `Script::from_lua` with `Lua::from_lua` in every static; give `synced_eval` a `&Lua`.**
- [ ] **Step 4: Run the lock, coordination and cache tests**

  Run: `cargo test --test lock --test fair_lock --test rw_lock --test semaphore --test latch --test rate_limiter --test hash_map_cache --test hash_set_cache --test local_cached_map --test it`
  Expected: PASS.
- [ ] **Step 5: Commit** `refactor: Lua scripts are run through the command seam`

### Task 4: Typed calls of the values

**Files:**
- Modify: `src/object.rs` (`get_i64_or_zero`, `exists`, `rename`, `expire`, `ttl`, `persist`, `del_all`), `src/values/{bucket,atomic_i64,atomic_f64,bit_set,bloom_filter,geo,hyper_log_log,id_generator}.rs`

**Interfaces:**
- Consumes: `Key::query`, `Key::query_once` (Task 1). A call that today goes through `no_retry(...)` or `redis_no_retry()` uses `query_once`.

- [ ] **Step 1: Replace each `self.key.core.redis().<method>(...)` and `no_retry(...)` call with `self.key.query(redis::cmd("<COMMAND>").arg(...))`, keeping the command, its arguments and its retry rule.**
- [ ] **Step 2: Run the value tests**

  Run: `cargo test --test it -- bucket atomic bit_set bloom geo hyper_log_log id_generator && cargo test --test bloom_filter`
  Expected: PASS.
- [ ] **Step 3: Commit** `refactor: the values send their commands through the seam`

### Task 5: Typed calls of the collections

**Files:**
- Modify: `src/collections/{hash_map,hash_set,sorted_set,list,vec,vec_deque,hash_map_cache,hash_set_cache,local_cached_map}.rs`

**Interfaces:**
- Consumes: `Key::query`, `Key::query_once`, `Core::query` with `Request::key` for two-key commands (`SMOVE`, `LMOVE`, `SUNION`/`SINTER`/`SDIFF` with other sets, `ZUNIONSTORE` and the like).
- Scans (`HSCAN`, `SSCAN`, `ZSCAN`) keep their cursor loop: each page is one `Key::query::<(String, Vec<Bytes>)>` with `SCAN`'s cursor, replacing fred's scan stream.
- Blocking calls (`pop_front_wait` and the like) stay on the blocking client until phase 2; only their non-blocking siblings move.

- [ ] **Step 1: Replace each typed fred call as in Task 4; rewrite the three scan streams as cursor loops yielding the same items in the same order.**
- [ ] **Step 2: Run the collection tests**

  Run: `cargo test --test it -- hash_map hash_set sorted_set vec vec_deque && cargo test --test hash_map_cache --test hash_set_cache --test local_cached_map`
  Expected: PASS.
- [ ] **Step 3: Commit** `refactor: the collections send their commands through the seam`

### Task 6: Locks, coordination and messaging

**Files:**
- Modify: `src/locks/{mod,fenced_lock,fair_lock,rw_lock,multi_lock,acquire}.rs`, `src/coordination/{adder,rate_limiter,semaphore,latch}.rs`, `src/messaging/{topic,delayed_queue}.rs`, `src/messaging/stream/{mod,requests}.rs`, `src/eviction.rs`, `src/locks/sync.rs` and `src/batch/engine.rs` (`fred::util::redis_keyslot` becomes `client::slot::keyslot`)

**Interfaces:**
- Consumes: `Key::query`, `Key::query_once`, `Core::eval`, `Core::eval_once`, `keyslot`.
- Stays on fred until phase 2: `PUBLISH`/`PUBSUB` through the pub/sub client, `XREAD`/`XREADGROUP` with `BLOCK` on the blocking client, `synced_eval`'s `WAIT` pipeline.

- [ ] **Step 1: Replace each remaining typed fred call and `fred::types::Value` argument list outside the files the Global Constraints exclude.**
- [ ] **Step 2: Check the boundary**

  Run: `grep -rln "fred::" src | sort`
  Expected: only `src/client/{core,config,tls,credentials,retry,fred_adapter}.rs`, `src/pubsub.rs`, `src/batch/{engine,mod,views}.rs`, `src/script.rs`, `src/locks/sync.rs`, `src/error.rs`.
- [ ] **Step 3: Run the whole suite**

  Run: `cargo test && cargo test --features tls-rustls --test tls`
  Expected: PASS, the same number of tests as on `main`.
- [ ] **Step 4: Run the benchmark against `main`**

  Run: `REDISSUN_BENCH_URL=redis://127.0.0.1:6390 cargo bench --bench compare` on this branch and on `main`, on a Redis started with `redis-server --port 6390 --save ''`.
  Expected: redissun within 10 % of `main` on every operation (the adapter adds one conversion per reply).
- [ ] **Step 5: Commit** `refactor: locks, coordination and messaging send their commands through the seam`, then open the pull request "redis-rs migration, phase 1: one seam for commands", with a `## Changelog` section saying the library's behaviour does not change.
