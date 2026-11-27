# redissun

redissun is a Rust port of Redisson on redis-rs and tokio.

## API design: Rust-idiomatic, not 1:1 Java

- Do not copy Redisson's Java interfaces 1:1. Design each public API the way a Rust user expects, close to the standard library types: `std::collections::HashMap` for maps (`insert`, `get`, `remove`, `entry`, `extend`, `iter`), `Option` instead of null, `Result` for errors, `Stream` for iteration, and builders with `IntoFuture` for optional settings.
- Do not keep Java names (`fastPut`, `putIfAbsent`, `computeIfPresent`, `readAllMap`, …) as method names. Merge Java overloads and `fast*` variants that differ only in what they return. Add `#[doc(alias = "fastPut")]` so people coming from Redisson still find the method.
- Feature parity stays: whatever Redisson can do, redissun can do too, with a Rust-shaped API.

## Keep 1:1: Lua scripts and Redis logic

- Port Redisson's Lua scripts, Redis commands, key names (`redissun_` prefix, `{name}` hash tags, per-key object names) and data layouts exactly. That logic is battle-tested, and Redisson and redissun clients must interoperate on the same data.
- Change the Rust surface freely; change the Redis-side behaviour only with a reason recorded in the PR.
