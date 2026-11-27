# 5: TimeSeries, TimeSeriesNative, TimeSeriesNatives (Redisson `RTimeSeries`, `RTimeSeriesNative`, `RTimeSeriesNatives`): Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Three new, experimental objects with every method of Redisson 4.8.0's `RTimeSeries` (73 methods in `javap org.redisson.api.RTimeSeries`), `RTimeSeriesNative` (35) and `RTimeSeriesNatives` (9). `TimeSeries<V, L, C>` runs Redisson's Lua scripts on Redisson's three keys, so Redisson and redissun clients share the data. `TimeSeriesNative` and `TimeSeriesNatives` send the `TS.*` commands of Redis 8 (or the RedisTimeSeries module) exactly as Redisson builds them.

**Architecture:** `TimeSeries` is a new module `src/collections/time_series/`. Redisson's script fragments (`NEXT_ID`, `STORE_ENTRY`, `RETENTION_CUTOFF`, `COLLECT_RANGE`, …) are copied verbatim as `const &str` and joined into `LazyLock<Lua>` statics exactly as Redisson concatenates them. Options are `IntoFuture` builders (`.label`, `.ttl`, `.retention`, `.rev`, `.limit`, …). Redisson's `TimeSeriesEvictionTask` runs through the crate's `eviction::schedule`. `TimeSeriesNative` and `TimeSeriesNatives` live in `src/collections/time_series_native/`. One private `args.rs` builds every argument list (Redisson's `appendCreationOptions` and `appendRangeOptions`), and one `reply.rs` parses RESP2 replies. Values there are `f64` and need no codec.

**Tech Stack:** Rust 2024, tokio, redis-rs, testcontainers (Redis 7.4, `redis:8.10-alpine`, a 7.4 Cluster and an 8.10 Cluster).

**Spec:** `docs/superpowers/specs/2026-11-26-redisson-4x-objects-design.md` ("### 5. `TimeSeries`, `TimeSeriesNative`, `TimeSeriesNatives`", "API conventions", "Constraints", "Testing"). The naming rules of `docs/superpowers/specs/2026-11-27-rust-idiomatic-api.md` replace the spec's naming lines. Redisson sources: `redisson-4.8.0-sources.jar` (`RedissonTimeSeries.java`, `RedissonTimeSeriesNative.java`, `RedissonTimeSeriesNatives.java`, `eviction/TimeSeriesEvictionTask.java`, `api/ts/*`, `api/tsnative/*`). Tests come from tag `redisson-4.8.0`: `RedissonTimeSeriesTest.java` (151 tests), `RedissonTimeSeriesNativeTest.java` (75) and `RedissonTimeSeriesNativesTest.java` (38).

**Branch:** `feat/time-series`, from `main`.

**Pull requests:** two, in this order. Each PR is its own sequence of tasks and ships on its own.
- **PR 5a `TimeSeries`:** Tasks 1–8.
- **PR 5b `TimeSeriesNative` + `TimeSeriesNatives`:** Tasks 9–14, branched from `main` after 5a is merged (`feat/time-series-native`).

## Owner decisions (delegated, 2026-11-28: "prostě to odmakej")

1. **Two PRs, 5a and 5b** (above). The two halves share no code, and 5a alone is about 40 scripts.
2. **Timestamps are `i64` milliseconds, not `u64`.** Redisson's `long` is signed. Its tests store negative timestamps and `Long.MIN_VALUE`/`Long.MAX_VALUE` (`testNegativeAndZeroTimestamps`, `testTimestampsAtTheEdgeOfTheLongRange`), and a `u64` API could not read such data. The input type is `Timestamp` with `From<i64>`, `From<i32>`, `From<u32>` and `From<SystemTime>`. `From<i32>` lets integer literals work. A `SystemTime` before 1970 gives a negative timestamp, and one past `i64::MAX` ms saturates. There is also `TryFrom<u64>`, which returns `Error::OutOfRange` above `i64::MAX`. Outputs are plain `i64`.
3. **The spec's layout line is outdated.** It says "a sorted set by timestamp, a hash of entries". In 4.8.0 the layout is a sorted set at the raw name (scored by timestamp), a sorted set `redisson__ts_ttl:{name}` (the same members, scored by expiry in Unix ms) and a hash `redisson__ts_seq:{name}` (fields `id`, `total`, `incremented`). Each member is `struct.pack('BBc0Lc0Lc0', 4, #id, id, #value, value, #field, field)`, where `id` is a 20-digit zero-padded counter and `field` is one flag byte (`\2` = no label, `\3` = label) followed by the encoded label. This plan ports 4.8.0, with `redissun__ts_ttl` and `redissun__ts_seq`.
4. **Variants that differ only in what they return are merged** (CLAUDE.md). `remove`/`getAndRemove` become `remove → Option<V>`, `removeAll`/`getAndRemoveAll` become `remove_all → Vec<V>`, and `get`/`getLatest` and `getInfo`/`getDebugInfo` become builders. Redisson's `removeAll(long)` script (count only) is not ported. `remove_all(ts).len()` gives the count.
5. **Every `add` overload uses Redisson's `ADD_ENTRY`** (the script of `add(TimeSeriesAddArgs)`). The deprecated `add(long, V[, L][, ttl])` overloads go through Redisson's `addAll(Map)` script, which stores the same member and differs only in the far-future expiry score of an entry without a TTL (`max(last score, now + 100 y) + 1` vs `now + 100 y`). Both mean "never". `extend` uses Redisson's `addAll(Collection, ttl)` scripts, because only they take a batch.
6. **`TimeSeriesNative*` tests run on `redis:8.10-alpine`**, Redisson's own test image (`RedisDockerTest.IMAGE = "redis:8.10"`), through a new `redis810_url()`. `redis8_url()` (8.8) lacks `EXCLUDEEMPTY` (8.10), and Redisson only tests the module on 8.10. If CI shows that 8.10-alpine lacks `TS.READ` or `TS.NRANGE`, switch the helper to `8.12-alpine`. The spec measured that it has the module.
7. **No capability probe for `TS.*`.** Each `TS.*` call is one command (or `try_create`'s one-line script), so nothing is left half-written. An "unknown command" reply (or "Unknown Redis command called from script") becomes `Error::Unsupported("TimeSeriesNative needs Redis 8.0 or newer, or the RedisTimeSeries module")`. Nothing is cached, so a server upgrade is seen at once. There is no `COMMAND INFO`. This is `map_cache_native/support.rs`'s rule ("never cache a no") without the probe round trip.
8. **Keyless `TimeSeriesNatives` commands** (`TS.MGET`, `TS.MRANGE`, `TS.MREVRANGE`, `TS.QUERYINDEX`, `TS.QUERYLABELS`) go to one master, as Redisson's `readAsync((String) null, …)` does. Redis 8's module keeps a cluster-wide index (Redisson's `testInClusterFansOutAndSplitsBySlot`).
9. **Counts and limits of `0` return nothing, without a call.** Rust's `take(0)` and `Vec::with_capacity(0)` work this way. In Redisson, `range(.., limit = 0)` means "no limit" and `first(-1)` means "all". In redissun, "all" is `range(..)`, and a limit is a `usize`, so it cannot be negative.
10. **RESP3 tests are not ported.** redissun's data connections speak RESP2. RESP3 is only used for pub/sub. The two Infinity tests are ported as RESP2 tests (`"inf"`/`"-inf"` bulk strings).

## Decisions (mine, by the rules: Rust-idiomatic, Redisson parity, same name ⇒ same behaviour)

- **Type and factory.** `pub struct TimeSeries<V, L, C: Codec> { key: Key, codec: C }` and `Client::time_series<V, L>(&self, name) -> TimeSeries<V, L, C>` (alias `getTimeSeries`). It schedules the clean-up task on creation, as `HashMapCache::new` does. Use `L = ()` for no labels. `with_codec` works as elsewhere.
- **Names (`javap` 4.8.0 → redissun):**

  | Redisson | redissun | Returns |
  |---|---|---|
  | `add(long, V)`, `add(long, V, L)`, `add(long, V, Duration)`, `add(long, V, L, Duration)`, `add(long, V, long, TimeUnit)`, `add(TimeSeriesAddArgs)` | `insert(ts, &v)` with `.label(&l)`, `.ttl(d)`, `.retention(d)` | `bool` (added; `false` when outside its retention window) |
  | `addIfAbsent(args)` | `try_insert(ts, &v)` (same options) | `bool` |
  | `addOrReplace(args)` | `set(ts, &v)` (same options) | `bool` (`true` = nothing live was there), like `HashMap::set` |
  | `addIfLess`, `addIfGreater`, `addAndSum` | `set_if_less`, `set_if_greater`, `set_sum` (same options) | `bool` |
  | `addAndGet(args)` | `add(ts, &delta)` (same options), rule 4 | `f64` (the running total) |
  | `addAll(Map[, ttl])`, `addAll(Map, long, TimeUnit)`, `addAll(Collection[, ttl])` | `extend(entries)` with `.ttl(d)` | `()` |
  | `size()` | `len()`, `is_empty()` | `usize`, `bool` |
  | `get(ts)`, `getEntry(ts)` | `get(ts)`, `entry(ts)` | `Option<V>`, `Option<TimeSeriesEntry<V, L>>` |
  | `getAll(ts)`, `getAllEntries(ts)` | `get_all(ts)`, `get_all_entries(ts)` | `Vec<V>`, `Vec<TimeSeriesEntry>` |
  | `remove(ts)`, `getAndRemove(ts)` | `remove(ts)` | `Option<V>` |
  | `getAndRemoveEntry(ts)` | `remove_entry(ts)` | `Option<TimeSeriesEntry>` |
  | `removeAll(ts)`, `getAndRemoveAll(ts)` | `remove_all(ts)` | `Vec<V>` |
  | `getAndRemoveAllEntries(ts)` | `remove_all_entries(ts)` | `Vec<TimeSeriesEntry>` |
  | `first()`, `first(n)`, `firstEntry()`, `firstEntries(n)` | `first()`, `first_many(n)`, `first_entry()`, `first_entries(n)` | ascending |
  | `last()`, `last(n)`, `lastEntry()`, `lastEntries(n)` | `last()`, `last_many(n)`, `last_entry()`, `last_entries(n)` | the newest `n`, **ascending** (Redisson's `EVAL_LIST_REVERSE` turns them around) |
  | `pollFirst()`, `pollFirst(n)`, `pollFirstEntry()`, `pollFirstEntries(n)` | `pop_first()`, `pop_first_many(n)`, `pop_first_entry()`, `pop_first_entries(n)` | ascending |
  | `pollLast…` | `pop_last()`, `pop_last_many(n)`, `pop_last_entry()`, `pop_last_entries(n)` | ascending |
  | `firstTimestamp()`, `lastTimestamp()` | the same in snake case | `Option<i64>` |
  | `range(s, e[, limit])`, `rangeReversed(…)`, `rangeByLabel(…)`, `rangeReversedByLabel(…)` | `range(r)` with `.rev()`, `.limit(n)`, `.label(&l)`, `.unlabelled()` | `Vec<V>` (descending with `.rev()`) |
  | `entryRange…` (8 overloads) | `range_entries(r)` (same options) | `Vec<TimeSeriesEntry>` |
  | `removeRange(s, e)`, `removeRangeByLabel(s, e, l)` | `remove_range(r)` with `.label(&l)`, `.unlabelled()` | `usize` |
  | `labels()`, `labels(s, e)` | `labels(r)` | `Vec<L>`, in order of first appearance |
  | `aggregate(TimeSeriesAggregationArgs)` | `aggregate(r, bucket, &[Aggregation])` with `.align_to(ts)`, `.filter_by_value(min, max)`, `.label(&l)`, `.unlabelled()` | `Vec<TimeSeriesBucket>` |
  | `readTail(TimeSeriesReadArgs)` | `read_after(ts)` with `.count(n)`, `.label(&l)`, `.unlabelled()` | `Vec<TimeSeriesEntry>` |
  | `info()` | `info()` | `TimeSeriesInfo` |
  | `iterator()`, `iterator(n)`, `stream()`, `stream(n)`, `Iterable` | `iter()`, `iter_with(page_size)` | `impl Stream<Item = Result<V>>` |
  | `addListener(ScoredSortedSetAddListener / ScoredSortedSetRemoveListener / DeletedObjectListener / ExpiredObjectListener)`, `removeListener` | `events()`, `events_of(&[TimeSeriesEventKind])`; drop the stream | `TimeSeriesEvents<C>` |
  | `delete`, `isExists`, `rename`, `expire`, `clearExpire`, `remainTimeToLive` | `Object` over the three keys | |
  | `TrackingListener`, `copy`, `renamenx`, `expireAt`, conditional `expire`, `sizeInMemory`, `destroy`, all `*Async` | not ported, as on every other object (`limitations.mdx`); every method is async | |

  Every renamed method carries `#[doc(alias = "<each Redisson name in its row>")]`.
- **`TimeSeriesEntry<V, L>`**: `#[derive(Clone, Debug, PartialEq, Eq)] pub struct TimeSeriesEntry<V, L> { pub timestamp: i64, pub value: V, pub label: Option<L> }`, with `From<(T, V)>` and `From<(T, V, L)>` for `T: Into<Timestamp>`. `extend` takes `impl IntoIterator<Item = E>` where `E: Into<TimeSeriesEntry<V, L>>`, so a `Vec<(i64, V)>` or a `BTreeMap<i64, V>` works directly.
- **Ranges.** `r: impl RangeBounds<T>` with `T: Into<Timestamp> + Copy`. `Included(x)` → `x`, `Excluded(x)` → `x ± 1` (checked: an overflow gives an empty result), `Unbounded` → `i64::MIN`/`i64::MAX`. When start > end, the result is empty without a call. Redisson's inclusive `(start, end)` is `start..=end`.
- **Label filters** (Redisson's `LABEL_MATCH` = 1, `LABEL_ABSENT` = 2). `.label(&l)` sends mode `1` and the encoded label. `.unlabelled()` is Redisson's `label(null)` and sends mode `2` and `0`. Without either, it sends mode `0` and `0`. The last call wins.
- **TTL and retention**, as Redisson's `expirationTime`/`retentionWindow`. A `.ttl` of zero, under 1 ms, or one that overflows `i64` ms means none, and sends `now + 3_153_600_000_000` (100 × 365 days). `.retention(d)` sends `d` in ms, saturating at `i64::MAX`. Zero means no retention. `now` is the client's clock (`object::now_millis()`), as Redisson's `System.currentTimeMillis()`.
- **Numeric codec check** (Redisson `checkNumericCodec`/`probeNumericCodec`, cached per handle in a `OnceLock<bool>`). It runs before `set_if_less`, `set_if_greater`, `set_sum`, `add` and `aggregate`. The codec encodes `1.5f64` and `-2.25f64`. If either is not ASCII text that parses back to the same `f64`, the call returns `Error::Config("<method> on '<name>' requires a codec that encodes numbers as text; use JsonCodec or StringCodec")` and sends nothing. If the codec fails to encode at all, the script decides, as in Redisson.
- **`aggregate`**: `Aggregation::{Count, Sum, Avg, Min, Max, ValueRange, First, Last, StdDevPopulation, StdDevSample, VariancePopulation, VarianceSample}`, with codes `1..=12` (Redisson's `ordinal() + 1`). Duplicates are dropped and first order is kept (Redisson's `LinkedHashSet`). The call returns `Error::Config` when the bucket is under 1 ms ("bucket interval should be defined and at least one millisecond") or the list is empty ("at least one aggregation should be defined"). `TimeSeriesBucket { pub timestamp: i64, values: Vec<(Aggregation, f64)> }` has `pub fn get(&self, a: Aggregation) -> Option<f64>` and `pub fn values(&self) -> &[(Aggregation, f64)]`. A value the script reports as `''` (undefined) is left out.
- **`TimeSeriesInfo`** (Redisson's 7 rows): `{ pub len: usize, pub total_entries: usize, pub first_timestamp: Option<i64>, pub last_timestamp: Option<i64>, pub memory_usage: u64, pub ttl: Option<Duration>, pub entries_issued: u64 }`. `pttl` `-1`/`-2` gives `None`. `entries_issued` saturates at `u64::MAX`, like Redisson's `parseEntriesIssued`.
- **Events.** `#[non_exhaustive] pub enum TimeSeriesEvent { Added, Removed, Deleted, Expired }` (Redisson's `ScoredSortedSetAddListener` on `__keyevent@*:zadd`, `ScoredSortedSetRemoveListener` on `__keyevent@*:zrem`, `DeletedObjectListener` on `del`, `ExpiredObjectListener` on `expired`). Each fires only when the payload equals the raw name, so the timeout set's own `zadd`/`zrem` are ignored. There is a matching `TimeSeriesEventKind`, and `TimeSeriesEvents` has `recv()` and `into_stream()`, as `MultimapEvents` has. No kinds is `Error::Config`. The doc says Redis sends these only with `notify-keyspace-events` `Ez` (`Eg` for del, `Ex` for expired).
- **`TimeSeriesNative`** (no type parameters; values `f64`): `Client::time_series_native(name)` (alias `getTimeSeriesNative`). Shared private `CreationOptions { retention, encoding, chunk_size, duplicate_policy, ignore: Option<(Duration, f64)>, labels: Vec<(String, String)> }`, plus a `creation_options!()` macro for the setters `.retention(d)`, `.encoding(e)`, `.chunk_size(n)`, `.duplicate_policy(p)`, `.ignore(max_time_diff, max_value_diff)`, `.label(name, value)` and `.labels(iter)`. They are written in Redisson's order: `RETENTION`, `ENCODING`, `CHUNK_SIZE`, `DUPLICATE_POLICY`, `IGNORE`, `LABELS`. A retention under 1 ms that is not zero is `Error::Config("retention below 1ms is not representable … never expires")`.

  | Redisson | redissun | Returns |
  |---|---|---|
  | `create()`, `create(TSCreateArgs)` | `create()` with the creation options | `()` |
  | `createIfAbsent()`, `createIfAbsent(args)` | `try_create()` (same options), rule 1 | `bool` |
  | `alter(TSAlterArgs)` | `alter()` (all options except `encoding`) | `()` |
  | `add(long, double)`, `add(TSAddArgs)` | `add(ts, value)` with `.on_duplicate(p)` and the creation options | `i64` |
  | `addCurrent(double)`, `TSAddArgs.currentSample` | `add_now(value)` (same options; sends `*`) | `i64` |
  | `addAll(Map<Long, Double>)` | `add_many(samples: impl IntoIterator<Item = (T, f64)>)` | `Vec<i64>` in input order |
  | `incrementBy(double)`, `incrementBy(TSIncrArgs)` | `incr_by(value)` with `.timestamp(ts)` and the creation options | `i64` |
  | `decrementBy(…)` | `decr_by(value)` (same) | `i64` |
  | `removeRange(from, to)` | `remove_range(r)` | `u64` |
  | `first()` | `first()` | `Option<Sample>` |
  | `size()`, `firstTimestamp()`, `lastTimestamp()` | `len()`, `first_timestamp()`, `last_timestamp()` | `u64`, `Option<i64>` (`None` when `totalSamples` is 0, read from the same `TS.INFO` reply) |
  | `get()`, `getLatest()` | `get()` with `.latest()` | `Option<Sample>` |
  | `range(from, to)`, `range(TSRangeArgs)`, `rangeReversed(…)` | `range(r)` with `.rev()`, `.latest()`, `.filter_by_ts(&[ts])`, `.filter_by_value(min, max)`, `.count(n)`, `.align(Align)`, `.aggregate(bucket, &[Aggregator])`, `.bucket_timestamp(b)`, `.empty()` | `Vec<Sample>` |
  | `read(long)`, `read(TSReadArgs)` | `read(ReadFrom)` with `.block(timeout, min_count)`, `.max_count(n)` | `Vec<Sample>` |
  | `createRule(TSRuleArgs)`, `deleteRule(dest)` | `create_rule(dest, aggregator, bucket)` with `.align_timestamp(ts)`, `delete_rule(dest)` | `()` |
  | `iterator()`, `iterator(n)`, `stream()`, `stream(n)`, `Iterable` | `iter()`, `iter_with(page_size)` | `impl Stream<Item = Result<Sample>>` |
  | `getLabels()` | `labels()` | `Vec<(String, String)>` in server order |
  | `getInfo()`, `getDebugInfo()` | `info()` with `.debug()` | `TsInfo` |
  | `RExpirable` | `Object` (one key) | |

- **`TimeSeriesNatives`** (`Client::time_series_natives()`, alias `getTimeSeriesNatives`):

  | Redisson | redissun | Returns |
  |---|---|---|
  | `addAll(Map<String, Map<Long, Double>>)` | `add_many(series: impl IntoIterator<Item = (N, S)>)`, `N: Into<String>`, `S: IntoIterator<Item = (T, f64)>` | `Vec<(String, Vec<i64>)>` in input order |
  | `getAll(TSMultiGetArgs)` | `get(&Filter)` with `.latest()`, `.with_labels()`, `.selected_labels(&[..])` | `Vec<SeriesSample>` |
  | `range(TSMultiRangeArgs)`, `rangeReversed(…)` | `range(&Filter, r)` with `TimeSeriesNative::range`'s options plus `.with_labels()`, `.selected_labels(..)`, `.group_by(label, Reducer)`, `.exclude_empty()` | `Vec<SeriesSamples>` |
  | `groupedRange(TSGroupedRangeArgs)`, `groupedRangeReversed(…)` | `grouped_range(&[&str], r)` with the range options, `.rev()` and `.aggregate_each(bucket, &[&[Aggregator]])` (one list per key) | `Vec<Sample>` |
  | `queryIndex(String...)` | `query_index(&Filter)` | `Vec<String>` |
  | `labelNames(String...)`, `labelValues(label, String...)` | `label_names()` / `label_values(label)` with `.filter(&Filter)` | `Vec<String>` |

  `Filter::new()` with `.eq(l, v)` (`l=v`), `.ne(l, v)` (`l!=v`), `.eq_any(l, vs)` (`l=(a,b)`), `.ne_any(l, vs)` (`l!=(a,b)`), `.has(l)` (`l!=`), `.missing(l)` (`l=`), `.raw(expr)`. An empty filter, where one is required, is `Error::Config("At least one filter expression is required")`. `SeriesSample { pub key: String, pub labels: Vec<(String, Option<String>)>, pub sample: Option<Sample> }`. `SeriesSamples { pub key, pub labels, pub samples: Vec<Sample> }`. A selected label that the series lacks is `None`, Redisson's `null`.
- **Shared native types** in `pub mod time_series_native`: `Sample { pub timestamp: i64, pub values: Vec<f64> }` (`PartialEq`, `fn value(&self) -> f64` = the first value), `DuplicatePolicy::{Block, First, Last, Min, Max, Sum}`, `Encoding::{Compressed, Uncompressed}`, `Aggregator` (15: `Avg` `avg`, `Sum`, `Min`, `Max`, `Range`, `Count`, `CountNan` `countNaN`, `CountAll` `countAll`, `First`, `Last`, `StdP` `std.p`, `StdS` `std.s`, `VarP` `var.p`, `VarS` `var.s`, `Twa` `twa`), `Reducer` (12: the same list without `First`, `Last`, `Twa`), `BucketTimestamp::{Start, End, Mid}` (`start`/`end`/`mid`), `Align::{Start, End, At(i64)}` (`-`/`+`/n), `ReadFrom::{At(T), Earliest, Last, Next}` (`n`/`-`/`+`/`$`), `TsInfo`, `Rule`, `ChunkInfo`, the builders and `Filter`. Number replies are parsed with `f64::from_str`, which reads `inf`, `-inf` and `nan`.

## Redis commands per method

**`TimeSeries`** (`RedissonTimeSeries.java`; KEYS are `[name, redissun__ts_ttl:{name}, redissun__ts_seq:{name}]` unless a row says `[1,2]`).

| redissun | Script (Redisson constant or method, line) | ARGV |
|---|---|---|
| `insert` | `ADD_ENTRY` (510) | `[20, retention, now, ts, flag 2/3, value, label or "", expiration]` (`encodeArgs`, 781) |
| `try_insert` / `set` | `ADD_IF_ABSENT` (520) / `ADD_OR_REPLACE` (551) | the same |
| `set_if_less` / `set_if_greater` / `set_sum` | `ADD_COMPARING` (635) | the same + policy `1`/`2`/`3` |
| `add` | `INCREMENT` (723); reply decoded as text, then `f64` | the same |
| `extend` / `extend(..).ttl(d)` | `addAllAsync(Collection, ttl)` scripts (236–290: without and with TTL) | `[20, expiration, (ts, flag, value, label)…]` |
| `len` | `sizeAsync` (1165), KEYS `[1,2]` | `[now]` |
| `get` / `entry` | `getAsync` (1179) / `getEntryAsync` (1200), KEYS `[1,2]` | `[now, ts]` |
| `get_all` / `get_all_entries` | `LIVE_AT_TIMESTAMP` + bodies (1240, 1258), KEYS `[1,2]` | `[now, ts]` |
| `remove` / `remove_entry` | `getAndRemoveAsync` (1369) / `getAndRemoveEntryAsync` (1392), KEYS `[1,2]` | `[now, ts]` |
| `remove_all` / `remove_all_entries` | `getAndRemoveAllAsync` (1303) / `…EntriesAsync` (1323), KEYS `[1,2]` | `[now, ts]` |
| `first*`, `last*`, `range`, `range_entries`, `*_timestamp` | `COLLECT_VALUES` / `COLLECT_ENTRIES` / `COLLECT_TIMESTAMPS` (`collectAsync`, 1906), KEYS `[1,2]` | `[now, start, end, limit, rev 0/1, label mode, label, whole 0]` |
| `pop_*` | `POLL_VALUES` / `POLL_ENTRIES` (`collectAndRemoveAsync`, 1921), KEYS `[1,2]` | `[now, i64::MIN, i64::MAX, n, rev, 0, 0, 0]` |
| `read_after` | `COLLECT_ENTRIES` with whole timestamps `1` (1097) | start = the next double above `ts` (`f64::next_up` when `ts as f64 as i64 <= ts`), end `i64::MAX` |
| `remove_range` | `removeRangeAsync` (1954), KEYS `[1,2]` | `[now, start, end, label mode, label]` |
| `labels` | `COLLECT_LABELS` (2094), KEYS `[1,2]` | `[now, start, end]` |
| `aggregate` | `AGGREGATE_RANGE` (`AGGREGATE` 1785 + `COLLECT_RANGE` + `CLOSE_LAST_BUCKET`), KEYS `[1,2]` | `[now, start, end, 0, 0, mode, label, bucket ms, alignment, filter 0/1, min, max, codes…]` (1040) |
| `info` | `INFO` (1668); reply decoded as text | `[now]` |
| `iter` | `scanIteratorAsync` (2376), KEYS `[1,2]` | `[pos, now, page]` |
| clean-up task | `TimeSeriesEvictionTask.execute` | `[now, KEYS_LIMIT]` |
| `Object::del` / `expire` / `persist` / `rename` | the crate's companion-key scripts over the three keys (Redisson's `deleteAsync`, `expireAsync`, `clearExpireAsync`, `renameAsync` with `kks`) | |

The number formats are Redisson's (`Long.toString`, `Double.toString`). The scripts parse them with `tonumber`, so Rust's `i64::to_string` and `f64` `Display` (shortest round trip) are equivalent.

**`TimeSeriesNative` / `Natives`:** exactly `RedissonTimeSeriesNative.java` and `RedissonTimeSeriesNatives.java`. `try_create` is the one script `if redis.call('exists', KEYS[1]) == 0 then redis.call('TS.CREATE', KEYS[1], unpack(ARGV)); return 1; end; return 0;`. Range options go in `appendRangeOptions` order: `from to [LATEST] [FILTER_BY_TS ts…] [FILTER_BY_VALUE min max] [selection] [COUNT n] [ALIGN a] [AGGREGATION aggs bucket_ms [BUCKETTIMESTAMP b] [EMPTY]]`. Natives add `[EXCLUDEEMPTY] FILTER f… [GROUPBY l REDUCE r]`. A blocking `read` sends `TS.READ key ts BLOCK ms min_count [MAX_COUNT n]` through `Core::blocking` (no command timeout), and a plain `read` sends `TS.READ key ts [MAX_COUNT n]`. `TS.NRANGE` sends `numkeys key… <range options>`, routed by the first key. `add_many` on `Natives` sends one `TS.MADD` per slot through `Core::query_by_slot`.

No Redis-side deviation from Redisson is planned. Each script is a verbatim copy. If one turns out to be needed, it gets a `/// Deviation from Redisson: …` comment and a PR line.

## Global Constraints

- `#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::todo, clippy::unimplemented)]`: no `unwrap`/`expect` in library code. Tests may unwrap.
- **Never run cargo, docker, builds or tests locally.** The laptop freezes. Every "Run" line below means: push the branch and read the GitHub CI log of the `test` job (`cargo test` runs every file in `tests/`). One push at a time.
- Lua: every script is copied verbatim from `RedissonTimeSeries.java` 4.8.0, with the line numbers above, KEYS and ARGV in Redisson's order and a `/// RedissonTimeSeries.<method or constant>` comment. Fragments are `const &str` with Redisson's names, joined with `[A, B, …].concat()` in the same order as Redisson's `+`. No text changes, not even in the error messages (`'RTimeSeries … is not a finite number'` and the others stay, because Redisson's tests and users match on them).
- Keys: the main sorted set is the raw name (`Key::new(core, name)`, no tag added). Companions are `prefix_name("redissun__ts_ttl", name)` and `prefix_name("redissun__ts_seq", name)`. The clean-up task is named `redissun__time_series_eviction:<name>`.
- Minimum server: `TimeSeries` runs on Redis/Valkey 6.2 (Lua `struct` only). `TimeSeriesNative*` need Redis 8.0 or the module. Multi-aggregator ranges, `countNaN` and `countAll` need 8.6, and `EXCLUDEEMPTY` needs 8.10. Errors the server returns are passed on unchanged.
- Every new public item is **experimental**: the first line of the type doc is "**Experimental.**", and so are the README rows and the docs pages.
- Optional settings are `#[must_use]` builders implementing `IntoFuture` with `type IntoFuture = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>`, never `bool`/sentinel parameters. The last call of a mutually exclusive pair wins (`.label`/`.unlabelled`). The rest are validated when awaited, before anything is sent.
- No private trait in a public bound (`private_bounds`). Borrowed values (`&W where V: Borrow<W>`, `W: Serialize + ?Sized + Sync`), as `HashMap::insert`.
- Exports: `TimeSeries`, `TimeSeriesEntry`, `Timestamp`, `TimeSeriesEvent`, `TimeSeriesEventKind`, `TimeSeriesNative`, `TimeSeriesNatives` at the crate root. `pub mod time_series { Insert, TryInsert, Set, SetIfLess, SetIfGreater, SetSum, Add, Range, RangeEntries, RemoveRange, Aggregate, ReadAfter, Extend, Aggregation, TimeSeriesBucket, TimeSeriesInfo, TimeSeriesEvents }` and `pub mod time_series_native { … }` as listed in Decisions.
- No git worktrees, no parallel subagents.

## Review Focus

1. **Several entries at one timestamp, and their order.** `insert` keeps every entry at a timestamp, in insertion order (the id counter). `get`/`entry`/`remove` reach only the first live one. `get_all`/`remove_all` reach all of them. `set`, `set_if_*`, `set_sum` and `add` leave exactly one entry. A user who mixes them expects exactly that. Pinned in Task 2 (`one_timestamp_holds_many_until_set_collapses_it`) and the Redisson ports `duplicates_keep_insertion_order`, `add_or_replace_collapses_duplicates`, `add_policies_collapse_duplicates_to_one_entry`.
2. **Expired entries that the clean-up has not removed yet.** No read may return one: `len`, `get*`, `first*`/`last*`, `range*`, `labels`, `aggregate`, `info.len`, `read_after`. `pop_*` must skip it without deleting it. `try_insert` and the policies treat it as absent. Pinned in Task 2 (`expired_entries_are_invisible_everywhere`, one entry with `.ttl(50ms)`, a 150 ms sleep, then every read) and the ports `pop_skips_expired_without_consuming_them`, `plural_accessors_skip_expired`, `aggregate_ignores_expired_entries`.
3. **Timestamps at the edges.** `-1`, `0`, `i64::MIN`, `i64::MAX` and values above 2^53 (scores are doubles). `range(..)` must cover all of them, `Excluded(i64::MAX)` must not overflow, and `range(5..5)` is empty without a call. Pinned in Task 1 (`timestamp_conversions`, a unit test) and Task 4 (`range_bounds_at_the_edges`), plus the ports `timestamps_at_the_edge_of_the_long_range`, `read_tail_above_two_to_the_fifty_three`.
4. **Data written by Redisson.** The key names, the member packing and the hash fields must be byte-identical, and members in the 4.7 layout (marker 2/3) must still read. Pinned in Task 1 (`the_layout_is_redissons`: after `insert(7, "v").label("l")`, `ZRANGE <n> 0 -1` gives one member that starts with `\x04\x14` and then 20 ASCII digits, `ZCARD redissun__ts_ttl:{<n>}` is 1, `HGET redissun__ts_seq:{<n>} id` is `"1"`) and the port `label_filter_reads_entries_written_by_an_older_version` (Task 4, with the keys renamed to `redissun__`).
5. **A server without the time series module.** On Redis 7.4, every `TimeSeriesNative` and `TimeSeriesNatives` method must return `Error::Unsupported` naming Redis 8.0. Not one may return a raw `Error::Redis`, and a second call after that must ask the server again. Pinned in Task 9 (`ts_commands_need_redis_8`) and Task 12 (`natives_need_redis_8`).

---

## PR 5a: `TimeSeries`

### Task 1: The type, layout, clean-up, `insert`, `try_insert`, `set`, `get`, `entry`, `len`, `Object`

**Files:**
- Create: `src/collections/time_series/mod.rs` (type, factory glue, `HasKey`, reads), `scripts.rs`, `builders.rs` (`Insert` family), `timestamp.rs` (`Timestamp`, `TimeSeriesEntry`, `bounds`), `website/content/docs/objects/time-series.mdx`, `tests/time_series.rs`
- Modify: `src/collections/mod.rs`, `src/client/mod.rs`, `src/lib.rs`, `src/eviction.rs` (a `pub(crate) fn time_series_arguments(_: Option<Duration>) -> Vec<Bytes>` = `[now_millis, KEYS_LIMIT]`), `website/content/docs/objects/meta.json` (`"time-series"` after `"sorted-set"`), `README.md`
- Test: unit tests in `timestamp.rs` and `scripts.rs`; `tests/time_series.rs`

**Interfaces:**
- Produces:
  - `pub struct Timestamp(i64)` with `pub const fn from_millis(i64) -> Self`, `pub const fn as_millis(self) -> i64`, and the `From`/`TryFrom` impls of owner decision 2.
  - `TimeSeriesEntry<V, L>` (Decisions). `pub(crate) fn bounds<T: Into<Timestamp> + Copy>(r: &impl RangeBounds<T>) -> Option<(i64, i64)>`.
  - `pub struct TimeSeries<V, L, C: Codec>`: `Clone`, `Debug` (`key.describe(f, "TimeSeries")`). `HasKey` with `companions() = [ttl, seq]` and `renamed_companions(new) = Some([prefix_name(.., new) ×2])`, so `Object` covers all three keys.
  - `pub(crate) fn new(key: Key, codec: C) -> Self`, which calls `eviction::schedule(core, "redissun__time_series_eviction:<name>", &EVICT, [name, ttl, seq], time_series_arguments, vec![])`.
  - `pub(crate) struct AddArgs { label: Option<Bytes>, ttl: Option<Duration>, retention: Option<Duration> }` with `fn encode(&self, ts: i64, value: Bytes, now: i64) -> Vec<Bytes>` (the ARGV of `encodeArgs`).
  - Builders `Insert`, `TryInsert`, `Set` sharing `.label(&'a L2)`, `.ttl(Duration)` and `.retention(Duration)`, each `IntoFuture<Output = Result<bool>>`, with `pub fn insert<'a, W, T>(&'a self, ts: T, v: &'a W) -> Insert<'a, V, L, C, W>`.
  - `get(ts) -> Result<Option<V>>`, `entry(ts) -> Result<Option<TimeSeriesEntry<V, L>>>`, `len() -> Result<usize>`, `is_empty()`.
  - `pub(crate) fn decode_entries(&self, flat: Vec<redis::Value>) -> Result<Vec<TimeSeriesEntry<V, L>>>`: groups of `[value, label, flag, ts]`, where the label is taken only when `flag == 3` (`TimeSeriesEntryReplayDecoder`). `decode_entry` does the same for `[flag, ts, value[, label]]`.
- [ ] **Step 1: Write failing unit tests.** `timestamp.rs`: `timestamp_conversions` (`5i32`, `-1i64`, `UNIX_EPOCH - 1s` → `-1000`, `u64::MAX` → `Err(OutOfRange)`). `bounds_cover_every_shape`: `..` → `(i64::MIN, i64::MAX)`, `3..7` → `(3, 6)`, `..=i64::MAX` → `(MIN, MAX)`, `(Excluded(i64::MAX), Unbounded)` → `None`, `5..5` → `None`. `scripts.rs`: `add_entry_is_redissons_concatenation`: `ADD_ENTRY.source()` starts with `"local sequenceWidth = tonumber(ARGV[1]); "` and ends with `"return added;"`, and its SHA-1 equals the SHA-1 of the text copied into the test from Redisson by hand (one script is enough to catch a join-order mistake).
- [ ] **Step 2: Write failing integration tests** in `tests/time_series.rs`. The header comment lists every Redisson test with its port name and the not-ported ones with reasons (owner decisions 9, 10 and the list in Task 7). Ports are named `redisson_<snake_case of the Java name without "test">`, and `test()` becomes `redisson_basic`. The helper is `ts(name) -> TimeSeries<String, String, JsonCodec>` on `redis_url()`. Own tests:
  - `the_layout_is_redissons` (Review Focus 4).
  - `insert_returns_true_and_get_reads_it`; `try_insert_keeps_the_live_entry`; `set_reports_new_and_replaces`.
  - `a_zero_or_sub_millisecond_ttl_means_none`: `.ttl(ZERO)` and `.ttl(500µs)` store entries whose `ZSCORE redissun__ts_ttl:{n}` is more than `now + 99 years`.
  - `object_methods_cover_the_three_keys`: `expire(10s)` gives all three keys a `PTTL > 0`. `rename(n2)` moves all three. `del()` deletes all three.
  - `the_clean_up_removes_expired_entries_and_the_counter`: a client with `.eviction_interval(1s)`, `insert(1,"a").ttl(100ms)`, and within 8 s `EXISTS` of all three keys is `0`.
  - Ports: `MultipleValues`, `Cleanup`, `GetEntry`, `AddIfAbsent`, `AddIfAbsentTreatsExpiredAsAbsent`, `AddOrReplace`, `AddArgs`, `AddArgsCarriesLabelAndTimeToLive`, `ZeroTimeToLiveMeansNoTimeToLive`, `ATimeToLiveThatCannotBeHonouredMeansNoTimeToLive`, `ExpireAtCoversEveryKey` (with `expire`, since `expireAt` is not ported), `RenameLeavesTheDestinationMirroringTheSource`, `SizeCountsAnEntryWithANegativeExpirationAsExpired` (writes the member with `raw_command` `ZADD`).
- [ ] **Step 3: Implement** the items in Interfaces. `scripts.rs` this task: the fragments `NEXT_ID`, `REMOVE_MEMBERS`, `ENTRY_ARGS`, `STORE_ENTRY`, `RETENTION_CUTOFF`, `WITHIN_RETENTION`, `RETENTION_TRIM`, `DECODE_LABEL`, `MATCHES_LABEL`, and the statics `ADD_ENTRY`, `ADD_IF_ABSENT`, `ADD_OR_REPLACE`, `SIZE`, `GET`, `GET_ENTRY`, `EVICT`. Writes use `eval_once`, because a retried `insert` would add a second entry. Reads use `eval`. Docs: create `time-series.mdx` (experimental; how it is stored, from owner decision 3; the method table so far). Add a README row `| [`TimeSeries`](https://empatixx.github.io/redissun/docs/objects/time-series) | values by timestamp with labels, TTL and retention | experimental |` after `SortedSet`.
- [ ] **Step 4: Run (CI only):** push. In the `test` job log: `cargo test --lib time_series` and `--test time_series`. Expected: `test result: ok.`, and the count of `tests/time_series.rs` equals the number of tests written.
- [ ] **Step 5: Commit** `feat(time_series): TimeSeries with Redisson's layout, insert, try_insert, set, get, entry, len, clean-up`

### Task 2: Reads and removals by timestamp, head and tail

**Files:**
- Modify: `scripts.rs` (`LIVE_AT_TIMESTAMP`, `GET_ALL`, `GET_ALL_ENTRIES`, `GET_AND_REMOVE`, `GET_AND_REMOVE_ENTRY`, `GET_AND_REMOVE_ALL`, `GET_AND_REMOVE_ALL_ENTRIES`, `FORMAT_TIMESTAMP`, `KEEP_MEMBERS`, `COLLECT_RANGE`, `UNPACK_VALUES`, `UNPACK_ENTRIES`, `UNPACK_TIMESTAMPS`, `REMOVE_COLLECTED`, `COLLECT_VALUES`, `COLLECT_ENTRIES`, `COLLECT_TIMESTAMPS`, `POLL_VALUES`, `POLL_ENTRIES`), `mod.rs`
- Test: `tests/time_series.rs`

**Interfaces:**
- Consumes: Task 1's `decode_entries`, `bounds`.
- Produces: `get_all`, `get_all_entries`, `remove`, `remove_entry`, `remove_all`, `remove_all_entries` (signatures in the names table, `ts: impl Into<Timestamp>`). `first`, `first_entry`, `first_many(n: usize)`, `first_entries(n)`, the `last*` and `pop_*` equivalents, and `first_timestamp`/`last_timestamp -> Result<Option<i64>>`. Private `async fn collect(&self, script, start, end, limit: usize, rev: bool, label: LabelFilter, whole: bool) -> Result<redis::Value>` with `pub(crate) enum LabelFilter { None, Match(Bytes), Absent }`, which Task 3 reuses. `last_*`/`pop_last_*` reverse the reply in Rust, as Redisson's `*_REVERSE` decoders do. `n == 0` gives an empty result without a call.
- [ ] **Step 1: Write failing tests.** Own:
  - `one_timestamp_holds_many_until_set_collapses_it` (Review Focus 1): three `insert(5, ..)` give `get_all(5) == [a, b, c]` and `get(5) == Some(a)`. Then `set(5, "z")` is `false`, and `get_all(5) == ["z"]`.
  - `expired_entries_are_invisible_everywhere` (Review Focus 2): with `insert(1,"x").ttl(50ms)` and `insert(2,"y")`, after 150 ms `len == 1`, `get(1) == None`, `get_all(1) == []`, `first() == Some("y")`, `first_timestamp() == Some(2)`, `last_many(5) == ["y"]`, and `pop_first() == Some("y")` while `ZCARD <n>` is still 1 (the expired one is left for the clean-up). Task 3 adds `range(..)`, `labels(..)` and `read_after(0)`, Task 5 adds `aggregate` and `info().len`, and Task 4 adds `try_insert(1, ..)` is `true`.
  - `head_and_tail_of_nothing`: on a missing name, every `first*`/`last*`/`pop_*` gives `None` or `[]`, and `first_many(0)` gives `[]`.
  - Ports: `Remove`, `GetAndRemoveEntry`, `GetAndRemove`, `test` (`redisson_basic`), `TTLLast`, `TTLFirst`, `PollLastEntries`, `PollFirstEntries`, `Poll`, `PollList`, `PollFirstEntry`, `PollLastEntry`, `LastEntries`, `FirstEntries`, `HeadTailOnEmptyAndFullyExpired`, `ZeroAndNegativeCount` (the `0` asserts; the `-1` ones become `range(..)`), `PollSkipsExpiredWithoutConsumingThem`, `LargeExpiredBacklog`, `GetAllAndRemoveAll` (`remove_all(t).len()` for the count), `GetAndRemoveAll`, `PluralAccessorsSkipExpired`, `SingularAndPluralTogether`, `DuplicateOrderSurvivesRemoval`, `DuplicatesKeepInsertionOrder`, `SameValueAtDifferentTimestamps`, `MixedLabelsAtSameTimestamp`.
- [ ] **Step 2: Implement.** Docs: method rows, plus the "several entries per timestamp" and "expired entries" paragraphs.
- [ ] **Step 3: Run (CI only)** as in Task 1. Expected: `test result: ok.`
- [ ] **Step 4: Commit** `feat(time_series): per-timestamp reads and removals, first, last, pop`

### Task 3: Ranges, label filters, `remove_range`, `labels`, `read_after`, `iter`

**Files:**
- Modify: `scripts.rs` (`REMOVE_RANGE`, `COLLECT_LABELS`, `SCAN`), `builders.rs` (`Range`, `RangeEntries`, `RemoveRange`, `ReadAfter`), `mod.rs`, `src/lib.rs`
- Test: `tests/time_series.rs`

**Interfaces:**
- Consumes: Task 2's `collect`, `LabelFilter`.
- Produces: `pub fn range<'a, T>(&'a self, r: impl RangeBounds<T>) -> Range<'a, V, L, C>` with `.rev()`, `.limit(usize)`, `.label(&'a L2)` and `.unlabelled()`, resolving to `Result<Vec<V>>`. `range_entries` is the same with `Vec<TimeSeriesEntry<V, L>>`. `remove_range(r)` has `.label`/`.unlabelled` → `usize`. `labels(r) -> Result<Vec<L>>`. `read_after(ts)` has `.count(usize)`, `.label` and `.unlabelled` → `Vec<TimeSeriesEntry>`. Unlike Redisson, `.count(0)` means no limit here, because `readTail` with count `0` reads everything (`TimeSeriesReadParams` default) and the cursor needs whole timestamps. This is documented. `iter() -> impl Stream<Item = Result<V>> + '_` (page 10) and `iter_with(page_size: usize)`, paging by rank with `SCAN` until `nextPos == "0"`.
- [ ] **Step 1: Write failing tests.** Own:
  - `range_bounds_at_the_edges` (Review Focus 3): entries at `i64::MIN`, `-1`, `0`, `2^53 + 1`, `i64::MAX`. `range(..)` gives all 5 in order. `range(..0)` gives 2. `range((Excluded(i64::MAX), Unbounded))` is `[]`. `range(5..5)` is `[]`. `range_entries(..=i64::MAX).rev().limit(1)` gives the `i64::MAX` entry, with timestamp `i64::MAX` (Redisson's `formatTimestamp` clamp).
  - `limit_zero_is_empty` (owner decision 9).
  - `label_filters`: `.label(&"cpu")`, `.unlabelled()`, and the last one called wins.
  - `iter_pages_through_everything`: 25 entries with `iter_with(3)` give the 25 values in order.
  - Add `range`, `labels` and `read_after` to `expired_entries_are_invisible_everywhere`.
  - Ports: `Order`, `Iterator`, `RangeReversed`, `Range`, `Label`, `BackfilledOrder`, `ReversedInsertionOrder`, `PollReversedInsertionOrder`, `TTLBackfilledOrder`, `DuplicateTimestampsAtBatchBoundary`, `AddOrReplaceCollapsesDuplicates`, `AddIfAbsentAndAddOrReplaceInSequence`, `AddArgsLabelAndTimeToLive`, `ExpiredDuplicateDoesNotMaskLiveSample`, `DuplicateOrderAcrossAddApis`, `MixedLabelsKeepInsertionOrder`, `EmptyLabelDistinctFromNoLabel`, `MixedLabelsDoNotClobberTimeToLive`, `NegativeAndZeroTimestamps`, `LargeTimestamps`, `RangeLimitAcrossDuplicateTimestamps`, `Retention`, `RetentionMeasuredAgainstHighestTimestamp`, `RetentionIsIndependentOfTimeToLive`, `NoRetentionKeepsEverything`, `RetentionRejectsInsteadOfReporting`, `RetentionAnchorsOnALiveEntry`, `RetentionOfAnUnrepresentableLength` (`.retention(Duration::MAX)`), `EachCallAppliesItsOwnRetention`, `AddArgsIsRefusedOnlyByItsOwnRetention`, `RangeByLabel`, `RangeByLabelSelectsUnlabelledWithNull` (`.unlabelled()`), `EntryRangeByLabel`, `RangeByLabelWithLimit`, `RemoveRangeByLabel`, `Labels`, `LabelsAcrossPages`, `LabelFilterIgnoresExpiredEntries`, `RangeByLabelWithNegativeLimit` (`.limit(0)`), `TimestampsAtTheEdgeOfTheLongRange`, `LabelFilterUnderAnotherCodec` (`StringCodec`), `LabelThatLooksLikeTheAbsentMarker`, `EntryRangeReversedByLabelWithLimit`, `RemoveRangeByLabelSkipsExpiredMatches`, `LabelFilterReadsEntriesWrittenByAnOlderVersion` (Review Focus 4; the Java `EVAL` through `client.script()`, with `redissun__ts_ttl`), `LimitLargerThanTheWindow`, `HeadAndTailAgreeWithTheOtherReadsWhenTheIndexesDisagree`, `RemovingMoreMembersThanOneCallCanCarry`, `ReadTail`, `ReadTailAdvancingACursor`, `ReadTailCountAndLabel`, `ReadTailAtTheEdgeOfTheLongRange`, `ReadTailIgnoresExpiredEntries`, `ReadTailAboveTwoToTheFiftyThree`, `ReadTailNeverEndsInsideATimestamp`.
- [ ] **Step 2: Implement.** Export the builders in `pub mod time_series`. Docs: the range/label/retention section and migration rows.
- [ ] **Step 3: Run (CI only).** Expected: `test result: ok.`
- [ ] **Step 4: Commit** `feat(time_series): ranges with label filters, remove_range, labels, read_after, iter`

### Task 4: `extend`, the comparing policies and `add`

**Files:**
- Modify: `scripts.rs` (`EXTEND`, `EXTEND_TTL`, `FORMAT_NUMBER`, `READ_NUMBER`, `ADD_COMPARING`, `INCREMENT`), `builders.rs` (`Extend`, `SetIfLess`, `SetIfGreater`, `SetSum`, `Add`), `mod.rs` (`numeric: OnceLock<bool>` field and `fn check_numeric(&self, method: &str) -> Result<()>`)
- Test: `tests/time_series.rs`, a test-only codec `F64BeCodec` (encodes every `f64` as 8 big-endian bytes) in the test file

**Interfaces:**
- Consumes: Task 1's `AddArgs`.
- Produces: `extend<I, E>(&self, entries: I) -> Extend<'_, V, L, C>` with `.ttl(d)` → `Result<()>`. An empty input sends nothing. `set_if_less`, `set_if_greater` and `set_sum` have the `Insert` options → `bool`. `add(ts, &delta)` has the same options → `f64`, parsed from the text reply with `str::parse::<f64>`.
- [ ] **Step 1: Write failing tests.** Own:
  - `extend_takes_tuples_and_entries`: `extend(vec![(1, "a".into()), (2, "b".into())])` and `extend([TimeSeriesEntry::from((3, "c".to_string(), "l".to_string()))])` give `len == 3` and `entry(3).label == Some("l")`.
  - `try_insert_over_an_expired_entry_is_true` (Review Focus 2's last assert).
  - `policies_refuse_a_binary_codec_before_sending`: on `with_codec(F64BeCodec)`, `set_sum` is `Err(Error::Config(m))` with `m` containing `"encodes numbers as text"`, and `EXISTS <n>` is 0.
  - Ports: `PutAll`, `AddIfLess`, `AddIfGreater`, `AddAndSum`, `AddPoliciesCollapseDuplicatesToOneEntry`, `AddPoliciesIgnoreExpiredEntries`, `TheComparingPoliciesInSequence`, `AddPoliciesCarryLabelsAndTimeToLive`, `AddPoliciesHonourRetention`, `AddPoliciesRequireANumericCodec`, `AddPoliciesRejectValuesThatAreNotFiniteNumbers` (`Error::Redis` containing `"is not a finite number"`), `AddPoliciesUnderEveryCodecTheMessageRecommends` (`JsonCodec`, `StringCodec`), `ASumIsRenderedAsShortAsItCanBe`, `AddAndGet`, `AddAndGetKeepsTheTotalOutsideTheEntries`, `AddAndGetIgnoresEntriesAddedByOtherMeans`, `AddAndGetRecordsALateIncrementAtTheLastTimestamp`, `AddAndGetFromSeveralThreadsLosesNothing` (8 tokio tasks × 50 `add`), `AddAndGetKeepsItsTotalThroughAnEvictionPass`, `AddAndGetRefusesAnAmountTooSmallToRecord`, `AddAndGetLeavesTheTotalWhereTheHistoryIsWhenItCannotFinish`, `AddAndGetInSequence`, `AddAndGetCarriesLabelsAndTimeToLive`, `AddAndGetRefusesWhatItCannotAdd`, `AddAndGetUnderTheOtherNumericCodecs` (`JsonCodec` with `i64` and `f64` values), `AddAndGetSurvivesTheCollectionBeingRenamedAndCopied` (the rename half; copy is not ported), `DeprecatedAddOverloadsAgreeWithTheArgumentsObject` (`insert` vs `extend` vs `extend(..).ttl`).
- [ ] **Step 2: Implement.** Docs: the policy table (`set`, `set_if_less`, `set_if_greater`, `set_sum`, `add`) and the numeric codec rule.
- [ ] **Step 3: Run (CI only).** Expected: `test result: ok.`
- [ ] **Step 4: Commit** `feat(time_series): extend, set_if_less, set_if_greater, set_sum and add`

### Task 5: `aggregate` and `info`

**Files:**
- Modify: `scripts.rs` (`AGGREGATE`, `CLOSE_LAST_BUCKET`, `AGGREGATE_RANGE`, `INFO`), `builders.rs` (`Aggregate`), new `src/collections/time_series/aggregate.rs` (`Aggregation`, `TimeSeriesBucket`, `decode_buckets`), `mod.rs`, `src/lib.rs`
- Test: unit tests in `aggregate.rs`; `tests/time_series.rs`

**Interfaces:**
- Produces: `aggregate<'a, T>(&'a self, r: impl RangeBounds<T>, bucket: Duration, aggregations: &'a [Aggregation]) -> Aggregate<'a, V, L, C>` with `.align_to(impl Into<Timestamp>)` (default `0`), `.filter_by_value(min: f64, max: f64)`, `.label`, `.unlabelled` → `Result<Vec<TimeSeriesBucket>>`. Bucket ms saturate at `i64::MAX`. `info() -> Result<TimeSeriesInfo>`.
- [ ] **Step 1: Write failing tests.** Unit: `decode_buckets` with stride `n + 1`, where `''` is left out (`[“0”, “2”, “”]` with `[Count, VarianceSample]` gives `get(Count) == Some(2.0)` and `get(VarianceSample) == None`). Integration: add `aggregate(.., [Count])` and `info().len` to `expired_entries_are_invisible_everywhere`. Ports: `Aggregate`, `AggregateReportsOnlyWhatWasAskedFor`, `AggregateSkipsEmptyBuckets`, `AggregateAlignment`, `AggregateNegativeTimestamps`, `AggregateFilterByValue`, `AggregateFilterByLabel`, `AggregateVariance`, `AggregateSampleVarianceOfOneValueIsUndefined`, `AggregateKeepsPrecisionOnLargeCloseValues`, `AggregateIgnoresExpiredEntries`, `AggregateRejectsABinaryCodec` (`F64BeCodec`), `AggregateRejectsAValueThatIsNotAFiniteNumber`, `AggregateReportsAnOverflowJavaCanRead` (reads back `f64::MAX`-sized sums), `AggregateAvgSurvivesASumThatCannotBeRepresented`, `AggregateAtTheBottomOfTheLongRange`, `AggregateBucketMustSurviveConversionToMillis` (`500µs` → `Error::Config`), `AggregateUnderEveryCodecTheMessageRecommends`, `AggregateArgumentValidation` (empty list, zero bucket), `AggregateEmptyRangeAndEmptyCollection`, `Info`, `InfoCountsWhatEvictionHasNotReclaimed`, `InfoReportsTheCollectionTimeToLive`, `InfoAtTheEdgeOfTheLongRange`, `InfoWithATimeToLivePastFourteenDigits`, `InfoOfACollectionThatHasEntirelyExpired`.
- [ ] **Step 2: Implement.** Docs: aggregation and info sections.
- [ ] **Step 3: Run (CI only).** Expected: `test result: ok.`
- [ ] **Step 4: Commit** `feat(time_series): aggregate and info`

### Task 6: Events and Cluster

**Files:**
- Create: `src/collections/time_series/events.rs`
- Modify: `mod.rs`, `src/lib.rs`
- Test: `tests/time_series.rs`

**Interfaces:**
- Produces: `pub async fn events(&self) -> Result<TimeSeriesEvents<C>>` (all four kinds) and `pub async fn events_of(&self, kinds: &[TimeSeriesEventKind]) -> Result<TimeSeriesEvents<C>>`, with aliases `addListener` and `ScoredSortedSetAddListener`/`ScoredSortedSetRemoveListener` on the enum variants. The pattern subscriptions use `core.pubsub()`'s `psubscribe_with_messages`, as `multimap/events.rs` does. `TimeSeriesEvents` has `recv() -> Result<TimeSeriesEvent>` and `into_stream()`.
- [ ] **Step 1: Write failing tests.**
  - `events_report_add_remove_delete_expire`: `CONFIG SET notify-keyspace-events Egzx` through `raw_command`, then `insert` → `Added`, `remove` → `Removed`, `expire(100ms)` then wait → `Expired`, a new `insert` then `del` → `Deleted`. No event comes from writes to another series. At the end, restore the old config.
  - `no_kinds_is_a_config_error`.
  - `every_operation_runs_on_a_cluster` (`Topology::cluster()`, 7.4): the name `ts:{c}` and a plain name `plain` each run `insert` with label and TTL, `try_insert`, `set`, `set_sum`, `add`, `extend`, `get_all`, `range_entries(..).rev()`, `labels`, `aggregate`, `info`, `read_after`, `pop_first`, `remove_range`, `iter`, `expire`, `rename` (to a name in the same slot) and `del`. None of them returns `CROSSSLOT`. The clean-up task removes an entry with a TTL on the cluster.
- [ ] **Step 2: Implement.**
- [ ] **Step 3: Run (CI only).** Expected: `test result: ok.`
- [ ] **Step 4: Commit** `feat(time_series): events; cluster test`

### Task 7: Docs for 5a

**Files:**
- Modify: `website/content/docs/objects/time-series.mdx`, `website/content/docs/guides/redisson-migration.mdx`, `website/content/docs/guides/limitations.mdx`, `README.md`, the header comment of `tests/time_series.rs`
- [ ] **Step 1: The object page** is complete: experimental; requirement (Redis/Valkey 6.2); how it is stored (three keys, the member format, shared with Redisson); several entries per timestamp; TTL vs retention; expired entries and the clean-up; the policies; ranges with label filters; aggregation; events (with the `notify-keyspace-events` letters); `Object`; a runnable example (insert, range, aggregate).
- [ ] **Step 2: Migration guide:** a naming row `| `RTimeSeries` | `TimeSeries` |` and a `## TimeSeries` section. It has one row for every row of the names table in Decisions, plus the notes: `last(n)` is ascending as in Redisson, a limit of `0` gives nothing (owner decision 9), `remove` returns the value (owner decision 4), and timestamps are `i64`.
- [ ] **Step 3: Limitations:** a `TimeSeries` item. The clean-up runs on the runtime that created the handle. Expiry uses each client's clock, as in Redisson. Not ported: `copy`, `renamenx`, `expireAt`, `sizeInMemory`, `destroy`, `TrackingListener`. Without listeners on the server's keyspace events, `events()` hears nothing.
- [ ] **Step 4: Check coverage:** every one of the 73 `javap` methods of `RTimeSeries` (plus `RTimeSeriesAsync`, `RExpirable`) appears in a migration row or in the not-ported line. Every one of the 151 Redisson tests appears in the test file header as ported (with its name) or not ported (with a reason). Not ported: `testCopyOntoANameWhoseCounterSurvived` and `testCopyLeavesTheDestinationAloneWhenItFails` (redissun's `Object` has no `copy`), `testRenamenxOfACollectionWrittenByAnOlderVersion` (no `renamenx` on `Object`), `testAggregateRejectsANullAggregation` (a Rust `Aggregation` cannot be null).
- [ ] **Step 5: Commit** `docs(time_series): object page, migration rows, limitations`

### Task 8: PR 5a text and CI verification

- [ ] **Step 1: Open the PR** `feat: TimeSeries, Redisson's RTimeSeries (#5a)` with this body:

```markdown
## Summary
New experimental `TimeSeries<V, L>`: values by timestamp with optional labels, per-entry TTL and retention, on Redisson 4.8.0's data layout and Lua scripts (`redissun__ts_ttl` / `redissun__ts_seq`), so Redisson and redissun clients share the data. Every method of `RTimeSeries`, with a Rust-shaped API.

## Changelog
### Added
- `TimeSeries` (experimental; `client.time_series::<V, L>(name)`):
  - `insert`, `try_insert`, `set`, `set_if_less`, `set_if_greater`, `set_sum`, `add`, each with `.label`, `.ttl` and `.retention`;
  - `extend` with `.ttl`;
  - `get`, `entry`, `get_all`, `get_all_entries`, `remove`, `remove_entry`, `remove_all`, `remove_all_entries`;
  - `first*`, `last*`, `pop_first*`, `pop_last*`, `first_timestamp`, `last_timestamp`;
  - `range` and `range_entries` with `.rev`, `.limit`, `.label` and `.unlabelled`;
  - `remove_range`, `labels`, `read_after`, `aggregate`, `info`, `len`, `iter`;
  - `events()`;
  - `Object` over its three keys, and the clean-up task.
- `Timestamp` (from `i64`, `i32`, `u32`, `SystemTime`; `TryFrom<u64>`), `TimeSeriesEntry`, `TimeSeriesEvent`.
- Docs: the `TimeSeries` page, migration rows, limitations.

### Behaviour that differs from Redisson on purpose
- Overloads that differ only in what they return are merged: `remove` returns the value, `remove_all` the values.
- A limit or count of `0` returns nothing; "all" is `range(..)`.

### Redis-side deviations from Redisson
- None: every script is Redisson 4.8.0's, verbatim, with the `redissun` prefix in key names.
```

- [ ] **Step 2: Verify on GitHub CI only** (never locally). The PR's checks must be green: `cargo test`, the TLS and moka jobs, clippy, fmt and the docs workflow. The `test` log must show `tests/time_series.rs` with `test result: ok.` and the expected count. Read the log of any failure and fix it in a new commit.
- [ ] **Step 3:** Check that the changelog, `time-series.mdx` and `limitations.mdx` agree on the requirements, the zero-limit rule and the not-ported list.

---

## PR 5b: `TimeSeriesNative` and `TimeSeriesNatives`

### Task 9: `TimeSeriesNative` writes, the Redis 8 check and the 8.10 test server

**Files:**
- Create: `src/collections/time_series_native/mod.rs` (type, `HasKey`, methods), `args.rs` (`CreationOptions`, `creation_options!`, `append_creation`), `reply.rs` (`Sample`, `TsInfo`, parsers), `builders.rs`, `support.rs` (`fn unsupported(e: Error) -> Error`), `website/content/docs/objects/time-series-native.mdx`, `tests/time_series_native.rs`
- Modify: `src/collections/mod.rs`, `src/client/mod.rs`, `src/lib.rs`, `objects/meta.json` (after `"time-series"`), `README.md`, `tests/common/mod.rs` (`pub async fn redis810_url() -> String` on `redis:8.10-alpine`, with env `REDISSUN_TEST_REDIS810_URL`, modelled on `redis8_url`, and `redis810_client()`)
- Test: unit tests in `args.rs`, `reply.rs` and `support.rs`; `tests/time_series_native.rs`

**Interfaces:**
- Produces:
  - `pub struct TimeSeriesNative { key: Key }` (`Clone`, `Debug`, `HasKey`).
  - `create()` → `Create<'_>`, `try_create()` → `TryCreate<'_>` (`bool`), `alter()` → `Alter<'_>`, `add(ts, value: f64)` → `Add<'_>` with `.on_duplicate(DuplicatePolicy)` (`i64`), `add_now(value)`, `add_many(samples) -> Result<Vec<i64>>` (empty input sends nothing), `incr_by(value)` / `decr_by(value)` → `IncrBy<'_>` with `.timestamp(ts)` (`i64`). Each takes the creation options of Decisions.
  - `labels() -> Result<Vec<(String, String)>>`, `info()` → `Info<'_>` with `.debug()` → `TsInfo`. They are needed by this task's tests.
  - `support::unsupported`: `Error::Redis(m)` with `m` containing (case-insensitive) `unknown command` or `unknown redis command` becomes `Error::Unsupported("TimeSeriesNative needs Redis 8.0 or newer, or the RedisTimeSeries module")`. Every call goes through it.
- [ ] **Step 1: Write failing unit tests.**
  - `args.rs`: `creation_options_follow_redissons_order`. All options give `RETENTION 1000 ENCODING COMPRESSED CHUNK_SIZE 128 DUPLICATE_POLICY MAX IGNORE 10 0.5 LABELS a 1 b 2`. `Duration::from_nanos(999_999)` is `Error::Config`, and `ZERO` gives `RETENTION 0`.
  - `reply.rs`: `sample_parses_inf_and_nan`. `TS.INFO` flat pairs give `TsInfo`, unknown fields are ignored, and `firstTimestamp` with `totalSamples 0` is `None`.
  - `support.rs`: the error mapping for both messages; other errors pass through.
- [ ] **Step 2: Write failing integration tests** (helper `native(name)` on `redis810_url()`). The header lists the Redisson tests as in Task 1.
  - `ts_commands_need_redis_8` (Review Focus 5): on `redis_url()` (7.4), `create`, `try_create`, `add`, `add_many`, `get`, `range`, `read`, `info` and `labels` each give `Error::Unsupported(m)` with `m` containing `"8.0"`, and a second `add` gives it again.
  - Ports: `Create`, `CreateWithArgs`, `CreateLabelByLabel`, `CreateTwiceFails`, `CreateIfAbsent`, `CreateIfAbsentOnAKeyOfAnotherType`, `CreateIfAbsentNoArgs`, `Alter`, `AlterKeepsLabelsWhenNotGiven`, `Add`, `AddCurrent`, `AddWithCreationArgs`, `AddOnDuplicate`, `AddCurrentSampleArgs`, `AddIgnoresNearDuplicate`, `IncrementBy`, `IncrementByArgs`, `IncrementByCreatesSeries`, `DecrementBy`, `DecrementByCurrentTimestamp`, `AddAll`, `AddAllEmpty`, `AddAllDoesNotCreateTheSeries` (any error element of the `TS.MADD` reply makes the call `Err(Error::Redis)`), `SubMillisecondRetentionIsRejected`, `Expirable`, `GetLabels`, `GetLabelsOnAnUnlabelledSeries`.
- [ ] **Step 3: Implement.** Docs: create `time-series-native.mdx` (experimental, Redis 8.0+ or the module, 8.6/8.10 for some options, the method table so far) and add a README row after `TimeSeries`.
- [ ] **Step 4: Run (CI only):** push. In the `test` log, `--lib time_series_native` and `--test time_series_native` show `test result: ok.`
- [ ] **Step 5: Commit** `feat(time_series_native): TimeSeriesNative create, alter, add, incr_by; Redis 8 check; 8.10 test server`

### Task 10: `TimeSeriesNative` reads, `read`, rules, `iter`

**Files:**
- Modify: `args.rs` (`RangeOptions`, `append_range`), `builders.rs` (`Get`, `Range`, `Read`, `CreateRule`), `mod.rs`, `reply.rs`
- Test: `tests/time_series_native.rs`

**Interfaces:**
- Consumes: Task 9's `CreationOptions`, `unsupported`, parsers.
- Produces:
  - `pub(crate) struct RangeOptions { latest, filter_by_ts: Vec<i64>, filter_by_value: Option<(f64, f64)>, count: Option<usize>, align: Option<Align>, aggregation: Option<(Duration, Vec<Vec<Aggregator>>)>, bucket_timestamp: Option<BucketTimestamp>, empty: bool }` with `fn append(&self, from: RangeEnd, to: RangeEnd, selection: &[Bytes], out: &mut Vec<Bytes>) -> Result<()>`. It enforces Redisson's three checks: `align` without aggregation, `Align::Start` with an open start, and `Align::End` with an open end, each `Error::Config` with Redisson's message. `RangeEnd::{Earliest, Latest, At(i64)}` comes from `RangeBounds` (`Unbounded` → `-`/`+`).
  - `get()` → `Get<'_>` with `.latest()` → `Option<Sample>`. `first() -> Option<Sample>` (`TS.RANGE key - + COUNT 1`). `len`, `first_timestamp`, `last_timestamp` (one `TS.INFO` each). `remove_range(r) -> u64`. `range(r)` → `Range<'_>` with `.rev()` and the range options → `Vec<Sample>`.
  - `read(from: ReadFrom)` → `Read<'_>` with `.block(timeout, min_count)` and `.max_count(n)`. A timeout under 1 ms is `Error::Config("block timeout must be at least 1ms")`.
  - `create_rule(dest: &str, a: Aggregator, bucket: Duration)` → `CreateRule<'_>` with `.align_timestamp(ts)`. `delete_rule(dest)`.
  - `iter()` / `iter_with(page_size)` page with `TS.RANGE key <next> + COUNT page`: the next page starts at the last timestamp + 1 and stops at a short page (Redisson `window`). A missing key mid-iteration yields `Err`, never a silent end.
- [ ] **Step 1: Write failing tests.** Own:
  - `range_bounds_map_to_dash_and_plus`: `range(..)` sends `- +` (seen through results). `range(1000..3000)` excludes 3000.
  - `blocking_read_does_not_hit_the_command_timeout`: a client with `.timeout(1s)`, and `read(ReadFrom::Next).block(2500ms, 1)` returns `[]` after ≥ 2.4 s.
  - Ports: `RemoveRange`, `GetOnEmptySeries`, `Get`, `GetLatestOnCompaction`, `Range`, `RangeReversed`, `RangeArgsSentinels`, `RangeCount`, `RangeFilterByValue`, `RangeFilterByTimestamp`, `RangeSingleAggregation`, `RangeMultipleAggregations`, `RangeAllAggregators`, `RangeEmptyBuckets`, `RangeAlignAndBucketTimestamp`, `RangeLatestOnCompaction`, `Read`, `ReadSentinelsAndMaxCount`, `ReadBlockingTimesOut`, `ReadBlockingSeesLateSample`, `ReadBlockingOutlastsTheResponseTimeout`, `BlockRejectsATimeoutThatWouldWaitForever`, `BlockingReadOnASeriesNamedBlock`, `CreateRule`, `CreateRuleWithAlignment`, `DeleteRule`, `DebugInfo`, `RetentionDropsOldSamples`, `AlignWithoutAggregationIsRejected`, `AlignNeedsTheBoundItAlignsOn`, `First`, `SizeAndTimestamps` (`None` on an empty series, Decisions), `IteratorPagesAcrossWindows`, `IteratorWhenTheLastWindowIsExactlyFull`, `IteratorOnEmptyAndAbsentSeries`, `IteratorFailsRatherThanTruncatingWhenTheSeriesGoes`, `IteratorIsAViewNotASnapshot`, `Iterable`, `Stream` and `IteratorAsync` (all three over `iter()`), `SamplesAreValueObjects` (`PartialEq`), `Resp3DecodesInfinity` → `redisson_decodes_infinity`, `Resp3DecodesNegativeInfinity` → `redisson_decodes_negative_infinity` (both on RESP2). Not ported: `Resp3ReadsMatchResp2` and `Resp3AggregationAndEmptyBuckets` (owner decision 10), `Async` (every method is async), `EveryBatchResponseMatchesItsMethod` and `InBatch` (batch views are a spec non-goal).
- [ ] **Step 2: Implement.** Docs: method rows, the range options and `read`.
- [ ] **Step 3: Run (CI only).** Expected: `test result: ok.`
- [ ] **Step 4: Commit** `feat(time_series_native): get, first, range with aggregation, read, rules, iter`

### Task 11: `TimeSeriesNatives`

**Files:**
- Create: `src/collections/time_series_native/natives.rs` (type, `Filter`, builders), `tests/time_series_natives.rs`
- Modify: `reply.rs` (`SeriesSample`, `SeriesSamples`), `src/client/mod.rs`, `src/lib.rs`
- Test: unit tests in `natives.rs`; `tests/time_series_natives.rs`

**Interfaces:**
- Consumes: Task 10's `RangeOptions`, `Sample`, `unsupported`.
- Produces: `pub struct TimeSeriesNatives { core: Arc<Core> }` and the table in Decisions. Keyless commands go through `core.masters().await?` → the first address → `core.query_master`. `grouped_range` is routed by its first key and gives `Error::Config("At least one key is required")` on no keys and `"Aggregations must hold one entry per key: …"` on a mismatch. `range` returns `Error::Config` for `group_by` with `exclude_empty`, for `group_by` with more than one aggregator, for `with_labels` with `selected_labels`, and for `selected_labels(&[])` (Redisson's four messages). `add_many` uses `Core::query_by_slot`, one `TS.MADD` per slot, and returns results in input order.
- [ ] **Step 1: Write failing tests.** Unit: `filter_expressions`. `Filter::new().eq("a","1").ne("b","2").eq_any("c",["x","y"]).ne_any("d",["z"]).has("e").missing("f")` gives `["a=1","b!=2","c=(x,y)","d!=(z)","e!=","f="]`. An empty filter is an error. Integration (setup as Redisson's `setUp`: `cpu:1`, `cpu:2` with labels and three samples each, unique names per test):
  - `natives_need_redis_8` (Review Focus 5) on 7.4.
  - Ports: `AddAll`, `AddAllEmpty`, `AddAllDoesNotCreateSeries`, `GetAll`, `GetAllWithLabels`, `GetAllSelectedLabels`, `GetAllRequiresFilter`, `GetAllFilterExpressions`, `Range`, `RangeReversed`, `RangeBoundsAndCount`, `RangeFilters`, `RangeAggregation`, `RangeGroupBy`, `RangeAllReducers`, `RangeExcludeEmpty`, `GroupByAndExcludeEmptyAreExclusive`, `GroupedRange`, `GroupedRangeReversed`, `GroupedRangeFillsMissingWithNaN` → `redisson_grouped_range_fills_missing_with_nan`, `GroupedRangeSameAggregationForEveryKey`, `GroupedRangePerKeyAggregations`, `GroupedRangeRejectsMismatchedAggregations`, `GroupedRangeRejectsNoKeys`, `QueryIndex`, `QueryIndexRequiresFilter`, `LabelNames`, `LabelValues`, `GroupByWithSeveralAggregatorsIsExclusive`, `WithLabelsAndSelectedLabelsAreExclusive`, `SelectedLabelsRequiresALabel`, `AlignNeedsTheBoundItAlignsOn`, `InClusterFansOutAndSplitsBySlot` (`Topology::cluster_on("redis:8.10-alpine")`, polling up to 5 s). Not ported: `Resp3ReadsMatchResp2`, `Resp3SelectedLabelsKeepsAbsentAsNull`, `Resp3GroupByLabelsMatchResp2`, `Resp3AggregatorsEntryIsNotReportedAsALabel` (owner decision 10), `Async`.
  - Because the label index is server-wide, each test filters on a label value unique to the test (`unique("area")`), so tests in parallel cannot see each other's series.
- [ ] **Step 2: Implement.** Docs: a `TimeSeriesNatives` section on `time-series-native.mdx`.
- [ ] **Step 3: Run (CI only).** Expected: `test result: ok.` for `tests/time_series_natives.rs`.
- [ ] **Step 4: Commit** `feat(time_series_native): TimeSeriesNatives with filters, mget, mrange, nrange, queryindex, querylabels`

### Task 12: Cluster for `TimeSeriesNative`

**Files:**
- Modify: `tests/time_series_native.rs`
- [ ] **Step 1: Write the test** `every_native_operation_runs_on_a_cluster` (`Topology::cluster_on("redis:8.10-alpine")`): on `{n}:t`, run `create` with labels, `try_create`, `add`, `add_many`, `incr_by`, `range` with aggregation, a blocking `read(Next).block(300ms, 1)` (routed to the key's master), `create_rule` to `{n}:t2`, `info`, `iter`, `expire` and `del`. None of them returns `CROSSSLOT` or `MOVED`.
- [ ] **Step 2: Run (CI only).** Expected: `test result: ok.`
- [ ] **Step 3: Commit** `test(time_series_native): cluster`

### Task 13: Docs for 5b

**Files:**
- Modify: `time-series-native.mdx`, `guides/redisson-migration.mdx`, `guides/limitations.mdx`, `README.md` (a `TimeSeriesNatives` mention in the `TimeSeriesNative` row), the test file headers
- [ ] **Step 1: Page:** experimental; requirements per feature (8.0 / 8.6 / 8.10); `f64` values without a codec; every builder option with its `TS.*` keyword; the filter syntax; the cluster note (filter commands go to one node, whose module answers for the cluster); `read` blocking; a runnable example.
- [ ] **Step 2: Migration:** naming rows `RTimeSeriesNative` → `TimeSeriesNative` and `RTimeSeriesNatives` → `TimeSeriesNatives`, a `## TimeSeriesNative` table (every row of the two tables in Decisions, with `TSCreateArgs`/`TSAddArgs`/`TSRangeArgs`/`TSReadArgs`/`TSRuleArgs`/`TSMultiGetArgs`/`TSMultiRangeArgs`/`TSGroupedRangeArgs` → builder options), and the enum mapping (`TSAggregation`, `TSReducer`, `TSDuplicatePolicy`, `TSEncoding`, `TSBucketTimestamp`).
- [ ] **Step 3: Limitations:** `TimeSeriesNative` is not in `Batch` (spec non-goal). `add_many` is not atomic: an error on one sample fails the call, but other samples may already be stored, as `TS.MADD` does. `TS.MGET`/`TS.MRANGE` go to one master.
- [ ] **Step 4: Check coverage** against `javap` (`RTimeSeriesNative` 35 and `RTimeSeriesNatives` 9 methods, plus the `Async` interfaces) and against the 75 + 38 Redisson tests, as in Task 7.
- [ ] **Step 5: Commit** `docs(time_series_native): object page, migration rows, limitations`

### Task 14: PR 5b text and final verification (GitHub CI only)

- [ ] **Step 1: Open the PR** `feat: TimeSeriesNative and TimeSeriesNatives, Redisson's RTimeSeriesNative(s) (#5b)`:

```markdown
## Summary
New experimental `TimeSeriesNative` (one Redis 8 time series, `TS.*`) and `TimeSeriesNatives` (commands over many series by label filter), with every method of Redisson 4.8.0's `RTimeSeriesNative` and `RTimeSeriesNatives`, sending the same commands and arguments.

## Changelog
### Added
- `TimeSeriesNative` (experimental, Redis 8.0+ or the RedisTimeSeries module; `client.time_series_native(name)`):
  - `create`, `try_create`, `alter`, each with retention, encoding, chunk size, duplicate policy, ignore and labels;
  - `add`, `add_now`, `add_many`, `incr_by`, `decr_by`;
  - `get` with `.latest`, `first`, `len`, `first_timestamp`, `last_timestamp`;
  - `range` with `.rev`, filters, count, aggregation, alignment, bucket timestamp and empty buckets;
  - `read` with `.block` and `.max_count`;
  - `remove_range`, `create_rule`, `delete_rule`, `labels`, `info` with `.debug`, `iter`, `Object`.
- `TimeSeriesNatives` (`client.time_series_natives()`): `add_many`, `get`, `range` (with labels, `group_by`, `exclude_empty`), `grouped_range`, `query_index`, `label_names`, `label_values`, and the `Filter` builder.
- Docs: the `TimeSeriesNative` page, migration rows, limitations.

### Behaviour that differs from Redisson on purpose
- `first_timestamp`/`last_timestamp` return `None` for an empty series; Redisson returns `0`.
- A missing time series module is `Error::Unsupported`, checked on every call (never cached).
```

- [ ] **Step 2: Final verification, on GitHub CI only** (no local cargo, docker or builds). Every check of PR 5b must be green: the `test` job (with `tests/time_series_native.rs` and `tests/time_series_natives.rs` showing `test result: ok.`), the TLS and moka jobs, clippy, fmt and docs. PR 5a's `tests/time_series.rs` must still pass on the same run. Read each failure in the CI log, fix it in a new commit and push again. Wait for green before asking for review.
- [ ] **Step 3:** Check that the changelog, `time-series-native.mdx` and `limitations.mdx` agree on the server requirements and the not-ported list.
