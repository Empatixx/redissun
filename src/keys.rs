//! Commands on keys by name, across every master: Redisson's `RKeys`.

use crate::client::core::Core;
use crate::client::request::Request;
use crate::codec::StringCodec;
use crate::error::{Error, Result};
use crate::messaging::topic::{PatternSubscriber, PatternTopic};
use crate::net::stream::Address;
use crate::object::{millis, Key};
use futures::{stream, Stream, StreamExt, TryStreamExt};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// How many keys `delete_by_pattern` reads with each `SCAN` and deletes with each command, as in Redisson.
const ERASE_CHUNK: usize = 500;

/// How many keys a [`KeysScan`] with a pattern asks each `SCAN` for, Redisson's default for `getKeysByPattern`.
const PATTERN_CHUNK: usize = 10;

/// The type of the value stored under a key, from `TYPE`: Redisson's `RType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum KeyType {
    /// `string`: [`Bucket`](crate::Bucket), [`AtomicI64`](crate::AtomicI64), [`BitSet`](crate::BitSet) and others. Redisson's `OBJECT`.
    String,
    /// `hash`: [`HashMap`](crate::HashMap) and others. Redisson's `MAP`.
    Hash,
    /// `list`: [`Vec`](crate::Vec) and [`VecDeque`](crate::VecDeque).
    List,
    /// `set`: [`HashSet`](crate::HashSet).
    Set,
    /// `zset`: [`SortedSet`](crate::SortedSet), [`Geo`](crate::Geo) and others. Redisson's `ZSET`.
    SortedSet,
    /// `stream`: [`Stream`](crate::Stream).
    Stream,
    /// `gcra`: the rate limiter type of Redis 8.
    Gcra,
    /// `ReJSON-RL`: a RedisJSON document.
    Json,
}

impl KeyType {
    /// The name `TYPE` gives and `SCAN … TYPE` takes.
    pub fn as_str(self) -> &'static str {
        match self {
            KeyType::String => "string",
            KeyType::Hash => "hash",
            KeyType::List => "list",
            KeyType::Set => "set",
            KeyType::SortedSet => "zset",
            KeyType::Stream => "stream",
            KeyType::Gcra => "gcra",
            KeyType::Json => "ReJSON-RL",
        }
    }

    /// The type `TYPE` named, `None` for a missing key (`none`). A type Redisson does not know is an error, as in Redisson.
    fn parse(name: &str) -> Result<Option<Self>> {
        let parsed = match name {
            "none" => return Ok(None),
            "string" => KeyType::String,
            "hash" => KeyType::Hash,
            "list" => KeyType::List,
            "set" => KeyType::Set,
            "zset" => KeyType::SortedSet,
            "stream" => KeyType::Stream,
            "gcra" => KeyType::Gcra,
            "ReJSON-RL" => KeyType::Json,
            other => return Err(Error::Redis(format!("can't recognize redis type: {other}"))),
        };
        Ok(Some(parsed))
    }
}

/// What [`Keys::iter_with`] reads: Redisson's `KeysScanOptions`. By default every key, with the server's page size and no limit.
#[derive(Clone, Debug, Default)]
pub struct KeysScan {
    pattern: Option<String>,
    limit: usize,
    chunk_size: Option<usize>,
    key_type: Option<KeyType>,
}

impl KeysScan {
    /// Every key, as [`Default`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Only the keys that match the glob-style `pattern` (`SCAN … MATCH`), Redisson's `getKeysByPattern`.
    /// Unless [`chunk_size`](KeysScan::chunk_size) says otherwise, each `SCAN` then asks for 10 keys, Redisson's default for pattern scans.
    pub fn pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }

    /// At most `limit` keys in all; zero, the default, is no limit.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// How many keys each `SCAN` asks a master for (`COUNT`); zero leaves it to the server. The default is 10 with a [`pattern`](KeysScan::pattern) and the server's page size without one.
    pub fn chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = Some(chunk_size);
        self
    }

    /// Only the keys whose value is of `key_type` (`SCAN … TYPE`).
    pub fn key_type(mut self, key_type: KeyType) -> Self {
        self.key_type = Some(key_type);
        self
    }
}

/// The key events [`Keys::listen`] reports, Redisson's `ObjectListener` kinds. Redis sends them only when its `notify-keyspace-events` setting enables them, as for Redisson.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum KeyEvent {
    /// A key was created (`new`, Redis 7.0 or newer, enabled by `n`), Redisson's `NewObjectListener`.
    New,
    /// A string was set (`set`, enabled by `$` with `E`), Redisson's `SetObjectListener`.
    Set,
    /// A key expired (`expired`, enabled by `x` with `E`), Redisson's `ExpiredObjectListener`.
    Expired,
    /// A key was deleted (`del`, enabled by `g` with `E`), Redisson's `DeletedObjectListener`.
    Deleted,
}

impl KeyEvent {
    fn pattern(self) -> &'static str {
        match self {
            KeyEvent::New => "__keyevent@*:new",
            KeyEvent::Set => "__keyevent@*:set",
            KeyEvent::Expired => "__keyevent@*:expired",
            KeyEvent::Deleted => "__keyevent@*:del",
        }
    }
}

/// The names of the keys a [`KeyEvent`] happened to, from [`Keys::listen`]. Dropping it stops listening, Redisson's `removeListener`.
pub struct KeyEvents {
    subscriber: PatternSubscriber<String, StringCodec>,
}

impl fmt::Debug for KeyEvents {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyEvents").finish_non_exhaustive()
    }
}

impl KeyEvents {
    /// Waits for the next event and returns the name of its key. Falling behind gives [`Error::Lagged`] as for a [`Subscriber`](crate::Subscriber).
    pub async fn recv(&mut self) -> Result<String> {
        let (_channel, key) = self.subscriber.recv().await?;
        Ok(key)
    }

    /// Turns the listener into a stream of key names. The stream never ends.
    pub fn into_stream(self) -> impl Stream<Item = Result<String>> {
        stream::unfold(self, |mut events| async move {
            Some((events.recv().await, events))
        })
    }
}

/// What [`Keys::migrate`] moves and where, Redisson's `MigrateArgs`.
#[derive(Clone)]
pub struct Migrate {
    keys: std::vec::Vec<String>,
    host: String,
    port: u16,
    database: i64,
    timeout: Duration,
    copy: bool,
    replace: bool,
    username: Option<String>,
    password: Option<String>,
}

impl fmt::Debug for Migrate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Migrate")
            .field("keys", &self.keys)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("timeout", &self.timeout)
            .field("copy", &self.copy)
            .field("replace", &self.replace)
            .finish_non_exhaustive()
    }
}

impl Migrate {
    /// Moves `keys` to `database` of the server at `host:port`, waiting at most `timeout` for it. The keys must share one hash slot in a cluster.
    pub fn new<K: Into<String>>(
        keys: impl IntoIterator<Item = K>,
        host: impl Into<String>,
        port: u16,
        database: i64,
        timeout: Duration,
    ) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
            host: host.into(),
            port,
            database,
            timeout,
            copy: false,
            replace: false,
            username: None,
            password: None,
        }
    }

    /// Keeps the keys here too (`COPY`), Redisson's `MigrateMode.COPY` and `RKeys.copy`.
    pub fn copy(mut self) -> Self {
        self.copy = true;
        self
    }

    /// Overwrites keys that exist on the other server (`REPLACE`), Redisson's `MigrateMode.REPLACE`.
    pub fn replace(mut self) -> Self {
        self.replace = true;
        self
    }

    /// Logs in to the other server as `username` (`AUTH2`); it needs a [`password`](Migrate::password).
    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self
    }

    /// Logs in to the other server with `password` (`AUTH`, or `AUTH2` with a [`username`](Migrate::username)).
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    fn command(&self) -> Result<redis::Cmd> {
        if self.keys.is_empty() {
            return Err(Error::Config("migrate needs at least one key".into()));
        }
        let mut cmd = redis::cmd("MIGRATE");
        cmd.arg(&self.host)
            .arg(self.port)
            .arg("")
            .arg(self.database)
            .arg(millis_or_zero(self.timeout)?);
        if self.copy {
            cmd.arg("COPY");
        }
        if self.replace {
            cmd.arg("REPLACE");
        }
        let password = self.password.as_deref().unwrap_or_default();
        match self.username.as_deref().filter(|name| !name.is_empty()) {
            Some(username) => {
                cmd.arg("AUTH2").arg(username).arg(password);
            }
            None if !password.is_empty() => {
                cmd.arg("AUTH").arg(password);
            }
            None => {}
        }
        cmd.arg("KEYS").arg(&self.keys);
        Ok(cmd)
    }
}

fn millis_or_zero(duration: Duration) -> Result<i64> {
    if duration.is_zero() {
        return Ok(0);
    }
    millis(duration)
}

fn epoch_millis(time: SystemTime) -> Result<i64> {
    let since = time
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::Config("time is before 1970".into()))?;
    i64::try_from(since.as_millis()).map_err(|_| Error::Config("time is too far away".into()))
}

/// Commands on keys by name, from [`Client::keys`](crate::Client::keys): Redisson's `RKeys`.
///
/// Calls that take many names send one command for all of them, or in a cluster one for each hash slot, as Redisson does. Calls that read or erase every key, such as [`iter`](Keys::iter), [`count`](Keys::count) and [`delete_by_pattern`](Keys::delete_by_pattern), go to every master.
///
/// ```no_run
/// # async fn run(client: redissun::Client) -> redissun::Result<()> {
/// use futures::TryStreamExt;
/// use redissun::KeysScan;
///
/// let keys = client.keys();
/// let sessions: Vec<String> = keys.iter_with(KeysScan::new().pattern("session:*")).try_collect().await?;
/// let deleted = keys.delete_by_pattern("cache:*").await?;
/// println!("{} sessions, {deleted} cache entries deleted", sessions.len());
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Keys {
    core: Arc<Core>,
}

impl fmt::Debug for Keys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Keys").finish_non_exhaustive()
    }
}

impl Keys {
    pub(crate) fn new(core: Arc<Core>) -> Self {
        Self { core }
    }

    fn key_cmd(name: &str, key: &str) -> redis::Cmd {
        let mut cmd = redis::cmd(name);
        cmd.arg(key);
        cmd
    }

    async fn on_key<T: redis::FromRedisValue>(&self, key: &str, cmd: redis::Cmd) -> Result<T> {
        self.core.query(Request::new(cmd).key(key.as_bytes())).await
    }

    async fn read_key<T: redis::FromRedisValue>(&self, key: &str, cmd: redis::Cmd) -> Result<T> {
        self.core
            .query(Request::new(cmd).key(key.as_bytes()).read())
            .await
    }

    /// The type of the value under `name`, `None` when there is no such key (`TYPE`), Redisson's `getType`.
    pub async fn key_type(&self, name: &str) -> Result<Option<KeyType>> {
        let reply: String = self.read_key(name, Self::key_cmd("TYPE", name)).await?;
        KeyType::parse(&reply)
    }

    /// The hash slot of `name`, Redisson's `getSlot`. It is computed here, so it needs no round trip and works without a cluster too.
    pub fn slot(&self, name: &str) -> u16 {
        crate::client::slot::keyslot(name.as_bytes())
    }

    /// Streams every key of every master, Redisson's `getKeys`.
    /// A key that exists the whole time is returned at least once, and may be returned twice.
    pub fn iter(&self) -> impl Stream<Item = Result<String>> + Send + 'static {
        self.iter_with(KeysScan::new())
    }

    /// Streams the keys `scan` asks for, one master after the other, Redisson's `getKeys(KeysScanOptions)`.
    /// For Redisson's `getKeysByPattern`, pass `KeysScan::new().pattern(p)`: it reads 10 keys at a time from each master, as Redisson does.
    #[doc(alias = "getKeysByPattern")]
    pub fn iter_with(&self, scan: KeysScan) -> impl Stream<Item = Result<String>> + Send + 'static {
        let core = self.core.clone();
        let limit = scan.limit;
        let scan = Arc::new(scan);
        let keys = stream::once(async move {
            let masters = core.masters().await?;
            let per_master = masters
                .into_iter()
                .map(move |master| scan_master(core.clone(), master, scan.clone()));
            Ok::<_, Error>(stream::iter(per_master).flatten())
        })
        .try_flatten();
        let limited: std::pin::Pin<Box<dyn Stream<Item = Result<String>> + Send>> = if limit > 0 {
            Box::pin(keys.take(limit))
        } else {
            Box::pin(keys)
        };
        limited
    }

    /// A random key, `None` when every master is empty (`RANDOMKEY`), Redisson's `randomKey`. The masters are asked in random order until one has a key.
    pub async fn random_key(&self) -> Result<Option<String>> {
        let mut masters = self.core.masters().await?;
        let start = (uuid::Uuid::new_v4().as_u128() % masters.len() as u128) as usize;
        masters.rotate_left(start);
        for master in &masters {
            let reply = self
                .core
                .query_master(master, &redis::cmd("RANDOMKEY"))
                .await?;
            let key: Option<String> = redis::from_redis_value(reply)?;
            if key.is_some() {
                return Ok(key);
            }
        }
        Ok(None)
    }

    async fn sum_by_slot<K: AsRef<str>>(
        &self,
        command: &str,
        names: &[K],
        read: bool,
    ) -> Result<u64> {
        if names.is_empty() {
            return Ok(0);
        }
        let keys: std::vec::Vec<&[u8]> =
            names.iter().map(|name| name.as_ref().as_bytes()).collect();
        let replies = self
            .core
            .query_by_slot(&keys, read, |group| {
                let mut cmd = redis::cmd(command);
                for &index in group {
                    cmd.arg(keys[index]);
                }
                cmd
            })
            .await?;
        replies.into_iter().try_fold(0u64, |sum, (_, reply)| {
            let count: u64 = redis::from_redis_value(reply)?;
            Ok(sum + count)
        })
    }

    /// Deletes the keys; returns how many existed (`DEL`), Redisson's `delete`. To delete an object, pass its [`name`](crate::Object::name).
    pub async fn delete<K: AsRef<str>>(&self, names: &[K]) -> Result<u64> {
        self.sum_by_slot("DEL", names, false).await
    }

    /// Like [`delete`](Keys::delete), freeing the memory in the background (`UNLINK`), Redisson's `unlink`.
    pub async fn unlink<K: AsRef<str>>(&self, names: &[K]) -> Result<u64> {
        self.sum_by_slot("UNLINK", names, false).await
    }

    /// Marks the keys as used now; returns how many exist (`TOUCH`), Redisson's `touch`.
    pub async fn touch<K: AsRef<str>>(&self, names: &[K]) -> Result<u64> {
        self.sum_by_slot("TOUCH", names, false).await
    }

    /// How many of the keys exist, a name given twice counting twice (`EXISTS`), Redisson's `countExists`.
    pub async fn count_exists<K: AsRef<str>>(&self, names: &[K]) -> Result<u64> {
        self.sum_by_slot("EXISTS", names, true).await
    }

    /// Deletes every key that matches the glob-style `pattern` on every master; returns how many were deleted, Redisson's `deleteByPattern`.
    /// Each master is read with `SCAN` and its keys are deleted 500 at a time, so this is not atomic. When a master fails after others deleted keys, the error says how many were deleted.
    pub async fn delete_by_pattern(&self, pattern: &str) -> Result<u64> {
        self.erase_by_pattern("DEL", pattern).await
    }

    /// Like [`delete_by_pattern`](Keys::delete_by_pattern) with `UNLINK`, Redisson's `unlinkByPattern`.
    pub async fn unlink_by_pattern(&self, pattern: &str) -> Result<u64> {
        self.erase_by_pattern("UNLINK", pattern).await
    }

    async fn erase_by_pattern(&self, command: &'static str, pattern: &str) -> Result<u64> {
        let masters = self.core.masters().await?;
        let scan = Arc::new(KeysScan::new().pattern(pattern).chunk_size(ERASE_CHUNK));
        let erased = futures::future::join_all(masters.into_iter().map(|master| {
            let scan = scan.clone();
            async move {
                let mut count = 0u64;
                let mut chunks = std::pin::pin!(
                    scan_master(self.core.clone(), master, scan).try_chunks(ERASE_CHUNK)
                );
                while let Some(chunk) = chunks.next().await {
                    let chunk = match chunk {
                        Ok(chunk) => chunk,
                        Err(error) => return (count, Some(error.1)),
                    };
                    match self.sum_by_slot(command, &chunk, false).await {
                        Ok(erased) => count += erased,
                        Err(error) => return (count, Some(error)),
                    }
                }
                (count, None)
            }
        }))
        .await;
        let total = erased.iter().map(|(count, _)| count).sum::<u64>();
        match erased.into_iter().find_map(|(_, error)| error) {
            None => Ok(total),
            Some(error) if total > 0 => Err(Error::Redis(format!(
                "{total} keys have been deleted. But one or more nodes has an error: {error}"
            ))),
            Some(error) => Err(error),
        }
    }

    /// How many keys there are on all the masters together (`DBSIZE`), Redisson's `count`.
    pub async fn count(&self) -> Result<u64> {
        let replies = self.core.query_masters(&redis::cmd("DBSIZE")).await?;
        replies.into_iter().try_fold(0u64, |sum, reply| {
            let count: u64 = redis::from_redis_value(reply)?;
            Ok(sum + count)
        })
    }

    /// The clock of a server (`TIME`), Redisson's `time`.
    pub async fn time(&self) -> Result<SystemTime> {
        let reply = self
            .core
            .query_value(Request::new(redis::cmd("TIME")))
            .await?;
        crate::nodes::time(reply)
    }

    async fn on_every_master(&self, command: &str, asynchronous: bool) -> Result<()> {
        let mut cmd = redis::cmd(command);
        if asynchronous {
            cmd.arg("ASYNC");
        }
        self.core.query_masters(&cmd).await?;
        Ok(())
    }

    /// Deletes every key of the client's database on every master (`FLUSHDB`), Redisson's `flushdb`.
    pub async fn flush_db(&self) -> Result<()> {
        self.on_every_master("FLUSHDB", false).await
    }

    /// Like [`flush_db`](Keys::flush_db), with each server freeing the memory in the background (`FLUSHDB ASYNC`), Redisson's `flushdbParallel`.
    pub async fn flush_db_parallel(&self) -> Result<()> {
        self.on_every_master("FLUSHDB", true).await
    }

    /// Deletes every key of every database on every master (`FLUSHALL`), Redisson's `flushall`.
    pub async fn flush_all(&self) -> Result<()> {
        self.on_every_master("FLUSHALL", false).await
    }

    /// Like [`flush_all`](Keys::flush_all), with each server freeing the memory in the background (`FLUSHALL ASYNC`), Redisson's `flushallParallel`.
    pub async fn flush_all_parallel(&self) -> Result<()> {
        self.on_every_master("FLUSHALL", true).await
    }

    /// The time `name` has left to live, `None` when it has no expiry or does not exist (`PTTL`), Redisson's `remainTimeToLive`.
    pub async fn ttl(&self, name: &str) -> Result<Option<Duration>> {
        let left: i64 = self.read_key(name, Self::key_cmd("PTTL", name)).await?;
        Ok(u64::try_from(left).ok().map(Duration::from_millis))
    }

    /// Renames `name` to `new_name`, replacing a key of that name (`RENAME`), Redisson's `rename`. In a cluster both must share one hash slot.
    pub async fn rename(&self, name: &str, new_name: &str) -> Result<()> {
        let mut cmd = Self::key_cmd("RENAME", name);
        cmd.arg(new_name);
        let () = self.on_key(name, cmd).await?;
        Ok(())
    }

    /// Renames `name` to `new_name` only when no key has that name; returns whether it did (`RENAMENX`), Redisson's `renamenx`.
    #[doc(alias = "renamenx")]
    pub async fn try_rename(&self, name: &str, new_name: &str) -> Result<bool> {
        let mut cmd = Self::key_cmd("RENAMENX", name);
        cmd.arg(new_name);
        self.on_key(name, cmd).await
    }

    /// Removes the expiry of `name`; returns whether it had one (`PERSIST`), Redisson's `clearExpire`.
    pub async fn persist(&self, name: &str) -> Result<bool> {
        self.on_key(name, Self::key_cmd("PERSIST", name)).await
    }

    /// Lets `name` expire after `ttl`; returns whether the key exists (`PEXPIRE`), Redisson's `expire`.
    pub async fn expire(&self, name: &str, ttl: Duration) -> Result<bool> {
        let mut cmd = Self::key_cmd("PEXPIRE", name);
        cmd.arg(millis(ttl)?);
        self.on_key(name, cmd).await
    }

    /// Lets `name` expire at `time`; returns whether the key exists (`PEXPIREAT`), Redisson's `expireAt`.
    pub async fn expire_at(&self, name: &str, time: SystemTime) -> Result<bool> {
        let mut cmd = Self::key_cmd("PEXPIREAT", name);
        cmd.arg(epoch_millis(time)?);
        self.on_key(name, cmd).await
    }

    async fn expire_each<K: AsRef<str>>(
        &self,
        command: &str,
        names: &[K],
        argument: i64,
    ) -> Result<u64> {
        let applied = futures::future::try_join_all(names.iter().map(|name| {
            let name = name.as_ref();
            let mut cmd = Self::key_cmd(command, name);
            cmd.arg(argument);
            self.on_key::<bool>(name, cmd)
        }))
        .await?;
        Ok(applied.into_iter().filter(|applied| *applied).count() as u64)
    }

    /// Lets each of the keys expire after `ttl`; returns how many exist, Redisson's `expire(Duration, String...)`.
    pub async fn expire_many<K: AsRef<str>>(&self, names: &[K], ttl: Duration) -> Result<u64> {
        self.expire_each("PEXPIRE", names, millis(ttl)?).await
    }

    /// Lets each of the keys expire at `time`; returns how many exist, Redisson's `expireAt(Instant, String...)`.
    pub async fn expire_at_many<K: AsRef<str>>(
        &self,
        names: &[K],
        time: SystemTime,
    ) -> Result<u64> {
        self.expire_each("PEXPIREAT", names, epoch_millis(time)?)
            .await
    }

    /// Moves keys to another Redis server (`MIGRATE`), Redisson's `migrate` and `copy`.
    pub async fn migrate(&self, migrate: Migrate) -> Result<()> {
        let cmd = migrate.command()?;
        let first = migrate.keys.first().map(String::as_str).unwrap_or_default();
        let _: redis::Value = self.on_key(first, cmd).await?;
        Ok(())
    }

    /// Moves `name` to `database` of the same server; returns whether it moved (`MOVE`), Redisson's `move`. A cluster has only database 0.
    pub async fn move_to_db(&self, name: &str, database: i64) -> Result<bool> {
        let mut cmd = Self::key_cmd("MOVE", name);
        cmd.arg(database);
        self.on_key(name, cmd).await
    }

    /// Swaps two databases of the server (`SWAPDB`), Redisson's `swapdb`. A cluster has only database 0.
    pub async fn swap_db(&self, first: i64, second: i64) -> Result<()> {
        let mut cmd = redis::cmd("SWAPDB");
        cmd.arg(first).arg(second);
        let () = self.core.query(Request::new(cmd)).await?;
        Ok(())
    }

    /// Starts receiving the names of the keys that `event` happens to, in every database, Redisson's `addListener`. Drop the [`KeyEvents`] to stop, Redisson's `removeListener`.
    pub async fn listen(&self, event: KeyEvent) -> Result<KeyEvents> {
        let topic: PatternTopic<String, StringCodec> =
            PatternTopic::new(Key::new(self.core.clone(), event.pattern()), StringCodec);
        Ok(KeyEvents {
            subscriber: topic.subscribe().await?,
        })
    }
}

/// The keys `scan` asks for on one master, read with `SCAN` until its cursor comes back to 0.
fn scan_master(
    core: Arc<Core>,
    master: Address,
    scan: Arc<KeysScan>,
) -> impl Stream<Item = Result<String>> + Send + 'static {
    stream::try_unfold(Some("0".to_string()), move |cursor| {
        let core = core.clone();
        let master = master.clone();
        let scan = scan.clone();
        async move {
            let Some(cursor) = cursor else {
                return Ok::<_, Error>(None);
            };
            let mut cmd = redis::cmd("SCAN");
            cmd.arg(&cursor);
            if let Some(pattern) = &scan.pattern {
                cmd.arg("MATCH").arg(pattern);
            }
            let chunk_size = scan.chunk_size.unwrap_or(if scan.pattern.is_some() {
                PATTERN_CHUNK
            } else {
                0
            });
            if chunk_size > 0 {
                cmd.arg("COUNT").arg(chunk_size);
            }
            if let Some(key_type) = scan.key_type {
                cmd.arg("TYPE").arg(key_type.as_str());
            }
            let reply = core.query_master(&master, &cmd).await?;
            let (next, page): (String, std::vec::Vec<String>) = redis::from_redis_value(reply)?;
            let next = (next != "0").then_some(next);
            Ok(Some((stream::iter(page.into_iter().map(Ok)), next)))
        }
    })
    .try_flatten()
}

#[cfg(test)]
mod tests {
    use super::{KeyType, Migrate};
    use std::time::Duration;

    #[test]
    fn every_type_redisson_knows_reads_back_from_its_name() {
        for key_type in [
            KeyType::String,
            KeyType::Hash,
            KeyType::List,
            KeyType::Set,
            KeyType::SortedSet,
            KeyType::Stream,
            KeyType::Gcra,
            KeyType::Json,
        ] {
            assert_eq!(KeyType::parse(key_type.as_str()).unwrap(), Some(key_type));
        }
        assert_eq!(KeyType::parse("none").unwrap(), None);
        assert!(KeyType::parse("vectorset").is_err());
    }

    fn sent(cmd: &redis::Cmd) -> std::vec::Vec<String> {
        cmd.args_iter()
            .filter_map(|arg| match arg {
                redis::Arg::Simple(bytes) => Some(String::from_utf8_lossy(bytes).to_string()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn migrate_sends_the_keys_after_its_options_as_redisson_does() {
        let cmd = Migrate::new(["a", "b"], "10.0.0.1", 6380, 2, Duration::from_secs(1))
            .copy()
            .replace()
            .username("user")
            .password("secret")
            .command()
            .unwrap();
        assert_eq!(
            sent(&cmd),
            [
                "MIGRATE", "10.0.0.1", "6380", "", "2", "1000", "COPY", "REPLACE", "AUTH2", "user",
                "secret", "KEYS", "a", "b"
            ]
        );
    }

    #[test]
    fn migrate_with_only_a_password_uses_auth() {
        let cmd = Migrate::new(["a"], "h", 1, 0, Duration::ZERO)
            .password("secret")
            .command()
            .unwrap();
        assert_eq!(
            sent(&cmd),
            ["MIGRATE", "h", "1", "", "0", "0", "AUTH", "secret", "KEYS", "a"]
        );
    }

    #[test]
    fn migrate_without_keys_is_an_error() {
        let keys: [&str; 0] = [];
        assert!(Migrate::new(keys, "h", 1, 0, Duration::ZERO)
            .command()
            .is_err());
    }
}
