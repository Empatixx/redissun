use crate::client::core::Core;
use crate::codec::{Codec, JsonCodec};
use crate::error::{Error, Result};
use bytes::Bytes;
use fred::clients::Client as RedisClient;
use fred::interfaces::{ClientLike, ClusterInterface, FunctionInterface, LuaInterface};
use fred::types::config::Server;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// Whether a script or function only reads, like Redisson's `RScript.Mode`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ScriptMode {
    /// The script only reads. It runs with `EVAL_RO`, `EVALSHA_RO` or `FCALL_RO`, which Redis refuses for a script that writes. Redis older than 7.0 gets the plain command instead.
    ReadOnly,
    /// The script may write. The default.
    #[default]
    ReadWrite,
}

/// A Lua script that knows its SHA-1 digest, so it is sent to Redis only when Redis does not have it yet. Cheap to clone; keep one in a `static` or a struct and run it with [`Script::run`].
#[derive(Clone)]
pub struct LuaScript {
    source: Arc<str>,
    sha1: Arc<str>,
}

impl LuaScript {
    /// Wraps the Lua `source` and computes its SHA-1 digest.
    pub fn new(source: impl Into<String>) -> Self {
        let source: String = source.into();
        let sha1 = fred::util::sha1_hash(&source);
        Self {
            source: source.into(),
            sha1: sha1.into(),
        }
    }

    /// The Lua source.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// The SHA-1 digest that `EVALSHA` and `SCRIPT EXISTS` use.
    pub fn sha1(&self) -> &str {
        &self.sha1
    }
}

impl fmt::Debug for LuaScript {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LuaScript")
            .field("sha1", &self.sha1)
            .finish_non_exhaustive()
    }
}

mod sealed {
    use crate::codec::Codec;
    use crate::error::Result;
    use fred::types::Value;

    pub trait FromReply: Sized {
        fn from_reply<C: Codec>(reply: Value, codec: &C) -> Result<Self>;
    }
}

use sealed::FromReply;

/// A Rust type that a script or function reply can become, like Redisson's `RScript.ReturnType`.
///
/// | Type | Redisson | Reply |
/// |---|---|---|
/// | `()` | | ignored |
/// | `bool` | `BOOLEAN` | Lua `true` or `1` is `true`; `false`, `nil` and `0` are `false` |
/// | `i64` | `LONG` | an integer, or text holding one |
/// | `f64` | | a number, or text holding one |
/// | `String` | `STRING` | text or a status reply, as it is (not through the codec) |
/// | [`Bytes`] | | the raw bytes |
/// | [`Decoded<V>`] | `VALUE` | text decoded by the codec, for values that objects stored |
/// | `Option<T>` | | `None` for `nil` |
/// | `Vec<T>` | `LIST`, `MAPVALUELIST` | a Lua table |
pub trait ScriptOutput: FromReply {}

/// A reply decoded by the client's codec, like Redisson's `ReturnType.VALUE`. Use it for values that objects stored, for example `redis.call('GET', KEYS[1])` on a [`Bucket`](crate::Bucket).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decoded<V>(pub V);

impl<V> Decoded<V> {
    /// The decoded value.
    pub fn into_inner(self) -> V {
        self.0
    }
}

fn unexpected(reply: &Value, wanted: &str) -> Error {
    Error::Redis(format!("the script returned {reply:?}, not {wanted}"))
}

fn reply_text(reply: &Value) -> Option<String> {
    match reply {
        Value::String(text) => Some(text.to_string()),
        Value::Bytes(bytes) => String::from_utf8(bytes.to_vec()).ok(),
        Value::Integer(number) => Some(number.to_string()),
        Value::Double(number) => Some(number.to_string()),
        Value::Boolean(flag) => Some(flag.to_string()),
        _ => None,
    }
}

impl FromReply for () {
    fn from_reply<C: Codec>(_reply: Value, _codec: &C) -> Result<Self> {
        Ok(())
    }
}

impl FromReply for bool {
    fn from_reply<C: Codec>(reply: Value, _codec: &C) -> Result<Self> {
        Ok(match reply {
            Value::Boolean(flag) => flag,
            Value::Integer(number) => number == 1,
            Value::Null => false,
            other => match reply_text(&other).as_deref() {
                Some("1") | Some("true") | Some("OK") => true,
                Some(_) => false,
                None => return Err(unexpected(&other, "a bool")),
            },
        })
    }
}

impl FromReply for i64 {
    fn from_reply<C: Codec>(reply: Value, _codec: &C) -> Result<Self> {
        match &reply {
            Value::Integer(number) => Ok(*number),
            other => reply_text(other)
                .and_then(|text| text.parse().ok())
                .ok_or_else(|| unexpected(&reply, "an integer")),
        }
    }
}

impl FromReply for f64 {
    fn from_reply<C: Codec>(reply: Value, _codec: &C) -> Result<Self> {
        match &reply {
            Value::Double(number) => Ok(*number),
            Value::Integer(number) => Ok(*number as f64),
            other => reply_text(other)
                .and_then(|text| text.parse().ok())
                .ok_or_else(|| unexpected(&reply, "a number")),
        }
    }
}

impl FromReply for String {
    fn from_reply<C: Codec>(reply: Value, _codec: &C) -> Result<Self> {
        reply_text(&reply).ok_or_else(|| unexpected(&reply, "text"))
    }
}

impl FromReply for Bytes {
    fn from_reply<C: Codec>(reply: Value, _codec: &C) -> Result<Self> {
        crate::reply::bytes(&reply).ok_or_else(|| unexpected(&reply, "bytes"))
    }
}

impl<V: DeserializeOwned> FromReply for Decoded<V> {
    fn from_reply<C: Codec>(reply: Value, codec: &C) -> Result<Self> {
        let bytes = crate::reply::bytes(&reply).ok_or_else(|| unexpected(&reply, "a value"))?;
        codec.decode(&bytes).map(Decoded)
    }
}

impl<T: FromReply> FromReply for Option<T> {
    fn from_reply<C: Codec>(reply: Value, codec: &C) -> Result<Self> {
        match reply {
            Value::Null => Ok(None),
            other => T::from_reply(other, codec).map(Some),
        }
    }
}

impl<T: FromReply> FromReply for Vec<T> {
    fn from_reply<C: Codec>(reply: Value, codec: &C) -> Result<Self> {
        match reply {
            Value::Array(items) => items
                .into_iter()
                .map(|item| T::from_reply(item, codec))
                .collect(),
            Value::Null => Ok(Vec::new()),
            other => Err(unexpected(&other, "a list")),
        }
    }
}

impl ScriptOutput for () {}
impl ScriptOutput for bool {}
impl ScriptOutput for i64 {}
impl ScriptOutput for f64 {}
impl ScriptOutput for String {}
impl ScriptOutput for Bytes {}
impl<V: DeserializeOwned> ScriptOutput for Decoded<V> {}
impl<T: ScriptOutput> ScriptOutput for Option<T> {}
impl<T: ScriptOutput> ScriptOutput for Vec<T> {}

/// Runs Lua scripts, like Redisson's `RScript`. Get it with [`Client::script`](crate::Client::script).
///
/// Arguments go through the client's codec, as in Redisson, so with the default JSON codec the string `5` reaches Lua as `"5"` with the quotes. Use [`Client::with_codec`](crate::Client::with_codec) with [`StringCodec`](crate::StringCodec) to pass plain text and numbers.
///
/// ```no_run
/// # async fn run(client: redissun::Client) -> redissun::Result<()> {
/// use redissun::StringCodec;
///
/// let script = client.with_codec(StringCodec).script();
/// let total: i64 = script
///     .eval("return redis.call('INCRBY', KEYS[1], ARGV[1])")
///     .key("visits")
///     .arg(&5)
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct Script<C: Codec = JsonCodec> {
    core: Arc<Core>,
    codec: C,
}

impl<C: Codec> fmt::Debug for Script<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Script").finish_non_exhaustive()
    }
}

impl<C: Codec> Clone for Script<C> {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
            codec: self.codec.clone(),
        }
    }
}

#[derive(Clone, Copy)]
enum Body<'a> {
    Source(&'a str),
    Sha(&'a str),
    Lua(&'a LuaScript),
    Function(&'a str),
}

/// One call of a script or function. Add keys and arguments, then `.await` it. The reply becomes any [`ScriptOutput`].
#[must_use = "a script call does nothing until it is awaited"]
pub struct ScriptCall<'a, C: Codec, R> {
    core: &'a Arc<Core>,
    codec: &'a C,
    body: Body<'a>,
    keys: Vec<String>,
    args: Result<Vec<Bytes>>,
    mode: ScriptMode,
    _reply: PhantomData<fn() -> R>,
}

impl<'a, C: Codec, R> ScriptCall<'a, C, R> {
    fn new(core: &'a Arc<Core>, codec: &'a C, body: Body<'a>) -> Self {
        Self {
            core,
            codec,
            body,
            keys: Vec::new(),
            args: Ok(Vec::new()),
            mode: ScriptMode::ReadWrite,
            _reply: PhantomData,
        }
    }

    /// Adds a key, which the script reads as `KEYS[n]`. In Redis Cluster all keys must share a hash slot, and the call goes to the node of the first key.
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.keys.push(key.into());
        self
    }

    /// Adds several keys.
    pub fn keys<K: Into<String>>(mut self, keys: impl IntoIterator<Item = K>) -> Self {
        self.keys.extend(keys.into_iter().map(Into::into));
        self
    }

    /// Adds an argument, encoded by the codec, which the script reads as `ARGV[n]`.
    pub fn arg<A: Serialize + ?Sized>(mut self, arg: &A) -> Self {
        if let Ok(args) = self.args.as_mut() {
            match self.codec.encode(arg) {
                Ok(bytes) => args.push(bytes),
                Err(error) => self.args = Err(error),
            }
        }
        self
    }

    /// Adds several arguments of one type.
    pub fn args<A: Serialize>(mut self, args: impl IntoIterator<Item = A>) -> Self {
        for arg in args {
            self = self.arg(&arg);
        }
        self
    }

    /// Adds an argument as raw bytes, without the codec.
    pub fn arg_bytes(mut self, arg: impl Into<Bytes>) -> Self {
        if let Ok(args) = self.args.as_mut() {
            args.push(arg.into());
        }
        self
    }

    /// Runs with `EVAL_RO`, `EVALSHA_RO` or `FCALL_RO`, the same as `.mode(ScriptMode::ReadOnly)`.
    pub fn read_only(self) -> Self {
        self.mode(ScriptMode::ReadOnly)
    }

    /// Sets the [`ScriptMode`]. Defaults to [`ScriptMode::ReadWrite`].
    pub fn mode(mut self, mode: ScriptMode) -> Self {
        self.mode = mode;
        self
    }
}

fn custom(name: &'static str, keys: &[String]) -> CustomCommand {
    let hash = match keys.first() {
        Some(key) => ClusterHash::Custom(fred::util::redis_keyslot(key.as_bytes())),
        None => ClusterHash::Random,
    };
    CustomCommand::new_static(name, hash, false)
}

fn command_args(head: &str, keys: &[String], args: &[Bytes]) -> Vec<Value> {
    let mut values = Vec::with_capacity(keys.len() + args.len() + 2);
    values.push(Value::from(head));
    values.push(Value::Integer(keys.len() as i64));
    values.extend(keys.iter().map(|key| Value::from(key.as_str())));
    values.extend(args.iter().map(|arg| Value::Bytes(arg.clone())));
    values
}

fn starts_with(error: &fred::error::Error, prefix: &str) -> bool {
    error.details().starts_with(prefix)
}

fn unknown_command(error: &fred::error::Error) -> bool {
    error.details().to_lowercase().contains("unknown command")
}

async fn send(
    client: &RedisClient,
    name: &'static str,
    head: &str,
    keys: &[String],
    args: &[Bytes],
) -> std::result::Result<Value, fred::error::Error> {
    client
        .custom(custom(name, keys), command_args(head, keys, args))
        .await
}

impl<C: Codec, R> ScriptCall<'_, C, R> {
    async fn reply(self) -> Result<Value> {
        let args = self.args?;
        let keys = self.keys;
        let client = self.core.redis();
        let read_only = self.mode == ScriptMode::ReadOnly
            && self.core.read_only_scripts.load(Ordering::Acquire);
        let (sha, source) = match self.body {
            Body::Source(source) => (fred::util::sha1_hash(source), Some(source)),
            Body::Sha(sha) => (sha.to_string(), None),
            Body::Lua(script) => (script.sha1().to_string(), Some(script.source())),
            Body::Function(name) => {
                let command = if read_only { "FCALL_RO" } else { "FCALL" };
                return match send(client, command, name, &keys, &args).await {
                    Err(error) if read_only && unknown_command(&error) => {
                        self.core.read_only_scripts.store(false, Ordering::Release);
                        Ok(send(client, "FCALL", name, &keys, &args).await?)
                    }
                    reply => Ok(reply?),
                };
            }
        };
        let (evalsha, eval) = if read_only {
            ("EVALSHA_RO", "EVAL_RO")
        } else {
            ("EVALSHA", "EVAL")
        };
        let error = match send(client, evalsha, &sha, &keys, &args).await {
            Ok(reply) => return Ok(reply),
            Err(error) => error,
        };
        if read_only && unknown_command(&error) {
            self.core.read_only_scripts.store(false, Ordering::Release);
            let fallback = ScriptCall::<C, R> {
                core: self.core,
                codec: self.codec,
                body: self.body,
                keys,
                args: Ok(args),
                mode: ScriptMode::ReadWrite,
                _reply: PhantomData,
            };
            return Box::pin(fallback.reply()).await;
        }
        match source {
            Some(source) if starts_with(&error, "NOSCRIPT") => {
                Ok(send(client, eval, source, &keys, &args).await?)
            }
            _ => Err(error.into()),
        }
    }
}

impl<'a, C: Codec, R: ScriptOutput + Send + 'a> IntoFuture for ScriptCall<'a, C, R> {
    type Output = Result<R>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let codec = self.codec;
            let reply = self.reply().await?;
            R::from_reply(reply, codec)
        })
    }
}

async fn primaries(client: &RedisClient) -> Vec<Option<Server>> {
    if !client.is_clustered() {
        return vec![None];
    }
    client
        .cached_cluster_state()
        .map(|state| state.unique_primary_nodes().into_iter().map(Some).collect())
        .unwrap_or_default()
}

impl<C: Codec> Script<C> {
    pub(crate) fn new(core: Arc<Core>, codec: C) -> Self {
        Self { core, codec }
    }

    /// Runs the Lua `source`, like Redisson's `eval`. As with Redisson's script cache, it sends `EVALSHA` first and the whole source only when Redis does not know the script yet.
    pub fn eval<'a, R>(&'a self, source: &'a str) -> ScriptCall<'a, C, R> {
        ScriptCall::new(&self.core, &self.codec, Body::Source(source))
    }

    /// Runs the script that Redis knows by this SHA-1 digest (`EVALSHA`), like Redisson's `evalSha`. Fails with a `NOSCRIPT` error when Redis does not have it.
    pub fn eval_sha<'a, R>(&'a self, sha1: &'a str) -> ScriptCall<'a, C, R> {
        ScriptCall::new(&self.core, &self.codec, Body::Sha(sha1))
    }

    /// Runs a [`LuaScript`] with `EVALSHA` and sends its source only when Redis does not have it, for example after a restart or `SCRIPT FLUSH`.
    pub fn run<'a, R>(&'a self, script: &'a LuaScript) -> ScriptCall<'a, C, R> {
        ScriptCall::new(&self.core, &self.codec, Body::Lua(script))
    }

    /// Loads the script into the script cache and returns its SHA-1 digest (`SCRIPT LOAD`), like Redisson's `scriptLoad`. In Redis Cluster it loads it on every master.
    pub async fn script_load(&self, source: &str) -> Result<String> {
        let client = self.core.redis();
        if client.is_clustered() {
            Ok(client.script_load_cluster(source).await?)
        } else {
            Ok(client.script_load(source).await?)
        }
    }

    /// For each digest, whether Redis has the script (`SCRIPT EXISTS`), like Redisson's `scriptExists`. In Redis Cluster a script counts only when every master has it.
    pub async fn script_exists(&self, sha1s: &[&str]) -> Result<Vec<bool>> {
        if sha1s.is_empty() {
            return Ok(Vec::new());
        }
        let digests: Vec<String> = sha1s.iter().map(|sha| sha.to_string()).collect();
        let client = self.core.redis();
        let mut found = vec![true; sha1s.len()];
        for server in primaries(client).await {
            let answer: Vec<bool> = match server {
                Some(server) => {
                    client
                        .with_cluster_node(server)
                        .script_exists(digests.clone())
                        .await?
                }
                None => client.script_exists(digests.clone()).await?,
            };
            found
                .iter_mut()
                .zip(answer)
                .for_each(|(found, here)| *found &= here);
        }
        Ok(found)
    }

    /// Removes every script from the script cache (`SCRIPT FLUSH`), like Redisson's `scriptFlush`. In Redis Cluster on every master.
    pub async fn script_flush(&self) -> Result<()> {
        let client = self.core.redis();
        if client.is_clustered() {
            Ok(client.script_flush_cluster(false).await?)
        } else {
            Ok(client.script_flush(false).await?)
        }
    }

    /// Stops the script that is running now (`SCRIPT KILL`), like Redisson's `scriptKill`. Fails with a `NOTBUSY` error when no script runs, and with `UNKILLABLE` when the script already wrote. In Redis Cluster on every master.
    pub async fn script_kill(&self) -> Result<()> {
        let client = self.core.redis();
        if client.is_clustered() {
            Ok(client.script_kill_cluster().await?)
        } else {
            Ok(client.script_kill().await?)
        }
    }
}

/// Loads and calls Redis functions (`FUNCTION LOAD`, `FCALL`), like Redisson's `RFunction`. Needs Redis 7.0 or newer. Get it with [`Client::function`](crate::Client::function).
///
/// Arguments go through the client's codec, as for [`Script`].
pub struct Function<C: Codec = JsonCodec> {
    core: Arc<Core>,
    codec: C,
}

impl<C: Codec> fmt::Debug for Function<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Function").finish_non_exhaustive()
    }
}

impl<C: Codec> Clone for Function<C> {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
            codec: self.codec.clone(),
        }
    }
}

impl<C: Codec> Function<C> {
    pub(crate) fn new(core: Arc<Core>, codec: C) -> Self {
        Self { core, codec }
    }

    /// Loads a library (`FUNCTION LOAD`) and returns its name, like Redisson's `load`. Fails when a library with that name exists. In Redis Cluster on every master.
    pub async fn load(&self, code: &str) -> Result<String> {
        self.load_library(false, code).await
    }

    /// Loads a library and replaces one with the same name (`FUNCTION LOAD REPLACE`), like Redisson's `loadAndReplace`.
    pub async fn load_and_replace(&self, code: &str) -> Result<String> {
        self.load_library(true, code).await
    }

    async fn load_library(&self, replace: bool, code: &str) -> Result<String> {
        let client = self.core.redis();
        if client.is_clustered() {
            Ok(client.function_load_cluster(replace, code).await?)
        } else {
            Ok(client.function_load(replace, code).await?)
        }
    }

    /// Deletes a library (`FUNCTION DELETE`), like Redisson's `delete`. In Redis Cluster on every master.
    pub async fn delete(&self, library: &str) -> Result<()> {
        let client = self.core.redis();
        if client.is_clustered() {
            Ok(client.function_delete_cluster(library).await?)
        } else {
            Ok(client.function_delete(library).await?)
        }
    }

    /// Deletes every library (`FUNCTION FLUSH`), like Redisson's `flush`. In Redis Cluster on every master.
    pub async fn flush(&self) -> Result<()> {
        let client = self.core.redis();
        if client.is_clustered() {
            Ok(client.function_flush_cluster(false).await?)
        } else {
            Ok(client.function_flush(false).await?)
        }
    }

    /// Stops the function that is running now (`FUNCTION KILL`), like Redisson's `kill`. Fails with a `NOTBUSY` error when none runs.
    pub async fn kill(&self) -> Result<()> {
        let _: Value = self.core.redis().function_kill().await?;
        Ok(())
    }

    /// Calls the function `name` (`FCALL`), like Redisson's `call`. Add keys and arguments, then `.await` it; `.read_only()` uses `FCALL_RO`.
    pub fn call<'a, R>(&'a self, name: &'a str) -> ScriptCall<'a, C, R> {
        ScriptCall::new(&self.core, &self.codec, Body::Function(name))
    }
}
