use crate::client::request::Request;
use crate::client::retry::Retry;
use crate::error::{Error, Result};
use crate::lua::Lua;
use crate::net::node::{is_connection_failure, Failure};
use crate::net::topology::Topology;
use crate::object::Key;
use bytes::Bytes;
use redis::Value;
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

const WAIT_UNKNOWN: u8 = 0;
const WAIT_SUPPORTED: u8 = 1;
const WAIT_UNSUPPORTED: u8 = 2;

pub(crate) struct LockSettings {
    pub(crate) check_synced_replicas: bool,
    pub(crate) replicas_sync_timeout: Duration,
    pub(crate) fair_wait_timeout: Duration,
    single: OnceLock<bool>,
    wait: AtomicU8,
    replicas: Mutex<HashMap<String, i64>>,
}

impl LockSettings {
    pub(crate) fn new(
        check_synced_replicas: bool,
        replicas_sync_timeout: Duration,
        fair_wait_timeout: Duration,
    ) -> Self {
        Self {
            check_synced_replicas,
            replicas_sync_timeout,
            fair_wait_timeout,
            single: OnceLock::new(),
            wait: AtomicU8::new(WAIT_UNKNOWN),
            replicas: Mutex::new(HashMap::new()),
        }
    }

    fn single(&self, topology: &Topology) -> bool {
        *self
            .single
            .get_or_init(|| matches!(topology, Topology::Single(_)))
    }

    fn cached(&self, node: &str) -> Option<i64> {
        self.replicas
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(node)
            .copied()
    }

    fn remember(&self, node: &str, replicas: Option<i64>) {
        let mut cache = self.replicas.lock().unwrap_or_else(|e| e.into_inner());
        match replicas {
            Some(replicas) => cache.insert(node.to_string(), replicas),
            None => cache.remove(node),
        };
    }
}

pub(crate) async fn with_sync_retry<T, F, Fut>(retry: &Retry, mut call: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut attempts = 0;
    let mut pause = Duration::ZERO;
    loop {
        match call().await {
            Err(Error::NoSyncedReplicas) if attempts < retry.attempts => {
                pause = retry.delay.delay(attempts, pause);
                attempts += 1;
                tokio::time::sleep(pause).await;
            }
            result => return result,
        }
    }
}

fn is_noscript(value: &Value) -> bool {
    matches!(value, Value::ServerError(error) if error.code() == "NOSCRIPT")
}

/// Sends `cmd` routed by `key`.
async fn on_key(key: &Key, routing: &str, cmd: redis::Cmd) -> Result<Value> {
    key.core
        .query_value(Request::new(cmd).key(routing.as_bytes()))
        .await
}

async fn plain_eval<R: redis::FromRedisValue>(
    key: &Key,
    script: &Lua,
    keys: Vec<String>,
    args: Vec<Bytes>,
    retry: bool,
) -> Result<R> {
    if retry {
        key.core.eval(script, keys, args).await
    } else {
        key.core.eval_once(script, keys, args).await
    }
}

async fn wait_supported(key: &Key, routing: &str) -> Result<bool> {
    let settings = &key.core.lock_settings;
    match settings.wait.load(Ordering::Acquire) {
        WAIT_SUPPORTED => return Ok(true),
        WAIT_UNSUPPORTED => return Ok(false),
        _ => {}
    }
    let mut probe = redis::cmd("WAIT");
    probe.arg(0).arg(0);
    let supported = match on_key(key, routing, probe).await {
        Ok(_) => true,
        Err(Error::Redis(message)) if message.to_lowercase().contains("unknown command") => false,
        Err(error) => return Err(error),
    };
    settings.wait.store(
        if supported {
            WAIT_SUPPORTED
        } else {
            WAIT_UNSUPPORTED
        },
        Ordering::Release,
    );
    Ok(supported)
}

async fn connected_replicas(key: &Key, routing: &str) -> Result<i64> {
    let mut cmd = redis::cmd("INFO");
    cmd.arg("replication");
    let info: String = redis::FromRedisValue::from_redis_value(on_key(key, routing, cmd).await?)?;
    Ok(info
        .lines()
        .find_map(|line| line.trim().strip_prefix("connected_slaves:"))
        .and_then(|count| count.trim().parse().ok())
        .unwrap_or(0))
}

/// What one try of a lock write that waits for replicas came to.
enum Step<R> {
    Done(Result<R>),
    /// The write did not run where it should: the connection was lost (and `retry` allows it, or Redis never saw it), the node is no master any more (`READONLY`), or the slot moved. Where the servers are is read again before the next try.
    Again(Error),
    /// Redis never saw the write: no connection could be opened. It waits for one within `timeout` without using an attempt.
    Unsent(Error),
}

pub(crate) async fn synced_eval<R: redis::FromRedisValue>(
    key: &Key,
    script: &Lua,
    keys: Vec<String>,
    args: Vec<Bytes>,
    retry: bool,
) -> Result<R> {
    let retry_settings = key.core.retry;
    let deadline = (!retry_settings.timeout.is_zero())
        .then(|| std::time::Instant::now() + retry_settings.timeout);
    let mut pause = Duration::ZERO;
    let mut attempt = 0;
    loop {
        match synced_step(key, script, &keys, &args, retry).await {
            Step::Done(result) => return result,
            Step::Unsent(error) => match deadline {
                Some(deadline) if std::time::Instant::now() < deadline => {
                    key.core.topology().refresh().await;
                    pause = retry_settings.delay.delay(0, pause);
                    let left = deadline.saturating_duration_since(std::time::Instant::now());
                    tokio::time::sleep(pause.min(left)).await;
                }
                Some(_) => return Err(Error::Timeout),
                None if attempt >= retry_settings.attempts => return Err(error),
                None => {
                    key.core.topology().refresh().await;
                    pause = retry_settings.delay.delay(attempt, pause);
                    attempt += 1;
                    tokio::time::sleep(pause).await;
                }
            },
            Step::Again(error) if attempt >= retry_settings.attempts => return Err(error),
            Step::Again(_) => {
                key.core.topology().refresh().await;
                pause = retry_settings.delay.delay(attempt, pause);
                attempt += 1;
                tokio::time::sleep(pause).await;
            }
        }
    }
}

async fn synced_step<R: redis::FromRedisValue>(
    key: &Key,
    script: &Lua,
    keys: &[String],
    args: &[Bytes],
    retry: bool,
) -> Step<R> {
    let settings = &key.core.lock_settings;
    let topology = key.core.topology();
    let plain = || plain_eval(key, script, keys.to_vec(), args.to_vec(), retry);
    if settings.single(topology) {
        return Step::Done(plain().await);
    }
    let routing = keys.first().cloned().unwrap_or_default();
    match wait_supported(key, &routing).await {
        Ok(true) => {}
        Ok(false) => return Step::Done(plain().await),
        Err(error) => return Step::Done(Err(error)),
    }
    let pool = match topology.pool_for(Some(routing.as_bytes())).await {
        Ok(pool) => pool,
        Err(failure @ Failure::Unsent(_)) => return Step::Unsent(failure.into()),
        Err(failure) if is_connection_failure(&failure) => return Step::Again(failure.into()),
        Err(failure) => return Step::Done(Err(failure.into())),
    };
    let node_name = pool.address().to_string();
    let available = match settings.cached(&node_name) {
        Some(available) => available,
        None => match connected_replicas(key, &routing).await {
            Ok(available) => {
                settings.remember(&node_name, Some(available));
                available
            }
            Err(error) => return Step::Done(Err(error)),
        },
    };
    if available <= 0 {
        return Step::Done(plain().await);
    }

    let timeout = settings.replicas_sync_timeout.as_millis() as i64;
    let command_timeout = key.core.retry.timeout;
    let wait_timeout =
        (!command_timeout.is_zero()).then(|| command_timeout + settings.replicas_sync_timeout);
    let mut reloaded = false;
    loop {
        // The script and its WAIT go on one connection, so WAIT waits for this write.
        let node = pool.next();
        let mut pipe = redis::pipe();
        pipe.cmd("EVALSHA")
            .arg(script.sha1())
            .arg(keys.len())
            .arg(keys);
        for arg in args {
            pipe.arg(&arg[..]);
        }
        pipe.cmd("WAIT").arg(available).arg(timeout);
        let sending = node.send_pipeline(&pipe, 0, 2);
        let sent = match wait_timeout {
            Some(limit) => tokio::time::timeout(limit, sending)
                .await
                .unwrap_or(Err(Failure::Other(Error::Timeout))),
            None => sending.await,
        };
        let mut replies = match sent {
            Ok(replies) => replies.into_iter(),
            // Sent once unless `retry`, but always again when Redis never saw it.
            Err(failure @ Failure::Unsent(_)) => return Step::Unsent(failure.into()),
            Err(failure @ Failure::Connection(_)) if retry => return Step::Again(failure.into()),
            Err(failure) => return Step::Done(Err(failure.into())),
        };
        let evaluated = replies.next().unwrap_or(Value::Nil);
        let synced = replies.next().unwrap_or(Value::Int(0));
        if is_noscript(&evaluated) && !reloaded {
            let mut load = redis::cmd("SCRIPT");
            load.arg("LOAD").arg(script.source());
            if let Err(failure) = node.send(&load).await {
                return Step::Done(Err(failure.into()));
            }
            reloaded = true;
            continue;
        }
        if let Value::ServerError(error) = evaluated {
            // Refused without running: the node became a replica, or the slot moved.
            return match crate::net::node::server_failure(error) {
                failure @ Failure::Redirect { .. } => Step::Again(failure.into()),
                Failure::Other(Error::Redis(message)) if message.starts_with("READONLY") => {
                    Step::Again(Error::Redis(message))
                }
                failure => Step::Done(Err(failure.into())),
            };
        }
        let synced: i64 = match redis::FromRedisValue::from_redis_value(synced) {
            Ok(synced) => synced,
            Err(error) => return Step::Done(Err(error.into())),
        };
        if synced != available {
            settings.remember(&node_name, None);
        }
        if settings.check_synced_replicas && synced == 0 {
            return Step::Done(Err(Error::NoSyncedReplicas));
        }
        return Step::Done(R::from_redis_value(evaluated).map_err(Error::from));
    }
}
