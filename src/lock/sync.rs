use crate::error::{Error, Result};
use crate::object::Key;
use crate::retry::Retry;
use bytes::Bytes;
use fred::clients::Client as RedisClient;
use fred::interfaces::{ClientLike, ClusterInterface};
use fred::prelude::{FromValue, Options};
use fred::types::scripts::Script;
use fred::types::{ClusterHash, CustomCommand, Value};
use fred::util::redis_keyslot;
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

    fn single(&self, client: &RedisClient) -> bool {
        *self
            .single
            .get_or_init(|| client.client_config().server.is_centralized())
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

fn custom(name: &'static str, slot: u16) -> CustomCommand {
    CustomCommand::new_static(name, ClusterHash::Custom(slot), false)
}

fn eval_args(script: &Script, keys: &[String], args: &[Bytes]) -> Vec<Value> {
    let mut values = Vec::with_capacity(keys.len() + args.len() + 2);
    values.push(Value::String(script.sha1().clone()));
    values.push(Value::Integer(keys.len() as i64));
    values.extend(keys.iter().map(|key| Value::String(key.as_str().into())));
    values.extend(args.iter().map(|arg| Value::Bytes(arg.clone())));
    values
}

fn options(retry: bool, timeout: Option<Duration>) -> Options {
    Options {
        max_attempts: (!retry).then_some(1),
        timeout,
        ..Default::default()
    }
}

fn is_noscript(error: &fred::error::Error) -> bool {
    error.details().starts_with("NOSCRIPT")
}

async fn plain_eval<R: FromValue>(
    key: &Key,
    script: &Script,
    keys: Vec<String>,
    args: Vec<Bytes>,
    retry: bool,
) -> Result<R> {
    if retry {
        key.core.eval(script, keys, args).await
    } else {
        key.core.eval_no_retry(script, keys, args).await
    }
}

async fn wait_supported(key: &Key, client: &RedisClient, slot: u16) -> Result<bool> {
    let settings = &key.core.lock_settings;
    match settings.wait.load(Ordering::Acquire) {
        WAIT_SUPPORTED => return Ok(true),
        WAIT_UNSUPPORTED => return Ok(false),
        _ => {}
    }
    let probe: std::result::Result<Value, _> = client
        .custom(
            custom("WAIT", slot),
            vec![Value::Integer(0), Value::Integer(0)],
        )
        .await;
    let supported = match probe {
        Ok(_) => true,
        Err(error) if error.details().to_lowercase().contains("unknown command") => false,
        Err(error) => return Err(error.into()),
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

fn node_of(client: &RedisClient, slot: u16) -> String {
    client
        .cached_cluster_state()
        .and_then(|state| state.get_server(slot).map(|server| server.to_string()))
        .unwrap_or_default()
}

async fn connected_replicas(client: &RedisClient, slot: u16) -> Result<i64> {
    let info: String = client
        .custom(custom("INFO", slot), vec![Value::from("replication")])
        .await?;
    Ok(info
        .lines()
        .find_map(|line| line.trim().strip_prefix("connected_slaves:"))
        .and_then(|count| count.trim().parse().ok())
        .unwrap_or(0))
}

pub(crate) async fn synced_eval<R: FromValue>(
    key: &Key,
    script: &Script,
    keys: Vec<String>,
    args: Vec<Bytes>,
    retry: bool,
) -> Result<R> {
    let client = key.core.redis().clone();
    let settings = &key.core.lock_settings;
    if settings.single(&client) {
        return plain_eval(key, script, keys, args, retry).await;
    }
    let slot = redis_keyslot(keys.first().map(String::as_bytes).unwrap_or_default());
    if !wait_supported(key, &client, slot).await? {
        return plain_eval(key, script, keys, args, retry).await;
    }
    let node = node_of(&client, slot);
    let available = match settings.cached(&node) {
        Some(available) => available,
        None => {
            let available = connected_replicas(&client, slot).await?;
            settings.remember(&node, Some(available));
            available
        }
    };
    if available <= 0 {
        return plain_eval(key, script, keys, args, retry).await;
    }

    let timeout = settings.replicas_sync_timeout.as_millis() as i64;
    let command_timeout = key.core.retry.timeout;
    let wait_timeout =
        (!command_timeout.is_zero()).then(|| command_timeout + settings.replicas_sync_timeout);
    let mut reloaded = false;
    loop {
        let pipeline = client.pipeline();
        let queued = pipeline.with_options(&options(retry, wait_timeout));
        let _: Value = queued
            .custom(custom("EVALSHA", slot), eval_args(script, &keys, &args))
            .await?;
        let _: Value = queued
            .custom(
                custom("WAIT", slot),
                vec![Value::Integer(available), Value::Integer(timeout)],
            )
            .await?;
        let mut replies = pipeline.try_all::<Value>().await.into_iter();
        let evaluated = replies.next().unwrap_or_else(|| Ok(Value::Null));
        let synced = replies.next().unwrap_or(Ok(Value::Integer(0)));
        let value = match evaluated {
            Err(error) if is_noscript(&error) && !reloaded => {
                script.load(&client).await?;
                reloaded = true;
                continue;
            }
            Err(error) => return Err(error.into()),
            Ok(value) => value,
        };
        let synced: i64 = synced?.convert()?;
        if synced != available {
            settings.remember(&node, None);
        }
        if settings.check_synced_replicas && synced == 0 {
            return Err(Error::NoSyncedReplicas);
        }
        return Ok(value.convert()?);
    }
}
