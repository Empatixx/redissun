mod acquire;
pub(crate) mod fair;
pub(crate) mod fair_lock;
pub(crate) mod fenced_lock;
mod guard;
pub(crate) mod multi_lock;
mod request;
pub(crate) mod rw_lock;
mod scripts;
pub(crate) mod sync;
mod watchdog;

pub(crate) use acquire::acquire;
pub use guard::LockGuard;
pub use request::{Lock, LockRequest};
pub(crate) use scripts::{READ_FORCE_UNLOCK, READ_IS_LOCKED, WRITE_FORCE_UNLOCK};
pub(crate) use sync::LockSettings;

use crate::error::{Error, Result};
use crate::object::{millis as to_millis, tagged, Key};
use bytes::Bytes;
use fred::interfaces::{HashesInterface, KeysInterface};
use fred::types::scripts::Script;
use scripts::{
    READ_RELEASE, READ_RENEW, READ_UNLOCK_MESSAGE, RELEASE, RENEW, UNLOCK_MESSAGE, WRITE_RELEASE,
};
use std::time::Duration;
use sync::{synced_eval, with_sync_retry};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    Exclusive,
    Read,
    Write,
    Fair,
    Fenced,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Wait {
    Once,
    For(Duration),
    Forever,
}

pub(crate) fn write_field(owner: &str) -> String {
    format!("{owner}:write")
}

fn timeout_prefix(key: &Key, owner: &str) -> String {
    format!("{}:{}:rwlock_timeout", key.redis_key(), owner)
}

pub(crate) fn token_key(key: &Key) -> String {
    format!("redissun__lock_token:{}", key.redis_key())
}

pub(crate) fn channel(name: &str) -> String {
    format!("redissun__unlock__{name}")
}

fn lease_arg(duration: Duration) -> Result<Bytes> {
    Ok(Bytes::from(to_millis(duration)?.to_string()))
}

fn unlock_latch(key: &Key, request: &str) -> String {
    format!("redissun__unlock_latch:{}:{request}", tagged(key.name()))
}

pub(crate) fn held_field(owner: &str, mode: Mode) -> String {
    match mode {
        Mode::Write => write_field(owner),
        _ => owner.to_string(),
    }
}

pub(crate) async fn release(key: &Key, owner: &str, lease: Duration, mode: Mode) -> Result<()> {
    let request = Uuid::new_v4().to_string();
    with_sync_retry(&key.core.retry, || {
        release_inner(key, owner, lease, mode, &request)
    })
    .await
}

async fn release_inner(
    key: &Key,
    owner: &str,
    lease: Duration,
    mode: Mode,
    request: &str,
) -> Result<()> {
    let latch = unlock_latch(key, request);
    let latch_ttl = Bytes::from(key.core.retry.unlock_latch_ttl().as_millis().to_string());
    let (script, keys, args): (&Script, Vec<String>, Vec<Bytes>) = match mode {
        Mode::Read => (
            &READ_RELEASE,
            vec![
                key.redis_key(),
                channel(key.name()),
                timeout_prefix(key, owner),
                key.redis_key(),
                latch.clone(),
            ],
            vec![
                Bytes::from_static(UNLOCK_MESSAGE.as_bytes()),
                Bytes::from(owner.to_string()),
                latch_ttl,
            ],
        ),
        Mode::Write => (
            &WRITE_RELEASE,
            vec![key.redis_key(), channel(key.name()), latch.clone()],
            vec![
                Bytes::from_static(READ_UNLOCK_MESSAGE.as_bytes()),
                lease_arg(lease)?,
                Bytes::from(write_field(owner)),
                latch_ttl,
            ],
        ),
        Mode::Fair => (
            &fair::RELEASE,
            [fair::keys(key), vec![latch.clone()]].concat(),
            vec![
                Bytes::from_static(UNLOCK_MESSAGE.as_bytes()),
                lease_arg(lease)?,
                Bytes::from(owner.to_string()),
                fair::now_millis(),
                latch_ttl,
                Bytes::from(fair::channel_prefix(key.name())),
            ],
        ),
        Mode::Exclusive | Mode::Fenced => (
            &RELEASE,
            vec![key.redis_key(), latch.clone()],
            vec![
                lease_arg(lease)?,
                Bytes::from(owner.to_string()),
                Bytes::from(channel(key.name())),
                Bytes::from_static(UNLOCK_MESSAGE.as_bytes()),
                latch_ttl,
            ],
        ),
    };
    let outcome: Option<i64> = synced_eval(key, script, keys, args, false).await?;
    let client = key.core.redis().clone();
    tokio::spawn(async move {
        let _: std::result::Result<i64, _> = client.del(latch).await;
    });
    outcome.map(|_| ()).ok_or(Error::LockNotHeld)
}

pub(crate) async fn renew(key: &Key, owner: &str, lease: Duration, mode: Mode) -> Result<bool> {
    let (script, keys, args): (&Script, Vec<String>, Vec<Bytes>) = match mode {
        Mode::Read => (
            &READ_RENEW,
            vec![key.redis_key(), key.redis_key()],
            vec![lease_arg(lease)?, Bytes::from(owner.to_string())],
        ),
        _ => (
            &RENEW,
            vec![key.redis_key()],
            vec![lease_arg(lease)?, Bytes::from(held_field(owner, mode))],
        ),
    };
    let renewed: i64 = synced_eval(key, script, keys, args, true).await?;
    Ok(renewed == 1)
}

pub(crate) async fn force_unlock(
    key: &Key,
    script: &Script,
    keys: Vec<String>,
    args: Vec<Bytes>,
) -> Result<bool> {
    with_sync_retry(&key.core.retry, || async {
        let removed: i64 = synced_eval(key, script, keys.clone(), args.clone(), true).await?;
        Ok(removed == 1)
    })
    .await
}

pub(crate) fn unlock_message() -> Bytes {
    Bytes::from_static(UNLOCK_MESSAGE.as_bytes())
}

pub(crate) fn read_unlock_message() -> Bytes {
    Bytes::from_static(READ_UNLOCK_MESSAGE.as_bytes())
}

pub(crate) async fn is_locked(key: &Key) -> Result<bool> {
    let found: i64 = key.core.redis().exists(key.redis_key()).await?;
    Ok(found > 0)
}

pub(crate) async fn is_held_by_current(key: &Key, mode: Mode) -> Result<bool> {
    let field = held_field(&key.core.owner(), mode);
    let held: bool = key.core.redis().hexists(key.redis_key(), field).await?;
    Ok(held)
}

pub(crate) async fn hold_count(key: &Key, mode: Mode) -> Result<u64> {
    let field = held_field(&key.core.owner(), mode);
    let count: Option<u64> = key.core.redis().hget(key.redis_key(), field).await?;
    Ok(count.unwrap_or(0))
}
