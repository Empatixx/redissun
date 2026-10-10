use super::scripts::{ACQUIRE, FENCED_ACQUIRE, READ_ACQUIRE, WRITE_ACQUIRE};
use super::sync::{synced_eval, with_sync_retry};
use super::{
    channel, fair, lease_arg, release_inner, timeout_prefix, token_key, watchdog, write_field,
    LockGuard, Mode, Wait,
};
use crate::error::{Error, Result};
use crate::object::Key;
use crate::shield::shielded;
use bytes::Bytes;
use fred::types::scripts::Script;
use fred::types::Value;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

enum Outcome {
    Acquired(Option<u64>),
    Busy(i64),
}

fn outcome(mode: Mode, value: Value) -> Result<Outcome> {
    if mode == Mode::Fenced {
        let reply: Vec<i64> = value.convert()?;
        return match reply.as_slice() {
            [-1, token] => Ok(Outcome::Acquired(Some(*token as u64))),
            [ttl, ..] => Ok(Outcome::Busy(*ttl)),
            [] => Err(Error::Redis(
                "empty reply from the fenced lock script".into(),
            )),
        };
    }
    let ttl: Option<i64> = value.convert()?;
    Ok(ttl.map_or(Outcome::Acquired(None), Outcome::Busy))
}

fn acquire_call(
    key: &Key,
    owner: &str,
    lease: Duration,
    mode: Mode,
    wait: Wait,
    fair_wait: Duration,
) -> Result<(&'static Script, Vec<String>, Vec<Bytes>)> {
    let argument = lease_arg(lease)?;
    Ok(match mode {
        Mode::Exclusive => (
            &ACQUIRE,
            vec![key.redis_key()],
            vec![argument, Bytes::from(owner.to_string())],
        ),
        Mode::Read => (
            &READ_ACQUIRE,
            vec![key.redis_key(), timeout_prefix(key, owner)],
            vec![
                argument,
                Bytes::from(owner.to_string()),
                Bytes::from(write_field(owner)),
            ],
        ),
        Mode::Write => (
            &WRITE_ACQUIRE,
            vec![key.redis_key()],
            vec![argument, Bytes::from(write_field(owner))],
        ),
        Mode::Fenced => (
            &FENCED_ACQUIRE,
            vec![key.redis_key(), token_key(key)],
            vec![argument, Bytes::from(owner.to_string())],
        ),
        Mode::Fair if wait == Wait::Once => (
            &fair::TRY_ACQUIRE,
            fair::keys(key),
            vec![
                argument,
                Bytes::from(owner.to_string()),
                fair::now_millis(),
                fair::wait_arg(fair_wait),
            ],
        ),
        Mode::Fair => (
            &fair::ACQUIRE,
            fair::keys(key),
            vec![
                argument,
                Bytes::from(owner.to_string()),
                fair::wait_arg(fair_wait),
                fair::now_millis(),
            ],
        ),
    })
}

async fn try_acquire(
    key: &Key,
    owner: &str,
    lease: Duration,
    mode: Mode,
    wait: Wait,
    fair_wait: Duration,
) -> Result<Outcome> {
    with_sync_retry(&key.core.retry, || async {
        let (script, keys, args) = acquire_call(key, owner, lease, mode, wait, fair_wait)?;
        match synced_eval::<Value>(key, script, keys, args, false).await {
            Err(Error::NoSyncedReplicas) => {
                let request = Uuid::new_v4().to_string();
                let _ = release_inner(key, owner, lease, mode, &request).await;
                Err(Error::NoSyncedReplicas)
            }
            Err(error) => Err(error),
            Ok(value) => outcome(mode, value),
        }
    })
    .await
}

enum Attempt {
    Acquired(LockGuard),
    Busy(i64),
}

#[allow(clippy::too_many_arguments)]
async fn attempt(
    key: &Key,
    owner: &str,
    lease: Duration,
    watchdog: bool,
    mode: Mode,
    wait: Wait,
    fair_wait: Duration,
) -> Result<Attempt> {
    let key = key.clone();
    let owner = owner.to_string();
    shielded(async move {
        Ok(
            match try_acquire(&key, &owner, lease, mode, wait, fair_wait).await? {
                Outcome::Busy(ttl) => Attempt::Busy(ttl),
                Outcome::Acquired(token) => {
                    let cancel = CancellationToken::new();
                    if watchdog {
                        watchdog::spawn(key.clone(), owner.clone(), lease, mode, cancel.clone());
                    }
                    Attempt::Acquired(
                        LockGuard::new(key, owner, lease, mode, cancel).with_token(token),
                    )
                }
            },
        )
    })
    .await
}

fn fair_wait_of(key: &Key, wait: Wait) -> Duration {
    match wait {
        Wait::For(wait) if wait >= Duration::from_millis(1) => wait,
        _ => key.core.lock_settings.fair_wait_timeout,
    }
}

pub(crate) async fn acquire(
    key: &Key,
    lease: Option<Duration>,
    wait: Wait,
    mode: Mode,
) -> Result<Option<LockGuard>> {
    let core = &key.core;
    let owner = core.owner();
    let watchdog = lease.is_none();
    let lease = lease.unwrap_or(core.lock_lease);
    lease_arg(lease)?;
    let fair_wait = fair_wait_of(key, wait);
    let started = Instant::now();

    if wait == Wait::Once {
        return Ok(
            match attempt(key, &owner, lease, watchdog, mode, wait, fair_wait).await? {
                Attempt::Acquired(guard) => Some(guard),
                Attempt::Busy(_) => None,
            },
        );
    }

    let deadline = match wait {
        Wait::For(wait) => Some(started + wait),
        _ => None,
    };
    let mut queued =
        (mode == Mode::Fair).then(|| fair::Queued::new(key.clone(), owner.clone(), fair_wait));
    let expired = |deadline: Option<Instant>| deadline.is_some_and(|d| Instant::now() >= d);

    if let Attempt::Acquired(guard) =
        attempt(key, &owner, lease, watchdog, mode, wait, fair_wait).await?
    {
        if let Some(queued) = queued.as_mut() {
            queued.disarm();
        }
        return Ok(Some(guard));
    }

    let failed = |mut queued: Option<fair::Queued>| async move {
        if let Some(queued) = queued.as_mut() {
            let _ = queued.acquire_failed().await;
        }
        Ok::<Option<LockGuard>, Error>(None)
    };
    if expired(deadline) {
        return failed(queued).await;
    }

    let wake_channel = match mode {
        Mode::Fair => fair::channel(key.name(), &owner),
        _ => channel(key.name()),
    };
    let subscription = match deadline {
        Some(deadline) => {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining, async {
                core.pubsub().await?.subscribe(&wake_channel).await
            })
            .await
            {
                Ok(subscription) => subscription?,
                Err(_) => return failed(queued).await,
            }
        }
        None => core.pubsub().await?.subscribe(&wake_channel).await?,
    };
    if expired(deadline) {
        return failed(queued).await;
    }
    let notify: &Notify = subscription.notify();

    loop {
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let ttl = match attempt(key, &owner, lease, watchdog, mode, wait, fair_wait).await? {
            Attempt::Acquired(guard) => {
                if let Some(queued) = queued.as_mut() {
                    queued.disarm();
                }
                return Ok(Some(guard));
            }
            Attempt::Busy(ttl) => ttl,
        };

        match deadline {
            None => {
                if ttl >= 0 {
                    let _ = tokio::time::timeout(Duration::from_millis(ttl as u64), notified).await;
                } else {
                    notified.await;
                }
            }
            Some(deadline) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return failed(queued).await;
                }
                let pause = if ttl >= 0 && Duration::from_millis(ttl as u64) < remaining {
                    Duration::from_millis(ttl as u64)
                } else {
                    remaining
                };
                let _ = tokio::time::timeout(pause, notified).await;
                if expired(Some(deadline)) {
                    return failed(queued).await;
                }
            }
        }
    }
}
