use crate::error::Result;
use crate::lock::release;
use crate::object::Key;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;

struct Held {
    key: Key,
    owner: String,
    lease: Duration,
    cancel: CancellationToken,
    runtime: Option<Handle>,
}

/// Proof that a lock is held. Dropping it releases the lock in the background.
#[must_use = "dropping the guard releases the lock in the background; call unlock().await to release it deterministically"]
pub struct LockGuard {
    held: Option<Held>,
}

impl LockGuard {
    pub(crate) fn new(key: Key, owner: String, lease: Duration, cancel: CancellationToken) -> Self {
        Self {
            held: Some(Held {
                key,
                owner,
                lease,
                cancel,
                runtime: Handle::try_current().ok(),
            }),
        }
    }

    /// Releases one hold and stops the watchdog. Fails with `Error::LockNotHeld` when the lease already expired.
    pub async fn unlock(mut self) -> Result<()> {
        let Some(held) = self.held.take() else {
            return Ok(());
        };
        held.cancel.cancel();
        release(&held.key, &held.owner, held.lease).await
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let Some(held) = self.held.take() else {
            return;
        };
        held.cancel.cancel();
        if let Some(runtime) = held.runtime {
            runtime.spawn(async move {
                let _ = release(&held.key, &held.owner, held.lease).await;
            });
        }
    }
}
