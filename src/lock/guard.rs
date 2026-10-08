use crate::error::Result;
use crate::lock::{release, Mode};
use crate::object::Key;
use std::fmt;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;

struct Held {
    key: Key,
    owner: String,
    lease: Duration,
    mode: Mode,
    cancel: CancellationToken,
    runtime: Option<Handle>,
}

/// Proof that a lock is held. Dropping it releases the lock in the background.
#[must_use = "dropping the guard releases the lock in the background; call unlock().await to release it deterministically"]
pub struct LockGuard {
    held: Option<Held>,
}

impl LockGuard {
    pub(crate) fn new(
        key: Key,
        owner: String,
        lease: Duration,
        mode: Mode,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            held: Some(Held {
                key,
                owner,
                lease,
                mode,
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
        release(&held.key, &held.owner, held.lease, held.mode).await
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
                let _ = release(&held.key, &held.owner, held.lease, held.mode).await;
            });
        }
    }
}

impl fmt::Debug for LockGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("LockGuard");
        match &self.held {
            Some(held) => debug.field("name", &held.key.name()),
            None => debug.field("released", &true),
        };
        debug.finish()
    }
}
