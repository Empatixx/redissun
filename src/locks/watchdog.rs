use crate::locks::{renew, Mode};
use crate::object::Key;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub(super) fn spawn(
    key: Key,
    owner: String,
    lease: Duration,
    mode: Mode,
    cancel: CancellationToken,
) {
    let period = lease / 3;
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(period) => {}
            }
            match renew(&key, &owner, lease, mode).await {
                Ok(true) => {}
                Ok(false) => break,
                // The lease runs on; the next period tries again, as Redisson's `RenewalTask` does.
                Err(error) => {
                    tracing::error!("Can't update lock {} expiration: {error}", key.name)
                }
            }
        }
    });
}
