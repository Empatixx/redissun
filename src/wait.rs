use std::time::Duration;
use tokio::time::Instant;

pub(crate) const FALLBACK_POLL: Duration = Duration::from_secs(2);

pub(crate) fn next_pause(deadline: Option<Instant>) -> Option<Duration> {
    match deadline {
        Some(deadline) => {
            let remaining = deadline.saturating_duration_since(Instant::now());
            (!remaining.is_zero()).then(|| remaining.min(FALLBACK_POLL))
        }
        None => Some(FALLBACK_POLL),
    }
}
