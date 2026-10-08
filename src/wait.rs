use crate::core::Core;
use crate::error::Result;
use fred::types::scripts::Script;
use std::future::Future;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::time::Instant;

pub(crate) const FALLBACK_POLL: Duration = Duration::from_secs(2);

pub(crate) static SET_IF_ABSENT_AND_PUBLISH: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if redis.call('EXISTS', KEYS[1]) == 0 then
            redis.call('SET', KEYS[1], ARGV[1])
            redis.call('PUBLISH', ARGV[3], ARGV[2])
            return 1
        end
        return 0",
    )
});

pub(crate) fn next_pause(deadline: Option<Instant>) -> Option<Duration> {
    match deadline {
        Some(deadline) => {
            let remaining = deadline.saturating_duration_since(Instant::now());
            (!remaining.is_zero()).then(|| remaining.min(FALLBACK_POLL))
        }
        None => Some(FALLBACK_POLL),
    }
}

pub(crate) async fn wait_on<T, F, Fut>(
    core: &Core,
    channel: &str,
    deadline: Option<Instant>,
    mut probe: F,
) -> Result<Option<T>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<T>>>,
{
    let subscription = core.pubsub.subscribe(channel).await?;
    let notify = subscription.notify();
    loop {
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        if let Some(found) = probe().await? {
            return Ok(Some(found));
        }

        let Some(pause) = next_pause(deadline) else {
            return Ok(None);
        };
        let _ = tokio::time::timeout(pause, notified).await;
    }
}
