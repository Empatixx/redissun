use crate::core::{Core, Evictor};
use crate::error::Result;
use bytes::Bytes;
use fred::types::scripts::Script;
use std::collections::VecDeque;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::Notify;

pub(crate) const KEYS_LIMIT: i64 = 100;
const MAX_DELAY: Duration = Duration::from_secs(30 * 60);
const MAX_LATCH: Duration = Duration::from_secs(30);

pub(crate) fn latch_key(name: &str) -> String {
    format!("redissun__execute_task_once_latch:{name}")
}

pub(crate) fn arguments(latch: Option<Duration>) -> Vec<Bytes> {
    let latch_millis = latch.map_or(0, |latch| latch.as_millis().max(1));
    vec![
        Bytes::from(KEYS_LIMIT.to_string()),
        Bytes::from(latch_millis.to_string()),
        Bytes::from(if latch.is_some() { "1" } else { "0" }),
    ]
}

pub(crate) fn schedule(core: &Arc<Core>, task: String, script: &'static Script, keys: Vec<String>) {
    let Ok(runtime) = Handle::try_current() else {
        return;
    };
    let mut tasks = core.evictors.lock().unwrap_or_else(|e| e.into_inner());
    if tasks
        .get(&task)
        .is_some_and(|evictor| !evictor.handle.is_finished())
    {
        return;
    }
    let handle = runtime.spawn(run(
        Arc::downgrade(core),
        script,
        keys,
        core.eviction_interval,
    ));
    tasks.insert(
        task,
        Evictor {
            handle,
            wake: Arc::new(Notify::new()),
        },
    );
}

struct Pacing {
    delay: Duration,
    min: Duration,
    max: Duration,
    history: VecDeque<i64>,
}

impl Pacing {
    fn new(min: Duration) -> Self {
        Self {
            delay: min,
            min,
            max: MAX_DELAY.max(min),
            history: VecDeque::with_capacity(3),
        }
    }

    fn record(&mut self, size: i64) {
        if self.history.len() == 2 {
            let first = self.history[0];
            let last = self.history[1];
            if first > last && last > size {
                self.delay = self.delay.mul_f64(1.5).min(self.max);
            }
            if first == last && last == size {
                if size >= KEYS_LIMIT {
                    self.delay = (self.delay / 4).max(self.min);
                }
                if size == 0 {
                    self.delay = self.delay.mul_f64(1.5).min(self.max);
                }
            }
            self.history.pop_front();
        }
        self.history.push_back(size);
    }
}

async fn run(core: Weak<Core>, script: &'static Script, keys: Vec<String>, min: Duration) {
    let mut pacing = Pacing::new(min);
    loop {
        tokio::time::sleep(pacing.delay).await;
        let Some(core) = core.upgrade() else {
            return;
        };
        let latch = pacing.delay.min(MAX_LATCH);
        let outcome: Result<i64> = core
            .eval_no_retry(script, keys.clone(), arguments(Some(latch)))
            .await;
        drop(core);
        if let Ok(size) = outcome {
            if size >= 0 {
                pacing.record(size);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_delay_grows_after_three_empty_runs_and_stops_at_thirty_minutes() {
        let mut pacing = Pacing::new(Duration::from_secs(5));
        pacing.record(0);
        pacing.record(0);
        assert_eq!(pacing.delay, Duration::from_secs(5));
        pacing.record(0);
        assert_eq!(pacing.delay, Duration::from_millis(7500));
        for _ in 0..100 {
            pacing.record(0);
        }
        assert_eq!(pacing.delay, MAX_DELAY);
    }

    #[test]
    fn the_delay_shrinks_while_full_batches_keep_coming() {
        let mut pacing = Pacing::new(Duration::from_secs(5));
        pacing.delay = Duration::from_secs(100);
        for _ in 0..3 {
            pacing.record(KEYS_LIMIT);
        }
        assert_eq!(pacing.delay, Duration::from_secs(25));
        for _ in 0..10 {
            pacing.record(KEYS_LIMIT);
        }
        assert_eq!(pacing.delay, Duration::from_secs(5));
    }

    #[test]
    fn a_falling_count_grows_the_delay() {
        let mut pacing = Pacing::new(Duration::from_secs(10));
        pacing.record(30);
        pacing.record(20);
        pacing.record(10);
        assert_eq!(pacing.delay, Duration::from_secs(15));
        pacing.record(10);
        assert_eq!(pacing.delay, Duration::from_secs(15));
    }
}
