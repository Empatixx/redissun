use crate::client::core::{Core, Evictor};
use crate::client::request::Request;
use crate::error::Result;
use crate::lua::Lua;
use bytes::Bytes;
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

/// The arguments of an eviction script for one run, given the latch time of that run (`None` when it runs without a latch).
pub(crate) type Arguments = fn(Option<Duration>) -> Vec<Bytes>;

/// Schedules the clean-up `task` of one object, unless it already runs in this client.
///
/// While any of the `wait_for` keys exists (they must share the slot of `keys`), a run skips the script; once one check finds none of them, the task stops checking. An empty `wait_for` never waits.
pub(crate) fn schedule(
    core: &Arc<Core>,
    task: String,
    script: &'static Lua,
    keys: Vec<String>,
    args: Arguments,
    wait_for: Vec<String>,
) {
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
        args,
        wait_for,
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

async fn run(
    core: Weak<Core>,
    script: &'static Lua,
    keys: Vec<String>,
    args: Arguments,
    mut wait_for: Vec<String>,
    min: Duration,
) {
    let mut pacing = Pacing::new(min);
    loop {
        tokio::time::sleep(pacing.delay).await;
        let Some(core) = core.upgrade() else {
            return;
        };
        if !wait_for.is_empty() {
            if !nothing_exists(&core, &wait_for).await {
                continue;
            }
            wait_for = Vec::new();
        }
        let latch = pacing.delay.min(MAX_LATCH);
        let outcome: Result<i64> = core
            .eval_once(script, keys.clone(), args(Some(latch)))
            .await;
        drop(core);
        if let Ok(size) = outcome {
            if size >= 0 {
                pacing.record(size);
            }
        }
    }
}

/// Whether none of `keys` exists, with one `EXISTS` routed by the first key; an error counts as "something may exist".
async fn nothing_exists(core: &Core, keys: &[String]) -> bool {
    let Some(first) = keys.first() else {
        return true;
    };
    let mut cmd = redis::cmd("EXISTS");
    for key in keys {
        cmd.arg(key);
    }
    let found: Result<i64> = core.query(Request::new(cmd).key(first.as_bytes())).await;
    matches!(found, Ok(0))
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
