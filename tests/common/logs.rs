//! Captures what redissun logs through `tracing`, for tests that check a log line.

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::subscriber::DefaultGuard;

/// The lines logged while its guard lives, on the test's thread (a `#[tokio::test]` runs its tasks there).
#[derive(Clone, Default)]
pub struct Logs(Arc<Mutex<Vec<u8>>>);

impl Logs {
    /// What redissun logged; other crates (testcontainers) are left out.
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap())
            .lines()
            .filter(|line| line.contains(" redissun"))
            .map(|line| format!("{line}\n"))
            .collect()
    }

    /// The captured lines that contain `needle`.
    pub fn lines_with(&self, needle: &str) -> Vec<String> {
        self.text()
            .lines()
            .filter(|line| line.contains(needle))
            .map(str::to_string)
            .collect()
    }

    pub fn contains(&self, needle: &str) -> bool {
        !self.lines_with(needle).is_empty()
    }

    /// Waits up to `limit` for a line with `needle`.
    pub async fn wait_for(&self, needle: &str, limit: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + limit;
        while tokio::time::Instant::now() < deadline {
            if self.contains(needle) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        self.contains(needle)
    }
}

struct Sink(Arc<Mutex<Vec<u8>>>);

impl Write for Sink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Starts capturing every level; the capture ends with the guard.
pub fn capture() -> (DefaultGuard, Logs) {
    let logs = Logs::default();
    let buffer = logs.0.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_ansi(false)
        .without_time()
        .with_writer(move || Sink(buffer.clone()))
        .finish();
    (tracing::subscriber::set_default(subscriber), logs)
}
