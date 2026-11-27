//! An in-memory [`MapLoader`]/[`MapWriter`] that records its calls, for the loader and writer tests.

use futures::Stream;
use redissun::{BoxError, MapLoader, MapWriter};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct Inner {
    data: Mutex<BTreeMap<String, String>>,
    loads: AtomicUsize,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    write_calls: Mutex<Vec<Vec<String>>>,
    delete_calls: Mutex<Vec<Vec<String>>>,
    fail_next: AtomicUsize,
    fail_always: AtomicBool,
    slow: Mutex<Duration>,
}

/// A store over a `BTreeMap<String, String>`; values of other types are kept as their JSON text.
#[derive(Clone, Default)]
pub struct MemoryStore(Arc<Inner>);

fn to_text<V: Serialize>(value: &V) -> String {
    match serde_json::to_value(value).unwrap() {
        serde_json::Value::String(text) => text,
        other => other.to_string(),
    }
}

fn from_text<V: DeserializeOwned>(text: &str) -> V {
    serde_json::from_str(text)
        .or_else(|_| serde_json::from_value(serde_json::Value::String(text.to_string())))
        .unwrap()
}

impl MemoryStore {
    pub fn with(entries: &[(&str, &str)]) -> Self {
        let store = Self::default();
        store
            .0
            .data
            .lock()
            .unwrap()
            .extend(entries.iter().map(|(k, v)| (k.to_string(), v.to_string())));
        store
    }

    pub fn snapshot(&self) -> BTreeMap<String, String> {
        self.0.data.lock().unwrap().clone()
    }

    pub fn loads(&self) -> usize {
        self.0.loads.load(Ordering::SeqCst)
    }

    pub fn max_in_flight(&self) -> usize {
        self.0.max_in_flight.load(Ordering::SeqCst)
    }

    pub fn write_calls(&self) -> Vec<Vec<String>> {
        self.0.write_calls.lock().unwrap().clone()
    }

    pub fn delete_calls(&self) -> Vec<Vec<String>> {
        self.0.delete_calls.lock().unwrap().clone()
    }

    /// The next `n` calls fail with `"injected"`.
    pub fn fail_next(&self, n: usize) {
        self.0.fail_next.store(n, Ordering::SeqCst);
    }

    pub fn fail_always(&self) {
        self.0.fail_always.store(true, Ordering::SeqCst);
    }

    /// Every call sleeps `delay` first.
    pub fn slow(&self, delay: Duration) {
        *self.0.slow.lock().unwrap() = delay;
    }

    /// Runs one call: counts it in flight, sleeps when slow, and fails when told to.
    async fn call<T>(&self, body: impl FnOnce() -> T) -> Result<T, BoxError> {
        let now = self.0.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.0.max_in_flight.fetch_max(now, Ordering::SeqCst);
        let delay = *self.0.slow.lock().unwrap();
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        let failed = self.0.fail_always.load(Ordering::SeqCst) || take_one(&self.0.fail_next);
        let result = if failed {
            Err("injected".into())
        } else {
            Ok(body())
        };
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
        result
    }
}

impl<V> MapLoader<String, V> for MemoryStore
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn load(&self, key: &String) -> Result<Option<V>, BoxError> {
        self.0.loads.fetch_add(1, Ordering::SeqCst);
        self.call(|| {
            self.0
                .data
                .lock()
                .unwrap()
                .get(key)
                .map(|text| from_text(text))
        })
        .await
    }

    fn load_all_keys(&self) -> impl Stream<Item = Result<String, BoxError>> + Send {
        let keys: Vec<String> = self.0.data.lock().unwrap().keys().cloned().collect();
        futures::stream::iter(keys.into_iter().map(Ok))
    }
}

impl<V> MapWriter<String, V> for MemoryStore
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn write(&self, entries: &[(String, V)]) -> Result<(), BoxError> {
        self.0
            .write_calls
            .lock()
            .unwrap()
            .push(entries.iter().map(|(k, _)| k.clone()).collect());
        self.call(|| {
            let mut data = self.0.data.lock().unwrap();
            for (k, v) in entries {
                data.insert(k.clone(), to_text(v));
            }
        })
        .await
    }

    async fn delete(&self, keys: &[String]) -> Result<(), BoxError> {
        self.0.delete_calls.lock().unwrap().push(keys.to_vec());
        self.call(|| {
            let mut data = self.0.data.lock().unwrap();
            for k in keys {
                data.remove(k);
            }
        })
        .await
    }
}

/// Takes one from `n` unless it is zero; returns whether it did.
fn take_one(n: &std::sync::atomic::AtomicUsize) -> bool {
    let mut current = n.load(Ordering::SeqCst);
    while current > 0 {
        match n.compare_exchange(current, current - 1, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return true,
            Err(actual) => current = actual,
        }
    }
    false
}
