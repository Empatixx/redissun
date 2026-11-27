use crate::client::request::Request;
use crate::coordination::rate_limiter::suffix_name;
use crate::coordination::semaphore::Semaphore;
use crate::error::{Error, Result};
use crate::object::{tagged, Key, Object};
use crate::pending::Pending;
use crate::pubsub::Subscription;
use crate::values::atomic_f64::AtomicF64;
use crate::values::atomic_i64::AtomicI64;
use bytes::Bytes;
use std::any::Any;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;
use tokio::sync::broadcast::{self, error::RecvError};
use tokio_util::sync::{CancellationToken, DropGuard};
use uuid::Uuid;

const RESET: &str = "0";
const SUM: &str = "1";
const SUM_WAIT: Duration = Duration::from_secs(60);

pub(crate) type Registry = Mutex<std::collections::HashMap<String, Weak<dyn Any + Send + Sync>>>;

mod sealed {
    use crate::error::Result;
    use crate::object::Key;
    use std::future::Future;
    use std::ops::{AddAssign, Neg};

    pub trait Number:
        Copy + Default + AddAssign + Neg<Output = Self> + Send + Sync + 'static
    {
        const ONE: Self;
        fn push(key: Key, value: Self) -> impl Future<Output = Result<()>> + Send;
        fn take(key: Key) -> impl Future<Output = Result<Self>> + Send;
    }
}

use sealed::Number;

/// The numbers an [`Adder`] can sum: `i64` and `f64`.
pub trait Summable: sealed::Number {}

impl Summable for i64 {}
impl Summable for f64 {}

impl Number for i64 {
    const ONE: Self = 1;

    async fn push(key: Key, value: Self) -> Result<()> {
        AtomicI64::new(key).add(value).await.map(drop)
    }

    async fn take(key: Key) -> Result<Self> {
        AtomicI64::new(key).take().await
    }
}

impl Number for f64 {
    const ONE: Self = 1.0;

    async fn push(key: Key, value: Self) -> Result<()> {
        AtomicF64::new(key).add(value).await.map(drop)
    }

    async fn take(key: Key) -> Result<Self> {
        AtomicF64::new(key).take().await
    }
}

fn channel(name: &str) -> String {
    format!("redissun_adder:{}", tagged(name))
}

fn part(key: &Key, id: &str, what: &str) -> Key {
    Key::new(
        key.core.clone(),
        suffix_name(key.name(), &format!("{id}:{what}")),
    )
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

struct Shared<N> {
    key: Key,
    local: Arc<Mutex<N>>,
    _stop: DropGuard,
}

impl<N> Drop for Shared<N> {
    fn drop(&mut self) {
        let mut adders = locked(&self.key.core.adders);
        if adders
            .get(self.key.name())
            .is_some_and(|adder| adder.strong_count() == 0)
        {
            adders.remove(self.key.name());
        }
    }
}

/// A counter that is cheap to add to and costly to read, like Redisson's `RLongAdder` (`Adder<i64>`) and `RDoubleAdder` (`Adder<f64>`).
///
/// [`add`](Adder::add) only changes a number in this program's memory and never talks to Redis. [`sum`](Adder::sum) asks every program that holds an adder of this name for its number and adds them up, and [`reset`](Adder::reset) sets them all to 0. The programs talk over the channel `redissun_adder:{name}`. Every adder of one name in one client is the same counter; dropping the last one leaves the group, and what it counted is gone.
pub struct Adder<N: Summable> {
    shared: Arc<Shared<N>>,
}

impl<N: Summable> Clone for Adder<N> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<N: Summable> fmt::Debug for Adder<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.shared.key.describe(f, "Adder")
    }
}

impl<N: Summable> Adder<N> {
    fn registered(
        adders: &std::collections::HashMap<String, Weak<dyn Any + Send + Sync>>,
        key: &Key,
    ) -> Result<Option<Self>> {
        let Some(found) = adders.get(key.name()).and_then(Weak::upgrade) else {
            return Ok(None);
        };
        match found.downcast::<Shared<N>>() {
            Ok(shared) => Ok(Some(Self { shared })),
            Err(_) => Err(Error::Config(format!(
                "the adder `{}` already counts another number type",
                key.name()
            ))),
        }
    }

    pub(crate) async fn open(key: Key) -> Result<Self> {
        if let Some(found) = Self::registered(&locked(&key.core.adders), &key)? {
            return Ok(found);
        }
        let (subscription, messages) = key
            .core
            .pubsub()
            .await?
            .subscribe_with_messages(&channel(key.name()))
            .await?;
        let local = Arc::new(Mutex::new(N::default()));
        let stop = CancellationToken::new();
        tokio::spawn(listen(
            key.clone(),
            local.clone(),
            subscription,
            messages,
            stop.clone(),
        ));
        let shared = Arc::new(Shared {
            key,
            local,
            _stop: stop.drop_guard(),
        });
        // Another call may have joined this name while this one was subscribing; the first one stays.
        let earlier = {
            let mut adders = locked(&shared.key.core.adders);
            let earlier = Self::registered(&adders, &shared.key)?;
            if earlier.is_none() {
                adders.insert(shared.key.name().to_string(), Arc::downgrade(&shared) as _);
            }
            earlier
        };
        Ok(earlier.unwrap_or(Self { shared }))
    }

    /// The name of the adder.
    pub fn name(&self) -> &str {
        self.shared.key.name()
    }

    /// Adds `value` to this program's number. It does not talk to Redis.
    pub fn add(&self, value: N) {
        *locked(&self.shared.local) += value;
    }

    /// Adds 1.
    pub fn incr(&self) {
        self.add(N::ONE);
    }

    /// Subtracts 1.
    pub fn decr(&self) {
        self.add(-N::ONE);
    }

    /// Adds up the numbers of every program. Like Redisson, it waits at most 60 seconds for their answers and then fails with [`Error::Timeout`]. Add `.timeout(duration)` to set the limit yourself; it then resolves to `None` when the time runs out.
    pub fn sum(&self) -> Pending<'_, N> {
        Pending::new(move |wait| async move {
            match self.ask(SUM, wait.or(Some(SUM_WAIT))).await? {
                Some(id) => Ok(Some(N::take(part(&self.shared.key, &id, "counter")).await?)),
                None => Ok(None),
            }
        })
    }

    /// Sets the number of every program to 0. Add `.timeout(duration)` to wait at most that long for them to confirm.
    pub fn reset(&self) -> Pending<'_, ()> {
        Pending::new(move |wait| async move { Ok(self.ask(RESET, wait).await?.map(drop)) })
    }

    async fn ask(&self, kind: &str, wait: Option<Duration>) -> Result<Option<String>> {
        let key = &self.shared.key;
        let id = Uuid::new_v4().simple().to_string();
        let channel = channel(key.name());
        let mut cmd = redis::cmd("PUBLISH");
        cmd.arg(&channel).arg(format!("{kind}:{id}"));
        // Routed by the channel's slot, as fred did: in Redis Cluster a reply counts only the receivers on the node that got it, and the adder waits for that many answers.
        let listeners: u64 = key
            .core
            .query(Request::new(cmd).key(channel.as_bytes()))
            .await?;
        let semaphore = Semaphore::new(part(key, &id, "semaphore"));
        let answered = match wait {
            Some(wait) => semaphore.acquire(listeners).timeout(wait).await?,
            None => Some(semaphore.acquire(listeners).await?),
        };
        let Some(permits) = answered else {
            return Ok(None);
        };
        permits.forget();
        semaphore.del().await?;
        Ok(Some(id))
    }
}

async fn listen<N: Summable>(
    key: Key,
    local: Arc<Mutex<N>>,
    _subscription: Subscription,
    mut messages: broadcast::Receiver<Bytes>,
    stop: CancellationToken,
) {
    loop {
        let message = tokio::select! {
            _ = stop.cancelled() => return,
            message = messages.recv() => message,
        };
        let payload = match message {
            Ok(payload) => payload,
            Err(RecvError::Lagged(_)) => continue,
            Err(RecvError::Closed) => return,
        };
        let Some((kind, id)) = std::str::from_utf8(&payload)
            .ok()
            .and_then(|text| text.split_once(':'))
        else {
            continue;
        };
        let answered = match kind {
            SUM => {
                let value = *locked(&local);
                N::push(part(&key, id, "counter"), value).await.is_ok()
            }
            RESET => {
                *locked(&local) = N::default();
                true
            }
            _ => continue,
        };
        // As in Redisson, a number that could not be sent is not confirmed, and a lost confirmation is left to the asker's time limit.
        if answered {
            let _ = Semaphore::new(part(&key, id, "semaphore")).release(1).await;
        }
    }
}
