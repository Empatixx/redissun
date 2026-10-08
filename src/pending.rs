use crate::error::{Error, Result};
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::time::Duration;

type Run<'a, T> = Box<
    dyn FnOnce(Option<Duration>) -> Pin<Box<dyn Future<Output = Result<Option<T>>> + Send + 'a>>
        + Send
        + 'a,
>;

/// A call that waits until it succeeds. Await it to wait without a limit, or call [`Pending::timeout`] first to wait at most that long.
///
/// ```ignore
/// let permits = semaphore.acquire(2).await?;
/// let maybe = semaphore.acquire(2).timeout(Duration::from_secs(1)).await?;
/// ```
#[must_use = "a pending call does nothing until it is awaited"]
pub struct Pending<'a, T> {
    run: Run<'a, T>,
}

impl<'a, T> Pending<'a, T> {
    pub(crate) fn new<F, Fut>(run: F) -> Self
    where
        F: FnOnce(Option<Duration>) -> Fut + Send + 'a,
        Fut: Future<Output = Result<Option<T>>> + Send + 'a,
    {
        Self {
            run: Box::new(move |wait| Box::pin(run(wait))),
        }
    }

    /// Waits at most `timeout`. The call then resolves to `None` when the time runs out.
    pub fn timeout(self, timeout: Duration) -> PendingTimeout<'a, T> {
        PendingTimeout {
            run: self.run,
            timeout,
        }
    }
}

impl<'a, T: 'a> IntoFuture for Pending<'a, T> {
    type Output = Result<T>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { (self.run)(None).await?.ok_or(Error::Timeout) })
    }
}

/// A [`Pending`] call with a time limit. It resolves to `None` when the time runs out.
#[must_use = "a pending call does nothing until it is awaited"]
pub struct PendingTimeout<'a, T> {
    run: Run<'a, T>,
    timeout: Duration,
}

impl<'a, T: 'a> IntoFuture for PendingTimeout<'a, T> {
    type Output = Result<Option<T>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        (self.run)(Some(self.timeout))
    }
}
