use crate::error::{Error, Result};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::runtime::Handle;

/// Runs `work` to the end even if the caller stops waiting for it.
///
/// The work runs in the caller's task. Only when the caller drops the future before it finished is the rest of the work handed to a new task, so a lock that was taken or released on the server is not left half done. That costs nothing when the caller waits, unlike a task for every call.
pub(crate) async fn shielded<T, F>(work: F) -> Result<T>
where
    T: Send + 'static,
    F: Future<Output = Result<T>> + Send + 'static,
{
    Shielded {
        work: Some(Box::pin(work)),
    }
    .await
}

struct Shielded<T, F>
where
    T: Send + 'static,
    F: Future<Output = Result<T>> + Send + 'static,
{
    work: Option<Pin<Box<F>>>,
}

impl<T, F> Future for Shielded<T, F>
where
    T: Send + 'static,
    F: Future<Output = Result<T>> + Send + 'static,
{
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<T>> {
        // `shielded` awaits this once, so it is never polled after it finished; an error instead of a panic keeps that a bug report, not a crash.
        let Some(work) = self.work.as_mut() else {
            return Poll::Ready(Err(Error::Redis(
                "a shielded call was polled after it finished".into(),
            )));
        };
        let output = std::task::ready!(work.as_mut().poll(cx));
        self.work = None;
        Poll::Ready(output)
    }
}

impl<T, F> Drop for Shielded<T, F>
where
    T: Send + 'static,
    F: Future<Output = Result<T>> + Send + 'static,
{
    fn drop(&mut self) {
        if let (Some(work), Ok(runtime)) = (self.work.take(), Handle::try_current()) {
            // What the work returns is dropped there: a lock guard it returns releases the lock.
            runtime.spawn(work);
        }
    }
}
