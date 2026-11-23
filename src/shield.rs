use crate::error::Result;
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

struct Shielded<F: Future + Send + 'static>
where
    F::Output: Send + 'static,
{
    work: Option<Pin<Box<F>>>,
}

impl<F> Future for Shielded<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let work = self
            .work
            .as_mut()
            .expect("a shielded future is not polled after it finished");
        let output = std::task::ready!(work.as_mut().poll(cx));
        self.work = None;
        Poll::Ready(output)
    }
}

impl<F> Drop for Shielded<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn drop(&mut self) {
        if let (Some(work), Ok(runtime)) = (self.work.take(), Handle::try_current()) {
            // What the work returns is dropped there: a lock guard it returns releases the lock.
            runtime.spawn(work);
        }
    }
}
