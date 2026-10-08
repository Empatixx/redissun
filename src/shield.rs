use crate::error::{Error, Result};
use std::future::Future;
use tokio::sync::oneshot;

pub(crate) async fn shielded<T, F>(work: F) -> Result<T>
where
    T: Send + 'static,
    F: Future<Output = Result<T>> + Send + 'static,
{
    let (sender, receiver) = oneshot::channel();
    tokio::spawn(async move {
        let _ = sender.send(work.await);
    });
    receiver
        .await
        .map_err(|_| Error::Redis("the background task ended without a reply".into()))?
}
