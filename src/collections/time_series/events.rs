//! Redisson's time series listeners (`ScoredSortedSetAddListener`, `ScoredSortedSetRemoveListener`, `DeletedObjectListener`, `ExpiredObjectListener`) as one stream of events.

use crate::error::{Error, Result};
use crate::object::Key;
use crate::pubsub::{PatternMessage, Subscription};
use futures::future::select_all;
use futures::{stream, Stream};
use std::fmt;
use tokio::sync::broadcast::{self, error::RecvError};

/// A change of a [`TimeSeries`](crate::TimeSeries), from [`events`](crate::TimeSeries::events).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TimeSeriesEvent {
    /// Entries were added (`zadd` on the series), Redisson's `ScoredSortedSetAddListener`.
    #[doc(alias = "ScoredSortedSetAddListener")]
    Added,
    /// Entries were removed (`zrem` on the series), Redisson's `ScoredSortedSetRemoveListener`.
    #[doc(alias = "ScoredSortedSetRemoveListener")]
    Removed,
    /// The series was deleted (`del`), Redisson's `DeletedObjectListener`.
    #[doc(alias = "DeletedObjectListener")]
    Deleted,
    /// The series expired (`expired`), Redisson's `ExpiredObjectListener`.
    #[doc(alias = "ExpiredObjectListener")]
    Expired,
}

/// The kinds of [`TimeSeriesEvent`], to listen to only some with [`events_of`](crate::TimeSeries::events_of).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TimeSeriesEventKind {
    /// [`TimeSeriesEvent::Added`].
    Added,
    /// [`TimeSeriesEvent::Removed`].
    Removed,
    /// [`TimeSeriesEvent::Deleted`].
    Deleted,
    /// [`TimeSeriesEvent::Expired`].
    Expired,
}

impl TimeSeriesEventKind {
    pub(crate) const ALL: [TimeSeriesEventKind; 4] = [
        TimeSeriesEventKind::Added,
        TimeSeriesEventKind::Removed,
        TimeSeriesEventKind::Deleted,
        TimeSeriesEventKind::Expired,
    ];

    /// The key event Redisson subscribes to.
    fn pattern(self) -> &'static str {
        match self {
            TimeSeriesEventKind::Added => "__keyevent@*:zadd",
            TimeSeriesEventKind::Removed => "__keyevent@*:zrem",
            TimeSeriesEventKind::Deleted => "__keyevent@*:del",
            TimeSeriesEventKind::Expired => "__keyevent@*:expired",
        }
    }
}

/// The changes of one [`TimeSeries`](crate::TimeSeries). Dropping it stops listening, Redisson's `removeListener`.
///
/// Redis sends these events only when its `notify-keyspace-events` setting enables them, as for Redisson: `E` with `z` for [`Added`](TimeSeriesEvent::Added) and [`Removed`](TimeSeriesEvent::Removed), `g` for [`Deleted`](TimeSeriesEvent::Deleted) and `x` for [`Expired`](TimeSeriesEvent::Expired). An event counts only when its key is the series' main sorted set, so the writes to the expiry set are not reported. It hears the node of the client's pub/sub connection, which in a cluster is one master.
pub struct TimeSeriesEvents {
    name: String,
    sources: Vec<(
        Subscription<PatternMessage>,
        broadcast::Receiver<PatternMessage>,
    )>,
}

impl fmt::Debug for TimeSeriesEvents {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimeSeriesEvents")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// Subscribes to the patterns of `kinds` for the series at `key`. No kinds are an [`Error::Config`].
pub(super) async fn subscribe(
    key: &Key,
    kinds: &[TimeSeriesEventKind],
) -> Result<TimeSeriesEvents> {
    if kinds.is_empty() {
        return Err(Error::Config("listen to at least one kind of event".into()));
    }
    let pubsub = key.core.pubsub().await?.clone();
    let mut sources = Vec::new();
    for kind in TimeSeriesEventKind::ALL {
        if kinds.contains(&kind) {
            sources.push(pubsub.psubscribe_with_messages(kind.pattern()).await?);
        }
    }
    Ok(TimeSeriesEvents {
        name: key.redis_key(),
        sources,
    })
}

impl TimeSeriesEvents {
    /// The event of a message, when it is about this series.
    fn event(&self, (channel, payload): PatternMessage) -> Option<TimeSeriesEvent> {
        if payload != self.name.as_bytes() {
            return None;
        }
        match channel.rsplit(':').next() {
            Some("zadd") => Some(TimeSeriesEvent::Added),
            Some("zrem") => Some(TimeSeriesEvent::Removed),
            Some("del") => Some(TimeSeriesEvent::Deleted),
            Some("expired") => Some(TimeSeriesEvent::Expired),
            _ => None,
        }
    }

    /// Waits for the next event. Events of different kinds come from different subscriptions and may be reordered among themselves. A listener that falls more than 256 messages behind gets [`Error::Lagged`] once and then continues with the newest.
    pub async fn recv(&mut self) -> Result<TimeSeriesEvent> {
        loop {
            let waiting = self
                .sources
                .iter_mut()
                .map(|(_, receiver)| Box::pin(receiver.recv()));
            let (outcome, _, _) = select_all(waiting).await;
            let message = match outcome {
                Ok(message) => message,
                Err(RecvError::Lagged(missed)) => return Err(Error::Lagged(missed as usize)),
                Err(RecvError::Closed) => {
                    return Err(Error::Redis("the subscription was closed".into()))
                }
            };
            if let Some(event) = self.event(message) {
                return Ok(event);
            }
        }
    }

    /// Turns the listener into a stream of events. The stream never ends.
    pub fn into_stream(self) -> impl Stream<Item = Result<TimeSeriesEvent>> {
        stream::unfold(self, |mut events| async move {
            Some((events.recv().await, events))
        })
    }
}
