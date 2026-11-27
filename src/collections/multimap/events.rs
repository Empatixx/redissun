//! Redisson's multimap listeners (`MapPutListener`, `MapRemoveListener`, `ListAddListener`/`SetAddListener`, `ListRemoveListener`/`SetRemoveListener`, `DeletedObjectListener`, `ExpiredObjectListener`) as one stream of events.
//!
//! The hash events come from [`MapEvents`]. The value events are `__keyevent@*:rpush`/`lrem` (list) or `sadd`/`srem` (set) whose key starts with the multimap's prefix, as Redisson's `m.startsWith(prefix)` filter (so a multimap `m` also hears the values keys of one named `{m}:x`).

use super::{prefix, Shape};
use crate::codec::Codec;
use crate::collections::map::events::{self, MapEvent, MapEventKind, MapEvents};
use crate::error::{Error, Result};
use crate::object::Key;
use crate::pubsub::{PatternMessage, Subscription};
use futures::future::select_all;
use futures::{stream, Stream};
use serde::de::DeserializeOwned;
use std::fmt;
use tokio::sync::broadcast::{self, error::RecvError};

/// A change of a multimap, from `events()` or `events_of(..)` on any multimap.
///
/// `key` names the changed key when the server sends subkey notifications (Redis 8.8 or newer with `T` in `notify-keyspace-events`); otherwise it is `None`, as in Redisson, whose listeners then get `null`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum MultimapEvent<K> {
    /// A key's field in the hash was written (`hset`), Redisson's `MapPutListener`. The put scripts write the field on every put, so this fires for a key that already exists as well, not only when a key is added.
    Put {
        /// The written key, when the server names it.
        key: Option<K>,
    },
    /// A key was removed from the hash (`hdel`), Redisson's `MapRemoveListener`.
    Removed {
        /// The removed key, when the server names it.
        key: Option<K>,
    },
    /// Values were added to the list or set of a key (`rpush`/`sadd`), Redisson's `ListAddListener`/`SetAddListener`.
    #[doc(alias = "ListAddListener")]
    #[doc(alias = "SetAddListener")]
    ValueAdded {
        /// The name of the values key, `{name}:<key hash>`.
        name: String,
    },
    /// Values were removed from the list or set of a key (`lrem`/`srem`), Redisson's `ListRemoveListener`/`SetRemoveListener`.
    #[doc(alias = "ListRemoveListener")]
    #[doc(alias = "SetRemoveListener")]
    ValueRemoved {
        /// The name of the values key, `{name}:<key hash>`.
        name: String,
    },
    /// The hash was deleted (`del`), Redisson's `DeletedObjectListener`.
    Deleted,
    /// The hash expired (`expired`), Redisson's `ExpiredObjectListener`.
    Expired,
}

/// The kinds of [`MultimapEvent`], to listen to only some with `events_of`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum MultimapEventKind {
    /// [`MultimapEvent::Put`].
    Put,
    /// [`MultimapEvent::Removed`].
    Removed,
    /// [`MultimapEvent::ValueAdded`].
    ValueAdded,
    /// [`MultimapEvent::ValueRemoved`].
    ValueRemoved,
    /// [`MultimapEvent::Deleted`].
    Deleted,
    /// [`MultimapEvent::Expired`].
    Expired,
}

impl MultimapEventKind {
    pub(crate) const ALL: [MultimapEventKind; 6] = [
        MultimapEventKind::Put,
        MultimapEventKind::Removed,
        MultimapEventKind::ValueAdded,
        MultimapEventKind::ValueRemoved,
        MultimapEventKind::Deleted,
        MultimapEventKind::Expired,
    ];

    /// The map kind this kind reads from the hash events, if any.
    fn map_kind(self) -> Option<MapEventKind> {
        match self {
            MultimapEventKind::Put => Some(MapEventKind::Put),
            MultimapEventKind::Removed => Some(MapEventKind::Removed),
            MultimapEventKind::Deleted => Some(MapEventKind::Deleted),
            MultimapEventKind::Expired => Some(MapEventKind::Expired),
            MultimapEventKind::ValueAdded | MultimapEventKind::ValueRemoved => None,
        }
    }

    /// The key event Redisson subscribes to for a value kind.
    fn value_pattern(self, shape: Shape) -> Option<&'static str> {
        match (self, shape) {
            (MultimapEventKind::ValueAdded, Shape::List) => Some("__keyevent@*:rpush"),
            (MultimapEventKind::ValueRemoved, Shape::List) => Some("__keyevent@*:lrem"),
            (MultimapEventKind::ValueAdded, Shape::Set) => Some("__keyevent@*:sadd"),
            (MultimapEventKind::ValueRemoved, Shape::Set) => Some("__keyevent@*:srem"),
            _ => None,
        }
    }
}

/// The changes of one multimap. Dropping it stops listening, Redisson's `removeListener`.
///
/// Redis sends these events only when its `notify-keyspace-events` setting enables them, as for Redisson: `E` with `h` for [`Put`](MultimapEvent::Put) and [`Removed`](MultimapEvent::Removed), `l` (list) or `s` (set) for the value events, `g` for [`Deleted`](MultimapEvent::Deleted), `x` for [`Expired`](MultimapEvent::Expired), and `T` (Redis 8.8 or newer) for the names of changed keys. It hears the node of the client's pub/sub connection, which in a cluster is one master.
///
/// The value events are kept when the values key starts with the multimap's prefix (`{name}:`), as Redisson's `startsWith` filter. So listening on `m` also hears the values keys of a multimap named `{m}:x`, whose values keys are `{m}:x:<hash>`, as in Redisson.
pub struct MultimapEvents<K, C: Codec> {
    prefix: String,
    map: Option<MapEvents<K, C>>,
    values: std::vec::Vec<(
        Subscription<PatternMessage>,
        broadcast::Receiver<PatternMessage>,
    )>,
}

impl<K, C: Codec> fmt::Debug for MultimapEvents<K, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultimapEvents")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

/// Subscribes to the patterns of `kinds` for the multimap at `key`. No kinds are an [`Error::Config`].
pub(crate) async fn subscribe<K, C: Codec>(
    key: &Key,
    codec: C,
    shape: Shape,
    kinds: &[MultimapEventKind],
) -> Result<MultimapEvents<K, C>> {
    if kinds.is_empty() {
        return Err(Error::Config("listen to at least one kind of event".into()));
    }
    let map_kinds: std::vec::Vec<MapEventKind> = MultimapEventKind::ALL
        .iter()
        .filter(|kind| kinds.contains(kind))
        .filter_map(|kind| kind.map_kind())
        .collect();
    let map = if map_kinds.is_empty() {
        None
    } else {
        Some(events::subscribe(key, codec, &map_kinds, None, false).await?)
    };
    let pubsub = key.core.pubsub().await?.clone();
    let mut values = std::vec::Vec::new();
    for kind in MultimapEventKind::ALL {
        if kinds.contains(&kind) {
            if let Some(pattern) = kind.value_pattern(shape) {
                values.push(pubsub.psubscribe_with_messages(pattern).await?);
            }
        }
    }
    Ok(MultimapEvents {
        prefix: prefix(key.name()),
        map,
        values,
    })
}

/// The multimap event of a map event; `None` for kinds a multimap does not report.
fn from_map<K>(event: MapEvent<K>) -> Option<MultimapEvent<K>> {
    match event {
        MapEvent::Put { key } => Some(MultimapEvent::Put { key }),
        MapEvent::Removed { key } => Some(MultimapEvent::Removed { key }),
        MapEvent::Deleted => Some(MultimapEvent::Deleted),
        MapEvent::Expired => Some(MultimapEvent::Expired),
        _ => None,
    }
}

/// The next message of any value subscription.
async fn next_value(
    values: &mut [(
        Subscription<PatternMessage>,
        broadcast::Receiver<PatternMessage>,
    )],
) -> Result<PatternMessage> {
    let waiting = values
        .iter_mut()
        .map(|(_, receiver)| Box::pin(receiver.recv()));
    let (outcome, _, _) = select_all(waiting).await;
    match outcome {
        Ok(message) => Ok(message),
        Err(RecvError::Lagged(missed)) => Err(Error::Lagged(missed as usize)),
        Err(RecvError::Closed) => Err(Error::Redis("the subscription was closed".into())),
    }
}

impl<K, C> MultimapEvents<K, C>
where
    K: DeserializeOwned + Send,
    C: Codec,
{
    /// The value event of a message, when it is about a values key of this multimap.
    fn value_event(&self, (channel, payload): PatternMessage) -> Option<MultimapEvent<K>> {
        if !payload.starts_with(self.prefix.as_bytes()) {
            return None;
        }
        let name = String::from_utf8_lossy(&payload).into_owned();
        match channel.rsplit(':').next() {
            Some("rpush" | "sadd") => Some(MultimapEvent::ValueAdded { name }),
            Some("lrem" | "srem") => Some(MultimapEvent::ValueRemoved { name }),
            _ => None,
        }
    }

    /// Waits for the next event. Events of one kind arrive in the order of the writes; events of different kinds come from different subscriptions and may be reordered among themselves. A hash event without subkey notifications arrives 50 ms after its write, as on [`MapEvents`]. A listener that falls more than 256 messages behind gets [`Error::Lagged`] once and then continues with the newest.
    pub async fn recv(&mut self) -> Result<MultimapEvent<K>> {
        loop {
            let message = match (&mut self.map, self.values.is_empty()) {
                (Some(map), true) => {
                    if let Some(event) = from_map(map.recv().await?) {
                        return Ok(event);
                    }
                    continue;
                }
                (Some(map), false) => tokio::select! {
                    event = map.recv() => {
                        if let Some(event) = from_map(event?) {
                            return Ok(event);
                        }
                        continue;
                    }
                    message = next_value(&mut self.values) => message?,
                },
                (None, false) => next_value(&mut self.values).await?,
                (None, true) => {
                    return Err(Error::Config("listen to at least one kind of event".into()))
                }
            };
            if let Some(event) = self.value_event(message) {
                return Ok(event);
            }
        }
    }

    /// Turns the listener into a stream of events. The stream never ends.
    pub fn into_stream(self) -> impl Stream<Item = Result<MultimapEvent<K>>> {
        stream::unfold(self, |mut events| async move {
            Some((events.recv().await, events))
        })
    }
}
