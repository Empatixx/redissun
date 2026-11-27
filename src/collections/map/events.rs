//! Redisson's map listeners (`MapPutListener`, `MapIncrListener`, `MapRemoveListener`, `DeletedObjectListener`, `ExpiredObjectListener`) as one stream of events.

use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::object::Key;
use crate::pubsub::{PatternMessage, Subscription};
use futures::future::select_all;
use futures::{stream, Stream};
use serde::de::DeserializeOwned;
use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::marker::PhantomData;
use std::time::Duration;
use tokio::sync::broadcast::{self, error::RecvError, error::TryRecvError};
use tokio::time::Instant;

/// How long a key event waits for the subkey event of the same write, Redisson's `MAP_FIELD_EVENT_DEDUPLICATION_TIMEOUT`.
const DEDUPLICATION: Duration = Duration::from_millis(50);

/// A change of a map, from [`HashMap::events`](crate::HashMap::events).
///
/// `key` names the changed entry when the server sends subkey notifications (Redis 8.8 or newer with `T` in `notify-keyspace-events`); otherwise it is `None`, as in Redisson, whose listeners then get `null`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum MapEvent<K> {
    /// An entry was written (`hset`), Redisson's `MapPutListener`.
    Put {
        /// The written key, when the server names it.
        key: Option<K>,
    },
    /// A value was incremented as a float (`hincrbyfloat`), Redisson's `MapIncrListener`.
    Incremented {
        /// The incremented key, when the server names it.
        key: Option<K>,
    },
    /// An entry was removed (`hdel`), Redisson's `MapRemoveListener`.
    Removed {
        /// The removed key, when the server names it.
        key: Option<K>,
    },
    /// The whole map was deleted (`del`), Redisson's `DeletedObjectListener`.
    Deleted,
    /// The whole map expired (`expired`), Redisson's `ExpiredObjectListener`.
    Expired,
}

/// The kinds of [`MapEvent`], to listen to only some with [`HashMap::events_of`](crate::HashMap::events_of).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum MapEventKind {
    /// [`MapEvent::Put`].
    Put,
    /// [`MapEvent::Incremented`].
    Incremented,
    /// [`MapEvent::Removed`].
    Removed,
    /// [`MapEvent::Deleted`].
    Deleted,
    /// [`MapEvent::Expired`].
    Expired,
}

impl MapEventKind {
    pub(crate) const ALL: [MapEventKind; 5] = [
        MapEventKind::Put,
        MapEventKind::Incremented,
        MapEventKind::Removed,
        MapEventKind::Deleted,
        MapEventKind::Expired,
    ];

    /// The patterns Redisson subscribes to for this kind: the subkey event first, when there is one.
    fn patterns(self) -> &'static [&'static str] {
        match self {
            MapEventKind::Put => &["__subkeyevent@*:hset", "__keyevent@*:hset"],
            MapEventKind::Incremented => {
                &["__subkeyevent@*:hincrbyfloat", "__keyevent@*:hincrbyfloat"]
            }
            MapEventKind::Removed => &["__subkeyevent@*:hdel", "__keyevent@*:hdel"],
            MapEventKind::Deleted => &["__keyevent@*:del"],
            MapEventKind::Expired => &["__keyevent@*:expired"],
        }
    }
}

/// The field event for the event name at the end of a channel, with the key it names.
fn field_event<K>(name: &str, key: Option<K>) -> Option<MapEvent<K>> {
    match name {
        "hset" => Some(MapEvent::Put { key }),
        "hincrbyfloat" => Some(MapEvent::Incremented { key }),
        "hdel" => Some(MapEvent::Removed { key }),
        _ => None,
    }
}

/// The changes of one map, from [`HashMap::events`](crate::HashMap::events). Dropping it stops listening, Redisson's `removeListener`.
///
/// Redis sends these events only when its `notify-keyspace-events` setting enables them, as for Redisson: `E` with `h` for the field events, `g` for [`MapEvent::Deleted`] and `x` for [`MapEvent::Expired`], and `T` (Redis 8.8 or newer) for the names of changed keys. Like [`Keys::listen`](crate::Keys::listen), it hears the node of the client's pub/sub connection, which in a cluster is one master.
pub struct MapEvents<K, C: Codec> {
    name: String,
    codec: C,
    sources: std::vec::Vec<(
        Subscription<PatternMessage>,
        broadcast::Receiver<PatternMessage>,
    )>,
    // Event names for which a subkey event arrived; their key events are duplicates from then on.
    subkey_seen: HashSet<String>,
    // Events in the order the server sent them, each with the time it may be returned. A key event of a field waits there for the subkey event of the same write and carries its event name so that one can drop it; everything else is due at once.
    queue: VecDeque<Queued<K>>,
    // A field whose subkey events are dropped: `HashMapCache`'s increment writes and deletes a temporary field.
    skip_field: Option<&'static [u8]>,
    _marker: PhantomData<fn() -> K>,
}

impl<K, C: Codec> fmt::Debug for MapEvents<K, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MapEvents")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

struct Queued<K> {
    due: Instant,
    key_event: Option<String>,
    event: Result<MapEvent<K>>,
}

/// Subscribes to the patterns of `kinds` for the map at `key`. Subkey events of `skip_field` are dropped before decoding.
pub(crate) async fn subscribe<K, C: Codec>(
    key: &Key,
    codec: C,
    kinds: &[MapEventKind],
    skip_field: Option<&'static [u8]>,
) -> Result<MapEvents<K, C>> {
    let pubsub = key.core.pubsub().await?.clone();
    let mut sources = std::vec::Vec::new();
    for kind in MapEventKind::ALL {
        if kinds.contains(&kind) {
            for pattern in kind.patterns() {
                sources.push(pubsub.psubscribe_with_messages(pattern).await?);
            }
        }
    }
    if sources.is_empty() {
        return Err(Error::Config("listen to at least one kind of event".into()));
    }
    Ok(MapEvents {
        name: key.redis_key(),
        codec,
        sources,
        subkey_seen: HashSet::new(),
        queue: VecDeque::new(),
        skip_field,
        _marker: PhantomData,
    })
}

fn received(outcome: std::result::Result<PatternMessage, RecvError>) -> Result<PatternMessage> {
    match outcome {
        Ok(message) => Ok(message),
        Err(RecvError::Lagged(missed)) => Err(Error::Lagged(missed as usize)),
        Err(RecvError::Closed) => Err(Error::Redis("the subscription was closed".into())),
    }
}

impl<K, C> MapEvents<K, C>
where
    K: DeserializeOwned + Send,
    C: Codec,
{
    fn buffered(&mut self) -> Option<Result<PatternMessage>> {
        for (_, receiver) in self.sources.iter_mut() {
            match receiver.try_recv() {
                Ok(message) => return Some(Ok(message)),
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Lagged(missed)) => {
                    return Some(Err(Error::Lagged(missed as usize)))
                }
                Err(TryRecvError::Closed) => {
                    return Some(Err(Error::Redis("the subscription was closed".into())))
                }
            }
        }
        None
    }

    fn push_now(&mut self, event: Result<MapEvent<K>>) {
        self.queue.push_back(Queued {
            due: Instant::now(),
            key_event: None,
            event,
        });
    }

    /// Queues the events of one message.
    fn take(&mut self, (channel, payload): PatternMessage) {
        let event_name = channel.rsplit(':').next().unwrap_or_default().to_string();
        if channel.starts_with("__subkeyevent@") {
            let Some((name, fields)) = parse_subkey(&payload) else {
                return;
            };
            if name != self.name.as_bytes() {
                return;
            }
            // From now on the key events of this name repeat what the subkey events say.
            self.queue
                .retain(|queued| queued.key_event.as_deref() != Some(event_name.as_str()));
            self.subkey_seen.insert(event_name.clone());
            for field in fields {
                if self.skip_field == Some(field) {
                    continue;
                }
                let event = self
                    .codec
                    .decode::<K>(field)
                    .map(|key| field_event(&event_name, Some(key)));
                match event {
                    Ok(Some(event)) => self.push_now(Ok(event)),
                    Ok(None) => {}
                    Err(error) => self.push_now(Err(error)),
                }
            }
            return;
        }
        if payload.as_ref() != self.name.as_bytes() {
            return;
        }
        match event_name.as_str() {
            "del" => self.push_now(Ok(MapEvent::Deleted)),
            "expired" => self.push_now(Ok(MapEvent::Expired)),
            name if self.subkey_seen.contains(name) => {}
            name => {
                if let Some(event) = field_event(name, None) {
                    self.queue.push_back(Queued {
                        due: Instant::now() + DEDUPLICATION,
                        key_event: Some(event_name),
                        event: Ok(event),
                    });
                }
            }
        }
    }

    /// Waits for the next event. Events of one kind arrive in the order of the writes; events of different kinds come from different subscriptions and may be reordered among themselves. A field event without subkey notifications arrives 50 ms after its write, the time Redisson waits for a subkey event of the same write. A listener that falls more than 256 messages behind gets [`Error::Lagged`] once and then continues with the newest.
    pub async fn recv(&mut self) -> Result<MapEvent<K>> {
        loop {
            // Read what has already arrived first: a subkey event in it may drop a waiting key event.
            if let Some(message) = self.buffered() {
                self.take(message?);
                continue;
            }
            if self
                .queue
                .front()
                .is_some_and(|queued| queued.due <= Instant::now())
            {
                if let Some(queued) = self.queue.pop_front() {
                    return queued.event;
                }
            }
            let deadline = self.queue.front().map(|queued| queued.due);
            let waiting = self
                .sources
                .iter_mut()
                .map(|(_, receiver)| Box::pin(receiver.recv()));
            let message = match deadline {
                Some(deadline) => tokio::select! {
                    (outcome, _, _) = select_all(waiting) => Some(received(outcome)?),
                    () = tokio::time::sleep_until(deadline) => None,
                },
                None => {
                    let (outcome, _, _) = select_all(waiting).await;
                    Some(received(outcome)?)
                }
            };
            if let Some(message) = message {
                self.take(message);
            }
        }
    }

    /// Turns the listener into a stream of events. The stream never ends.
    pub fn into_stream(self) -> impl Stream<Item = Result<MapEvent<K>>> {
        stream::unfold(self, |mut events| async move {
            Some((events.recv().await, events))
        })
    }
}

/// Splits a subkey event, `<length>:<key>|<length>:<field>[,<length>:<field>…]`, as Redisson's `decodeSubkeyEventMessage` does.
fn parse_subkey(payload: &[u8]) -> Option<(&[u8], std::vec::Vec<&[u8]>)> {
    let (name, rest) = length_prefixed(payload)?;
    let rest = rest.strip_prefix(b"|")?;
    let mut fields = std::vec::Vec::new();
    let mut rest = rest;
    loop {
        let (field, after) = length_prefixed(rest)?;
        fields.push(field);
        if after.is_empty() {
            return Some((name, fields));
        }
        rest = after.strip_prefix(b",")?;
    }
}

/// `<length>:<bytes>` at the start of `input`, and what follows.
fn length_prefixed(input: &[u8]) -> Option<(&[u8], &[u8])> {
    let colon = input.iter().position(|&byte| byte == b':')?;
    let length: usize = std::str::from_utf8(&input[..colon]).ok()?.parse().ok()?;
    let start = colon + 1;
    let end = start.checked_add(length)?;
    Some((input.get(start..end)?, input.get(end..)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_subkey_event() {
        let (name, fields) = parse_subkey(b"1:m|2:f1,2:f2").unwrap();
        assert_eq!(name, b"m");
        assert_eq!(fields, vec![&b"f1"[..], &b"f2"[..]]);
    }

    #[test]
    fn parses_fields_with_separators_inside() {
        let (name, fields) = parse_subkey(b"3:a|b|5:x,y:z").unwrap();
        assert_eq!(name, b"a|b");
        assert_eq!(fields, vec![&b"x,y:z"[..]]);
    }

    #[test]
    fn rejects_malformed_subkey_events() {
        assert!(parse_subkey(b"5:m|2:f1").is_none());
        assert!(parse_subkey(b"1:m2:f1").is_none());
        assert!(parse_subkey(b"1:m|").is_none());
        assert!(parse_subkey(b"1:m|9:f1").is_none());
        assert!(parse_subkey(b"1:m|2:f1;2:f2").is_none());
        assert!(parse_subkey(b"x:m|2:f1").is_none());
    }
}
