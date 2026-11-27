//! The entry events of a [`HashMapCache`](super::HashMapCache): Redisson's `struct.pack('Lc0Lc0[Lc0]', …)` payload and the listener.

use crate::codec::Codec;
use crate::error::{Error, Result};
use crate::pubsub::Subscription;
use bytes::Bytes;
use futures::future::select_all;
use futures::{stream, Stream};
use serde::de::DeserializeOwned;
use std::fmt;
use std::marker::PhantomData;
use tokio::sync::broadcast::{self, error::RecvError, error::TryRecvError};

/// How the Redis server writes the `L` (native `unsigned long`) of `struct.pack`, Redisson's `BaseEventCodec.OSType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LengthPrefix {
    /// 8 bytes, little-endian: 64-bit Linux and macOS on x86 and ARM.
    U64Le,
    /// 4 bytes, little-endian: a Windows server.
    U32Le,
    /// 8 bytes, big-endian: HP NonStop.
    U64Be,
}

impl LengthPrefix {
    fn width(self) -> usize {
        match self {
            LengthPrefix::U32Le => 4,
            LengthPrefix::U64Le | LengthPrefix::U64Be => 8,
        }
    }

    /// The size of the `dLc0` header in front of every stored value: the 8-byte double and the length.
    #[allow(dead_code)]
    pub(crate) fn value_header(self) -> usize {
        8 + self.width()
    }

    fn read(self, bytes: &[u8]) -> Option<u64> {
        match self {
            LengthPrefix::U32Le => Some(u64::from(u32::from_le_bytes(bytes.try_into().ok()?))),
            LengthPrefix::U64Le => Some(u64::from_le_bytes(bytes.try_into().ok()?)),
            LengthPrefix::U64Be => Some(u64::from_be_bytes(bytes.try_into().ok()?)),
        }
    }
}

/// Picks the [`LengthPrefix`] from the reply of `INFO server`, as Redisson's `addListenerAsync` does: no `os` field or one that contains `Windows` gives 4 bytes, `NONSTOP` big-endian, anything else 8 bytes little-endian.
pub(crate) fn length_prefix(info_server: &str) -> LengthPrefix {
    let os = info_server
        .lines()
        .find_map(|line| line.trim_end_matches('\r').strip_prefix("os:"));
    match os {
        None => LengthPrefix::U32Le,
        Some(os) if os.contains("Windows") => LengthPrefix::U32Le,
        Some(os) if os.contains("NONSTOP") => LengthPrefix::U64Be,
        Some(_) => LengthPrefix::U64Le,
    }
}

/// Splits one `Lc0` part off the front of `payload`.
fn part(payload: &[u8], prefix: LengthPrefix) -> Option<(&[u8], &[u8])> {
    let width = prefix.width();
    let length = usize::try_from(prefix.read(payload.get(..width)?)?).ok()?;
    let end = width.checked_add(length)?;
    Some((payload.get(width..end)?, payload.get(end..)?))
}

/// The key, the value and the old value of an event payload.
type Parts<'a> = (&'a [u8], &'a [u8], Option<&'a [u8]>);

/// Splits an event payload into the key, the value and, when bytes remain, the old value. `None` when a length runs past the end.
pub(crate) fn split_event(payload: &[u8], prefix: LengthPrefix) -> Option<Parts<'_>> {
    let (key, rest) = part(payload, prefix)?;
    let (value, rest) = part(rest, prefix)?;
    if rest.is_empty() {
        return Some((key, value, None));
    }
    let (old, rest) = part(rest, prefix)?;
    rest.is_empty().then_some((key, value, Some(old)))
}

/// A change of a [`HashMapCache`](super::HashMapCache), received through [`EntryEvents`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum EntryEvent<K, V> {
    /// A new entry was added, or an expired one was written again.
    Created {
        /// The key of the entry.
        key: K,
        /// The new value.
        value: V,
    },
    /// The value of a live entry was replaced.
    Updated {
        /// The key of the entry.
        key: K,
        /// The new value.
        value: V,
        /// The value that was replaced.
        previous: V,
    },
    /// An entry was removed by [`HashMapCache::remove`](super::HashMapCache::remove) or dropped because the cache was full.
    Removed {
        /// The key of the entry.
        key: K,
        /// The value it had.
        value: V,
    },
    /// The background clean-up deleted an expired entry.
    Expired {
        /// The key of the entry.
        key: K,
        /// The value it had.
        value: V,
    },
}

/// The kinds of [`EntryEvent`], each published on its own channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EntryEventKind {
    /// [`EntryEvent::Created`].
    Created,
    /// [`EntryEvent::Updated`].
    Updated,
    /// [`EntryEvent::Removed`].
    Removed,
    /// [`EntryEvent::Expired`].
    Expired,
}

impl EntryEventKind {
    pub(crate) const ALL: [EntryEventKind; 4] = [
        EntryEventKind::Removed,
        EntryEventKind::Created,
        EntryEventKind::Updated,
        EntryEventKind::Expired,
    ];
}

/// Receives the [`EntryEvent`]s of a [`HashMapCache`](super::HashMapCache). Dropping the last listener of a channel in a client unsubscribes from Redis.
pub struct EntryEvents<K, V, C: Codec> {
    sources: Vec<(EntryEventKind, Subscription, broadcast::Receiver<Bytes>)>,
    prefix: LengthPrefix,
    codec: C,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V, C: Codec> fmt::Debug for EntryEvents<K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EntryEvents").finish_non_exhaustive()
    }
}

fn received(outcome: std::result::Result<Bytes, RecvError>) -> Result<Bytes> {
    match outcome {
        Ok(payload) => Ok(payload),
        Err(RecvError::Lagged(missed)) => Err(Error::Lagged(missed as usize)),
        Err(RecvError::Closed) => Err(Error::Redis("the subscription was closed".into())),
    }
}

impl<K, V, C: Codec> EntryEvents<K, V, C> {
    pub(crate) fn new(
        sources: Vec<(EntryEventKind, Subscription, broadcast::Receiver<Bytes>)>,
        prefix: LengthPrefix,
        codec: C,
    ) -> Self {
        Self {
            sources,
            prefix,
            codec,
            _marker: PhantomData,
        }
    }
}

impl<K, V, C> EntryEvents<K, V, C>
where
    K: DeserializeOwned + Send,
    V: DeserializeOwned + Send,
    C: Codec,
{
    fn parse(&self, kind: EntryEventKind, payload: &[u8]) -> Result<EntryEvent<K, V>> {
        let malformed = || Error::Codec("malformed cache event".into());
        let (key, value, previous) = split_event(payload, self.prefix).ok_or_else(malformed)?;
        let key = self.codec.decode(key)?;
        let value = self.codec.decode(value)?;
        match kind {
            EntryEventKind::Created => Ok(EntryEvent::Created { key, value }),
            EntryEventKind::Updated => Ok(EntryEvent::Updated {
                key,
                value,
                previous: self.codec.decode(previous.ok_or_else(malformed)?)?,
            }),
            EntryEventKind::Removed => Ok(EntryEvent::Removed { key, value }),
            EntryEventKind::Expired => Ok(EntryEvent::Expired { key, value }),
        }
    }

    fn buffered(&mut self) -> Option<(EntryEventKind, Result<Bytes>)> {
        for (kind, _, receiver) in self.sources.iter_mut() {
            match receiver.try_recv() {
                Ok(payload) => return Some((*kind, Ok(payload))),
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Lagged(missed)) => {
                    return Some((*kind, Err(Error::Lagged(missed as usize))))
                }
                Err(TryRecvError::Closed) => {
                    return Some((
                        *kind,
                        Err(Error::Redis("the subscription was closed".into())),
                    ))
                }
            }
        }
        None
    }

    /// Waits for the next event. Events of one kind arrive in order; events of different kinds come from different channels, and when several are waiting, removals are returned first, then creations, updates and expirations. A listener that falls more than 256 events behind on a channel gets [`Error::Lagged`] once and then continues with the newest events.
    pub async fn recv(&mut self) -> Result<EntryEvent<K, V>> {
        let (kind, payload) = match self.buffered() {
            Some((kind, payload)) => (kind, payload?),
            None => {
                let kinds: Vec<EntryEventKind> =
                    self.sources.iter().map(|(kind, _, _)| *kind).collect();
                let waiting = self
                    .sources
                    .iter_mut()
                    .map(|(_, _, receiver)| Box::pin(receiver.recv()));
                let (outcome, index, _) = select_all(waiting).await;
                let kind = kinds
                    .get(index)
                    .copied()
                    .ok_or_else(|| Error::Redis("the subscription was closed".into()))?;
                (kind, received(outcome)?)
            }
        };
        self.parse(kind, &payload)
    }

    /// Turns the listener into a stream of events. The stream never ends.
    pub fn into_stream(self) -> impl Stream<Item = Result<EntryEvent<K, V>>> {
        stream::unfold(self, |mut events| async move {
            Some((events.recv().await, events))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn part64(bytes: &[u8]) -> Vec<u8> {
        let mut out = (bytes.len() as u64).to_le_bytes().to_vec();
        out.extend_from_slice(bytes);
        out
    }

    #[test]
    fn parse_entry_event_reads_two_parts() {
        let payload = [
            1, 0, 0, 0, 0, 0, 0, 0, b'k', 2, 0, 0, 0, 0, 0, 0, 0, b'v', b'1',
        ];
        assert_eq!(
            split_event(&payload, LengthPrefix::U64Le),
            Some((&b"k"[..], &b"v1"[..], None))
        );
    }

    #[test]
    fn parse_entry_event_reads_the_old_value() {
        let payload = [part64(b"k"), part64(b"new"), part64(b"old")].concat();
        assert_eq!(
            split_event(&payload, LengthPrefix::U64Le),
            Some((&b"k"[..], &b"new"[..], Some(&b"old"[..])))
        );
    }

    #[test]
    fn parse_entry_event_keeps_colons_and_nuls() {
        let payload = [part64(b"k"), part64(b"a:\0:1")].concat();
        assert_eq!(
            split_event(&payload, LengthPrefix::U64Le),
            Some((&b"k"[..], &b"a:\0:1"[..], None))
        );
    }

    #[test]
    fn parse_entry_event_allows_an_empty_value() {
        let payload = [part64(b"k"), part64(b"")].concat();
        assert_eq!(
            split_event(&payload, LengthPrefix::U64Le),
            Some((&b"k"[..], &b""[..], None))
        );
    }

    #[test]
    fn parse_entry_event_uses_four_bytes_on_windows() {
        let payload = [1, 0, 0, 0, b'k', 1, 0, 0, 0, b'v'];
        assert_eq!(
            split_event(&payload, LengthPrefix::U32Le),
            Some((&b"k"[..], &b"v"[..], None))
        );
    }

    #[test]
    fn parse_entry_event_reads_big_endian_lengths_on_nonstop() {
        let payload = [0, 0, 0, 0, 0, 0, 0, 1, b'k', 0, 0, 0, 0, 0, 0, 0, 1, b'v'];
        assert_eq!(
            split_event(&payload, LengthPrefix::U64Be),
            Some((&b"k"[..], &b"v"[..], None))
        );
    }

    #[test]
    fn parse_entry_event_rejects_truncation() {
        let payload = [1, 0, 0, 0, 0, 0, 0, 0, b'k', 9, 0, 0, 0, 0, 0, 0, 0, b'v'];
        assert_eq!(split_event(&payload, LengthPrefix::U64Le), None);
        assert_eq!(split_event(&[], LengthPrefix::U64Le), None);
        assert_eq!(split_event(&[1, 0, 0], LengthPrefix::U64Le), None);
        let huge = [0xff; 8];
        assert_eq!(split_event(&huge, LengthPrefix::U64Le), None);
        let old_truncated = [
            part64(b"k"),
            part64(b"v"),
            vec![5, 0, 0, 0, 0, 0, 0, 0, b'o'],
        ]
        .concat();
        assert_eq!(split_event(&old_truncated, LengthPrefix::U64Le), None);
    }

    #[test]
    fn length_prefix_follows_the_os() {
        assert_eq!(
            length_prefix("# Server\r\nredis_version:7.4.0\r\nos:Linux 6.1 x86_64\r\n"),
            LengthPrefix::U64Le
        );
        assert_eq!(length_prefix("os:Windows"), LengthPrefix::U32Le);
        assert_eq!(length_prefix("os:NONSTOP"), LengthPrefix::U64Be);
        assert_eq!(length_prefix(""), LengthPrefix::U32Le);
        assert_eq!(LengthPrefix::U64Le.value_header(), 16);
        assert_eq!(LengthPrefix::U64Be.value_header(), 16);
        assert_eq!(LengthPrefix::U32Le.value_header(), 12);
    }
}
