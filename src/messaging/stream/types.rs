use crate::error::{Error, Result};
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

/// The id of a stream entry: a millisecond time and a sequence number, written `1700000000000-0`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StreamId(pub(super) String);

impl StreamId {
    /// The smallest possible id, as an open lower end of a range (`-`).
    pub fn min() -> Self {
        Self("-".into())
    }

    /// The largest possible id, as an open upper end of a range (`+`).
    pub fn max() -> Self {
        Self("+".into())
    }

    /// The id `0-0`, which is before every entry. Reading after it returns everything.
    pub fn zero() -> Self {
        Self("0-0".into())
    }

    /// The id of the newest entry at the time of the call (`$`). Use it to read only entries added later.
    pub fn latest() -> Self {
        Self("$".into())
    }

    /// The id `millis-sequence`.
    pub fn new(millis: u64, sequence: u64) -> Self {
        Self(format!("{millis}-{sequence}"))
    }

    /// The id `millis-*` for [`StreamAdd::id`]: Redis picks the next sequence number for that time.
    pub fn auto_sequence(millis: u64) -> Self {
        Self(format!("{millis}-*"))
    }

    /// This id as an exclusive end of a [`Stream::range`] (`(id`). [`StreamId::min`] and [`StreamId::max`] stay as they are.
    pub fn exclusive(&self) -> Self {
        match self.0.as_str() {
            "-" | "+" => self.clone(),
            id => Self(format!("({id}")),
        }
    }

    /// The id as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for StreamId {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self> {
        let number = |part: &str| {
            !part.is_empty()
                && part.bytes().all(|byte| byte.is_ascii_digit())
                && part.parse::<u64>().is_ok()
        };
        let well_formed = match text.split_once('-') {
            Some((time, sequence)) => number(time) && number(sequence),
            None => number(text),
        };
        if well_formed || matches!(text, "-" | "+" | "$") {
            Ok(Self(text.to_string()))
        } else {
            Err(Error::Config(format!("{text:?} is not a stream id")))
        }
    }
}

/// One entry of a [`Stream`]: its id and its fields in the order they were added.
#[derive(Clone, Debug, PartialEq)]
pub struct StreamEntry<K, V> {
    /// The id of the entry.
    pub id: StreamId,
    /// The fields of the entry.
    pub fields: Vec<(K, V)>,
}

/// Summary of the entries a group has delivered and not yet acknowledged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingSummary {
    /// Number of pending entries.
    pub count: u64,
    /// The smallest pending id.
    pub first: Option<StreamId>,
    /// The largest pending id.
    pub last: Option<StreamId>,
    /// Each consumer with its number of pending entries.
    pub consumers: Vec<(String, u64)>,
}

/// One entry that was delivered to a consumer and not yet acknowledged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingEntry {
    /// The id of the entry.
    pub id: StreamId,
    /// The consumer that holds it.
    pub consumer: String,
    /// Time since it was last delivered.
    pub idle: Duration,
    /// How many times it was delivered.
    pub deliveries: u64,
}

/// The result of [`Stream::auto_claim`].
#[derive(Clone, Debug, PartialEq)]
pub struct AutoClaim<K, V> {
    /// The id to continue the scan from; `0-0` when the scan is complete.
    pub next: StreamId,
    /// The entries now held by the claiming consumer.
    pub entries: Vec<StreamEntry<K, V>>,
    /// Pending ids whose entries no longer exist; Redis dropped them from the pending list.
    pub deleted: Vec<StreamId>,
}

/// Information about a stream, from `XINFO STREAM`.
#[derive(Clone, Debug, PartialEq)]
pub struct StreamInfo<K, V> {
    /// Number of entries.
    pub length: u64,
    /// Number of keys in the underlying radix tree.
    pub radix_tree_keys: u64,
    /// Number of nodes in the underlying radix tree.
    pub radix_tree_nodes: u64,
    /// Number of consumer groups.
    pub groups: u64,
    /// The id of the newest entry ever added.
    pub last_generated_id: StreamId,
    /// The oldest entry, if any.
    pub first_entry: Option<StreamEntry<K, V>>,
    /// The newest entry, if any.
    pub last_entry: Option<StreamEntry<K, V>>,
}

/// A consumer group of a stream, from `XINFO GROUPS`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamGroup {
    /// The group name.
    pub name: String,
    /// Number of consumers.
    pub consumers: u64,
    /// Number of delivered and not yet acknowledged entries.
    pub pending: u64,
    /// The id of the last entry delivered to the group.
    pub last_delivered_id: StreamId,
}

/// A consumer of a group, from `XINFO CONSUMERS`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamConsumer {
    /// The consumer name.
    pub name: String,
    /// Number of entries it holds unacknowledged.
    pub pending: u64,
    /// Time since it last interacted with the group.
    pub idle: Duration,
}
