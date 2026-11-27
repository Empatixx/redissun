//! The sync messages, the channel and the updates log of a local cache.
//!
//! A message is the sender's 16-byte instance id, one kind byte, then the body: `I` the key, `U` `[key_len: u32 BE][key][value]`, `E` `[ttl_timeout: i64 BE][idle_delta: i64 BE][key_len: u32 BE][key][value]`, `C` nothing.

use crate::object::{tagged, Key};
use bytes::Bytes;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

pub(crate) const ID_LEN: usize = 16;
pub(crate) const LOG_ID_LEN: usize = 8;
pub(crate) const INVALIDATE: u8 = b'I';
pub(crate) const UPDATE: u8 = b'U';
pub(crate) const CLEAR: u8 = b'C';
/// An update with the entry's deadlines, so the receiver's copy does not outlive the entry.
pub(crate) const EXPIRING_UPDATE: u8 = b'E';
pub(crate) const UPDATES_LOG_TIME: Duration = Duration::from_secs(10 * 60);
pub(crate) const UPDATES_LOG_KEEP: Duration = Duration::from_secs(11 * 60);

pub(crate) fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as i64)
}

pub(crate) fn message(id: &[u8; ID_LEN], kind: u8, body: &[&[u8]]) -> Bytes {
    let mut payload =
        std::vec::Vec::with_capacity(ID_LEN + 1 + body.iter().map(|p| p.len()).sum::<usize>());
    payload.extend_from_slice(id);
    payload.push(kind);
    body.iter().for_each(|part| payload.extend_from_slice(part));
    Bytes::from(payload)
}

/// An `E` message: `key` now holds `value` until the deadline `ttl_timeout` (the writer's Unix ms) and with the max idle `idle_delta` (ms), `0` meaning no limit.
pub(crate) fn expiring_update(
    id: &[u8; ID_LEN],
    ttl_timeout: i64,
    idle_delta: i64,
    key: &[u8],
    value: &[u8],
) -> Bytes {
    message(
        id,
        EXPIRING_UPDATE,
        &[
            &ttl_timeout.to_be_bytes(),
            &idle_delta.to_be_bytes(),
            &(key.len() as u32).to_be_bytes(),
            key,
            value,
        ],
    )
}

pub(crate) fn channel(key: &Key) -> String {
    format!("redissun__local_cache:{}", key.redis_key())
}

pub(crate) fn updates_log(key: &Key) -> String {
    updates_log_name(key.name())
}

/// The updates log of the object `name`, in its slot.
pub(crate) fn updates_log_name(name: &str) -> String {
    format!("redissun__cache_updates_log:{}", tagged(name))
}

/// The scripts' `now`, updates log entry (8 random bytes, then the key) and keep time for a change of `field`.
pub(crate) fn log_arguments(field: &Bytes) -> [Bytes; 3] {
    let mut entry = Uuid::new_v4().as_bytes()[..LOG_ID_LEN].to_vec();
    entry.extend_from_slice(field);
    [
        Bytes::from(now_millis().to_string()),
        Bytes::from(entry),
        Bytes::from(UPDATES_LOG_KEEP.as_millis().to_string()),
    ]
}

#[cfg(test)]
mod tests {
    use super::super::local::Local;
    use super::super::options::{CacheProvider, EvictionPolicy, ReconnectionStrategy};
    use super::super::store::LocalStore;
    use super::*;

    fn local() -> Local {
        Local::new(
            LocalStore::new(CacheProvider::Builtin, EvictionPolicy::None, 0, None, None),
            ReconnectionStrategy::None,
            false,
        )
    }

    #[tokio::test]
    async fn an_expiring_update_round_trips() {
        let a = local();
        let b = [7u8; ID_LEN];
        let k = Bytes::from_static(b"k");
        let now = now_millis();
        a.apply(&expiring_update(&b, now + 60_000, 0, b"k", b"v"));
        assert_eq!(a.lookup(&k), Some(Some(Bytes::from_static(b"v"))));

        a.apply(&expiring_update(&b, now - 1, 0, b"k", b"w"));
        assert_eq!(a.lookup(&k), None);

        a.apply(&expiring_update(&a.id, now + 60_000, 0, b"k", b"x"));
        assert_eq!(a.lookup(&k), None);
    }
}
