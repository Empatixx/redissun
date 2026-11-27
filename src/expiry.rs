//! When written keys or hash fields expire: [`Expiry`] and its command options.

use crate::error::{Error, Result};
use crate::object::millis;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// When the keys written by [`Buckets::update`](crate::Buckets::update) and [`Buckets::try_set`](crate::Buckets::try_set) expire, Redisson's `SetArgs`; also the time [`MapCacheNative::extend`](crate::MapCacheNative::extend) and its all-or-nothing forms give entries, Redisson's `PutArgs`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expiry {
    /// The keys get no expiry; keys that are overwritten lose theirs.
    None,
    /// The keys expire after this time (`PX`), Redisson's `timeToLive`.
    After(Duration),
    /// The keys expire at this time (`PXAT`), Redisson's `expireAt`.
    At(SystemTime),
    /// Keys that are overwritten keep their time to live (`KEEPTTL`), Redisson's `keepTTL`.
    KeepTtl,
}

/// The option and argument of `expiry` in `MSETEX` and `HSETEX`: `PX <ms>`, `PXAT <unix ms>` or `KEEPTTL`.
pub(crate) fn expiry_args(expiry: Expiry) -> Result<Option<(&'static str, Option<i64>)>> {
    Ok(match expiry {
        Expiry::None => None,
        Expiry::After(ttl) => Some(("PX", Some(millis(ttl)?))),
        Expiry::At(time) => {
            let since = time
                .duration_since(UNIX_EPOCH)
                .map_err(|_| Error::Config("time is before 1970".into()))?;
            let at = i64::try_from(since.as_millis())
                .map_err(|_| Error::Config("time is too far away".into()))?;
            Some(("PXAT", Some(at)))
        }
        Expiry::KeepTtl => Some(("KEEPTTL", None)),
    })
}

#[cfg(test)]
mod tests {
    use super::{expiry_args, Expiry};
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn expiry_becomes_the_option_redisson_sends() {
        assert_eq!(expiry_args(Expiry::None).unwrap(), None);
        assert_eq!(
            expiry_args(Expiry::After(Duration::from_millis(1500))).unwrap(),
            Some(("PX", Some(1500)))
        );
        assert_eq!(
            expiry_args(Expiry::At(UNIX_EPOCH + Duration::from_secs(2))).unwrap(),
            Some(("PXAT", Some(2000)))
        );
        assert_eq!(
            expiry_args(Expiry::KeepTtl).unwrap(),
            Some(("KEEPTTL", None))
        );
    }
}
