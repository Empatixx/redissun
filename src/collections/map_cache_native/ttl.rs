//! A field's time to live as a script argument: `hpexpire <ms>` or `hpexpireat <unix ms>`.

use crate::error::{Error, Result};
use crate::object::millis;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The time a write gives an entry: `.ttl(d)` or `.expire_at(t)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FieldTtl {
    /// Expire `d` after the write, on the server's clock. Zero means no time to live.
    After(Duration),
    /// Expire at `t`, on the caller's clock.
    At(SystemTime),
}

impl FieldTtl {
    /// The script's `ARGV`: milliseconds and the command, Redisson's `getExpireCommand`. `None` for a zero TTL, which takes the path without a time, as Redisson's `ms == 0` does.
    pub(crate) fn script_args(self) -> Result<Option<(i64, &'static str)>> {
        match self {
            FieldTtl::After(d) if d.is_zero() => Ok(None),
            FieldTtl::After(d) => Ok(Some((millis(d)?, "hpexpire"))),
            FieldTtl::At(t) => {
                let since = t
                    .duration_since(UNIX_EPOCH)
                    .map_err(|_| Error::Config("time is before 1970".into()))?;
                let ms = i64::try_from(since.as_millis())
                    .map_err(|_| Error::Config("time is too far in the future".into()))?;
                Ok(Some((ms, "hpexpireat")))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn a_zero_ttl_is_no_ttl() {
        assert_eq!(FieldTtl::After(Duration::ZERO).script_args().unwrap(), None);
    }

    #[test]
    fn a_ttl_is_hpexpire_in_millis() {
        assert_eq!(
            FieldTtl::After(Duration::from_millis(1500))
                .script_args()
                .unwrap(),
            Some((1500, "hpexpire"))
        );
    }

    #[test]
    fn a_deadline_is_hpexpireat_in_unix_millis() {
        assert_eq!(
            FieldTtl::At(UNIX_EPOCH + Duration::from_secs(10))
                .script_args()
                .unwrap(),
            Some((10_000, "hpexpireat"))
        );
    }

    #[test]
    fn a_deadline_before_1970_is_a_config_error() {
        assert!(matches!(
            FieldTtl::At(UNIX_EPOCH - Duration::from_secs(1)).script_args(),
            Err(Error::Config(_))
        ));
    }

    #[test]
    fn a_ttl_too_long_is_a_config_error() {
        assert!(matches!(
            FieldTtl::After(Duration::MAX).script_args(),
            Err(Error::Config(_))
        ));
    }
}
