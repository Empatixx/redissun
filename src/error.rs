use thiserror::Error;

/// Errors returned by redissun operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// The Redis server or the connection reported an error.
    #[error("redis error: {0}")]
    Redis(String),
    /// A value could not be encoded or decoded by the codec.
    #[error("codec error: {0}")]
    Codec(String),
    /// The client configuration is invalid.
    #[error("configuration error: {0}")]
    Config(String),
    /// The lock is not held by the caller, for example because its lease expired.
    #[error("lock is not held by the current owner")]
    LockNotHeld,
    /// The operation did not complete in time.
    #[error("operation timed out")]
    Timeout,
    /// The operation is not supported by this object.
    #[error("unsupported operation: {0}")]
    Unsupported(String),
}

/// Result alias used by every fallible redissun call.
pub type Result<T> = std::result::Result<T, Error>;

impl From<fred::error::Error> for Error {
    fn from(error: fred::error::Error) -> Self {
        Error::Redis(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_are_stable() {
        assert_eq!(Error::Redis("boom".into()).to_string(), "redis error: boom");
        assert_eq!(Error::Codec("bad".into()).to_string(), "codec error: bad");
        assert_eq!(
            Error::Config("missing".into()).to_string(),
            "configuration error: missing"
        );
        assert_eq!(
            Error::LockNotHeld.to_string(),
            "lock is not held by the current owner"
        );
        assert_eq!(Error::Timeout.to_string(), "operation timed out");
        assert_eq!(
            Error::Unsupported("rename".into()).to_string(),
            "unsupported operation: rename"
        );
    }
}
