use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("redis error: {0}")]
    Redis(String),
    #[error("codec error: {0}")]
    Codec(String),
    #[error("configuration error: {0}")]
    Config(String),
    #[error("lock is not held by the current owner")]
    LockNotHeld,
    #[error("operation timed out")]
    Timeout,
}

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
    }
}
