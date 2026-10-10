use crate::error::Result;
use fred::error::{Error as FredError, ErrorKind};
use fred::types::config::{CredentialProvider, Server};
use futures::future::BoxFuture;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// A username and password for `AUTH`, like Redisson's `Credentials`. `Debug` never prints the password.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Credentials {
    /// The ACL user, or `None` for the `default` user.
    pub username: Option<String>,
    /// The password, or `None` to skip `AUTH`.
    pub password: Option<String>,
}

impl Credentials {
    /// Credentials of the ACL user `username`.
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: Some(username.into()),
            password: Some(password.into()),
        }
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("username", &self.username)
            .field("password_set", &self.password.is_some())
            .finish()
    }
}

type Resolve = dyn Fn(String) -> BoxFuture<'static, Result<Credentials>> + Send + Sync;

#[derive(Clone)]
pub(crate) struct Resolver {
    resolve: Arc<Resolve>,
    pub(crate) refresh: Option<Duration>,
}

impl Resolver {
    pub(crate) fn new<F, Fut>(resolve: F) -> Self
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Credentials>> + Send + 'static,
    {
        Self {
            resolve: Arc::new(move |address| Box::pin(resolve(address))),
            refresh: None,
        }
    }
}

impl fmt::Debug for Resolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Resolver")
            .field("refresh", &self.refresh)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl CredentialProvider for Resolver {
    async fn fetch(
        &self,
        server: Option<&Server>,
    ) -> std::result::Result<(Option<String>, Option<String>), FredError> {
        let address = server.map(|server| server.to_string()).unwrap_or_default();
        let credentials = (self.resolve)(address)
            .await
            .map_err(|error| FredError::new(ErrorKind::Auth, error.to_string()))?;
        Ok((credentials.username, credentials.password))
    }

    fn refresh_interval(&self) -> Option<Duration> {
        self.refresh
    }
}
