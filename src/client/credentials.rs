use crate::error::Result;
use futures::future::BoxFuture;
use std::fmt;
use std::future::Future;
use std::sync::Arc;

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
}

impl Resolver {
    pub(crate) fn new<F, Fut>(resolve: F) -> Self
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Credentials>> + Send + 'static,
    {
        Self {
            resolve: Arc::new(move |address| Box::pin(resolve(address))),
        }
    }
}

impl Resolver {
    /// Asks for the credentials of a new connection to `address` (`host:port`).
    pub(crate) async fn resolve(&self, address: &str) -> Result<Credentials> {
        (self.resolve)(address.to_string()).await
    }
}

impl fmt::Debug for Resolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Resolver").finish_non_exhaustive()
    }
}
