use crate::client::Client;
use crate::codec::{Codec, JsonCodec};
use crate::core::Core;
use crate::error::{Error, Result};
use std::time::Duration;

pub struct ClientBuilder<C: Codec = JsonCodec> {
    url: Option<String>,
    pool_size: usize,
    lock_lease: Duration,
    codec: C,
}

impl ClientBuilder<JsonCodec> {
    pub(crate) fn new() -> Self {
        Self {
            url: None,
            pool_size: 4,
            lock_lease: Duration::from_secs(30),
            codec: JsonCodec,
        }
    }
}

impl<C: Codec> ClientBuilder<C> {
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    pub fn pool_size(mut self, pool_size: usize) -> Self {
        self.pool_size = pool_size;
        self
    }

    pub fn lock_lease(mut self, lock_lease: Duration) -> Self {
        self.lock_lease = lock_lease;
        self
    }

    pub fn codec<N: Codec>(self, codec: N) -> ClientBuilder<N> {
        ClientBuilder {
            url: self.url,
            pool_size: self.pool_size,
            lock_lease: self.lock_lease,
            codec,
        }
    }

    pub async fn build(self) -> Result<Client<C>> {
        let url = self
            .url
            .ok_or_else(|| Error::Config("url is required".into()))?;
        if self.pool_size == 0 {
            return Err(Error::Config("pool_size must be at least 1".into()));
        }
        if self.lock_lease.is_zero() {
            return Err(Error::Config("lock_lease must be positive".into()));
        }
        let core = Core::connect(&url, self.pool_size, self.lock_lease).await?;
        Ok(Client::from_parts(core, self.codec))
    }
}
