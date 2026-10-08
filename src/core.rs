use crate::error::{Error, Result};
use crate::pubsub::PubSub;
use bytes::Bytes;
use fred::clients::{Client as RedisClient, Pool};
use fred::interfaces::{ClientInterface, ClientLike};
use fred::prelude::{Builder, Config, FromValue, ReconnectPolicy};
use fred::types::scripts::Script;
use fred::types::ClientUnblockFlag;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use uuid::Uuid;

pub(crate) struct Core {
    pool: Pool,
    id: String,
    pub(crate) lock_lease: Duration,
    pub(crate) pubsub: Arc<PubSub>,
}

impl Core {
    pub(crate) async fn connect(
        url: &str,
        pool_size: usize,
        lock_lease: Duration,
        connect_timeout: Duration,
    ) -> Result<Arc<Self>> {
        let config = Config::from_url(url).map_err(|e| Error::Config(e.to_string()))?;
        let mut builder = Builder::from_config(config);
        builder.set_policy(ReconnectPolicy::new_exponential(0, 100, 30_000, 2));
        let pool = builder
            .build_pool(pool_size)
            .map_err(|e| Error::Config(e.to_string()))?;
        match tokio::time::timeout(connect_timeout, pool.init()).await {
            Ok(connected) => {
                connected?;
            }
            Err(_) => {
                let abandoned = pool.clone();
                tokio::spawn(async move {
                    let _ = abandoned.quit().await;
                });
                return Err(Error::Timeout);
            }
        }
        let pubsub = PubSub::start(pool.next()).await?;
        Ok(Arc::new(Self {
            pool,
            id: Uuid::new_v4().to_string(),
            lock_lease,
            pubsub,
        }))
    }

    pub(crate) fn redis(&self) -> &RedisClient {
        self.pool.next()
    }

    pub(crate) async fn blocking_client(&self) -> Result<BlockingClient> {
        let client = self.redis().clone_new();
        client.init().await?;
        let id: i64 = client.client_id().await?;
        Ok(BlockingClient {
            client,
            admin: self.redis().clone(),
            id,
        })
    }

    pub(crate) fn client_id(&self) -> &str {
        &self.id
    }

    pub(crate) fn owner(&self) -> String {
        match tokio::task::try_id() {
            Some(task) => format!("{}:{}", self.id, task),
            None => format!("{}:main", self.id),
        }
    }

    pub(crate) async fn eval<R: FromValue>(
        &self,
        script: &Script,
        keys: Vec<String>,
        args: Vec<Bytes>,
    ) -> Result<R> {
        Ok(script.evalsha_with_reload(self.redis(), keys, args).await?)
    }
}

pub(crate) struct BlockingClient {
    client: RedisClient,
    admin: RedisClient,
    id: i64,
}

impl std::ops::Deref for BlockingClient {
    type Target = RedisClient;

    fn deref(&self) -> &RedisClient {
        &self.client
    }
}

impl Drop for BlockingClient {
    fn drop(&mut self) {
        if let Ok(runtime) = Handle::try_current() {
            let client = self.client.clone();
            let admin = self.admin.clone();
            let id = self.id;
            runtime.spawn(async move {
                let _ = admin
                    .client_unblock::<i64, _>(id, Some(ClientUnblockFlag::Error))
                    .await;
                let _ = client.quit().await;
            });
        }
    }
}

impl Drop for Core {
    fn drop(&mut self) {
        let Ok(runtime) = Handle::try_current() else {
            return;
        };
        let pool = self.pool.clone();
        let pubsub = self.pubsub.clone();
        runtime.spawn(async move {
            let _ = pool.quit().await;
            pubsub.quit().await;
        });
    }
}
