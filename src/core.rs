use crate::error::{Error, Result};
use crate::pubsub::PubSub;
use bytes::Bytes;
use fred::clients::{Client as RedisClient, Pool};
use fred::interfaces::ClientLike;
use fred::prelude::{Builder, Config, FromValue, ReconnectPolicy};
use fred::types::scripts::Script;
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
