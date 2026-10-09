use crate::error::{Error, Result};
use crate::pubsub::PubSub;
use bytes::Bytes;
use fred::clients::{Client as RedisClient, Pool};
use fred::interfaces::{ClientInterface, ClientLike, LuaInterface};
use fred::prelude::{Builder, Config, FromValue, Options, ReconnectPolicy};
use fred::types::client::ClientKillFilter;
use fred::types::scripts::Script;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use uuid::Uuid;

pub(crate) struct Evictor {
    pub(crate) handle: tokio::task::JoinHandle<()>,
    pub(crate) wake: Arc<tokio::sync::Notify>,
}

pub(crate) struct Core {
    pool: Pool,
    id: String,
    pub(crate) lock_lease: Duration,
    pub(crate) lock_settings: crate::lock::LockSettings,
    pub(crate) eviction_interval: Duration,
    pub(crate) pubsub: Arc<PubSub>,
    pub(crate) evictors: std::sync::Mutex<std::collections::HashMap<String, Evictor>>,
}

impl Core {
    pub(crate) async fn connect(
        url: &str,
        pool_size: usize,
        lock_lease: Duration,
        connect_timeout: Duration,
        eviction_interval: Duration,
        lock_settings: crate::lock::LockSettings,
    ) -> Result<Arc<Self>> {
        let config = Config::from_url(url).map_err(|e| Error::Config(e.to_string()))?;
        let mut builder = Builder::from_config(config);
        builder.set_policy(ReconnectPolicy::new_exponential(0, 100, 30_000, 2));
        builder.with_performance_config(|performance| {
            performance.broadcast_channel_capacity = 4096;
        });
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
            lock_settings,
            eviction_interval,
            pubsub,
            evictors: std::sync::Mutex::new(std::collections::HashMap::new()),
        }))
    }

    pub(crate) fn redis(&self) -> &RedisClient {
        self.pool.next()
    }

    pub(crate) async fn blocking_client(&self) -> Result<BlockingClient> {
        let client = self.redis().clone_new();
        client.init().await?;
        let id: i64 = match client.client_id().await {
            Ok(id) => id,
            Err(error) => {
                let _ = client.quit().await;
                return Err(error.into());
            }
        };
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

    pub(crate) async fn eval_no_retry<R: FromValue>(
        &self,
        script: &Script,
        keys: Vec<String>,
        args: Vec<Bytes>,
    ) -> Result<R> {
        let client = self.redis();
        let once = client.with_options(&Options {
            max_attempts: Some(1),
            ..Default::default()
        });
        let sha = script.sha1().clone();
        match once.evalsha(sha.clone(), keys.clone(), args.clone()).await {
            Err(error) if error.details().starts_with("NOSCRIPT") => {
                script.load(client).await?;
                Ok(once.evalsha(sha, keys, args).await?)
            }
            result => Ok(result?),
        }
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
                    .client_kill::<i64>(vec![ClientKillFilter::ID(id.to_string())])
                    .await;
                let _ = client.quit().await;
            });
        }
    }
}

impl Drop for Core {
    fn drop(&mut self) {
        let evictors = self.evictors.get_mut().unwrap_or_else(|e| e.into_inner());
        evictors.values().for_each(|evictor| evictor.handle.abort());
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
