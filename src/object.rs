use crate::core::Core;
use crate::error::Result;
use fred::interfaces::KeysInterface;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

pub trait Object {
    fn name(&self) -> &str;
    fn del(&self) -> impl Future<Output = Result<bool>> + Send;
    fn exists(&self) -> impl Future<Output = Result<bool>> + Send;
    fn rename(&self, new_name: &str) -> impl Future<Output = Result<()>> + Send;
    fn expire(&self, ttl: Duration) -> impl Future<Output = Result<bool>> + Send;
    fn ttl(&self) -> impl Future<Output = Result<Option<Duration>>> + Send;
    fn persist(&self) -> impl Future<Output = Result<bool>> + Send;
}

#[derive(Clone)]
pub struct Key {
    pub(crate) core: Arc<Core>,
    pub(crate) name: Arc<str>,
}

impl Key {
    pub(crate) fn new(core: Arc<Core>, name: impl Into<Arc<str>>) -> Self {
        Self {
            core,
            name: name.into(),
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn redis_key(&self) -> String {
        self.name.to_string()
    }

    pub(crate) async fn del(&self) -> Result<bool> {
        let removed: i64 = self.core.redis().del(self.redis_key()).await?;
        Ok(removed > 0)
    }

    pub(crate) async fn exists(&self) -> Result<bool> {
        let found: i64 = self.core.redis().exists(self.redis_key()).await?;
        Ok(found > 0)
    }

    pub(crate) async fn rename(&self, new_name: &str) -> Result<()> {
        self.core
            .redis()
            .rename::<(), _, _>(self.redis_key(), new_name.to_string())
            .await?;
        Ok(())
    }

    pub(crate) async fn expire(&self, ttl: Duration) -> Result<bool> {
        let applied: bool = self
            .core
            .redis()
            .pexpire(self.redis_key(), ttl.as_millis() as i64, None)
            .await?;
        Ok(applied)
    }

    pub(crate) async fn ttl(&self) -> Result<Option<Duration>> {
        let millis: i64 = self.core.redis().pttl(self.redis_key()).await?;
        Ok((millis >= 0).then(|| Duration::from_millis(millis as u64)))
    }

    pub(crate) async fn persist(&self) -> Result<bool> {
        let cleared: bool = self.core.redis().persist(self.redis_key()).await?;
        Ok(cleared)
    }
}

mod sealed {
    use super::Key;

    pub trait HasKey {
        fn key(&self) -> &Key;
    }
}

pub(crate) use sealed::HasKey;

impl<T: HasKey + Sync> Object for T {
    fn name(&self) -> &str {
        self.key().name()
    }

    fn del(&self) -> impl Future<Output = Result<bool>> + Send {
        self.key().del()
    }

    fn exists(&self) -> impl Future<Output = Result<bool>> + Send {
        self.key().exists()
    }

    fn rename(&self, new_name: &str) -> impl Future<Output = Result<()>> + Send {
        self.key().rename(new_name)
    }

    fn expire(&self, ttl: Duration) -> impl Future<Output = Result<bool>> + Send {
        self.key().expire(ttl)
    }

    fn ttl(&self) -> impl Future<Output = Result<Option<Duration>>> + Send {
        self.key().ttl()
    }

    fn persist(&self) -> impl Future<Output = Result<bool>> + Send {
        self.key().persist()
    }
}
