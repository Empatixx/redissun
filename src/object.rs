use crate::core::Core;
use crate::error::{Error, Result};
use fred::interfaces::KeysInterface;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// Operations on the Redis key behind a distributed object.
pub trait Object {
    /// The Redis key name of the object.
    fn name(&self) -> &str;
    /// Deletes the key; returns whether it existed.
    fn del(&self) -> impl Future<Output = Result<bool>> + Send;
    /// Returns whether the key exists.
    fn exists(&self) -> impl Future<Output = Result<bool>> + Send;
    /// Renames the key.
    fn rename(&self, new_name: &str) -> impl Future<Output = Result<()>> + Send;
    /// Sets a time to live; returns whether it was applied.
    fn expire(&self, ttl: Duration) -> impl Future<Output = Result<bool>> + Send;
    /// Remaining time to live, or `None` when the key has no expiry or does not exist.
    fn ttl(&self) -> impl Future<Output = Result<Option<Duration>>> + Send;
    /// Removes the expiry; returns whether one was removed.
    fn persist(&self) -> impl Future<Output = Result<bool>> + Send;
}

pub(crate) fn tagged(name: &str) -> String {
    let has_tag = name
        .find('{')
        .and_then(|open| name[open + 1..].find('}'))
        .is_some_and(|length| length > 0);
    if has_tag {
        name.to_string()
    } else {
        format!("{{{name}}}")
    }
}

pub(crate) fn millis(duration: Duration) -> Result<i64> {
    if duration.is_zero() {
        return Err(Error::Config("duration must be positive".into()));
    }
    i64::try_from(duration.as_nanos().div_ceil(1_000_000))
        .map_err(|_| Error::Config("duration is too long".into()))
}

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Key").field(&self.name).finish()
    }
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

    pub(crate) fn describe(&self, f: &mut fmt::Formatter<'_>, type_name: &str) -> fmt::Result {
        f.debug_struct(type_name)
            .field("name", &self.name())
            .finish()
    }

    pub(crate) async fn get_i64_or_zero(&self) -> Result<i64> {
        let value: Option<i64> = self.core.redis().get(self.redis_key()).await?;
        Ok(value.unwrap_or(0))
    }

    pub(crate) fn redis_key(&self) -> String {
        self.name.to_string()
    }

    pub(crate) async fn del_all(&self, companions: Vec<String>) -> Result<bool> {
        let keys: Vec<String> = std::iter::once(self.redis_key())
            .chain(companions)
            .collect();
        let removed: i64 = self.core.redis().del(keys).await?;
        Ok(removed > 0)
    }

    pub(crate) async fn expire_all(&self, ttl: Duration, companions: Vec<String>) -> Result<bool> {
        let applied = self.expire(ttl).await?;
        for companion in companions {
            self.core
                .redis()
                .pexpire::<bool, _>(companion, millis(ttl)?, None)
                .await?;
        }
        Ok(applied)
    }

    pub(crate) async fn persist_all(&self, companions: Vec<String>) -> Result<bool> {
        let cleared = self.persist().await?;
        for companion in companions {
            self.core.redis().persist::<bool, _>(companion).await?;
        }
        Ok(cleared)
    }

    pub(crate) async fn command(
        &self,
        name: &'static str,
        args: Vec<fred::types::Value>,
        key_offset: usize,
    ) -> Result<fred::types::Value> {
        let command = fred::types::CustomCommand::new_static(
            name,
            fred::types::ClusterHash::Offset(key_offset),
            false,
        );
        Ok(fred::interfaces::ClientLike::custom(self.core.redis(), command, args).await?)
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
            .pexpire(self.redis_key(), millis(ttl)?, None)
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

        fn companions(&self) -> Vec<String> {
            Vec::new()
        }
    }
}

pub(crate) use sealed::HasKey;

impl<T: HasKey + Sync> Object for T {
    fn name(&self) -> &str {
        self.key().name()
    }

    fn del(&self) -> impl Future<Output = Result<bool>> + Send {
        self.key().del_all(self.companions())
    }

    fn exists(&self) -> impl Future<Output = Result<bool>> + Send {
        self.key().exists()
    }

    fn rename(&self, new_name: &str) -> impl Future<Output = Result<()>> + Send {
        let companions = self.companions();
        async move {
            if companions.is_empty() {
                self.key().rename(new_name).await
            } else {
                Err(Error::Unsupported("rename".into()))
            }
        }
    }

    fn expire(&self, ttl: Duration) -> impl Future<Output = Result<bool>> + Send {
        self.key().expire_all(ttl, self.companions())
    }

    fn ttl(&self) -> impl Future<Output = Result<Option<Duration>>> + Send {
        self.key().ttl()
    }

    fn persist(&self) -> impl Future<Output = Result<bool>> + Send {
        self.key().persist_all(self.companions())
    }
}

#[cfg(test)]
mod tests {
    use super::tagged;

    #[test]
    fn names_get_a_hash_tag_unless_they_already_have_a_real_one() {
        assert_eq!(tagged("api"), "{api}");
        assert_eq!(tagged("{team}:api"), "{team}:api");
        assert_eq!(tagged("a{}b"), "{a{}b}");
        assert_eq!(tagged("}a{"), "{}a{}");
    }
}
