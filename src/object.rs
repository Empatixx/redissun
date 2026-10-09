use crate::core::Core;
use crate::error::{Error, Result};
use bytes::Bytes;
use fred::interfaces::KeysInterface;
use fred::types::scripts::Script;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

static EXPIRE_ALL: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local result = 0
        for j = 1, #KEYS, 1 do
            if redis.call('PEXPIRE', KEYS[j], ARGV[1]) == 1 then
                result = 1
            end
        end
        return result",
    )
});

static PERSIST_ALL: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local result = 0
        for j = 1, #KEYS, 1 do
            if redis.call('PERSIST', KEYS[j]) == 1 then
                result = 1
            end
        end
        return result",
    )
});

static RENAME_ALL: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local half = #KEYS / 2
        for j = 1, half, 1 do
            if redis.call('EXISTS', KEYS[j]) == 1 then
                redis.call('RENAME', KEYS[j], KEYS[half + j])
            else
                redis.call('DEL', KEYS[half + j])
            end
        end
        return 1",
    )
});

/// Operations on the Redis key behind a distributed object.
pub trait Object {
    /// The Redis key name of the object.
    fn name(&self) -> &str;
    /// Deletes the key; returns whether it existed.
    fn del(&self) -> impl Future<Output = Result<bool>> + Send;
    /// Returns whether the key exists.
    fn exists(&self) -> impl Future<Output = Result<bool>> + Send;
    /// Renames the key, and any companion keys with it, in one atomic step.
    /// Fails when a key without companions does not exist. This handle keeps its old name: get a new handle for `new_name`.
    fn rename(&self, new_name: &str) -> impl Future<Output = Result<()>> + Send;
    /// Sets a time to live on the key and its companion keys atomically; returns whether any of them got it.
    fn expire(&self, ttl: Duration) -> impl Future<Output = Result<bool>> + Send;
    /// Remaining time to live, or `None` when the key has no expiry or does not exist.
    fn ttl(&self) -> impl Future<Output = Result<Option<Duration>>> + Send;
    /// Removes the expiry from the key and its companion keys atomically; returns whether any was removed.
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

pub(crate) fn block_seconds(timeout: Duration) -> f64 {
    if timeout.is_zero() {
        0.0
    } else {
        timeout.as_secs().max(1) as f64
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
        if companions.is_empty() {
            return self.expire(ttl).await;
        }
        let applied: i64 = self
            .core
            .eval(
                &EXPIRE_ALL,
                self.with(companions),
                vec![Bytes::from(millis(ttl)?.to_string())],
            )
            .await?;
        Ok(applied == 1)
    }

    pub(crate) async fn persist_all(&self, companions: Vec<String>) -> Result<bool> {
        if companions.is_empty() {
            return self.persist().await;
        }
        let cleared: i64 = self
            .core
            .eval(&PERSIST_ALL, self.with(companions), Vec::new())
            .await?;
        Ok(cleared == 1)
    }

    pub(crate) async fn rename_all(
        &self,
        new_name: &str,
        companions: Vec<String>,
        renamed: Vec<String>,
    ) -> Result<()> {
        if companions.is_empty() {
            return self.rename(new_name).await;
        }
        let keys = self
            .with(companions)
            .into_iter()
            .chain(std::iter::once(new_name.to_string()))
            .chain(renamed)
            .collect();
        self.core.eval::<i64>(&RENAME_ALL, keys, Vec::new()).await?;
        Ok(())
    }

    fn with(&self, companions: Vec<String>) -> Vec<String> {
        std::iter::once(self.redis_key())
            .chain(companions)
            .collect()
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

        fn renamed_companions(&self, _new_name: &str) -> Option<Vec<String>> {
            self.companions().is_empty().then(Vec::new)
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
        let renamed = self.renamed_companions(new_name);
        async move {
            match renamed {
                Some(renamed) if renamed.len() == companions.len() => {
                    self.key().rename_all(new_name, companions, renamed).await
                }
                _ => Err(Error::Unsupported("rename".into())),
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
