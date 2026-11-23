use crate::client::core::Core;
use crate::client::request::{command, Arg, Request};
use crate::error::{Error, Result};
use crate::lua::Lua;
use bytes::Bytes;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

static EXPIRE_ALL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local result = 0
        for j = 1, #KEYS, 1 do
            if redis.call('PEXPIRE', KEYS[j], ARGV[1]) == 1 then
                result = 1
            end
        end
        return result",
    )
});

static PERSIST_ALL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
        "local result = 0
        for j = 1, #KEYS, 1 do
            if redis.call('PERSIST', KEYS[j]) == 1 then
                result = 1
            end
        end
        return result",
    )
});

static RENAME_ALL: LazyLock<Lua> = LazyLock::new(|| {
    Lua::from_lua(
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
        let value: Option<i64> = self.query(self.key_cmd("GET")).await?;
        Ok(value.unwrap_or(0))
    }

    /// The command `name` with this object's key as its first argument.
    pub(crate) fn key_cmd(&self, name: &str) -> redis::Cmd {
        let mut cmd = redis::cmd(name);
        cmd.arg(self.redis_key());
        cmd
    }

    pub(crate) fn redis_key(&self) -> String {
        self.name.to_string()
    }

    pub(crate) async fn del_all(&self, companions: Vec<String>) -> Result<bool> {
        let keys: Vec<String> = std::iter::once(self.redis_key())
            .chain(companions)
            .collect();
        let mut cmd = redis::cmd("DEL");
        cmd.arg(keys);
        let removed: i64 = self.query(cmd).await?;
        Ok(removed > 0)
    }

    pub(crate) async fn del_with(
        &self,
        script: &Lua,
        keys: Vec<String>,
        args: Vec<Bytes>,
    ) -> Result<bool> {
        let removed: i64 = self.core.eval(script, keys, args).await?;
        Ok(removed == 1)
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

    pub(crate) async fn expire_with(
        &self,
        script: &Lua,
        ttl: Duration,
        companions: Vec<String>,
    ) -> Result<bool> {
        let applied: i64 = self
            .core
            .eval(
                script,
                self.with(companions),
                vec![Bytes::from(millis(ttl)?.to_string())],
            )
            .await?;
        Ok(applied == 1)
    }

    pub(crate) async fn persist_with(&self, script: &Lua, companions: Vec<String>) -> Result<bool> {
        let cleared: i64 = self
            .core
            .eval(script, self.with(companions), Vec::new())
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

    /// Sends the command `name` with `args`, routed by this object's key, and returns the reply as it came.
    pub(crate) async fn command(
        &self,
        name: &str,
        args: std::vec::Vec<Arg>,
    ) -> Result<redis::Value> {
        self.query(command(name, args)).await
    }

    /// Sends `cmd`, routed by this object's key, and reads the reply as `T`.
    pub(crate) async fn query<T: redis::FromRedisValue>(&self, cmd: redis::Cmd) -> Result<T> {
        self.core
            .query(Request::new(cmd).key(self.redis_key().as_bytes()))
            .await
    }

    /// Like [`query`](Key::query) for a command that is never sent again after its connection failed.
    pub(crate) async fn query_once<T: redis::FromRedisValue>(&self, cmd: redis::Cmd) -> Result<T> {
        self.core
            .query(Request::new(cmd).key(self.redis_key().as_bytes()).once())
            .await
    }

    pub(crate) async fn exists(&self) -> Result<bool> {
        let found: i64 = self.query(self.key_cmd("EXISTS")).await?;
        Ok(found > 0)
    }

    pub(crate) async fn rename(&self, new_name: &str) -> Result<()> {
        let mut cmd = self.key_cmd("RENAME");
        cmd.arg(new_name);
        let () = self.query(cmd).await?;
        Ok(())
    }

    pub(crate) async fn expire(&self, ttl: Duration) -> Result<bool> {
        let mut cmd = self.key_cmd("PEXPIRE");
        cmd.arg(millis(ttl)?);
        let applied: bool = self.query(cmd).await?;
        Ok(applied)
    }

    pub(crate) async fn ttl(&self) -> Result<Option<Duration>> {
        let millis: i64 = self.query(self.key_cmd("PTTL")).await?;
        Ok((millis >= 0).then(|| Duration::from_millis(millis as u64)))
    }

    pub(crate) async fn persist(&self) -> Result<bool> {
        let cleared: bool = self.query(self.key_cmd("PERSIST")).await?;
        Ok(cleared)
    }
}

mod sealed {
    use super::Key;
    use crate::lua::Lua;
    use bytes::Bytes;

    pub trait HasKey {
        fn key(&self) -> &Key;

        fn companions(&self) -> Vec<String> {
            Vec::new()
        }

        fn renamed_companions(&self, _new_name: &str) -> Option<Vec<String>> {
            self.companions().is_empty().then(Vec::new)
        }

        fn renamed_key(&self, new_name: &str) -> String {
            new_name.to_string()
        }

        fn del_script(&self) -> Option<(&'static Lua, Vec<String>, Vec<Bytes>)> {
            None
        }

        fn expire_script(&self) -> Option<&'static Lua> {
            None
        }

        fn persist_script(&self) -> Option<&'static Lua> {
            None
        }
    }
}

pub(crate) use sealed::HasKey;

impl<T: HasKey + Sync> Object for T {
    fn name(&self) -> &str {
        self.key().name()
    }

    fn del(&self) -> impl Future<Output = Result<bool>> + Send {
        let companions = self.companions();
        let script = self.del_script();
        async move {
            match script {
                Some((script, keys, args)) => self.key().del_with(script, keys, args).await,
                None => self.key().del_all(companions).await,
            }
        }
    }

    fn exists(&self) -> impl Future<Output = Result<bool>> + Send {
        self.key().exists()
    }

    fn rename(&self, new_name: &str) -> impl Future<Output = Result<()>> + Send {
        let companions = self.companions();
        let new_name = self.renamed_key(new_name);
        let renamed = self.renamed_companions(&new_name);
        async move {
            match renamed {
                Some(renamed) if renamed.len() == companions.len() => {
                    self.key().rename_all(&new_name, companions, renamed).await
                }
                _ => Err(Error::Unsupported("rename".into())),
            }
        }
    }

    fn expire(&self, ttl: Duration) -> impl Future<Output = Result<bool>> + Send {
        let companions = self.companions();
        let script = self.expire_script();
        async move {
            match script {
                Some(script) => self.key().expire_with(script, ttl, companions).await,
                None => self.key().expire_all(ttl, companions).await,
            }
        }
    }

    fn ttl(&self) -> impl Future<Output = Result<Option<Duration>>> + Send {
        self.key().ttl()
    }

    fn persist(&self) -> impl Future<Output = Result<bool>> + Send {
        let companions = self.companions();
        let script = self.persist_script();
        async move {
            match script {
                Some(script) => self.key().persist_with(script, companions).await,
                None => self.key().persist_all(companions).await,
            }
        }
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
