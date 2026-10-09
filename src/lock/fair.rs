use crate::error::Result;
use crate::lock::sync::{synced_eval, with_sync_retry};
use crate::object::Key;
use bytes::Bytes;
use fred::types::scripts::Script;
use std::sync::LazyLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::runtime::Handle;

const REMOVE_STALE: &str = "while true do
    local firstThreadId2 = redis.call('lindex', KEYS[2], 0)
    if firstThreadId2 == false then
        break
    end
    local timeout = redis.call('zscore', KEYS[3], firstThreadId2)
    if timeout ~= false and tonumber(timeout) <= tonumber(NOW) then
        redis.call('zrem', KEYS[3], firstThreadId2)
        redis.call('lpop', KEYS[2])
    else
        break
    end
end
";

fn remove_stale(now: &str) -> String {
    REMOVE_STALE.replace("NOW", now)
}

pub(crate) static ACQUIRE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(format!(
        "{}
        if (redis.call('exists', KEYS[1]) == 0)
            and ((redis.call('exists', KEYS[2]) == 0) or (redis.call('lindex', KEYS[2], 0) == ARGV[2])) then
            redis.call('lpop', KEYS[2])
            redis.call('zrem', KEYS[3], ARGV[2])
            local keys = redis.call('zrange', KEYS[3], 0, -1)
            for i = 1, #keys, 1 do
                redis.call('zincrby', KEYS[3], -tonumber(ARGV[3]), keys[i])
            end
            redis.call('hset', KEYS[1], ARGV[2], 1)
            redis.call('pexpire', KEYS[1], ARGV[1])
            return nil
        end
        if redis.call('hexists', KEYS[1], ARGV[2]) == 1 then
            redis.call('hincrby', KEYS[1], ARGV[2], 1)
            redis.call('pexpire', KEYS[1], ARGV[1])
            return nil
        end
        local timeout = redis.call('zscore', KEYS[3], ARGV[2])
        if timeout ~= false then
            local ttl = redis.call('pttl', KEYS[1])
            return math.max(0, ttl)
        end
        local lastThreadId = redis.call('lindex', KEYS[2], -1)
        local ttl
        if lastThreadId ~= false and lastThreadId ~= ARGV[2] and redis.call('zscore', KEYS[3], lastThreadId) ~= false then
            ttl = tonumber(redis.call('zscore', KEYS[3], lastThreadId)) - tonumber(ARGV[4])
        else
            ttl = redis.call('pttl', KEYS[1])
        end
        local timeout = ttl + tonumber(ARGV[3]) + tonumber(ARGV[4])
        if redis.call('zadd', KEYS[3], timeout, ARGV[2]) == 1 then
            redis.call('rpush', KEYS[2], ARGV[2])
        end
        return ttl",
        remove_stale("ARGV[4]")
    ))
});

pub(crate) static TRY_ACQUIRE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(format!(
        "{}
        if (redis.call('exists', KEYS[1]) == 0)
            and ((redis.call('exists', KEYS[2]) == 0) or (redis.call('lindex', KEYS[2], 0) == ARGV[2])) then
            redis.call('lpop', KEYS[2])
            redis.call('zrem', KEYS[3], ARGV[2])
            local keys = redis.call('zrange', KEYS[3], 0, -1)
            for i = 1, #keys, 1 do
                redis.call('zincrby', KEYS[3], -tonumber(ARGV[4]), keys[i])
            end
            redis.call('hset', KEYS[1], ARGV[2], 1)
            redis.call('pexpire', KEYS[1], ARGV[1])
            return nil
        end
        if redis.call('hexists', KEYS[1], ARGV[2]) == 1 then
            redis.call('hincrby', KEYS[1], ARGV[2], 1)
            redis.call('pexpire', KEYS[1], ARGV[1])
            return nil
        end
        return 1",
        remove_stale("ARGV[3]")
    ))
});

pub(crate) static RELEASE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(format!(
        "local val = redis.call('get', KEYS[4])
        if val ~= false then
            return tonumber(val)
        end
        {}
        if (redis.call('exists', KEYS[1]) == 0) then
            local nextThreadId = redis.call('lindex', KEYS[2], 0)
            if nextThreadId ~= false then
                redis.call('publish', ARGV[6] .. ':' .. nextThreadId, ARGV[1])
            end
            redis.call('set', KEYS[4], 1, 'px', ARGV[5])
            return 1
        end
        if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then
            return nil
        end
        local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1)
        if (counter > 0) then
            redis.call('pexpire', KEYS[1], ARGV[2])
            redis.call('set', KEYS[4], 0, 'px', ARGV[5])
            return 0
        end
        redis.call('del', KEYS[1])
        redis.call('set', KEYS[4], 1, 'px', ARGV[5])
        local nextThreadId = redis.call('lindex', KEYS[2], 0)
        if nextThreadId ~= false then
            redis.call('publish', ARGV[6] .. ':' .. nextThreadId, ARGV[1])
        end
        return 1",
        remove_stale("ARGV[4]")
    ))
});

pub(crate) static ACQUIRE_FAILED: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local queue = redis.call('lrange', KEYS[1], 0, -1)
        local i = 1
        while i <= #queue and queue[i] ~= ARGV[1] do
            i = i + 1
        end
        i = i + 1
        while i <= #queue do
            redis.call('zincrby', KEYS[2], -tonumber(ARGV[2]), queue[i])
            i = i + 1
        end
        redis.call('zrem', KEYS[2], ARGV[1])
        redis.call('lrem', KEYS[1], 0, ARGV[1])
        return 1",
    )
});

pub(crate) static FORCE_UNLOCK: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(format!(
        "{}
        if (redis.call('del', KEYS[1]) == 1) then
            local nextThreadId = redis.call('lindex', KEYS[2], 0)
            if nextThreadId ~= false then
                redis.call('publish', ARGV[3] .. ':' .. nextThreadId, ARGV[1])
            end
            return 1
        end
        return 0",
        remove_stale("ARGV[2]")
    ))
});

pub(crate) fn now_millis() -> Bytes {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    Bytes::from(now.to_string())
}

pub(crate) fn queue_key(key: &Key) -> String {
    format!("redissun__lock_queue:{}", key.redis_key())
}

pub(crate) fn timeout_key(key: &Key) -> String {
    format!("redissun__lock_timeout:{}", key.redis_key())
}

pub(crate) fn keys(key: &Key) -> Vec<String> {
    vec![key.redis_key(), queue_key(key), timeout_key(key)]
}

pub(crate) fn channel_prefix(name: &str) -> String {
    format!("redissun__unlock__{name}")
}

pub(crate) fn channel(name: &str, owner: &str) -> String {
    format!("{}:{owner}", channel_prefix(name))
}

pub(crate) fn wait_arg(wait: Duration) -> Bytes {
    Bytes::from(wait.as_millis().to_string())
}

pub(crate) struct Queued {
    key: Key,
    owner: String,
    wait: Duration,
    runtime: Option<Handle>,
    armed: bool,
}

impl Queued {
    pub(crate) fn new(key: Key, owner: String, wait: Duration) -> Self {
        Self {
            key,
            owner,
            wait,
            runtime: Handle::try_current().ok(),
            armed: true,
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }

    pub(crate) async fn acquire_failed(&mut self) -> Result<()> {
        self.armed = false;
        acquire_failed(&self.key, &self.owner, self.wait).await
    }
}

impl Drop for Queued {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(runtime) = self.runtime.clone() {
            let (key, owner, wait) = (self.key.clone(), self.owner.clone(), self.wait);
            runtime.spawn(async move {
                let _ = acquire_failed(&key, &owner, wait).await;
            });
        }
    }
}

async fn acquire_failed(key: &Key, owner: &str, wait: Duration) -> Result<()> {
    with_sync_retry(&key.core.retry, || async {
        let _: i64 = synced_eval(
            key,
            &ACQUIRE_FAILED,
            vec![queue_key(key), timeout_key(key)],
            vec![Bytes::from(owner.to_string()), wait_arg(wait)],
            true,
        )
        .await?;
        Ok(())
    })
    .await
}
