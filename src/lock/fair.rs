use crate::object::Key;
use bytes::Bytes;
use fred::types::scripts::Script;
use std::sync::LazyLock;
use tokio::runtime::Handle;

pub(crate) const SLOT_MILLIS: u64 = 5000;

const NOW: &str = "local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
";

const PURGE_STALE: &str = "while true do
    local head = redis.call('LINDEX', KEYS[2], 0)
    if head == false then break end
    if head == ARGV[2] then break end
    local due = redis.call('ZSCORE', KEYS[3], head)
    if due == false or tonumber(due) <= now then
        redis.call('ZREM', KEYS[3], head)
        redis.call('LPOP', KEYS[2])
    else
        break
    end
end
";

const NOTIFY_HEAD: &str = "local next = redis.call('LINDEX', KEYS[2], 0)
if next ~= false then
    redis.call('PUBLISH', PREFIX .. ':' .. next, MESSAGE)
end
";

fn script(body: &str, prefix: &str, message: &str) -> Script {
    let notify = NOTIFY_HEAD
        .replace("PREFIX", prefix)
        .replace("MESSAGE", message);
    let source = body
        .replace("--NOW--", NOW)
        .replace("--PURGE--", PURGE_STALE)
        .replace("--NOTIFY--", &notify);
    Script::from_lua(source)
}

pub(crate) static ACQUIRE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(format!(
        "{NOW}{PURGE_STALE}
        if redis.call('EXISTS', KEYS[1]) == 0
            and (redis.call('EXISTS', KEYS[2]) == 0 or redis.call('LINDEX', KEYS[2], 0) == ARGV[2]) then
            redis.call('LPOP', KEYS[2])
            redis.call('ZREM', KEYS[3], ARGV[2])
            for _, waiting in ipairs(redis.call('ZRANGE', KEYS[3], 0, -1)) do
                redis.call('ZINCRBY', KEYS[3], -tonumber(ARGV[3]), waiting)
            end
            redis.call('HSET', KEYS[1], ARGV[2], 1)
            redis.call('PEXPIRE', KEYS[1], ARGV[1])
            return nil
        end
        if redis.call('HEXISTS', KEYS[1], ARGV[2]) == 1 then
            redis.call('HINCRBY', KEYS[1], ARGV[2], 1)
            redis.call('PEXPIRE', KEYS[1], ARGV[1])
            return nil
        end
        local head = redis.call('LINDEX', KEYS[2], 0)
        local ttl
        if head ~= false and head ~= ARGV[2] then
            ttl = tonumber(redis.call('ZSCORE', KEYS[3], head)) - now
        else
            ttl = redis.call('PTTL', KEYS[1])
        end
        if ttl < 0 then
            ttl = 0
        end
        local due = ttl + tonumber(ARGV[3]) + now
        if redis.call('ZADD', KEYS[3], due, ARGV[2]) == 1 then
            redis.call('RPUSH', KEYS[2], ARGV[2])
        end
        return ttl"
    ))
});

pub(crate) static RELEASE: LazyLock<Script> = LazyLock::new(|| {
    let body = "--NOW----PURGE--
        if redis.call('HEXISTS', KEYS[1], ARGV[2]) == 0 then
            --NOTIFY--
            return nil
        end
        local count = redis.call('HINCRBY', KEYS[1], ARGV[2], -1)
        if count > 0 then
            redis.call('PEXPIRE', KEYS[1], ARGV[1])
            return 0
        end
        redis.call('DEL', KEYS[1])
        --NOTIFY--
        return 1";
    script(body, "ARGV[3]", "'unlocked'")
});

pub(crate) static CANCEL: LazyLock<Script> = LazyLock::new(|| {
    let body = "if redis.call('LINDEX', KEYS[2], 0) == ARGV[1] then
            for _, waiting in ipairs(redis.call('ZRANGE', KEYS[3], 0, -1)) do
                redis.call('ZINCRBY', KEYS[3], -tonumber(ARGV[2]), waiting)
            end
        end
        redis.call('ZREM', KEYS[3], ARGV[1])
        redis.call('LREM', KEYS[2], 0, ARGV[1])
        if redis.call('EXISTS', KEYS[1]) == 0 then
            --NOTIFY--
        end
        return 1";
    script(body, "ARGV[3]", "'unlocked'")
});

pub(crate) static FORCE_UNLOCK: LazyLock<Script> = LazyLock::new(|| {
    let body = "local removed = redis.call('DEL', KEYS[1])
        --NOTIFY--
        return removed";
    script(body, "ARGV[1]", "'unlocked'")
});

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

pub(crate) struct Queued {
    key: Key,
    owner: String,
    runtime: Option<Handle>,
    armed: bool,
}

impl Queued {
    pub(crate) fn new(key: Key, owner: String) -> Self {
        Self {
            key,
            owner,
            runtime: Handle::try_current().ok(),
            armed: true,
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }

    pub(crate) async fn cancel(&mut self) {
        if self.armed {
            self.armed = false;
            let _ = leave(&self.key, &self.owner).await;
        }
    }
}

impl Drop for Queued {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(runtime) = self.runtime.clone() {
            let (key, owner) = (self.key.clone(), self.owner.clone());
            runtime.spawn(async move {
                let _ = leave(&key, &owner).await;
            });
        }
    }
}

async fn leave(key: &Key, owner: &str) -> crate::error::Result<()> {
    let _: i64 = key
        .core
        .eval(
            &CANCEL,
            keys(key),
            vec![
                Bytes::from(owner.to_string()),
                Bytes::from(SLOT_MILLIS.to_string()),
                Bytes::from(channel_prefix(key.name())),
            ],
        )
        .await?;
    Ok(())
}
