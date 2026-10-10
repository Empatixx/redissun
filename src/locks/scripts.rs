use fred::types::scripts::Script;
use std::sync::LazyLock;

pub(super) const UNLOCK_MESSAGE: &str = "0";
pub(super) const READ_UNLOCK_MESSAGE: &str = "1";

pub(super) static ACQUIRE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if ((redis.call('exists', KEYS[1]) == 0)
                or (redis.call('hexists', KEYS[1], ARGV[2]) == 1)) then
            redis.call('hincrby', KEYS[1], ARGV[2], 1)
            redis.call('pexpire', KEYS[1], ARGV[1])
            return nil
        end
        return redis.call('pttl', KEYS[1])",
    )
});

pub(super) static FENCED_ACQUIRE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if (redis.call('exists', KEYS[1]) == 0
                or (redis.call('hexists', KEYS[1], ARGV[2]) == 1)) then
            local token = redis.call('incr', KEYS[2])
            redis.call('hincrby', KEYS[1], ARGV[2], 1)
            redis.call('pexpire', KEYS[1], ARGV[1])
            return {-1, token}
        end
        return {redis.call('pttl', KEYS[1]), -1}",
    )
});

pub(super) static RELEASE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local val = redis.call('get', KEYS[2])
        if val ~= false then
            return tonumber(val)
        end
        if (redis.call('hexists', KEYS[1], ARGV[2]) == 0) then
            return nil
        end
        local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1)
        if (counter > 0) then
            redis.call('pexpire', KEYS[1], ARGV[1])
            redis.call('set', KEYS[2], 0, 'px', ARGV[5])
            return 0
        else
            redis.call('del', KEYS[1])
            redis.call('publish', ARGV[3], ARGV[4])
            redis.call('set', KEYS[2], 1, 'px', ARGV[5])
            return 1
        end",
    )
});

pub(super) static RENEW: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then
            redis.call('pexpire', KEYS[1], ARGV[1])
            return 1
        end
        return 0",
    )
});

pub(super) static FORCE_UNLOCK: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if (redis.call('del', KEYS[1]) == 1) then
            redis.call('publish', ARGV[1], ARGV[2])
            return 1
        else
            return 0
        end",
    )
});

pub(super) static READ_ACQUIRE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local mode = redis.call('hget', KEYS[1], 'mode')
        if (mode == false) then
            redis.call('hset', KEYS[1], 'mode', 'read')
            redis.call('hset', KEYS[1], ARGV[2], 1)
            redis.call('set', KEYS[2] .. ':1', 1)
            redis.call('pexpire', KEYS[2] .. ':1', ARGV[1])
            redis.call('pexpire', KEYS[1], ARGV[1])
            return nil
        end
        if (mode == 'read') or (mode == 'write' and redis.call('hexists', KEYS[1], ARGV[3]) == 1) then
            local ind = redis.call('hincrby', KEYS[1], ARGV[2], 1)
            local key = KEYS[2] .. ':' .. ind
            redis.call('set', key, 1)
            redis.call('pexpire', key, ARGV[1])
            local remainTime = redis.call('pttl', KEYS[1])
            redis.call('pexpire', KEYS[1], math.max(remainTime, ARGV[1]))
            return nil
        end
        return redis.call('pttl', KEYS[1])",
    )
});

pub(super) static WRITE_ACQUIRE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local mode = redis.call('hget', KEYS[1], 'mode')
        if (mode == false) then
            redis.call('hset', KEYS[1], 'mode', 'write')
            redis.call('hset', KEYS[1], ARGV[2], 1)
            redis.call('pexpire', KEYS[1], ARGV[1])
            return nil
        end
        if (mode == 'write') then
            if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then
                redis.call('hincrby', KEYS[1], ARGV[2], 1)
                local currentExpire = redis.call('pttl', KEYS[1])
                redis.call('pexpire', KEYS[1], currentExpire + ARGV[1])
                return nil
            end
        end
        return redis.call('pttl', KEYS[1])",
    )
});

pub(super) static READ_RELEASE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local val = redis.call('get', KEYS[5])
        if val ~= false then
            return tonumber(val)
        end
        local mode = redis.call('hget', KEYS[1], 'mode')
        if (mode == false) then
            redis.call('publish', KEYS[2], ARGV[1])
            redis.call('set', KEYS[5], 1, 'px', ARGV[3])
            return nil
        end
        local lockExists = redis.call('hexists', KEYS[1], ARGV[2])
        if (lockExists == 0) then
            return nil
        end
        local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1)
        if (counter == 0) then
            redis.call('hdel', KEYS[1], ARGV[2])
        end
        redis.call('del', KEYS[3] .. ':' .. (counter + 1))
        if (redis.call('hlen', KEYS[1]) > 1) then
            local maxRemainTime = -3
            local keys = redis.call('hkeys', KEYS[1])
            for n, key in ipairs(keys) do
                counter = tonumber(redis.call('hget', KEYS[1], key))
                if type(counter) == 'number' then
                    for i = counter, 1, -1 do
                        local remainTime = redis.call('pttl', KEYS[4] .. ':' .. key .. ':rwlock_timeout:' .. i)
                        maxRemainTime = math.max(remainTime, maxRemainTime)
                    end
                end
            end
            if maxRemainTime > 0 then
                redis.call('pexpire', KEYS[1], maxRemainTime)
                redis.call('set', KEYS[5], 0, 'px', ARGV[3])
                return 0
            end
            if mode == 'write' then
                redis.call('set', KEYS[5], 0, 'px', ARGV[3])
                return 0
            end
        end
        redis.call('del', KEYS[1])
        redis.call('publish', KEYS[2], ARGV[1])
        redis.call('set', KEYS[5], 1, 'px', ARGV[3])
        return 1",
    )
});

pub(super) static WRITE_RELEASE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local val = redis.call('get', KEYS[3])
        if val ~= false then
            return tonumber(val)
        end
        local mode = redis.call('hget', KEYS[1], 'mode')
        if (mode == false) then
            redis.call('publish', KEYS[2], ARGV[1])
            redis.call('set', KEYS[3], 1, 'px', ARGV[4])
            return nil
        end
        if (mode == 'write') then
            local lockExists = redis.call('hexists', KEYS[1], ARGV[3])
            if (lockExists == 0) then
                return nil
            else
                local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1)
                if (counter > 0) then
                    redis.call('pexpire', KEYS[1], ARGV[2])
                    redis.call('set', KEYS[3], 0, 'px', ARGV[4])
                    return 0
                else
                    redis.call('hdel', KEYS[1], ARGV[3])
                    if (redis.call('hlen', KEYS[1]) == 1) then
                        redis.call('del', KEYS[1])
                        redis.call('publish', KEYS[2], ARGV[1])
                    else
                        redis.call('hset', KEYS[1], 'mode', 'read')
                    end
                    redis.call('set', KEYS[3], 1, 'px', ARGV[4])
                    return 1
                end
            end
        end
        return nil",
    )
});

pub(super) static READ_RENEW: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local counter = redis.call('hget', KEYS[1], ARGV[2])
        if (counter ~= false) then
            for c = counter, 1, -1 do
                redis.call('pexpire', KEYS[2] .. ':' .. ARGV[2] .. ':rwlock_timeout:' .. c, ARGV[1])
            end
            redis.call('pexpire', KEYS[1], ARGV[1])
            return 1
        end
        return 0",
    )
});

pub(crate) static READ_FORCE_UNLOCK: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if (redis.call('hget', KEYS[1], 'mode') == 'read') then
            redis.call('del', KEYS[1])
            redis.call('publish', KEYS[2], ARGV[1])
            return 1
        end
        return 0",
    )
});

pub(crate) static WRITE_FORCE_UNLOCK: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "if (redis.call('hget', KEYS[1], 'mode') == 'write') then
            redis.call('del', KEYS[1])
            redis.call('publish', KEYS[2], ARGV[1])
            return 1
        end
        return 0",
    )
});

pub(crate) static READ_IS_LOCKED: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local mode = redis.call('hget', KEYS[1], 'mode')
        if (mode == 'read') or (mode == 'write' and redis.call('hlen', KEYS[1]) > 2) then
            return 1
        end
        return 0",
    )
});
