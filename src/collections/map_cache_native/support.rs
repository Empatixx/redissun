//! Which hash field commands the server has, probed once per client with `COMMAND INFO`.

use crate::error::{Error, Result};
use crate::object::Key;

/// The hash field commands the server knows, read once per client.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct HashFieldSupport {
    /// `HPEXPIRE` and the other field TTL commands: Redis 7.4 or Valkey 9.
    field_ttl: bool,
    /// `HSETEX`: Redis 8.0.
    hsetex: bool,
}

/// What a method needs from the server.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Needs {
    /// Field TTLs (`HPEXPIRE`, `HPTTL`, `HPERSIST`).
    FieldTtl,
    /// `HSETEX`, which implies field TTLs.
    Hsetex,
}

/// Reads the reply of `COMMAND INFO HPEXPIRE HSETEX`: one entry per command, `nil` for a command the server does not have.
pub(crate) fn parse_command_info(reply: &redis::Value) -> HashFieldSupport {
    let redis::Value::Array(entries) = reply else {
        return HashFieldSupport::default();
    };
    let known = |i: usize| {
        entries
            .get(i)
            .is_some_and(|entry| !matches!(entry, redis::Value::Nil))
    };
    HashFieldSupport {
        field_ttl: known(0),
        hsetex: known(1),
    }
}

/// Fails with [`Error::Unsupported`] when the server lacks what `needs` names. The first call per client asks the server, routed by `key`, and caches the answer.
///
/// It runs before any write: a script that fails at `hpexpire` has already done its `hset`.
pub(crate) async fn require(key: &Key, needs: Needs) -> Result<()> {
    let support = probe(key).await?;
    if !support.field_ttl {
        return Err(Error::Unsupported(
            "MapCacheNative needs Redis 7.4 or newer (HPEXPIRE)".into(),
        ));
    }
    if needs == Needs::Hsetex && !support.hsetex {
        return Err(Error::Unsupported(
            "MapCacheNative needs Redis 8.0 or newer (HSETEX)".into(),
        ));
    }
    Ok(())
}

/// Whether the server has field TTLs, without failing when it has not: methods that also run on older servers ask it to pick their script.
pub(crate) async fn has_field_ttl(key: &Key) -> Result<bool> {
    Ok(probe(key).await?.field_ttl)
}

async fn probe(key: &Key) -> Result<HashFieldSupport> {
    let support = key
        .core
        .hash_field_support
        .get_or_try_init(|| async {
            let mut cmd = redis::cmd("COMMAND");
            cmd.arg("INFO").arg("HPEXPIRE").arg("HSETEX");
            let reply: redis::Value = key.query(cmd).await?;
            Ok::<_, Error>(parse_command_info(&reply))
        })
        .await?;
    Ok(*support)
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::Value;

    fn info() -> Value {
        Value::Array(vec![Value::SimpleString("hpexpire".into())])
    }

    #[test]
    fn both_commands_present() {
        let support = parse_command_info(&Value::Array(vec![info(), info()]));
        assert!(support.field_ttl);
        assert!(support.hsetex);
    }

    #[test]
    fn hsetex_missing() {
        let support = parse_command_info(&Value::Array(vec![info(), Value::Nil]));
        assert!(support.field_ttl);
        assert!(!support.hsetex);
    }

    #[test]
    fn both_missing() {
        let support = parse_command_info(&Value::Array(vec![Value::Nil, Value::Nil]));
        assert!(!support.field_ttl);
        assert!(!support.hsetex);
    }

    #[test]
    fn a_non_array_reply_means_neither() {
        let support = parse_command_info(&Value::Okay);
        assert!(!support.field_ttl);
        assert!(!support.hsetex);
    }
}
