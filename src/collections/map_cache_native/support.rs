//! Which hash field commands the server has, probed with the hash commands themselves (`HPTTL`, `HSETEX`), which any user of the map may run, unlike `COMMAND INFO`.

use std::sync::atomic::{AtomicBool, Ordering};

use crate::error::{Error, Result};
use crate::object::Key;

/// The hash field commands this client has seen the server answer. Only a yes is kept: a no is asked again on the next call, so a client started before a server upgrade picks up the new commands.
#[derive(Debug, Default)]
pub(crate) struct HashFieldSupport {
    /// `HPEXPIRE` and the other field TTL commands: Redis 7.4 or Valkey 9.
    field_ttl: AtomicBool,
    /// `HSETEX`: Redis 8.0.
    hsetex: AtomicBool,
}

/// One command the probe asks about.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Command {
    /// `HPTTL`, standing for every field TTL command.
    FieldTtl,
    /// `HSETEX`.
    Hsetex,
}

/// What the server said about a command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Answer {
    /// It ran the command or refused its arguments: it has it.
    Yes,
    /// It replied "unknown command".
    No,
    /// Another error reply, such as `NOPERM`: no answer either way.
    Unknown,
}

impl HashFieldSupport {
    fn flag(&self, command: Command) -> &AtomicBool {
        match command {
            Command::FieldTtl => &self.field_ttl,
            Command::Hsetex => &self.hsetex,
        }
    }

    /// `Yes` when an earlier probe saw the command, `None` when it has to be asked.
    fn cached(&self, command: Command) -> Option<Answer> {
        self.flag(command)
            .load(Ordering::Acquire)
            .then_some(Answer::Yes)
    }

    /// Keeps `answer` when it is a yes, and returns it.
    fn remember(&self, command: Command, answer: Answer) -> Answer {
        if answer == Answer::Yes {
            self.flag(command).store(true, Ordering::Release);
        }
        answer
    }
}

/// What a method needs from the server.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Needs {
    /// Field TTLs (`HPEXPIRE`, `HPTTL`, `HPERSIST`).
    FieldTtl,
    /// `HSETEX`, which implies field TTLs.
    Hsetex,
}

fn unknown_command(message: &str) -> bool {
    message.to_lowercase().contains("unknown command")
}

/// Reads the reply of `HPTTL <key> FIELDS 1 <field>`: any reply means the server has field TTLs, "unknown command" that it has not. Another error reply answers nothing; other failures are passed on.
fn hpttl_answer(reply: Result<redis::Value>) -> Result<Answer> {
    match reply {
        Ok(_) => Ok(Answer::Yes),
        Err(Error::Redis(message)) if unknown_command(&message) => Ok(Answer::No),
        Err(Error::Redis(_)) => Ok(Answer::Unknown),
        Err(error) => Err(error),
    }
}

/// Reads the reply of `HSETEX <key> FIELDS 0`, which a server with `HSETEX` turns down for its arguments: only "unknown command" means it has none. Redis looks the command up before it checks arguments or ACLs.
fn hsetex_answer(reply: Result<redis::Value>) -> Result<Answer> {
    match reply {
        Ok(_) => Ok(Answer::Yes),
        Err(Error::Redis(message)) if unknown_command(&message) => Ok(Answer::No),
        Err(Error::Redis(_)) => Ok(Answer::Yes),
        Err(error) => Err(error),
    }
}

/// Asks the server, routed by `key`, whether it has `command`, unless an earlier call saw it.
async fn probe(key: &Key, command: Command) -> Result<Answer> {
    let support = &key.core.hash_field_support;
    if let Some(answer) = support.cached(command) {
        return Ok(answer);
    }
    let answer = match command {
        Command::FieldTtl => {
            let mut cmd = redis::cmd("HPTTL");
            cmd.arg(key.redis_key())
                .arg("FIELDS")
                .arg(1)
                .arg("redissun_probe");
            hpttl_answer(key.query(cmd).await)?
        }
        Command::Hsetex => {
            let mut cmd = redis::cmd("HSETEX");
            cmd.arg(key.redis_key()).arg("FIELDS").arg(0);
            hsetex_answer(key.query(cmd).await)?
        }
    };
    Ok(support.remember(command, answer))
}

/// Fails with [`Error::Unsupported`] when the server lacks what `needs` names. A server that gave no answer either way is let through: the method's own command reports its error.
///
/// It runs before any write: a script that fails at `hpexpire` has already done its `hset`.
pub(crate) async fn require(key: &Key, needs: Needs) -> Result<()> {
    let hsetex = match needs {
        Needs::Hsetex => Some(probe(key, Command::Hsetex).await?),
        Needs::FieldTtl => None,
    };
    if hsetex == Some(Answer::Yes) {
        return Ok(());
    }
    if probe(key, Command::FieldTtl).await? == Answer::No {
        return Err(Error::Unsupported(
            "MapCacheNative needs Redis 7.4 or newer (HPEXPIRE)".into(),
        ));
    }
    if hsetex == Some(Answer::No) {
        return Err(Error::Unsupported(
            "MapCacheNative needs Redis 8.0 or newer (HSETEX)".into(),
        ));
    }
    Ok(())
}

/// Whether the server surely has field TTLs, without failing when it has not: methods that also run on older servers ask it to pick their script, and take the plain one when the server gave no answer.
pub(crate) async fn has_field_ttl(key: &Key) -> Result<bool> {
    Ok(probe(key, Command::FieldTtl).await? == Answer::Yes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::Value;

    fn server(message: &str) -> Result<Value> {
        Err(Error::Redis(message.into()))
    }

    #[test]
    fn hpttl_reply_means_field_ttls() {
        let reply = Ok(Value::Array(vec![Value::Int(-2)]));
        assert_eq!(hpttl_answer(reply).ok(), Some(Answer::Yes));
    }

    #[test]
    fn hpttl_unknown_command_means_none() {
        let reply = server("ERR unknown command 'HPTTL', with args beginning with: ");
        assert_eq!(hpttl_answer(reply).ok(), Some(Answer::No));
    }

    #[test]
    fn hpttl_noperm_answers_nothing() {
        let reply = server("NOPERM User app has no permissions to run the 'hpttl' command");
        assert_eq!(hpttl_answer(reply).ok(), Some(Answer::Unknown));
    }

    #[test]
    fn a_timeout_is_passed_on() {
        assert!(matches!(
            hpttl_answer(Err(Error::Timeout)),
            Err(Error::Timeout)
        ));
        assert!(matches!(
            hsetex_answer(Err(Error::Timeout)),
            Err(Error::Timeout)
        ));
    }

    #[test]
    fn hsetex_argument_error_means_present() {
        let reply = server("ERR wrong number of arguments for 'hsetex' command");
        assert_eq!(hsetex_answer(reply).ok(), Some(Answer::Yes));
    }

    #[test]
    fn hsetex_unknown_command_means_missing() {
        let reply = server("ERR unknown command 'HSETEX', with args beginning with: ");
        assert_eq!(hsetex_answer(reply).ok(), Some(Answer::No));
    }

    #[test]
    fn a_yes_is_cached() {
        let support = HashFieldSupport::default();
        assert_eq!(support.cached(Command::FieldTtl), None);
        support.remember(Command::FieldTtl, Answer::Yes);
        assert_eq!(support.cached(Command::FieldTtl), Some(Answer::Yes));
        assert_eq!(support.cached(Command::Hsetex), None);
    }

    #[test]
    fn a_no_or_no_answer_is_asked_again() {
        let support = HashFieldSupport::default();
        assert_eq!(support.remember(Command::FieldTtl, Answer::No), Answer::No);
        assert_eq!(support.cached(Command::FieldTtl), None);
        support.remember(Command::Hsetex, Answer::Unknown);
        assert_eq!(support.cached(Command::Hsetex), None);
    }
}
