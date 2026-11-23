//! A command on its way to Redis: what to send, which key routes it, and whether it may be sent again.

/// A command with what routes it. Build it with [`Request::new`], then [`key`](Request::key) and [`once`](Request::once).
pub(crate) struct Request {
    cmd: redis::Cmd,
    key: Option<Vec<u8>>,
    once: bool,
}

impl Request {
    pub(crate) fn new(cmd: redis::Cmd) -> Self {
        Self {
            cmd,
            key: None,
            once: false,
        }
    }

    /// Routes the command to the slot of `key`. Without a key it goes to any node.
    pub(crate) fn key(mut self, key: &[u8]) -> Self {
        self.key = Some(key.to_vec());
        self
    }

    /// The command is never sent again after its connection failed, for commands Redisson sends once.
    pub(crate) fn once(mut self) -> Self {
        self.once = true;
        self
    }

    pub(crate) fn cmd(&self) -> &redis::Cmd {
        &self.cmd
    }

    pub(crate) fn routing_key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    pub(crate) fn is_once(&self) -> bool {
        self.once
    }
}

/// One argument of a command, for code that builds a list of arguments before it knows the command.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Arg {
    Bytes(Vec<u8>),
    Int(i64),
    Double(f64),
}

impl From<&str> for Arg {
    fn from(text: &str) -> Self {
        Arg::Bytes(text.as_bytes().to_vec())
    }
}

impl From<String> for Arg {
    fn from(text: String) -> Self {
        Arg::Bytes(text.into_bytes())
    }
}

impl From<Vec<u8>> for Arg {
    fn from(bytes: Vec<u8>) -> Self {
        Arg::Bytes(bytes)
    }
}

impl From<bytes::Bytes> for Arg {
    fn from(bytes: bytes::Bytes) -> Self {
        Arg::Bytes(bytes.to_vec())
    }
}

impl From<i64> for Arg {
    fn from(number: i64) -> Self {
        Arg::Int(number)
    }
}

impl From<f64> for Arg {
    fn from(number: f64) -> Self {
        Arg::Double(number)
    }
}

impl Arg {
    /// The bytes this argument goes out as, for a script argument.
    pub(crate) fn into_bytes(self) -> bytes::Bytes {
        match self {
            Arg::Bytes(bytes) => bytes::Bytes::from(bytes),
            Arg::Int(number) => bytes::Bytes::from(number.to_string()),
            Arg::Double(number) => bytes::Bytes::from(number.to_string()),
        }
    }
}

impl redis::ToRedisArgs for Arg {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        match self {
            Arg::Bytes(bytes) => out.write_arg(bytes),
            Arg::Int(number) => out.write_arg(number.to_string().as_bytes()),
            // As fred wrote a double: Rust's shortest form, `1.5`, `inf`.
            Arg::Double(number) => out.write_arg(number.to_string().as_bytes()),
        }
    }
}

/// A command named `name` with `args`.
pub(crate) fn command(name: &str, args: impl IntoIterator<Item = Arg>) -> redis::Cmd {
    let mut cmd = redis::cmd(name);
    for arg in args {
        cmd.arg(arg);
    }
    cmd
}

#[cfg(test)]
mod tests {
    use super::{Arg, Request};

    #[test]
    fn args_go_out_as_redis_sends_them() {
        let mut cmd = redis::cmd("X");
        for arg in [
            Arg::from("GET"),
            Arg::from(-7i64),
            Arg::from(1.5f64),
            Arg::from(bytes::Bytes::from_static(b"\xff")),
        ] {
            cmd.arg(arg);
        }
        let sent: Vec<Vec<u8>> = cmd
            .args_iter()
            .filter_map(|arg| match arg {
                redis::Arg::Simple(bytes) => Some(bytes.to_vec()),
                _ => None,
            })
            .collect();
        assert_eq!(
            sent,
            vec![
                b"X".to_vec(),
                b"GET".to_vec(),
                b"-7".to_vec(),
                b"1.5".to_vec(),
                vec![0xff]
            ]
        );
    }

    #[test]
    fn a_request_is_routed_by_its_key_and_may_be_sent_again_by_default() {
        let mut cmd = redis::cmd("GET");
        cmd.arg("k");
        let request = Request::new(cmd).key(b"{tag}k");
        assert_eq!(request.routing_key(), Some(&b"{tag}k"[..]));
        assert!(!request.is_once());
        assert!(Request::new(redis::cmd("PING")).once().is_once());
    }
}
