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

#[cfg(test)]
mod tests {
    use super::Request;

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
