//! One reconnecting connection to one Redis node.

use crate::client::credentials::{Credentials, Resolver};
use crate::client::retry::DelayStrategy;
use crate::error::Error;
use crate::net::stream::{connect, Address, StreamSettings};
use redis::aio::MultiplexedConnection;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;

/// Where a node's credentials come from.
#[derive(Clone)]
pub(crate) enum NodeCredentials {
    None,
    Fixed(Credentials),
    /// Asked for every new connection with the node's `host:port`, like Redisson's `CredentialsResolver`.
    Resolver(Resolver),
}

/// How the connections of a node are opened.
#[derive(Clone)]
pub(crate) struct NodeSettings {
    pub(crate) stream: StreamSettings,
    pub(crate) db: Option<i64>,
    pub(crate) credentials: NodeCredentials,
    /// Sent with `CLIENT SETNAME` on every new connection, so a reconnected connection keeps its name.
    pub(crate) client_name: Option<String>,
    /// RESP3, for a connection that receives pub/sub messages as push frames.
    pub(crate) resp3: bool,
    pub(crate) push: Option<UnboundedSender<redis::PushInfo>>,
    pub(crate) reconnection_delay: DelayStrategy,
}

/// Why a command did not get its reply.
#[derive(Debug)]
pub(crate) enum Failure {
    /// The connection failed or could not be opened; the command may be sent again.
    Connection(String),
    /// Redis answered with an error, or something else went wrong; sending it again does not help.
    Other(Error),
}

impl From<Failure> for Error {
    fn from(failure: Failure) -> Self {
        match failure {
            Failure::Connection(message) => Error::Redis(message),
            Failure::Other(error) => error,
        }
    }
}

pub(crate) fn is_connection_failure(failure: &Failure) -> bool {
    matches!(failure, Failure::Connection(_))
}

fn failure(error: redis::RedisError) -> Failure {
    if error.is_io_error() || error.is_connection_dropped() || error.is_connection_refusal() {
        Failure::Connection(error.to_string())
    } else {
        Failure::Other(Error::Redis(error.to_string()))
    }
}

struct Live {
    connection: MultiplexedConnection,
    id: i64,
    generation: u64,
}

#[derive(Default)]
struct State {
    live: Option<Live>,
    generation: u64,
    failures: u32,
    pause: Duration,
    failed_at: Option<Instant>,
}

/// A connection to one node that opens itself on first use and again after it failed, waiting the `reconnection_delay` between attempts.
pub(crate) struct Node {
    address: Address,
    settings: Arc<NodeSettings>,
    state: Mutex<State>,
}

impl Node {
    pub(crate) fn new(address: Address, settings: Arc<NodeSettings>) -> Self {
        Self {
            address,
            settings,
            state: Mutex::new(State::default()),
        }
    }

    pub(crate) fn address(&self) -> &Address {
        &self.address
    }

    /// The live connection, opened first when there is none.
    async fn live(&self) -> std::result::Result<(MultiplexedConnection, u64), Failure> {
        let mut state = self.state.lock().await;
        if let Some(live) = &state.live {
            return Ok((live.connection.clone(), live.generation));
        }
        if let Some(failed_at) = state.failed_at {
            let pause = self
                .settings
                .reconnection_delay
                .delay(state.failures.saturating_sub(1), state.pause);
            state.pause = pause;
            let since = failed_at.elapsed();
            if since < pause {
                tokio::time::sleep(pause - since).await;
            }
        }
        match self.open().await {
            Ok((connection, id)) => {
                state.generation += 1;
                state.failures = 0;
                state.failed_at = None;
                let generation = state.generation;
                state.live = Some(Live {
                    connection: connection.clone(),
                    id,
                    generation,
                });
                Ok((connection, generation))
            }
            Err(error) => {
                state.failures = state.failures.saturating_add(1);
                state.failed_at = Some(Instant::now());
                Err(error)
            }
        }
    }

    async fn open(&self) -> std::result::Result<(MultiplexedConnection, i64), Failure> {
        let settings = &self.settings;
        let stream = connect(&self.address, &settings.stream)
            .await
            .map_err(|error| Failure::Connection(error.to_string()))?;
        let credentials = match &settings.credentials {
            NodeCredentials::None => Credentials::default(),
            NodeCredentials::Fixed(credentials) => credentials.clone(),
            NodeCredentials::Resolver(resolver) => resolver
                .resolve(&self.address.to_string())
                .await
                .map_err(Failure::Other)?,
        };
        let mut info = redis::RedisConnectionInfo::default().set_skip_set_lib_name();
        if let Some(db) = settings.db {
            info = info.set_db(db);
        }
        if let Some(username) = &credentials.username {
            info = info.set_username(username);
        }
        if let Some(password) = &credentials.password {
            info = info.set_password(password);
        }
        if settings.resp3 {
            info = info.set_protocol(redis::ProtocolVersion::RESP3);
        }
        let mut config = redis::AsyncConnectionConfig::new();
        if let Some(push) = &settings.push {
            config = config.set_push_sender(push.clone());
        }
        let (mut connection, driver) =
            MultiplexedConnection::new_with_config(&info, stream, config)
                .await
                .map_err(failure)?;
        tokio::spawn(driver);
        if let Some(name) = &settings.client_name {
            let mut cmd = redis::cmd("CLIENT");
            cmd.arg("SETNAME").arg(name);
            connection
                .send_packed_command(&cmd)
                .await
                .map_err(failure)?;
        }
        let mut cmd = redis::cmd("CLIENT");
        cmd.arg("ID");
        let id = match connection
            .send_packed_command(&cmd)
            .await
            .map_err(failure)?
        {
            redis::Value::Int(id) => id,
            _ => 0,
        };
        Ok((connection, id))
    }

    /// Forgets the connection of `generation` after it failed, so the next command opens a new one.
    async fn broken(&self, generation: u64) {
        let mut state = self.state.lock().await;
        if state
            .live
            .as_ref()
            .is_some_and(|live| live.generation == generation)
        {
            state.live = None;
            state.failures = state.failures.saturating_add(1);
            state.failed_at = Some(Instant::now());
        }
    }

    /// Sends `cmd` and returns the reply; a Redis error reply is a `Failure::Other`.
    pub(crate) async fn send(
        &self,
        cmd: &redis::Cmd,
    ) -> std::result::Result<redis::Value, Failure> {
        let (mut connection, generation) = self.live().await?;
        match connection.send_packed_command(cmd).await {
            Ok(value) => Ok(value),
            Err(error) => {
                let failure = failure(error);
                if is_connection_failure(&failure) {
                    self.broken(generation).await;
                }
                Err(failure)
            }
        }
    }

    /// The `CLIENT ID` of the live connection, `None` while there is none.
    pub(crate) async fn client_id(&self) -> Option<i64> {
        self.state.lock().await.live.as_ref().map(|live| live.id)
    }

    /// Closes the live connection; the next command opens a new one without waiting.
    pub(crate) async fn reset(&self) {
        let mut state = self.state.lock().await;
        state.live = None;
    }
}

#[cfg(test)]
mod tests {
    use super::{Node, NodeCredentials, NodeSettings};
    use crate::client::credentials::{Credentials, Resolver};
    use crate::client::retry::DelayStrategy;
    use crate::net::stream::{Address, StreamSettings};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::ContainerAsync;
    use testcontainers::ImageExt;
    use testcontainers_modules::redis::Redis;

    async fn redis() -> (ContainerAsync<Redis>, Address) {
        let container = Redis::default().with_tag("7.4").start().await.unwrap();
        let port = container.get_host_port_ipv4(6379).await.unwrap();
        (container, Address::new("127.0.0.1", port))
    }

    fn settings(name: Option<&str>, credentials: NodeCredentials) -> NodeSettings {
        NodeSettings {
            stream: StreamSettings {
                connect_timeout: Duration::from_secs(5),
                tcp_no_delay: true,
                keep_alive: None,
                #[cfg(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc"))]
                tls: None,
            },
            db: None,
            credentials,
            client_name: name.map(str::to_string),
            resp3: false,
            push: None,
            reconnection_delay: DelayStrategy::Constant(Duration::from_millis(10)),
        }
    }

    fn command(name: &str, args: &[&str]) -> redis::Cmd {
        let mut cmd = redis::cmd(name);
        for arg in args {
            cmd.arg(*arg);
        }
        cmd
    }

    async fn names(admin: &Node) -> Vec<String> {
        let list: String = redis::FromRedisValue::from_redis_value(
            admin.send(&command("CLIENT", &["LIST"])).await.unwrap(),
        )
        .unwrap();
        list.lines()
            .filter_map(|line| {
                line.split_whitespace()
                    .find_map(|field| field.strip_prefix("name="))
            })
            .filter(|name| !name.is_empty())
            .map(str::to_string)
            .collect()
    }

    #[tokio::test]
    async fn a_node_sends_commands_and_reads_replies() {
        let (_container, address) = redis().await;
        let node = Node::new(address, Arc::new(settings(None, NodeCredentials::None)));
        node.send(&command("SET", &["k", "v"])).await.unwrap();
        let value = node.send(&command("GET", &["k"])).await.unwrap();
        assert_eq!(value, redis::Value::BulkString(b"v".to_vec()));
    }

    #[tokio::test]
    async fn the_client_name_is_set_again_after_the_connection_is_killed() {
        let (_container, address) = redis().await;
        let node = Node::new(
            address.clone(),
            Arc::new(settings(Some("named-node"), NodeCredentials::None)),
        );
        let admin = Node::new(address, Arc::new(settings(None, NodeCredentials::None)));
        node.send(&command("PING", &[])).await.unwrap();
        assert_eq!(names(&admin).await, vec!["named-node".to_string()]);
        admin
            .send(&command(
                "CLIENT",
                &["KILL", "TYPE", "normal", "SKIPME", "yes"],
            ))
            .await
            .unwrap();
        let mut reconnected = false;
        for _ in 0..50 {
            if node.send(&command("PING", &[])).await.is_ok() {
                reconnected = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(reconnected, "the node never reconnected");
        assert_eq!(names(&admin).await, vec!["named-node".to_string()]);
    }

    #[tokio::test]
    async fn a_command_on_a_killed_connection_is_a_connection_failure() {
        let (_container, address) = redis().await;
        let node = Node::new(
            address.clone(),
            Arc::new(settings(None, NodeCredentials::None)),
        );
        let admin = Node::new(
            address,
            Arc::new(settings(Some("admin"), NodeCredentials::None)),
        );
        node.send(&command("PING", &[])).await.unwrap();
        admin
            .send(&command(
                "CLIENT",
                &["KILL", "TYPE", "normal", "SKIPME", "yes"],
            ))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let failure = node.send(&command("PING", &[])).await.unwrap_err();
        assert!(super::is_connection_failure(&failure), "{failure:?}");
    }

    #[tokio::test]
    async fn the_resolver_is_asked_with_the_node_address() {
        let (_container, address) = redis().await;
        let asked = Arc::new(Mutex::new(Vec::new()));
        let seen = asked.clone();
        let resolver = Resolver::new(move |address| {
            seen.lock().unwrap().push(address);
            async { Ok(Credentials::default()) }
        });
        let node = Node::new(
            address.clone(),
            Arc::new(settings(None, NodeCredentials::Resolver(resolver))),
        );
        node.send(&command("PING", &[])).await.unwrap();
        assert_eq!(*asked.lock().unwrap(), vec![address.to_string()]);
    }
}
