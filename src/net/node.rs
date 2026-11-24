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
    /// Sends `READONLY` on every new connection: a replica of Redis Cluster serves reads only to a connection that sent it.
    pub(crate) readonly: bool,
}

/// A subscribe-like command of a pub/sub connection.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Subscription {
    Subscribe,
    Psubscribe,
    Unsubscribe,
    Punsubscribe,
}

/// Why a command did not get its reply.
#[derive(Debug)]
pub(crate) enum Failure {
    /// The connection failed while the command was out; it may have run, so only a command that may run twice is sent again.
    Connection(String),
    /// The connection could not be opened, or was closed before the command was written: Redis never saw it, so even a command sent once may go again.
    Unsent(String),
    /// Redis Cluster answered `MOVED` (`ask: false`) or `ASK` (`ask: true`): the slot is served by `address`.
    Redirect { ask: bool, address: Address },
    /// Redis answered with an error, or something else went wrong; sending it again does not help.
    Other(Error),
}

impl From<Failure> for Error {
    fn from(failure: Failure) -> Self {
        match failure {
            Failure::Connection(message) | Failure::Unsent(message) => Error::Redis(message),
            Failure::Redirect { ask, address } => Error::Redis(format!(
                "{} to {address} not followed",
                if ask { "ASK" } else { "MOVED" }
            )),
            Failure::Other(error) => error,
        }
    }
}

/// The name of `cmd` (`GET`, `EVALSHA`…) as logged; its arguments may hold values or secrets and are never logged.
pub(crate) fn command_name(cmd: &redis::Cmd) -> String {
    match cmd.args_iter().next() {
        Some(redis::Arg::Simple(name)) => String::from_utf8_lossy(name).to_uppercase(),
        _ => String::new(),
    }
}

/// Whether Redis turned the login down (`WRONGPASS`, `NOAUTH`).
fn auth_rejected(error: &redis::RedisError) -> bool {
    matches!(error.kind(), redis::ErrorKind::AuthenticationFailed)
        || error
            .code()
            .is_some_and(|code| matches!(code, "WRONGPASS" | "NOAUTH"))
}

pub(crate) fn is_connection_failure(failure: &Failure) -> bool {
    matches!(failure, Failure::Connection(_) | Failure::Unsent(_))
}

/// Whether the command never reached Redis: the connection was refused. redis-rs reports a write on a closed socket and a reply lost on a closed one alike (`broken pipe`), so neither counts.
fn never_sent(error: &redis::RedisError) -> bool {
    error.is_connection_refusal()
}

/// `host:port` as Redis writes it in `MOVED`, `ASK` and `CLUSTER SLOTS`; the last colon separates the port, so an IPv6 host keeps its own.
pub(crate) fn parse_address(text: &str) -> Option<Address> {
    let (host, port) = text.rsplit_once(':')?;
    Some(Address::new(
        host.trim_matches(['[', ']']),
        port.parse().ok()?,
    ))
}

/// An error reply, with its text as Redis sent it (`NOSCRIPT No matching script…`), which callers match on.
pub(crate) fn server_failure(error: redis::ServerError) -> Failure {
    let message = match error.details() {
        Some(details) => format!("{} {details}", error.code()),
        None => error.code().to_string(),
    };
    match failure(redis::RedisError::from(error)) {
        redirect @ Failure::Redirect { .. } => redirect,
        _ => Failure::Other(Error::Redis(message)),
    }
}

fn failure(error: redis::RedisError) -> Failure {
    if let Some((address, _)) = error.redirect_node() {
        if let Some(address) = parse_address(address) {
            let ask = matches!(
                error.kind(),
                redis::ErrorKind::Server(redis::ServerErrorKind::Ask)
            );
            return Failure::Redirect { ask, address };
        }
    }
    if never_sent(&error) {
        Failure::Unsent(error.to_string())
    } else if error.is_io_error() || error.is_connection_dropped() {
        Failure::Connection(error.to_string())
    } else {
        Failure::Other(Error::Redis(error.to_string()))
    }
}

struct Live {
    connection: MultiplexedConnection,
    id: i64,
    generation: u64,
    /// The task that reads the connection; it ends when the connection ends, so the next command opens a new one instead of failing on it.
    driver: tokio::task::JoinHandle<()>,
    /// The socket, to see that the server closed it before the driver did: redis-rs would write the next command to it and then fail it like one in flight.
    probe: Option<socket2::Socket>,
    written: Arc<tokio::sync::watch::Sender<u64>>,
}

impl Live {
    fn ended(&self) -> bool {
        self.driver.is_finished()
            || self
                .probe
                .as_ref()
                .is_some_and(crate::net::stream::closed_by_server)
    }
}

#[derive(Default)]
struct State {
    live: Option<Arc<Live>>,
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
    /// The live connection again, read without waiting on `state`: every command takes it from here, and only opening a connection locks `state`.
    current: std::sync::RwLock<Option<Arc<Live>>>,
}

impl Node {
    pub(crate) fn new(address: Address, settings: Arc<NodeSettings>) -> Self {
        Self {
            address,
            settings,
            state: Mutex::new(State::default()),
            current: std::sync::RwLock::new(None),
        }
    }

    pub(crate) fn address(&self) -> &Address {
        &self.address
    }

    pub(crate) fn settings_ref(&self) -> &Arc<NodeSettings> {
        &self.settings
    }

    /// The live connection, opened first when there is none.
    async fn live(&self) -> std::result::Result<(MultiplexedConnection, u64), Failure> {
        let current = self
            .current
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(live) = current.filter(|live| !live.ended()) {
            return Ok((live.connection.clone(), live.generation));
        }
        let mut state = self.state.lock().await;
        if let Some(live) = &state.live {
            if !live.ended() {
                return Ok((live.connection.clone(), live.generation));
            }
            self.set_live(&mut state, None);
        }
        // A node that had a connection, or failed to open one, is reconnecting, as Redisson's `ConnectionWatchdog` logs it.
        let reconnecting = state.generation > 0 || state.failed_at.is_some();
        if reconnecting {
            tracing::debug!("reconnecting to {}", self.address);
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
            Ok((connection, id, driver, probe, written)) => {
                state.generation += 1;
                state.failures = 0;
                state.failed_at = None;
                let generation = state.generation;
                let live = Arc::new(Live {
                    connection: connection.clone(),
                    id,
                    generation,
                    driver,
                    probe,
                    written,
                });
                self.set_live(&mut state, Some(live));
                if reconnecting {
                    tracing::debug!("connected to {}", self.address);
                }
                Ok((connection, generation))
            }
            Err(error) => {
                state.failures = state.failures.saturating_add(1);
                state.failed_at = Some(Instant::now());
                // Opening failed before the caller's command was written, so Redis never saw it.
                Err(match error {
                    Failure::Connection(message) => Failure::Unsent(message),
                    other => other,
                })
            }
        }
    }

    async fn open(
        &self,
    ) -> std::result::Result<
        (
            MultiplexedConnection,
            i64,
            tokio::task::JoinHandle<()>,
            Option<socket2::Socket>,
            Arc<tokio::sync::watch::Sender<u64>>,
        ),
        Failure,
    > {
        let settings = &self.settings;
        let stream =
            connect(&self.address, &settings.stream)
                .await
                .map_err(|error| match error {
                    Error::Redis(message) => Failure::Unsent(message),
                    other => Failure::Unsent(other.to_string()),
                })?;
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
        // redissun limits commands itself (`timeout`, none for blocking calls); redis-rs must not cut them short.
        let mut config = redis::AsyncConnectionConfig::new().set_response_timeout(None);
        if let Some(push) = &settings.push {
            config = config.set_push_sender(push.clone());
        }
        let probe = stream.probe();
        let written = stream.written();
        let (mut connection, driver) =
            MultiplexedConnection::new_with_config(&info, stream, config)
                .await
                .map_err(|error| {
                    if auth_rejected(&error) {
                        tracing::debug!(
                            "connection to {} closed due to AUTH error response",
                            self.address
                        );
                    }
                    failure(error)
                })?;
        let driver = tokio::spawn(driver);
        // An error reply (`BUSY` while a script runs) fails the opening, so no connection is kept without its name.
        let mut setup = async |cmd: redis::Cmd| match connection.send_packed_command(&cmd).await {
            Ok(redis::Value::ServerError(error)) => Err(server_failure(error)),
            Ok(value) => Ok(value),
            Err(error) => Err(failure(error)),
        };
        if let Some(name) = &settings.client_name {
            let mut cmd = redis::cmd("CLIENT");
            cmd.arg("SETNAME").arg(name);
            setup(cmd).await?;
        }
        if settings.readonly {
            setup(redis::cmd("READONLY")).await?;
        }
        let mut cmd = redis::cmd("CLIENT");
        cmd.arg("ID");
        let id = match setup(cmd).await? {
            redis::Value::Int(id) => id,
            _ => 0,
        };
        Ok((connection, id, driver, probe, written))
    }

    /// Forgets the connection of `generation` after it failed, so the next command opens a new one.
    async fn broken(&self, generation: u64) {
        let mut state = self.state.lock().await;
        if state
            .live
            .as_ref()
            .is_some_and(|live| live.generation == generation)
        {
            self.set_live(&mut state, None);
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
        tracing::trace!("command {} to {}", command_name(cmd), self.address);
        match connection.send_packed_command(cmd).await {
            // redis-rs hands an error reply over as a value; it is a failure like any other.
            Ok(redis::Value::ServerError(error)) => Err(server_failure(error)),
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

    /// Sends `count` commands of `pipe` from `offset` on one connection and returns their replies; a pipeline whose commands answer with errors is still one `Ok` when `pipe` ignores errors.
    pub(crate) async fn send_pipeline(
        &self,
        pipe: &redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> std::result::Result<Vec<redis::Value>, Failure> {
        let (mut connection, generation) = self.live().await?;
        tracing::trace!("pipeline of {count} commands to {}", self.address);
        match connection.send_packed_commands(pipe, offset, count).await {
            Ok(values) => Ok(values),
            Err(error) => {
                let failure = failure(error);
                if is_connection_failure(&failure) {
                    self.broken(generation).await;
                }
                Err(failure)
            }
        }
    }

    /// Sends `SUBSCRIBE`, `PSUBSCRIBE`, `UNSUBSCRIBE` or `PUNSUBSCRIBE` for `names` on a RESP3 connection, whose confirmations arrive as push messages.
    pub(crate) async fn subscription(
        &self,
        command: Subscription,
        names: &[String],
    ) -> std::result::Result<(), Failure> {
        let (mut connection, generation) = self.live().await?;
        let sent = match command {
            Subscription::Subscribe => connection.subscribe(names).await,
            Subscription::Psubscribe => connection.psubscribe(names).await,
            Subscription::Unsubscribe => connection.unsubscribe(names).await,
            Subscription::Punsubscribe => connection.punsubscribe(names).await,
        };
        if let Err(error) = sent {
            let failure = failure(error);
            if is_connection_failure(&failure) {
                self.broken(generation).await;
            }
            return Err(failure);
        }
        Ok(())
    }

    /// Logs the live connection in again (`AUTH`) with what the resolver gives for this node now. A node without a live connection or without a resolver is left alone.
    pub(crate) async fn renew_credentials(&self) -> std::result::Result<(), Failure> {
        let NodeCredentials::Resolver(resolver) = &self.settings.credentials else {
            return Ok(());
        };
        let live = {
            let state = self.state.lock().await;
            state
                .live
                .as_ref()
                .filter(|live| !live.ended())
                .map(|live| (live.connection.clone(), live.generation))
        };
        let Some((mut connection, generation)) = live else {
            return Ok(());
        };
        let credentials = resolver
            .resolve(&self.address.to_string())
            .await
            .map_err(Failure::Other)?;
        let Some(password) = credentials.password else {
            return Ok(());
        };
        let mut cmd = redis::cmd("AUTH");
        if let Some(username) = credentials.username {
            cmd.arg(username);
        }
        cmd.arg(password);
        match connection.send_packed_command(&cmd).await {
            Ok(redis::Value::ServerError(error)) => Err(server_failure(error)),
            Ok(_) => Ok(()),
            Err(error) => {
                let failure = failure(error);
                if is_connection_failure(&failure) {
                    self.broken(generation).await;
                }
                Err(failure)
            }
        }
    }

    /// Opens the connection if needed and gives the count of bytes written to it, so a batch can tell when its pipeline is out.
    pub(crate) async fn written(
        &self,
    ) -> std::result::Result<Arc<tokio::sync::watch::Sender<u64>>, Failure> {
        let (_, generation) = self.live().await?;
        let state = self.state.lock().await;
        match &state.live {
            Some(live) if live.generation == generation => Ok(live.written.clone()),
            _ => Err(Failure::Unsent(
                "the connection closed while it opened".into(),
            )),
        }
    }

    /// The `CLIENT ID` of the live connection, `None` while there is none.
    pub(crate) async fn client_id(&self) -> Option<i64> {
        self.state.lock().await.live.as_ref().map(|live| live.id)
    }

    /// Closes the live connection; the next command opens a new one without waiting.
    pub(crate) async fn reset(&self) {
        let mut state = self.state.lock().await;
        self.set_live(&mut state, None);
    }

    fn set_live(&self, state: &mut State, live: Option<Arc<Live>>) {
        *self.current.write().unwrap_or_else(|e| e.into_inner()) = live.clone();
        state.live = live;
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

    #[test]
    fn a_refused_connection_never_reached_redis() {
        let error =
            redis::RedisError::from(std::io::Error::from(std::io::ErrorKind::ConnectionRefused));
        assert!(matches!(super::failure(error), super::Failure::Unsent(_)));
    }

    #[test]
    fn a_closed_connection_may_have_run_the_command() {
        // redis-rs answers every command in flight on a closed connection with this.
        let error = redis::RedisError::from(std::io::Error::from(std::io::ErrorKind::BrokenPipe));
        assert!(matches!(
            super::failure(error),
            super::Failure::Connection(_)
        ));
    }

    #[test]
    fn a_reset_connection_may_have_run_the_command() {
        let error =
            redis::RedisError::from(std::io::Error::from(std::io::ErrorKind::ConnectionReset));
        assert!(matches!(
            super::failure(error),
            super::Failure::Connection(_)
        ));
    }

    async fn redis() -> (ContainerAsync<Redis>, Address) {
        let container = Redis::default().with_tag("7.4").start().await.unwrap();
        let port = container.get_host_port_ipv4(6379).await.unwrap();
        let address = Address::new("127.0.0.1", port);
        // The port is mapped before Redis listens on it.
        let probe = Node::new(
            address.clone(),
            Arc::new(settings(None, NodeCredentials::None)),
        );
        for _ in 0..100 {
            if probe.send(&command("PING", &[])).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        (container, address)
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
            readonly: false,
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

    #[test]
    fn addresses_are_read_like_redis_writes_them() {
        assert_eq!(
            super::parse_address("127.0.0.1:6381"),
            Some(Address::new("127.0.0.1", 6381))
        );
        assert_eq!(
            super::parse_address("[::1]:7000"),
            Some(Address::new("::1", 7000))
        );
        assert_eq!(super::parse_address("host"), None);
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
    async fn an_error_reply_is_a_failure_not_a_value() {
        let (_container, address) = redis().await;
        let node = Node::new(address, Arc::new(settings(None, NodeCredentials::None)));
        let failure = node
            .send(&command(
                "EVALSHA",
                &["0000000000000000000000000000000000000000", "0"],
            ))
            .await
            .unwrap_err();
        assert!(
            matches!(&failure, super::Failure::Other(crate::error::Error::Redis(message)) if message.contains("NOSCRIPT")),
            "{failure:?}"
        );
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
    async fn a_connection_closed_while_it_opens_leaves_the_command_unsent() {
        // A server that accepts and hangs up, as an old master does during a failover.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                drop(socket);
            }
        });
        let node = Node::new(
            Address::new("127.0.0.1", port),
            Arc::new(settings(Some("worker"), NodeCredentials::None)),
        );
        let sent = node.send(&command("INCR", &["counter"])).await;
        assert!(matches!(sent, Err(super::Failure::Unsent(_))), "{sent:?}");
    }

    #[tokio::test]
    async fn a_connection_the_server_closed_is_replaced_before_the_next_command() {
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
        tokio::time::sleep(Duration::from_millis(300)).await;
        // A command that may be sent only once must not land on the closed connection.
        node.send(&command("PING", &[])).await.unwrap();
    }

    #[tokio::test]
    async fn a_connection_the_server_closed_is_replaced_even_before_its_reader_noticed() {
        let (_container, address) = redis().await;
        let node = Node::new(
            address.clone(),
            Arc::new(settings(None, NodeCredentials::None)),
        );
        node.send(&command("PING", &[])).await.unwrap();
        // The test runs on one thread and kills the connection without yielding to it, so its reader has not seen the close when the next command is written.
        let mut admin =
            std::net::TcpStream::connect((address.host.as_str(), address.port)).unwrap();
        std::io::Write::write_all(
            &mut admin,
            b"*6\r\n$6\r\nCLIENT\r\n$4\r\nKILL\r\n$4\r\nTYPE\r\n$6\r\nnormal\r\n$6\r\nSKIPME\r\n$3\r\nyes\r\n",
        )
        .unwrap();
        let mut reply = [0u8; 16];
        let read = std::io::Read::read(&mut admin, &mut reply).unwrap();
        assert!(reply[..read].starts_with(b":") && &reply[..read] != b":0\r\n");
        std::thread::sleep(Duration::from_millis(200));
        node.send(&command("PING", &[])).await.unwrap();
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
