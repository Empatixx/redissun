use super::engine::{Failure, Limits};
use crate::client::core::Core;
use crate::error::{Error, Result};
use bytes::Bytes;
use fred::interfaces::ClientLike;
use fred::types::config::Server;
use fred::types::Value;
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

const READ_AT_ONCE: usize = 64 * 1024;

pub(super) trait Io: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// One answer of Redis.
#[derive(Debug)]
pub(super) enum Frame {
    Simple(Bytes),
    Error(String),
    Integer(i64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

impl Frame {
    /// The answer as the batch objects decode it. An error anywhere in it is the error of the command.
    pub(super) fn value(self) -> Result<Value> {
        Ok(match self {
            Frame::Simple(data) | Frame::Bulk(data) => Value::Bytes(data),
            Frame::Integer(number) => Value::Integer(number),
            Frame::Null => Value::Null,
            Frame::Error(message) => return Err(Error::Redis(message)),
            Frame::Array(items) => Value::Array(
                items
                    .into_iter()
                    .map(Frame::value)
                    .collect::<Result<Vec<_>>>()?,
            ),
        })
    }
}

fn line(data: &[u8]) -> Option<(&[u8], usize)> {
    let end = data.windows(2).position(|pair| pair == b"\r\n")?;
    Some((&data[..end], end + 2))
}

fn number(digits: &[u8]) -> Option<i64> {
    std::str::from_utf8(digits).ok()?.parse().ok()
}

/// Reads one answer from the start of `data`. `Ok(None)` means that it is not all there yet, `Err` that it is not RESP.
fn parse(data: &[u8]) -> std::result::Result<Option<(Frame, usize)>, ()> {
    let Some((&kind, rest)) = data.split_first() else {
        return Ok(None);
    };
    let Some((head, used)) = line(rest) else {
        return Ok(None);
    };
    let used = used + 1;
    Ok(Some(match kind {
        b'+' => (Frame::Simple(Bytes::copy_from_slice(head)), used),
        b'-' => (
            Frame::Error(String::from_utf8_lossy(head).into_owned()),
            used,
        ),
        b':' => (Frame::Integer(number(head).ok_or(())?), used),
        b'$' => {
            let length = number(head).ok_or(())?;
            if length < 0 {
                return Ok(Some((Frame::Null, used)));
            }
            let end = used + length as usize;
            if data.len() < end + 2 {
                return Ok(None);
            }
            (
                Frame::Bulk(Bytes::copy_from_slice(&data[used..end])),
                end + 2,
            )
        }
        b'*' => {
            let length = number(head).ok_or(())?;
            if length < 0 {
                return Ok(Some((Frame::Null, used)));
            }
            let mut items = Vec::with_capacity(length as usize);
            let mut at = used;
            for _ in 0..length {
                let Some((item, used)) = parse(&data[at..])? else {
                    return Ok(None);
                };
                items.push(item);
                at += used;
            }
            (Frame::Array(items), at)
        }
        _ => return Err(()),
    }))
}

fn bulk(out: &mut Vec<u8>, data: &[u8]) {
    out.push(b'$');
    out.extend_from_slice(data.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
}

/// Appends a command to `out` as Redis reads it.
pub(super) fn encode(out: &mut Vec<u8>, name: &str, args: &[Value]) -> Result<()> {
    out.push(b'*');
    out.extend_from_slice((args.len() + 1).to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    bulk(out, name.as_bytes());
    for arg in args {
        match arg {
            Value::String(text) => bulk(out, text.as_bytes()),
            Value::Bytes(data) => bulk(out, data),
            Value::Integer(number) => bulk(out, number.to_string().as_bytes()),
            Value::Double(number) => bulk(out, number.to_string().as_bytes()),
            Value::Boolean(flag) => bulk(out, flag.to_string().as_bytes()),
            other => {
                return Err(Error::Config(format!(
                    "a batch cannot send the argument {other:?}"
                )))
            }
        }
    }
    Ok(())
}

fn text(value: impl Into<String>) -> Value {
    Value::from(value.into())
}

/// A connection to one Redis node that one batch uses at a time, like a connection that a Redisson batch takes from the pool. The batch writes its commands as one pipeline and knows when they are written, which is when the response timeout starts.
pub(crate) struct Wire {
    io: Box<dyn Io>,
    pub(super) server: Server,
    inbox: Vec<u8>,
    read: usize,
}

impl Wire {
    /// Opens a connection to `server` and signs in the way the client does: `AUTH`, `SELECT` and `CLIENT SETNAME`.
    pub(super) async fn open(core: &Core, server: &Server) -> std::result::Result<Self, Failure> {
        let wait = core.redis().connection_config().connection_timeout;
        if wait.is_zero() {
            return Self::connect(core, server).await;
        }
        tokio::time::timeout(wait, Self::connect(core, server))
            .await
            .unwrap_or_else(|_| Err(Failure::Connect("the connect timed out".into())))
    }

    async fn connect(core: &Core, server: &Server) -> std::result::Result<Self, Failure> {
        let client = core.redis();
        let config = client.client_config();
        let tcp = TcpStream::connect((&*server.host, server.port))
            .await
            .map_err(|error| Failure::Connect(error.to_string()))?;
        let nodelay = client.connection_config().tcp.nodelay.unwrap_or(true);
        tcp.set_nodelay(nodelay)
            .map_err(|error| Failure::Connect(error.to_string()))?;
        let mut wire = Self {
            io: secure(&config, server, tcp).await?,
            server: server.clone(),
            inbox: Vec::new(),
            read: 0,
        };
        let (username, password) = match &config.credential_provider {
            Some(provider) => provider
                .fetch(Some(server))
                .await
                .map_err(|error| Failure::Connect(error.to_string()))?,
            None => (config.username.clone(), config.password.clone()),
        };
        let mut out = Vec::new();
        let mut expected = 0;
        let mut greet = |name: &str, args: Vec<Value>| {
            expected += 1;
            encode(&mut out, name, &args)
        };
        if let Some(password) = password {
            let args = username.into_iter().chain([password]).map(text);
            greet("AUTH", args.collect())?;
        }
        if let (Some(database), false) = (config.database, client.is_clustered()) {
            greet("SELECT", vec![Value::Integer(database.into())])?;
        }
        if let Some(name) = core.client_name() {
            greet("CLIENT", vec![text("SETNAME"), text(name)])?;
        }
        if expected > 0 {
            for frame in wire.exchange(&out, expected, None).await? {
                if let Frame::Error(message) = frame {
                    return Err(Failure::Other(Error::Redis(message)));
                }
            }
        }
        Ok(wire)
    }

    /// Writes `out` and reads `expected` answers. As in Redisson, the whole of `out` must be written within `limits.write`, and the answers must be there within `limits.response` after that. Answers are read while `out` is written, so a big batch does not pile up in Redis.
    pub(super) async fn exchange(
        &mut self,
        out: &[u8],
        expected: usize,
        limits: Option<Limits>,
    ) -> std::result::Result<Vec<Frame>, Failure> {
        let mut frames = Vec::with_capacity(expected);
        let mut written = 0;
        let mut flushed = false;
        let mut deadline = limits.map(|limits| Box::pin(tokio::time::sleep(limits.write)));
        let mut chunk = vec![0u8; READ_AT_ONCE];
        poll_fn(|cx| {
            loop {
                let mut progressed = false;
                if written < out.len() {
                    match Pin::new(&mut self.io).poll_write(cx, &out[written..]) {
                        Poll::Ready(Ok(0)) => return Poll::Ready(Err(closed())),
                        Poll::Ready(Ok(sent)) => {
                            written += sent;
                            progressed = true;
                        }
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(broken(error))),
                        Poll::Pending => {}
                    }
                }
                if written == out.len() && !flushed {
                    match Pin::new(&mut self.io).poll_flush(cx) {
                        Poll::Ready(Ok(())) => {
                            flushed = true;
                            progressed = true;
                            if let (Some(deadline), Some(limits)) = (deadline.as_mut(), limits) {
                                let end = tokio::time::Instant::now() + limits.response;
                                deadline.as_mut().reset(end);
                            }
                        }
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(broken(error))),
                        Poll::Pending => {}
                    }
                }
                while frames.len() < expected {
                    match parse(&self.inbox[self.read..]) {
                        Ok(Some((frame, used))) => {
                            frames.push(frame);
                            self.read += used;
                        }
                        Ok(None) => break,
                        Err(()) => {
                            let error =
                                Error::Redis("redis sent an answer that is not RESP".into());
                            return Poll::Ready(Err(Failure::Other(error)));
                        }
                    }
                }
                if frames.len() == expected && flushed {
                    return Poll::Ready(Ok(()));
                }
                if frames.len() < expected {
                    let mut buffer = ReadBuf::new(&mut chunk);
                    match Pin::new(&mut self.io).poll_read(cx, &mut buffer) {
                        Poll::Ready(Ok(())) if buffer.filled().is_empty() => {
                            return Poll::Ready(Err(closed()))
                        }
                        Poll::Ready(Ok(())) => {
                            self.inbox.drain(..self.read);
                            self.read = 0;
                            self.inbox.extend_from_slice(buffer.filled());
                            progressed = true;
                        }
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(broken(error))),
                        Poll::Pending => {}
                    }
                }
                if !progressed {
                    break;
                }
            }
            if let Some(deadline) = deadline.as_mut() {
                if deadline.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(Err(Failure::Timeout));
                }
            }
            Poll::Pending
        })
        .await?;
        if self.read == self.inbox.len() {
            self.inbox.clear();
            self.read = 0;
        }
        Ok(frames)
    }

    /// Writes `out` without waiting for answers: Redis keeps them until [`exchange`](Self::exchange) reads them.
    pub(super) async fn send(&mut self, out: &[u8]) -> std::result::Result<(), Failure> {
        self.exchange(out, 0, None).await.map(|_| ())
    }

    /// Whether Redis answers a `PING` within `wait`.
    pub(crate) async fn ping(&mut self, wait: Duration) -> bool {
        let limits = Limits {
            wait,
            write: wait,
            response: wait,
        };
        let answer = self
            .exchange(b"*1\r\n$4\r\nPING\r\n", 1, Some(limits))
            .await;
        matches!(answer.as_deref(), Ok([Frame::Simple(_)]))
    }
}

fn closed() -> Failure {
    Failure::Io("redis closed the connection".into())
}

fn broken(error: std::io::Error) -> Failure {
    Failure::Io(error.to_string())
}

#[cfg(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc"))]
async fn secure(
    config: &fred::types::config::Config,
    server: &Server,
    tcp: TcpStream,
) -> std::result::Result<Box<dyn Io>, Failure> {
    use fred::types::config::TlsConnector;
    let Some(tls) = &config.tls else {
        return Ok(Box::new(tcp));
    };
    let name = server.tls_server_name.as_deref().unwrap_or(&server.host);
    let name = fred::rustls::pki_types::ServerName::try_from(name.to_string())
        .map_err(|error| Failure::Other(Error::Config(format!("TLS: {error}"))))?;
    match &tls.connector {
        TlsConnector::Rustls(connector) => {
            let stream = connector.connect(name, tcp).await;
            Ok(Box::new(
                stream.map_err(|error| Failure::Connect(error.to_string()))?,
            ))
        }
        #[allow(unreachable_patterns)]
        _ => Err(Failure::Other(Error::Config(
            "TLS: a batch needs the rustls connector".into(),
        ))),
    }
}

#[cfg(not(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc")))]
async fn secure(
    _config: &fred::types::config::Config,
    _server: &Server,
    tcp: TcpStream,
) -> std::result::Result<Box<dyn Io>, Failure> {
    Ok(Box::new(tcp))
}
