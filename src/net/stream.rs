//! A TCP connection, plain or TLS, opened by redissun.

use crate::error::{Error, Result};
use socket2::{SockRef, TcpKeepalive};
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// A node's host and port.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Address {
    pub(crate) host: String,
    pub(crate) port: u16,
}

impl Address {
    pub(crate) fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }
}

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

/// How a connection is opened.
#[derive(Clone)]
pub(crate) struct StreamSettings {
    pub(crate) connect_timeout: Duration,
    pub(crate) tcp_no_delay: bool,
    pub(crate) keep_alive: Option<TcpKeepalive>,
    /// The TLS configuration for a `rediss://` URL, `None` for plain TCP.
    #[cfg(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc"))]
    pub(crate) tls: Option<std::sync::Arc<rustls::ClientConfig>>,
}

/// An open connection, plain or TLS.
pub(crate) struct Stream {
    transport: Transport,
    /// Every byte written so far, so a batch knows when its pipeline is out and its answers are timed.
    written: Arc<tokio::sync::watch::Sender<u64>>,
}

enum Transport {
    Tcp(TcpStream),
    #[cfg(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc"))]
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

/// Opens a connection to `address`, within `settings.connect_timeout` (TLS handshake included).
pub(crate) async fn connect(address: &Address, settings: &StreamSettings) -> Result<Stream> {
    match tokio::time::timeout(settings.connect_timeout, open(address, settings)).await {
        Ok(stream) => stream,
        Err(_) => Err(Error::Timeout),
    }
}

async fn open(address: &Address, settings: &StreamSettings) -> Result<Stream> {
    let tcp = TcpStream::connect((address.host.as_str(), address.port))
        .await
        .map_err(|error| Error::Redis(format!("cannot connect to {address}: {error}")))?;
    tcp.set_nodelay(settings.tcp_no_delay)
        .map_err(|error| Error::Redis(error.to_string()))?;
    if let Some(keep_alive) = &settings.keep_alive {
        SockRef::from(&tcp)
            .set_tcp_keepalive(keep_alive)
            .map_err(|error| Error::Redis(error.to_string()))?;
    }
    #[cfg(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc"))]
    if let Some(config) = &settings.tls {
        let name =
            rustls::pki_types::ServerName::try_from(address.host.clone()).map_err(|error| {
                Error::Config(format!("invalid TLS host name {}: {error}", address.host))
            })?;
        let tls = tokio_rustls::TlsConnector::from(config.clone())
            .connect(name, tcp)
            .await
            .map_err(|error| {
                Error::Redis(format!("TLS handshake with {address} failed: {error}"))
            })?;
        return Ok(Stream::new(Transport::Tls(Box::new(tls))));
    }
    Ok(Stream::new(Transport::Tcp(tcp)))
}

impl Stream {
    fn new(transport: Transport) -> Self {
        Self {
            transport,
            written: Arc::new(tokio::sync::watch::Sender::new(0)),
        }
    }

    /// Counts the bytes written to this stream from now on, and those written before.
    pub(crate) fn written(&self) -> Arc<tokio::sync::watch::Sender<u64>> {
        self.written.clone()
    }

    /// A second handle to the socket under this stream, to look at it while redis-rs owns the stream.
    pub(crate) fn probe(&self) -> Option<socket2::Socket> {
        #[cfg(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc"))]
        let tcp = match &self.transport {
            Transport::Tcp(tcp) => tcp,
            Transport::Tls(tls) => tls.get_ref().0,
        };
        #[cfg(not(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc")))]
        let Transport::Tcp(tcp) = &self.transport;
        SockRef::from(tcp).try_clone().ok()
    }
}

/// Whether the server closed `socket`: a read would end there (`peek` gives 0 bytes) or fail. Data still to read, or none yet, means open.
pub(crate) fn closed_by_server(socket: &socket2::Socket) -> bool {
    let mut byte = [std::mem::MaybeUninit::<u8>::uninit()];
    match socket.peek(&mut byte) {
        Ok(read) => read == 0,
        Err(error) => !matches!(
            error.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
        ),
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut self.get_mut().transport {
            Transport::Tcp(tcp) => Pin::new(tcp).poll_read(cx, buf),
            #[cfg(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc"))]
            Transport::Tls(tls) => Pin::new(tls).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let written = match &mut this.transport {
            Transport::Tcp(tcp) => Pin::new(tcp).poll_write(cx, buf),
            #[cfg(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc"))]
            Transport::Tls(tls) => Pin::new(tls).poll_write(cx, buf),
        };
        if let Poll::Ready(Ok(count)) = &written {
            let count = *count as u64;
            if count > 0 {
                this.written.send_modify(|total| *total += count);
            }
        }
        written
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().transport {
            Transport::Tcp(tcp) => Pin::new(tcp).poll_flush(cx),
            #[cfg(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc"))]
            Transport::Tls(tls) => Pin::new(tls).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().transport {
            Transport::Tcp(tcp) => Pin::new(tcp).poll_shutdown(cx),
            #[cfg(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc"))]
            Transport::Tls(tls) => Pin::new(tls).poll_shutdown(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{connect, Address, StreamSettings};
    use crate::error::Error;
    use std::time::{Duration, Instant};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn settings(timeout: Duration) -> StreamSettings {
        StreamSettings {
            connect_timeout: timeout,
            tcp_no_delay: true,
            keep_alive: None,
            #[cfg(any(feature = "tls-rustls", feature = "tls-rustls-aws-lc"))]
            tls: None,
        }
    }

    #[tokio::test]
    async fn the_bytes_written_are_counted() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = Address::new("127.0.0.1", listener.local_addr().unwrap().port());
        let mut stream = connect(&address, &settings(Duration::from_secs(1)))
            .await
            .unwrap();
        let written = stream.written();
        stream.write_all(b"PING\r\n").await.unwrap();
        stream.write_all(b"PING\r\n").await.unwrap();
        assert_eq!(*written.borrow(), 12);
    }

    #[tokio::test]
    async fn a_closed_port_fails_with_a_redis_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let address = Address::new("127.0.0.1", port);
        let result = connect(&address, &settings(Duration::from_secs(1))).await;
        assert!(matches!(result, Err(Error::Redis(_))));
    }

    #[tokio::test]
    async fn the_connect_timeout_limits_connecting() {
        let started = Instant::now();
        let address = Address::new("10.255.255.1", 6379);
        let result = connect(&address, &settings(Duration::from_millis(200))).await;
        assert!(result.is_err());
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn a_plain_stream_carries_bytes_both_ways() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0u8; 4];
            socket.read_exact(&mut buffer).await.unwrap();
            socket.write_all(&buffer).await.unwrap();
        });
        let mut stream = connect(
            &Address::new("127.0.0.1", port),
            &settings(Duration::from_secs(1)),
        )
        .await
        .unwrap();
        stream.write_all(b"ping").await.unwrap();
        let mut echoed = [0u8; 4];
        stream.read_exact(&mut echoed).await.unwrap();
        assert_eq!(&echoed, b"ping");
    }
}
