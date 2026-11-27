//! A TCP proxy to the test Redis that loses one reply: the command reaches Redis, the reply never reaches the client.

use super::redis_url;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A proxy to Redis. Once [`arm`](Cutter::arm)ed, the next chunk a client sends that contains every marker is forwarded to Redis, and then the connection is closed before the reply is relayed, as when a connection dies after Redis ran the command.
pub struct Cutter {
    pub port: u16,
    armed: Arc<Mutex<Option<Vec<Vec<u8>>>>>,
    cuts: Arc<std::sync::atomic::AtomicUsize>,
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    needle.is_empty()
        || haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

impl Cutter {
    pub async fn start() -> Cutter {
        let target = redis_url().await.trim_start_matches("redis://").to_string();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let armed: Arc<Mutex<Option<Vec<Vec<u8>>>>> = Arc::default();
        let cuts: Arc<std::sync::atomic::AtomicUsize> = Arc::default();
        let (shared, counted) = (armed.clone(), cuts.clone());
        tokio::spawn(async move {
            while let Ok((mut client, _)) = listener.accept().await {
                let (target, armed, cuts) = (target.clone(), shared.clone(), counted.clone());
                tokio::spawn(async move {
                    let Ok(mut server) = TcpStream::connect(target).await else {
                        return;
                    };
                    let (mut up, mut down) = (vec![0u8; 64 * 1024], vec![0u8; 64 * 1024]);
                    loop {
                        tokio::select! {
                            read = client.read(&mut up) => {
                                let Ok(n) = read else { return };
                                if n == 0 {
                                    return;
                                }
                                let chunk = &up[..n];
                                let cut = {
                                    let mut armed = armed.lock().unwrap();
                                    let hit = armed
                                        .as_ref()
                                        .is_some_and(|markers| markers.iter().all(|m| contains(chunk, m)));
                                    if hit {
                                        *armed = None;
                                    }
                                    hit
                                };
                                if server.write_all(chunk).await.is_err() {
                                    return;
                                }
                                if cut {
                                    // Redis runs the command; its reply stays unread, and both sockets close.
                                    tokio::time::sleep(Duration::from_millis(100)).await;
                                    cuts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                    return;
                                }
                            }
                            read = server.read(&mut down) => {
                                let Ok(n) = read else { return };
                                if n == 0 || client.write_all(&down[..n]).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                });
            }
        });
        Cutter { port, armed, cuts }
    }

    /// Loses the reply of the next command whose bytes contain every one of `markers`.
    pub fn arm(&self, markers: &[&str]) {
        *self.armed.lock().unwrap() = Some(markers.iter().map(|m| m.as_bytes().to_vec()).collect());
    }

    /// How many replies were lost so far.
    pub fn cuts(&self) -> usize {
        self.cuts.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// A client through this proxy that retries a failed command (4 attempts), without pings.
    pub async fn client(&self) -> redissun::Client {
        redissun::Client::builder()
            .url(format!("redis://127.0.0.1:{}", self.port))
            .timeout(Duration::from_secs(5))
            .reconnection_delay(redissun::DelayStrategy::Constant(Duration::from_millis(50)))
            .ping_connection_interval(Duration::ZERO)
            .build()
            .await
            .unwrap()
    }
}
