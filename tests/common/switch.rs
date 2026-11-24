//! A TCP proxy to the test Redis that can be switched off.

use super::redis_url;
use tokio::net::TcpStream;

/// A TCP proxy to Redis that can be switched off: it then closes its connections and hangs up on new ones.
pub struct Switch {
    pub port: u16,
    on: std::sync::Arc<std::sync::atomic::AtomicBool>,
    generation: std::sync::Arc<tokio::sync::watch::Sender<u64>>,
    /// Connections that arrived while the switch was off.
    pub refused: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl Switch {
    pub async fn start() -> Switch {
        let target = redis_url().await.trim_start_matches("redis://").to_string();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let on = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let generation = std::sync::Arc::new(tokio::sync::watch::Sender::new(0u64));
        let refused = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (accepting, epoch, counted) = (on.clone(), generation.clone(), refused.clone());
        tokio::spawn(async move {
            while let Ok((mut client, _)) = listener.accept().await {
                if !accepting.load(std::sync::atomic::Ordering::SeqCst) {
                    counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    continue;
                }
                let target = target.clone();
                let mut ended = epoch.subscribe();
                tokio::spawn(async move {
                    let Ok(mut server) = TcpStream::connect(target).await else {
                        return;
                    };
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut client, &mut server) => {}
                        _ = ended.changed() => {}
                    }
                });
            }
        });
        Switch {
            port,
            on,
            generation,
            refused,
        }
    }

    pub fn off(&self) {
        self.on.store(false, std::sync::atomic::Ordering::SeqCst);
        self.generation.send_modify(|generation| *generation += 1);
    }

    pub fn on(&self) {
        self.on.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}
