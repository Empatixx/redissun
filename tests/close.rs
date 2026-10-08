mod common;

use common::{client, redis_url};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, Instant};

async fn connected_clients() -> usize {
    let url = redis_url().await;
    let address = url.trim_start_matches("redis://").to_string();
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(b"INFO clients\r\n").await.unwrap();
    let mut buffer = [0u8; 4096];
    let read = stream.read(&mut buffer).await.unwrap();
    let text = String::from_utf8_lossy(&buffer[..read]).to_string();
    text.lines()
        .find_map(|line| line.strip_prefix("connected_clients:"))
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

#[tokio::test]
async fn dropping_the_last_client_closes_its_connections() {
    let baseline = connected_clients().await;
    let client = client().await;
    assert!(connected_clients().await > baseline);

    drop(client);

    let deadline = Instant::now() + Duration::from_secs(5);
    while connected_clients().await > baseline {
        assert!(
            Instant::now() < deadline,
            "connections were not closed after the client was dropped"
        );
        sleep(Duration::from_millis(100)).await;
    }
}
