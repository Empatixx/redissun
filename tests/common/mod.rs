#![allow(dead_code)]

use redissun::{Client, ClientBuilder};
use std::sync::OnceLock;
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::redis::Redis;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::OnceCell;
use uuid::Uuid;

static CONTAINER: OnceCell<(ContainerAsync<Redis>, String)> = OnceCell::const_new();
static CONTAINER_ID: OnceLock<String> = OnceLock::new();

extern "C" fn remove_container() {
    if let Some(id) = CONTAINER_ID.get() {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", id])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
}

pub async fn redis_url() -> String {
    if let Ok(url) = std::env::var("REDISSUN_TEST_REDIS_URL") {
        return url;
    }
    CONTAINER
        .get_or_init(|| async {
            let container = Redis::default().with_tag("7.4").start().await.unwrap();
            let port = container.get_host_port_ipv4(6379).await.unwrap();
            if CONTAINER_ID.set(container.id().to_string()).is_ok() {
                unsafe {
                    libc::atexit(remove_container);
                }
            }
            wait_until_ready(port).await;
            (container, format!("redis://127.0.0.1:{port}"))
        })
        .await
        .1
        .clone()
}

pub async fn connect_with(configure: impl Fn(ClientBuilder) -> ClientBuilder) -> Client {
    let url = redis_url().await;
    let mut last = None;
    for _ in 0..30 {
        match configure(Client::builder().url(url.clone())).build().await {
            Ok(client) => return client,
            Err(error) => last = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("could not connect to redis: {:?}", last);
}

pub async fn client() -> Client {
    connect_with(|builder| builder).await
}

pub fn unique(prefix: &str) -> String {
    format!("{prefix}:{}", Uuid::new_v4())
}

async fn wait_until_ready(port: u16) {
    for _ in 0..100 {
        if ping(port).await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("redis did not answer PING on port {port}");
}

async fn ping(port: u16) -> bool {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)).await else {
        return false;
    };
    let mut reply = [0u8; 7];
    stream.write_all(b"PING\r\n").await.is_ok()
        && stream.read_exact(&mut reply).await.is_ok()
        && &reply == b"+PONG\r\n"
}

pub async fn raw_command(parts: &[&str]) -> String {
    let url = redis_url().await;
    let address = url.trim_start_matches("redis://").to_string();
    let mut stream = TcpStream::connect(address).await.unwrap();
    let mut request = format!("*{}\r\n", parts.len());
    for part in parts {
        request.push_str(&format!("${}\r\n{}\r\n", part.len(), part));
    }
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut buffer = vec![0u8; 16384];
    let read = stream.read(&mut buffer).await.unwrap();
    String::from_utf8_lossy(&buffer[..read]).to_string()
}

pub async fn subscribed_channels(pattern: &str) -> usize {
    let reply = raw_command(&["PUBSUB", "CHANNELS", pattern]).await;
    reply.lines().filter(|line| line.starts_with('$')).count()
}
