#![allow(dead_code)]

use redgrid::Client;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::redis::Redis;
use tokio::sync::OnceCell;
use uuid::Uuid;

static CONTAINER: OnceCell<(ContainerAsync<Redis>, String)> = OnceCell::const_new();

pub async fn redis_url() -> String {
    if let Ok(url) = std::env::var("REDGRID_TEST_REDIS_URL") {
        return url;
    }
    CONTAINER
        .get_or_init(|| async {
            let container = Redis::default().with_tag("7.4").start().await.unwrap();
            let port = container.get_host_port_ipv4(6379).await.unwrap();
            (container, format!("redis://127.0.0.1:{port}"))
        })
        .await
        .1
        .clone()
}

pub async fn client() -> Client {
    Client::builder()
        .url(redis_url().await)
        .build()
        .await
        .unwrap()
}

pub fn unique(prefix: &str) -> String {
    format!("{prefix}:{}", Uuid::new_v4())
}
