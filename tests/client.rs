mod common;

use common::redis_url;
use redissun::{Client, Error};
use std::time::Duration;

#[tokio::test]
async fn missing_url_is_a_config_error() {
    let result = Client::builder().build().await;
    assert!(matches!(result, Err(Error::Config(_))));
}

#[tokio::test]
async fn zero_pool_size_is_a_config_error() {
    let result = Client::builder()
        .url(redis_url().await)
        .pool_size(0)
        .build()
        .await;
    assert!(matches!(result, Err(Error::Config(_))));
}

#[tokio::test]
async fn zero_lock_lease_is_a_config_error() {
    let result = Client::builder()
        .url(redis_url().await)
        .lock_lease(Duration::ZERO)
        .build()
        .await;
    assert!(matches!(result, Err(Error::Config(_))));
}

#[tokio::test]
async fn malformed_url_is_a_config_error() {
    let result = Client::builder().url("not a url").build().await;
    assert!(matches!(result, Err(Error::Config(_))));
}

#[tokio::test]
async fn unreachable_server_fails_instead_of_hanging() {
    let attempt = tokio::time::timeout(
        Duration::from_secs(15),
        Client::builder().url("redis://127.0.0.1:1").build(),
    )
    .await;
    assert!(matches!(attempt, Ok(Err(_))));
}

#[tokio::test]
async fn zero_connect_timeout_is_a_config_error() {
    let result = Client::builder()
        .url(redis_url().await)
        .connect_timeout(Duration::ZERO)
        .build()
        .await;
    assert!(matches!(result, Err(Error::Config(_))));
}
