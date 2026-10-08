use redgrid::Client;
use std::time::Duration;

#[tokio::main]
async fn main() -> redgrid::Result<()> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let client = Client::builder().url(url).build().await?;

    let lock = client.lock("example:lock");
    let guard = lock.lock().await?;
    println!(
        "holding the lock, hold count = {}",
        lock.hold_count().await?
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    guard.unlock().await?;
    println!("released, locked = {}", lock.is_locked().await?);
    Ok(())
}
