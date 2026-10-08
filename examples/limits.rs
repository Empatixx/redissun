use redissun::Object;
use redissun::{Client, RateType};
use std::time::Duration;

#[tokio::main]
async fn main() -> redissun::Result<()> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let client = Client::builder().url(url).build().await?;

    let limiter = client.rate_limiter("example:api");
    limiter
        .try_set_rate(RateType::Overall, 5, Duration::from_secs(1))
        .await?;
    for call in 1..=7 {
        println!("call {call} allowed: {}", limiter.try_acquire(1).await?);
    }

    let semaphore = client.semaphore("example:workers");
    semaphore.try_set_permits(2).await?;
    let first = semaphore.acquire(1).await?;
    let second = semaphore.acquire(1).await?;
    println!("workers left: {}", semaphore.available_permits().await?);
    first.release().await?;
    second.release().await?;

    let counter = client.atomic_long("example:visits");
    println!("visits: {}", counter.incr().await?);

    let latch = client.count_down_latch("example:ready");
    latch.try_set_count(1).await?;
    latch.count_down().await?;
    latch.wait().await?;
    println!("latch open");

    limiter.del().await?;
    semaphore.del().await?;
    counter.del().await?;
    Ok(())
}
