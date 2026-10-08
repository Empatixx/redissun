use redissun::Client;
use std::time::Duration;

#[tokio::main]
async fn main() -> redissun::Result<()> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let client = Client::builder().url(url).build().await?;

    let news = client.topic::<String>("example:news");
    let mut subscriber = news.subscribe().await?;
    news.publish("hello").await?;
    println!("received = {}", subscriber.recv().await?);

    let jobs = client.vec_deque::<String>("example:jobs");
    println!(
        "waiting for a job = {:?}",
        jobs.pop_front_for(Duration::from_millis(200)).await?
    );
    Ok(())
}
