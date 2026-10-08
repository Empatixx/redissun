use futures::TryStreamExt;
use redissun::Client;

#[tokio::main]
async fn main() -> redissun::Result<()> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let client = Client::builder().url(url).build().await?;

    let names = client.vec::<String>("example:names");
    names.push("jirka").await?;
    names.push("eva").await?;
    names.insert(1, "petr").await?;
    let all: Vec<String> = names.iter().try_collect().await?;
    println!("names = {all:?}");
    names.clear().await?;

    let jobs = client.vec_deque::<String>("example:jobs");
    jobs.push_back("first").await?;
    jobs.push_back("second").await?;
    println!("next job = {:?}", jobs.pop_front().await?);
    jobs.clear().await?;

    let tags = client.hash_set::<String>("example:tags");
    tags.extend(["rust", "redis"]).await?;
    println!("has rust = {}", tags.contains("rust").await?);
    tags.clear().await?;
    Ok(())
}
