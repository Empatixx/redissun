use futures::TryStreamExt;
use redissun::Client;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct User {
    name: String,
    age: u32,
}

#[tokio::main]
async fn main() -> redissun::Result<()> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let client = Client::builder().url(url).build().await?;

    let users = client.map::<String, User>("example:users");
    users
        .insert(
            "jirka".into(),
            User {
                name: "Jirka".into(),
                age: 30,
            },
        )
        .await?;

    println!("jirka = {:?}", users.get("jirka").await?);
    println!("len = {}", users.len().await?);

    let all: Vec<(String, User)> = users.iter().try_collect().await?;
    println!("entries = {all:?}");

    users.clear().await?;
    Ok(())
}
