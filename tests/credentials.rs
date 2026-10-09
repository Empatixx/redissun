mod common;

use common::{raw_command, redis_url, unique};
use redissun::{Client, Credentials, Error};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

struct AclUser {
    name: String,
    password: String,
}

impl AclUser {
    async fn create() -> AclUser {
        let name = unique("user").replace(':', "-");
        let password = unique("pw").replace(':', "-");
        let reply = raw_command(&[
            "ACL",
            "SETUSER",
            &name,
            "on",
            &format!(">{password}"),
            "~*",
            "&*",
            "+@all",
        ])
        .await;
        assert_eq!(reply, "+OK\r\n");
        AclUser { name, password }
    }

    async fn delete(self) {
        raw_command(&["ACL", "DELUSER", &self.name]).await;
    }
}

fn address(url: &str) -> String {
    url.trim_start_matches("redis://").to_string()
}

async fn works(client: &Client) {
    let bucket = client.bucket::<String>(unique("auth"));
    bucket.set("value").await.unwrap();
    assert_eq!(bucket.get().await.unwrap().as_deref(), Some("value"));
}

#[tokio::test]
async fn username_and_password_log_in_as_the_acl_user() {
    let user = AclUser::create().await;
    let client = Client::builder()
        .url(redis_url().await)
        .username(user.name.clone())
        .password(user.password.clone())
        .client_name(user.name.clone())
        .build()
        .await
        .unwrap();
    works(&client).await;
    let list = raw_command(&["CLIENT", "LIST"]).await;
    let line = list
        .lines()
        .find(|line| line.contains(&format!(" name={} ", user.name)))
        .unwrap();
    assert!(line.contains(&format!(" user={} ", user.name)), "{line}");
    user.delete().await;
}

#[tokio::test]
async fn builder_credentials_replace_the_url_credentials() {
    let user = AclUser::create().await;
    let url = format!("redis://nobody:wrong@{}", address(&redis_url().await));
    let client = Client::builder()
        .url(url)
        .username(user.name.clone())
        .password(user.password.clone())
        .build()
        .await
        .unwrap();
    works(&client).await;
    user.delete().await;
}

#[tokio::test]
async fn a_wrong_password_fails() {
    let user = AclUser::create().await;
    let attempt = Client::builder()
        .url(redis_url().await)
        .username(user.name.clone())
        .password("wrong")
        .connect_timeout(Duration::from_secs(5))
        .build()
        .await;
    assert!(attempt.is_err());
    user.delete().await;
}

#[tokio::test]
async fn the_resolver_is_asked_for_every_connection() {
    let user = AclUser::create().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let addresses = Arc::new(std::sync::Mutex::new(Vec::new()));
    let credentials = Credentials::new(user.name.clone(), user.password.clone());
    let client = {
        let (calls, addresses) = (calls.clone(), addresses.clone());
        Client::builder()
            .url(redis_url().await)
            .pool_size(2)
            .credentials_resolver(move |address| {
                calls.fetch_add(1, Ordering::SeqCst);
                addresses.lock().unwrap().push(address);
                let credentials = credentials.clone();
                async move { Ok(credentials) }
            })
            .build()
            .await
            .unwrap()
    };
    works(&client).await;
    assert!(calls.load(Ordering::SeqCst) >= 2);
    let expected = address(&redis_url().await);
    let port = expected.rsplit(':').next().unwrap().to_string();
    assert!(addresses
        .lock()
        .unwrap()
        .iter()
        .all(|seen| seen.ends_with(&format!(":{port}"))));
    user.delete().await;
}

#[tokio::test]
async fn a_resolver_error_fails_the_connection() {
    let attempt = Client::builder()
        .url(redis_url().await)
        .connect_timeout(Duration::from_secs(5))
        .credentials_resolver(|_| async { Err(Error::Config("vault is down".into())) })
        .build()
        .await;
    assert!(attempt.is_err());
}

#[test]
fn debug_hides_the_password() {
    let text = format!("{:?}", Credentials::new("app", "hunter2"));
    assert!(text.contains("app"));
    assert!(!text.contains("hunter2"));
}
