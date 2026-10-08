mod common;

use common::{client, unique};
use redissun::Client;

#[tokio::test]
async fn debug_output_never_contains_the_connection_url() {
    let builder = Client::builder().url("redis://user:supersecret@127.0.0.1:1");
    let text = format!("{builder:?}");
    assert!(text.contains("ClientBuilder"));
    assert!(!text.contains("supersecret"));
}

#[tokio::test]
async fn public_handles_print_their_name() {
    let client = client().await;
    let name = unique("debug");
    assert!(format!("{client:?}").contains("Client"));
    assert!(format!("{:?}", client.bucket::<String>(name.clone())).contains(&name));
    assert!(format!("{:?}", client.hash_map::<String, String>(name.clone())).contains(&name));
    let lock = client.lock(name.clone());
    assert!(format!("{lock:?}").contains(&name));
    let guard = lock.lock().await.unwrap();
    assert!(format!("{guard:?}").contains(&name));
    guard.unlock().await.unwrap();
}
