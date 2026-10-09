mod common;

use common::{client, connect_with, raw_command, redis_url, unique};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout, Instant};

async fn kill_every_client_connection() {
    let url = redis_url().await;
    let address = url.trim_start_matches("redis://").to_string();
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"*4\r\n$6\r\nCLIENT\r\n$4\r\nKILL\r\n$4\r\nTYPE\r\n$6\r\nnormal\r\n")
        .await
        .unwrap();
}

static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn clients_recover_and_waiters_still_wake_after_every_connection_is_killed() {
    let _turn = ONE_AT_A_TIME.lock().await;
    let holder_client = client().await;
    let waiter_client = client().await;
    let name = unique("lock");
    let bucket = holder_client.bucket::<String>(unique("bucket"));
    bucket.set(&"before".to_string()).await.unwrap();

    let holder = holder_client.lock(name.clone()).lock().await.unwrap();
    let waiter_lock = waiter_client.lock(name);
    let waiter = tokio::spawn(async move { waiter_lock.lock().await.unwrap() });
    sleep(Duration::from_millis(300)).await;

    kill_every_client_connection().await;

    let deadline = Instant::now() + Duration::from_secs(10);
    let recovered = loop {
        if let Ok(value) = bucket.get().await {
            break value;
        }
        assert!(Instant::now() < deadline, "client never reconnected");
        sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(recovered.as_deref(), Some("before"));

    holder.unlock().await.unwrap();
    let guard = timeout(Duration::from_secs(5), waiter)
        .await
        .expect("waiter should be notified after the pub/sub connection came back")
        .unwrap();
    guard.unlock().await.unwrap();
}

async fn kill_pubsub_connections() {
    common::raw_command(&["CLIENT", "KILL", "TYPE", "pubsub"]).await;
}

#[tokio::test]
async fn topic_subscriber_keeps_receiving_after_the_pubsub_connection_is_killed() {
    let _turn = ONE_AT_A_TIME.lock().await;
    let client = client().await;
    let topic = client.topic::<String>(unique("topic"));
    let mut subscriber = topic.subscribe().await.unwrap();

    kill_pubsub_connections().await;

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        topic.publish("ping").await.unwrap();
        if let Ok(message) = timeout(Duration::from_millis(300), subscriber.recv()).await {
            assert_eq!(message.unwrap(), "ping");
            break;
        }
        assert!(Instant::now() < deadline, "subscription was never restored");
    }
}

#[tokio::test]
async fn local_cached_map_sees_remote_changes_after_the_pubsub_connection_is_killed() {
    let _turn = ONE_AT_A_TIME.lock().await;
    let reader_client = client().await;
    let writer_client = client().await;
    let name = unique("lcm");
    let reader = reader_client
        .local_cached_map::<String, String>(name.clone())
        .build()
        .await
        .unwrap();
    let writer = writer_client
        .local_cached_map::<String, String>(name)
        .build()
        .await
        .unwrap();
    writer.insert("key", "v1").await.unwrap();
    assert_eq!(reader.get("key").await.unwrap().as_deref(), Some("v1"));

    kill_pubsub_connections().await;
    sleep(Duration::from_secs(2)).await;
    assert_eq!(reader.get("key").await.unwrap().as_deref(), Some("v1"));

    writer.insert("key", "v2").await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while reader.get("key").await.unwrap().as_deref() != Some("v2") {
        assert!(
            Instant::now() < deadline,
            "reader kept serving a stale value from its local cache"
        );
        sleep(Duration::from_millis(100)).await;
    }
}

struct AclUser {
    name: String,
}

impl AclUser {
    async fn create() -> AclUser {
        let name = format!("flaky{}", uuid::Uuid::new_v4().simple());
        let reply = raw_command(&[
            "ACL", "SETUSER", &name, "on", ">secret", "~*", "&*", "+@all",
        ])
        .await;
        assert!(reply.starts_with("+OK"), "{reply}");
        AclUser { name }
    }

    async fn client(&self) -> redissun::Client {
        let url = redis_url()
            .await
            .replace("redis://", &format!("redis://{}:secret@", self.name));
        connect_with(|builder| builder.url(url.clone())).await
    }

    async fn allow_subscribe(&self, allowed: bool) {
        let rule = if allowed { "+@pubsub" } else { "-@pubsub" };
        let reply = raw_command(&["ACL", "SETUSER", &self.name, rule, "+publish", "+ping"]).await;
        assert!(reply.starts_with("+OK"), "{reply}");
    }

    async fn kill_connections(&self) {
        raw_command(&["CLIENT", "KILL", "USER", &self.name]).await;
    }

    async fn delete(self) {
        raw_command(&["ACL", "DELUSER", &self.name]).await;
    }
}

async fn receives_again(
    publisher: &redissun::Topic<String, redissun::JsonCodec>,
    subscriber: &mut redissun::Subscriber<String, redissun::JsonCodec>,
    within: Duration,
) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        publisher.publish("ping").await.unwrap();
        if let Ok(message) = timeout(Duration::from_millis(300), subscriber.recv()).await {
            assert_eq!(message.unwrap(), "ping");
            return true;
        }
    }
    false
}

#[tokio::test]
async fn reattach() {
    let _turn = ONE_AT_A_TIME.lock().await;
    let client = client().await;
    let name = unique("topic");
    let topic = client.topic::<String>(name.clone());
    let mut subscriber = topic.subscribe().await.unwrap();
    let mut pattern = client
        .pattern_topic::<String>(format!("{name}*"))
        .subscribe()
        .await
        .unwrap();

    kill_pubsub_connections().await;
    sleep(Duration::from_millis(500)).await;

    assert!(receives_again(&topic, &mut subscriber, Duration::from_secs(10)).await);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match timeout(Duration::from_millis(300), pattern.recv()).await {
            Ok(Ok((channel, message))) => {
                assert_eq!(channel, name);
                assert_eq!(message, "ping");
                break;
            }
            Ok(Err(redissun::Error::Lagged(_))) => {}
            other => {
                assert!(
                    Instant::now() < deadline,
                    "pattern was not restored: {other:?}"
                );
                topic.publish("ping").await.unwrap();
            }
        }
    }
}

#[tokio::test]
async fn a_failed_resubscribe_is_retried_until_it_succeeds() {
    let _turn = ONE_AT_A_TIME.lock().await;
    let user = AclUser::create().await;
    let client = user.client().await;
    let name = unique("topic");
    let topic = client.topic::<String>(name.clone());
    let mut subscriber = topic.subscribe().await.unwrap();
    let publisher = common::client().await.topic::<String>(name);
    assert!(receives_again(&publisher, &mut subscriber, Duration::from_secs(5)).await);

    user.allow_subscribe(false).await;
    user.kill_connections().await;
    sleep(Duration::from_millis(2500)).await;
    assert_eq!(publisher.publish("lost").await.unwrap(), 0);

    user.allow_subscribe(true).await;
    assert!(
        receives_again(&publisher, &mut subscriber, Duration::from_secs(10)).await,
        "the subscription was never restored after the first resubscribe failed"
    );
    user.delete().await;
}

#[tokio::test]
async fn add_listener_failover() {
    let _turn = ONE_AT_A_TIME.lock().await;
    let user = AclUser::create().await;
    let client = user.client().await;
    let topic = client.topic::<String>(unique("topic"));

    user.allow_subscribe(false).await;
    assert!(topic.subscribe().await.is_err());
    user.allow_subscribe(true).await;

    let mut subscriber = topic.subscribe().await.unwrap();
    assert!(receives_again(&topic, &mut subscriber, Duration::from_secs(5)).await);
    user.delete().await;
}
