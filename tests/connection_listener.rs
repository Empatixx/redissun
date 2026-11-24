//! Redisson's `ConnectionListener`: told when a node gets connected and when it loses its connections.

mod common;

use common::switch::Switch;
use common::topology::Topology;
use common::unique;
use redissun::{Client, ConnectionListener, DelayStrategy, NodeType, ReadMode};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::{sleep, Instant};

#[derive(Clone, Debug, PartialEq)]
enum Event {
    Connect(String, NodeType),
    Disconnect(String, NodeType),
}

#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<Event>>>);

impl Recorder {
    fn events(&self) -> Vec<Event> {
        self.0.lock().unwrap().clone()
    }

    fn count(&self, wanted: impl Fn(&Event) -> bool) -> usize {
        self.events().iter().filter(|event| wanted(event)).count()
    }

    async fn wait_until(&self, done: impl Fn(&[Event]) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while !done(&self.events()) {
            assert!(Instant::now() < deadline, "{:?}", self.events());
            sleep(Duration::from_millis(50)).await;
        }
    }
}

impl ConnectionListener for Recorder {
    fn on_connect(&self, address: &str, node: NodeType) {
        self.0
            .lock()
            .unwrap()
            .push(Event::Connect(address.to_string(), node));
    }

    fn on_disconnect(&self, address: &str, node: NodeType) {
        self.0
            .lock()
            .unwrap()
            .push(Event::Disconnect(address.to_string(), node));
    }
}

async fn through(switch: &Switch, recorder: &Recorder) -> Client {
    Client::builder()
        .url(format!("redis://127.0.0.1:{}", switch.port))
        .pool_size(3)
        .reconnection_delay(DelayStrategy::Constant(Duration::from_millis(50)))
        .ping_connection_interval(Duration::ZERO)
        .timeout(Duration::from_secs(5))
        .connection_listener(recorder.clone())
        .build()
        .await
        .unwrap()
}

#[tokio::test]
async fn a_pool_of_several_connections_is_one_connect() {
    let switch = Switch::start().await;
    let recorder = Recorder::default();
    let _client = through(&switch, &recorder).await;
    let address = format!("127.0.0.1:{}", switch.port);
    assert_eq!(
        recorder.events(),
        vec![Event::Connect(address, NodeType::Master)]
    );
}

#[tokio::test]
async fn losing_the_node_is_one_disconnect_and_getting_it_back_one_connect() {
    let switch = Switch::start().await;
    let recorder = Recorder::default();
    let client = through(&switch, &recorder).await;
    let address = format!("127.0.0.1:{}", switch.port);
    switch.off();
    recorder.wait_until(|events| events.len() >= 2).await;
    sleep(Duration::from_millis(200)).await;
    assert_eq!(
        recorder.events(),
        vec![
            Event::Connect(address.clone(), NodeType::Master),
            Event::Disconnect(address.clone(), NodeType::Master),
        ]
    );
    switch.on();
    client
        .bucket::<String>(unique("back"))
        .set("again")
        .await
        .unwrap();
    assert_eq!(
        recorder.events().last(),
        Some(&Event::Connect(address, NodeType::Master))
    );
    assert_eq!(recorder.events().len(), 3);
}

#[tokio::test]
async fn a_connection_of_its_own_coming_and_going_is_no_event() {
    let switch = Switch::start().await;
    let recorder = Recorder::default();
    let client = through(&switch, &recorder).await;
    let queue = client.vec_deque::<String>(unique("blocking"));
    let waited = queue
        .pop_front_wait()
        .timeout(Duration::from_millis(200))
        .await
        .unwrap();
    assert_eq!(waited, None);
    sleep(Duration::from_millis(200)).await;
    assert_eq!(recorder.events().len(), 1, "{:?}", recorder.events());
    // The blocking connection is not counted any more, so losing the pool is the node's disconnect.
    switch.off();
    recorder.wait_until(|events| events.len() >= 2).await;
    assert!(matches!(
        recorder.events()[1],
        Event::Disconnect(_, NodeType::Master)
    ));
}

#[tokio::test]
async fn a_dropped_client_reports_nothing_more() {
    let switch = Switch::start().await;
    let recorder = Recorder::default();
    let client = through(&switch, &recorder).await;
    drop(client);
    sleep(Duration::from_millis(500)).await;
    assert_eq!(recorder.events().len(), 1, "{:?}", recorder.events());
}

#[tokio::test]
async fn sentinel_master_and_replicas_are_told_apart() {
    let sentinel = Topology::sentinel().await;
    let recorder = Recorder::default();
    let client = Client::builder()
        .url(sentinel.sentinel_url())
        .read_mode(ReadMode::Replica)
        .connection_listener(recorder.clone())
        .build()
        .await
        .unwrap();
    let bucket = client.bucket::<String>(unique("kinds"));
    bucket.set("value").await.unwrap();
    let _: Option<String> = bucket.get().await.unwrap();
    let master = sentinel.sentinel_master_port().await;
    // A replica pool opens with the first read that goes to it, once Sentinel has reported the replicas.
    let deadline = Instant::now() + Duration::from_secs(30);
    while recorder.count(|event| matches!(event, Event::Connect(_, NodeType::Replica))) == 0 {
        assert!(Instant::now() < deadline, "{:?}", recorder.events());
        let _: Option<String> = bucket.get().await.unwrap();
        sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        recorder.count(
            |event| *event == Event::Connect(format!("127.0.0.1:{master}"), NodeType::Master)
        ),
        1,
        "{:?}",
        recorder.events()
    );
    assert!(
        recorder.count(|event| matches!(event, Event::Connect(_, NodeType::Sentinel))) >= 1,
        "{:?}",
        recorder.events()
    );
    // Sentinel is asked again every second for the replicas; its connections are kept, so that is no new event.
    let before = recorder.count(|event| matches!(event, Event::Connect(_, NodeType::Sentinel)));
    sleep(Duration::from_secs(3)).await;
    assert_eq!(
        recorder.count(|event| matches!(event, Event::Connect(_, NodeType::Sentinel))),
        before
    );
    assert_eq!(
        recorder.count(|event| matches!(event, Event::Disconnect(..))),
        0,
        "{:?}",
        recorder.events()
    );
}
