use crate::error::Error;
use crate::error::Result;
use crate::net::node::{Node, NodeSettings, Subscription as Command};
use crate::net::topology::Topology;
use bytes::Bytes;
use redis::{PushInfo, PushKind, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::{broadcast, mpsc, oneshot, Mutex, Notify};

const MESSAGE_BUFFER: usize = 256;
const RESUBSCRIBE_PAUSE: Duration = Duration::from_secs(1);

pub(crate) type PatternMessage = (String, Bytes);

struct Activation {
    active: Arc<AtomicBool>,
    done: oneshot::Sender<()>,
}

pub(crate) struct Entry<T> {
    notify: Notify,
    messages: std::sync::Mutex<Option<broadcast::Sender<T>>>,
    lost: broadcast::Sender<usize>,
    holders: AtomicUsize,
    active: Arc<AtomicBool>,
}

impl<T: Clone> Entry<T> {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            notify: Notify::new(),
            messages: std::sync::Mutex::new(Some(broadcast::channel(MESSAGE_BUFFER).0)),
            lost: broadcast::channel(16).0,
            holders: AtomicUsize::new(1),
            active: Arc::new(AtomicBool::new(false)),
        })
    }

    fn active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    fn deliver(&self, message: T) {
        if !self.active() {
            return;
        }
        self.notify.notify_waiters();
        if let Some(sender) = self.sender().as_ref() {
            let _ = sender.send(message);
        }
    }

    fn receiver(&self) -> broadcast::Receiver<T> {
        match self.sender().as_ref() {
            Some(sender) => sender.subscribe(),
            None => broadcast::channel(1).1,
        }
    }

    fn close(&self) {
        self.sender().take();
        self.notify.notify_waiters();
    }

    fn sender(&self) -> std::sync::MutexGuard<'_, Option<broadcast::Sender<T>>> {
        self.messages.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[derive(Default)]
pub(crate) struct Registry {
    channels: HashMap<String, Arc<Entry<Bytes>>>,
    patterns: HashMap<String, Arc<Entry<PatternMessage>>>,
}

impl Registry {
    fn wake_all(&self) {
        self.channels
            .values()
            .for_each(|entry| entry.notify.notify_waiters());
        self.patterns
            .values()
            .for_each(|entry| entry.notify.notify_waiters());
    }
}

pub(crate) trait Delivery: Clone + Send + Sync + 'static {
    const PATTERN: bool;

    fn entries(registry: &mut Registry) -> &mut HashMap<String, Arc<Entry<Self>>>;
}

impl Delivery for Bytes {
    const PATTERN: bool = false;

    fn entries(registry: &mut Registry) -> &mut HashMap<String, Arc<Entry<Self>>> {
        &mut registry.channels
    }
}

impl Delivery for PatternMessage {
    const PATTERN: bool = true;

    fn entries(registry: &mut Registry) -> &mut HashMap<String, Arc<Entry<Self>>> {
        &mut registry.patterns
    }
}

pub(crate) struct PubSub {
    topology: Arc<Topology>,
    push: mpsc::UnboundedSender<PushInfo>,
    node: Mutex<Arc<Node>>,
    registry: Mutex<Registry>,
    reconnects: broadcast::Sender<()>,
    activations: mpsc::UnboundedSender<Activation>,
}

/// Any key: the pub/sub connection goes to the master of its slot. Redis Cluster passes a published message to every node, so one node hears every channel.
const PUBSUB_KEY: &[u8] = b"redissun:pubsub";

pub(crate) struct Subscription<T: Delivery = Bytes> {
    pubsub: Arc<PubSub>,
    name: String,
    entry: Arc<Entry<T>>,
}

impl<T: Delivery> Subscription<T> {
    pub(crate) fn notify(&self) -> &Notify {
        &self.entry.notify
    }

    pub(crate) fn lost(&self) -> broadcast::Receiver<usize> {
        self.entry.lost.subscribe()
    }
}

impl<T: Delivery> Drop for Subscription<T> {
    fn drop(&mut self) {
        if self.entry.holders.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        let Ok(runtime) = Handle::try_current() else {
            return;
        };
        let pubsub = self.pubsub.clone();
        let name = std::mem::take(&mut self.name);
        runtime.spawn(async move { pubsub.release::<T>(name).await });
    }
}

impl PubSub {
    /// Opens the pub/sub connection: RESP3, so messages arrive as push frames, to the master that serves `PUBSUB_KEY`.
    pub(crate) async fn start(topology: Arc<Topology>) -> Result<Arc<Self>> {
        let (push, pushes) = mpsc::unbounded_channel();
        let node = open(&topology, &push).await?;
        node.send(&redis::cmd("PING")).await.map_err(Error::from)?;
        let reconnects = broadcast::channel(16).0;
        let (activations, requests) = mpsc::unbounded_channel();
        let pubsub = Arc::new(Self {
            topology,
            push,
            node: Mutex::new(node),
            registry: Mutex::new(Registry::default()),
            reconnects,
            activations,
        });
        tokio::spawn(dispatch(Arc::downgrade(&pubsub), pushes, requests));
        Ok(pubsub)
    }

    /// The pub/sub connection, for the `PING` check.
    pub(crate) async fn node(&self) -> Arc<Node> {
        self.node.lock().await.clone()
    }

    pub(crate) fn reconnects(&self) -> broadcast::Receiver<()> {
        self.reconnects.subscribe()
    }

    pub(crate) async fn quit(&self) {
        self.node.lock().await.reset().await;
    }

    async fn listen(&self, names: Vec<String>, pattern: bool) -> Result<()> {
        let command = if pattern {
            Command::Psubscribe
        } else {
            Command::Subscribe
        };
        let node = self.node().await;
        node.subscription(command, &names)
            .await
            .map_err(Error::from)?;
        Ok(())
    }

    async fn unlisten(&self, name: String, pattern: bool) {
        let command = if pattern {
            Command::Punsubscribe
        } else {
            Command::Unsubscribe
        };
        let node = self.node().await;
        let _ = node.subscription(command, &[name]).await;
    }

    pub(crate) async fn subscribe(self: &Arc<Self>, channel: &str) -> Result<Subscription> {
        Ok(self.subscribe_with_messages(channel).await?.0)
    }

    pub(crate) async fn subscribe_with_messages(
        self: &Arc<Self>,
        channel: &str,
    ) -> Result<(Subscription, broadcast::Receiver<Bytes>)> {
        self.join(channel).await
    }

    pub(crate) async fn psubscribe_with_messages(
        self: &Arc<Self>,
        pattern: &str,
    ) -> Result<(
        Subscription<PatternMessage>,
        broadcast::Receiver<PatternMessage>,
    )> {
        self.join(pattern).await
    }

    async fn join<T: Delivery>(
        self: &Arc<Self>,
        name: &str,
    ) -> Result<(Subscription<T>, broadcast::Receiver<T>)> {
        // Subscribing runs to its end on a task of its own, even when the caller stops waiting (a lock wait that times out), so a channel Redis subscribed always gets its entry. A subscription nobody took comes back here, and dropping it unsubscribes.
        let (handoff, taken) = oneshot::channel();
        let this = self.clone();
        let owned = name.to_string();
        tokio::spawn(async move {
            let _ = handoff.send(this.enter::<T>(&owned).await);
        });
        let (subscription, receiver) = taken
            .await
            .map_err(|_| Error::Redis("the subscription was abandoned".into()))??;
        self.activate(&subscription.entry.active).await;
        Ok((subscription, receiver))
    }

    async fn enter<T: Delivery>(
        self: &Arc<Self>,
        name: &str,
    ) -> Result<(Subscription<T>, broadcast::Receiver<T>)> {
        let (subscription, receiver) = {
            let mut registry = self.registry.lock().await;
            let existing = T::entries(&mut registry).get(name).cloned();
            let entry = match existing {
                Some(entry) => {
                    entry.holders.fetch_add(1, Ordering::AcqRel);
                    entry
                }
                None => {
                    self.listen(vec![name.to_string()], T::PATTERN).await?;
                    // The reply to PING comes after the subscription's confirmation, so the subscription is in place once it arrives.
                    self.node()
                        .await
                        .send(&redis::cmd("PING"))
                        .await
                        .map_err(Error::from)?;
                    let entry = Entry::new();
                    T::entries(&mut registry).insert(name.to_string(), entry.clone());
                    entry
                }
            };
            let receiver = entry.receiver();
            let subscription = Subscription {
                pubsub: self.clone(),
                name: name.to_string(),
                entry,
            };
            (subscription, receiver)
        };
        Ok((subscription, receiver))
    }

    async fn activate(&self, active: &Arc<AtomicBool>) {
        if active.load(Ordering::Acquire) {
            return;
        }
        let (done, finished) = oneshot::channel();
        let request = Activation {
            active: active.clone(),
            done,
        };
        if self.activations.send(request).is_err() || finished.await.is_err() {
            active.store(true, Ordering::Release);
        }
    }

    pub(crate) async fn listeners<T: Delivery>(&self, name: &str) -> usize {
        let mut registry = self.registry.lock().await;
        T::entries(&mut registry)
            .get(name)
            .map_or(0, |entry| entry.holders.load(Ordering::Acquire))
    }

    pub(crate) async fn close<T: Delivery>(&self, name: &str) {
        let mut registry = self.registry.lock().await;
        if let Some(entry) = T::entries(&mut registry).remove(name) {
            entry.close();
            self.unlisten(name.to_string(), T::PATTERN).await;
        }
    }

    async fn release<T: Delivery>(&self, name: String) {
        let mut registry = self.registry.lock().await;
        let entries = T::entries(&mut registry);
        let unused = entries
            .get(&name)
            .is_some_and(|entry| entry.holders.load(Ordering::Acquire) == 0);
        if unused {
            entries.remove(&name);
            self.unlisten(name, T::PATTERN).await;
        }
    }

    /// Subscribes every channel and pattern again after the connection was lost, on the node that serves `PUBSUB_KEY` now, which moves after a failover.
    async fn restore(&self) -> Result<()> {
        let registry = self.registry.lock().await;
        let channels: Vec<String> = registry.channels.keys().cloned().collect();
        let patterns: Vec<String> = registry.patterns.keys().cloned().collect();
        let outcome = async {
            let node = open(&self.topology, &self.push).await?;
            node.send(&redis::cmd("PING")).await.map_err(Error::from)?;
            *self.node.lock().await = node;
            if !channels.is_empty() {
                self.listen(channels, false).await?;
            }
            if !patterns.is_empty() {
                self.listen(patterns, true).await?;
            }
            self.node()
                .await
                .send(&redis::cmd("PING"))
                .await
                .map_err(Error::from)?;
            Ok(())
        }
        .await;
        registry.wake_all();
        outcome
    }
}

/// A pub/sub `Node` to the master that serves `PUBSUB_KEY`, with the client's settings, RESP3 and the push sender.
async fn open(topology: &Topology, push: &mpsc::UnboundedSender<PushInfo>) -> Result<Arc<Node>> {
    let pool = topology
        .pool_for(Some(PUBSUB_KEY))
        .await
        .map_err(Error::from)?;
    let mut settings = NodeSettings::clone(topology.settings());
    settings.resp3 = true;
    settings.push = Some(push.clone());
    Ok(Arc::new(Node::new(
        pool.address().clone(),
        Arc::new(settings),
    )))
}

/// Subscribes everything again until it succeeds, then tells the listeners, which re-check what they wait for.
async fn resubscribe(pubsub: Weak<PubSub>) {
    loop {
        let Some(strong) = pubsub.upgrade() else {
            return;
        };
        if strong.restore().await.is_ok() {
            let _ = strong.reconnects.send(());
            return;
        }
        drop(strong);
        tokio::time::sleep(RESUBSCRIBE_PAUSE).await;
    }
}

struct Burst {
    channel: String,
    payload: Bytes,
    remaining: Vec<Arc<Entry<PatternMessage>>>,
}

async fn dispatch(
    pubsub: Weak<PubSub>,
    mut pushes: mpsc::UnboundedReceiver<PushInfo>,
    mut activations: mpsc::UnboundedReceiver<Activation>,
) {
    let mut burst: Option<Burst> = None;
    loop {
        tokio::select! {
            push = pushes.recv() => {
                let Some(push) = push else {
                    return;
                };
                if !handle(&pubsub, &mut burst, push).await {
                    return;
                }
            }
            activation = activations.recv() => {
                let Some(activation) = activation else {
                    return;
                };
                // Messages that arrived before the subscriber took its receiver are delivered first.
                while let Ok(push) = pushes.try_recv() {
                    if !handle(&pubsub, &mut burst, push).await {
                        return;
                    }
                }
                activation.active.store(true, Ordering::Release);
                let _ = activation.done.send(());
            }
        }
    }
}

fn text_of(value: &Value) -> Option<String> {
    match value {
        Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
        Value::SimpleString(text) => Some(text.clone()),
        _ => None,
    }
}

fn bytes_of(value: &Value) -> Option<Bytes> {
    match value {
        Value::BulkString(bytes) => Some(Bytes::copy_from_slice(bytes)),
        Value::SimpleString(text) => Some(Bytes::copy_from_slice(text.as_bytes())),
        _ => None,
    }
}

async fn handle(pubsub: &Weak<PubSub>, burst: &mut Option<Burst>, push: PushInfo) -> bool {
    let Some(strong) = pubsub.upgrade() else {
        return false;
    };
    match push.kind {
        PushKind::Message => {
            let registry = strong.registry.lock().await;
            if let (Some(channel), Some(payload)) = (
                push.data.first().and_then(text_of),
                push.data.get(1).and_then(bytes_of),
            ) {
                if let Some(entry) = registry.channels.get(&channel) {
                    entry.deliver(payload);
                }
            }
        }
        PushKind::PMessage => {
            let registry = strong.registry.lock().await;
            if let (Some(channel), Some(payload)) = (
                push.data.get(1).and_then(text_of),
                push.data.get(2).and_then(bytes_of),
            ) {
                if let Some(entry) = next_pattern(&registry, burst, &channel, &payload) {
                    entry.deliver((channel, payload));
                }
            }
        }
        PushKind::Disconnection => {
            *burst = None;
            tokio::spawn(resubscribe(pubsub.clone()));
        }
        _ => {}
    }
    true
}

fn next_pattern(
    registry: &Registry,
    burst: &mut Option<Burst>,
    channel: &str,
    payload: &Bytes,
) -> Option<Arc<Entry<PatternMessage>>> {
    if let Some(current) = burst.as_mut() {
        if current.channel == channel && current.payload == *payload {
            if let Some(entry) = current.remaining.pop() {
                return Some(entry);
            }
        }
    }
    let mut matching: Vec<Arc<Entry<PatternMessage>>> = registry
        .patterns
        .iter()
        .filter(|(pattern, _)| glob_match(pattern.as_bytes(), channel.as_bytes()))
        .map(|(_, entry)| entry.clone())
        .collect();
    let first = matching.pop();
    *burst = Some(Burst {
        channel: channel.to_string(),
        payload: payload.clone(),
        remaining: matching,
    });
    first
}

pub(crate) fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    match pattern.split_first() {
        None => text.is_empty(),
        Some((b'*', rest)) => {
            let rest = &rest[rest.iter().take_while(|&&byte| byte == b'*').count()..];
            rest.is_empty() || (0..=text.len()).any(|skip| glob_match(rest, &text[skip..]))
        }
        Some((b'?', rest)) => !text.is_empty() && glob_match(rest, &text[1..]),
        Some((b'[', rest)) => {
            let Some((&first, text_rest)) = text.split_first() else {
                return false;
            };
            let (matched, after) = class_match(rest, first);
            matched && glob_match(after, text_rest)
        }
        Some((b'\\', rest)) if !rest.is_empty() => {
            text.first() == Some(&rest[0]) && glob_match(&rest[1..], &text[1..])
        }
        Some((&literal, rest)) => text.first() == Some(&literal) && glob_match(rest, &text[1..]),
    }
}

fn class_match(class: &[u8], byte: u8) -> (bool, &[u8]) {
    let (negated, mut class) = match class.split_first() {
        Some((b'^', rest)) => (true, rest),
        _ => (false, class),
    };
    let mut matched = false;
    loop {
        match class {
            [] => break,
            [b']', rest @ ..] => {
                class = rest;
                break;
            }
            [b'\\', escaped, rest @ ..] => {
                matched |= *escaped == byte;
                class = rest;
            }
            [low, b'-', high, rest @ ..] if *high != b']' => {
                let (low, high) = if low <= high {
                    (*low, *high)
                } else {
                    (*high, *low)
                };
                matched |= (low..=high).contains(&byte);
                class = rest;
            }
            [single, rest @ ..] => {
                matched |= *single == byte;
                class = rest;
            }
        }
    }
    (matched != negated, class)
}

#[cfg(test)]
mod tests {
    use super::glob_match;

    #[test]
    fn glob_follows_redis_rules() {
        let cases: &[(&str, &str, bool)] = &[
            ("news.*", "news.art", true),
            ("news.*", "news", false),
            ("h?llo", "hello", true),
            ("h?llo", "hllo", false),
            ("h[ae]llo", "hallo", true),
            ("h[ae]llo", "hillo", false),
            ("h[^e]llo", "hallo", true),
            ("h[^e]llo", "hello", false),
            ("h[a-b]llo", "hbllo", true),
            ("h[a-b]llo", "hcllo", false),
            ("a\\*b", "a*b", true),
            ("a\\*b", "axb", false),
            ("*", "", true),
            ("test*", "test1", true),
        ];
        for (pattern, text, expected) in cases {
            assert_eq!(
                glob_match(pattern.as_bytes(), text.as_bytes()),
                *expected,
                "{pattern} vs {text}"
            );
        }
    }
}
