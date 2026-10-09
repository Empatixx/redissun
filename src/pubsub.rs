use crate::error::Result;
use bytes::Bytes;
use fred::clients::Client as RedisClient;
use fred::interfaces::{ClientLike, EventInterface, PubsubInterface};
use fred::types::config::Server;
use fred::types::{Message, MessageKind};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::broadcast::error::{RecvError, TryRecvError};
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

    fn missed(&self, count: usize) {
        self.notify.notify_waiters();
        let _ = self.lost.send(count);
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
    client: RedisClient,
    registry: Mutex<Registry>,
    reconnects: broadcast::Sender<()>,
    activations: mpsc::UnboundedSender<Activation>,
}

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

async fn listen(client: &RedisClient, names: Vec<String>, pattern: bool) -> Result<()> {
    if pattern {
        client.psubscribe(names).await?;
    } else {
        client.subscribe(names).await?;
    }
    Ok(())
}

async fn unlisten(client: &RedisClient, name: String, pattern: bool) {
    let _ = if pattern {
        client.punsubscribe(name).await
    } else {
        client.unsubscribe(name).await
    };
}

impl PubSub {
    pub(crate) async fn start(base: &RedisClient) -> Result<Arc<Self>> {
        let client = base.clone_new();
        client.init().await?;
        let receiver = client.message_rx();
        let reconnects = broadcast::channel(16).0;
        let reconnected = client.reconnect_rx();
        let (activations, requests) = mpsc::unbounded_channel();
        let pubsub = Arc::new(Self {
            client,
            registry: Mutex::new(Registry::default()),
            reconnects,
            activations,
        });
        tokio::spawn(resubscribe(Arc::downgrade(&pubsub), reconnected));
        tokio::spawn(dispatch(Arc::downgrade(&pubsub), receiver, requests));
        Ok(pubsub)
    }

    pub(crate) fn reconnects(&self) -> broadcast::Receiver<()> {
        self.reconnects.subscribe()
    }

    pub(crate) async fn quit(&self) {
        let _ = self.client.quit().await;
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
        let (subscription, receiver) = {
            let mut registry = self.registry.lock().await;
            let entries = T::entries(&mut registry);
            let entry = match entries.get(name) {
                Some(entry) => {
                    entry.holders.fetch_add(1, Ordering::AcqRel);
                    entry.clone()
                }
                None => {
                    listen(&self.client, vec![name.to_string()], T::PATTERN).await?;
                    self.client.ping::<()>(None).await?;
                    let entry = Entry::new();
                    entries.insert(name.to_string(), entry.clone());
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
        self.activate(&subscription.entry.active).await;
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
            unlisten(&self.client, name.to_string(), T::PATTERN).await;
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
            unlisten(&self.client, name, T::PATTERN).await;
        }
    }

    async fn restore(&self) -> Result<()> {
        let registry = self.registry.lock().await;
        let channels: Vec<String> = registry.channels.keys().cloned().collect();
        let patterns: Vec<String> = registry.patterns.keys().cloned().collect();
        let outcome = async {
            if !channels.is_empty() {
                listen(&self.client, channels, false).await?;
            }
            if !patterns.is_empty() {
                listen(&self.client, patterns, true).await?;
            }
            self.client.ping::<()>(None).await?;
            Ok(())
        }
        .await;
        registry.wake_all();
        outcome
    }
}

async fn resubscribe(pubsub: Weak<PubSub>, mut reconnected: broadcast::Receiver<Server>) {
    while !matches!(reconnected.recv().await, Err(RecvError::Closed)) {
        loop {
            let Some(pubsub) = pubsub.upgrade() else {
                return;
            };
            if pubsub.restore().await.is_ok() {
                let _ = pubsub.reconnects.send(());
                break;
            }
            drop(pubsub);
            tokio::time::sleep(RESUBSCRIBE_PAUSE).await;
        }
    }
}

struct Burst {
    channel: String,
    payload: Bytes,
    remaining: Vec<Arc<Entry<PatternMessage>>>,
}

async fn dispatch(
    pubsub: Weak<PubSub>,
    mut receiver: broadcast::Receiver<Message>,
    mut activations: mpsc::UnboundedReceiver<Activation>,
) {
    let mut burst: Option<Burst> = None;
    loop {
        tokio::select! {
            message = receiver.recv() => {
                if !handle(&pubsub, &mut burst, message).await {
                    return;
                }
            }
            activation = activations.recv() => {
                let Some(activation) = activation else {
                    return;
                };
                loop {
                    let message = match receiver.try_recv() {
                        Ok(message) => Ok(message),
                        Err(TryRecvError::Lagged(missed)) => Err(RecvError::Lagged(missed)),
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Closed) => Err(RecvError::Closed),
                    };
                    if !handle(&pubsub, &mut burst, message).await {
                        return;
                    }
                }
                activation.active.store(true, Ordering::Release);
                let _ = activation.done.send(());
            }
        }
    }
}

async fn handle(
    pubsub: &Weak<PubSub>,
    burst: &mut Option<Burst>,
    message: std::result::Result<Message, RecvError>,
) -> bool {
    let Some(pubsub) = pubsub.upgrade() else {
        return false;
    };
    let registry = pubsub.registry.lock().await;
    match message {
        Ok(message) => {
            let Ok(payload) = message.value.convert::<Bytes>() else {
                return true;
            };
            if message.kind == MessageKind::PMessage {
                let channel = message.channel.to_string();
                if let Some(entry) = next_pattern(&registry, burst, &channel, &payload) {
                    entry.deliver((channel, payload));
                }
            } else if let Some(entry) = registry.channels.get(&*message.channel) {
                entry.deliver(payload);
            }
            true
        }
        Err(RecvError::Lagged(missed)) => {
            let missed = usize::try_from(missed).unwrap_or(usize::MAX);
            *burst = None;
            registry
                .channels
                .values()
                .for_each(|entry| entry.missed(missed));
            registry
                .patterns
                .values()
                .for_each(|entry| entry.missed(missed));
            let _ = pubsub.reconnects.send(());
            true
        }
        Err(RecvError::Closed) => false,
    }
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
