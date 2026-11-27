//! Write-behind: Redisson's `MapWriteBehindTask`, with redissun's own queue.
//!
//! A write pushes one encoded [`Task`] to a Redis list in the map's slot. A background task pops the list every `delay`, keeps the latest operation of each key ([`Pending`]) and hands the writer batches of `batch_size`. The queue holds redissun's own format, not Redisson's Java-serialized tasks.

use crate::client::core::Core;
use crate::codec::Codec;
use crate::collections::map::store::{retry, ErasedWriter};
use crate::error::{Error, Result};
use crate::object::{suffix_name, Key};
use bytes::{BufMut, Bytes, BytesMut};
use serde::de::DeserializeOwned;
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::oneshot;

/// The format version, the first byte of every payload.
const VERSION: u8 = 1;

/// The queue of the map `name`: `{name}:redissun_write-behind-queue`, in the map's slot.
pub(crate) fn queue_name(name: &str) -> String {
    suffix_name(name, "redissun_write-behind-queue")
}

/// The write-behind of each queue a client drains, by queue name, as Redisson's `WriteBehindService` keeps one task per map name: every build of one name in one client shares one drainer and one background task, so a key's changes reach the writer in the order they were queued.
///
/// Each entry holds the shared [`WriteBehind`] part (`Weak<Shared<K, V, C>>`, erased) and the drain lock. A build whose key, value or codec types differ from the live one gets its own drainer, which still takes the same drain lock.
pub(crate) type Registry = std::sync::Mutex<HashMap<String, Registered>>;

/// One entry of the [`Registry`].
pub(crate) struct Registered {
    shared: Weak<dyn Any + Send + Sync>,
    lock: Weak<tokio::sync::Mutex<()>>,
}

/// One change of the map, with the key and value as the map's codec encoded them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Task {
    Write(Vec<(Bytes, Bytes)>),
    Delete(Vec<Bytes>),
}

fn put_part(out: &mut BytesMut, part: &[u8]) {
    // A part longer than u32::MAX cannot be a Redis value (512 MB at most).
    out.put_u32(u32::try_from(part.len()).unwrap_or(u32::MAX));
    out.put_slice(part);
}

fn count_of(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}

/// The payload of `task`: the version, `W` or `D`, a `u32` BE count, then each key as a `u32` BE length plus the bytes, followed for `W` by the value in the same form.
pub(crate) fn encode(task: &Task) -> Bytes {
    let mut out = BytesMut::new();
    out.put_u8(VERSION);
    match task {
        Task::Write(entries) => {
            out.put_u8(b'W');
            out.put_u32(count_of(entries.len()));
            for (k, v) in entries {
                put_part(&mut out, k);
                put_part(&mut out, v);
            }
        }
        Task::Delete(keys) => {
            out.put_u8(b'D');
            out.put_u32(count_of(keys.len()));
            for k in keys {
                put_part(&mut out, k);
            }
        }
    }
    out.freeze()
}

/// Reads payloads without ever panicking.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        if self.0.len() < n {
            return None;
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Some(head)
    }

    fn u8(&mut self) -> Option<u8> {
        self.take(1).and_then(|b| b.first().copied())
    }

    fn u32(&mut self) -> Option<usize> {
        let bytes: [u8; 4] = self.take(4)?.try_into().ok()?;
        usize::try_from(u32::from_be_bytes(bytes)).ok()
    }

    fn part(&mut self) -> Option<Bytes> {
        let len = self.u32()?;
        self.take(len).map(Bytes::copy_from_slice)
    }
}

/// The task in `payload`; `None` for another version, an unknown kind, truncation or trailing bytes.
pub(crate) fn decode(payload: &[u8]) -> Option<Task> {
    let mut reader = Reader(payload);
    if reader.u8()? != VERSION {
        return None;
    }
    let kind = reader.u8()?;
    let count = reader.u32()?;
    // Every part takes at least its 4-byte length, so a larger count is truncated; this also bounds the allocation.
    let capacity = count.min(reader.0.len() / 4);
    let task = match kind {
        b'W' => {
            let mut entries = Vec::with_capacity(capacity);
            for _ in 0..count {
                entries.push((reader.part()?, reader.part()?));
            }
            Task::Write(entries)
        }
        b'D' => {
            let mut keys = Vec::with_capacity(capacity);
            for _ in 0..count {
                keys.push(reader.part()?);
            }
            Task::Delete(keys)
        }
        _ => return None,
    };
    reader.0.is_empty().then_some(task)
}

/// The operations popped from the queue and not yet sent, one per encoded key, the latest.
pub(crate) struct Pending {
    batch_size: usize,
    /// The pending writes in the order of their first write; a slot is `None` once the key was deleted after it.
    writes: Vec<Option<(Bytes, Bytes)>>,
    write_at: HashMap<Bytes, usize>,
    deletes: Vec<Option<Bytes>>,
    delete_at: HashMap<Bytes, usize>,
}

impl Pending {
    pub(crate) fn new(batch_size: usize) -> Self {
        Self {
            batch_size: batch_size.max(1),
            writes: Vec::new(),
            write_at: HashMap::new(),
            deletes: Vec::new(),
            delete_at: HashMap::new(),
        }
    }

    /// Adds the task's operations and returns the batches that just reached `batch_size`.
    pub(crate) fn push(&mut self, task: Task) -> std::vec::Vec<Task> {
        let mut full = Vec::new();
        match task {
            Task::Write(entries) => {
                for (k, v) in entries {
                    if let Some(at) = self.delete_at.remove(&k) {
                        if let Some(slot) = self.deletes.get_mut(at) {
                            *slot = None;
                        }
                    }
                    match self.write_at.get(&k) {
                        Some(&at) => {
                            if let Some(slot) = self.writes.get_mut(at) {
                                *slot = Some((k, v));
                            }
                        }
                        None => {
                            self.write_at.insert(k.clone(), self.writes.len());
                            self.writes.push(Some((k, v)));
                        }
                    }
                    if self.write_at.len() >= self.batch_size {
                        full.extend(self.take_writes());
                    }
                }
            }
            Task::Delete(keys) => {
                for k in keys {
                    if let Some(at) = self.write_at.remove(&k) {
                        if let Some(slot) = self.writes.get_mut(at) {
                            *slot = None;
                        }
                    }
                    if !self.delete_at.contains_key(&k) {
                        self.delete_at.insert(k.clone(), self.deletes.len());
                        self.deletes.push(Some(k));
                    }
                    if self.delete_at.len() >= self.batch_size {
                        full.extend(self.take_deletes());
                    }
                }
            }
        }
        full
    }

    /// The rest: the deletes, then the writes, empty ones left out.
    pub(crate) fn finish(&mut self) -> std::vec::Vec<Task> {
        self.take_deletes()
            .into_iter()
            .chain(self.take_writes())
            .collect()
    }

    fn take_writes(&mut self) -> Option<Task> {
        self.write_at.clear();
        let entries: Vec<(Bytes, Bytes)> = self.writes.drain(..).flatten().collect();
        (!entries.is_empty()).then_some(Task::Write(entries))
    }

    fn take_deletes(&mut self) -> Option<Task> {
        self.delete_at.clear();
        let keys: Vec<Bytes> = self.deletes.drain(..).flatten().collect();
        (!keys.is_empty()).then_some(Task::Delete(keys))
    }
}

/// What the map's handles and its background task share to drain the queue: the writer, the codec and the drain lock.
pub(crate) struct Drainer<K, V, C: Codec> {
    /// The map's name, for the logs.
    name: Arc<str>,
    batch_size: usize,
    writer: Arc<dyn ErasedWriter<K, V>>,
    codec: C,
    attempts: usize,
    interval: Duration,
    /// One drain at a time, so that a key's operations reach the writer in order; shared by every drainer of the queue in this client.
    lock: Arc<tokio::sync::Mutex<()>>,
}

impl<K, V, C> Drainer<K, V, C>
where
    K: DeserializeOwned + Send + Sync,
    V: DeserializeOwned + Send + Sync,
    C: Codec,
{
    /// Pops the queue until it is empty and hands the writer what it held. A failed batch is logged and dropped, as Redisson does; the first error is returned.
    pub(crate) async fn drain(&self, queue: &Key) -> Result<()> {
        let _drain = self.lock.lock().await;
        let mut pending = Pending::new(self.batch_size);
        let mut first: Option<Error> = None;
        let mut popped = 0usize;
        loop {
            // At most once: a pop whose reply was lost took a task; popping again would skip it without a word.
            let payload: Option<Vec<u8>> = match queue.query_once(queue.key_cmd("LPOP")).await {
                Ok(payload) => payload,
                Err(e) => {
                    tracing::error!(map = &*self.name, error = %e, "write-behind queue poll failed");
                    first.get_or_insert(e);
                    break;
                }
            };
            let Some(payload) = payload else {
                break;
            };
            popped += 1;
            let Some(task) = decode(&payload) else {
                tracing::error!(
                    map = &*self.name,
                    bytes = payload.len(),
                    "skipped a write-behind queue entry that is not redissun's"
                );
                continue;
            };
            for batch in pending.push(task) {
                if let Err(e) = self.send(batch).await {
                    first.get_or_insert(e);
                }
            }
        }
        for batch in pending.finish() {
            if let Err(e) = self.send(batch).await {
                first.get_or_insert(e);
            }
        }
        if popped > 0 {
            tracing::debug!(
                map = &*self.name,
                tasks = popped,
                "write-behind queue drained"
            );
        }
        first.map_or(Ok(()), Err)
    }

    /// Decodes one batch and hands it to the writer with retries; entries the codec cannot read are logged and left out.
    async fn send(&self, batch: Task) -> Result<()> {
        let mut first: Option<Error> = None;
        match batch {
            Task::Write(entries) => {
                let decoded: Vec<(K, V)> = entries
                    .iter()
                    .filter_map(|(k, v)| {
                        let entry = self
                            .codec
                            .decode::<K>(k)
                            .and_then(|k| Ok((k, self.codec.decode::<V>(v)?)));
                        self.keep(entry, &mut first)
                    })
                    .collect();
                if !decoded.is_empty() {
                    if let Err(e) = retry(self.attempts, self.interval, &self.name, || {
                        self.writer.write(&decoded)
                    })
                    .await
                    {
                        tracing::error!(
                            map = &*self.name,
                            entries = decoded.len(),
                            "map writer failed, write-behind batch dropped"
                        );
                        first.get_or_insert(Error::Writer(e));
                    }
                }
            }
            Task::Delete(keys) => {
                let decoded: Vec<K> = keys
                    .iter()
                    .filter_map(|k| self.keep(self.codec.decode::<K>(k), &mut first))
                    .collect();
                if !decoded.is_empty() {
                    if let Err(e) = retry(self.attempts, self.interval, &self.name, || {
                        self.writer.delete(&decoded)
                    })
                    .await
                    {
                        tracing::error!(
                            map = &*self.name,
                            keys = decoded.len(),
                            "map writer failed, write-behind batch dropped"
                        );
                        first.get_or_insert(Error::Writer(e));
                    }
                }
            }
        }
        first.map_or(Ok(()), Err)
    }

    fn keep<T>(&self, decoded: Result<T>, first: &mut Option<Error>) -> Option<T> {
        match decoded {
            Ok(value) => Some(value),
            Err(e) => {
                tracing::error!(map = &*self.name, error = %e, "skipped a write-behind entry the codec cannot read");
                first.get_or_insert(e);
                None
            }
        }
    }
}

/// Lives as long as the map's store. Dropping it hands the queue, and with it the connection, to the background task for its last drain.
struct Liveness {
    queue: Key,
    last: Option<oneshot::Sender<Key>>,
}

impl Drop for Liveness {
    fn drop(&mut self) {
        if let Some(last) = self.last.take() {
            // The task is gone only when the runtime shut down; then nothing drains.
            let _ = last.send(self.queue.clone());
        }
    }
}

/// What every build of one map name in one client shares: the drainer and the liveness of its background task.
struct Shared<K, V, C: Codec> {
    drainer: Arc<Drainer<K, V, C>>,
    alive: Liveness,
}

/// The write-behind part of a map's store. Builds of one name in one client share it through the core's [`Registry`]; the background task drains once more and stops when the last of them is dropped.
pub(crate) struct WriteBehind<K, V, C: Codec> {
    shared: Arc<Shared<K, V, C>>,
}

/// The settings of [`WriteBehind::start`].
pub(crate) struct Settings<K, V> {
    pub(crate) batch_size: usize,
    pub(crate) delay: Duration,
    pub(crate) writer: Arc<dyn ErasedWriter<K, V>>,
    pub(crate) attempts: usize,
    pub(crate) interval: Duration,
}

impl<K, V, C> WriteBehind<K, V, C>
where
    K: DeserializeOwned + Send + Sync + 'static,
    V: DeserializeOwned + Send + Sync + 'static,
    C: Codec,
{
    /// Joins the write-behind of the map `map` that this client runs already, or starts its background task; that needs a running tokio runtime.
    ///
    /// A build that joins keeps the first build's writer, batch size, delay and retries, as Redisson's `WriteBehindService.getOrCreateTask` does.
    pub(crate) fn start(map: &Key, codec: C, settings: Settings<K, V>) -> Result<Self> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| Error::Config("write-behind needs a running tokio runtime".into()))?;
        let queue_name: Arc<str> = queue_name(map.name()).into();
        let mut registry = map
            .core
            .write_behind
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let live = registry.get(&*queue_name);
        let lock = live.and_then(|entry| entry.lock.upgrade());
        if let Some(shared) = live.and_then(|entry| entry.shared.upgrade()) {
            if let Ok(shared) = shared.downcast::<Shared<K, V, C>>() {
                return Ok(Self { shared });
            }
            tracing::warn!(
                map = map.name(),
                "another write-behind build of this name has other types; it gets its own drainer, which shares the drain lock"
            );
        }
        let lock = lock.unwrap_or_default();
        let queue = Key::new(map.core.clone(), queue_name.clone());
        let drainer = Arc::new(Drainer {
            name: map.name.clone(),
            batch_size: settings.batch_size,
            writer: settings.writer,
            codec,
            attempts: settings.attempts,
            interval: settings.interval,
            lock: lock.clone(),
        });
        let (last, stopped) = oneshot::channel();
        runtime.spawn(run(
            Arc::downgrade(&map.core),
            queue_name.clone(),
            drainer.clone(),
            settings.delay,
            stopped,
        ));
        let shared = Arc::new(Shared {
            drainer,
            alive: Liveness {
                queue,
                last: Some(last),
            },
        });
        let erased: Arc<dyn Any + Send + Sync> = shared.clone();
        registry
            .retain(|_, entry| entry.shared.strong_count() > 0 || entry.lock.strong_count() > 0);
        let replace = registry
            .get(&*queue_name)
            .is_none_or(|entry| entry.shared.strong_count() == 0);
        if replace {
            registry.insert(
                queue_name.to_string(),
                Registered {
                    shared: Arc::downgrade(&erased),
                    lock: Arc::downgrade(&lock),
                },
            );
        }
        Ok(Self { shared })
    }
}

impl<K, V, C> WriteBehind<K, V, C>
where
    K: DeserializeOwned + Send + Sync,
    V: DeserializeOwned + Send + Sync,
    C: Codec,
{
    /// Queues one task; an error of `RPUSH` is the caller's. It is sent at most once, so a lost reply does not queue the task twice.
    pub(crate) async fn push(&self, task: &Task) -> Result<()> {
        let queue = &self.shared.alive.queue;
        let mut cmd = queue.key_cmd("RPUSH");
        cmd.arg(encode(task).as_ref());
        let _: i64 = queue.query_once(cmd).await?;
        Ok(())
    }

    /// Drains the queue now.
    pub(crate) async fn drain(&self) -> Result<()> {
        self.shared.drainer.drain(&self.shared.alive.queue).await
    }
}

/// The background task: drains every `delay` while the client lives, and once more when the map's last handle is gone.
async fn run<K, V, C>(
    core: Weak<Core>,
    queue: Arc<str>,
    drainer: Arc<Drainer<K, V, C>>,
    delay: Duration,
    mut stopped: oneshot::Receiver<Key>,
) where
    K: DeserializeOwned + Send + Sync,
    V: DeserializeOwned + Send + Sync,
    C: Codec,
{
    loop {
        tokio::select! {
            last = &mut stopped => {
                if let Ok(queue) = last {
                    // Errors are logged inside; nobody is left to report them to.
                    let _ = drainer.drain(&queue).await;
                }
                return;
            }
            () = tokio::time::sleep(delay) => {
                let Some(core) = core.upgrade() else {
                    return;
                };
                let _ = drainer.drain(&Key::new(core, queue.clone())).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(text: &str) -> Bytes {
        Bytes::copy_from_slice(text.as_bytes())
    }

    fn write(entries: &[(&str, &str)]) -> Task {
        Task::Write(entries.iter().map(|(k, v)| (b(k), b(v))).collect())
    }

    fn delete(keys: &[&str]) -> Task {
        Task::Delete(keys.iter().map(|k| b(k)).collect())
    }

    #[test]
    fn task_payload_round_trips() {
        for task in [write(&[("a", ""), ("b", "2")]), delete(&["", "k"])] {
            assert_eq!(decode(&encode(&task)), Some(task));
        }
    }

    #[test]
    fn decode_rejects_truncation_and_foreign_bytes() {
        assert_eq!(decode(&[]), None);
        assert_eq!(decode(&[0xac, 0xed, 0x00, 0x05]), None);
        let good = encode(&write(&[("a", "1")])).to_vec();
        let mut version = good.clone();
        version[0] = 2;
        assert_eq!(decode(&version), None);
        let mut kind = good.clone();
        kind[1] = b'X';
        assert_eq!(decode(&kind), None);
        let mut count = good.clone();
        count[2..6].copy_from_slice(&1000u32.to_be_bytes());
        assert_eq!(decode(&count), None);
        let mut trailing = good.clone();
        trailing.push(0);
        assert_eq!(decode(&trailing), None);
        for end in 0..good.len() {
            assert_eq!(decode(&good[..end]), None, "truncated at {end}");
        }
    }

    #[test]
    fn pending_keeps_the_last_operation_per_key() {
        let mut pending = Pending::new(50);
        assert!(pending.push(write(&[("a", "1")])).is_empty());
        assert!(pending.push(delete(&["a"])).is_empty());
        assert!(pending.push(write(&[("b", "2")])).is_empty());
        assert!(pending.push(write(&[("a", "3")])).is_empty());
        assert_eq!(pending.finish(), vec![write(&[("b", "2"), ("a", "3")])]);
        assert!(pending.finish().is_empty());
    }

    #[test]
    fn pending_write_then_delete_ends_deleted() {
        let mut pending = Pending::new(50);
        assert!(pending.push(write(&[("a", "1")])).is_empty());
        assert!(pending.push(delete(&["a"])).is_empty());
        assert_eq!(pending.finish(), vec![delete(&["a"])]);
    }

    #[test]
    fn pending_flushes_full_batches() {
        let mut pending = Pending::new(2);
        let mut batches = Vec::new();
        for i in 0..5 {
            batches.extend(pending.push(write(&[(&i.to_string(), "v")])));
        }
        assert_eq!(
            batches,
            vec![
                write(&[("0", "v"), ("1", "v")]),
                write(&[("2", "v"), ("3", "v")])
            ]
        );
        assert_eq!(pending.finish(), vec![write(&[("4", "v")])]);
    }

    #[test]
    fn pending_splits_one_large_task_and_puts_deletes_first() {
        let mut pending = Pending::new(2);
        let batches = pending.push(write(&[("a", "1"), ("b", "2"), ("c", "3")]));
        assert_eq!(batches, vec![write(&[("a", "1"), ("b", "2")])]);
        assert!(pending.push(delete(&["x"])).is_empty());
        assert_eq!(pending.finish(), vec![delete(&["x"]), write(&[("c", "3")])]);
    }

    #[test]
    fn queue_name_follows_suffix_name() {
        assert_eq!(queue_name("m"), "{m}:redissun_write-behind-queue");
        assert_eq!(queue_name("{t}:m"), "{t}:m:redissun_write-behind-queue");
    }
}
