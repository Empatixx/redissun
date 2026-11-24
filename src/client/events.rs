//! Connection events, Redisson's `ConnectionListener` and `ConnectionEventsHub`.

use crate::net::stream::Address;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// The role of a node a connection goes to, Redisson's `NodeType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum NodeType {
    /// A master, or the single server.
    Master,
    /// A replica that takes reads (see [`ReadMode`](crate::ReadMode)).
    Replica,
    /// A Sentinel that names the master and its replicas.
    Sentinel,
}

/// Told when a node gets connected and when it is no longer, like Redisson's `ConnectionListener`.
///
/// A node counts as connected from its first open connection until it has none left: a pool of several connections to one node is one `on_connect`, and one `on_disconnect` once all of them are gone. A dropped client reports nothing more. Both methods run on the task that opened or lost the connection, so they should return quickly.
///
/// ```no_run
/// use redissun::{Client, ConnectionListener, NodeType};
///
/// struct Report;
///
/// impl ConnectionListener for Report {
///     fn on_connect(&self, address: &str, node: NodeType) {
///         println!("{node:?} {address} connected");
///     }
///
///     fn on_disconnect(&self, address: &str, node: NodeType) {
///         println!("{node:?} {address} disconnected");
///     }
/// }
///
/// # async fn run() -> redissun::Result<()> {
/// let client = Client::builder()
///     .url("redis://127.0.0.1:6379")
///     .connection_listener(Report)
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub trait ConnectionListener: Send + Sync + 'static {
    /// `address` (`host:port`) got its first connection, or one again after it had none.
    fn on_connect(&self, address: &str, node: NodeType) {
        let _ = (address, node);
    }

    /// `address` lost its last connection.
    fn on_disconnect(&self, address: &str, node: NodeType) {
        let _ = (address, node);
    }
}

/// Counts the open connections of each address and tells the listener when a count leaves or reaches zero.
pub(crate) struct Events {
    listener: Arc<dyn ConnectionListener>,
    open: Mutex<HashMap<Address, usize>>,
    closed: AtomicBool,
}

impl Events {
    pub(crate) fn new(listener: Arc<dyn ConnectionListener>) -> Self {
        Self {
            listener,
            open: Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
        }
    }

    /// A connection to `address` opened.
    pub(crate) fn opened(&self, address: &Address, node: NodeType) {
        let first = {
            let mut open = self.open.lock().unwrap_or_else(|e| e.into_inner());
            let count = open.entry(address.clone()).or_default();
            *count += 1;
            *count == 1
        };
        if first && !self.closed.load(Ordering::Acquire) {
            self.listener.on_connect(&address.to_string(), node);
        }
    }

    /// A connection to `address` that [`opened`](Events::opened) ended.
    pub(crate) fn ended(&self, address: &Address, node: NodeType) {
        let last = {
            let mut open = self.open.lock().unwrap_or_else(|e| e.into_inner());
            match open.get_mut(address) {
                Some(count) if *count > 1 => {
                    *count -= 1;
                    false
                }
                Some(_) => {
                    open.remove(address);
                    true
                }
                None => false,
            }
        };
        if last && !self.closed.load(Ordering::Acquire) {
            self.listener.on_disconnect(&address.to_string(), node);
        }
    }

    /// The client is going away: nothing is reported after this, as Redisson reports nothing while it shuts down.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }
}
