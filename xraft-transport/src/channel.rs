use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, Mutex};

use xraft_core::error::XraftError;
use xraft_core::rpc::RpcEnvelope;
use xraft_core::traits::{TransportReceiver, TransportSender};
use xraft_core::types::NodeId;

/// In-process transport using `tokio::sync::mpsc` channels per node.
///
/// Each node has a single inbound queue. Senders route messages into
/// the target node's inbox. Provides `split()` for sender/receiver
/// separation as required by the architecture.
pub struct ChannelTransport {
    inboxes: Arc<HashMap<NodeId, mpsc::Sender<RpcEnvelope>>>,
    receivers: HashMap<NodeId, mpsc::Receiver<RpcEnvelope>>,
}

impl ChannelTransport {
    /// Create a new channel transport mesh for the given set of node IDs.
    /// Each node gets an inbound mpsc channel with the specified buffer capacity.
    pub fn new(node_ids: &[NodeId], buffer_size: usize) -> Self {
        let mut senders = HashMap::new();
        let mut receivers = HashMap::new();

        for &node_id in node_ids {
            let (tx, rx) = mpsc::channel(buffer_size);
            senders.insert(node_id, tx);
            receivers.insert(node_id, rx);
        }

        Self {
            inboxes: Arc::new(senders),
            receivers,
        }
    }
}

    /// Split the transport into per-node sender/receiver pairs.
    ///
    /// Returns a map from NodeId to (sender, receiver). Each sender can
    /// deliver messages to any node in the cluster; each receiver only
    /// receives messages destined for that specific node.
    pub fn split(
        mut self,
    ) -> HashMap<NodeId, (Box<dyn TransportSender>, Box<dyn TransportReceiver>)> {
        let mut result = HashMap::new();
        let node_ids: Vec<NodeId> = self.receivers.keys().copied().collect();
        for node_id in node_ids {
            let rx = self
                .receivers
                .remove(&node_id)
                .expect("receiver must exist for node");
            let sender = ChannelSender {
                inboxes: Arc::clone(&self.inboxes),
            };
            let receiver = ChannelReceiver { rx };
            result.insert(
                node_id,
                (
                    Box::new(sender) as Box<dyn TransportSender>,
                    Box::new(receiver) as Box<dyn TransportReceiver>,
                ),
            );
        }
        result
    }

    /// Get a reference to the shared inbox senders (used by NetworkSimulator).
    pub fn inboxes(&self) -> Arc<HashMap<NodeId, mpsc::Sender<RpcEnvelope>>> {
        Arc::clone(&self.inboxes)
    }

    /// Take the receiver for a specific node (used by NetworkSimulator).
    pub fn take_receiver(&mut self, node_id: NodeId) -> Option<mpsc::Receiver<RpcEnvelope>> {
        self.receivers.remove(&node_id)
    }
}

/// Sender half: can deliver messages to any node's inbox.
#[derive(Clone)]
pub struct ChannelSender {
    inboxes: Arc<HashMap<NodeId, mpsc::Sender<RpcEnvelope>>>,
}

#[async_trait]
impl TransportSender for ChannelSender {
    async fn send(&self, target: NodeId, message: RpcEnvelope) -> Result<(), XraftError> {
        let tx = self.inboxes.get(&target).ok_or_else(|| {
            XraftError::TransportError(io::Error::new(
                io::ErrorKind::NotFound,
                format!("no channel for target node {target}"),
            ))
        })?;
        tx.send(message).await.map_err(|_| {
            XraftError::TransportError(io::Error::new(
                io::ErrorKind::ConnectionReset,
                format!("channel closed for target node {target}"),
            ))
        })
    }
}

/// Receiver half: receives messages from this node's inbound queue.
pub struct ChannelReceiver {
    rx: mpsc::Receiver<RpcEnvelope>,
}

#[async_trait]
impl TransportReceiver for ChannelReceiver {
    async fn recv(&mut self) -> Result<RpcEnvelope, XraftError> {
        self.rx.recv().await.ok_or_else(|| {
            XraftError::TransportError(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "all senders dropped",
            ))
        })
    }
}

/// Wraps a `ChannelReceiver` with thread-safe access for the simulator.
pub struct SharedChannelReceiver {
    pub(crate) inner: Mutex<mpsc::Receiver<RpcEnvelope>>,
}

impl SharedChannelReceiver {
    pub fn new(rx: mpsc::Receiver<RpcEnvelope>) -> Self {
        Self {
            inner: Mutex::new(rx),
        }
    }
}
