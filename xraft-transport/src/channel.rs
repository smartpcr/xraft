//! In-process channel-based transport for deterministic testing.
//!
//! `ChannelTransport` allocates one bounded `tokio::sync::mpsc` queue per
//! node. The transport is split into per-node sender/receiver halves that
//! implement the `TransportSender` / `TransportReceiver` traits defined
//! in `xraft_core::traits` (architecture §4.4 split-transport pattern).
//!
//! `NetworkSimulator` (see `simulator.rs`) wraps this transport to inject
//! faults (drops, delays, reordering, partitions).

use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc;

use xraft_core::rpc::RpcEnvelope;
use xraft_core::traits::{TransportReceiver, TransportSender};
use xraft_core::types::NodeId;

/// In-process transport using `tokio::sync::mpsc` channels per node.
///
/// Each node has a single bounded inbound queue. Senders route messages
/// into the target node's inbox. Use [`split`](Self::split) for the common
/// per-node sender/receiver pairs, or the lower-level [`inboxes`](Self::inboxes)
/// and [`take_receiver`](Self::take_receiver) accessors when wrapping the
/// transport (e.g. for the network simulator).
pub struct ChannelTransport {
    inboxes: Arc<HashMap<NodeId, mpsc::Sender<RpcEnvelope>>>,
    receivers: HashMap<NodeId, mpsc::Receiver<RpcEnvelope>>,
}

impl ChannelTransport {
    /// Create a new channel transport mesh for the given set of node IDs.
    /// Each node gets an inbound mpsc channel with the specified buffer
    /// capacity.
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

    /// Split the transport into per-node sender/receiver pairs.
    ///
    /// Each sender can deliver messages to any node in the cluster; each
    /// receiver only receives messages destined for that specific node.
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

    /// Get a shared handle to all inbound senders. Used by `NetworkSimulator`
    /// to deliver fault-injected messages directly to the destination inbox.
    pub fn inboxes(&self) -> Arc<HashMap<NodeId, mpsc::Sender<RpcEnvelope>>> {
        Arc::clone(&self.inboxes)
    }

    /// Take the receive half for `node_id`. Returns `None` if the receiver
    /// was already taken or the node was not part of the mesh.
    pub fn take_receiver(&mut self, node_id: NodeId) -> Option<mpsc::Receiver<RpcEnvelope>> {
        self.receivers.remove(&node_id)
    }
}

/// Sender half: routes messages to any node's inbox.
///
/// `&self` access is required by `TransportSender` for concurrent sends
/// by `IoStage`. Cheaply clonable.
#[derive(Clone)]
pub struct ChannelSender {
    inboxes: Arc<HashMap<NodeId, mpsc::Sender<RpcEnvelope>>>,
}

#[async_trait]
impl TransportSender for ChannelSender {
    async fn send(&self, target: NodeId, message: RpcEnvelope) -> io::Result<()> {
        let tx = self.inboxes.get(&target).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("no channel for target node {target}"),
            )
        })?;
        tx.send(message).await.map_err(|_| {
            io::Error::new(
                io::ErrorKind::ConnectionReset,
                format!("channel closed for target node {target}"),
            )
        })
    }
}

/// Receiver half: pulls messages from this node's inbound queue.
///
/// `&mut self` access is required by `TransportReceiver` for exclusive
/// use by a single `ReceiverTask`.
pub struct ChannelReceiver {
    rx: mpsc::Receiver<RpcEnvelope>,
}

#[async_trait]
impl TransportReceiver for ChannelReceiver {
    async fn recv(&mut self) -> io::Result<RpcEnvelope> {
        self.rx.recv().await.ok_or_else(|| {
            io::Error::new(io::ErrorKind::ConnectionReset, "all senders dropped")
        })
    }
}
