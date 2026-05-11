use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

use crate::message::RaftMessage;
use crate::transport::Transport;
use crate::NodeId;

/// A pair of sender/receiver endpoints for one node.
pub struct ChannelEndpoint {
    pub tx: mpsc::Sender<RaftMessage>,
    pub rx: Option<mpsc::Receiver<RaftMessage>>,
}

/// In-process channel-based transport.
///
/// Every node gets a bounded mpsc channel. `send()` looks up the
/// destination's sender; `take_receiver()` hands the receive half
/// to the node's driver loop.
pub struct ChannelTransport {
    endpoints: HashMap<NodeId, ChannelEndpoint>,
}

impl ChannelTransport {
    /// Create a new transport with channels pre-allocated for `nodes`.
    pub fn new(nodes: &[NodeId], buffer: usize) -> Self {
        let mut endpoints = HashMap::new();
        for &id in nodes {
            let (tx, rx) = mpsc::channel(buffer);
            endpoints.insert(id, ChannelEndpoint { tx, rx: Some(rx) });
        }
        Self { endpoints }
    }

    /// Split into a shared sender handle and per-node receivers.
    pub fn split(self) -> (Arc<Mutex<HashMap<NodeId, mpsc::Sender<RaftMessage>>>>,
                           HashMap<NodeId, mpsc::Receiver<RaftMessage>>) {
        let mut senders = HashMap::new();
        let mut receivers = HashMap::new();
        for (id, ep) in self.endpoints {
            senders.insert(id, ep.tx);
            if let Some(rx) = ep.rx {
                receivers.insert(id, rx);
            }
        }
        (Arc::new(Mutex::new(senders)), receivers)
    }

    /// Return a snapshot of all node-to-sender mappings (for the simulator).
    pub fn inboxes(&self) -> HashMap<NodeId, mpsc::Sender<RaftMessage>> {
        self.endpoints
            .iter()
            .map(|(&id, ep)| (id, ep.tx.clone()))
            .collect()
    }

    /// Take the receive half for `node`, leaving `None` in its place.
    ///
    /// Panics if the receiver was already taken or the node doesn't exist.
    pub fn take_receiver(&mut self, node: NodeId) -> mpsc::Receiver<RaftMessage> {
        self.endpoints
            .get_mut(&node)
            .expect("unknown node")
            .rx
            .take()
            .expect("receiver already taken")
    }
}

#[async_trait::async_trait]
impl Transport for ChannelTransport {
    async fn send(&self, to: NodeId, msg: RaftMessage) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let sender = self
            .endpoints
            .get(&to)
            .ok_or_else(|| format!("no channel for node {to:?}"))?;
        sender.tx.send(msg).await.map_err(|e| Box::new(e) as _)
    }
}
