//! In-process channel-based transport for deterministic testing.
//!
//! Uses `tokio::sync::mpsc` channels to deliver `RpcEnvelope` messages
//! between nodes without any network I/O. Each node gets a
//! `ChannelTransportSender` (can send to any peer) and a
//! `ChannelTransportReceiver` (receives inbound messages).

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use xraft_core::rpc::RpcEnvelope;
use xraft_core::traits::{TransportReceiver, TransportSender};
use xraft_core::types::NodeId;

/// Channel-based transport sender that routes messages to peers via mpsc channels.
///
/// Implements `TransportSender` with `&self` (shared reference) because the
/// `IoStage` may send to multiple peers concurrently. Thread-safe via `Arc`.
pub struct ChannelTransportSender {
    /// Map from target NodeId to that node's inbound channel sender.
    peers: Arc<HashMap<NodeId, mpsc::Sender<RpcEnvelope>>>,
}

impl ChannelTransportSender {
    /// Create a new sender with access to all peer channels.
    pub fn new(peers: HashMap<NodeId, mpsc::Sender<RpcEnvelope>>) -> Self {
        Self {
            peers: Arc::new(peers),
        }
    }
}

#[async_trait]
impl TransportSender for ChannelTransportSender {
    async fn send(
        &self,
        target: NodeId,
        message: RpcEnvelope,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let tx = self
            .peers
            .get(&target)
            .ok_or_else(|| format!("no channel for target {:?}", target))?;
        tx.send(message)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        Ok(())
    }
}

/// Channel-based transport receiver for inbound messages.
///
/// Implements `TransportReceiver` with `&mut self` (exclusive access) because
/// only the `ReceiverTask` reads from the network per architecture §4.4.
pub struct ChannelTransportReceiver {
    rx: mpsc::Receiver<RpcEnvelope>,
}

impl ChannelTransportReceiver {
    /// Create a new receiver wrapping an mpsc receiver.
    pub fn new(rx: mpsc::Receiver<RpcEnvelope>) -> Self {
        Self { rx }
    }
}

#[async_trait]
impl TransportReceiver for ChannelTransportReceiver {
    async fn recv(&mut self) -> Result<RpcEnvelope, Box<dyn std::error::Error + Send + Sync>> {
        self.rx
            .recv()
            .await
            .ok_or_else(|| "channel closed".into())
    }
}

/// Create a fully-connected channel network for N nodes.
///
/// Returns a map from NodeId to (sender, receiver) pairs. Each sender can
/// route messages to any other node in the network.
pub fn create_channel_network(
    node_ids: &[NodeId],
) -> HashMap<NodeId, (ChannelTransportSender, ChannelTransportReceiver)> {
    let buffer_size = 1024;

    // Create one inbound channel per node
    let mut inbound_txs: HashMap<NodeId, mpsc::Sender<RpcEnvelope>> = HashMap::new();
    let mut inbound_rxs: HashMap<NodeId, mpsc::Receiver<RpcEnvelope>> = HashMap::new();

    for &nid in node_ids {
        let (tx, rx) = mpsc::channel(buffer_size);
        inbound_txs.insert(nid, tx);
        inbound_rxs.insert(nid, rx);
    }

    // Each node gets a sender with access to all other nodes' inbound channels
    let mut result = HashMap::new();
    for &nid in node_ids {
        let mut peer_map = HashMap::new();
        for (&peer_id, tx) in &inbound_txs {
            if peer_id != nid {
                peer_map.insert(peer_id, tx.clone());
            }
        }
        let sender = ChannelTransportSender::new(peer_map);
        let receiver = ChannelTransportReceiver::new(inbound_rxs.remove(&nid).unwrap());
        result.insert(nid, (sender, receiver));
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use xraft_core::rpc::{RpcPayload, VoteRequest};
    use xraft_core::types::Term;

    #[tokio::test]
    async fn send_and_receive_between_nodes() {
        let nodes = vec![NodeId(1), NodeId(2), NodeId(3)];
        let mut network = create_channel_network(&nodes);

        let (sender_1, _) = network.remove(&NodeId(1)).unwrap();
        let (_, mut receiver_2) = network.remove(&NodeId(2)).unwrap();

        let envelope = RpcEnvelope {
            cluster_id: "test-cluster".to_string(),
            source: NodeId(1),
            leader_epoch: 1,
            payload: RpcPayload::VoteRequest(VoteRequest {
                term: Term(1),
                candidate_id: NodeId(1),
                last_log_offset: 0,
                last_log_term: Term(0),
                is_pre_vote: false,
            }),
        };

        sender_1.send(NodeId(2), envelope).await.unwrap();
        let received = receiver_2.recv().await.unwrap();
        assert_eq!(received.source, NodeId(1));
        assert_eq!(received.leader_epoch, 1);
        assert_eq!(received.cluster_id, "test-cluster");
    }

    #[tokio::test]
    async fn send_to_unknown_peer_returns_error() {
        let nodes = vec![NodeId(1), NodeId(2)];
        let mut network = create_channel_network(&nodes);
        let (sender_1, _) = network.remove(&NodeId(1)).unwrap();

        let envelope = RpcEnvelope {
            cluster_id: "test-cluster".to_string(),
            source: NodeId(1),
            leader_epoch: 1,
            payload: RpcPayload::VoteRequest(VoteRequest {
                term: Term(1),
                candidate_id: NodeId(1),
                last_log_offset: 0,
                last_log_term: Term(0),
                is_pre_vote: false,
            }),
        };

        let result = sender_1.send(NodeId(99), envelope).await;
        assert!(result.is_err());
    }
}
