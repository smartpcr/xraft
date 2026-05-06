use std::collections::HashMap;
use std::sync::Arc;
use std::task::Poll;

use async_trait::async_trait;
use tokio::sync::mpsc;

use xraft_core::error::{Result, XraftError};
use xraft_core::rpc::RpcEnvelope;
use xraft_core::types::{ClusterId, NodeId};

use crate::codec::RpcCodec;

/// Network of in-process channels wired per node pair.
///
/// For each ordered `(source, destination)` pair, a dedicated
/// `tokio::sync::mpsc` channel is created. Call [`take`] to
/// extract a per-node [`ChannelTransport`] which can then be
/// [`split`](ChannelTransport::split) into trait-object halves.
pub struct ChannelNetwork {
    transports: HashMap<NodeId, ChannelTransport>,
}

impl ChannelNetwork {
    /// Create a new in-process network for the given set of node IDs.
    ///
    /// One `mpsc` channel per ordered `(src, dst)` pair is created,
    /// each bounded to `buffer` messages.
    pub fn new(cluster_id: ClusterId, node_ids: &[NodeId], buffer: usize) -> Self {
        // Per-pair channels: outbound[src][dst] = tx, inbound[dst][src] = rx
        let mut outbound: HashMap<NodeId, HashMap<NodeId, mpsc::Sender<Vec<u8>>>> =
            HashMap::new();
        let mut inbound: HashMap<NodeId, HashMap<NodeId, mpsc::Receiver<Vec<u8>>>> =
            HashMap::new();

        for &nid in node_ids {
            outbound.entry(nid).or_default();
            inbound.entry(nid).or_default();
        }

        for &src in node_ids {
            for &dst in node_ids {
                if src == dst {
                    continue;
                }
                let (tx, rx) = mpsc::channel(buffer);
                outbound.get_mut(&src).unwrap().insert(dst, tx);
                inbound.get_mut(&dst).unwrap().insert(src, rx);
            }
        }

        let mut transports = HashMap::new();
        for &nid in node_ids {
            let out = outbound.remove(&nid).unwrap();
            let inb = inbound.remove(&nid).unwrap();
            transports.insert(
                nid,
                ChannelTransport {
                    cluster_id,
                    outbound: Arc::new(out),
                    inbound_receivers: inb,
                },
            );
        }

        Self { transports }
    }

    /// Extract the [`ChannelTransport`] for `node_id`.
    ///
    /// Returns `Err` if `node_id` is unknown or was already taken.
    pub fn take(&mut self, node_id: NodeId) -> Result<ChannelTransport> {
        self.transports.remove(&node_id).ok_or_else(|| {
            XraftError::TransportError(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("no transport for node {node_id:?} (unknown or already taken)"),
            ))
        })
    }
}

/// Per-node in-process transport.
///
/// Created via [`ChannelNetwork::take`]. Call [`split`](Self::split) once
/// to obtain `(Box<dyn TransportSender>, Box<dyn TransportReceiver>)`.
/// `split` consumes `self` per architecture §4.4.
pub struct ChannelTransport {
    cluster_id: ClusterId,
    outbound: Arc<HashMap<NodeId, mpsc::Sender<Vec<u8>>>>,
    /// Per-pair inbound receivers keyed by source `NodeId`.
    inbound_receivers: HashMap<NodeId, mpsc::Receiver<Vec<u8>>>,
}

impl ChannelTransport {
    /// Split into `(sender, receiver)` trait-object halves.
    ///
    /// Consumes `self` — each transport can only be split once.
    /// Matches the architecture §4.4 signature:
    /// `fn split(self) -> (Box<dyn TransportSender>, Box<dyn TransportReceiver>)`
    pub fn split(
        self,
    ) -> (
        Box<dyn xraft_core::traits::TransportSender>,
        Box<dyn xraft_core::traits::TransportReceiver>,
    ) {
        let sender = ChannelSender {
            codec: Arc::new(RpcCodec::new(self.cluster_id)),
            senders: self.outbound,
        };
        // Convert the per-pair HashMap into a Vec for efficient polling.
        let receivers: Vec<mpsc::Receiver<Vec<u8>>> =
            self.inbound_receivers.into_values().collect();
        let receiver = ChannelReceiver {
            codec: RpcCodec::new(self.cluster_id),
            receivers,
        };
        (Box::new(sender), Box::new(receiver))
    }
}

/// Outbound half — `Send + Sync`, takes `&self` for concurrent sends.
pub struct ChannelSender {
    codec: Arc<RpcCodec>,
    senders: Arc<HashMap<NodeId, mpsc::Sender<Vec<u8>>>>,
}

#[async_trait]
impl xraft_core::traits::TransportSender for ChannelSender {
    async fn send(&self, target: NodeId, message: RpcEnvelope) -> Result<()> {
        let bytes = self.codec.encode(&message)?;
        let tx = self.senders.get(&target).ok_or_else(|| {
            XraftError::TransportError(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("no channel for node {target:?}"),
            ))
        })?;
        tx.send(bytes).await.map_err(|_| {
            XraftError::TransportError(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "receiver dropped",
            ))
        })
    }
}

/// Inbound half — `Send`, takes `&mut self` (exclusive access).
///
/// Merges all per-pair inbound channels for this node, delivering
/// messages from any peer. Each channel corresponds to a distinct
/// `(source, this_node)` pair.
pub struct ChannelReceiver {
    codec: RpcCodec,
    receivers: Vec<mpsc::Receiver<Vec<u8>>>,
}

#[async_trait]
impl xraft_core::traits::TransportReceiver for ChannelReceiver {
    async fn recv(&mut self) -> Result<RpcEnvelope> {
        let bytes = std::future::poll_fn(|cx| {
            let mut i = 0;
            while i < self.receivers.len() {
                match self.receivers[i].poll_recv(cx) {
                    Poll::Ready(Some(bytes)) => return Poll::Ready(Some(bytes)),
                    Poll::Ready(None) => {
                        // This per-pair channel is closed; remove it.
                        self.receivers.swap_remove(i);
                    }
                    Poll::Pending => {
                        i += 1;
                    }
                }
            }
            if self.receivers.is_empty() {
                Poll::Ready(None)
            } else {
                Poll::Pending
            }
        })
        .await
        .ok_or(XraftError::Shutdown)?;

        self.codec.decode(&bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xraft_core::rpc::{RpcPayload, VoteRequest};
    use xraft_core::types::Term;

    fn vote_request_envelope(cluster_id: ClusterId, source: NodeId) -> RpcEnvelope {
        RpcEnvelope {
            cluster_id,
            leader_epoch: Term(1),
            source,
            payload: RpcPayload::VoteRequest(VoteRequest {
                term: Term(2),
                candidate_id: source,
                last_log_offset: 5,
                last_log_term: Term(1),
                is_pre_vote: false,
            }),
        }
    }

    #[tokio::test]
    async fn send_and_receive_all_fields_intact() {
        let cid = ClusterId(uuid::Uuid::new_v4());
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let mut network = ChannelNetwork::new(cid, &[n1, n2], 16);
        let t1 = network.take(n1).unwrap();
        let t2 = network.take(n2).unwrap();
        let (sender_a, _recv_a) = t1.split();
        let (_sender_b, mut recv_b) = t2.split();

        let env = vote_request_envelope(cid, n1);
        sender_a.send(n2, env.clone()).await.unwrap();
        let received = recv_b.recv().await.unwrap();
        assert_eq!(env, received);
    }

    #[tokio::test]
    async fn cluster_id_fencing() {
        let cid_x = ClusterId(uuid::Uuid::new_v4());
        let cid_y = ClusterId(uuid::Uuid::new_v4());
        let n1 = NodeId(1);
        let n2 = NodeId(2);

        let mut network = ChannelNetwork::new(cid_y, &[n1, n2], 16);
        let t1 = network.take(n1).unwrap();
        let t2 = network.take(n2).unwrap();
        let (sender_a, _recv_a) = t1.split();
        let (_sender_b, mut recv_b) = t2.split();

        // Envelope carries cluster X but the transport expects cluster Y
        let env = vote_request_envelope(cid_x, n1);
        sender_a.send(n2, env).await.unwrap();

        let result = recv_b.recv().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), XraftError::InvalidClusterId));
    }

    #[tokio::test]
    async fn send_to_unknown_target_returns_error() {
        let cid = ClusterId(uuid::Uuid::new_v4());
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let n_unknown = NodeId(99);

        let mut network = ChannelNetwork::new(cid, &[n1, n2], 16);
        let t1 = network.take(n1).unwrap();
        let (sender_a, _recv_a) = t1.split();

        let env = vote_request_envelope(cid, n1);
        let result = sender_a.send(n_unknown, env).await;
        assert!(result.is_err(), "sending to unknown target must fail");
    }

    #[tokio::test]
    async fn recv_returns_shutdown_when_all_senders_dropped() {
        let cid = ClusterId(uuid::Uuid::new_v4());
        let n1 = NodeId(1);
        let n2 = NodeId(2);

        let mut network = ChannelNetwork::new(cid, &[n1, n2], 16);
        let t1 = network.take(n1).unwrap();
        let t2 = network.take(n2).unwrap();
        let (sender_a, _recv_a) = t1.split();
        let (_sender_b, mut recv_b) = t2.split();

        // Drop the only sender that can reach n2
        drop(sender_a);
        drop(_sender_b);

        let result = recv_b.recv().await;
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), XraftError::Shutdown),
            "recv must return Shutdown when all sender halves are dropped"
        );
    }

    #[tokio::test]
    async fn take_unknown_node_returns_error() {
        let cid = ClusterId(uuid::Uuid::new_v4());
        let n1 = NodeId(1);
        let n_unknown = NodeId(99);

        let mut network = ChannelNetwork::new(cid, &[n1], 16);
        let result = network.take(n_unknown);
        assert!(result.is_err(), "take for unknown node must return Err");
    }

    #[tokio::test]
    async fn take_twice_returns_error() {
        let cid = ClusterId(uuid::Uuid::new_v4());
        let n1 = NodeId(1);
        let n2 = NodeId(2);

        let mut network = ChannelNetwork::new(cid, &[n1, n2], 16);
        let _t1 = network.take(n1).unwrap();
        let result = network.take(n1);
        assert!(result.is_err(), "second take for same node must return Err");
    }

    #[tokio::test]
    async fn per_pair_channels_are_independent() {
        let cid = ClusterId(uuid::Uuid::new_v4());
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let n3 = NodeId(3);

        let mut network = ChannelNetwork::new(cid, &[n1, n2, n3], 16);
        let t1 = network.take(n1).unwrap();
        let t2 = network.take(n2).unwrap();
        let t3 = network.take(n3).unwrap();

        let (sender_1, _r1) = t1.split();
        let (sender_2, _r2) = t2.split();
        let (_sender_3, mut recv_3) = t3.split();

        // Both n1 and n2 send to n3 via independent per-pair channels
        let env1 = vote_request_envelope(cid, n1);
        let env2 = vote_request_envelope(cid, n2);

        sender_1.send(n3, env1.clone()).await.unwrap();
        sender_2.send(n3, env2.clone()).await.unwrap();

        // Receiver merges both per-pair channels; both messages arrive
        let mut received = Vec::new();
        received.push(recv_3.recv().await.unwrap());
        received.push(recv_3.recv().await.unwrap());

        assert!(received.contains(&env1));
        assert!(received.contains(&env2));
    }

    #[tokio::test]
    async fn per_pair_channel_independence_partial_close() {
        // Dropping one sender (n1→n3) does NOT prevent n2→n3 from working.
        let cid = ClusterId(uuid::Uuid::new_v4());
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let n3 = NodeId(3);

        let mut network = ChannelNetwork::new(cid, &[n1, n2, n3], 16);
        let t1 = network.take(n1).unwrap();
        let t2 = network.take(n2).unwrap();
        let t3 = network.take(n3).unwrap();

        let (sender_1, _r1) = t1.split();
        let (sender_2, _r2) = t2.split();
        let (_sender_3, mut recv_3) = t3.split();

        // Drop n1's sender — the n1→n3 pair channel closes.
        drop(sender_1);

        // n2→n3 must still work.
        let env2 = vote_request_envelope(cid, n2);
        sender_2.send(n3, env2.clone()).await.unwrap();
        let received = recv_3.recv().await.unwrap();
        assert_eq!(env2, received);
    }

    #[tokio::test]
    async fn leader_epoch_not_fenced_at_transport() {
        let cid = ClusterId(uuid::Uuid::new_v4());
        let n1 = NodeId(1);
        let n2 = NodeId(2);

        let mut network = ChannelNetwork::new(cid, &[n1, n2], 16);
        let t1 = network.take(n1).unwrap();
        let t2 = network.take(n2).unwrap();
        let (sender_a, _r1) = t1.split();
        let (_s2, mut recv_b) = t2.split();

        let mut env = vote_request_envelope(cid, n1);
        env.leader_epoch = Term(999);

        sender_a.send(n2, env.clone()).await.unwrap();
        let received = recv_b.recv().await.unwrap();
        assert_eq!(received.leader_epoch, Term(999));
    }

    #[tokio::test]
    async fn send_to_closed_receiver_returns_error() {
        let cid = ClusterId(uuid::Uuid::new_v4());
        let n1 = NodeId(1);
        let n2 = NodeId(2);

        let mut network = ChannelNetwork::new(cid, &[n1, n2], 16);
        let t1 = network.take(n1).unwrap();
        let t2 = network.take(n2).unwrap();
        let (sender_a, _recv_a) = t1.split();
        let (_sender_b, recv_b) = t2.split();

        // Drop n2's receiver — the channel for n1→n2 closes.
        drop(recv_b);

        let env = vote_request_envelope(cid, n1);
        let result = sender_a.send(n2, env).await;
        assert!(result.is_err(), "send to closed receiver must fail");
    }

    #[tokio::test]
    async fn bidirectional_communication() {
        let cid = ClusterId(uuid::Uuid::new_v4());
        let n1 = NodeId(1);
        let n2 = NodeId(2);

        let mut network = ChannelNetwork::new(cid, &[n1, n2], 16);
        let t1 = network.take(n1).unwrap();
        let t2 = network.take(n2).unwrap();
        let (sender_a, mut recv_a) = t1.split();
        let (sender_b, mut recv_b) = t2.split();

        let env_a = vote_request_envelope(cid, n1);
        let env_b = vote_request_envelope(cid, n2);

        // n1 → n2 and n2 → n1 simultaneously
        sender_a.send(n2, env_a.clone()).await.unwrap();
        sender_b.send(n1, env_b.clone()).await.unwrap();

        let received_b = recv_b.recv().await.unwrap();
        let received_a = recv_a.recv().await.unwrap();

        assert_eq!(env_a, received_b);
        assert_eq!(env_b, received_a);
    }
}
