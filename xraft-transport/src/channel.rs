//! In-process channel transport for testing.
//!
//! Uses in-memory message queues to simulate network communication between
//! Raft nodes. Supports deterministic message ordering and fault injection
//! (message drop, delay, partition).

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use xraft_core::log_entry::LogEntry;
use xraft_core::types::{NodeId, Term};

/// Messages exchanged between Raft nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftMessage {
    /// Leader sends entries to followers.
    AppendEntries {
        leader_id: NodeId,
        term: Term,
        prev_log_offset: u64,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    /// Follower acknowledges append.
    AppendEntriesResponse {
        node_id: NodeId,
        term: Term,
        success: bool,
        match_offset: u64,
    },
    /// Candidate requests vote.
    VoteRequest {
        candidate_id: NodeId,
        term: Term,
        last_log_offset: u64,
        last_log_term: Term,
    },
    /// Node responds to vote request.
    VoteResponse {
        node_id: NodeId,
        term: Term,
        granted: bool,
    },
}

/// Internal shared state for the transport.
#[derive(Debug)]
struct TransportInner {
    /// Queues of messages: `(from, to) -> queue`.
    queues: HashMap<(NodeId, NodeId), VecDeque<RaftMessage>>,
    /// Set of node IDs in the cluster.
    nodes: Vec<NodeId>,
    /// Partitioned node pairs — messages between these are dropped.
    partitions: Vec<(NodeId, NodeId)>,
}

/// In-process transport using message queues per node pair.
///
/// Thread-safe via `Arc<Mutex<...>>` so it can be shared across nodes.
/// Provides `send()` and `recv()` for deterministic message passing.
#[derive(Debug, Clone)]
pub struct ChannelTransport {
    inner: Arc<Mutex<TransportInner>>,
}

impl ChannelTransport {
    /// Create a transport for the given set of nodes.
    pub fn new(nodes: Vec<NodeId>) -> Self {
        let mut queues = HashMap::new();
        for &from in &nodes {
            for &to in &nodes {
                if from != to {
                    queues.insert((from, to), VecDeque::new());
                }
            }
        }
        Self {
            inner: Arc::new(Mutex::new(TransportInner {
                queues,
                nodes,
                partitions: Vec::new(),
            })),
        }
    }

    /// Send a message from one node to another. Messages to partitioned
    /// nodes are silently dropped.
    pub fn send(&self, from: NodeId, to: NodeId, msg: RaftMessage) {
        let mut inner = self.inner.lock().unwrap();
        // Check partition
        if inner
            .partitions
            .iter()
            .any(|&(a, b)| (a == from && b == to) || (a == to && b == from))
        {
            return; // message dropped due to partition
        }
        if let Some(queue) = inner.queues.get_mut(&(from, to)) {
            queue.push_back(msg);
        }
    }

    /// Receive all pending messages for a node, returning `(from, message)`.
    pub fn recv(&self, to: NodeId) -> Vec<(NodeId, RaftMessage)> {
        let mut inner = self.inner.lock().unwrap();
        let mut messages = Vec::new();
        let nodes: Vec<NodeId> = inner.nodes.clone();
        for from in nodes {
            if from == to {
                continue;
            }
            if let Some(queue) = inner.queues.get_mut(&(from, to)) {
                while let Some(msg) = queue.pop_front() {
                    messages.push((from, msg));
                }
            }
        }
        messages
    }

    /// Drain and return all pending messages across all queues.
    pub fn drain_all(&self) -> Vec<(NodeId, NodeId, RaftMessage)> {
        let mut inner = self.inner.lock().unwrap();
        let mut messages = Vec::new();
        for (&(from, to), queue) in &mut inner.queues {
            while let Some(msg) = queue.pop_front() {
                messages.push((from, to, msg));
            }
        }
        messages
    }

    /// Add a network partition between two nodes.
    pub fn add_partition(&self, a: NodeId, b: NodeId) {
        self.inner.lock().unwrap().partitions.push((a, b));
    }

    /// Remove all partitions.
    pub fn clear_partitions(&self) {
        self.inner.lock().unwrap().partitions.clear();
    }

    /// Return the number of pending messages across all queues.
    pub fn pending_count(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.queues.values().map(|q| q.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_and_receive() {
        let nodes = vec![NodeId(1), NodeId(2), NodeId(3)];
        let transport = ChannelTransport::new(nodes);

        transport.send(
            NodeId(1),
            NodeId(2),
            RaftMessage::VoteRequest {
                candidate_id: NodeId(1),
                term: Term(1),
                last_log_offset: 0,
                last_log_term: Term(0),
            },
        );

        let msgs = transport.recv(NodeId(2));
        assert_eq!(msgs.len(), 1);
        let (from, msg) = &msgs[0];
        assert_eq!(*from, NodeId(1));
        match msg {
            RaftMessage::VoteRequest { candidate_id, term, .. } => {
                assert_eq!(*candidate_id, NodeId(1));
                assert_eq!(*term, Term(1));
            }
            _ => panic!("expected VoteRequest"),
        }

        // Node 3 has no messages
        assert!(transport.recv(NodeId(3)).is_empty());
    }

    #[test]
    fn partition_drops_messages() {
        let nodes = vec![NodeId(1), NodeId(2)];
        let transport = ChannelTransport::new(nodes);

        transport.add_partition(NodeId(1), NodeId(2));

        transport.send(
            NodeId(1),
            NodeId(2),
            RaftMessage::VoteRequest {
                candidate_id: NodeId(1),
                term: Term(1),
                last_log_offset: 0,
                last_log_term: Term(0),
            },
        );

        // Message should be dropped
        assert!(transport.recv(NodeId(2)).is_empty());

        // Clear partition and try again
        transport.clear_partitions();
        transport.send(
            NodeId(1),
            NodeId(2),
            RaftMessage::VoteResponse {
                node_id: NodeId(1),
                term: Term(1),
                granted: true,
            },
        );
        assert_eq!(transport.recv(NodeId(2)).len(), 1);
    }

    #[test]
    fn drain_all_returns_everything() {
        let nodes = vec![NodeId(1), NodeId(2), NodeId(3)];
        let transport = ChannelTransport::new(nodes);

        transport.send(
            NodeId(1),
            NodeId(2),
            RaftMessage::VoteResponse {
                node_id: NodeId(1),
                term: Term(1),
                granted: true,
            },
        );
        transport.send(
            NodeId(2),
            NodeId(3),
            RaftMessage::VoteResponse {
                node_id: NodeId(2),
                term: Term(1),
                granted: false,
            },
        );

        let all = transport.drain_all();
        assert_eq!(all.len(), 2);
        assert_eq!(transport.pending_count(), 0);
    }
}
