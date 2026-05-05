use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use tokio::sync::{mpsc, Mutex, RwLock};

use xraft_core::error::XraftError;
use xraft_core::rpc::RpcEnvelope;
use xraft_core::traits::{TransportReceiver, TransportSender};
use xraft_core::types::NodeId;

use crate::channel::ChannelTransport;

/// Per-link fault injection rules.
#[derive(Debug, Clone)]
struct LinkRules {
    /// Probability of dropping a message (0.0 = never, 1.0 = always).
    drop_probability: f64,
    /// Optional latency range (min, max). Messages are delayed by a random
    /// duration uniformly sampled from this range.
    delay_range: Option<(Duration, Duration)>,
    /// Whether to buffer and reorder messages on this link.
    reorder: bool,
}

impl Default for LinkRules {
    fn default() -> Self {
        Self {
            drop_probability: 0.0,
            delay_range: None,
            reorder: false,
        }
    }
}

/// Shared mutable state for the simulator, protected by RwLock for
/// concurrent access from multiple senders.
struct SimulatorState {
    /// Per-directed-link fault rules. Key is (source, destination).
    link_rules: HashMap<(NodeId, NodeId), LinkRules>,
    /// Directed partition edges. If (A, B) is in this set, messages
    /// from A to B are silently dropped.
    partitions: HashSet<(NodeId, NodeId)>,
    /// Per-link reorder buffer. Messages are buffered here and delivered
    /// in shuffled order on flush.
    reorder_buffers: HashMap<(NodeId, NodeId), Vec<RpcEnvelope>>,
    /// Seeded RNG for deterministic fault injection.
    rng: StdRng,
}

/// Control handle for the network simulator. Clonable — multiple test
/// threads can configure faults concurrently.
///
/// `NetworkSimulator` wraps a `ChannelTransport` and intercepts messages
/// to apply fault injection (drops, delays, reordering, partitions).
#[derive(Clone)]
pub struct NetworkSimulator {
    state: Arc<RwLock<SimulatorState>>,
    inboxes: Arc<HashMap<NodeId, mpsc::Sender<RpcEnvelope>>>,
}

impl NetworkSimulator {
    /// Create a simulator and produce per-node transport halves wired through
    /// fault injection.
    ///
    /// Returns `(control_handle, per_node_senders_and_receivers)`.
    pub fn create(
        node_ids: &[NodeId],
        buffer_size: usize,
        seed: u64,
    ) -> (
        Self,
        HashMap<NodeId, (Box<dyn TransportSender>, Box<dyn TransportReceiver>)>,
    ) {
        let mut transport = ChannelTransport::new(node_ids, buffer_size);
        let inboxes = transport.inboxes();

        let state = SimulatorState {
            link_rules: HashMap::new(),
            partitions: HashSet::new(),
            reorder_buffers: HashMap::new(),
            rng: StdRng::seed_from_u64(seed),
        };

        let handle = Self {
            state: Arc::new(RwLock::new(state)),
            inboxes: Arc::clone(&inboxes),
        };

        let mut result = HashMap::new();
        for &node_id in node_ids {
            let rx = transport
                .take_receiver(node_id)
                .expect("receiver must exist");
            let sender = SimulatorSender {
                source: node_id,
                state: Arc::clone(&handle.state),
                inboxes: Arc::clone(&inboxes),
            };
            let receiver = SimulatorReceiver {
                rx: Mutex::new(rx),
            };
            result.insert(
                node_id,
                (
                    Box::new(sender) as Box<dyn TransportSender>,
                    Box::new(receiver) as Box<dyn TransportReceiver>,
                ),
            );
        }

        (handle, result)
    }

    // ── Fault configuration ─────────────────────────────────────────

    /// Set the drop probability for messages from `from` to `to`.
    /// `probability` must be in [0.0, 1.0].
    pub async fn set_drop_probability(&self, from: NodeId, to: NodeId, probability: f64) {
        let mut state = self.state.write().await;
        state
            .link_rules
            .entry((from, to))
            .or_default()
            .drop_probability = probability.clamp(0.0, 1.0);
    }

    /// Set the latency range for messages from `from` to `to`.
    /// Each message is delayed by a random duration in `[min_delay, max_delay]`.
    /// If `min_delay > max_delay`, the values are swapped.
    pub async fn set_delay(
        &self,
        from: NodeId,
        to: NodeId,
        min_delay: Duration,
        max_delay: Duration,
    ) {
        let (lo, hi) = if min_delay > max_delay {
            (max_delay, min_delay)
        } else {
            (min_delay, max_delay)
        };
        let mut state = self.state.write().await;
        state
            .link_rules
            .entry((from, to))
            .or_default()
            .delay_range = Some((lo, hi));
    }

    /// Clear the delay for messages from `from` to `to`.
    pub async fn clear_delay(&self, from: NodeId, to: NodeId) {
        let mut state = self.state.write().await;
        if let Some(rules) = state.link_rules.get_mut(&(from, to)) {
            rules.delay_range = None;
        }
    }

    /// Enable message reordering on the link from `from` to `to`.
    /// Messages will be buffered and delivered in shuffled order
    /// when `flush_reorder_buffer` is called.
    pub async fn enable_reordering(&self, from: NodeId, to: NodeId) {
        let mut state = self.state.write().await;
        state
            .link_rules
            .entry((from, to))
            .or_default()
            .reorder = true;
        state
            .reorder_buffers
            .entry((from, to))
            .or_insert_with(Vec::new);
    }

    /// Disable reordering on a link and flush any buffered messages
    /// through the fault-injection pipeline.
    pub async fn disable_reordering(&self, from: NodeId, to: NodeId) {
        let msgs = {
            let mut state = self.state.write().await;
            if let Some(rules) = state.link_rules.get_mut(&(from, to)) {
                rules.reorder = false;
            }
            if let Some(mut buffer) = state.reorder_buffers.remove(&(from, to)) {
                buffer.shuffle(&mut state.rng);
                buffer
            } else {
                Vec::new()
            }
        };
        // Deliver each message through the fault pipeline (partition/drop/delay)
        for msg in msgs {
            if let Err(e) = apply_faults_and_deliver(&self.state, &self.inboxes, from, to, msg).await {
                tracing::warn!("flush delivery failed on {from}->{to}: {e}");
            }
        }
    }

    /// Flush the reorder buffer for a specific link, delivering all
    /// buffered messages in shuffled order through the fault-injection
    /// pipeline (respecting partitions, drops, and delays).
    pub async fn flush_reorder_buffer(&self, from: NodeId, to: NodeId) {
        let msgs = {
            let mut state = self.state.write().await;
            if let Some(buffer) = state.reorder_buffers.get_mut(&(from, to)) {
                let mut drained: Vec<RpcEnvelope> = buffer.drain(..).collect();
                drained.shuffle(&mut state.rng);
                drained
            } else {
                Vec::new()
            }
        };
        for msg in msgs {
            if let Err(e) = apply_faults_and_deliver(&self.state, &self.inboxes, from, to, msg).await {
                tracing::warn!("flush delivery failed on {from}->{to}: {e}");
            }
        }
    }

    /// Create a full (bidirectional) partition between two sets of nodes.
    /// All messages between nodes in `set_a` and nodes in `set_b` are blocked
    /// in both directions.
    pub async fn partition(&self, set_a: &[NodeId], set_b: &[NodeId]) {
        let mut state = self.state.write().await;
        for &a in set_a {
            for &b in set_b {
                state.partitions.insert((a, b));
                state.partitions.insert((b, a));
            }
        }
    }

    /// Create an asymmetric (one-way) partition: messages from `from` to `to`
    /// are blocked, but messages from `to` to `from` are allowed.
    pub async fn partition_one_way(&self, from: NodeId, to: NodeId) {
        let mut state = self.state.write().await;
        state.partitions.insert((from, to));
    }

    /// Heal all partitions, restoring full connectivity.
    pub async fn heal_partition(&self) {
        let mut state = self.state.write().await;
        state.partitions.clear();
    }

    /// Heal a specific directed link partition.
    pub async fn heal_link(&self, from: NodeId, to: NodeId) {
        let mut state = self.state.write().await;
        state.partitions.remove(&(from, to));
    }

    /// Clear all fault rules (drops, delays, reordering, partitions).
    pub async fn reset(&self) {
        let mut state = self.state.write().await;
        state.link_rules.clear();
        state.partitions.clear();
        state.reorder_buffers.clear();
    }

    /// Check if a directed link is currently partitioned.
    pub async fn is_partitioned(&self, from: NodeId, to: NodeId) -> bool {
        let state = self.state.read().await;
        state.partitions.contains(&(from, to))
    }
}

/// Deliver a single message applying partition, drop, and delay rules.
/// Does NOT handle reordering — that is the caller's responsibility.
async fn apply_faults_and_deliver(
    state: &RwLock<SimulatorState>,
    inboxes: &HashMap<NodeId, mpsc::Sender<RpcEnvelope>>,
    from: NodeId,
    to: NodeId,
    message: RpcEnvelope,
) -> Result<(), XraftError> {
    let (is_partitioned, drop_prob, delay_range) = {
        let s = state.read().await;
        let link = (from, to);
        let is_partitioned = s.partitions.contains(&link);
        let rules = s.link_rules.get(&link);
        let drop_prob = rules.map_or(0.0, |r| r.drop_probability);
        let delay_range = rules.and_then(|r| r.delay_range);
        (is_partitioned, drop_prob, delay_range)
    };

    if is_partitioned {
        return Ok(());
    }

    if drop_prob > 0.0 {
        let roll: f64 = {
            let mut s = state.write().await;
            s.rng.gen()
        };
        if roll < drop_prob {
            return Ok(());
        }
    }

    if let Some((min_d, max_d)) = delay_range {
        let delay = if min_d >= max_d {
            min_d
        } else {
            let nanos = {
                let mut s = state.write().await;
                s.rng.gen_range(min_d.as_nanos()..=max_d.as_nanos())
            };
            Duration::from_nanos(nanos as u64)
        };
        tokio::time::sleep(delay).await;
    }

    let tx = inboxes.get(&to).ok_or_else(|| {
        XraftError::TransportError(io::Error::new(
            io::ErrorKind::NotFound,
            format!("no channel for target node {to}"),
        ))
    })?;
    tx.send(message).await.map_err(|_| {
        XraftError::TransportError(io::Error::new(
            io::ErrorKind::ConnectionReset,
            format!("channel closed for target node {to}"),
        ))
    })
}

/// Sender that applies fault injection rules before forwarding to the
/// underlying channel.
pub struct SimulatorSender {
    source: NodeId,
    state: Arc<RwLock<SimulatorState>>,
    inboxes: Arc<HashMap<NodeId, mpsc::Sender<RpcEnvelope>>>,
}

#[async_trait]
impl TransportSender for SimulatorSender {
    async fn send(&self, target: NodeId, message: RpcEnvelope) -> Result<(), XraftError> {
        // Snapshot fault rules under lock, then release before any async work.
        let (is_partitioned, drop_prob, delay_range, reorder) = {
            let state = self.state.read().await;
            let link = (self.source, target);
            let is_partitioned = state.partitions.contains(&link);
            let rules = state.link_rules.get(&link);
            let drop_prob = rules.map_or(0.0, |r| r.drop_probability);
            let delay_range = rules.and_then(|r| r.delay_range);
            let reorder = rules.map_or(false, |r| r.reorder);
            (is_partitioned, drop_prob, delay_range, reorder)
        };

        // Rule precedence: partition → drop → reorder/delay → deliver

        // 1. Partition check — silently drop
        if is_partitioned {
            return Ok(());
        }

        // 2. Drop probability check
        if drop_prob > 0.0 {
            let roll: f64 = {
                let mut state = self.state.write().await;
                state.rng.gen()
            };
            if roll < drop_prob {
                return Ok(());
            }
        }

        // 3. Reordering — buffer the message instead of delivering
        if reorder {
            let mut state = self.state.write().await;
            state
                .reorder_buffers
                .entry((self.source, target))
                .or_default()
                .push(message);
            return Ok(());
        }

        // 4. Delay
        if let Some((min_d, max_d)) = delay_range {
            let delay = if min_d >= max_d {
                min_d
            } else {
                let nanos = {
                    let mut state = self.state.write().await;
                    state.rng.gen_range(min_d.as_nanos()..=max_d.as_nanos())
                };
                Duration::from_nanos(nanos as u64)
            };
            tokio::time::sleep(delay).await;
        }

        // 5. Deliver to target's inbox
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

/// Receiver that reads from the underlying channel. No fault injection
/// is applied on the receive side — all injection happens at send time.
pub struct SimulatorReceiver {
    rx: Mutex<mpsc::Receiver<RpcEnvelope>>,
}

#[async_trait]
impl TransportReceiver for SimulatorReceiver {
    async fn recv(&mut self) -> Result<RpcEnvelope, XraftError> {
        let mut rx = self.rx.lock().await;
        rx.recv().await.ok_or_else(|| {
            XraftError::TransportError(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "all senders dropped",
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;
    use xraft_core::rpc::{RpcPayload, VoteRequest};
    use xraft_core::types::{ClusterId, Term};

    fn make_envelope(source: NodeId) -> RpcEnvelope {
        RpcEnvelope {
            cluster_id: ClusterId(uuid::Uuid::nil()),
            leader_epoch: Term(1),
            source,
            payload: RpcPayload::VoteRequest(VoteRequest {
                term: Term(1),
                candidate_id: source,
                last_log_offset: 0,
                last_log_term: Term(0),
                is_pre_vote: false,
            }),
        }
    }

    #[tokio::test]
    async fn test_full_partition() {
        // Given a 3-node cluster
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let n3 = NodeId(3);
        let (sim, mut transports) =
            NetworkSimulator::create(&[n1, n2, n3], 64, 42);

        let (s1, _r1) = transports.remove(&n1).unwrap();
        let (s2, mut r2) = transports.remove(&n2).unwrap();
        let (s3, mut r3) = transports.remove(&n3).unwrap();

        // When a partition isolates N1 from {N2, N3}
        sim.partition(&[n1], &[n2, n3]).await;

        // N1 sends to N2 and N3 — should be silently dropped
        s1.send(n2, make_envelope(n1)).await.unwrap();
        s1.send(n3, make_envelope(n1)).await.unwrap();

        // N2 sends to N1 — should be silently dropped
        s2.send(n1, make_envelope(n2)).await.unwrap();

        // N3 sends to N1 — should be silently dropped
        s3.send(n1, make_envelope(n3)).await.unwrap();

        // But N2 → N3 should still work (not partitioned from each other)
        s2.send(n3, make_envelope(n2)).await.unwrap();

        // And N3 → N2 should still work
        s3.send(n2, make_envelope(n3)).await.unwrap();

        // Then: N2 receives message from N3 (not from N1)
        let msg = r2.recv().await.unwrap();
        assert_eq!(msg.source, n3);

        // N3 receives message from N2 (not from N1)
        let msg = r3.recv().await.unwrap();
        assert_eq!(msg.source, n2);

        // Verify N1 receives nothing by attempting recv with a timeout
        let (_, mut r1) = {
            // We already took r1 above, need to verify it's empty
            // N1's receiver should have no messages
            // Let's use a timeout to check
            drop(s1);
            drop(s2);
            drop(s3);
            ((), _r1)
        };

        let result = tokio::time::timeout(Duration::from_millis(50), r1.recv()).await;
        assert!(
            result.is_err(),
            "N1 should receive no messages during partition"
        );
    }

    #[tokio::test]
    async fn test_asymmetric_partition() {
        // Given a directed partition where N1→N2 is blocked but N2→N1 is allowed
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let (sim, mut transports) =
            NetworkSimulator::create(&[n1, n2], 64, 42);

        let (s1, mut r1) = transports.remove(&n1).unwrap();
        let (s2, mut r2) = transports.remove(&n2).unwrap();

        sim.partition_one_way(n1, n2).await;

        // When N1 sends to N2 — should be dropped
        s1.send(n2, make_envelope(n1)).await.unwrap();

        // When N2 sends to N1 — should be delivered
        s2.send(n1, make_envelope(n2)).await.unwrap();

        // Then: N1 receives message from N2
        let msg = r1.recv().await.unwrap();
        assert_eq!(msg.source, n2);

        // Then: N2 should NOT receive message from N1
        let result = tokio::time::timeout(Duration::from_millis(50), r2.recv()).await;
        assert!(
            result.is_err(),
            "N2 should not receive messages from N1 (asymmetric partition)"
        );
    }

    #[tokio::test]
    async fn test_message_delay_200ms() {
        // Given a 200 ms delay on the N1→N2 link
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let (sim, mut transports) =
            NetworkSimulator::create(&[n1, n2], 64, 42);

        let (s1, mut r2) = {
            let (s1, _r1) = transports.remove(&n1).unwrap();
            let (_s2, r2) = transports.remove(&n2).unwrap();
            (s1, r2)
        };

        let delay = Duration::from_millis(200);
        sim.set_delay(n1, n2, delay, delay).await;

        // When a message is sent
        let start = Instant::now();
        s1.send(n2, make_envelope(n1)).await.unwrap();

        // Then it is delivered after at least 200 ms
        let msg = r2.recv().await.unwrap();
        let elapsed = start.elapsed();

        assert_eq!(msg.source, n1);
        assert!(
            elapsed >= delay,
            "message should be delayed by at least 200ms, was delivered in {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn test_heal_partition_restores_connectivity() {
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let (sim, mut transports) =
            NetworkSimulator::create(&[n1, n2], 64, 42);

        let (s1, mut r2) = {
            let (s1, _r1) = transports.remove(&n1).unwrap();
            let (_s2, r2) = transports.remove(&n2).unwrap();
            (s1, r2)
        };

        // Partition, then heal
        sim.partition(&[n1], &[n2]).await;
        assert!(sim.is_partitioned(n1, n2).await);
        assert!(sim.is_partitioned(n2, n1).await);

        // Message during partition — dropped
        s1.send(n2, make_envelope(n1)).await.unwrap();
        let result = tokio::time::timeout(Duration::from_millis(50), r2.recv()).await;
        assert!(result.is_err(), "message should be dropped during partition");

        // Heal
        sim.heal_partition().await;
        assert!(!sim.is_partitioned(n1, n2).await);

        // Message after healing — delivered
        s1.send(n2, make_envelope(n1)).await.unwrap();
        let msg = r2.recv().await.unwrap();
        assert_eq!(msg.source, n1);
    }

    #[tokio::test]
    async fn test_message_drop_probability() {
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let (sim, mut transports) =
            NetworkSimulator::create(&[n1, n2], 256, 42);

        let (s1, mut r2) = {
            let (s1, _r1) = transports.remove(&n1).unwrap();
            let (_s2, r2) = transports.remove(&n2).unwrap();
            (s1, r2)
        };

        // 100% drop rate — all messages should be dropped
        sim.set_drop_probability(n1, n2, 1.0).await;
        for _ in 0..10 {
            s1.send(n2, make_envelope(n1)).await.unwrap();
        }
        let result = tokio::time::timeout(Duration::from_millis(50), r2.recv()).await;
        assert!(result.is_err(), "all messages should be dropped at 100% rate");

        // 0% drop rate — all messages should be delivered
        sim.set_drop_probability(n1, n2, 0.0).await;
        for _ in 0..5 {
            s1.send(n2, make_envelope(n1)).await.unwrap();
        }
        let mut count = 0;
        for _ in 0..5 {
            let result = tokio::time::timeout(Duration::from_millis(100), r2.recv()).await;
            if result.is_ok() {
                count += 1;
            }
        }
        assert_eq!(count, 5, "all 5 messages should be delivered at 0% drop rate");
    }

    #[tokio::test]
    async fn test_message_reordering() {
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let (sim, mut transports) =
            NetworkSimulator::create(&[n1, n2], 256, 42);

        let (s1, mut r2) = {
            let (s1, _r1) = transports.remove(&n1).unwrap();
            let (_s2, r2) = transports.remove(&n2).unwrap();
            (s1, r2)
        };

        sim.enable_reordering(n1, n2).await;

        // Send 10 messages — they should be buffered
        for i in 0..10 {
            let mut env = make_envelope(n1);
            env.leader_epoch = Term(i);
            s1.send(n2, env).await.unwrap();
        }

        // Nothing delivered yet (buffered)
        let result = tokio::time::timeout(Duration::from_millis(50), r2.recv()).await;
        assert!(result.is_err(), "messages should be buffered for reordering");

        // Flush — messages delivered in shuffled order
        sim.flush_reorder_buffer(n1, n2).await;

        let mut received_epochs = Vec::new();
        for _ in 0..10 {
            let msg = tokio::time::timeout(Duration::from_millis(100), r2.recv())
                .await
                .expect("should receive flushed message")
                .unwrap();
            received_epochs.push(msg.leader_epoch.0);
        }

        assert_eq!(received_epochs.len(), 10);
        // All messages should be delivered (possibly reordered)
        let mut sorted = received_epochs.clone();
        sorted.sort();
        assert_eq!(sorted, (0..10).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test_delay_with_range() {
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let (sim, mut transports) =
            NetworkSimulator::create(&[n1, n2], 64, 42);

        let (s1, mut r2) = {
            let (s1, _r1) = transports.remove(&n1).unwrap();
            let (_s2, r2) = transports.remove(&n2).unwrap();
            (s1, r2)
        };

        let min_delay = Duration::from_millis(50);
        let max_delay = Duration::from_millis(150);
        sim.set_delay(n1, n2, min_delay, max_delay).await;

        let start = Instant::now();
        s1.send(n2, make_envelope(n1)).await.unwrap();
        let _msg = r2.recv().await.unwrap();
        let elapsed = start.elapsed();

        assert!(
            elapsed >= min_delay,
            "message should be delayed by at least {min_delay:?}, was {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn test_heal_specific_link() {
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let n3 = NodeId(3);
        let (sim, mut transports) =
            NetworkSimulator::create(&[n1, n2, n3], 64, 42);

        let (s1, _r1) = transports.remove(&n1).unwrap();
        let (_s2, mut r2) = transports.remove(&n2).unwrap();
        let (_s3, mut r3) = transports.remove(&n3).unwrap();

        // Partition N1 from both N2 and N3
        sim.partition(&[n1], &[n2, n3]).await;

        // Heal only the N1→N2 link
        sim.heal_link(n1, n2).await;

        // N1→N2 should now work
        s1.send(n2, make_envelope(n1)).await.unwrap();
        let msg = r2.recv().await.unwrap();
        assert_eq!(msg.source, n1);

        // N1→N3 should still be blocked
        s1.send(n3, make_envelope(n1)).await.unwrap();
        let result = tokio::time::timeout(Duration::from_millis(50), r3.recv()).await;
        assert!(result.is_err(), "N1→N3 should still be partitioned");
    }

    #[tokio::test]
    async fn test_combined_partition_and_delay() {
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let n3 = NodeId(3);
        let (sim, mut transports) =
            NetworkSimulator::create(&[n1, n2, n3], 64, 42);

        let (s1, _r1) = transports.remove(&n1).unwrap();
        let (_s2, mut r2) = transports.remove(&n2).unwrap();
        let (_s3, mut r3) = transports.remove(&n3).unwrap();

        // Partition N1 from N3, but add delay on N1→N2
        sim.partition_one_way(n1, n3).await;
        let delay = Duration::from_millis(100);
        sim.set_delay(n1, n2, delay, delay).await;

        // N1→N3 blocked by partition (partition takes precedence over delay)
        s1.send(n3, make_envelope(n1)).await.unwrap();
        let result = tokio::time::timeout(Duration::from_millis(50), r3.recv()).await;
        assert!(result.is_err());

        // N1→N2 delayed
        let start = Instant::now();
        s1.send(n2, make_envelope(n1)).await.unwrap();
        let _msg = r2.recv().await.unwrap();
        let elapsed = start.elapsed();
        assert!(elapsed >= delay);
    }
}
