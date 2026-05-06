//! Async Replication Scenario Tests
//!
//! Exercises the same `NodeState` consensus engine through an async harness
//! using `ChannelTransport` (from `xraft-transport`) for true async message
//! delivery via `tokio::sync::mpsc` channels.
//!
//! Each node runs as a separate tokio task, with its own:
//! - `EventLoopDriver` (owns RaftNode, TestStateMachine, NoOpListener, DeferredCompletionQueue)
//! - `ChannelTransportSender` (TransportSender trait for async message delivery)
//! - `ChannelTransportReceiver` (TransportReceiver trait for async message receipt)
//! - `MemoryLogStore` (LogStore trait for persistent log storage)
//! - `IoStage` (executes IoActionBatch through trait objects)
//!
//! This harness validates the transport-level contracts that the synchronous
//! `SimulatedCluster` cannot exercise: true async send/receive, channel
//! backpressure, and concurrent proposal submission from multiple tasks.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{timeout, Duration};
use xraft_core::*;
use xraft_core::rpc::{RpcEnvelope, RpcPayload};
use xraft_core::traits::TransportReceiver;
use xraft_core::event_loop::{EventLoopDriver, InboundMessage};
use xraft_core::io_action::{IoAction, IoActionBatch};
use xraft_core::io_stage::IoStage;
use xraft_core::listener::NoOpListener;
use xraft_storage::MemoryLogStore;
use xraft_transport::create_channel_network;
use xraft_test::TestStateMachine;

/// Shared async node: EventLoopDriver + IoStage for async operation.
struct AsyncNode {
    driver: EventLoopDriver<TestStateMachine, NoOpListener>,
    io_stage: IoStage,
}

impl AsyncNode {
    fn new(node: RaftNode, log_store: MemoryLogStore, transport_sender: Box<dyn TransportSender>) -> Self {
        let config = node.config.clone();
        let sm = TestStateMachine::new();
        let listener = NoOpListener;
        let driver = EventLoopDriver::new(
            node,
            sm,
            listener,
            "test-cluster".to_string(),
            config,
        );
        let io_stage = IoStage::new(
            Box::new(log_store),
            Box::new(xraft_storage::MemoryQuorumStateStore::new()),
            transport_sender,
        );
        Self { driver, io_stage }
    }

    /// Execute an IoActionBatch through the IoStage — async version.
    /// Splits SendRpc (which genuinely yields with ChannelTransport) from
    /// storage operations (which resolve immediately with MemoryLogStore).
    async fn execute_batch_async(&self, batch: &IoActionBatch) {
        self.io_stage.execute(batch).await.unwrap();
    }
}

type SharedNode = Arc<Mutex<AsyncNode>>;

/// Create a cluster of async nodes with channel transport + IoStage.
fn create_async_cluster(
    node_count: u64,
) -> (
    HashMap<NodeId, SharedNode>,
    HashMap<NodeId, Box<dyn TransportReceiver>>,
) {
    let config = RaftConfig::default();
    let voter_set: Vec<VoterInfo> = (1..=node_count)
        .map(|i| VoterInfo {
            node_id: NodeId(i),
            endpoint: format!("127.0.0.1:{}", 9000 + i),
        })
        .collect();

    let node_ids: Vec<NodeId> = voter_set.iter().map(|v| v.node_id).collect();
    let network = create_channel_network(&node_ids);

    let mut nodes = HashMap::new();
    let mut receivers: HashMap<NodeId, Box<dyn TransportReceiver>> = HashMap::new();

    for (nid, (sender, receiver)) in network {
        let mut raft_node = RaftNode::new(nid, voter_set.clone(), config.clone());
        raft_node.bootstrap();
        let log_store = MemoryLogStore::new();
        let async_node = AsyncNode::new(raft_node, log_store, Box::new(sender));
        nodes.insert(nid, Arc::new(Mutex::new(async_node)));
        receivers.insert(nid, Box::new(receiver));
    }

    (nodes, receivers)
}

/// Run an election for a candidate via async transport.
async fn async_elect_leader(
    candidate_id: NodeId,
    nodes: &HashMap<NodeId, SharedNode>,
    receivers: &mut HashMap<NodeId, Box<dyn TransportReceiver>>,
) {
    let now = 0u64;

    // Start election through EventLoopDriver
    {
        let mut node = nodes.get(&candidate_id).unwrap().lock().await;
        let batch = node.driver.start_election(now);
        node.execute_batch_async(&batch).await;
    }

    // Each voter receives VoteRequest and processes through EventLoopDriver
    let voter_ids: Vec<NodeId> = nodes.keys().filter(|id| **id != candidate_id).copied().collect();
    for voter_id in &voter_ids {
        let receiver = receivers.get_mut(voter_id).unwrap();
        let envelope = receiver.recv().await.unwrap();
        if let RpcPayload::VoteRequest(req) = envelope.payload {
            let mut node = nodes.get(voter_id).unwrap().lock().await;
            let msg = InboundMessage::VoteRequest { from: envelope.source, request: req };
            let (batch, _) = node.driver.process(msg, now);
            node.execute_batch_async(&batch).await;
        }
    }

    // Candidate processes vote responses through EventLoopDriver
    let candidate_rx = receivers.get_mut(&candidate_id).unwrap();
    for _ in 0..voter_ids.len() {
        let envelope = candidate_rx.recv().await.unwrap();
        if let RpcPayload::VoteResponse(resp) = envelope.payload {
            let mut node = nodes.get(&candidate_id).unwrap().lock().await;
            let msg = InboundMessage::VoteResponse { from: envelope.source, response: resp };
            let (batch, _) = node.driver.process(msg, now);
            node.execute_batch_async(&batch).await;
        }
    }

    {
        let node = nodes.get(&candidate_id).unwrap().lock().await;
        assert!(node.driver.node.is_leader(), "Failed to elect {:?}", candidate_id);
    }
}

/// Run one async fetch round through EventLoopDriver + IoStage.
async fn async_fetch_round(
    leader_id: NodeId,
    nodes: &HashMap<NodeId, SharedNode>,
    receivers: &mut HashMap<NodeId, Box<dyn TransportReceiver>>,
) {
    let now = 0u64;

    let follower_ids: Vec<NodeId> = nodes.keys().filter(|id| **id != leader_id).copied().collect();

    // Followers build and send FetchRequests via IoStage
    for fid in &follower_ids {
        let node = nodes.get(fid).unwrap().lock().await;
        let fetch_req = node.driver.build_fetch_request();
        let envelope = RpcEnvelope {
            cluster_id: "test-cluster".to_string(),
            source: *fid,
            leader_epoch: node.driver.node.term().0,
            payload: RpcPayload::FetchRequest(fetch_req),
        };
        let batch = IoActionBatch {
            actions: vec![IoAction::SendRpc(leader_id, envelope)],
        };
        node.execute_batch_async(&batch).await;
    }

    // Leader receives and processes FetchRequests through EventLoopDriver
    let leader_rx = receivers.get_mut(&leader_id).unwrap();
    for _ in 0..follower_ids.len() {
        let envelope = leader_rx.recv().await.unwrap();
        if let RpcPayload::FetchRequest(req) = envelope.payload {
            let mut node = nodes.get(&leader_id).unwrap().lock().await;
            let msg = InboundMessage::FetchRequest { from: envelope.source, request: req };
            let (batch, _) = node.driver.process(msg, now);
            node.execute_batch_async(&batch).await;
        }
    }

    // Followers receive FetchResponses and process through EventLoopDriver
    for fid in &follower_ids {
        let follower_rx = receivers.get_mut(fid).unwrap();
        let envelope = follower_rx.recv().await.unwrap();
        if let RpcPayload::FetchResponse(resp) = envelope.payload {
            let mut node = nodes.get(fid).unwrap().lock().await;
            let msg = InboundMessage::FetchResponse { from: envelope.source, response: resp };
            let (batch, _) = node.driver.process(msg, now);
            node.execute_batch_async(&batch).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Async Test 1: Full replication via ChannelTransport + EventLoopDriver
// ---------------------------------------------------------------------------

#[tokio::test]
async fn async_full_replication_100_entries() {
    let (nodes, mut receivers) = create_async_cluster(3);
    let leader_id = NodeId(1);

    async_elect_leader(leader_id, &nodes, &mut receivers).await;

    // Propose 100 entries through EventLoopDriver
    for i in 0..100u32 {
        let mut node = nodes.get(&leader_id).unwrap().lock().await;
        let msg = InboundMessage::Propose(AppRecord::new(i.to_be_bytes().to_vec()));
        let (batch, _rx) = node.driver.process(msg, 0);
        node.execute_batch_async(&batch).await;
    }

    // Replicate via async fetch rounds
    for _ in 0..10 {
        async_fetch_round(leader_id, &nodes, &mut receivers).await;
    }

    // Verify convergence
    let leader_node = nodes.get(&leader_id).unwrap().lock().await;
    let expected_leo = leader_node.driver.node.log_end_offset();
    let expected_hw = leader_node.driver.node.high_watermark();
    drop(leader_node);

    assert!(expected_hw >= 101, "HW should cover all entries (got {})", expected_hw);

    for nid in [NodeId(1), NodeId(2), NodeId(3)] {
        let node = nodes.get(&nid).unwrap().lock().await;
        assert_eq!(
            node.driver.node.log_end_offset(), expected_leo,
            "Node {:?} LEO mismatch", nid
        );
        assert_eq!(
            node.driver.node.high_watermark(), expected_hw,
            "Node {:?} HW mismatch", nid
        );
        assert_eq!(
            node.driver.state_machine.applied_count(), 100,
            "Node {:?} SM should have applied 100 commands (got {})",
            nid, node.driver.state_machine.applied_count()
        );
        assert_eq!(
            node.driver.state_machine.duplicate_apply_count(), 0,
            "Node {:?} SM should have zero duplicate applies", nid
        );

        // Verify LogStore consistency via IoStage
        assert_eq!(
            node.io_stage.log_store().log_end_offset(), expected_leo,
            "Node {:?} LogStore LEO mismatch", nid
        );
    }
}

// ---------------------------------------------------------------------------
// Async Test 2: Concurrent proposals from multiple tasks
// ---------------------------------------------------------------------------

#[tokio::test]
async fn async_concurrent_proposals() {
    let (nodes, mut receivers) = create_async_cluster(3);
    let leader_id = NodeId(1);

    async_elect_leader(leader_id, &nodes, &mut receivers).await;

    // Submit proposals from concurrent tasks via an async proposal channel.
    let (proposal_tx, mut proposal_rx) = mpsc::channel::<AppRecord>(256);
    let leader_clone = nodes.get(&leader_id).unwrap().clone();

    // Spawn proposal consumer task — processes through EventLoopDriver
    let consumer = tokio::spawn(async move {
        let mut offsets = Vec::new();
        while let Some(record) = proposal_rx.recv().await {
            let mut node = leader_clone.lock().await;
            let msg = InboundMessage::Propose(record);
            let (batch, _rx) = node.driver.process(msg, 0);
            node.execute_batch_async(&batch).await;
            offsets.push(node.driver.node.log_end_offset() - 1);
        }
        offsets
    });

    // Spawn 5 concurrent producer tasks, each proposing 20 entries
    let mut producer_handles = Vec::new();
    for task_id in 0..5u32 {
        let tx = proposal_tx.clone();
        let handle = tokio::spawn(async move {
            for i in 0..20u32 {
                let val = task_id * 100 + i;
                let record = AppRecord::new(val.to_be_bytes().to_vec());
                tx.send(record).await.unwrap();
            }
        });
        producer_handles.push(handle);
    }
    drop(proposal_tx);

    for h in producer_handles {
        timeout(Duration::from_secs(5), h).await.unwrap().unwrap();
    }

    let offsets = timeout(Duration::from_secs(5), consumer).await.unwrap().unwrap();
    assert_eq!(offsets.len(), 100, "Should have 100 proposed offsets");

    // Run fetch rounds to replicate
    for _ in 0..10 {
        async_fetch_round(leader_id, &nodes, &mut receivers).await;
    }

    // Verify all nodes converged
    let leader_node = nodes.get(&leader_id).unwrap().lock().await;
    let final_hw = leader_node.driver.node.high_watermark();
    let final_leo = leader_node.driver.node.log_end_offset();
    drop(leader_node);

    assert!(final_hw >= 101, "HW should cover LCM + 100 commands (got {})", final_hw);

    for nid in [NodeId(1), NodeId(2), NodeId(3)] {
        let node = nodes.get(&nid).unwrap().lock().await;
        assert_eq!(
            node.driver.node.log_end_offset(), final_leo,
            "Node {:?} LEO mismatch after concurrent proposals", nid
        );
        assert_eq!(
            node.driver.node.high_watermark(), final_hw,
            "Node {:?} HW mismatch after concurrent proposals", nid
        );
        assert_eq!(
            node.driver.state_machine.applied_count(), 100,
            "Node {:?} SM should have applied 100 commands (got {})",
            nid, node.driver.state_machine.applied_count()
        );
        assert_eq!(
            node.driver.state_machine.duplicate_apply_count(), 0,
            "Node {:?} SM should have no duplicates", nid
        );

        // Apply order monotonic
        let order = node.driver.state_machine.apply_order();
        for i in 1..order.len() {
            assert!(
                order[i] > order[i - 1],
                "Node {:?} apply order not monotonic: {} -> {}",
                nid, order[i - 1], order[i]
            );
        }
    }

    // Verify all 100 unique proposal values are present
    let leader_node = nodes.get(&leader_id).unwrap().lock().await;
    let mut applied_values: Vec<u32> = Vec::new();
    for (_, record) in leader_node.driver.state_machine.applied_entries() {
        if record.data.len() == 4 {
            let val = u32::from_be_bytes(record.data[..4].try_into().unwrap());
            applied_values.push(val);
        }
    }
    applied_values.sort();
    applied_values.dedup();
    assert_eq!(
        applied_values.len(), 100,
        "Should have 100 unique applied values (got {})",
        applied_values.len()
    );

    // Verify DCQ is empty after full replication
    assert!(
        leader_node.driver.completion_queue.all_completed(),
        "Leader DCQ should have no pending proposals"
    );
}
