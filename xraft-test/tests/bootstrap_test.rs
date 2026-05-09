use std::net::SocketAddr;
use std::time::Duration;

use xraft_core::config::RaftConfig;
use xraft_core::consensus_state::Role;
use xraft_core::error::XraftError;
use xraft_core::quorum_state::QuorumState;
use xraft_core::raft_node::RaftNode;
use xraft_core::snapshot::{Snapshot, SnapshotMetadata};
use xraft_core::app_record::AppSnapshot;
use xraft_core::types::{ClusterId, NodeId, Term};
use xraft_core::voter::{VoterInfo, VotersRecord};

use xraft_test::mocks::*;

fn voter(id: u64, port: u16) -> VoterInfo {
    VoterInfo {
        node_id: NodeId(id),
        endpoint: SocketAddr::from(([127, 0, 0, 1], port)),
    }
}

fn config_for(node_id: NodeId) -> RaftConfig {
    RaftConfig::with_node_id(node_id)
}

async fn make_fresh_node(
    node_id: NodeId,
) -> RaftNode<MockStateMachine, MockListener> {
    RaftNode::new(
        config_for(node_id),
        Box::new(MockLogStore::new()),
        Box::new(MockQuorumStateStore::new()),
        Box::new(MockSnapshotIO::new()),
        Box::new(MockTransportSender::new()),
        Box::new(MockTransportReceiver),
        Box::new(MockClock::new()),
        MockStateMachine,
        MockListener,
    )
    .await
    .expect("fresh node should create successfully")
}

// ───────────────────────────────────────────────────────────────
// Fresh bootstrap — 3-node cluster
// ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn fresh_bootstrap_3_nodes() {
    let cluster_id = ClusterId::new();
    let voters = vec![voter(1, 8001), voter(2, 8002), voter(3, 8003)];

    // Bootstrap all three nodes
    for &id in &[1u64, 2, 3] {
        let mut node = make_fresh_node(NodeId(id)).await;
        node.bootstrap(cluster_id, voters.clone()).await.unwrap();

        let state = node.read().unwrap();
        assert_eq!(state.role, Role::Follower);
        assert_eq!(state.current_term, Term::ZERO);
        assert_eq!(state.voter_set.len(), 3);
        assert_eq!(state.log_end_offset, 0);
        assert_eq!(state.high_watermark, 0);
        assert!(node.is_bootstrapped());
    }
}

#[tokio::test]
async fn fresh_bootstrap_cluster_id_matches() {
    let cluster_id = ClusterId::new();
    let voters = vec![voter(1, 8001), voter(2, 8002)];

    let mut node = make_fresh_node(NodeId(1)).await;
    node.bootstrap(cluster_id, voters).await.unwrap();

    assert_eq!(node.cluster_id(), cluster_id);
}

#[tokio::test]
async fn bootstrap_does_not_append_log_entries() {
    let cluster_id = ClusterId::new();
    let voters = vec![voter(1, 8001)];

    let log_store = MockLogStore::new();

    let mut node = RaftNode::new(
        config_for(NodeId(1)),
        Box::new(log_store),
        Box::new(MockQuorumStateStore::new()),
        Box::new(MockSnapshotIO::new()),
        Box::new(MockTransportSender::new()),
        Box::new(MockTransportReceiver),
        Box::new(MockClock::new()),
        MockStateMachine,
        MockListener,
    )
    .await
    .unwrap();

    node.bootstrap(cluster_id, voters).await.unwrap();

    // Log must remain empty — no entries appended during bootstrap
    assert_eq!(node.read().unwrap().log_end_offset, 0);
}

#[tokio::test]
async fn bootstrap_does_not_save_quorum_state() {
    let cluster_id = ClusterId::new();
    let voters = vec![voter(1, 8001)];

    let qs_store = MockQuorumStateStore::new();

    let mut node = RaftNode::new(
        config_for(NodeId(1)),
        Box::new(MockLogStore::new()),
        Box::new(qs_store),
        Box::new(MockSnapshotIO::new()),
        Box::new(MockTransportSender::new()),
        Box::new(MockTransportReceiver),
        Box::new(MockClock::new()),
        MockStateMachine,
        MockListener,
    )
    .await
    .unwrap();

    node.bootstrap(cluster_id, voters).await.unwrap();

    // Quorum state must not have been saved during bootstrap
    // (it's only saved when the node first votes during the election)
    let loaded = node.quorum_state_store.load().await.unwrap();
    assert!(loaded.is_none(), "quorum state should not be persisted during bootstrap");
}

// ───────────────────────────────────────────────────────────────
// Double bootstrap rejection
// ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn double_bootstrap_rejected() {
    let cluster_id = ClusterId::new();
    let voters = vec![voter(1, 8001), voter(2, 8002)];

    let mut node = make_fresh_node(NodeId(1)).await;
    node.bootstrap(cluster_id, voters.clone()).await.unwrap();

    // Second call must fail
    let err = node.bootstrap(cluster_id, voters).await.unwrap_err();
    assert!(
        matches!(err, XraftError::AlreadyBootstrapped { .. }),
        "expected AlreadyBootstrapped, got: {err:?}"
    );
}

// ───────────────────────────────────────────────────────────────
// Existing data: new() calls recover() and bootstrap() rejects
// ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn new_with_existing_log_recovers_to_follower() {
    let cluster_id = ClusterId::new();
    let voters = vec![voter(1, 8001)];

    // Create a node with existing log data — new() should call recover()
    let node = RaftNode::new(
        config_for(NodeId(1)),
        Box::new(MockLogStore::with_end_offset(1)),
        Box::new(MockQuorumStateStore::new()),
        Box::new(MockSnapshotIO::new()),
        Box::new(MockTransportSender::new()),
        Box::new(MockTransportReceiver),
        Box::new(MockClock::new()),
        MockStateMachine,
        MockListener,
    )
    .await
    .expect("new() with existing data should recover, not fail");

    let state = node.read().unwrap();
    assert_eq!(state.role, Role::Follower, "recovered node should be Follower");
    assert!(node.is_bootstrapped(), "recovered node should be marked bootstrapped");

    // bootstrap() should be rejected on a recovered node
    let mut node = node;
    let err = node.bootstrap(cluster_id, voters).await.unwrap_err();
    assert!(
        matches!(err, XraftError::AlreadyBootstrapped { .. }),
        "expected AlreadyBootstrapped after recovery, got: {err:?}"
    );
}

#[tokio::test]
async fn new_with_existing_quorum_state_recovers() {
    let qs = QuorumState {
        current_term: Term(3),
        voted_for: Some(NodeId(2)),
        leader_id: None,
        leader_epoch: Term(0),
        cluster_id: ClusterId(uuid::Uuid::nil()),
    };

    let node = RaftNode::new(
        config_for(NodeId(1)),
        Box::new(MockLogStore::new()),
        Box::new(MockQuorumStateStore::with_state(qs.clone())),
        Box::new(MockSnapshotIO::new()),
        Box::new(MockTransportSender::new()),
        Box::new(MockTransportReceiver),
        Box::new(MockClock::new()),
        MockStateMachine,
        MockListener,
    )
    .await
    .expect("new() with existing quorum state should recover");

    let state = node.read().unwrap();
    assert_eq!(state.role, Role::Follower);
    assert_eq!(state.current_term, Term(3));
    assert!(node.is_bootstrapped());
}

#[tokio::test]
async fn new_with_existing_snapshot_recovers() {
    let snap = Snapshot {
        metadata: SnapshotMetadata {
            last_included_offset: 50,
            last_included_term: Term(2),
            voters: vec![voter(1, 8001), voter(2, 8002)],
            leader_epoch: Term(2),
        },
        app_snapshot: AppSnapshot { data: vec![1, 2, 3] },
    };

    let node = RaftNode::new(
        config_for(NodeId(1)),
        Box::new(MockLogStore::new()),
        Box::new(MockQuorumStateStore::new()),
        Box::new(MockSnapshotIO::with_snapshot(snap)),
        Box::new(MockTransportSender::new()),
        Box::new(MockTransportReceiver),
        Box::new(MockClock::new()),
        MockStateMachine,
        MockListener,
    )
    .await
    .expect("new() with existing snapshot should recover");

    let state = node.read().unwrap();
    assert_eq!(state.role, Role::Follower);
    assert_eq!(state.voter_set.len(), 2);
    assert!(node.is_bootstrapped());
}

// ───────────────────────────────────────────────────────────────
// Bootstrap guard: bootstrap() directly rejects all three sources
// ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn bootstrap_rejects_existing_log_directly() {
    let cluster_id = ClusterId::new();
    let voters = vec![voter(1, 8001)];

    // Create a fresh node, then swap in a log with data to test bootstrap guard
    let mut node = make_fresh_node(NodeId(1)).await;
    node.log_store = Box::new(MockLogStore::with_end_offset(1));

    let err = node.bootstrap(cluster_id, voters).await.unwrap_err();
    assert!(
        matches!(err, XraftError::AlreadyBootstrapped { .. }),
        "expected AlreadyBootstrapped for non-empty log, got: {err:?}"
    );
}

#[tokio::test]
async fn bootstrap_rejects_existing_quorum_state_directly() {
    let cluster_id = ClusterId::new();
    let voters = vec![voter(1, 8001)];

    let qs = QuorumState {
        current_term: Term(1),
        voted_for: Some(NodeId(1)),
        leader_id: None,
        leader_epoch: Term(0),
        cluster_id: ClusterId(uuid::Uuid::nil()),
    };

    let mut node = make_fresh_node(NodeId(1)).await;
    node.quorum_state_store = Box::new(MockQuorumStateStore::with_state(qs));

    let err = node.bootstrap(cluster_id, voters).await.unwrap_err();
    assert!(
        matches!(err, XraftError::AlreadyBootstrapped { .. }),
        "expected AlreadyBootstrapped for existing quorum state, got: {err:?}"
    );
}

#[tokio::test]
async fn bootstrap_rejects_existing_snapshot_directly() {
    let cluster_id = ClusterId::new();
    let voters = vec![voter(1, 8001)];

    let snap = Snapshot {
        metadata: SnapshotMetadata {
            last_included_offset: 100,
            last_included_term: Term(1),
            voters: vec![voter(1, 8001)],
            leader_epoch: Term(1),
        },
        app_snapshot: AppSnapshot { data: vec![42] },
    };

    let mut node = make_fresh_node(NodeId(1)).await;
    node.snapshot_io = Box::new(MockSnapshotIO::with_snapshot(snap));

    let err = node.bootstrap(cluster_id, voters).await.unwrap_err();
    assert!(
        matches!(err, XraftError::AlreadyBootstrapped { .. }),
        "expected AlreadyBootstrapped for existing snapshot, got: {err:?}"
    );
}

// ───────────────────────────────────────────────────────────────
// Input validation
// ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn bootstrap_rejects_nil_cluster_id() {
    let nil_id = ClusterId(uuid::Uuid::nil());
    let voters = vec![voter(1, 8001)];

    let mut node = make_fresh_node(NodeId(1)).await;
    let err = node.bootstrap(nil_id, voters).await.unwrap_err();
    assert!(
        matches!(err, XraftError::InvalidBootstrapConfig { .. }),
        "expected InvalidBootstrapConfig for nil cluster_id, got: {err:?}"
    );
}

#[tokio::test]
async fn bootstrap_rejects_empty_voters() {
    let cluster_id = ClusterId::new();

    let mut node = make_fresh_node(NodeId(1)).await;
    let err = node.bootstrap(cluster_id, vec![]).await.unwrap_err();
    assert!(
        matches!(err, XraftError::InvalidBootstrapConfig { .. }),
        "expected InvalidBootstrapConfig for empty voters, got: {err:?}"
    );
}

#[tokio::test]
async fn bootstrap_rejects_missing_self() {
    let cluster_id = ClusterId::new();
    // Node 1 trying to bootstrap with voters [2, 3]
    let voters = vec![voter(2, 8002), voter(3, 8003)];

    let mut node = make_fresh_node(NodeId(1)).await;
    let err = node.bootstrap(cluster_id, voters).await.unwrap_err();
    assert!(
        matches!(err, XraftError::InvalidBootstrapConfig { .. }),
        "expected InvalidBootstrapConfig for missing self, got: {err:?}"
    );
}

#[tokio::test]
async fn bootstrap_rejects_duplicate_voters() {
    let cluster_id = ClusterId::new();
    let voters = vec![voter(1, 8001), voter(1, 8002)]; // duplicate node_id=1

    let mut node = make_fresh_node(NodeId(1)).await;
    let err = node.bootstrap(cluster_id, voters).await.unwrap_err();
    assert!(
        matches!(err, XraftError::InvalidBootstrapConfig { .. }),
        "expected InvalidBootstrapConfig for duplicate voters, got: {err:?}"
    );
}

// ───────────────────────────────────────────────────────────────
// Single-node bootstrap
// ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn single_node_bootstrap_transitions_to_follower() {
    let cluster_id = ClusterId::new();
    let voters = vec![voter(1, 8001)];

    let mut node = make_fresh_node(NodeId(1)).await;
    node.bootstrap(cluster_id, voters.clone()).await.unwrap();

    let state = node.read().unwrap();
    assert_eq!(state.role, Role::Follower);
    assert_eq!(state.current_term, Term::ZERO);
    assert_eq!(state.voter_set.len(), 1);
    assert_eq!(state.voter_set[0].node_id, NodeId(1));
    assert_eq!(state.log_end_offset, 0);
    assert!(node.is_bootstrapped());
}

#[tokio::test]
async fn single_node_bootstrap_full_election_flow() {
    let cluster_id = ClusterId::new();
    let voters = vec![voter(1, 8001)];

    let mut node = make_fresh_node(NodeId(1)).await;
    node.bootstrap(cluster_id, voters.clone()).await.unwrap();

    // Before election: Follower, term 0, empty log, no quorum-state persisted
    assert_eq!(node.read().unwrap().role, Role::Follower);
    assert_eq!(node.read().unwrap().current_term, Term::ZERO);
    let qs_before = node.quorum_state_store.load().await.unwrap();
    assert!(qs_before.is_none(), "quorum state should not be persisted before election");

    // Simulate election timeout expiry
    node.handle_election_timeout().await.unwrap();

    // After election: Leader, term 1
    let state = node.read().unwrap();
    assert_eq!(state.role, Role::Leader);
    assert_eq!(state.current_term, Term(1));
    assert_eq!(state.leader_id, Some(NodeId(1)));

    // Quorum-state must now be persisted (fsync-before-ack during self-vote)
    let qs_after = node.quorum_state_store.load().await.unwrap();
    assert!(qs_after.is_some(), "quorum state must be persisted during election");
    let qs = qs_after.unwrap();
    assert_eq!(qs.current_term, Term(1));
    assert_eq!(qs.voted_for, Some(NodeId(1)));

    // Log must contain LeaderChangeMessage + VotersRecord (2 entries)
    assert_eq!(state.log_end_offset, 2);

    // Verify entry types
    let entry0 = node.log_store.entry_at(0).await.unwrap().unwrap();
    assert_eq!(entry0.entry_type, xraft_core::log_entry::EntryType::LeaderChangeMessage);
    assert_eq!(entry0.term, Term(1));

    let entry1 = node.log_store.entry_at(1).await.unwrap().unwrap();
    assert_eq!(entry1.entry_type, xraft_core::log_entry::EntryType::VotersRecord);
    assert_eq!(entry1.term, Term(1));
}

#[tokio::test]
async fn multi_node_election_timeout_becomes_candidate() {
    let cluster_id = ClusterId::new();
    let voters = vec![voter(1, 8001), voter(2, 8002), voter(3, 8003)];

    let mut node = make_fresh_node(NodeId(1)).await;
    node.bootstrap(cluster_id, voters).await.unwrap();

    // Simulate election timeout — with 3 nodes, 1 self-vote is not a quorum
    node.handle_election_timeout().await.unwrap();

    let state = node.read().unwrap();
    assert_eq!(state.role, Role::Candidate);
    assert_eq!(state.current_term, Term(1));
    // Quorum-state persisted even though election not yet won
    let qs = node.quorum_state_store.load().await.unwrap().unwrap();
    assert_eq!(qs.current_term, Term(1));
    assert_eq!(qs.voted_for, Some(NodeId(1)));
    // Log must remain empty — no entries until election is won
    assert_eq!(state.log_end_offset, 0);
}

#[tokio::test]
async fn election_timeout_from_unattached_rejected() {
    let node = make_fresh_node(NodeId(1)).await;
    // Node is Unattached, has not bootstrapped
    let mut node = node;
    let err = node.handle_election_timeout().await.unwrap_err();
    assert!(
        matches!(err, XraftError::InvalidElectionState { .. }),
        "expected InvalidElectionState, got: {err:?}"
    );
}

#[tokio::test]
async fn bootstrap_sets_election_deadline_in_future() {
    let cluster_id = ClusterId::new();
    let voters = vec![voter(1, 8001)];

    let mut node = make_fresh_node(NodeId(1)).await;
    let before = std::time::Instant::now();
    node.bootstrap(cluster_id, voters).await.unwrap();

    // Election deadline must be set beyond current time
    assert!(
        node.election_deadline() >= before,
        "election deadline must be set after bootstrap"
    );
}

#[tokio::test]
async fn bootstrap_sets_no_leader() {
    let cluster_id = ClusterId::new();
    let voters = vec![voter(1, 8001), voter(2, 8002), voter(3, 8003)];

    let mut node = make_fresh_node(NodeId(1)).await;
    node.bootstrap(cluster_id, voters).await.unwrap();

    let state = node.read().unwrap();
    assert!(state.leader_id.is_none(), "no leader should be set after bootstrap");
}

// ───────────────────────────────────────────────────────────────
// Constructor — Unattached state
// ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn new_node_starts_unattached() {
    let node = make_fresh_node(NodeId(1)).await;
    let state = node.read().unwrap();
    assert_eq!(state.role, Role::Unattached);
    assert_eq!(state.current_term, Term::ZERO);
    assert!(state.voter_set.is_empty());
    assert!(!node.is_bootstrapped());
}

#[tokio::test]
async fn new_node_with_existing_log_recovers() {
    let node = RaftNode::new(
        config_for(NodeId(1)),
        Box::new(MockLogStore::with_end_offset(5)),
        Box::new(MockQuorumStateStore::new()),
        Box::new(MockSnapshotIO::new()),
        Box::new(MockTransportSender::new()),
        Box::new(MockTransportReceiver),
        Box::new(MockClock::new()),
        MockStateMachine,
        MockListener,
    )
    .await
    .expect("new() with existing log should recover");

    let state = node.read().unwrap();
    assert_eq!(state.role, Role::Follower);
    assert!(node.is_bootstrapped());
}

// ───────────────────────────────────────────────────────────────
// 3-node cluster shared ClusterId consistency
// ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn fresh_bootstrap_3_nodes_share_cluster_id() {
    let cluster_id = ClusterId::new();
    let voters = vec![voter(1, 8001), voter(2, 8002), voter(3, 8003)];

    let mut nodes = Vec::new();
    for &id in &[1u64, 2, 3] {
        let mut node = make_fresh_node(NodeId(id)).await;
        node.bootstrap(cluster_id, voters.clone()).await.unwrap();
        nodes.push(node);
    }

    // All three nodes must share the exact same ClusterId
    for node in &nodes {
        assert_eq!(node.cluster_id(), cluster_id);
    }

    // All three nodes must have the same in-memory voter set
    for node in &nodes {
        assert_eq!(node.voter_set().len(), 3);
        let ids: Vec<NodeId> = node.voter_set().iter().map(|v| v.node_id).collect();
        assert!(ids.contains(&NodeId(1)));
        assert!(ids.contains(&NodeId(2)));
        assert!(ids.contains(&NodeId(3)));
    }
}

// ───────────────────────────────────────────────────────────────
// VotersRecord payload is correctly deserializable after election
// ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn single_node_voters_record_payload_roundtrips() {
    let cluster_id = ClusterId::new();
    let voters = vec![voter(1, 8001)];

    let mut node = make_fresh_node(NodeId(1)).await;
    node.bootstrap(cluster_id, voters.clone()).await.unwrap();
    node.handle_election_timeout().await.unwrap();

    // Read the VotersRecord entry (offset 1) and deserialize its payload
    let entry = node.log_store.entry_at(1).await.unwrap().unwrap();
    assert_eq!(entry.entry_type, xraft_core::log_entry::EntryType::VotersRecord);

    let record: VotersRecord = bincode::deserialize(&entry.payload).unwrap();
    assert_eq!(record.version, 1);
    assert_eq!(record.voters.len(), 1);
    assert_eq!(record.voters[0].node_id, NodeId(1));
}

// ───────────────────────────────────────────────────────────────
// Single-node HW advancement after becoming leader
// ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn single_node_leader_advances_high_watermark() {
    let cluster_id = ClusterId::new();
    let voters = vec![voter(1, 8001)];

    let mut node = make_fresh_node(NodeId(1)).await;
    node.bootstrap(cluster_id, voters).await.unwrap();
    node.handle_election_timeout().await.unwrap();

    let state = node.read().unwrap();
    assert_eq!(state.role, Role::Leader);
    // HW must be advanced to log_end_offset for single-node clusters
    assert_eq!(state.high_watermark, state.log_end_offset);
    assert_eq!(state.high_watermark, 2); // LCM + VotersRecord
}

// ───────────────────────────────────────────────────────────────
// node_id() and voter_set() accessors
// ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn node_id_accessor() {
    let node = make_fresh_node(NodeId(42)).await;
    assert_eq!(node.node_id(), NodeId(42));
}

#[tokio::test]
async fn voter_set_accessor_empty_before_bootstrap() {
    let node = make_fresh_node(NodeId(1)).await;
    assert!(node.voter_set().is_empty());
}

#[tokio::test]
async fn voter_set_accessor_populated_after_bootstrap() {
    let cluster_id = ClusterId::new();
    let voters = vec![voter(1, 8001), voter(2, 8002)];

    let mut node = make_fresh_node(NodeId(1)).await;
    node.bootstrap(cluster_id, voters.clone()).await.unwrap();

    assert_eq!(node.voter_set().len(), 2);
    assert_eq!(node.voter_set()[0].node_id, NodeId(1));
    assert_eq!(node.voter_set()[1].node_id, NodeId(2));
}

// ───────────────────────────────────────────────────────────────
// Timer-driven election via tick() — exercises the real timeout path
// ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn single_node_tick_driven_election() {
    let cluster_id = ClusterId::new();
    let voters = vec![voter(1, 8001)];

    let (shared_clock, clock) = SharedMockClock::new();
    clock.set_election_timeout(Duration::from_millis(200));

    let mut node = RaftNode::new(
        config_for(NodeId(1)),
        Box::new(MockLogStore::new()),
        Box::new(MockQuorumStateStore::new()),
        Box::new(MockSnapshotIO::new()),
        Box::new(MockTransportSender::new()),
        Box::new(MockTransportReceiver),
        Box::new(shared_clock),
        MockStateMachine,
        MockListener,
    )
    .await
    .unwrap();

    node.bootstrap(cluster_id, voters).await.unwrap();
    assert_eq!(node.read().unwrap().role, Role::Follower);

    // tick() before deadline — should remain Follower
    clock.advance(Duration::from_millis(100));
    node.tick().await.unwrap();
    assert_eq!(node.read().unwrap().role, Role::Follower);

    // tick() at/past deadline — election fires, single-node wins immediately
    clock.advance(Duration::from_millis(150));
    node.tick().await.unwrap();

    let state = node.read().unwrap();
    assert_eq!(state.role, Role::Leader);
    assert_eq!(state.current_term, Term(1));
    assert_eq!(state.log_end_offset, 2); // LCM + VotersRecord

    // Quorum-state persisted during the self-vote
    let qs = node.quorum_state_store.load().await.unwrap().unwrap();
    assert_eq!(qs.current_term, Term(1));
    assert_eq!(qs.voted_for, Some(NodeId(1)));
}

#[tokio::test]
async fn single_node_poll_driven_election() {
    let cluster_id = ClusterId::new();
    let voters = vec![voter(1, 8001)];

    let (shared_clock, clock) = SharedMockClock::new();
    clock.set_election_timeout(Duration::from_millis(300));

    let mut node = RaftNode::new(
        config_for(NodeId(1)),
        Box::new(MockLogStore::new()),
        Box::new(MockQuorumStateStore::new()),
        Box::new(MockSnapshotIO::new()),
        Box::new(MockTransportSender::new()),
        Box::new(MockTransportReceiver),
        Box::new(shared_clock),
        MockStateMachine,
        MockListener,
    )
    .await
    .unwrap();

    node.bootstrap(cluster_id, voters).await.unwrap();
    assert_eq!(node.read().unwrap().role, Role::Follower);

    // poll() sleeps until the election deadline, then fires the timeout
    node.poll().await.unwrap();

    let state = node.read().unwrap();
    assert_eq!(state.role, Role::Leader);
    assert_eq!(state.current_term, Term(1));
    assert_eq!(state.log_end_offset, 2);
}

#[tokio::test]
async fn multi_node_tick_driven_becomes_candidate() {
    let cluster_id = ClusterId::new();
    let voters = vec![voter(1, 8001), voter(2, 8002), voter(3, 8003)];

    let (shared_clock, clock) = SharedMockClock::new();
    clock.set_election_timeout(Duration::from_millis(200));

    let mut node = RaftNode::new(
        config_for(NodeId(1)),
        Box::new(MockLogStore::new()),
        Box::new(MockQuorumStateStore::new()),
        Box::new(MockSnapshotIO::new()),
        Box::new(MockTransportSender::new()),
        Box::new(MockTransportReceiver),
        Box::new(shared_clock),
        MockStateMachine,
        MockListener,
    )
    .await
    .unwrap();

    node.bootstrap(cluster_id, voters).await.unwrap();

    // Advance past the election deadline
    clock.advance(Duration::from_millis(250));
    node.tick().await.unwrap();

    // 3-node cluster: 1 self-vote is not a quorum, stays Candidate
    let state = node.read().unwrap();
    assert_eq!(state.role, Role::Candidate);
    assert_eq!(state.current_term, Term(1));
    assert_eq!(state.log_end_offset, 0); // no entries until election is won
}
