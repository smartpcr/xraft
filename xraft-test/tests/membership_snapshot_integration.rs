use std::net::SocketAddr;

use xraft_core::consensus_state::Role;
use xraft_core::log_entry::{EntryType, LogEntry};
use xraft_core::membership::MembershipManager;
use xraft_core::node_state::NodeState;
use xraft_core::rpc::{AddVoterRequest, RemoveVoterRequest, UpdateVoterRequest};
use xraft_core::snapshot::SnapshotMetadata;
use xraft_core::snapshot_coordinator::SnapshotCoordinator;
use xraft_core::traits::LogStore;
use xraft_core::types::{ClusterId, NodeId, Term, VoterInfo, VotersRecord};

use xraft_test::{MemoryLogStore, MemorySnapshotStore, MemoryStateMachine};

fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

fn make_voter(id: u64, port: u16) -> VoterInfo {
    VoterInfo {
        node_id: NodeId(id),
        endpoint: addr(port),
    }
}

/// Protocol-level bootstrap: bootstrap → win election → append
/// LeaderChangeMessage + initial VotersRecord → advance HW.
///
/// This mirrors the architecture §5.9 flow: the initial VotersRecord is NOT
/// written during bootstrap; it is appended by the leader after winning the
/// first election.
async fn bootstrap_leader(
    log_store: &MemoryLogStore,
) -> (NodeState, Vec<VoterInfo>) {
    let initial_voters = vec![
        make_voter(1, 5001),
        make_voter(2, 5002),
        make_voter(3, 5003),
    ];
    let mut state = NodeState::new(NodeId(1), ClusterId::new());
    // Phase 1: bootstrap → Follower (§5.9)
    state.bootstrap(initial_voters.clone());
    assert_eq!(state.role, Role::Follower);

    // Phase 2: win election → Leader
    state.become_leader();
    assert_eq!(state.role, Role::Leader);
    assert_eq!(state.current_term, Term(1));

    // Phase 3: leader appends LeaderChangeMessage (first entry of new term)
    let lcm_entry = LogEntry {
        offset: state.log_end_offset,
        term: state.current_term,
        entry_type: EntryType::LeaderChangeMessage,
        payload: Vec::new(),
    };
    log_store.append(&[lcm_entry]).await.unwrap();
    state.log_end_offset += 1;

    // Phase 4: leader appends initial VotersRecord
    let record = VotersRecord {
        version: 1,
        voters: initial_voters.clone(),
    };
    let payload = bincode::serialize(&record).unwrap();
    let vr_entry = LogEntry {
        offset: state.log_end_offset,
        term: state.current_term,
        entry_type: EntryType::VotersRecord,
        payload,
    };
    log_store.append(&[vr_entry]).await.unwrap();
    let vr_offset = state.log_end_offset;
    state.log_end_offset += 1;
    state.pending_membership_change = Some(
        xraft_core::node_state::PendingMembershipChange {
            offset: vr_offset,
            voters: initial_voters.clone(),
        },
    );

    // Phase 5: quorum replication → HW advances, VotersRecord committed
    let entries = log_store
        .read(state.high_watermark, state.log_end_offset)
        .await
        .unwrap();
    state.advance_high_watermark(state.log_end_offset, &entries).unwrap();
    assert!(state.pending_membership_change.is_none());
    assert_eq!(state.voter_set, initial_voters);

    (state, initial_voters)
}

/// Helper: add a voter via the leader's protocol path and commit via HW advance.
async fn add_voter_and_commit(
    state: &mut NodeState,
    log_store: &MemoryLogStore,
    node_id: u64,
    port: u16,
) {
    let resp = MembershipManager::handle_add_voter(
        state,
        log_store,
        AddVoterRequest {
            node_id: NodeId(node_id),
            endpoint: addr(port),
        },
    )
    .await
    .unwrap();
    assert!(resp.success);

    // Simulate quorum replication → leader advances HW
    let entries = log_store
        .read(state.high_watermark, state.log_end_offset)
        .await
        .unwrap();
    state.advance_high_watermark(state.log_end_offset, &entries).unwrap();
    assert!(state.pending_membership_change.is_none());
}

/// Helper: remove a voter via the leader's protocol path and commit via HW advance.
async fn remove_voter_and_commit(
    state: &mut NodeState,
    log_store: &MemoryLogStore,
    node_id: u64,
) {
    let resp = MembershipManager::handle_remove_voter(
        state,
        log_store,
        RemoveVoterRequest {
            node_id: NodeId(node_id),
        },
    )
    .await
    .unwrap();
    assert!(resp.success);

    let entries = log_store
        .read(state.high_watermark, state.log_end_offset)
        .await
        .unwrap();
    state.advance_high_watermark(state.log_end_offset, &entries).unwrap();
    assert!(state.pending_membership_change.is_none());
}

// =========================================================================
// Scenario: Update voter endpoint
// =========================================================================

#[tokio::test]
async fn test_update_voter_endpoint() {
    let log_store = MemoryLogStore::new();
    let (mut state, _) = bootstrap_leader(&log_store).await;

    assert_eq!(
        MembershipManager::find_voter_endpoint(&state.voter_set, NodeId(2)),
        Some(addr(5002))
    );

    // Update N2's endpoint to 127.0.0.1:6002
    let resp = MembershipManager::handle_update_voter(
        &mut state,
        &log_store,
        UpdateVoterRequest {
            node_id: NodeId(2),
            new_endpoint: addr(6002),
        },
    )
    .await
    .unwrap();

    assert!(resp.success);
    assert!(resp.error.is_none());

    // Pending change should exist — NOT yet committed
    assert!(state.pending_membership_change.is_some());

    // Committed voter_set unchanged (still old endpoint)
    assert_eq!(
        MembershipManager::find_voter_endpoint(&state.voter_set, NodeId(2)),
        Some(addr(5002))
    );

    // A VotersRecord entry was appended to the log
    let vr_offset = state.pending_membership_change.as_ref().unwrap().offset;
    let entries = log_store.read(vr_offset, vr_offset + 1).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].entry_type, EntryType::VotersRecord);

    // Simulate quorum replication: advance HW past the VotersRecord
    let committed = log_store
        .read(state.high_watermark, state.log_end_offset)
        .await
        .unwrap();
    state
        .advance_high_watermark(state.log_end_offset, &committed)
        .unwrap();

    // NOW N2's endpoint is committed to 6002
    assert_eq!(
        MembershipManager::find_voter_endpoint(&state.voter_set, NodeId(2)),
        Some(addr(6002))
    );
    assert!(state.pending_membership_change.is_none());
}

// =========================================================================
// Scenario: Update voter — not leader
// =========================================================================

#[tokio::test]
async fn test_update_voter_not_leader() {
    let log_store = MemoryLogStore::new();
    let (mut state, _) = bootstrap_leader(&log_store).await;
    state.role = Role::Follower;

    let resp = MembershipManager::handle_update_voter(
        &mut state,
        &log_store,
        UpdateVoterRequest {
            node_id: NodeId(2),
            new_endpoint: addr(6002),
        },
    )
    .await
    .unwrap();

    assert!(!resp.success);
    assert!(matches!(
        resp.error,
        Some(xraft_core::rpc::MembershipError::NotLeader { .. })
    ));
}

// =========================================================================
// Scenario: Update voter — node not found
// =========================================================================

#[tokio::test]
async fn test_update_voter_node_not_found() {
    let log_store = MemoryLogStore::new();
    let (mut state, _) = bootstrap_leader(&log_store).await;

    let resp = MembershipManager::handle_update_voter(
        &mut state,
        &log_store,
        UpdateVoterRequest {
            node_id: NodeId(99),
            new_endpoint: addr(6099),
        },
    )
    .await
    .unwrap();

    assert!(!resp.success);
    assert!(matches!(
        resp.error,
        Some(xraft_core::rpc::MembershipError::NodeNotFound)
    ));
}

// =========================================================================
// Scenario: Update voter — change in progress
// =========================================================================

#[tokio::test]
async fn test_update_voter_change_in_progress() {
    let log_store = MemoryLogStore::new();
    let (mut state, _) = bootstrap_leader(&log_store).await;

    let _ = MembershipManager::handle_update_voter(
        &mut state,
        &log_store,
        UpdateVoterRequest {
            node_id: NodeId(2),
            new_endpoint: addr(6002),
        },
    )
    .await
    .unwrap();

    // Second update while first is pending should fail
    let resp = MembershipManager::handle_update_voter(
        &mut state,
        &log_store,
        UpdateVoterRequest {
            node_id: NodeId(3),
            new_endpoint: addr(6003),
        },
    )
    .await
    .unwrap();

    assert!(!resp.success);
    assert!(matches!(
        resp.error,
        Some(xraft_core::rpc::MembershipError::ChangeInProgress)
    ));
}

// =========================================================================
// Scenario: Voter set in snapshot metadata
// =========================================================================

#[tokio::test]
async fn test_voter_set_in_snapshot_metadata() {
    let log_store = MemoryLogStore::new();
    let (mut state, _) = bootstrap_leader(&log_store).await;
    let snapshot_io = MemorySnapshotStore::new();
    let state_machine = MemoryStateMachine::new();

    // Add N4 and N5 via protocol-level flow
    add_voter_and_commit(&mut state, &log_store, 4, 5004).await;
    add_voter_and_commit(&mut state, &log_store, 5, 5005).await;

    assert_eq!(state.voter_set.len(), 5);

    // Take snapshot
    let snapshot = SnapshotCoordinator::create_snapshot(&state, &state_machine, &snapshot_io, &log_store)
        .await
        .unwrap();

    // Snapshot metadata contains all 5 committed voters
    assert_eq!(snapshot.metadata.voters.len(), 5);
    let voter_ids: Vec<u64> = snapshot
        .metadata
        .voters
        .iter()
        .map(|v| v.node_id.0)
        .collect();
    assert!(voter_ids.contains(&1));
    assert!(voter_ids.contains(&2));
    assert!(voter_ids.contains(&3));
    assert!(voter_ids.contains(&4));
    assert!(voter_ids.contains(&5));
}

// =========================================================================
// Scenario: Recover voter set from snapshot (clean — no log tail)
// =========================================================================

#[tokio::test]
async fn test_recover_voter_set_from_snapshot() {
    let log_store = MemoryLogStore::new();
    let (mut state, _) = bootstrap_leader(&log_store).await;
    let snapshot_io = MemorySnapshotStore::new();
    let state_machine = MemoryStateMachine::new();

    add_voter_and_commit(&mut state, &log_store, 4, 5004).await;
    add_voter_and_commit(&mut state, &log_store, 5, 5005).await;

    // Snapshot with 5 voters
    let _ = SnapshotCoordinator::create_snapshot(&state, &state_machine, &snapshot_io, &log_store)
        .await
        .unwrap();

    // Simulate restart: fresh state, truncated log (only snapshot survives)
    let mut recovered_state = NodeState::new(NodeId(1), state.cluster_id);
    let mut recovered_sm = MemoryStateMachine::new();
    log_store
        .truncate_prefix(state.high_watermark)
        .await
        .unwrap();

    let recovered = SnapshotCoordinator::recover_from_snapshot(
        &mut recovered_state,
        &mut recovered_sm,
        &snapshot_io,
        &log_store,
    )
    .await
    .unwrap();

    assert!(recovered);
    // Committed voter set restored from snapshot
    assert_eq!(recovered_state.voter_set.len(), 5);
    // HW = snapshot.last_included_offset + 1
    assert_eq!(recovered_state.high_watermark, state.high_watermark);
    // No pending changes (log was truncated)
    assert!(recovered_state.pending_membership_change.is_none());
    // Recovered as Follower
    assert_eq!(recovered_state.role, Role::Follower);

    // Verify consistency
    let consistent = SnapshotCoordinator::verify_voter_set_consistency(
        &recovered_state,
        &log_store,
        &snapshot_io,
    )
    .await
    .unwrap();
    assert!(consistent);
}

// =========================================================================
// Scenario: Full lifecycle — bootstrap → add → snapshot → remove →
//           snapshot → recover from second snapshot
// =========================================================================

#[tokio::test]
async fn test_full_membership_snapshot_lifecycle() {
    let log_store = MemoryLogStore::new();
    let (mut state, _) = bootstrap_leader(&log_store).await;
    let snapshot_io = MemorySnapshotStore::new();
    let state_machine = MemoryStateMachine::new();

    // --- Phase 1: Add N4 (committed) ---
    add_voter_and_commit(&mut state, &log_store, 4, 5004).await;
    assert_eq!(state.voter_set.len(), 4);

    // --- Phase 2: First snapshot (voters = {N1,N2,N3,N4}) ---
    let snap1 = SnapshotCoordinator::create_snapshot(&state, &state_machine, &snapshot_io, &log_store)
        .await
        .unwrap();
    assert_eq!(snap1.metadata.voters.len(), 4);

    // --- Phase 3: Remove N3 (committed) ---
    remove_voter_and_commit(&mut state, &log_store, 3).await;
    assert_eq!(state.voter_set.len(), 3);
    assert!(!state.voter_set.iter().any(|v| v.node_id == NodeId(3)));

    // --- Phase 4: Second snapshot (voters = {N1,N2,N4}) ---
    let snap2 = SnapshotCoordinator::create_snapshot(&state, &state_machine, &snapshot_io, &log_store)
        .await
        .unwrap();
    assert_eq!(snap2.metadata.voters.len(), 3);

    // --- Phase 5: Recover from second snapshot ---
    let mut recovered_state = NodeState::new(NodeId(1), state.cluster_id);
    let mut recovered_sm = MemoryStateMachine::new();
    log_store
        .truncate_prefix(state.high_watermark)
        .await
        .unwrap();

    let recovered = SnapshotCoordinator::recover_from_snapshot(
        &mut recovered_state,
        &mut recovered_sm,
        &snapshot_io,
        &log_store,
    )
    .await
    .unwrap();

    assert!(recovered);
    assert_eq!(recovered_state.voter_set.len(), 3);
    let voter_ids: Vec<u64> = recovered_state
        .voter_set
        .iter()
        .map(|v| v.node_id.0)
        .collect();
    assert!(voter_ids.contains(&1));
    assert!(voter_ids.contains(&2));
    assert!(voter_ids.contains(&4));
    assert!(!voter_ids.contains(&3));
    assert!(recovered_state.pending_membership_change.is_none());
    assert_eq!(recovered_state.role, Role::Follower);

    let consistent = SnapshotCoordinator::verify_voter_set_consistency(
        &recovered_state,
        &log_store,
        &snapshot_io,
    )
    .await
    .unwrap();
    assert!(consistent);
}

// =========================================================================
// Scenario: Recovery with uncommitted log tail — VotersRecord stays pending
// =========================================================================

#[tokio::test]
async fn test_recover_with_uncommitted_log_tail() {
    let log_store = MemoryLogStore::new();
    let (mut state, _) = bootstrap_leader(&log_store).await;
    let snapshot_io = MemorySnapshotStore::new();
    let state_machine = MemoryStateMachine::new();

    // Add N4 and commit
    add_voter_and_commit(&mut state, &log_store, 4, 5004).await;

    // Snapshot at this point — committed voters = {N1,N2,N3,N4}
    let _ = SnapshotCoordinator::create_snapshot(&state, &state_machine, &snapshot_io, &log_store)
        .await
        .unwrap();
    let snap_hw = state.high_watermark;

    // Add N5 — appended to log but NOT committed (no HW advance)
    let resp = MembershipManager::handle_add_voter(
        &mut state,
        &log_store,
        AddVoterRequest {
            node_id: NodeId(5),
            endpoint: addr(5005),
        },
    )
    .await
    .unwrap();
    assert!(resp.success);
    // N5 VotersRecord is at log_end_offset - 1, uncommitted
    assert!(state.pending_membership_change.is_some());

    // Simulate crash and recovery — log tail with uncommitted N5 survives
    let mut recovered_state = NodeState::new(NodeId(1), state.cluster_id);
    let mut recovered_sm = MemoryStateMachine::new();

    // Truncate only entries before the snapshot (keep the uncommitted tail)
    log_store.truncate_prefix(snap_hw).await.unwrap();

    let recovered = SnapshotCoordinator::recover_from_snapshot(
        &mut recovered_state,
        &mut recovered_sm,
        &snapshot_io,
        &log_store,
    )
    .await
    .unwrap();

    assert!(recovered);

    // Committed voter_set restored from snapshot — only 4 voters (N5 NOT applied)
    assert_eq!(recovered_state.voter_set.len(), 4);
    assert!(!recovered_state
        .voter_set
        .iter()
        .any(|v| v.node_id == NodeId(5)));

    // HW stays at snapshot level — NOT advanced to log end
    assert_eq!(recovered_state.high_watermark, snap_hw);
    assert!(recovered_state.log_end_offset > recovered_state.high_watermark);

    // N5's VotersRecord is tracked as pending (not committed)
    assert!(recovered_state.pending_membership_change.is_some());
    let pending = recovered_state.pending_membership_change.as_ref().unwrap();
    assert_eq!(pending.voters.len(), 5); // proposed set includes N5
    assert!(pending.voters.iter().any(|v| v.node_id == NodeId(5)));

    // Verify consistency — committed voter_set matches snapshot, pending is tracked
    let consistent = SnapshotCoordinator::verify_voter_set_consistency(
        &recovered_state,
        &log_store,
        &snapshot_io,
    )
    .await
    .unwrap();
    assert!(consistent);

    // Simulate leader confirming commitment via Fetch response (leader HW advances)
    let leader_hw = recovered_state.log_end_offset;
    let entries = log_store
        .read(recovered_state.high_watermark, leader_hw)
        .await
        .unwrap();
    recovered_state.advance_high_watermark(leader_hw, &entries).unwrap();

    // NOW N5 is committed
    assert_eq!(recovered_state.voter_set.len(), 5);
    assert!(recovered_state
        .voter_set
        .iter()
        .any(|v| v.node_id == NodeId(5)));
    assert!(recovered_state.pending_membership_change.is_none());
}

// =========================================================================
// Scenario: Recovery — uncommitted tail truncated by new leader
// =========================================================================

#[tokio::test]
async fn test_recover_uncommitted_tail_can_be_truncated() {
    let log_store = MemoryLogStore::new();
    let (mut state, _) = bootstrap_leader(&log_store).await;
    let snapshot_io = MemorySnapshotStore::new();
    let state_machine = MemoryStateMachine::new();

    add_voter_and_commit(&mut state, &log_store, 4, 5004).await;

    let _ = SnapshotCoordinator::create_snapshot(&state, &state_machine, &snapshot_io, &log_store)
        .await
        .unwrap();
    let snap_hw = state.high_watermark;

    // Append uncommitted N5 VotersRecord
    let _ = MembershipManager::handle_add_voter(
        &mut state,
        &log_store,
        AddVoterRequest {
            node_id: NodeId(5),
            endpoint: addr(5005),
        },
    )
    .await
    .unwrap();

    // Recovery
    let mut recovered_state = NodeState::new(NodeId(1), state.cluster_id);
    let mut recovered_sm = MemoryStateMachine::new();
    log_store.truncate_prefix(snap_hw).await.unwrap();

    let _ = SnapshotCoordinator::recover_from_snapshot(
        &mut recovered_state,
        &mut recovered_sm,
        &snapshot_io,
        &log_store,
    )
    .await
    .unwrap();

    // N5 is pending, not committed
    assert_eq!(recovered_state.voter_set.len(), 4);
    assert!(recovered_state.pending_membership_change.is_some());

    // New leader truncates the uncommitted tail (N5 was from deposed leader)
    log_store
        .truncate_suffix(recovered_state.high_watermark)
        .await
        .unwrap();
    recovered_state.log_end_offset = recovered_state.high_watermark;
    recovered_state.pending_membership_change = None;

    // Committed voter set is still the snapshot's 4-voter set
    assert_eq!(recovered_state.voter_set.len(), 4);
    assert!(recovered_state.pending_membership_change.is_none());
}

// =========================================================================
// Scenario: Snapshot metadata serialization roundtrip
// =========================================================================

#[tokio::test]
async fn test_snapshot_metadata_voters_roundtrip() {
    let voters = vec![
        make_voter(1, 5001),
        make_voter(2, 5002),
        make_voter(10, 6010),
    ];

    let metadata = SnapshotMetadata {
        last_included_offset: 42,
        last_included_term: Term(5),
        voters: voters.clone(),
        leader_epoch: Term(5),
    };

    let encoded = bincode::serialize(&metadata).unwrap();
    let decoded: SnapshotMetadata = bincode::deserialize(&encoded).unwrap();

    assert_eq!(decoded.voters.len(), 3);
    assert_eq!(decoded.voters, voters);
    assert_eq!(decoded.last_included_offset, 42);
}

// =========================================================================
// Scenario: Update voter then snapshot then recover — endpoint persisted
// =========================================================================

#[tokio::test]
async fn test_update_voter_snapshot_recover() {
    let log_store = MemoryLogStore::new();
    let (mut state, _) = bootstrap_leader(&log_store).await;
    let snapshot_io = MemorySnapshotStore::new();
    let state_machine = MemoryStateMachine::new();

    // Update N2's endpoint and commit
    let resp = MembershipManager::handle_update_voter(
        &mut state,
        &log_store,
        UpdateVoterRequest {
            node_id: NodeId(2),
            new_endpoint: addr(6002),
        },
    )
    .await
    .unwrap();
    assert!(resp.success);
    let entries = log_store
        .read(state.high_watermark, state.log_end_offset)
        .await
        .unwrap();
    state.advance_high_watermark(state.log_end_offset, &entries).unwrap();
    assert_eq!(
        MembershipManager::find_voter_endpoint(&state.voter_set, NodeId(2)),
        Some(addr(6002))
    );

    // Snapshot and recover
    let _ = SnapshotCoordinator::create_snapshot(&state, &state_machine, &snapshot_io, &log_store)
        .await
        .unwrap();
    log_store
        .truncate_prefix(state.high_watermark)
        .await
        .unwrap();

    let mut recovered = NodeState::new(NodeId(1), state.cluster_id);
    let mut recovered_sm = MemoryStateMachine::new();
    SnapshotCoordinator::recover_from_snapshot(
        &mut recovered,
        &mut recovered_sm,
        &snapshot_io,
        &log_store,
    )
    .await
    .unwrap();

    // Updated endpoint survived snapshot+recovery
    assert_eq!(
        MembershipManager::find_voter_endpoint(&recovered.voter_set, NodeId(2)),
        Some(addr(6002))
    );
}

// =========================================================================
// Scenario: verify_voter_set_consistency compares against last committed VR
// =========================================================================

#[tokio::test]
async fn test_verify_compares_against_committed_voters_record() {
    let log_store = MemoryLogStore::new();
    let (mut state, _) = bootstrap_leader(&log_store).await;
    let snapshot_io = MemorySnapshotStore::new();
    let state_machine = MemoryStateMachine::new();

    // Snapshot with 3 voters (the initial set)
    let _ = SnapshotCoordinator::create_snapshot(&state, &state_machine, &snapshot_io, &log_store)
        .await
        .unwrap();

    // Add N4 and commit — voter_set now differs from snapshot metadata
    add_voter_and_commit(&mut state, &log_store, 4, 5004).await;
    assert_eq!(state.voter_set.len(), 4);

    // verify_voter_set_consistency should pass: voter_set matches the last
    // committed VotersRecord in the log (the AddVoter record), even though
    // it differs from the snapshot metadata.
    let consistent = SnapshotCoordinator::verify_voter_set_consistency(
        &state, &log_store, &snapshot_io,
    )
    .await
    .unwrap();
    assert!(consistent);
}

// =========================================================================
// Scenario: corrupt VotersRecord in committed log → error propagated
// =========================================================================

#[tokio::test]
async fn test_corrupt_voters_record_errors_on_hw_advance() {
    let log_store = MemoryLogStore::new();
    let (mut state, _) = bootstrap_leader(&log_store).await;

    // Append a VotersRecord with corrupt payload
    let corrupt_entry = LogEntry {
        offset: state.log_end_offset,
        term: state.current_term,
        entry_type: EntryType::VotersRecord,
        payload: vec![0xFF, 0xFE, 0xFD], // not valid bincode
    };
    log_store.append(&[corrupt_entry]).await.unwrap();
    state.log_end_offset += 1;

    // Advancing HW to commit the corrupt entry should return an error
    let entries = log_store
        .read(state.high_watermark, state.log_end_offset)
        .await
        .unwrap();
    let result = state.advance_high_watermark(state.log_end_offset, &entries);
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("failed to deserialize committed VotersRecord"));
}
