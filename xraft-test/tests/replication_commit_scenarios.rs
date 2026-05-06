//! Replication and Commit Scenario Tests (Stage 10.2)
//!
//! Integration tests that validate end-to-end replication behaviour using the
//! full architecture pipeline:
//!
//! **EventLoopDriver** processes inbound messages following the three-phase
//! commit notification sequence (architecture §4.1):
//!   1. Mutate NodeState
//!   2. Invoke callbacks: StateMachine::apply → Listener::handle_commit →
//!      DeferredCompletionQueue::complete
//!   3. Produce IoActionBatch
//!
//! **IoStage** executes IoActionBatch through trait objects:
//!   - `LogStore` (via `MemoryLogStore`) for log persistence
//!   - `QuorumStateStore` (via `MemoryQuorumStateStore`) for quorum state
//!   - `TransportSender` (via `SimulatedTransportSender` + `MessageBus`)
//!     for envelope-based RPC delivery
//!
//! **Additional trait coverage:**
//!   - `StateMachine` (via `TestStateMachine`) for commit notification
//!   - `Listener` (via `TestListener`) for commit batch notification
//!   - `DeferredCompletionQueue` with `tokio::sync::oneshot` for proposal futures
//!   - `Clock` (via `SimulatedClock`) for deterministic time control
//!   - `InvariantChecker` for Raft safety property verification
//!
//! These tests exercise:
//!   1. Full replication of 100 entries with storage and SM verification
//!   2. Two-round commit visibility (HW propagation delay)
//!   3. Leader failure with partial replication — uncommitted tail truncated
//!   4. Log divergence and truncation after leader change
//!   5. Interleaved proposals with HW advancement and oneshot-backed DCQ completion

use xraft_core::*;
use xraft_test::{InvariantChecker, SimulatedCluster};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_record(id: u32) -> AppRecord {
    AppRecord::new(id.to_be_bytes().to_vec())
}

/// Elect N1 as leader in a fresh 3-node cluster.
fn cluster_with_leader() -> SimulatedCluster {
    let mut cluster = SimulatedCluster::new(3);
    cluster.elect_leader(NodeId(1));
    cluster
}

/// Run enough fetch rounds for replication and HW propagation.
fn replicate_and_commit(cluster: &mut SimulatedCluster, rounds: usize) {
    for _ in 0..rounds {
        cluster.run_fetch_round();
    }
}

/// Synchronous executor for trait-object async methods in test assertions.
fn poll_now<T>(future: impl std::future::Future<Output = T>) -> T {
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    unsafe fn clone_fn(data: *const ()) -> RawWaker {
        RawWaker::new(data, &VTABLE)
    }
    unsafe fn nop_fn(_: *const ()) {}
    static VTABLE: RawWakerVTable =
        RawWakerVTable::new(clone_fn, nop_fn, nop_fn, nop_fn);

    let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) };
    let mut cx = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("trait object should never yield in tests"),
    }
}

// ---------------------------------------------------------------------------
// Test 1: Propose 100 entries and verify all committed across all nodes
// ---------------------------------------------------------------------------

#[test]
fn full_replication_100_entries() {
    let mut cluster = cluster_with_leader();
    let leader = NodeId(1);
    let mut checker = InvariantChecker::new();

    // Propose 100 command entries — each is persisted via LogStore trait object
    for i in 0..100u32 {
        let offset = cluster.propose(&make_record(i));
        assert!(offset.is_some(), "Proposal {} should succeed on leader", i);
    }

    // Verify leader's log is persisted in LogStore (via IoStage)
    assert!(
        cluster.verify_storage_consistency(leader),
        "Leader log should be consistent with LogStore after proposals"
    );

    // Run invariant checker during replication (historical append-only)
    checker.check_all(&cluster);

    // Replicate and commit (routes through EventLoopDriver → IoStage)
    replicate_and_commit(&mut cluster, 10);

    // All nodes should have same log_end_offset and high_watermark
    let expected_leo = cluster.node_leo(leader);
    let expected_hw = cluster.node_hw(leader);

    for id in [NodeId(1), NodeId(2), NodeId(3)] {
        assert_eq!(
            cluster.node_leo(id),
            expected_leo,
            "Node {:?} log_end_offset mismatch",
            id
        );
        assert_eq!(
            cluster.node_hw(id),
            expected_hw,
            "Node {:?} high_watermark mismatch",
            id
        );
    }

    // Verify the log contents are identical across all nodes
    let leader_log = cluster.node_log(leader);
    for id in [NodeId(2), NodeId(3)] {
        let log = cluster.node_log(id);
        assert_eq!(
            log.len(),
            leader_log.len(),
            "Node {:?} log length mismatch",
            id
        );
        for (a, b) in leader_log.iter().zip(log.iter()) {
            assert_eq!(a.offset, b.offset, "Offset mismatch at {}", a.offset);
            assert_eq!(a.term, b.term, "Term mismatch at offset {}", a.offset);
            assert_eq!(a.data, b.data, "Data mismatch at offset {}", a.offset);
        }
    }

    // HW must cover all 100 command entries + the LeaderChangeMessage
    assert!(
        expected_hw >= 101,
        "HW ({}) should cover all 101 entries (1 LCM + 100 commands)",
        expected_hw,
    );

    // Verify StateMachine::apply was called (via EventLoopDriver three-phase
    // commit notification) for all 100 command entries on each node.
    for id in [NodeId(1), NodeId(2), NodeId(3)] {
        let sm = cluster.state_machine(id);
        assert_eq!(
            sm.applied_count(), 100,
            "Node {:?} SM should have applied exactly 100 command entries (got {})",
            id, sm.applied_count()
        );
        // No duplicate applies
        assert_eq!(
            sm.duplicate_apply_count(), 0,
            "Node {:?} SM should have zero duplicate applies",
            id
        );
        // Verify each command was applied with the correct data
        for i in 0..100u32 {
            let expected_data = i.to_be_bytes().to_vec();
            let offset = (i as u64) + 1; // commands start at offset 1 (after LCM at 0)
            let applied = sm.get_applied(offset);
            assert!(
                applied.is_some(),
                "Node {:?} SM should have applied entry at offset {} (command {})",
                id, offset, i
            );
            assert_eq!(
                applied.unwrap().data, expected_data,
                "Node {:?} SM applied wrong data at offset {} (command {})",
                id, offset, i
            );
        }
        // Verify apply order is strictly monotonic
        let order = sm.apply_order();
        for j in 1..order.len() {
            assert!(
                order[j] > order[j - 1],
                "Node {:?} SM apply order not monotonic: {} -> {}",
                id, order[j - 1], order[j]
            );
        }
    }

    // Verify Listener::handle_commit was called on each node
    for id in [NodeId(1), NodeId(2), NodeId(3)] {
        let listener = cluster.listener(id);
        assert!(
            listener.total_committed() >= 100,
            "Node {:?} Listener should have been notified of 100 committed records (got {})",
            id, listener.total_committed()
        );
    }

    // Verify LogStore trait object consistency for all nodes (via IoStage)
    for id in [NodeId(1), NodeId(2), NodeId(3)] {
        assert!(
            cluster.verify_storage_consistency(id),
            "Node {:?} log should be consistent with LogStore",
            id
        );
    }

    // Run all 5 Raft safety invariants (including historical append-only)
    checker.check_all(&cluster);
}

// ---------------------------------------------------------------------------
// Test 2: Two-round commit visibility
// ---------------------------------------------------------------------------

#[test]
fn two_round_commit_visibility() {
    let mut cluster = cluster_with_leader();
    let _leader = NodeId(1);
    let follower = NodeId(2);
    let mut checker = InvariantChecker::new();

    // Propose one entry
    cluster.propose(&make_record(1));

    // --- Round 1 ---
    // Followers send FetchRequest (via TransportSender) with fetch_offset=0.
    // Leader handles, advances HW, sends FetchResponse back via transport.
    // After round 1 followers have entries but HW was computed with old offsets.
    cluster.run_fetch_round();

    let hw_after_round1 = cluster.node_hw(follower);

    // Follower SM apply count after round 1
    let sm_count_r1 = cluster.state_machine(follower).applied_count();

    // --- Round 2 ---
    // Followers send Fetch with updated fetch_offset → leader advances HW.
    cluster.run_fetch_round();

    let hw_after_round2 = cluster.node_hw(follower);

    // The follower's HW must have advanced between round 1 and round 2.
    assert!(
        hw_after_round2 > hw_after_round1,
        "Follower HW should advance on second fetch round: round1={} round2={}",
        hw_after_round1,
        hw_after_round2,
    );

    // After round 2 the entry should be committed
    assert!(
        hw_after_round2 >= 2,
        "HW after two rounds should cover LCM + command (got {})",
        hw_after_round2,
    );

    // Verify SM was called (via EventLoopDriver three-phase commit) AFTER HW advanced
    let sm_count_r2 = cluster.state_machine(follower).applied_count();
    assert!(
        sm_count_r2 > sm_count_r1,
        "Follower SM apply count should increase after HW advances: r1={} r2={}",
        sm_count_r1, sm_count_r2,
    );
    assert_eq!(
        sm_count_r2, 1,
        "Follower SM should have applied exactly 1 command entry (got {})",
        sm_count_r2,
    );

    // Verify Listener was notified of the committed entry
    let listener = cluster.listener(follower);
    assert!(
        listener.total_committed() >= 1,
        "Follower Listener should have been notified of at least 1 committed record"
    );

    // Verify QuorumStateStore has correct term via IoStage trait object
    let qs = poll_now(cluster.quorum_store(follower).load()).unwrap();
    assert!(qs.is_some(), "QuorumStateStore should have persisted state");
    assert_eq!(
        qs.unwrap().current_term,
        cluster.node(follower).term(),
        "Persisted term should match node term"
    );

    checker.check_all(&cluster);
}

// ---------------------------------------------------------------------------
// Test 3: Leader failure with partial replication — uncommitted tail truncated
//
// Uses a 5-node cluster to demonstrate that uncommitted entries on a minority
// of nodes are correctly truncated when a new leader is elected from the
// majority that doesn't have those entries.
//
// Topology:
//   1. All 5 nodes commit 50 entries
//   2. Partition leader N1 so it can only reach N2
//   3. Propose 20 more entries → replicated to N2 only (2/5, not majority)
//   4. Crash N1
//   5. Elect N3 (wins with N3+N4+N5 = 3/5 majority)
//   6. N3 does NOT have the uncommitted tail
//   7. After heal + replication, N2's uncommitted tail is truncated
// ---------------------------------------------------------------------------

#[test]
fn leader_failure_during_replication() {
    let mut cluster = SimulatedCluster::new(5);
    cluster.elect_leader(NodeId(1));
    let old_leader = NodeId(1);
    let mut checker = InvariantChecker::new();

    // Phase 1: Propose 50 entries and fully replicate + commit on all 5 nodes
    for i in 0..50u32 {
        cluster.propose(&make_record(i));
    }
    replicate_and_commit(&mut cluster, 10);

    let committed_hw = cluster.node_hw(old_leader);
    assert!(
        committed_hw >= 51,
        "All 50 commands + LCM should be committed (got {})",
        committed_hw
    );

    // Verify SM applied 50 entries on all nodes
    for nid in 1..=5u64 {
        let sm_count = cluster.state_machine(NodeId(nid)).applied_count();
        assert_eq!(sm_count, 50, "Node N{} SM should have applied 50 commands", nid);
    }

    // Snapshot invariants mid-test (captures leader log for append-only check)
    checker.check_all(&cluster);

    // Phase 2: Partition leader so it can only reach N2
    cluster.partition(old_leader, NodeId(3));
    cluster.partition(old_leader, NodeId(4));
    cluster.partition(old_leader, NodeId(5));

    // Propose uncommitted entries — only reach N1 + N2 (2/5, not majority)
    for i in 50..70u32 {
        cluster.propose(&make_record(i));
    }

    // Run one fetch round — entries replicate to N2 only (N3-N5 partitioned)
    cluster.run_fetch_round();

    // Assert the partial-replication topology before crashing
    let n2_leo = cluster.node_leo(NodeId(2));
    assert!(
        n2_leo > committed_hw,
        "N2 should have uncommitted entries (LEO={}, committed_hw={})",
        n2_leo, committed_hw
    );

    // N3, N4, N5 should NOT have the uncommitted entries
    for nid in [NodeId(3), NodeId(4), NodeId(5)] {
        assert_eq!(
            cluster.node_leo(nid), committed_hw,
            "Node {:?} should not have uncommitted entries (partitioned, LEO={})",
            nid, cluster.node_leo(nid)
        );
    }

    // HW should NOT have advanced on the leader (only 2/5 have entries)
    assert_eq!(
        cluster.node_hw(old_leader), committed_hw,
        "HW should not advance without majority replication"
    );

    // Verify uncommitted entries are NOT on the would-be new leader (N3)
    let n3_log = cluster.node_log(NodeId(3));
    for i in 50..70u32 {
        let data = i.to_be_bytes().to_vec();
        assert!(
            !n3_log.iter().any(|e| e.data == data),
            "N3 should not have uncommitted entry {} before leader crash",
            i
        );
    }

    // Phase 3: Crash the old leader
    cluster.stop_node(old_leader);
    cluster.heal_partitions();
    cluster.advance_time(500);

    // Verify QuorumStateStore persisted the old leader's state (via IoStage)
    let qs = poll_now(cluster.quorum_store(old_leader).load()).unwrap();
    assert!(qs.is_some(), "Quorum state should be persisted before crash");

    // Phase 4: Elect N3 as the new leader
    // N3 can win with votes from N4, N5 (3/5 = majority)
    // N2 will reject N3's VoteRequest because N2 has a longer log
    cluster.elect_leader(NodeId(3));

    let new_leader = NodeId(3);
    assert!(
        cluster.node(new_leader).is_leader(),
        "N3 should be the new leader"
    );

    // New leader should have committed entries + new LCM, but NOT the uncommitted tail
    let new_leader_leo = cluster.node_leo(new_leader);
    assert_eq!(
        new_leader_leo, committed_hw + 1, // +1 for new leader's LCM
        "New leader should have committed entries + new LCM only (got {})",
        new_leader_leo
    );

    // Verify entries 0..committed_hw are present on the new leader
    for offset in 0..committed_hw {
        assert!(
            cluster.node(new_leader).log().iter().any(|e| e.offset == offset),
            "New leader missing committed entry at offset {}",
            offset,
        );
    }

    // Phase 5: New proposals should succeed on the new leader
    for i in 100..110u32 {
        let offset = cluster.propose(&make_record(i));
        assert!(offset.is_some(), "Proposal {} on new leader should succeed", i);
    }

    // Replicate among N2, N3, N4, N5 (N1 stopped)
    // N2's uncommitted tail (entries 50..70) should be truncated via divergence detection
    replicate_and_commit(&mut cluster, 20);

    // Phase 6: Verify N2's uncommitted tail was truncated
    let n2_log = cluster.node_log(NodeId(2));
    for i in 50..70u32 {
        let data = i.to_be_bytes().to_vec();
        let found = n2_log.iter().any(|e| e.data == data);
        assert!(
            !found,
            "Uncommitted entry {} should have been truncated from N2",
            i
        );
    }

    // The new leader's entries (100..110) should be present on N2
    for i in 100..110u32 {
        let data = i.to_be_bytes().to_vec();
        let found = n2_log.iter().any(|e| e.data == data);
        assert!(
            found,
            "New leader entry {} should be replicated to N2 after catch-up",
            i
        );
    }

    // All active nodes should converge
    let active_ids = [NodeId(2), NodeId(3), NodeId(4), NodeId(5)];
    let expected_leo = cluster.node_leo(new_leader);
    let expected_hw = cluster.node_hw(new_leader);
    for nid in active_ids {
        assert_eq!(
            cluster.node_leo(nid), expected_leo,
            "Node {:?} LEO should match new leader",
            nid
        );
        assert_eq!(
            cluster.node_hw(nid), expected_hw,
            "Node {:?} HW should match new leader",
            nid
        );
    }

    // Verify LogStore consistency after truncation + catch-up
    for nid in active_ids {
        assert!(
            cluster.verify_storage_consistency(nid),
            "Node {:?} LogStore should be consistent after truncation + catch-up",
            nid
        );
    }

    // SM on N2 should have been reset and re-applied after catching up
    let n2_sm = cluster.state_machine(NodeId(2));
    assert!(
        n2_sm.applied_count() >= 50,
        "N2 SM should have applied at least the 50 original committed commands (got {})",
        n2_sm.applied_count()
    );

    checker.check_all(&cluster);
}

// ---------------------------------------------------------------------------
// Test 4: Log divergence and truncation after leader change
// ---------------------------------------------------------------------------

#[test]
fn log_divergence_and_truncation() {
    let mut cluster = cluster_with_leader();
    let old_leader = NodeId(1);
    let mut checker = InvariantChecker::new();

    // Propose some entries and replicate them
    for i in 0..5u32 {
        cluster.propose(&make_record(i));
    }
    replicate_and_commit(&mut cluster, 10);

    let common_hw = cluster.node_hw(old_leader);
    assert!(common_hw >= 6, "Baseline entries should be committed (got {})", common_hw);

    // Verify SM applied 5 commands on all nodes
    for id in [NodeId(1), NodeId(2), NodeId(3)] {
        let sm_count = cluster.state_machine(id).applied_count();
        assert_eq!(sm_count, 5, "Node {:?} SM should have applied 5 commands before divergence", id);
    }

    // Snapshot invariants at baseline
    checker.check_all(&cluster);

    // Partition the old leader from the rest.
    cluster.partition(old_leader, NodeId(2));
    cluster.partition(old_leader, NodeId(3));

    // Old leader proposes entries that ONLY exist on N1 (uncommitted)
    for i in 100..105u32 {
        cluster.propose_to(old_leader, &make_record(i));
    }
    let _divergent_leo = cluster.node_leo(old_leader);

    // Stop the old leader entirely
    cluster.stop_node(old_leader);
    cluster.heal_partitions();
    cluster.advance_time(500);

    // Elect N2 as the new leader
    cluster.elect_leader(NodeId(2));
    let new_leader = NodeId(2);

    // New leader proposes its own entries (different data from old leader's divergent ones)
    for i in 200..205u32 {
        cluster.propose_to(new_leader, &make_record(i));
    }
    replicate_and_commit(&mut cluster, 10);

    let new_leader_leo = cluster.node_leo(new_leader);
    let n3_leo = cluster.node_leo(NodeId(3));
    assert_eq!(
        new_leader_leo, n3_leo,
        "N2 and N3 should have converged"
    );

    // Restart the old leader — recovers from LogStore trait object
    // HW reset to 0, SM reset (volatile)
    cluster.restart_node(old_leader);

    assert_eq!(
        cluster.node_hw(old_leader), 0,
        "HW should be 0 after restart (not persisted)"
    );

    // Verify SM was reset on restart
    let n1_sm = cluster.state_machine(old_leader);
    assert_eq!(
        n1_sm.applied_count(), 0,
        "SM should be reset on restart (volatile state lost)"
    );

    // Divergence detection (epoch-based) should truncate N1's extra entries.
    replicate_and_commit(&mut cluster, 20);

    // N1 should match the new leader's log
    let n1_leo = cluster.node_leo(old_leader);
    assert_eq!(
        n1_leo,
        cluster.node_leo(new_leader),
        "After truncation N1 LEO should match new leader"
    );

    // Verify the divergent entries from the old leader are gone
    let n1_log = cluster.node_log(old_leader);
    for i in 100..105u32 {
        let divergent_data = i.to_be_bytes().to_vec();
        let found = n1_log.iter().any(|e| e.data == divergent_data);
        assert!(
            !found,
            "Divergent entry {} should have been truncated from N1",
            i,
        );
    }

    // The new leader's entries (200..205) should be present on N1
    for i in 200..205u32 {
        let expected_data = i.to_be_bytes().to_vec();
        let found = n1_log.iter().any(|e| e.data == expected_data);
        assert!(
            found,
            "New leader entry {} should be replicated to N1 after catch-up",
            i,
        );
    }

    // N1 SM re-applied after recovery
    let n1_sm = cluster.state_machine(old_leader);
    assert!(
        n1_sm.applied_count() >= 5,
        "N1 SM should have re-applied at least the original 5 committed commands (got {})",
        n1_sm.applied_count()
    );
    assert_eq!(
        n1_sm.duplicate_apply_count(), 0,
        "N1 SM should have zero duplicate applies after restart"
    );

    // LogStore consistency after truncation + catch-up
    assert!(
        cluster.verify_storage_consistency(old_leader),
        "N1 LogStore should be consistent after truncation + catch-up"
    );

    checker.check_all(&cluster);
}

// ---------------------------------------------------------------------------
// Test 5: Interleaved proposals with HW advancement and DCQ completion tracking
//
// Uses the DeferredCompletionQueue (oneshot-backed) from the EventLoopDriver
// to track proposal completion. Each propose_with_completion() returns a
// tokio::sync::oneshot::Receiver that fires when the entry is committed.
// ---------------------------------------------------------------------------

#[test]
fn concurrent_proposals_with_hw_advancement() {
    let mut cluster = cluster_with_leader();
    let leader = NodeId(1);
    let mut checker = InvariantChecker::new();

    let mut prev_hw = cluster.node_hw(leader);
    let mut hw_advances = 0u32;
    let mut sm_apply_counts: Vec<usize> = Vec::new();
    let mut completion_receivers = Vec::new();

    // Interleave proposals and fetch rounds. Proposals arrive while previous
    // entries are still being replicated, exercising concurrent proposal
    // acceptance with ongoing replication via the EventLoopDriver pipeline.
    //
    // Each proposal goes through: EventLoopDriver::process(Propose) →
    //   DeferredCompletionQueue::enqueue → IoActionBatch(AppendLog) →
    //   IoStage::execute → LogStore::append
    for batch in 0..10u32 {
        // Propose a batch, tracking each via DeferredCompletionQueue (oneshot)
        for i in 0..10u32 {
            let val = batch * 10 + i;
            let result = cluster.propose_with_completion(&make_record(val));
            assert!(result.is_some(), "Proposal {} should succeed", val);
            let (offset, rx) = result.unwrap();
            completion_receivers.push((offset, rx));
        }

        // Run invariant checker during interleaved operation
        checker.check_all(&cluster);

        // Single fetch round via EventLoopDriver → IoStage → TransportSender
        cluster.run_fetch_round();

        let hw_now = cluster.node_hw(leader);
        if hw_now > prev_hw {
            hw_advances += 1;
            prev_hw = hw_now;
        }

        let sm_count = cluster.state_machine(leader).applied_count();
        sm_apply_counts.push(sm_count);
    }

    // HW should have advanced multiple times
    assert!(
        hw_advances >= 2,
        "HW should advance multiple times during interleaved propose/fetch (got {} advances)",
        hw_advances,
    );

    // SM apply count monotonically increasing
    for i in 1..sm_apply_counts.len() {
        assert!(
            sm_apply_counts[i] >= sm_apply_counts[i - 1],
            "SM apply count should be monotonically increasing: {} -> {} at batch {}",
            sm_apply_counts[i - 1], sm_apply_counts[i], i,
        );
    }

    // Some oneshot receivers should have fired during interleaved operation
    let mut mid_completed = 0;
    for (_, rx) in &mut completion_receivers {
        if rx.try_recv().is_ok() {
            mid_completed += 1;
        }
    }
    assert!(
        mid_completed > 0,
        "Some proposals should complete during interleaved operation via DeferredCompletionQueue"
    );

    // Fully converge
    replicate_and_commit(&mut cluster, 10);

    let final_hw = cluster.node_hw(leader);

    // All remaining oneshot receivers should fire after full convergence
    // (DeferredCompletionQueue::complete is called in EventLoopDriver::process)
    let mut post_completed = 0;
    for (_, rx) in &mut completion_receivers {
        if rx.try_recv().is_ok() {
            post_completed += 1;
        }
    }
    let total_completed = mid_completed + post_completed;
    assert_eq!(
        total_completed, 100,
        "All 100 proposals should be completed via DeferredCompletionQueue oneshot (got {})",
        total_completed,
    );

    // Verify DeferredCompletionQueue is empty on the leader
    let dcq = cluster.completion_queue(leader);
    assert!(
        dcq.all_completed(),
        "Leader's DeferredCompletionQueue should have no pending proposals (has {})",
        dcq.pending_count()
    );

    assert_eq!(
        final_hw, 101,
        "Final HW should cover all 101 entries (1 LCM + 100 commands)",
    );

    assert!(cluster.all_converged(), "All nodes should converge");

    for id in [NodeId(2), NodeId(3)] {
        assert_eq!(
            cluster.node_hw(id), final_hw,
            "Follower {:?} HW should match leader", id,
        );
    }

    // All 100 commands applied on all nodes via three-phase commit, no duplicates
    for id in [NodeId(1), NodeId(2), NodeId(3)] {
        let sm = cluster.state_machine(id);
        assert_eq!(
            sm.applied_count(), 100,
            "Node {:?} SM should have applied all 100 commands (got {})",
            id, sm.applied_count()
        );
        assert_eq!(
            sm.duplicate_apply_count(), 0,
            "Node {:?} SM should have zero duplicate applies", id
        );
    }

    // SM apply order strictly monotonic
    for id in [NodeId(1), NodeId(2), NodeId(3)] {
        let sm = cluster.state_machine(id);
        let order = sm.apply_order();
        for i in 1..order.len() {
            assert!(
                order[i] > order[i - 1],
                "Node {:?} SM apply order not monotonic: {} -> {} at index {}",
                id, order[i - 1], order[i], i,
            );
        }
    }

    // Listener verify
    for id in [NodeId(1), NodeId(2), NodeId(3)] {
        let listener = cluster.listener(id);
        assert_eq!(
            listener.total_committed(), 100,
            "Node {:?} Listener should have been notified of 100 committed records (got {})",
            id, listener.total_committed()
        );
    }

    // LogStore consistency (via IoStage)
    for id in [NodeId(1), NodeId(2), NodeId(3)] {
        assert!(
            cluster.verify_storage_consistency(id),
            "Node {:?} log should be consistent with LogStore", id
        );
    }

    checker.check_all(&cluster);
}
