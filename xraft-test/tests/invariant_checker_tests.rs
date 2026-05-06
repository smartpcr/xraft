//! Integration tests for the InvariantChecker — Stage 9.2 test scenarios.

use std::collections::HashMap;

use xraft_core::consensus_state::Role;
use xraft_core::log_entry::{EntryType, LogEntry};
use xraft_core::types::{NodeId, Offset, Term};
use xraft_test::invariant_checker::{AppliedEntry, InvariantChecker, NodeSnapshot};
use xraft_test::simulated_cluster::{
    ElectionManager, ReplicationManager, SimulatedCluster,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn cmd_entry(offset: u64, term: u64, data: &[u8]) -> LogEntry {
    LogEntry::command(Offset(offset), Term(term), data.to_vec())
}

fn make_entries(n: u64, term: u64) -> Vec<LogEntry> {
    (0..n).map(|i| cmd_entry(i, term, &i.to_le_bytes())).collect()
}

fn snapshot(
    id: u64,
    term: u64,
    role: Role,
    entries: Vec<LogEntry>,
    hw: u64,
) -> NodeSnapshot {
    let applied: HashMap<u64, AppliedEntry> = entries
        .iter()
        .filter(|e| e.offset.0 < hw)
        .map(|e| (e.offset.0, AppliedEntry::from_log_entry(e)))
        .collect();
    NodeSnapshot {
        node_id: NodeId(id),
        current_term: Term(term),
        role,
        leader_id: if role == Role::Leader {
            Some(NodeId(id))
        } else {
            Some(NodeId(1))
        },
        voted_for: Some(NodeId(1)),
        high_watermark: hw,
        log_start_offset: 0,
        log_entries: entries,
        applied_entries: applied,
    }
}

// ---------------------------------------------------------------------------
// Scenario 1: All five invariants pass on a healthy 3-node cluster after
//             100 proposals — using both direct snapshots and SimulatedCluster.
// ---------------------------------------------------------------------------

#[test]
fn all_five_invariants_pass_healthy_3_node_cluster() {
    let entries = make_entries(100, 1);
    let snaps = vec![
        snapshot(1, 1, Role::Leader, entries.clone(), 100),
        snapshot(2, 1, Role::Follower, entries.clone(), 100),
        snapshot(3, 1, Role::Follower, entries, 100),
    ];

    let mut checker = InvariantChecker::new();
    let violations = checker.check_all(&snaps);
    assert!(
        violations.is_empty(),
        "all five Raft invariants should pass on a healthy cluster: {violations:?}"
    );
}

/// 100 proposals through a real SimulatedCluster (backed by MemoryLogStore
/// and ChannelTransport) with automatic post-transition invariant checking.
#[test]
fn all_five_invariants_pass_100_proposals_via_simulated_cluster() {
    let mut cluster = SimulatedCluster::new(3);

    cluster.elect_leader(NodeId(1), Term(1));

    for i in 0u64..100 {
        cluster.propose(i.to_le_bytes().to_vec());
    }

    cluster.replicate();
    cluster.commit(100);

    let v = cluster.check_invariants();
    assert!(
        v.is_empty(),
        "all five invariants should pass after 100 proposals via SimulatedCluster: {v:?}"
    );
}

// ---------------------------------------------------------------------------
// Scenario 2: Append-only leader violation detection.
//
// A deliberately buggy ReplicationManager allows the leader to overwrite a
// previously-appended entry. InvariantChecker panics with
// "append-only leader log" violation.
// ---------------------------------------------------------------------------

#[test]
#[should_panic(expected = "append-only leader log")]
fn append_only_leader_violation_detected() {
    let mut checker = InvariantChecker::new();

    let entries = make_entries(100, 1);
    let snaps_before = vec![
        snapshot(1, 1, Role::Leader, entries, 50),
        snapshot(2, 1, Role::Follower, make_entries(100, 1), 50),
        snapshot(3, 1, Role::Follower, make_entries(100, 1), 50),
    ];
    let v = checker.check_all(&snaps_before);
    assert!(v.is_empty());

    // Buggy ReplicationManager overwrites entry at offset 25.
    let mut corrupted_entries = make_entries(100, 1);
    corrupted_entries[25] = cmd_entry(25, 1, b"OVERWRITTEN-BY-BUG");

    let snaps_after = vec![
        snapshot(1, 1, Role::Leader, corrupted_entries, 50),
        snapshot(2, 1, Role::Follower, make_entries(100, 1), 50),
        snapshot(3, 1, Role::Follower, make_entries(100, 1), 50),
    ];
    checker.check_all_or_panic(&snaps_after);
}

/// Append-only violation through real SimulatedCluster with injected
/// buggy ReplicationManager.
#[test]
#[should_panic(expected = "append-only leader log")]
fn append_only_violation_via_simulated_cluster() {
    let mut cluster = SimulatedCluster::new(3);
    cluster.elect_leader(NodeId(1), Term(1));

    for i in 0u64..10 {
        cluster.propose(i.to_le_bytes().to_vec());
    }

    // Swap in a buggy ReplicationManager.
    cluster.set_replication_manager(ReplicationManager::buggy_overwrite(5));

    // This triggers the overwrite → post_transition_check panics.
    cluster.propose(b"BAD_OVERWRITE".to_vec());
}

// ---------------------------------------------------------------------------
// Scenario 3: Election safety violation detection.
//
// A deliberately buggy ElectionManager allows two leaders in the same term.
// InvariantChecker panics with "at most one leader per term" violation.
// ---------------------------------------------------------------------------

#[test]
#[should_panic(expected = "at most one leader per term")]
fn election_safety_violation_detected() {
    let mut checker = InvariantChecker::new();
    let entries = make_entries(100, 1);

    // Buggy ElectionManager: nodes 1 and 2 both claim leadership in term 1.
    let snaps = vec![
        snapshot(1, 1, Role::Leader, entries.clone(), 100),
        snapshot(2, 1, Role::Leader, entries.clone(), 100),
        snapshot(3, 1, Role::Follower, entries, 100),
    ];
    checker.check_all_or_panic(&snaps);
}

/// Election safety violation through real SimulatedCluster with injected
/// buggy ElectionManager.
#[test]
#[should_panic(expected = "at most one leader per term")]
fn election_safety_violation_via_simulated_cluster() {
    let mut cluster = SimulatedCluster::new(3);
    cluster.elect_leader(NodeId(1), Term(1));

    // Swap in a buggy ElectionManager.
    cluster.set_election_manager(ElectionManager::buggy_duplicate_leaders());

    // Node 2 also claims term 1 → post_transition_check panics.
    cluster.elect_leader(NodeId(2), Term(1));
}

/// Election safety across separate calls — node A is leader in term T,
/// then later node B claims leadership in the same term.
#[test]
#[should_panic(expected = "at most one leader per term")]
fn election_safety_violation_across_transitions() {
    let mut checker = InvariantChecker::new();
    let entries = make_entries(50, 1);

    let snaps1 = vec![
        snapshot(1, 1, Role::Leader, entries.clone(), 50),
        snapshot(2, 1, Role::Follower, entries.clone(), 50),
    ];
    assert!(checker.check_all(&snaps1).is_empty());

    // Buggy ElectionManager: node 2 also claims leadership in term 1.
    let snaps2 = vec![
        snapshot(1, 1, Role::Follower, entries.clone(), 50),
        snapshot(2, 1, Role::Leader, entries, 50),
    ];
    checker.check_all_or_panic(&snaps2);
}

// ---------------------------------------------------------------------------
// Additional scenario: log matching violation.
// ---------------------------------------------------------------------------

#[test]
#[should_panic(expected = "log matching")]
fn log_matching_violation_detected() {
    let mut checker = InvariantChecker::new();

    let entries_a = vec![
        cmd_entry(0, 1, b"x"),
        cmd_entry(1, 1, b"x"),
        cmd_entry(2, 1, b"x"),
        cmd_entry(3, 1, b"x"),
        cmd_entry(4, 1, b"a4"),
        cmd_entry(5, 2, b"shared"),
    ];
    let entries_b = vec![
        cmd_entry(0, 1, b"x"),
        cmd_entry(1, 1, b"x"),
        cmd_entry(2, 1, b"x"),
        cmd_entry(3, 1, b"x"),
        cmd_entry(4, 3, b"b4"), // different term!
        cmd_entry(5, 2, b"shared"),
    ];
    let snaps = vec![
        snapshot(1, 2, Role::Leader, entries_a, 3),
        snapshot(2, 2, Role::Follower, entries_b, 3),
    ];
    checker.check_all_or_panic(&snaps);
}

/// Log matching detects same offset/term with different payload.
#[test]
#[should_panic(expected = "log matching")]
fn log_matching_same_term_different_payload_detected() {
    let mut checker = InvariantChecker::new();

    let entries_a = vec![
        cmd_entry(0, 1, b"same"),
        cmd_entry(1, 1, b"AAA"),
    ];
    let entries_b = vec![
        cmd_entry(0, 1, b"same"),
        cmd_entry(1, 1, b"BBB"), // same term, different payload
    ];
    let snaps = vec![
        snapshot(1, 1, Role::Leader, entries_a, 0),
        snapshot(2, 1, Role::Follower, entries_b, 0),
    ];
    checker.check_all_or_panic(&snaps);
}

// ---------------------------------------------------------------------------
// Additional scenario: state machine safety violation.
// ---------------------------------------------------------------------------

#[test]
#[should_panic(expected = "state machine safety")]
fn state_machine_safety_violation_detected() {
    let mut checker = InvariantChecker::new();

    let entries = make_entries(10, 1);
    let mut snap_a = snapshot(1, 1, Role::Leader, entries.clone(), 10);
    let mut snap_b = snapshot(2, 1, Role::Follower, entries, 10);

    // Inject different applied entries at offset 5 — different payloads.
    snap_a.applied_entries.insert(
        5,
        AppliedEntry {
            term: Term(1),
            entry_type: EntryType::Command,
            payload: b"DATA-A".to_vec(),
        },
    );
    snap_b.applied_entries.insert(
        5,
        AppliedEntry {
            term: Term(1),
            entry_type: EntryType::Command,
            payload: b"DATA-B".to_vec(),
        },
    );

    checker.check_all_or_panic(&[snap_a, snap_b]);
}

/// State machine safety catches entries with same payload but different terms.
#[test]
#[should_panic(expected = "state machine safety")]
fn state_machine_safety_same_payload_different_term() {
    let mut checker = InvariantChecker::new();

    let entries = make_entries(10, 1);
    let mut snap_a = snapshot(1, 1, Role::Leader, entries.clone(), 10);
    let mut snap_b = snapshot(2, 1, Role::Follower, entries, 10);

    snap_a.applied_entries.insert(
        5,
        AppliedEntry {
            term: Term(1),
            entry_type: EntryType::Command,
            payload: b"SAME".to_vec(),
        },
    );
    snap_b.applied_entries.insert(
        5,
        AppliedEntry {
            term: Term(2),
            entry_type: EntryType::Command,
            payload: b"SAME".to_vec(),
        },
    );

    checker.check_all_or_panic(&[snap_a, snap_b]);
}

// ---------------------------------------------------------------------------
// Additional scenario: leader completeness violation.
// ---------------------------------------------------------------------------

#[test]
#[should_panic(expected = "leader completeness")]
fn leader_completeness_violation_detected() {
    let mut checker = InvariantChecker::new();

    let entries1 = make_entries(100, 1);
    let snaps1 = vec![snapshot(1, 1, Role::Leader, entries1, 50)];
    assert!(checker.check_all(&snaps1).is_empty());

    // New leader in term 2 has a corrupted entry at offset 20.
    let mut entries2: Vec<LogEntry> = (0..100)
        .map(|i| {
            if i < 50 {
                cmd_entry(i, 1, &i.to_le_bytes())
            } else {
                cmd_entry(i, 2, &i.to_le_bytes())
            }
        })
        .collect();
    entries2[20] = cmd_entry(20, 99, b"WRONG-ENTRY");

    let snaps2 = vec![snapshot(2, 2, Role::Leader, entries2, 80)];
    checker.check_all_or_panic(&snaps2);
}
