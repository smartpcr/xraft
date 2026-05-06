//! Invariant Checker — verifies the five Raft safety invariants.
//!
//! After each state transition the checker inspects a snapshot of every node
//! and panics (or returns a structured violation) if any invariant is broken.
//!
//! # Invariants
//!
//! 1. **Election safety** — at most one leader per term across all nodes.
//! 2. **Append-only leader log** — a leader never overwrites or deletes its
//!    own log entries.
//! 3. **Log matching** — if two nodes have an entry at the same offset and
//!    term, all preceding entries match.
//! 4. **Leader completeness** — an elected leader's log contains every
//!    previously committed entry.
//! 5. **State machine safety** — no two nodes have applied different entries
//!    at the same offset.

use std::collections::HashMap;
use std::fmt;

use xraft_core::consensus_state::Role;
use xraft_core::log_entry::{EntryType, LogEntry};
use xraft_core::types::{NodeId, Term};

// ---------------------------------------------------------------------------
// Applied entry — full identity of an applied state-machine entry
// ---------------------------------------------------------------------------

/// Complete identity of an entry applied to a node's state machine.
///
/// Includes term and entry_type so that two different log entries with
/// identical payloads at the same offset are correctly distinguished.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedEntry {
    pub term: Term,
    pub entry_type: EntryType,
    pub payload: Vec<u8>,
}

impl AppliedEntry {
    /// Create an `AppliedEntry` from a `LogEntry`.
    pub fn from_log_entry(entry: &LogEntry) -> Self {
        Self {
            term: entry.term,
            entry_type: entry.entry_type.clone(),
            payload: entry.payload.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// Node snapshot — lightweight view of a single node at a point in time
// ---------------------------------------------------------------------------

/// Snapshot of one node's observable state used for invariant verification.
#[derive(Debug, Clone)]
pub struct NodeSnapshot {
    pub node_id: NodeId,
    pub current_term: Term,
    pub role: Role,
    pub leader_id: Option<NodeId>,
    pub voted_for: Option<NodeId>,
    pub high_watermark: u64,
    pub log_start_offset: u64,
    /// Full log entries present on this node (from `log_start_offset`).
    pub log_entries: Vec<LogEntry>,
    /// Entries applied to this node's state machine, keyed by offset.
    /// Includes full entry identity (term, entry_type, payload) so that
    /// different entries with identical payloads are distinguished.
    pub applied_entries: HashMap<u64, AppliedEntry>,
}

// ---------------------------------------------------------------------------
// Violation type
// ---------------------------------------------------------------------------

/// A structured invariant violation.
#[derive(Debug, Clone)]
pub struct InvariantViolation {
    pub invariant: &'static str,
    pub message: String,
}

impl fmt::Display for InvariantViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.invariant, self.message)
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Selects which of the five checks to run.
#[derive(Debug, Clone)]
pub struct InvariantCheckerConfig {
    pub check_election_safety: bool,
    pub check_append_only_leader: bool,
    pub check_log_matching: bool,
    pub check_leader_completeness: bool,
    pub check_state_machine_safety: bool,
}

impl Default for InvariantCheckerConfig {
    fn default() -> Self {
        Self {
            check_election_safety: true,
            check_append_only_leader: true,
            check_log_matching: true,
            check_leader_completeness: true,
            check_state_machine_safety: true,
        }
    }
}

// ---------------------------------------------------------------------------
// InvariantChecker
// ---------------------------------------------------------------------------

/// Checks the five Raft safety invariants across a cluster.
///
/// Maintains history across multiple calls so that temporal invariants
/// (append-only, leader completeness) can be verified.
#[derive(Debug)]
pub struct InvariantChecker {
    config: InvariantCheckerConfig,
    /// `(term) -> node_id` — the first leader we observed for each term.
    observed_leaders_by_term: HashMap<u64, NodeId>,
    /// `(node_id, term) -> Vec<LogEntry>` — leader log recorded during a
    /// leadership epoch for the append-only check.
    leader_log_history: HashMap<(NodeId, u64), Vec<LogEntry>>,
    /// Full committed entries (offset -> LogEntry). Populated from the
    /// leader's log up to `high_watermark`. Stores the complete `LogEntry`
    /// so all fields (term, entry_type, payload) are compared.
    committed_entries: HashMap<u64, LogEntry>,
}

impl InvariantChecker {
    /// Create a new checker with all five checks enabled.
    pub fn new() -> Self {
        Self::with_config(InvariantCheckerConfig::default())
    }

    /// Create a new checker with the given configuration.
    pub fn with_config(config: InvariantCheckerConfig) -> Self {
        Self {
            config,
            observed_leaders_by_term: HashMap::new(),
            leader_log_history: HashMap::new(),
            committed_entries: HashMap::new(),
        }
    }

    // -- public API ---------------------------------------------------------

    /// Run all enabled invariant checks. Returns a list of violations (empty
    /// when everything is healthy).
    pub fn check_all(&mut self, snapshots: &[NodeSnapshot]) -> Vec<InvariantViolation> {
        let mut violations = Vec::new();

        if self.config.check_election_safety {
            self.check_election_safety(snapshots, &mut violations);
        }
        if self.config.check_append_only_leader {
            self.check_append_only_leader(snapshots, &mut violations);
        }
        if self.config.check_log_matching {
            Self::check_log_matching(snapshots, &mut violations);
        }
        if self.config.check_leader_completeness {
            self.check_leader_completeness(snapshots, &mut violations);
        }
        if self.config.check_state_machine_safety {
            Self::check_state_machine_safety(snapshots, &mut violations);
        }

        violations
    }

    /// Run all enabled checks; **panic** on the first violation.
    pub fn check_all_or_panic(&mut self, snapshots: &[NodeSnapshot]) {
        let violations = self.check_all(snapshots);
        if let Some(v) = violations.first() {
            panic!("{v}");
        }
    }

    // -- individual checks --------------------------------------------------

    /// (1) At most one leader per term across all nodes.
    ///
    /// Tracks leaders across invocations so that a term cannot have two
    /// different leaders even if they appear in separate snapshots.
    fn check_election_safety(
        &mut self,
        snapshots: &[NodeSnapshot],
        violations: &mut Vec<InvariantViolation>,
    ) {
        for snap in snapshots {
            if snap.role != Role::Leader {
                continue;
            }
            let term = snap.current_term.0;
            if let Some(&prev_leader) = self.observed_leaders_by_term.get(&term) {
                if prev_leader != snap.node_id {
                    violations.push(InvariantViolation {
                        invariant: "at most one leader per term",
                        message: format!(
                            "term {term}: node {} and node {} both observed as leader",
                            prev_leader, snap.node_id
                        ),
                    });
                }
            } else {
                self.observed_leaders_by_term.insert(term, snap.node_id);
            }
        }
    }

    /// (2) Append-only leader log — a leader never overwrites or deletes its
    /// own entries.
    ///
    /// Scoped to `(node_id, term)` leadership epochs so that a node losing
    /// leadership and later regaining it does not cause a false positive.
    fn check_append_only_leader(
        &mut self,
        snapshots: &[NodeSnapshot],
        violations: &mut Vec<InvariantViolation>,
    ) {
        for snap in snapshots {
            if snap.role != Role::Leader {
                continue;
            }
            let key = (snap.node_id, snap.current_term.0);
            if let Some(prev_log) = self.leader_log_history.get(&key) {
                // Every entry that was in the previous snapshot must still be
                // present and identical — unless it has been compacted (offset
                // is below the current log_start_offset).
                for prev_entry in prev_log {
                    if prev_entry.offset.0 < snap.log_start_offset {
                        continue;
                    }
                    let idx = (prev_entry.offset.0 - snap.log_start_offset) as usize;
                    match snap.log_entries.get(idx) {
                        Some(cur) if *cur == *prev_entry => {}
                        _ => {
                            violations.push(InvariantViolation {
                                invariant: "append-only leader log",
                                message: format!(
                                    "leader {} term {}: entry at offset {} was overwritten or deleted",
                                    snap.node_id, snap.current_term, prev_entry.offset
                                ),
                            });
                        }
                    }
                }
            }
            self.leader_log_history.insert(key, snap.log_entries.clone());
        }
    }

    /// (3) Log matching — if two nodes share an entry at the same offset and
    /// term, all preceding entries must match.
    fn check_log_matching(
        snapshots: &[NodeSnapshot],
        violations: &mut Vec<InvariantViolation>,
    ) {
        for i in 0..snapshots.len() {
            for j in (i + 1)..snapshots.len() {
                Self::check_log_matching_pair(&snapshots[i], &snapshots[j], violations);
            }
        }
    }

    fn check_log_matching_pair(
        a: &NodeSnapshot,
        b: &NodeSnapshot,
        violations: &mut Vec<InvariantViolation>,
    ) {
        let start = a.log_start_offset.max(b.log_start_offset);
        let end_a = a.log_start_offset + a.log_entries.len() as u64;
        let end_b = b.log_start_offset + b.log_entries.len() as u64;
        let end = end_a.min(end_b);

        let mut matched = false;
        let mut offset = end;
        while offset > start {
            offset -= 1;
            let idx_a = (offset - a.log_start_offset) as usize;
            let idx_b = (offset - b.log_start_offset) as usize;
            let ea = &a.log_entries[idx_a];
            let eb = &b.log_entries[idx_b];

            if ea.term == eb.term {
                if ea.entry_type != eb.entry_type || ea.payload != eb.payload {
                    violations.push(InvariantViolation {
                        invariant: "log matching",
                        message: format!(
                            "nodes {} and {} have same offset {offset} and term {} \
                             but different entry content (entry_type or payload mismatch)",
                            a.node_id, b.node_id, ea.term
                        ),
                    });
                    return;
                }
                matched = true;
            } else if matched {
                violations.push(InvariantViolation {
                    invariant: "log matching",
                    message: format!(
                        "nodes {} and {} have matching entry at a later offset \
                         but diverge at offset {offset} (terms {} vs {})",
                        a.node_id, b.node_id, ea.term, eb.term
                    ),
                });
                return;
            }
        }
    }

    /// (4) Leader completeness — a newly elected leader's log contains all
    /// previously committed entries (full entry comparison).
    fn check_leader_completeness(
        &mut self,
        snapshots: &[NodeSnapshot],
        violations: &mut Vec<InvariantViolation>,
    ) {
        // Update committed_entries from any leader snapshot.
        for snap in snapshots {
            if snap.role == Role::Leader {
                for entry in &snap.log_entries {
                    if entry.offset.0 < snap.high_watermark {
                        self.committed_entries
                            .entry(entry.offset.0)
                            .or_insert_with(|| entry.clone());
                    }
                }
            }
        }

        // For every leader, verify its log contains all known committed entries.
        for snap in snapshots {
            if snap.role != Role::Leader {
                continue;
            }
            for (&offset, committed) in &self.committed_entries {
                if offset < snap.log_start_offset {
                    continue;
                }
                let idx = (offset - snap.log_start_offset) as usize;
                match snap.log_entries.get(idx) {
                    Some(entry) if *entry == *committed => {}
                    Some(entry) => {
                        violations.push(InvariantViolation {
                            invariant: "leader completeness",
                            message: format!(
                                "leader {} term {}: entry at offset {offset} differs \
                                 from committed record (committed term {}, entry_type {:?}, \
                                 leader has term {}, entry_type {:?})",
                                snap.node_id, snap.current_term,
                                committed.term, committed.entry_type,
                                entry.term, entry.entry_type
                            ),
                        });
                    }
                    None => {
                        violations.push(InvariantViolation {
                            invariant: "leader completeness",
                            message: format!(
                                "leader {} term {}: missing committed entry at offset {offset}",
                                snap.node_id, snap.current_term
                            ),
                        });
                    }
                }
            }
        }
    }

    /// (5) State machine safety — no two nodes have applied different entries
    /// at the same offset.
    ///
    /// Compares full `AppliedEntry` (term + entry_type + payload) so that
    /// different entries with identical payloads are correctly detected.
    fn check_state_machine_safety(
        snapshots: &[NodeSnapshot],
        violations: &mut Vec<InvariantViolation>,
    ) {
        let mut applied: HashMap<u64, (NodeId, &AppliedEntry)> = HashMap::new();

        for snap in snapshots {
            for (&offset, entry) in &snap.applied_entries {
                if let Some(&(first_node, first_entry)) = applied.get(&offset) {
                    if entry != first_entry {
                        violations.push(InvariantViolation {
                            invariant: "state machine safety",
                            message: format!(
                                "nodes {} and {} applied different entries at offset {offset} \
                                 (terms {}/{}, payloads differ: {})",
                                first_node, snap.node_id,
                                first_entry.term, entry.term,
                                first_entry.payload != entry.payload
                                    || first_entry.entry_type != entry.entry_type
                            ),
                        });
                    }
                } else {
                    applied.insert(offset, (snap.node_id, entry));
                }
            }
        }
    }
}

impl Default for InvariantChecker {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use xraft_core::log_entry::LogEntry;
    use xraft_core::types::{NodeId, Offset, Term};

    fn cmd_entry(offset: u64, term: u64, data: &[u8]) -> LogEntry {
        LogEntry::command(Offset(offset), Term(term), data.to_vec())
    }

    fn healthy_snapshot(
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

    fn make_100_entries(term: u64) -> Vec<LogEntry> {
        (0..100)
            .map(|i| cmd_entry(i, term, &i.to_le_bytes()))
            .collect()
    }

    // -- Check 1: election safety -------------------------------------------

    #[test]
    fn election_safety_single_leader_passes() {
        let mut checker = InvariantChecker::new();
        let snaps = vec![
            healthy_snapshot(1, 1, Role::Leader, make_100_entries(1), 100),
            healthy_snapshot(2, 1, Role::Follower, make_100_entries(1), 100),
            healthy_snapshot(3, 1, Role::Follower, make_100_entries(1), 100),
        ];
        let v = checker.check_all(&snaps);
        assert!(v.is_empty(), "expected no violations, got: {v:?}");
    }

    #[test]
    #[should_panic(expected = "at most one leader per term")]
    fn election_safety_two_leaders_same_term_panics() {
        let mut checker = InvariantChecker::new();
        let snaps = vec![
            healthy_snapshot(1, 1, Role::Leader, make_100_entries(1), 100),
            healthy_snapshot(2, 1, Role::Leader, make_100_entries(1), 100),
            healthy_snapshot(3, 1, Role::Follower, make_100_entries(1), 100),
        ];
        checker.check_all_or_panic(&snaps);
    }

    #[test]
    #[should_panic(expected = "at most one leader per term")]
    fn election_safety_two_leaders_across_calls_panics() {
        let mut checker = InvariantChecker::new();
        let snap1 = vec![
            healthy_snapshot(1, 1, Role::Leader, make_100_entries(1), 100),
            healthy_snapshot(2, 1, Role::Follower, make_100_entries(1), 100),
        ];
        assert!(checker.check_all(&snap1).is_empty());

        let snap2 = vec![
            healthy_snapshot(1, 1, Role::Follower, make_100_entries(1), 100),
            healthy_snapshot(2, 1, Role::Leader, make_100_entries(1), 100),
        ];
        checker.check_all_or_panic(&snap2);
    }

    // -- Check 2: append-only leader log ------------------------------------

    #[test]
    fn append_only_leader_log_passes() {
        let mut checker = InvariantChecker::new();
        let entries = make_100_entries(1);
        let snaps = vec![
            healthy_snapshot(1, 1, Role::Leader, entries.clone(), 100),
            healthy_snapshot(2, 1, Role::Follower, entries.clone(), 100),
            healthy_snapshot(3, 1, Role::Follower, entries, 100),
        ];
        assert!(checker.check_all(&snaps).is_empty());
        assert!(checker.check_all(&snaps).is_empty());
    }

    #[test]
    #[should_panic(expected = "append-only leader log")]
    fn append_only_leader_log_overwrite_panics() {
        let mut checker = InvariantChecker::new();
        let entries = make_100_entries(1);
        let snaps1 = vec![healthy_snapshot(1, 1, Role::Leader, entries, 50)];
        assert!(checker.check_all(&snaps1).is_empty());

        let mut entries2 = make_100_entries(1);
        entries2[10] = cmd_entry(10, 1, b"CORRUPTED");
        let snaps2 = vec![healthy_snapshot(1, 1, Role::Leader, entries2, 50)];
        checker.check_all_or_panic(&snaps2);
    }

    #[test]
    fn append_only_leader_log_new_epoch_no_false_positive() {
        let config = InvariantCheckerConfig {
            check_election_safety: false,
            check_append_only_leader: true,
            check_log_matching: false,
            check_leader_completeness: false,
            check_state_machine_safety: false,
        };
        let mut checker = InvariantChecker::with_config(config);

        let entries1 = make_100_entries(1);
        let snaps1 = vec![healthy_snapshot(1, 1, Role::Leader, entries1, 50)];
        assert!(checker.check_all(&snaps1).is_empty());

        let entries2: Vec<LogEntry> = (0..80)
            .map(|i| cmd_entry(i, 2, &(i * 10).to_le_bytes()))
            .collect();
        let snaps2 = vec![healthy_snapshot(1, 2, Role::Leader, entries2, 40)];
        let v = checker.check_all(&snaps2);
        assert!(
            v.is_empty(),
            "expected no violation for new epoch, got: {v:?}"
        );
    }

    #[test]
    fn append_only_leader_log_compaction_permitted() {
        let config = InvariantCheckerConfig {
            check_election_safety: false,
            check_append_only_leader: true,
            check_log_matching: false,
            check_leader_completeness: false,
            check_state_machine_safety: false,
        };
        let mut checker = InvariantChecker::with_config(config);

        let entries = make_100_entries(1);
        let snaps1 = vec![healthy_snapshot(1, 1, Role::Leader, entries, 50)];
        assert!(checker.check_all(&snaps1).is_empty());

        let entries2: Vec<LogEntry> = (20..100)
            .map(|i| cmd_entry(i, 1, &i.to_le_bytes()))
            .collect();
        let mut snap = healthy_snapshot(1, 1, Role::Leader, entries2, 50);
        snap.log_start_offset = 20;
        let snaps2 = vec![snap];
        let v = checker.check_all(&snaps2);
        assert!(
            v.is_empty(),
            "compaction should not cause violation, got: {v:?}"
        );
    }

    // -- Check 3: log matching ----------------------------------------------

    #[test]
    fn log_matching_identical_logs_passes() {
        let mut checker = InvariantChecker::new();
        let entries = make_100_entries(1);
        let snaps = vec![
            healthy_snapshot(1, 1, Role::Leader, entries.clone(), 100),
            healthy_snapshot(2, 1, Role::Follower, entries.clone(), 100),
            healthy_snapshot(3, 1, Role::Follower, entries, 100),
        ];
        let v = checker.check_all(&snaps);
        assert!(v.is_empty(), "expected no violations, got: {v:?}");
    }

    #[test]
    #[should_panic(expected = "log matching")]
    fn log_matching_violation_panics() {
        let mut checker = InvariantChecker::new();
        let entries_a = vec![
            cmd_entry(0, 1, b"a0"),
            cmd_entry(1, 1, b"a1"),
            cmd_entry(2, 1, b"a2"),
            cmd_entry(3, 1, b"a3"),
            cmd_entry(4, 1, b"a4"),
            cmd_entry(5, 2, b"a5"),
        ];
        let entries_b = vec![
            cmd_entry(0, 1, b"b0"),
            cmd_entry(1, 1, b"b1"),
            cmd_entry(2, 1, b"b2"),
            cmd_entry(3, 1, b"b3"),
            cmd_entry(4, 3, b"b4"),
            cmd_entry(5, 2, b"b5"),
        ];
        let snaps = vec![
            healthy_snapshot(1, 2, Role::Leader, entries_a, 3),
            healthy_snapshot(2, 2, Role::Follower, entries_b, 3),
        ];
        checker.check_all_or_panic(&snaps);
    }

    #[test]
    #[should_panic(expected = "log matching")]
    fn log_matching_same_term_different_payload_panics() {
        let mut checker = InvariantChecker::new();
        let entries_a = vec![
            cmd_entry(0, 1, b"same0"),
            cmd_entry(1, 1, b"same1"),
            cmd_entry(2, 1, b"same2"),
            cmd_entry(3, 1, b"payload_A"),
        ];
        let entries_b = vec![
            cmd_entry(0, 1, b"same0"),
            cmd_entry(1, 1, b"same1"),
            cmd_entry(2, 1, b"same2"),
            cmd_entry(3, 1, b"payload_B"),
        ];
        let snaps = vec![
            healthy_snapshot(1, 1, Role::Leader, entries_a, 2),
            healthy_snapshot(2, 1, Role::Follower, entries_b, 2),
        ];
        checker.check_all_or_panic(&snaps);
    }

    #[test]
    #[should_panic(expected = "log matching")]
    fn log_matching_same_term_different_entry_type_panics() {
        let mut checker = InvariantChecker::new();
        let entries_a = vec![
            cmd_entry(0, 1, b"x"),
            LogEntry {
                offset: Offset(1),
                term: Term(1),
                entry_type: EntryType::Command,
                payload: vec![],
            },
        ];
        let entries_b = vec![
            cmd_entry(0, 1, b"x"),
            LogEntry {
                offset: Offset(1),
                term: Term(1),
                entry_type: EntryType::LeaderChangeMessage,
                payload: vec![],
            },
        ];
        let snaps = vec![
            healthy_snapshot(1, 1, Role::Leader, entries_a, 0),
            healthy_snapshot(2, 1, Role::Follower, entries_b, 0),
        ];
        checker.check_all_or_panic(&snaps);
    }

    #[test]
    fn leader_completeness_passes() {
        let mut checker = InvariantChecker::new();
        let entries = make_100_entries(1);
        let snaps = vec![healthy_snapshot(1, 1, Role::Leader, entries, 50)];
        assert!(checker.check_all(&snaps).is_empty());

        let entries2: Vec<LogEntry> = (0..100)
            .map(|i| {
                if i < 50 {
                    cmd_entry(i, 1, &i.to_le_bytes())
                } else {
                    cmd_entry(i, 2, &i.to_le_bytes())
                }
            })
            .collect();
        let snaps2 = vec![healthy_snapshot(2, 2, Role::Leader, entries2, 80)];
        let v = checker.check_all(&snaps2);
        assert!(v.is_empty(), "expected no violations, got: {v:?}");
    }

    #[test]
    #[should_panic(expected = "leader completeness")]
    fn leader_completeness_missing_committed_entry_panics() {
        let mut checker = InvariantChecker::new();
        let entries = make_100_entries(1);
        let snaps = vec![healthy_snapshot(1, 1, Role::Leader, entries, 50)];
        assert!(checker.check_all(&snaps).is_empty());

        let mut entries2: Vec<LogEntry> = (0..100)
            .map(|i| {
                if i < 50 {
                    cmd_entry(i, 1, &i.to_le_bytes())
                } else {
                    cmd_entry(i, 2, &i.to_le_bytes())
                }
            })
            .collect();
        entries2[30] = cmd_entry(30, 99, b"WRONG");
        let snaps2 = vec![healthy_snapshot(2, 2, Role::Leader, entries2, 80)];
        checker.check_all_or_panic(&snaps2);
    }

    // -- Check 5: state machine safety --------------------------------------

    #[test]
    fn state_machine_safety_passes() {
        let mut checker = InvariantChecker::new();
        let entries = make_100_entries(1);
        let snaps = vec![
            healthy_snapshot(1, 1, Role::Leader, entries.clone(), 50),
            healthy_snapshot(2, 1, Role::Follower, entries.clone(), 50),
            healthy_snapshot(3, 1, Role::Follower, entries, 50),
        ];
        let v = checker.check_all(&snaps);
        assert!(v.is_empty(), "expected no violations, got: {v:?}");
    }

    #[test]
    #[should_panic(expected = "state machine safety")]
    fn state_machine_safety_different_data_panics() {
        let mut checker = InvariantChecker::new();
        let entries = make_100_entries(1);
        let mut snap_a = healthy_snapshot(1, 1, Role::Leader, entries.clone(), 50);
        let mut snap_b = healthy_snapshot(2, 1, Role::Follower, entries, 50);

        // Different applied entries at offset 10 — different payloads.
        snap_a.applied_entries.insert(
            10,
            AppliedEntry {
                term: Term(1),
                entry_type: EntryType::Command,
                payload: b"AAAA".to_vec(),
            },
        );
        snap_b.applied_entries.insert(
            10,
            AppliedEntry {
                term: Term(1),
                entry_type: EntryType::Command,
                payload: b"BBBB".to_vec(),
            },
        );

        checker.check_all_or_panic(&[snap_a, snap_b]);
    }

    #[test]
    #[should_panic(expected = "state machine safety")]
    fn state_machine_safety_same_payload_different_term_panics() {
        let mut checker = InvariantChecker::new();
        let entries = make_100_entries(1);
        let mut snap_a = healthy_snapshot(1, 1, Role::Leader, entries.clone(), 50);
        let mut snap_b = healthy_snapshot(2, 1, Role::Follower, entries, 50);

        // Same payload, different term — must still be detected.
        snap_a.applied_entries.insert(
            10,
            AppliedEntry {
                term: Term(1),
                entry_type: EntryType::Command,
                payload: b"SAME".to_vec(),
            },
        );
        snap_b.applied_entries.insert(
            10,
            AppliedEntry {
                term: Term(2),
                entry_type: EntryType::Command,
                payload: b"SAME".to_vec(),
            },
        );

        checker.check_all_or_panic(&[snap_a, snap_b]);
    }

    // -- Composite scenarios ------------------------------------------------

    #[test]
    fn all_five_invariants_pass_healthy_cluster() {
        let mut checker = InvariantChecker::new();
        let entries = make_100_entries(1);
        let snaps = vec![
            healthy_snapshot(1, 1, Role::Leader, entries.clone(), 100),
            healthy_snapshot(2, 1, Role::Follower, entries.clone(), 100),
            healthy_snapshot(3, 1, Role::Follower, entries, 100),
        ];
        let v = checker.check_all(&snaps);
        assert!(
            v.is_empty(),
            "all five invariants should pass on a healthy cluster, got: {v:?}"
        );
    }

    #[test]
    fn configurable_checks() {
        let config = InvariantCheckerConfig {
            check_election_safety: false,
            check_append_only_leader: true,
            check_log_matching: true,
            check_leader_completeness: true,
            check_state_machine_safety: true,
        };
        let mut checker = InvariantChecker::with_config(config);
        let entries = make_100_entries(1);
        let snaps = vec![
            healthy_snapshot(1, 1, Role::Leader, entries.clone(), 100),
            healthy_snapshot(2, 1, Role::Leader, entries, 100),
        ];
        let v = checker.check_all(&snaps);
        assert!(v.is_empty(), "election safety disabled, should pass: {v:?}");
    }
}
