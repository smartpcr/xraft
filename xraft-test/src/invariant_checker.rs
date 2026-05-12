use xraft_core::*;
use xraft_core::log_entry::LogEntry;
use crate::SimulatedCluster;
use std::collections::HashMap;

/// Stateful verifier for the five Raft safety invariants.
///
/// Unlike a stateless checker, this tracks leader log snapshots across
/// invocations so that the "leader append-only" property is verified
/// historically: a leader in term T must never overwrite or delete entries
/// it previously held while still leading in term T.
///
/// Invariants checked:
///   1. Election safety: at most one leader per term
///   2. Leader append-only: a leader never overwrites/deletes its own entries
///      (verified via historical log snapshots per (node_id, term))
///   3. Log matching: if two logs have an entry at the same (offset, term), all
///      preceding entries match (including data)
///   4. Leader completeness: an elected leader's log contains every entry
///      committed in prior terms
///   5. State machine safety: no two nodes apply different entries at the same
///      offset; no duplicate applies within a single process lifetime
pub struct InvariantChecker {
    /// Historical log snapshots keyed by (node_id, term) for leader append-only.
    leader_log_snapshots: HashMap<(NodeId, Term), Vec<LogEntry>>,
}

impl InvariantChecker {
    pub fn new() -> Self {
        Self {
            leader_log_snapshots: HashMap::new(),
        }
    }

    /// Run all Raft invariant checks. Panics on any violation.
    pub fn check_all(&mut self, cluster: &SimulatedCluster) {
        Self::check_leader_election_safety(cluster);
        self.check_leader_append_only(cluster);
        Self::check_log_matching(cluster);
        Self::check_leader_completeness(cluster);
        Self::check_state_machine_safety(cluster);
    }

    /// Invariant 1: At most one leader per term.
    pub fn check_leader_election_safety(cluster: &SimulatedCluster) {
        let mut leaders_per_term: HashMap<Term, Vec<NodeId>> = HashMap::new();

        for id in cluster.node_ids() {
            if cluster.stopped_nodes.contains(&id) {
                continue;
            }
            let node = cluster.node(id);
            if node.is_leader() {
                leaders_per_term
                    .entry(node.term())
                    .or_default()
                    .push(id);
            }
        }

        for (term, leaders) in &leaders_per_term {
            assert!(
                leaders.len() <= 1,
                "INVARIANT VIOLATION: Multiple leaders in term {:?}: {:?}",
                term,
                leaders
            );
        }
    }

    /// Invariant 2: Leader never overwrites or deletes its own entries.
    pub fn check_leader_append_only(&mut self, cluster: &SimulatedCluster) {
        for id in cluster.node_ids() {
            if cluster.stopped_nodes.contains(&id) {
                continue;
            }
            let node = cluster.node(id);
            if !node.is_leader() {
                continue;
            }

            let log = node.log();

            // Basic monotonicity: offsets must be strictly increasing
            for i in 1..log.len() {
                assert!(
                    log[i].offset > log[i - 1].offset,
                    "INVARIANT VIOLATION: Leader {:?} has non-monotonic offsets at index {}",
                    id, i
                );
            }

            // Term monotonicity
            for i in 1..log.len() {
                assert!(
                    log[i].term >= log[i - 1].term,
                    "INVARIANT VIOLATION: Leader {:?} has decreasing terms: {:?} at offset {} \
                     followed by {:?} at offset {}",
                    id, log[i - 1].term, log[i - 1].offset, log[i].term, log[i].offset
                );
            }

            // Historical append-only
            let key = (id, node.term());
            if let Some(prev_snapshot) = self.leader_log_snapshots.get(&key) {
                for prev_entry in prev_snapshot {
                    let current_entry = log.iter().find(|e| e.offset == prev_entry.offset);
                    assert!(
                        current_entry.is_some(),
                        "INVARIANT VIOLATION: Leader {:?} in term {:?} deleted entry at offset {}",
                        id, node.term(), prev_entry.offset
                    );
                    let ce = current_entry.unwrap();
                    assert_eq!(
                        ce.term, prev_entry.term,
                        "INVARIANT VIOLATION: Leader {:?} in term {:?} changed term at offset {} \
                         from {:?} to {:?}",
                        id, node.term(), prev_entry.offset, prev_entry.term, ce.term
                    );
                    assert_eq!(
                        ce.data, prev_entry.data,
                        "INVARIANT VIOLATION: Leader {:?} in term {:?} changed data at offset {}",
                        id, node.term(), prev_entry.offset
                    );
                }
                assert!(
                    log.len() >= prev_snapshot.len(),
                    "INVARIANT VIOLATION: Leader {:?} in term {:?} log shrank from {} to {} entries",
                    id, node.term(), prev_snapshot.len(), log.len()
                );
            }

            self.leader_log_snapshots.insert(key, log.to_vec());
        }
    }

    /// Invariant 3: Log matching.
    pub fn check_log_matching(cluster: &SimulatedCluster) {
        let active_ids: Vec<NodeId> = cluster
            .node_ids()
            .into_iter()
            .filter(|id| !cluster.stopped_nodes.contains(id))
            .collect();

        for i in 0..active_ids.len() {
            for j in (i + 1)..active_ids.len() {
                let log_a = cluster.node(active_ids[i]).log();
                let log_b = cluster.node(active_ids[j]).log();

                for entry_a in log_a {
                    for entry_b in log_b {
                        if entry_a.offset == entry_b.offset && entry_a.term == entry_b.term {
                            Self::verify_prefix_match(
                                log_a,
                                log_b,
                                entry_a.offset,
                                active_ids[i],
                                active_ids[j],
                            );
                        }
                    }
                }
            }
        }
    }

    fn verify_prefix_match(
        log_a: &[LogEntry],
        log_b: &[LogEntry],
        up_to_offset: u64,
        node_a: NodeId,
        node_b: NodeId,
    ) {
        for offset in 0..=up_to_offset {
            let a = log_a.iter().find(|e| e.offset == offset);
            let b = log_b.iter().find(|e| e.offset == offset);

            match (a, b) {
                (Some(ea), Some(eb)) => {
                    assert_eq!(
                        ea.term, eb.term,
                        "INVARIANT VIOLATION: Log matching at offset {} between {:?} and {:?}",
                        offset, node_a, node_b
                    );
                    assert_eq!(
                        ea.entry_type, eb.entry_type,
                        "INVARIANT VIOLATION: Log matching entry types at offset {} between {:?} and {:?}",
                        offset, node_a, node_b
                    );
                    assert_eq!(
                        ea.data, eb.data,
                        "INVARIANT VIOLATION: Log matching data at offset {} between {:?} and {:?}",
                        offset, node_a, node_b
                    );
                }
                (Some(_), None) => {
                    let b_has_later = log_b.iter().any(|e| e.offset > offset);
                    assert!(
                        !b_has_later,
                        "INVARIANT VIOLATION: Log matching prefix gap at offset {} — \
                         {:?} has it, {:?} has a gap but has later entries",
                        offset, node_a, node_b
                    );
                }
                (None, Some(_)) => {
                    let a_has_later = log_a.iter().any(|e| e.offset > offset);
                    assert!(
                        !a_has_later,
                        "INVARIANT VIOLATION: Log matching prefix gap at offset {} — \
                         {:?} has it, {:?} has a gap but has later entries",
                        offset, node_b, node_a
                    );
                }
                (None, None) => {}
            }
        }
    }

    /// Invariant 4: Leader completeness.
    pub fn check_leader_completeness(cluster: &SimulatedCluster) {
        let active_ids: Vec<NodeId> = cluster
            .node_ids()
            .into_iter()
            .filter(|id| !cluster.stopped_nodes.contains(id))
            .collect();

        let leader = active_ids.iter().find(|id| cluster.node(**id).is_leader());
        let leader = match leader {
            Some(l) => *l,
            None => return,
        };
        let leader_node = cluster.node(leader);

        let total_voters = cluster.voter_count();
        let majority = total_voters / 2 + 1;
        let all_ids = cluster.node_ids();
        let max_offset = all_ids
            .iter()
            .map(|id| cluster.node(*id).log_end_offset())
            .max()
            .unwrap_or(0);

        for offset in 0..max_offset {
            let mut entry_counts: HashMap<(Term, Vec<u8>), usize> = HashMap::new();
            for id in &all_ids {
                if let Some(entry) = cluster.node(*id).log().iter().find(|e| e.offset == offset) {
                    *entry_counts
                        .entry((entry.term, entry.data.clone()))
                        .or_insert(0) += 1;
                }
            }

            for ((term, data), count) in &entry_counts {
                if *count >= majority {
                    let leader_entry = leader_node.log().iter().find(|e| e.offset == offset);
                    assert!(
                        leader_entry.is_some(),
                        "INVARIANT VIOLATION: Leader completeness — leader {:?} missing \
                         committed entry at offset {} (term {:?}, present on {}/{} voters)",
                        leader, offset, term, count, total_voters
                    );
                    let le = leader_entry.unwrap();
                    assert_eq!(le.term, *term);
                    assert_eq!(le.data, *data);
                }
            }
        }
    }

    /// Invariant 5: State machine safety.
    pub fn check_state_machine_safety(cluster: &SimulatedCluster) {
        let active_ids: Vec<NodeId> = cluster
            .node_ids()
            .into_iter()
            .filter(|id| !cluster.stopped_nodes.contains(id))
            .collect();

        if active_ids.is_empty() {
            return;
        }

        // Check no gaps in committed prefix
        for id in &active_ids {
            let node = cluster.node(*id);
            let hw = node.high_watermark();
            for offset in 0..hw {
                let entry = node.log().iter().find(|e| e.offset == offset);
                assert!(
                    entry.is_some(),
                    "INVARIANT VIOLATION: State machine safety — node {:?} missing \
                     committed entry at offset {} (HW={})",
                    id, offset, hw
                );
            }
        }

        // Check no duplicate applies
        for id in &active_ids {
            let sm = cluster.state_machine(*id);
            assert_eq!(
                sm.duplicate_apply_count(),
                0,
                "INVARIANT VIOLATION: State machine safety — node {:?} had {} duplicate applies",
                id,
                sm.duplicate_apply_count()
            );

            let order = sm.apply_order();
            for i in 1..order.len() {
                assert!(
                    order[i] > order[i - 1],
                    "INVARIANT VIOLATION: State machine safety — node {:?} apply order \
                     not strictly increasing: {} followed by {} at index {}",
                    id,
                    order[i - 1],
                    order[i],
                    i
                );
            }
        }

        // Cross-node comparison: applied entries at same offset must match
        for i in 0..active_ids.len() {
            for j in (i + 1)..active_ids.len() {
                let sm_a = cluster.state_machine(active_ids[i]);
                let sm_b = cluster.state_machine(active_ids[j]);

                for (offset, record_a) in sm_a.applied_entries() {
                    if let Some(record_b) = sm_b.get_applied(*offset) {
                        assert_eq!(
                            record_a.data, record_b.data,
                            "INVARIANT VIOLATION: State machine safety at offset {}. \
                             {:?} applied {:?}, {:?} applied {:?}",
                            offset,
                            active_ids[i],
                            record_a.data,
                            active_ids[j],
                            record_b.data
                        );
                    }
                }
            }
        }

        // Log-based consistency for committed entries
        for i in 0..active_ids.len() {
            for j in (i + 1)..active_ids.len() {
                let hw_a = cluster.node(active_ids[i]).high_watermark();
                let hw_b = cluster.node(active_ids[j]).high_watermark();
                let common_hw = std::cmp::min(hw_a, hw_b);

                for offset in 0..common_hw {
                    let entry_a = cluster
                        .node(active_ids[i])
                        .log()
                        .iter()
                        .find(|e| e.offset == offset);
                    let entry_b = cluster
                        .node(active_ids[j])
                        .log()
                        .iter()
                        .find(|e| e.offset == offset);

                    if let (Some(ea), Some(eb)) = (entry_a, entry_b) {
                        assert_eq!(ea.term, eb.term);
                        assert_eq!(ea.data, eb.data);
                    }
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