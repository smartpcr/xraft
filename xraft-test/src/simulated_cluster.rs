//! Simulated cluster — deterministic multi-node Raft cluster for testing.
//!
//! Uses real `MemoryLogStore` from xraft-storage and `ChannelTransport`
//! from xraft-transport for integration. Provides `ElectionManager` and
//! `ReplicationManager` structs with injectable buggy behaviour for
//! invariant-violation tests.

use std::collections::HashMap;

use crate::invariant_checker::{
    AppliedEntry, InvariantChecker, InvariantCheckerConfig, InvariantViolation, NodeSnapshot,
};
use xraft_core::consensus_state::Role;
use xraft_core::log_entry::LogEntry;
use xraft_core::traits::LogStore;
use xraft_core::types::{NodeId, Offset, Term};
use xraft_storage::MemoryLogStore;
use xraft_transport::{ChannelTransport, RaftMessage};

// ---------------------------------------------------------------------------
// RaftNode — per-node state backed by MemoryLogStore
// ---------------------------------------------------------------------------

/// State of a single Raft node, backed by a real `MemoryLogStore`.
///
/// This is a lightweight stand-in for the full `RaftNode` (not yet
/// implemented) that uses the actual storage backend from xraft-storage.
#[derive(Debug)]
pub struct RaftNode {
    pub node_id: NodeId,
    pub current_term: Term,
    pub role: Role,
    pub leader_id: Option<NodeId>,
    pub voted_for: Option<NodeId>,
    pub high_watermark: u64,
    /// Persistent log backed by `MemoryLogStore` (implements `LogStore` trait).
    pub log_store: MemoryLogStore,
    /// Entries applied to this node's state machine.
    applied_entries: HashMap<u64, AppliedEntry>,
}

impl RaftNode {
    fn new(id: u64) -> Self {
        Self {
            node_id: NodeId(id),
            current_term: Term(0),
            role: Role::Unattached,
            leader_id: None,
            voted_for: None,
            high_watermark: 0,
            log_store: MemoryLogStore::new(),
            applied_entries: HashMap::new(),
        }
    }

    fn snapshot(&self) -> NodeSnapshot {
        NodeSnapshot {
            node_id: self.node_id,
            current_term: self.current_term,
            role: self.role,
            leader_id: self.leader_id,
            voted_for: self.voted_for,
            high_watermark: self.high_watermark,
            log_start_offset: self.log_store.log_start_offset(),
            log_entries: self.log_store.entries(),
            applied_entries: self.applied_entries.clone(),
        }
    }

    /// Apply all committed-but-not-yet-applied entries from the log store.
    fn apply_committed(&mut self) {
        let entries = self.log_store.entries();
        for entry in &entries {
            let off = entry.offset.0;
            if off < self.high_watermark && !self.applied_entries.contains_key(&off) {
                self.applied_entries
                    .insert(off, AppliedEntry::from_log_entry(entry));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ElectionManager — manages leader elections with optional fault injection
// ---------------------------------------------------------------------------

/// Manages leader elections. In normal mode, ensures at most one leader per
/// term by demoting all other nodes. In buggy mode, allows duplicate leaders
/// in the same term (simulating a faulty ElectionManager).
pub struct ElectionManager {
    allow_duplicate_leaders: bool,
}

impl ElectionManager {
    /// Create a correct election manager.
    pub fn new() -> Self {
        Self {
            allow_duplicate_leaders: false,
        }
    }

    /// Create a deliberately buggy election manager that allows two leaders
    /// in the same term.
    pub fn buggy_duplicate_leaders() -> Self {
        Self {
            allow_duplicate_leaders: true,
        }
    }

    /// Elect `node_id` as leader in `term`. In normal mode, all other nodes
    /// become followers. In buggy mode, existing leaders are not demoted.
    pub fn elect(
        &self,
        nodes: &mut [RaftNode],
        node_id: NodeId,
        term: Term,
        transport: &ChannelTransport,
    ) {
        for node in nodes.iter_mut() {
            if node.node_id == node_id {
                node.current_term = term;
                node.role = Role::Leader;
                node.leader_id = Some(node_id);
                node.voted_for = Some(node_id);
            } else if !self.allow_duplicate_leaders {
                // Normal: demote to follower
                node.current_term = term;
                node.role = Role::Follower;
                node.leader_id = Some(node_id);
                node.voted_for = Some(node_id);
            } else {
                // Buggy: don't demote existing leaders in same term
                if node.current_term < term {
                    node.current_term = term;
                    node.role = Role::Follower;
                    node.leader_id = Some(node_id);
                }
            }
        }

        // Send VoteResponse messages through the transport to simulate
        // the election protocol communication.
        let node_ids: Vec<NodeId> = nodes.iter().map(|n| n.node_id).collect();
        for &nid in &node_ids {
            if nid != node_id {
                transport.send(
                    nid,
                    node_id,
                    RaftMessage::VoteResponse {
                        node_id: nid,
                        term,
                        granted: true,
                    },
                );
            }
        }
        // Drain the transport — the election is synchronous in simulation.
        let _ = transport.drain_all();
    }
}

impl Default for ElectionManager {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// ReplicationManager — manages log replication with optional fault injection
// ---------------------------------------------------------------------------

/// Manages log replication. In normal mode, appends entries to the leader's
/// `MemoryLogStore` and replicates to followers via `ChannelTransport`.
/// In buggy mode, overwrites a previously-appended entry (simulating a
/// faulty ReplicationManager).
pub struct ReplicationManager {
    /// When set, the leader overwrites the entry at this offset instead of
    /// appending a new entry.
    overwrite_offset: Option<u64>,
}

impl ReplicationManager {
    /// Create a correct replication manager.
    pub fn new() -> Self {
        Self {
            overwrite_offset: None,
        }
    }

    /// Create a deliberately buggy replication manager that overwrites the
    /// entry at the given offset on the next `propose` call.
    pub fn buggy_overwrite(offset: u64) -> Self {
        Self {
            overwrite_offset: Some(offset),
        }
    }

    /// Append a command entry to the leader's log via `MemoryLogStore`.
    /// Returns the offset of the appended (or overwritten) entry.
    pub fn propose(&self, leader: &mut RaftNode, data: Vec<u8>) -> u64 {
        if let Some(overwrite_off) = self.overwrite_offset {
            // Buggy: overwrite an existing entry in the MemoryLogStore.
            let entry = LogEntry::command(
                Offset(overwrite_off),
                leader.current_term,
                data,
            );
            leader.log_store.overwrite_entry(overwrite_off, entry);
            return overwrite_off;
        }

        // Normal: append to the MemoryLogStore.
        let offset = leader.log_store.log_end_offset();
        let entry = LogEntry::command(Offset(offset), leader.current_term, data);
        leader
            .log_store
            .append_sync(&[entry])
            .expect("MemoryLogStore append failed");
        offset
    }

    /// Replicate the leader's log to followers via ChannelTransport.
    /// Followers receive AppendEntries messages and adopt the leader's
    /// entries into their own `MemoryLogStore`.
    pub fn replicate(
        nodes: &mut [RaftNode],
        transport: &ChannelTransport,
    ) {
        let leader_idx = nodes
            .iter()
            .position(|n| n.role == Role::Leader)
            .expect("no leader elected");
        let leader_entries = nodes[leader_idx].log_store.entries();
        let leader_start = nodes[leader_idx].log_store.log_start_offset();
        let leader_id = nodes[leader_idx].node_id;
        let leader_term = nodes[leader_idx].current_term;
        let leader_hw = nodes[leader_idx].high_watermark;

        // Send AppendEntries to each follower via transport.
        let node_ids: Vec<NodeId> = nodes.iter().map(|n| n.node_id).collect();
        for &nid in &node_ids {
            if nid == leader_id {
                continue;
            }
            transport.send(
                leader_id,
                nid,
                RaftMessage::AppendEntries {
                    leader_id,
                    term: leader_term,
                    prev_log_offset: leader_start,
                    prev_log_term: leader_term,
                    entries: leader_entries.clone(),
                    leader_commit: leader_hw,
                },
            );
        }

        // Process messages: followers adopt leader's entries into their
        // MemoryLogStore.
        for node in nodes.iter_mut() {
            if node.node_id == leader_id || node.role != Role::Follower {
                continue;
            }
            let messages = transport.recv(node.node_id);
            for (_from, msg) in messages {
                if let RaftMessage::AppendEntries { entries, .. } = msg {
                    let follower_end = node.log_store.log_end_offset();
                    let new_entries: Vec<LogEntry> = entries
                        .into_iter()
                        .filter(|e| e.offset.0 >= follower_end)
                        .collect();
                    if !new_entries.is_empty() {
                        node.log_store
                            .append_sync(&new_entries)
                            .expect("follower MemoryLogStore append failed");
                    }
                }
            }
        }
    }
}

impl Default for ReplicationManager {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// SimulatedCluster
// ---------------------------------------------------------------------------

/// A simulated Raft cluster with real `MemoryLogStore` and `ChannelTransport`
/// integration. Uses `ElectionManager` and `ReplicationManager` structs for
/// election and replication, with support for swapping in buggy variants.
pub struct SimulatedCluster {
    nodes: Vec<RaftNode>,
    transport: ChannelTransport,
    checker: Option<InvariantChecker>,
    check_after_transition: bool,
    election_manager: ElectionManager,
    replication_manager: ReplicationManager,
}

impl SimulatedCluster {
    /// Create a cluster of `size` nodes with invariant checking enabled.
    /// Each node uses a real `MemoryLogStore` and nodes are connected
    /// via `ChannelTransport`.
    pub fn new(size: usize) -> Self {
        let node_ids: Vec<NodeId> = (1..=size as u64).map(NodeId).collect();
        let nodes: Vec<RaftNode> = (1..=size as u64).map(RaftNode::new).collect();
        let transport = ChannelTransport::new(node_ids);
        Self {
            nodes,
            transport,
            checker: Some(InvariantChecker::new()),
            check_after_transition: true,
            election_manager: ElectionManager::new(),
            replication_manager: ReplicationManager::new(),
        }
    }

    /// Create a cluster with a custom checker configuration.
    pub fn new_with_config(size: usize, config: InvariantCheckerConfig) -> Self {
        let node_ids: Vec<NodeId> = (1..=size as u64).map(NodeId).collect();
        let nodes: Vec<RaftNode> = (1..=size as u64).map(RaftNode::new).collect();
        let transport = ChannelTransport::new(node_ids);
        Self {
            nodes,
            transport,
            checker: Some(InvariantChecker::with_config(config)),
            check_after_transition: true,
            election_manager: ElectionManager::new(),
            replication_manager: ReplicationManager::new(),
        }
    }

    /// Create a cluster without invariant checking.
    pub fn new_without_checker(size: usize) -> Self {
        let node_ids: Vec<NodeId> = (1..=size as u64).map(NodeId).collect();
        let nodes: Vec<RaftNode> = (1..=size as u64).map(RaftNode::new).collect();
        let transport = ChannelTransport::new(node_ids);
        Self {
            nodes,
            transport,
            checker: None,
            check_after_transition: false,
            election_manager: ElectionManager::new(),
            replication_manager: ReplicationManager::new(),
        }
    }

    /// Replace the election manager (use `ElectionManager::buggy_duplicate_leaders()`
    /// to inject a deliberately buggy ElectionManager).
    pub fn set_election_manager(&mut self, mgr: ElectionManager) {
        self.election_manager = mgr;
    }

    /// Replace the replication manager (use `ReplicationManager::buggy_overwrite(offset)`
    /// to inject a deliberately buggy ReplicationManager).
    pub fn set_replication_manager(&mut self, mgr: ReplicationManager) {
        self.replication_manager = mgr;
    }

    /// Access the ChannelTransport.
    pub fn transport(&self) -> &ChannelTransport {
        &self.transport
    }

    /// Enable or disable post-transition invariant checking.
    pub fn set_check_after_transition(&mut self, enabled: bool) {
        self.check_after_transition = enabled;
    }

    /// Returns whether post-transition checking is active.
    pub fn check_after_transition(&self) -> bool {
        self.check_after_transition && self.checker.is_some()
    }

    /// Access the underlying checker, if present.
    pub fn checker(&self) -> Option<&InvariantChecker> {
        self.checker.as_ref()
    }

    /// Access the underlying checker mutably, if present.
    pub fn checker_mut(&mut self) -> Option<&mut InvariantChecker> {
        self.checker.as_mut()
    }

    /// Replace the checker.
    pub fn set_checker(&mut self, checker: Option<InvariantChecker>) {
        self.checker = checker;
    }

    /// Access node by index (0-based).
    pub fn node(&self, idx: usize) -> &RaftNode {
        &self.nodes[idx]
    }

    // -- Cluster operations -------------------------------------------------

    /// Elect `node_id` as leader in `term` via the `ElectionManager`.
    /// Runs invariant checks after the transition.
    pub fn elect_leader(&mut self, node_id: NodeId, term: Term) {
        self.election_manager
            .elect(&mut self.nodes, node_id, term, &self.transport);
        self.post_transition_check();
    }

    /// Propose data to the leader via the `ReplicationManager`.
    /// Returns the offset at which the entry was appended.
    pub fn propose(&mut self, data: Vec<u8>) -> u64 {
        let leader_idx = self
            .nodes
            .iter()
            .position(|n| n.role == Role::Leader)
            .expect("no leader elected");
        let offset = self
            .replication_manager
            .propose(&mut self.nodes[leader_idx], data);
        self.post_transition_check();
        offset
    }

    /// Replicate the leader's log to all followers via `ChannelTransport`.
    pub fn replicate(&mut self) {
        ReplicationManager::replicate(&mut self.nodes, &self.transport);
        self.post_transition_check();
    }

    /// Advance the high watermark on all nodes and apply committed entries.
    pub fn commit(&mut self, new_hw: u64) {
        for node in &mut self.nodes {
            let end = node.log_store.log_end_offset();
            if new_hw > node.high_watermark && new_hw <= end {
                node.high_watermark = new_hw;
                node.apply_committed();
            }
        }
        self.post_transition_check();
    }

    /// Take snapshots of all nodes.
    pub fn snapshots(&self) -> Vec<NodeSnapshot> {
        self.nodes.iter().map(|n| n.snapshot()).collect()
    }

    /// Run invariant checks against the current cluster state.
    pub fn check_invariants(&mut self) -> Vec<InvariantViolation> {
        let snaps = self.snapshots();
        match self.checker.as_mut() {
            Some(checker) => checker.check_all(&snaps),
            None => Vec::new(),
        }
    }

    /// Run invariant checks; **panic** on the first violation.
    pub fn check_invariants_or_panic(&mut self) {
        let snaps = self.snapshots();
        if let Some(checker) = self.checker.as_mut() {
            checker.check_all_or_panic(&snaps);
        }
    }

    // -- Internal -----------------------------------------------------------

    fn post_transition_check(&mut self) {
        if !self.check_after_transition {
            return;
        }
        let snaps = self.snapshots();
        if let Some(checker) = self.checker.as_mut() {
            checker.check_all_or_panic(&snaps);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Basic wiring -------------------------------------------------------

    #[test]
    fn cluster_with_checker_runs_checks() {
        let mut cluster = SimulatedCluster::new(3);
        assert!(cluster.check_after_transition());
        cluster.elect_leader(NodeId(1), Term(1));
        let v = cluster.check_invariants();
        assert!(v.is_empty());
    }

    #[test]
    fn cluster_without_checker_skips() {
        let mut cluster = SimulatedCluster::new_without_checker(3);
        assert!(!cluster.check_after_transition());
        let v = cluster.check_invariants();
        assert!(v.is_empty(), "checker disabled, should return no violations");
    }

    #[test]
    fn cluster_toggle_checking() {
        let mut cluster = SimulatedCluster::new(3);
        assert!(cluster.check_after_transition());
        cluster.set_check_after_transition(false);
        assert!(!cluster.check_after_transition());
        cluster.set_check_after_transition(true);
        assert!(cluster.check_after_transition());
    }

    #[test]
    fn cluster_custom_config_disables_election_safety() {
        let config = InvariantCheckerConfig {
            check_election_safety: false,
            ..InvariantCheckerConfig::default()
        };
        let mut cluster = SimulatedCluster::new_with_config(3, config);
        // Disable post-transition checks to manually set two leaders.
        cluster.set_check_after_transition(false);
        cluster.set_election_manager(ElectionManager::buggy_duplicate_leaders());
        cluster.elect_leader(NodeId(1), Term(1));
        cluster.elect_leader(NodeId(2), Term(1));
        cluster.set_check_after_transition(true);
        // Election safety is disabled, so no panic.
        let v = cluster.check_invariants();
        assert!(v.is_empty(), "election safety disabled: {v:?}");
    }

    #[test]
    fn nodes_use_memory_log_store() {
        let mut cluster = SimulatedCluster::new(3);
        cluster.elect_leader(NodeId(1), Term(1));
        cluster.propose(b"hello".to_vec());

        // Verify the leader's MemoryLogStore has the entry.
        let leader = cluster.node(0);
        assert_eq!(leader.log_store.log_end_offset(), 1);
        let entry = leader.log_store.entry_at_sync(0).unwrap().unwrap();
        assert_eq!(entry.payload, b"hello");
    }

    #[test]
    fn transport_routes_replication_messages() {
        let mut cluster = SimulatedCluster::new(3);
        cluster.elect_leader(NodeId(1), Term(1));
        cluster.propose(b"data".to_vec());
        cluster.replicate();

        // All nodes should have the entry in their MemoryLogStore.
        for i in 0..3 {
            let node = cluster.node(i);
            assert_eq!(
                node.log_store.log_end_offset(),
                1,
                "node {} should have 1 entry",
                i
            );
        }
    }

    // -- Scenario: all five invariants pass after 100 proposals --------------

    #[test]
    fn all_five_invariants_pass_after_100_proposals() {
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
            "all five invariants should pass after 100 proposals, got: {v:?}"
        );
    }

    // -- Scenario: append-only violation via buggy ReplicationManager --------

    #[test]
    #[should_panic(expected = "append-only leader log")]
    fn append_only_violation_via_buggy_replication_manager() {
        let mut cluster = SimulatedCluster::new(3);
        cluster.elect_leader(NodeId(1), Term(1));

        for i in 0u64..10 {
            cluster.propose(i.to_le_bytes().to_vec());
        }

        // Swap in a buggy ReplicationManager that overwrites entry at offset 5.
        cluster.set_replication_manager(ReplicationManager::buggy_overwrite(5));

        // This triggers the overwrite → post_transition_check panics.
        cluster.propose(b"BAD_OVERWRITE".to_vec());
    }

    // -- Scenario: election safety violation via buggy ElectionManager --------

    #[test]
    #[should_panic(expected = "at most one leader per term")]
    fn election_safety_violation_via_buggy_election_manager() {
        let mut cluster = SimulatedCluster::new(3);
        cluster.elect_leader(NodeId(1), Term(1));

        // Swap in a buggy ElectionManager.
        cluster.set_election_manager(ElectionManager::buggy_duplicate_leaders());

        // Node 2 also becomes leader in term 1 — panics.
        cluster.elect_leader(NodeId(2), Term(1));
    }

    // -- Proposal processing -------------------------------------------------

    #[test]
    fn propose_and_replicate_workflow() {
        let mut cluster = SimulatedCluster::new(3);
        cluster.elect_leader(NodeId(1), Term(1));

        for i in 0u64..5 {
            let off = cluster.propose(i.to_le_bytes().to_vec());
            assert_eq!(off, i);
        }

        cluster.replicate();
        cluster.commit(3);

        let snaps = cluster.snapshots();
        assert_eq!(snaps.len(), 3);
        for snap in &snaps {
            assert_eq!(snap.log_entries.len(), 5);
            assert_eq!(snap.high_watermark, 3);
            assert_eq!(snap.applied_entries.len(), 3);
        }
    }

    #[test]
    fn multi_term_leadership_transitions() {
        let mut cluster = SimulatedCluster::new(3);

        // Term 1: node 1 leads, proposes 20 entries.
        cluster.elect_leader(NodeId(1), Term(1));
        for i in 0u64..20 {
            cluster.propose(i.to_le_bytes().to_vec());
        }
        cluster.replicate();
        cluster.commit(20);

        // Term 2: node 2 takes over, proposes 30 more entries.
        cluster.elect_leader(NodeId(2), Term(2));
        for i in 20u64..50 {
            cluster.propose(i.to_le_bytes().to_vec());
        }
        cluster.replicate();
        cluster.commit(50);

        let v = cluster.check_invariants();
        assert!(v.is_empty(), "multi-term scenario should pass: {v:?}");
    }
}
