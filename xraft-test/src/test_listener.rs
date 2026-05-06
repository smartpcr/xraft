//! Test implementation of the `Listener` trait for integration testing.
//!
//! Records all commit and leader-change events so tests can verify
//! the three-phase commit notification sequence.

use xraft_core::listener::Listener;
use xraft_core::types::{AppRecord, NodeId, Term};

/// Test listener that records commit and leader-change events.
pub struct TestListener {
    /// All committed record batches, in order.
    commit_batches: Vec<Vec<AppRecord>>,
    /// Total number of committed records across all batches.
    total_committed: usize,
    /// Leader change events: (new_leader, term).
    leader_changes: Vec<(Option<NodeId>, Term)>,
}

impl TestListener {
    pub fn new() -> Self {
        Self {
            commit_batches: Vec::new(),
            total_committed: 0,
            leader_changes: Vec::new(),
        }
    }

    /// Reset all recorded events.
    pub fn reset(&mut self) {
        self.commit_batches.clear();
        self.total_committed = 0;
        self.leader_changes.clear();
    }

    /// Total number of committed records observed.
    pub fn total_committed(&self) -> usize {
        self.total_committed
    }

    /// Number of commit batches observed.
    pub fn batch_count(&self) -> usize {
        self.commit_batches.len()
    }

    /// Leader change events recorded.
    pub fn leader_changes(&self) -> &[(Option<NodeId>, Term)] {
        &self.leader_changes
    }
}

impl Default for TestListener {
    fn default() -> Self {
        Self::new()
    }
}

impl Listener for TestListener {
    fn handle_commit(&mut self, committed: &[AppRecord]) {
        self.total_committed += committed.len();
        self.commit_batches.push(committed.to_vec());
    }

    fn handle_leader_change(&mut self, new_leader: Option<NodeId>, term: Term) {
        self.leader_changes.push((new_leader, term));
    }
}
