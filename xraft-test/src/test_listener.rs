//! Test implementation of the `Listener` trait for integration testing.
//!
//! Records all commit and leader-change events so tests can verify
//! the three-phase commit notification sequence.

use xraft_core::app_record::AppRecord;
use xraft_core::listener::Listener;
use xraft_core::snapshot::SnapshotReader;
use xraft_core::types::{NodeId, Term};

/// Test listener that records commit and leader-change events.
pub struct TestListener {
    /// All committed record batches, in order. Each entry is a vec of (offset, record) pairs.
    commit_batches: Vec<Vec<(u64, AppRecord)>>,
    /// Total number of committed records across all batches.
    total_committed: usize,
    /// Leader change events: (leader_id, term).
    leader_changes: Vec<(NodeId, Term)>,
    /// Whether a snapshot load was observed.
    snapshot_loaded: bool,
    /// Whether shutdown was signalled.
    shutdown_called: bool,
}

impl TestListener {
    pub fn new() -> Self {
        Self {
            commit_batches: Vec::new(),
            total_committed: 0,
            leader_changes: Vec::new(),
            snapshot_loaded: false,
            shutdown_called: false,
        }
    }

    /// Reset all recorded events.
    pub fn reset(&mut self) {
        self.commit_batches.clear();
        self.total_committed = 0;
        self.leader_changes.clear();
        self.snapshot_loaded = false;
        self.shutdown_called = false;
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
    pub fn leader_changes(&self) -> &[(NodeId, Term)] {
        &self.leader_changes
    }

    /// Whether a snapshot load was observed.
    pub fn snapshot_loaded(&self) -> bool {
        self.snapshot_loaded
    }

    /// Whether shutdown was signalled.
    pub fn shutdown_called(&self) -> bool {
        self.shutdown_called
    }
}

impl Default for TestListener {
    fn default() -> Self {
        Self::new()
    }
}

impl Listener for TestListener {
    fn handle_commit(&mut self, batch: &[(u64, AppRecord)]) {
        self.total_committed += batch.len();
        self.commit_batches.push(batch.to_vec());
    }

    fn handle_load_snapshot(&mut self, _reader: SnapshotReader) {
        self.snapshot_loaded = true;
    }

    fn handle_leader_change(&mut self, leader_id: NodeId, term: Term) {
        self.leader_changes.push((leader_id, term));
    }

    fn begin_shutdown(&mut self) {
        self.shutdown_called = true;
    }
}
