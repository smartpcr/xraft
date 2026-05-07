use serde::{Deserialize, Serialize};

use crate::app_record::AppSnapshot;
use crate::types::Term;
use crate::voter::VoterInfo;

/// Consensus metadata stored alongside every snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub last_included_offset: u64,
    pub last_included_term: Term,
    pub voters: Vec<VoterInfo>,
    pub leader_epoch: Term,
}

/// A complete snapshot: consensus metadata + application payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub metadata: SnapshotMetadata,
    pub app_snapshot: AppSnapshot,
}

/// Identifies a snapshot by its last included offset and epoch.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SnapshotId {
    pub end_offset: u64,
    pub epoch: Term,
}

/// Opaque handle for writing snapshot chunks received from a leader.
pub struct SnapshotWriter {
    _private: (),
}

impl SnapshotWriter {
    /// Create a new `SnapshotWriter`. Actual I/O backing will be provided
    /// by concrete `SnapshotIO` implementations.
    pub fn new() -> Self {
        Self { _private: () }
    }
}

impl Default for SnapshotWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// Opaque handle for reading snapshot chunks during follower restore.
pub struct SnapshotReader {
    _private: (),
}

impl SnapshotReader {
    /// Create a new `SnapshotReader`. Actual I/O backing will be provided
    /// by concrete `SnapshotIO` implementations.
    pub fn new() -> Self {
        Self { _private: () }
    }
}

impl Default for SnapshotReader {
    fn default() -> Self {
        Self::new()
    }
}
