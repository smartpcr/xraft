use crate::app_record::AppSnapshot;
use crate::types::Term;
use crate::voter::VoterInfo;
use serde::{Deserialize, Serialize};

/// Consensus metadata stored alongside the snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub last_included_offset: u64,
    pub last_included_term: Term,
    pub voters: Vec<VoterInfo>,
    pub leader_epoch: Term,
}

/// Complete snapshot: consensus metadata + application state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub metadata: SnapshotMetadata,
    pub app_snapshot: AppSnapshot,
}

/// Wraps a snapshot chunk stream for follower restore (placeholder for Phase 7).
pub struct SnapshotReader {
    pub snapshot: Snapshot,
}

/// Wraps a chunked write session for receiving snapshots (placeholder for Phase 7).
pub struct SnapshotWriter;
