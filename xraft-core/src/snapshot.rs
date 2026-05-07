use serde::{Deserialize, Serialize};

use crate::app_record::AppSnapshot;
use crate::types::{Term, VoterInfo};

/// Consensus metadata included in every snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    /// Last log entry offset included in this snapshot.
    pub last_included_offset: u64,
    /// Term of the last included entry.
    pub last_included_term: Term,
    /// Voter set at the time the snapshot was taken (committed voters).
    pub voters: Vec<VoterInfo>,
    /// Leader epoch at snapshot time.
    pub leader_epoch: Term,
}

/// Complete snapshot: consensus metadata + application state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub metadata: SnapshotMetadata,
    pub app_snapshot: AppSnapshot,
}

/// Identifier for a snapshot (used in FetchSnapshot RPCs).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotId {
    pub end_offset: u64,
    pub epoch: Term,
}
