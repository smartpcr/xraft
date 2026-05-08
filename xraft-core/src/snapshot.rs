use serde::{Deserialize, Serialize};

use crate::app_record::AppSnapshot;
use crate::types::Term;
use crate::voter::VoterInfo;

/// Consensus metadata for a snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub last_included_offset: u64,
    pub last_included_term: Term,
    pub voters: Vec<VoterInfo>,
    pub leader_epoch: Term,
}

/// Complete snapshot: consensus metadata + application payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub metadata: SnapshotMetadata,
    pub app_snapshot: AppSnapshot,
}

/// Wraps a snapshot chunk stream for follower restore.
pub struct SnapshotReader {
    pub data: Vec<u8>,
}

/// Wraps a chunked write session for receiving snapshots from leader.
pub struct SnapshotWriter {
    pub data: Vec<u8>,
}
