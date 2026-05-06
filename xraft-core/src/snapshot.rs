use crate::app_record::AppSnapshot;
use crate::types::Term;
use crate::voter::VoterInfo;

/// Unique identifier for a snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SnapshotId(pub String);

/// Consensus metadata included in every snapshot.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnapshotMetadata {
    pub last_included_offset: u64,
    pub last_included_term: Term,
    pub voters: Vec<VoterInfo>,
    pub leader_epoch: Term,
}

/// A complete snapshot: consensus metadata + application state.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Snapshot {
    pub metadata: SnapshotMetadata,
    pub app_snapshot: AppSnapshot,
}

/// Writer for receiving snapshot chunks from a leader.
pub struct SnapshotWriter {
    pub id: SnapshotId,
}

/// Reader for serving snapshot chunks to a follower
/// (used by `Listener::handle_load_snapshot` and `FetchSnapshot` RPC).
pub struct SnapshotReader {
    pub id: SnapshotId,
}
