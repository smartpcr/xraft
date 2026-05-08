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

/// Composite snapshot: consensus metadata + application payload.
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
    pub chunks: Vec<Vec<u8>>,
}

impl SnapshotWriter {
    pub fn new() -> Self {
        Self { chunks: Vec::new() }
    }

    pub fn write_chunk(&mut self, chunk: &[u8]) {
        self.chunks.push(chunk.to_vec());
    }

    pub fn finalize(self) -> Vec<u8> {
        self.chunks.into_iter().flatten().collect()
    }
}

impl Default for SnapshotWriter {
    fn default() -> Self {
        Self::new()
    }
}
