use serde::{Deserialize, Serialize};

use crate::app_record::AppSnapshot;
use crate::voter::VoterInfo;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::io;
use std::path::PathBuf;
use tokio::sync::Mutex;

/// Identifier for a snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SnapshotId {
    pub end_offset: u64,
    pub epoch: u64,
}

/// Metadata for a snapshot (consensus side).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub last_included_offset: u64,
    pub last_included_term: u64,
    pub voters: Vec<VoterInfo>,
    pub leader_epoch: u64,
}

/// A complete snapshot: consensus metadata + application payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub metadata: SnapshotMetadata,
    pub app_snapshot: AppSnapshot,
}

/// Writer for receiving a snapshot chunk by chunk from a leader.
pub struct SnapshotWriter {
    pub id: SnapshotId,
    pub data: Vec<u8>,
}

impl SnapshotWriter {
    pub fn new(id: SnapshotId) -> Self {
        Self {
            id,
            data: Vec::new(),
        }
    }

    pub fn write_chunk(&mut self, chunk: &[u8]) {
        self.data.extend_from_slice(chunk);
    }

    pub fn id(&self) -> &SnapshotId {
        &self.id
    }
}

/// Reader for delivering a snapshot to a follower for restore.
pub struct SnapshotReader {
    pub data: Vec<u8>,
}

/// A reader for snapshot chunks (used during follower restore).
#[derive(Debug)]
pub struct SnapshotReader {
    pub data: Bytes,
    pub metadata: SnapshotMetadata,
}
