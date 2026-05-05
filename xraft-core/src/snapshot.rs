use serde::{Deserialize, Serialize};

use crate::app_record::AppSnapshot;
use crate::types::NodeId;

/// Voter identity and network endpoint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoterInfo {
    pub node_id: NodeId,
    pub endpoint: String,
}

/// A record describing the current voter set (membership configuration).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VotersRecord {
    pub version: u64,
    pub voters: Vec<VoterInfo>,
}

/// Metadata attached to a snapshot, capturing consensus state at the
/// point the snapshot was taken.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub last_included_offset: u64,
    pub last_included_term: u64,
    pub voters: Vec<VoterInfo>,
    pub leader_epoch: u64,
}

/// A complete snapshot: consensus metadata paired with the application's
/// opaque state-machine snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub metadata: SnapshotMetadata,
    pub app_snapshot: AppSnapshot,
}

/// Unique identifier for a snapshot, used during chunked transfer.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SnapshotId {
    pub last_included_offset: u64,
    pub last_included_term: u64,
}

/// Reader for restoring a snapshot on a follower.
///
/// Wraps an in-memory snapshot that can be consumed by the application's
/// `Listener::handle_load_snapshot` callback. The reader provides access
/// to both the consensus metadata and the application snapshot payload.
///
/// Future implementations may support chunked/streaming reads; the
/// current in-memory representation is a placeholder that preserves
/// the correct API shape.
#[derive(Debug)]
pub struct SnapshotReader {
    snapshot: Snapshot,
}

impl SnapshotReader {
    /// Creates a new `SnapshotReader` from a complete `Snapshot`.
    pub fn new(snapshot: Snapshot) -> Self {
        Self { snapshot }
    }

    /// Returns the snapshot metadata (offsets, term, voters, epoch).
    pub fn metadata(&self) -> &SnapshotMetadata {
        &self.snapshot.metadata
    }

    /// Consumes the reader and returns the application snapshot payload.
    pub fn into_app_snapshot(self) -> AppSnapshot {
        self.snapshot.app_snapshot
    }

    /// Consumes the reader and returns the full `Snapshot`.
    pub fn into_snapshot(self) -> Snapshot {
        self.snapshot
    }

    /// Returns a reference to the application snapshot data.
    pub fn app_data(&self) -> &[u8] {
        &self.snapshot.app_snapshot.data
    }

    /// Returns a reference to the contained application records.
    pub fn app_snapshot(&self) -> &AppSnapshot {
        &self.snapshot.app_snapshot
    }
}

/// Writer for receiving a snapshot from the leader, chunk by chunk.
///
/// Used during snapshot transfer to incrementally build a complete
/// snapshot on the receiving node. Chunks are appended in order;
/// once all chunks are received, the writer is finalised into a
/// complete `Snapshot`.
#[derive(Debug)]
pub struct SnapshotWriter {
    metadata: SnapshotMetadata,
    chunks: Vec<u8>,
}

impl SnapshotWriter {
    /// Creates a new `SnapshotWriter` for the given snapshot metadata.
    pub fn new(metadata: SnapshotMetadata) -> Self {
        Self {
            metadata,
            chunks: Vec::new(),
        }
    }

    /// Appends a chunk of snapshot data.
    pub fn write_chunk(&mut self, data: &[u8]) {
        self.chunks.extend_from_slice(data);
    }

    /// Finalises the writer, producing a complete `Snapshot`.
    pub fn finish(self) -> Snapshot {
        Snapshot {
            metadata: self.metadata,
            app_snapshot: AppSnapshot::new(self.chunks),
        }
    }
}
