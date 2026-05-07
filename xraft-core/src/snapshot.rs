use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::app_record::AppSnapshot;
use crate::types::{Term, VoterInfo};

/// Consensus metadata stored alongside a snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    /// Last log entry offset included in this snapshot.
    pub last_included_offset: u64,
    /// Term of the last included entry.
    pub last_included_term: Term,
    /// Voter set at snapshot time.
    pub voters: Vec<VoterInfo>,
    /// Leader epoch at snapshot time.
    pub leader_epoch: Term,
}

/// Composite snapshot: consensus metadata (owned by xraft) plus application
/// state machine state (owned by the application).
///
/// The split ensures xraft can read consensus metadata (voter set, offsets)
/// without deserialising the application payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub metadata: SnapshotMetadata,
    pub app_snapshot: AppSnapshot,
}

/// Wraps a snapshot chunk stream for follower restore.
///
/// Used by `Listener::handle_load_snapshot` to read snapshot data in chunks.
/// The internal representation is intentionally opaque — concrete I/O
/// semantics are defined by the storage backend in later stages.
#[derive(Debug)]
pub struct SnapshotReader {
    /// The snapshot data available for reading.
    data: Bytes,
    /// Current read position within the data.
    position: u64,
}

impl SnapshotReader {
    /// Create a new `SnapshotReader` over the given data.
    pub fn new(data: Bytes) -> Self {
        Self { data, position: 0 }
    }

    /// Read the next chunk of up to `max_bytes` from the snapshot.
    /// Returns `(chunk, is_last)`.
    pub fn read_chunk(&mut self, max_bytes: usize) -> (Bytes, bool) {
        let start = self.position as usize;
        let end = (start + max_bytes).min(self.data.len());
        let chunk = self.data.slice(start..end);
        self.position = end as u64;
        let is_last = end >= self.data.len();
        (chunk, is_last)
    }

    /// Returns the total size of the snapshot data.
    pub fn len(&self) -> u64 {
        self.data.len() as u64
    }

    /// Returns `true` if the snapshot data is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

/// Wraps a chunked write session for receiving snapshots from the leader.
///
/// Used by `SnapshotIO::begin_receive` to accept snapshot chunks from the
/// leader during snapshot transfer. The internal representation is
/// intentionally opaque — concrete I/O semantics are defined by the storage
/// backend in later stages.
#[derive(Debug)]
pub struct SnapshotWriter {
    /// Accumulated snapshot data chunks.
    chunks: Vec<u8>,
}

impl SnapshotWriter {
    /// Create a new empty `SnapshotWriter`.
    pub fn new() -> Self {
        Self { chunks: Vec::new() }
    }

    /// Write a chunk of snapshot data.
    pub fn write_chunk(&mut self, chunk: &[u8]) {
        self.chunks.extend_from_slice(chunk);
    }

    /// Finalize the write session and return the accumulated data.
    pub fn finalize(self) -> Vec<u8> {
        self.chunks
    }

    /// Returns the number of bytes written so far.
    pub fn bytes_written(&self) -> u64 {
        self.chunks.len() as u64
    }
}

impl Default for SnapshotWriter {
    fn default() -> Self {
        Self::new()
    }
}
