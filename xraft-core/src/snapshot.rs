use serde::{Deserialize, Serialize};

use crate::app_record::AppSnapshot;
use crate::types::{Term, VoterInfo};

/// Consensus metadata included in a snapshot.
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

/// A complete snapshot: consensus metadata + application payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub metadata: SnapshotMetadata,
    pub app_snapshot: AppSnapshot,
}

/// Writer handle for receiving a snapshot chunk-by-chunk from a leader.
///
/// Backend-agnostic: the inner implementation is provided by the storage layer
/// via a boxed trait object, keeping `xraft-core` free of file-system details.
pub struct SnapshotWriter {
    inner: Box<dyn SnapshotWriterInner>,
}

/// Internal trait that storage backends implement for chunked snapshot writes.
#[async_trait::async_trait]
pub trait SnapshotWriterInner: Send {
    /// Append a data chunk to the in-progress snapshot.
    async fn write_chunk(&mut self, data: &[u8]) -> std::io::Result<()>;

    /// Finalize the snapshot: fsync + atomic rename.
    async fn finalize(self: Box<Self>) -> std::io::Result<()>;
}

impl SnapshotWriter {
    /// Create a new `SnapshotWriter` wrapping a storage-provided backend.
    pub fn new(inner: Box<dyn SnapshotWriterInner>) -> Self {
        Self { inner }
    }

    /// Append a data chunk to the in-progress snapshot.
    pub async fn write_chunk(&mut self, data: &[u8]) -> std::io::Result<()> {
        self.inner.write_chunk(data).await
    }

    /// Finalize the snapshot: fsync + atomic rename to final path.
    pub async fn finalize(self) -> std::io::Result<()> {
        self.inner.finalize().await
    }
}

impl std::fmt::Debug for SnapshotWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotWriter").finish_non_exhaustive()
    }
}

/// Reader handle for streaming snapshot chunks to a follower.
/// Placeholder for future workstreams.
#[derive(Debug)]
pub struct SnapshotReader {
    _private: (),
}
