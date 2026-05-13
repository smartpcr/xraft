use async_trait::async_trait;
use bytes::Bytes;

use crate::error::Result;
use crate::log_entry::LogEntry;
use crate::quorum_state::QuorumState;
use crate::snapshot::{Snapshot, SnapshotId, SnapshotWriter};

/// Trait for durable log storage. All methods take `&self` with interior
/// mutability (e.g., `tokio::sync::Mutex<File>`) consistent with `Send + Sync`.
#[async_trait]
pub trait LogStore: Send + Sync + 'static {
    /// Append entries. Must fsync before returning Ok.
    async fn append(&self, entries: &[LogEntry]) -> Result<()>;

    /// Read entries in [start_offset, end_offset).
    async fn read(&self, start_offset: u64, end_offset: u64) -> Result<Vec<LogEntry>>;

    /// Truncate the log suffix starting at the given offset (for divergence).
    async fn truncate_suffix(&self, from_offset: u64) -> Result<()>;

    /// Truncate the log prefix up to the given offset (after snapshot).
    async fn truncate_prefix(&self, up_to_offset: u64) -> Result<()>;

    /// The first offset still in the log.
    fn log_start_offset(&self) -> u64;
    fn log_end_offset(&self) -> u64;

    /// Read the entry at the given offset. Returns None if out of bounds.
    async fn entry_at(&self, offset: u64) -> Result<Option<LogEntry>>;
}

/// Trait for persisting quorum (voting) state.
#[async_trait]
pub trait QuorumStateStore: Send + Sync + 'static {
    /// Load persisted quorum state. Returns None if no state file exists.
    async fn load(&self) -> Result<Option<QuorumState>>;

    /// Persist quorum state. Must fsync before returning Ok.
    async fn save(&self, state: &QuorumState) -> Result<()>;
}

/// Trait for snapshot I/O.
#[async_trait]
pub trait SnapshotIO: Send + Sync + 'static {
    /// Write a complete snapshot atomically.
    async fn save(&self, snapshot: &Snapshot) -> Result<()>;

    /// Load the latest snapshot, if any.
    async fn load_latest(&self) -> Result<Option<Snapshot>>;

    /// Read a chunk of the snapshot at the given byte position.
    async fn read_chunk(
        &self,
        id: &SnapshotId,
        position: u64,
        max_bytes: u32,
    ) -> Result<(Bytes, bool)>;

    /// Begin writing a snapshot received from a leader, chunk by chunk.
    async fn begin_receive(&self, id: &SnapshotId) -> Result<SnapshotWriter>;
}