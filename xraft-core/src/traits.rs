use std::io;
use std::time::Duration;

use async_trait::async_trait;

use crate::error::Result;
use crate::log_entry::LogEntry;
use crate::snapshot::{Snapshot, SnapshotId};
use crate::quorum_state::QuorumState;
use crate::app_record::{AppRecord, AppSnapshot};

/// Durable log storage.
#[async_trait]
pub trait LogStore: Send + Sync + 'static {
    /// Append entries to the log.
    async fn append(&self, entries: &[LogEntry]) -> Result<()>;
    /// Read entries in range [start_offset, end_offset).
    async fn read(&self, start_offset: u64, end_offset: u64) -> Result<Vec<LogEntry>>;
    /// Remove all entries at and after the given offset.
    async fn truncate_suffix(&self, from_offset: u64) -> Result<()>;
    /// Delete entries before the given offset.
    async fn truncate_prefix(&self, up_to_offset: u64) -> Result<()>;
    /// First offset still in the log (after compaction).
    fn log_start_offset(&self) -> u64;
    /// Next offset to be appended.
    fn log_end_offset(&self) -> u64;
    /// Read a single entry at the given offset.
    async fn entry_at(&self, offset: u64) -> Result<Option<LogEntry>>;
}

/// Durable quorum state persistence.
#[async_trait]
pub trait QuorumStateStore: Send + Sync + 'static {
    async fn load(&self) -> Result<Option<QuorumState>>;
    async fn save(&self, state: &QuorumState) -> Result<()>;
}

/// Snapshot I/O operations.
#[async_trait]
pub trait SnapshotIO: Send + Sync + 'static {
    async fn save(&self, snapshot: &Snapshot) -> Result<()>;
    async fn load_latest(&self) -> Result<Option<Snapshot>>;
    async fn read_chunk(
        &self,
        id: &SnapshotId,
        position: u64,
        max_bytes: u32,
    ) -> Result<(Vec<u8>, bool)>;
}

/// Application state machine.
pub trait StateMachine: Send + 'static {
    fn apply(&mut self, offset: u64, record: &AppRecord) -> Result<()>;
    fn snapshot(&self) -> Result<AppSnapshot>;
    fn restore(&mut self, snapshot: AppSnapshot) -> Result<()>;
}
