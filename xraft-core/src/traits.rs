use std::io;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::time::Instant;

use crate::log_entry::LogEntry;
use crate::quorum_state::QuorumState;
use crate::rpc::{RpcEnvelope, SnapshotId};
use crate::snapshot::{Snapshot, SnapshotWriter};
use crate::types::NodeId;

/// Durable log storage trait.
#[async_trait]
pub trait LogStore: Send + Sync + 'static {
    async fn append(&self, entries: &[LogEntry]) -> Result<(), io::Error>;
    async fn read(&self, start_offset: u64, end_offset: u64) -> Result<Vec<LogEntry>, io::Error>;
    async fn truncate_suffix(&self, from_offset: u64) -> Result<(), io::Error>;
    async fn truncate_prefix(&self, up_to_offset: u64) -> Result<(), io::Error>;
    fn log_start_offset(&self) -> u64;
    fn log_end_offset(&self) -> u64;
    async fn entry_at(&self, offset: u64) -> Result<Option<LogEntry>, io::Error>;
}

/// Durable, append-only log store.
///
/// All mutating methods take `&self` — implementations use interior
/// mutability (e.g., `tokio::sync::Mutex<File>`) consistent with the
/// `Send + Sync` bound. The `IoStage` holds an owned `Box<dyn LogStore>`
/// and invokes methods via `&self`.
#[async_trait]
pub trait LogStore: Send + Sync + 'static {
    /// Append entries. Must fsync before returning Ok.
    async fn append(&self, entries: &[LogEntry]) -> Result<()>;

    /// Read entries in `[start_offset, end_offset)`.
    async fn read(&self, start_offset: u64, end_offset: u64) -> Result<Vec<LogEntry>>;

    /// Truncate the log suffix starting at the given offset (for divergence).
    /// Removes all entries at and after `from_offset`.
    async fn truncate_suffix(&self, from_offset: u64) -> Result<()>;

    /// Truncate the log prefix up to the given offset (after snapshot).
    /// Deletes segment files whose entries are all before `up_to_offset`.
    async fn truncate_prefix(&self, up_to_offset: u64) -> Result<()>;

    /// The first offset still in the log.
    fn log_start_offset(&self) -> u64;

    /// The next offset to be written (one past the last entry).
    fn log_end_offset(&self) -> u64;

    /// Read the entry at the given offset, returning `None` if out of bounds.
    async fn entry_at(&self, offset: u64) -> Result<Option<LogEntry>>;
}
