use async_trait::async_trait;
use bytes::Bytes;

use crate::app_record::{AppRecord, AppSnapshot};
use crate::log_entry::LogEntry;
use crate::snapshot::{Snapshot, SnapshotId, SnapshotWriter};

/// Durable log storage. All mutating methods take `&self` with interior
/// mutability (e.g., `tokio::sync::Mutex<File>`) and `Sync` bound, matching
/// the IoStage concurrent dispatch model (architecture §4.1).
#[async_trait]
pub trait LogStore: Send + Sync + 'static {
    /// Append entries. Must fsync before returning Ok.
    async fn append(&self, entries: &[LogEntry]) -> std::io::Result<()>;

    /// Read entries in [start_offset, end_offset).
    async fn read(&self, start_offset: u64, end_offset: u64) -> std::io::Result<Vec<LogEntry>>;

    /// Truncate the log suffix starting at the given offset (for divergence).
    async fn truncate_suffix(&self, from_offset: u64) -> std::io::Result<()>;

    /// Truncate the log prefix up to the given offset (after snapshot).
    /// All entries before `up_to_offset` are removed.
    async fn truncate_prefix(&self, up_to_offset: u64) -> std::io::Result<()>;

    /// The first offset still in the log.
    fn log_start_offset(&self) -> u64;

    /// The next offset to be written (one past the last entry).
    fn log_end_offset(&self) -> u64;

    /// Read the entry at the given offset.
    async fn entry_at(&self, offset: u64) -> std::io::Result<Option<LogEntry>>;
}

/// Snapshot persistence. Writes are atomic (write-to-temp, fsync, rename).
#[async_trait]
pub trait SnapshotIO: Send + Sync + 'static {
    /// Write a complete snapshot atomically. Must fsync before returning Ok.
    async fn save(&self, snapshot: &Snapshot) -> std::io::Result<()>;

    /// Load the latest snapshot, if any.
    async fn load_latest(&self) -> std::io::Result<Option<Snapshot>>;

    /// Read a chunk of a snapshot at the given byte position.
    async fn read_chunk(
        &self,
        id: &SnapshotId,
        position: u64,
        max_bytes: u32,
    ) -> std::io::Result<(Bytes, bool)>;

    /// Begin writing a snapshot received from a leader, chunk by chunk.
    async fn begin_receive(&self, id: &SnapshotId) -> std::io::Result<SnapshotWriter>;
}

/// Network transport for sending RPCs to peer nodes.
///
/// Injected into `IoStage` so that `SendRpc` actions execute through a real
/// (or test-mock) transport rather than a hardcoded placeholder.
#[async_trait]
pub trait NetworkSender: Send + Sync + 'static {
    /// Send an RPC payload to the given peer. The payload is an opaque byte
    /// buffer; higher-level RPC envelope framing is defined in a later stage.
    async fn send(&self, target: crate::types::NodeId, data: Vec<u8>) -> std::io::Result<()>;
}

/// Application state machine. Synchronous trait (not async) per architecture
/// §4.1 — callbacks are invoked by the EventLoop, not the IoStage.
pub trait StateMachine: Send + 'static {
    /// Apply a committed command entry to the state machine.
    fn apply(&mut self, offset: u64, record: &AppRecord) -> std::io::Result<()>;

    /// Take a point-in-time snapshot of the current state machine state.
    fn snapshot(&self) -> std::io::Result<AppSnapshot>;

    /// Restore the state machine from a snapshot.
    fn restore(&mut self, snapshot: AppSnapshot) -> std::io::Result<()>;
}
