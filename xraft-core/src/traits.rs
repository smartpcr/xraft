use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::time::Instant;

use crate::app_record::{AppRecord, AppSnapshot};
use crate::error::Result;
use crate::log_entry::LogEntry;
use crate::quorum_state::QuorumState;
use crate::rpc::RpcEnvelope;
use crate::snapshot::{Snapshot, SnapshotId, SnapshotWriter};

// ---------------------------------------------------------------------------
// Storage traits
// ---------------------------------------------------------------------------

/// Trait for log storage. All methods take `&self` — implementations use
/// interior mutability (e.g., `tokio::sync::Mutex`) consistent with the
/// `Send + Sync` bound.
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

    /// Read the entry at the given offset.
    async fn entry_at(&self, offset: u64) -> Result<Option<LogEntry>>;
}

/// Trait for quorum state persistence. Must fsync before returning Ok.
#[async_trait]
pub trait QuorumStateStore: Send + Sync + 'static {
    /// Load persisted quorum state.
    async fn load(&self) -> Result<Option<QuorumState>>;

    /// Persist quorum state. Must fsync before returning Ok.
    async fn save(&self, state: &QuorumState) -> Result<()>;
}

/// Trait for snapshot I/O. Method names match architecture §4.1.
#[async_trait]
pub trait SnapshotIO: Send + Sync + 'static {
    /// Write a complete snapshot atomically.
    async fn save(&self, snapshot: &Snapshot) -> Result<()>;

    /// Load the latest snapshot, if any.
    async fn load_latest(&self) -> Result<Option<Snapshot>>;

    /// Read a chunk of the snapshot at the given byte position.
    /// Returns (data, is_last_chunk).
    async fn read_chunk(
        &self,
        id: &SnapshotId,
        position: u64,
        max_bytes: u32,
    ) -> Result<(Bytes, bool)>;

    /// Begin writing a snapshot received from a leader, chunk by chunk.
    async fn begin_receive(&self, id: &SnapshotId) -> Result<SnapshotWriter>;

    /// Finalize a received snapshot, persisting the accumulated data.
    async fn complete_receive(
        &self,
        writer: SnapshotWriter,
        metadata: crate::snapshot::SnapshotMetadata,
    ) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Transport traits
// ---------------------------------------------------------------------------

/// Trait for sending RPC messages to peers.
///
/// Each `TransportSender` instance is typically bound to a single peer
/// connection, so no target address is needed at send time.
#[async_trait]
pub trait TransportSender: Send + Sync + 'static {
    /// Send an RPC envelope to the peer. Returns once the message has been
    /// handed off to the transport layer (not necessarily acknowledged).
    async fn send(&self, envelope: RpcEnvelope) -> Result<()>;
}

/// Trait for receiving inbound RPC messages.
#[async_trait]
pub trait TransportReceiver: Send + 'static {
    /// Block until the next inbound RPC envelope arrives.
    async fn recv(&self) -> Result<RpcEnvelope>;
}

// ---------------------------------------------------------------------------
// Runtime traits
// ---------------------------------------------------------------------------

/// Clock trait for time management. Used directly by the EventLoop
/// for timer management, NOT mediated by IoAction.
#[async_trait]
pub trait Clock: Send + 'static {
    /// Current instant.
    fn now(&self) -> Instant;

    /// Sleep until the given deadline.
    async fn sleep_until(&self, deadline: Instant);

    /// Generate a random election timeout in [min, max].
    fn random_election_timeout(&self) -> Duration;
}

/// Trait for the application state machine driven by committed log entries.
#[async_trait]
pub trait StateMachine: Send + Sync + 'static {
    /// Apply a committed record to the state machine.
    async fn apply(&self, record: AppRecord) -> Result<()>;

    /// Produce a point-in-time snapshot of the current state.
    async fn snapshot(&self) -> Result<AppSnapshot>;

    /// Restore state from a previously captured snapshot.
    async fn restore(&self, snapshot: AppSnapshot) -> Result<()>;
}