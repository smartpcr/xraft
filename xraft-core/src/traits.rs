use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::time::Instant;

use crate::app_record::{AppRecord, AppSnapshot};
use crate::error::Result;
use crate::log_entry::LogEntry;
use crate::quorum_state::QuorumState;
use crate::rpc::RpcEnvelope;
use crate::snapshot::{Snapshot, SnapshotId, SnapshotMetadata, SnapshotWriter};
use crate::types::NodeId;

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

    /// The next offset to be assigned (exclusive upper bound).
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
        metadata: SnapshotMetadata,
    ) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Transport traits
// ---------------------------------------------------------------------------

/// Trait for sending RPC messages to peers.
///
/// A single `TransportSender` is shared across the `IoStage` and routes
/// envelopes to different peers based on the `target` argument.
#[async_trait]
pub trait TransportSender: Send + Sync + 'static {
    /// Send an RPC envelope to the given peer. Returns once the message
    /// has been handed off to the transport layer (not necessarily
    /// acknowledged by the remote end).
    async fn send(&self, target: NodeId, message: RpcEnvelope) -> Result<()>;
}

/// Trait for receiving inbound RPC messages.
///
/// `recv` takes `&mut self` because only the single `ReceiverTask` reads
/// from the network and the underlying `tokio::sync::mpsc::Receiver::recv`
/// requires exclusive access.
#[async_trait]
pub trait TransportReceiver: Send + 'static {
    /// Block until the next inbound RPC envelope arrives.
    async fn recv(&mut self) -> Result<RpcEnvelope>;
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
///
/// Synchronous (not `#[async_trait]`) per architecture §4.1: application
/// callbacks are invoked synchronously by the `EventLoop`.
pub trait StateMachine: Send + 'static {
    /// Apply a committed record to the state machine at the given offset.
    fn apply(&mut self, offset: u64, record: &AppRecord) -> Result<()>;

    /// Produce a point-in-time snapshot of the current state.
    fn snapshot(&self) -> Result<AppSnapshot>;

    /// Restore state from a previously captured snapshot.
    fn restore(&mut self, snapshot: AppSnapshot) -> Result<()>;
}
