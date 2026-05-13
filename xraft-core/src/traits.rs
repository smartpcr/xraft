//! Stage 1.4: trait definitions for Storage, Transport, Runtime, and
//! the application `StateMachine`.
//!
//! Grouping (per architecture §4.1 / §4.4):
//!
//! * **Storage / I/O** — `LogStore`, `QuorumStateStore`, `SnapshotIO`,
//!   `TransportSender`. Mediated by the `IoStage`, may be called
//!   concurrently from multiple tasks, therefore `Send + Sync +
//!   'static` and take `&self` with interior mutability.
//! * **Transport receive** — `TransportReceiver`. Single consumer
//!   (the `ReceiverTask`); `Send + 'static`, takes `&mut self`.
//! * **Runtime** — `Clock`. Used directly by the `EventLoop` for timer
//!   management; `Send + 'static` (single-threaded event loop, no
//!   `Sync` required), `#[async_trait]` for `Box<dyn Clock>` object
//!   safety.
//! * **Application** — `StateMachine`. Synchronous callback driven by
//!   the `EventLoop`; `Send + 'static`.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;

use crate::app_record::{AppRecord, AppSnapshot};
use crate::error::Result;
use crate::log_entry::LogEntry;
use crate::quorum_state::QuorumState;
use crate::rpc::RpcEnvelope;
use crate::snapshot::{Snapshot, SnapshotId, SnapshotWriter};
use crate::types::NodeId;

// ---------------------------------------------------------------------
// Storage / I/O group
// ---------------------------------------------------------------------

/// Durable replicated-log storage.
///
/// All methods take `&self`; concrete implementations use interior
/// mutability (e.g. `tokio::sync::Mutex<File>`) so that the `IoStage`
/// can dispatch multiple log operations concurrently
/// (architecture §4.1).
#[async_trait]
pub trait LogStore: Send + Sync + 'static {
    /// Append `entries` and fsync before returning `Ok`.
    async fn append(&self, entries: &[LogEntry]) -> Result<()>;

    /// Read entries in the half-open range `[start_offset, end_offset)`.
    async fn read(&self, start_offset: u64, end_offset: u64) -> Result<Vec<LogEntry>>;

    /// Truncate the log suffix starting at `from_offset`
    /// (used on divergence with the leader's log).
    async fn truncate_suffix(&self, from_offset: u64) -> Result<()>;

    /// Truncate the log prefix up to (but not including) `up_to_offset`
    /// (used after a snapshot is durable).
    async fn truncate_prefix(&self, up_to_offset: u64) -> Result<()>;

    /// Offset of the first entry still present in the log
    /// (`log_end_offset` if the log is empty).
    fn log_start_offset(&self) -> u64;

    /// Exclusive upper bound — one past the last appended offset.
    fn log_end_offset(&self) -> u64;

    /// Read a single entry by offset. Returns `Ok(None)` if the offset
    /// is outside `[log_start_offset, log_end_offset)`.
    async fn entry_at(&self, offset: u64) -> Result<Option<LogEntry>>;
}

/// Persisted voting / quorum state required for crash recovery.
#[async_trait]
pub trait QuorumStateStore: Send + Sync + 'static {
    /// Load persisted state. Returns `Ok(None)` when no state file
    /// exists (fresh node).
    async fn load(&self) -> Result<Option<QuorumState>>;

    /// Persist `state` and fsync before returning `Ok`.
    async fn save(&self, state: &QuorumState) -> Result<()>;
}

/// Snapshot read / write I/O.
#[async_trait]
pub trait SnapshotIO: Send + Sync + 'static {
    /// Persist a complete snapshot atomically.
    async fn save(&self, snapshot: &Snapshot) -> Result<()>;

    /// Load the most recent snapshot, if any.
    async fn load_latest(&self) -> Result<Option<Snapshot>>;

    /// Read up to `max_bytes` of snapshot `id` starting at `position`.
    /// Returns `(chunk, is_last_chunk)`.
    async fn read_chunk(
        &self,
        id: &SnapshotId,
        position: u64,
        max_bytes: u32,
    ) -> Result<(Bytes, bool)>;

    /// Begin a chunked receive session for a snapshot streamed from
    /// the leader.
    async fn begin_receive(&self, id: &SnapshotId) -> Result<SnapshotWriter>;
}

/// Outbound side of the network transport.
///
/// `&self` because the `IoStage` may dispatch sends to multiple peers
/// concurrently from a shared sender handle (architecture §4.4).
#[async_trait]
pub trait TransportSender: Send + Sync + 'static {
    async fn send(&self, target: NodeId, message: RpcEnvelope) -> Result<()>;
}

// ---------------------------------------------------------------------
// Transport receive (single consumer)
// ---------------------------------------------------------------------

/// Inbound side of the network transport.
///
/// `&mut self` because the `ReceiverTask` is the single owner of the
/// receive side (architecture §4.4); no `Sync` required.
#[async_trait]
pub trait TransportReceiver: Send + 'static {
    /// Await the next inbound envelope.
    async fn recv(&mut self) -> Result<RpcEnvelope>;
}

// ---------------------------------------------------------------------
// Runtime group
// ---------------------------------------------------------------------

/// Pluggable time source used by the `EventLoop` for timer management
/// (election timeouts, fetch intervals, check-quorum deadlines).
///
/// `#[async_trait]` is required so that `sleep_until` (an async method)
/// remains object-safe behind `Box<dyn Clock>`. The trait is
/// `Send + 'static` (not `Sync`) because only the single-threaded
/// event loop calls it.
#[async_trait]
pub trait Clock: Send + 'static {
    /// Current monotonic instant.
    fn now(&self) -> Instant;

    /// Sleep until `deadline`. Returns immediately if `deadline` is in
    /// the past.
    async fn sleep_until(&self, deadline: Instant);

    /// Draw a uniformly-random election timeout from the configured
    /// `[election_timeout_min, election_timeout_max]` range.
    fn random_election_timeout(&self) -> Duration;
}

// ---------------------------------------------------------------------
// Application group
// ---------------------------------------------------------------------

/// Application-supplied state machine driven by the `EventLoop`.
///
/// Synchronous on purpose (architecture §4.1): `apply` / `snapshot` /
/// `restore` are called inline by the event loop and must not block
/// the consensus tasks. Long-running work belongs in a separate
/// application task driven via channels.
pub trait StateMachine: Send + 'static {
    /// Apply a committed command at `offset`.
    fn apply(&mut self, offset: u64, record: &AppRecord) -> Result<()>;

    /// Produce a snapshot of the current application state.
    fn snapshot(&self) -> Result<AppSnapshot>;

    /// Restore application state from a snapshot.
    fn restore(&mut self, snapshot: AppSnapshot) -> Result<()>;
}

// ---------------------------------------------------------------------
// Static object-safety assertions
// ---------------------------------------------------------------------
//
// These force a compile error if any Stage 1.4 trait stops being
// object-safe (which would break trait-object injection into
// `RaftNode` per architecture §4.1).

#[doc(hidden)]
mod object_safety {
    use super::*;

    #[allow(dead_code)]
    fn assert_object_safe<T: ?Sized>() {}

    #[allow(dead_code)]
    fn _checks() {
        assert_object_safe::<dyn LogStore>();
        assert_object_safe::<dyn QuorumStateStore>();
        assert_object_safe::<dyn SnapshotIO>();
        assert_object_safe::<dyn TransportSender>();
        assert_object_safe::<dyn TransportReceiver>();
        assert_object_safe::<dyn Clock>();
        assert_object_safe::<dyn StateMachine>();
    }
}
