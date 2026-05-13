//! Stage 1.4 — pluggable trait contracts.
//!
//! Three categories, distinguished by their caller and bounds (per
//! architecture §4.1):
//!
//! 1. **Storage / Network-Send I/O** — async, `Send + Sync + 'static`,
//!    every method takes `&self`. Implementations are injected as
//!    `Box<dyn …>` trait objects into the `IoStage`, which fans out
//!    `IoAction`s concurrently across multiple peers. The `Sync` bound
//!    is required because the stage may borrow `&self` simultaneously
//!    from several spawned tasks within one batch; implementations use
//!    interior mutability (e.g., `tokio::sync::Mutex<File>`) for the
//!    write paths.
//!
//!    These traits are: [`LogStore`], [`QuorumStateStore`],
//!    [`SnapshotIO`], [`TransportSender`].
//!
//! 2. **Runtime** — async, `Send + 'static` (no `Sync`), used by the
//!    single-threaded callers (`EventLoop`, `ReceiverTask`). Injected
//!    as `Box<dyn …>` trait objects but never shared across tasks.
//!
//!    These traits are: [`TransportReceiver`] (`recv` takes
//!    `&mut self` — only the `ReceiverTask` reads from the network)
//!    and [`Clock`] (used by the `EventLoop` for timer management; not
//!    mediated by `IoAction`).
//!
//! 3. **Application** — synchronous (no `#[async_trait]`), invoked by
//!    the `EventLoop` during commit processing *before* any
//!    `IoAction` is dispatched. Monomorphised as a generic parameter
//!    on `RaftNode<S, L>` rather than `Box<dyn …>`.
//!
//!    This category is [`StateMachine`].

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
use crate::types::NodeId;

// =========================================================================
// Storage / Network-Send I/O traits
// =========================================================================

/// Durable, append-only replicated log.
///
/// Every method takes `&self` — implementations use interior
/// mutability (e.g., `tokio::sync::Mutex<File>`) so the `IoStage` can
/// hold shared borrows of the trait object while concurrent
/// `AppendLog` / `TruncateSuffix` / read operations are in flight.
#[async_trait]
pub trait LogStore: Send + Sync + 'static {
    /// Append `entries` to the tail of the log.
    ///
    /// Must be durable (`fsync` or equivalent) before returning `Ok`.
    /// Implementations must reject non-contiguous appends.
    async fn append(&self, entries: &[LogEntry]) -> Result<()>;

    /// Read entries in the half-open range `[start_offset, end_offset)`.
    async fn read(&self, start_offset: u64, end_offset: u64) -> Result<Vec<LogEntry>>;

    /// Truncate the log *suffix* starting at `from_offset` (inclusive),
    /// used when the leader's log diverges from this node's.
    async fn truncate_suffix(&self, from_offset: u64) -> Result<()>;

    /// Truncate the log *prefix* up to `up_to_offset` (exclusive),
    /// used after a snapshot has covered earlier entries.
    async fn truncate_prefix(&self, up_to_offset: u64) -> Result<()>;

    /// First offset still present in the log (inclusive).
    fn log_start_offset(&self) -> u64;

    /// Next offset to be assigned by `append` (exclusive upper bound).
    fn log_end_offset(&self) -> u64;

    /// Read the entry at `offset`, or `None` if outside `[start, end)`.
    async fn entry_at(&self, offset: u64) -> Result<Option<LogEntry>>;
}

/// Durable storage for the per-node quorum state record.
#[async_trait]
pub trait QuorumStateStore: Send + Sync + 'static {
    /// Load the persisted state, or `None` if no state has been written.
    async fn load(&self) -> Result<Option<QuorumState>>;

    /// Persist `state` durably (`fsync` or equivalent) before returning.
    async fn save(&self, state: &QuorumState) -> Result<()>;
}

/// Durable storage and chunked transfer for state-machine snapshots.
#[async_trait]
pub trait SnapshotIO: Send + Sync + 'static {
    /// Write a complete snapshot atomically.
    async fn save(&self, snapshot: &Snapshot) -> Result<()>;

    /// Load the most recent snapshot, if any.
    async fn load_latest(&self) -> Result<Option<Snapshot>>;

    /// Read a chunk of the snapshot identified by `id` starting at
    /// byte `position`, up to `max_bytes`. Returns `(bytes, is_last)`.
    async fn read_chunk(
        &self,
        id: &SnapshotId,
        position: u64,
        max_bytes: u32,
    ) -> Result<(Bytes, bool)>;

    /// Begin receiving a snapshot from the leader. Returns a writer
    /// session that subsequent `FetchSnapshotResponse` chunks are
    /// appended to.
    async fn begin_receive(&self, id: &SnapshotId) -> Result<SnapshotWriter>;
}

/// Outbound side of the network transport.
///
/// `send` takes `&self` so the `IoStage` can dispatch RPCs to many
/// peers concurrently from a single owned trait object.
#[async_trait]
pub trait TransportSender: Send + Sync + 'static {
    /// Send `message` to `target`. Returns once the message has been
    /// handed to the transport — the call does **not** wait for an
    /// application-level ack.
    async fn send(&self, target: NodeId, message: RpcEnvelope) -> Result<()>;
}

// =========================================================================
// Runtime traits
// =========================================================================

/// Inbound side of the network transport.
///
/// `recv` takes `&mut self` because only the single `ReceiverTask`
/// reads from the network. Splitting the transport into a
/// `Send + Sync` sender and a `Send`-only receiver (rather than a
/// single `Send + Sync` trait) mirrors `tokio::sync::mpsc::Receiver`
/// and avoids forcing implementations to wrap their receive half in a
/// `Mutex`.
#[async_trait]
pub trait TransportReceiver: Send + 'static {
    /// Block until the next inbound RPC arrives.
    async fn recv(&mut self) -> Result<RpcEnvelope>;
}

/// Time source used by the `EventLoop` for timer management.
///
/// `Clock` is a *Runtime* trait — it is consumed directly by the
/// single-threaded event loop and is **not** mediated by `IoAction` /
/// `IoStage` (architecture §4.1). It is injected as
/// `Box<dyn Clock>`; the `#[async_trait]` attribute is required for
/// `async fn sleep_until` to be object-safe under `dyn Clock`.
#[async_trait]
pub trait Clock: Send + 'static {
    /// The current instant according to this clock.
    fn now(&self) -> Instant;

    /// Suspend until `deadline` has been reached.
    async fn sleep_until(&self, deadline: Instant);

    /// Generate a new randomised election-timeout duration.
    fn random_election_timeout(&self) -> Duration;
}

// =========================================================================
// Application trait
// =========================================================================

/// The user-supplied state machine driven by committed log entries.
///
/// Synchronous (intentionally **not** `#[async_trait]`): the
/// `EventLoop` invokes these methods in-process during commit
/// processing, before any `IoAction` is dispatched. They must be
/// non-blocking and inexpensive.
///
/// Only `AppRecord` payloads (i.e., `LogEntry`s with
/// `entry_type == EntryType::Command`) are surfaced here — control
/// records (`LeaderChangeMessage`, `VotersRecord`) are filtered out by
/// the protocol layer.
pub trait StateMachine: Send + 'static {
    /// Apply the record at `offset` to the state machine. Returning
    /// `Err` is treated as irrecoverable by the event loop (it logs,
    /// calls `Listener::begin_shutdown`, and halts the node).
    fn apply(&mut self, offset: u64, record: &AppRecord) -> Result<()>;

    /// Produce a point-in-time snapshot of the current state.
    fn snapshot(&self) -> Result<AppSnapshot>;

    /// Replace current state with the contents of `snapshot`.
    fn restore(&mut self, snapshot: AppSnapshot) -> Result<()>;
}

// =========================================================================
// Object-safety pinning tests
// =========================================================================
//
// The trait *contracts* described above are part of xraft's public
// API. Object-safety is a load-bearing property — the protocol layer
// stores `Box<dyn LogStore>`, `Box<dyn TransportSender>`, etc. If a
// future change accidentally adds a generic method, a `Self: Sized`
// bound, or a `where Self: …` clause that breaks dyn-compatibility,
// these compile-time assertions fail rather than silently shipping a
// regression.

#[cfg(test)]
mod object_safety {
    use super::*;

    #[allow(dead_code)]
    fn assert_log_store_dyn(_: Box<dyn LogStore>) {}
    #[allow(dead_code)]
    fn assert_quorum_state_store_dyn(_: Box<dyn QuorumStateStore>) {}
    #[allow(dead_code)]
    fn assert_snapshot_io_dyn(_: Box<dyn SnapshotIO>) {}
    #[allow(dead_code)]
    fn assert_transport_sender_dyn(_: Box<dyn TransportSender>) {}
    #[allow(dead_code)]
    fn assert_transport_receiver_dyn(_: Box<dyn TransportReceiver>) {}
    #[allow(dead_code)]
    fn assert_clock_dyn(_: Box<dyn Clock>) {}
    #[allow(dead_code)]
    fn assert_state_machine_dyn(_: Box<dyn StateMachine>) {}
}
