use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;

use crate::app_record::{AppRecord, AppSnapshot};
use crate::log_entry::LogEntry;
use crate::quorum_state::QuorumState;
use crate::rpc::RpcEnvelope;
use crate::snapshot::{Snapshot, SnapshotId, SnapshotWriter};
use crate::types::NodeId;

/// Result type used across all trait methods.
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

// ---------------------------------------------------------------------------
// Storage traits
// ---------------------------------------------------------------------------

/// Durable, append-only replicated log.
///
/// All methods take `&self`; implementations use interior mutability
/// (e.g. `tokio::sync::Mutex<File>`) so the store is `Sync` and can be
/// dispatched concurrently from `IoStage`.
#[async_trait]
pub trait LogStore: Send + Sync + 'static {
    async fn append(&self, entries: &[LogEntry]) -> Result<()>;
    async fn read(&self, start_offset: u64, end_offset: u64) -> Result<Vec<LogEntry>>;
    async fn truncate_suffix(&self, from_offset: u64) -> Result<()>;
    async fn truncate_prefix(&self, up_to_offset: u64) -> Result<()>;
    fn log_start_offset(&self) -> u64;
    fn log_end_offset(&self) -> u64;
    async fn entry_at(&self, offset: u64) -> Result<Option<LogEntry>>;
}

/// Persisted quorum state (current term + voted-for).
#[async_trait]
pub trait QuorumStateStore: Send + Sync + 'static {
    async fn load(&self) -> Result<Option<QuorumState>>;
    async fn save(&self, state: &QuorumState) -> Result<()>;
}

/// Snapshot persistence: save, load, and streaming chunk I/O.
#[async_trait]
pub trait SnapshotIO: Send + Sync + 'static {
    async fn save(&self, snapshot: &Snapshot) -> Result<()>;
    async fn load_latest(&self) -> Result<Option<Snapshot>>;
    async fn read_chunk(
        &self,
        id: &SnapshotId,
        position: u64,
        max_bytes: u32,
    ) -> Result<(Bytes, bool)>;
    async fn begin_receive(&self, id: &SnapshotId) -> Result<SnapshotWriter>;
}

// ---------------------------------------------------------------------------
// Transport traits
// ---------------------------------------------------------------------------

/// Sends RPC messages to peer nodes.
///
/// Takes `&self` (shared reference) because `IoStage` may send to multiple
/// peers concurrently.
#[async_trait]
pub trait TransportSender: Send + Sync + 'static {
    async fn send(&self, target: NodeId, message: RpcEnvelope) -> Result<()>;
}

/// Receives RPC messages from the network.
///
/// Takes `&mut self` (exclusive access) because only the `ReceiverTask`
/// reads from the network.
#[async_trait]
pub trait QuorumStateStore: Send + Sync + 'static {
    async fn load(&self) -> Result<Option<QuorumState>>;
    async fn save(&self, state: &QuorumState) -> Result<()>;
}

/// Abstraction over time for the event loop.
///
/// Production: wraps `tokio::time`.
/// Test: `SimulatedClock` with manual tick (in `xraft-test`).
///
/// Used directly by the EventLoop for timer management (election timeouts,
/// check-quorum deadlines). Not mediated by `IoAction`.
pub trait Clock: Send + 'static {
    /// Current instant.
    fn now(&self) -> Instant;

    /// Generate a random election timeout in [min, max].
    fn random_election_timeout(&self) -> Duration;
}

// ---------------------------------------------------------------------------
// Runtime traits
// ---------------------------------------------------------------------------

/// Clock abstraction for the event loop (election timeouts, check-quorum).
///
/// Used directly by `EventLoop`, NOT mediated by `IoAction` / `IoStage`.
/// Does not require `Sync` — only the single-threaded event loop calls it.
#[async_trait]
pub trait Clock: Send + 'static {
    fn now(&self) -> Instant;
    async fn sleep_until(&self, deadline: Instant);
    fn random_election_timeout(&self) -> Duration;
}

/// Application state machine driven synchronously by the event loop.
pub trait StateMachine: Send + 'static {
    fn apply(&mut self, offset: u64, record: &AppRecord) -> Result<()>;
    fn snapshot(&self) -> Result<AppSnapshot>;
    fn restore(&mut self, snapshot: AppSnapshot) -> Result<()>;
}
