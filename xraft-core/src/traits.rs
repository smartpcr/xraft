use async_trait::async_trait;
use bytes::Bytes;
use tokio::time::{Duration, Instant};

use crate::app_record::{AppRecord, AppSnapshot};
use crate::error::XraftResult;
use crate::log_entry::LogEntry;
use crate::quorum_state::QuorumState;
use crate::rpc::{RpcEnvelope, SnapshotId};
use crate::snapshot::{Snapshot, SnapshotWriter};
use crate::types::NodeId;

/// Durable append-only log.
#[async_trait]
pub trait LogStore: Send + Sync + 'static {
    async fn append(&self, entries: &[LogEntry]) -> XraftResult<()>;
    async fn read(&self, start_offset: u64, end_offset: u64) -> XraftResult<Vec<LogEntry>>;
    async fn truncate_suffix(&self, from_offset: u64) -> XraftResult<()>;
    async fn truncate_prefix(&self, up_to_offset: u64) -> XraftResult<()>;
    fn log_start_offset(&self) -> u64;
    fn log_end_offset(&self) -> u64;
    async fn entry_at(&self, offset: u64) -> XraftResult<Option<LogEntry>>;
}

/// Persisted voting state.
#[async_trait]
pub trait QuorumStateStore: Send + Sync + 'static {
    async fn load(&self) -> XraftResult<Option<QuorumState>>;
    async fn save(&self, state: &QuorumState) -> XraftResult<()>;
}

/// Snapshot storage.
#[async_trait]
pub trait SnapshotIO: Send + Sync + 'static {
    async fn save(&self, snapshot: &Snapshot) -> XraftResult<()>;
    async fn load_latest(&self) -> XraftResult<Option<Snapshot>>;
    async fn read_chunk(
        &self,
        id: &SnapshotId,
        position: u64,
        max_bytes: u32,
    ) -> XraftResult<(Bytes, bool)>;
    async fn begin_receive(&self, id: &SnapshotId) -> XraftResult<SnapshotWriter>;
}

/// Outbound RPC sender. Takes `&self` for concurrent sends.
#[async_trait]
pub trait TransportSender: Send + Sync + 'static {
    async fn send(&self, target: NodeId, message: RpcEnvelope) -> XraftResult<()>;
}

/// Inbound RPC receiver. Takes `&mut self` — exclusive access by ReceiverTask.
#[async_trait]
pub trait TransportReceiver: Send + 'static {
    async fn recv(&mut self) -> XraftResult<RpcEnvelope>;
}

/// Deterministic time source. Used by EventLoop for timer management.
#[async_trait]
pub trait Clock: Send + 'static {
    fn now(&self) -> Instant;
    async fn sleep_until(&self, deadline: Instant);
    fn random_election_timeout(&self) -> Duration;
}

/// Application state machine. Synchronous — invoked by EventLoop.
pub trait StateMachine: Send + 'static {
    fn apply(&mut self, offset: u64, record: &AppRecord) -> XraftResult<()>;
    fn snapshot(&self) -> XraftResult<AppSnapshot>;
    fn restore(&mut self, snapshot: AppSnapshot) -> XraftResult<()>;
}
