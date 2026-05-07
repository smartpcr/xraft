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

/// Persisted voting state storage.
#[async_trait]
pub trait QuorumStateStore: Send + Sync + 'static {
    async fn load(&self) -> Result<Option<QuorumState>, io::Error>;
    async fn save(&self, state: &QuorumState) -> Result<(), io::Error>;
}

/// Snapshot I/O operations.
#[async_trait]
pub trait SnapshotIO: Send + Sync + 'static {
    async fn save(&self, snapshot: &Snapshot) -> Result<(), io::Error>;
    async fn load_latest(&self) -> Result<Option<Snapshot>, io::Error>;
    async fn read_chunk(
        &self,
        id: &SnapshotId,
        position: u64,
        max_bytes: u32,
    ) -> Result<(Bytes, bool), io::Error>;
    async fn begin_receive(&self, id: &SnapshotId) -> Result<SnapshotWriter, io::Error>;
}

/// Outbound RPC transport (shared reference for concurrent sends).
#[async_trait]
pub trait TransportSender: Send + Sync + 'static {
    async fn send(&self, target: NodeId, message: RpcEnvelope) -> Result<(), io::Error>;
}

/// Inbound RPC transport (exclusive access for sequential reads).
#[async_trait]
pub trait TransportReceiver: Send + 'static {
    async fn recv(&mut self) -> Result<RpcEnvelope, io::Error>;
}

/// Runtime clock trait for timer management in the EventLoop.
#[async_trait]
pub trait Clock: Send + 'static {
    fn now(&self) -> Instant;
    async fn sleep_until(&self, deadline: Instant);
    fn random_election_timeout(&self) -> Duration;
}

/// Application state machine — synchronous callbacks invoked by the EventLoop.
pub trait StateMachine: Send + 'static {
    fn apply(&mut self, offset: u64, record: &crate::app_record::AppRecord) -> Result<(), io::Error>;
    fn snapshot(&self) -> Result<crate::app_record::AppSnapshot, io::Error>;
    fn restore(&mut self, snapshot: crate::app_record::AppSnapshot) -> Result<(), io::Error>;
}
