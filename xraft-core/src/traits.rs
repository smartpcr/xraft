use crate::app_record::{AppRecord, AppSnapshot};
use crate::error::Result;
use crate::log_entry::LogEntry;
use crate::rpc::RpcEnvelope;
use crate::snapshot::{Snapshot, SnapshotWriter};
use crate::types::{NodeId, Term};
use crate::rpc::SnapshotId;
use async_trait::async_trait;
use bytes::Bytes;
use std::time::Duration;
use tokio::time::Instant;

use crate::error::Result;
use crate::rpc::RpcEnvelope;
use crate::types::NodeId;

/// Outbound RPC transport — sends messages to peers.
///
/// Takes `&self` (shared reference) because the `IoStage` may send to
/// multiple peers concurrently via `tokio::join!`.
#[async_trait]
pub trait TransportSender: Send + Sync + 'static {
    async fn send(&self, target: NodeId, message: RpcEnvelope) -> Result<()>;
}

/// Inbound RPC transport — receives messages from the network.
///
/// Takes `&mut self` (exclusive access) because only the `ReceiverTask`
/// reads from the network. Does NOT require `Sync`.
#[async_trait]
pub trait QuorumStateStore: Send + Sync + 'static {
    async fn load(&self) -> Result<Option<crate::quorum_state::QuorumState>>;
    async fn save(&self, state: &crate::quorum_state::QuorumState) -> Result<()>;
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
    ) -> Result<(Bytes, bool)>;
    async fn begin_receive(&self, id: &SnapshotId) -> Result<SnapshotWriter>;
}

/// Outbound RPC transport.
#[async_trait]
pub trait TransportSender: Send + Sync + 'static {
    async fn send(&self, target: NodeId, message: RpcEnvelope) -> Result<()>;
}

/// Inbound RPC transport.
#[async_trait]
pub trait TransportReceiver: Send + 'static {
    async fn recv(&mut self) -> Result<RpcEnvelope>;
}

/// Time abstraction for deterministic testing.
#[async_trait]
pub trait Clock: Send + 'static {
    fn now(&self) -> Instant;
    async fn sleep_until(&self, deadline: Instant);
    fn random_election_timeout(&self) -> Duration;
}

/// Application state machine. Receives only AppRecords (not control records).
pub trait StateMachine: Send + 'static {
    fn apply(&mut self, offset: u64, record: &AppRecord) -> Result<()>;
    fn snapshot(&self) -> Result<AppSnapshot>;
    fn restore(&mut self, snapshot: AppSnapshot) -> Result<()>;
}

/// Application callbacks for commit notifications and lifecycle events.
pub trait Listener: Send + 'static {
    /// Called when a batch of application records is committed (HW advanced).
    fn handle_commit(&mut self, batch: &[(u64, AppRecord)]);
    /// Called when a snapshot must be loaded.
    fn handle_load_snapshot(&mut self, reader: crate::snapshot::SnapshotReader);
    /// Called on leadership change.
    fn handle_leader_change(&mut self, leader_id: NodeId, term: Term);
    /// Called during graceful shutdown.
    fn begin_shutdown(&mut self);
}
