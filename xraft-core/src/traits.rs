use async_trait::async_trait;
use bytes::Bytes;
use tokio::time::{Duration, Instant};

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
