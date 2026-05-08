use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;

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

/// Manages snapshot files on disk.
#[async_trait]
pub trait SnapshotIO: Send + Sync + 'static {
    async fn save(&self, snapshot: &Snapshot) -> std::io::Result<()>;
    /// Returns `None` when no snapshot exists.
    async fn load_latest(&self) -> std::io::Result<Option<Snapshot>>;
    async fn read_chunk(
        &self,
        id: &SnapshotId,
        position: u64,
        max_bytes: u32,
    ) -> std::io::Result<(Bytes, bool)>;
    async fn begin_receive(&self, id: &SnapshotId) -> std::io::Result<SnapshotWriter>;
}

/// Outbound RPC transport. Takes `&self` for concurrent sends.
#[async_trait]
pub trait TransportSender: Send + Sync + 'static {
    async fn send(
        &self,
        target: crate::types::NodeId,
        message: RpcEnvelope,
    ) -> std::io::Result<()>;
}

/// Inbound RPC transport. Takes `&mut self` for exclusive read access.
#[async_trait]
pub trait TransportReceiver: Send + 'static {
    async fn recv(&mut self) -> std::io::Result<RpcEnvelope>;
}

/// Runtime clock for timer management (election timeouts, check-quorum).
/// Used by the EventLoop, not mediated by IoAction.
#[async_trait]
pub trait Clock: Send + 'static {
    fn now(&self) -> Instant;
    async fn sleep_until(&self, deadline: Instant);
    fn random_election_timeout(&self) -> Duration;
}

/// Application state machine. Synchronous callbacks invoked by the EventLoop.
pub trait StateMachine: Send + 'static {
    fn apply(&mut self, offset: u64, record: &AppRecord) -> Result<(), crate::error::XraftError>;
    fn snapshot(&self) -> Result<AppSnapshot, crate::error::XraftError>;
    fn restore(&mut self, snapshot: AppSnapshot) -> Result<(), crate::error::XraftError>;
}
