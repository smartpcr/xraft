use std::io;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
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
