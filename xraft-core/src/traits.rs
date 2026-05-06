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

/// Snapshot I/O operations.
#[async_trait]
pub trait SnapshotIO: Send + Sync + 'static {
    /// Write a complete snapshot atomically. Must fsync before returning Ok.
    async fn save(&self, snapshot: &Snapshot) -> std::io::Result<()>;

    /// Load the latest snapshot, if any.
    async fn load_latest(&self) -> std::io::Result<Option<Snapshot>>;

    /// Read a chunk of a snapshot at the given byte position.
    async fn read_chunk(
        &self,
        id: &SnapshotId,
        position: u64,
        max_bytes: u32,
    ) -> std::io::Result<(Bytes, bool)>;

    /// Begin writing a snapshot received from a leader, chunk by chunk.
    async fn begin_receive(&self, id: &SnapshotId) -> std::io::Result<SnapshotWriter>;
}

/// Network transport for sending RPCs to peer nodes.
///
/// Injected into `IoStage` so that `SendRpc` actions execute through a real
/// (or test-mock) transport rather than a hardcoded placeholder.
#[async_trait]
pub trait NetworkSender: Send + Sync + 'static {
    /// Send an RPC payload to the given peer. The payload is an opaque byte
    /// buffer; higher-level RPC envelope framing is defined in a later stage.
    async fn send(&self, target: crate::types::NodeId, data: Vec<u8>) -> std::io::Result<()>;
}

/// Application state machine. Synchronous trait (not async) per architecture
/// §4.1 — callbacks are invoked by the EventLoop, not the IoStage.
pub trait StateMachine: Send + 'static {
    /// Apply a committed command entry to the state machine.
    fn apply(&mut self, offset: u64, record: &AppRecord) -> std::io::Result<()>;

    /// Take a point-in-time snapshot of the current state machine state.
    fn snapshot(&self) -> std::io::Result<AppSnapshot>;

    /// Restore the state machine from a snapshot.
    fn restore(&mut self, snapshot: AppSnapshot) -> std::io::Result<()>;
}
