use async_trait::async_trait;

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
pub trait LogStore: Send + Sync + 'static {
    async fn append(&self, entries: &[LogEntry]) -> Result<()>;
    async fn read(&self, start_offset: u64, end_offset: u64) -> Result<Vec<LogEntry>>;
    async fn truncate_suffix(&self, from_offset: u64) -> Result<()>;
    async fn truncate_prefix(&self, up_to_offset: u64) -> Result<()>;
    fn log_start_offset(&self) -> u64;
    fn log_end_offset(&self) -> u64;
    async fn entry_at(&self, offset: u64) -> Result<Option<LogEntry>>;
}

/// Trait for application state machine.
pub trait StateMachine: Send + 'static {
    fn apply(&mut self, offset: u64, data: &[u8]) -> Result<()>;
    fn snapshot(&self) -> Result<Vec<u8>>;
    fn restore(&mut self, snapshot: Vec<u8>) -> Result<()>;
}
