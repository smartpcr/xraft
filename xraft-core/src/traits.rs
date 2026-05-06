use async_trait::async_trait;
use crate::types::{NodeId, Term, AppRecord, AppSnapshot};
use crate::log_entry::LogEntry;
use crate::rpc::RpcEnvelope;

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
    async fn load(&self) -> Result<Option<QuorumState>, Box<dyn std::error::Error + Send + Sync>>;
    async fn save(&self, state: &QuorumState) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// Trait for sending RPCs to other nodes.
#[async_trait]
pub trait TransportSender: Send + Sync + 'static {
    async fn send(&self, target: NodeId, message: RpcEnvelope) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// Trait for receiving RPCs from other nodes.
#[async_trait]
pub trait TransportReceiver: Send + 'static {
    async fn recv(&mut self) -> Result<RpcEnvelope, Box<dyn std::error::Error + Send + Sync>>;
}

/// Deterministic clock for the event loop.
#[async_trait]
pub trait Clock: Send + 'static {
    fn now_ms(&self) -> u64;
    fn random_election_timeout_ms(&self) -> u64;
}

/// Application state machine.
pub trait StateMachine: Send + 'static {
    fn apply(&mut self, offset: u64, record: &AppRecord) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn snapshot(&self) -> Result<AppSnapshot, Box<dyn std::error::Error + Send + Sync>>;
    fn restore(&mut self, snapshot: AppSnapshot) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}
