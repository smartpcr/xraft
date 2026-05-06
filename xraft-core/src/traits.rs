use async_trait::async_trait;
use crate::types::{NodeId, Term, AppRecord, AppSnapshot};
use crate::log_entry::LogEntry;
use crate::rpc::RpcEnvelope;

/// Trait for log storage.
#[async_trait]
pub trait LogStore: Send + Sync + 'static {
    async fn append(&self, entries: &[LogEntry]) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    async fn read(&self, start_offset: u64, end_offset: u64) -> Result<Vec<LogEntry>, Box<dyn std::error::Error + Send + Sync>>;
    async fn truncate_suffix(&self, from_offset: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn log_start_offset(&self) -> u64;
    fn log_end_offset(&self) -> u64;
    async fn entry_at(&self, offset: u64) -> Result<Option<LogEntry>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Persistent voting state.
#[derive(Debug, Clone)]
pub struct QuorumState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub leader_epoch: u64,
}

/// Trait for persisting quorum state (term, vote).
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
