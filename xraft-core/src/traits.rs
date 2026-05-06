use crate::quorum_state::QuorumState;
use async_trait::async_trait;
use std::time::{Duration, Instant};

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
    async fn load(&self) -> Result<Option<QuorumState>>;
    async fn save(&self, state: &QuorumState) -> Result<()>;
}

/// Abstraction over time for the event loop.
///
/// Production: wraps `tokio::time`.
/// Test: `SimulatedClock` with manual tick (in `xraft-test`).
///
/// Used directly by the EventLoop for timer management (election timeouts,
/// check-quorum deadlines). Not mediated by `IoAction`.
pub trait Clock: Send + 'static {
    /// Current instant.
    fn now(&self) -> Instant;

    /// Generate a random election timeout in [min, max].
    fn random_election_timeout(&self) -> Duration;
}
