use crate::quorum_state::QuorumState;
use async_trait::async_trait;
use std::time::{Duration, Instant};

pub type Result<T> = std::result::Result<T, crate::error::XraftError>;

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
