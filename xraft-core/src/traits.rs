use crate::quorum_state::QuorumState;
use async_trait::async_trait;
use std::io;

use crate::quorum_state::QuorumState;

/// Durable store for quorum voting state.
///
/// Implementations must fsync before returning `Ok` from `save`.
/// `load` returns `None` when no persisted state exists (first boot).
#[async_trait]
pub trait QuorumStateStore: Send + Sync + 'static {
    /// Load persisted quorum state.
    async fn load(&self) -> io::Result<Option<QuorumState>>;

    /// Persist quorum state. Must fsync before returning Ok.
    async fn save(&self, state: &QuorumState) -> io::Result<()>;
}
