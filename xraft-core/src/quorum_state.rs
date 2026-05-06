use crate::types::{NodeId, Term};

/// Quorum state persisted to stable storage before any vote acknowledgement.
/// Contains the minimum durable state required for election safety.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub leader_id: Option<NodeId>,
    pub leader_epoch: u64,
}

/// Durable store for [`QuorumState`]. Implementations must guarantee that
/// `persist` is durable (fsync'd) before returning `Ok`. The event loop
/// must call `persist` before sending any vote responses or broadcasting
/// vote requests — violating this ordering breaks Raft's election safety.
pub trait QuorumStateStore {
    /// Persist the quorum state to stable storage. Must be durable (fsync)
    /// before returning `Ok`.
    fn persist(&mut self, state: &QuorumState) -> Result<(), String>;

    /// Load the most recently persisted quorum state, or `None` if no state
    /// has been persisted yet (first boot).
    fn load(&self) -> Result<Option<QuorumState>, String>;
}

/// In-memory implementation of [`QuorumStateStore`] for testing.
/// NOT suitable for production — data is lost on process restart.
#[derive(Debug, Default)]
pub struct InMemoryQuorumStateStore {
    state: Option<QuorumState>,
    /// Count of persist calls, for test assertions.
    pub persist_count: u64,
    /// If set, `persist` will return this error.
    pub fail_next_persist: Option<String>,
}

impl InMemoryQuorumStateStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the currently stored state, if any.
    pub fn stored_state(&self) -> Option<&QuorumState> {
        self.state.as_ref()
    }
}

impl QuorumStateStore for InMemoryQuorumStateStore {
    fn persist(&mut self, state: &QuorumState) -> Result<(), String> {
        if let Some(err) = self.fail_next_persist.take() {
            return Err(err);
        }
        self.persist_count += 1;
        self.state = Some(state.clone());
        Ok(())
    }

    fn load(&self) -> Result<Option<QuorumState>, String> {
        Ok(self.state.clone())
    }
}
