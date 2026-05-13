use std::sync::Mutex;

use xraft_core::error::Result;
use xraft_core::quorum_state::QuorumState;
use xraft_core::traits::QuorumStateStore;

/// In-memory [`QuorumStateStore`] for tests and simulation.
#[derive(Debug, Default)]
pub struct MemoryQuorumStateStore {
    state: Mutex<QuorumState>,
}

impl MemoryQuorumStateStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_state(state: QuorumState) -> Self {
        Self {
            state: Mutex::new(state),
        }
    }
}

impl QuorumStateStore for MemoryQuorumStateStore {
    fn load(&self) -> Result<QuorumState> {
        Ok(self
            .state
            .lock()
            .expect("quorum state mutex poisoned")
            .clone())
    }

    fn save(&mut self, state: &QuorumState) -> Result<()> {
        *self.state.lock().expect("quorum state mutex poisoned") = state.clone();
        Ok(())
    }
}
