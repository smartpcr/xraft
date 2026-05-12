use async_trait::async_trait;
use std::sync::Mutex;
use xraft_core::quorum_state::QuorumState;
use xraft_core::traits::{QuorumStateStore, Result};

/// In-memory QuorumStateStore for deterministic testing.
/// `load()` returns `None` when no state has been saved.
pub struct MemoryQuorumStateStore {
    state: Mutex<Option<QuorumState>>,
}

impl MemoryQuorumStateStore {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(None),
        }
    }

    pub fn last_saved(&self) -> Option<QuorumState> {
        self.state.lock().unwrap().clone()
    }
}

impl Default for MemoryQuorumStateStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl QuorumStateStore for MemoryQuorumStateStore {
    async fn load(&self) -> Result<Option<QuorumState>> {
        Ok(self.state.lock().unwrap().clone())
    }

    async fn save(&self, state: &QuorumState) -> Result<()> {
        *self.state.lock().unwrap() = Some(state.clone());
        Ok(())
    }
}
