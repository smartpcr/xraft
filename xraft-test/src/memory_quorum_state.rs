use std::sync::Mutex;

use async_trait::async_trait;
use xraft_core::error::Result;
use xraft_core::quorum_state::QuorumState;
use xraft_core::traits::QuorumStateStore;

/// In-memory quorum state store implementing `QuorumStateStore`.
/// `load()` returns `None` when no state has been saved
/// (matching architecture §4.1 `Option<QuorumState>` contract).
pub struct MemoryQuorumStateStore {
    state: Mutex<Option<QuorumState>>,
}

impl MemoryQuorumStateStore {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(None),
        }
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
        let state = self.state.lock().unwrap();
        Ok(state.clone())
    }

    async fn save(&self, state: &QuorumState) -> Result<()> {
        let mut current = self.state.lock().unwrap();
        *current = Some(state.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xraft_core::types::NodeId;

    #[tokio::test]
    async fn test_load_returns_none_initially() {
        let store = MemoryQuorumStateStore::new();
        let result = store.load().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_save_and_load() {
        let store = MemoryQuorumStateStore::new();

        let state = QuorumState {
            current_term: 5,
            voted_for: Some(NodeId(1)),
            leader_id: Some(NodeId(1)),
            leader_epoch: 3,
        };

        store.save(&state).await.unwrap();

        let loaded = store.load().await.unwrap().unwrap();
        assert_eq!(loaded, state);
    }

    #[tokio::test]
    async fn test_save_overwrites() {
        let store = MemoryQuorumStateStore::new();

        let state1 = QuorumState {
            current_term: 1,
            voted_for: None,
            leader_id: None,
            leader_epoch: 0,
        };
        store.save(&state1).await.unwrap();

        let state2 = QuorumState {
            current_term: 5,
            voted_for: Some(NodeId(2)),
            leader_id: Some(NodeId(2)),
            leader_epoch: 2,
        };
        store.save(&state2).await.unwrap();

        let loaded = store.load().await.unwrap().unwrap();
        assert_eq!(loaded.current_term, 5);
        assert_eq!(loaded.voted_for, Some(NodeId(2)));
    }
}
