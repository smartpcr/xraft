//! In-memory implementation of the `QuorumStateStore` trait for testing.

use async_trait::async_trait;
use std::sync::RwLock;
use xraft_core::traits::{QuorumState, QuorumStateStore};

/// Thread-safe in-memory quorum state store implementing `QuorumStateStore`.
///
/// Stores the latest term, vote, and leader epoch in memory.
/// Returns `None` from `load()` when no state has been saved,
/// matching the architecture §4.1 `Option<QuorumState>` contract.
pub struct MemoryQuorumStateStore {
    state: RwLock<Option<QuorumState>>,
}

impl MemoryQuorumStateStore {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(None),
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
    async fn load(
        &self,
    ) -> Result<Option<QuorumState>, Box<dyn std::error::Error + Send + Sync>> {
        let state = self.state.read().unwrap();
        Ok(state.clone())
    }

    async fn save(
        &self,
        state: &QuorumState,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut stored = self.state.write().unwrap();
        *stored = Some(state.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xraft_core::types::{NodeId, Term};

    #[tokio::test]
    async fn load_returns_none_initially() {
        let store = MemoryQuorumStateStore::new();
        let state = store.load().await.unwrap();
        assert!(state.is_none());
    }

    #[tokio::test]
    async fn save_and_load_roundtrip() {
        let store = MemoryQuorumStateStore::new();
        let qs = QuorumState {
            current_term: Term(5),
            voted_for: Some(NodeId(2)),
            leader_epoch: 3,
        };
        store.save(&qs).await.unwrap();

        let loaded = store.load().await.unwrap().expect("should have state");
        assert_eq!(loaded.current_term, Term(5));
        assert_eq!(loaded.voted_for, Some(NodeId(2)));
        assert_eq!(loaded.leader_epoch, 3);
    }

    #[tokio::test]
    async fn save_overwrites_previous() {
        let store = MemoryQuorumStateStore::new();
        let qs1 = QuorumState {
            current_term: Term(1),
            voted_for: None,
            leader_epoch: 0,
        };
        store.save(&qs1).await.unwrap();

        let qs2 = QuorumState {
            current_term: Term(3),
            voted_for: Some(NodeId(1)),
            leader_epoch: 2,
        };
        store.save(&qs2).await.unwrap();

        let loaded = store.load().await.unwrap().unwrap();
        assert_eq!(loaded.current_term, Term(3));
    }
}
