use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use xraft_core::quorum_state::QuorumState;
use xraft_core::traits::QuorumStateStore;

pub struct MockQuorumStateStore {
    state: Mutex<Option<QuorumState>>,
    save_count: AtomicU64,
}

impl MockQuorumStateStore {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(None),
            save_count: AtomicU64::new(0),
        }
    }

    /// Creates a store with existing quorum state.
    pub fn with_state(qs: QuorumState) -> Self {
        Self {
            state: Mutex::new(Some(qs)),
            save_count: AtomicU64::new(0),
        }
    }

    pub fn save_count(&self) -> u64 {
        self.save_count.load(Ordering::SeqCst)
    }
}

impl Default for MockQuorumStateStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl QuorumStateStore for MockQuorumStateStore {
    async fn load(&self) -> std::io::Result<Option<QuorumState>> {
        Ok(self.state.lock().unwrap().clone())
    }

    async fn save(&self, state: &QuorumState) -> std::io::Result<()> {
        self.save_count.fetch_add(1, Ordering::SeqCst);
        *self.state.lock().unwrap() = Some(state.clone());
        Ok(())
    }
}
