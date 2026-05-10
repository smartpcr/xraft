use std::io;
use std::sync::Mutex;

use xraft_core::quorum_state::QuorumState;
use xraft_core::traits::QuorumStateStore;

/// In-memory implementation of [`QuorumStateStore`].
///
/// Stores quorum state in a `Mutex`-protected `Option` so it can be
/// shared across threads without external synchronisation.
pub struct MemoryQuorumStateStore {
    state: Mutex<Option<QuorumState>>,
}

impl MemoryQuorumStateStore {
    /// Creates a new, empty store.
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

impl QuorumStateStore for MemoryQuorumStateStore {
    fn load(&self) -> io::Result<Option<QuorumState>> {
        let guard = self.state.lock().map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("lock poisoned: {e}"))
        })?;
        Ok(guard.clone())
    }

    fn save(&self, state: QuorumState) -> io::Result<()> {
        let mut guard = self.state.lock().map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("lock poisoned: {e}"))
        })?;
        *guard = Some(state);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_returns_none_when_empty() {
        let store = MemoryQuorumStateStore::new();
        assert!(store.load().unwrap().is_none());
    }

    #[test]
    fn save_then_load_round_trips() {
        let store = MemoryQuorumStateStore::new();
        let qs = QuorumState {
            voted_for: Some(1),
            current_term: 5,
        };
        store.save(qs.clone()).unwrap();
        let loaded = store.load().unwrap().expect("should be Some");
        assert_eq!(loaded.current_term, qs.current_term);
        assert_eq!(loaded.voted_for, qs.voted_for);
    }
}
