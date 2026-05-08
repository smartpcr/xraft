use std::sync::Mutex;

use async_trait::async_trait;
use xraft_core::error::Result;
use xraft_core::snapshot::{Snapshot, SnapshotId};
use xraft_core::traits::SnapshotIO;

/// In-memory snapshot store for testing.
pub struct MemorySnapshotStore {
    inner: Mutex<Vec<Snapshot>>,
}

impl MemorySnapshotStore {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Vec::new()),
        }
    }
}

impl Default for MemorySnapshotStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SnapshotIO for MemorySnapshotStore {
    async fn save(&self, snapshot: &Snapshot) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.push(snapshot.clone());
        Ok(())
    }

    async fn load_latest(&self) -> Result<Option<Snapshot>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .iter()
            .max_by_key(|s| s.metadata.last_included_offset)
            .cloned())
    }

    async fn read_chunk(
        &self,
        _id: &SnapshotId,
        _position: u64,
        _max_bytes: u32,
    ) -> Result<(Vec<u8>, bool)> {
        Ok((Vec::new(), true))
    }
}
