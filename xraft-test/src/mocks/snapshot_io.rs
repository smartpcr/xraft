use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use xraft_core::rpc::SnapshotId;
use xraft_core::snapshot::{Snapshot, SnapshotWriter};
use xraft_core::traits::SnapshotIO;

pub struct MockSnapshotIO {
    snapshot: Mutex<Option<Snapshot>>,
}

impl MockSnapshotIO {
    pub fn new() -> Self {
        Self {
            snapshot: Mutex::new(None),
        }
    }

    /// Creates a snapshot IO that reports an existing snapshot.
    pub fn with_snapshot(snap: Snapshot) -> Self {
        Self {
            snapshot: Mutex::new(Some(snap)),
        }
    }
}

impl Default for MockSnapshotIO {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SnapshotIO for MockSnapshotIO {
    async fn save(&self, snapshot: &Snapshot) -> std::io::Result<()> {
        *self.snapshot.lock().unwrap() = Some(snapshot.clone());
        Ok(())
    }

    async fn load_latest(&self) -> std::io::Result<Option<Snapshot>> {
        Ok(self.snapshot.lock().unwrap().clone())
    }

    async fn read_chunk(
        &self,
        _id: &SnapshotId,
        _position: u64,
        _max_bytes: u32,
    ) -> std::io::Result<(Bytes, bool)> {
        Ok((Bytes::new(), true))
    }

    async fn begin_receive(&self, _id: &SnapshotId) -> std::io::Result<SnapshotWriter> {
        Ok(SnapshotWriter::new())
    }
}
