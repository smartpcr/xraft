use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use xraft_core::app_record::AppSnapshot;
use xraft_core::error::{Result, XraftError};
use xraft_core::snapshot::{Snapshot, SnapshotId, SnapshotMetadata, SnapshotWriter};
use xraft_core::traits::SnapshotIO;

/// In-memory snapshot storage implementing `SnapshotIO`.
/// Uses interior mutability via `std::sync::Mutex`.
pub struct MemorySnapshotStore {
    inner: Mutex<SnapshotInner>,
}

struct SnapshotInner {
    /// The latest saved snapshot, if any.
    current: Option<Snapshot>,
}

impl MemorySnapshotStore {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(SnapshotInner { current: None }),
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
        inner.current = Some(snapshot.clone());
        Ok(())
    }

    async fn load_latest(&self) -> Result<Option<Snapshot>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.current.clone())
    }

    async fn read_chunk(
        &self,
        id: &SnapshotId,
        position: u64,
        max_bytes: u32,
    ) -> Result<(Bytes, bool)> {
        let inner = self.inner.lock().unwrap();
        let snapshot = inner.current.as_ref().ok_or_else(|| {
            XraftError::StorageError("no snapshot available".to_string())
        })?;

        // Verify ID matches
        let current_id = SnapshotId {
            end_offset: snapshot.metadata.last_included_offset,
            epoch: snapshot.metadata.leader_epoch,
        };
        if *id != current_id {
            return Err(XraftError::StorageError(
                "snapshot id mismatch".to_string(),
            ));
        }

        let data = &snapshot.app_snapshot.data;
        let pos = position as usize;
        if pos >= data.len() {
            return Ok((Bytes::new(), true));
        }

        let end = (pos + max_bytes as usize).min(data.len());
        let chunk = Bytes::copy_from_slice(&data[pos..end]);
        let is_last = end >= data.len();
        Ok((chunk, is_last))
    }

    async fn begin_receive(&self, id: &SnapshotId) -> Result<SnapshotWriter> {
        Ok(SnapshotWriter::new(id.clone()))
    }

    async fn complete_receive(
        &self,
        writer: SnapshotWriter,
        metadata: SnapshotMetadata,
    ) -> Result<()> {
        let snapshot = Snapshot {
            metadata,
            app_snapshot: AppSnapshot {
                data: writer.data,
            },
        };
        let mut inner = self.inner.lock().unwrap();
        inner.current = Some(snapshot);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xraft_core::app_record::AppSnapshot;
    use xraft_core::snapshot::SnapshotMetadata;
    use xraft_core::voter::VoterInfo;
    use xraft_core::types::NodeId;

    fn make_test_snapshot() -> Snapshot {
        Snapshot {
            metadata: SnapshotMetadata {
                last_included_offset: 100,
                last_included_term: 5,
                voters: vec![VoterInfo {
                    node_id: NodeId(1),
                    endpoint: "localhost:8080".to_string(),
                }],
                leader_epoch: 3,
            },
            app_snapshot: AppSnapshot {
                data: b"test-snapshot-data".to_vec(),
            },
        }
    }

    #[tokio::test]
    async fn test_save_and_load_latest() {
        let store = MemorySnapshotStore::new();

        // Initially empty
        assert!(store.load_latest().await.unwrap().is_none());

        let snapshot = make_test_snapshot();
        store.save(&snapshot).await.unwrap();

        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(loaded, snapshot);
    }

    #[tokio::test]
    async fn test_read_chunk() {
        let store = MemorySnapshotStore::new();
        let snapshot = make_test_snapshot();
        store.save(&snapshot).await.unwrap();

        let id = SnapshotId {
            end_offset: 100,
            epoch: 3,
        };

        // Read first chunk
        let (chunk, is_last) = store.read_chunk(&id, 0, 10).await.unwrap();
        assert_eq!(chunk.len(), 10);
        assert!(!is_last);

        // Read remaining
        let (chunk2, is_last2) = store.read_chunk(&id, 10, 100).await.unwrap();
        assert_eq!(chunk2.len(), 8); // "test-snapshot-data" is 18 bytes
        assert!(is_last2);
    }

    #[tokio::test]
    async fn test_read_chunk_past_end() {
        let store = MemorySnapshotStore::new();
        let snapshot = make_test_snapshot();
        store.save(&snapshot).await.unwrap();

        let id = SnapshotId {
            end_offset: 100,
            epoch: 3,
        };

        let (chunk, is_last) = store.read_chunk(&id, 1000, 10).await.unwrap();
        assert!(chunk.is_empty());
        assert!(is_last);
    }

    #[tokio::test]
    async fn test_begin_receive_and_complete() {
        let store = MemorySnapshotStore::new();
        let id = SnapshotId {
            end_offset: 200,
            epoch: 4,
        };

        let mut writer = store.begin_receive(&id).await.unwrap();
        writer.write_chunk(b"chunk1");
        writer.write_chunk(b"chunk2");

        assert_eq!(writer.data, b"chunk1chunk2");
        assert_eq!(writer.id(), &id);

        // Finalize — the received data persists into the store
        let metadata = SnapshotMetadata {
            last_included_offset: 200,
            last_included_term: 10,
            voters: vec![VoterInfo {
                node_id: NodeId(1),
                endpoint: "localhost:9090".to_string(),
            }],
            leader_epoch: 4,
        };
        store.complete_receive(writer, metadata.clone()).await.unwrap();

        // Verify the snapshot is now loadable
        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(loaded.metadata, metadata);
        assert_eq!(loaded.app_snapshot.data, b"chunk1chunk2");
    }

    #[tokio::test]
    async fn test_snapshot_overwrite() {
        let store = MemorySnapshotStore::new();

        let snap1 = make_test_snapshot();
        store.save(&snap1).await.unwrap();

        let mut snap2 = make_test_snapshot();
        snap2.metadata.last_included_offset = 200;
        snap2.app_snapshot.data = b"newer-data".to_vec();
        store.save(&snap2).await.unwrap();

        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(loaded.metadata.last_included_offset, 200);
        assert_eq!(loaded.app_snapshot.data, b"newer-data");
    }
}
