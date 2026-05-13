use std::path::{Path, PathBuf};

use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::error::{StorageError, StorageResult};
use crate::model::{SnapshotChunk, SnapshotMetadata};

/// Default chunk size for snapshot transfers (64 KiB).
const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;

/// On-disk snapshot store.
///
/// Snapshots are stored as flat files under `<base_dir>/<term>-<index>.snap`
/// with an adjacent `.meta` JSON sidecar for metadata.
#[derive(Debug, Clone)]
pub struct SnapshotStore {
    base_dir: PathBuf,
    chunk_size: usize,
}

impl SnapshotStore {
    /// Create a new [`SnapshotStore`] rooted at `base_dir`.
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    /// Override the default chunk size used by [`read_chunk`].
    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        assert!(chunk_size > 0, "chunk_size must be > 0");
        self.chunk_size = chunk_size;
        self
    }

    // -----------------------------------------------------------------------
    // Path helpers
    // -----------------------------------------------------------------------

    fn snapshot_path(&self, meta: &SnapshotMetadata) -> PathBuf {
        self.base_dir
            .join(format!("{}-{}.snap", meta.term, meta.index))
    }

    fn meta_path(&self, meta: &SnapshotMetadata) -> PathBuf {
        self.base_dir
            .join(format!("{}-{}.meta", meta.term, meta.index))
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Persist `data` as a new snapshot described by `meta`.
    pub async fn save_snapshot(
        &self,
        meta: &SnapshotMetadata,
        data: &[u8],
    ) -> StorageResult<()> {
        fs::create_dir_all(&self.base_dir).await?;

        let snap_path = self.snapshot_path(meta);
        let meta_path = self.meta_path(meta);

        let meta_json = serde_json::to_vec(meta)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        fs::write(&snap_path, data).await?;
        fs::write(&meta_path, &meta_json).await?;

        Ok(())
    }

    /// Load the full snapshot bytes for the given metadata.
    pub async fn load_snapshot(
        &self,
        meta: &SnapshotMetadata,
    ) -> StorageResult<Vec<u8>> {
        let path = self.snapshot_path(meta);
        if !path.exists() {
            return Err(StorageError::NotFound(format!(
                "snapshot file not found: {}",
                path.display()
            )));
        }
        let data = fs::read(&path).await?;
        Ok(data)
    }

    /// Return the total size in bytes of the snapshot file.
    pub async fn snapshot_size(
        &self,
        meta: &SnapshotMetadata,
    ) -> StorageResult<u64> {
        let path = self.snapshot_path(meta);
        let file_meta = fs::metadata(&path).await?;
        Ok(file_meta.len())
    }

    /// Read a single chunk of a snapshot starting at `position`.
    ///
    /// Opens the file, seeks to `position`, and reads up to `chunk_size`
    /// bytes — each call is O(1) in snapshot size regardless of `position`.
    pub async fn read_chunk(
        &self,
        meta: &SnapshotMetadata,
        position: u64,
    ) -> StorageResult<SnapshotChunk> {
        let path = self.snapshot_path(meta);
        let file_len = fs::metadata(&path).await?.len();

        if position >= file_len {
            return Ok(SnapshotChunk {
                data: Vec::new(),
                position,
                done: true,
            });
        }

        let mut file = fs::File::open(&path).await?;
        file.seek(std::io::SeekFrom::Start(position)).await?;

        let remaining = (file_len - position) as usize;
        let to_read = remaining.min(self.chunk_size);
        let mut buf = vec![0u8; to_read];
        let n = file.read_exact(&mut buf).await.map(|_| to_read)?;

        let next_position = position + n as u64;
        Ok(SnapshotChunk {
            data: buf,
            position,
            done: next_position >= file_len,
        })
    }

    /// Write a chunk received during snapshot installation.
    ///
    /// If `position == 0` the file is created/truncated; otherwise the chunk
    /// is appended at the given offset.
    pub async fn write_chunk(
        &self,
        meta: &SnapshotMetadata,
        chunk: &SnapshotChunk,
    ) -> StorageResult<()> {
        fs::create_dir_all(&self.base_dir).await?;
        let path = self.snapshot_path(meta);

        let mut file = if chunk.position == 0 {
            fs::File::create(&path).await?
        } else {
            fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .await?
        };

        file.seek(std::io::SeekFrom::Start(chunk.position)).await?;
        file.write_all(&chunk.data).await?;
        file.flush().await?;

        if chunk.done {
            let meta_path = self.meta_path(meta);
            let meta_json = serde_json::to_vec(meta)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            fs::write(&meta_path, &meta_json).await?;
        }

        Ok(())
    }

    /// List all snapshots available on disk, sorted by (term, index) desc.
    pub async fn list_snapshots(&self) -> StorageResult<Vec<SnapshotMetadata>> {
        let mut entries = Vec::new();
        if !self.base_dir.exists() {
            return Ok(entries);
        }

        let mut dir = fs::read_dir(&self.base_dir).await?;
        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("meta") {
                let bytes = fs::read(&path).await?;
                let meta: SnapshotMetadata = serde_json::from_slice(&bytes)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                entries.push(meta);
            }
        }

        entries.sort_by(|a, b| {
            (b.term, b.index).cmp(&(a.term, a.index))
        });

        Ok(entries)
    }

    /// Delete all snapshots older than the one described by `keep`.
    pub async fn purge_older_than(
        &self,
        keep: &SnapshotMetadata,
    ) -> StorageResult<usize> {
        let all = self.list_snapshots().await?;
        let mut removed = 0usize;

        for snap in &all {
            if (snap.term, snap.index) < (keep.term, keep.index) {
                let _ = fs::remove_file(self.snapshot_path(snap)).await;
                let _ = fs::remove_file(self.meta_path(snap)).await;
                removed += 1;
            }
        }

        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_meta() -> SnapshotMetadata {
        SnapshotMetadata {
            term: 3,
            index: 100,
        }
    }

    #[tokio::test]
    async fn read_chunk_seeks_without_reading_entire_file() {
        let dir = TempDir::new().unwrap();
        let store = SnapshotStore::new(dir.path()).with_chunk_size(4);
        let meta = test_meta();
        let data = b"hello world!";

        store.save_snapshot(&meta, data).await.unwrap();

        // First chunk: "hell"
        let c0 = store.read_chunk(&meta, 0).await.unwrap();
        assert_eq!(c0.data, b"hell");
        assert!(!c0.done);

        // Second chunk: "o wo"
        let c1 = store.read_chunk(&meta, 4).await.unwrap();
        assert_eq!(c1.data, b"o wo");
        assert!(!c1.done);

        // Third chunk: "rld!"
        let c2 = store.read_chunk(&meta, 8).await.unwrap();
        assert_eq!(c2.data, b"rld!");
        assert!(c2.done);

        // Past-end: empty, done
        let c3 = store.read_chunk(&meta, 12).await.unwrap();
        assert!(c3.data.is_empty());
        assert!(c3.done);
    }
}
