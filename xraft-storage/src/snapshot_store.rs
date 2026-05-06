use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::Mutex;
use xraft_core::snapshot::{Snapshot, SnapshotId, SnapshotWriter};
use xraft_core::traits::SnapshotIO;
use xraft_core::Result;

/// File-based snapshot store.
///
/// Snapshots are stored as `<snap_dir>/<offset>-<epoch>.snap`.
/// Writes are atomic: write to temp file, fsync, rename.
pub struct SnapshotStore {
    snap_dir: PathBuf,
    latest: Mutex<Option<Snapshot>>,
}

impl SnapshotStore {
    /// Open or create the snapshot store directory.
    pub async fn open(snap_dir: &Path) -> Result<Self> {
        tokio::fs::create_dir_all(snap_dir).await?;

        // Load latest snapshot if any
        let latest = Self::find_latest(snap_dir).await?;

        Ok(Self {
            snap_dir: snap_dir.to_path_buf(),
            latest: Mutex::new(latest),
        })
    }

    async fn find_latest(snap_dir: &Path) -> Result<Option<Snapshot>> {
        let mut best: Option<(u64, PathBuf)> = None;

        let mut entries = tokio::fs::read_dir(snap_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.ends_with(".snap") {
                let stem = name_str.trim_end_matches(".snap");
                if let Some((offset_str, _epoch_str)) = stem.split_once('-') {
                    if let Ok(offset) = offset_str.parse::<u64>() {
                        if best.as_ref().map_or(true, |(best_off, _)| offset > *best_off) {
                            best = Some((offset, entry.path()));
                        }
                    }
                }
            }
        }

        if let Some((_, path)) = best {
            let data = tokio::fs::read(&path).await?;
            let snapshot: Snapshot = bincode::deserialize(&data).map_err(|e| {
                xraft_core::XraftError::Corruption(format!("snapshot deserialize: {e}"))
            })?;
            Ok(Some(snapshot))
        } else {
            Ok(None)
        }
    }

    /// Snapshot filename follows architecture §3.4: `<offset>-<term>.snap`.
    /// `SnapshotId.epoch` carries the term value in this context.
    fn snapshot_path(&self, id: &SnapshotId) -> PathBuf {
        self.snap_dir
            .join(format!("{}-{}.snap", id.end_offset, id.epoch))
    }
}

#[async_trait]
impl SnapshotIO for SnapshotStore {
    async fn save(&self, snapshot: &Snapshot) -> Result<()> {
        // File naming follows architecture §3.4: <offset>-<term>.snap
        let id = SnapshotId {
            end_offset: snapshot.metadata.last_included_offset,
            epoch: snapshot.metadata.last_included_term,
        };

        let data = bincode::serialize(snapshot).map_err(|e| {
            xraft_core::XraftError::SerializationError(format!("snapshot serialize: {e}"))
        })?;

        // Use a unique temp name to avoid collisions with concurrent saves
        let temp = self.snap_dir.join(format!(
            "{}-{}.{}.snap.tmp",
            id.end_offset, id.epoch, std::process::id()
        ));
        let final_path = self.snapshot_path(&id);

        tokio::fs::write(&temp, &data).await?;
        {
            // Open with write access so sync_all works on Windows
            let f = tokio::fs::OpenOptions::new()
                .write(true)
                .open(&temp)
                .await?;
            f.sync_all().await?;
        }
        tokio::fs::rename(&temp, &final_path).await?;

        // Hold lock only for cache update — serializes the logical "latest" state
        *self.latest.lock().await = Some(snapshot.clone());

        Ok(())
    }

    async fn load_latest(&self) -> Result<Option<Snapshot>> {
        let latest = self.latest.lock().await;
        Ok(latest.clone())
    }

    async fn read_chunk(
        &self,
        id: &SnapshotId,
        position: u64,
        max_bytes: u32,
    ) -> Result<(Bytes, bool)> {
        let path = self.snapshot_path(id);
        let data = tokio::fs::read(&path).await?;

        let start = position as usize;
        if start >= data.len() {
            return Ok((Bytes::new(), true));
        }

        let end = std::cmp::min(start + max_bytes as usize, data.len());
        let chunk = Bytes::copy_from_slice(&data[start..end]);
        let done = end >= data.len();

        Ok((chunk, done))
    }

    async fn begin_receive(&self, id: &SnapshotId) -> Result<SnapshotWriter> {
        let temp = self.snap_dir.join(format!(
            "{}-{}.{}.snap.tmp",
            id.end_offset, id.epoch, std::process::id()
        ));
        let final_path = self.snapshot_path(id);

        let file = tokio::fs::File::create(&temp).await?;
        Ok(SnapshotWriter::new(temp, final_path, file))
    }
}
