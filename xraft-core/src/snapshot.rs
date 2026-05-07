use serde::{Deserialize, Serialize};

use crate::app_record::AppSnapshot;
use crate::voter::VoterInfo;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::io;
use std::path::PathBuf;
use tokio::sync::Mutex;

/// Identifies a specific snapshot by its last included offset and epoch.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SnapshotId {
    pub end_offset: u64,
    pub epoch: u64,
}

/// Metadata for a snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub last_included_offset: u64,
    pub last_included_term: u64,
    pub voters: Vec<VoterInfo>,
    pub leader_epoch: u64,
}

/// A complete snapshot: consensus metadata + application payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub metadata: SnapshotMetadata,
    pub app_snapshot: AppSnapshot,
}

/// A chunked write session for receiving snapshots from a leader.
pub struct SnapshotWriter {
    temp_path: PathBuf,
    final_path: PathBuf,
    file: Mutex<Option<tokio::fs::File>>,
    bytes_written: Mutex<u64>,
}

impl SnapshotWriter {
    /// Create a new snapshot writer.
    pub fn new(temp_path: PathBuf, final_path: PathBuf, file: tokio::fs::File) -> Self {
        Self {
            temp_path,
            final_path,
            file: Mutex::new(Some(file)),
            bytes_written: Mutex::new(0),
        }
    }

    /// Write a chunk of snapshot data at the expected position.
    pub async fn write_chunk(&self, position: u64, data: &[u8]) -> io::Result<()> {
        use tokio::io::AsyncWriteExt;
        use tokio::io::AsyncSeekExt;

        let mut file_guard = self.file.lock().await;
        let file = file_guard
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "writer already finalized"))?;

        file.seek(std::io::SeekFrom::Start(position)).await?;
        file.write_all(data).await?;

        let mut written = self.bytes_written.lock().await;
        *written = position + data.len() as u64;
        Ok(())
    }

    /// Finalize the snapshot: fsync and atomically move to final path.
    pub async fn finish(self) -> io::Result<()> {
        let mut file_guard = self.file.lock().await;
        if let Some(file) = file_guard.take() {
            file.sync_all().await?;
            drop(file);
            tokio::fs::rename(&self.temp_path, &self.final_path).await?;
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "writer already finalized",
            ))
        }
    }

    /// Abort the receive session, cleaning up the temp file.
    pub async fn abort(self) -> io::Result<()> {
        let mut file_guard = self.file.lock().await;
        if let Some(file) = file_guard.take() {
            drop(file);
            let _ = tokio::fs::remove_file(&self.temp_path).await;
        }
        Ok(())
    }
}

impl std::fmt::Debug for SnapshotWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotWriter")
            .field("temp_path", &self.temp_path)
            .field("final_path", &self.final_path)
            .finish()
    }
}

/// A reader for snapshot chunks (used during follower restore).
#[derive(Debug)]
pub struct SnapshotReader {
    pub data: Bytes,
    pub metadata: SnapshotMetadata,
}
