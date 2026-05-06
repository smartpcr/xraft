use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use tokio::sync::RwLock;
use xraft_core::Result;

/// In-memory mapping of leader epoch → start offset, persisted to file.
///
/// Used for divergence detection during Fetch RPCs. Rebuilt from log scan
/// on recovery if checkpoint file is absent.
pub struct LeaderEpochCheckpoint {
    file_path: PathBuf,
    epochs: RwLock<BTreeMap<u64, u64>>,
}

impl LeaderEpochCheckpoint {
    /// Open or create a leader epoch checkpoint.
    pub async fn open(dir: &Path) -> Result<Self> {
        tokio::fs::create_dir_all(dir).await?;
        let file_path = dir.join("leader-epoch-checkpoint");

        let epochs = if file_path.exists() {
            let data = tokio::fs::read(&file_path).await?;
            if data.is_empty() {
                BTreeMap::new()
            } else {
                bincode::deserialize(&data).map_err(|e| {
                    xraft_core::XraftError::Corruption(format!(
                        "leader-epoch-checkpoint parse error: {e}"
                    ))
                })?
            }
        } else {
            BTreeMap::new()
        };

        Ok(Self {
            file_path,
            epochs: RwLock::new(epochs),
        })
    }

    /// Append a new epoch entry. Idempotent if (epoch, offset) already present.
    pub async fn append(&self, epoch: u64, start_offset: u64) -> Result<()> {
        let mut epochs = self.epochs.write().await;
        epochs.insert(epoch, start_offset);
        self.persist(&epochs).await
    }

    /// Look up the start offset for a given epoch.
    pub async fn lookup(&self, epoch: u64) -> Option<u64> {
        let epochs = self.epochs.read().await;
        epochs.get(&epoch).copied()
    }

    /// Look up the start offset for the epoch at or before the given epoch.
    pub async fn lookup_le(&self, epoch: u64) -> Option<(u64, u64)> {
        let epochs = self.epochs.read().await;
        epochs.range(..=epoch).next_back().map(|(&e, &o)| (e, o))
    }

    /// Get all epoch entries.
    pub async fn all_epochs(&self) -> BTreeMap<u64, u64> {
        self.epochs.read().await.clone()
    }

    /// Rebuild from a set of (epoch, start_offset) pairs (e.g., from log scan).
    pub async fn rebuild(&self, entries: BTreeMap<u64, u64>) -> Result<()> {
        let mut epochs = self.epochs.write().await;
        *epochs = entries;
        self.persist(&epochs).await
    }

    async fn persist(&self, epochs: &BTreeMap<u64, u64>) -> Result<()> {
        let data = bincode::serialize(epochs).map_err(|e| {
            xraft_core::XraftError::SerializationError(format!(
                "leader-epoch-checkpoint serialize: {e}"
            ))
        })?;

        let temp = self.file_path.with_extension("tmp");
        tokio::fs::write(&temp, &data).await?;
        {
            // Open with write access so sync_all works on Windows
            let f = tokio::fs::OpenOptions::new()
                .write(true)
                .open(&temp)
                .await?;
            f.sync_all().await?;
        }
        tokio::fs::rename(&temp, &self.file_path).await?;

        Ok(())
    }
}
