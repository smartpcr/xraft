use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::sync::Mutex;
use xraft_core::traits::QuorumStateStore;
use xraft_core::{QuorumState, Result};

/// Persists voting state to a JSON file with atomic write (temp + rename).
///
/// File: `<dir>/quorum-state`
pub struct QuorumStateFile {
    dir: PathBuf,
    cached: Mutex<Option<QuorumState>>,
}

impl QuorumStateFile {
    pub async fn open(dir: &Path) -> Result<Self> {
        tokio::fs::create_dir_all(dir).await?;

        let state_path = dir.join("quorum-state");
        let cached = if state_path.exists() {
            let data = tokio::fs::read_to_string(&state_path).await?;
            let state: QuorumState = serde_json::from_str(&data).map_err(|e| {
                xraft_core::XraftError::Corruption(format!("quorum-state parse error: {e}"))
            })?;
            Some(state)
        } else {
            None
        };

        Ok(Self {
            dir: dir.to_path_buf(),
            cached: Mutex::new(cached),
        })
    }

    fn state_path(&self) -> PathBuf {
        self.dir.join("quorum-state")
    }
}

#[async_trait]
impl QuorumStateStore for QuorumStateFile {
    async fn load(&self) -> Result<Option<QuorumState>> {
        let cached = self.cached.lock().await;
        Ok(cached.clone())
    }

    async fn save(&self, state: &QuorumState) -> Result<()> {
        // Hold the lock for the entire write to serialize concurrent saves.
        let mut cached = self.cached.lock().await;

        let json = serde_json::to_string_pretty(state).map_err(|e| {
            xraft_core::XraftError::SerializationError(format!("quorum-state serialize: {e}"))
        })?;

        // Use a unique temp name to avoid collisions with any other writer
        let temp = self.dir.join(format!(
            "quorum-state.{}.tmp",
            std::process::id()
        ));
        let final_path = self.state_path();

        // Write to temp, fsync, rename (atomic on most filesystems)
        tokio::fs::write(&temp, json.as_bytes()).await?;
        {
            // Open with write access so sync_all works on Windows
            let f = tokio::fs::OpenOptions::new()
                .write(true)
                .open(&temp)
                .await?;
            f.sync_all().await?;
        }
        tokio::fs::rename(&temp, &final_path).await?;

        *cached = Some(state.clone());

        Ok(())
    }
}
