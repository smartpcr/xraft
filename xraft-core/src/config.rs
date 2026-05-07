use serde::{Deserialize, Serialize};

/// Configuration for a Raft node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub fetch_interval_ms: u64,
    pub max_batch_size: usize,
    pub max_fetch_bytes: u32,
    /// Number of committed entries between snapshots.
    pub snapshot_interval: u64,
    pub data_dir: PathBuf,
    /// Maximum segment file size in bytes before rolling to a new segment.
    pub segment_max_bytes: u64,
    /// Sparse index interval: write an index entry every N records.
    pub index_interval: u32,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            fetch_interval_ms: 50,
            max_batch_size: 256,
            max_fetch_bytes: 1024 * 1024,
            snapshot_interval: 10_000,
            data_dir: PathBuf::from("data"),
            segment_max_bytes: 64 * 1024 * 1024, // 64 MiB
            index_interval: 256,
        }
    }
}

impl RaftConfig {
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.election_timeout_min_ms >= self.election_timeout_max_ms {
            return Err(
                "election_timeout_min must be less than election_timeout_max".to_string(),
            );
        }
        if self.fetch_interval_ms >= self.election_timeout_min_ms {
            return Err(
                "fetch_interval must be less than election_timeout_min".to_string(),
            );
        }
        Ok(())
    }

    pub fn election_timeout_min(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.election_timeout_min_ms)
    }

    pub fn election_timeout_max(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.election_timeout_max_ms)
    }
}
