use serde::{Deserialize, Serialize};

/// Configuration for the Raft node.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub fetch_interval_ms: u64,
    pub max_batch_size: usize,
    pub max_fetch_bytes: u32,
    /// Number of committed entries between snapshots.
    pub snapshot_interval: u64,
    pub data_dir: String,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            fetch_interval_ms: 50,
            max_batch_size: 100,
            max_fetch_bytes: 1_048_576,
            snapshot_interval: 1000,
            data_dir: "data".to_string(),
        }
    }
}

impl RaftConfig {
    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.election_timeout_min_ms >= self.election_timeout_max_ms {
            return Err(
                "election_timeout_min_ms must be < election_timeout_max_ms".to_string(),
            );
        }
        if self.fetch_interval_ms >= self.election_timeout_min_ms {
            return Err("fetch_interval_ms must be < election_timeout_min_ms".to_string());
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
