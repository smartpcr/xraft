use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub fetch_interval_ms: u64,
    pub max_batch_size: usize,
    pub max_fetch_bytes: usize,
    pub snapshot_interval: u64,
    pub data_dir: String,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            fetch_interval_ms: 50,
            max_batch_size: 1000,
            max_fetch_bytes: 1_048_576,
            snapshot_interval: 10000,
            data_dir: "data".to_string(),
        }
    }
}

impl RaftConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.election_timeout_min_ms >= self.election_timeout_max_ms {
            return Err("election_timeout_min must be less than election_timeout_max".into());
        }
        if self.fetch_interval_ms >= self.election_timeout_min_ms {
            return Err("fetch_interval must be less than election_timeout_min".into());
        }
        Ok(())
    }

    pub fn election_timeout_min(&self) -> Duration {
        Duration::from_millis(self.election_timeout_min_ms)
    }

    pub fn election_timeout_max(&self) -> Duration {
        Duration::from_millis(self.election_timeout_max_ms)
    }
}
