use std::path::PathBuf;

/// Configuration for the Raft node.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub fetch_interval: Duration,
    pub max_batch_size: usize,
    pub max_fetch_bytes: u32,
    pub snapshot_interval: u64,
    pub data_dir: PathBuf,
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
}
