use std::time::Duration;

/// Configuration for a Raft node.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub fetch_interval: Duration,
    pub max_batch_size: usize,
    pub max_fetch_bytes: usize,
    pub snapshot_interval: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            fetch_interval: Duration::from_millis(100),
            max_batch_size: 1000,
            max_fetch_bytes: 1024 * 1024,
            snapshot_interval: 10_000,
        }
    }
}

impl RaftConfig {
    /// Validates that timing constraints are consistent.
    pub fn validate(&self) -> Result<(), String> {
        if self.election_timeout_min >= self.election_timeout_max {
            return Err(
                "election_timeout_min must be less than election_timeout_max".into(),
            );
        }
        if self.fetch_interval >= self.election_timeout_min {
            return Err(
                "fetch_interval must be less than election_timeout_min".into(),
            );
        }
        Ok(())
    }
}
