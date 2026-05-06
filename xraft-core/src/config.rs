use std::fmt;
use std::path::PathBuf;

/// Error returned when [`RaftConfig::validate`] detects an invalid configuration.
///
/// This is a construction-time error, separate from [`crate::XraftError`] which
/// covers runtime protocol failures. Keeping them distinct preserves the six-variant
/// contract of `XraftError` (architecture §3.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// `election_timeout_min_ms` is not strictly less than `election_timeout_max_ms`.
    ElectionTimeoutRange {
        min_ms: u64,
        max_ms: u64,
    },
    /// `fetch_interval_ms` is not strictly less than `election_timeout_min_ms`.
    FetchIntervalTooHigh {
        fetch_interval_ms: u64,
        election_timeout_min_ms: u64,
    },
    /// A numeric field that must be positive was set to zero.
    ZeroValue {
        field: &'static str,
    },
}

impl ConfigError {
    /// Returns a human-readable description of the validation failure.
    pub fn message(&self) -> String {
        self.to_string()
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::ElectionTimeoutRange { min_ms, max_ms } => write!(
                f,
                "invalid raft config: election_timeout_min_ms ({min_ms}) must be less than election_timeout_max_ms ({max_ms})"
            ),
            ConfigError::FetchIntervalTooHigh {
                fetch_interval_ms,
                election_timeout_min_ms,
            } => write!(
                f,
                "invalid raft config: fetch_interval_ms ({fetch_interval_ms}) must be less than election_timeout_min_ms ({election_timeout_min_ms})"
            ),
            ConfigError::ZeroValue { field } => {
                write!(f, "invalid raft config: {field} must be greater than 0")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

/// Construction-time configuration for a Raft node.
///
/// **Validation invariant (checked at construction):**
/// `fetch_interval_ms < election_timeout_min_ms < election_timeout_max_ms`
///
/// This satisfies the Raft timing requirement
/// `broadcastTime << electionTimeout << avgTimeBetweenFailures`.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// Lower bound for randomised election timeout (ms).
    pub election_timeout_min_ms: u64,
    /// Upper bound for randomised election timeout (ms).
    pub election_timeout_max_ms: u64,
    /// Follower's periodic Fetch RPC interval (ms).
    pub fetch_interval_ms: u64,
    /// Max entries drained from BatchAccumulator per tick.
    pub max_batch_size: usize,
    /// Max response payload for a single Fetch RPC (bytes).
    pub max_fetch_bytes: u32,
    /// Committed entries between automatic snapshots.
    pub snapshot_interval: u64,
    /// Root directory for log segments, snapshots, quorum-state.
    pub data_dir: PathBuf,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            fetch_interval_ms: 50,
            max_batch_size: 256,
            max_fetch_bytes: 1_048_576, // 1 MiB
            snapshot_interval: 10_000,
            data_dir: PathBuf::from("data"),
        }
    }
}

impl RaftConfig {
    /// Validate the configuration, returning a [`ConfigError`] if any invariant is violated.
    ///
    /// Checks:
    /// - `election_timeout_min_ms < election_timeout_max_ms`
    /// - `fetch_interval_ms < election_timeout_min_ms`
    /// - `max_batch_size > 0`
    /// - `max_fetch_bytes > 0`
    /// - `snapshot_interval > 0`
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.election_timeout_min_ms >= self.election_timeout_max_ms {
            return Err(ConfigError::ElectionTimeoutRange {
                min_ms: self.election_timeout_min_ms,
                max_ms: self.election_timeout_max_ms,
            });
        }
        if self.fetch_interval_ms >= self.election_timeout_min_ms {
            return Err(ConfigError::FetchIntervalTooHigh {
                fetch_interval_ms: self.fetch_interval_ms,
                election_timeout_min_ms: self.election_timeout_min_ms,
            });
        }
        if self.max_batch_size == 0 {
            return Err(ConfigError::ZeroValue {
                field: "max_batch_size",
            });
        }
        if self.max_fetch_bytes == 0 {
            return Err(ConfigError::ZeroValue {
                field: "max_fetch_bytes",
            });
        }
        if self.snapshot_interval == 0 {
            return Err(ConfigError::ZeroValue {
                field: "snapshot_interval",
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let config = RaftConfig::default();
        config.validate().expect("default config should be valid");
    }

    #[test]
    fn default_config_satisfies_timing_invariant() {
        let config = RaftConfig::default();
        assert!(
            config.fetch_interval_ms < config.election_timeout_min_ms,
            "fetch_interval_ms must be less than election_timeout_min_ms"
        );
        assert!(
            config.election_timeout_min_ms < config.election_timeout_max_ms,
            "election_timeout_min_ms must be less than election_timeout_max_ms"
        );
    }

    #[test]
    fn config_error_displays_with_prefix() {
        let err = ConfigError::ElectionTimeoutRange {
            min_ms: 500,
            max_ms: 300,
        };
        assert!(err.to_string().contains("invalid raft config"));
        assert!(err.to_string().contains("500"));
    }

    #[test]
    fn config_error_structured_variants() {
        let err = ConfigError::ZeroValue {
            field: "max_batch_size",
        };
        assert_eq!(
            err,
            ConfigError::ZeroValue {
                field: "max_batch_size"
            }
        );
        assert!(err.message().contains("max_batch_size"));
    }

    #[test]
    fn config_error_implements_std_error() {
        let err = ConfigError::ZeroValue {
            field: "snapshot_interval",
        };
        let _: &dyn std::error::Error = &err;
    }

    #[test]
    fn error_when_min_exceeds_max_timeout() {
        let config = RaftConfig {
            election_timeout_min_ms: 500,
            election_timeout_max_ms: 300,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert_eq!(
            err,
            ConfigError::ElectionTimeoutRange {
                min_ms: 500,
                max_ms: 300
            }
        );
    }

    #[test]
    fn error_when_min_equals_max_timeout() {
        let config = RaftConfig {
            election_timeout_min_ms: 300,
            election_timeout_max_ms: 300,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn error_when_fetch_interval_exceeds_min_timeout() {
        let config = RaftConfig {
            fetch_interval_ms: 200,
            election_timeout_min_ms: 150,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert_eq!(
            err,
            ConfigError::FetchIntervalTooHigh {
                fetch_interval_ms: 200,
                election_timeout_min_ms: 150
            }
        );
    }

    #[test]
    fn error_when_fetch_interval_equals_min_timeout() {
        let config = RaftConfig {
            fetch_interval_ms: 150,
            election_timeout_min_ms: 150,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn error_when_max_batch_size_is_zero() {
        let config = RaftConfig {
            max_batch_size: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn error_when_max_fetch_bytes_is_zero() {
        let config = RaftConfig {
            max_fetch_bytes: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn error_when_snapshot_interval_is_zero() {
        let config = RaftConfig {
            snapshot_interval: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn valid_custom_config() {
        let config = RaftConfig {
            election_timeout_min_ms: 200,
            election_timeout_max_ms: 400,
            fetch_interval_ms: 100,
            max_batch_size: 512,
            max_fetch_bytes: 2_097_152,
            snapshot_interval: 50_000,
            data_dir: PathBuf::from("/var/lib/xraft"),
        };
        config.validate().expect("custom config should be valid");
    }
}
