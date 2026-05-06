use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unique identifier for a Raft node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u64);

/// Raft term (epoch). Monotonically increasing logical clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Term(pub u64);

/// Log entry offset (0-indexed position in the log).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Offset(pub u64);

/// Unique identifier for a Raft cluster backed by a UUID.
/// Used to construct the canonical data directory layout: `data/<cluster_id>/log/`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ClusterId(pub Uuid);

impl ClusterId {
    /// Create a new cluster identifier from a UUID.
    pub fn new(id: Uuid) -> Self {
        Self(id)
    }

    /// Generate a random (v4) cluster identifier.
    pub fn random() -> Self {
        Self(Uuid::new_v4())
    }

    /// Parse a cluster identifier from a string (hyphenated UUID format).
    pub fn parse(s: &str) -> Result<Self, uuid::Error> {
        Ok(Self(Uuid::parse_str(s)?))
    }

    /// Return the cluster id as a hyphenated UUID string.
    pub fn as_str(&self) -> String {
        self.0.to_string()
    }

    /// Return the inner UUID.
    pub fn uuid(&self) -> Uuid {
        self.0
    }
}

impl fmt::Display for ClusterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Uuid> for ClusterId {
    fn from(u: Uuid) -> Self {
        Self(u)
    }
}
