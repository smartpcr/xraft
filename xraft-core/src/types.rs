use serde::{Deserialize, Serialize};
use std::fmt;

/// Unique numeric identifier for a node within a cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u64);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({})", self.0)
    }
}

/// Monotonically increasing logical clock identifying an election cycle.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct Term(pub u64);

impl Term {
    pub const ZERO: Term = Term(0);

    pub fn next(self) -> Term {
        Term(self.0 + 1)
    }
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Term({})", self.0)
    }
}

/// Cluster identity for RPC fencing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClusterId(pub uuid::Uuid);

impl Default for ClusterId {
    fn default() -> Self {
        ClusterId(uuid::Uuid::nil())
    }
}

impl fmt::Display for ClusterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClusterId({})", self.0)
    }
}

/// Log offset (0-indexed).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct Offset(pub u64);

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Offset({})", self.0)
    }
}

impl From<u64> for Offset {
    fn from(v: u64) -> Self {
        Offset(v)
    }
}

impl From<Offset> for u64 {
    fn from(o: Offset) -> u64 {
        o.0
    }
}
