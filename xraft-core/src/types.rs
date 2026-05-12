use std::fmt;

use serde::{Deserialize, Serialize};

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
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "T{}", self.0)
    }
}

/// Position in the log (0-indexed).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct Offset(pub u64);

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "@{}", self.0)
    }
}

/// Cluster identity for fencing — prevents cross-cluster message delivery.
/// Generated once by the operator.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn term_ordering() {
        assert!(Term(3) < Term(5));
        assert!(Term(5) > Term(3));
        assert_eq!(Term(3), Term(3));
    }

    #[test]
    fn node_id_ordering() {
        assert!(NodeId(1) < NodeId(2));
    }

    #[test]
    fn offset_ordering() {
        assert!(Offset(0) < Offset(100));
    }

    #[test]
    fn cluster_id_equality() {
        let id = uuid::Uuid::new_v4();
        assert_eq!(ClusterId(id), ClusterId(id));
    }
}
