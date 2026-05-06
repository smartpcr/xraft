use std::fmt;

/// Unique numeric identifier for a node within the cluster.
///
/// Newtype wrapper around `u64`, consistent with Stage 1.2 of the
/// implementation plan. Additional core types (`Term`, `ClusterId`,
/// `Offset`) will be added here in Stage 1.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub u64);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
