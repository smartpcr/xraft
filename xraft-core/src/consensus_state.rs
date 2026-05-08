use serde::{Deserialize, Serialize};
use std::fmt;

use crate::types::{NodeId, Term};

/// Role a node occupies in the Raft protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    /// Not attached to any leader; waiting for an election.
    Unattached,
    Follower,
    /// Running for leader election.
    Candidate,
    /// Active leader for the current term.
    Leader,
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Role::Unattached => write!(f, "Unattached"),
            Role::Follower => write!(f, "Follower"),
            Role::Candidate => write!(f, "Candidate"),
            Role::Leader => write!(f, "Leader"),
        }
    }
}

/// Observable consensus state of a node, projected from internal state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusState {
    pub current_term: Term,
    pub role: Role,
    pub leader_id: Option<NodeId>,
    pub voted_for: Option<NodeId>,
    pub high_watermark: u64,
    pub log_end_offset: u64,
    pub log_start_offset: u64,
}
