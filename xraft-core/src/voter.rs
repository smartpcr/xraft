use serde::{Deserialize, Serialize};

use crate::types::Term;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoterInfo {
    pub id: u64,
    pub endpoint: String,
    pub last_known_term: Term,
}

impl VoterInfo {
    pub fn new(id: u64, endpoint: impl Into<String>) -> Self {
        Self {
            id,
            endpoint: endpoint.into(),
            last_known_term: Term::default(),
        }
    }
}
