use std::net::SocketAddr;

use crate::consensus_state::Role;
use crate::error::{Result, XraftError};
use crate::log_entry::{EntryType, LogEntry};
use crate::node_state::{NodeState, PendingMembershipChange};
use crate::rpc::{
    AddVoterRequest, MembershipChangeResponse, MembershipError, RemoveVoterRequest,
    UpdateVoterRequest,
};
use crate::traits::LogStore;
use crate::types::{NodeId, VoterInfo, VotersRecord};

/// Manages dynamic quorum membership changes.
/// Enforces the single-change invariant: rejects any membership RPC while
/// an uncommitted VotersRecord exists in the log.
pub struct MembershipManager;

impl MembershipManager {
    /// Handle a request to add a new voter to the cluster.
    ///
    /// Validates:
    /// - This node is the leader
    /// - No other membership change is in-flight
    /// - Node is not already a voter
    ///
    /// On success, appends a VotersRecord entry to the log and updates
    /// pending_membership_change in NodeState.
    pub async fn handle_add_voter(
        state: &mut NodeState,
        log_store: &dyn LogStore,
        request: AddVoterRequest,
    ) -> Result<MembershipChangeResponse> {
        // Validate leader
        if state.role != Role::Leader {
            return Ok(MembershipChangeResponse::err(MembershipError::NotLeader {
                leader_id: state.leader_id,
            }));
        }

        // Validate no change in progress
        if state.pending_membership_change.is_some() {
            return Ok(MembershipChangeResponse::err(
                MembershipError::ChangeInProgress,
            ));
        }

        // Validate node is not already a voter
        if state.voter_set.iter().any(|v| v.node_id == request.node_id) {
            return Ok(MembershipChangeResponse::err(
                MembershipError::NodeAlreadyVoter,
            ));
        }

        // Build new voter set with the added node
        let mut new_voters = state.voter_set.clone();
        new_voters.push(VoterInfo {
            node_id: request.node_id,
            endpoint: request.endpoint,
        });

        // Create and append VotersRecord
        let record = VotersRecord {
            version: 1,
            voters: new_voters.clone(),
        };
        let payload =
            bincode::serialize(&record).map_err(|e| XraftError::SerializationError(e.to_string()))?;

        let entry = LogEntry {
            offset: state.log_end_offset,
            term: state.current_term,
            entry_type: EntryType::VotersRecord,
            payload,
        };

        log_store.append(&[entry]).await?;

        // Set pending membership change
        let offset = state.log_end_offset;
        state.pending_membership_change = Some(PendingMembershipChange {
            offset,
            voters: new_voters,
        });
        state.log_end_offset += 1;

        Ok(MembershipChangeResponse::ok())
    }

    /// Handle a request to remove a voter from the cluster.
    ///
    /// Validates:
    /// - This node is the leader
    /// - No other membership change is in-flight
    /// - Node exists in voter set
    ///
    /// On success, appends a VotersRecord entry without the removed node.
    pub async fn handle_remove_voter(
        state: &mut NodeState,
        log_store: &dyn LogStore,
        request: RemoveVoterRequest,
    ) -> Result<MembershipChangeResponse> {
        // Validate leader
        if state.role != Role::Leader {
            return Ok(MembershipChangeResponse::err(MembershipError::NotLeader {
                leader_id: state.leader_id,
            }));
        }

        // Validate no change in progress
        if state.pending_membership_change.is_some() {
            return Ok(MembershipChangeResponse::err(
                MembershipError::ChangeInProgress,
            ));
        }

        // Validate node exists in voter set
        if !state.voter_set.iter().any(|v| v.node_id == request.node_id) {
            return Ok(MembershipChangeResponse::err(
                MembershipError::NodeNotFound,
            ));
        }

        // Build new voter set without the removed node
        let new_voters: Vec<VoterInfo> = state
            .voter_set
            .iter()
            .filter(|v| v.node_id != request.node_id)
            .cloned()
            .collect();

        // Create and append VotersRecord
        let record = VotersRecord {
            version: 1,
            voters: new_voters.clone(),
        };
        let payload =
            bincode::serialize(&record).map_err(|e| XraftError::SerializationError(e.to_string()))?;

        let entry = LogEntry {
            offset: state.log_end_offset,
            term: state.current_term,
            entry_type: EntryType::VotersRecord,
            payload,
        };

        log_store.append(&[entry]).await?;

        // Set pending membership change
        let offset = state.log_end_offset;
        state.pending_membership_change = Some(PendingMembershipChange {
            offset,
            voters: new_voters,
        });
        state.log_end_offset += 1;

        Ok(MembershipChangeResponse::ok())
    }

    /// Handle a request to update the endpoint of an existing voter.
    ///
    /// Validates:
    /// - This node is the leader
    /// - No other membership change is in-flight
    /// - Node exists in voter set
    ///
    /// On success, appends a VotersRecord entry with the updated endpoint.
    pub async fn handle_update_voter(
        state: &mut NodeState,
        log_store: &dyn LogStore,
        request: UpdateVoterRequest,
    ) -> Result<MembershipChangeResponse> {
        // Validate leader
        if state.role != Role::Leader {
            return Ok(MembershipChangeResponse::err(MembershipError::NotLeader {
                leader_id: state.leader_id,
            }));
        }

        // Validate no change in progress
        if state.pending_membership_change.is_some() {
            return Ok(MembershipChangeResponse::err(
                MembershipError::ChangeInProgress,
            ));
        }

        // Validate node exists in voter set
        if !state.voter_set.iter().any(|v| v.node_id == request.node_id) {
            return Ok(MembershipChangeResponse::err(
                MembershipError::NodeNotFound,
            ));
        }

        // Build new voter set with updated endpoint
        let new_voters: Vec<VoterInfo> = state
            .voter_set
            .iter()
            .map(|v| {
                if v.node_id == request.node_id {
                    VoterInfo {
                        node_id: v.node_id,
                        endpoint: request.new_endpoint,
                    }
                } else {
                    v.clone()
                }
            })
            .collect();

        // Create and append VotersRecord
        let record = VotersRecord {
            version: 1,
            voters: new_voters.clone(),
        };
        let payload =
            bincode::serialize(&record).map_err(|e| XraftError::SerializationError(e.to_string()))?;

        let entry = LogEntry {
            offset: state.log_end_offset,
            term: state.current_term,
            entry_type: EntryType::VotersRecord,
            payload,
        };

        log_store.append(&[entry]).await?;

        // Set pending membership change
        let offset = state.log_end_offset;
        state.pending_membership_change = Some(PendingMembershipChange {
            offset,
            voters: new_voters,
        });
        state.log_end_offset += 1;

        Ok(MembershipChangeResponse::ok())
    }

    /// Commit a pending membership change by advancing the high watermark
    /// past the VotersRecord offset and applying it to the committed voter set.
    ///
    /// This is called when the event loop determines that a majority of the
    /// **new** voter set has replicated the VotersRecord.
    pub async fn commit_pending_change(
        state: &mut NodeState,
        log_store: &dyn LogStore,
    ) -> Result<()> {
        let pending = match &state.pending_membership_change {
            Some(p) => p.clone(),
            None => return Ok(()),
        };

        // Read the VotersRecord entry from the log
        let entries = log_store
            .read(pending.offset, pending.offset + 1)
            .await?;

        // Advance HW past the pending VotersRecord
        let new_hw = pending.offset + 1;
        state.advance_high_watermark(new_hw, &entries)?;

        Ok(())
    }

    /// Utility: Check if update_voter should update endpoint for node_id.
    pub fn find_voter_endpoint(
        voter_set: &[VoterInfo],
        node_id: NodeId,
    ) -> Option<SocketAddr> {
        voter_set
            .iter()
            .find(|v| v.node_id == node_id)
            .map(|v| v.endpoint)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn make_voter(id: u64, port: u16) -> VoterInfo {
        VoterInfo {
            node_id: NodeId(id),
            endpoint: format!("127.0.0.1:{port}").parse::<SocketAddr>().unwrap(),
        }
    }

    #[test]
    fn test_find_voter_endpoint() {
        let voters = vec![make_voter(1, 5001), make_voter(2, 5002), make_voter(3, 5003)];

        assert_eq!(
            MembershipManager::find_voter_endpoint(&voters, NodeId(2)),
            Some("127.0.0.1:5002".parse().unwrap())
        );
        assert_eq!(
            MembershipManager::find_voter_endpoint(&voters, NodeId(99)),
            None
        );
    }
}
