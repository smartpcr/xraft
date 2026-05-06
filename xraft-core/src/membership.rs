//! Membership Manager — Add Voter (Stage 8.1)
//!
//! Implements dynamic membership changes for the xraft Raft protocol.
//! Enforces the single-change invariant: at most one uncommitted VotersRecord
//! may exist in the log at a time.
//!
//! # Quorum transition (dual semantics)
//!
//! Once a VotersRecord is appended to the log:
//! - **HW advancement** immediately uses the **new** (pending) voter set for
//!   entries at or after the VotersRecord's offset, and the **committed** voter
//!   set for entries before it (architecture §5.5).
//! - **Elections, Check Quorum, and `read().voter_set`** continue to use the
//!   **committed** voter set until the VotersRecord is committed.
//!
//! On commit, `voter_set` is atomically replaced and `pending_membership_change`
//! is cleared.
//!
//! # Integration with EventLoop / LogStore
//!
//! `MembershipManager` operates on `NodeState` (in-memory protocol state) and
//! a `&mut dyn LogStore` (architecture §2.2). In the full system the
//! `EventLoop` (§2.1) dispatches inbound RPCs to `MembershipManager` methods
//! and runs `try_advance_high_watermark` + `try_commit_membership_change` after
//! every state mutation. `handle_fetch_request` integrates with the Fetch path
//! used by both voters and observers.

use crate::log_entry::LogEntry;
use crate::node_state::{
    FollowerProgress, LogStoreError, NodeState, PendingMembershipChange, Role, SyncLogOps,
};
use crate::rpc::{
    AddVoterRequest, DeregisterObserverRequest, DeregisterObserverResponse, DivergingEpoch,
    FetchRequest, FetchResponse, MembershipChangeResponse, MembershipError,
    RegisterObserverRequest, RegisterObserverResponse, SnapshotId,
};
use crate::types::NodeId;
use crate::voter::{Endpoint, VoterInfo, VotersRecord};

/// Configuration for the MembershipManager.
#[derive(Debug, Clone)]
pub struct MembershipConfig {
    /// Maximum allowed gap between observer's fetch_offset and leader's
    /// log_end_offset for threshold-based readiness (secondary heuristic).
    /// The primary safety gate is `fetch_offset >= high_watermark`.
    pub catch_up_threshold: u64,
    /// Maximum number of entries returned per Fetch response.
    pub max_fetch_entries: usize,
}

impl Default for MembershipConfig {
    fn default() -> Self {
        MembershipConfig {
            catch_up_threshold: 10,
            max_fetch_entries: 100,
        }
    }
}

/// Manages cluster membership changes (add/remove voter, observer lifecycle).
///
/// Enforces the single-change invariant and dual-quorum semantics per
/// architecture §5.5.
///
/// In the full system, the `EventLoop` (architecture §2.1) owns the
/// `MembershipManager` and calls its methods when processing inbound RPCs.
/// The `EventLoop` also drives HW advancement and membership commit after
/// every state mutation.
#[derive(Debug)]
pub struct MembershipManager {
    pub config: MembershipConfig,
}

impl MembershipManager {
    pub fn new(config: MembershipConfig) -> Self {
        MembershipManager { config }
    }

    /// Register a new node as an observer (non-voting) — internal helper.
    ///
    /// Observers replicate the log via Fetch but do not contribute to quorum.
    /// The leader tracks their `FollowerProgress` with `is_voter = false` so
    /// their `fetch_offset` is excluded from HW calculation (architecture §5.4).
    ///
    /// For RPC-facing registration, use `handle_register_observer` which
    /// performs leader validation and returns a structured response.
    ///
    /// Returns `true` if newly registered, `false` if already an observer or voter.
    fn register_observer_internal(
        &self,
        node_id: NodeId,
        endpoint: Endpoint,
        state: &mut NodeState,
    ) -> bool {
        if state.is_voter(node_id) || state.is_observer(node_id) {
            return false;
        }
        state.observers.insert(node_id);
        state.observer_endpoints.insert(node_id, endpoint);
        state.follower_state.insert(
            node_id,
            FollowerProgress {
                node_id,
                fetch_offset: 0,
                is_voter: false,
            },
        );
        true
    }

    /// Handle a RegisterObserver RPC on the leader.
    ///
    /// Validates that this node is the leader before registering.
    /// Returns a `RegisterObserverResponse` with appropriate error if
    /// this node is not the leader, or if the node is already a voter
    /// or observer.
    pub fn handle_register_observer(
        &self,
        request: &RegisterObserverRequest,
        state: &mut NodeState,
    ) -> RegisterObserverResponse {
        let leader_id = state.leader_id;

        if state.role != Role::Leader {
            return RegisterObserverResponse::error(
                MembershipError::NotLeader { leader_id },
                leader_id,
            );
        }

        if state.is_voter(request.node_id) {
            return RegisterObserverResponse::error(
                MembershipError::NodeAlreadyVoter,
                leader_id,
            );
        }

        let registered = self.register_observer_internal(
            request.node_id,
            request.endpoint,
            state,
        );

        if registered {
            RegisterObserverResponse::success(leader_id)
        } else {
            // Already an observer
            RegisterObserverResponse::error(
                MembershipError::ChangeInProgress,
                leader_id,
            )
        }
    }

    /// Handle a DeregisterObserver RPC on the leader.
    ///
    /// Removes the observer from the cluster. Rejects if:
    /// - This node is not the leader → `NotLeader`
    /// - Target is a voter → `NodeAlreadyVoter`
    /// - Target is being promoted (pending membership change) → `ChangeInProgress`
    /// - Target is not a registered observer → `NodeNotFound`
    pub fn handle_deregister_observer(
        &self,
        request: &DeregisterObserverRequest,
        state: &mut NodeState,
    ) -> DeregisterObserverResponse {
        let leader_id = state.leader_id;

        if state.role != Role::Leader {
            return DeregisterObserverResponse::error(
                MembershipError::NotLeader { leader_id },
                leader_id,
            );
        }

        if state.is_voter(request.node_id) {
            return DeregisterObserverResponse::error(
                MembershipError::NodeAlreadyVoter,
                leader_id,
            );
        }

        // Reject if this observer is currently being promoted
        if let Some(ref pending) = state.pending_membership_change {
            if pending.promoted_node_id == request.node_id {
                return DeregisterObserverResponse::error(
                    MembershipError::ChangeInProgress,
                    leader_id,
                );
            }
        }

        if !state.is_observer(request.node_id) {
            return DeregisterObserverResponse::error(
                MembershipError::NodeNotFound,
                leader_id,
            );
        }

        state.observers.remove(&request.node_id);
        state.observer_endpoints.remove(&request.node_id);
        state.follower_state.remove(&request.node_id);

        DeregisterObserverResponse::success(leader_id)
    }

    /// Update a replica's fetch_offset (called when the leader processes
    /// a Fetch request from this follower or observer).
    pub fn update_fetch_offset(
        &self,
        node_id: NodeId,
        fetch_offset: u64,
        state: &mut NodeState,
    ) {
        if let Some(fp) = state.follower_state.get_mut(&node_id) {
            fp.fetch_offset = fetch_offset;
        }
    }

    /// Check whether an observer is caught up enough for promotion.
    ///
    /// Promotion eligibility enforces two gates:
    /// 1. **Primary safety gate**: `fetch_offset >= high_watermark` — the
    ///    observer must have replicated all committed entries. This is the
    ///    hard requirement from architecture §5.5 that makes it safe for
    ///    the new voter's `fetch_offset` to count towards committing the
    ///    VotersRecord itself.
    /// 2. **Secondary heuristic**: `fetch_offset` must be within
    ///    `catch_up_threshold` entries of `log_end_offset` — prevents
    ///    promoting a node that has committed entries but is far behind
    ///    the leader's tip.
    pub fn is_observer_caught_up(&self, node_id: NodeId, state: &NodeState) -> bool {
        let fp = match state.follower_state.get(&node_id) {
            Some(fp) => fp,
            None => return false,
        };
        // Gate 1: must have replicated at least up to current HW
        if fp.fetch_offset < state.high_watermark {
            return false;
        }
        // Gate 2: must be within threshold of log tip
        let gap = state.log_end_offset.saturating_sub(fp.fetch_offset);
        gap <= self.config.catch_up_threshold
    }

    /// Process an AddVoter RPC on the leader.
    ///
    /// Validates:
    /// 1. This node is the leader
    /// 2. No other membership change is in-flight (single-change invariant)
    /// 3. Target node is not already a voter
    /// 4. Target node is registered as an observer
    /// 5. Observer is caught up (fetch_offset ≥ HW)
    ///
    /// On success: appends a VotersRecord control entry to the log and sets
    /// `pending_membership_change`. HW advancement immediately uses the new
    /// voter set for entries at or after the VotersRecord's offset.
    pub fn handle_add_voter(
        &self,
        request: &AddVoterRequest,
        state: &mut NodeState,
        log: &dyn SyncLogOps,
    ) -> Result<MembershipChangeResponse, LogStoreError> {
        let leader_id = state.leader_id;

        // 1. Must be the leader
        if state.role != Role::Leader {
            return Ok(MembershipChangeResponse::error(
                MembershipError::NotLeader { leader_id },
                leader_id,
            ));
        }

        // 2. Single-change invariant: reject if any uncommitted VotersRecord
        //    exists. Check both the in-memory pending flag AND scan the log.
        if state.pending_membership_change.is_some()
            || log.has_uncommitted_voters_record_sync(state.high_watermark)
        {
            return Ok(MembershipChangeResponse::error(
                MembershipError::ChangeInProgress,
                leader_id,
            ));
        }

        // 3. Target must not already be a voter
        if state.is_voter(request.node_id) {
            return Ok(MembershipChangeResponse::error(
                MembershipError::NodeAlreadyVoter,
                leader_id,
            ));
        }

        // 4. Target must be a registered observer
        if !state.is_observer(request.node_id) {
            return Ok(MembershipChangeResponse::error(
                MembershipError::NodeNotFound,
                leader_id,
            ));
        }

        // 5. Observer must be caught up
        if !self.is_observer_caught_up(request.node_id, state) {
            return Ok(MembershipChangeResponse::error(
                MembershipError::NodeNotCaughtUp,
                leader_id,
            ));
        }

        // Build the new voter set: current voters + new voter
        let mut new_voters = state.voter_set.clone();
        new_voters.push(VoterInfo {
            node_id: request.node_id,
            endpoint: request.endpoint.clone(),
        });

        let voters_record = VotersRecord {
            version: 1,
            voters: new_voters.clone(),
        };

        // Append VotersRecord to log via the SyncLogOps trait.
        // Append is fallible — storage errors propagate to the caller
        // as LogStoreError (distinct from MembershipError validation errors).
        let append_offset = state.log_end_offset;
        let entry = LogEntry::voters_record(append_offset, state.current_term, &voters_record);
        log.append_entry(entry)?;

        // Append succeeded — now mutate in-memory state.
        state.log_end_offset = append_offset + 1;

        // Capture the promoted node's endpoint before removing from observer maps
        let promoted_endpoint = state
            .observer_endpoints
            .get(&request.node_id)
            .cloned()
            .unwrap_or_else(|| request.endpoint.clone());

        // Set pending membership change — HW advancement will now use the
        // new voter set for entries at or after this offset.
        state.pending_membership_change = Some(PendingMembershipChange {
            offset: append_offset,
            voters: new_voters,
            promoted_node_id: request.node_id,
            promoted_endpoint,
        });

        // Remove from observers set (node is now in the pending voter set)
        state.observers.remove(&request.node_id);
        state.observer_endpoints.remove(&request.node_id);

        Ok(MembershipChangeResponse::success(leader_id))
    }

    /// Handle an inbound Fetch request from a follower or observer.
    ///
    /// This is the primary integration point between the membership manager
    /// and the Fetch replication path (architecture §3.3 / §5.4). The
    /// EventLoop dispatches FetchRequests here; the returned FetchResponse
    /// is sent back via the IoStage's TransportSender.
    ///
    /// Handles:
    /// - Unknown replica validation (returns empty response)
    /// - Snapshot indication when `fetch_offset < log_start_offset`
    /// - Log divergence detection via `last_fetched_epoch`
    /// - `max_bytes` enforcement from FetchRequest
    /// - HW re-calculation and membership commit check
    pub fn handle_fetch_request(
        &self,
        request: &FetchRequest,
        state: &mut NodeState,
        log: &dyn SyncLogOps,
    ) -> FetchResponse {
        let log_start = log.start_offset();

        // Reject unknown replicas: must be a voter or registered observer
        if !state.is_voter(request.replica_id)
            && !state.is_observer(request.replica_id)
            && !self.is_pending_voter(request.replica_id, state)
        {
            return FetchResponse {
                leader_id: state.node_id,
                leader_epoch: state.current_term,
                high_watermark: state.high_watermark,
                log_start_offset: log_start,
                entries: vec![],
                diverging_epoch: None,
                snapshot_id: None,
            };
        }

        // Case 1: fetch_offset < log_start_offset → need snapshot transfer.
        // Do NOT update fetch_offset — invalid offsets must not count toward quorum.
        if request.fetch_offset < log_start {
            let snapshot_epoch = log.entry_term_at(log_start).unwrap_or(state.current_term);
            return FetchResponse {
                leader_id: state.node_id,
                leader_epoch: state.current_term,
                high_watermark: state.high_watermark,
                log_start_offset: log_start,
                entries: vec![],
                diverging_epoch: None,
                snapshot_id: Some(SnapshotId {
                    end_offset: log_start,
                    epoch: snapshot_epoch,
                }),
            };
        }

        // Case 2: divergence detection — compare last_fetched_epoch with
        // the leader's term at the follower's previous entry.
        // Do NOT update fetch_offset — divergent offsets must not count toward quorum.
        if request.fetch_offset > 0 && request.fetch_offset > log_start {
            let prev_offset = request.fetch_offset - 1;
            if let Some(leader_term) = log.entry_term_at(prev_offset) {
                if leader_term != request.last_fetched_epoch {
                    let end_offset = log.epoch_end_offset(request.last_fetched_epoch);
                    return FetchResponse {
                        leader_id: state.node_id,
                        leader_epoch: state.current_term,
                        high_watermark: state.high_watermark,
                        log_start_offset: log_start,
                        entries: vec![],
                        diverging_epoch: Some(DivergingEpoch {
                            epoch: request.last_fetched_epoch,
                            end_offset,
                        }),
                        snapshot_id: None,
                    };
                }
            }
        }

        // Validation passed — update the replica's replication progress.
        // Only valid fetch offsets reach this point, so counting them
        // toward quorum is safe.
        self.update_fetch_offset(request.replica_id, request.fetch_offset, state);

        // Re-calculate HW after updating follower progress
        self.try_advance_high_watermark(state);

        // Check if the pending VotersRecord is now committed
        self.try_commit_membership_change(state);

        // Case 3: normal — read entries respecting both max_entries and max_bytes
        let max_entries = self.config.max_fetch_entries;
        let entries = log.read_entries_bounded(
            request.fetch_offset,
            max_entries,
            request.max_bytes,
        );

        FetchResponse {
            leader_id: state.node_id,
            leader_epoch: state.current_term,
            high_watermark: state.high_watermark,
            log_start_offset: log_start,
            entries,
            diverging_epoch: None,
            snapshot_id: None,
        }
    }

    /// Check if a node is in the pending voter set (promoted but not yet committed).
    fn is_pending_voter(&self, node_id: NodeId, state: &NodeState) -> bool {
        if let Some(ref pending) = state.pending_membership_change {
            pending.voters.iter().any(|v| v.node_id == node_id)
        } else {
            false
        }
    }

    /// Advance the high watermark considering dual-quorum semantics.
    ///
    /// Uses `NodeState::compute_high_watermark` which applies the committed
    /// voter set for entries before the pending VotersRecord and the new
    /// voter set for entries at or after it (architecture §5.5).
    pub fn try_advance_high_watermark(&self, state: &mut NodeState) {
        let new_hw = state.compute_high_watermark(state.log_end_offset);
        if new_hw > state.high_watermark {
            state.high_watermark = new_hw;
        }
    }

    /// Check if the pending membership change has been committed (HW has
    /// advanced past the VotersRecord's offset) and finalize if so.
    ///
    /// Returns `true` if a membership change was committed.
    pub fn try_commit_membership_change(&self, state: &mut NodeState) -> bool {
        let should_commit = if let Some(ref pending) = state.pending_membership_change {
            // The VotersRecord at `pending.offset` is committed when
            // `high_watermark > pending.offset` (HW is exclusive).
            state.high_watermark > pending.offset
        } else {
            false
        };

        if should_commit {
            state.commit_membership_change();
            true
        } else {
            false
        }
    }

    /// Handle log truncation: if the uncommitted VotersRecord is discarded
    /// during divergence handling, clear the pending membership change and
    /// restore the promoted node as an observer.
    ///
    /// This ensures the node can be re-promoted via a future AddVoter once
    /// the new leader stabilises, rather than being lost from both the
    /// voter set and the observer set.
    pub fn handle_log_truncation(&self, state: &mut NodeState, truncate_from: u64) {
        if let Some(pending) = state.pending_membership_change.take() {
            if pending.offset >= truncate_from {
                // Restore the promoted node as an observer so a retry
                // of AddVoter can succeed without re-registration.
                state.observers.insert(pending.promoted_node_id);
                state
                    .observer_endpoints
                    .insert(pending.promoted_node_id, pending.promoted_endpoint);
                // FollowerProgress entry already exists; reset is_voter
                if let Some(fp) = state.follower_state.get_mut(&pending.promoted_node_id) {
                    fp.is_voter = false;
                }
            } else {
                // VotersRecord is before truncation point — keep it.
                state.pending_membership_change = Some(pending);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_state::InMemoryLog;
    use crate::rpc::{
        DeregisterObserverRequest, FetchRequest, RegisterObserverRequest,
    };
    use crate::types::Term;
    use std::net::SocketAddr;

    fn node(id: u64) -> NodeId {
        NodeId(id)
    }

    fn endpoint(id: u64) -> Endpoint {
        format!("127.0.0.1:{}", 9000 + id).parse::<SocketAddr>().unwrap()
    }

    fn voter(id: u64) -> VoterInfo {
        VoterInfo {
            node_id: NodeId(id),
            endpoint: endpoint(id),
        }
    }

    fn setup_3_node_leader() -> (NodeState, InMemoryLog, MembershipManager) {
        let voters = vec![voter(1), voter(2), voter(3)];
        let state = NodeState::new_leader(node(1), Term(1), voters);
        let log = InMemoryLog::new();
        let mgr = MembershipManager::new(MembershipConfig::default());
        (state, log, mgr)
    }

    /// Helper: populate log with N command entries and catch up followers.
    fn populate_log(
        state: &mut NodeState,
        log: &InMemoryLog,
        count: u64,
    ) {
        for i in 0..count {
            log.append_entry(LogEntry::command(i, Term(1), vec![i as u8]))
                .unwrap();
            state.log_end_offset = i + 1;
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Scenario: Add voter — 3-node cluster, N4 observer caught up,
    // AddVoter(N4) appends VotersRecord [N1,N2,N3,N4], HW uses new
    // voter set (majority = 3 of 4), once committed N4 is a voter.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_add_voter_success() {
        let (mut state, log, mgr) = setup_3_node_leader();

        populate_log(&mut state, &log, 10);

        // Simulate followers catching up (N2, N3 have fetched all entries)
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 10;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 10;
        mgr.try_advance_high_watermark(&mut state);
        assert_eq!(state.high_watermark, 10);

        // Register N4 as observer via RPC handler
        let reg_req = RegisterObserverRequest {
            node_id: node(4),
            endpoint: endpoint(4),
        };
        let reg_resp = mgr.handle_register_observer(&reg_req, &mut state);
        assert!(reg_resp.success);
        assert!(state.is_observer(node(4)));
        assert_eq!(state.observer_endpoints.get(&node(4)), Some(&endpoint(4)));
        assert!(!state.follower_state.get(&node(4)).unwrap().is_voter);

        mgr.update_fetch_offset(node(4), 10, &mut state);
        assert!(mgr.is_observer_caught_up(node(4), &state));

        // AddVoter(N4)
        let request = AddVoterRequest {
            node_id: node(4),
            endpoint: endpoint(4),
        };
        let response = mgr.handle_add_voter(&request, &mut state, &log).unwrap();
        assert!(response.success);
        assert!(response.error.is_none());

        // VotersRecord should be appended at offset 10
        assert_eq!(state.log_end_offset, 11);
        assert!(state.pending_membership_change.is_some());
        let pending = state.pending_membership_change.as_ref().unwrap();
        assert_eq!(pending.offset, 10);
        assert_eq!(pending.voters.len(), 4);
        assert_eq!(pending.promoted_node_id, node(4));

        // N4 is no longer an observer (moved to pending voter set)
        assert!(!state.is_observer(node(4)));
        assert!(!state.observer_endpoints.contains_key(&node(4)));

        // HW advancement should now use the NEW voter set (4 voters, majority = 3)
        let effective_voters = state.effective_voter_set_for_hw(10);
        assert_eq!(effective_voters.len(), 4);

        // Simulate N4 fetching past the VotersRecord
        mgr.update_fetch_offset(node(4), 11, &mut state);
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 11;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 11;

        mgr.try_advance_high_watermark(&mut state);
        assert_eq!(state.high_watermark, 11);

        // VotersRecord at offset 10 is now committed (11 > 10)
        let committed = mgr.try_commit_membership_change(&mut state);
        assert!(committed);

        // After commit: voter_set should be [N1,N2,N3,N4], pending cleared
        assert!(state.pending_membership_change.is_none());
        assert_eq!(state.voter_set.len(), 4);
        assert!(state.is_voter(node(4)));
        assert!(state.follower_state.get(&node(4)).unwrap().is_voter);

        let committed_voters = state.effective_voter_set_for_hw(11);
        assert_eq!(committed_voters.len(), 4);
    }

    // ─────────────────────────────────────────────────────────────────
    // Scenario: Concurrent change rejected — membership change in
    // progress → second AddVoter returns ChangeInProgress.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_concurrent_change_rejected() {
        let (mut state, log, mgr) = setup_3_node_leader();

        populate_log(&mut state, &log, 5);
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 5;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 5;
        mgr.try_advance_high_watermark(&mut state);

        // Register and catch up two observers
        mgr.register_observer_internal(node(4), endpoint(4), &mut state);
        mgr.update_fetch_offset(node(4), 5, &mut state);
        mgr.register_observer_internal(node(5), endpoint(5), &mut state);
        mgr.update_fetch_offset(node(5), 5, &mut state);

        // First AddVoter succeeds
        let req1 = AddVoterRequest {
            node_id: node(4),
            endpoint: endpoint(4),
        };
        let resp1 = mgr.handle_add_voter(&req1, &mut state, &log).unwrap();
        assert!(resp1.success);

        // Second AddVoter while first is in progress → ChangeInProgress
        let req2 = AddVoterRequest {
            node_id: node(5),
            endpoint: endpoint(5),
        };
        let resp2 = mgr.handle_add_voter(&req2, &mut state, &log).unwrap();
        assert!(!resp2.success);
        assert_eq!(resp2.error, Some(MembershipError::ChangeInProgress));
    }

    // ─────────────────────────────────────────────────────────────────
    // Scenario: Observer catch-up — N4 joins with empty log, leader has
    // 1000 entries. N4 fetches to within 10 entries → eligible.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_observer_catch_up() {
        let (mut state, log, mgr) = setup_3_node_leader();

        // Leader has 1000 entries
        populate_log(&mut state, &log, 1000);
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 1000;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 1000;
        mgr.try_advance_high_watermark(&mut state);
        assert_eq!(state.high_watermark, 1000);

        // Register N4 as observer with fetch_offset = 0 (empty log)
        mgr.register_observer_internal(node(4), endpoint(4), &mut state);
        assert!(!mgr.is_observer_caught_up(node(4), &state));

        // N4 fetches up to 500 — still not caught up (gap = 500 > threshold 10)
        mgr.update_fetch_offset(node(4), 500, &mut state);
        assert!(!mgr.is_observer_caught_up(node(4), &state));

        // N4 fetches up to 990 — within threshold of log_end, but below HW
        // Primary safety gate (fetch_offset >= HW) is NOT satisfied: 990 < 1000
        mgr.update_fetch_offset(node(4), 990, &mut state);
        assert!(!mgr.is_observer_caught_up(node(4), &state));

        // N4 fetches up to 1000 — caught up (fetch_offset >= HW AND gap = 0)
        mgr.update_fetch_offset(node(4), 1000, &mut state);
        assert!(mgr.is_observer_caught_up(node(4), &state));

        // Verify that AddVoter now succeeds
        let request = AddVoterRequest {
            node_id: node(4),
            endpoint: endpoint(4),
        };
        let response = mgr.handle_add_voter(&request, &mut state, &log).unwrap();
        assert!(response.success);
    }

    // ─────────────────────────────────────────────────────────────────
    // Scenario: Observer not caught up — N4 is 500 entries behind,
    // AddVoter(N4) returns NodeNotCaughtUp.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_observer_not_caught_up() {
        let (mut state, log, mgr) = setup_3_node_leader();

        populate_log(&mut state, &log, 1000);
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 1000;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 1000;
        mgr.try_advance_high_watermark(&mut state);

        // Register N4, only caught up to offset 500 (500 behind leader)
        mgr.register_observer_internal(node(4), endpoint(4), &mut state);
        mgr.update_fetch_offset(node(4), 500, &mut state);
        assert!(!mgr.is_observer_caught_up(node(4), &state));

        // AddVoter(N4) should fail with NodeNotCaughtUp
        let request = AddVoterRequest {
            node_id: node(4),
            endpoint: endpoint(4),
        };
        let response = mgr.handle_add_voter(&request, &mut state, &log).unwrap();
        assert!(!response.success);
        assert_eq!(response.error, Some(MembershipError::NodeNotCaughtUp));
    }

    // ─────────────────────────────────────────────────────────────────
    // Not leader → NotLeader error with leader_id hint.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_add_voter_not_leader() {
        let voters = vec![voter(1), voter(2), voter(3)];
        let mut state = NodeState {
            node_id: node(2),
            current_term: Term(1),
            role: Role::Follower,
            leader_id: Some(node(1)),
            log_end_offset: 0,
            high_watermark: 0,
            voter_set: voters,
            observers: std::collections::HashSet::new(),
            observer_endpoints: std::collections::HashMap::new(),
            pending_membership_change: None,
            follower_state: std::collections::HashMap::new(),
        };
        let log = InMemoryLog::new();
        let mgr = MembershipManager::new(MembershipConfig::default());

        let request = AddVoterRequest {
            node_id: node(4),
            endpoint: endpoint(4),
        };
        let response = mgr.handle_add_voter(&request, &mut state, &log).unwrap();
        assert!(!response.success);
        assert_eq!(
            response.error,
            Some(MembershipError::NotLeader {
                leader_id: Some(node(1))
            })
        );
    }

    // ─────────────────────────────────────────────────────────────────
    // AddVoter for existing voter → NodeAlreadyVoter.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_add_voter_already_voter() {
        let (mut state, log, mgr) = setup_3_node_leader();

        let request = AddVoterRequest {
            node_id: node(2),
            endpoint: endpoint(2),
        };
        let response = mgr.handle_add_voter(&request, &mut state, &log).unwrap();
        assert!(!response.success);
        assert_eq!(response.error, Some(MembershipError::NodeAlreadyVoter));
    }

    // ─────────────────────────────────────────────────────────────────
    // AddVoter for unknown node → NodeNotFound.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_add_voter_node_not_found() {
        let (mut state, log, mgr) = setup_3_node_leader();

        let request = AddVoterRequest {
            node_id: node(99),
            endpoint: endpoint(99),
        };
        let response = mgr.handle_add_voter(&request, &mut state, &log).unwrap();
        assert!(!response.success);
        assert_eq!(response.error, Some(MembershipError::NodeNotFound));
    }

    // ─────────────────────────────────────────────────────────────────
    // Dual quorum semantics: after append but before commit, HW uses
    // new voter set for entries at/after VotersRecord, committed
    // voter set for entries before it.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_dual_quorum_semantics() {
        let (mut state, log, mgr) = setup_3_node_leader();

        populate_log(&mut state, &log, 10);
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 10;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 10;
        mgr.try_advance_high_watermark(&mut state);

        // Register and catch up N4
        mgr.register_observer_internal(node(4), endpoint(4), &mut state);
        mgr.update_fetch_offset(node(4), 10, &mut state);

        // Add N4
        let request = AddVoterRequest {
            node_id: node(4),
            endpoint: endpoint(4),
        };
        let resp = mgr.handle_add_voter(&request, &mut state, &log).unwrap();
        assert!(resp.success);

        // After append, before commit:
        // - Committed voter_set should still be [N1, N2, N3]
        assert_eq!(state.voter_set.len(), 3);
        assert!(!state.is_voter(node(4)));

        // - Effective voter set for HW at offset ≥ VotersRecord should be [N1,N2,N3,N4]
        let eff = state.effective_voter_set_for_hw(10);
        assert_eq!(eff.len(), 4);

        // - Effective voter set for earlier offsets should be [N1,N2,N3]
        let eff_old = state.effective_voter_set_for_hw(9);
        assert_eq!(eff_old.len(), 3);

        assert!(state.pending_membership_change.is_some());
    }

    // ─────────────────────────────────────────────────────────────────
    // Dual-quorum: entries before VotersRecord are NOT blocked by the
    // new quorum.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_hw_entries_before_voters_record_use_committed_quorum() {
        let (mut state, log, mgr) = setup_3_node_leader();

        populate_log(&mut state, &log, 10);
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 10;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 10;
        mgr.try_advance_high_watermark(&mut state);
        assert_eq!(state.high_watermark, 10);

        mgr.register_observer_internal(node(4), endpoint(4), &mut state);
        mgr.update_fetch_offset(node(4), 10, &mut state);

        let request = AddVoterRequest {
            node_id: node(4),
            endpoint: endpoint(4),
        };
        mgr.handle_add_voter(&request, &mut state, &log).unwrap();

        // Add more command entries after the VotersRecord
        for i in 11..15 {
            log.append_entry(LogEntry::command(i, Term(1), vec![]))
                .unwrap();
            state.log_end_offset = i + 1;
        }

        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 13;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 13;
        mgr.update_fetch_offset(node(4), 10, &mut state);

        mgr.try_advance_high_watermark(&mut state);
        assert_eq!(state.high_watermark, 13);

        // VotersRecord at offset 10 is committed (HW=13 > 10)
        assert!(mgr.try_commit_membership_change(&mut state));
    }

    // ─────────────────────────────────────────────────────────────────
    // Dual-quorum: when committed quorum hasn't reached VotersRecord,
    // HW only advances using the committed voter set.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_hw_committed_quorum_hasnt_reached_voters_record() {
        let (mut state, log, mgr) = setup_3_node_leader();

        populate_log(&mut state, &log, 10);
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 7;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 6;
        mgr.try_advance_high_watermark(&mut state);
        assert_eq!(state.high_watermark, 7);

        mgr.register_observer_internal(node(4), endpoint(4), &mut state);
        mgr.update_fetch_offset(node(4), 7, &mut state);

        let request = AddVoterRequest {
            node_id: node(4),
            endpoint: endpoint(4),
        };
        mgr.handle_add_voter(&request, &mut state, &log).unwrap();

        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 8;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 8;

        mgr.try_advance_high_watermark(&mut state);
        assert_eq!(state.high_watermark, 8);

        // VotersRecord NOT committed (8 < 10)
        assert!(!mgr.try_commit_membership_change(&mut state));
    }

    // ─────────────────────────────────────────────────────────────────
    // Log truncation clears pending membership change AND restores
    // the promoted node as an observer.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_log_truncation_restores_observer() {
        let (mut state, log, mgr) = setup_3_node_leader();

        populate_log(&mut state, &log, 10);
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 10;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 10;
        mgr.try_advance_high_watermark(&mut state);

        mgr.register_observer_internal(node(4), endpoint(4), &mut state);
        mgr.update_fetch_offset(node(4), 10, &mut state);

        let request = AddVoterRequest {
            node_id: node(4),
            endpoint: endpoint(4),
        };
        mgr.handle_add_voter(&request, &mut state, &log).unwrap();
        assert!(state.pending_membership_change.is_some());
        assert!(!state.is_observer(node(4)));

        // Truncate from offset 10 (discards the VotersRecord)
        log.truncate_suffix_sync(10);
        state.log_end_offset = 10;
        mgr.handle_log_truncation(&mut state, 10);
        assert!(state.pending_membership_change.is_none());

        // N4 should be restored as an observer
        assert!(state.is_observer(node(4)));
        assert_eq!(state.observer_endpoints.get(&node(4)), Some(&endpoint(4)));
        assert!(!state.follower_state.get(&node(4)).unwrap().is_voter);

        assert_eq!(state.voter_set.len(), 3);

        // Retry should succeed without re-registration
        let retry_request = AddVoterRequest {
            node_id: node(4),
            endpoint: endpoint(4),
        };
        let retry_resp = mgr.handle_add_voter(&retry_request, &mut state, &log).unwrap();
        assert!(retry_resp.success);
    }

    // ─────────────────────────────────────────────────────────────────
    // HW advancement with 4-voter quorum (majority = 3 of 4).
    // Verifies N4's fetch_offset counts toward committing the
    // VotersRecord itself.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_hw_advancement_with_new_voter_set() {
        let (mut state, log, mgr) = setup_3_node_leader();

        populate_log(&mut state, &log, 10);
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 10;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 10;
        mgr.try_advance_high_watermark(&mut state);
        assert_eq!(state.high_watermark, 10);

        mgr.register_observer_internal(node(4), endpoint(4), &mut state);
        mgr.update_fetch_offset(node(4), 10, &mut state);

        let request = AddVoterRequest {
            node_id: node(4),
            endpoint: endpoint(4),
        };
        mgr.handle_add_voter(&request, &mut state, &log).unwrap();

        // With 4 voters, majority = 3. Leader (N1) is at 11.
        mgr.try_advance_high_watermark(&mut state);
        assert_eq!(state.high_watermark, 10);

        // VotersRecord not yet committed (HW=10, need HW > 10)
        assert!(!mgr.try_commit_membership_change(&mut state));

        // N2 and N4 fetch past offset 10
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 11;
        mgr.update_fetch_offset(node(4), 11, &mut state);

        mgr.try_advance_high_watermark(&mut state);
        assert_eq!(state.high_watermark, 11);

        // VotersRecord is now committed
        assert!(mgr.try_commit_membership_change(&mut state));
        assert!(state.pending_membership_change.is_none());
        assert_eq!(state.voter_set.len(), 4);
        assert!(state.is_voter(node(4)));
    }

    // ─────────────────────────────────────────────────────────────────
    // Fetch-path integration: handle_fetch_request updates progress,
    // advances HW, returns entries with max_bytes enforcement,
    // and uses log_start_offset from the log store.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_fetch_request_integration() {
        let (mut state, log, mgr) = setup_3_node_leader();

        populate_log(&mut state, &log, 10);
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 5;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 5;

        // Register observer N4
        mgr.register_observer_internal(node(4), endpoint(4), &mut state);

        // Observer N4 sends Fetch with offset 0
        let fetch_req = FetchRequest {
            replica_id: node(4),
            fetch_offset: 0,
            last_fetched_epoch: Term(0),
            max_bytes: 65536,
        };
        let fetch_resp = mgr.handle_fetch_request(&fetch_req, &mut state, &log);

        assert_eq!(fetch_resp.leader_id, node(1));
        assert_eq!(fetch_resp.leader_epoch, Term(1));
        assert_eq!(fetch_resp.entries.len(), 10);
        assert_eq!(fetch_resp.high_watermark, 5);
        assert_eq!(fetch_resp.log_start_offset, 0);
        assert!(fetch_resp.diverging_epoch.is_none());
        assert!(fetch_resp.snapshot_id.is_none());

        // Voter N2 sends Fetch with offset 8 → updates voter progress
        let fetch_req2 = FetchRequest {
            replica_id: node(2),
            fetch_offset: 8,
            last_fetched_epoch: Term(1),
            max_bytes: 65536,
        };
        let fetch_resp2 = mgr.handle_fetch_request(&fetch_req2, &mut state, &log);

        // HW should have advanced (N2=8, N3=5 → majority = 8)
        assert_eq!(fetch_resp2.high_watermark, 8);
        assert_eq!(fetch_resp2.entries.len(), 2); // entries at offset 8, 9
    }

    // ─────────────────────────────────────────────────────────────────
    // Fetch from unknown replica returns empty response.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_fetch_unknown_replica() {
        let (mut state, log, mgr) = setup_3_node_leader();
        populate_log(&mut state, &log, 5);

        let fetch_req = FetchRequest {
            replica_id: node(99),
            fetch_offset: 0,
            last_fetched_epoch: Term(1),
            max_bytes: 65536,
        };
        let fetch_resp = mgr.handle_fetch_request(&fetch_req, &mut state, &log);
        assert!(fetch_resp.entries.is_empty());
    }

    // ─────────────────────────────────────────────────────────────────
    // Fetch with max_bytes enforcement limits response size.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_fetch_max_bytes_enforcement() {
        let (mut state, log, mgr) = setup_3_node_leader();
        populate_log(&mut state, &log, 100);
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 100;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 100;

        // Use a very small max_bytes to limit response
        let fetch_req = FetchRequest {
            replica_id: node(2),
            fetch_offset: 0,
            last_fetched_epoch: Term(1),
            max_bytes: 50, // very small — should return only a few entries
        };
        let fetch_resp = mgr.handle_fetch_request(&fetch_req, &mut state, &log);
        // With ~25 bytes per entry (1 byte payload + 24 overhead), 50 bytes fits ~2
        assert!(fetch_resp.entries.len() < 100);
        assert!(!fetch_resp.entries.is_empty());
    }

    // ─────────────────────────────────────────────────────────────────
    // Fetch with snapshot indication when fetch_offset < log_start_offset.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_fetch_snapshot_needed() {
        let (mut state, log, mgr) = setup_3_node_leader();
        populate_log(&mut state, &log, 100);
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 100;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 100;
        mgr.try_advance_high_watermark(&mut state);

        // Simulate log compaction — truncate prefix up to offset 50
        log.truncate_suffix_sync(0); // clear all
        // Re-populate only from 50+
        for i in 50..100 {
            log.append_entry(LogEntry::command(i, Term(1), vec![i as u8]))
                .unwrap();
        }
        // Manually set start offset to simulate compaction
        log.set_start_offset_for_test(50);

        // Register observer that is far behind
        mgr.register_observer_internal(node(4), endpoint(4), &mut state);

        let fetch_req = FetchRequest {
            replica_id: node(4),
            fetch_offset: 10, // behind log_start_offset of 50
            last_fetched_epoch: Term(1),
            max_bytes: 65536,
        };
        let fetch_resp = mgr.handle_fetch_request(&fetch_req, &mut state, &log);
        assert!(fetch_resp.snapshot_id.is_some());
        assert!(fetch_resp.entries.is_empty());
        assert_eq!(fetch_resp.log_start_offset, 50);
    }

    // ─────────────────────────────────────────────────────────────────
    // Observer Fetch-based catch-up and promotion via handle_fetch_request.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_observer_fetch_catch_up_and_promote() {
        let (mut state, log, mgr) = setup_3_node_leader();

        populate_log(&mut state, &log, 20);
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 20;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 20;
        mgr.try_advance_high_watermark(&mut state);
        assert_eq!(state.high_watermark, 20);

        // Register N4 observer
        mgr.register_observer_internal(node(4), endpoint(4), &mut state);

        // Simulate N4 sending Fetch requests and catching up
        for batch_start in (0..20).step_by(5) {
            let req = FetchRequest {
                replica_id: node(4),
                fetch_offset: batch_start,
                last_fetched_epoch: Term(1),
                max_bytes: 65536,
            };
            let _resp = mgr.handle_fetch_request(&req, &mut state, &log);
        }

        // Final Fetch at offset 20 → fully caught up
        let final_req = FetchRequest {
            replica_id: node(4),
            fetch_offset: 20,
            last_fetched_epoch: Term(1),
            max_bytes: 65536,
        };
        mgr.handle_fetch_request(&final_req, &mut state, &log);
        assert!(mgr.is_observer_caught_up(node(4), &state));

        // Now promote
        let add_req = AddVoterRequest {
            node_id: node(4),
            endpoint: endpoint(4),
        };
        let resp = mgr.handle_add_voter(&add_req, &mut state, &log).unwrap();
        assert!(resp.success);
    }

    // ─────────────────────────────────────────────────────────────────
    // LogStore trait: InMemoryLog used via &dyn SyncLogOps.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_log_store_trait_usage() {
        let (mut state, _log, mgr) = setup_3_node_leader();

        let log: Box<dyn SyncLogOps> = Box::new(InMemoryLog::new());

        for i in 0..5 {
            log.append_entry(LogEntry::command(i, Term(1), vec![]))
                .unwrap();
            state.log_end_offset = i + 1;
        }
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 5;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 5;
        mgr.try_advance_high_watermark(&mut state);

        mgr.register_observer_internal(node(4), endpoint(4), &mut state);
        mgr.update_fetch_offset(node(4), 5, &mut state);

        let request = AddVoterRequest {
            node_id: node(4),
            endpoint: endpoint(4),
        };
        let response = mgr.handle_add_voter(&request, &mut state, log.as_ref()).unwrap();
        assert!(response.success);
        assert_eq!(log.end_offset(), 6);
        assert!(log.has_uncommitted_voters_record_sync(5));
    }

    // ─────────────────────────────────────────────────────────────────
    // Register observer via RPC: leader validation.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_register_observer_not_leader() {
        let voters = vec![voter(1), voter(2), voter(3)];
        let mut state = NodeState {
            node_id: node(2),
            current_term: Term(1),
            role: Role::Follower,
            leader_id: Some(node(1)),
            log_end_offset: 0,
            high_watermark: 0,
            voter_set: voters,
            observers: std::collections::HashSet::new(),
            observer_endpoints: std::collections::HashMap::new(),
            pending_membership_change: None,
            follower_state: std::collections::HashMap::new(),
        };
        let mgr = MembershipManager::new(MembershipConfig::default());

        let req = RegisterObserverRequest {
            node_id: node(4),
            endpoint: endpoint(4),
        };
        let resp = mgr.handle_register_observer(&req, &mut state);
        assert!(!resp.success);
        assert_eq!(
            resp.error,
            Some(MembershipError::NotLeader {
                leader_id: Some(node(1))
            })
        );
    }

    // ─────────────────────────────────────────────────────────────────
    // Register observer: leader accepts, existing voter rejected.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_register_observer_leader_validates() {
        let (mut state, _log, mgr) = setup_3_node_leader();

        // Register new observer → success
        let req = RegisterObserverRequest {
            node_id: node(4),
            endpoint: endpoint(4),
        };
        let resp = mgr.handle_register_observer(&req, &mut state);
        assert!(resp.success);
        assert!(state.is_observer(node(4)));

        // Try to register existing voter → NodeAlreadyVoter
        let req2 = RegisterObserverRequest {
            node_id: node(2),
            endpoint: endpoint(2),
        };
        let resp2 = mgr.handle_register_observer(&req2, &mut state);
        assert!(!resp2.success);
        assert_eq!(resp2.error, Some(MembershipError::NodeAlreadyVoter));
    }

    // ─────────────────────────────────────────────────────────────────
    // Deregister observer: explicit removal via RPC.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_deregister_observer() {
        let (mut state, _log, mgr) = setup_3_node_leader();

        // Register N4
        mgr.register_observer_internal(node(4), endpoint(4), &mut state);
        assert!(state.is_observer(node(4)));

        // Deregister N4
        let req = DeregisterObserverRequest { node_id: node(4) };
        let resp = mgr.handle_deregister_observer(&req, &mut state);
        assert!(resp.success);
        assert!(!state.is_observer(node(4)));
        assert!(!state.observer_endpoints.contains_key(&node(4)));
        assert!(!state.follower_state.contains_key(&node(4)));
    }

    // ─────────────────────────────────────────────────────────────────
    // Deregister observer: reject if voter or pending promotion.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_deregister_observer_rejects_voter() {
        let (mut state, _log, mgr) = setup_3_node_leader();

        let req = DeregisterObserverRequest { node_id: node(2) };
        let resp = mgr.handle_deregister_observer(&req, &mut state);
        assert!(!resp.success);
        assert_eq!(resp.error, Some(MembershipError::NodeAlreadyVoter));
    }

    #[test]
    fn test_deregister_observer_rejects_pending_promotion() {
        let (mut state, log, mgr) = setup_3_node_leader();

        populate_log(&mut state, &log, 5);
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 5;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 5;
        mgr.try_advance_high_watermark(&mut state);

        mgr.register_observer_internal(node(4), endpoint(4), &mut state);
        mgr.update_fetch_offset(node(4), 5, &mut state);

        let add_req = AddVoterRequest {
            node_id: node(4),
            endpoint: endpoint(4),
        };
        mgr.handle_add_voter(&add_req, &mut state, &log).unwrap();

        // N4 is now being promoted — deregister should fail
        let dereg_req = DeregisterObserverRequest { node_id: node(4) };
        let resp = mgr.handle_deregister_observer(&dereg_req, &mut state);
        assert!(!resp.success);
        assert_eq!(resp.error, Some(MembershipError::ChangeInProgress));
    }

    // ─────────────────────────────────────────────────────────────────
    // Deregister observer: not leader → NotLeader.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_deregister_observer_not_leader() {
        let voters = vec![voter(1), voter(2), voter(3)];
        let mut state = NodeState {
            node_id: node(2),
            current_term: Term(1),
            role: Role::Follower,
            leader_id: Some(node(1)),
            log_end_offset: 0,
            high_watermark: 0,
            voter_set: voters,
            observers: std::collections::HashSet::new(),
            observer_endpoints: std::collections::HashMap::new(),
            pending_membership_change: None,
            follower_state: std::collections::HashMap::new(),
        };
        let mgr = MembershipManager::new(MembershipConfig::default());

        let req = DeregisterObserverRequest { node_id: node(4) };
        let resp = mgr.handle_deregister_observer(&req, &mut state);
        assert!(!resp.success);
        assert_eq!(
            resp.error,
            Some(MembershipError::NotLeader {
                leader_id: Some(node(1))
            })
        );
    }

    // ─────────────────────────────────────────────────────────────────
    // Observer catch-up threshold edge cases.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_catch_up_threshold_boundary() {
        let config = MembershipConfig {
            catch_up_threshold: 10,
            max_fetch_entries: 100,
        };
        let mgr = MembershipManager::new(config);
        let voters = vec![voter(1), voter(2), voter(3)];
        let mut state = NodeState::new_leader(node(1), Term(1), voters);
        state.log_end_offset = 1000;
        state.high_watermark = 995; // HW < log_end to test threshold gate

        mgr.register_observer_internal(node(4), endpoint(4), &mut state);

        // gap = 11 → not caught up (threshold gate fails)
        mgr.update_fetch_offset(node(4), 989, &mut state);
        assert!(!mgr.is_observer_caught_up(node(4), &state));

        // fetch_offset = 990 → gap = 10 (within threshold), but 990 < HW=995 → not caught up
        mgr.update_fetch_offset(node(4), 990, &mut state);
        assert!(!mgr.is_observer_caught_up(node(4), &state));

        // fetch_offset = 995 → meets HW gate, gap = 5 (within threshold) → caught up
        mgr.update_fetch_offset(node(4), 995, &mut state);
        assert!(mgr.is_observer_caught_up(node(4), &state));

        // fetch_offset = 1000 → meets HW gate, gap = 0 → caught up
        mgr.update_fetch_offset(node(4), 1000, &mut state);
        assert!(mgr.is_observer_caught_up(node(4), &state));
    }

    // ─────────────────────────────────────────────────────────────────
    // Fallible append: log store error is propagated.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_add_voter_log_append_failure() {
        use crate::node_state::LogStoreError;

        /// A log store that always fails on append.
        struct FailingLog;

        impl SyncLogOps for FailingLog {
            fn append_entry(&self, _entry: LogEntry) -> Result<(), LogStoreError> {
                Err(LogStoreError::new("disk full"))
            }
            fn has_uncommitted_voters_record_sync(&self, _hw: u64) -> bool {
                false
            }
            fn read_entries(&self, _from: u64, _max: usize) -> Vec<LogEntry> {
                vec![]
            }
            fn read_entries_bounded(
                &self,
                _from: u64,
                _max_entries: usize,
                _max_bytes: u32,
            ) -> Vec<LogEntry> {
                vec![]
            }
            fn end_offset(&self) -> u64 {
                0
            }
            fn start_offset(&self) -> u64 {
                0
            }
            fn entry_term_at(&self, _offset: u64) -> Option<Term> {
                None
            }
            fn epoch_end_offset(&self, _epoch: Term) -> u64 {
                0
            }
            fn truncate_suffix_sync(&self, _from: u64) {}
        }

        let (mut state, _real_log, mgr) = setup_3_node_leader();
        state.log_end_offset = 5;
        state.high_watermark = 5;

        mgr.register_observer_internal(node(4), endpoint(4), &mut state);
        mgr.update_fetch_offset(node(4), 5, &mut state);

        let failing_log = FailingLog;
        let request = AddVoterRequest {
            node_id: node(4),
            endpoint: endpoint(4),
        };
        let response = mgr.handle_add_voter(&request, &mut state, &failing_log);
        assert!(response.is_err());
        let err = response.unwrap_err();
        assert!(err.message.contains("disk full"));
        // State should not have been mutated on failure
        assert!(state.pending_membership_change.is_none());
        assert!(state.is_observer(node(4)));
        assert_eq!(state.log_end_offset, 5);
    }

    // ─────────────────────────────────────────────────────────────────
    // Observer promotion safety: fetch_offset >= HW gate.
    // An observer within threshold of log_end but below HW is rejected.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_observer_promotion_requires_hw_gate() {
        let (mut state, log, mgr) = setup_3_node_leader();

        populate_log(&mut state, &log, 100);
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 100;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 100;
        mgr.try_advance_high_watermark(&mut state);
        assert_eq!(state.high_watermark, 100);

        mgr.register_observer_internal(node(4), endpoint(4), &mut state);

        // Observer at 95 → within threshold (gap=5), but 95 < HW=100
        mgr.update_fetch_offset(node(4), 95, &mut state);
        assert!(!mgr.is_observer_caught_up(node(4), &state));

        // Attempt AddVoter → NodeNotCaughtUp
        let request = AddVoterRequest {
            node_id: node(4),
            endpoint: endpoint(4),
        };
        let response = mgr.handle_add_voter(&request, &mut state, &log).unwrap();
        assert!(!response.success);
        assert_eq!(response.error, Some(MembershipError::NodeNotCaughtUp));

        // Observer catches up to HW → now eligible
        mgr.update_fetch_offset(node(4), 100, &mut state);
        assert!(mgr.is_observer_caught_up(node(4), &state));

        let response2 = mgr.handle_add_voter(&request, &mut state, &log).unwrap();
        assert!(response2.success);
    }

    // ─────────────────────────────────────────────────────────────────
    // Election and Check Quorum integration: after VotersRecord commit,
    // the new voter participates in elections and Check Quorum.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_election_voter_set_after_commit() {
        let (mut state, log, mgr) = setup_3_node_leader();

        populate_log(&mut state, &log, 10);
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 10;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 10;
        mgr.try_advance_high_watermark(&mut state);

        mgr.register_observer_internal(node(4), endpoint(4), &mut state);
        mgr.update_fetch_offset(node(4), 10, &mut state);

        let request = AddVoterRequest {
            node_id: node(4),
            endpoint: endpoint(4),
        };
        mgr.handle_add_voter(&request, &mut state, &log).unwrap();

        // Before commit: election voter set is still 3 nodes
        assert_eq!(state.election_voter_set().len(), 3);
        assert!(!state.can_vote_for(node(4)));

        // Simulate followers catching up past VotersRecord
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 11;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 11;
        mgr.update_fetch_offset(node(4), 11, &mut state);
        mgr.try_advance_high_watermark(&mut state);
        mgr.try_commit_membership_change(&mut state);

        // After commit: election voter set includes N4
        assert_eq!(state.election_voter_set().len(), 4);
        assert!(state.can_vote_for(node(4)));
    }

    #[test]
    fn test_check_quorum_after_commit() {
        let (mut state, log, mgr) = setup_3_node_leader();

        populate_log(&mut state, &log, 10);
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 10;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 10;
        mgr.try_advance_high_watermark(&mut state);

        mgr.register_observer_internal(node(4), endpoint(4), &mut state);
        mgr.update_fetch_offset(node(4), 10, &mut state);

        let request = AddVoterRequest {
            node_id: node(4),
            endpoint: endpoint(4),
        };
        mgr.handle_add_voter(&request, &mut state, &log).unwrap();

        // Before commit: Check Quorum uses 3-node set
        // Leader (N1) + N2 = 2 of 3 → quorum met
        let mut fetchers = std::collections::HashSet::new();
        fetchers.insert(node(2));
        assert!(state.check_quorum_met(&fetchers));

        // Commit the membership change
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 11;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 11;
        mgr.update_fetch_offset(node(4), 11, &mut state);
        mgr.try_advance_high_watermark(&mut state);
        mgr.try_commit_membership_change(&mut state);

        // After commit: Check Quorum uses 4-node set
        // Leader (N1) + N2 = 2 of 4 → NOT quorum (need 3 of 4)
        assert!(!state.check_quorum_met(&fetchers));

        // Leader (N1) + N2 + N3 = 3 of 4 → quorum met
        fetchers.insert(node(3));
        assert!(state.check_quorum_met(&fetchers));

        // Leader (N1) + N2 + N4 = 3 of 4 → quorum met (N4 participates!)
        let mut fetchers2 = std::collections::HashSet::new();
        fetchers2.insert(node(2));
        fetchers2.insert(node(4));
        assert!(state.check_quorum_met(&fetchers2));
    }

    // ─────────────────────────────────────────────────────────────────
    // ConsensusState projection: voter_set only updates on commit.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_consensus_state_reflects_committed_voters() {
        let (mut state, log, mgr) = setup_3_node_leader();

        populate_log(&mut state, &log, 10);
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 10;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 10;
        mgr.try_advance_high_watermark(&mut state);

        mgr.register_observer_internal(node(4), endpoint(4), &mut state);
        mgr.update_fetch_offset(node(4), 10, &mut state);

        let request = AddVoterRequest {
            node_id: node(4),
            endpoint: endpoint(4),
        };
        mgr.handle_add_voter(&request, &mut state, &log).unwrap();

        // Before commit: consensus state shows 3 voters
        let cs = state.to_consensus_state();
        assert_eq!(cs.voter_set.len(), 3);

        // Commit
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 11;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 11;
        mgr.update_fetch_offset(node(4), 11, &mut state);
        mgr.try_advance_high_watermark(&mut state);
        mgr.try_commit_membership_change(&mut state);

        // After commit: consensus state shows 4 voters
        let cs2 = state.to_consensus_state();
        assert_eq!(cs2.voter_set.len(), 4);
    }

    // ─────────────────────────────────────────────────────────────────
    // apply_voters_record integration: event loop calls this when
    // HW advances past a VotersRecord entry.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_apply_voters_record_integration() {
        let (mut state, log, mgr) = setup_3_node_leader();

        populate_log(&mut state, &log, 10);
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 10;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 10;
        mgr.try_advance_high_watermark(&mut state);

        mgr.register_observer_internal(node(4), endpoint(4), &mut state);
        mgr.update_fetch_offset(node(4), 10, &mut state);

        let request = AddVoterRequest {
            node_id: node(4),
            endpoint: endpoint(4),
        };
        mgr.handle_add_voter(&request, &mut state, &log).unwrap();

        // Commit via apply_voters_record (the event loop's integration path)
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 11;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 11;
        mgr.update_fetch_offset(node(4), 11, &mut state);
        mgr.try_advance_high_watermark(&mut state);

        // Event loop detects HW > VotersRecord offset and calls apply_voters_record
        assert!(state.high_watermark > state.pending_membership_change.as_ref().unwrap().offset);
        state.apply_voters_record();

        assert!(state.pending_membership_change.is_none());
        assert_eq!(state.voter_set.len(), 4);
        assert!(state.is_voter(node(4)));
        assert_eq!(state.election_voter_set().len(), 4);
        assert_eq!(state.check_quorum_voter_set().len(), 4);
    }

    // ─────────────────────────────────────────────────────────────────
    // Fetch-driven membership commit: handle_fetch_request commits
    // VotersRecord when HW advances past it.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_fetch_driven_membership_commit() {
        let (mut state, log, mgr) = setup_3_node_leader();

        populate_log(&mut state, &log, 10);
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 10;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 10;
        mgr.try_advance_high_watermark(&mut state);

        mgr.register_observer_internal(node(4), endpoint(4), &mut state);
        mgr.update_fetch_offset(node(4), 10, &mut state);

        let request = AddVoterRequest {
            node_id: node(4),
            endpoint: endpoint(4),
        };
        mgr.handle_add_voter(&request, &mut state, &log).unwrap();
        assert!(state.pending_membership_change.is_some());

        // N2 fetches at offset 11 → HW advances, VotersRecord committed
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 11;
        let fetch_req = FetchRequest {
            replica_id: node(2),
            fetch_offset: 11,
            last_fetched_epoch: Term(1),
            max_bytes: 65536,
        };
        // This Fetch updates N2's offset, recalculates HW, and commits
        let fetch_resp = mgr.handle_fetch_request(&fetch_req, &mut state, &log);

        // VotersRecord should be committed via the Fetch path
        assert!(state.pending_membership_change.is_none());
        assert_eq!(state.voter_set.len(), 4);
        assert!(state.is_voter(node(4)));
        assert!(fetch_resp.high_watermark >= 11);
    }

    // ─────────────────────────────────────────────────────────────────
    // Follower/observer applies committed VotersRecord from the log.
    // Unlike the leader, followers have no pending_membership_change;
    // they must deserialize the VotersRecord from the log entry and
    // apply it directly via apply_voters_record_from_log.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_follower_apply_voters_record_from_log() {
        // Simulate a follower with 3-voter set
        let voters = vec![voter(1), voter(2), voter(3)];
        let mut state = NodeState {
            node_id: node(2),
            current_term: Term(1),
            role: Role::Follower,
            leader_id: Some(node(1)),
            log_end_offset: 11,
            high_watermark: 11,
            voter_set: voters,
            observers: {
                let mut s = std::collections::HashSet::new();
                s.insert(node(4));
                s
            },
            observer_endpoints: {
                let mut m = std::collections::HashMap::new();
                m.insert(node(4), endpoint(4));
                m
            },
            pending_membership_change: None,
            follower_state: std::collections::HashMap::new(),
        };

        // Add follower progress for N4 (observer)
        state.follower_state.insert(
            node(4),
            FollowerProgress {
                node_id: node(4),
                fetch_offset: 10,
                is_voter: false,
            },
        );

        assert!(!state.is_voter(node(4)));
        assert!(state.is_observer(node(4)));
        assert_eq!(state.voter_set.len(), 3);

        // Simulate: the committed VotersRecord entry is deserialized from the log
        let new_record = VotersRecord {
            version: 1,
            voters: vec![voter(1), voter(2), voter(3), voter(4)],
        };

        // Follower applies the VotersRecord from the log
        state.apply_voters_record_from_log(&new_record);

        // Voter set should now include N4
        assert_eq!(state.voter_set.len(), 4);
        assert!(state.is_voter(node(4)));
        // N4 should no longer be an observer
        assert!(!state.is_observer(node(4)));
        assert!(!state.observer_endpoints.contains_key(&node(4)));
        // N4's follower progress should be marked as voter
        assert!(state.follower_state.get(&node(4)).unwrap().is_voter);
    }

    // ─────────────────────────────────────────────────────────────────
    // Leader applies VotersRecord from log (has pending change).
    // Verifies apply_voters_record_from_log works on leader path too.
    // ─────────────────────────────────────────────────────────────────
    #[test]
    fn test_leader_apply_voters_record_from_log() {
        let (mut state, log, mgr) = setup_3_node_leader();

        populate_log(&mut state, &log, 10);
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 10;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 10;
        mgr.try_advance_high_watermark(&mut state);

        mgr.register_observer_internal(node(4), endpoint(4), &mut state);
        mgr.update_fetch_offset(node(4), 10, &mut state);

        let request = AddVoterRequest {
            node_id: node(4),
            endpoint: endpoint(4),
        };
        mgr.handle_add_voter(&request, &mut state, &log).unwrap();
        assert!(state.pending_membership_change.is_some());

        // Advance HW past VotersRecord
        state.follower_state.get_mut(&node(2)).unwrap().fetch_offset = 11;
        state.follower_state.get_mut(&node(3)).unwrap().fetch_offset = 11;
        mgr.update_fetch_offset(node(4), 11, &mut state);
        mgr.try_advance_high_watermark(&mut state);
        assert!(state.high_watermark > 10);

        // Deserialize the VotersRecord from the log entry
        let record = VotersRecord {
            version: 1,
            voters: vec![voter(1), voter(2), voter(3), voter(4)],
        };

        // Leader applies via the same apply_voters_record_from_log path
        state.apply_voters_record_from_log(&record);

        assert!(state.pending_membership_change.is_none());
        assert_eq!(state.voter_set.len(), 4);
        assert!(state.is_voter(node(4)));
    }
}
