// -----------------------------------------------------------------------
// Copyright (c) Microsoft Corp. All rights reserved.
// -----------------------------------------------------------------------

use std::collections::HashSet;
use std::fmt;

/// Unique identifier for a node in the Raft cluster.
pub type NodeId = u64;

/// A snapshot of the current voter set at a particular log offset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VotersRecord {
    /// The set of node IDs that are voting members.
    pub voters: HashSet<NodeId>,
    /// The log offset at which this configuration was written.
    pub offset: u64,
}

impl VotersRecord {
    pub fn new(voters: HashSet<NodeId>, offset: u64) -> Self {
        Self { voters, offset }
    }

    /// Returns true if the given node is a voting member.
    pub fn contains(&self, node_id: NodeId) -> bool {
        self.voters.contains(&node_id)
    }

    /// Returns the number of voters required for a majority quorum.
    pub fn quorum_size(&self) -> usize {
        self.voters.len() / 2 + 1
    }
}

/// Tracks an in-flight (uncommitted) membership change.
#[derive(Clone, Debug)]
struct PendingMembershipChange {
    /// The proposed new voter configuration.
    record: VotersRecord,
    /// The log offset where the change entry was appended.
    offset: u64,
}

/// Manages cluster membership, enforcing the single pending-change invariant
/// required by the Raft joint-consensus protocol.
pub struct MembershipState {
    /// The last committed (durable) voter configuration.
    committed: VotersRecord,
    /// At most one uncommitted membership change may be in flight at a time.
    /// Guarded by the single-change invariant: a new change cannot be proposed
    /// while this is `Some`.
    pending_membership_change: Option<PendingMembershipChange>,
}

impl MembershipState {
    /// Creates a new `MembershipState` from a committed voter record.
    pub fn new(committed: VotersRecord) -> Self {
        Self {
            committed,
            pending_membership_change: None,
        }
    }

    /// Restores membership state from a snapshot.
    pub fn restore_from_snapshot(snapshot_voters: VotersRecord) -> Self {
        Self {
            committed: snapshot_voters,
            pending_membership_change: None,
        }
    }

    /// Returns the currently committed voter configuration.
    pub fn committed_voters(&self) -> &VotersRecord {
        &self.committed
    }

    /// Returns the effective (possibly uncommitted) voter configuration.
    /// If a change is pending, the pending config is authoritative for
    /// quorum calculations during replication.
    pub fn effective_voters(&self) -> &VotersRecord {
        match &self.pending_membership_change {
            Some(pending) => &pending.record,
            None => &self.committed,
        }
    }

    /// Returns `true` when a membership change is already in flight.
    pub fn has_pending_change(&self) -> bool {
        self.pending_membership_change.is_some()
    }

    /// Proposes a new membership change.
    ///
    /// # Errors
    ///
    /// Returns `MembershipError::ChangeAlreadyPending` if there is already an
    /// uncommitted membership change — the Raft protocol allows at most one
    /// pending configuration change at a time.
    pub fn propose_change(
        &mut self,
        new_voters: HashSet<NodeId>,
        log_offset: u64,
    ) -> Result<&VotersRecord, MembershipError> {
        // --- single-change invariant ---
        if let Some(ref pending) = self.pending_membership_change {
            return Err(MembershipError::ChangeAlreadyPending {
                pending_offset: pending.offset,
            });
        }

        let record = VotersRecord::new(new_voters, log_offset);
        self.pending_membership_change = Some(PendingMembershipChange {
            record,
            offset: log_offset,
        });
        Ok(&self.pending_membership_change.as_ref().unwrap().record)
    }

    /// Commits the pending membership change once its log entry is committed.
    ///
    /// # Errors
    ///
    /// Returns `MembershipError::NoPendingChange` if there is nothing to
    /// commit, or `MembershipError::OffsetMismatch` if the committed offset
    /// does not match the pending entry.
    pub fn commit_change(&mut self, committed_offset: u64) -> Result<&VotersRecord, MembershipError> {
        let pending = match self.pending_membership_change.take() {
            Some(p) => p,
            None => return Err(MembershipError::NoPendingChange),
        };

        if pending.offset != committed_offset {
            // Put it back — the caller got confused.
            self.pending_membership_change = Some(pending);
            return Err(MembershipError::OffsetMismatch {
                expected: self.pending_membership_change.as_ref().unwrap().offset,
                actual: committed_offset,
            });
        }

        self.committed = pending.record;
        Ok(&self.committed)
    }

    /// Clears the pending membership change if its log offset falls at or
    /// beyond `truncate_from`.
    ///
    /// This **must** be called whenever `truncate_suffix` is invoked on the
    /// log store — e.g. when a new leader truncates a follower's uncommitted
    /// suffix. Without this, the in-memory `pending_membership_change` flag
    /// would point at a log entry that no longer exists, permanently blocking
    /// new membership changes.
    pub fn clear_pending_if_truncated(&mut self, truncate_from: u64) {
        if let Some(ref pending) = self.pending_membership_change {
            if pending.offset >= truncate_from {
                self.pending_membership_change = None;
            }
        }
    }

    /// Unconditionally clears any pending membership change.
    /// Useful when reverting to a snapshot that predates the pending entry.
    pub fn clear_pending(&mut self) {
        self.pending_membership_change = None;
    }

    /// Applies a snapshot, replacing both committed and pending state.
    pub fn apply_snapshot(&mut self, snapshot_voters: VotersRecord) {
        self.committed = snapshot_voters;
        self.pending_membership_change = None;
    }
}

impl fmt::Display for MembershipState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MembershipState {{ committed: {:?}, pending: {} }}",
            self.committed.voters,
            if self.has_pending_change() {
                format!("Some(offset={})", self.pending_membership_change.as_ref().unwrap().offset)
            } else {
                "None".to_string()
            }
        )
    }
}

/// Errors produced by membership operations.
#[derive(Debug, PartialEq, Eq)]
pub enum MembershipError {
    /// A membership change is already in flight at the given log offset.
    ChangeAlreadyPending { pending_offset: u64 },
    /// No pending change exists to commit.
    NoPendingChange,
    /// The offset supplied to `commit_change` does not match the pending entry.
    OffsetMismatch { expected: u64, actual: u64 },
}

impl fmt::Display for MembershipError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ChangeAlreadyPending { pending_offset } => {
                write!(f, "membership change already pending at offset {pending_offset}")
            }
            Self::NoPendingChange => write!(f, "no pending membership change to commit"),
            Self::OffsetMismatch { expected, actual } => {
                write!(f, "offset mismatch: expected {expected}, got {actual}")
            }
        }
    }
}

impl std::error::Error for MembershipError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn voters(ids: &[u64]) -> HashSet<NodeId> {
        ids.iter().copied().collect()
    }

    fn initial_state() -> MembershipState {
        MembershipState::new(VotersRecord::new(voters(&[1, 2, 3]), 0))
    }

    #[test]
    fn propose_and_commit_change() {
        let mut state = initial_state();
        state.propose_change(voters(&[1, 2, 3, 4]), 5).unwrap();
        assert!(state.has_pending_change());

        let committed = state.commit_change(5).unwrap();
        assert_eq!(committed.voters, voters(&[1, 2, 3, 4]));
        assert!(!state.has_pending_change());
    }

    #[test]
    fn single_change_invariant_rejects_second_proposal() {
        let mut state = initial_state();
        state.propose_change(voters(&[1, 2, 3, 4]), 5).unwrap();

        let err = state.propose_change(voters(&[1, 2]), 6).unwrap_err();
        assert_eq!(err, MembershipError::ChangeAlreadyPending { pending_offset: 5 });
    }

    #[test]
    fn clear_pending_if_truncated_clears_at_boundary() {
        let mut state = initial_state();
        state.propose_change(voters(&[1, 2, 3, 4]), 10).unwrap();
        assert!(state.has_pending_change());

        // Truncation starts exactly at the pending offset — must clear.
        state.clear_pending_if_truncated(10);
        assert!(!state.has_pending_change());
    }

    #[test]
    fn clear_pending_if_truncated_clears_beyond() {
        let mut state = initial_state();
        state.propose_change(voters(&[1, 2, 3, 4]), 10).unwrap();

        // Truncation starts before the pending offset — must clear.
        state.clear_pending_if_truncated(8);
        assert!(!state.has_pending_change());
    }

    #[test]
    fn clear_pending_if_truncated_preserves_when_before() {
        let mut state = initial_state();
        state.propose_change(voters(&[1, 2, 3, 4]), 10).unwrap();

        // Truncation starts after the pending offset — do not clear.
        state.clear_pending_if_truncated(11);
        assert!(state.has_pending_change());
    }

    #[test]
    fn clear_pending_if_truncated_noop_when_no_pending() {
        let mut state = initial_state();
        // Should not panic.
        state.clear_pending_if_truncated(5);
        assert!(!state.has_pending_change());
    }

    #[test]
    fn truncation_unblocks_new_proposal() {
        let mut state = initial_state();
        state.propose_change(voters(&[1, 2, 3, 4]), 10).unwrap();

        // Simulate leader truncation removing the uncommitted entry.
        state.clear_pending_if_truncated(10);

        // A new proposal should now succeed.
        state.propose_change(voters(&[1, 2, 5]), 12).unwrap();
        assert!(state.has_pending_change());
    }

    #[test]
    fn apply_snapshot_resets_pending() {
        let mut state = initial_state();
        state.propose_change(voters(&[1, 2, 3, 4]), 5).unwrap();

        state.apply_snapshot(VotersRecord::new(voters(&[1, 2, 3]), 20));
        assert!(!state.has_pending_change());
        assert_eq!(state.committed_voters().offset, 20);
    }

    #[test]
    fn effective_voters_reflects_pending() {
        let mut state = initial_state();
        assert_eq!(state.effective_voters().voters, voters(&[1, 2, 3]));

        state.propose_change(voters(&[1, 2, 3, 4]), 5).unwrap();
        assert_eq!(state.effective_voters().voters, voters(&[1, 2, 3, 4]));
    }

    #[test]
    fn quorum_size() {
        let record = VotersRecord::new(voters(&[1, 2, 3]), 0);
        assert_eq!(record.quorum_size(), 2);

        let record = VotersRecord::new(voters(&[1, 2, 3, 4, 5]), 0);
        assert_eq!(record.quorum_size(), 3);
    }
}
