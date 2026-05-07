// -----------------------------------------------------------------------
// Copyright (c) Microsoft Corp. All rights reserved.
// -----------------------------------------------------------------------

use std::collections::BTreeMap;

use tracing::{debug, info, warn};

use crate::error::{RaftError, RaftResult};
use crate::log_entry::{LogEntry, LogEntryPayload};
use crate::membership::{VoterSet, VotersRecord};
use crate::node::NodeId;

/// Unique term identifier for a leader epoch.
pub type Term = u64;

/// Tracks an in-flight membership change that has been appended to the log
/// but not yet committed.
#[derive(Debug, Clone)]
pub struct PendingMembershipChange {
    /// Log offset where the VotersRecord entry was appended.
    pub offset: u64,
    /// The proposed new voter set.
    pub proposed_voter_set: VoterSet,
}

/// Per-node mutable state that tracks the high watermark (commit index),
/// active voter set, and pending membership changes.
///
/// # Invariants
///
/// - `high_watermark` never decreases.
/// - `voter_set` reflects all committed `VotersRecord` entries up to
///   `high_watermark`.
/// - At most one membership change may be pending at a time.
#[derive(Debug)]
pub struct NodeState {
    /// This node's identity.
    node_id: NodeId,
    /// The highest committed log offset (exclusive upper bound).
    high_watermark: u64,
    /// Current leader epoch / term.
    leader_epoch: Term,
    /// Last included term from a snapshot, used for Fetch divergence
    /// detection at the log start offset.
    snapshot_last_term: Term,
    /// The currently committed voter set.
    voter_set: VoterSet,
    /// Full voters record tracking membership history.
    voters_record: VotersRecord,
    /// An uncommitted membership change, if any.
    pending_membership_change: Option<PendingMembershipChange>,
    /// The log-start offset (after compaction / snapshot).
    log_start_offset: u64,
}

impl NodeState {
    /// Creates a new `NodeState` with the given identity and initial voter set.
    pub fn new(node_id: NodeId, initial_voter_set: VoterSet, voters_record: VotersRecord) -> Self {
        Self {
            node_id,
            high_watermark: 0,
            leader_epoch: 0,
            snapshot_last_term: 0,
            voter_set: initial_voter_set,
            voters_record,
            pending_membership_change: None,
            log_start_offset: 0,
        }
    }

    /// Returns the current high watermark (exclusive upper bound of
    /// committed offsets).
    pub fn high_watermark(&self) -> u64 {
        self.high_watermark
    }

    /// Returns a reference to the current committed voter set.
    pub fn voter_set(&self) -> &VoterSet {
        &self.voter_set
    }

    /// Returns a reference to the voters record.
    pub fn voters_record(&self) -> &VotersRecord {
        &self.voters_record
    }

    /// Returns the pending membership change, if any.
    pub fn pending_membership_change(&self) -> Option<&PendingMembershipChange> {
        self.pending_membership_change.as_ref()
    }

    /// Returns the current leader epoch / term.
    pub fn leader_epoch(&self) -> Term {
        self.leader_epoch
    }

    /// Returns the snapshot last term.
    pub fn snapshot_last_term(&self) -> Term {
        self.snapshot_last_term
    }

    /// Returns the log start offset.
    pub fn log_start_offset(&self) -> u64 {
        self.log_start_offset
    }

    /// Sets the leader epoch.
    pub fn set_leader_epoch(&mut self, term: Term) {
        self.leader_epoch = term;
    }

    /// Records a pending membership change at the given log offset.
    ///
    /// # Errors
    ///
    /// Returns an error if a membership change is already pending.
    pub fn propose_membership_change(
        &mut self,
        offset: u64,
        proposed_voter_set: VoterSet,
    ) -> RaftResult<()> {
        if let Some(ref pending) = self.pending_membership_change {
            return Err(RaftError::MembershipChangeInProgress(format!(
                "cannot propose membership change at offset {}; \
                 change already pending at offset {}",
                offset, pending.offset
            )));
        }
        self.pending_membership_change = Some(PendingMembershipChange {
            offset,
            proposed_voter_set,
        });
        Ok(())
    }

    /// Advances the high watermark to `new_hw`, applying any `VotersRecord`
    /// entries found in `committed_entries` that fall within the newly
    /// committed range `[self.high_watermark, new_hw)`.
    ///
    /// # Contiguity requirement
    ///
    /// **Callers must supply a complete, contiguous slice** of log entries
    /// covering every offset in `[self.high_watermark, new_hw)`. This method
    /// validates that the supplied entries form a gap-free sequence; if any
    /// offset is missing, it returns an error rather than silently skipping
    /// a potential `VotersRecord` entry.
    ///
    /// # Errors
    ///
    /// - Returns [`RaftError::InvalidArgument`] if `new_hw` is less than or
    ///   equal to the current high watermark.
    /// - Returns [`RaftError::NonContiguousCommit`] if `committed_entries`
    ///   does not cover every offset in `[self.high_watermark, new_hw)`.
    pub fn advance_high_watermark(
        &mut self,
        new_hw: u64,
        committed_entries: &[LogEntry],
    ) -> RaftResult<()> {
        if new_hw <= self.high_watermark {
            return Err(RaftError::InvalidArgument(format!(
                "new high watermark ({}) must exceed current ({})",
                new_hw, self.high_watermark
            )));
        }

        let expected_count = (new_hw - self.high_watermark) as usize;

        // Phase 1: filter entries that fall within the commit range.
        let range_entries: Vec<&LogEntry> = committed_entries
            .iter()
            .filter(|e| e.offset >= self.high_watermark && e.offset < new_hw)
            .collect();

        // Phase 2: validate contiguity — every offset in
        // [self.high_watermark, new_hw) must be present.
        if range_entries.len() != expected_count {
            return Err(RaftError::NonContiguousCommit(format!(
                "expected {} entries for commit range [{}, {}), but got {}; \
                 committed_entries must be a complete, contiguous slice",
                expected_count, self.high_watermark, new_hw, range_entries.len()
            )));
        }

        // Build a set of the offsets we received for a gap check.
        let mut seen_offsets: Vec<u64> = range_entries.iter().map(|e| e.offset).collect();
        seen_offsets.sort_unstable();
        seen_offsets.dedup();

        if seen_offsets.len() != expected_count {
            return Err(RaftError::NonContiguousCommit(format!(
                "committed_entries contains duplicate offsets; \
                 expected {} unique offsets in [{}, {}), got {}",
                expected_count, self.high_watermark, new_hw, seen_offsets.len()
            )));
        }

        for (i, &offset) in seen_offsets.iter().enumerate() {
            let expected_offset = self.high_watermark + i as u64;
            if offset != expected_offset {
                return Err(RaftError::NonContiguousCommit(format!(
                    "gap in committed_entries: expected offset {}, found {}",
                    expected_offset, offset
                )));
            }
        }

        // Phase 3: apply VotersRecord entries and resolve pending membership
        // changes.
        for entry in &range_entries {
            if let LogEntryPayload::VotersRecord(ref record) = entry.payload {
                debug!(
                    node = %self.node_id,
                    offset = entry.offset,
                    "applying committed VotersRecord entry"
                );
                self.voter_set = record.voter_set.clone();
                self.voters_record = record.clone();

                // If this committed entry matches the pending membership
                // change, clear the pending flag.
                if let Some(ref pending) = self.pending_membership_change {
                    if pending.offset == entry.offset {
                        info!(
                            node = %self.node_id,
                            offset = entry.offset,
                            "pending membership change committed"
                        );
                        self.pending_membership_change = None;
                    }
                }
            }
        }

        // Phase 4: advance the watermark.
        self.high_watermark = new_hw;

        debug!(
            node = %self.node_id,
            high_watermark = self.high_watermark,
            "high watermark advanced"
        );

        Ok(())
    }

    /// Restores node state from snapshot metadata.
    ///
    /// This is called during snapshot installation. The node adopts the
    /// snapshot's voter set, voters record, and commit position.
    pub fn restore_from_snapshot_metadata(
        &mut self,
        last_included_index: u64,
        last_included_term: Term,
        leader_epoch: Term,
        voter_set: VoterSet,
        voters_record: VotersRecord,
    ) {
        info!(
            node = %self.node_id,
            last_included_index,
            last_included_term,
            leader_epoch,
            "restoring state from snapshot metadata"
        );

        self.high_watermark = last_included_index;
        self.leader_epoch = leader_epoch;
        self.snapshot_last_term = last_included_term;
        self.log_start_offset = last_included_index;
        self.voter_set = voter_set;
        self.voters_record = voters_record;
        // A snapshot supersedes any in-flight membership change.
        self.pending_membership_change = None;
    }

    /// Clears the pending membership change if its offset is at or beyond
    /// `truncate_from`. This must be called whenever the log suffix is
    /// truncated (e.g. when a new leader overwrites a follower's
    /// uncommitted tail).
    pub fn clear_pending_if_truncated(&mut self, truncate_from: u64) {
        if let Some(ref pending) = self.pending_membership_change {
            if pending.offset >= truncate_from {
                warn!(
                    node = %self.node_id,
                    pending_offset = pending.offset,
                    truncate_from,
                    "clearing pending membership change due to log truncation"
                );
                self.pending_membership_change = None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node_id(id: u32) -> NodeId {
        NodeId::from(id)
    }

    fn make_data_entry(offset: u64, term: Term) -> LogEntry {
        LogEntry {
            offset,
            term,
            payload: LogEntryPayload::Data(vec![]),
        }
    }

    fn make_voters_record_entry(offset: u64, term: Term, voter_set: VoterSet) -> LogEntry {
        LogEntry {
            offset,
            term,
            payload: LogEntryPayload::VotersRecord(VotersRecord::new(voter_set, offset)),
        }
    }

    fn default_state() -> NodeState {
        let voter_set = VoterSet::from_iter(vec![make_node_id(1), make_node_id(2), make_node_id(3)]);
        let voters_record = VotersRecord::new(voter_set.clone(), 0);
        NodeState::new(make_node_id(1), voter_set, voters_record)
    }

    #[test]
    fn advance_hw_rejects_non_contiguous_entries() {
        let mut state = default_state();

        // Provide entries at offsets 0 and 2, skipping 1.
        let entries = vec![
            make_data_entry(0, 1),
            make_data_entry(2, 1),
        ];

        let result = state.advance_high_watermark(3, &entries);
        assert!(result.is_err());
        assert!(
            matches!(&result, Err(RaftError::NonContiguousCommit(_))),
            "expected NonContiguousCommit, got {:?}",
            result
        );
        // HW must not have advanced.
        assert_eq!(state.high_watermark(), 0);
    }

    #[test]
    fn advance_hw_rejects_insufficient_entry_count() {
        let mut state = default_state();

        // Only provide 2 entries for a range of 3.
        let entries = vec![
            make_data_entry(0, 1),
            make_data_entry(1, 1),
        ];

        let result = state.advance_high_watermark(3, &entries);
        assert!(result.is_err());
        assert_eq!(state.high_watermark(), 0);
    }

    #[test]
    fn advance_hw_applies_voters_record() {
        let mut state = default_state();

        let new_voter_set = VoterSet::from_iter(vec![
            make_node_id(1),
            make_node_id(2),
            make_node_id(4),
        ]);

        // Propose the membership change first.
        state
            .propose_membership_change(1, new_voter_set.clone())
            .unwrap();
        assert!(state.pending_membership_change().is_some());

        let entries = vec![
            make_data_entry(0, 1),
            make_voters_record_entry(1, 1, new_voter_set.clone()),
            make_data_entry(2, 1),
        ];

        state.advance_high_watermark(3, &entries).unwrap();
        assert_eq!(state.high_watermark(), 3);
        assert_eq!(state.voter_set(), &new_voter_set);
        // Pending membership change should be cleared.
        assert!(state.pending_membership_change().is_none());
    }

    #[test]
    fn advance_hw_rejects_backward_movement() {
        let mut state = default_state();

        let entries = vec![make_data_entry(0, 1)];
        state.advance_high_watermark(1, &entries).unwrap();

        let result = state.advance_high_watermark(1, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn advance_hw_rejects_duplicate_offsets() {
        let mut state = default_state();

        let entries = vec![
            make_data_entry(0, 1),
            make_data_entry(0, 1), // duplicate
        ];

        let result = state.advance_high_watermark(2, &entries);
        assert!(result.is_err());
    }

    #[test]
    fn clear_pending_if_truncated_clears_when_at_boundary() {
        let mut state = default_state();
        let new_vs = VoterSet::from_iter(vec![make_node_id(1), make_node_id(2)]);
        state.propose_membership_change(5, new_vs).unwrap();
        assert!(state.pending_membership_change().is_some());

        state.clear_pending_if_truncated(5);
        assert!(state.pending_membership_change().is_none());
    }

    #[test]
    fn clear_pending_if_truncated_preserves_when_below_boundary() {
        let mut state = default_state();
        let new_vs = VoterSet::from_iter(vec![make_node_id(1), make_node_id(2)]);
        state.propose_membership_change(3, new_vs).unwrap();

        state.clear_pending_if_truncated(5);
        assert!(state.pending_membership_change().is_some());
    }

    #[test]
    fn restore_from_snapshot_stores_last_included_term() {
        let mut state = default_state();
        let vs = VoterSet::from_iter(vec![make_node_id(1), make_node_id(2)]);
        let vr = VotersRecord::new(vs.clone(), 10);

        state.restore_from_snapshot_metadata(10, 3, 5, vs, vr);
        assert_eq!(state.snapshot_last_term(), 3);
        assert_eq!(state.high_watermark(), 10);
        assert_eq!(state.leader_epoch(), 5);
    }
}