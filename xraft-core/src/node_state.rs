use std::collections::{HashMap, HashSet};

use crate::consensus_state::{ConsensusState, Role};
use crate::error::{Result, XraftError};
use crate::follower_progress::FollowerProgress;
use crate::log_entry::{EntryType, LogEntry};
use crate::types::{ClusterId, NodeId, Term, VoterInfo, VotersRecord};

/// Tracks a VotersRecord that has been appended but not yet committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingMembershipChange {
    /// Log offset of the uncommitted VotersRecord.
    pub offset: u64,
    /// The proposed new voter set.
    pub voters: Vec<VoterInfo>,
}

/// Full internal protocol state (pub(crate) visibility in production code).
/// Exposed publicly here for test infrastructure access.
#[derive(Debug, Clone)]
pub struct NodeState {
    pub node_id: NodeId,
    pub cluster_id: ClusterId,
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub role: Role,
    pub leader_id: Option<NodeId>,

    // Log boundaries
    pub log_start_offset: u64,
    pub log_end_offset: u64,
    /// Exclusive upper bound of committed offsets. Entry at offset O is
    /// committed iff O < high_watermark.
    pub high_watermark: u64,

    /// The committed voter set — used for elections, Check Quorum, and read().
    pub voter_set: Vec<VoterInfo>,
    pub observers: HashSet<NodeId>,

    /// At most one pending membership change (leader-only).
    pub pending_membership_change: Option<PendingMembershipChange>,

    /// Per-follower replication progress (leader-only).
    pub follower_state: HashMap<NodeId, FollowerProgress>,
}

impl NodeState {
    /// Create a new NodeState with the given node_id and cluster_id.
    pub fn new(node_id: NodeId, cluster_id: ClusterId) -> Self {
        Self {
            node_id,
            cluster_id,
            current_term: Term(0),
            voted_for: None,
            role: Role::Unattached,
            leader_id: None,
            log_start_offset: 0,
            log_end_offset: 0,
            high_watermark: 0,
            voter_set: Vec::new(),
            observers: HashSet::new(),
            pending_membership_change: None,
            follower_state: HashMap::new(),
        }
    }

    /// Bootstrap the node with an initial voter set (per architecture §5.9).
    ///
    /// Sets term=0, role=Follower. The node must win an election to become
    /// Leader and append LeaderChangeMessage + VotersRecord to the log.
    pub fn bootstrap(&mut self, initial_voters: Vec<VoterInfo>) {
        self.voter_set = initial_voters;
        self.current_term = Term(0);
        self.role = Role::Follower;
        self.leader_id = None;
    }

    /// Simulate winning an election: increment term, become Leader.
    ///
    /// In production this is driven by the election manager; exposed here
    /// so tests can exercise the protocol path without a full EventLoop.
    pub fn become_leader(&mut self) {
        self.current_term = Term(self.current_term.0 + 1);
        self.role = Role::Leader;
        self.voted_for = Some(self.node_id);
        self.leader_id = Some(self.node_id);
    }

    /// Project internal state to public ConsensusState.
    pub fn project(&self) -> ConsensusState {
        ConsensusState {
            node_id: self.node_id,
            current_term: self.current_term,
            role: self.role,
            leader_id: self.leader_id,
            log_end_offset: self.log_end_offset,
            high_watermark: self.high_watermark,
            voter_set: self.voter_set.clone(),
        }
    }

    /// Apply committed entries up to `new_hw`, processing control records.
    ///
    /// Two-phase commit: first validates all VotersRecord entries can be
    /// deserialized, then advances HW and applies changes. If any
    /// VotersRecord is corrupt, HW is NOT advanced — the caller must
    /// treat this as a fatal error.
    pub fn advance_high_watermark(
        &mut self,
        new_hw: u64,
        committed_entries: &[LogEntry],
    ) -> Result<()> {
        if new_hw <= self.high_watermark {
            return Ok(());
        }

        // Phase 1: validate — deserialize all VotersRecords before mutating state.
        // Collect (offset, deserialized_record) pairs for entries being committed.
        let mut vr_updates: Vec<(u64, VotersRecord)> = Vec::new();
        for entry in committed_entries {
            if entry.offset >= self.high_watermark
                && entry.offset < new_hw
                && entry.entry_type == EntryType::VotersRecord
            {
                let record = bincode::deserialize::<VotersRecord>(&entry.payload)
                    .map_err(|e| {
                        XraftError::SerializationError(format!(
                            "failed to deserialize committed VotersRecord at offset {}: {}",
                            entry.offset, e
                        ))
                    })?;
                vr_updates.push((entry.offset, record));
            }
        }

        // Phase 2: apply — all deserialization succeeded, safe to mutate.
        self.high_watermark = new_hw;

        for (offset, record) in vr_updates {
            self.voter_set = record.voters;
            if let Some(ref pending) = self.pending_membership_change {
                if pending.offset == offset {
                    self.pending_membership_change = None;
                }
            }
        }
        Ok(())
    }

    /// Restore state from a snapshot's metadata.
    pub fn restore_from_snapshot_metadata(
        &mut self,
        last_included_offset: u64,
        last_included_term: Term,
        voters: Vec<VoterInfo>,
        leader_epoch: Term,
    ) {
        self.voter_set = voters;
        self.high_watermark = last_included_offset + 1;
        self.log_start_offset = last_included_offset + 1;
        self.log_end_offset = last_included_offset + 1;
        self.current_term = leader_epoch;
        self.pending_membership_change = None;

        // Preserve last_included_term for consistency
        let _ = last_included_term;
    }

    /// Replay a log tail entry discovered after snapshot recovery.
    ///
    /// These entries have **unknown committed status** — the recovering node
    /// cannot know whether they were committed before the crash. Per the
    /// architecture (§5.10), HW is NOT advanced during recovery; the
    /// authoritative HW comes from the leader via Fetch responses.
    ///
    /// VotersRecord entries in the tail are tracked as pending membership
    /// changes (the latest one overwrites any earlier pending). They are
    /// NOT applied to the committed `voter_set` until the leader confirms
    /// commitment by advancing HW past their offset.
    ///
    /// Returns an error if a VotersRecord entry cannot be deserialized —
    /// corrupt membership data in the log tail must not be silently ignored.
    pub fn replay_log_tail_entry(&mut self, entry: &LogEntry) -> Result<()> {
        // Track VotersRecord as pending, not committed
        if entry.entry_type == EntryType::VotersRecord {
            let record = bincode::deserialize::<VotersRecord>(&entry.payload)
                .map_err(|e| {
                    XraftError::SerializationError(format!(
                        "failed to deserialize VotersRecord at offset {} during recovery: {}",
                        entry.offset, e
                    ))
                })?;
            self.pending_membership_change = Some(PendingMembershipChange {
                offset: entry.offset,
                voters: record.voters,
            });
        }
        // Advance log_end_offset only — HW stays at snapshot level
        if entry.offset + 1 > self.log_end_offset {
            self.log_end_offset = entry.offset + 1;
        }
        Ok(())
    }
}
