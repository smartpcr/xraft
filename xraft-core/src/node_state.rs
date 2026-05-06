use std::collections::{HashMap, HashSet};
use crate::types::{NodeId, Term, Role, VoterInfo, AppRecord};
use crate::log_entry::{LogEntry, EntryType};
use crate::rpc::*;
use crate::consensus_state::ConsensusState;

/// Per-follower replication progress tracked by the leader.
#[derive(Debug, Clone)]
pub struct FollowerProgress {
    pub node_id: NodeId,
    pub fetch_offset: u64,
    pub last_fetch_time_ms: u64,
}

/// Leader epoch checkpoint entry — maps an epoch (term) to the log offset
/// where that epoch begins.
#[derive(Debug, Clone)]
pub struct EpochEntry {
    pub epoch: u64,
    pub start_offset: u64,
}

/// Full internal protocol state of a Raft node.
#[derive(Debug)]
pub struct NodeState {
    pub node_id: NodeId,
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub role: Role,
    pub leader_id: Option<NodeId>,

    // Log — in-memory for deterministic testing; production uses LogStore trait
    pub log: Vec<LogEntry>,
    pub log_start_offset: u64,
    pub high_watermark: u64,

    // Voter set
    pub voter_set: Vec<VoterInfo>,

    // Leader-only state
    pub follower_progress: HashMap<NodeId, FollowerProgress>,

    // Election state
    pub votes_received: HashSet<NodeId>,
    pub pre_votes_received: HashSet<NodeId>,
    pub election_deadline_ms: u64,
    pub check_quorum_deadline_ms: u64,
    pub last_heard_from_leader_ms: u64,

    // Leader epoch checkpoint: ordered list of (epoch, start_offset).
    // Maintained by ALL nodes — followers record epoch transitions when
    // receiving entries from a new leader term; leaders record when
    // they append their LeaderChangeMessage.
    pub leader_epoch_checkpoint: Vec<EpochEntry>,

    // State machine applied offset
    pub last_applied: u64,
}

impl NodeState {
    pub fn new(node_id: NodeId, voter_set: Vec<VoterInfo>) -> Self {
        Self {
            node_id,
            current_term: Term(0),
            voted_for: None,
            role: Role::Unattached,
            leader_id: None,
            log: Vec::new(),
            log_start_offset: 0,
            high_watermark: 0,
            voter_set,
            follower_progress: HashMap::new(),
            votes_received: HashSet::new(),
            pre_votes_received: HashSet::new(),
            election_deadline_ms: 0,
            check_quorum_deadline_ms: 0,
            last_heard_from_leader_ms: 0,
            leader_epoch_checkpoint: Vec::new(),
            last_applied: 0,
        }
    }

    /// Get current log_end_offset (next offset to be appended).
    pub fn log_end_offset(&self) -> u64 {
        if self.log.is_empty() {
            self.log_start_offset
        } else {
            self.log.last().unwrap().offset + 1
        }
    }

    /// Get the term of the last log entry (or Term(0) if empty).
    pub fn last_log_term(&self) -> Term {
        self.log.last().map(|e| e.term).unwrap_or(Term(0))
    }

    /// Get the offset of the last log entry (or 0 if empty).
    pub fn last_log_offset(&self) -> u64 {
        if self.log.is_empty() {
            0
        } else {
            self.log.last().unwrap().offset
        }
    }

    /// Append entries to the log, tracking epoch transitions.
    pub fn append_entries(&mut self, entries: Vec<LogEntry>) {
        for entry in entries {
            self.maybe_record_epoch(&entry);
            self.log.push(entry);
        }
    }

    /// Append a single entry and return its offset.
    pub fn append_entry(&mut self, mut entry: LogEntry) -> u64 {
        let offset = self.log_end_offset();
        entry.offset = offset;
        self.maybe_record_epoch(&entry);
        self.log.push(entry);
        offset
    }

    /// Record an epoch boundary if this entry starts a new epoch.
    fn maybe_record_epoch(&mut self, entry: &LogEntry) {
        let prev_term = self.log.last().map(|e| e.term.0).unwrap_or(0);
        if entry.term.0 != prev_term || self.leader_epoch_checkpoint.is_empty() {
            // Avoid duplicate epoch entries for the same epoch
            if self.leader_epoch_checkpoint.last().map(|e| e.epoch) != Some(entry.term.0) {
                self.leader_epoch_checkpoint.push(EpochEntry {
                    epoch: entry.term.0,
                    start_offset: entry.offset,
                });
            }
        }
    }

    /// Read entries from [start, end) exclusive.
    pub fn read_entries(&self, start: u64, end: u64) -> Vec<LogEntry> {
        self.log.iter()
            .filter(|e| e.offset >= start && e.offset < end)
            .cloned()
            .collect()
    }

    /// Truncate log from the given offset onward (for divergence resolution).
    /// Clamps the truncation point to max(from_offset, high_watermark) to
    /// prevent truncating committed entries. If from_offset < high_watermark,
    /// the truncation is safely clamped rather than panicking.
    pub fn truncate_suffix(&mut self, from_offset: u64) {
        // Clamp to protect committed and applied entries
        let safe_offset = from_offset
            .max(self.high_watermark)
            .max(self.last_applied);

        self.log.retain(|e| e.offset < safe_offset);
        // Remove epoch checkpoint entries at or beyond the truncation point
        self.leader_epoch_checkpoint
            .retain(|e| e.start_offset < safe_offset);
    }

    /// Get entry at a specific offset.
    pub fn entry_at(&self, offset: u64) -> Option<&LogEntry> {
        self.log.iter().find(|e| e.offset == offset)
    }

    /// Become a candidate and start election.
    pub fn start_election(&mut self, now_ms: u64, election_timeout_ms: u64) {
        self.current_term = self.current_term.next();
        self.role = Role::Candidate;
        self.voted_for = Some(self.node_id);
        self.leader_id = None;
        self.votes_received.clear();
        self.votes_received.insert(self.node_id);
        self.election_deadline_ms = now_ms + election_timeout_ms;
    }

    /// Become leader.
    pub fn become_leader(&mut self, now_ms: u64, check_quorum_interval_ms: u64) {
        self.role = Role::Leader;
        self.leader_id = Some(self.node_id);
        self.follower_progress.clear();
        self.check_quorum_deadline_ms = now_ms + check_quorum_interval_ms;

        // Initialize follower progress for all voters except self
        for voter in &self.voter_set {
            if voter.node_id != self.node_id {
                self.follower_progress.insert(voter.node_id, FollowerProgress {
                    node_id: voter.node_id,
                    fetch_offset: 0,
                    last_fetch_time_ms: now_ms,
                });
            }
        }

        // Append LeaderChangeMessage (no-op) as first entry of new term.
        // The epoch checkpoint is recorded via maybe_record_epoch.
        let offset = self.log_end_offset();
        let entry = LogEntry::leader_change(offset, self.current_term);
        self.maybe_record_epoch(&entry);
        self.log.push(entry);
    }

    /// Become follower for the given term.
    pub fn become_follower(&mut self, term: Term, leader_id: Option<NodeId>, now_ms: u64, election_timeout_ms: u64) {
        self.current_term = term;
        self.role = Role::Follower;
        self.leader_id = leader_id;
        self.voted_for = None;
        self.votes_received.clear();
        self.follower_progress.clear();
        self.election_deadline_ms = now_ms + election_timeout_ms;
        self.last_heard_from_leader_ms = now_ms;
    }

    /// Step down to Unattached (used when leader fails quorum check).
    pub fn step_down(&mut self) {
        self.role = Role::Unattached;
        self.leader_id = None;
        self.follower_progress.clear();
    }

    /// Propose a command — leader only.
    pub fn propose(&mut self, record: &AppRecord) -> Option<u64> {
        if self.role != Role::Leader {
            return None;
        }
        let offset = self.log_end_offset();
        let entry = LogEntry::command(offset, self.current_term, record);
        self.log.push(entry);
        Some(offset)
    }

    /// Handle a Vote request and return the response.
    pub fn handle_vote_request(&mut self, req: &VoteRequest, now_ms: u64, election_timeout_ms: u64) -> VoteResponse {
        // Step down if request has higher term
        if req.term > self.current_term {
            self.become_follower(req.term, None, now_ms, election_timeout_ms);
        }

        let mut vote_granted = false;

        if req.term >= self.current_term {
            let can_vote = self.voted_for.is_none() || self.voted_for == Some(req.candidate_id);
            let log_ok = req.last_log_term > self.last_log_term()
                || (req.last_log_term == self.last_log_term() && req.last_log_offset >= self.last_log_offset());

            if can_vote && log_ok {
                self.voted_for = Some(req.candidate_id);
                self.current_term = req.term;
                vote_granted = true;
                self.election_deadline_ms = now_ms + election_timeout_ms;
            }
        }

        VoteResponse {
            term: self.current_term,
            vote_granted,
            is_pre_vote: req.is_pre_vote,
        }
    }

    /// Handle a Vote response. Returns true if we won the election.
    pub fn handle_vote_response(&mut self, resp: &VoteResponse, from: NodeId, now_ms: u64, election_timeout_ms: u64, check_quorum_ms: u64) -> bool {
        if resp.term > self.current_term {
            self.become_follower(resp.term, None, now_ms, election_timeout_ms);
            return false;
        }

        if self.role != Role::Candidate {
            return false;
        }

        if resp.vote_granted {
            self.votes_received.insert(from);
        }

        let majority = self.voter_set.len() / 2 + 1;
        if self.votes_received.len() >= majority {
            self.become_leader(now_ms, check_quorum_ms);
            return true;
        }
        false
    }

    /// Handle a Fetch request from a follower — leader side.
    /// Returns a FetchResponse.
    ///
    /// Processing order per architecture §4.1:
    /// 1. Validate divergence BEFORE recording follower progress
    /// 2. Update follower progress only after divergence validation passes
    /// 3. Advance high watermark
    /// 4. Return entries + HW
    pub fn handle_fetch_request(&mut self, req: &FetchRequest, now_ms: u64) -> FetchResponse {
        // Check for log divergence FIRST — do not let a divergent follower's
        // advertised fetch_offset influence HW calculation.
        if let Some(diverging) = self.check_divergence(req.last_fetched_epoch, req.fetch_offset) {
            return FetchResponse {
                leader_id: self.node_id,
                leader_epoch: self.current_term.0,
                high_watermark: self.high_watermark,
                log_start_offset: self.log_start_offset,
                entries: vec![],
                diverging_epoch: Some(diverging),
                snapshot_id: None,
            };
        }

        // Only update follower progress AFTER divergence validation passes
        if let Some(progress) = self.follower_progress.get_mut(&req.replica_id) {
            progress.fetch_offset = req.fetch_offset;
            progress.last_fetch_time_ms = now_ms;
        }

        // Advance high watermark before responding
        self.advance_high_watermark();

        // Read entries starting from fetch_offset
        let entries = self.read_entries(req.fetch_offset, self.log_end_offset());

        FetchResponse {
            leader_id: self.node_id,
            leader_epoch: self.current_term.0,
            high_watermark: self.high_watermark,
            log_start_offset: self.log_start_offset,
            entries,
            diverging_epoch: None,
            snapshot_id: None,
        }
    }

    /// Check for log divergence against leader-epoch checkpoint.
    /// Returns a `DivergingEpoch` if the follower's last fetched epoch
    /// does not match the leader's epoch history at that offset range.
    ///
    /// Handles the same-epoch extra-tail case: if the follower claims an
    /// epoch that IS the leader's latest epoch but fetch_offset exceeds the
    /// leader's LEO, the follower has entries from a stale leader in the
    /// same term — truncate to the leader's LEO.
    fn check_divergence(&self, last_fetched_epoch: u64, fetch_offset: u64) -> Option<DivergingEpoch> {
        if last_fetched_epoch == 0 || self.leader_epoch_checkpoint.is_empty() {
            return None;
        }

        // Find the epoch boundary for last_fetched_epoch
        let mut epoch_end = None;
        let mut found = false;
        for i in 0..self.leader_epoch_checkpoint.len() {
            if self.leader_epoch_checkpoint[i].epoch == last_fetched_epoch {
                found = true;
                // The epoch ends where the next epoch starts
                if i + 1 < self.leader_epoch_checkpoint.len() {
                    epoch_end = Some(self.leader_epoch_checkpoint[i + 1].start_offset);
                } else {
                    // This IS the latest epoch on the leader.
                    // The epoch extends to the leader's LEO.
                    epoch_end = Some(self.log_end_offset());
                }
                break;
            } else if self.leader_epoch_checkpoint[i].epoch > last_fetched_epoch {
                // This epoch never existed on the leader — the follower has entries
                // from a term the leader doesn't know about. Truncate to this
                // epoch's start offset.
                epoch_end = Some(self.leader_epoch_checkpoint[i].start_offset);
                break;
            }
        }

        // If the epoch wasn't found and no higher epoch exists, the follower
        // is at an epoch beyond the leader's knowledge — divergence at LEO
        if !found && epoch_end.is_none() {
            return Some(DivergingEpoch {
                epoch: last_fetched_epoch,
                end_offset: self.log_end_offset(),
            });
        }

        if let Some(end) = epoch_end {
            if fetch_offset > end {
                return Some(DivergingEpoch {
                    epoch: last_fetched_epoch,
                    end_offset: end,
                });
            }
        }

        None
    }

    /// Handle a Fetch response on the follower side.
    /// Returns the number of new entries actually appended.
    pub fn handle_fetch_response(&mut self, resp: &FetchResponse, now_ms: u64, election_timeout_ms: u64) -> usize {
        // Reject stale responses from old terms
        if resp.leader_epoch < self.current_term.0 {
            return 0;
        }

        // Step up to leader's term if higher
        if resp.leader_epoch > self.current_term.0 {
            self.become_follower(
                Term(resp.leader_epoch),
                Some(resp.leader_id),
                now_ms,
                election_timeout_ms,
            );
        } else {
            // Same term — validate leader identity
            if self.leader_id.is_some() && self.leader_id != Some(resp.leader_id) {
                return 0; // conflicting leader in same term
            }
            self.leader_id = Some(resp.leader_id);
            self.last_heard_from_leader_ms = now_ms;
            self.election_deadline_ms = now_ms + election_timeout_ms;
            // Ensure we're a follower (not candidate/unattached) when receiving
            // a valid response at our term
            if self.role != Role::Follower {
                self.role = Role::Follower;
            }
        }

        // Handle divergence — truncate our log
        if let Some(ref div) = resp.diverging_epoch {
            self.truncate_suffix(div.end_offset);
            return 0;
        }

        // Check for conflicting entries in the overlap range and truncate
        // at the first conflict. This is a safety net even if epoch-based
        // divergence detection misses an edge case.
        for entry in &resp.entries {
            if let Some(local) = self.log.iter().find(|e| e.offset == entry.offset) {
                if local.term != entry.term || local.data != entry.data {
                    // Conflict detected — truncate from this offset
                    self.truncate_suffix(entry.offset);
                    break;
                }
            }
        }

        // Append new entries, tracking epoch transitions
        let mut appended = 0usize;
        for entry in &resp.entries {
            if entry.offset >= self.log_end_offset() {
                self.maybe_record_epoch(entry);
                self.log.push(entry.clone());
                appended += 1;
            }
        }

        // Update high watermark from leader's HW, constrained to our log end
        // (follower should not claim HW beyond entries it actually has)
        let effective_hw = std::cmp::min(resp.high_watermark, self.log_end_offset());
        if effective_hw > self.high_watermark {
            self.high_watermark = effective_hw;
        }

        appended
    }

    /// Advance HW based on majority replication — leader only.
    /// Uses the canonical algorithm: sort all voters' fetch_offsets descending,
    /// pick index ⌊V/2⌋.
    pub fn advance_high_watermark(&mut self) {
        if self.role != Role::Leader {
            return;
        }

        let mut offsets: Vec<u64> = Vec::new();

        // Leader's own log_end_offset
        offsets.push(self.log_end_offset());

        // Each follower's fetch_offset
        for progress in self.follower_progress.values() {
            offsets.push(progress.fetch_offset);
        }

        // Sort descending
        offsets.sort_unstable_by(|a, b| b.cmp(a));

        let voter_count = offsets.len();
        if voter_count == 0 {
            return;
        }

        // Majority index: ⌊V/2⌋ (0-indexed)
        let majority_index = voter_count / 2;
        let candidate = offsets[majority_index];

        // HW never decreases
        if candidate > self.high_watermark {
            self.high_watermark = candidate;
        }
    }

    /// Apply committed entries to the state machine.
    /// Returns the AppRecords that were applied.
    pub fn get_committable_entries(&self) -> Vec<(u64, AppRecord)> {
        let mut results = Vec::new();
        for entry in &self.log {
            if entry.offset >= self.last_applied && entry.offset < self.high_watermark {
                if entry.entry_type == EntryType::Command {
                    results.push((entry.offset, AppRecord { data: entry.data.clone() }));
                }
            }
        }
        results
    }

    /// Mark entries as applied up to the given offset.
    pub fn mark_applied(&mut self, up_to: u64) {
        if up_to > self.last_applied {
            self.last_applied = up_to;
        }
    }

    /// Create a public ConsensusState projection.
    pub fn to_consensus_state(&self) -> ConsensusState {
        ConsensusState {
            node_id: self.node_id,
            current_term: self.current_term,
            role: self.role,
            leader_id: self.leader_id,
            log_end_offset: self.log_end_offset(),
            high_watermark: self.high_watermark,
            voter_set: self.voter_set.clone(),
        }
    }
}
