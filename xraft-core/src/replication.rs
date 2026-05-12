use crate::app_record::AppRecord;
use crate::config::RaftConfig;
use crate::deferred_completion::DeferredCompletionQueue;
use crate::error::{Result, XraftError};
use crate::io_action::{IoAction, IoActionBatch};
use crate::log_entry::EntryType;
use crate::node_state::{NodeState, Role};
use crate::quorum_state::QuorumState;
use crate::rpc::{
    FetchRequest, FetchResponse, FetchSnapshotRequest, RpcEnvelope, RpcPayload,
    SnapshotId,
};
use crate::traits::{Clock, Listener, StateMachine};
use crate::types::{NodeId, Term};
use crate::voter::VotersRecord;
use std::time::Duration;
use tokio::time::Instant;

// ══════════════════════════════════════════════════════════════
// RaftNode integration
// ══════════════════════════════════════════════════════════════

/// Wires the follower's Fetch RPC pipeline into the RaftNode event loop.
///
/// `RaftNode` is the top-level entry point that the event loop calls.
/// It dispatches inbound RPC messages, drives periodic Fetch ticks via the
/// `Clock` abstraction, and collects `IoActionBatch`es for the I/O stage.
///
/// ```text
/// loop {
///     tokio::select! {
///         _ = clock.sleep_until(node.next_fetch_deadline(&state)) => {
///             let batch = node.tick(&mut state, &clock);
///             io_stage.execute(batch).await;
///         }
///         envelope = transport.recv() => {
///             let batch = node.handle_message(
///                 &mut state, &envelope, &mut sm, &mut listener,
///                 &mut queue, &mut sc, &clock,
///             );
///             if let Ok(batch) = batch {
///                 io_stage.execute(batch.io_batch).await;
///             }
///         }
///     }
/// }
/// ```
pub struct RaftNode {
    replication: ReplicationManager,
}

impl RaftNode {
    pub fn new(config: &RaftConfig) -> Self {
        Self {
            replication: ReplicationManager::new(config),
        }
    }

    /// Called by the event loop when the fetch deadline elapses.
    /// Returns an `IoActionBatch` containing a `SendRpc` action if a leader
    /// is known, or an empty batch otherwise.
    pub fn tick<C: Clock>(&self, state: &mut NodeState, clock: &C) -> IoActionBatch {
        let now = clock.now();
        self.replication
            .poll_fetch(state, now)
            .unwrap_or_default()
    }

    /// Returns the `Instant` of the next fetch deadline, for the event loop's
    /// `tokio::select!` sleep branch.
    pub fn next_fetch_deadline(&self, state: &NodeState) -> Instant {
        state.fetch_deadline
    }

    /// Dispatch an inbound `RpcEnvelope` through the follower pipeline.
    ///
    /// Currently handles `FetchResponse`; other RPC types are passed through
    /// to the caller for upstream dispatch (returns `Err(InvalidResponse)`
    /// for unhandled message types in this module).
    pub fn handle_message<S: StateMachine, L: Listener, SC: SnapshotCoordinator, C: Clock>(
        &self,
        state: &mut NodeState,
        envelope: &RpcEnvelope,
        state_machine: &mut S,
        listener: &mut L,
        completion_queue: &mut DeferredCompletionQueue,
        snapshot_coordinator: &mut SC,
        clock: &C,
    ) -> Result<FetchResponseResult> {
        // Envelope-level cluster_id fencing.
        if envelope.cluster_id != state.cluster_id {
            return Err(XraftError::InvalidClusterId);
        }

        match &envelope.payload {
            RpcPayload::FetchResponse(response) => {
                // Validate envelope source matches the response's leader_id.
                if envelope.source != response.leader_id {
                    return Err(XraftError::InvalidResponse(format!(
                        "envelope source {} does not match response leader_id {}",
                        envelope.source, response.leader_id
                    )));
                }
                self.replication.handle_fetch_response(
                    state,
                    response,
                    state_machine,
                    listener,
                    completion_queue,
                    snapshot_coordinator,
                    clock,
                )
            }
            other => Err(XraftError::InvalidResponse(format!(
                "RaftNode follower pipeline does not handle {:?}",
                std::mem::discriminant(other)
            ))),
        }
    }

    /// Access the underlying `ReplicationManager`.
    pub fn replication(&self) -> &ReplicationManager {
        &self.replication
    }
}

// ══════════════════════════════════════════════════════════════
// EventLoop integration: FollowerEventLoop
// ══════════════════════════════════════════════════════════════

/// Drives periodic Fetch RPCs through the Clock/EventLoop.
///
/// The `EventLoop` (or its equivalent `tokio::select!` loop) should call
/// `FollowerEventLoop::run_tick` on each iteration. Internally it uses
/// `Clock::now()` to decide whether `fetch_deadline` has elapsed and, if so,
/// fires a FetchRequest. The caller feeds the resulting `IoActionBatch` to
/// the I/O stage.
///
/// ```text
/// loop {
///     tokio::select! {
///         // branch 1: fetch tick
///         _ = clock.sleep_until(state.fetch_deadline) => {
///             let batch = follower_loop.run_tick(&mut state, &clock);
///             io_stage.execute(batch).await;
///         }
///         // branch 2: inbound RPC
///         msg = transport.recv() => { /* dispatch */ }
///     }
/// }
/// ```
pub struct FollowerEventLoop {
    replication: ReplicationManager,
}

impl FollowerEventLoop {
    pub fn new(config: &RaftConfig) -> Self {
        Self {
            replication: ReplicationManager::new(config),
        }
    }

    /// Called by the event loop on each iteration. Checks the fetch deadline
    /// against `clock.now()` and fires a Fetch RPC if due.
    pub fn run_tick<C: Clock>(&self, state: &mut NodeState, clock: &C) -> IoActionBatch {
        let now = clock.now();
        self.replication
            .poll_fetch(state, now)
            .unwrap_or_default()
    }

    /// Returns how long until the next fetch deadline, for use with
    /// `Clock::sleep_until` in a `tokio::select!` branch.
    pub fn time_until_next_tick<C: Clock>(&self, state: &NodeState, clock: &C) -> Duration {
        self.replication.time_until_fetch(state, clock.now())
    }

    /// Delegate to `ReplicationManager::handle_fetch_response`.
    pub fn handle_response<S: StateMachine, L: Listener, SC: SnapshotCoordinator, C: Clock>(
        &self,
        state: &mut NodeState,
        response: &FetchResponse,
        state_machine: &mut S,
        listener: &mut L,
        completion_queue: &mut DeferredCompletionQueue,
        snapshot_coordinator: &mut SC,
        clock: &C,
    ) -> Result<FetchResponseResult> {
        self.replication.handle_fetch_response(
            state,
            response,
            state_machine,
            listener,
            completion_queue,
            snapshot_coordinator,
            clock,
        )
    }

    /// Access the underlying `ReplicationManager`.
    pub fn replication(&self) -> &ReplicationManager {
        &self.replication
    }
}

/// Handles Fetch request/response processing on the follower side.
///
/// On the follower: sends periodic Fetch RPCs to the known leader, processes
/// responses, truncates log on divergence, and updates the local high watermark.
pub struct ReplicationManager {
    /// Maximum Fetch response payload size (from config).
    max_fetch_bytes: u32,
    /// Interval between periodic Fetch RPCs.
    fetch_interval: Duration,
}

/// Result of processing a fetch response on the follower side.
#[derive(Debug)]
pub struct FetchResponseResult {
    /// I/O actions to execute (log appends, truncations, RPCs).
    pub io_batch: IoActionBatch,
    /// Whether a snapshot transfer was initiated via the SnapshotCoordinator.
    pub snapshot_transfer_initiated: bool,
}

/// Trait for coordinating snapshot transfers (Phase 7 provides the full impl).
///
/// When a FetchResponse contains a `snapshot_id`, the ReplicationManager
/// delegates to this coordinator to initiate the snapshot transfer flow.
pub trait SnapshotCoordinator: Send + 'static {
    /// Begin a snapshot transfer for the given snapshot ID from the given leader.
    /// Returns IoActions needed to start the transfer (e.g., FetchSnapshotRequest RPC).
    fn begin_snapshot_transfer(
        &mut self,
        leader_id: NodeId,
        snapshot_id: &SnapshotId,
        state: &NodeState,
    ) -> IoActionBatch;

    /// Returns true if a snapshot transfer is currently in progress.
    fn is_transfer_in_progress(&self) -> bool;
}

/// Default SnapshotCoordinator that initiates a FetchSnapshotRequest RPC.
/// Full chunked transfer logic is implemented in Phase 7.
pub struct DefaultSnapshotCoordinator {
    in_progress: bool,
    max_fetch_bytes: u32,
}

impl DefaultSnapshotCoordinator {
    pub fn new(max_fetch_bytes: u32) -> Self {
        Self {
            in_progress: false,
            max_fetch_bytes,
        }
    }
}

impl SnapshotCoordinator for DefaultSnapshotCoordinator {
    fn begin_snapshot_transfer(
        &mut self,
        leader_id: NodeId,
        snapshot_id: &SnapshotId,
        state: &NodeState,
    ) -> IoActionBatch {
        self.in_progress = true;
        let mut batch = IoActionBatch::new();
        let request = FetchSnapshotRequest {
            snapshot_id: snapshot_id.clone(),
            position: 0,
            max_bytes: self.max_fetch_bytes,
        };
        let envelope = RpcEnvelope {
            cluster_id: state.cluster_id,
            leader_epoch: state.current_term,
            source: state.node_id,
            payload: RpcPayload::FetchSnapshotRequest(request),
        };
        batch.push(IoAction::SendRpc(leader_id, envelope));
        batch
    }

    fn is_transfer_in_progress(&self) -> bool {
        self.in_progress
    }
}

impl ReplicationManager {
    pub fn new(config: &RaftConfig) -> Self {
        ReplicationManager {
            max_fetch_bytes: config.max_fetch_bytes,
            fetch_interval: config.fetch_interval,
        }
    }

    /// Build a `FetchRequest` RPC to send to the current leader.
    ///
    /// Returns `None` if no leader is known (follower should wait for election).
    pub fn build_fetch_request(&self, state: &NodeState) -> Option<(NodeId, RpcEnvelope)> {
        // Do not send Fetch if no leader is known.
        let leader_id = state.leader_id?;

        // Do not send Fetch unless we are a follower.
        if state.role != Role::Follower {
            return None;
        }

        let request = FetchRequest {
            replica_id: state.node_id,
            fetch_offset: state.log_end_offset,
            last_fetched_epoch: state.last_log_term(),
            max_bytes: self.max_fetch_bytes,
        };

        let envelope = RpcEnvelope {
            cluster_id: state.cluster_id,
            leader_epoch: state.current_term,
            source: state.node_id,
            payload: RpcPayload::FetchRequest(request),
        };

        Some((leader_id, envelope))
    }

    /// Produce `IoAction`s for a follower's periodic fetch tick.
    ///
    /// Called when `fetch_deadline` has elapsed. Advances the fetch_deadline
    /// by `fetch_interval` and sends a FetchRequest if a leader is known.
    pub fn on_fetch_tick(&self, state: &mut NodeState, now: Instant) -> IoActionBatch {
        // Advance fetch_deadline for the next tick.
        state.fetch_deadline = now + self.fetch_interval;

        let mut batch = IoActionBatch::new();
        if let Some((leader_id, envelope)) = self.build_fetch_request(state) {
            batch.push(IoAction::SendRpc(leader_id, envelope));
        }
        batch
    }

    /// Check whether the fetch deadline has elapsed and, if so, execute the
    /// periodic fetch tick. Intended to be called by the `EventLoop` on each
    /// iteration — this wires the periodic fetch to the event-loop/clock.
    ///
    /// Returns `Some(IoActionBatch)` with a `SendRpc` action when a fetch
    /// fires, or `None` if the deadline has not yet elapsed.
    pub fn poll_fetch(&self, state: &mut NodeState, now: Instant) -> Option<IoActionBatch> {
        if now >= state.fetch_deadline && state.role == Role::Follower {
            Some(self.on_fetch_tick(state, now))
        } else {
            None
        }
    }

    /// Returns the duration until the next fetch deadline, for use with
    /// `Clock::sleep_until` in the EventLoop's `tokio::select!`.
    pub fn time_until_fetch(&self, state: &NodeState, now: Instant) -> Duration {
        if now >= state.fetch_deadline {
            Duration::ZERO
        } else {
            state.fetch_deadline - now
        }
    }

    /// Validate a FetchResponse and update term/leader state if needed.
    ///
    /// If `response.leader_epoch > state.current_term`, the follower adopts the
    /// higher term, clears `voted_for`, updates `leader_id`, steps down to
    /// Follower (if not already).
    ///
    /// Returns `Ok(Some(QuorumState))` when quorum state must be persisted
    /// (term bump or same-term leader discovery). The caller is responsible
    /// for appending the `PersistQuorumState` IoAction *after* running
    /// application callbacks, per architecture §4.1.
    ///
    /// Rejects stale-term responses, mismatched leader IDs, and responses
    /// received after the node is no longer a follower.
    ///
    /// IMPORTANT: This method validates all preconditions BEFORE mutating any
    /// state. If any check fails, the node state is left unchanged (atomicity).
    fn validate_and_update_term(
        &self,
        state: &mut NodeState,
        response: &FetchResponse,
    ) -> Result<Option<QuorumState>> {
        // Reject responses from a leader with a stale term.
        if response.leader_epoch < state.current_term {
            return Err(XraftError::InvalidResponse(format!(
                "stale leader epoch {} < current term {}",
                response.leader_epoch.0, state.current_term.0
            )));
        }

        // Higher leader_epoch: adopt the new term, step down, update leader.
        if response.leader_epoch > state.current_term {
            state.current_term = response.leader_epoch;
            state.voted_for = None;
            state.leader_id = Some(response.leader_id);
            state.role = Role::Follower;
            // Clear election state from any in-progress election.
            state.votes_received.clear();
            state.pre_votes_received.clear();

            return Ok(Some(QuorumState {
                current_term: state.current_term,
                voted_for: state.voted_for,
                leader_id: state.leader_id,
                leader_epoch: state.current_term,
            }));
        }

        // Same term — validate preconditions BEFORE mutating state.
        // The node MUST be a follower to accept a same-term FetchResponse.
        // A Candidate or Leader receiving a same-term response from a
        // different leader has a stale in-flight response — reject it
        // without mutating any state.
        if state.role != Role::Follower {
            return Err(XraftError::InvalidResponse(format!(
                "node is {:?}, not Follower — ignoring stale fetch response in same term {}",
                state.role, state.current_term.0
            )));
        }

        // Validate leader consistency within the same term.
        if let Some(known_leader) = state.leader_id {
            if response.leader_id != known_leader {
                return Err(XraftError::InvalidResponse(format!(
                    "response from {} but known leader is {}",
                    response.leader_id, known_leader
                )));
            }
            // Same leader, same term — no state change needed.
        } else {
            // Same-term leader discovery: we didn't know the leader yet —
            // record it and persist quorum state so a crash doesn't forget
            // who the leader is.
            state.leader_id = Some(response.leader_id);

            return Ok(Some(QuorumState {
                current_term: state.current_term,
                voted_for: state.voted_for,
                leader_id: state.leader_id,
                leader_epoch: state.current_term,
            }));
        }

        // Validate mutually-exclusive response fields.
        if response.snapshot_id.is_some() && response.diverging_epoch.is_some() {
            return Err(XraftError::InvalidResponse(
                "response contains both snapshot_id and diverging_epoch".into(),
            ));
        }

        Ok(None)
    }

    /// Reset election and fetch deadlines on receipt of a valid response.
    fn reset_timers<C: Clock>(&self, state: &mut NodeState, clock: &C) {
        let now = clock.now();
        // Reset election timer using the Clock abstraction's randomised timeout,
        // proving the leader is alive and preventing spurious elections.
        state.election_deadline = now + clock.random_election_timeout();

        // Reset fetch deadline for the next periodic fetch.
        state.fetch_deadline = now + self.fetch_interval;
    }

    /// Process a `FetchResponse` received from the leader.
    ///
    /// Implements the follower-side Fetch response handling per architecture §4.1:
    /// 1. Validate response leader/term.
    /// 2. Reset election and fetch timers.
    /// 3. If `snapshot_id` is present, delegate to SnapshotCoordinator.
    /// 4. If `diverging_epoch` is present, truncate local log and re-fetch.
    /// 5. Otherwise, append received entries, advance HW, and run three-phase
    ///    commit notification (all synchronous, before IoActions are produced).
    ///
    /// Processing order per architecture: (1) mutate NodeState, (2) invoke
    /// application callbacks synchronously, (3) collect IoActions.
    pub fn handle_fetch_response<S: StateMachine, L: Listener, SC: SnapshotCoordinator, C: Clock>(
        &self,
        state: &mut NodeState,
        response: &FetchResponse,
        state_machine: &mut S,
        listener: &mut L,
        completion_queue: &mut DeferredCompletionQueue,
        snapshot_coordinator: &mut SC,
        clock: &C,
    ) -> Result<FetchResponseResult> {
        let mut io_batch = IoActionBatch::new();

        // Step 0: Validate response and update term/leader state if needed.
        // Returns deferred quorum state to persist AFTER callbacks (§4.1).
        let deferred_quorum_state = self.validate_and_update_term(state, response)?;

        // Validate mutually-exclusive response fields (after term/role validation).
        if response.snapshot_id.is_some() && response.diverging_epoch.is_some() {
            return Err(XraftError::InvalidResponse(
                "response contains both snapshot_id and diverging_epoch".into(),
            ));
        }

        // Always reset timers on any valid fetch response.
        self.reset_timers(state, clock);

        // Handle snapshot requirement — delegate to SnapshotCoordinator.
        if let Some(ref sid) = response.snapshot_id {
            // Guard: do not start a new transfer if one is already in progress.
            if snapshot_coordinator.is_transfer_in_progress() {
                // Persist quorum state if needed, then return without starting
                // a duplicate transfer.
                if let Some(qs) = deferred_quorum_state {
                    io_batch.push(IoAction::PersistQuorumState(qs));
                }
                return Ok(FetchResponseResult {
                    io_batch,
                    snapshot_transfer_initiated: false,
                });
            }
            let leader_id = state.leader_id.unwrap_or(response.leader_id);
            let snapshot_batch =
                snapshot_coordinator.begin_snapshot_transfer(leader_id, sid, state);
            for action in snapshot_batch.actions {
                io_batch.push(action);
            }
            // Persist quorum state AFTER snapshot delegation (no callbacks here).
            if let Some(qs) = deferred_quorum_state {
                io_batch.push(IoAction::PersistQuorumState(qs));
            }
            return Ok(FetchResponseResult {
                io_batch,
                snapshot_transfer_initiated: true,
            });
        }

        // Handle log divergence — truncate and prepare for re-fetch.
        if let Some(ref diverging) = response.diverging_epoch {
            // Guard: if end_offset exceeds our log end, it's nonsensical —
            // we can't truncate to a point beyond what we have. Clamp to
            // log_end_offset so we don't create holes. This is a no-op
            // truncation but we still re-fetch from the correct offset.
            let truncation_point = std::cmp::min(diverging.end_offset, state.log_end_offset);

            // Safety: HW must NEVER decrease. If the truncation point is below
            // HW, the committed prefix has diverged — this is a fatal protocol
            // violation (architecture §5.10 never-decrease-HW invariant).
            if truncation_point < state.high_watermark {
                return Err(XraftError::InvalidResponse(format!(
                    "divergence truncation point {} is below committed high_watermark {} \
                     — cannot un-commit entries (safety violation)",
                    truncation_point, state.high_watermark
                )));
            }

            // Step 1: Mutate state — truncate in-memory log.
            state.truncate_suffix(truncation_point);

            // Clear pending membership change if it was beyond the
            // truncation point — those entries no longer exist.
            if let Some(ref pending) = state.pending_membership_change {
                if pending.offset >= truncation_point {
                    state.pending_membership_change = None;
                }
            }

            // Remove any leader-epoch checkpoint entries beyond the
            // truncation point — those epochs are from truncated entries.
            let to_remove: Vec<Term> = state
                .leader_epoch_checkpoint
                .iter()
                .filter(|(_, &offset)| offset >= truncation_point)
                .map(|(&term, _)| term)
                .collect();
            for term in to_remove {
                state.leader_epoch_checkpoint.remove(&term);
            }

            // Fail pending completions for truncated offsets — those entries
            // no longer exist and their completions can never fire.
            completion_queue.fail_at_or_above(truncation_point);

            // Step 3: Collect IoActions — truncate durable log + re-fetch.
            io_batch.push(IoAction::TruncateSuffix(truncation_point));
            if let Some((leader_id, envelope)) = self.build_fetch_request(state) {
                io_batch.push(IoAction::SendRpc(leader_id, envelope));
            }
            // Persist quorum state AFTER IoActions for divergence (no callbacks).
            if let Some(qs) = deferred_quorum_state {
                io_batch.push(IoAction::PersistQuorumState(qs));
            }

            return Ok(FetchResponseResult {
                io_batch,
                snapshot_transfer_initiated: false,
            });
        }

        // ── Normal path ──
        // Step 1: Mutate NodeState — append entries and advance HW.
        let has_entries = !response.entries.is_empty();

        if has_entries {
            // Validate entry offsets before appending (avoid panic).
            if let Some(first) = response.entries.first() {
                if first.offset != state.log_end_offset {
                    return Err(XraftError::InvalidResponse(format!(
                        "first entry offset {} does not match log_end_offset {}",
                        first.offset, state.log_end_offset
                    )));
                }
            }
            // Validate entries are contiguous.
            for window in response.entries.windows(2) {
                if window[1].offset != window[0].offset + 1 {
                    return Err(XraftError::InvalidResponse(format!(
                        "non-contiguous entries: offset {} followed by {}",
                        window[0].offset, window[1].offset
                    )));
                }
            }
            state.append_entries(&response.entries);
        }

        // Advance high watermark: min(leader_HW, local_log_end_offset)
        // per architecture §5.10 rule 4. HW must NEVER decrease.
        let new_hw = std::cmp::min(response.high_watermark, state.log_end_offset);
        let old_hw = state.high_watermark;

        if new_hw > old_hw {
            state.high_watermark = new_hw;
        }

        // Step 2: Invoke application callbacks synchronously (§4.1 three-phase
        // commit notification). This MUST happen before IoActions are produced.
        if new_hw > old_hw {
            self.execute_commit_notification(
                state,
                old_hw,
                new_hw,
                state_machine,
                listener,
                completion_queue,
            )?;
        }

        // Step 3: Collect IoActions — produced AFTER callbacks per architecture.
        // PersistQuorumState comes last — after callbacks have run.
        if has_entries {
            io_batch.push(IoAction::AppendLog(response.entries.clone()));
        }
        if let Some(qs) = deferred_quorum_state {
            io_batch.push(IoAction::PersistQuorumState(qs));
        }

        Ok(FetchResponseResult {
            io_batch,
            snapshot_transfer_initiated: false,
        })
    }

    /// Execute the three-phase commit notification for newly committed entries.
    ///
    /// Called when HW advances from `old_hw` to `new_hw`. Processes entries
    /// in the range [old_hw, new_hw) in fixed order per architecture §4.1:
    ///   1. StateMachine::apply for each Command entry; process VotersRecord
    ///      and LeaderChangeMessage internally for bookkeeping
    ///   2. Listener::handle_commit with batch of AppRecord values
    ///   3. DeferredCompletionQueue::complete for all entries < new HW
    ///
    /// If StateMachine::apply returns Err, the node halts (crash-stop) per §6.3.
    /// The listener is notified via begin_shutdown() before the error propagates.
    fn execute_commit_notification<S: StateMachine, L: Listener>(
        &self,
        state: &mut NodeState,
        old_hw: u64,
        new_hw: u64,
        state_machine: &mut S,
        listener: &mut L,
        completion_queue: &mut DeferredCompletionQueue,
    ) -> Result<()> {
        let committed_entries = state.entries_in_range(old_hw, new_hw);

        // Phase 1: Apply Command entries to state machine and process control
        // records internally for bookkeeping.
        for entry in &committed_entries {
            match entry.entry_type {
                EntryType::Command => {
                    let record = entry.as_app_record();
                    // If apply returns Err, halt node (crash-stop) — committed
                    // entries cannot be skipped (architecture §4.1, §6.3).
                    // Notify listener before halting.
                    if let Err(e) = state_machine.apply(entry.offset, &record) {
                        listener.begin_shutdown();
                        return Err(XraftError::StateMachineApplyError(format!(
                            "apply failed at offset {}: {} — node halting (crash-stop)",
                            entry.offset, e
                        )));
                    }
                }
                EntryType::VotersRecord => {
                    // Deserialize and validate VotersRecord — surface errors
                    // instead of silently ignoring malformed records.
                    let record: VotersRecord =
                        bincode::deserialize::<VotersRecord>(&entry.payload).map_err(|e| {
                            XraftError::InvalidResponse(format!(
                                "failed to deserialize VotersRecord at offset {}: {}",
                                entry.offset, e
                            ))
                        })?;
                    state.voter_set = record.voters;
                    // Clear pending membership change if it matches this offset.
                    if let Some(ref pending) = state.pending_membership_change {
                        if pending.offset <= entry.offset {
                            state.pending_membership_change = None;
                        }
                    }
                }
                EntryType::LeaderChangeMessage => {
                    // Record the (term, start_offset) pair in the leader-epoch
                    // checkpoint per architecture §3.2, lines 506-513.
                    // This is purely internal bookkeeping — no external callback.
                    state
                        .leader_epoch_checkpoint
                        .insert(entry.term, entry.offset);
                }
            }
        }

        // Phase 2: Notify listener with batch of committed AppRecords.
        // Always called once when HW advances, even if the batch contains
        // only control records (empty app-record batch). Per architecture §4.1,
        // the listener phase runs once per HW advancement.
        let app_records: Vec<(u64, AppRecord)> = committed_entries
            .iter()
            .filter(|e| e.entry_type == EntryType::Command)
            .map(|e| (e.offset, e.as_app_record()))
            .collect();

        listener.handle_commit(&app_records);

        // Phase 3: Complete deferred futures for committed offsets.
        completion_queue.complete(new_hw);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_record::{AppRecord, AppSnapshot};
    use crate::config::RaftConfig;
    use crate::deferred_completion::DeferredCompletionQueue;
    use crate::error::Result;
    use crate::log_entry::LogEntry;
    use crate::node_state::{NodeState, Role};
    use crate::rpc::{DivergingEpoch, FetchResponse, SnapshotId};
    use crate::snapshot::SnapshotReader;
    use crate::types::{ClusterId, NodeId, Term};
    use crate::voter::{VoterInfo, VotersRecord};
    use bytes::Bytes;
    use std::net::SocketAddr;

    /// Minimal state machine for testing.
    struct TestStateMachine {
        applied: Vec<(u64, AppRecord)>,
        fail_at_offset: Option<u64>,
    }

    impl TestStateMachine {
        fn new() -> Self {
            TestStateMachine {
                applied: Vec::new(),
                fail_at_offset: None,
            }
        }

        fn failing_at(offset: u64) -> Self {
            TestStateMachine {
                applied: Vec::new(),
                fail_at_offset: Some(offset),
            }
        }
    }

    impl StateMachine for TestStateMachine {
        fn apply(&mut self, offset: u64, record: &AppRecord) -> Result<()> {
            if self.fail_at_offset == Some(offset) {
                return Err(XraftError::StateMachineApplyError(format!(
                    "intentional failure at offset {offset}"
                )));
            }
            self.applied.push((offset, record.clone()));
            Ok(())
        }

        fn snapshot(&self) -> Result<AppSnapshot> {
            Ok(AppSnapshot {
                data: Vec::new(),
            })
        }

        fn restore(&mut self, _snapshot: AppSnapshot) -> Result<()> {
            Ok(())
        }
    }

    /// Minimal listener for testing.
    struct TestListener {
        committed: Vec<Vec<(u64, AppRecord)>>,
        shutdown_called: bool,
        leader_changes: Vec<(NodeId, Term)>,
    }

    impl TestListener {
        fn new() -> Self {
            TestListener {
                committed: Vec::new(),
                shutdown_called: false,
                leader_changes: Vec::new(),
            }
        }
    }

    impl Listener for TestListener {
        fn handle_commit(&mut self, batch: &[(u64, AppRecord)]) {
            self.committed.push(batch.to_vec());
        }

        fn handle_load_snapshot(&mut self, _reader: SnapshotReader) {}

        fn handle_leader_change(&mut self, leader_id: NodeId, term: Term) {
            self.leader_changes.push((leader_id, term));
        }

        fn begin_shutdown(&mut self) {
            self.shutdown_called = true;
        }
    }

    /// Test snapshot coordinator that tracks calls.
    struct TestSnapshotCoordinator {
        transfers_started: Vec<SnapshotId>,
        in_progress: bool,
    }

    impl TestSnapshotCoordinator {
        fn new() -> Self {
            Self {
                transfers_started: Vec::new(),
                in_progress: false,
            }
        }
    }

    impl SnapshotCoordinator for TestSnapshotCoordinator {
        fn begin_snapshot_transfer(
            &mut self,
            leader_id: NodeId,
            snapshot_id: &SnapshotId,
            state: &NodeState,
        ) -> IoActionBatch {
            self.transfers_started.push(snapshot_id.clone());
            self.in_progress = true;
            // Delegate to DefaultSnapshotCoordinator behavior for IoActions.
            let mut batch = IoActionBatch::new();
            let request = crate::rpc::FetchSnapshotRequest {
                snapshot_id: snapshot_id.clone(),
                position: 0,
                max_bytes: 1024 * 1024,
            };
            let envelope = RpcEnvelope {
                cluster_id: state.cluster_id,
                leader_epoch: state.current_term,
                source: state.node_id,
                payload: RpcPayload::FetchSnapshotRequest(request),
            };
            batch.push(IoAction::SendRpc(leader_id, envelope));
            batch
        }

        fn is_transfer_in_progress(&self) -> bool {
            self.in_progress
        }
    }

    use async_trait::async_trait;

    /// Deterministic clock for testing.
    struct TestClock {
        instant: Instant,
        election_timeout: Duration,
    }

    impl TestClock {
        fn new() -> Self {
            Self {
                instant: Instant::now(),
                election_timeout: Duration::from_millis(200),
            }
        }

        fn at(instant: Instant) -> Self {
            Self {
                instant,
                election_timeout: Duration::from_millis(200),
            }
        }
    }

    #[async_trait]
    impl Clock for TestClock {
        fn now(&self) -> Instant {
            self.instant
        }
        async fn sleep_until(&self, _deadline: Instant) {}
        fn random_election_timeout(&self) -> Duration {
            self.election_timeout
        }
    }

    fn make_entry(offset: u64, term: Term) -> LogEntry {
        LogEntry {
            offset,
            term,
            entry_type: EntryType::Command,
            payload: Bytes::from(format!("data-{offset}")),
        }
    }

    fn make_entries(start: u64, end: u64, term: Term) -> Vec<LogEntry> {
        (start..end).map(|o| make_entry(o, term)).collect()
    }

    fn make_follower_state(node_id: u64, leader_id: u64) -> NodeState {
        let mut state = NodeState::new(NodeId(node_id), ClusterId::default());
        state.role = Role::Follower;
        state.leader_id = Some(NodeId(leader_id));
        state.current_term = Term(1);
        state
    }

    fn make_fetch_response(
        entries: Vec<LogEntry>,
        high_watermark: u64,
    ) -> FetchResponse {
        FetchResponse {
            leader_id: NodeId(1),
            leader_epoch: Term(1),
            high_watermark,
            log_start_offset: 0,
            entries,
            diverging_epoch: None,
            snapshot_id: None,
        }
    }

    fn clock() -> TestClock {
        TestClock::new()
    }

    // ── Scenario: Normal replication ──
    // Given leader N1 with entries 0–9,
    // When follower N2 sends Fetch(offset=0),
    // Then N2 receives entries 0–9 and appends them locally.
    #[test]
    fn test_normal_replication() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        // Follower starts with empty log.
        assert_eq!(state.log_end_offset, 0);

        // Simulate receiving entries 0–9 from leader.
        let entries = make_entries(0, 10, Term(1));
        let response = make_fetch_response(entries.clone(), 10);

        let result = mgr
            .handle_fetch_response(
                &mut state, &response, &mut sm, &mut listener, &mut queue,
                &mut sc, &clock(),
            )
            .expect("should succeed");

        // Entries should be appended locally.
        assert_eq!(state.log_end_offset, 10);
        assert_eq!(state.log.len(), 10);
        assert_eq!(state.high_watermark, 10);
        assert!(!result.snapshot_transfer_initiated);

        // State machine should have applied all 10 entries.
        assert_eq!(sm.applied.len(), 10);

        // Listener should have been notified.
        assert_eq!(listener.committed.len(), 1);
        assert_eq!(listener.committed[0].len(), 10);

        // I/O batch should contain AppendLog.
        assert!(!result.io_batch.is_empty());

        // Verify IoAction ordering: AppendLog must come AFTER callbacks
        // (callbacks already executed above before io_batch was returned).
        assert!(result
            .io_batch
            .actions
            .iter()
            .any(|a| matches!(a, IoAction::AppendLog(_))));
    }

    // ── Scenario: Incremental fetch ──
    // Given N2 has entries 0–4,
    // When N2 sends Fetch(offset=5) and leader has entries 0–9,
    // Then N2 receives entries 5–9 only.
    #[test]
    fn test_incremental_fetch() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        // Pre-populate follower with entries 0–4 (already committed).
        let initial_entries = make_entries(0, 5, Term(1));
        state.append_entries(&initial_entries);
        state.high_watermark = 5;

        assert_eq!(state.log_end_offset, 5);

        // Verify the fetch request would have offset=5.
        let (leader_id, envelope) = mgr.build_fetch_request(&state).unwrap();
        assert_eq!(leader_id, NodeId(1));
        if let RpcPayload::FetchRequest(req) = &envelope.payload {
            assert_eq!(req.fetch_offset, 5);
        } else {
            panic!("Expected FetchRequest payload");
        }

        // Simulate receiving entries 5–9 from leader.
        let new_entries = make_entries(5, 10, Term(1));
        let response = make_fetch_response(new_entries.clone(), 10);

        let _result = mgr
            .handle_fetch_response(
                &mut state, &response, &mut sm, &mut listener, &mut queue,
                &mut sc, &clock(),
            )
            .expect("should succeed");

        // Should now have entries 0–9.
        assert_eq!(state.log_end_offset, 10);
        assert_eq!(state.log.len(), 10);
        assert_eq!(state.high_watermark, 10);

        // Only entries 5–9 should be applied (new commits).
        assert_eq!(sm.applied.len(), 5);
        assert_eq!(sm.applied[0].0, 5);
        assert_eq!(sm.applied[4].0, 9);
    }

    // ── Scenario: Election timer reset ──
    // Given follower N2,
    // When it receives a Fetch response (even empty),
    // Then its election timer is reset (election_deadline updated).
    #[test]
    fn test_election_timer_reset_on_empty_response() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        // Set election deadline in the past to prove it gets reset.
        let old_deadline = Instant::now() - Duration::from_secs(10);
        state.election_deadline = old_deadline;

        // Empty response (no entries, HW=0).
        let response = make_fetch_response(vec![], 0);
        let current = Instant::now();

        let _result = mgr
            .handle_fetch_response(
                &mut state, &response, &mut sm, &mut listener, &mut queue,
                &mut sc, &TestClock::at(current),
            )
            .expect("should succeed");

        // Election deadline should have been reset to the future.
        assert!(state.election_deadline > old_deadline);
        assert!(state.election_deadline >= current + config.election_timeout_min);

        // No entries appended, no state machine calls.
        assert_eq!(state.log_end_offset, 0);
        assert!(sm.applied.is_empty());
        assert!(listener.committed.is_empty());
    }

    // ── Scenario: Fetch deadline updated on response ──
    #[test]
    fn test_fetch_deadline_updated_on_response() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        let _old_fetch_deadline = state.fetch_deadline;
        let current = Instant::now() + Duration::from_secs(1);

        let response = make_fetch_response(vec![], 0);
        let _result = mgr
            .handle_fetch_response(
                &mut state, &response, &mut sm, &mut listener, &mut queue,
                &mut sc, &TestClock::at(current),
            )
            .expect("should succeed");

        // Fetch deadline should be advanced.
        assert_eq!(state.fetch_deadline, current + config.fetch_interval);
    }

    // ── Scenario: on_fetch_tick updates fetch_deadline ──
    #[test]
    fn test_on_fetch_tick_updates_deadline() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        let current = Instant::now();

        let _batch = mgr.on_fetch_tick(&mut state, current);
        assert_eq!(state.fetch_deadline, current + config.fetch_interval);
    }

    // ── Scenario: Divergence truncation ──
    // Given N2 has entries diverging at offset 5,
    // When Fetch response includes DivergingEpoch{epoch=2, end_offset=5},
    // Then N2 truncates its log to offset 5 and re-fetches.
    #[test]
    fn test_divergence_truncation() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        // Pre-populate follower with entries 0–7 (some diverging after offset 5).
        // HW is at 5 — only entries 0–4 are committed; 5–7 are uncommitted.
        let initial_entries = make_entries(0, 8, Term(1));
        state.append_entries(&initial_entries);
        state.high_watermark = 5; // committed up to 5
        // Bump current_term so the response from term 2 is accepted.
        state.current_term = Term(2);
        assert_eq!(state.log_end_offset, 8);

        // Park completions for offsets 5, 6, 7 (uncommitted entries that will
        // be truncated — their completions must be failed/cleaned up).
        let (tx5, mut rx5) = tokio::sync::oneshot::channel();
        let (tx6, mut rx6) = tokio::sync::oneshot::channel();
        let (tx7, mut rx7) = tokio::sync::oneshot::channel();
        queue.park(5, tx5);
        queue.park(6, tx6);
        queue.park(7, tx7);

        // Response indicates divergence at epoch 2, truncate to offset 5.
        // end_offset=5 is == HW, so this is safe (only uncommitted entries truncated).
        let response = FetchResponse {
            leader_id: NodeId(1),
            leader_epoch: Term(2),
            high_watermark: 5,
            log_start_offset: 0,
            entries: vec![],
            diverging_epoch: Some(DivergingEpoch {
                epoch: Term(2),
                end_offset: 5,
            }),
            snapshot_id: None,
        };

        let result = mgr
            .handle_fetch_response(
                &mut state, &response, &mut sm, &mut listener, &mut queue,
                &mut sc, &clock(),
            )
            .expect("should succeed");

        // Log should be truncated to offset 5.
        assert_eq!(state.log_end_offset, 5);
        assert_eq!(state.log.len(), 5);

        // HW must NOT decrease — it stays at 5.
        assert_eq!(state.high_watermark, 5);

        // Pending completions for truncated offsets should be failed.
        assert!(rx5.try_recv().is_err(), "offset 5 completion should be failed");
        assert!(rx6.try_recv().is_err(), "offset 6 completion should be failed");
        assert!(rx7.try_recv().is_err(), "offset 7 completion should be failed");
        assert_eq!(queue.len(), 0, "all truncated completions should be cleaned up");

        // I/O batch should contain TruncateSuffix and a re-fetch SendRpc.
        let actions = &result.io_batch.actions;
        assert!(actions
            .iter()
            .any(|a| matches!(a, IoAction::TruncateSuffix(5))));
        assert!(actions
            .iter()
            .any(|a| matches!(a, IoAction::SendRpc(_, _))));

        // No state machine applies during truncation.
        assert!(sm.applied.is_empty());
    }

    // ── Leader-not-known state ──
    // If no leader is known, do not send Fetch (wait for election).
    #[test]
    fn test_no_fetch_without_leader() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = NodeState::new(NodeId(2), ClusterId::default());
        state.role = Role::Follower;
        state.leader_id = None; // No leader known.

        // Should not build a fetch request.
        assert!(mgr.build_fetch_request(&state).is_none());

        // on_fetch_tick should produce an empty batch.
        let batch = mgr.on_fetch_tick(&mut state, Instant::now());
        assert!(batch.is_empty());
    }

    // ── Non-follower should not send Fetch ──
    #[test]
    fn test_no_fetch_from_non_follower() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = NodeState::new(NodeId(1), ClusterId::default());
        state.role = Role::Leader;
        state.leader_id = Some(NodeId(1));

        assert!(mgr.build_fetch_request(&state).is_none());
    }

    // ── Snapshot ID in response delegates to SnapshotCoordinator ──
    #[test]
    fn test_snapshot_id_delegates_to_coordinator() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        let response = FetchResponse {
            leader_id: NodeId(1),
            leader_epoch: Term(1),
            high_watermark: 100,
            log_start_offset: 50,
            entries: vec![],
            diverging_epoch: None,
            snapshot_id: Some(SnapshotId {
                end_offset: 50,
                epoch: Term(1),
            }),
        };

        let result = mgr
            .handle_fetch_response(
                &mut state, &response, &mut sm, &mut listener, &mut queue,
                &mut sc, &clock(),
            )
            .expect("should succeed");

        assert!(result.snapshot_transfer_initiated);
        // Coordinator should have been called.
        assert_eq!(sc.transfers_started.len(), 1);
        assert_eq!(sc.transfers_started[0].end_offset, 50);
        assert_eq!(sc.transfers_started[0].epoch, Term(1));
        assert!(sc.is_transfer_in_progress());

        // I/O batch should contain a FetchSnapshotRequest SendRpc.
        assert!(result
            .io_batch
            .actions
            .iter()
            .any(|a| matches!(a, IoAction::SendRpc(_, _))));
    }

    // ── HW advancement with mixed entry types ──
    // Control records (LeaderChangeMessage, VotersRecord) are NOT applied
    // to state machine, and NOT included in listener commit batch.
    // VotersRecord IS processed internally for bookkeeping.
    #[test]
    fn test_commit_processes_control_records_internally() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        let voters_record = VotersRecord {
            version: 1,
            voters: vec![
                VoterInfo {
                    node_id: NodeId(1),
                    endpoint: "127.0.0.1:5001".parse::<SocketAddr>().unwrap(),
                },
                VoterInfo {
                    node_id: NodeId(2),
                    endpoint: "127.0.0.1:5002".parse::<SocketAddr>().unwrap(),
                },
                VoterInfo {
                    node_id: NodeId(3),
                    endpoint: "127.0.0.1:5003".parse::<SocketAddr>().unwrap(),
                },
            ],
        };

        let entries = vec![
            make_entry(0, Term(1)),                                    // Command
            LogEntry::leader_change(1, Term(1)),                       // Control
            make_entry(2, Term(1)),                                    // Command
            LogEntry::voters_record(3, Term(1), &voters_record),       // Control
        ];

        let response = make_fetch_response(entries, 4);

        mgr.handle_fetch_response(
            &mut state, &response, &mut sm, &mut listener, &mut queue,
            &mut sc, &clock(),
        )
        .expect("should succeed");

        // Only 2 Command entries should be applied to state machine.
        assert_eq!(sm.applied.len(), 2);
        assert_eq!(sm.applied[0].0, 0);
        assert_eq!(sm.applied[1].0, 2);

        // Listener should get 2 app records (not the control records).
        assert_eq!(listener.committed.len(), 1);
        assert_eq!(listener.committed[0].len(), 2);

        // VotersRecord should have been processed internally — voter_set updated.
        assert_eq!(state.voter_set.len(), 3);
        assert_eq!(state.voter_set[0].node_id, NodeId(1));
        assert_eq!(state.voter_set[1].node_id, NodeId(2));
        assert_eq!(state.voter_set[2].node_id, NodeId(3));
    }

    // ── HW is min(leader_HW, local_log_end_offset) ──
    #[test]
    fn test_hw_capped_at_local_log_end() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        // Receive 5 entries but leader claims HW=100.
        let entries = make_entries(0, 5, Term(1));
        let response = FetchResponse {
            leader_id: NodeId(1),
            leader_epoch: Term(1),
            high_watermark: 100, // Higher than our log end.
            log_start_offset: 0,
            entries,
            diverging_epoch: None,
            snapshot_id: None,
        };

        mgr.handle_fetch_response(
            &mut state, &response, &mut sm, &mut listener, &mut queue,
            &mut sc, &clock(),
        )
        .expect("should succeed");

        // HW should be capped at log_end_offset (5), not leader's HW (100).
        assert_eq!(state.high_watermark, 5);
        assert_eq!(state.log_end_offset, 5);
    }

    // ── Deferred completions fire on HW advance ──
    #[test]
    fn test_deferred_completions_fire() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        // Park completions for offsets 0, 1, 2.
        let (tx0, mut rx0) = tokio::sync::oneshot::channel();
        let (tx1, mut rx1) = tokio::sync::oneshot::channel();
        let (tx2, mut rx2) = tokio::sync::oneshot::channel();
        queue.park(0, tx0);
        queue.park(1, tx1);
        queue.park(2, tx2);

        // Receive entries 0–2 with HW=2 (offsets 0,1 are committed).
        let entries = make_entries(0, 3, Term(1));
        let response = make_fetch_response(entries, 2);

        mgr.handle_fetch_response(
            &mut state, &response, &mut sm, &mut listener, &mut queue,
            &mut sc, &clock(),
        )
        .expect("should succeed");

        // Offsets 0 and 1 should be completed (< HW=2).
        assert_eq!(rx0.try_recv().unwrap(), 0);
        assert_eq!(rx1.try_recv().unwrap(), 1);

        // Offset 2 should still be pending (not < HW=2).
        assert!(rx2.try_recv().is_err());
        assert_eq!(queue.len(), 1);
    }

    // ── StateMachine::apply error halts node (crash-stop) ──
    #[test]
    fn test_state_machine_apply_error_halts() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        let mut sm = TestStateMachine::failing_at(2); // Fail at offset 2.
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        let entries = make_entries(0, 5, Term(1));
        let response = make_fetch_response(entries, 5);

        let result = mgr.handle_fetch_response(
            &mut state, &response, &mut sm, &mut listener, &mut queue,
            &mut sc, &clock(),
        );

        // Should return an error (crash-stop semantics).
        assert!(result.is_err());
        match result.unwrap_err() {
            XraftError::StateMachineApplyError(msg) => {
                assert!(msg.contains("offset 2"), "error should mention offset: {msg}");
                assert!(msg.contains("halting"), "error should mention halting: {msg}");
            }
            other => panic!("expected StateMachineApplyError, got: {other}"),
        }

        // Listener::begin_shutdown must have been called before error propagation.
        assert!(listener.shutdown_called, "begin_shutdown should be called on apply error");

        // Only offsets 0 and 1 should have been applied before the failure.
        assert_eq!(sm.applied.len(), 2);
        assert_eq!(sm.applied[0].0, 0);
        assert_eq!(sm.applied[1].0, 1);
    }

    // ── Response validation: stale term rejected ──
    #[test]
    fn test_stale_term_rejected() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        state.current_term = Term(5);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        let response = FetchResponse {
            leader_id: NodeId(1),
            leader_epoch: Term(3), // Stale term.
            high_watermark: 0,
            log_start_offset: 0,
            entries: vec![],
            diverging_epoch: None,
            snapshot_id: None,
        };

        let result = mgr.handle_fetch_response(
            &mut state, &response, &mut sm, &mut listener, &mut queue,
            &mut sc, &clock(),
        );

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            XraftError::InvalidResponse(_)
        ));
    }

    // ── Response validation: wrong leader rejected ──
    #[test]
    fn test_wrong_leader_rejected() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1); // Known leader is N1.
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        let response = FetchResponse {
            leader_id: NodeId(3), // Wrong leader.
            leader_epoch: Term(1),
            high_watermark: 0,
            log_start_offset: 0,
            entries: vec![],
            diverging_epoch: None,
            snapshot_id: None,
        };

        let result = mgr.handle_fetch_response(
            &mut state, &response, &mut sm, &mut listener, &mut queue,
            &mut sc, &clock(),
        );

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            XraftError::InvalidResponse(_)
        ));
    }

    // ── Invalid entry offset does not panic ──
    #[test]
    fn test_invalid_entry_offset_returns_error() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        // Entry starts at offset 5 but follower's log_end_offset is 0.
        let entries = make_entries(5, 8, Term(1));
        let response = make_fetch_response(entries, 8);

        let result = mgr.handle_fetch_response(
            &mut state, &response, &mut sm, &mut listener, &mut queue,
            &mut sc, &clock(),
        );

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            XraftError::InvalidResponse(_)
        ));

        // State should be unchanged.
        assert_eq!(state.log_end_offset, 0);
    }

    // ── Non-contiguous entries return error ──
    #[test]
    fn test_non_contiguous_entries_returns_error() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        // Gap: offset 0, then offset 2 (missing 1).
        let entries = vec![make_entry(0, Term(1)), make_entry(2, Term(1))];
        let response = make_fetch_response(entries, 3);

        let result = mgr.handle_fetch_response(
            &mut state, &response, &mut sm, &mut listener, &mut queue,
            &mut sc, &clock(),
        );

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            XraftError::InvalidResponse(_)
        ));
    }

    // ── IoAction::AppendLog is produced AFTER callbacks ──
    // Verify ordering: state mutation + callbacks happen before IoActions.
    #[test]
    fn test_append_log_io_action_after_callbacks() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        let entries = make_entries(0, 3, Term(1));
        let response = make_fetch_response(entries, 3);

        let result = mgr
            .handle_fetch_response(
                &mut state, &response, &mut sm, &mut listener, &mut queue,
                &mut sc, &clock(),
            )
            .expect("should succeed");

        // Callbacks were invoked (state machine applied, listener notified)
        // BEFORE the FetchResponseResult is returned, which means before
        // any IoAction could be dispatched.
        assert_eq!(sm.applied.len(), 3);
        assert_eq!(listener.committed.len(), 1);

        // IoAction::AppendLog is in the batch (produced after callbacks).
        assert!(result
            .io_batch
            .actions
            .iter()
            .any(|a| matches!(a, IoAction::AppendLog(_))));
    }

    // ── poll_fetch integrates with EventLoop/Clock ──
    #[test]
    fn test_poll_fetch_fires_when_deadline_elapsed() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);

        // Set fetch_deadline in the past.
        let past = Instant::now() - Duration::from_secs(1);
        state.fetch_deadline = past;

        let current = Instant::now();
        let result = mgr.poll_fetch(&mut state, current);
        assert!(result.is_some(), "poll_fetch should fire when deadline elapsed");
        let batch = result.unwrap();
        assert!(!batch.is_empty(), "should produce a SendRpc action");

        // Fetch deadline should have advanced.
        assert_eq!(state.fetch_deadline, current + config.fetch_interval);
    }

    #[test]
    fn test_poll_fetch_does_not_fire_before_deadline() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);

        // Set fetch_deadline in the future.
        let future = Instant::now() + Duration::from_secs(10);
        state.fetch_deadline = future;

        let result = mgr.poll_fetch(&mut state, Instant::now());
        assert!(result.is_none(), "poll_fetch should not fire before deadline");
    }

    #[test]
    fn test_poll_fetch_skipped_for_non_follower() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        state.role = Role::Candidate;
        state.fetch_deadline = Instant::now() - Duration::from_secs(1);

        let result = mgr.poll_fetch(&mut state, Instant::now());
        assert!(result.is_none(), "non-follower should not poll fetch");
    }

    #[test]
    fn test_time_until_fetch() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);

        let now = Instant::now();
        state.fetch_deadline = now + Duration::from_millis(100);

        let dur = mgr.time_until_fetch(&state, now);
        assert!(dur <= Duration::from_millis(100));
        assert!(dur > Duration::ZERO);

        // When deadline is in the past, returns ZERO.
        state.fetch_deadline = now - Duration::from_secs(1);
        let dur = mgr.time_until_fetch(&state, now);
        assert_eq!(dur, Duration::ZERO);
    }

    // ── LeaderChangeMessage updates leader-epoch checkpoint ──
    #[test]
    fn test_leader_change_message_updates_epoch_checkpoint() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        // Entries: command at 0, LeaderChangeMessage at 1 (term 1),
        // command at 2, LeaderChangeMessage at 3 (term 2).
        let entries = vec![
            make_entry(0, Term(1)),
            LogEntry::leader_change(1, Term(1)),
            make_entry(2, Term(1)),
            LogEntry::leader_change(3, Term(2)),
        ];

        // Update state term to accept the response.
        state.current_term = Term(2);
        let response = FetchResponse {
            leader_id: NodeId(1),
            leader_epoch: Term(2),
            high_watermark: 4,
            log_start_offset: 0,
            entries,
            diverging_epoch: None,
            snapshot_id: None,
        };

        mgr.handle_fetch_response(
            &mut state, &response, &mut sm, &mut listener, &mut queue,
            &mut sc, &clock(),
        )
        .expect("should succeed");

        // Leader-epoch checkpoint should have two entries.
        assert_eq!(state.leader_epoch_checkpoint.len(), 2);
        assert_eq!(state.leader_epoch_checkpoint[&Term(1)], 1);
        assert_eq!(state.leader_epoch_checkpoint[&Term(2)], 3);
    }

    // ── VotersRecord deserialization error surfaces ──
    #[test]
    fn test_voters_record_deserialization_error_surfaces() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        // Craft a VotersRecord entry with garbage payload.
        let bad_voters_entry = LogEntry {
            offset: 0,
            term: Term(1),
            entry_type: EntryType::VotersRecord,
            payload: Bytes::from("not-valid-bincode"),
        };

        let response = make_fetch_response(vec![bad_voters_entry], 1);

        let result = mgr.handle_fetch_response(
            &mut state, &response, &mut sm, &mut listener, &mut queue,
            &mut sc, &clock(),
        );

        assert!(result.is_err());
        match result.unwrap_err() {
            XraftError::InvalidResponse(msg) => {
                assert!(
                    msg.contains("VotersRecord"),
                    "error should mention VotersRecord: {msg}"
                );
            }
            other => panic!("expected InvalidResponse, got: {other}"),
        }
    }

    // ── Divergence truncation clears pending state and fails completions ──
    #[test]
    fn test_divergence_truncation_clears_state_and_fails_completions() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        // Pre-populate follower with entries 0–7, HW=5 (entries 5-7 uncommitted).
        let initial_entries = make_entries(0, 8, Term(1));
        state.append_entries(&initial_entries);
        state.high_watermark = 5;
        state.current_term = Term(2);

        // Set pending membership change at offset 6 (beyond truncation point).
        state.pending_membership_change = Some(crate::node_state::PendingMembershipChange {
            offset: 6,
            voters: vec![],
        });

        // Set leader-epoch checkpoint entries.
        state.leader_epoch_checkpoint.insert(Term(1), 0);
        state.leader_epoch_checkpoint.insert(Term(2), 6);  // Beyond truncation

        // Park completions for uncommitted offsets.
        let (tx5, mut rx5) = tokio::sync::oneshot::channel();
        let (tx6, mut rx6) = tokio::sync::oneshot::channel();
        queue.park(5, tx5);
        queue.park(6, tx6);

        // Truncate to offset 5 (== HW, safe).
        let response = FetchResponse {
            leader_id: NodeId(1),
            leader_epoch: Term(2),
            high_watermark: 5,
            log_start_offset: 0,
            entries: vec![],
            diverging_epoch: Some(DivergingEpoch {
                epoch: Term(2),
                end_offset: 5,
            }),
            snapshot_id: None,
        };

        let _result = mgr
            .handle_fetch_response(
                &mut state, &response, &mut sm, &mut listener, &mut queue,
                &mut sc, &clock(),
            )
            .expect("should succeed");

        // Log truncated to offset 5.
        assert_eq!(state.log_end_offset, 5);

        // HW stays at 5 (never decreases).
        assert_eq!(state.high_watermark, 5);

        // Pending membership change at offset 6 should be cleared.
        assert!(state.pending_membership_change.is_none());

        // Leader-epoch checkpoint: entry at offset 6 (Term(2)) should be removed.
        assert_eq!(state.leader_epoch_checkpoint.len(), 1);
        assert!(state.leader_epoch_checkpoint.contains_key(&Term(1)));
        assert!(!state.leader_epoch_checkpoint.contains_key(&Term(2)));

        // Pending completions for offsets 5, 6 should be failed.
        assert!(rx5.try_recv().is_err());
        assert!(rx6.try_recv().is_err());
        assert_eq!(queue.len(), 0);
    }

    // ── Divergence below HW is a safety violation ──
    #[test]
    fn test_divergence_below_hw_is_safety_violation() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        // Entries 0–7, HW=7 (all committed).
        let initial_entries = make_entries(0, 8, Term(1));
        state.append_entries(&initial_entries);
        state.high_watermark = 7;
        state.current_term = Term(2);

        // Truncation at offset 5 is BELOW HW=7 — safety violation.
        let response = FetchResponse {
            leader_id: NodeId(1),
            leader_epoch: Term(2),
            high_watermark: 5,
            log_start_offset: 0,
            entries: vec![],
            diverging_epoch: Some(DivergingEpoch {
                epoch: Term(2),
                end_offset: 5,
            }),
            snapshot_id: None,
        };

        let result = mgr.handle_fetch_response(
            &mut state, &response, &mut sm, &mut listener, &mut queue,
            &mut sc, &clock(),
        );

        assert!(result.is_err());
        match result.unwrap_err() {
            XraftError::InvalidResponse(msg) => {
                assert!(msg.contains("safety violation"), "should mention safety: {msg}");
            }
            other => panic!("expected InvalidResponse, got: {other}"),
        }

        // State should be unchanged.
        assert_eq!(state.log_end_offset, 8);
        assert_eq!(state.high_watermark, 7);
    }

    // ── Listener::handle_commit called even with empty app-record batch ──
    #[test]
    fn test_handle_commit_called_for_control_records_only() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        // Only control records — no Command entries.
        let entries = vec![
            LogEntry::leader_change(0, Term(1)),
            LogEntry::leader_change(1, Term(1)),
        ];

        let response = make_fetch_response(entries, 2);

        mgr.handle_fetch_response(
            &mut state, &response, &mut sm, &mut listener, &mut queue,
            &mut sc, &clock(),
        )
        .expect("should succeed");

        // No state machine applies (no Command entries).
        assert!(sm.applied.is_empty());

        // Listener::handle_commit MUST still be called once (with empty batch)
        // per the three-phase notification — the listener phase runs once
        // when HW advances.
        assert_eq!(
            listener.committed.len(),
            1,
            "handle_commit must be called even with empty app-record batch"
        );
        assert!(
            listener.committed[0].is_empty(),
            "batch should be empty (only control records)"
        );
    }

    // ══════════════════════════════════════════════════════════════
    // Integrated follower behavior tests — simulate the full
    // EventLoop tick cycle (poll_fetch → receive response → process)
    // rather than testing ReplicationManager methods in isolation.
    // ══════════════════════════════════════════════════════════════

    /// Simulates a minimal EventLoop cycle for the follower:
    /// 1. poll_fetch fires a FetchRequest
    /// 2. Response comes back from leader
    /// 3. handle_fetch_response processes it
    /// Returns (IoActionBatch from poll, FetchResponseResult from response).
    fn run_follower_cycle(
        mgr: &ReplicationManager,
        state: &mut NodeState,
        response: &FetchResponse,
        sm: &mut TestStateMachine,
        listener: &mut TestListener,
        queue: &mut DeferredCompletionQueue,
        sc: &mut TestSnapshotCoordinator,
    ) -> (Option<IoActionBatch>, Result<FetchResponseResult>) {
        let clk = clock();
        let instant = clk.now();

        // Step 1: EventLoop checks fetch deadline (simulating Clock integration).
        let fetch_batch = mgr.poll_fetch(state, instant);

        // Step 2: Process response (simulating EventLoop dispatch).
        let result = mgr.handle_fetch_response(
            state, response, sm, listener, queue, sc, &clk,
        );

        (fetch_batch, result)
    }

    // ── Integrated: Full follower cycle from fetch to commit ──
    #[test]
    fn test_integrated_follower_full_cycle() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        // Set fetch_deadline in the past so poll_fetch fires.
        state.fetch_deadline = Instant::now() - Duration::from_secs(1);

        let entries = make_entries(0, 5, Term(1));
        let response = make_fetch_response(entries, 5);

        let (fetch_batch, result) =
            run_follower_cycle(&mgr, &mut state, &response, &mut sm, &mut listener, &mut queue, &mut sc);

        // poll_fetch should have produced a SendRpc.
        assert!(fetch_batch.is_some());
        let fb = fetch_batch.unwrap();
        assert!(fb.actions.iter().any(|a| matches!(a, IoAction::SendRpc(_, _))));

        // Response processing should succeed.
        let result = result.expect("should succeed");
        assert!(!result.snapshot_transfer_initiated);

        // Entries appended, HW advanced, SM applied, listener notified.
        assert_eq!(state.log_end_offset, 5);
        assert_eq!(state.high_watermark, 5);
        assert_eq!(sm.applied.len(), 5);
        assert_eq!(listener.committed.len(), 1);
        assert_eq!(listener.committed[0].len(), 5);

        // Fetch deadline advanced.
        assert!(state.fetch_deadline > Instant::now() - Duration::from_secs(2));
    }

    // ── Integrated: Follower divergence → truncation → re-fetch → catch up ──
    #[test]
    fn test_integrated_follower_divergence_recovery() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        // Phase 1: Follower has entries 0–7 in term 1.
        let initial = make_entries(0, 8, Term(1));
        state.append_entries(&initial);
        state.high_watermark = 5;
        state.current_term = Term(2);
        state.fetch_deadline = Instant::now() - Duration::from_secs(1);

        // Phase 2: Leader sends divergence — truncate to offset 5.
        let diverge_response = FetchResponse {
            leader_id: NodeId(1),
            leader_epoch: Term(2),
            high_watermark: 5,
            log_start_offset: 0,
            entries: vec![],
            diverging_epoch: Some(DivergingEpoch {
                epoch: Term(2),
                end_offset: 5,
            }),
            snapshot_id: None,
        };

        let (_, result) = run_follower_cycle(
            &mgr, &mut state, &diverge_response,
            &mut sm, &mut listener, &mut queue, &mut sc,
        );
        result.expect("truncation should succeed");
        assert_eq!(state.log_end_offset, 5);
        assert_eq!(state.high_watermark, 5);

        // Phase 3: Re-fetch brings new entries 5–9 from new term.
        state.fetch_deadline = Instant::now() - Duration::from_secs(1);
        let new_entries = make_entries(5, 10, Term(2));
        let catch_up_response = FetchResponse {
            leader_id: NodeId(1),
            leader_epoch: Term(2),
            high_watermark: 10,
            log_start_offset: 0,
            entries: new_entries,
            diverging_epoch: None,
            snapshot_id: None,
        };

        let (_, result) = run_follower_cycle(
            &mgr, &mut state, &catch_up_response,
            &mut sm, &mut listener, &mut queue, &mut sc,
        );
        result.expect("catch-up should succeed");

        assert_eq!(state.log_end_offset, 10);
        assert_eq!(state.high_watermark, 10);
        // Entries 5–9 committed via SM.
        assert_eq!(sm.applied.len(), 5);
    }

    // ── Integrated: Follower no-leader → poll_fetch returns None ──
    #[test]
    fn test_integrated_follower_no_leader_no_fetch() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = NodeState::new(NodeId(2), ClusterId::default());
        state.role = Role::Follower;
        state.leader_id = None;
        state.fetch_deadline = Instant::now() - Duration::from_secs(1);

        let result = mgr.poll_fetch(&mut state, Instant::now());
        // poll_fetch fires (deadline elapsed + follower) but produces empty batch.
        assert!(result.is_some());
        assert!(result.unwrap().is_empty(), "no SendRpc without known leader");
    }

    // ── Integrated: Follower snapshot delegation cycle ──
    #[test]
    fn test_integrated_follower_snapshot_cycle() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        state.fetch_deadline = Instant::now() - Duration::from_secs(1);

        let response = FetchResponse {
            leader_id: NodeId(1),
            leader_epoch: Term(1),
            high_watermark: 100,
            log_start_offset: 50,
            entries: vec![],
            diverging_epoch: None,
            snapshot_id: Some(SnapshotId {
                end_offset: 50,
                epoch: Term(1),
            }),
        };

        let (fetch_batch, result) = run_follower_cycle(
            &mgr, &mut state, &response,
            &mut sm, &mut listener, &mut queue, &mut sc,
        );

        // poll_fetch should have fired.
        assert!(fetch_batch.is_some());

        // Response should delegate to snapshot coordinator.
        let result = result.expect("should succeed");
        assert!(result.snapshot_transfer_initiated);
        assert_eq!(sc.transfers_started.len(), 1);

        // No SM applies or listener commits during snapshot delegation.
        assert!(sm.applied.is_empty());
        assert!(listener.committed.is_empty());
    }

    // ── Integrated: Three-phase commit order (apply → handle_commit → complete) ──
    #[test]
    fn test_integrated_three_phase_commit_order() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        // Park a completion.
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        queue.park(0, tx);

        state.fetch_deadline = Instant::now() - Duration::from_secs(1);
        let entries = make_entries(0, 3, Term(1));
        let response = make_fetch_response(entries, 3);

        let (_, result) = run_follower_cycle(
            &mgr, &mut state, &response,
            &mut sm, &mut listener, &mut queue, &mut sc,
        );
        result.expect("should succeed");

        // Phase 1: SM applied 3 entries.
        assert_eq!(sm.applied.len(), 3);

        // Phase 2: Listener notified once with 3 records.
        assert_eq!(listener.committed.len(), 1);
        assert_eq!(listener.committed[0].len(), 3);

        // Phase 3: Completion for offset 0 fired (0 < HW=3).
        assert_eq!(rx.try_recv().unwrap(), 0);
    }

    // ── Integrated: Apply error halts node with begin_shutdown ──
    #[test]
    fn test_integrated_apply_error_halts_node() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        let mut sm = TestStateMachine::failing_at(1);
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        state.fetch_deadline = Instant::now() - Duration::from_secs(1);
        let entries = make_entries(0, 3, Term(1));
        let response = make_fetch_response(entries, 3);

        let (_, result) = run_follower_cycle(
            &mgr, &mut state, &response,
            &mut sm, &mut listener, &mut queue, &mut sc,
        );

        // Error should propagate.
        assert!(result.is_err());

        // begin_shutdown called for crash-stop.
        assert!(listener.shutdown_called);

        // Only offset 0 applied before failure at offset 1.
        assert_eq!(sm.applied.len(), 1);
        assert_eq!(sm.applied[0].0, 0);
    }

    // ══════════════════════════════════════════════════════════════
    // Protocol-safety and term-update tests
    // ══════════════════════════════════════════════════════════════

    // ── Higher leader_epoch updates current_term, leader, and persists quorum state ──
    #[test]
    fn test_higher_leader_epoch_updates_term_and_persists() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        state.current_term = Term(1);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        // Response from leader with higher epoch (term 3).
        let response = FetchResponse {
            leader_id: NodeId(1),
            leader_epoch: Term(3),
            high_watermark: 0,
            log_start_offset: 0,
            entries: vec![],
            diverging_epoch: None,
            snapshot_id: None,
        };

        let result = mgr
            .handle_fetch_response(
                &mut state, &response, &mut sm, &mut listener, &mut queue,
                &mut sc, &clock(),
            )
            .expect("should succeed");

        // Term should be updated.
        assert_eq!(state.current_term, Term(3));
        // voted_for should be cleared.
        assert_eq!(state.voted_for, None);
        // leader_id should be set.
        assert_eq!(state.leader_id, Some(NodeId(1)));
        // Role should be Follower.
        assert_eq!(state.role, Role::Follower);

        // PersistQuorumState IoAction should be produced.
        assert!(result.io_batch.actions.iter().any(|a| matches!(
            a,
            IoAction::PersistQuorumState(qs) if qs.current_term == Term(3)
        )));
    }

    // ── Candidate steps down on higher leader_epoch ──
    #[test]
    fn test_candidate_steps_down_on_higher_epoch() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = NodeState::new(NodeId(2), ClusterId::default());
        state.role = Role::Candidate;
        state.current_term = Term(2);
        state.leader_id = None;
        state.votes_received.insert(NodeId(2));
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        let response = FetchResponse {
            leader_id: NodeId(1),
            leader_epoch: Term(5),
            high_watermark: 0,
            log_start_offset: 0,
            entries: vec![],
            diverging_epoch: None,
            snapshot_id: None,
        };

        let result = mgr
            .handle_fetch_response(
                &mut state, &response, &mut sm, &mut listener, &mut queue,
                &mut sc, &clock(),
            )
            .expect("should succeed");

        assert_eq!(state.role, Role::Follower);
        assert_eq!(state.current_term, Term(5));
        assert_eq!(state.leader_id, Some(NodeId(1)));
        assert!(state.votes_received.is_empty(), "election state cleared");
        assert!(result.io_batch.actions.iter().any(|a| matches!(
            a,
            IoAction::PersistQuorumState(_)
        )));
    }

    // ── Mutually-exclusive snapshot_id and diverging_epoch rejected ──
    #[test]
    fn test_mutually_exclusive_snapshot_and_divergence_rejected() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        let response = FetchResponse {
            leader_id: NodeId(1),
            leader_epoch: Term(1),
            high_watermark: 0,
            log_start_offset: 0,
            entries: vec![],
            diverging_epoch: Some(DivergingEpoch {
                epoch: Term(1),
                end_offset: 0,
            }),
            snapshot_id: Some(SnapshotId {
                end_offset: 0,
                epoch: Term(1),
            }),
        };

        let result = mgr.handle_fetch_response(
            &mut state, &response, &mut sm, &mut listener, &mut queue,
            &mut sc, &clock(),
        );

        assert!(result.is_err());
        match result.unwrap_err() {
            XraftError::InvalidResponse(msg) => {
                assert!(msg.contains("both"), "should mention both fields: {msg}");
            }
            other => panic!("expected InvalidResponse, got: {other}"),
        }
    }

    // ── FollowerEventLoop wires Clock to periodic fetch ──
    #[test]
    fn test_follower_event_loop_run_tick() {
        use async_trait::async_trait;

        struct TestClock {
            now: Instant,
        }

        #[async_trait]
        impl Clock for TestClock {
            fn now(&self) -> Instant {
                self.now
            }
            async fn sleep_until(&self, _deadline: Instant) {}
            fn random_election_timeout(&self) -> Duration {
                Duration::from_millis(200)
            }
        }

        let config = RaftConfig::default();
        let event_loop = FollowerEventLoop::new(&config);
        let mut state = make_follower_state(2, 1);

        // Set fetch_deadline in the past.
        let clock_now = Instant::now();
        state.fetch_deadline = clock_now - Duration::from_secs(1);

        let clock = TestClock { now: clock_now };

        // run_tick should fire a Fetch RPC.
        let batch = event_loop.run_tick(&mut state, &clock);
        assert!(!batch.is_empty(), "run_tick should fire SendRpc");
        assert!(batch.actions.iter().any(|a| matches!(a, IoAction::SendRpc(_, _))));

        // Fetch deadline should advance.
        assert_eq!(state.fetch_deadline, clock_now + config.fetch_interval);

        // Calling again before deadline should produce empty batch.
        let batch2 = event_loop.run_tick(&mut state, &clock);
        assert!(batch2.is_empty(), "should not fire before deadline");
    }

    // ── FollowerEventLoop handle_response delegates correctly ──
    #[test]
    fn test_follower_event_loop_handle_response() {
        use async_trait::async_trait;

        struct TestClock {
            now: Instant,
        }

        #[async_trait]
        impl Clock for TestClock {
            fn now(&self) -> Instant {
                self.now
            }
            async fn sleep_until(&self, _deadline: Instant) {}
            fn random_election_timeout(&self) -> Duration {
                Duration::from_millis(200)
            }
        }

        let config = RaftConfig::default();
        let event_loop = FollowerEventLoop::new(&config);
        let mut state = make_follower_state(2, 1);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        let clock = TestClock { now: Instant::now() };

        let entries = make_entries(0, 3, Term(1));
        let response = make_fetch_response(entries, 3);

        let result = event_loop.handle_response(
            &mut state, &response, &mut sm, &mut listener, &mut queue, &mut sc, &clock,
        ).expect("should succeed");

        assert_eq!(state.log_end_offset, 3);
        assert_eq!(state.high_watermark, 3);
        assert_eq!(sm.applied.len(), 3);
        assert!(!result.snapshot_transfer_initiated);
    }

    // ── Divergence end_offset beyond log_end_offset is clamped ──
    #[test]
    fn test_divergence_end_offset_beyond_log_clamped() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        state.current_term = Term(2);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        // Pre-populate follower with entries 0–4, HW=3.
        let initial_entries = make_entries(0, 5, Term(1));
        state.append_entries(&initial_entries);
        state.high_watermark = 3;

        // Divergence end_offset=10 is beyond log_end_offset=5.
        let response = FetchResponse {
            leader_id: NodeId(1),
            leader_epoch: Term(2),
            high_watermark: 3,
            log_start_offset: 0,
            entries: vec![],
            diverging_epoch: Some(DivergingEpoch {
                epoch: Term(2),
                end_offset: 10,
            }),
            snapshot_id: None,
        };

        let result = mgr
            .handle_fetch_response(
                &mut state, &response, &mut sm, &mut listener, &mut queue,
                &mut sc, &clock(),
            )
            .expect("should succeed — clamped to log end");

        // log_end_offset must NOT have moved forward to 10.
        // It should stay at 5 (clamped — nothing to truncate beyond log end).
        assert_eq!(state.log_end_offset, 5, "log_end_offset must not move forward");
        assert_eq!(state.log.len(), 5, "log entries must be unchanged");
        assert_eq!(state.high_watermark, 3, "HW must not change");

        // A re-fetch should still be issued from the current log_end_offset.
        let has_refetch = result.io_batch.actions.iter().any(|a| {
            if let IoAction::SendRpc(_, env) = a {
                if let RpcPayload::FetchRequest(req) = &env.payload {
                    return req.fetch_offset == 5;
                }
            }
            false
        });
        assert!(has_refetch, "re-fetch should be issued from log_end_offset=5");

        // TruncateSuffix(5) is a no-op but should still be in the batch for
        // the I/O layer to confirm.
        assert!(result.io_batch.actions.iter().any(|a| matches!(a, IoAction::TruncateSuffix(5))));
    }

    // ── Stale in-flight response after role change to Candidate is rejected ──
    #[test]
    fn test_stale_response_after_role_change_to_candidate() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        state.current_term = Term(3);

        // Node transitions to Candidate (started election).
        state.role = Role::Candidate;
        state.leader_id = None;
        state.votes_received.insert(NodeId(2));

        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        // Stale fetch response from the old leader in the same term.
        // The node is now a Candidate, so it should reject this response.
        let response = FetchResponse {
            leader_id: NodeId(1),
            leader_epoch: Term(3),
            high_watermark: 0,
            log_start_offset: 0,
            entries: vec![],
            diverging_epoch: None,
            snapshot_id: None,
        };

        let result = mgr.handle_fetch_response(
            &mut state, &response, &mut sm, &mut listener, &mut queue,
            &mut sc, &clock(),
        );

        assert!(result.is_err(), "should reject response when node is Candidate");
        match result.unwrap_err() {
            XraftError::InvalidResponse(msg) => {
                assert!(msg.contains("Candidate"), "error should mention role: {msg}");
            }
            other => panic!("expected InvalidResponse, got: {other}"),
        }

        // Role should remain Candidate — response must not alter state.
        assert_eq!(state.role, Role::Candidate);
    }

    // ── Stale in-flight response after role change to Leader is rejected ──
    #[test]
    fn test_stale_response_after_role_change_to_leader() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        state.current_term = Term(5);

        // Node became Leader in term 5.
        state.role = Role::Leader;
        state.leader_id = Some(NodeId(2)); // self is leader

        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        // Old fetch response from term 5 leader N1 arrives late.
        let response = FetchResponse {
            leader_id: NodeId(1),
            leader_epoch: Term(5),
            high_watermark: 0,
            log_start_offset: 0,
            entries: vec![],
            diverging_epoch: None,
            snapshot_id: None,
        };

        let result = mgr.handle_fetch_response(
            &mut state, &response, &mut sm, &mut listener, &mut queue,
            &mut sc, &clock(),
        );

        // Should be rejected: wrong leader in same term (N1 vs N2).
        assert!(result.is_err(), "should reject response from wrong leader");

        // Role should remain Leader.
        assert_eq!(state.role, Role::Leader);
    }

    // ── Same-term leader discovery persists quorum state ──
    #[test]
    fn test_same_term_leader_discovery_persists_quorum_state() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);

        // Follower knows term 3 but doesn't know who the leader is yet.
        let mut state = NodeState::new(NodeId(2), ClusterId::default());
        state.role = Role::Follower;
        state.current_term = Term(3);
        state.leader_id = None; // Leader unknown.

        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        // Response from N1 in the same term discovers the leader.
        let response = FetchResponse {
            leader_id: NodeId(1),
            leader_epoch: Term(3),
            high_watermark: 0,
            log_start_offset: 0,
            entries: vec![],
            diverging_epoch: None,
            snapshot_id: None,
        };

        let result = mgr
            .handle_fetch_response(
                &mut state, &response, &mut sm, &mut listener, &mut queue,
                &mut sc, &clock(),
            )
            .expect("should succeed");

        // Leader should be recorded.
        assert_eq!(state.leader_id, Some(NodeId(1)));

        // PersistQuorumState should be produced to persist the leader discovery.
        assert!(
            result.io_batch.actions.iter().any(|a| matches!(
                a,
                IoAction::PersistQuorumState(qs)
                    if qs.current_term == Term(3)
                    && qs.leader_id == Some(NodeId(1))
            )),
            "should persist quorum state on same-term leader discovery"
        );
    }

    // ── PersistQuorumState is produced AFTER callbacks ──
    // Verify that when a term bump triggers PersistQuorumState AND entries
    // are committed, the callbacks run first (SM apply, listener, completions)
    // and then IoActions including PersistQuorumState are in the returned batch.
    #[test]
    fn test_persist_quorum_state_after_callbacks_on_term_bump() {
        let config = RaftConfig::default();
        let mgr = ReplicationManager::new(&config);
        let mut state = make_follower_state(2, 1);
        state.current_term = Term(1);
        let mut sm = TestStateMachine::new();
        let mut listener = TestListener::new();
        let mut queue = DeferredCompletionQueue::new();
        let mut sc = TestSnapshotCoordinator::new();

        let (tx, mut rx) = tokio::sync::oneshot::channel();
        queue.park(0, tx);

        // Response with higher term AND entries to commit.
        let entries = make_entries(0, 3, Term(2));
        let response = FetchResponse {
            leader_id: NodeId(1),
            leader_epoch: Term(2),
            high_watermark: 3,
            log_start_offset: 0,
            entries,
            diverging_epoch: None,
            snapshot_id: None,
        };

        let result = mgr
            .handle_fetch_response(
                &mut state, &response, &mut sm, &mut listener, &mut queue,
                &mut sc, &clock(),
            )
            .expect("should succeed");

        // Callbacks ran synchronously before the result was returned:
        assert_eq!(sm.applied.len(), 3, "SM should have applied 3 entries");
        assert_eq!(listener.committed.len(), 1, "listener should have been notified");
        assert_eq!(rx.try_recv().unwrap(), 0, "completion should have fired");

        // Both PersistQuorumState and AppendLog should be in the batch
        // (produced after callbacks).
        assert!(result.io_batch.actions.iter().any(|a| matches!(
            a,
            IoAction::PersistQuorumState(qs) if qs.current_term == Term(2)
        )));
        assert!(result.io_batch.actions.iter().any(|a| matches!(
            a,
            IoAction::AppendLog(_)
        )));
    }
}
