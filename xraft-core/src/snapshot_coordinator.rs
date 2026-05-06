use crate::error::{Result, XraftError};
use crate::log_entry::EntryType;
use crate::node_state::NodeState;
use crate::snapshot::{Snapshot, SnapshotMetadata};
use crate::traits::{LogStore, SnapshotIO, StateMachine};
use crate::types::VotersRecord;

/// Coordinates snapshot creation and recovery.
pub struct SnapshotCoordinator;

impl SnapshotCoordinator {
    /// Create a snapshot capturing the current committed state.
    ///
    /// The snapshot includes:
    /// - Consensus metadata (offsets, term, voter set, leader epoch)
    /// - Application state machine snapshot
    ///
    /// The voter set in the snapshot is the **committed** voter set from
    /// NodeState, ensuring recovery restores the correct membership.
    ///
    /// `last_included_term` is derived from the actual log entry at
    /// `last_included_offset`, not from `current_term`. If the entry has
    /// been compacted and a previous snapshot covers that exact offset,
    /// the previous snapshot's term is used. Otherwise, an error is returned.
    pub async fn create_snapshot(
        state: &NodeState,
        state_machine: &dyn StateMachine,
        snapshot_io: &dyn SnapshotIO,
        log_store: &dyn LogStore,
    ) -> Result<Snapshot> {
        if state.high_watermark == 0 {
            return Err(XraftError::Other("no committed entries to snapshot".into()));
        }

        // The snapshot covers up to HW - 1 (last committed offset)
        let last_included_offset = state.high_watermark - 1;

        // Derive last_included_term from the actual log entry
        let last_included_term = if let Some(entry) = log_store.entry_at(last_included_offset).await? {
            entry.term
        } else if let Some(prev_snap) = snapshot_io.load_latest().await? {
            // Entry compacted — only safe if previous snapshot covers this exact offset
            if prev_snap.metadata.last_included_offset == last_included_offset {
                prev_snap.metadata.last_included_term
            } else {
                return Err(XraftError::Other(format!(
                    "cannot determine term for offset {}: entry compacted and no matching snapshot",
                    last_included_offset
                )));
            }
        } else {
            return Err(XraftError::Other(format!(
                "cannot determine term for offset {}: entry not in log and no snapshot",
                last_included_offset
            )));
        };

        // Get application snapshot
        let app_snapshot = state_machine.snapshot()?;

        let metadata = SnapshotMetadata {
            last_included_offset,
            last_included_term,
            // Use committed voter set — this is the key integration point
            voters: state.voter_set.clone(),
            leader_epoch: state.current_term,
        };

        let snapshot = Snapshot {
            metadata,
            app_snapshot,
        };

        // Persist the snapshot
        snapshot_io.save(&snapshot).await?;

        Ok(snapshot)
    }

    /// Recover node state from a snapshot.
    ///
    /// Recovery flow (per architecture §5.10):
    /// 1. Load latest snapshot
    /// 2. Restore committed voter set from snapshot metadata
    /// 3. Restore state machine from app snapshot
    /// 4. Scan log tail for bookkeeping — advance `log_end_offset` and track
    ///    any VotersRecord entries as **pending** (uncommitted). The
    ///    `high_watermark` is NOT advanced — it stays at
    ///    `snapshot.last_included_offset + 1`. Entries between HW and
    ///    log_end_offset have unknown committed status; the authoritative HW
    ///    comes from the leader via Fetch responses after recovery.
    /// 5. Transition to Follower role; begin accepting RPCs.
    pub async fn recover_from_snapshot(
        state: &mut NodeState,
        state_machine: &mut dyn StateMachine,
        snapshot_io: &dyn SnapshotIO,
        log_store: &dyn LogStore,
    ) -> Result<bool> {
        // Step 1: Load latest snapshot
        let snapshot = match snapshot_io.load_latest().await? {
            Some(s) => s,
            None => return Ok(false),
        };

        // Step 2: Restore committed voter set and consensus metadata
        state.restore_from_snapshot_metadata(
            snapshot.metadata.last_included_offset,
            snapshot.metadata.last_included_term,
            snapshot.metadata.voters.clone(),
            snapshot.metadata.leader_epoch,
        );

        // Step 3: Restore application state machine
        state_machine.restore(snapshot.app_snapshot)?;

        // Step 4: Scan log tail for bookkeeping only.
        // DO NOT advance HW or apply VotersRecord to committed voter_set.
        // VotersRecord entries are tracked as pending membership changes.
        let replay_start = snapshot.metadata.last_included_offset + 1;
        let log_end = log_store.log_end_offset();

        if log_end > replay_start {
            let entries = log_store.read(replay_start, log_end).await?;
            for entry in &entries {
                state.replay_log_tail_entry(entry)?;
            }
        }

        // Step 5: Transition to Follower
        state.role = crate::consensus_state::Role::Follower;

        Ok(true)
    }

    /// Truncate the log prefix after a successful snapshot.
    /// Entries up to and including last_included_offset can be removed.
    pub async fn truncate_log_after_snapshot(
        state: &mut NodeState,
        log_store: &dyn LogStore,
        last_included_offset: u64,
    ) -> Result<()> {
        let new_start = last_included_offset + 1;
        log_store.truncate_prefix(new_start).await?;
        state.log_start_offset = new_start;
        Ok(())
    }

    /// Verify post-recovery voter set consistency.
    ///
    /// Checks that the committed `voter_set` in NodeState matches the last
    /// committed `VotersRecord`. The "last committed VotersRecord" is
    /// determined by scanning committed log entries between the snapshot end
    /// and HW — if any VotersRecord exists there, the last one (by offset)
    /// is the authoritative committed set. If none exists, the snapshot
    /// metadata voters are authoritative.
    ///
    /// Also checks that any uncommitted VotersRecord entries in the log tail
    /// (between HW and log_end_offset) are properly tracked as
    /// `pending_membership_change`.
    ///
    /// Returns `true` if the state is consistent.
    pub async fn verify_voter_set_consistency(
        state: &NodeState,
        log_store: &dyn LogStore,
        snapshot_io: &dyn SnapshotIO,
    ) -> Result<bool> {
        // 1. Determine the authoritative committed voter set.
        //    Start from snapshot metadata; override with last committed VR if any.
        if let Some(snapshot) = snapshot_io.load_latest().await? {
            let snap_end = snapshot.metadata.last_included_offset + 1;
            let mut expected_voters = snapshot.metadata.voters.clone();

            // If HW advanced past the snapshot, scan for committed VotersRecords
            if state.high_watermark > snap_end {
                let committed_entries =
                    log_store.read(snap_end, state.high_watermark).await?;
                // Find the last committed VotersRecord (highest offset)
                let last_committed_vr = committed_entries
                    .iter()
                    .rev()
                    .find(|e| e.entry_type == EntryType::VotersRecord);

                if let Some(entry) = last_committed_vr {
                    let record: VotersRecord = bincode::deserialize(&entry.payload)
                        .map_err(|e| XraftError::SerializationError(format!(
                            "failed to deserialize committed VotersRecord at offset {}: {}",
                            entry.offset, e
                        )))?;
                    expected_voters = record.voters;
                }
            }

            // Compare actual voter_set against expected
            if state.voter_set != expected_voters {
                return Ok(false);
            }
        }

        // 2. Any VotersRecord in the uncommitted tail (HW..log_end_offset)
        //    must be tracked as pending_membership_change.
        let start = state.high_watermark.max(log_store.log_start_offset());
        let end = log_store.log_end_offset();

        if end > start {
            let tail_entries = log_store.read(start, end).await?;
            let last_tail_vr = tail_entries
                .iter()
                .rev()
                .find(|e| e.entry_type == EntryType::VotersRecord);

            match (last_tail_vr, &state.pending_membership_change) {
                (Some(entry), Some(pending)) => {
                    // Pending offset must match the last VotersRecord in tail
                    if pending.offset != entry.offset {
                        return Ok(false);
                    }
                    let record: VotersRecord = bincode::deserialize(&entry.payload)
                        .map_err(|e| XraftError::SerializationError(e.to_string()))?;
                    if pending.voters != record.voters {
                        return Ok(false);
                    }
                }
                (Some(_), None) => return Ok(false), // VR in tail but no pending
                (None, Some(_)) => {} // pending from before snapshot — ok if HW hasn't advanced
                (None, None) => {}
            }
        }

        Ok(true)
    }
}
