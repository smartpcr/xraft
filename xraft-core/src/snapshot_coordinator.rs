// -----------------------------------------------------------------------
// Copyright (c) Microsoft Corp. All rights reserved.
// -----------------------------------------------------------------------

use std::sync::Arc;

use tracing::{debug, info, warn};

use crate::error::{RaftError, RaftResult};
use crate::log_store::LogStore;
use crate::membership::{VoterSet, VotersRecord};
use crate::node::{NodeId, NodeRole};
use crate::snapshot_io::{SnapshotIo, SnapshotMetadata};
use crate::state::RaftState;

/// Coordinates snapshot creation and recovery, ensuring that the voter set
/// recorded in a snapshot is consistent with the log store and the active
/// membership configuration after restoration.
pub struct SnapshotCoordinator {
    node_id: NodeId,
}

impl SnapshotCoordinator {
    pub fn new(node_id: NodeId) -> Self {
        Self { node_id }
    }

    // ------------------------------------------------------------------
    // Snapshot creation
    // ------------------------------------------------------------------

    /// Creates a snapshot of the current state and persists it via
    /// `snapshot_io`. Returns the metadata of the newly created snapshot.
    pub async fn create_snapshot(
        &self,
        state: &RaftState,
        log_store: &dyn LogStore,
        snapshot_io: &dyn SnapshotIo,
    ) -> RaftResult<SnapshotMetadata> {
        let last_applied = state.last_applied_index();
        let last_term = state.last_applied_term();
        let voter_set = state.voter_set().clone();
        let voters_record = state.voters_record().clone();

        info!(
            node = %self.node_id,
            last_applied_index = last_applied,
            last_applied_term = last_term,
            voters = ?voter_set,
            "creating snapshot"
        );

        // 1. Serialise application state.
        let app_state = state.serialize_application_state()?;

        // 2. Build metadata.
        let metadata = SnapshotMetadata {
            last_included_index: last_applied,
            last_included_term: last_term,
            voter_set: voter_set.clone(),
            voters_record: voters_record.clone(),
        };

        // 3. Persist.
        snapshot_io.save(metadata.clone(), &app_state).await?;

        // 4. Compact the log up to the snapshot point.
        log_store.compact(last_applied).await?;

        info!(
            node = %self.node_id,
            last_included_index = last_applied,
            "snapshot created and log compacted"
        );

        Ok(metadata)
    }

    // ------------------------------------------------------------------
    // Snapshot recovery
    // ------------------------------------------------------------------

    /// Recovers node state from the latest available snapshot.
    ///
    /// Returns `Ok(true)` if a snapshot was found and successfully applied,
    /// or `Ok(false)` if no snapshot is available.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot exists but cannot be applied, or if
    /// post-restore consistency checks fail (e.g. voter-set mismatch).
    pub async fn recover_from_snapshot(
        &self,
        state: &mut RaftState,
        log_store: &dyn LogStore,
        snapshot_io: &dyn SnapshotIo,
    ) -> RaftResult<bool> {
        // Step 1 — load the latest snapshot (if any).
        let (metadata, app_state) = match snapshot_io.load_latest().await? {
            Some(pair) => pair,
            None => {
                debug!(node = %self.node_id, "no snapshot found; skipping recovery");
                return Ok(false);
            }
        };

        info!(
            node = %self.node_id,
            last_included_index = metadata.last_included_index,
            last_included_term = metadata.last_included_term,
            voters = ?metadata.voter_set,
            "recovering from snapshot"
        );

        // Step 2 — restore application state.
        state.restore_application_state(&app_state)?;

        // Step 3 — apply snapshot metadata to Raft state.
        state.set_last_applied(metadata.last_included_index, metadata.last_included_term);
        state.set_commit_index(metadata.last_included_index);

        // Step 4 — restore voter set and voters record from the snapshot.
        state.set_voter_set(metadata.voter_set.clone());
        state.set_voters_record(metadata.voters_record.clone());

        // Step 5 — transition role. After a snapshot install the node
        // demotes itself to Follower to avoid stale-leader scenarios.
        state.transition_role(NodeRole::Follower);

        // Step 6 — verify that the restored voter set is consistent with
        // the log store and snapshot metadata. A corrupt snapshot or an
        // inconsistent log tail could silently produce an incorrect
        // committed voter set, breaking election quorum safety.
        Self::verify_voter_set_consistency(state, log_store, snapshot_io).await?;

        info!(
            node = %self.node_id,
            last_applied_index = state.last_applied_index(),
            "snapshot recovery complete"
        );

        Ok(true)
    }

    // ------------------------------------------------------------------
    // Post-restore consistency verification
    // ------------------------------------------------------------------

    /// Verifies that the active voter set in `state` is consistent with
    /// the log store and the latest snapshot metadata.
    ///
    /// Specifically this checks:
    /// 1. Every voter in the current set appears in the voters record.
    /// 2. The voters record's effective index does not exceed the commit
    ///    index (i.e. no uncommitted membership change is treated as
    ///    committed).
    /// 3. If the log contains membership-change entries after the
    ///    snapshot's last-included-index, the voter set accounts for them.
    ///
    /// Returns `Ok(())` on success, or a [`RaftError::InconsistentVoterSet`]
    /// if any check fails.
    async fn verify_voter_set_consistency(
        state: &RaftState,
        log_store: &dyn LogStore,
        snapshot_io: &dyn SnapshotIo,
    ) -> RaftResult<()> {
        let voter_set = state.voter_set();
        let voters_record = state.voters_record();
        let commit_index = state.commit_index();

        // Check 1: every active voter must be present in the record.
        for voter in voter_set.iter() {
            if !voters_record.contains(voter) {
                warn!(
                    voter = %voter,
                    "voter present in active set but missing from voters record"
                );
                return Err(RaftError::InconsistentVoterSet(format!(
                    "voter {} is in the active voter set but not in the voters record",
                    voter
                )));
            }
        }

        // Check 2: effective index of the record must not exceed commit.
        if let Some(effective_index) = voters_record.effective_index() {
            if effective_index > commit_index {
                warn!(
                    effective_index,
                    commit_index,
                    "voters record effective index exceeds commit index"
                );
                return Err(RaftError::InconsistentVoterSet(format!(
                    "voters record effective index ({}) exceeds commit index ({})",
                    effective_index, commit_index
                )));
            }
        }

        // Check 3: replay any membership entries in the log tail that
        // follow the snapshot boundary and verify the voter set reflects
        // them.
        if let Some((snapshot_meta, _)) = snapshot_io.load_latest().await? {
            let tail_start = snapshot_meta.last_included_index + 1;
            let membership_entries =
                log_store.membership_entries_in_range(tail_start, commit_index).await?;

            if let Some(latest_entry) = membership_entries.last() {
                let expected_voter_set = &latest_entry.voter_set;
                if voter_set != expected_voter_set {
                    warn!(
                        current = ?voter_set,
                        expected = ?expected_voter_set,
                        entry_index = latest_entry.index,
                        "voter set does not reflect latest committed membership entry"
                    );
                    return Err(RaftError::InconsistentVoterSet(format!(
                        "voter set does not match latest committed membership entry at index {}",
                        latest_entry.index
                    )));
                }
            }
        }

        debug!("voter set consistency verified successfully");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Unit tests would go here, using mock implementations of
    // LogStore, SnapshotIo, and RaftState.
}
