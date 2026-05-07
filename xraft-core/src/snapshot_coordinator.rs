// -----------------------------------------------------------------------
// Copyright (c) Microsoft Corp. All rights reserved.
// -----------------------------------------------------------------------

use crate::log::Log;
use crate::node::{NodeId, Role};
use crate::state::RaftState;
use crate::voters::VotersRecord;

use std::collections::HashMap;
use std::io;

/// Metadata attached to every snapshot so that recovery can restore
/// the full Raft state without replaying the log.
#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotMetadata {
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub leader_epoch: u64,
    pub voters: VotersRecord,
}

/// A point-in-time snapshot of the replicated state machine together
/// with the Raft metadata needed for safe recovery.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub metadata: SnapshotMetadata,
    pub data: Vec<u8>,
}

/// Coordinates snapshot creation, installation and compaction.
pub struct SnapshotCoordinator {
    /// Directory (or abstract sink) where snapshots are persisted.
    snapshot_dir: String,
    /// The most recent snapshot, if any.
    latest: Option<Snapshot>,
}

impl SnapshotCoordinator {
    pub fn new(snapshot_dir: String) -> Self {
        Self {
            snapshot_dir,
            latest: None,
        }
    }

    /// Create a snapshot capturing the current replicated state.
    ///
    /// # Arguments
    ///
    /// * `state`        – current Raft state (term, commit index, voters, …).
    /// * `leader_epoch` – the term of the leader whose log this snapshot
    ///                    represents.  Callers **must** supply the correct
    ///                    value; for leaders this is `state.current_term`,
    ///                    while followers should use the term received in the
    ///                    `InstallSnapshot` RPC (or similar) from the leader.
    /// * `log`          – the replicated log, used to look up the term of the
    ///                    last included entry.
    /// * `state_machine_data` – opaque, serialised state-machine bytes.
    pub fn create_snapshot(
        &mut self,
        state: &RaftState,
        leader_epoch: u64,
        log: &Log,
        state_machine_data: Vec<u8>,
    ) -> io::Result<&Snapshot> {
        let last_included_index = state.commit_index;
        let last_included_term = log
            .term_at(last_included_index)
            .unwrap_or(state.current_term);

        let metadata = SnapshotMetadata {
            last_included_index,
            last_included_term,
            leader_epoch,
            voters: state.voters.clone(),
        };

        let snapshot = Snapshot {
            metadata,
            data: state_machine_data,
        };

        self.persist_snapshot(&snapshot)?;
        self.latest = Some(snapshot);

        Ok(self.latest.as_ref().unwrap())
    }

    /// Install a snapshot received from the leader (follower path).
    pub fn install_snapshot(&mut self, snapshot: Snapshot) -> io::Result<()> {
        self.persist_snapshot(&snapshot)?;
        self.latest = Some(snapshot);
        Ok(())
    }

    /// Return the latest snapshot, if one exists.
    pub fn latest_snapshot(&self) -> Option<&Snapshot> {
        self.latest.as_ref()
    }

    /// Compact the log by discarding entries already covered by the
    /// latest snapshot.
    pub fn compact_log(&self, log: &mut Log) -> io::Result<()> {
        if let Some(ref snap) = self.latest {
            log.discard_before(snap.metadata.last_included_index)?;
        }
        Ok(())
    }

    // ── private helpers ─────────────────────────────────────────────

    fn persist_snapshot(&self, snapshot: &Snapshot) -> io::Result<()> {
        let path = format!(
            "{}/snapshot-{}.bin",
            self.snapshot_dir, snapshot.metadata.last_included_index
        );
        std::fs::write(&path, &snapshot.data)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal stub so tests compile without pulling in the full Log.
    mod fake {
        pub struct FakeLog {
            pub terms: std::collections::HashMap<u64, u64>,
        }

        impl FakeLog {
            pub fn term_at(&self, index: u64) -> Option<u64> {
                self.terms.get(&index).copied()
            }
            pub fn discard_before(&mut self, _index: u64) -> std::io::Result<()> {
                Ok(())
            }
        }
    }

    #[test]
    fn leader_epoch_comes_from_caller_not_current_term() {
        // Simulate a follower whose current_term is 5 creating a
        // snapshot on behalf of a leader in term 3.
        let state = RaftState {
            current_term: 5,
            commit_index: 10,
            role: Role::Follower,
            voters: VotersRecord::default(),
        };

        let mut coord = SnapshotCoordinator::new("/tmp/test-snap".into());
        let log = fake::FakeLog {
            terms: [(10, 3)].into_iter().collect(),
        };

        // The caller explicitly passes leader_epoch = 3.
        let snap = coord
            .create_snapshot(&state, 3, &log, vec![1, 2, 3])
            .unwrap();

        assert_eq!(snap.metadata.leader_epoch, 3);
        assert_ne!(snap.metadata.leader_epoch, state.current_term);
    }
}
