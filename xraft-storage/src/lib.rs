//! # xraft-storage
//!
//! Durable storage backend for the xraft Raft consensus implementation.
//!
//! Provides [`StorageEngine`] — a facade that owns and coordinates:
//! - [`SegmentLog`] — append-only segmented log implementing [`LogStore`]
//! - [`QuorumStateFile`] — atomic JSON file for voting state implementing [`QuorumStateStore`]
//! - [`LeaderEpochCheckpoint`] — epoch→offset mapping for divergence detection
//! - [`SnapshotStore`] — file-based snapshots implementing [`SnapshotIO`]
//!
//! All public types are `Send + Sync` for use from async tasks.

pub mod leader_epoch_checkpoint;
pub mod quorum_state_file;
pub mod segment;
pub mod segment_log;
pub mod snapshot_store;

pub use leader_epoch_checkpoint::LeaderEpochCheckpoint;
pub use quorum_state_file::QuorumStateFile;
pub use segment_log::SegmentLog;
pub use snapshot_store::SnapshotStore;

use std::collections::BTreeMap;
use std::sync::Arc;

use xraft_core::traits::{LogStore, QuorumStateStore, SnapshotIO};
use xraft_core::{EntryType, RaftConfig, Result, Snapshot};

/// Unified storage facade that owns all durable state for a Raft node.
///
/// Created via [`StorageEngine::open`], which sets up the directory layout,
/// opens or recovers each component, and rebuilds the leader-epoch checkpoint
/// from a log scan if no checkpoint file exists.
pub struct StorageEngine {
    pub log: Arc<SegmentLog>,
    pub quorum_state: Arc<QuorumStateFile>,
    pub leader_epochs: Arc<LeaderEpochCheckpoint>,
    pub snapshots: Arc<SnapshotStore>,
    pub latest_snapshot: Option<Snapshot>,
}

// Static assertions: all public types must be Send + Sync.
const _: () = {
    fn assert_send_sync<T: Send + Sync>() {}
    fn assertions() {
        assert_send_sync::<StorageEngine>();
        assert_send_sync::<SegmentLog>();
        assert_send_sync::<QuorumStateFile>();
        assert_send_sync::<LeaderEpochCheckpoint>();
        assert_send_sync::<SnapshotStore>();
    }
};

impl StorageEngine {
    /// Open the storage engine, creating the directory layout if needed.
    ///
    /// Disk layout (matches architecture §3.4):
    /// ```text
    /// <data_dir>/log/
    /// ├── 00000000000000000000.log
    /// ├── ...
    /// ├── snapshot/
    /// │   └── <offset>-<epoch>.snap
    /// ├── quorum-state
    /// ├── leader-epoch-checkpoint
    /// └── log-start-offset
    /// ```
    pub async fn open(config: &RaftConfig) -> Result<Self> {
        let data_dir = &config.data_dir;
        let log_dir = data_dir.join("log");
        let snap_dir = log_dir.join("snapshot");

        // Step 1: Create directory layout
        tokio::fs::create_dir_all(&log_dir).await?;
        tokio::fs::create_dir_all(&snap_dir).await?;

        // Step 2: Open segment log
        let log = SegmentLog::open(&log_dir).await?;

        // Step 3: Load quorum state (lives under log_dir per architecture §3.4)
        let quorum_state = QuorumStateFile::open(&log_dir).await?;

        // Step 4: Load snapshots
        let snapshots = SnapshotStore::open(&snap_dir).await?;
        let latest_snapshot = snapshots.load_latest().await?;

        // Step 5: Open leader epoch checkpoint (lives under log_dir per §3.4),
        //         always rebuild from log to reconcile stale checkpoints.
        let leader_epochs = LeaderEpochCheckpoint::open(&log_dir).await?;

        // Always rebuild by scanning the log for LeaderChangeMessage entries.
        // This reconciles stale checkpoint data with the actual log contents.
        let start = log.start_offset().await;
        let end = log.end_offset().await;

        if start < end {
            let mut epoch_map = BTreeMap::new();
            let entries = log.read(start, end).await?;
            for entry in &entries {
                if entry.entry_type == EntryType::LeaderChangeMessage {
                    epoch_map.entry(entry.term).or_insert(entry.offset);
                }
            }
            leader_epochs.rebuild(epoch_map).await?;
        } else {
            // Empty log — clear any stale checkpoint
            leader_epochs.rebuild(BTreeMap::new()).await?;
        }

        Ok(Self {
            log: Arc::new(log),
            quorum_state: Arc::new(quorum_state),
            leader_epochs: Arc::new(leader_epochs),
            snapshots: Arc::new(snapshots),
            latest_snapshot,
        })
    }

    /// Get a reference to the log store (as a trait object).
    pub fn log_store(&self) -> Arc<dyn LogStore> {
        self.log.clone()
    }

    /// Get a reference to the quorum state store (as a trait object).
    pub fn quorum_state_store(&self) -> Arc<dyn QuorumStateStore> {
        self.quorum_state.clone()
    }

    /// Get a reference to the snapshot I/O (as a trait object).
    pub fn snapshot_io(&self) -> Arc<dyn SnapshotIO> {
        self.snapshots.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xraft_core::log_entry::LogEntry;
    use xraft_core::snapshot::{Snapshot, SnapshotMetadata};
    use xraft_core::{AppSnapshot, EntryType, QuorumState, RaftConfig};

    fn test_config(dir: &std::path::Path) -> RaftConfig {
        RaftConfig {
            data_dir: dir.to_path_buf(),
            ..Default::default()
        }
    }

    fn make_entry(offset: u64, term: u64, entry_type: EntryType) -> LogEntry {
        LogEntry {
            offset,
            term,
            entry_type,
            payload: vec![offset as u8, term as u8],
        }
    }

    /// Full lifecycle test: open → append → snapshot → truncate prefix → reopen → verify recovery.
    #[tokio::test]
    async fn test_full_lifecycle() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(tmp.path());

        // First open: fresh directory
        {
            let engine = StorageEngine::open(&config).await.unwrap();
            assert!(engine.latest_snapshot.is_none());

            let log = &engine.log;
            let quorum = &engine.quorum_state;
            let snapshots = &engine.snapshots;

            // Append entries
            let entries: Vec<LogEntry> = (0..20)
                .map(|i| make_entry(i, 1, EntryType::Command))
                .collect();
            log.append(&entries).await.unwrap();

            // Verify read
            let read_back = log.read(0, 20).await.unwrap();
            assert_eq!(read_back.len(), 20);
            assert_eq!(read_back[0].offset, 0);
            assert_eq!(read_back[19].offset, 19);

            // Save quorum state
            let state = QuorumState {
                current_term: 3,
                voted_for: Some(1),
                leader_id: Some(1),
                leader_epoch: 2,
            };
            quorum.save(&state).await.unwrap();

            // Take a snapshot at offset 10
            let snap = Snapshot {
                metadata: SnapshotMetadata {
                    last_included_offset: 10,
                    last_included_term: 1,
                    voters: vec![],
                    leader_epoch: 2,
                },
                app_snapshot: AppSnapshot {
                    data: vec![42; 64],
                },
            };
            snapshots.save(&snap).await.unwrap();

            // Truncate prefix up to offset 10
            log.truncate_prefix(10).await.unwrap();

            // Verify prefix truncation
            let after_trunc = log.read(0, 20).await.unwrap();
            assert_eq!(after_trunc.len(), 10); // entries 10..20
            assert_eq!(after_trunc[0].offset, 10);
        }

        // Reopen: verify recovery
        {
            let engine = StorageEngine::open(&config).await.unwrap();

            // Snapshot recovered
            assert!(engine.latest_snapshot.is_some());
            let snap = engine.latest_snapshot.as_ref().unwrap();
            assert_eq!(snap.metadata.last_included_offset, 10);
            assert_eq!(snap.app_snapshot.data, vec![42; 64]);

            // Quorum state recovered
            let state = engine.quorum_state.load().await.unwrap().unwrap();
            assert_eq!(state.current_term, 3);
            assert_eq!(state.voted_for, Some(1));

            // Log entries recovered (10..20)
            let entries = engine.log.read(10, 20).await.unwrap();
            assert_eq!(entries.len(), 10);
            assert_eq!(entries[0].offset, 10);
            assert_eq!(entries[9].offset, 19);
        }
    }

    /// Concurrent trait usage: LogStore and QuorumStateStore from different tasks.
    #[tokio::test]
    async fn test_concurrent_trait_usage() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(tmp.path());
        let engine = StorageEngine::open(&config).await.unwrap();

        let log: Arc<dyn LogStore> = engine.log_store();
        let quorum: Arc<dyn QuorumStateStore> = engine.quorum_state_store();
        let snapshot_io: Arc<dyn SnapshotIO> = engine.snapshot_io();

        // Spawn concurrent tasks to exercise Send + Sync
        let log_handle = {
            let log = log.clone();
            tokio::spawn(async move {
                let entries: Vec<LogEntry> = (0..10)
                    .map(|i| make_entry(i, 1, EntryType::Command))
                    .collect();
                log.append(&entries).await.unwrap();
                let result = log.read(0, 10).await.unwrap();
                assert_eq!(result.len(), 10);
            })
        };

        let quorum_handle = {
            let quorum = quorum.clone();
            tokio::spawn(async move {
                let state = QuorumState {
                    current_term: 5,
                    voted_for: Some(2),
                    leader_id: Some(2),
                    leader_epoch: 4,
                };
                quorum.save(&state).await.unwrap();
                let loaded = quorum.load().await.unwrap().unwrap();
                assert_eq!(loaded.current_term, 5);
            })
        };

        let snap_handle = {
            let snapshot_io = snapshot_io.clone();
            tokio::spawn(async move {
                // Just verify trait object usage compiles with Send + Sync
                let latest = snapshot_io.load_latest().await.unwrap();
                assert!(latest.is_none());
            })
        };

        log_handle.await.unwrap();
        quorum_handle.await.unwrap();
        snap_handle.await.unwrap();
    }

    /// Verify leader-epoch checkpoint is rebuilt from log scan.
    #[tokio::test]
    async fn test_leader_epoch_rebuild() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(tmp.path());

        {
            let engine = StorageEngine::open(&config).await.unwrap();

            // Append entries including LeaderChangeMessage entries
            let entries = vec![
                make_entry(0, 1, EntryType::LeaderChangeMessage),
                make_entry(1, 1, EntryType::Command),
                make_entry(2, 1, EntryType::Command),
                make_entry(3, 3, EntryType::LeaderChangeMessage),
                make_entry(4, 3, EntryType::Command),
                make_entry(5, 5, EntryType::LeaderChangeMessage),
            ];
            engine.log.append(&entries).await.unwrap();
        }

        // Reopen — checkpoint should be rebuilt from log
        {
            let engine = StorageEngine::open(&config).await.unwrap();

            let epochs = engine.leader_epochs.all_epochs().await;
            assert_eq!(epochs.len(), 3);
            assert_eq!(epochs[&1], 0); // epoch 1 starts at offset 0
            assert_eq!(epochs[&3], 3); // epoch 3 starts at offset 3
            assert_eq!(epochs[&5], 5); // epoch 5 starts at offset 5

            // Verify lookup
            assert_eq!(engine.leader_epochs.lookup(3).await, Some(3));
            assert_eq!(engine.leader_epochs.lookup(2).await, None);
            assert_eq!(
                engine.leader_epochs.lookup_le(4).await,
                Some((3, 3))
            );
        }
    }

    /// Test entry_at for single-entry reads.
    #[tokio::test]
    async fn test_entry_at() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(tmp.path());
        let engine = StorageEngine::open(&config).await.unwrap();

        let entries: Vec<LogEntry> = (0..5)
            .map(|i| make_entry(i, 1, EntryType::Command))
            .collect();
        engine.log.append(&entries).await.unwrap();

        let e = engine.log.entry_at(3).await.unwrap().unwrap();
        assert_eq!(e.offset, 3);

        let none = engine.log.entry_at(10).await.unwrap();
        assert!(none.is_none());
    }

    /// Test truncate_suffix removes tail entries.
    #[tokio::test]
    async fn test_truncate_suffix() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(tmp.path());
        let engine = StorageEngine::open(&config).await.unwrap();

        let entries: Vec<LogEntry> = (0..100)
            .map(|i| make_entry(i, 1, EntryType::Command))
            .collect();
        engine.log.append(&entries).await.unwrap();

        engine.log.truncate_suffix(50).await.unwrap();

        let remaining = engine.log.read(0, 100).await.unwrap();
        assert_eq!(remaining.len(), 50);
        assert_eq!(remaining.last().unwrap().offset, 49);
    }

    /// Static assertion that StorageEngine is Send + Sync.
    #[allow(dead_code)]
    fn assert_send_sync_bounds() {
        fn is_send_sync<T: Send + Sync>() {}
        is_send_sync::<StorageEngine>();
        is_send_sync::<SegmentLog>();
        is_send_sync::<QuorumStateFile>();
        is_send_sync::<LeaderEpochCheckpoint>();
        is_send_sync::<SnapshotStore>();

        // Trait objects must be sendable
        is_send_sync::<Arc<dyn LogStore>>();
        is_send_sync::<Arc<dyn QuorumStateStore>>();
        is_send_sync::<Arc<dyn SnapshotIO>>();
    }
}
