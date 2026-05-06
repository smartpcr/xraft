use tracing::{info, warn};

use crate::app_record::AppRecord;
use crate::config::RaftConfig;
use crate::io_action::{IoAction, IoActionBatch};
use crate::io_stage::{IoResult, IoStage};
use crate::snapshot_coordinator::{SnapshotAction, SnapshotCoordinator};
use crate::traits::{LogStore, NetworkSender, SnapshotIO, StateMachine};
use crate::types::Term;
use crate::voter::VoterInfo;

/// The single-threaded event loop that drives the Raft node.
///
/// This struct implements the commit-path integration for periodic
/// snapshotting as described in architecture §4.1. The event loop:
///
/// 1. Applies committed entries to the `StateMachine`.
/// 2. Advances the high watermark and checks if a snapshot is needed.
/// 3. Prepares snapshots via `SnapshotCoordinator` (synchronous).
/// 4. Dispatches I/O actions via `IoStage::execute()`.
/// 5. Processes I/O results — on `SnapshotSaved`, schedules truncation;
///    on `SnapshotSaveFailed`, resets the coordinator for retry.
///
/// The full event loop (RPC dispatch, election timeouts, proposal handling)
/// is implemented in later stages. This module implements the snapshot-
/// related commit path that was the missing integration piece.
pub struct EventLoop<SM, L, S, N> {
    state_machine: SM,
    io_stage: IoStage<L, S, N>,
    snapshot_coord: SnapshotCoordinator,
    /// Current high watermark (exclusive upper bound of committed offsets).
    high_watermark: u64,
    /// Current term of the last committed entry.
    current_term: Term,
    /// Current voter set.
    voters: Vec<VoterInfo>,
    /// Current leader epoch.
    leader_epoch: Term,
}

impl<SM, L, S, N> EventLoop<SM, L, S, N>
where
    SM: StateMachine,
    L: LogStore,
    S: SnapshotIO,
    N: NetworkSender,
{
    /// Construct the event loop. The `IoStage` is moved in (sole ownership).
    pub fn new(
        state_machine: SM,
        io_stage: IoStage<L, S, N>,
        config: &RaftConfig,
        initial_snapshot_offset: Option<u64>,
    ) -> Self {
        Self {
            state_machine,
            io_stage,
            snapshot_coord: SnapshotCoordinator::new(config, initial_snapshot_offset),
            high_watermark: initial_snapshot_offset.map_or(0, |o| o + 1),
            current_term: Term(0),
            voters: Vec::new(),
            leader_epoch: Term(0),
        }
    }

    /// Set the current voter set (called when voters change).
    pub fn set_voters(&mut self, voters: Vec<VoterInfo>) {
        self.voters = voters;
    }

    /// Set the leader epoch (called on leader change).
    pub fn set_leader_epoch(&mut self, epoch: Term) {
        self.leader_epoch = epoch;
    }

    /// Access the snapshot coordinator (for queries).
    pub fn snapshot_coordinator(&self) -> &SnapshotCoordinator {
        &self.snapshot_coord
    }

    /// Access the I/O stage (for log queries).
    pub fn io_stage(&self) -> &IoStage<L, S, N> {
        &self.io_stage
    }

    /// The commit-path handler: apply committed entries, advance HW,
    /// trigger snapshotting if needed, and execute all resulting I/O.
    ///
    /// This is the production integration point. In a full Raft node,
    /// this method is called by the event loop whenever the high watermark
    /// advances (e.g., after processing a successful Fetch response that
    /// reveals new committed entries).
    ///
    /// ## Processing order (architecture §4.1)
    ///
    /// 1. Apply each committed entry to the state machine.
    /// 2. Advance HW and check if snapshot is needed.
    /// 3. If needed, prepare snapshot (synchronous SM capture).
    /// 4. Collect `IoAction::SaveSnapshot` into the batch.
    /// 5. Execute the I/O batch via `IoStage`.
    /// 6. Process results — on save success, schedule truncation in a
    ///    follow-up batch; on save failure, reset for retry.
    pub async fn apply_committed_entries(
        &mut self,
        entries: &[(u64, Term, AppRecord)],
    ) -> std::io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        // Phase 1: Apply entries to the state machine (synchronous callbacks).
        for (offset, term, record) in entries {
            self.state_machine.apply(*offset, record)?;
            self.current_term = *term;
        }

        // Phase 2: Advance the high watermark.
        let last_offset = entries.last().expect("non-empty").0;
        let new_hw = last_offset + 1;
        self.high_watermark = new_hw;

        let needs_snapshot = self.snapshot_coord.on_high_watermark_advance(new_hw);

        // Phase 3: Prepare snapshot if interval reached.
        let mut batch = IoActionBatch::new();
        if needs_snapshot {
            if let Some(action) = self.snapshot_coord.prepare_snapshot(
                new_hw,
                self.current_term,
                self.voters.clone(),
                self.leader_epoch,
                &self.state_machine,
            ) {
                match action {
                    SnapshotAction::SaveSnapshot(snap) => {
                        batch.push(IoAction::SaveSnapshot(snap));
                    }
                    SnapshotAction::TruncatePrefix(up_to) => {
                        batch.push(IoAction::TruncatePrefix(up_to));
                    }
                }
            }
        }

        // Phase 4: Execute I/O batch (save snapshot).
        if !batch.is_empty() {
            let results = self.io_stage.execute(&mut batch).await;
            self.process_io_results(results).await?;
        }

        Ok(())
    }

    /// Process I/O results from `IoStage::execute()`.
    ///
    /// When a `SnapshotSaved` result arrives, the coordinator produces
    /// a `TruncatePrefix` action which is executed in a follow-up batch.
    /// This ensures the snapshot is fully persisted (fsync) before any
    /// log entries are truncated.
    ///
    /// When a `SnapshotSaveFailed` result arrives, the coordinator resets
    /// and retries after the next `snapshot_interval` commits. No
    /// truncation occurs — no data is lost.
    async fn process_io_results(&mut self, results: Vec<IoResult>) -> std::io::Result<()> {
        for result in results {
            match result {
                IoResult::SnapshotSaved => {
                    // Snapshot persisted — schedule log prefix truncation.
                    if let Some(SnapshotAction::TruncatePrefix(up_to)) =
                        self.snapshot_coord.on_snapshot_saved()
                    {
                        info!(up_to, "executing log prefix truncation");
                        let mut truncate_batch = IoActionBatch::new();
                        truncate_batch.push(IoAction::TruncatePrefix(up_to));
                        let truncate_results =
                            self.io_stage.execute(&mut truncate_batch).await;
                        for tr in truncate_results {
                            match tr {
                                IoResult::PrefixTruncated => {
                                    self.snapshot_coord.on_truncation_completed();
                                }
                                IoResult::PrefixTruncateFailed(e) => {
                                    // Snapshot is already durable; truncation
                                    // can be retried on next startup.
                                    warn!("prefix truncation failed \
                                           (snapshot is durable): {e}");
                                }
                                _ => {}
                            }
                        }
                    }
                }
                IoResult::SnapshotSaveFailed(_e) => {
                    // Save failed — reset coordinator, skip truncation.
                    self.snapshot_coord.on_snapshot_save_failed();
                }
                IoResult::PrefixTruncated => {
                    self.snapshot_coord.on_truncation_completed();
                }
                IoResult::PrefixTruncateFailed(e) => {
                    warn!("prefix truncation failed: {e}");
                }
                // Other results are handled by their respective subsystems.
                IoResult::LogAppended
                | IoResult::SuffixTruncated
                | IoResult::RpcSent
                | IoResult::RpcFailed(_)
                | IoResult::StorageFailed(_) => {}
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_record::{AppRecord, AppSnapshot};
    use crate::config::RaftConfig;
    use crate::log_entry::{EntryType, LogEntry};
    use crate::snapshot::{Snapshot, SnapshotId, SnapshotWriter};
    use crate::types::Term;
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::io;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Mutex;

    // ── In-memory LogStore ─────────────────────────────────────────

    struct MemLog {
        entries: Mutex<Vec<LogEntry>>,
        start_offset: AtomicU64,
    }

    impl MemLog {
        fn new() -> Self {
            Self {
                entries: Mutex::new(Vec::new()),
                start_offset: AtomicU64::new(0),
            }
        }
    }

    #[async_trait]
    impl LogStore for MemLog {
        async fn append(&self, entries: &[LogEntry]) -> io::Result<()> {
            self.entries.lock().unwrap().extend(entries.iter().cloned());
            Ok(())
        }

        async fn read(&self, start: u64, end: u64) -> io::Result<Vec<LogEntry>> {
            let guard = self.entries.lock().unwrap();
            let base = self.start_offset.load(Ordering::SeqCst);
            Ok(guard
                .iter()
                .filter(|e| e.offset >= start && e.offset < end && e.offset >= base)
                .cloned()
                .collect())
        }

        async fn truncate_suffix(&self, from: u64) -> io::Result<()> {
            self.entries.lock().unwrap().retain(|e| e.offset < from);
            Ok(())
        }

        async fn truncate_prefix(&self, up_to_offset: u64) -> io::Result<()> {
            self.entries
                .lock()
                .unwrap()
                .retain(|e| e.offset >= up_to_offset);
            self.start_offset.store(up_to_offset, Ordering::SeqCst);
            Ok(())
        }

        fn log_start_offset(&self) -> u64 {
            self.start_offset.load(Ordering::SeqCst)
        }

        fn log_end_offset(&self) -> u64 {
            let guard = self.entries.lock().unwrap();
            guard.last().map_or(
                self.start_offset.load(Ordering::SeqCst),
                |e| e.offset + 1,
            )
        }

        async fn entry_at(&self, offset: u64) -> io::Result<Option<LogEntry>> {
            Ok(self
                .entries
                .lock()
                .unwrap()
                .iter()
                .find(|e| e.offset == offset)
                .cloned())
        }
    }

    // ── In-memory SnapshotIO ───────────────────────────────────────

    struct MemSnapshotIO {
        saved: Mutex<Option<Snapshot>>,
        fail_save: AtomicBool,
    }

    impl MemSnapshotIO {
        fn new() -> Self {
            Self {
                saved: Mutex::new(None),
                fail_save: AtomicBool::new(false),
            }
        }

        fn set_fail_save(&self, fail: bool) {
            self.fail_save.store(fail, Ordering::SeqCst);
        }

        fn saved_snapshot(&self) -> Option<Snapshot> {
            self.saved.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl SnapshotIO for MemSnapshotIO {
        async fn save(&self, snapshot: &Snapshot) -> io::Result<()> {
            if self.fail_save.load(Ordering::SeqCst) {
                return Err(io::Error::other("simulated IO error"));
            }
            *self.saved.lock().unwrap() = Some(snapshot.clone());
            Ok(())
        }

        async fn load_latest(&self) -> io::Result<Option<Snapshot>> {
            Ok(self.saved.lock().unwrap().clone())
        }

        async fn read_chunk(
            &self,
            _id: &SnapshotId,
            _pos: u64,
            _max: u32,
        ) -> io::Result<(Bytes, bool)> {
            unimplemented!()
        }

        async fn begin_receive(&self, _id: &SnapshotId) -> io::Result<SnapshotWriter> {
            unimplemented!()
        }
    }

    // ── Noop NetworkSender ───────────────────────────────────────

    struct NoopNetwork;

    #[async_trait]
    impl NetworkSender for NoopNetwork {
        async fn send(&self, _target: crate::types::NodeId, _data: Vec<u8>) -> io::Result<()> {
            Ok(())
        }
    }

    // ── Trivial StateMachine ───────────────────────────────────────

    struct CounterSM {
        count: u64,
    }

    impl CounterSM {
        fn new() -> Self {
            Self { count: 0 }
        }
    }

    impl StateMachine for CounterSM {
        fn apply(&mut self, _offset: u64, _record: &AppRecord) -> io::Result<()> {
            self.count += 1;
            Ok(())
        }

        fn snapshot(&self) -> io::Result<AppSnapshot> {
            Ok(AppSnapshot {
                data: self.count.to_le_bytes().to_vec(),
            })
        }

        fn restore(&mut self, snapshot: AppSnapshot) -> io::Result<()> {
            let bytes: [u8; 8] = snapshot
                .data
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad snapshot"))?;
            self.count = u64::from_le_bytes(bytes);
            Ok(())
        }
    }

    // ── Helpers ────────────────────────────────────────────────────

    fn make_records(count: u64) -> Vec<(u64, Term, AppRecord)> {
        (0..count)
            .map(|i| {
                (
                    i,
                    Term(1),
                    AppRecord {
                        data: Bytes::from(vec![0u8; 8]),
                    },
                )
            })
            .collect()
    }

    fn make_records_range(start: u64, count: u64, term: u64) -> Vec<(u64, Term, AppRecord)> {
        (start..start + count)
            .map(|i| {
                (
                    i,
                    Term(term),
                    AppRecord {
                        data: Bytes::from(vec![0u8; 8]),
                    },
                )
            })
            .collect()
    }

    fn test_config(interval: u64) -> RaftConfig {
        RaftConfig {
            snapshot_interval: interval,
            ..RaftConfig::default()
        }
    }

    // ── Integration tests ─────────────────────────────────────────

    /// Full EventLoop integration: committing 100 entries triggers
    /// automatic snapshot at offset 99 and truncates log to offset 100.
    #[tokio::test]
    async fn eventloop_automatic_snapshot_at_interval() {
        let config = test_config(100);
        let log = MemLog::new();
        let snap_io = MemSnapshotIO::new();

        // Pre-populate log entries (in production, AppendLog IoAction does this).
        let entries: Vec<LogEntry> = (0..100)
            .map(|i| LogEntry {
                offset: i,
                term: Term(1),
                entry_type: EntryType::Command,
                payload: Some(AppRecord {
                    data: Bytes::from(vec![0u8; 8]),
                }),
            })
            .collect();
        log.append(&entries).await.unwrap();

        let io_stage = IoStage::new(log, snap_io, NoopNetwork);
        let sm = CounterSM::new();
        let mut event_loop = EventLoop::new(sm, io_stage, &config, None);

        // Commit 100 entries through the real EventLoop path.
        let records = make_records(100);
        event_loop.apply_committed_entries(&records).await.unwrap();

        // Verify: snapshot was taken at offset 99.
        let snap = event_loop
            .io_stage()
            .snapshot_io()
            .saved_snapshot()
            .expect("snapshot should be saved");
        assert_eq!(snap.metadata.last_included_offset, 99);
        assert_eq!(snap.metadata.last_included_term, Term(1));

        // Verify: log prefix truncated, log_start_offset = 100.
        assert_eq!(event_loop.io_stage().log_store().log_start_offset(), 100);

        // Verify: entries 0–99 are gone.
        let remaining = event_loop
            .io_stage()
            .log_store()
            .read(0, 100)
            .await
            .unwrap();
        assert!(remaining.is_empty());

        // Verify: coordinator bookkeeping.
        assert_eq!(
            event_loop.snapshot_coordinator().last_snapshot_offset(),
            Some(99)
        );
        assert_eq!(event_loop.snapshot_coordinator().commits_since_snapshot(), 0);
    }

    /// EventLoop log reclamation: after snapshot at offset 499 (HW=500),
    /// log_start_offset==500 and entries 0–499 are removed.
    #[tokio::test]
    async fn eventloop_log_reclamation() {
        let config = test_config(500);
        let log = MemLog::new();
        let snap_io = MemSnapshotIO::new();

        let entries: Vec<LogEntry> = (0..500)
            .map(|i| LogEntry {
                offset: i,
                term: Term(1),
                entry_type: EntryType::Command,
                payload: Some(AppRecord {
                    data: Bytes::from(vec![0u8; 8]),
                }),
            })
            .collect();
        log.append(&entries).await.unwrap();

        let io_stage = IoStage::new(log, snap_io, NoopNetwork);
        let sm = CounterSM::new();
        let mut event_loop = EventLoop::new(sm, io_stage, &config, None);

        let records = make_records(500);
        event_loop.apply_committed_entries(&records).await.unwrap();

        assert_eq!(event_loop.io_stage().log_store().log_start_offset(), 500);
        let remaining = event_loop
            .io_stage()
            .log_store()
            .read(0, 500)
            .await
            .unwrap();
        assert!(remaining.is_empty());
    }

    /// EventLoop snapshot-before-truncation: if save fails, truncation
    /// is skipped and no log data is lost.
    #[tokio::test]
    async fn eventloop_save_failure_skips_truncation() {
        let config = test_config(100);
        let log = MemLog::new();
        let snap_io = MemSnapshotIO::new();
        snap_io.set_fail_save(true);

        let entries: Vec<LogEntry> = (0..100)
            .map(|i| LogEntry {
                offset: i,
                term: Term(1),
                entry_type: EntryType::Command,
                payload: Some(AppRecord {
                    data: Bytes::from(vec![0u8; 8]),
                }),
            })
            .collect();
        log.append(&entries).await.unwrap();

        let io_stage = IoStage::new(log, snap_io, NoopNetwork);
        let sm = CounterSM::new();
        let mut event_loop = EventLoop::new(sm, io_stage, &config, None);

        let records = make_records(100);
        // EventLoop handles save failure gracefully — entries still applied.
        event_loop.apply_committed_entries(&records).await.unwrap();

        // Log must NOT be truncated.
        assert_eq!(event_loop.io_stage().log_store().log_start_offset(), 0);
        let remaining = event_loop
            .io_stage()
            .log_store()
            .read(0, 100)
            .await
            .unwrap();
        assert_eq!(remaining.len(), 100);

        // No snapshot recorded.
        assert_eq!(
            event_loop.snapshot_coordinator().last_snapshot_offset(),
            None
        );

        // Counter reset — retry after next interval.
        assert_eq!(event_loop.snapshot_coordinator().commits_since_snapshot(), 0);
    }

    /// Multi-batch commit: two full intervals produce two snapshots
    /// with correct offsets and log truncation.
    #[tokio::test]
    async fn eventloop_multi_batch_snapshots() {
        let config = test_config(10);
        let log = MemLog::new();
        let snap_io = MemSnapshotIO::new();

        let entries: Vec<LogEntry> = (0..25)
            .map(|i| LogEntry {
                offset: i,
                term: Term(1),
                entry_type: EntryType::Command,
                payload: Some(AppRecord {
                    data: Bytes::from(vec![0u8; 8]),
                }),
            })
            .collect();
        log.append(&entries).await.unwrap();

        let io_stage = IoStage::new(log, snap_io, NoopNetwork);
        let sm = CounterSM::new();
        let mut event_loop = EventLoop::new(sm, io_stage, &config, None);

        // Batch 1: 10 entries → snapshot at offset 9, truncate to 10.
        let batch1 = make_records(10);
        event_loop.apply_committed_entries(&batch1).await.unwrap();
        assert_eq!(
            event_loop.snapshot_coordinator().last_snapshot_offset(),
            Some(9)
        );
        assert_eq!(event_loop.io_stage().log_store().log_start_offset(), 10);

        // Batch 2: entries 10–19 → snapshot at offset 19, truncate to 20.
        let batch2 = make_records_range(10, 10, 2);
        event_loop.apply_committed_entries(&batch2).await.unwrap();
        assert_eq!(
            event_loop.snapshot_coordinator().last_snapshot_offset(),
            Some(19)
        );
        assert_eq!(event_loop.io_stage().log_store().log_start_offset(), 20);

        // Batch 3: entries 20–24 → not enough for another snapshot.
        let batch3 = make_records_range(20, 5, 2);
        event_loop.apply_committed_entries(&batch3).await.unwrap();
        assert_eq!(
            event_loop.snapshot_coordinator().last_snapshot_offset(),
            Some(19)
        );
        assert_eq!(event_loop.snapshot_coordinator().commits_since_snapshot(), 5);
    }
}
