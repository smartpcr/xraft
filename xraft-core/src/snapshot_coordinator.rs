use tracing::{info, warn};

use crate::config::RaftConfig;
use crate::snapshot::{Snapshot, SnapshotMetadata};
use crate::traits::{LogStore, SnapshotIO, StateMachine};
use crate::types::Term;
use crate::voter::VoterInfo;

/// Actions produced by `SnapshotCoordinator` for the `IoStage` to execute.
///
/// These mirror the snapshot-related variants of the architecture's `IoAction`
/// enum (`SaveSnapshot`, `TruncatePrefix`). The event loop collects these into
/// an `IoActionBatch` for the `IoStage`.
#[derive(Debug)]
pub enum SnapshotAction {
    /// Persist a snapshot atomically (fsync before returning OK).
    /// Maps to `IoAction::SaveSnapshot(Snapshot)`.
    SaveSnapshot(Snapshot),
    /// Truncate log entries before the given offset.
    /// Maps to `IoAction::TruncatePrefix(u64)`.
    TruncatePrefix(u64),
}

/// Coordinates periodic snapshotting and log prefix truncation.
///
/// Designed to integrate with the `EventLoop` / `IoStage` architecture:
///
/// 1. **EventLoop** calls [`SnapshotCoordinator::on_high_watermark_advance`]
///    each time HW moves forward (after applying committed entries via
///    `StateMachine::apply`). The call site is the EventLoop's
///    `apply_committed_entries` handler, right after bumping the HW.
/// 2. When `on_high_watermark_advance` returns `true`, EventLoop calls
///    [`SnapshotCoordinator::prepare_snapshot`] — a **synchronous** operation
///    that captures the state-machine snapshot and returns a
///    [`SnapshotAction::SaveSnapshot`].
/// 3. EventLoop adds the `SaveSnapshot` action to the `IoActionBatch`.
/// 4. After `IoStage` completes the save, EventLoop calls
///    [`SnapshotCoordinator::on_snapshot_saved`], which returns a
///    [`SnapshotAction::TruncatePrefix`] action for the next `IoActionBatch`.
/// 5. After `IoStage` completes truncation, EventLoop calls
///    [`SnapshotCoordinator::on_truncation_completed`] to finalise bookkeeping.
///
/// ## Integration with the commit path
///
/// The coordinator is **not** a standalone background task. It is driven
/// entirely by the single-threaded `EventLoop`, which owns the
/// `SnapshotCoordinator` as a field. The typical integration looks like:
///
/// ```ignore
/// // Inside EventLoop::apply_committed_entries():
/// for entry in new_committed_entries {
///     state_machine.apply(entry.offset, &entry.payload);
/// }
/// if self.snapshot_coord.on_high_watermark_advance(new_hw) {
///     if let Some(action) = self.snapshot_coord.prepare_snapshot(
///         new_hw, last_term, voters.clone(), leader_epoch, &self.state_machine,
///     ) {
///         io_batch.push(action);
///     }
/// }
/// ```
///
/// On `IoStage` completion callback:
///
/// ```ignore
/// // IoStage reports SaveSnapshot completed successfully:
/// if let Some(truncate) = self.snapshot_coord.on_snapshot_saved() {
///     io_batch.push(truncate);
/// }
///
/// // IoStage reports SaveSnapshot failed:
/// self.snapshot_coord.on_snapshot_save_failed();
/// ```
///
/// If `StateMachine::snapshot()` fails, the attempt is skipped and retried
/// after the next `snapshot_interval` committed entries (architecture §6.3).
pub struct SnapshotCoordinator {
    snapshot_interval: u64,
    /// High watermark last observed by the coordinator.
    last_observed_hw: u64,
    /// Offset of the last successful snapshot (None if never snapshotted).
    last_snapshot_offset: Option<u64>,
    /// Term at the last successful snapshot offset.
    last_snapshot_term: Option<Term>,
    /// Number of entries committed since the last snapshot (or start).
    commits_since_snapshot: u64,
    /// Set after `prepare_snapshot` succeeds; cleared by `on_snapshot_saved`
    /// or on failure. Guards against duplicate `prepare_snapshot` calls
    /// while a save is in-flight.
    pending_snapshot: Option<SnapshotMetadata>,
    /// If `TruncatePrefix` fails after a successful snapshot save, the
    /// truncation offset is stored here for retry on the next commit cycle.
    /// Coalesced with `max()` so a newer snapshot's truncation supersedes.
    pending_truncation: Option<u64>,
}

impl SnapshotCoordinator {
    /// Create a coordinator from [`RaftConfig`].
    ///
    /// # Panics
    ///
    /// Panics if `config.snapshot_interval == 0`. Callers must validate the
    /// config via [`RaftConfig::validate`] before constructing the coordinator.
    pub fn new(config: &RaftConfig, last_snapshot_offset: Option<u64>) -> Self {
        assert!(
            config.snapshot_interval > 0,
            "snapshot_interval must be > 0"
        );
        Self {
            snapshot_interval: config.snapshot_interval,
            last_observed_hw: last_snapshot_offset.map_or(0, |o| o + 1),
            last_snapshot_offset,
            last_snapshot_term: None,
            commits_since_snapshot: 0,
            pending_snapshot: None,
            pending_truncation: None,
        }
    }

    // ── Event-loop integration API ────────────────────────────────

    /// Notify the coordinator that the high watermark has advanced.
    ///
    /// `new_hw` is the **exclusive** upper bound of committed offsets
    /// (entries `[0, new_hw)` are committed). The coordinator internally
    /// tracks the delta to count new commits.
    ///
    /// Called by the EventLoop every time it advances the high watermark
    /// after applying committed entries. Returns `true` if a snapshot
    /// should be triggered now.
    pub fn on_high_watermark_advance(&mut self, new_hw: u64) -> bool {
        if new_hw <= self.last_observed_hw {
            return false;
        }
        let delta = new_hw - self.last_observed_hw;
        self.last_observed_hw = new_hw;
        self.commits_since_snapshot = self.commits_since_snapshot.saturating_add(delta);
        self.snapshot_needed()
    }

    /// Whether the snapshot interval has been reached.
    pub fn snapshot_needed(&self) -> bool {
        self.commits_since_snapshot >= self.snapshot_interval && self.pending_snapshot.is_none()
    }

    /// Whether a snapshot save is currently in-flight.
    pub fn has_pending_snapshot(&self) -> bool {
        self.pending_snapshot.is_some()
    }

    /// Prepare a snapshot for persistence (synchronous, EventLoop-side).
    ///
    /// `high_watermark` is the **exclusive** commit boundary.
    /// `last_committed_term` is the term of the entry at offset `hw - 1`
    /// (the EventLoop already knows this from applying entries).
    ///
    /// On `StateMachine::snapshot()` error the attempt is **skipped** —
    /// the counter resets and the next snapshot is retried after another
    /// `snapshot_interval` commits (architecture §6.3). The node
    /// continues operating normally.
    ///
    /// Returns `Some(SnapshotAction::SaveSnapshot(..))` if a snapshot was
    /// prepared, or `None` if:
    /// - the interval has not been reached
    /// - HW is 0
    /// - a previous `prepare_snapshot` is still pending (save in-flight)
    /// - the state-machine snapshot failed
    pub fn prepare_snapshot<SM: StateMachine>(
        &mut self,
        high_watermark: u64,
        last_committed_term: Term,
        voters: Vec<VoterInfo>,
        leader_epoch: Term,
        state_machine: &SM,
    ) -> Option<SnapshotAction> {
        if high_watermark == 0 {
            return None;
        }

        // Reject if a snapshot save is already in-flight.
        if self.pending_snapshot.is_some() {
            warn!("prepare_snapshot called while a save is in-flight; skipping");
            return None;
        }

        if !self.snapshot_needed() {
            return None;
        }

        let last_included_offset = high_watermark - 1;

        // Guard against re-snapshotting the same offset.
        if let Some(prev) = self.last_snapshot_offset {
            if last_included_offset <= prev {
                return None;
            }
        }

        // Capture application state (synchronous).
        let app_snapshot = match state_machine.snapshot() {
            Ok(snap) => snap,
            Err(e) => {
                warn!(
                    offset = last_included_offset,
                    "StateMachine::snapshot() failed, skipping snapshot \
                     (will retry after next interval): {e}"
                );
                // Reset counter so retry happens after another full interval.
                self.commits_since_snapshot = 0;
                return None;
            }
        };

        let metadata = SnapshotMetadata {
            last_included_offset,
            last_included_term: last_committed_term,
            voters,
            leader_epoch,
        };

        let snapshot = Snapshot {
            metadata: metadata.clone(),
            app_snapshot,
        };

        self.pending_snapshot = Some(metadata);

        Some(SnapshotAction::SaveSnapshot(snapshot))
    }

    /// Called by the EventLoop after `IoStage` has successfully persisted
    /// the snapshot (fsync complete).
    ///
    /// Updates bookkeeping and returns a `TruncatePrefix` action for the
    /// next `IoActionBatch`. Entries up to and including
    /// `last_included_offset` are covered by the snapshot, so the log
    /// prefix can be safely reclaimed.
    pub fn on_snapshot_saved(&mut self) -> Option<SnapshotAction> {
        let meta = self.pending_snapshot.take()?;
        self.last_snapshot_offset = Some(meta.last_included_offset);
        self.last_snapshot_term = Some(meta.last_included_term);
        self.commits_since_snapshot = 0;

        info!(
            last_included_offset = meta.last_included_offset,
            "snapshot saved, scheduling log prefix truncation"
        );

        Some(SnapshotAction::TruncatePrefix(
            meta.last_included_offset + 1,
        ))
    }

    /// Called by the EventLoop if `IoStage` snapshot save **failed**.
    ///
    /// The pending snapshot is discarded and the counter is reset so the
    /// next attempt happens after another full `snapshot_interval` commits.
    /// No log truncation occurs — no data is lost.
    pub fn on_snapshot_save_failed(&mut self) {
        if let Some(ref meta) = self.pending_snapshot {
            warn!(
                offset = meta.last_included_offset,
                "snapshot save failed, skipping truncation; \
                 will retry after next interval"
            );
        }
        self.pending_snapshot = None;
        self.commits_since_snapshot = 0;
    }

    /// Called by the EventLoop after `IoStage` has completed
    /// `TruncatePrefix`. Currently a no-op hook for future extensions.
    pub fn on_truncation_completed(&mut self) {
        // Bookkeeping is already updated in `on_snapshot_saved`.
    }

    // ── Query API ─────────────────────────────────────────────────

    /// The offset covered by the most recent successful snapshot.
    pub fn last_snapshot_offset(&self) -> Option<u64> {
        self.last_snapshot_offset
    }

    /// The term at the most recent successful snapshot offset.
    pub fn last_snapshot_term(&self) -> Option<Term> {
        self.last_snapshot_term
    }

    /// Number of committed entries since the last snapshot.
    pub fn commits_since_snapshot(&self) -> u64 {
        self.commits_since_snapshot
    }

    // ── Convenience (test / standalone) ───────────────────────────

    /// Execute the full snapshot-then-truncate sequence in one call.
    ///
    /// This is a convenience method that combines `prepare_snapshot`,
    /// `SnapshotIO::save`, and `LogStore::truncate_prefix` in the correct
    /// order, enforcing that the snapshot is fully persisted (fsync)
    /// before any log entries are truncated.
    ///
    /// ## Error semantics
    ///
    /// - `StateMachine::snapshot()` failure → skipped, returns `Ok(false)`.
    /// - `SnapshotIO::save()` failure → truncation skipped, error
    ///   propagated as `Err`. The coordinator resets so a retry occurs
    ///   after the next interval.
    /// - `LogStore::truncate_prefix()` failure → propagated as `Err`.
    ///   The snapshot is already durable, so the coordinator bookkeeping
    ///   is updated even if truncation fails.
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_snapshot<L, S, SM>(
        &mut self,
        high_watermark: u64,
        last_committed_term: Term,
        voters: Vec<VoterInfo>,
        leader_epoch: Term,
        log_store: &L,
        snapshot_io: &S,
        state_machine: &SM,
    ) -> std::io::Result<bool>
    where
        L: LogStore,
        S: SnapshotIO,
        SM: StateMachine,
    {
        // Phase 1: prepare snapshot (synchronous).
        let snapshot = match self.prepare_snapshot(
            high_watermark,
            last_committed_term,
            voters,
            leader_epoch,
            state_machine,
        ) {
            Some(SnapshotAction::SaveSnapshot(snap)) => snap,
            _ => return Ok(false),
        };

        // Phase 2: persist snapshot (must fsync before truncation).
        if let Err(e) = snapshot_io.save(&snapshot).await {
            self.on_snapshot_save_failed();
            return Err(e);
        }

        // Phase 3: schedule + execute truncation.
        let truncate_action = self.on_snapshot_saved();
        if let Some(SnapshotAction::TruncatePrefix(up_to)) = truncate_action {
            log_store.truncate_prefix(up_to).await?;
            self.on_truncation_completed();
        }

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_record::{AppRecord, AppSnapshot};
    use crate::config::RaftConfig;
    use crate::log_entry::{EntryType, LogEntry};
    use crate::snapshot::SnapshotId;
    use crate::snapshot::SnapshotWriter;
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

    // ── Trivial StateMachine ───────────────────────────────────────

    struct CounterSM {
        count: u64,
        fail_snapshot: bool,
    }

    impl CounterSM {
        fn new() -> Self {
            Self {
                count: 0,
                fail_snapshot: false,
            }
        }
    }

    impl StateMachine for CounterSM {
        fn apply(&mut self, _offset: u64, _record: &AppRecord) -> io::Result<()> {
            self.count += 1;
            Ok(())
        }

        fn snapshot(&self) -> io::Result<AppSnapshot> {
            if self.fail_snapshot {
                return Err(io::Error::other("simulated SM snapshot error"));
            }
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

    fn make_entries(start: u64, count: u64, term: u64) -> Vec<LogEntry> {
        (start..start + count)
            .map(|i| LogEntry {
                offset: i,
                term: Term(term),
                entry_type: EntryType::Command,
                payload: Some(AppRecord {
                    data: Bytes::from(vec![0u8; 8]),
                }),
            })
            .collect()
    }

    fn test_config(snapshot_interval: u64) -> RaftConfig {
        RaftConfig {
            snapshot_interval,
            ..RaftConfig::default()
        }
    }

    // ── Tests ──────────────────────────────────────────────────────

    /// Automatic snapshot: HW=100 (exclusive) triggers snapshot at
    /// last_included_offset=99, log_start_offset becomes 100.
    #[tokio::test]
    async fn automatic_snapshot_after_interval() {
        let config = test_config(100);
        let log = MemLog::new();
        let snap_io = MemSnapshotIO::new();
        let mut sm = CounterSM::new();
        let mut coord = SnapshotCoordinator::new(&config, None);

        // Populate log with 100 entries (offsets 0..99), term 1.
        let entries = make_entries(0, 100, 1);
        log.append(&entries).await.unwrap();
        for e in &entries {
            if let Some(ref rec) = e.payload {
                sm.apply(e.offset, rec).unwrap();
            }
        }

        // Advance HW to 100 (exclusive) — 100 entries committed.
        assert!(coord.on_high_watermark_advance(100));

        // Execute the full snapshot sequence.
        // HW=100 → last_included_offset=99, term=1.
        let snapped = coord
            .execute_snapshot(100, Term(1), vec![], Term(1), &log, &snap_io, &sm)
            .await
            .unwrap();
        assert!(snapped);

        // Verify log truncation.
        assert_eq!(log.log_start_offset(), 100);

        // Verify snapshot metadata.
        {
            let saved = snap_io.saved.lock().unwrap();
            let snap = saved.as_ref().expect("snapshot should be saved");
            assert_eq!(snap.metadata.last_included_offset, 99);
            assert_eq!(snap.metadata.last_included_term, Term(1));
        }

        // Verify entries 0–99 are gone.
        let remaining = log.read(0, 100).await.unwrap();
        assert!(remaining.is_empty());

        // Verify coordinator bookkeeping.
        assert_eq!(coord.last_snapshot_offset(), Some(99));
        assert_eq!(coord.last_snapshot_term(), Some(Term(1)));
        assert_eq!(coord.commits_since_snapshot(), 0);
    }

    /// Log reclamation: after snapshot at last_included_offset=499
    /// (HW=500), log_start_offset==500 and entries 0–499 are gone.
    #[tokio::test]
    async fn log_reclamation() {
        let config = test_config(500);
        let log = MemLog::new();
        let snap_io = MemSnapshotIO::new();
        let mut sm = CounterSM::new();
        let mut coord = SnapshotCoordinator::new(&config, None);

        let entries = make_entries(0, 500, 1);
        log.append(&entries).await.unwrap();
        for e in &entries {
            if let Some(ref rec) = e.payload {
                sm.apply(e.offset, rec).unwrap();
            }
        }

        coord.on_high_watermark_advance(500);
        coord
            .execute_snapshot(500, Term(1), vec![], Term(1), &log, &snap_io, &sm)
            .await
            .unwrap();

        assert_eq!(log.log_start_offset(), 500);
        let remaining = log.read(0, 500).await.unwrap();
        assert!(remaining.is_empty());
    }

    /// Snapshot before truncation: if save fails, truncation is skipped
    /// and no data is lost.
    #[tokio::test]
    async fn snapshot_save_failure_skips_truncation() {
        let config = test_config(100);
        let log = MemLog::new();
        let snap_io = MemSnapshotIO::new();
        snap_io.set_fail_save(true);
        let mut sm = CounterSM::new();
        let mut coord = SnapshotCoordinator::new(&config, None);

        let entries = make_entries(0, 100, 1);
        log.append(&entries).await.unwrap();
        for e in &entries {
            if let Some(ref rec) = e.payload {
                sm.apply(e.offset, rec).unwrap();
            }
        }

        coord.on_high_watermark_advance(100);
        let result = coord
            .execute_snapshot(100, Term(1), vec![], Term(1), &log, &snap_io, &sm)
            .await;

        // Save failed — returns Err, not Ok(false).
        assert!(result.is_err());

        // Log must NOT be truncated.
        assert_eq!(log.log_start_offset(), 0);
        let remaining = log.read(0, 100).await.unwrap();
        assert_eq!(remaining.len(), 100);

        // No snapshot recorded.
        assert_eq!(coord.last_snapshot_offset(), None);

        // Counter reset — retry after next interval.
        assert_eq!(coord.commits_since_snapshot(), 0);
    }

    /// HW exclusivity: passing HW=100 snapshots offset 99, not 100.
    #[tokio::test]
    async fn hw_exclusivity_derives_correct_offset() {
        let config = test_config(100);
        let log = MemLog::new();
        let sm = CounterSM::new();
        let mut coord = SnapshotCoordinator::new(&config, None);

        let entries = make_entries(0, 100, 3);
        log.append(&entries).await.unwrap();

        coord.on_high_watermark_advance(100);

        // prepare_snapshot with HW=100, term=3
        let action = coord.prepare_snapshot(100, Term(3), vec![], Term(1), &sm);
        match action {
            Some(SnapshotAction::SaveSnapshot(ref snap)) => {
                assert_eq!(snap.metadata.last_included_offset, 99);
                assert_eq!(snap.metadata.last_included_term, Term(3));
            }
            _ => panic!("expected SaveSnapshot action"),
        }
    }

    /// StateMachine::snapshot() failure: skipped, counter reset, retry
    /// happens after next interval.
    #[tokio::test]
    async fn sm_snapshot_failure_skips_and_resets_counter() {
        let config = test_config(100);
        let mut sm = CounterSM::new();
        sm.fail_snapshot = true;
        let mut coord = SnapshotCoordinator::new(&config, None);

        coord.on_high_watermark_advance(100);
        assert!(coord.snapshot_needed());

        // prepare_snapshot should return None (SM error → skip).
        let action = coord.prepare_snapshot(100, Term(1), vec![], Term(1), &sm);
        assert!(action.is_none());

        // Counter is reset — must commit another full interval to retry.
        assert_eq!(coord.commits_since_snapshot(), 0);
        assert!(!coord.snapshot_needed());

        // After another 100 commits, snapshot is retried.
        coord.on_high_watermark_advance(200);
        assert!(coord.snapshot_needed());
    }

    /// Config validation: snapshot_interval = 0 is rejected.
    #[test]
    fn config_validates_snapshot_interval() {
        let config = RaftConfig {
            snapshot_interval: 0,
            ..RaftConfig::default()
        };
        assert!(config.validate().is_err());
    }

    /// Constructor panics on zero interval.
    #[test]
    #[should_panic(expected = "snapshot_interval must be > 0")]
    fn zero_snapshot_interval_panics() {
        let config = RaftConfig {
            snapshot_interval: 0,
            ..RaftConfig::default()
        };
        let _ = SnapshotCoordinator::new(&config, None);
    }

    /// Action-oriented API: prepare → save → truncate flow.
    #[tokio::test]
    async fn action_oriented_flow() {
        let config = test_config(50);
        let log = MemLog::new();
        let snap_io = MemSnapshotIO::new();
        let sm = CounterSM::new();
        let mut coord = SnapshotCoordinator::new(&config, None);

        let entries = make_entries(0, 50, 2);
        log.append(&entries).await.unwrap();

        // Step 1: HW advances.
        assert!(coord.on_high_watermark_advance(50));

        // Step 2: EventLoop calls prepare_snapshot (synchronous).
        let action = coord
            .prepare_snapshot(50, Term(2), vec![], Term(1), &sm)
            .expect("should produce SaveSnapshot");

        let snapshot = match action {
            SnapshotAction::SaveSnapshot(snap) => snap,
            _ => panic!("expected SaveSnapshot"),
        };
        assert_eq!(snapshot.metadata.last_included_offset, 49);
        assert_eq!(snapshot.metadata.last_included_term, Term(2));

        // Step 3: IoStage saves the snapshot.
        snap_io.save(&snapshot).await.unwrap();

        // Step 4: EventLoop notified of save success.
        let truncate = coord
            .on_snapshot_saved()
            .expect("should produce TruncatePrefix");
        match truncate {
            SnapshotAction::TruncatePrefix(offset) => {
                assert_eq!(offset, 50);
                log.truncate_prefix(offset).await.unwrap();
            }
            _ => panic!("expected TruncatePrefix"),
        }

        // Step 5: Truncation complete.
        coord.on_truncation_completed();

        assert_eq!(log.log_start_offset(), 50);
        assert_eq!(coord.last_snapshot_offset(), Some(49));
        assert_eq!(coord.commits_since_snapshot(), 0);
    }

    /// Does not re-snapshot the same offset.
    #[tokio::test]
    async fn no_duplicate_snapshot() {
        let config = test_config(50);
        let sm = CounterSM::new();
        let mut coord = SnapshotCoordinator::new(&config, None);

        coord.on_high_watermark_advance(50);
        let action = coord.prepare_snapshot(50, Term(1), vec![], Term(1), &sm);
        assert!(action.is_some());

        // Simulate save success.
        coord.on_snapshot_saved();

        // Artificially inflate counter without advancing HW.
        // Even if counter > interval, same offset is rejected.
        coord.commits_since_snapshot = 100;
        let action = coord.prepare_snapshot(50, Term(1), vec![], Term(1), &sm);
        assert!(action.is_none());
    }

    /// Save failure via action API: on_snapshot_save_failed resets state.
    #[tokio::test]
    async fn save_failure_via_action_api() {
        let config = test_config(100);
        let sm = CounterSM::new();
        let mut coord = SnapshotCoordinator::new(&config, None);

        coord.on_high_watermark_advance(100);
        let _action = coord.prepare_snapshot(100, Term(1), vec![], Term(1), &sm);
        assert!(coord.pending_snapshot.is_some());

        // Simulate save failure.
        coord.on_snapshot_save_failed();

        assert!(coord.pending_snapshot.is_none());
        assert_eq!(coord.last_snapshot_offset(), None);
        assert_eq!(coord.commits_since_snapshot(), 0);
        assert!(!coord.snapshot_needed());
    }

    /// Duplicate prepare_snapshot while a save is in-flight is rejected.
    #[tokio::test]
    async fn duplicate_prepare_rejected_while_pending() {
        let config = test_config(50);
        let sm = CounterSM::new();
        let mut coord = SnapshotCoordinator::new(&config, None);

        coord.on_high_watermark_advance(50);
        let first = coord.prepare_snapshot(50, Term(1), vec![], Term(1), &sm);
        assert!(first.is_some());
        assert!(coord.has_pending_snapshot());

        // Second prepare while first save is in-flight — must be rejected.
        coord.commits_since_snapshot = 100; // artificially inflate
        let second = coord.prepare_snapshot(50, Term(1), vec![], Term(1), &sm);
        assert!(second.is_none());

        // snapshot_needed returns false while pending.
        assert!(!coord.snapshot_needed());
    }

    /// Integration-style test: simulates the EventLoop commit path.
    ///
    /// Mirrors the integration pattern documented on SnapshotCoordinator:
    /// EventLoop applies entries → advances HW → checks snapshot_needed →
    /// prepares snapshot → IoStage saves → EventLoop truncates.
    #[tokio::test]
    async fn eventloop_integration_simulation() {
        let config = test_config(10);
        let log = MemLog::new();
        let snap_io = MemSnapshotIO::new();
        let mut sm = CounterSM::new();
        let mut coord = SnapshotCoordinator::new(&config, None);

        // Simulate 25 entries committed in three batches.
        // Batch 1: offsets 0–9 (term 1)
        let batch1 = make_entries(0, 10, 1);
        log.append(&batch1).await.unwrap();
        for e in &batch1 {
            if let Some(ref rec) = e.payload {
                sm.apply(e.offset, rec).unwrap();
            }
        }
        // EventLoop: advance HW after applying committed entries.
        let needs_snapshot = coord.on_high_watermark_advance(10);
        assert!(needs_snapshot);

        // EventLoop: prepare snapshot and push to IoActionBatch.
        if let Some(action) = coord.prepare_snapshot(
            10, Term(1), vec![], Term(1), &sm,
        ) {
            match action {
                SnapshotAction::SaveSnapshot(ref snap) => {
                    // IoStage: persist the snapshot (fsync).
                    snap_io.save(snap).await.unwrap();
                }
                _ => panic!("expected SaveSnapshot"),
            }
            // IoStage reports success → EventLoop gets truncation action.
            if let Some(SnapshotAction::TruncatePrefix(up_to)) = coord.on_snapshot_saved() {
                log.truncate_prefix(up_to).await.unwrap();
                coord.on_truncation_completed();
            }
        }

        assert_eq!(log.log_start_offset(), 10);
        assert_eq!(coord.last_snapshot_offset(), Some(9));
        assert_eq!(coord.last_snapshot_term(), Some(Term(1)));

        // Batch 2: offsets 10–19 (term 2) — another full interval.
        let batch2 = make_entries(10, 10, 2);
        log.append(&batch2).await.unwrap();
        for e in &batch2 {
            if let Some(ref rec) = e.payload {
                sm.apply(e.offset, rec).unwrap();
            }
        }
        let needs_snapshot = coord.on_high_watermark_advance(20);
        assert!(needs_snapshot);

        // Full execute_snapshot convenience path.
        let snapped = coord
            .execute_snapshot(20, Term(2), vec![], Term(2), &log, &snap_io, &sm)
            .await
            .unwrap();
        assert!(snapped);
        assert_eq!(log.log_start_offset(), 20);
        assert_eq!(coord.last_snapshot_offset(), Some(19));

        // Batch 3: offsets 20–24 (term 2) — not enough for another snapshot.
        let batch3 = make_entries(20, 5, 2);
        log.append(&batch3).await.unwrap();
        let needs_snapshot = coord.on_high_watermark_advance(25);
        assert!(!needs_snapshot);
        assert_eq!(coord.commits_since_snapshot(), 5);
    }
}
