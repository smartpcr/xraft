use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use tracing::{debug, info, warn};
use xraft_core::log_entry::LogEntry;
use xraft_core::traits::LogStore;
use xraft_core::XraftError;

use crate::segment::Segment;

const LOG_META_FILENAME: &str = "log_meta";

/// Multi-segment log store implementing the `LogStore` trait.
///
/// Manages a series of `Segment` files in a directory. Segments are rolled
/// when the active segment reaches `max_bytes`. Interior mutability is
/// provided via `std::sync::Mutex` (no await points while the lock is held).
pub struct SegmentLog {
    inner: Mutex<SegmentLogInner>,
    /// Cached start offset, updated after durable mutations.
    start_offset: AtomicU64,
    /// Cached end offset (next offset to be written).
    end_offset: AtomicU64,
}

struct SegmentLogInner {
    dir: PathBuf,
    segments: Vec<Segment>,
    log_start_offset: u64,
    max_bytes: u64,
    index_interval: u32,
}

impl SegmentLog {
    /// Open or create a segment log in the given directory.
    pub fn open(dir: &Path, max_bytes: u64, index_interval: u32) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;

        let persisted_start = Self::load_meta(dir)?;

        // Discover existing segment files.
        let mut base_offsets = Self::discover_segments(dir)?;
        base_offsets.sort();

        let mut segments = Vec::new();
        for &base in &base_offsets {
            match Segment::open(dir, base, max_bytes, index_interval) {
                Ok(seg) => segments.push(seg),
                Err(e) => {
                    warn!(base_offset = base, error = %e, "failed to open segment, stopping here");
                    // Remove this and all subsequent segments — they are after corruption.
                    break;
                }
            }
        }

        // Validate cross-segment continuity: each segment's base_offset must equal
        // the previous segment's next_offset.
        let mut valid_count = segments.len();
        for i in 1..segments.len() {
            let prev_next = segments[i - 1].next_offset();
            let curr_base = segments[i].base_offset();
            if curr_base != prev_next {
                warn!(
                    prev_next_offset = prev_next,
                    segment_base = curr_base,
                    "segment gap/overlap detected, discarding segment {} and beyond", i
                );
                valid_count = i;
                break;
            }
        }

        // Remove segments beyond the valid chain.
        while segments.len() > valid_count {
            if let Some(seg) = segments.pop() {
                let _ = seg.remove();
            }
        }

        // Remove empty segments that aren't the tail.
        // (Keep the last segment even if empty — it's the active segment.)
        while segments.len() > 1 && segments.last().map_or(false, |s| s.is_empty()) {
            // Actually, an empty tail segment is fine. Only remove empty
            // non-tail segments that would break the chain.
            break;
        }

        // Determine logical start offset.
        let log_start_offset = if let Some(persisted) = persisted_start {
            // Use persisted value, but ensure it's not before the first segment.
            if segments.is_empty() {
                persisted
            } else {
                persisted.max(segments[0].base_offset())
            }
        } else if segments.is_empty() {
            0
        } else {
            segments[0].base_offset()
        };

        let log_end_offset = segments
            .last()
            .map_or(log_start_offset, |s| s.next_offset());

        // If no segments exist, create an initial one.
        if segments.is_empty() {
            let seg = Segment::create(dir, log_start_offset, max_bytes, index_interval)?;
            segments.push(seg);
        }

        info!(
            dir = %dir.display(),
            num_segments = segments.len(),
            log_start_offset,
            log_end_offset,
            "segment log opened"
        );

        Ok(Self {
            inner: Mutex::new(SegmentLogInner {
                dir: dir.to_path_buf(),
                segments,
                log_start_offset,
                max_bytes,
                index_interval,
            }),
            start_offset: AtomicU64::new(log_start_offset),
            end_offset: AtomicU64::new(log_end_offset),
        })
    }

    /// Discover segment base offsets from `.log` filenames in the directory.
    fn discover_segments(dir: &Path) -> io::Result<Vec<u64>> {
        let mut offsets = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(stem) = name_str.strip_suffix(".log") {
                if stem.len() == 20 {
                    if let Ok(base) = stem.parse::<u64>() {
                        offsets.push(base);
                    }
                }
            }
        }
        Ok(offsets)
    }

    /// Load the persisted `log_start_offset` from the metadata file.
    fn load_meta(dir: &Path) -> io::Result<Option<u64>> {
        let meta_path = dir.join(LOG_META_FILENAME);
        match std::fs::read_to_string(&meta_path) {
            Ok(content) => {
                let trimmed = content.trim();
                match trimmed.parse::<u64>() {
                    Ok(v) => Ok(Some(v)),
                    Err(_) => {
                        warn!(path = %meta_path.display(), "corrupt log_meta, ignoring");
                        Ok(None)
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Persist `log_start_offset` atomically.
    fn persist_meta(dir: &Path, log_start_offset: u64) -> io::Result<()> {
        let meta_path = dir.join(LOG_META_FILENAME);
        let tmp_path = dir.join(format!("{LOG_META_FILENAME}.tmp"));
        std::fs::write(&tmp_path, log_start_offset.to_string())?;

        // fsync the temp file.
        let f = std::fs::File::open(&tmp_path)?;
        f.sync_all()?;
        drop(f);

        // Atomic rename.
        std::fs::rename(&tmp_path, &meta_path)?;
        Ok(())
    }

    /// Find the index of the segment that contains `offset`.
    fn find_segment_idx(segments: &[Segment], offset: u64) -> Option<usize> {
        if segments.is_empty() {
            return None;
        }
        // Binary search: find the last segment whose base_offset <= offset.
        let idx = segments.partition_point(|s| s.base_offset() <= offset);
        if idx == 0 {
            return None;
        }
        let seg_idx = idx - 1;
        // Check that offset is within this segment's range.
        if offset < segments[seg_idx].next_offset() {
            Some(seg_idx)
        } else {
            None
        }
    }
}

#[async_trait]
impl LogStore for SegmentLog {
    async fn append(&self, entries: &[LogEntry]) -> xraft_core::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut guard = self.inner.lock().map_err(|_| {
            XraftError::StorageError(io::Error::new(io::ErrorKind::Other, "lock poisoned"))
        })?;

        let inner = &mut *guard;

        // Validate contiguous offsets starting from current end.
        let current_end = inner
            .segments
            .last()
            .map_or(inner.log_start_offset, |s| s.next_offset());

        for (i, entry) in entries.iter().enumerate() {
            let expected = current_end + i as u64;
            if entry.offset != expected {
                return Err(XraftError::StorageError(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "non-contiguous offset: expected {expected}, got {}",
                        entry.offset
                    ),
                )));
            }
        }

        for entry in entries {
            // Roll to new segment if current is full.
            if inner.segments.last().map_or(true, |s| s.is_full()) {
                let base = inner
                    .segments
                    .last()
                    .map_or(inner.log_start_offset, |s| s.next_offset());
                let seg =
                    Segment::create(&inner.dir, base, inner.max_bytes, inner.index_interval)?;
                inner.segments.push(seg);
            }

            let active = inner
                .segments
                .last_mut()
                .expect("segments must be non-empty");
            active.append_entry(entry)?;
        }

        // Fsync the active segment.
        if let Some(active) = inner.segments.last() {
            active.flush()?;
        }

        // Update atomic end offset after durable write.
        let new_end = inner
            .segments
            .last()
            .map_or(inner.log_start_offset, |s| s.next_offset());
        self.end_offset.store(new_end, Ordering::Release);

        Ok(())
    }

    async fn read(&self, start_offset: u64, end_offset: u64) -> xraft_core::Result<Vec<LogEntry>> {
        let guard = self.inner.lock().map_err(|_| {
            XraftError::StorageError(io::Error::new(io::ErrorKind::Other, "lock poisoned"))
        })?;

        let inner = &*guard;

        // Clamp to valid range.
        let effective_start = start_offset.max(inner.log_start_offset);
        let log_end = inner
            .segments
            .last()
            .map_or(inner.log_start_offset, |s| s.next_offset());
        let effective_end = end_offset.min(log_end);

        if effective_start >= effective_end {
            return Ok(Vec::new());
        }

        let mut result = Vec::new();

        // Find the first relevant segment.
        let start_idx = Self::find_segment_idx(&inner.segments, effective_start)
            .unwrap_or(0);

        for seg in &inner.segments[start_idx..] {
            if seg.base_offset() >= effective_end {
                break;
            }
            let entries = seg.read_range(effective_start, effective_end)?;
            result.extend(entries);
        }

        Ok(result)
    }

    async fn entry_at(&self, offset: u64) -> xraft_core::Result<Option<LogEntry>> {
        let end = match offset.checked_add(1) {
            Some(end) => end,
            None => return Ok(None), // u64::MAX overflow guard
        };
        let entries = self.read(offset, end).await?;
        Ok(entries.into_iter().next())
    }

    async fn truncate_suffix(&self, from_offset: u64) -> xraft_core::Result<()> {
        let mut guard = self.inner.lock().map_err(|_| {
            XraftError::StorageError(io::Error::new(io::ErrorKind::Other, "lock poisoned"))
        })?;

        let inner = &mut *guard;

        let log_end = inner
            .segments
            .last()
            .map_or(inner.log_start_offset, |s| s.next_offset());

        if from_offset >= log_end {
            return Ok(());
        }

        // Clamp: cannot truncate below start.
        let effective_from = from_offset.max(inner.log_start_offset);

        // Find the segment containing effective_from.
        let seg_idx = Self::find_segment_idx(&inner.segments, effective_from);

        match seg_idx {
            Some(idx) => {
                // Remove all segments after idx.
                while inner.segments.len() > idx + 1 {
                    let seg = inner.segments.pop().unwrap();
                    seg.remove()?;
                }
                // Truncate the segment at effective_from.
                inner.segments[idx].truncate_at(effective_from)?;

                // If the truncated segment is now empty and it's not the only one,
                // we might remove it. But keep it as the active segment.
            }
            None => {
                // effective_from is before all segments — truncate everything.
                while let Some(seg) = inner.segments.pop() {
                    seg.remove()?;
                }
                // Create a fresh segment at log_start_offset.
                let seg = Segment::create(
                    &inner.dir,
                    inner.log_start_offset,
                    inner.max_bytes,
                    inner.index_interval,
                )?;
                inner.segments.push(seg);
            }
        }

        let new_end = inner
            .segments
            .last()
            .map_or(inner.log_start_offset, |s| s.next_offset());
        self.end_offset.store(new_end, Ordering::Release);

        Ok(())
    }

    async fn truncate_prefix(&self, up_to_offset: u64) -> xraft_core::Result<()> {
        let mut guard = self.inner.lock().map_err(|_| {
            XraftError::StorageError(io::Error::new(io::ErrorKind::Other, "lock poisoned"))
        })?;

        let inner = &mut *guard;

        if up_to_offset <= inner.log_start_offset {
            return Ok(());
        }

        // Clamp up_to_offset to log_end so we never set start past end.
        let log_end = inner
            .segments
            .last()
            .map_or(inner.log_start_offset, |s| s.next_offset());
        let effective_offset = up_to_offset.min(log_end);

        // Remove segments whose entries are ALL before effective_offset
        // (i.e., segment.next_offset() <= effective_offset).
        while !inner.segments.is_empty() {
            if inner.segments[0].next_offset() <= effective_offset {
                let seg = inner.segments.remove(0);
                debug!(
                    base_offset = seg.base_offset(),
                    next_offset = seg.next_offset(),
                    "removing prefix segment"
                );
                seg.remove()?;
            } else {
                break;
            }
        }

        // If all segments were removed, create a fresh one at the new start.
        if inner.segments.is_empty() {
            let seg = Segment::create(
                &inner.dir,
                effective_offset,
                inner.max_bytes,
                inner.index_interval,
            )?;
            inner.segments.push(seg);
        }

        // Update logical start offset.
        inner.log_start_offset = effective_offset;

        // Persist the new start offset.
        Self::persist_meta(&inner.dir, effective_offset)?;

        // Update end_offset BEFORE start_offset to avoid a window where start > end.
        let new_end = inner
            .segments
            .last()
            .map_or(effective_offset, |s| s.next_offset().max(effective_offset));
        self.end_offset.store(new_end, Ordering::Release);

        self.start_offset.store(effective_offset, Ordering::Release);

        Ok(())
    }

    fn log_start_offset(&self) -> u64 {
        self.start_offset.load(Ordering::Acquire)
    }

    fn log_end_offset(&self) -> u64 {
        self.end_offset.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tempfile::TempDir;
    use xraft_core::log_entry::EntryType;
    use xraft_core::types::Term;

    fn make_entry(offset: u64, term: u64) -> LogEntry {
        LogEntry {
            offset,
            term: Term(term),
            entry_type: EntryType::Command,
            payload: Bytes::from(format!("payload-{offset}")),
        }
    }

    fn make_entries(start: u64, end: u64) -> Vec<LogEntry> {
        (start..end).map(|i| make_entry(i, 1)).collect()
    }

    #[tokio::test]
    async fn test_open_empty_directory() {
        let dir = TempDir::new().unwrap();
        let log = SegmentLog::open(dir.path(), 1024 * 1024, 4).unwrap();
        assert_eq!(log.log_start_offset(), 0);
        assert_eq!(log.log_end_offset(), 0);
    }

    #[tokio::test]
    async fn test_append_and_read_back() {
        let dir = TempDir::new().unwrap();
        let log = SegmentLog::open(dir.path(), 1024 * 1024, 4).unwrap();

        let entries = make_entries(0, 100);
        log.append(&entries).await.unwrap();

        assert_eq!(log.log_end_offset(), 100);

        let read = log.read(0, 100).await.unwrap();
        assert_eq!(read.len(), 100);
        for (i, entry) in read.iter().enumerate() {
            assert_eq!(entry.offset, i as u64);
        }
    }

    #[tokio::test]
    async fn test_entry_at() {
        let dir = TempDir::new().unwrap();
        let log = SegmentLog::open(dir.path(), 1024 * 1024, 4).unwrap();

        let entries = make_entries(0, 10);
        log.append(&entries).await.unwrap();

        let entry = log.entry_at(5).await.unwrap();
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().offset, 5);

        // Out of bounds.
        assert!(log.entry_at(10).await.unwrap().is_none());
        assert!(log.entry_at(100).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_truncate_suffix() {
        let dir = TempDir::new().unwrap();
        let log = SegmentLog::open(dir.path(), 1024 * 1024, 4).unwrap();

        let entries = make_entries(0, 100);
        log.append(&entries).await.unwrap();

        log.truncate_suffix(50).await.unwrap();

        assert_eq!(log.log_end_offset(), 50);

        let read = log.read(0, 100).await.unwrap();
        assert_eq!(read.len(), 50);
        for (i, entry) in read.iter().enumerate() {
            assert_eq!(entry.offset, i as u64);
        }

        // entry_at boundary
        assert!(log.entry_at(49).await.unwrap().is_some());
        assert!(log.entry_at(50).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_truncate_prefix_multi_segment() {
        let dir = TempDir::new().unwrap();
        // Small segment size to force multiple segments.
        let log = SegmentLog::open(dir.path(), 256, 4).unwrap();

        let entries = make_entries(0, 3000);
        log.append(&entries).await.unwrap();

        // Verify we have multiple segments.
        {
            let guard = log.inner.lock().unwrap();
            assert!(guard.segments.len() >= 3, "expected at least 3 segments, got {}", guard.segments.len());
        }

        log.truncate_prefix(1000).await.unwrap();

        assert_eq!(log.log_start_offset(), 1000);

        // Entries before 1000 should be gone.
        let read = log.read(0, 1000).await.unwrap();
        assert!(read.is_empty());

        // Entries from 1000 onward should still be readable.
        let read = log.read(1000, 1100).await.unwrap();
        assert_eq!(read.len(), 100);
        assert_eq!(read[0].offset, 1000);
    }

    #[tokio::test]
    async fn test_recovery_after_crash() {
        let dir = TempDir::new().unwrap();

        // Write some entries.
        {
            let log = SegmentLog::open(dir.path(), 1024 * 1024, 4).unwrap();
            let entries = make_entries(0, 50);
            log.append(&entries).await.unwrap();
        }

        // Corrupt the last record by appending garbage bytes.
        {
            let mut log_files: Vec<_> = std::fs::read_dir(dir.path())
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().map_or(false, |ext| ext == "log"))
                .collect();
            log_files.sort_by_key(|e| e.path());

            let last_log = log_files.last().unwrap().path();
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&last_log)
                .unwrap();
            // Write a partial/corrupt record: valid-looking header but bad CRC.
            let bad_crc: u32 = 0xDEAD_BEEF;
            let bad_len: u32 = 100;
            f.write_all(&bad_crc.to_le_bytes()).unwrap();
            f.write_all(&bad_len.to_le_bytes()).unwrap();
            // Write fewer bytes than bad_len claims.
            f.write_all(&[0u8; 50]).unwrap();
            f.flush().unwrap();
        }

        // Reopen — recovery should truncate the corrupt record.
        let log = SegmentLog::open(dir.path(), 1024 * 1024, 4).unwrap();

        assert_eq!(log.log_end_offset(), 50);

        let read = log.read(0, 50).await.unwrap();
        assert_eq!(read.len(), 50);
        for (i, entry) in read.iter().enumerate() {
            assert_eq!(entry.offset, i as u64);
        }
    }

    #[tokio::test]
    async fn test_segment_rollover() {
        let dir = TempDir::new().unwrap();
        let log = SegmentLog::open(dir.path(), 256, 4).unwrap();

        let entries = make_entries(0, 100);
        log.append(&entries).await.unwrap();

        {
            let guard = log.inner.lock().unwrap();
            assert!(guard.segments.len() > 1, "expected multiple segments");
        }

        // Read back all entries.
        let read = log.read(0, 100).await.unwrap();
        assert_eq!(read.len(), 100);
    }

    #[tokio::test]
    async fn test_reopen_preserves_data() {
        let dir = TempDir::new().unwrap();
        let dir_path = dir.path().to_path_buf();

        {
            let log = SegmentLog::open(&dir_path, 256, 4).unwrap();
            let entries = make_entries(0, 200);
            log.append(&entries).await.unwrap();
        }

        // Reopen.
        let log = SegmentLog::open(&dir_path, 256, 4).unwrap();
        assert_eq!(log.log_start_offset(), 0);
        assert_eq!(log.log_end_offset(), 200);

        let read = log.read(0, 200).await.unwrap();
        assert_eq!(read.len(), 200);
    }

    #[tokio::test]
    async fn test_truncate_prefix_persists_across_restart() {
        let dir = TempDir::new().unwrap();
        let dir_path = dir.path().to_path_buf();

        {
            let log = SegmentLog::open(&dir_path, 256, 4).unwrap();
            let entries = make_entries(0, 500);
            log.append(&entries).await.unwrap();
            log.truncate_prefix(200).await.unwrap();
            assert_eq!(log.log_start_offset(), 200);
        }

        // Reopen — start offset should be preserved.
        let log = SegmentLog::open(&dir_path, 256, 4).unwrap();
        assert_eq!(log.log_start_offset(), 200);

        // Entries before 200 should not be readable.
        assert!(log.entry_at(199).await.unwrap().is_none());
        assert!(log.entry_at(200).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_append_non_contiguous_fails() {
        let dir = TempDir::new().unwrap();
        let log = SegmentLog::open(dir.path(), 1024 * 1024, 4).unwrap();

        let entries = make_entries(0, 5);
        log.append(&entries).await.unwrap();

        // Try to append with a gap.
        let bad_entries = vec![make_entry(10, 1)];
        let result = log.append(&bad_entries).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_truncate_suffix_then_append() {
        let dir = TempDir::new().unwrap();
        let log = SegmentLog::open(dir.path(), 1024 * 1024, 4).unwrap();

        let entries = make_entries(0, 100);
        log.append(&entries).await.unwrap();

        log.truncate_suffix(50).await.unwrap();

        // Append new entries starting from 50.
        let new_entries: Vec<_> = (50..80).map(|i| make_entry(i, 2)).collect();
        log.append(&new_entries).await.unwrap();

        assert_eq!(log.log_end_offset(), 80);

        let read = log.read(50, 80).await.unwrap();
        assert_eq!(read.len(), 30);
        // New entries should have term 2.
        for entry in &read {
            assert_eq!(entry.term, Term(2));
        }
    }

    #[tokio::test]
    async fn test_truncate_suffix_noop_at_end() {
        let dir = TempDir::new().unwrap();
        let log = SegmentLog::open(dir.path(), 1024 * 1024, 4).unwrap();

        let entries = make_entries(0, 10);
        log.append(&entries).await.unwrap();

        // Truncate at end is a no-op.
        log.truncate_suffix(10).await.unwrap();
        assert_eq!(log.log_end_offset(), 10);

        // Truncate beyond end is a no-op.
        log.truncate_suffix(100).await.unwrap();
        assert_eq!(log.log_end_offset(), 10);
    }

    #[tokio::test]
    async fn test_recovery_corrupt_crc_mid_segment() {
        let dir = TempDir::new().unwrap();

        // Write entries.
        {
            let log = SegmentLog::open(dir.path(), 1024 * 1024, 4).unwrap();
            let entries = make_entries(0, 20);
            log.append(&entries).await.unwrap();
        }

        // Corrupt a byte in the middle of the segment file.
        {
            let mut log_files: Vec<_> = std::fs::read_dir(dir.path())
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().map_or(false, |ext| ext == "log"))
                .collect();
            log_files.sort_by_key(|e| e.path());
            let log_file = log_files[0].path();

            let mut data = std::fs::read(&log_file).unwrap();
            // Corrupt a byte somewhere in the middle (around entry 10).
            let corrupt_pos = data.len() / 2;
            data[corrupt_pos] ^= 0xFF;
            std::fs::write(&log_file, &data).unwrap();
        }

        // Reopen — should recover up to the corrupt entry.
        let log = SegmentLog::open(dir.path(), 1024 * 1024, 4).unwrap();

        let end = log.log_end_offset();
        assert!(end > 0, "some entries should survive");
        assert!(end < 20, "corrupt entry and after should be truncated");

        let read = log.read(0, end).await.unwrap();
        assert_eq!(read.len(), end as usize);
    }

    #[tokio::test]
    async fn test_truncate_prefix_deletes_segment_files() {
        let dir = TempDir::new().unwrap();
        // Small segments to get multiple files.
        let log = SegmentLog::open(dir.path(), 256, 4).unwrap();

        let entries = make_entries(0, 3000);
        log.append(&entries).await.unwrap();

        // Snapshot segment info (base_offset, next_offset, path) before truncation.
        let segments_before: Vec<(u64, u64, PathBuf, PathBuf)> = {
            let guard = log.inner.lock().unwrap();
            guard.segments.iter().map(|s| {
                let base = s.base_offset();
                let next = s.next_offset();
                let log_path = s.log_path().to_path_buf();
                let idx_path = dir.path().join(crate::segment::Segment::filename(base, "index"));
                (base, next, log_path, idx_path)
            }).collect()
        };
        assert!(segments_before.len() >= 3, "need at least 3 segment files");

        log.truncate_prefix(1000).await.unwrap();

        // Every segment whose entries are entirely before offset 1000
        // (next_offset <= 1000) must have its files deleted.
        for (base, next, log_path, idx_path) in &segments_before {
            if *next <= 1000 {
                assert!(
                    !log_path.exists(),
                    "segment log file {:?} (base={}, next={}) should have been deleted",
                    log_path, base, next
                );
                assert!(
                    !idx_path.exists(),
                    "segment index file {:?} (base={}, next={}) should have been deleted",
                    idx_path, base, next
                );
            }
        }

        // Remaining segment files should all have base_offset such that they
        // contain entries at or after 1000.
        let remaining_log_files: Vec<PathBuf> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "log"))
            .map(|e| e.path())
            .collect();
        assert!(
            remaining_log_files.len() < segments_before.len(),
            "some segment files should have been deleted: before={}, after={}",
            segments_before.len(),
            remaining_log_files.len()
        );

        // Data at and after 1000 is still readable.
        let read = log.read(1000, 1100).await.unwrap();
        assert_eq!(read.len(), 100);
        assert_eq!(read[0].offset, 1000);
    }

    #[tokio::test]
    async fn test_truncate_prefix_all_segments_before_offset() {
        let dir = TempDir::new().unwrap();
        let log = SegmentLog::open(dir.path(), 256, 4).unwrap();

        let entries = make_entries(0, 100);
        log.append(&entries).await.unwrap();

        let end = log.log_end_offset();

        // Truncate prefix at or beyond all entries — should delete all segment files
        // and create a fresh empty segment.
        log.truncate_prefix(end).await.unwrap();

        assert_eq!(log.log_start_offset(), end);
        assert_eq!(log.log_end_offset(), end);

        // Should be able to read (empty result).
        let read = log.read(0, end + 10).await.unwrap();
        assert!(read.is_empty());
    }

    #[tokio::test]
    async fn test_truncate_prefix_beyond_end_clamps() {
        let dir = TempDir::new().unwrap();
        let log = SegmentLog::open(dir.path(), 1024 * 1024, 4).unwrap();

        let entries = make_entries(0, 50);
        log.append(&entries).await.unwrap();

        // Truncate prefix way beyond end.
        log.truncate_prefix(99999).await.unwrap();

        // start should be clamped to end, not 99999.
        assert_eq!(log.log_start_offset(), 50);
        assert_eq!(log.log_end_offset(), 50);
        assert!(log.log_start_offset() <= log.log_end_offset());
    }

    #[tokio::test]
    async fn test_recovery_full_length_corrupt_crc_record() {
        let dir = TempDir::new().unwrap();

        // Write some entries.
        {
            let log = SegmentLog::open(dir.path(), 1024 * 1024, 4).unwrap();
            let entries = make_entries(0, 50);
            log.append(&entries).await.unwrap();
        }

        // Append a full-length record with valid length but bad CRC.
        {
            let mut log_files: Vec<_> = std::fs::read_dir(dir.path())
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().map_or(false, |ext| ext == "log"))
                .collect();
            log_files.sort_by_key(|e| e.path());

            let last_log = log_files.last().unwrap().path();

            // Serialize a fake entry to get realistic data.
            let fake_entry = make_entry(50, 1);
            let entry_data = bincode::serialize(&fake_entry).unwrap();
            let entry_len = entry_data.len() as u32;

            // Compute the correct CRC, then corrupt it.
            let mut crc_payload = Vec::with_capacity(4 + entry_data.len());
            crc_payload.extend_from_slice(&entry_len.to_le_bytes());
            crc_payload.extend_from_slice(&entry_data);
            let bad_crc = crc32c::crc32c(&crc_payload) ^ 0xFFFF_FFFF; // flip all bits

            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&last_log)
                .unwrap();
            f.write_all(&bad_crc.to_le_bytes()).unwrap();
            f.write_all(&entry_len.to_le_bytes()).unwrap();
            f.write_all(&entry_data).unwrap();
            f.flush().unwrap();
        }

        // Reopen — recovery should truncate the corrupt-CRC record.
        let log = SegmentLog::open(dir.path(), 1024 * 1024, 4).unwrap();

        assert_eq!(log.log_end_offset(), 50, "corrupt record should be truncated");

        let read = log.read(0, 50).await.unwrap();
        assert_eq!(read.len(), 50);
        for (i, entry) in read.iter().enumerate() {
            assert_eq!(entry.offset, i as u64);
        }
    }

    #[tokio::test]
    async fn test_recovery_wrong_offset_in_record() {
        let dir = TempDir::new().unwrap();

        // Write some entries.
        {
            let log = SegmentLog::open(dir.path(), 1024 * 1024, 4).unwrap();
            let entries = make_entries(0, 10);
            log.append(&entries).await.unwrap();
        }

        // Append a valid-CRC record but with wrong offset (999 instead of 10).
        {
            let mut log_files: Vec<_> = std::fs::read_dir(dir.path())
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().map_or(false, |ext| ext == "log"))
                .collect();
            log_files.sort_by_key(|e| e.path());

            let last_log = log_files.last().unwrap().path();

            let wrong_entry = make_entry(999, 1); // wrong offset
            let entry_data = bincode::serialize(&wrong_entry).unwrap();
            let entry_len = entry_data.len() as u32;

            let mut crc_payload = Vec::with_capacity(4 + entry_data.len());
            crc_payload.extend_from_slice(&entry_len.to_le_bytes());
            crc_payload.extend_from_slice(&entry_data);
            let crc = crc32c::crc32c(&crc_payload);

            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&last_log)
                .unwrap();
            f.write_all(&crc.to_le_bytes()).unwrap();
            f.write_all(&entry_len.to_le_bytes()).unwrap();
            f.write_all(&entry_data).unwrap();
            f.flush().unwrap();
        }

        // Reopen — recovery should reject the wrong-offset record even though CRC is valid.
        let log = SegmentLog::open(dir.path(), 1024 * 1024, 4).unwrap();

        assert_eq!(log.log_end_offset(), 10, "wrong-offset record should be truncated");
    }
}
