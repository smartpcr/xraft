use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use tokio::sync::Mutex;

use xraft_core::error::XraftError;
use xraft_core::log_entry::LogEntry;
use xraft_core::traits::LogStore;
use xraft_core::types::ClusterId;

use crate::segment::{batch_disk_size, Segment, SerializedEntry};

/// Default segment size limit in bytes.
const DEFAULT_MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024; // 64 MiB

/// Default sparse index interval (every Nth entry).
const DEFAULT_INDEX_INTERVAL: u32 = 16;

/// Configuration for the segment log.
pub struct SegmentLogConfig {
    /// Maximum segment file size in bytes before rolling to a new segment.
    /// Must be greater than zero.
    pub max_segment_size: u64,
    /// Sparse index recording interval (every Nth entry).
    /// Must be greater than zero.
    pub index_interval: u32,
}

impl SegmentLogConfig {
    /// Validate configuration values. Returns an error if any value is invalid.
    fn validate(&self) -> io::Result<()> {
        if self.max_segment_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "max_segment_size must be greater than zero",
            ));
        }
        if self.index_interval == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "index_interval must be greater than zero",
            ));
        }
        Ok(())
    }
}

impl Default for SegmentLogConfig {
    fn default() -> Self {
        Self {
            max_segment_size: DEFAULT_MAX_SEGMENT_SIZE,
            index_interval: DEFAULT_INDEX_INTERVAL,
        }
    }
}

/// Internal mutable state protected by a Mutex.
struct SegmentLogInner {
    segments: Vec<Segment>,
}

/// Manages a series of log segments on disk, implementing the `LogStore` trait.
///
/// Directory layout (when created via `open_for_cluster`):
///   `<base_dir>/data/<cluster_id>/log/00000000000000000000.log`
///   `<base_dir>/data/<cluster_id>/log/00000000000000000000.index`
///   …
///
/// Or a custom directory when created via `open`.
pub struct SegmentLog {
    log_dir: PathBuf,
    config: SegmentLogConfig,
    inner: Mutex<SegmentLogInner>,
    /// Atomic start/end offsets for sync accessors.
    start_offset: AtomicU64,
    end_offset: AtomicU64,
}

impl SegmentLog {
    /// Open or create a segment log using the canonical cluster directory
    /// layout: `<base_dir>/data/<cluster_id>/log/`.
    pub fn open_for_cluster(
        base_dir: &Path,
        cluster_id: &ClusterId,
        config: SegmentLogConfig,
    ) -> io::Result<Self> {
        let log_dir = base_dir
            .join("data")
            .join(cluster_id.as_str())
            .join("log");
        Self::open(&log_dir, config)
    }

    /// Open or create a segment log in the given directory.
    pub fn open(log_dir: &Path, config: SegmentLogConfig) -> io::Result<Self> {
        config.validate()?;
        fs::create_dir_all(log_dir)?;

        // Discover existing segment files by scanning for `.log` files.
        let mut base_offsets = Self::discover_segments(log_dir)?;
        base_offsets.sort();

        let mut segments = Vec::new();
        let mut truncate_from: Option<usize> = None;

        for (idx, base) in base_offsets.iter().enumerate() {
            // If a previous segment was truncated (recovery), don't open later ones
            if truncate_from.is_some() {
                break;
            }

            let seg = Segment::open(log_dir, *base, config.index_interval)?;

            // Check offset continuity: this segment's base must match the
            // previous segment's next_offset. If not, the log is torn.
            if let Some(prev) = segments.last() {
                let prev: &Segment = prev;
                if prev.next_offset() != seg.base_offset() {
                    // Previous segment was truncated during recovery — discard
                    // this segment and all subsequent ones.
                    truncate_from = Some(idx);
                    break;
                }
            }

            segments.push(seg);
        }

        // Remove segment files that come after a truncated segment
        if let Some(from_idx) = truncate_from {
            for base in &base_offsets[from_idx..] {
                let stem = crate::segment::segment_filename(*base);
                let _ = fs::remove_file(log_dir.join(format!("{stem}.log")));
                let _ = fs::remove_file(log_dir.join(format!("{stem}.index")));
            }
        }

        // If no segments exist, create the initial segment at offset 0.
        if segments.is_empty() {
            let seg = Segment::create(log_dir, 0, config.index_interval)?;
            segments.push(seg);
        }

        let start_offset = segments.first().map_or(0, |s| s.base_offset());
        let end_offset = segments.last().map_or(0, |s| s.next_offset());

        Ok(Self {
            log_dir: log_dir.to_path_buf(),
            config,
            inner: Mutex::new(SegmentLogInner { segments }),
            start_offset: AtomicU64::new(start_offset),
            end_offset: AtomicU64::new(end_offset),
        })
    }

    /// Discover segment base offsets from `.log` filenames in the directory.
    fn discover_segments(dir: &Path) -> io::Result<Vec<u64>> {
        let mut offsets = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("log") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Ok(offset) = stem.parse::<u64>() {
                        offsets.push(offset);
                    }
                }
            }
        }
        Ok(offsets)
    }

    /// Roll to a new segment. Must be called with inner lock held.
    fn roll_segment(
        inner: &mut SegmentLogInner,
        log_dir: &Path,
        config: &SegmentLogConfig,
    ) -> io::Result<()> {
        let next_base = inner.segments.last().map_or(0, |s| s.next_offset());
        let seg = Segment::create(log_dir, next_base, config.index_interval)?;

        // Sync parent directory to ensure new file entry is durable.
        #[cfg(unix)]
        {
            let dir_file = fs::File::open(log_dir)?;
            dir_file.sync_all()?;
        }

        inner.segments.push(seg);
        Ok(())
    }

    /// Find the segment index containing the given offset.
    fn find_segment(segments: &[Segment], offset: u64) -> Option<usize> {
        if segments.is_empty() {
            return None;
        }
        // Binary search: find the last segment whose base_offset <= offset
        let mut lo = 0;
        let mut hi = segments.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if segments[mid].base_offset() <= offset {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == 0 {
            None
        } else {
            Some(lo - 1)
        }
    }
}

#[async_trait]
impl LogStore for SegmentLog {
    async fn append(&self, entries: &[LogEntry]) -> Result<(), XraftError> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut inner = self.inner.lock().await;

        // Validate offset continuity
        let current_end = self.end_offset.load(Ordering::Acquire);
        if entries[0].offset != current_end {
            return Err(XraftError::StorageError(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "offset discontinuity: expected {}, got {}",
                    current_end, entries[0].offset
                ),
            )));
        }
        for window in entries.windows(2) {
            if window[1].offset != window[0].offset + 1 {
                return Err(XraftError::StorageError(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "non-contiguous offsets: {} followed by {}",
                        window[0].offset, window[1].offset
                    ),
                )));
            }
        }

        // Pre-serialize all entries for size calculation and batch writing
        let serialized: Vec<SerializedEntry> = entries
            .iter()
            .map(SerializedEntry::from_entry)
            .collect::<Result<_, _>>()
            .map_err(XraftError::StorageError)?;

        // Write entries in sub-batches, rolling segments as needed.
        // Each sub-batch is written as a single CRC-protected batch.
        // end_offset is updated after each successful sub-batch to keep
        // the live log consistent even if a later sub-batch fails.
        let mut pos = 0;
        while pos < entries.len() {
            // Roll if active segment is at or over the size limit
            let active_size = inner
                .segments
                .last()
                .expect("at least one segment exists")
                .file_size();
            if active_size > 0 && active_size >= self.config.max_segment_size {
                Self::roll_segment(&mut inner, &self.log_dir, &self.config)?;
            }

            let active_size = inner
                .segments
                .last()
                .expect("at least one segment exists")
                .file_size();

            // Determine how many entries fit in the current segment.
            // Always write at least one entry per sub-batch (even if it
            // alone exceeds the threshold on an empty segment).
            let mut end = pos + 1;

            // If even one entry doesn't fit alongside existing data, roll first
            let one_batch_size = batch_disk_size(&serialized[pos..end]);
            if active_size > 0 && active_size + one_batch_size > self.config.max_segment_size {
                Self::roll_segment(&mut inner, &self.log_dir, &self.config)?;
                // Fresh segment — recalculate with size = 0
            }

            let active_size = inner
                .segments
                .last()
                .expect("at least one segment exists")
                .file_size();

            // Greedily expand the sub-batch while it fits
            while end < entries.len() {
                let candidate_size = batch_disk_size(&serialized[pos..end + 1]);
                if active_size + candidate_size > self.config.max_segment_size {
                    break;
                }
                end += 1;
            }

            // Write the sub-batch as a single CRC-protected batch
            let active = inner
                .segments
                .last_mut()
                .expect("at least one segment exists");
            active
                .append_batch(&entries[pos..end], &serialized[pos..end])
                .map_err(XraftError::StorageError)?;

            // Update end_offset immediately after each successful sub-batch
            // so the live state is consistent with what's durably on disk.
            let new_end = active.next_offset();
            self.end_offset.store(new_end, Ordering::Release);

            pos = end;
        }

        Ok(())
    }

    async fn read(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Vec<LogEntry>, XraftError> {
        if start_offset >= end_offset {
            return Ok(Vec::new());
        }

        let mut inner = self.inner.lock().await;

        let seg_start = Self::find_segment(&inner.segments, start_offset);
        let seg_end = Self::find_segment(&inner.segments, end_offset.saturating_sub(1));

        let seg_start = match seg_start {
            Some(i) => i,
            None => return Ok(Vec::new()),
        };
        let seg_end = seg_end.unwrap_or(inner.segments.len() - 1);

        let mut result = Vec::new();
        for i in seg_start..=seg_end {
            let seg = &mut inner.segments[i];
            let entries = seg
                .read(start_offset, end_offset)
                .map_err(XraftError::StorageError)?;
            result.extend(entries);
        }

        Ok(result)
    }

    async fn truncate_suffix(&self, _from_offset: u64) -> Result<(), XraftError> {
        Err(XraftError::StorageError(io::Error::new(
            io::ErrorKind::Unsupported,
            "truncate_suffix not yet implemented (Stage 2.2)",
        )))
    }

    async fn truncate_prefix(&self, _up_to_offset: u64) -> Result<(), XraftError> {
        Err(XraftError::StorageError(io::Error::new(
            io::ErrorKind::Unsupported,
            "truncate_prefix not yet implemented (Stage 2.2)",
        )))
    }

    fn log_start_offset(&self) -> u64 {
        self.start_offset.load(Ordering::Acquire)
    }

    fn log_end_offset(&self) -> u64 {
        self.end_offset.load(Ordering::Acquire)
    }

    async fn entry_at(&self, offset: u64) -> Result<Option<LogEntry>, XraftError> {
        let current_start = self.log_start_offset();
        let current_end = self.log_end_offset();

        if offset < current_start || offset >= current_end {
            return Ok(None);
        }

        let entries = self.read(offset, offset + 1).await?;
        Ok(entries.into_iter().next())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Seek, Write};
    use tempfile::TempDir;
    use xraft_core::log_entry::EntryType;
    use xraft_core::types::Term;

    fn make_entry(offset: u64, term: u64, payload: &[u8]) -> LogEntry {
        LogEntry {
            offset,
            term: Term(term),
            entry_type: EntryType::Command,
            payload: payload.to_vec(),
        }
    }

    #[tokio::test]
    async fn append_and_read_100_entries() {
        let dir = TempDir::new().unwrap();
        let log = SegmentLog::open(dir.path(), SegmentLogConfig::default()).unwrap();

        let entries: Vec<LogEntry> = (0..100)
            .map(|i| make_entry(i, 1 + i / 10, &format!("payload-{i}").into_bytes()))
            .collect();

        log.append(&entries).await.unwrap();

        assert_eq!(log.log_start_offset(), 0);
        assert_eq!(log.log_end_offset(), 100);

        let read_back = log.read(0, 100).await.unwrap();
        assert_eq!(read_back.len(), 100);
        for (i, entry) in read_back.iter().enumerate() {
            let i = i as u64;
            assert_eq!(entry.offset, i);
            assert_eq!(entry.term, Term(1 + i / 10));
            assert_eq!(entry.payload, format!("payload-{i}").into_bytes());
        }
    }

    /// Corrupt a byte mid-segment on a live (non-reopened) SegmentLog and verify
    /// that reading past the corruption returns `StorageError(InvalidData)`.
    #[tokio::test]
    async fn crc_integrity_live_read_returns_storage_error() {
        let dir = TempDir::new().unwrap();
        let log = SegmentLog::open(dir.path(), SegmentLogConfig::default()).unwrap();

        // Write 10 entries as individual batches (separate append calls)
        for i in 0..10u64 {
            log.append(&[make_entry(i, 1, &[i as u8; 32])])
                .await
                .unwrap();
        }

        // Corrupt batch 5's payload on disk without closing the SegmentLog.
        // Each batch has the same size; target the entry data inside batch 5.
        let log_path = dir.path().join("00000000000000000000.log");
        {
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&log_path)
                .unwrap();
            let mut raw = Vec::new();
            f.read_to_end(&mut raw).unwrap();
            let batch_size = raw.len() / 10;
            let corrupt_pos = batch_size * 5 + 14; // inside entry data
            raw[corrupt_pos] ^= 0xFF;
            f.seek(std::io::SeekFrom::Start(0)).unwrap();
            f.write_all(&raw).unwrap();
            f.sync_all().unwrap();
        }

        // Reading through the same SegmentLog handle must return StorageError
        let result = log.read(0, 10).await;
        match result {
            Err(XraftError::StorageError(ref e)) => {
                assert_eq!(e.kind(), io::ErrorKind::InvalidData);
                assert!(
                    e.to_string().contains("CRC"),
                    "expected CRC error, got: {e}"
                );
            }
            Ok(_) => panic!("expected StorageError from CRC mismatch, got Ok"),
            Err(other) => panic!("expected StorageError, got: {other}"),
        }
    }

    #[tokio::test]
    async fn segment_rollover() {
        let dir = TempDir::new().unwrap();
        let config = SegmentLogConfig {
            max_segment_size: 1024, // 1 KB
            index_interval: 4,
        };
        let log = SegmentLog::open(dir.path(), config).unwrap();

        // Write 100 entries in batches of 5 to trigger multiple rollovers.
        for batch_start in (0..100).step_by(5) {
            let entries: Vec<LogEntry> = (batch_start..batch_start + 5)
                .map(|i| make_entry(i, 1, &[0xABu8; 32]))
                .collect();
            log.append(&entries).await.unwrap();
        }

        assert_eq!(log.log_end_offset(), 100);

        // Verify multiple segment files were created
        let log_files: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .and_then(|ext| ext.to_str())
                    == Some("log")
            })
            .collect();
        assert!(
            log_files.len() > 1,
            "expected multiple segment files, got {}",
            log_files.len()
        );

        // Verify no segment file exceeds the size threshold (except possibly
        // the very first batch on an empty segment, which is always allowed).
        for f in &log_files {
            let size = f.metadata().unwrap().len();
            // A single batch may slightly exceed the threshold when the segment
            // was empty. But it must not be wildly over.
            assert!(
                size <= 1024 + 512,
                "segment {} is {} bytes, exceeds 1 KB + tolerance",
                f.path().display(),
                size
            );
        }

        // Read all entries back across segments
        let read_back = log.read(0, 100).await.unwrap();
        assert_eq!(read_back.len(), 100);
        for (i, entry) in read_back.iter().enumerate() {
            assert_eq!(entry.offset, i as u64);
        }
    }

    #[tokio::test]
    async fn append_rejects_offset_discontinuity() {
        let dir = TempDir::new().unwrap();
        let log = SegmentLog::open(dir.path(), SegmentLogConfig::default()).unwrap();

        let entries = vec![make_entry(0, 1, b"a"), make_entry(1, 1, b"b")];
        log.append(&entries).await.unwrap();

        // Gap: offset 5 instead of 2
        let bad = vec![make_entry(5, 1, b"c")];
        let err = log.append(&bad).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn entry_at_returns_correct_entry() {
        let dir = TempDir::new().unwrap();
        let log = SegmentLog::open(dir.path(), SegmentLogConfig::default()).unwrap();

        let entries: Vec<LogEntry> = (0..10)
            .map(|i| make_entry(i, i + 1, &[i as u8; 4]))
            .collect();
        log.append(&entries).await.unwrap();

        let e = log.entry_at(5).await.unwrap().unwrap();
        assert_eq!(e.offset, 5);
        assert_eq!(e.term, Term(6));

        assert!(log.entry_at(10).await.unwrap().is_none());
        assert!(log.entry_at(100).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn read_partial_range() {
        let dir = TempDir::new().unwrap();
        let log = SegmentLog::open(dir.path(), SegmentLogConfig::default()).unwrap();

        let entries: Vec<LogEntry> = (0..50)
            .map(|i| make_entry(i, 1, &[i as u8]))
            .collect();
        log.append(&entries).await.unwrap();

        let partial = log.read(10, 20).await.unwrap();
        assert_eq!(partial.len(), 10);
        assert_eq!(partial[0].offset, 10);
        assert_eq!(partial[9].offset, 19);
    }

    #[tokio::test]
    async fn read_empty_range() {
        let dir = TempDir::new().unwrap();
        let log = SegmentLog::open(dir.path(), SegmentLogConfig::default()).unwrap();

        let result = log.read(0, 0).await.unwrap();
        assert!(result.is_empty());

        let result = log.read(5, 3).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn append_empty_is_noop() {
        let dir = TempDir::new().unwrap();
        let log = SegmentLog::open(dir.path(), SegmentLogConfig::default()).unwrap();
        log.append(&[]).await.unwrap();
        assert_eq!(log.log_end_offset(), 0);
    }

    /// Verify the canonical `data/<cluster_id>/log/` directory layout via
    /// the `open_for_cluster` constructor.
    #[tokio::test]
    async fn directory_layout_via_open_for_cluster() {
        let dir = TempDir::new().unwrap();
        let cluster_id = ClusterId::random();
        let log = SegmentLog::open_for_cluster(
            dir.path(),
            &cluster_id,
            SegmentLogConfig::default(),
        )
        .unwrap();

        let entries = vec![make_entry(0, 1, b"hello")];
        log.append(&entries).await.unwrap();

        let log_dir = dir
            .path()
            .join("data")
            .join(cluster_id.as_str())
            .join("log");
        assert!(log_dir.exists(), "cluster log directory should be created");
        assert!(log_dir.join("00000000000000000000.log").exists());
        assert!(log_dir.join("00000000000000000000.index").exists());
    }

    #[tokio::test]
    async fn config_rejects_zero_max_segment_size() {
        let dir = TempDir::new().unwrap();
        let config = SegmentLogConfig {
            max_segment_size: 0,
            index_interval: 16,
        };
        let result = SegmentLog::open(dir.path(), config);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn config_rejects_zero_index_interval() {
        let dir = TempDir::new().unwrap();
        let config = SegmentLogConfig {
            max_segment_size: 1024,
            index_interval: 0,
        };
        let result = SegmentLog::open(dir.path(), config);
        assert!(result.is_err());
    }
}
