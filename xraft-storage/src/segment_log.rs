use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use tokio::sync::Mutex;
use xraft_core::traits::LogStore;
use xraft_core::{LogEntry, Result, XraftError};

use crate::segment::Segment;

/// Default segment size limit in bytes (64 MB).
const DEFAULT_SEGMENT_SIZE_LIMIT: u64 = 64 * 1024 * 1024;

/// Manages a series of log segment files covering the full offset range.
///
/// Directory layout: `<log_dir>/00000000000000000000.log`, etc.
/// Segment filenames are zero-padded 20-digit base offsets.
///
/// `log_start_offset` is persisted in a `log-start-offset` file so that
/// prefix truncation survives restarts even when entries are hidden inside
/// a retained segment.
pub struct SegmentLog {
    log_dir: PathBuf,
    segment_size_limit: u64,
    /// Atomic copies of start/end for lock-free sync reads (trait contract).
    start_offset_atomic: AtomicU64,
    end_offset_atomic: AtomicU64,
    inner: Mutex<SegmentLogInner>,
}

struct SegmentLogInner {
    segments: Vec<Segment>,
    log_start_offset: u64,
    log_end_offset: u64,
}

impl SegmentLog {
    /// Open or create a segment log in the given directory.
    pub async fn open(log_dir: &Path) -> Result<Self> {
        Self::open_with_limit(log_dir, DEFAULT_SEGMENT_SIZE_LIMIT).await
    }

    /// Open with a custom segment size limit (useful for testing rollover).
    pub async fn open_with_limit(log_dir: &Path, segment_size_limit: u64) -> Result<Self> {
        tokio::fs::create_dir_all(log_dir).await?;

        let mut segment_files = Vec::new();
        let mut entries = tokio::fs::read_dir(log_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.ends_with(".log") {
                let base_str = name_str.trim_end_matches(".log");
                if let Ok(base_offset) = base_str.parse::<u64>() {
                    segment_files.push(base_offset);
                }
            }
        }
        segment_files.sort();

        let mut segments = Vec::new();
        for base_offset in &segment_files {
            let seg_path = Self::segment_path(log_dir, *base_offset);
            let segment = Segment::open(&seg_path, *base_offset).await?;
            segments.push(segment);
        }

        // Remove empty trailing segments (from crash during rollover)
        while segments.len() > 1 {
            let last = segments.last().unwrap();
            if last.entry_count().await == 0 {
                let path = last.path().to_path_buf();
                segments.pop();
                let _ = tokio::fs::remove_file(&path).await;
            } else {
                break;
            }
        }

        // Recover persisted log_start_offset (survives prefix truncation
        // that hides entries inside a retained segment).
        let persisted_start = Self::load_start_offset(log_dir).await;

        let physical_start = segments
            .first()
            .map(|s| s.base_offset())
            .unwrap_or(0);

        // Use the larger of persisted and physical start offset.
        let log_start_offset = persisted_start.max(physical_start);

        let log_end_offset = if let Some(last) = segments.last() {
            last.next_offset().await
        } else {
            // If no segments, start and end should match.
            log_start_offset
        };

        Ok(Self {
            log_dir: log_dir.to_path_buf(),
            segment_size_limit,
            start_offset_atomic: AtomicU64::new(log_start_offset),
            end_offset_atomic: AtomicU64::new(log_end_offset),
            inner: Mutex::new(SegmentLogInner {
                segments,
                log_start_offset,
                log_end_offset,
            }),
        })
    }

    fn segment_path(log_dir: &Path, base_offset: u64) -> PathBuf {
        log_dir.join(format!("{:020}.log", base_offset))
    }

    fn start_offset_path(log_dir: &Path) -> PathBuf {
        log_dir.join("log-start-offset")
    }

    /// Load persisted log_start_offset from file; returns 0 if absent.
    async fn load_start_offset(log_dir: &Path) -> u64 {
        let path = Self::start_offset_path(log_dir);
        match tokio::fs::read_to_string(&path).await {
            Ok(s) => s.trim().parse::<u64>().unwrap_or(0),
            Err(_) => 0,
        }
    }

    /// Persist log_start_offset to file (atomic write).
    async fn persist_start_offset(log_dir: &Path, offset: u64) -> Result<()> {
        let path = Self::start_offset_path(log_dir);
        let tmp = log_dir.join("log-start-offset.tmp");
        tokio::fs::write(&tmp, offset.to_string()).await?;
        tokio::fs::rename(&tmp, &path).await?;
        Ok(())
    }

    /// Return the log start offset.
    pub async fn start_offset(&self) -> u64 {
        self.inner.lock().await.log_start_offset
    }

    /// Return the log end offset (next to be written).
    pub async fn end_offset(&self) -> u64 {
        self.inner.lock().await.log_end_offset
    }
}

#[async_trait]
impl LogStore for SegmentLog {
    async fn append(&self, entries: &[LogEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut inner = self.inner.lock().await;

        // Verify offset continuity
        if entries[0].offset != inner.log_end_offset {
            return Err(XraftError::Corruption(format!(
                "append offset mismatch: expected {}, got {}",
                inner.log_end_offset, entries[0].offset
            )));
        }

        // Create first segment if none exist
        if inner.segments.is_empty() {
            let seg_path = Self::segment_path(&self.log_dir, entries[0].offset);
            let segment = Segment::open(&seg_path, entries[0].offset).await?;
            inner.segments.push(segment);
        }

        // Check if we need to roll to a new segment
        let last_seg = inner.segments.last().unwrap();
        if last_seg.file_size().await >= self.segment_size_limit {
            let new_base = entries[0].offset;
            let seg_path = Self::segment_path(&self.log_dir, new_base);
            let segment = Segment::open(&seg_path, new_base).await?;
            inner.segments.push(segment);
        }

        let last_seg = inner.segments.last().unwrap();
        last_seg.append(entries).await?;

        inner.log_end_offset += entries.len() as u64;
        self.end_offset_atomic.store(inner.log_end_offset, Ordering::Release);

        Ok(())
    }

    async fn read(&self, start_offset: u64, end_offset: u64) -> Result<Vec<LogEntry>> {
        let inner = self.inner.lock().await;
        let mut result = Vec::new();

        // Clamp to logical bounds: never return entries before log_start_offset
        let effective_start = start_offset.max(inner.log_start_offset);
        if effective_start >= end_offset {
            return Ok(result);
        }

        for segment in &inner.segments {
            let seg_base = segment.base_offset();
            let seg_end = segment.next_offset().await;

            // Skip segments entirely outside our range
            if seg_end <= effective_start || seg_base >= end_offset {
                continue;
            }

            let entries = segment.read(effective_start, end_offset).await?;
            result.extend(entries);
        }

        Ok(result)
    }

    async fn truncate_suffix(&self, from_offset: u64) -> Result<()> {
        let mut inner = self.inner.lock().await;

        if from_offset >= inner.log_end_offset {
            return Ok(());
        }

        // Walk segments in reverse, removing those entirely after from_offset
        while inner.segments.len() > 1 {
            let last = inner.segments.last().unwrap();
            if last.base_offset() >= from_offset {
                let path = last.path().to_path_buf();
                inner.segments.pop();
                let _ = tokio::fs::remove_file(&path).await;
            } else {
                break;
            }
        }

        // Truncate within the last remaining segment
        if let Some(last) = inner.segments.last() {
            last.truncate_suffix(from_offset).await?;
        }

        inner.log_end_offset = from_offset;
        self.end_offset_atomic.store(from_offset, Ordering::Release);

        Ok(())
    }

    async fn truncate_prefix(&self, up_to_offset: u64) -> Result<()> {
        let mut inner = self.inner.lock().await;

        // Remove segment files whose entries are all before up_to_offset
        let mut to_remove = Vec::new();
        for (i, segment) in inner.segments.iter().enumerate() {
            let seg_end = segment.next_offset().await;
            if seg_end <= up_to_offset {
                to_remove.push(i);
            }
        }

        // Remove from the front (indices are ascending)
        for &idx in to_remove.iter().rev() {
            let seg = inner.segments.remove(idx);
            let _ = tokio::fs::remove_file(seg.path()).await;
        }

        inner.log_start_offset = up_to_offset;
        self.start_offset_atomic.store(up_to_offset, Ordering::Release);

        // Persist so the truncated start offset survives restart
        Self::persist_start_offset(&self.log_dir, up_to_offset).await?;

        Ok(())
    }

    fn log_start_offset(&self) -> u64 {
        self.start_offset_atomic.load(Ordering::Acquire)
    }

    fn log_end_offset(&self) -> u64 {
        self.end_offset_atomic.load(Ordering::Acquire)
    }

    async fn entry_at(&self, offset: u64) -> Result<Option<LogEntry>> {
        let entries = self.read(offset, offset + 1).await?;
        Ok(entries.into_iter().next())
    }
}
