//! Leader-epoch checkpoint — persists and caches `leader_epoch → start_offset`.
//!
//! Each entry records the log offset at which a new leader's tenure began.
//! Entries are strictly ordered: epoch keys are monotonically increasing and
//! start offsets are monotonically non-decreasing. The file format is
//! line-delimited `<epoch> <start_offset>\n` for easy inspection.
//!
//! Persistence uses the same atomic pattern as `QuorumStateFile`: write temp
//! file → fsync → rename → fsync parent directory.

use std::io;
use std::path::{Path, PathBuf};
use xraft_core::Term;

const CHECKPOINT_FILENAME: &str = "leader-epoch-checkpoint";

/// In-memory checkpoint mapping `leader_epoch → start_offset`, backed by a file.
///
/// Entries are kept sorted by epoch (ascending). Both epoch keys and start
/// offsets must be monotonically increasing / non-decreasing respectively.
#[derive(Debug)]
pub struct LeaderEpochCheckpoint {
    /// Sorted by epoch ascending. Invariant: epochs strictly increase,
    /// start_offsets are monotonically non-decreasing.
    epochs: Vec<EpochEntry>,
    dir: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct EpochEntry {
    epoch: Term,
    start_offset: u64,
}

impl LeaderEpochCheckpoint {
    /// Create a new empty checkpoint that will persist to
    /// `dir/leader-epoch-checkpoint`.
    ///
    /// The directory is created if it does not already exist.
    pub fn new(dir: impl Into<PathBuf>) -> io::Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            epochs: Vec::new(),
            dir,
        })
    }

    /// Load a checkpoint from `dir/leader-epoch-checkpoint`. Returns an empty
    /// checkpoint if the file does not exist.
    ///
    /// The directory is created if it does not already exist.
    pub fn load(dir: impl Into<PathBuf>) -> io::Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        let file_path = dir.join(CHECKPOINT_FILENAME);
        let data = match std::fs::read_to_string(&file_path) {
            Ok(d) => d,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Ok(Self {
                    epochs: Vec::new(),
                    dir,
                });
            }
            Err(e) => return Err(e),
        };

        let mut epochs = Vec::new();
        for (line_no, line) in data.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() != 2 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "leader-epoch-checkpoint line {}: expected '<epoch> <offset>', got '{}'",
                        line_no + 1,
                        line
                    ),
                ));
            }
            let epoch: u64 = parts[0].parse().map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("leader-epoch-checkpoint line {}: bad epoch: {e}", line_no + 1),
                )
            })?;
            let offset: u64 = parts[1].parse().map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("leader-epoch-checkpoint line {}: bad offset: {e}", line_no + 1),
                )
            })?;

            // Validate ordering invariant.
            if let Some(prev) = epochs.last() {
                let prev: &EpochEntry = prev;
                if Term(epoch) <= prev.epoch {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "leader-epoch-checkpoint line {}: epoch {} is not strictly greater than previous {}",
                            line_no + 1, epoch, prev.epoch.0
                        ),
                    ));
                }
                if offset < prev.start_offset {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "leader-epoch-checkpoint line {}: start_offset {} is less than previous {}",
                            line_no + 1, offset, prev.start_offset
                        ),
                    ));
                }
            }

            epochs.push(EpochEntry {
                epoch: Term(epoch),
                start_offset: offset,
            });
        }

        Ok(Self { epochs, dir })
    }

    fn file_path(&self) -> PathBuf {
        self.dir.join(CHECKPOINT_FILENAME)
    }

    /// Append a new epoch entry. The epoch must be strictly greater than any
    /// existing epoch, and `start_offset` must be >= the last entry's offset.
    ///
    /// Persists the full checkpoint atomically. The in-memory cache is only
    /// updated after persistence succeeds, so an I/O failure never leaves
    /// memory ahead of durable state.
    pub fn append(&mut self, epoch: Term, start_offset: u64) -> io::Result<()> {
        if let Some(last) = self.epochs.last() {
            if epoch <= last.epoch {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "epoch {} must be strictly greater than last epoch {}",
                        epoch.0, last.epoch.0
                    ),
                ));
            }
            if start_offset < last.start_offset {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "start_offset {} must be >= last start_offset {}",
                        start_offset, last.start_offset
                    ),
                ));
            }
        }

        let new_entry = EpochEntry {
            epoch,
            start_offset,
        };

        // Persist with the new entry included, but don't touch the
        // in-memory Vec until persistence succeeds.
        self.persist_with_extra(&new_entry)?;

        self.epochs.push(new_entry);
        Ok(())
    }

    /// Look up the start offset for an exact epoch match.
    pub fn lookup(&self, epoch: Term) -> Option<u64> {
        self.epochs
            .binary_search_by_key(&epoch, |e| e.epoch)
            .ok()
            .map(|idx| self.epochs[idx].start_offset)
    }

    /// Find the entry with the largest epoch <= the given epoch.
    /// Returns `(epoch, start_offset)` or `None` if no such entry exists.
    pub fn lookup_le(&self, epoch: Term) -> Option<(Term, u64)> {
        match self.epochs.binary_search_by_key(&epoch, |e| e.epoch) {
            Ok(idx) => {
                let e = &self.epochs[idx];
                Some((e.epoch, e.start_offset))
            }
            Err(idx) => {
                if idx == 0 {
                    None
                } else {
                    let e = &self.epochs[idx - 1];
                    Some((e.epoch, e.start_offset))
                }
            }
        }
    }

    /// Return the number of epoch entries.
    pub fn len(&self) -> usize {
        self.epochs.len()
    }

    /// Return `true` if there are no epoch entries.
    pub fn is_empty(&self) -> bool {
        self.epochs.is_empty()
    }

    /// Persist the full checkpoint (plus an optional extra trailing entry) to
    /// disk atomically. Called by `append` with the not-yet-pushed entry so
    /// that the in-memory Vec is only mutated after durable write succeeds.
    fn persist_with_extra(&self, extra: &EpochEntry) -> io::Result<()> {
        let mut content = String::new();
        for entry in &self.epochs {
            content.push_str(&format!("{} {}\n", entry.epoch.0, entry.start_offset));
        }
        content.push_str(&format!("{} {}\n", extra.epoch.0, extra.start_offset));
        self.persist_content(&content)
    }

    /// Write `content` to the checkpoint file using temp-write/fsync/rename.
    fn persist_content(&self, content: &str) -> io::Result<()> {
        let file_path = self.file_path();
        let tmp_path = file_path.with_extension("tmp");

        // Write → fsync
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&tmp_path)?;
            file.write_all(content.as_bytes())?;
            file.flush()?;
            file.sync_all()?;
        }

        // Atomic replace (cross-platform) → fsync parent
        atomic_replace(&tmp_path, &file_path)?;
        fsync_dir(&self.dir)?;

        Ok(())
    }
}

/// Cross-platform atomic file replacement: move `src` over `dst`.
///
/// On Unix, `rename(2)` atomically replaces `dst`.
/// On Windows, we call `MoveFileExW` with `MOVEFILE_REPLACE_EXISTING`,
/// which atomically replaces the destination. This avoids the
/// remove-then-rename anti-pattern that can lose data on crash.
#[cfg(unix)]
fn atomic_replace(src: &Path, dst: &Path) -> io::Result<()> {
    std::fs::rename(src, dst)
}

#[cfg(windows)]
fn atomic_replace(src: &Path, dst: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    const MOVEFILE_REPLACE_EXISTING: u32 = 0x1;

    extern "system" {
        fn MoveFileExW(
            lpExistingFileName: *const u16,
            lpNewFileName: *const u16,
            dwFlags: u32,
        ) -> i32;
    }

    let src_w: Vec<u16> = src.as_os_str().encode_wide().chain(Some(0)).collect();
    let dst_w: Vec<u16> = dst.as_os_str().encode_wide().chain(Some(0)).collect();

    let ok = unsafe { MoveFileExW(src_w.as_ptr(), dst_w.as_ptr(), MOVEFILE_REPLACE_EXISTING) };

    if ok == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(any(unix, windows)))]
fn atomic_replace(src: &Path, dst: &Path) -> io::Result<()> {
    std::fs::rename(src, dst)
}

/// Fsync a directory to ensure rename metadata is durable.
#[cfg(unix)]
fn fsync_dir(dir: &Path) -> io::Result<()> {
    let f = std::fs::File::open(dir)?;
    f.sync_all()
}

#[cfg(not(unix))]
fn fsync_dir(_dir: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_append_and_lookup() {
        let dir = TempDir::new().unwrap();
        let mut cp = LeaderEpochCheckpoint::new(dir.path()).unwrap();

        cp.append(Term(1), 0).unwrap();
        cp.append(Term(3), 50).unwrap();
        cp.append(Term(5), 120).unwrap();

        assert_eq!(cp.lookup(Term(1)), Some(0));
        assert_eq!(cp.lookup(Term(3)), Some(50));
        assert_eq!(cp.lookup(Term(5)), Some(120));
    }

    #[test]
    fn test_lookup_missing_epoch() {
        let dir = TempDir::new().unwrap();
        let mut cp = LeaderEpochCheckpoint::new(dir.path()).unwrap();

        cp.append(Term(1), 0).unwrap();
        cp.append(Term(3), 50).unwrap();
        cp.append(Term(5), 120).unwrap();

        assert_eq!(cp.lookup(Term(2)), None);
        assert_eq!(cp.lookup(Term(4)), None);
        assert_eq!(cp.lookup(Term(6)), None);
    }

    #[test]
    fn test_lookup_le() {
        let dir = TempDir::new().unwrap();
        let mut cp = LeaderEpochCheckpoint::new(dir.path()).unwrap();

        cp.append(Term(1), 0).unwrap();
        cp.append(Term(3), 50).unwrap();
        cp.append(Term(5), 120).unwrap();

        // Exact matches
        assert_eq!(cp.lookup_le(Term(1)), Some((Term(1), 0)));
        assert_eq!(cp.lookup_le(Term(3)), Some((Term(3), 50)));
        assert_eq!(cp.lookup_le(Term(5)), Some((Term(5), 120)));

        // Between epochs
        assert_eq!(cp.lookup_le(Term(2)), Some((Term(1), 0)));
        assert_eq!(cp.lookup_le(Term(4)), Some((Term(3), 50)));

        // Beyond last epoch
        assert_eq!(cp.lookup_le(Term(100)), Some((Term(5), 120)));

        // Before first epoch
        assert_eq!(cp.lookup_le(Term(0)), None);
    }

    #[test]
    fn test_persist_and_reload() {
        let dir = TempDir::new().unwrap();

        {
            let mut cp = LeaderEpochCheckpoint::new(dir.path()).unwrap();
            cp.append(Term(1), 0).unwrap();
            cp.append(Term(3), 50).unwrap();
            cp.append(Term(5), 120).unwrap();
        }

        // Reload from the same directory
        let cp2 = LeaderEpochCheckpoint::load(dir.path()).unwrap();
        assert_eq!(cp2.lookup(Term(1)), Some(0));
        assert_eq!(cp2.lookup(Term(3)), Some(50));
        assert_eq!(cp2.lookup(Term(5)), Some(120));
        assert_eq!(cp2.len(), 3);
    }

    #[test]
    fn test_empty_checkpoint() {
        let dir = TempDir::new().unwrap();
        let cp = LeaderEpochCheckpoint::new(dir.path()).unwrap();

        assert_eq!(cp.lookup(Term(1)), None);
        assert_eq!(cp.lookup_le(Term(1)), None);
        assert!(cp.is_empty());
        assert_eq!(cp.len(), 0);
    }

    #[test]
    fn test_load_missing_file_returns_empty() {
        let dir = TempDir::new().unwrap();
        let cp = LeaderEpochCheckpoint::load(dir.path()).unwrap();

        assert!(cp.is_empty());
    }

    #[test]
    fn test_append_rejects_non_increasing_epoch() {
        let dir = TempDir::new().unwrap();
        let mut cp = LeaderEpochCheckpoint::new(dir.path()).unwrap();

        cp.append(Term(3), 50).unwrap();

        // Same epoch
        let err = cp.append(Term(3), 60).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        // Lower epoch
        let err = cp.append(Term(1), 60).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn test_append_rejects_decreasing_offset() {
        let dir = TempDir::new().unwrap();
        let mut cp = LeaderEpochCheckpoint::new(dir.path()).unwrap();

        cp.append(Term(1), 50).unwrap();

        let err = cp.append(Term(2), 30).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn test_file_format_is_human_readable() {
        let dir = TempDir::new().unwrap();
        let mut cp = LeaderEpochCheckpoint::new(dir.path()).unwrap();

        cp.append(Term(1), 0).unwrap();
        cp.append(Term(3), 50).unwrap();

        let content =
            std::fs::read_to_string(dir.path().join(CHECKPOINT_FILENAME)).unwrap();
        assert_eq!(content, "1 0\n3 50\n");
    }

    #[test]
    fn test_load_validates_ordering() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CHECKPOINT_FILENAME);

        // Write a corrupted file with out-of-order epochs
        std::fs::write(&path, "5 100\n3 50\n").unwrap();

        let err = LeaderEpochCheckpoint::load(dir.path()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_no_tmp_file_left_after_persist() {
        let dir = TempDir::new().unwrap();
        let mut cp = LeaderEpochCheckpoint::new(dir.path()).unwrap();

        cp.append(Term(1), 0).unwrap();

        let tmp_path = dir.path().join(CHECKPOINT_FILENAME).with_extension("tmp");
        assert!(!tmp_path.exists());
    }

    #[test]
    fn test_new_creates_directory() {
        let dir = TempDir::new().unwrap();
        let subdir = dir.path().join("nested").join("storage");
        assert!(!subdir.exists());

        let mut cp = LeaderEpochCheckpoint::new(&subdir).unwrap();
        assert!(subdir.exists());

        cp.append(Term(1), 0).unwrap();
        assert_eq!(cp.lookup(Term(1)), Some(0));
    }
}
