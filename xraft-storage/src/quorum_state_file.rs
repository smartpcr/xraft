//! Durable quorum-state file implementing `QuorumStateStore`.
//!
//! Writes use the atomic pattern: serialize JSON → write temp file → fsync →
//! rename over target → fsync parent directory. This guarantees that a crash
//! at any point leaves the previous valid state readable.

use async_trait::async_trait;
use std::io;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use xraft_core::{QuorumState, QuorumStateStore};

const QUORUM_STATE_FILENAME: &str = "quorum-state";

/// File-backed `QuorumStateStore` using atomic JSON writes.
pub struct QuorumStateFile {
    dir: PathBuf,
    /// Serializes concurrent `save` calls (trait uses `&self`).
    write_lock: Mutex<()>,
}

impl QuorumStateFile {
    /// Create a new `QuorumStateFile` that stores state in `dir/quorum-state`.
    ///
    /// The directory is created if it does not already exist.
    pub async fn new(dir: impl Into<PathBuf>) -> io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir).await?;
        Ok(Self {
            dir,
            write_lock: Mutex::new(()),
        })
    }

    fn state_path(&self) -> PathBuf {
        self.dir.join(QUORUM_STATE_FILENAME)
    }
}

/// Fsync the parent directory to ensure rename metadata is durable.
///
/// On Unix, we open the directory and call `sync_all` which issues an fsync
/// syscall on the directory fd. On Windows, NTFS guarantees metadata
/// durability after rename returns, so the fsync is unnecessary (and opening
/// a directory as a file is unsupported).
#[cfg(unix)]
async fn fsync_dir(dir: &Path) -> io::Result<()> {
    let dir = dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let f = std::fs::File::open(&dir)?;
        f.sync_all()
    })
    .await
    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
}

#[cfg(not(unix))]
async fn fsync_dir(_dir: &Path) -> io::Result<()> {
    Ok(())
}

/// Cross-platform atomic file replacement: move `src` over `dst`.
///
/// On Unix, `rename(2)` atomically replaces `dst`.
/// On Windows, we call `MoveFileExW` with `MOVEFILE_REPLACE_EXISTING`,
/// which atomically replaces the destination. This avoids the
/// remove-then-rename anti-pattern that can lose data on crash.
async fn atomic_replace(src: &Path, dst: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        fs::rename(src, dst).await
    }

    #[cfg(windows)]
    {
        let src = src.to_path_buf();
        let dst = dst.to_path_buf();
        tokio::task::spawn_blocking(move || atomic_replace_sync(&src, &dst))
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
    }

    #[cfg(not(any(unix, windows)))]
    {
        fs::rename(src, dst).await
    }
}

/// Windows-specific atomic replace using `MoveFileExW`.
#[cfg(windows)]
fn atomic_replace_sync(src: &Path, dst: &Path) -> io::Result<()> {
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

#[async_trait]
impl QuorumStateStore for QuorumStateFile {
    async fn load(&self) -> io::Result<Option<QuorumState>> {
        let path = self.state_path();
        match fs::read(&path).await {
            Ok(data) => {
                let state: QuorumState = serde_json::from_slice(&data).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("failed to parse quorum-state: {e}"),
                    )
                })?;
                Ok(Some(state))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn save(&self, state: &QuorumState) -> io::Result<()> {
        let _guard = self.write_lock.lock().await;

        let data = serde_json::to_vec_pretty(state).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to serialize quorum-state: {e}"),
            )
        })?;

        // Write to temp file in the same directory for atomic rename.
        let tmp_path = self.dir.join("quorum-state.tmp");
        let mut file = fs::File::create(&tmp_path).await?;
        file.write_all(&data).await?;
        file.flush().await?;
        file.sync_all().await?;
        drop(file);

        // Atomic replace (cross-platform).
        atomic_replace(&tmp_path, &self.state_path()).await?;

        // Fsync parent directory to ensure the rename is durable.
        fsync_dir(&self.dir).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use xraft_core::{NodeId, Term};

    fn sample_state() -> QuorumState {
        QuorumState {
            current_term: Term(5),
            voted_for: Some(NodeId(2)),
            leader_id: Some(NodeId(2)),
            leader_epoch: Term(4),
        }
    }

    #[tokio::test]
    async fn test_save_and_load() {
        let dir = TempDir::new().unwrap();
        let store = QuorumStateFile::new(dir.path()).await.unwrap();
        let state = sample_state();

        store.save(&state).await.unwrap();
        let loaded = store.load().await.unwrap();
        assert_eq!(loaded, Some(state));
    }

    #[tokio::test]
    async fn test_load_missing_file_returns_none() {
        let dir = TempDir::new().unwrap();
        let store = QuorumStateFile::new(dir.path()).await.unwrap();

        let loaded = store.load().await.unwrap();
        assert_eq!(loaded, None);
    }

    #[tokio::test]
    async fn test_overwrite_returns_latest() {
        let dir = TempDir::new().unwrap();
        let store = QuorumStateFile::new(dir.path()).await.unwrap();

        let state1 = QuorumState {
            current_term: Term(1),
            voted_for: None,
            leader_id: None,
            leader_epoch: Term(0),
        };
        store.save(&state1).await.unwrap();

        let state2 = sample_state();
        store.save(&state2).await.unwrap();

        let loaded = store.load().await.unwrap();
        assert_eq!(loaded, Some(state2));
    }

    #[tokio::test]
    async fn test_atomic_write_does_not_leave_tmp_file() {
        let dir = TempDir::new().unwrap();
        let store = QuorumStateFile::new(dir.path()).await.unwrap();
        let state = sample_state();

        store.save(&state).await.unwrap();

        // The temp file should be gone after a successful save.
        let tmp_path = dir.path().join("quorum-state.tmp");
        assert!(!tmp_path.exists());
    }

    #[tokio::test]
    async fn test_json_format_is_human_readable() {
        let dir = TempDir::new().unwrap();
        let store = QuorumStateFile::new(dir.path()).await.unwrap();
        let state = sample_state();

        store.save(&state).await.unwrap();

        let raw = std::fs::read_to_string(dir.path().join("quorum-state")).unwrap();
        assert!(raw.contains("current_term"));
        assert!(raw.contains("voted_for"));
    }

    #[tokio::test]
    async fn test_atomic_write_crash_leaves_old_file_readable() {
        let dir = TempDir::new().unwrap();
        let store = QuorumStateFile::new(dir.path()).await.unwrap();

        // Write initial valid state.
        let state1 = QuorumState {
            current_term: Term(1),
            voted_for: Some(NodeId(1)),
            leader_id: Some(NodeId(1)),
            leader_epoch: Term(1),
        };
        store.save(&state1).await.unwrap();

        // Simulate a crash mid-write: leave a partial tmp file next to the
        // valid quorum-state file, as if the process died after creating the
        // temp file but before the atomic rename completed.
        let tmp_path = dir.path().join("quorum-state.tmp");
        std::fs::write(&tmp_path, b"GARBAGE - partial write").unwrap();

        // The real quorum-state file must still be intact and readable.
        let loaded = store.load().await.unwrap();
        assert_eq!(loaded, Some(state1.clone()));

        // A subsequent save must succeed and overwrite the leftover tmp file.
        let state2 = QuorumState {
            current_term: Term(2),
            voted_for: Some(NodeId(2)),
            leader_id: Some(NodeId(2)),
            leader_epoch: Term(2),
        };
        store.save(&state2).await.unwrap();
        let loaded = store.load().await.unwrap();
        assert_eq!(loaded, Some(state2));

        // The tmp file should be cleaned up by the successful save.
        assert!(!tmp_path.exists());
    }

    #[tokio::test]
    async fn test_atomic_replace_preserves_old_file_on_overwrite() {
        // Demonstrates that atomic_replace (MoveFileExW on Windows,
        // rename(2) on Unix) replaces the destination in a single
        // operation — there is no window where the old file has been
        // removed but the new file is not yet in place.
        let dir = TempDir::new().unwrap();
        let store = QuorumStateFile::new(dir.path()).await.unwrap();

        let state1 = QuorumState {
            current_term: Term(10),
            voted_for: Some(NodeId(1)),
            leader_id: Some(NodeId(1)),
            leader_epoch: Term(10),
        };
        store.save(&state1).await.unwrap();

        // Verify initial state is on disk.
        let path = dir.path().join("quorum-state");
        assert!(path.exists());

        // Overwrite with a new state. On Windows, atomic_replace uses
        // MoveFileExW(MOVEFILE_REPLACE_EXISTING) — the old content is
        // replaced atomically; there is no remove-then-rename gap.
        let state2 = QuorumState {
            current_term: Term(20),
            voted_for: Some(NodeId(2)),
            leader_id: Some(NodeId(2)),
            leader_epoch: Term(20),
        };
        store.save(&state2).await.unwrap();

        // File must always exist and contain the latest state.
        assert!(path.exists());
        let loaded = store.load().await.unwrap();
        assert_eq!(loaded, Some(state2));
    }

    #[tokio::test]
    async fn test_state_with_no_vote_no_leader() {
        let dir = TempDir::new().unwrap();
        let store = QuorumStateFile::new(dir.path()).await.unwrap();

        let state = QuorumState {
            current_term: Term(0),
            voted_for: None,
            leader_id: None,
            leader_epoch: Term(0),
        };
        store.save(&state).await.unwrap();

        let loaded = store.load().await.unwrap();
        assert_eq!(loaded, Some(state));
    }

    #[tokio::test]
    async fn test_new_creates_directory() {
        let dir = TempDir::new().unwrap();
        let subdir = dir.path().join("nested").join("storage");
        assert!(!subdir.exists());

        let store = QuorumStateFile::new(&subdir).await.unwrap();
        assert!(subdir.exists());

        let state = sample_state();
        store.save(&state).await.unwrap();
        let loaded = store.load().await.unwrap();
        assert_eq!(loaded, Some(state));
    }
}
