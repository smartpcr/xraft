use crate::quorum_state::QuorumState;
use async_trait::async_trait;
use bytes::Bytes;

use crate::rpc::SnapshotId;
use crate::snapshot::{Snapshot, SnapshotWriter};

/// Async trait for snapshot I/O operations. Implementations must be
/// `Send + Sync + 'static` because the `IoStage` invokes methods via
/// shared `&self` references from concurrent async tasks.
#[async_trait]
pub trait SnapshotIO: Send + Sync + 'static {
    /// Write a complete snapshot atomically.
    async fn save(&self, snapshot: &Snapshot) -> anyhow::Result<()>;

    /// Load the latest snapshot, if any.
    async fn load_latest(&self) -> anyhow::Result<Option<Snapshot>>;

    /// Read a chunk of the snapshot at the given byte position.
    /// Returns `(data, is_last_chunk)`.
    async fn read_chunk(
        &self,
        id: &SnapshotId,
        position: u64,
        max_bytes: u32,
    ) -> anyhow::Result<(Bytes, bool)>;

    /// Begin writing a snapshot received from a leader, chunk by chunk.
    async fn begin_receive(&self, id: &SnapshotId) -> anyhow::Result<SnapshotWriter>;
}
