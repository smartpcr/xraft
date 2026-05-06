use async_trait::async_trait;

use crate::error::XraftError;
use crate::log_entry::LogEntry;

/// Durable append-only log. All mutating methods take `&self` — implementations
/// use interior mutability (e.g. `tokio::sync::Mutex<File>`) and must be
/// `Send + Sync`.
#[async_trait]
pub trait LogStore: Send + Sync + 'static {
    /// Append entries. Must fsync before returning Ok.
    async fn append(&self, entries: &[LogEntry]) -> Result<(), XraftError>;

    /// Read entries in `[start_offset, end_offset)`.
    async fn read(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Vec<LogEntry>, XraftError>;

    /// Truncate the log suffix starting at the given offset (for divergence).
    async fn truncate_suffix(&self, from_offset: u64) -> Result<(), XraftError>;

    /// Truncate the log prefix up to the given offset (after snapshot).
    async fn truncate_prefix(&self, up_to_offset: u64) -> Result<(), XraftError>;

    /// The first offset still in the log.
    fn log_start_offset(&self) -> u64;

    /// The next offset to be written.
    fn log_end_offset(&self) -> u64;

    /// Read the entry at the given offset; returns `None` if out of bounds.
    async fn entry_at(&self, offset: u64) -> Result<Option<LogEntry>, XraftError>;
}
