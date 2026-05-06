//! IoStage — executes IoActionBatch through trait objects.
//!
//! Per architecture §4.1, the IoStage is owned by the event loop task and
//! executes I/O actions concurrently via `tokio::join!`. Actions on different
//! trait objects (LogStore, TransportSender, QuorumStateStore) run in parallel;
//! at most one log-write action per batch avoids concurrent LogStore mutation.

use crate::io_action::{IoAction, IoActionBatch};
use crate::traits::{LogStore, QuorumStateStore, TransportSender};

/// Executes `IoAction` batches produced by the EventLoop.
///
/// Holds owned trait objects for LogStore, QuorumStateStore, and
/// TransportSender. No `Arc` wrapping — the IoStage is the sole owner.
/// Concurrent access within a batch uses shared `&self` borrows (safe
/// because all I/O traits require `Sync`).
pub struct IoStage {
    log_store: Box<dyn LogStore>,
    quorum_store: Box<dyn QuorumStateStore>,
    transport: Box<dyn TransportSender>,
}

impl IoStage {
    pub fn new(
        log_store: Box<dyn LogStore>,
        quorum_store: Box<dyn QuorumStateStore>,
        transport: Box<dyn TransportSender>,
    ) -> Self {
        Self {
            log_store,
            quorum_store,
            transport,
        }
    }

    /// Execute all actions in the batch. Actions on different trait objects
    /// could run concurrently in a full async implementation; here we
    /// execute them sequentially since the in-memory implementations
    /// resolve in a single poll.
    pub async fn execute(
        &self,
        batch: &IoActionBatch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for action in &batch.actions {
            match action {
                IoAction::PersistQuorumState(qs) => {
                    self.quorum_store.save(qs).await?;
                }
                IoAction::AppendLog(entries) => {
                    if !entries.is_empty() {
                        self.log_store.append(entries).await?;
                    }
                }
                IoAction::TruncateSuffix(offset) => {
                    self.log_store.truncate_suffix(*offset).await?;
                }
                IoAction::TruncatePrefix(_offset) => {
                    // TruncatePrefix not yet implemented in LogStore trait
                    // This is a placeholder for log compaction
                }
                IoAction::SendRpc(target, envelope) => {
                    self.transport.send(*target, envelope.clone()).await?;
                }
            }
        }
        Ok(())
    }

    /// Access the LogStore for verification in tests.
    pub fn log_store(&self) -> &dyn LogStore {
        self.log_store.as_ref()
    }

    /// Access the QuorumStateStore for verification in tests.
    pub fn quorum_store(&self) -> &dyn QuorumStateStore {
        self.quorum_store.as_ref()
    }
}
