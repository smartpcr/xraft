use crate::log_entry::LogEntry;
use crate::snapshot::Snapshot;
use crate::types::NodeId;

/// Actions produced by the `EventLoop` for the `IoStage` to execute.
///
/// Each variant maps to exactly one I/O trait method call. The event loop
/// collects actions into an [`IoActionBatch`] during message processing,
/// then hands the batch to `IoStage::execute()`.
///
/// Application callbacks (`StateMachine::apply`, etc.) are NOT `IoAction`
/// variants — they are synchronous calls invoked by the EventLoop before
/// the `IoAction` batch is produced (architecture §4.1).
#[derive(Debug)]
pub enum IoAction {
    /// Append entries to the log and fsync.
    AppendLog(Vec<LogEntry>),
    /// Truncate the log suffix from the given offset (divergence).
    TruncateSuffix(u64),
    /// Truncate the log prefix up to the given offset (compaction).
    TruncatePrefix(u64),
    /// Write a complete snapshot atomically (fsync before returning).
    SaveSnapshot(Snapshot),
    /// Send an RPC to a peer node (placeholder — full RPC envelope
    /// is defined in a later stage).
    SendRpc(NodeId, Vec<u8>),
}

/// A batch of [`IoAction`] values collected during one event-loop iteration.
///
/// The `IoStage` executes actions within a batch concurrently across
/// different trait objects (e.g., log write runs concurrently with
/// network sends). Operations on the *same* trait object are serialised.
#[derive(Debug, Default)]
pub struct IoActionBatch {
    actions: Vec<IoAction>,
}

impl IoActionBatch {
    pub fn new() -> Self {
        Self {
            actions: Vec::new(),
        }
    }

    pub fn push(&mut self, action: IoAction) {
        self.actions.push(action);
    }

    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }

    pub fn len(&self) -> usize {
        self.actions.len()
    }

    pub fn drain(&mut self) -> impl Iterator<Item = IoAction> + '_ {
        self.actions.drain(..)
    }

    pub fn iter(&self) -> impl Iterator<Item = &IoAction> {
        self.actions.iter()
    }
}
