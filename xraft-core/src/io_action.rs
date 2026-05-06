use crate::log_entry::LogEntry;
use crate::quorum_state::QuorumState;
use crate::rpc::RpcEnvelope;
use crate::snapshot::Snapshot;
use crate::types::NodeId;

/// Describes a single external I/O operation to be executed by the IoStage.
#[derive(Debug, Clone)]
pub enum IoAction {
    /// Persist voting state to quorum-state file.
    PersistQuorumState(QuorumState),
    /// Append entries to the durable log.
    AppendLog(Vec<LogEntry>),
    /// Truncate the log from offset (for divergence).
    TruncateSuffix(u64),
    /// Truncate the log up to offset (for compaction after snapshot).
    TruncatePrefix(u64),
    /// Send a message to a peer.
    SendRpc(NodeId, RpcEnvelope),
    /// Write a snapshot atomically.
    SaveSnapshot(Snapshot),
}

/// Batch of IoActions collected during a single message processing cycle.
#[derive(Debug, Clone, Default)]
pub struct IoActionBatch {
    pub actions: Vec<IoAction>,
}

impl IoActionBatch {
    pub fn new() -> Self {
        IoActionBatch {
            actions: Vec::new(),
        }
    }

    pub fn push(&mut self, action: IoAction) {
        self.actions.push(action);
    }

    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }
}
