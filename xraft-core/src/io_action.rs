use crate::quorum_state::QuorumState;
use crate::rpc::RpcEnvelope;
use crate::types::NodeId;

/// Actions the event loop collects and dispatches to the IoStage.
///
/// Each variant maps to a concrete I/O operation executed by an injected trait
/// object (`QuorumStateStore`, `TransportSender`, `LogStore`, `SnapshotIO`).
/// The event loop never performs I/O directly — it emits `IoAction` values that
/// the `IoStage` executes concurrently via `tokio::join!`.
#[derive(Debug, Clone)]
pub enum IoAction {
    /// Persist quorum state (current_term, voted_for) to durable storage.
    /// Must complete (fsync) before any vote response is sent.
    PersistQuorumState(QuorumState),

    /// Send an RPC envelope to a specific peer node.
    SendRpc(NodeId, RpcEnvelope),
    // Future variants (implemented by other workstreams):
    // AppendLog(Vec<LogEntry>),
    // TruncateSuffix(u64),
    // TruncatePrefix(u64),
    // SaveSnapshot(Snapshot),
}

/// A batch of IoActions collected during a single event-loop iteration.
#[derive(Debug, Clone, Default)]
pub struct IoActionBatch {
    pub actions: Vec<IoAction>,
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
}
