//! IoAction and IoActionBatch — the event loop → I/O stage contract.
//!
//! Per architecture §4.1, the event loop processes each inbound message in
//! a strict sequence: (1) mutate NodeState; (2) invoke application callbacks
//! synchronously; (3) collect `IoAction` values into an `IoActionBatch`;
//! (4) pass the batch to `IoStage::execute()` for concurrent dispatch.
//!
//! Application callbacks (StateMachine::apply, Listener::handle_commit,
//! Listener::handle_leader_change) are NOT IoAction variants — they are
//! invoked directly by the EventLoop before the batch is produced.

use crate::log_entry::LogEntry;
use crate::rpc::RpcEnvelope;
use crate::traits::QuorumState;
use crate::types::NodeId;

/// A single I/O operation produced by the event loop for execution by the IoStage.
#[derive(Debug)]
pub enum IoAction {
    /// Persist quorum state (term, voted_for) to durable storage.
    PersistQuorumState(QuorumState),
    /// Append entries to the log and fsync.
    AppendLog(Vec<LogEntry>),
    /// Truncate log from the given offset onward (divergence resolution).
    TruncateSuffix(u64),
    /// Truncate log up to the given offset (compaction).
    TruncatePrefix(u64),
    /// Send an RPC envelope to a peer node.
    SendRpc(NodeId, RpcEnvelope),
}

/// A batch of IoActions to be executed concurrently by the IoStage.
///
/// Within a batch, actions on different trait objects run concurrently
/// via `tokio::join!`. At most one log-write action appears per batch
/// to avoid concurrent mutation of a single LogStore.
#[derive(Debug, Default)]
pub struct IoActionBatch {
    pub actions: Vec<IoAction>,
}

impl IoActionBatch {
    pub fn new() -> Self {
        Self { actions: Vec::new() }
    }

    pub fn push(&mut self, action: IoAction) {
        self.actions.push(action);
    }

    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }
}
