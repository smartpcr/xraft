use crate::log_entry::LogEntry;
use crate::quorum_state::QuorumState;
use crate::rpc::RpcEnvelope;
use crate::snapshot::Snapshot;
use crate::traits::{LogStore, QuorumStateStore, SnapshotIO, TransportSender};
use crate::types::NodeId;

/// Actions produced by the event loop for the IoStage to execute.
#[derive(Debug)]
pub enum IoAction {
    PersistQuorumState(QuorumState),
    AppendLog(Vec<LogEntry>),
    TruncateSuffix(u64),
    TruncatePrefix(u64),
    SendRpc(NodeId, RpcEnvelope),
    SaveSnapshot(Snapshot),
}

/// Batch of IoActions collected during a single event loop iteration.
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

/// Executes IoAction batches by dispatching to I/O trait objects.
pub struct IoStage {
    pub log_store: Box<dyn LogStore>,
    pub transport: Box<dyn TransportSender>,
    pub quorum_store: Box<dyn QuorumStateStore>,
    pub snapshot_io: Box<dyn SnapshotIO>,
}

impl IoStage {
    pub fn new(
        log_store: Box<dyn LogStore>,
        transport: Box<dyn TransportSender>,
        quorum_store: Box<dyn QuorumStateStore>,
        snapshot_io: Box<dyn SnapshotIO>,
    ) -> Self {
        Self {
            log_store,
            transport,
            quorum_store,
            snapshot_io,
        }
    }

    /// Execute all actions in the batch. Storage ops and network sends are
    /// partitioned into two groups and dispatched via `tokio::try_join!` so
    /// that storage I/O overlaps with network I/O. Within each group,
    /// operations run sequentially to preserve ordering (log append order
    /// matters for storage; network sends are awaited one-by-one).
    pub async fn execute(&self, batch: &IoActionBatch) -> Result<(), std::io::Error> {
        // Partition into storage futures and network futures.
        // Storage ops execute sequentially (log ordering matters).
        // Network ops execute concurrently with storage.
        let mut storage_actions: Vec<&IoAction> = Vec::new();
        let mut network_actions: Vec<&IoAction> = Vec::new();

        for action in &batch.actions {
            match action {
                IoAction::SendRpc(_, _) => network_actions.push(action),
                _ => storage_actions.push(action),
            }
        }

        let storage_fut = async {
            for action in &storage_actions {
                match action {
                    IoAction::PersistQuorumState(state) => {
                        self.quorum_store.save(state).await?;
                    }
                    IoAction::AppendLog(entries) => {
                        self.log_store.append(entries).await?;
                    }
                    IoAction::TruncateSuffix(offset) => {
                        self.log_store.truncate_suffix(*offset).await?;
                    }
                    IoAction::TruncatePrefix(offset) => {
                        self.log_store.truncate_prefix(*offset).await?;
                    }
                    IoAction::SaveSnapshot(snapshot) => {
                        self.snapshot_io.save(snapshot).await?;
                    }
                    IoAction::SendRpc(_, _) => unreachable!(),
                }
            }
            Ok::<(), std::io::Error>(())
        };

        let network_fut = async {
            // Network sends are awaited sequentially within this group.
            // The group itself runs concurrently with storage via try_join!.
            for action in &network_actions {
                if let IoAction::SendRpc(target, envelope) = action {
                    self.transport.send(*target, envelope.clone()).await?;
                }
            }
            Ok::<(), std::io::Error>(())
        };

        tokio::try_join!(storage_fut, network_fut)?;
        Ok(())
    }
}
