use tracing::{info, warn};

use crate::io_action::{IoAction, IoActionBatch};
use crate::traits::{LogStore, NetworkSender, SnapshotIO};

/// Executes [`IoActionBatch`] values produced by the `EventLoop`.
///
/// The `IoStage` is **owned** by the event loop task (moved in at startup).
/// The event loop calls `execute(&self, batch)` inline — no separate task,
/// no message queue. Within a batch, actions targeting different trait
/// objects run concurrently via `tokio::join!`; actions on the *same*
/// trait object are serialised (architecture §4.1).
///
/// This struct holds owned trait objects for the injected I/O
/// implementations. No `Arc` wrapping is needed — the `IoStage` is the
/// sole owner and `execute` takes `&self`.
pub struct IoStage<L, S, N> {
    log_store: L,
    snapshot_io: S,
    network: N,
}

/// Result of executing a single [`IoAction`].
#[derive(Debug)]
pub enum IoResult {
    /// `SaveSnapshot` completed successfully.
    SnapshotSaved,
    /// `SaveSnapshot` failed with an I/O error.
    SnapshotSaveFailed(std::io::Error),
    /// `TruncatePrefix` completed successfully.
    PrefixTruncated,
    /// `TruncatePrefix` failed with an I/O error.
    PrefixTruncateFailed(std::io::Error),
    /// `AppendLog` completed successfully.
    LogAppended,
    /// `TruncateSuffix` completed successfully.
    SuffixTruncated,
    /// `SendRpc` completed successfully.
    RpcSent,
    /// `SendRpc` failed.
    RpcFailed(std::io::Error),
    /// A storage operation failed.
    StorageFailed(std::io::Error),
}

impl<L, S, N> IoStage<L, S, N>
where
    L: LogStore,
    S: SnapshotIO,
    N: NetworkSender,
{
    pub fn new(log_store: L, snapshot_io: S, network: N) -> Self {
        Self {
            log_store,
            snapshot_io,
            network,
        }
    }

    /// Reference to the log store (for queries by the EventLoop).
    pub fn log_store(&self) -> &L {
        &self.log_store
    }

    /// Reference to the snapshot I/O (for queries by the EventLoop).
    pub fn snapshot_io(&self) -> &S {
        &self.snapshot_io
    }

    /// Reference to the network sender (for queries by the EventLoop).
    pub fn network(&self) -> &N {
        &self.network
    }

    /// Execute a batch of I/O actions concurrently across trait objects.
    ///
    /// Actions are partitioned into three groups — log, snapshot, network —
    /// and each group's actions run sequentially within the group. The three
    /// groups execute concurrently via `tokio::join!` (architecture §4.1).
    /// Results are returned in the same order as the input batch.
    pub async fn execute(&self, batch: &mut IoActionBatch) -> Vec<IoResult> {
        let mut log_actions: Vec<(usize, IoAction)> = Vec::new();
        let mut snap_actions: Vec<(usize, IoAction)> = Vec::new();
        let mut net_actions: Vec<(usize, IoAction)> = Vec::new();

        for (idx, action) in batch.drain().enumerate() {
            match &action {
                IoAction::AppendLog(_)
                | IoAction::TruncateSuffix(_)
                | IoAction::TruncatePrefix(_) => log_actions.push((idx, action)),
                IoAction::SaveSnapshot(_) => snap_actions.push((idx, action)),
                IoAction::SendRpc(_, _) => net_actions.push((idx, action)),
            }
        }

        let log_fut = self.execute_log_group(&log_actions);
        let snap_fut = self.execute_snap_group(&snap_actions);
        let net_fut = self.execute_net_group(&net_actions);

        let (log_results, snap_results, net_results) =
            tokio::join!(log_fut, snap_fut, net_fut);

        // Merge results back in original order.
        let total = log_results.len() + snap_results.len() + net_results.len();
        let mut indexed: Vec<(usize, IoResult)> = Vec::with_capacity(total);
        indexed.extend(log_results);
        indexed.extend(snap_results);
        indexed.extend(net_results);
        indexed.sort_by_key(|(idx, _)| *idx);
        indexed.into_iter().map(|(_, r)| r).collect()
    }

    async fn execute_log_group(
        &self,
        actions: &[(usize, IoAction)],
    ) -> Vec<(usize, IoResult)> {
        let mut results = Vec::with_capacity(actions.len());
        for (idx, action) in actions {
            let result = match action {
                IoAction::AppendLog(entries) => match self.log_store.append(entries).await {
                    Ok(()) => {
                        info!(count = entries.len(), "log entries appended");
                        IoResult::LogAppended
                    }
                    Err(e) => {
                        warn!("log append failed: {e}");
                        IoResult::StorageFailed(e)
                    }
                },
                IoAction::TruncateSuffix(from) => {
                    match self.log_store.truncate_suffix(*from).await {
                        Ok(()) => IoResult::SuffixTruncated,
                        Err(e) => IoResult::StorageFailed(e),
                    }
                }
                IoAction::TruncatePrefix(up_to) => {
                    match self.log_store.truncate_prefix(*up_to).await {
                        Ok(()) => {
                            info!(up_to, "log prefix truncated");
                            IoResult::PrefixTruncated
                        }
                        Err(e) => {
                            warn!(up_to, "log prefix truncation failed: {e}");
                            IoResult::PrefixTruncateFailed(e)
                        }
                    }
                }
                _ => unreachable!("log group should only contain log actions"),
            };
            results.push((*idx, result));
        }
        results
    }

    async fn execute_snap_group(
        &self,
        actions: &[(usize, IoAction)],
    ) -> Vec<(usize, IoResult)> {
        let mut results = Vec::with_capacity(actions.len());
        for (idx, action) in actions {
            let result = match action {
                IoAction::SaveSnapshot(snapshot) => {
                    let offset = snapshot.metadata.last_included_offset;
                    match self.snapshot_io.save(snapshot).await {
                        Ok(()) => {
                            info!(offset, "snapshot saved");
                            IoResult::SnapshotSaved
                        }
                        Err(e) => {
                            warn!(offset, "snapshot save failed: {e}");
                            IoResult::SnapshotSaveFailed(e)
                        }
                    }
                }
                _ => unreachable!("snap group should only contain snapshot actions"),
            };
            results.push((*idx, result));
        }
        results
    }

    async fn execute_net_group(
        &self,
        actions: &[(usize, IoAction)],
    ) -> Vec<(usize, IoResult)> {
        let mut results = Vec::with_capacity(actions.len());
        for (idx, action) in actions {
            let result = match action {
                IoAction::SendRpc(node_id, data) => {
                    match self.network.send(*node_id, data.clone()).await {
                        Ok(()) => {
                            info!(%node_id, "RPC sent");
                            IoResult::RpcSent
                        }
                        Err(e) => {
                            warn!(%node_id, "RPC send failed: {e}");
                            IoResult::RpcFailed(e)
                        }
                    }
                }
                _ => unreachable!("net group should only contain network actions"),
            };
            results.push((*idx, result));
        }
        results
    }
}
