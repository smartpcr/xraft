use crate::app_record::AppRecord;
use crate::snapshot::SnapshotReader;
use crate::types::{NodeId, Term};

/// Application callback interface invoked synchronously by the EventLoop.
///
/// Modelled on KRaft's `RaftClient.Listener`. All callbacks are
/// synchronous, in-process calls — not external I/O. Must be
/// lightweight and non-blocking.
pub trait Listener: Send + 'static {
    /// Called when a batch of application records is committed (HW advanced).
    /// Only application records appear here; control records are filtered.
    fn handle_commit(&mut self, batch: &[(u64, AppRecord)]);

    /// Called when a snapshot must be loaded (after FetchSnapshot completes).
    fn handle_load_snapshot(&mut self, reader: SnapshotReader);

    /// Called on leadership change.
    fn handle_leader_change(&mut self, leader_id: NodeId, term: Term);

    /// Called during graceful shutdown.
    fn begin_shutdown(&mut self);
}
