use crate::app_record::AppRecord;
use crate::snapshot::SnapshotReader;
use crate::types::{NodeId, Term};

/// Application callback interface invoked synchronously by the EventLoop.
pub trait Listener: Send + 'static {
    /// Called when entries are committed. Each tuple is (offset, record).
    fn handle_commit(&mut self, batch: &[(u64, AppRecord)]);

    /// Called when a snapshot is loaded (during recovery or transfer).
    fn handle_load_snapshot(&mut self, reader: SnapshotReader);

    /// Called when a new leader is elected.
    fn handle_leader_change(&mut self, leader_id: NodeId, term: Term);

    /// Called when shutdown begins.
    fn begin_shutdown(&mut self);
}
