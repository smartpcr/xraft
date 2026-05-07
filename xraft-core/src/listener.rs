use crate::app_record::AppRecord;
use crate::snapshot::SnapshotReader;
use crate::types::{NodeId, Term};

/// Application callback interface invoked synchronously by the EventLoop.
/// Modelled on KRaft's `RaftClient.Listener`.
pub trait Listener: Send + 'static {
    fn handle_commit(&mut self, batch: &[(u64, AppRecord)]);
    fn handle_load_snapshot(&mut self, reader: SnapshotReader);
    fn handle_leader_change(&mut self, leader_id: NodeId, term: Term);
    fn begin_shutdown(&mut self);
}
