use xraft_core::app_record::AppRecord;
use xraft_core::listener::Listener;
use xraft_core::snapshot::SnapshotReader;
use xraft_core::types::{NodeId, Term};

pub struct MockListener;

impl Listener for MockListener {
    fn handle_commit(&mut self, _batch: &[(u64, AppRecord)]) {}
    fn handle_load_snapshot(&mut self, _reader: SnapshotReader) {}
    fn handle_leader_change(&mut self, _leader_id: NodeId, _term: Term) {}
    fn begin_shutdown(&mut self) {}
}
