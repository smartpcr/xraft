use crate::app_record::AppRecord;
use crate::snapshot::SnapshotReader;
use crate::types::{NodeId, Term};

/// Internal dispatch enum matching each `Listener` callback.
/// Used by the EventLoop for callback dispatch.
pub enum ListenerEvent {
    Commit { batch: Vec<(u64, AppRecord)> },
    LoadSnapshot { reader: SnapshotReader },
    LeaderChange { leader_id: NodeId, term: Term },
    Shutdown,
}
