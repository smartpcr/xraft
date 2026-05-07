use crate::app_record::AppRecord;
use crate::snapshot::SnapshotReader;
use crate::types::{NodeId, Term};

/// Internal event enum for dispatching Listener callbacks.
/// These are synchronous in-process calls, NOT IoAction variants.
pub enum ListenerEvent {
    Commit { batch: Vec<(u64, AppRecord)> },
    LoadSnapshot { reader: SnapshotReader },
    LeaderChange { leader_id: NodeId, term: Term },
    Shutdown,
}
