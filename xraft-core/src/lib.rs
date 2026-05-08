//! xraft-core: Raft consensus protocol engine.

pub mod app_record;
pub mod listener;
pub mod listener_event;
pub mod snapshot;
pub mod types;
pub mod voter;

// Re-export core domain types for convenience.
pub use app_record::{AppRecord, AppSnapshot};
pub use listener::Listener;
pub use listener_event::ListenerEvent;
pub use snapshot::{
    Snapshot, SnapshotId, SnapshotMetadata, SnapshotReader, SnapshotWriter, VoterInfo,
    VotersRecord,
};
pub use types::{ClusterId, NodeId, Offset, Term};
