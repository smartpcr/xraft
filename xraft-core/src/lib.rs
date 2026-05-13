//! xraft-core: Raft consensus protocol engine.

pub mod app_record;
pub mod config;
pub mod consensus_state;
pub mod error;
pub mod follower_progress;
pub mod listener;
pub mod listener_event;
pub mod log_entry;
pub mod node_state;
pub mod quorum_state;
pub mod raft_node;
pub mod rpc;
pub mod snapshot;
pub mod traits;
pub mod types;
pub mod voter;

// Re-export core domain types for convenience.
pub use app_record::{AppRecord, AppSnapshot};
pub use listener::Listener;
pub use listener_event::ListenerEvent;
pub use snapshot::{
    Snapshot, SnapshotId, SnapshotMetadata, SnapshotReader, SnapshotWriter, VotersRecord,
};
pub use types::{ClusterId, NodeId, Offset, Term};
pub use voter::VoterInfo;
