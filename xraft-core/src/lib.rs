pub mod app_record;
pub mod error;
pub mod log_entry;
pub mod rpc;
pub mod types;
pub mod log_entry;
pub mod rpc;
pub mod traits;
pub mod error;
pub mod config;
pub mod consensus_state;
pub mod error;
pub mod follower_progress;
pub mod log_entry;
pub mod membership;
pub mod node_state;
pub mod quorum_state;
pub mod rpc;
pub mod snapshot;
pub mod traits;
pub mod types;

// Re-exports for convenience
pub use app_record::{AppRecord, AppSnapshot};
pub use config::RaftConfig;
pub use consensus_state::{ConsensusState, Role};
pub use error::{Result, XraftError};
pub use follower_progress::FollowerProgress;
pub use log_entry::{EntryType, LogEntry};
pub use membership::MembershipManager;
pub use node_state::{NodeState, PendingMembershipChange};
pub use quorum_state::QuorumState;
pub use rpc::{
    AddVoterRequest, MembershipChangeResponse, MembershipError, RemoveVoterRequest,
    UpdateVoterRequest,
};
pub use snapshot::{Snapshot, SnapshotId, SnapshotMetadata};
pub use snapshot_coordinator::SnapshotCoordinator;
pub use traits::{LogStore, QuorumStateStore, SnapshotIO, StateMachine};
pub use types::{ClusterId, NodeId, Offset, Term, VoterInfo, VotersRecord};
