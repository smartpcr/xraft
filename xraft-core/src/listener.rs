use crate::app_record::AppRecord;
use crate::snapshot::SnapshotReader;
use crate::types::{NodeId, Term};

/// Application callback interface invoked by the `EventLoop` during
/// message processing, before any `IoAction` is dispatched.
///
/// Modelled on KRaft's `RaftClient.Listener`. All methods are
/// synchronous, infallible, and must not panic — a panic aborts the
/// event loop and halts the node.
///
/// Implementations must be lightweight and non-blocking. Applications
/// that need heavy processing should hand off work to their own async
/// tasks from within these callbacks.
pub trait Listener: Send + 'static {
    /// Called when a batch of application records is committed (HW advanced).
    ///
    /// Only application records appear here; control records
    /// (`LeaderChangeMessage`, `VotersRecord`) are filtered by the
    /// consensus layer. Each tuple contains `(offset, record)`.
    ///
    /// This is the primary mechanism for applications to build their own
    /// queryable read-side state.
    fn handle_commit(&mut self, batch: &[(u64, AppRecord)]);

    /// Called when a snapshot must be loaded (after `FetchSnapshot` completes).
    ///
    /// The `SnapshotReader` provides access to both consensus metadata and
    /// the application snapshot payload. The reader is consumed by this call.
    fn handle_load_snapshot(&mut self, reader: SnapshotReader);

    /// Called on leadership change.
    ///
    /// `leader_id` is the new leader's node identifier; `term` is the
    /// term in which the leadership change occurred.
    fn handle_leader_change(&mut self, leader_id: NodeId, term: Term);

    /// Called during graceful shutdown.
    ///
    /// Implementations should release resources and prepare for the node
    /// to stop. No further callbacks will be invoked after this one.
    fn begin_shutdown(&mut self);
}
