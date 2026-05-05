use crate::app_record::AppRecord;
use crate::snapshot::SnapshotReader;
use crate::types::{NodeId, Term};

/// Internal event type used by the `EventLoop` for dispatching
/// `Listener` callbacks.
///
/// Each variant corresponds to exactly one `Listener` method.
/// These are synchronous in-process calls — NOT `IoAction` variants.
/// The `EventLoop` constructs a `ListenerEvent`, then dispatches it
/// to the application's `Listener` implementation.
#[derive(Debug)]
pub enum ListenerEvent {
    /// A batch of application records has been committed.
    /// Maps to `Listener::handle_commit`.
    Commit {
        /// Committed `(offset, record)` pairs. Only application records;
        /// control records are filtered.
        batch: Vec<(u64, AppRecord)>,
    },

    /// A snapshot must be loaded on a follower.
    /// Maps to `Listener::handle_load_snapshot`.
    LoadSnapshot {
        /// Reader providing access to the snapshot data.
        reader: SnapshotReader,
    },

    /// Leadership has changed.
    /// Maps to `Listener::handle_leader_change`.
    LeaderChange {
        /// The new leader's node identifier.
        leader_id: NodeId,
        /// The term in which the change occurred.
        term: Term,
    },

    /// Graceful shutdown initiated.
    /// Maps to `Listener::begin_shutdown`.
    Shutdown,
}

impl ListenerEvent {
    /// Dispatches this event to the given `Listener`, invoking the
    /// corresponding callback method.
    pub fn dispatch<L: crate::listener::Listener>(self, listener: &mut L) {
        match self {
            ListenerEvent::Commit { batch } => {
                listener.handle_commit(&batch);
            }
            ListenerEvent::LoadSnapshot { reader } => {
                listener.handle_load_snapshot(reader);
            }
            ListenerEvent::LeaderChange { leader_id, term } => {
                listener.handle_leader_change(leader_id, term);
            }
            ListenerEvent::Shutdown => {
                listener.begin_shutdown();
            }
        }
    }
}
