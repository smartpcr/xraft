//! Listener trait for application-level commit and leader-change notification.
//!
//! Per architecture §4.1, the EventLoop invokes Listener callbacks synchronously
//! during message processing — after StateMachine::apply and before
//! DeferredCompletionQueue::complete. The Listener receives committed AppRecord
//! values (control records are filtered) and leadership changes.

use crate::types::{AppRecord, NodeId, Term};

/// Application callback for commit and leader-change events.
///
/// Invoked by the EventLoop in the three-phase commit notification sequence:
/// 1. `StateMachine::apply` — per committed command entry
/// 2. `Listener::handle_commit` — batch of committed AppRecords
/// 3. `DeferredCompletionQueue::complete` — resolves client futures
pub trait Listener: Send + 'static {
    /// Called once with the full batch of newly committed application records.
    /// Control records (LeaderChangeMessage, VotersRecord) are filtered.
    fn handle_commit(&mut self, committed: &[AppRecord]);

    /// Called when the leadership changes (new leader elected or stepped down).
    fn handle_leader_change(&mut self, new_leader: Option<NodeId>, term: Term);
}

/// No-op listener for tests that don't need commit notification.
pub struct NoOpListener;

impl Listener for NoOpListener {
    fn handle_commit(&mut self, _committed: &[AppRecord]) {}
    fn handle_leader_change(&mut self, _new_leader: Option<NodeId>, _term: Term) {}
}
