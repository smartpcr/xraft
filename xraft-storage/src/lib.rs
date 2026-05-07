//! xraft-storage: Storage backends for the Raft protocol.
//!
//! Provides in-memory implementations of the `LogStore` and `QuorumStateStore`
//! traits for testing, and a module structure for future durable
//! (segment-log based) storage.

mod memory_log;
mod memory_log_store;
mod segment;
mod segment_index;
mod segment_log;

pub use segment_log::{SegmentLog, SegmentLogConfig};