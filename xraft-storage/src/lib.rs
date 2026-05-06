//! xraft-storage: Storage backends for the Raft protocol.
//!
//! Provides in-memory implementations of the `LogStore` and `QuorumStateStore`
//! traits for testing, and a module structure for future durable
//! (segment-log based) storage.

pub use segment_log::SegmentLog;
