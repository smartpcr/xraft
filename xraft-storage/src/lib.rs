//! xraft-storage: Storage backends for the Raft protocol.
//!
//! Provides in-memory implementations of the `LogStore` and `QuorumStateStore`
//! traits for testing, and a module structure for future durable
//! (segment-log based) storage.

pub mod memory_log;
pub mod memory_quorum_state;

pub use memory_log::MemoryLogStore;
pub use memory_quorum_state::MemoryQuorumStateStore;
