//! xraft-storage: Durable storage backends for xraft.

mod memory_log_store;
pub use memory_log_store::MemoryLogStore;
