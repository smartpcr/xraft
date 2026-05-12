use xraft_core::app_record::{AppRecord, AppSnapshot};
use xraft_core::error::Result;
use xraft_core::traits::StateMachine;

/// Trivial in-memory state machine for testing.
/// Stores applied offsets so tests can verify commit order.
pub struct MemoryStateMachine {
    pub applied_offsets: Vec<u64>,
    pub snapshot_data: Vec<u8>,
}

impl MemoryStateMachine {
    pub fn new() -> Self {
        Self {
            applied_offsets: Vec::new(),
            snapshot_data: Vec::new(),
        }
    }
}

impl Default for MemoryStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl StateMachine for MemoryStateMachine {
    fn apply(&mut self, offset: u64, _record: &AppRecord) -> Result<()> {
        self.applied_offsets.push(offset);
        Ok(())
    }

    fn snapshot(&self) -> Result<AppSnapshot> {
        Ok(AppSnapshot {
            data: self.snapshot_data.clone(),
        })
    }

    fn restore(&mut self, snapshot: AppSnapshot) -> Result<()> {
        self.snapshot_data = snapshot.data;
        self.applied_offsets.clear();
        Ok(())
    }
}
