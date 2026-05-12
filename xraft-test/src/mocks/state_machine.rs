use xraft_core::app_record::{AppRecord, AppSnapshot};
use xraft_core::error::XraftError;
use xraft_core::traits::StateMachine;

pub struct MockStateMachine;

impl StateMachine for MockStateMachine {
    fn apply(&mut self, _offset: u64, _record: &AppRecord) -> Result<(), XraftError> {
        Ok(())
    }

    fn snapshot(&self) -> Result<AppSnapshot, XraftError> {
        Ok(AppSnapshot {
            data: Vec::new(),
        })
    }

    fn restore(&mut self, _snapshot: AppSnapshot) -> Result<(), XraftError> {
        Ok(())
    }
}
