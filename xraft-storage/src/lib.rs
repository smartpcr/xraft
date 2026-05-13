//! xraft-storage: durable log and snapshot storage.

mod segment;
mod segment_index;
mod segment_log;

pub use segment_log::{SegmentLog, SegmentLogConfig};
