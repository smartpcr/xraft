//! xraft-transport: Async RPC transport layer.
//!
//! Provides an in-process channel-based transport for testing and a module
//! structure for future TCP-based production transport.

pub mod channel;

pub use channel::{ChannelTransportSender, ChannelTransportReceiver, create_channel_network};
