//! Minimal, hand-written glue for the vendored *synchronous* pieces of h2's
//! `proto` module: the stream state machine (`streams::state`), flow control
//! (`streams::flow_control`), and the error types (`error`). h2's async proto
//! modules (connection, ping_pong, go_away, and the streams orchestration in
//! recv/send/prioritize/streams/store/buffer/counts) are intentionally NOT
//! vendored — that logic is rewritten in Python. Only the items the vendored
//! files reference are re-exported here.

#![allow(dead_code)]

pub mod error;
pub mod streams;

pub use self::error::{Error, GoAway, Initiator};

// From h2 proto/mod.rs — used by flow_control.
pub type WindowSize = u32;
pub const MAX_WINDOW_SIZE: WindowSize = (1 << 31) - 1; // i32::MAX as u32
