//! Minimal, hand-written glue for the vendored *synchronous* pieces of h2's
//! `proto` module: the stream state machine (`streams::state`), flow control
//! (`streams::flow_control`), and the error types (`error`). h2's async proto
//! modules (connection, ping_pong, go_away, and the streams orchestration in
//! recv/send/prioritize/streams/store/buffer/counts) are intentionally NOT
//! vendored — their *state* is re-expressed in httpunk's `src/h2/conn.rs`
//! (`H2Streams`, one mutex per connection, the mirror of `Streams::inner`) and
//! their *async* half (tasks, I/O) in Python. Only the items the vendored files
//! reference are re-exported here.

#![allow(dead_code)]

pub mod error;
pub mod streams;

pub use self::error::{Error, GoAway, Initiator};

/// h2 proto/streams/send.rs L43-48 (0.4.19): which public API called `poll_reset` —
/// the `mode` of `State::ensure_reason`. The file that defines it (`send.rs`) is not
/// vendored (async orchestration), so the two-variant enum is mirrored here.
#[derive(Debug)]
pub enum PollReset {
    AwaitingHeaders,
    Streaming,
}

// From h2 proto/mod.rs — used by flow_control.
pub type WindowSize = u32;
pub const MAX_WINDOW_SIZE: WindowSize = (1 << 31) - 1; // i32::MAX as u32
