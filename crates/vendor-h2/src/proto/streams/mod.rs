//! Hand-written glue: exposes the two vendored synchronous stream modules.
//! The rest of h2's `proto/streams/*` (the store + orchestration) is re-expressed
//! in httpunk's `src/h2/conn.rs` (state) and Python (async machinery).

#![allow(dead_code)]

pub mod flow_control;
pub mod state;

pub use self::flow_control::{FlowControl, Window};
pub use self::state::State;
