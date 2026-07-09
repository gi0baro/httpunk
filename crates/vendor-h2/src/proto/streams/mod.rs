//! Hand-written glue: exposes the two vendored synchronous stream modules.
//! The rest of h2's `proto/streams/*` (the async orchestration) lives in Python.

#![allow(dead_code)]

pub mod flow_control;
pub mod state;

pub use self::flow_control::{FlowControl, Window};
pub use self::state::State;
