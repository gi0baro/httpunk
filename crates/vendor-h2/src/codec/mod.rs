//! Hand-written glue: exposes the vendored `codec::error` types (`UserError`,
//! `SendError`) that the state machine and proto error types reference. h2's
//! async codec (FramedRead/FramedWrite over AsyncRead/Write) is not vendored.

#![allow(dead_code)]

pub mod error;

pub use self::error::{SendError, UserError};
