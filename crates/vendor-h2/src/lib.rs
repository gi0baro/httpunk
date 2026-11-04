//! Vendored from hyperium/h2 (MIT licensed — see `crates/vendor-h2/LICENSE`).
//! Pinned version is in `crates/vendor-h2/UPSTREAM_VERSION`; re-vendor with
//! `scripts/vendor-h2.sh`, which enumerates every modification to upstream.
//!
//! Only h2's **synchronous, sans-IO** modules are vendored: `frame` (HTTP/2
//! frame parse/serialize) and `hpack` (HPACK header compression), the tiny
//! `ext::Protocol` type they depend on, the stream state machine + flow control
//! (`proto::streams::{state,flow_control}`), the error types (`proto::error`,
//! `codec::error`, the public `error`). h2's async `codec`, the streams store
//! and orchestration, `client` and `server` are intentionally *not* vendored —
//! their state is re-expressed in httpunk's `src/h2/conn.rs`, their async half
//! in Python.
//!
//! The vendored `.rs` files are kept **byte-identical to upstream** modulo the
//! script's edits: dropping `hpack/test/` (heavy external test-only deps) and its
//! module reference, the `state.rs` `recv_open` shim and test-module strip, and a
//! uniform `pub(crate)` -> `pub` widening. This is a separate crate, so upstream's
//! `crate::` paths resolve natively. A `git diff` between two vendored versions
//! shows only genuine upstream changes.

#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(clippy::all)]
#![allow(clippy::pedantic)]

// Vendored from h2 lib.rs: used by the vendored proto modules. Defined before
// the `mod` declarations so macro textual scoping makes it visible to them.
macro_rules! proto_err {
    (conn: $($msg:tt)+) => {
        tracing::debug!("connection error PROTOCOL_ERROR -- {};", format_args!($($msg)+))
    };
    (stream: $($msg:tt)+) => {
        tracing::debug!("stream error PROTOCOL_ERROR -- {};", format_args!($($msg)+))
    };
}

pub mod codec;
pub mod error;
pub mod ext;
pub mod frame;
pub mod hpack;
pub mod proto;

// h2 lib.rs re-exports its public `Error` at the crate root; `proto/streams/state.rs`
// names it as `crate::Error` (`ensure_reason`).
pub use crate::error::Error;
