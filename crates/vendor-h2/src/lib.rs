//! Vendored from hyperium/h2 (MIT licensed — see `LICENSE-h2` and
//! `THIRD-PARTY.md`). Pinned version is in `src/h2/UPSTREAM_VERSION`; re-vendor
//! with `scripts/vendor-h2.sh`.
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
//! The `.rs` files under here are kept **byte-identical to upstream modulo a
//! uniform `crate::` -> `crate::` rewrite** the vendor script applies, so
//! this crate's h2 code is isolated under `crate::` — symmetric with the
//! vendored hyper code under `crate::hyper::`, neither owning the crate root. A
//! `git diff` between two vendored versions still shows only genuine upstream
//! changes. The other edit the vendor script applies is dropping `hpack/test/`
//! (heavy external test-only deps) and its module reference.

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
