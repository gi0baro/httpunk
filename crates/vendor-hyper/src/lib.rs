//! Vendored from hyperium/hyper 1.10.1 (MIT licensed — see `THIRD-PARTY.md`).
//!
//! Only hyper's **synchronous, sans-IO** HTTP/1 pieces are vendored: `proto::h1`
//! head parse/encode (`role`) and the body `Encoder` (`encode`), plus the tiny
//! support types they depend on (`headers`, `ext`, `error`, `body::length`,
//! `proto::MessageHead`). hyper's async `conn`, `decode`, `io`, `dispatch`,
//! `client`, `server` and `upgrade` are intentionally *not* vendored — that
//! orchestration is rewritten in Python.
//!
//! The `.rs` files under here are kept **byte-identical to upstream** so a
//! `git diff` between two vendored versions shows only genuine upstream changes.
//! Because this is a separate crate, hyper's own crate-root paths (`crate::proto`,
//! `crate::error`, `crate::Method`, …) resolve here natively — the re-exports
//! below reproduce the names hyper's `lib.rs` re-exports at its crate root, and
//! the `pub mod`s mirror hyper's `src/` layout. Deviations from upstream are
//! enumerated in `THIRD-PARTY.md`. The httpunk-authored bridge/facade
//! (`proto::h1::httpunk`) is re-exported publicly at the bottom for the main crate.

#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_macros)]
// Vendored items keep upstream's `pub(super) enum Parse` visibility even though
// some `pub` signatures reference it; don't tighten vendored visibility.
#![allow(private_interfaces)]
// hyper's `trace.rs` references the custom `hyper_unstable_tracing` cfg (only
// live under its unbuilt `tracing` feature); silence the check-cfg warning
// rather than editing the vendored macro module.
#![allow(unexpected_cfgs)]
#![allow(clippy::all)]
#![allow(clippy::pedantic)]

// Vendored macro modules (hyper `src/cfg.rs` and `src/trace.rs`). Kept
// byte-identical (they contain no `crate::` paths). `#[macro_use]` reproduces
// hyper's textual macro scoping so the vendored modules below can use
// `cfg_feature!`/`cfg_client!`/`cfg_server!` and the `trace!`/`debug!`/… no-op
// logging wrappers.
#[macro_use]
mod cfg;
#[macro_use]
mod trace;

// hyper's `lib.rs` re-exports these at its crate root; reproduce them here so
// the vendored files' `crate::{...}` paths resolve.
pub use http::{header, HeaderMap, Method, Request, Response, StatusCode, Uri, Version};

pub use self::error::{Error, Result};

pub mod body;
mod common; // only common/date (the server's Date header); see common/mod.rs
pub mod error;
pub mod ext;
pub mod headers;
pub mod proto;

// httpunk's public facade over the sans-IO codec (the only public API of this
// crate — the rest is hyper-internal machinery). See
// `proto/h1/httpunk.rs`.
pub use crate::proto::h1::httpunk::{
    encode_request, encode_response, parse_request, parse_response, BodyDecode, BodyDecoder,
    BodyEncoder, ParsedHead, ParsedRequest,
};
