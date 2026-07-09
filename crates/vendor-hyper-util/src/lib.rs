//! Vendored hyper-util (`hyper_util`) sans-IO code — currently just the proxy
//! matcher. Hand-written crate-root glue: it owns the crate root so upstream's own
//! `self::`/`crate::` paths resolve verbatim (no namespace rewrite), and re-exposes
//! only the vendored `client::proxy::matcher`. The genuinely-IO / async and
//! platform-specific hyper-util code (legacy client, pool, connect, `from_system`)
//! is not vendored. See ../../THIRD-PARTY.md.

// The vendored `.rs` files are byte-identical to upstream, so they are exempt from
// this workspace's lints (upstream targets a different clippy/rustc version and
// keeps unused-in-our-build items). Same blanket allows as vendor-h2/vendor-hyper.
#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(private_interfaces)]
#![allow(unexpected_cfgs)]
#![allow(clippy::all)]
#![allow(clippy::pedantic)]

pub mod client;
