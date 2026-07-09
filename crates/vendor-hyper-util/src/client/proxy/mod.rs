//! Hand-written module glue (not from upstream): exposes only the vendored
//! `matcher`. Upstream's `proxy/mod.rs` also wires SOCKS/tunnel connectors, which
//! are IO and not vendored.

pub mod matcher;
