//! Vendored hyper-util (`hyper_util`) sans-IO proxy matcher plus upstream's
//! feature-gated `client/mod.rs` and `client/proxy/mod.rs`. Only this crate root is
//! hand-written: upstream's `lib.rs` unconditionally wires unvendored common/rt
//! modules. Owning the crate root preserves upstream's `self::`/`crate::` paths.
//! `client-proxy` is on by default; legacy, pool and platform features stay off
//! because their code/dependencies are not vendored. `scripts/vendor-hyper-util.sh`
//! enumerates every modification to the copied files.

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
