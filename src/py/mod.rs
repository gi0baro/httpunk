//! The PyO3 adapter layer: everything exposed to Python. Kept outside the
//! vendored `crate::h2` module so it gets full lints (h2/ blanket-allows clippy
//! for the vendored code). All exposed pyclasses are `frozen` with internal
//! `Mutex` for sound sharing across the runtime's worker threads.
//!
//! Organized by protocol: `h2` today, `h1` later. Rust struct names are kept
//! short/internal; the Python-facing names (the pyclass `name = "..."`) are all
//! `H2`-prefixed so their origin is unambiguous from Python.

mod errors;
mod h1;
mod h2;
mod http;
mod proxy;

use pyo3::prelude::*;

/// Register all pyclasses, exceptions, and enums on the extension module.
pub fn register(m: &Bound<PyModule>) -> PyResult<()> {
    errors::register(m)?;
    http::register(m)?;
    h2::register(m)?;
    h1::register(m)?;
    proxy::register(m)?;
    Ok(())
}
