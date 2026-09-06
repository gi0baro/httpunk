//! HTTP/1 PyO3 classes exposed to Python. The Python-facing names are
//! `H1`-prefixed (`H1Codec`, `H1ResponseHead`) via each pyclass's `name = "..."`.
//! The codec drives the vendored hyper h1 sans-IO core (`crate::hyper`).

mod codec;
mod conn;
mod errors;

use pyo3::prelude::*;

/// Register the HTTP/1 pyclasses on the extension module.
pub fn register(m: &Bound<PyModule>) -> PyResult<()> {
    errors::register(m)?;
    m.add_class::<codec::H1Codec>()?;
    m.add_class::<codec::ResponseHead>()?;
    m.add_class::<codec::RequestHead>()?;
    m.add_class::<codec::H1BodyDecoder>()?;
    m.add_class::<conn::H1ServerState>()?;
    m.add_class::<conn::H1ClientState>()?;
    m.add_class::<conn::OnceLatch>()?;
    for (name, value) in conn::CLIENT_CONSTANTS {
        m.add(name, value)?;
    }
    for (name, value) in conn::CONSTANTS {
        m.add(name, value)?;
    }
    Ok(())
}
