//! Protocol-neutral error taxonomy — the errors that are not specific to any one
//! HTTP version. `HTTPunkError` is the root of everything httpunk raises;
//! `ConnectionClosedError` is a transport failure both HTTP/1 and HTTP/2 surface
//! (hyper `Kind::Io`, h2 `Error::Io`). The HTTP/2-specific errors live in
//! `h2::errors` and the HTTP/1-specific ones (hyper's other `Kind`s) in
//! `h1::errors`; both derive from `HTTPunkError`.

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(
    _httpunk,
    HTTPunkError,
    PyException,
    "Base class for every httpunk error (HTTP/1 and HTTP/2)."
);
create_exception!(
    _httpunk,
    ConnectionClosedError,
    HTTPunkError,
    "The transport failed (reset / IO error) with work still in flight — hyper's \
     `Kind::Io`, h2's `Error::Io`: a transport failure, not a protocol violation (so \
     no GOAWAY) and not the HTTP state noticing an EOF (HTTP/1: `H1IncompleteMessageError`, \
     `H1BodyError`). Protocol-neutral: raised on both HTTP/1 and HTTP/2, hence it sits \
     under HTTPunkError."
);

pub fn register(m: &Bound<PyModule>) -> PyResult<()> {
    m.add("HTTPunkError", m.py().get_type::<HTTPunkError>())?;
    m.add(
        "ConnectionClosedError",
        m.py().get_type::<ConnectionClosedError>(),
    )?;
    Ok(())
}
