//! Protocol-neutral error taxonomy — the errors that are not specific to any one
//! HTTP version. `HTTPunkError` is the root of everything httpunk raises;
//! `ConnectionClosedError` is a transport failure both HTTP/1 and HTTP/2 surface.
//! The HTTP/2-specific errors live in `h2::streams` and derive from `HTTPunkError`.

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
    "The transport closed (EOF/reset/IO error) with work still in flight — a \
     transport failure, not a protocol violation (so no GOAWAY). Protocol-neutral: \
     raised on both HTTP/1 and HTTP/2, hence it sits under HTTPunkError, not H2Error."
);

pub fn register(m: &Bound<PyModule>) -> PyResult<()> {
    m.add("HTTPunkError", m.py().get_type::<HTTPunkError>())?;
    m.add(
        "ConnectionClosedError",
        m.py().get_type::<ConnectionClosedError>(),
    )?;
    Ok(())
}
