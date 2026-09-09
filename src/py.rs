//! Helpers over the Python object model that the protocol modules share: the
//! places httpunk has to reach past pyo3's safe surface, kept together so each
//! trick is written (and justified) once. See BOUNDARY_NOTES.md for the rules they
//! implement.

use http::Method;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyString};

/// A `bytes` of exactly `len` bytes whose storage `fill` writes in full
/// (BOUNDARY_NOTES.md rule 4). A Rust producer that knows its output size writes
/// straight into the object's storage: `PyBytes::new` is this for a contiguous
/// source (one memcpy); `PyBytes::new_with` is not (it zero-fills the buffer first,
/// a second pass over the payload); over-allocating and shrinking with
/// `_PyBytes_Resize` loses on the GIL build above glibc's mmap threshold. So:
/// allocate uninitialised, fill once.
///
/// `fill` MUST write every byte of the slice it is given: the storage is
/// uninitialised memory until then, and the object is returned as-is.
pub(crate) fn bytes_filled(
    py: Python<'_>,
    len: usize,
    fill: impl FnOnce(&mut [u8]),
) -> PyResult<Bound<'_, PyBytes>> {
    // SAFETY: `PyBytes_FromStringAndSize(NULL, len)` allocates a bytes object of
    // `len` uninitialised bytes (its documented contract) and returns a new
    // reference or NULL with an exception set; the slice covers exactly that
    // storage, which nothing else can see before the object is returned, and
    // `fill` initialises all of it. The object is then owned by the `Bound`.
    unsafe {
        let obj = pyo3::ffi::PyBytes_FromStringAndSize(
            std::ptr::null(),
            pyo3::ffi::Py_ssize_t::try_from(len).map_err(|_| {
                pyo3::exceptions::PyOverflowError::new_err("bytes size does not fit Py_ssize_t")
            })?,
        );
        if obj.is_null() {
            return Err(PyErr::fetch(py));
        }
        if len > 0 {
            let data = pyo3::ffi::PyBytes_AsString(obj).cast::<u8>();
            fill(std::slice::from_raw_parts_mut(data, len));
        }
        Ok(Bound::from_owned_ptr(py, obj).cast_into_unchecked())
    }
}

// ----- closed vocabularies, interned once (BOUNDARY_NOTES.md rule 6) -----
//
// A `str` for a value from a fixed set is one shared object for the whole process,
// built on first use and handed out by reference: the standard methods, the schemes,
// the body kinds. A value outside the set gets a fresh `str`, as any string would.

const STANDARD_METHODS: [Method; 9] = [
    Method::GET,
    Method::POST,
    Method::PUT,
    Method::DELETE,
    Method::HEAD,
    Method::OPTIONS,
    Method::CONNECT,
    Method::PATCH,
    Method::TRACE,
];
static METHODS: PyOnceLock<[Py<PyString>; 9]> = PyOnceLock::new();

/// The `str` for `method`: the shared object for one of the nine standard methods
/// (`http::Method` resolves those without allocating too), else a fresh `str`.
pub(crate) fn method_str(py: Python<'_>, method: &Method) -> Py<PyString> {
    let table = METHODS.get_or_init(py, || {
        STANDARD_METHODS.map(|m| PyString::intern(py, m.as_str()).unbind())
    });
    match STANDARD_METHODS.iter().position(|m| m == method) {
        Some(i) => table[i].clone_ref(py),
        None => PyString::new(py, method.as_str()).unbind(),
    }
}

const SCHEMES: [&str; 2] = ["http", "https"];
static SCHEME_STRS: PyOnceLock<[Py<PyString>; 2]> = PyOnceLock::new();

/// The `str` for a `:scheme` / URL scheme: shared for `http` and `https`.
pub(crate) fn scheme_str(py: Python<'_>, scheme: &str) -> Py<PyString> {
    let table = SCHEME_STRS.get_or_init(py, || SCHEMES.map(|s| PyString::intern(py, s).unbind()));
    match SCHEMES.iter().position(|s| *s == scheme) {
        Some(i) => table[i].clone_ref(py),
        None => PyString::new(py, scheme).unbind(),
    }
}

/// The h1 body framing kinds the drivers switch on (`H1BodyDecoder`).
pub(crate) const BODY_KINDS: [&str; 4] = ["empty", "length", "chunked", "close"];
static BODY_KIND_STRS: PyOnceLock<[Py<PyString>; 4]> = PyOnceLock::new();

/// The `str` for a body kind: always one of `BODY_KINDS`, always the shared object.
pub(crate) fn body_kind_str(py: Python<'_>, kind: &'static str) -> Py<PyString> {
    let table =
        BODY_KIND_STRS.get_or_init(py, || BODY_KINDS.map(|s| PyString::intern(py, s).unbind()));
    let i = BODY_KINDS
        .iter()
        .position(|s| *s == kind)
        .expect("a known body kind");
    table[i].clone_ref(py)
}
