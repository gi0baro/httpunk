//! Protocol-neutral error taxonomy — the errors that are not specific to any one
//! HTTP version. `HTTPunkError` is the root of everything httpunk raises;
//! `HTTPConnectionError` is the wide parent of what a connection reports (hyper's
//! `Io`, `Body`, `BodyWrite`, `Canceled`, `ChannelClosed` kinds), `UserError` the
//! parent of local misuse. The HTTP/2-specific errors live in `h2::errors` and the
//! HTTP/1-specific ones in `h1::errors`; both derive from these.

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::intern;
use pyo3::prelude::*;

/// `create_exception!` with more than one base: the same expansion, except that the
/// base handed to `PyErr_NewExceptionWithDoc` is a tuple of classes.
macro_rules! create_exception_multi {
    ($module: ident, $name: ident, ($($base: ty),+), $doc: expr) => {
        #[repr(transparent)]
        pub struct $name(pyo3::PyAny);

        pyo3::impl_exception_boilerplate!($name);
        pyo3::pyobject_native_type_named!($name);

        unsafe impl pyo3::type_object::PyTypeInfo for $name {
            #[inline]
            fn type_object_raw(py: pyo3::Python<'_>) -> *mut pyo3::ffi::PyTypeObject {
                static TYPE_OBJECT: pyo3::sync::PyOnceLock<pyo3::Py<pyo3::types::PyType>> =
                    pyo3::sync::PyOnceLock::new();
                TYPE_OBJECT
                    .get_or_init(py, || {
                        let bases =
                            pyo3::types::PyTuple::new(py, [$(py.get_type::<$base>()),+])
                                .expect("bases tuple");
                        // SAFETY: the C API takes one class or a tuple of classes as `base`.
                        unsafe {
                            pyo3::Bound::from_owned_ptr_or_err(
                                py,
                                pyo3::ffi::PyErr_NewExceptionWithDoc(
                                    concat!(stringify!($module), ".", stringify!($name), "\0")
                                        .as_ptr()
                                        .cast(),
                                    concat!($doc, "\0").as_ptr().cast(),
                                    bases.as_ptr(),
                                    std::ptr::null_mut(),
                                ),
                            )
                        }
                        .expect("Failed to initialize new exception type.")
                        .cast_into::<pyo3::types::PyType>()
                        .expect("a type")
                        .unbind()
                    })
                    .as_ptr()
                    .cast()
            }
        }
    };
}

pub(crate) use create_exception_multi;

create_exception!(
    _httpunk,
    HTTPunkError,
    PyException,
    "Base class for every httpunk error (HTTP/1 and HTTP/2)."
);
create_exception!(
    _httpunk,
    HTTPConnectionError,
    HTTPunkError,
    "The connection reported a failure: its transport, a body read or write, a \
     request it did not send, or its own closure. Protocol-neutral. `errno` is the \
     number of the OS error behind the failure, `None` when there is none (the class \
     attribute)."
);
create_exception!(
    _httpunk,
    TransportError,
    HTTPConnectionError,
    "The connection's transport failed or ended with work in flight (hyper `Kind::Io`, \
     h2 `Error::Io`). args = (message: str,)."
);
create_exception!(
    _httpunk,
    BodyError,
    HTTPConnectionError,
    "A body could not be read from the connection (hyper `Kind::Body`)."
);
create_exception!(
    _httpunk,
    BodyWriteError,
    HTTPConnectionError,
    "A message head or body could not be written to the connection (hyper \
     `Kind::BodyWrite`). args = (message: str,)."
);
create_exception!(
    _httpunk,
    RequestNotSentError,
    HTTPConnectionError,
    "The connection could not take the request: nothing of it reached the wire, so it \
     is safe to retry on another connection, whatever its body (hyper `Kind::Canceled` \
     with the request given back). args = (message: str,)."
);
create_exception!(
    _httpunk,
    ConnectionClosedError,
    HTTPConnectionError,
    "The connection is closed: closed locally (`close()`, `abort()`, a cancelled call), \
     or asked for after it ended (hyper `Kind::ChannelClosed`). args = (message: str,)."
);
create_exception!(
    _httpunk,
    UserError,
    HTTPunkError,
    "Local misuse the protocol stack detects (hyper `Kind::User`, h2 `UserError`)."
);

pub fn register(m: &Bound<PyModule>) -> PyResult<()> {
    let py = m.py();
    py.get_type::<HTTPConnectionError>()
        .setattr(intern!(py, "errno"), py.None())?;
    m.add("HTTPunkError", py.get_type::<HTTPunkError>())?;
    m.add("HTTPConnectionError", py.get_type::<HTTPConnectionError>())?;
    m.add("TransportError", py.get_type::<TransportError>())?;
    m.add("BodyError", py.get_type::<BodyError>())?;
    m.add("BodyWriteError", py.get_type::<BodyWriteError>())?;
    m.add("RequestNotSentError", py.get_type::<RequestNotSentError>())?;
    m.add(
        "ConnectionClosedError",
        py.get_type::<ConnectionClosedError>(),
    )?;
    m.add("UserError", py.get_type::<UserError>())?;
    Ok(())
}
