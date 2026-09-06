//! `H2Reason` — h2's `frame::Reason` error codes (RFC 9113 §7) as a Python enum,
//! defined in Rust with pyo3's simple-enum support: one class attribute per code,
//! `__int__`/`__index__` (a member passes straight into any `u32` parameter and
//! `int(member)` is its code), `==` against both members and plain ints
//! (`eq_int`), a hash equal to the code's so members and codes interchange as
//! dict keys, and `H2Reason(code)` looking a code up (`ValueError` when unknown —
//! h2's `Reason` is an open `u32`, so an unknown peer code stays a plain int on
//! the Python side, `exceptions._reason`).
//!
//! The vendored `frame::Reason` constants are the single source of truth for the
//! codes: `TABLE` pairs every member with its constant, and registration asserts
//! the two agree (a debug build catches a drift at import).

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use vendor_h2::frame::Reason;

#[pyclass(
    module = "httpunk._httpunk",
    name = "H2Reason",
    eq,
    eq_int,
    frozen,
    skip_from_py_object
)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[repr(u32)]
pub enum H2Reason {
    #[pyo3(name = "NO_ERROR")]
    NoError = 0,
    #[pyo3(name = "PROTOCOL_ERROR")]
    ProtocolError = 1,
    #[pyo3(name = "INTERNAL_ERROR")]
    InternalError = 2,
    #[pyo3(name = "FLOW_CONTROL_ERROR")]
    FlowControlError = 3,
    #[pyo3(name = "SETTINGS_TIMEOUT")]
    SettingsTimeout = 4,
    #[pyo3(name = "STREAM_CLOSED")]
    StreamClosed = 5,
    #[pyo3(name = "FRAME_SIZE_ERROR")]
    FrameSizeError = 6,
    #[pyo3(name = "REFUSED_STREAM")]
    RefusedStream = 7,
    #[pyo3(name = "CANCEL")]
    Cancel = 8,
    #[pyo3(name = "COMPRESSION_ERROR")]
    CompressionError = 9,
    #[pyo3(name = "CONNECT_ERROR")]
    ConnectError = 10,
    #[pyo3(name = "ENHANCE_YOUR_CALM")]
    EnhanceYourCalm = 11,
    #[pyo3(name = "INADEQUATE_SECURITY")]
    InadequateSecurity = 12,
    #[pyo3(name = "HTTP_1_1_REQUIRED")]
    Http11Required = 13,
}

/// Every member, its vendored constant, and its Python name — one table.
const TABLE: [(H2Reason, Reason, &str); 14] = [
    (H2Reason::NoError, Reason::NO_ERROR, "NO_ERROR"),
    (
        H2Reason::ProtocolError,
        Reason::PROTOCOL_ERROR,
        "PROTOCOL_ERROR",
    ),
    (
        H2Reason::InternalError,
        Reason::INTERNAL_ERROR,
        "INTERNAL_ERROR",
    ),
    (
        H2Reason::FlowControlError,
        Reason::FLOW_CONTROL_ERROR,
        "FLOW_CONTROL_ERROR",
    ),
    (
        H2Reason::SettingsTimeout,
        Reason::SETTINGS_TIMEOUT,
        "SETTINGS_TIMEOUT",
    ),
    (
        H2Reason::StreamClosed,
        Reason::STREAM_CLOSED,
        "STREAM_CLOSED",
    ),
    (
        H2Reason::FrameSizeError,
        Reason::FRAME_SIZE_ERROR,
        "FRAME_SIZE_ERROR",
    ),
    (
        H2Reason::RefusedStream,
        Reason::REFUSED_STREAM,
        "REFUSED_STREAM",
    ),
    (H2Reason::Cancel, Reason::CANCEL, "CANCEL"),
    (
        H2Reason::CompressionError,
        Reason::COMPRESSION_ERROR,
        "COMPRESSION_ERROR",
    ),
    (
        H2Reason::ConnectError,
        Reason::CONNECT_ERROR,
        "CONNECT_ERROR",
    ),
    (
        H2Reason::EnhanceYourCalm,
        Reason::ENHANCE_YOUR_CALM,
        "ENHANCE_YOUR_CALM",
    ),
    (
        H2Reason::InadequateSecurity,
        Reason::INADEQUATE_SECURITY,
        "INADEQUATE_SECURITY",
    ),
    (
        H2Reason::Http11Required,
        Reason::HTTP_1_1_REQUIRED,
        "HTTP_1_1_REQUIRED",
    ),
];

impl H2Reason {
    /// The member for a wire code, if the code is one of the RFC set.
    pub fn from_code(code: u32) -> Option<Self> {
        TABLE
            .iter()
            .find(|(member, _, _)| *member as u32 == code)
            .map(|(member, _, _)| *member)
    }

    fn py_name(self) -> &'static str {
        TABLE
            .iter()
            .find(|(member, _, _)| *member == self)
            .map_or("?", |(_, _, name)| name)
    }

    /// Registration-time check that the members carry the vendored constants' values
    /// (the `#[repr(u32)]` discriminants must be literals, so they are asserted rather
    /// than derived).
    pub(super) fn check_table() {
        debug_assert!(
            TABLE
                .iter()
                .all(|(member, reason, _)| *member as u32 == u32::from(*reason)),
            "H2Reason discriminants drifted from the vendored frame::Reason constants"
        );
    }
}

#[pymethods]
#[allow(clippy::trivially_copy_pass_by_ref)] // pyo3 dictates the `&self` receiver
impl H2Reason {
    /// `H2Reason(code)`: the member for `code`; `ValueError` if it is not an RFC code.
    #[new]
    fn new(code: u32) -> PyResult<Self> {
        Self::from_code(code)
            .ok_or_else(|| PyValueError::new_err(format!("{code} is not a valid H2Reason")))
    }

    /// The wire code (a member passes into any `int` parameter as its code).
    fn __index__(&self) -> u32 {
        *self as u32
    }

    /// Same hash as the code's `int`, so a member and its code interchange as keys.
    fn __hash__(&self) -> u64 {
        *self as u64
    }

    #[getter]
    fn value(&self) -> u32 {
        *self as u32
    }

    #[getter]
    fn name(&self) -> &'static str {
        self.py_name()
    }

    fn __repr__(&self) -> String {
        format!("H2Reason.{}", self.py_name())
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}
