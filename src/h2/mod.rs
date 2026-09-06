//! HTTP/2 PyO3 classes exposed to Python. Rust struct names are short/internal
//! (e.g. `Headers`); the Python names are `H2`-prefixed (e.g. `H2FrameHeaders`)
//! via each pyclass's `name = "..."`.

mod codec;
mod errors;
mod streams;

use pyo3::prelude::*;
use pyo3::types::PyDict;

use vendor_h2::frame::Reason;

/// Build a Python `enum.IntEnum` named `H2Reason` whose members' values come
/// from the vendored `frame::Reason` constants (single source of truth). An
/// `IntEnum` is used deliberately: reason codes cross the FFI boundary as plain
/// `u32` (h2's `Reason` is an open `u32` newtype, so unknown peer codes stay
/// ints), and `IntEnum` members *are* ints — so they pass straight into `u32`
/// params and compare equal to `error_code` fields.
fn build_reason(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let members = PyDict::new(py);
    for (name, reason) in [
        ("NO_ERROR", Reason::NO_ERROR),
        ("PROTOCOL_ERROR", Reason::PROTOCOL_ERROR),
        ("INTERNAL_ERROR", Reason::INTERNAL_ERROR),
        ("FLOW_CONTROL_ERROR", Reason::FLOW_CONTROL_ERROR),
        ("SETTINGS_TIMEOUT", Reason::SETTINGS_TIMEOUT),
        ("STREAM_CLOSED", Reason::STREAM_CLOSED),
        ("FRAME_SIZE_ERROR", Reason::FRAME_SIZE_ERROR),
        ("REFUSED_STREAM", Reason::REFUSED_STREAM),
        ("CANCEL", Reason::CANCEL),
        ("COMPRESSION_ERROR", Reason::COMPRESSION_ERROR),
        ("CONNECT_ERROR", Reason::CONNECT_ERROR),
        ("ENHANCE_YOUR_CALM", Reason::ENHANCE_YOUR_CALM),
        ("INADEQUATE_SECURITY", Reason::INADEQUATE_SECURITY),
        ("HTTP_1_1_REQUIRED", Reason::HTTP_1_1_REQUIRED),
    ] {
        members.set_item(name, u32::from(reason))?;
    }
    let int_enum = py.import("enum")?.getattr("IntEnum")?;
    let reason = int_enum.call1(("H2Reason", members))?;
    reason.setattr("__module__", "httpunk._httpunk")?;
    Ok(reason.unbind())
}

/// Register the HTTP/2 pyclasses, exceptions, and enums on the extension module.
pub fn register(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<codec::H2Codec>()?;
    m.add_class::<codec::Headers>()?;
    m.add_class::<codec::Data>()?;
    m.add_class::<codec::Settings>()?;
    m.add_class::<codec::WindowUpdate>()?;
    m.add_class::<codec::Ping>()?;
    m.add_class::<codec::GoAway>()?;
    m.add_class::<codec::RstStream>()?;
    m.add_class::<codec::Priority>()?;
    m.add_class::<codec::StreamErrorFrame>()?;

    m.add_class::<streams::H2StreamState>()?;
    m.add_class::<streams::H2FlowControl>()?;
    m.add_class::<streams::H2ContentLength>()?;
    m.add_class::<streams::H2DataFrameBudget>()?;
    errors::register(m)?;
    m.add("H2Reason", build_reason(m.py())?)?;
    // The vendored h2's protocol constants (frame/settings.rs, frame/stream_id.rs) and
    // the proto/mod.rs budget constants mirrored in `streams`: the single source of
    // truth for the driver's defaults and range checks.
    m.add(
        "H2_DEFAULT_HEADER_TABLE_SIZE",
        vendor_h2::frame::DEFAULT_SETTINGS_HEADER_TABLE_SIZE,
    )?;
    m.add(
        "H2_DEFAULT_INITIAL_WINDOW_SIZE",
        vendor_h2::frame::DEFAULT_INITIAL_WINDOW_SIZE,
    )?;
    m.add(
        "H2_DEFAULT_MAX_FRAME_SIZE",
        vendor_h2::frame::DEFAULT_MAX_FRAME_SIZE,
    )?;
    m.add(
        "H2_MAX_MAX_FRAME_SIZE",
        vendor_h2::frame::MAX_MAX_FRAME_SIZE,
    )?;
    m.add(
        "H2_MAX_STREAM_ID",
        u32::from(vendor_h2::frame::StreamId::MAX),
    )?;
    m.add(
        "H2_DEFAULT_DATA_FRAME_OVERHEAD_THRESHOLD",
        streams::DEFAULT_DATA_FRAME_OVERHEAD_THRESHOLD,
    )?;
    m.add(
        "H2_DEFAULT_DATA_FRAME_BUDGET",
        streams::DEFAULT_DATA_FRAME_BUDGET,
    )?;
    m.add(
        "H2_MAX_RECV_EMPTY_DATA_FRAMES",
        streams::MAX_RECV_EMPTY_DATA_FRAMES,
    )?;
    m.add(
        "H2_PREFACE",
        pyo3::types::PyBytes::new(m.py(), vendor_hyper::H2_PREFACE),
    )?;

    Ok(())
}
