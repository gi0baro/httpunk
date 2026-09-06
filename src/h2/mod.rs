//! HTTP/2 PyO3 classes exposed to Python. Rust struct names are short/internal
//! (e.g. `Headers`); the Python names are `H2`-prefixed (e.g. `H2FrameHeaders`)
//! via each pyclass's `name = "..."`.

mod codec;
mod conn;
mod errors;
mod reason;
mod settings;
mod streams;

use pyo3::prelude::*;
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

    m.add_class::<conn::H2Streams>()?;
    m.add_class::<conn::Stopped>()?;
    m.add_class::<conn::RecvHeadersVerdict>()?;
    m.add_class::<conn::RecvDataVerdict>()?;
    m.add_class::<conn::SendVerdict>()?;
    m.add_class::<conn::ResetVerdict>()?;
    // Verdict flags + HEADERS verdict kinds (see conn.rs).
    m.add("H2_FLAG_WAKE", conn::FLAG_WAKE)?;
    m.add("H2_FLAG_SLOT_FREED", conn::FLAG_SLOT_FREED)?;
    m.add("H2_FLAG_CONN_DONE", conn::FLAG_CONN_DONE)?;
    m.add("H2_FLAG_STOP_ACCEPTING", conn::FLAG_STOP_ACCEPTING)?;
    m.add("H2_HEADERS_IGNORED", conn::HEADERS_IGNORED)?;
    m.add("H2_HEADERS_OPENED", conn::HEADERS_OPENED)?;
    m.add("H2_HEADERS_HEAD", conn::HEADERS_HEAD)?;
    m.add("H2_HEADERS_TRAILERS", conn::HEADERS_TRAILERS)?;
    errors::register(m)?;
    reason::H2Reason::check_table();
    m.add_class::<reason::H2Reason>()?;
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
