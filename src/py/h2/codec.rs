//! PyO3 surface for the HTTP/2 frame/HPACK layer: `H2Codec` (a synchronous,
//! zero-I/O frame reader/serializer over the vendored `vendor_h2::{frame, hpack}`)
//! and the `Frame` event classes it produces.
//!
//! `H2Codec` is `frozen` with a `std::sync::Mutex` guarding its mutable state
//! (HPACK coder + read buffer), so it is `Sync` and safe to share across the
//! runtime's worker threads without PyO3's runtime borrow-checking. (Locks are
//! `.unwrap()`ed: the release profile is `panic = "abort"`, so a poisoned lock
//! can never be observed; in debug a poisoned lock surfaces as a clean panic.)

use bytes::{Buf, BufMut, Bytes, BytesMut};
use http::{Method, StatusCode, Uri};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::sync::Mutex;

use super::streams::H2ProtocolError;
use crate::py::http::HeaderMap;
use vendor_h2::frame::{self, HEADER_LEN, Head, Kind};
use vendor_h2::hpack;

/// Default HPACK dynamic table size (SETTINGS_HEADER_TABLE_SIZE, RFC 7540 §6.5.2).
const DEFAULT_HEADER_TABLE_SIZE: usize = 4096;
/// Generous cap on the decoded header list size before we bail (abuse guard).
const DEFAULT_MAX_HEADER_LIST_SIZE: usize = 16 << 20;
/// Default HTTP/2 frame size cap (SETTINGS_MAX_FRAME_SIZE, RFC 7540 §6.5.2).
const DEFAULT_MAX_FRAME_SIZE: usize = 16384;
/// Largest permitted SETTINGS_MAX_FRAME_SIZE (2^24 - 1, RFC 7540 §6.5.2).
const MAX_MAX_FRAME_SIZE: u32 = (1 << 24) - 1;
/// Largest permitted SETTINGS_INITIAL_WINDOW_SIZE (2^31 - 1, RFC 7540 §6.5.2).
const MAX_WINDOW_SIZE: u32 = (1 << 31) - 1;

const FLAG_END_STREAM: u8 = 0x1;

fn value_err<E: std::fmt::Display>(what: &str, e: E) -> PyErr {
    PyValueError::new_err(format!("{what}: {e}"))
}

fn encode_headers_frame(
    encoder: &mut hpack::Encoder,
    hframe: frame::Headers,
    max_frame_size: usize,
) -> BytesMut {
    // HEADERS, then as many CONTINUATION frames as the block needs: each
    // `encode` writes one frame and returns the remaining block, if any (h2
    // frame/headers.rs `Headers`/`Continuation::encode`). The per-frame budget is
    // the peer's negotiated SETTINGS_MAX_FRAME_SIZE **plus** the 9-byte frame
    // header (h2 framed_write.rs: `max_frame_size + HEADER_LEN`), so a full
    // `max_frame_size` payload fits.
    let limit = HEADER_LEN + max_frame_size;
    let mut dst = BytesMut::new();
    let mut cont = {
        let mut limited = (&mut dst).limit(limit);
        hframe.encode(encoder, &mut limited)
    };
    while let Some(c) = cont {
        let mut limited = (&mut dst).limit(limit);
        cont = c.encode(&mut limited);
    }
    dst
}

/// A connection-level protocol error: the driver sends GOAWAY(reason) and tears
/// the connection down. `args = (reason: int, message)`.
fn protocol_err(reason: frame::Reason, msg: &str) -> PyErr {
    H2ProtocolError::new_err((Some(u32::from(reason)), msg.to_string()))
}

/// A frame that fails to load below the header layer (SETTINGS / PING /
/// WINDOW_UPDATE / DATA / RESET / GO_AWAY / PRIORITY) is a **connection-level**
/// PROTOCOL_ERROR in h2 — the reason is chosen by frame *kind* (always
/// PROTOCOL_ERROR from `decode_frame`), never by the `frame::Error` variant, so
/// e.g. a bad-length PING/WINDOW_UPDATE is PROTOCOL_ERROR, not FRAME_SIZE_ERROR
/// (framed_read.rs `decode_frame` maps every loader error to `library_go_away`).
fn load_err(e: frame::Error) -> PyErr {
    protocol_err(
        frame::Reason::PROTOCOL_ERROR,
        &format!("failed to load frame: {e:?}"),
    )
}

/// Build a stream-level error *event* (h2 `Error::library_reset(id, reason)`).
/// Emitted inline in the frame stream (not raised) so the driver RSTs just that
/// stream and keeps decoding the rest of the batch — matching h2, where a
/// stream error for one frame does not discard frames already yielded.
fn stream_err_event(
    py: Python<'_>,
    stream_id: frame::StreamId,
    reason: frame::Reason,
) -> PyResult<Py<PyAny>> {
    Ok(Py::new(
        py,
        StreamErrorFrame {
            stream_id: u32::from(stream_id),
            error_code: u32::from(reason),
        },
    )?
    .into_any())
}

/// Outcome of an HPACK `load_hpack` call, classified exactly as h2's
/// `decode_frame` header_block macro does (framed_read.rs).
enum HpackOutcome {
    Done,
    NeedMore,
    StreamReset,
}

/// Classify an HPACK decode result (h2 framed_read.rs header_block match):
/// `NeedMore` before END_HEADERS = keep buffering; `MalformedMessage` = a
/// **stream** error (RST_STREAM, connection survives); `HeaderListWayTooLarge` =
/// connection ENHANCE_YOUR_CALM; any other HPACK error (incl. `NeedMore` *at*
/// END_HEADERS) = connection PROTOCOL_ERROR.
fn classify_hpack(
    res: Result<(), frame::Error>,
    is_end_headers: bool,
) -> Result<HpackOutcome, PyErr> {
    match res {
        Ok(()) => Ok(HpackOutcome::Done),
        Err(frame::Error::Hpack(hpack::DecoderError::NeedMore(_))) if !is_end_headers => {
            Ok(HpackOutcome::NeedMore)
        }
        Err(frame::Error::MalformedMessage) => Ok(HpackOutcome::StreamReset),
        Err(frame::Error::HeaderListWayTooLarge) => Err(protocol_err(
            frame::Reason::ENHANCE_YOUR_CALM,
            "decoded header list size over abuse limit",
        )),
        Err(e) => Err(protocol_err(
            frame::Reason::PROTOCOL_ERROR,
            &format!("HPACK decoding failed: {e:?}"),
        )),
    }
}

/// Upper bound on CONTINUATION frames per header block (h2 codec heuristic) —
/// this is the CONTINUATION-flood DoS guard.
fn calc_max_continuation_frames(header_max: usize, frame_max: usize) -> usize {
    let min_frames_for_list = (header_max / frame_max).max(1);
    let padding = min_frames_for_list >> 2; // ~25%
    min_frames_for_list.saturating_add(padding).max(5)
}

/// A HEADERS frame whose header block is still being assembled from
/// CONTINUATION frames (h2 codec's `Partial`).
struct Partial {
    frame: frame::Headers,
    buf: BytesMut,
    count: usize, // CONTINUATION frames seen (flood guard)
}

/// Build a Python `H2FrameHeaders` event from a fully-decoded HEADERS frame.
fn headers_event(py: Python<'_>, h: frame::Headers) -> PyResult<Py<PyAny>> {
    let stream_id = u32::from(h.stream_id());
    let end_stream = h.is_end_stream();
    let (pseudo, fields) = h.into_parts();
    // The decoded frame already owns an `http::HeaderMap`; wrap it directly.
    let headers = Py::new(py, HeaderMap::from_inner(fields))?;
    Ok(Py::new(
        py,
        Headers {
            stream_id,
            end_stream,
            end_headers: true,
            method: pseudo.method.map(|m| m.to_string()),
            scheme: pseudo.scheme.map(|s| s.as_str().to_string()),
            authority: pseudo.authority.map(|s| s.as_str().to_string()),
            path: pseudo.path.map(|s| s.as_str().to_string()),
            status: pseudo.status.map(|s| s.as_u16()),
            headers,
        },
    )?
    .into_any())
}

// ===== Frame event classes ==============================================

#[pyclass(module = "httpunk._httpunk", name = "H2FrameHeaders", frozen)]
pub struct Headers {
    #[pyo3(get)]
    pub stream_id: u32,
    #[pyo3(get)]
    pub end_stream: bool,
    #[pyo3(get)]
    pub end_headers: bool,
    #[pyo3(get)]
    pub method: Option<String>,
    #[pyo3(get)]
    pub scheme: Option<String>,
    #[pyo3(get)]
    pub authority: Option<String>,
    #[pyo3(get)]
    pub path: Option<String>,
    #[pyo3(get)]
    pub status: Option<u16>,
    /// Regular header fields as a `httpunk.http.HeaderMap`.
    #[pyo3(get)]
    pub headers: Py<HeaderMap>,
}

#[pymethods]
impl Headers {
    fn __repr__(&self) -> String {
        format!(
            "Headers(stream_id={}, status={:?}, end_stream={}, fields={})",
            self.stream_id,
            self.status,
            self.end_stream,
            self.headers.get().len(),
        )
    }
}

#[pyclass(module = "httpunk._httpunk", name = "H2FrameData", frozen)]
pub struct Data {
    #[pyo3(get)]
    pub stream_id: u32,
    #[pyo3(get)]
    pub end_stream: bool,
    #[pyo3(get)]
    pub data: Py<PyBytes>,
    /// h2 `frame::Data::flow_controlled_len` — payload + padding + the pad-length
    /// byte. Flow control counts padding; `data` (the payload) does not, so the
    /// driver must account windows on this, not `len(data)` (h2 recv.rs L643).
    #[pyo3(get)]
    pub flow_controlled_len: usize,
}

#[pymethods]
impl Data {
    fn __repr__(&self, py: Python<'_>) -> String {
        format!(
            "Data(stream_id={}, end_stream={}, len={})",
            self.stream_id,
            self.end_stream,
            self.data.bind(py).len().unwrap_or(0),
        )
    }
}

#[pyclass(module = "httpunk._httpunk", name = "H2FrameSettings", frozen)]
pub struct Settings {
    #[pyo3(get)]
    pub ack: bool,
    #[pyo3(get)]
    pub header_table_size: Option<u32>,
    #[pyo3(get)]
    pub enable_push: Option<bool>,
    #[pyo3(get)]
    pub max_concurrent_streams: Option<u32>,
    #[pyo3(get)]
    pub initial_window_size: Option<u32>,
    #[pyo3(get)]
    pub max_frame_size: Option<u32>,
    #[pyo3(get)]
    pub max_header_list_size: Option<u32>,
}

#[pymethods]
impl Settings {
    fn __repr__(&self) -> String {
        format!(
            "Settings(ack={}, max_concurrent_streams={:?}, initial_window_size={:?}, max_frame_size={:?})",
            self.ack, self.max_concurrent_streams, self.initial_window_size, self.max_frame_size,
        )
    }
}

#[pyclass(module = "httpunk._httpunk", name = "H2FrameWindowUpdate", frozen)]
pub struct WindowUpdate {
    #[pyo3(get)]
    pub stream_id: u32,
    #[pyo3(get)]
    pub increment: u32,
}

#[pyclass(module = "httpunk._httpunk", name = "H2FramePing", frozen)]
pub struct Ping {
    #[pyo3(get)]
    pub ack: bool,
    #[pyo3(get)]
    pub data: Py<PyBytes>,
}

#[pyclass(module = "httpunk._httpunk", name = "H2FrameGoAway", frozen)]
pub struct GoAway {
    #[pyo3(get)]
    pub last_stream_id: u32,
    #[pyo3(get)]
    pub error_code: u32,
    #[pyo3(get)]
    pub debug_data: Py<PyBytes>,
}

#[pyclass(module = "httpunk._httpunk", name = "H2FrameRstStream", frozen)]
pub struct RstStream {
    #[pyo3(get)]
    pub stream_id: u32,
    #[pyo3(get)]
    pub error_code: u32,
}

#[pyclass(module = "httpunk._httpunk", name = "H2FramePriority", frozen)]
pub struct Priority {
    #[pyo3(get)]
    pub stream_id: u32,
}

/// A stream-level protocol error detected while decoding (h2
/// `Error::library_reset`): the driver RSTs `stream_id` with `error_code` and
/// keeps the connection alive. Surfaced as an event (not raised) so frames
/// already decoded in the same `receive()` batch are not discarded.
#[pyclass(module = "httpunk._httpunk", name = "H2FrameStreamError", frozen)]
pub struct StreamErrorFrame {
    #[pyo3(get)]
    pub stream_id: u32,
    #[pyo3(get)]
    pub error_code: u32,
}

// ===== The codec ========================================================

/// Mutable codec state, guarded by the `H2Codec` mutex.
struct Codec {
    decoder: hpack::Decoder,
    encoder: hpack::Encoder,
    buf: BytesMut,
    max_header_list_size: usize,
    max_continuation_frames: usize,
    /// Largest frame payload we accept on receive (our advertised
    /// SETTINGS_MAX_FRAME_SIZE; updated when our own SETTINGS is ACKed). h2
    /// rejects an over-size declared length with GOAWAY(FRAME_SIZE_ERROR) before
    /// buffering the payload (framed_read.rs / LengthDelimitedCodec).
    max_recv_frame_size: usize,
    /// Peer's advertised SETTINGS_MAX_FRAME_SIZE — the per-frame budget when we
    /// serialize (HEADERS/CONTINUATION splitting, DATA size check).
    send_max_frame_size: usize,
    partial: Option<Partial>, // a HEADERS block awaiting CONTINUATION frames
}

#[pyclass(module = "httpunk._httpunk", name = "H2Codec", frozen)]
pub struct H2Codec {
    inner: Mutex<Codec>,
    #[pyo3(get)]
    role_client: bool,
}

#[pymethods]
impl H2Codec {
    #[new]
    #[pyo3(signature = (role = "client"))]
    fn new(role: &str) -> PyResult<Self> {
        let role_client = match role {
            "client" => true,
            "server" => false,
            other => {
                return Err(PyValueError::new_err(format!(
                    "role must be 'client' or 'server', got {other:?}"
                )));
            }
        };
        Ok(Self {
            inner: Mutex::new(Codec {
                decoder: hpack::Decoder::new(DEFAULT_HEADER_TABLE_SIZE),
                encoder: hpack::Encoder::default(),
                buf: BytesMut::new(),
                max_header_list_size: DEFAULT_MAX_HEADER_LIST_SIZE,
                max_continuation_frames: calc_max_continuation_frames(
                    DEFAULT_MAX_HEADER_LIST_SIZE,
                    DEFAULT_MAX_FRAME_SIZE,
                ),
                max_recv_frame_size: DEFAULT_MAX_FRAME_SIZE,
                send_max_frame_size: DEFAULT_MAX_FRAME_SIZE,
                partial: None,
            }),
            role_client,
        })
    }

    /// Feed inbound bytes; return the list of complete frames now decodable.
    fn receive(&self, py: Python<'_>, data: &[u8]) -> PyResult<Vec<Py<PyAny>>> {
        let mut c = self.inner.lock().unwrap();
        c.buf.extend_from_slice(data);
        let mut out: Vec<Py<PyAny>> = Vec::new();

        loop {
            if c.buf.len() < HEADER_LEN {
                break;
            }
            let payload_len = (usize::from(c.buf[0]) << 16)
                | (usize::from(c.buf[1]) << 8)
                | usize::from(c.buf[2]);
            // Enforce SETTINGS_MAX_FRAME_SIZE on the *declared* length before
            // buffering the payload (h2: LengthDelimitedCodec `max_frame_length`
            // -> GOAWAY(FRAME_SIZE_ERROR); framed_read.rs:425). This both rejects
            // frames h2 rejects and prevents buffering an over-size payload.
            if payload_len > c.max_recv_frame_size {
                return Err(protocol_err(
                    frame::Reason::FRAME_SIZE_ERROR,
                    "frame length exceeds SETTINGS_MAX_FRAME_SIZE",
                ));
            }
            let total = HEADER_LEN + payload_len;
            if c.buf.len() < total {
                break;
            }
            let frame_buf = c.buf.split_to(total);
            if let Some(obj) = c.decode_one(py, frame_buf)? {
                out.push(obj);
            }
        }
        Ok(out)
    }

    /// Number of bytes currently buffered awaiting a complete frame.
    fn buffered(&self) -> usize {
        self.inner.lock().unwrap().buf.len()
    }

    // ===== settings application (HPACK table sizes) =================

    /// Apply the peer's SETTINGS_HEADER_TABLE_SIZE: bounds the table *our*
    /// encoder may use (h2 codec `set_send_header_table_size`).
    fn set_send_header_table_size(&self, val: u32) {
        self.inner
            .lock()
            .unwrap()
            .encoder
            .update_max_size(val as usize);
    }

    /// Apply our own SETTINGS_HEADER_TABLE_SIZE (on peer ACK): queues a table
    /// size update for *our* decoder (h2 codec `set_recv_header_table_size`).
    fn set_recv_header_table_size(&self, val: u32) {
        self.inner
            .lock()
            .unwrap()
            .decoder
            .queue_size_update(val as usize);
    }

    /// Apply our own SETTINGS_MAX_FRAME_SIZE (on peer ACK): the largest frame
    /// payload we now accept on receive. Also recomputes the CONTINUATION-flood
    /// cap, which is derived from the frame size (h2 framed_read.rs
    /// `set_max_frame_size` -> `calc_max_continuation_frames`).
    fn set_max_recv_frame_size(&self, val: u32) {
        let mut c = self.inner.lock().unwrap();
        c.max_recv_frame_size = val as usize;
        c.max_continuation_frames =
            calc_max_continuation_frames(c.max_header_list_size, c.max_recv_frame_size);
    }

    /// Apply our own SETTINGS_MAX_HEADER_LIST_SIZE (on peer ACK): the decoded
    /// header-list size bound, which also feeds the CONTINUATION-flood cap (h2
    /// framed_read.rs `set_max_header_list_size`).
    fn set_max_header_list_size(&self, val: u32) {
        let mut c = self.inner.lock().unwrap();
        c.max_header_list_size = val as usize;
        c.max_continuation_frames =
            calc_max_continuation_frames(c.max_header_list_size, c.max_recv_frame_size);
    }

    /// Apply the peer's SETTINGS_MAX_FRAME_SIZE: the per-frame payload budget for
    /// what *we* serialize (h2 framed_write.rs `set_max_frame_size`).
    fn set_send_max_frame_size(&self, val: u32) {
        self.inner.lock().unwrap().send_max_frame_size = val as usize;
    }

    // ===== serialize (outbound) =====================================

    #[pyo3(signature = (*, header_table_size=None, enable_push=None, max_concurrent_streams=None,
                        initial_window_size=None, max_frame_size=None, max_header_list_size=None))]
    #[allow(clippy::too_many_arguments)]
    fn serialize_settings(
        &self,
        py: Python<'_>,
        header_table_size: Option<u32>,
        enable_push: Option<bool>,
        max_concurrent_streams: Option<u32>,
        initial_window_size: Option<u32>,
        max_frame_size: Option<u32>,
        max_header_list_size: Option<u32>,
    ) -> PyResult<Py<PyBytes>> {
        // Pre-validate the ranges the vendored `frame::Settings` setters assert
        // on, so a bad value surfaces as a Python error instead of aborting the
        // process (release builds are `panic = "abort"`). RFC 7540 §6.5.2.
        if let Some(v) = max_frame_size
            && !(DEFAULT_MAX_FRAME_SIZE as u32..=MAX_MAX_FRAME_SIZE).contains(&v)
        {
            return Err(PyValueError::new_err(format!(
                "max_frame_size must be in [{DEFAULT_MAX_FRAME_SIZE}, {MAX_MAX_FRAME_SIZE}], got {v}"
            )));
        }
        if let Some(v) = initial_window_size
            && v > MAX_WINDOW_SIZE
        {
            return Err(PyValueError::new_err(format!(
                "initial_window_size must be <= {MAX_WINDOW_SIZE}, got {v}"
            )));
        }
        let mut s = frame::Settings::default();
        if let Some(v) = header_table_size {
            s.set_header_table_size(Some(v));
        }
        if let Some(v) = enable_push {
            s.set_enable_push(v);
        }
        if let Some(v) = max_concurrent_streams {
            s.set_max_concurrent_streams(Some(v));
        }
        if let Some(v) = initial_window_size {
            s.set_initial_window_size(Some(v));
        }
        if let Some(v) = max_frame_size {
            s.set_max_frame_size(Some(v));
        }
        if let Some(v) = max_header_list_size {
            s.set_max_header_list_size(Some(v));
        }
        let mut dst = BytesMut::new();
        s.encode(&mut dst);
        Ok(PyBytes::new(py, &dst).unbind())
    }

    fn serialize_settings_ack(&self, py: Python<'_>) -> Py<PyBytes> {
        let mut dst = BytesMut::new();
        frame::Settings::ack().encode(&mut dst);
        PyBytes::new(py, &dst).unbind()
    }

    #[pyo3(signature = (stream_id, method, url, headers=None, end_stream=false))]
    fn serialize_request_headers(
        &self,
        py: Python<'_>,
        stream_id: u32,
        method: &str,
        url: &str,
        headers: Option<&HeaderMap>,
        end_stream: bool,
    ) -> PyResult<Py<PyBytes>> {
        let method =
            Method::from_bytes(method.as_bytes()).map_err(|e| value_err("invalid method", e))?;
        let uri: Uri = url.parse().map_err(|e| value_err("invalid url", e))?;
        let pseudo = frame::Pseudo::request(method, uri, None);
        let fields = headers.map(HeaderMap::snapshot).unwrap_or_default();
        let mut hframe = frame::Headers::new(frame::StreamId::from(stream_id), pseudo, fields);
        // A bodyless request carries END_STREAM on HEADERS (h2 `send_request`
        // with `end_of_stream`), rather than a trailing empty DATA frame.
        if end_stream {
            hframe.set_end_stream();
        }
        let mut c = self.inner.lock().unwrap();
        let max = c.send_max_frame_size;
        let dst = encode_headers_frame(&mut c.encoder, hframe, max);
        Ok(PyBytes::new(py, &dst).unbind())
    }

    #[pyo3(signature = (stream_id, status, headers=None, end_stream=false))]
    fn serialize_response_headers(
        &self,
        py: Python<'_>,
        stream_id: u32,
        status: u16,
        headers: Option<&HeaderMap>,
        end_stream: bool,
    ) -> PyResult<Py<PyBytes>> {
        let status = StatusCode::from_u16(status).map_err(|e| value_err("invalid status", e))?;
        let pseudo = frame::Pseudo::response(status);
        let fields = headers.map(HeaderMap::snapshot).unwrap_or_default();
        let mut hframe = frame::Headers::new(frame::StreamId::from(stream_id), pseudo, fields);
        // A bodyless response carries END_STREAM on HEADERS (e.g. a HEAD response,
        // 204/304), rather than a trailing empty DATA frame.
        if end_stream {
            hframe.set_end_stream();
        }
        let mut c = self.inner.lock().unwrap();
        let max = c.send_max_frame_size;
        let dst = encode_headers_frame(&mut c.encoder, hframe, max);
        Ok(PyBytes::new(py, &dst).unbind())
    }

    /// A trailing HEADERS frame (no pseudo-headers, END_STREAM set) — request/response
    /// trailers sent after the DATA frames (h2 `frame::Headers::trailers`) (F45).
    fn serialize_trailers(
        &self,
        py: Python<'_>,
        stream_id: u32,
        trailers: &HeaderMap,
    ) -> PyResult<Py<PyBytes>> {
        let fields = trailers.snapshot();
        let hframe = frame::Headers::trailers(frame::StreamId::from(stream_id), fields);
        let mut c = self.inner.lock().unwrap();
        let max = c.send_max_frame_size;
        let dst = encode_headers_frame(&mut c.encoder, hframe, max);
        Ok(PyBytes::new(py, &dst).unbind())
    }

    #[pyo3(signature = (stream_id, data, end_stream=false))]
    fn serialize_data(
        &self,
        py: Python<'_>,
        stream_id: u32,
        data: &[u8],
        end_stream: bool,
    ) -> PyResult<Py<PyBytes>> {
        // A DATA payload may not exceed the peer's SETTINGS_MAX_FRAME_SIZE (h2:
        // `Encoder::buffer` -> `UserError::PayloadTooBig`, framed_write.rs). The
        // 3-byte length field would also overflow past 2^24-1.
        let max = self.inner.lock().unwrap().send_max_frame_size;
        if data.len() > max {
            return Err(super::streams::user_payload_too_big(data.len(), max));
        }
        let flags = if end_stream { FLAG_END_STREAM } else { 0 };
        let head = Head::new(Kind::Data, flags, frame::StreamId::from(stream_id));
        let mut dst = BytesMut::with_capacity(HEADER_LEN + data.len());
        head.encode(data.len(), &mut dst);
        dst.extend_from_slice(data);
        Ok(PyBytes::new(py, &dst).unbind())
    }

    fn serialize_window_update(
        &self,
        py: Python<'_>,
        stream_id: u32,
        increment: u32,
    ) -> Py<PyBytes> {
        let mut dst = BytesMut::new();
        frame::WindowUpdate::new(frame::StreamId::from(stream_id), increment).encode(&mut dst);
        PyBytes::new(py, &dst).unbind()
    }

    fn serialize_ping_ack(&self, py: Python<'_>, payload: &[u8]) -> PyResult<Py<PyBytes>> {
        if payload.len() != 8 {
            return Err(PyValueError::new_err(
                "ping payload must be exactly 8 bytes",
            ));
        }
        let mut p = [0u8; 8];
        p.copy_from_slice(payload);
        let mut dst = BytesMut::new();
        frame::Ping::pong(p).encode(&mut dst);
        Ok(PyBytes::new(py, &dst).unbind())
    }

    /// A PING frame (not ACK) carrying an 8-byte payload — for keep-alive.
    fn serialize_ping(&self, py: Python<'_>, payload: &[u8]) -> PyResult<Py<PyBytes>> {
        if payload.len() != 8 {
            return Err(PyValueError::new_err(
                "ping payload must be exactly 8 bytes",
            ));
        }
        let mut p = [0u8; 8];
        p.copy_from_slice(payload);
        let mut dst = BytesMut::new();
        frame::Ping::new(p).encode(&mut dst);
        Ok(PyBytes::new(py, &dst).unbind())
    }

    /// A GOAWAY frame (connection shutdown), `stream_id = 0`.
    #[pyo3(signature = (last_stream_id, error_code, debug_data = None))]
    fn serialize_go_away(
        &self,
        py: Python<'_>,
        last_stream_id: u32,
        error_code: u32,
        debug_data: Option<&[u8]>,
    ) -> Py<PyBytes> {
        let sid = frame::StreamId::from(last_stream_id);
        let reason = frame::Reason::from(error_code);
        let ga = match debug_data {
            Some(dd) if !dd.is_empty() => {
                frame::GoAway::with_debug_data(sid, reason, Bytes::copy_from_slice(dd))
            }
            _ => frame::GoAway::new(sid, reason),
        };
        let mut dst = BytesMut::new();
        ga.encode(&mut dst);
        PyBytes::new(py, &dst).unbind()
    }

    /// A RST_STREAM frame — abruptly terminate `stream_id`.
    fn serialize_rst_stream(&self, py: Python<'_>, stream_id: u32, error_code: u32) -> Py<PyBytes> {
        let mut dst = BytesMut::new();
        frame::Reset::new(
            frame::StreamId::from(stream_id),
            frame::Reason::from(error_code),
        )
        .encode(&mut dst);
        PyBytes::new(py, &dst).unbind()
    }
}

impl Codec {
    fn decode_one(
        &mut self,
        py: Python<'_>,
        mut frame_buf: BytesMut,
    ) -> PyResult<Option<Py<PyAny>>> {
        let head = Head::parse(&frame_buf[..HEADER_LEN]);

        // While a header block is being assembled, only CONTINUATION may follow.
        if self.partial.is_some() && head.kind() != Kind::Continuation {
            return Err(protocol_err(
                frame::Reason::PROTOCOL_ERROR,
                "expected CONTINUATION frame",
            ));
        }

        let obj = match head.kind() {
            Kind::Headers => {
                // Mirrors h2 framed_read.rs `header_block!(Headers, ...)`.
                frame_buf.advance(HEADER_LEN);
                let (mut h, mut payload) = match frame::Headers::load(head, frame_buf) {
                    Ok(res) => res,
                    // A stream cannot depend on itself: stream error (RFC §5.4.2).
                    Err(frame::Error::InvalidDependencyId) => {
                        return Ok(Some(stream_err_event(
                            py,
                            head.stream_id(),
                            frame::Reason::PROTOCOL_ERROR,
                        )?));
                    }
                    Err(e) => return Err(load_err(e)),
                };
                let is_end_headers = h.is_end_headers();
                // Load HPACK incrementally (h2 decodes even when !END_HEADERS, so
                // the dynamic table stays in sync and over-size is tracked).
                match classify_hpack(
                    h.load_hpack(&mut payload, self.max_header_list_size, &mut self.decoder),
                    is_end_headers,
                )? {
                    HpackOutcome::StreamReset => {
                        return Ok(Some(stream_err_event(
                            py,
                            head.stream_id(),
                            frame::Reason::PROTOCOL_ERROR,
                        )?));
                    }
                    HpackOutcome::Done | HpackOutcome::NeedMore => {}
                }
                if is_end_headers {
                    headers_event(py, h)?
                } else {
                    // Defer until the terminating CONTINUATION (END_HEADERS).
                    self.partial = Some(Partial {
                        frame: h,
                        buf: payload,
                        count: 0,
                    });
                    return Ok(None);
                }
            }
            Kind::Continuation => {
                // Mirrors h2 framed_read.rs `Kind::Continuation`.
                let is_end_headers = (head.flag() & 0x4) == 0x4;
                let mut partial = self.partial.take().ok_or_else(|| {
                    protocol_err(
                        frame::Reason::PROTOCOL_ERROR,
                        "received unexpected CONTINUATION frame",
                    )
                })?;
                if partial.frame.stream_id() != head.stream_id() {
                    return Err(protocol_err(
                        frame::Reason::PROTOCOL_ERROR,
                        "CONTINUATION frame stream ID does not match previous frame stream ID",
                    ));
                }
                // CONTINUATION-flood cap (reset on END_HEADERS).
                if is_end_headers {
                    partial.count = 0;
                } else {
                    let cnt = partial.count + 1;
                    if cnt > self.max_continuation_frames {
                        return Err(protocol_err(
                            frame::Reason::ENHANCE_YOUR_CALM,
                            "too_many_continuations",
                        ));
                    }
                    partial.count = cnt;
                }
                // Extend the pending block. The oversize guard fires only once the
                // *decoded* block is already over-size (h2's `is_over_size`), and
                // then it is COMPRESSION_ERROR (not ENHANCE_YOUR_CALM) — the block
                // is still decoded to keep the HPACK table in sync.
                if partial.buf.is_empty() {
                    partial.buf = frame_buf.split_off(HEADER_LEN);
                } else {
                    if partial.frame.is_over_size()
                        && partial.buf.len() + frame_buf.len() > self.max_header_list_size
                    {
                        return Err(protocol_err(
                            frame::Reason::COMPRESSION_ERROR,
                            "CONTINUATION frame header block size over ignorable limit",
                        ));
                    }
                    partial.buf.extend_from_slice(&frame_buf[HEADER_LEN..]);
                }
                match classify_hpack(
                    partial.frame.load_hpack(
                        &mut partial.buf,
                        self.max_header_list_size,
                        &mut self.decoder,
                    ),
                    is_end_headers,
                )? {
                    HpackOutcome::StreamReset => {
                        return Ok(Some(stream_err_event(
                            py,
                            head.stream_id(),
                            frame::Reason::PROTOCOL_ERROR,
                        )?));
                    }
                    HpackOutcome::Done | HpackOutcome::NeedMore => {}
                }
                if is_end_headers {
                    headers_event(py, partial.frame)?
                } else {
                    self.partial = Some(partial);
                    return Ok(None);
                }
            }
            Kind::Data => {
                frame_buf.advance(HEADER_LEN);
                let d = frame::Data::load(head, frame_buf.freeze()).map_err(load_err)?;
                Py::new(
                    py,
                    Data {
                        stream_id: u32::from(d.stream_id()),
                        end_stream: d.is_end_stream(),
                        flow_controlled_len: d.flow_controlled_len(),
                        data: PyBytes::new(py, d.payload().as_ref()).unbind(),
                    },
                )?
                .into_any()
            }
            Kind::Settings => {
                let s = frame::Settings::load(head, &frame_buf[HEADER_LEN..]).map_err(load_err)?;
                Py::new(
                    py,
                    Settings {
                        ack: s.is_ack(),
                        header_table_size: s.header_table_size(),
                        enable_push: s.is_push_enabled(),
                        max_concurrent_streams: s.max_concurrent_streams(),
                        initial_window_size: s.initial_window_size(),
                        max_frame_size: s.max_frame_size(),
                        max_header_list_size: s.max_header_list_size(),
                    },
                )?
                .into_any()
            }
            Kind::WindowUpdate => {
                let w =
                    frame::WindowUpdate::load(head, &frame_buf[HEADER_LEN..]).map_err(load_err)?;
                Py::new(
                    py,
                    WindowUpdate {
                        stream_id: u32::from(w.stream_id()),
                        increment: w.size_increment(),
                    },
                )?
                .into_any()
            }
            Kind::Ping => {
                let p = frame::Ping::load(head, &frame_buf[HEADER_LEN..]).map_err(load_err)?;
                Py::new(
                    py,
                    Ping {
                        ack: p.is_ack(),
                        data: PyBytes::new(py, &p.payload()[..]).unbind(),
                    },
                )?
                .into_any()
            }
            Kind::GoAway => {
                let g = frame::GoAway::load(&frame_buf[HEADER_LEN..]).map_err(load_err)?;
                Py::new(
                    py,
                    GoAway {
                        last_stream_id: u32::from(g.last_stream_id()),
                        error_code: u32::from(g.reason()),
                        debug_data: PyBytes::new(py, g.debug_data().as_ref()).unbind(),
                    },
                )?
                .into_any()
            }
            Kind::Reset => {
                let r = frame::Reset::load(head, &frame_buf[HEADER_LEN..]).map_err(load_err)?;
                Py::new(
                    py,
                    RstStream {
                        stream_id: u32::from(r.stream_id()),
                        error_code: u32::from(r.reason()),
                    },
                )?
                .into_any()
            }
            Kind::Priority => {
                // h2 framed_read.rs `Kind::Priority`.
                if u32::from(head.stream_id()) == 0 {
                    return Err(protocol_err(
                        frame::Reason::PROTOCOL_ERROR,
                        "PRIORITY frame with stream ID 0",
                    ));
                }
                match frame::Priority::load(head, &frame_buf[HEADER_LEN..]) {
                    Ok(_) => Py::new(
                        py,
                        Priority {
                            stream_id: u32::from(head.stream_id()),
                        },
                    )?
                    .into_any(),
                    // A stream cannot depend on itself: stream error (RFC §5.4.2).
                    Err(frame::Error::InvalidDependencyId) => {
                        return Ok(Some(stream_err_event(
                            py,
                            head.stream_id(),
                            frame::Reason::PROTOCOL_ERROR,
                        )?));
                    }
                    Err(e) => return Err(load_err(e)),
                }
            }
            Kind::PushPromise => {
                // We advertise SETTINGS_ENABLE_PUSH = 0, so any PUSH_PROMISE is a
                // connection error (h2 recv.rs `ensure_can_reserve` -> PROTOCOL_
                // ERROR GOAWAY). h2 first HPACK-decodes the block to keep the
                // dynamic table synced, but since we tear the connection down that
                // decode has no observable effect, so we reject immediately.
                return Err(protocol_err(
                    frame::Reason::PROTOCOL_ERROR,
                    "PUSH_PROMISE received but server push is disabled",
                ));
            }
            // Unknown/extension frame types are silently ignored (h2
            // framed_read.rs `Kind::Unknown => Ok(None)`, RFC 7540 §4.1).
            Kind::Unknown => return Ok(None),
        };
        Ok(Some(obj))
    }
}
