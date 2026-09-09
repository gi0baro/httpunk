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
use http::header::{CONTENT_LENGTH, DATE};
use http::{Method, StatusCode, Uri};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};
use std::sync::Mutex;

use super::errors::{H2ProtocolError, map_user_err, user_payload_too_big};
use crate::http::HeaderMap;
use vendor_h2::codec::UserError;
use vendor_h2::frame::{self, HEADER_LEN, Head, Kind};
use vendor_h2::hpack;
use vendor_hyper::{H2_PREFACE, date_header_value};

use crate::py::{method_str, scheme_str};

/// Default HPACK dynamic table size (SETTINGS_HEADER_TABLE_SIZE, RFC 7540 §6.5.2).
const DEFAULT_HEADER_TABLE_SIZE: usize = 4096;
/// Generous cap on the decoded header list size before we bail (abuse guard).
pub(super) const DEFAULT_MAX_HEADER_LIST_SIZE: usize = 16 << 20;
/// Default HTTP/2 frame size cap (SETTINGS_MAX_FRAME_SIZE, RFC 7540 §6.5.2).
pub(super) const DEFAULT_MAX_FRAME_SIZE: usize = 16384;
/// Largest permitted SETTINGS_MAX_FRAME_SIZE (2^24 - 1, RFC 7540 §6.5.2).
pub(super) const MAX_MAX_FRAME_SIZE: u32 = (1 << 24) - 1;
/// Largest permitted SETTINGS_INITIAL_WINDOW_SIZE (2^31 - 1, RFC 7540 §6.5.2).
pub(super) const MAX_WINDOW_SIZE: u32 = (1 << 31) - 1;

const FLAG_END_STREAM: u8 = 0x1;

fn value_err<E: std::fmt::Display>(what: &str, e: E) -> PyErr {
    PyValueError::new_err(format!("{what}: {e}"))
}

fn encode_headers_frame(
    encoder: &mut hpack::Encoder,
    hframe: frame::Headers,
    max_frame_size: usize,
    dst: &mut BytesMut,
) {
    // HEADERS, then as many CONTINUATION frames as the block needs: each
    // `encode` writes one frame and returns the remaining block, if any (h2
    // frame/headers.rs `Headers`/`Continuation::encode`). The per-frame budget is
    // the peer's negotiated SETTINGS_MAX_FRAME_SIZE **plus** the 9-byte frame
    // header (h2 framed_write.rs: `max_frame_size + HEADER_LEN`), so a full
    // `max_frame_size` payload fits.
    let limit = HEADER_LEN + max_frame_size;
    let mut cont = {
        let mut limited = (&mut *dst).limit(limit);
        hframe.encode(encoder, &mut limited)
    };
    while let Some(c) = cont {
        let mut limited = (&mut *dst).limit(limit);
        cont = c.encode(&mut limited);
    }
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
/// The RFC 9113 §6.5.2 range for SETTINGS_MAX_FRAME_SIZE (h2 frame/settings.rs
/// asserts it): every entry point that stores one validates here, so an
/// out-of-range value is a `ValueError`, never a zero divisor or a truncated
/// 3-byte length field.
pub(super) fn check_max_frame_size(val: u32) -> PyResult<()> {
    if !(DEFAULT_MAX_FRAME_SIZE as u32..=MAX_MAX_FRAME_SIZE).contains(&val) {
        return Err(PyValueError::new_err(format!(
            "max_frame_size must be in [{DEFAULT_MAX_FRAME_SIZE}, {MAX_MAX_FRAME_SIZE}], got {val}"
        )));
    }
    Ok(())
}

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
    let is_informational = h.is_informational();
    // h2 recv.rs `recv_headers` (L157-175): the FIRST `content-length` value, via
    // `frame::parse_u64` (ASCII digits only, > 19 digits rejected outright); a value
    // that does not parse is a stream PROTOCOL_ERROR — flagged for the driver.
    let (content_length, content_length_invalid) = match h.fields().get(CONTENT_LENGTH) {
        None => (None, false),
        Some(v) => match frame::parse_u64(v.as_bytes()) {
            Ok(n) => (Some(n), false),
            Err(_) => (None, true),
        },
    };
    let (pseudo, fields) = h.into_parts();
    // The decoded frame already owns an `http::HeaderMap`; wrap it directly.
    let headers = Py::new(py, HeaderMap::from_inner(fields))?;
    Ok(Py::new(
        py,
        Headers {
            stream_id,
            end_stream,
            end_headers: true,
            // Each pseudo-header becomes ONE `str`, here, handed out by reference on
            // every read (BOUNDARY_NOTES rule 7); method and scheme are the shared
            // interned objects of their closed sets (rule 6).
            method: pseudo.method.as_ref().map(|m| method_str(py, m)),
            scheme: pseudo.scheme.as_ref().map(|s| scheme_str(py, s.as_str())),
            authority: pseudo
                .authority
                .as_ref()
                .map(|s| PyString::new(py, s.as_str()).unbind()),
            path: pseudo
                .path
                .as_ref()
                .map(|s| PyString::new(py, s.as_str()).unbind()),
            status: pseudo.status.map(|s| s.as_u16()),
            headers,
            content_length,
            content_length_invalid,
            is_informational,
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
    pub method: Option<Py<PyString>>,
    #[pyo3(get)]
    pub scheme: Option<Py<PyString>>,
    #[pyo3(get)]
    pub authority: Option<Py<PyString>>,
    #[pyo3(get)]
    pub path: Option<Py<PyString>>,
    #[pyo3(get)]
    pub status: Option<u16>,
    /// Regular header fields as a `httpunk.http.HeaderMap`.
    #[pyo3(get)]
    pub headers: Py<HeaderMap>,
    /// The first `content-length` value, parsed as h2's `recv_headers` does
    /// (`frame::parse_u64`); `None` when absent or unparsable.
    #[pyo3(get)]
    pub content_length: Option<u64>,
    /// A `content-length` was present but did not parse — h2: stream PROTOCOL_ERROR.
    #[pyo3(get)]
    pub content_length_invalid: bool,
    /// A 1xx response head (h2 `frame::Headers::is_informational`).
    #[pyo3(get)]
    pub is_informational: bool,
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
    /// The flow-controlled overhead the app never sees: `flow_controlled_len` minus the
    /// payload (the padding + its length byte, 0 when unpadded) — what h2's `recv_data`
    /// releases back to the windows on the app's behalf (recv.rs L740-750).
    #[pyo3(get)]
    pub padding: usize,
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

/// The inbound half of the codec: the HPACK decoder, the frame read buffer, a
/// HEADERS block awaiting CONTINUATION, the receive limits, and (server role) the
/// client connection preface. `H2Streams` keeps it under ITS OWN mutex, separate
/// from the connection state: a batch's HPACK decode must never make an app task
/// wait for the stream windows, and nothing the decoder does needs the state — the
/// only coupling is our own SETTINGS taking effect on the peer's ACK, applied under
/// both locks in one step (order: state → decoder; nothing takes them in reverse).
pub(super) struct Decoder {
    decoder: hpack::Decoder,
    buf: BytesMut,
    max_header_list_size: usize,
    max_continuation_frames: usize,
    /// Largest frame payload we accept on receive (our advertised
    /// SETTINGS_MAX_FRAME_SIZE; updated when our own SETTINGS is ACKed). h2
    /// rejects an over-size declared length with GOAWAY(FRAME_SIZE_ERROR) before
    /// buffering the payload (framed_read.rs / LengthDelimitedCodec).
    max_recv_frame_size: usize,
    /// Server side: the client connection preface being matched (`feed_preface`),
    /// `None` once matched — h2 server.rs `Handshaking::ReadPreface`.
    preface_buf: Option<BytesMut>,
    /// Server role: the first bytes must be the client preface (consumed by
    /// `receive_frames` itself); `preface_ok` once it matched.
    expect_preface: bool,
    preface_ok: bool,
    partial: Option<Partial>, // a HEADERS block awaiting CONTINUATION frames
}

impl Decoder {
    pub(super) fn new(expect_preface: bool) -> Self {
        Decoder {
            decoder: hpack::Decoder::new(DEFAULT_HEADER_TABLE_SIZE),
            buf: BytesMut::new(),
            max_header_list_size: DEFAULT_MAX_HEADER_LIST_SIZE,
            max_continuation_frames: calc_max_continuation_frames(
                DEFAULT_MAX_HEADER_LIST_SIZE,
                DEFAULT_MAX_FRAME_SIZE,
            ),
            max_recv_frame_size: DEFAULT_MAX_FRAME_SIZE,
            preface_buf: None,
            expect_preface,
            preface_ok: !expect_preface,
            partial: None,
        }
    }

    /// Feed inbound bytes; return the complete frames now decodable. A server-role
    /// decoder consumes the client's 24-byte preface first (h2 server.rs L1427-1441:
    /// a mismatch is a connection PROTOCOL_ERROR).
    pub(super) fn receive_frames(
        &mut self,
        py: Python<'_>,
        data: &[u8],
    ) -> PyResult<Vec<Py<PyAny>>> {
        if self.expect_preface && !self.preface_ok {
            match self.feed_preface(data) {
                None => return Ok(Vec::new()),
                Some(false) => {
                    return Err(protocol_err(
                        frame::Reason::PROTOCOL_ERROR,
                        "bad client connection preface",
                    ));
                }
                Some(true) => self.preface_ok = true, // the bytes past it are in `buf`
            }
        } else {
            self.buf.extend_from_slice(data);
        }
        let mut out: Vec<Py<PyAny>> = Vec::new();
        loop {
            if self.buf.len() < HEADER_LEN {
                break;
            }
            let payload_len = (usize::from(self.buf[0]) << 16)
                | (usize::from(self.buf[1]) << 8)
                | usize::from(self.buf[2]);
            // Enforce SETTINGS_MAX_FRAME_SIZE on the *declared* length before
            // buffering the payload (h2: LengthDelimitedCodec `max_frame_length`
            // -> GOAWAY(FRAME_SIZE_ERROR); framed_read.rs:425). This both rejects
            // frames h2 rejects and prevents buffering an over-size payload.
            if payload_len > self.max_recv_frame_size {
                return Err(protocol_err(
                    frame::Reason::FRAME_SIZE_ERROR,
                    "frame length exceeds SETTINGS_MAX_FRAME_SIZE",
                ));
            }
            let total = HEADER_LEN + payload_len;
            if self.buf.len() < total {
                break;
            }
            let frame_buf = self.buf.split_to(total);
            if let Some(obj) = self.decode_one(py, frame_buf)? {
                out.push(obj);
            }
        }
        Ok(out)
    }

    pub(super) fn buffered(&self) -> usize {
        self.buf.len()
    }

    /// Feed bytes of the client connection preface (RFC 9113 §3.4) — h2 server.rs
    /// L1427-1441: `None` while the bytes so far match but the 24 are not all in yet,
    /// `Some(true)` once the whole preface matched (any bytes past it are already in
    /// the frame buffer), `Some(false)` on a mismatch.
    pub(super) fn feed_preface(&mut self, data: &[u8]) -> Option<bool> {
        let buf = self.preface_buf.get_or_insert_with(BytesMut::new);
        buf.extend_from_slice(data);
        let n = buf.len().min(H2_PREFACE.len());
        if buf[..n] != H2_PREFACE[..n] {
            return Some(false);
        }
        if buf.len() < H2_PREFACE.len() {
            return None;
        }
        let mut full = self.preface_buf.take().unwrap_or_default();
        let rest = full.split_off(H2_PREFACE.len());
        self.buf.extend_from_slice(&rest);
        Some(true)
    }

    /// `util.auto`'s seam (hyper-util's `Rewind`, without the wrapper): the bytes the
    /// protocol sniff already read off the transport — the client preface, for a
    /// server — go straight into the decoder, so the driver reads the raw transport
    /// from the first frame on. Same verdict as `feed_preface` (`Some(true)` once the
    /// preface is complete, then the bytes are frame bytes).
    pub(super) fn prime(&mut self, data: &[u8]) -> Option<bool> {
        if self.expect_preface && !self.preface_ok {
            let verdict = self.feed_preface(data);
            if verdict == Some(true) {
                self.preface_ok = true;
            }
            return verdict;
        }
        self.buf.extend_from_slice(data);
        Some(true)
    }

    /// Apply our own SETTINGS_HEADER_TABLE_SIZE (on peer ACK): queues a table
    /// size update for *our* decoder (h2 codec `set_recv_header_table_size`).
    pub(super) fn set_recv_header_table_size(&mut self, val: u32) {
        self.decoder.queue_size_update(val as usize);
    }

    /// Apply our own SETTINGS_MAX_FRAME_SIZE (on peer ACK): the largest frame
    /// payload we now accept on receive. Also recomputes the CONTINUATION-flood
    /// cap, which is derived from the frame size (h2 framed_read.rs
    /// `set_max_frame_size` -> `calc_max_continuation_frames`).
    pub(super) fn set_max_recv_frame_size(&mut self, val: u32) -> PyResult<()> {
        check_max_frame_size(val)?; // also keeps `calc_max_continuation_frames` off a zero divisor
        self.max_recv_frame_size = val as usize;
        self.max_continuation_frames =
            calc_max_continuation_frames(self.max_header_list_size, self.max_recv_frame_size);
        Ok(())
    }

    /// Apply our own SETTINGS_MAX_HEADER_LIST_SIZE (on peer ACK): the decoded
    /// header-list size bound, which also feeds the CONTINUATION-flood cap (h2
    /// framed_read.rs `set_max_header_list_size`).
    pub(super) fn set_max_header_list_size(&mut self, val: u32) {
        self.max_header_list_size = val as usize;
        self.max_continuation_frames =
            calc_max_continuation_frames(self.max_header_list_size, self.max_recv_frame_size);
    }
}

/// The outbound half of the codec: the HPACK encoder and the peer's frame-size
/// limit. `H2Streams` keeps it INSIDE the connection state: the encoder's dynamic
/// table mutates on encode, so "encode order == wire order" holds only when
/// encoding and appending to the connection's pending buffer are one locked step,
/// and the peer's SETTINGS must fold into it and into the stream windows together.
pub(super) struct Encoder {
    encoder: hpack::Encoder,
    /// Peer's advertised SETTINGS_MAX_FRAME_SIZE — the per-frame budget when we
    /// serialize (HEADERS/CONTINUATION splitting, DATA size check).
    send_max_frame_size: usize,
}

impl Encoder {
    pub(super) fn new() -> Self {
        Encoder {
            encoder: hpack::Encoder::default(),
            send_max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        }
    }

    /// HPACK-encode a HEADERS (or trailers) frame and append it — with as many
    /// CONTINUATION frames as the block needs — to `dst`.
    pub(super) fn encode_headers(&mut self, hframe: frame::Headers, dst: &mut BytesMut) {
        encode_headers_frame(&mut self.encoder, hframe, self.send_max_frame_size, dst);
    }

    /// Frame a DATA payload into `dst`. A payload may not exceed the peer's
    /// SETTINGS_MAX_FRAME_SIZE (h2 `Encoder::buffer` -> `UserError::PayloadTooBig`).
    pub(super) fn encode_data(
        &self,
        stream_id: u32,
        data: &[u8],
        end_stream: bool,
        dst: &mut BytesMut,
    ) -> PyResult<()> {
        if data.len() > self.send_max_frame_size {
            return Err(user_payload_too_big(data.len(), self.send_max_frame_size));
        }
        let flags = if end_stream { FLAG_END_STREAM } else { 0 };
        let head = Head::new(Kind::Data, flags, frame::StreamId::from(stream_id));
        dst.reserve(HEADER_LEN + data.len());
        head.encode(data.len(), dst);
        dst.extend_from_slice(data);
        Ok(())
    }

    /// Apply the peer's SETTINGS_HEADER_TABLE_SIZE: bounds the table *our*
    /// encoder may use (h2 codec `set_send_header_table_size`).
    pub(super) fn set_send_header_table_size(&mut self, val: u32) {
        self.encoder.update_max_size(val as usize);
    }

    /// Apply the peer's SETTINGS_MAX_FRAME_SIZE: the per-frame payload budget for
    /// what *we* serialize (h2 framed_write.rs `set_max_frame_size`).
    pub(super) fn set_send_max_frame_size(&mut self, val: u32) -> PyResult<()> {
        check_max_frame_size(val)?; // the 3-byte length field cannot carry more than 2^24-1
        self.send_max_frame_size = val as usize;
        Ok(())
    }
}

/// Both halves behind one mutex: the standalone `H2Codec` pyclass (tests, low-level
/// use). The drivers never use it — `H2Streams` owns the halves separately.
pub(super) struct Codec {
    dec: Decoder,
    enc: Encoder,
}

// ----- frame builders / control-frame encoders shared by `H2Codec` and `H2Streams` -----
//
// Pure (no codec state): the HEADERS builders parse/validate the request parts
// BEFORE any lock is taken, so a connection-state method never fails half-way
// through a critical section.

/// Build a request HEADERS frame (h2 client.rs `Peer::convert_send_message`): the
/// request-target as `http::Uri` sees it — absolute-form carries its own scheme +
/// authority; origin-form (a bare path) takes the connection's, which hyper's h2
/// client fills in the same way (proto/h2/client.rs).
pub(super) fn build_request_headers(
    stream_id: u32,
    method: &str,
    target: &str,
    fields: http::HeaderMap,
    end_stream: bool,
    scheme: Option<&str>,
    authority: Option<&str>,
) -> PyResult<frame::Headers> {
    let method =
        Method::from_bytes(method.as_bytes()).map_err(|e| value_err("invalid method", e))?;
    let uri: Uri = target.parse().map_err(|e| value_err("invalid url", e))?;
    let uri = if uri.scheme().is_some() && uri.authority().is_some() {
        uri
    } else {
        let (Some(scheme), Some(authority)) = (scheme, authority) else {
            return Err(PyValueError::new_err(
                "request target is a bare path but the connection has no authority; \
                 pass authority=... to H2Connection or use an absolute-URL target",
            ));
        };
        Uri::builder()
            .scheme(scheme)
            .authority(authority)
            .path_and_query(target)
            .build()
            .map_err(|e| value_err("invalid url", e))?
    };
    let pseudo = frame::Pseudo::request(method, uri, None);
    let mut hframe = frame::Headers::new(frame::StreamId::from(stream_id), pseudo, fields);
    // A bodyless request carries END_STREAM on HEADERS (h2 `send_request` with
    // `end_of_stream`), rather than a trailing empty DATA frame.
    if end_stream {
        hframe.set_end_stream();
    }
    Ok(hframe)
}

/// Build a response HEADERS frame. `auto_date`: hyper's h2 server inserts `Date`
/// when the app didn't set one (proto/h2/server.rs L484
/// `entry(DATE).or_insert_with(date::update_and_header_value)`).
pub(super) fn build_response_headers(
    stream_id: u32,
    status: u16,
    mut fields: http::HeaderMap,
    end_stream: bool,
    auto_date: bool,
) -> PyResult<frame::Headers> {
    let status = StatusCode::from_u16(status).map_err(|e| value_err("invalid status", e))?;
    let pseudo = frame::Pseudo::response(status);
    if auto_date {
        fields.entry(DATE).or_insert_with(date_header_value);
    }
    let mut hframe = frame::Headers::new(frame::StreamId::from(stream_id), pseudo, fields);
    // A bodyless response carries END_STREAM on HEADERS (e.g. a HEAD response,
    // 204/304), rather than a trailing empty DATA frame.
    if end_stream {
        hframe.set_end_stream();
    }
    Ok(hframe)
}

/// A trailing HEADERS frame (no pseudo-headers, END_STREAM set) — request/response
/// trailers sent after the DATA frames (h2 `frame::Headers::trailers`) (F45).
pub(super) fn build_trailers(stream_id: u32, fields: http::HeaderMap) -> frame::Headers {
    frame::Headers::trailers(frame::StreamId::from(stream_id), fields)
}

pub(super) fn encode_window_update(dst: &mut BytesMut, stream_id: u32, increment: u32) {
    frame::WindowUpdate::new(frame::StreamId::from(stream_id), increment).encode(dst);
}

pub(super) fn encode_rst_stream(dst: &mut BytesMut, stream_id: u32, error_code: u32) {
    frame::Reset::new(
        frame::StreamId::from(stream_id),
        frame::Reason::from(error_code),
    )
    .encode(dst);
}

/// A GOAWAY frame (connection shutdown), `stream_id = 0`.
pub(super) fn encode_go_away(
    dst: &mut BytesMut,
    last_stream_id: u32,
    error_code: u32,
    debug_data: &[u8],
) {
    let sid = frame::StreamId::from(last_stream_id);
    let reason = frame::Reason::from(error_code);
    let ga = if debug_data.is_empty() {
        frame::GoAway::new(sid, reason)
    } else {
        frame::GoAway::with_debug_data(sid, reason, Bytes::copy_from_slice(debug_data))
    };
    ga.encode(dst);
}

fn ping_payload(payload: &[u8]) -> PyResult<[u8; 8]> {
    if payload.len() != 8 {
        return Err(PyValueError::new_err(
            "ping payload must be exactly 8 bytes",
        ));
    }
    let mut p = [0u8; 8];
    p.copy_from_slice(payload);
    Ok(p)
}

/// A PING frame (`ack=false`: a keep-alive / shutdown ping; `ack=true`: its PONG).
pub(super) fn encode_ping(dst: &mut BytesMut, payload: &[u8], ack: bool) -> PyResult<()> {
    let p = ping_payload(payload)?;
    let ping = if ack {
        frame::Ping::pong(p)
    } else {
        frame::Ping::new(p)
    };
    ping.encode(dst);
    Ok(())
}

pub(super) fn encode_settings_ack(dst: &mut BytesMut) {
    frame::Settings::ack().encode(dst);
}

/// The values a local SETTINGS frame advertises (RFC 7540 §6.5.2). `None` = not sent.
#[derive(Clone, Copy, Default)]
pub(super) struct SettingsValues {
    pub header_table_size: Option<u32>,
    pub enable_push: Option<bool>,
    pub max_concurrent_streams: Option<u32>,
    pub initial_window_size: Option<u32>,
    pub max_frame_size: Option<u32>,
    pub max_header_list_size: Option<u32>,
}

/// Encode a SETTINGS frame. Pre-validates the ranges the vendored `frame::Settings`
/// setters assert on, so a bad value surfaces as a Python error instead of aborting
/// the process (release builds are `panic = "abort"`). RFC 7540 §6.5.2.
pub(super) fn encode_settings(dst: &mut BytesMut, v: &SettingsValues) -> PyResult<()> {
    if let Some(f) = v.max_frame_size
        && !(DEFAULT_MAX_FRAME_SIZE as u32..=MAX_MAX_FRAME_SIZE).contains(&f)
    {
        return Err(PyValueError::new_err(format!(
            "max_frame_size must be in [{DEFAULT_MAX_FRAME_SIZE}, {MAX_MAX_FRAME_SIZE}], got {f}"
        )));
    }
    if let Some(w) = v.initial_window_size
        && w > MAX_WINDOW_SIZE
    {
        return Err(PyValueError::new_err(format!(
            "initial_window_size must be <= {MAX_WINDOW_SIZE}, got {w}"
        )));
    }
    let mut s = frame::Settings::default();
    if let Some(x) = v.header_table_size {
        s.set_header_table_size(Some(x));
    }
    if let Some(x) = v.enable_push {
        s.set_enable_push(x);
    }
    if let Some(x) = v.max_concurrent_streams {
        s.set_max_concurrent_streams(Some(x));
    }
    if let Some(x) = v.initial_window_size {
        s.set_initial_window_size(Some(x));
    }
    if let Some(x) = v.max_frame_size {
        s.set_max_frame_size(Some(x));
    }
    if let Some(x) = v.max_header_list_size {
        s.set_max_header_list_size(Some(x));
    }
    s.encode(dst);
    Ok(())
}

/// The standalone codec pyclass: the same `Codec` behind its own mutex, for tests
/// and low-level use (the drivers use the one embedded in `H2Streams`).
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
                dec: Decoder::new(false),
                enc: Encoder::new(),
            }),
            role_client,
        })
    }

    /// Feed inbound bytes; return the list of complete frames now decodable.
    fn receive(&self, py: Python<'_>, data: &[u8]) -> PyResult<Vec<Py<PyAny>>> {
        self.inner.lock().unwrap().dec.receive_frames(py, data)
    }

    /// Number of bytes currently buffered awaiting a complete frame.
    fn buffered(&self) -> usize {
        self.inner.lock().unwrap().dec.buffered()
    }

    /// See `Codec::feed_preface`.
    fn feed_preface(&self, data: &[u8]) -> Option<bool> {
        self.inner.lock().unwrap().dec.feed_preface(data)
    }

    /// Classify a prefix of a connection's first bytes against the client preface —
    /// hyper-util `server::conn::auto` `read_version`: `None` = matches so far but
    /// incomplete, `Some(True)` = the full preface, `Some(False)` = diverged (HTTP/1).
    #[staticmethod]
    fn match_preface(data: &[u8]) -> Option<bool> {
        let n = data.len().min(H2_PREFACE.len());
        if data[..n] != H2_PREFACE[..n] {
            return Some(false);
        }
        if data.len() < H2_PREFACE.len() {
            return None;
        }
        Some(true)
    }

    /// h2 send.rs `check_headers` (RFC 9113 §8.2.2): connection-specific fields in an
    /// outbound HEADERS block, and a `te` other than exactly `trailers`, are
    /// `UserError::MalformedHeaders` (`H2UserError`). A caller error, not a protocol
    /// one: the connection and stream stay usable.
    #[staticmethod]
    fn check_send_headers(headers: &HeaderMap) -> PyResult<()> {
        headers.with_inner(check_send_fields)
    }

    /// `method == Method::HEAD` on the parsed `http::Method` (hyper proto/h2/client.rs
    /// `is_head`): case-sensitive, as HTTP methods are.
    #[staticmethod]
    fn method_is_head(method: &str) -> bool {
        Method::from_bytes(method.as_bytes()).is_ok_and(|m| m == Method::HEAD)
    }

    /// h2 `StreamId::is_client_initiated` (odd ids).
    #[staticmethod]
    fn is_client_initiated(stream_id: u32) -> bool {
        frame::StreamId::from(stream_id).is_client_initiated()
    }

    // ===== settings application (HPACK table sizes) =================

    fn set_send_header_table_size(&self, val: u32) {
        self.inner
            .lock()
            .unwrap()
            .enc
            .set_send_header_table_size(val);
    }

    fn set_recv_header_table_size(&self, val: u32) {
        self.inner
            .lock()
            .unwrap()
            .dec
            .set_recv_header_table_size(val);
    }

    fn set_max_recv_frame_size(&self, val: u32) -> PyResult<()> {
        self.inner.lock().unwrap().dec.set_max_recv_frame_size(val)
    }

    fn set_max_header_list_size(&self, val: u32) {
        self.inner.lock().unwrap().dec.set_max_header_list_size(val);
    }

    fn set_send_max_frame_size(&self, val: u32) -> PyResult<()> {
        self.inner.lock().unwrap().enc.set_send_max_frame_size(val)
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
        let mut dst = BytesMut::new();
        encode_settings(
            &mut dst,
            &SettingsValues {
                header_table_size,
                enable_push,
                max_concurrent_streams,
                initial_window_size,
                max_frame_size,
                max_header_list_size,
            },
        )?;
        Ok(PyBytes::new(py, &dst).unbind())
    }

    fn serialize_settings_ack(&self, py: Python<'_>) -> Py<PyBytes> {
        let mut dst = BytesMut::new();
        encode_settings_ack(&mut dst);
        PyBytes::new(py, &dst).unbind()
    }

    #[pyo3(signature = (stream_id, method, target, headers=None, end_stream=false, *, scheme=None, authority=None))]
    #[allow(clippy::too_many_arguments)] // the request's URI parts, as hyper's h2 client takes them
    fn serialize_request_headers(
        &self,
        py: Python<'_>,
        stream_id: u32,
        method: &str,
        target: &str,
        headers: Option<&HeaderMap>,
        end_stream: bool,
        scheme: Option<&str>,
        authority: Option<&str>,
    ) -> PyResult<Py<PyBytes>> {
        let fields = headers.map(HeaderMap::snapshot).unwrap_or_default();
        let hframe = build_request_headers(
            stream_id, method, target, fields, end_stream, scheme, authority,
        )?;
        let mut dst = BytesMut::new();
        self.inner
            .lock()
            .unwrap()
            .enc
            .encode_headers(hframe, &mut dst);
        Ok(PyBytes::new(py, &dst).unbind())
    }

    #[pyo3(signature = (stream_id, status, headers=None, end_stream=false, auto_date=false))]
    fn serialize_response_headers(
        &self,
        py: Python<'_>,
        stream_id: u32,
        status: u16,
        headers: Option<&HeaderMap>,
        end_stream: bool,
        auto_date: bool,
    ) -> PyResult<Py<PyBytes>> {
        let fields = headers.map(HeaderMap::snapshot).unwrap_or_default();
        let hframe = build_response_headers(stream_id, status, fields, end_stream, auto_date)?;
        let mut dst = BytesMut::new();
        self.inner
            .lock()
            .unwrap()
            .enc
            .encode_headers(hframe, &mut dst);
        Ok(PyBytes::new(py, &dst).unbind())
    }

    fn serialize_trailers(
        &self,
        py: Python<'_>,
        stream_id: u32,
        trailers: &HeaderMap,
    ) -> Py<PyBytes> {
        let hframe = build_trailers(stream_id, trailers.snapshot());
        let mut dst = BytesMut::new();
        self.inner
            .lock()
            .unwrap()
            .enc
            .encode_headers(hframe, &mut dst);
        PyBytes::new(py, &dst).unbind()
    }

    #[pyo3(signature = (stream_id, data, end_stream=false))]
    fn serialize_data(
        &self,
        py: Python<'_>,
        stream_id: u32,
        data: &[u8],
        end_stream: bool,
    ) -> PyResult<Py<PyBytes>> {
        let mut dst = BytesMut::new();
        // Check and encode under ONE acquisition, so a concurrent SETTINGS-driven
        // `set_send_max_frame_size` cannot lower the bound between the two.
        self.inner
            .lock()
            .unwrap()
            .enc
            .encode_data(stream_id, data, end_stream, &mut dst)?;
        Ok(PyBytes::new(py, &dst).unbind())
    }

    fn serialize_window_update(
        &self,
        py: Python<'_>,
        stream_id: u32,
        increment: u32,
    ) -> Py<PyBytes> {
        let mut dst = BytesMut::new();
        encode_window_update(&mut dst, stream_id, increment);
        PyBytes::new(py, &dst).unbind()
    }

    fn serialize_ping_ack(&self, py: Python<'_>, payload: &[u8]) -> PyResult<Py<PyBytes>> {
        let mut dst = BytesMut::new();
        encode_ping(&mut dst, payload, true)?;
        Ok(PyBytes::new(py, &dst).unbind())
    }

    /// A PING frame (not ACK) carrying an 8-byte payload — for keep-alive.
    fn serialize_ping(&self, py: Python<'_>, payload: &[u8]) -> PyResult<Py<PyBytes>> {
        let mut dst = BytesMut::new();
        encode_ping(&mut dst, payload, false)?;
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
        let mut dst = BytesMut::new();
        encode_go_away(
            &mut dst,
            last_stream_id,
            error_code,
            debug_data.unwrap_or(&[]),
        );
        PyBytes::new(py, &dst).unbind()
    }

    /// A RST_STREAM frame — abruptly terminate `stream_id`.
    fn serialize_rst_stream(&self, py: Python<'_>, stream_id: u32, error_code: u32) -> Py<PyBytes> {
        let mut dst = BytesMut::new();
        encode_rst_stream(&mut dst, stream_id, error_code);
        PyBytes::new(py, &dst).unbind()
    }
}

/// h2 send.rs `check_headers` over a raw `http::HeaderMap` (see `H2Codec::check_send_headers`).
pub(super) fn check_send_fields(fields: &http::HeaderMap) -> PyResult<()> {
    if fields.contains_key(http::header::CONNECTION)
        || fields.contains_key(http::header::TRANSFER_ENCODING)
        || fields.contains_key(http::header::UPGRADE)
        || fields.contains_key("keep-alive")
        || fields.contains_key("proxy-connection")
    {
        return Err(map_user_err(UserError::MalformedHeaders));
    }
    if fields
        .get(http::header::TE)
        .is_some_and(|te| te != "trailers")
    {
        return Err(map_user_err(UserError::MalformedHeaders));
    }
    Ok(())
}

impl Decoder {
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
                        padding: d.flow_controlled_len() - d.payload().len(),
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
