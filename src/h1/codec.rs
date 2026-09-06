//! PyO3 surface for the HTTP/1 sans-IO codec. `H1Codec` drives the vendored
//! hyper h1 head parse/encode + body `Encoder` (via
//! `vendor_hyper::proto::h1::httpunk`) with zero I/O — the h1 analogue of
//! `H2Codec`. `frozen` with a `std::sync::Mutex` over the small parse/encode
//! state, so it is `Sync` across the runtime's worker threads.
//!
//! The connection state machine (request/response lifecycle, keep-alive) lives
//! in Python; the sans-IO byte work — head parse/encode, body-frame encode, and
//! body decode (`H1BodyDecoder`) — is all Rust, over the vendored hyper core.

use bytes::BytesMut;
use http::{Method, StatusCode, Uri};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::sync::Mutex;

use super::errors::map_hyper_err;
use crate::http::HeaderMap;
use vendor_hyper::{
    BodyDecode, BodyDecoder, BodyEncoder, CONTINUE_RESPONSE, DEFAULT_MAX_BUFFER_SIZE,
    MINIMUM_MAX_BUFFER_SIZE, connection_any_close, encode_request, encode_response, parse_request,
    parse_response, server_parse_failure, too_large_error,
};

/// Map a facade `BodyDecode` to the `(body_kind, content_length)` a Python driver
/// hands to `H1BodyDecoder`. Shared by the request (server) and response (client)
/// head-parse paths.
fn body_kind(body: &BodyDecode) -> (&'static str, Option<u64>) {
    match body {
        BodyDecode::Empty => ("empty", Some(0)),
        BodyDecode::Length(n) => ("length", Some(*n)),
        BodyDecode::Chunked => ("chunked", None),
        BodyDecode::CloseDelimited => ("close", None),
    }
}

/// Caller-argument validation (an invalid method / URL / status / header name is
/// rejected by the `http` crate before hyper sees it): a plain `ValueError`. Wire
/// and encoder errors from hyper itself go through `map_hyper_err` instead.
fn value_err<E: std::fmt::Display>(what: &str, e: E) -> PyErr {
    PyValueError::new_err(format!("{what}: {e}"))
}

struct State {
    /// Received bytes not yet consumed by a head parse — hyper's persistent
    /// `read_buf`: it outlives a message (`reset` keeps it), so pipelined bytes are
    /// never copied out and back in.
    buf: BytesMut,
    /// Method of the in-flight request (a response's bodyless-ness can depend
    /// on it, e.g. a response to HEAD).
    req_method: Option<Method>,
    /// Body framing for the in-flight request (chunked / content-length).
    encoder: Option<BodyEncoder>,
    /// The in-flight request carried `Connection: close` (any line): hyper's client
    /// `encode_head` -> `connection_any_close` -> `disable_keep_alive` (1.11.1).
    request_connection_close: bool,
    /// hyper conn.rs `state.allow_trailer_fields` (L328): the parsed request declared
    /// `TE: trailers`. True until a request head is parsed, so a client-role codec
    /// (which never parses one) always sends its trailers (`write_trailers` gates on
    /// `T::is_server()`).
    allow_trailer_fields: bool,
    /// After a failed `receive_request_head`: the automatic response status hyper's
    /// `Server::on_error` picks, `None` when hyper answers nothing (h2 preface).
    parse_error_status: Option<u16>,
    /// The encoder verdicts of the last `serialize_response` (hyper `Encoder::is_last`
    /// / `is_close_delimited`), read by the driver for its reuse decision.
    response_is_last: bool,
    response_close_delimited: bool,
}

/// A synchronous HTTP/1 codec (client + server roles).
#[pyclass(module = "httpunk._httpunk", name = "H1Codec", frozen)]
pub struct H1Codec {
    inner: Mutex<State>,
    /// Server-role parse/encode options (hyper `server::conn::http1::Builder`):
    /// `max_headers` (None = hyper's default 100), `ignore_invalid_headers`
    /// (httparse `ignore_invalid_headers_in_requests`), `title_case_headers`,
    /// `date_header` (`auto_date_header`), `max_buf_size` (hyper io.rs: the read
    /// buffer cap a still-incomplete head may reach before `Parse::TooLarge`; both
    /// roles). Immutable per codec; the driver builds one codec per connection.
    max_headers: Option<usize>,
    ignore_invalid_headers: bool,
    title_case_headers: bool,
    date_header: bool,
    max_buf_size: usize,
}

#[pymethods]
impl H1Codec {
    #[new]
    #[pyo3(signature = (*, max_headers=None, ignore_invalid_headers=false, title_case_headers=false, date_header=true, max_buf_size=DEFAULT_MAX_BUFFER_SIZE))]
    fn new(
        max_headers: Option<usize>,
        ignore_invalid_headers: bool,
        title_case_headers: bool,
        date_header: bool,
        max_buf_size: usize,
    ) -> PyResult<Self> {
        if max_buf_size < MINIMUM_MAX_BUFFER_SIZE {
            // hyper io.rs `set_max_buf_size` asserts this (L86-90).
            return Err(PyValueError::new_err(format!(
                "the max_buf_size cannot be smaller than {MINIMUM_MAX_BUFFER_SIZE}"
            )));
        }
        Ok(H1Codec {
            inner: Mutex::new(State {
                buf: BytesMut::new(),
                req_method: None,
                encoder: None,
                request_connection_close: false,
                allow_trailer_fields: true,
                parse_error_status: None,
                response_is_last: false,
                response_close_delimited: false,
            }),
            max_headers,
            ignore_invalid_headers,
            title_case_headers,
            date_header,
            max_buf_size,
        })
    }

    /// Start the next message on this connection: drop the per-message state, keep
    /// the read buffer (hyper's `read_buf` persists across messages, so bytes of a
    /// pipelined request already received stay right here).
    pub(super) fn reset(&self) {
        let mut st = self.inner.lock().unwrap();
        st.req_method = None;
        st.encoder = None;
        st.request_connection_close = false;
        st.allow_trailer_fields = true;
        st.parse_error_status = None;
        st.response_is_last = false;
        st.response_close_delimited = false;
    }

    /// Append received bytes to the read buffer WITHOUT parsing — bytes another
    /// reader took past a message (the body decoder's leftover, a watcher's read) go
    /// back where hyper's single `read_buf` would have kept them.
    pub(super) fn feed(&self, data: &[u8]) {
        self.inner.lock().unwrap().buf.extend_from_slice(data);
    }

    /// The interim response hyper's server writes for `Expect: 100-continue`
    /// (conn.rs L409).
    #[staticmethod]
    fn continue_response(py: Python<'_>) -> Py<PyBytes> {
        PyBytes::new(py, CONTINUE_RESPONSE).unbind()
    }

    /// Serialize a request head (request line + headers). `content_length` /
    /// `chunked` pick the body framing (mutually exclusive; neither = no body);
    /// the returned bytes are the head, and the body `Encoder` is retained for
    /// `serialize_data`/`serialize_end`. `http10` = the peer is known to speak
    /// HTTP/1.0 (hyper `enforce_version`: keep-alive re-asserted, request line
    /// downgraded). Chunked trailers are allow-listed from the request's own
    /// `Trailer` header, as hyper's `Client::encode` does.
    #[pyo3(signature = (method, url, headers=None, *, http10=false, content_length=None, chunked=false))]
    fn serialize_request(
        &self,
        py: Python<'_>,
        method: &str,
        url: &str,
        headers: Option<&HeaderMap>,
        http10: bool,
        content_length: Option<u64>,
        chunked: bool,
    ) -> PyResult<Py<PyBytes>> {
        let m =
            Method::from_bytes(method.as_bytes()).map_err(|e| value_err("invalid method", e))?;
        let uri: Uri = url.parse().map_err(|e| value_err("invalid url", e))?;
        let fields = headers.map(HeaderMap::snapshot).unwrap_or_default();
        let body = if chunked {
            Some(None)
        } else {
            content_length.map(Some)
        };
        let connection_close = connection_any_close(&fields);
        let (dst, encoder) =
            encode_request(m.clone(), uri, fields, body, http10).map_err(map_hyper_err)?;
        let mut st = self.inner.lock().unwrap();
        st.req_method = Some(m);
        st.encoder = Some(encoder);
        st.request_connection_close = connection_close;
        Ok(PyBytes::new(py, &dst).unbind())
    }

    /// The request serialized by `serialize_request` carried `Connection: close` on any
    /// line (hyper `connection_any_close`): the client must not reuse the connection
    /// whatever the response says (conn.rs `encode_head` -> `disable_keep_alive`).
    #[getter]
    fn request_connection_close(&self) -> bool {
        self.inner.lock().unwrap().request_connection_close
    }

    /// Any `Connection` line of `headers` carries a `close` token — hyper
    /// `headers::connection_any_close`; the server driver's keep-alive negotiation
    /// reads a response's close intent with it before `serialize_response`.
    #[staticmethod]
    fn connection_close(headers: &HeaderMap) -> bool {
        headers.with_inner(connection_any_close)
    }

    /// Frame one body chunk (chunked prefix/CRLF, or raw for content-length).
    fn serialize_data(&self, py: Python<'_>, chunk: &[u8]) -> PyResult<Py<PyBytes>> {
        if chunk.is_empty() {
            return Ok(PyBytes::new(py, b"").unbind());
        }
        let mut st = self.inner.lock().unwrap();
        let enc = st
            .encoder
            .as_mut()
            .ok_or_else(|| PyValueError::new_err("serialize_data with no request in flight"))?;
        Ok(PyBytes::new(py, &enc.encode(chunk)).unbind())
    }

    /// Finish the body: the chunked terminator `0\r\n\r\n`, or empty for a
    /// content-length body. Raises `H1UserError(kind="body_write_aborted")` if a
    /// declared Content-Length wasn't filled (hyper `end_body` -> `NotEof`).
    fn serialize_end(&self, py: Python<'_>) -> PyResult<Py<PyBytes>> {
        let mut st = self.inner.lock().unwrap();
        let out = match st.encoder.take() {
            Some(enc) => enc.end().map_err(map_hyper_err)?,
            None => Vec::new(),
        };
        Ok(PyBytes::new(py, &out).unbind())
    }

    /// Finish a chunked body with trailing headers instead of a bare terminator.
    /// Only the fields the message's own `Trailer` header declared are emitted; the
    /// rest are dropped by hyper's `encode_trailers`, and a non-chunked body gets the
    /// bare terminator. On the server, trailers go out only if the request declared
    /// `TE: trailers` (conn.rs `write_trailers` L740: "trailers not allowed to be
    /// sent"), the body then ending as usual (`end_body`).
    fn serialize_trailers(&self, py: Python<'_>, trailers: &HeaderMap) -> PyResult<Py<PyBytes>> {
        let mut st = self.inner.lock().unwrap();
        let allowed = st.allow_trailer_fields;
        let out = match st.encoder.take() {
            Some(enc) if allowed => enc
                .end_with_trailers(trailers.snapshot())
                .map_err(map_hyper_err)?,
            Some(enc) => enc.end().map_err(map_hyper_err)?,
            None => Vec::new(),
        };
        Ok(PyBytes::new(py, &out).unbind())
    }

    /// One message in one buffer: `head` + the framed `body` (if any) + the body's end
    /// (`trailers`, else the bare terminator) — hyper's `WriteBuf` flatten strategy for
    /// a small immediate body, so the driver issues a single write and copies nothing.
    /// A bodyless framing (`body_is_eof`) writes no body, whatever `body` holds.
    #[pyo3(signature = (head, body=None, trailers=None))]
    fn serialize_head_and_body(
        &self,
        py: Python<'_>,
        head: &[u8],
        body: Option<&[u8]>,
        trailers: Option<&HeaderMap>,
    ) -> PyResult<Py<PyBytes>> {
        let mut st = self.inner.lock().unwrap();
        let allowed = st.allow_trailer_fields;
        let mut out = Vec::with_capacity(head.len() + body.map_or(0, <[u8]>::len) + 16);
        out.extend_from_slice(head);
        match st.encoder.take() {
            None => {}
            Some(enc) if enc.is_eof() => out.extend(enc.end().map_err(map_hyper_err)?),
            Some(mut enc) => {
                if let Some(chunk) = body {
                    out.extend(enc.encode(chunk));
                }
                let end = match trailers {
                    Some(t) if allowed => enc.end_with_trailers(t.snapshot()),
                    _ => enc.end(),
                };
                out.extend(end.map_err(map_hyper_err)?);
            }
        }
        Ok(PyBytes::new(py, &out).unbind())
    }

    /// True when the in-flight body framing carries no body (a bodyless response
    /// to HEAD/204/304, or a zero-length request). The driver skips polling the
    /// caller's body in this case, mirroring hyper's `write_head` `encoder.is_eof()`
    /// gate (conn.rs) — so a supplied body iterable is never drained (G37). No
    /// encoder in flight also counts as "no body".
    fn body_is_eof(&self) -> bool {
        self.inner
            .lock()
            .unwrap()
            .encoder
            .as_ref()
            .is_none_or(BodyEncoder::is_eof)
    }

    /// Feed received bytes; if a full response head is now available, consume it
    /// and return an `H1ResponseHead` (leftover bytes are the start of the body,
    /// drained via `take_body`). Returns `None` if more bytes are needed.
    fn receive_head(&self, py: Python<'_>, data: &[u8]) -> PyResult<Option<Py<PyAny>>> {
        let mut st = self.inner.lock().unwrap();
        st.buf.extend_from_slice(data);
        let req_method = st.req_method.clone();
        match parse_response(&mut st.buf, &req_method).map_err(map_hyper_err)? {
            // hyper io.rs L202-207: a head still incomplete at `max_buf_size` is
            // `Parse::TooLarge` (the client just fails the connection).
            None if st.buf.len() >= self.max_buf_size => Err(map_hyper_err(too_large_error())),
            Some(head) => {
                let (kind, content_length) = body_kind(&head.body);
                let headers = Py::new(py, HeaderMap::from_inner(head.headers))?;
                let event = Py::new(
                    py,
                    ResponseHead {
                        status: head.status,
                        keep_alive: head.keep_alive,
                        headers,
                        body_kind: kind.to_string(),
                        content_length,
                        is_upgrade: head.wants_upgrade,
                        http10: head.http10,
                    },
                )?;
                Ok(Some(event.into_any()))
            }
            None => Ok(None),
        }
    }

    // ===== server side =====

    /// Feed received bytes; if a full request head is available, consume it and
    /// return an `H1RequestHead` (leftover = the start of the request body,
    /// drained via `take_body`) — via the facade's `parse_request` (hyper
    /// `Server::parse`). Records the request method for `serialize_response`.
    pub(super) fn receive_request_head(
        &self,
        py: Python<'_>,
        data: &[u8],
    ) -> PyResult<Option<Py<PyAny>>> {
        let mut st = self.inner.lock().unwrap();
        st.buf.extend_from_slice(data);
        let parsed = match parse_request(&mut st.buf, self.max_headers, self.ignore_invalid_headers)
        {
            // hyper io.rs L202-207: a head still incomplete at `max_buf_size` is
            // `Parse::TooLarge` -> `Server::on_error` answers 431.
            Ok(None) if st.buf.len() >= self.max_buf_size => Err(too_large_error()),
            other => other,
        };
        let parsed = match parsed {
            Ok(parsed) => parsed,
            Err(e) => {
                // conn.rs `on_parse_error`: the h2 preface becomes `VersionH2` (no
                // response); otherwise `Server::on_error` picks the automatic status.
                let (e, status) = server_parse_failure(&st.buf, e);
                st.parse_error_status = status;
                return Err(map_hyper_err(e));
            }
        };
        match parsed {
            Some(head) => {
                let (kind, content_length) = body_kind(&head.body);
                // Remember the method so a response's bodyless-ness (HEAD/204/304)
                // is computed correctly by `encode_response`.
                st.req_method = Method::from_bytes(head.method.as_bytes()).ok();
                st.allow_trailer_fields = head.allow_trailers; // conn.rs `read_head` L328
                let headers = Py::new(py, HeaderMap::from_inner(head.headers))?;
                let event = Py::new(
                    py,
                    RequestHead {
                        method: head.method,
                        target: head.target,
                        keep_alive: head.keep_alive,
                        headers,
                        body_kind: kind.to_string(),
                        content_length,
                        expect_continue: head.expect_continue,
                        is_upgrade: head.wants_upgrade,
                        http10: head.http10,
                        allow_trailers: head.allow_trailers,
                    },
                )?;
                Ok(Some(event.into_any()))
            }
            None => Ok(None),
        }
    }

    /// Serialize a response head (status line + headers) via the facade's
    /// `encode_response` (hyper `Server::encode`), retaining the body `Encoder`
    /// for `serialize_data`/`serialize_end`. Uses the request method recorded by
    /// `receive_request_head` for bodyless-ness (HEAD/204/304). `keep_alive` is
    /// hyper's `wants_keep_alive()` (the request was keep-alive, no shutdown,
    /// keep-alive enabled) and `http10` whether the request was HTTP/1.0: conn.rs
    /// `enforce_version` runs over the head first (`Connection: close` inserted when
    /// keep-alive is off, keep-alive re-asserted to a 1.0 peer, version downgraded —
    /// an unknown-length 1.0 body is then close-delimited). The encoder's verdicts
    /// are read back via `response_is_last` / `response_close_delimited`. Writes a
    /// `Date` header unless the codec was built with `date_header=False`.
    #[pyo3(signature = (status, headers=None, *, keep_alive=true, http10=false, content_length=None, chunked=false))]
    #[allow(clippy::too_many_arguments)] // faithful mirror of hyper's Encode fields
    pub(super) fn serialize_response(
        &self,
        py: Python<'_>,
        status: u16,
        headers: Option<&HeaderMap>,
        keep_alive: bool,
        http10: bool,
        content_length: Option<u64>,
        chunked: bool,
    ) -> PyResult<Py<PyBytes>> {
        let fields = headers.map(HeaderMap::snapshot).unwrap_or_default();
        let body = if chunked {
            Some(None)
        } else {
            content_length.map(Some)
        };
        let status = StatusCode::from_u16(status).map_err(|e| value_err("invalid status", e))?;
        // ONE critical section from reading `req_method` to publishing the encoder and
        // its verdicts: the call is one atomic step, as every codec method must be.
        // hyper `Server::encode` rejects a 1xx (not 101) status and a
        // content-length + transfer-encoding pair as `User` errors
        // (`H1UserError`); the connection then closes (conn.rs `encode_head`
        // -> `Writing::Closed` + the error stored on the connection).
        let dst = {
            let mut st = self.inner.lock().unwrap();
            let (dst, encoder) = encode_response(
                status,
                fields,
                body,
                st.req_method.clone(),
                keep_alive,
                http10,
                self.title_case_headers,
                self.date_header,
            )
            .map_err(map_hyper_err)?;
            st.response_is_last = encoder.is_last();
            st.response_close_delimited = encoder.is_close_delimited();
            st.encoder = Some(encoder);
            dst
        };
        Ok(PyBytes::new(py, &dst).unbind())
    }

    /// hyper `Encoder::is_last` of the last `serialize_response`: the connection
    /// closes after this response (keep-alive off, a response `Connection: close`, a
    /// 101, or a 2xx to CONNECT) — conn.rs `try_keep_alive`.
    #[getter]
    pub(super) fn response_is_last(&self) -> bool {
        self.inner.lock().unwrap().response_is_last
    }

    /// hyper `Encoder::is_close_delimited` of the last `serialize_response`: the body
    /// ends by closing the connection (an unknown-length HTTP/1.0 response).
    #[getter]
    pub(super) fn response_close_delimited(&self) -> bool {
        self.inner.lock().unwrap().response_close_delimited
    }

    /// After a failed `receive_request_head`: the automatic response status hyper's
    /// server writes (`Server::on_error`: 400 / 414 / 431), or `None` when it answers
    /// nothing and just closes (an HTTP/2 preface: `Parse::VersionH2`).
    #[getter]
    fn parse_error_status(&self) -> Option<u16> {
        self.inner.lock().unwrap().parse_error_status
    }

    /// Drain the bytes buffered after the head — the body bytes already received,
    /// to hand to the Python body decoder.
    pub(super) fn take_body(&self, py: Python<'_>) -> Py<PyBytes> {
        PyBytes::new(py, &self.take_body_raw()).unbind()
    }

    /// Number of bytes currently buffered (unparsed head, or post-head body).
    pub(super) fn buffered(&self) -> usize {
        self.inner.lock().unwrap().buf.len()
    }
}

impl H1Codec {
    /// `take_body` for a Rust consumer (the server state feeds the decoder itself).
    pub(super) fn take_body_raw(&self) -> BytesMut {
        self.inner.lock().unwrap().buf.split()
    }
}

/// A parsed HTTP/1 response head (the event `receive_head` yields).
#[pyclass(module = "httpunk._httpunk", name = "H1ResponseHead", frozen)]
pub struct ResponseHead {
    #[pyo3(get)]
    pub status: u16,
    #[pyo3(get)]
    pub keep_alive: bool,
    #[pyo3(get)]
    pub headers: Py<HeaderMap>,
    /// One of "empty" | "length" | "chunked" | "close".
    #[pyo3(get)]
    pub body_kind: String,
    #[pyo3(get)]
    pub content_length: Option<u64>,
    /// The response switches protocols (101 upgrade, or 2xx to CONNECT): the
    /// connection becomes a raw tunnel the caller takes over.
    #[pyo3(get)]
    pub is_upgrade: bool,
    /// The response was HTTP/1.0 — the driver downgrades later requests on the
    /// reused connection to HTTP/1.0 (hyper `enforce_version`).
    #[pyo3(get)]
    pub http10: bool,
}

#[pymethods]
impl ResponseHead {
    fn __repr__(&self) -> String {
        format!(
            "H1ResponseHead(status={}, body_kind={:?}, keep_alive={})",
            self.status, self.body_kind, self.keep_alive,
        )
    }
}

/// A parsed HTTP/1 request head (the event `receive_request_head` yields).
#[pyclass(module = "httpunk._httpunk", name = "H1RequestHead", frozen)]
pub struct RequestHead {
    #[pyo3(get)]
    pub method: String,
    /// The request-target verbatim (origin/absolute/authority form).
    #[pyo3(get)]
    pub target: String,
    #[pyo3(get)]
    pub keep_alive: bool,
    #[pyo3(get)]
    pub headers: Py<HeaderMap>,
    /// One of "empty" | "length" | "chunked" | "close".
    #[pyo3(get)]
    pub body_kind: String,
    #[pyo3(get)]
    pub content_length: Option<u64>,
    /// The client sent `Expect: 100-continue`.
    #[pyo3(get)]
    pub expect_continue: bool,
    /// A CONNECT / Upgrade request — the connection becomes a tunnel.
    #[pyo3(get)]
    pub is_upgrade: bool,
    /// The request was HTTP/1.0 (the response must reflect the version).
    #[pyo3(get)]
    pub http10: bool,
    /// The request declared `TE: trailers` (hyper `te_is_trailers`): response trailers
    /// may be sent; otherwise hyper's server drops them.
    #[pyo3(get)]
    pub allow_trailers: bool,
}

#[pymethods]
impl RequestHead {
    fn __repr__(&self) -> String {
        format!(
            "H1RequestHead(method={:?}, target={:?}, body_kind={:?})",
            self.method, self.target, self.body_kind,
        )
    }
}

/// A synchronous HTTP/1 response-body decoder (content-length / chunked /
/// close-delimited) — wraps the vendored hyper `Decoder` via the sans-IO
/// `vendor_hyper::BodyDecoder` facade.
#[pyclass(module = "httpunk._httpunk", name = "H1BodyDecoder", frozen)]
pub struct H1BodyDecoder {
    inner: Mutex<BodyDecoder>,
}

#[pymethods]
impl H1BodyDecoder {
    /// `kind`: "empty" | "length" | "chunked" | "close" (from `H1ResponseHead.body_kind`);
    /// `length` is the Content-Length when `kind == "length"`.
    #[new]
    #[pyo3(signature = (kind, length=0))]
    pub(super) fn new(kind: &str, length: u64) -> Self {
        H1BodyDecoder {
            inner: Mutex::new(BodyDecoder::new(kind, length)),
        }
    }

    /// Append received body bytes.
    pub(super) fn feed(&self, data: &[u8]) {
        self.inner.lock().unwrap().feed(data);
    }

    /// Signal that the transport closed (close-delimited bodies end here).
    fn mark_eof(&self) {
        self.inner.lock().unwrap().mark_eof();
    }

    /// Pull one body chunk: `bytes` if available, else `None` — end vs. need-more
    /// is distinguished by `is_complete`. Raises `H1BodyError` (hyper `Kind::Body`)
    /// on a malformed or truncated body; `io_kind` tells which
    /// (`unexpected_eof` = the transport closed mid-body).
    fn decode(&self, py: Python<'_>) -> PyResult<Option<Py<PyBytes>>> {
        match self.inner.lock().unwrap().decode().map_err(map_hyper_err)? {
            Some(chunk) => Ok(Some(PyBytes::new(py, &chunk).unbind())),
            None => Ok(None),
        }
    }

    #[getter]
    pub(super) fn is_complete(&self) -> bool {
        self.inner.lock().unwrap().is_complete()
    }

    /// Drain and return the bytes buffered past the completed body — the start of
    /// the next pipelined request (hyper keeps these in its persistent read
    /// buffer). The server driver feeds them back to the codec (`H1Codec.feed`).
    pub(super) fn take_buffered(&self, py: Python<'_>) -> Py<PyBytes> {
        PyBytes::new(py, &self.inner.lock().unwrap().take_buffered()).unbind()
    }

    /// Bytes buffered past the body, without moving them — hyper's
    /// `!read_buf().is_empty()` (`require_empty_read`, conn.rs L463-465).
    #[getter]
    pub(super) fn buffered(&self) -> usize {
        self.inner.lock().unwrap().buffered()
    }

    /// The chunked trailers (a `httpunk.http.HeaderMap`) once the body is
    /// complete, if the peer sent any; taken (moved out), so `None` afterward.
    fn take_trailers(&self, py: Python<'_>) -> PyResult<Option<Py<HeaderMap>>> {
        match self.inner.lock().unwrap().take_trailers() {
            Some(map) => Ok(Some(Py::new(py, HeaderMap::from_inner(map))?)),
            None => Ok(None),
        }
    }
}
