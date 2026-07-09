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
use http::{HeaderName, Method, Uri};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::sync::Mutex;

use crate::py::http::HeaderMap;
use vendor_hyper::{
    BodyDecode, BodyDecoder, BodyEncoder, encode_request, encode_response, parse_request,
    parse_response,
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

fn value_err<E: std::fmt::Display>(what: &str, e: E) -> PyErr {
    PyValueError::new_err(format!("{what}: {e}"))
}

struct State {
    /// Accumulates received bytes until a full response head parses.
    buf: BytesMut,
    /// Method of the in-flight request (a response's bodyless-ness can depend
    /// on it, e.g. a response to HEAD).
    req_method: Option<Method>,
    /// Body framing for the in-flight request (chunked / content-length).
    encoder: Option<BodyEncoder>,
}

/// A synchronous HTTP/1 client codec.
#[pyclass(module = "httpunk._httpunk", name = "H1Codec", frozen)]
pub struct H1Codec {
    inner: Mutex<State>,
}

#[pymethods]
impl H1Codec {
    #[new]
    fn new() -> Self {
        H1Codec {
            inner: Mutex::new(State {
                buf: BytesMut::new(),
                req_method: None,
                encoder: None,
            }),
        }
    }

    /// Serialize a request head (request line + headers). `content_length` /
    /// `chunked` pick the body framing (mutually exclusive; neither = no body);
    /// the returned bytes are the head, and the body `Encoder` is retained for
    /// `serialize_data`/`serialize_end`.
    #[pyo3(signature = (method, url, headers=None, *, http10=false, content_length=None, chunked=false, trailer_fields=Vec::new()))]
    #[allow(clippy::too_many_arguments)] // faithful mirror of hyper's request Encode inputs
    fn serialize_request(
        &self,
        py: Python<'_>,
        method: &str,
        url: &str,
        headers: Option<&HeaderMap>,
        http10: bool,
        content_length: Option<u64>,
        chunked: bool,
        trailer_fields: Vec<String>,
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
        // The declared chunked trailer field names (from the request's `Trailer`
        // header) — the body may then emit them via `serialize_trailers` (F45).
        let trailers = trailer_fields
            .iter()
            .map(|n| HeaderName::from_bytes(n.as_bytes()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| value_err("invalid trailer field name", e))?;
        let (dst, encoder) = encode_request(m.clone(), uri, fields, body, http10, trailers)
            .map_err(|e| value_err("failed to encode request", e))?;
        let mut st = self.inner.lock().unwrap();
        st.req_method = Some(m);
        st.encoder = Some(encoder);
        Ok(PyBytes::new(py, &dst).unbind())
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
    /// content-length body. Errors if a declared Content-Length wasn't filled.
    fn serialize_end(&self, py: Python<'_>) -> PyResult<Py<PyBytes>> {
        let mut st = self.inner.lock().unwrap();
        let out = match st.encoder.take() {
            Some(enc) => enc.end().map_err(PyValueError::new_err)?,
            None => Vec::new(),
        };
        Ok(PyBytes::new(py, &out).unbind())
    }

    /// Finish a chunked body with trailing headers instead of a bare terminator (F45).
    /// Only the fields declared as trailer fields at `serialize_request` (the `Trailer`
    /// header) are emitted; the rest are dropped by hyper's `encode_trailers`.
    fn serialize_trailers(&self, py: Python<'_>, trailers: &HeaderMap) -> PyResult<Py<PyBytes>> {
        let mut st = self.inner.lock().unwrap();
        let out = match st.encoder.take() {
            Some(enc) => enc
                .end_with_trailers(trailers.snapshot())
                .map_err(PyValueError::new_err)?,
            None => Vec::new(),
        };
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
        match parse_response(&mut st.buf, &req_method)
            .map_err(|e| PyValueError::new_err(format!("malformed HTTP/1 response: {e}")))?
        {
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
    fn receive_request_head(&self, py: Python<'_>, data: &[u8]) -> PyResult<Option<Py<PyAny>>> {
        let mut st = self.inner.lock().unwrap();
        st.buf.extend_from_slice(data);
        match parse_request(&mut st.buf)
            .map_err(|e| PyValueError::new_err(format!("malformed HTTP/1 request: {e}")))?
        {
            Some(head) => {
                let (kind, content_length) = body_kind(&head.body);
                // Remember the method so a response's bodyless-ness (HEAD/204/304)
                // is computed correctly by `encode_response`.
                st.req_method = Method::from_bytes(head.method.as_bytes()).ok();
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
    /// `receive_request_head` for bodyless-ness (HEAD/204/304), `keep_alive` to
    /// decide `Connection: close`, and `http10` to set the response version
    /// (an unknown-length 1.0 body is close-delimited, not chunked). Writes a
    /// `Date` header.
    #[pyo3(signature = (status, headers=None, *, keep_alive=true, http10=false, content_length=None, chunked=false))]
    #[allow(clippy::too_many_arguments)] // faithful mirror of hyper's Encode fields
    fn serialize_response(
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
        let req_method = self.inner.lock().unwrap().req_method.clone();
        let (dst, encoder) = encode_response(status, fields, body, req_method, keep_alive, http10)
            .map_err(|e| value_err("failed to encode response", e))?;
        self.inner.lock().unwrap().encoder = Some(encoder);
        Ok(PyBytes::new(py, &dst).unbind())
    }

    /// Drain the bytes buffered after the head — the body bytes already received,
    /// to hand to the Python body decoder.
    fn take_body(&self, py: Python<'_>) -> Py<PyBytes> {
        let mut st = self.inner.lock().unwrap();
        let out = st.buf.split();
        PyBytes::new(py, &out).unbind()
    }

    /// Number of bytes currently buffered (unparsed head, or post-head body).
    fn buffered(&self) -> usize {
        self.inner.lock().unwrap().buf.len()
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
    fn new(kind: &str, length: u64) -> Self {
        H1BodyDecoder {
            inner: Mutex::new(BodyDecoder::new(kind, length)),
        }
    }

    /// Append received body bytes.
    fn feed(&self, data: &[u8]) {
        self.inner.lock().unwrap().feed(data);
    }

    /// Signal that the transport closed (close-delimited bodies end here).
    fn mark_eof(&self) {
        self.inner.lock().unwrap().mark_eof();
    }

    /// Pull one body chunk: `bytes` if available, else `None` — end vs. need-more
    /// is distinguished by `is_complete`.
    fn decode(&self, py: Python<'_>) -> PyResult<Option<Py<PyBytes>>> {
        match self
            .inner
            .lock()
            .unwrap()
            .decode()
            .map_err(|e| PyValueError::new_err(format!("malformed HTTP/1 body: {e}")))?
        {
            Some(chunk) => Ok(Some(PyBytes::new(py, &chunk).unbind())),
            None => Ok(None),
        }
    }

    #[getter]
    fn is_complete(&self) -> bool {
        self.inner.lock().unwrap().is_complete()
    }

    /// Drain and return the bytes buffered past the completed body — the start of
    /// the next pipelined request (hyper keeps these in its persistent read
    /// buffer). The server driver carries them into the next request's codec.
    fn take_buffered(&self, py: Python<'_>) -> Py<PyBytes> {
        PyBytes::new(py, &self.inner.lock().unwrap().take_buffered()).unbind()
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
