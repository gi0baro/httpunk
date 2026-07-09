//! httpunk's public bridge/facade over the vendored hyper h1 sans-IO codec.
//!
//! This is **not** vendored hyper code. It lives inside this crate (as a child
//! of `proto::h1`) so it can construct the module-private `Encode`/`ParseContext`
//! and unpack `ParsedMessage`, and so it can drive the crate-private `Encoder` —
//! the glue hyper's own `conn.rs` provided, which we do not vendor (that
//! orchestration is Python's). It exposes only `pub` facade types (plain data +
//! `BodyEncoder`) so the PyO3 layer in the main crate never needs hyper's
//! `pub` internals. Re-exported at the crate root (`vendor_hyper`) by
//! lib.rs.

use std::task::{Context, Poll, Waker};

use bytes::{Buf, Bytes, BytesMut};
use http::{HeaderMap, HeaderName, Method, StatusCode, Uri, Version};

use super::decode::Decoder;
use super::io::MemRead;
use super::role::{Client, Server};
use super::{Encode, Encoder, Http1Transaction, ParseContext};
use crate::body::DecodedLength;
use crate::proto::{BodyLength, MessageHead, RequestLine};

/// How a response body is framed on the wire (mapped from hyper's `DecodedLength`).
pub enum BodyDecode {
    /// No body (Content-Length: 0, or a bodyless status / response to HEAD).
    Empty,
    /// `Content-Length: N`.
    Length(u64),
    /// `Transfer-Encoding: chunked`.
    Chunked,
    /// Delimited by connection close (neither length nor chunked).
    CloseDelimited,
}

/// A parsed response head, with everything the Python driver needs.
pub struct ParsedHead {
    pub status: u16,
    pub keep_alive: bool,
    pub headers: HeaderMap,
    pub body: BodyDecode,
    /// The response switches protocols — a 101 upgrade, or a 2xx to a CONNECT
    /// request (a tunnel). The connection is no longer HTTP after the head; the
    /// caller takes over the raw transport (hyper `wants_upgrade` / `Upgraded`).
    pub wants_upgrade: bool,
    /// The response was HTTP/1.0. hyper's client remembers this (`state.version`,
    /// conn.rs L295) and downgrades subsequent requests on the reused connection
    /// to HTTP/1.0 (`enforce_version`, conn.rs L682-702).
    pub http10: bool,
}

/// A parsed request head (server side), with everything the Python driver needs.
pub struct ParsedRequest {
    pub method: String,
    /// The request-target verbatim: origin-form path+query, absolute-form (proxy),
    /// or authority-form (CONNECT).
    pub target: String,
    pub headers: HeaderMap,
    pub body: BodyDecode,
    pub keep_alive: bool,
    /// The client sent `Expect: 100-continue` (the server should send an interim
    /// 100 before reading the body — surfaced for the driver to handle).
    pub expect_continue: bool,
    /// A CONNECT request or an Upgrade — the connection becomes a tunnel.
    pub wants_upgrade: bool,
    /// The request was HTTP/1.0 (vs 1.1). The driver must reflect this in the
    /// response version — hyper `enforce_version`/`fix_keep_alive` (conn.rs): a
    /// 1.0 response defaults to close and cannot use chunked framing.
    pub http10: bool,
}

/// Owns the vendored body `Encoder` and frames request-body chunks into bytes,
/// so hyper's body `Encoder` never crosses the crate boundary.
pub struct BodyEncoder(Encoder);

impl BodyEncoder {
    /// True when the framing carries no body (a `Content-Length: 0` / bodyless
    /// encoder). hyper's `write_head` uses `Encoder::is_eof()` to skip polling the
    /// body entirely (conn.rs L595-605); the driver does the same so a bodyless
    /// response/request never drains the caller's body iterable.
    pub fn is_eof(&self) -> bool {
        self.0.is_eof()
    }

    /// Frame one body chunk (chunked size-prefix/CRLF, or raw for content-length).
    pub fn encode(&mut self, chunk: &[u8]) -> Vec<u8> {
        if chunk.is_empty() {
            return Vec::new();
        }
        let mut buf = self.0.encode(Bytes::copy_from_slice(chunk));
        let n = buf.remaining();
        buf.copy_to_bytes(n).to_vec()
    }

    /// Finish the body: chunked terminator `0\r\n\r\n`, or empty for a
    /// content-length body. `Err` if a declared Content-Length wasn't filled.
    pub fn end(self) -> Result<Vec<u8>, &'static str> {
        match self.0.end::<Bytes>() {
            Ok(Some(mut buf)) => {
                let n = buf.remaining();
                Ok(buf.copy_to_bytes(n).to_vec())
            }
            Ok(None) => Ok(Vec::new()),
            Err(_) => Err("request body shorter than declared Content-Length"),
        }
    }

    /// Finish a chunked body with trailing headers: emits the terminating `0\r\n` +
    /// the trailer block + `\r\n`. Only fields declared via
    /// `into_chunked_with_trailing_fields` (the `Trailer` header) are sent (hyper's
    /// `encode_trailers` filters + validates). Falls back to the plain terminator when
    /// no declared trailer survives (or the body isn't chunked).
    pub fn end_with_trailers(self, trailers: HeaderMap) -> Result<Vec<u8>, &'static str> {
        match self.0.encode_trailers::<Bytes>(trailers, false) {
            Some(mut buf) => {
                let n = buf.remaining();
                Ok(buf.copy_to_bytes(n).to_vec())
            }
            None => self.end(),
        }
    }
}

fn map_body(d: DecodedLength) -> BodyDecode {
    if d == DecodedLength::CHUNKED {
        BodyDecode::Chunked
    } else if d == DecodedLength::CLOSE_DELIMITED {
        BodyDecode::CloseDelimited
    } else {
        match d.into_opt() {
            Some(n) if n > 0 => BodyDecode::Length(n),
            _ => BodyDecode::Empty,
        }
    }
}

/// Encode a request head into bytes and return the body `BodyEncoder`.
///
/// `body`: `None` = no body; `Some(Some(n))` = `Content-Length: n`; `Some(None)`
/// = `Transfer-Encoding: chunked`. (hyper's `set_length` injects the matching
/// header and returns the framing `Encoder`.)
pub fn encode_request(
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Option<Option<u64>>,
    http10: bool,
    trailer_fields: Vec<HeaderName>,
) -> Result<(Vec<u8>, BodyEncoder), String> {
    // The request-target is serialized *as given* (hyper role.rs L1200 writes
    // `msg.head.subject.1` via its `Display`, "not enforced or validated" —
    // client/conn/http1.rs L194-204): a path-and-query `Uri` yields origin-form
    // (`GET /path`), an absolute `Uri` yields absolute-form (`GET http://…`, for
    // proxies), and an authority `Uri` yields authority-form (`CONNECT host:port`).
    // The caller chooses the form via the target they pass; we do not reduce it.
    // `http10` downgrades the request line to HTTP/1.0 — the driver sets it once
    // it has seen a 1.0 response on the connection (hyper `enforce_version`).
    let mut head = MessageHead {
        version: if http10 {
            Version::HTTP_10
        } else {
            Version::HTTP_11
        },
        subject: RequestLine(method, uri),
        headers,
        extensions: http::Extensions::new(),
    };
    let body_len = match body {
        None => None,
        Some(Some(n)) => Some(BodyLength::Known(n)),
        Some(None) => Some(BodyLength::Unknown),
    };
    let mut req_method = None;
    let enc = Encode {
        head: &mut head,
        body: body_len,
        // `keep_alive`/`date_header` are server-only fields (compiled in now that
        // the `server` feature is on); `Client::encode` ignores them.
        keep_alive: true,
        req_method: &mut req_method,
        title_case_headers: false,
        date_header: false,
    };
    let mut dst = Vec::new();
    let encoder = Client::encode(enc, &mut dst).map_err(|e| format!("{e}"))?;
    // Declare the chunked trailer fields (from the request's `Trailer` header) so the
    // body may later emit them via `end_with_trailers`. A no-op unless the body is
    // chunked (hyper `Encoder::into_chunked_with_trailing_fields`).
    let encoder = if trailer_fields.is_empty() {
        encoder
    } else {
        encoder.into_chunked_with_trailing_fields(trailer_fields)
    };
    Ok((dst, BodyEncoder(encoder)))
}

/// Parse a response head from `buf`. `Ok(None)` means "need more bytes"; on
/// `Ok(Some(_))` the head has been consumed from `buf` (leftover = body bytes).
/// `req_method` is the method of the request this responds to (bodyless-ness of
/// some responses — e.g. to HEAD — depends on it).
pub fn parse_response(
    buf: &mut BytesMut,
    req_method: &Option<Method>,
) -> Result<Option<ParsedHead>, String> {
    if buf.is_empty() {
        return Ok(None);
    }
    let mut cached_headers: Option<HeaderMap> = None;
    let mut method = req_method.clone();
    let mut on_informational = None;
    let ctx = ParseContext {
        cached_headers: &mut cached_headers,
        req_method: &mut method,
        h1_parser_config: httparse::ParserConfig::default(),
        h1_max_headers: None,
        preserve_header_case: false,
        h09_responses: false,
        on_informational: &mut on_informational,
    };
    // hyper's `Parse` error type is module-private, so map it to a String here
    // (the caller in src/py can't name it).
    match Client::parse(buf, ctx).map_err(|e| format!("{e:?}"))? {
        Some(parsed) => Ok(Some(ParsedHead {
            status: parsed.head.subject.as_u16(),
            keep_alive: parsed.keep_alive,
            body: map_body(parsed.decode),
            wants_upgrade: parsed.wants_upgrade,
            http10: parsed.head.version == Version::HTTP_10,
            headers: parsed.head.headers,
        })),
        None => Ok(None),
    }
}

/// Parse a request head from `buf` (server side; hyper role.rs `Server::parse`
/// L137). `Ok(None)` = need more bytes; on `Ok(Some(_))` the head is consumed
/// (leftover = the request body bytes).
pub fn parse_request(buf: &mut BytesMut) -> Result<Option<ParsedRequest>, String> {
    if buf.is_empty() {
        return Ok(None);
    }
    let mut cached_headers: Option<HeaderMap> = None;
    let mut method: Option<Method> = None;
    let mut on_informational = None;
    let ctx = ParseContext {
        cached_headers: &mut cached_headers,
        req_method: &mut method,
        h1_parser_config: httparse::ParserConfig::default(),
        h1_max_headers: None,
        preserve_header_case: false,
        h09_responses: false,
        on_informational: &mut on_informational,
    };
    match Server::parse(buf, ctx).map_err(|e| format!("{e:?}"))? {
        Some(parsed) => {
            let RequestLine(m, uri) = parsed.head.subject;
            Ok(Some(ParsedRequest {
                method: m.to_string(),
                target: uri.to_string(),
                headers: parsed.head.headers,
                body: map_body(parsed.decode),
                keep_alive: parsed.keep_alive,
                expect_continue: parsed.expect_continue,
                wants_upgrade: parsed.wants_upgrade,
                http10: parsed.head.version == Version::HTTP_10,
            }))
        }
        None => Ok(None),
    }
}

/// Encode a response head into bytes and return the body `BodyEncoder` (server
/// side; hyper role.rs `Server::encode` L364). `body`: `None` = no body;
/// `Some(Some(n))` = `Content-Length: n`; `Some(None)` = `Transfer-Encoding:
/// chunked`. `req_method` is the method of the request being answered (a response
/// to HEAD, or 204/304, carries no body — `Server::can_have_content_length` L512
/// uses it). `keep_alive` decides whether `Connection: close` is written.
/// `http10` sets the response version to HTTP/1.0 (status line + no-chunked
/// gating — an unknown-length 1.0 body is close-delimited, role.rs L907-910); the
/// driver derives it from the request. The `Date` header is written like hyper's
/// server (common/date.rs).
pub fn encode_response(
    status: u16,
    headers: HeaderMap,
    body: Option<Option<u64>>,
    req_method: Option<Method>,
    keep_alive: bool,
    http10: bool,
) -> Result<(Vec<u8>, BodyEncoder), String> {
    let status = StatusCode::from_u16(status).map_err(|e| format!("{e}"))?;
    let mut head = MessageHead {
        version: if http10 {
            Version::HTTP_10
        } else {
            Version::HTTP_11
        },
        subject: status,
        headers,
        extensions: http::Extensions::new(),
    };
    let body_len = match body {
        None => None,
        Some(Some(n)) => Some(BodyLength::Known(n)),
        Some(None) => Some(BodyLength::Unknown),
    };
    let mut req_method = req_method;
    let enc = Encode {
        head: &mut head,
        body: body_len,
        keep_alive,
        req_method: &mut req_method,
        title_case_headers: false,
        date_header: true,
    };
    let mut dst = Vec::new();
    let encoder = Server::encode(enc, &mut dst).map_err(|e| format!("{e}"))?;
    Ok((dst, BodyEncoder(encoder)))
}

/// A synchronous `MemRead` over an in-memory buffer, so the vendored `Decoder`
/// (written Poll-first over hyper's async `MemRead`) can be driven sans-IO:
/// data present -> `Ready(bytes)`; empty and not yet EOF -> `Pending` (which the
/// facade reads as "need more"); empty at EOF -> `Ready(empty)` (hyper's decoder
/// treats an empty read as end-of-transport).
struct SyncMemRead {
    buf: BytesMut,
    eof: bool,
}

impl MemRead for SyncMemRead {
    fn read_mem(&mut self, _cx: &mut Context<'_>, len: usize) -> Poll<std::io::Result<Bytes>> {
        if !self.buf.is_empty() {
            let n = len.min(self.buf.len());
            Poll::Ready(Ok(self.buf.split_to(n).freeze()))
        } else if self.eof {
            Poll::Ready(Ok(Bytes::new()))
        } else {
            Poll::Pending
        }
    }
}

/// Drives the vendored hyper body `Decoder` synchronously (the sans-IO body
/// decoder for content-length / chunked / close-delimited responses). Feed body
/// bytes with `feed`, `mark_eof` on transport close, then pull chunks with
/// `decode`. Because the vendored decoder is pure (it only threads `cx` to
/// `read_mem`, never touching the waker), a no-op `Waker` is sound.
pub struct BodyDecoder {
    decoder: Decoder,
    read: SyncMemRead,
    done: bool,
    trailers: Option<HeaderMap>,
}

impl BodyDecoder {
    /// `kind`: "empty" | "length" | "chunked" | "close"; `length` is the
    /// Content-Length for "length".
    pub fn new(kind: &str, length: u64) -> BodyDecoder {
        let decoder = match kind {
            "length" => Decoder::length(length),
            "chunked" => Decoder::chunked(None, None),
            "close" => Decoder::eof(),
            _ => Decoder::length(0), // "empty"
        };
        // A zero-length body (Content-Length: 0, or a bodyless status) is already
        // at EOF, so the decoder is complete before any bytes arrive — otherwise
        // `is_complete()` would stay false until a decode() call, and a caller
        // that never reads a bodyless response would never release the connection.
        let done = decoder.is_eof();
        BodyDecoder {
            decoder,
            read: SyncMemRead {
                buf: BytesMut::new(),
                eof: false,
            },
            done,
            trailers: None,
        }
    }

    /// The trailing headers (chunked trailers) once the body is complete, if the
    /// peer sent any; taken (moved out) so the caller owns them.
    pub fn take_trailers(&mut self) -> Option<HeaderMap> {
        self.trailers.take()
    }

    pub fn feed(&mut self, data: &[u8]) {
        self.read.buf.extend_from_slice(data);
    }

    pub fn mark_eof(&mut self) {
        self.read.eof = true;
    }

    pub fn is_complete(&self) -> bool {
        self.done
    }

    /// Drain and return the bytes still buffered past the completed body — the
    /// start of the next pipelined request. hyper keeps these in its persistent
    /// connection read buffer; the sans-IO facade hands them back so the driver
    /// can carry them into the next request's codec (else a pipelined request is
    /// lost and the connection deadlocks). Empty if none buffered.
    pub fn take_buffered(&mut self) -> Vec<u8> {
        self.read.buf.split().to_vec()
    }

    /// One decode step: `Ok(Some(chunk))` = body data; `Ok(None)` = no chunk right
    /// now — end vs. need-more is distinguished by `is_complete()`. `Err` on a
    /// malformed body. (Trailers terminate the body; they are captured and
    /// available via `take_trailers`.)
    pub fn decode(&mut self) -> Result<Option<Bytes>, String> {
        if self.done {
            return Ok(None);
        }
        let mut cx = Context::from_waker(Waker::noop());
        match self.decoder.decode(&mut cx, &mut self.read) {
            Poll::Pending => Ok(None), // need more bytes
            Poll::Ready(Ok(frame)) => {
                if frame.is_data() {
                    let data = frame.into_data().unwrap_or_default();
                    if data.is_empty() {
                        self.done = true; // end of body
                        Ok(None)
                    } else {
                        // A length body reaches EOF on the very frame that consumes
                        // its last byte (remaining == 0). Mirror hyper's
                        // `poll_read_body`, which checks `decoder.is_eof()` right after
                        // the data frame and transitions to `KeepAlive` — rather than
                        // waiting for a follow-up empty decode. Without this,
                        // `is_complete()` lags one `decode()` behind, so a single-poll
                        // drain of a fully-buffered length body sees the data but not
                        // completion and needlessly closes the connection.
                        self.done = self.decoder.is_eof();
                        Ok(Some(data))
                    }
                } else {
                    // A trailers frame ends the body; capture the trailing
                    // headers so the caller can surface them (h2/hyper deliver
                    // them as `Frame::trailers`).
                    self.trailers = frame.into_trailers().ok();
                    self.done = true;
                    Ok(None)
                }
            }
            Poll::Ready(Err(e)) => Err(e.to_string()),
        }
    }
}
