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
use http::{HeaderMap, Method, StatusCode, Uri, Version};

use http::header::{HeaderValue, CONNECTION};

use super::decode::Decoder;
use super::io::MemRead;
use super::role::{Client, Server};
use super::{Encode, EncodedBuf, Encoder, Http1Transaction, ParseContext};
use crate::body::DecodedLength;
use crate::error::{Header, Kind, Parse, User};
use crate::headers::connection_keep_alive;
use crate::proto::{BodyLength, MessageHead, RequestLine};

// ===== constants and helpers hyper keeps in its (non-vendored) `conn.rs` / `io.rs` =====
//
// Those files are the async connection driver, which httpunk rewrites in Python;
// the byte-level pieces embedded in them are mirrored here, verbatim, so no wire
// literal or framing rule lives on the Python side.

/// The HTTP/2 connection preface (conn.rs L29). An h1 server that fails to parse a
/// head starting with it reports `Parse::VersionH2` and closes without a response.
pub const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// The interim response hyper's server writes for `Expect: 100-continue` when the
/// body is first polled (conn.rs L409).
pub const CONTINUE_RESPONSE: &[u8] = b"HTTP/1.1 100 Continue\r\n\r\n";

/// hyper `io.rs` `DEFAULT_MAX_BUFFER_SIZE` (L23) / `MINIMUM_MAX_BUFFER_SIZE` (L18):
/// the read buffer cap a still-incomplete head may grow to before it is rejected as
/// `Parse::TooLarge`, and the smallest cap `http1::Builder::max_buf_size` accepts.
pub const DEFAULT_MAX_BUFFER_SIZE: usize = 8192 + 4096 * 100;
pub const MINIMUM_MAX_BUFFER_SIZE: usize = 8192;

/// conn.rs `has_h2_prefix` (L206-209): the read buffer starts with the h2 preface.
pub fn has_h2_prefix(read_buf: &[u8]) -> bool {
    read_buf.len() >= 24 && read_buf[..24] == *H2_PREFACE
}

/// The `Parse::TooLarge` error hyper's `io.rs` raises once the read buffer reaches
/// `max_buf_size` without a complete head (L202-207).
pub fn too_large_error() -> crate::Error {
    crate::Error::new_too_large()
}

/// What hyper's server does with a head parse failure — conn.rs `on_parse_error`
/// (L819-835): a buffer holding the h2 preface becomes `Parse::VersionH2` (no
/// response, the connection just closes); otherwise `Server::on_error` picks the
/// automatic response status (400 / 414 / 431, role.rs `on_error`), `None` when
/// hyper answers nothing. Returns the error to raise and that status.
pub fn server_parse_failure(read_buf: &[u8], e: crate::Error) -> (crate::Error, Option<u16>) {
    if has_h2_prefix(read_buf) {
        return (crate::Error::new_version_h2(), None);
    }
    let status = Server::on_error(&e).map(|msg| msg.subject.as_u16());
    (e, status)
}

/// conn.rs `enforce_version` + `fix_keep_alive` (L666-712), applied to an outgoing
/// head before it is encoded. `remote_http10` is hyper's `state.version == HTTP_10`
/// (the peer is known to speak 1.0); `wants_keep_alive` is `state.wants_keep_alive()`
/// (`KA::Disabled` when false). Both roles: a 1.1 head to a 1.0 peer gains
/// `Connection: keep-alive` when it carries no keep-alive token on its FIRST
/// `Connection` line and keep-alive is wanted, and is downgraded to 1.0; a 1.1 peer
/// gets `Connection: close` inserted (replacing the header) once keep-alive is off.
fn enforce_version<S>(head: &mut MessageHead<S>, remote_http10: bool, wants_keep_alive: bool) {
    if remote_http10 {
        // fix_keep_alive: the head is still HTTP/1.1 here (hyper downgrades it below).
        let outgoing_is_keep_alive = head
            .headers
            .get(CONNECTION)
            .is_some_and(connection_keep_alive);
        if !outgoing_is_keep_alive && wants_keep_alive {
            head.headers
                .insert(CONNECTION, HeaderValue::from_static("keep-alive"));
        }
        head.version = Version::HTTP_10;
    } else if !wants_keep_alive {
        head.headers
            .insert(CONNECTION, HeaderValue::from_static("close"));
    }
}

// ===== hyper `Error` classification =====
//
// httpunk mirrors hyper's error taxonomy as Python exception classes, one per
// public `Kind` (`httpunk.exceptions.H1*`). hyper exposes only `is_*` queries
// plus `Display`/`source()`; the Python side needs a stable discriminant for the
// kind and its sub-variant. `Kind` / `Parse` / `User` are `pub(super)` in
// `crate::error` (crate-visible), so this glue reads them here rather than
// patching the verbatim-vendored error.rs. Tags are the snake_case variant names.

/// The top-level `Kind` of a hyper error as a stable tag.
pub fn error_kind(e: &crate::Error) -> &'static str {
    match *e.kind() {
        Kind::Parse(_) => "parse",
        Kind::User(_) => "user",
        Kind::IncompleteMessage => "incomplete_message",
        Kind::UnexpectedMessage => "unexpected_message",
        Kind::Canceled => "canceled",
        Kind::ChannelClosed => "channel_closed",
        Kind::Io => "io",
        Kind::HeaderTimeout => "header_timeout",
        Kind::Body => "body",
        Kind::BodyWrite => "body_write",
        Kind::Shutdown => "shutdown",
    }
}

/// The `Parse` variant as a stable tag (`None` unless `Kind::Parse`). `Header(h)`
/// variants are `header_<h>` so the nesting stays visible.
pub fn error_parse_kind(e: &crate::Error) -> Option<&'static str> {
    let Kind::Parse(ref p) = *e.kind() else {
        return None;
    };
    Some(match *p {
        Parse::Method => "method",
        Parse::Version => "version",
        Parse::VersionH2 => "version_h2",
        Parse::Uri => "uri",
        Parse::UriTooLong => "uri_too_long",
        Parse::Header(Header::Token) => "header_token",
        Parse::Header(Header::ContentLengthInvalid) => "header_content_length_invalid",
        Parse::Header(Header::TransferEncodingInvalid) => "header_transfer_encoding_invalid",
        Parse::Header(Header::TransferEncodingUnexpected) => "header_transfer_encoding_unexpected",
        Parse::TooLarge => "too_large",
        Parse::Status => "status",
        Parse::Internal => "internal",
    })
}

/// The `User` variant as a stable tag (`None` unless `Kind::User`).
pub fn error_user_kind(e: &crate::Error) -> Option<&'static str> {
    let Kind::User(ref u) = *e.kind() else {
        return None;
    };
    Some(match *u {
        User::Body => "body",
        User::BodyWriteAborted => "body_write_aborted",
        User::Service => "service",
        User::UnexpectedHeader => "unexpected_header",
        User::UnsupportedStatusCode => "unsupported_status_code",
        User::NoUpgrade => "no_upgrade",
        User::ManualUpgrade => "manual_upgrade",
        User::DispatchGone => "dispatch_gone",
    })
}

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
    /// The parsed method and request-target as hyper holds them (`RequestLine`):
    /// no string is rendered here — the caller decides what to build from them.
    pub method: Method,
    /// The request-target verbatim: origin-form path+query, absolute-form (proxy),
    /// or authority-form (CONNECT).
    pub target: Uri,
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
    /// The request declared `TE: trailers`, so the response may carry trailers —
    /// hyper conn.rs `read_head`: `allow_trailer_fields = te_is_trailers(headers)`;
    /// without it `write_trailers` drops them.
    pub allow_trailers: bool,
}

/// Any `Connection` header line carries a `close` token — hyper `headers::
/// connection_any_close`, what the client's `encode_head` checks to disable
/// keep-alive up front (1.11.1) and what `Server::encode` treats as `is_last`.
pub fn connection_any_close(headers: &HeaderMap) -> bool {
    crate::headers::connection_any_close(headers)
}

/// Owns the vendored body `Encoder` and frames body chunks, so hyper's body
/// `Encoder` never crosses the crate boundary. Nothing here copies payload bytes:
/// a chunk is framed over a BORROW of the caller's buffer (`FramedChunk`) and
/// written out once, into the caller's destination, by `write_to` — hyper's own
/// shape, where `Encoder::encode` wraps the caller's `Buf` (`BufKind::Exact` /
/// `Chunked`) and the write buffer copies it at most once.
pub struct BodyEncoder(Encoder);

/// One body chunk as hyper's `Encoder` framed it, over a borrowed chunk: the
/// chunked size line + chunk + CRLF, the chunk verbatim (content-length within the
/// declared length, or close-delimited), or the chunk truncated to what the declared
/// Content-Length still allows (`BufKind::Limited`). Payload bytes are touched only
/// by `write_to`.
pub struct FramedChunk<'a> {
    buf: EncodedBuf<&'a [u8]>,
    verbatim: bool,
}

impl FramedChunk<'_> {
    /// Bytes `write_to` will produce.
    pub fn len(&self) -> usize {
        self.buf.remaining()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The framed bytes ARE the chunk (hyper `BufKind::Exact`): the caller can hand
    /// its own buffer on without copying anything.
    pub fn is_verbatim(&self) -> bool {
        self.verbatim
    }

    /// Write the framed chunk into `dst`, which must be exactly `len()` long.
    pub fn write_to(self, dst: &mut [u8]) {
        write_buf(self.buf, dst);
    }
}

/// How a body ends: nothing (content-length / close-delimited), the fixed chunked
/// terminator, or a chunked terminator carrying a trailer block.
pub enum BodyTail {
    None,
    /// `0\r\n\r\n` (`CHUNKED_TERMINATOR`), what hyper's `Encoder::end` emits for a
    /// chunked body — a constant, so the caller may keep one buffer for it.
    Terminator,
    Trailers(EncodedBuf<Bytes>),
}

/// The chunked body terminator hyper's `Encoder::end` emits (encode.rs
/// `BufKind::ChunkedEnd`).
pub const CHUNKED_TERMINATOR: &[u8] = b"0\r\n\r\n";

impl BodyTail {
    pub fn len(&self) -> usize {
        match self {
            BodyTail::None => 0,
            BodyTail::Terminator => CHUNKED_TERMINATOR.len(),
            BodyTail::Trailers(b) => b.remaining(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Write the tail into `dst`, which must be exactly `len()` long.
    pub fn write_to(self, dst: &mut [u8]) {
        match self {
            BodyTail::None => {}
            BodyTail::Terminator => dst.copy_from_slice(CHUNKED_TERMINATOR),
            BodyTail::Trailers(b) => write_buf(b, dst),
        }
    }
}

fn write_buf(mut buf: impl Buf, dst: &mut [u8]) {
    debug_assert_eq!(buf.remaining(), dst.len());
    let mut off = 0;
    while buf.has_remaining() {
        let chunk = buf.chunk();
        let n = chunk.len();
        dst[off..off + n].copy_from_slice(chunk);
        off += n;
        buf.advance(n);
    }
}

impl BodyEncoder {
    /// True when the framing carries no body (a `Content-Length: 0` / bodyless
    /// encoder). hyper's `write_head` uses `Encoder::is_eof()` to skip polling the
    /// body entirely (conn.rs L595-605); the driver does the same so a bodyless
    /// response/request never drains the caller's body iterable.
    pub fn is_eof(&self) -> bool {
        self.0.is_eof()
    }

    /// The connection must close once this message is written — hyper
    /// `Encoder::is_last`: set by `Server::encode` for `Encode.keep_alive == false`, a
    /// response `Connection: close`, a 101, or a 2xx to CONNECT (role.rs L392-411,
    /// L839-843). conn.rs `try_keep_alive` closes on it.
    pub fn is_last(&self) -> bool {
        self.0.is_last()
    }

    /// The body is delimited by closing the connection (an unknown-length HTTP/1.0
    /// response, role.rs `set_length`): never reusable (`Encoder::is_close_delimited`).
    pub fn is_close_delimited(&self) -> bool {
        self.0.is_close_delimited()
    }

    /// The body is `Transfer-Encoding: chunked` (`Encoder::is_chunked`).
    pub fn is_chunked(&self) -> bool {
        self.0.is_chunked()
    }

    /// Frame one non-empty body chunk over a borrow of it: hyper's `Encoder::encode`
    /// with the chunk as the `Buf` — the encoder's Content-Length accounting and its
    /// truncation of an over-long chunk (`BufKind::Limited`) run exactly as upstream's.
    pub fn encode<'a>(&mut self, chunk: &'a [u8]) -> FramedChunk<'a> {
        debug_assert!(!chunk.is_empty(), "encode() called with an empty chunk");
        let chunked = self.0.is_chunked();
        let buf = self.0.encode(chunk);
        // `Exact` is the only kind whose output is the input: a content-length chunk
        // within the declared length, or a close-delimited one. `Limited` is shorter,
        // `Chunked` longer.
        let verbatim = !chunked && buf.remaining() == chunk.len();
        FramedChunk { buf, verbatim }
    }

    /// Finish the body: the chunked terminator, or nothing for a content-length /
    /// close-delimited body. `Err` if a declared Content-Length wasn't filled —
    /// hyper's `end_body` maps the encoder's `NotEof` to
    /// `Error::new_body_write_aborted()` (`User::BodyWriteAborted`, conn.rs
    /// `end_body`) and closes the write side; the driver does the same.
    pub fn end(self) -> Result<BodyTail, crate::Error> {
        match self.0.end::<Bytes>() {
            Ok(Some(buf)) => {
                // encode.rs `end`: the one buffer it returns is `ChunkedEnd(b"0\r\n\r\n")`.
                debug_assert_eq!(buf.remaining(), CHUNKED_TERMINATOR.len());
                Ok(BodyTail::Terminator)
            }
            Ok(None) => Ok(BodyTail::None),
            Err(not_eof) => Err(crate::Error::new_body_write_aborted().with(not_eof)),
        }
    }

    /// Finish a chunked body with trailing headers: the terminating `0\r\n` + the
    /// trailer block + `\r\n`. Only fields declared via
    /// `into_chunked_with_trailing_fields` (the `Trailer` header) are sent (hyper's
    /// `encode_trailers` filters + validates). Falls back to the plain terminator when
    /// no declared trailer survives (or the body isn't chunked).
    pub fn end_with_trailers(self, trailers: HeaderMap) -> Result<BodyTail, crate::Error> {
        match self.0.encode_trailers::<Bytes>(trailers, false) {
            Some(buf) => Ok(BodyTail::Trailers(buf)),
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

/// Encode a request head into `dst` and return the body `BodyEncoder`.
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
    dst: &mut Vec<u8>,
) -> Result<BodyEncoder, crate::Error> {
    // The request-target is serialized *as given* (hyper role.rs L1200 writes
    // `msg.head.subject.1` via its `Display`, "not enforced or validated" —
    // client/conn/http1.rs L194-204): a path-and-query `Uri` yields origin-form
    // (`GET /path`), an absolute `Uri` yields absolute-form (`GET http://…`, for
    // proxies), and an authority `Uri` yields authority-form (`CONNECT host:port`).
    // The caller chooses the form via the target they pass; we do not reduce it.
    // `http10` = the peer answered in HTTP/1.0 earlier on this connection (hyper
    // `state.version`): `enforce_version` then re-asserts keep-alive and downgrades
    // the request line. The client's keep-alive is off only when this request itself
    // says `Connection: close` (conn.rs `encode_head` L619-626, 1.11.1).
    let mut head = MessageHead {
        version: Version::HTTP_11,
        subject: RequestLine(method, uri),
        headers,
        extensions: http::Extensions::new(),
    };
    let wants_keep_alive = !crate::headers::connection_any_close(&head.headers);
    enforce_version(&mut head, http10, wants_keep_alive);
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
    // `dst` is the caller's reusable head buffer (hyper's `WriteBuf.headers`, kept
    // across messages): cleared here, written once; on error hyper rewinds it.
    dst.clear();
    // `Client::encode` reads the request's own `Trailer` header to allow-list the
    // chunked trailer fields the body may emit (role.rs L1420-1431); undeclared
    // fields are dropped by `encode_trailers`, as on the server.
    let encoder = Client::encode(enc, dst)?;
    Ok(BodyEncoder(encoder))
}

/// Parse a response head from `buf`. `Ok(None)` means "need more bytes"; on
/// `Ok(Some(_))` the head has been consumed from `buf` (leftover = body bytes).
/// `req_method` is the method of the request this responds to (bodyless-ness of
/// some responses — e.g. to HEAD — depends on it).
pub fn parse_response(
    buf: &mut BytesMut,
    req_method: &Option<Method>,
) -> Result<Option<ParsedHead>, crate::Error> {
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
    // `Parse` is module-private but converts into the public `crate::Error`, whose
    // `httpunk_*_kind` accessors give the PyO3 layer the variant.
    match Client::parse(buf, ctx)? {
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
/// (leftover = the request body bytes). `max_headers` / `ignore_invalid_headers`
/// are hyper's `http1::Builder::max_headers` (None = hyper's default of 100) and
/// `ignore_invalid_headers` (httparse `ignore_invalid_headers_in_requests`).
pub fn parse_request(
    buf: &mut BytesMut,
    max_headers: Option<usize>,
    ignore_invalid_headers: bool,
) -> Result<Option<ParsedRequest>, crate::Error> {
    if buf.is_empty() {
        return Ok(None);
    }
    let mut cached_headers: Option<HeaderMap> = None;
    let mut method: Option<Method> = None;
    let mut on_informational = None;
    let mut parser_config = httparse::ParserConfig::default();
    parser_config.ignore_invalid_headers_in_requests(ignore_invalid_headers);
    let ctx = ParseContext {
        cached_headers: &mut cached_headers,
        req_method: &mut method,
        h1_parser_config: parser_config,
        h1_max_headers: max_headers,
        preserve_header_case: false,
        h09_responses: false,
        on_informational: &mut on_informational,
    };
    match Server::parse(buf, ctx)? {
        Some(parsed) => {
            let RequestLine(m, uri) = parsed.head.subject;
            Ok(Some(ParsedRequest {
                method: m,
                target: uri,
                allow_trailers: crate::headers::te_is_trailers(&parsed.head.headers),
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

/// Encode a response head into `dst` and return the body `BodyEncoder` (server
/// side; hyper role.rs `Server::encode` L364). `body`: `None` = no body;
/// `Some(Some(n))` = `Content-Length: n`; `Some(None)` = `Transfer-Encoding:
/// chunked`. `req_method` is the method of the request being answered (a response
/// to HEAD, or 204/304, carries no body — `Server::can_have_content_length` L512
/// uses it). `keep_alive` decides whether `Connection: close` is written.
/// `http10` sets the response version to HTTP/1.0 (status line + no-chunked
/// gating — an unknown-length 1.0 body is close-delimited, role.rs L907-910); the
/// driver derives it from the request. `date_header` writes the `Date` header like
/// hyper's server (common/date.rs; `http1::Builder::auto_date_header`, default on);
/// `title_case_headers` is `http1::Builder::title_case_headers`.
pub fn encode_response(
    status: StatusCode,
    headers: HeaderMap,
    body: Option<Option<u64>>,
    req_method: Option<Method>,
    keep_alive: bool,
    http10: bool,
    title_case_headers: bool,
    date_header: bool,
    dst: &mut Vec<u8>,
) -> Result<BodyEncoder, crate::Error> {
    // `keep_alive` is hyper's `wants_keep_alive()` (the request was keep-alive, no
    // graceful shutdown, keep-alive enabled); `http10` its `state.version == HTTP_10`
    // (the request was 1.0). conn.rs `encode_head` runs `enforce_version` over the
    // head first (L628), then `Server::encode` derives `is_last` from the result.
    let mut head = MessageHead {
        version: Version::HTTP_11,
        subject: status,
        headers,
        extensions: http::Extensions::new(),
    };
    enforce_version(&mut head, http10, keep_alive);
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
        title_case_headers,
        date_header,
    };
    // Refresh the per-thread `Date` cache HERE, not only at request parse. hyper refreshes
    // it in `Server::parse` (role.rs L506) and `Server::encode` merely copies it (L976),
    // which is fresh when parse and encode of one exchange run on the same thread. Under
    // a work-stealing runtime (tonio) the head is parsed on one OS thread and the
    // response encoded on another whose cache was last refreshed by ITS last parse —
    // arbitrarily long ago on a quiet server. `update()` is one clock read (the same
    // cost hyper pays per parse), so pay it per response and never emit a stale Date.
    // Runtime-forced divergence from upstream; the h2 path (`date_header_value`) does the same.
    if date_header {
        crate::common::date::update();
    }
    // `dst` is the caller's reusable head buffer (hyper's `WriteBuf.headers`, kept
    // across messages): cleared here, written once; on error hyper rewinds it.
    dst.clear();
    // `Server::encode` fails for the user errors hyper reports on the connection
    // (`User::UnexpectedHeader`, `User::UnsupportedStatusCode`); `dst` is rewound
    // to before the half-pushed head, so nothing of it reaches the wire.
    let encoder = Server::encode(enc, dst)?;
    Ok(BodyEncoder(encoder))
}

/// The current `Date` header value, from hyper's per-thread once-a-second cache
/// (common/date.rs `update` + `extend`) — so the h2 server's `Date` (hyper
/// proto/h2/server.rs L484 `or_insert_with(date::update_and_header_value)`) is the
/// same bytes the h1 encoder writes.
pub fn date_header_value() -> HeaderValue {
    // hyper's h2 server path (proto/h2/server.rs `date::update_and_header_value`):
    // the cached `HeaderValue` beside the cached bytes, refreshed once a second and
    // handed out as a clone (a refcount bump). The vendoring shim un-gates it.
    crate::common::date::update_and_header_value()
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

    /// Take over a buffer of body bytes another Rust buffer already holds (the
    /// bytes that arrived with the head, split off the codec's read buffer): a move
    /// when this decoder's buffer is empty, an append after any it already holds —
    /// `BytesMut::unsplit` — never a copy through a Python object.
    pub fn feed_buf(&mut self, data: BytesMut) {
        self.read.buf.unsplit(data);
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
    pub fn take_buffered(&mut self) -> BytesMut {
        self.read.buf.split()
    }

    /// Bytes buffered past the body (hyper's `read_buf` non-emptiness check,
    /// `require_empty_read` conn.rs L463-465) — without moving them out.
    pub fn buffered(&self) -> usize {
        self.read.buf.len()
    }

    /// One decode step: `Ok(Some(chunk))` = body data; `Ok(None)` = no chunk right
    /// now — end vs. need-more is distinguished by `is_complete()`. `Err` on a
    /// malformed body: the decoder's `io::Error` wrapped as hyper's dispatcher does
    /// (`Error::new_body(e)`, `Kind::Body`) — a truncated body (`UnexpectedEof`)
    /// and a framing error (`InvalidInput` / `InvalidData`) are the same kind,
    /// distinguished by the io kind of the cause. (Trailers terminate the body;
    /// they are captured and available via `take_trailers`.)
    pub fn decode(&mut self) -> Result<Option<Bytes>, crate::Error> {
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
            Poll::Ready(Err(e)) => Err(crate::Error::new_body(e)),
        }
    }
}
