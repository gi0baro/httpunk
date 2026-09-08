"""Type stubs for the `httpunk._httpunk` Rust extension module.

Hand-maintained to match the PyO3 surface in `src/py/**`. Everything here is
implemented in Rust: the `http`-crate `HeaderMap`, the sans-IO HTTP/1 and HTTP/2
codecs + their frame/head events, the vendored h2 stream-state and flow-control
wrappers, the HTTP/1 and HTTP/2 connection states (`H1ServerState`,
`H1ClientState`, `H2Streams` + its verdicts, `OnceLatch`), the error
taxonomy, and the `H2Reason` code enum.

All classes are `frozen` (immutable identity, internally `Mutex`-guarded) and safe
to share across the runtime's worker threads. Getter attributes are read-only.
"""

from collections.abc import Iterable, Iterator, Mapping
from typing import TypeVar, overload

__version__: str

_T = TypeVar("_T")

# Header names/values accept `str` or `bytes` on input; names come back as
# lowercase `str`, values as `bytes` (validated by the `http` crate).
HeaderNameLike = str | bytes
HeaderValueLike = str | bytes

# ===========================================================================
# Errors  (protocol-neutral root + ConnectionClosedError in src/errors.rs; the H1*
# errors in src/h1/errors.rs; the H2* protocol errors in src/h2/errors.rs)
# ===========================================================================

class HTTPunkError(Exception):
    """Base class for every httpunk error (HTTP/1 and HTTP/2)."""

class H2Error(HTTPunkError):
    """Base class for every httpunk HTTP/2 protocol error."""

class H2ProtocolError(H2Error):
    """Connection-level protocol violation (-> GOAWAY). args = (reason: int | None, message: str)."""

class H2StreamError(H2Error):
    """Stream-level protocol violation (-> RST_STREAM; the connection survives).
    args = (stream_id: int, reason: int, initiator: str)."""

class H2UserError(H2Error):
    """Local API misuse from the h2 state machine. args = (kind: str, message: str)."""

class H2FlowControlError(H2Error):
    """Flow-control window over/underflow. args = (reason: int,)."""

class ConnectionClosedError(HTTPunkError):
    """The transport failed (reset / IO error) with work still in flight — hyper's
    `Kind::Io`, h2's `Error::Io`: a transport failure, not a protocol violation (so
    no GOAWAY) and not the HTTP state noticing an EOF (HTTP/1: `H1IncompleteMessageError`,
    `H1BodyError`). Protocol-neutral: raised on both HTTP/1 and HTTP/2, hence it sits
    under HTTPunkError."""

class H1Error(HTTPunkError):
    """Base class for every httpunk HTTP/1 error — one subclass per public hyper
    `Error` kind the h1 codec can produce. Messages are hyper's `Display` text."""

class H1ParseError(H1Error):
    """A malformed HTTP/1 message head (hyper `Kind::Parse`). args = (kind: str, message: str);
    `kind` is the hyper `Parse` variant: `method`, `version`, `version_h2`, `uri`,
    `uri_too_long`, `header_token`, `header_content_length_invalid`,
    `header_transfer_encoding_invalid`, `header_transfer_encoding_unexpected`,
    `too_large`, `status`, `internal`."""

class H1BodyError(H1Error):
    """A body that could not be decoded (hyper `Kind::Body`): a framing error, or the
    transport closing mid-body. args = (io_kind: str, message: str); `io_kind` is the
    io kind of hyper's cause: `unexpected_eof` = truncated; `invalid_input` /
    `invalid_data` = malformed chunk framing."""

class H1IncompleteMessageError(H1Error):
    """The connection closed while a message was still expected (hyper
    `Kind::IncompleteMessage`): EOF before the response head, or the client closing
    its side while the server's response is in flight. args = (message: str,)."""

class H1UnexpectedMessageError(H1Error):
    """Bytes on an idle client connection (hyper `Kind::UnexpectedMessage`): the
    connection is poisoned; the next `request()` raises it with `request_unsent = True`.
    args = (message: str,)."""

class H1UserError(H1Error):
    """Local misuse hyper reports on the wire path (hyper `Kind::User`), closing the
    connection. args = (kind: str, message: str); `kind`: `body_write_aborted`,
    `unexpected_header`, `unsupported_status_code`."""

# ===========================================================================
# HeaderMap  (src/py/http/mod.rs)
# ===========================================================================

class HeaderMap:
    """An ordered, case-insensitive, multi-valued header collection (the `http`
    crate's `HeaderMap`). Names normalize to lowercase `str`; values are `bytes`."""

    def __init__(
        self,
        init: HeaderMap
        | Mapping[HeaderNameLike, HeaderValueLike]
        | Iterable[tuple[HeaderNameLike, HeaderValueLike]]
        | None = ...,
    ) -> None: ...
    def __getitem__(self, name: HeaderNameLike) -> bytes: ...
    @overload
    def get(self, name: HeaderNameLike) -> bytes | None: ...
    @overload
    def get(self, name: HeaderNameLike, default: _T) -> bytes | _T: ...
    def get_all(self, name: HeaderNameLike) -> list[bytes]: ...
    def add(self, name: HeaderNameLike, value: HeaderValueLike) -> None:
        """Append a value for `name`, keeping any existing ones (multi-valued)."""

    def __setitem__(self, name: HeaderNameLike, value: HeaderValueLike) -> None: ...
    def __delitem__(self, name: HeaderNameLike) -> None: ...
    def setdefault(self, name: HeaderNameLike, value: HeaderValueLike) -> bytes: ...
    def __contains__(self, name: HeaderNameLike) -> bool: ...
    def keys(self) -> list[str]:
        """Distinct names, in iteration order."""

    def values(self) -> list[bytes]:
        """Every value, in order (duplicates included)."""

    def items(self) -> list[tuple[str, bytes]]:
        """Every `(name, value)` pair, in order (duplicates included)."""

    def raw_items(self) -> list[tuple[bytes, bytes]]:
        """Every `(name, value)` pair with the name as raw `bytes` (already lowercase
        ASCII), in order, duplicates included — the exact shape ASGI servers want for a
        scope's `headers`, in one boundary crossing with no per-name re-encoding."""

    def __iter__(self) -> Iterator[str]: ...
    def __len__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    def __repr__(self) -> str: ...

# ===========================================================================
# HTTP/1 codec  (src/py/h1/codec.rs)
# ===========================================================================

def uri_parts(url: str) -> tuple[str | None, str | None, int | None, str | None]:
    """`(scheme, host, port, authority)` of `url` via the `http` crate's `Uri` (hyper's
    parser): `host` is the host to dial (an IPv6 literal without brackets); `port` is
    the explicit one or the scheme's default (http/ws 80, https/wss 443); `authority`
    is `Uri::host` (brackets kept) + `:port`. Raises `ValueError` on an unparsable URL."""

def http_date() -> bytes:
    """The current `Date` header value (IMF-fixdate) from hyper's cached per-second clock."""

class H1Codec:
    """A synchronous, zero-I/O HTTP/1 codec over the vendored hyper h1 core.
    Drives head parse/encode + body-frame encode for one request/response."""

    def __init__(
        self,
        *,
        max_headers: int | None = ...,
        ignore_invalid_headers: bool = ...,
        title_case_headers: bool = ...,
        date_header: bool = ...,
        max_buf_size: int = ...,
    ) -> None:
        """Options (hyper `server::conn::http1::Builder`): `max_headers` (None = hyper's
        100), `ignore_invalid_headers`, `title_case_headers`, `date_header`
        (`auto_date_header`) — server role only; `max_buf_size` (hyper io.rs: the read
        buffer cap a still-incomplete head may reach before `Parse::TooLarge`, default
        8192 + 4096 * 100, minimum 8192 — raises `ValueError` below it) — both roles.
        One codec per connection: see `reset`."""

    def reset(self) -> None:
        """Start the next message on this connection: drop the per-message state, keep
        the read buffer (hyper's `read_buf` persists across messages)."""

    def feed(self, data: bytes) -> None:
        """Append received bytes to the read buffer without parsing — bytes another
        reader took past a message (a body decoder's leftover, a watcher's read)."""

    @staticmethod
    def continue_response() -> bytes:
        """`HTTP/1.1 100 Continue\r\n\r\n` — hyper conn.rs L409."""

    def serialize_request(
        self,
        method: str,
        url: str,
        headers: HeaderMap | None = ...,
        *,
        http10: bool = ...,
        content_length: int | None = ...,
        chunked: bool = ...,
    ) -> bytes:
        """Serialize a request head (request line + headers); retains the body
        encoder for `serialize_data`/`serialize_end`/`serialize_trailers`. `http10`:
        the peer is known to speak HTTP/1.0 (hyper `enforce_version` / `fix_keep_alive`
        run first: keep-alive re-asserted, request line downgraded). Chunked trailers
        are allow-listed from the request's own `Trailer` header (hyper `Client::encode`)."""

    @property
    def request_connection_close(self) -> bool:
        """The last `serialize_request` carried `Connection: close` on any line (hyper
        `connection_any_close`): never reuse the connection, whatever the response says."""

    @staticmethod
    def connection_close(headers: HeaderMap) -> bool:
        """Any `Connection` line of `headers` carries a `close` token (hyper
        `headers::connection_any_close`)."""

    def serialize_response(
        self,
        status: int,
        headers: HeaderMap | None = ...,
        *,
        keep_alive: bool = ...,
        http10: bool = ...,
        content_length: int | None = ...,
        chunked: bool = ...,
    ) -> bytes:
        """Serialize a response head (server side; writes a `Date` header, and
        suppresses the body for HEAD/204/304 via the recorded request method)."""

    def serialize_data(self, chunk: bytes) -> bytes:
        """Frame one body chunk (chunked prefix/CRLF, or raw for content-length)."""

    def serialize_end(self) -> bytes:
        """Finish the body: the chunked terminator, or empty for content-length."""

    def serialize_head_and_body(self, head: bytes, body: bytes | None = ..., trailers: HeaderMap | None = ...) -> bytes:
        """One message in one buffer: `head` + the framed `body` (if any) + its end
        (`trailers`, else the bare terminator) — hyper's `WriteBuf` flatten for a small
        immediate body. A bodyless framing (`body_is_eof`) writes no body."""

    def serialize_trailers(self, trailers: HeaderMap) -> bytes:
        """Finish a chunked body with a trailer block (the fields the message's own
        `Trailer` header declared; on the server only if the request said `TE: trailers`,
        else the bare terminator)
        instead of a bare terminator; falls back to `serialize_end` if none apply."""

    def body_is_eof(self) -> bool:
        """True when the in-flight framing carries no body (bodyless response, or
        a zero-length request) — the driver skips polling the caller's body."""

    def receive_head(self, data: bytes) -> H1ResponseHead | None:
        """Feed received bytes (client side); return the response head once a full
        one is available, else None. Leftover bytes are the start of the body."""

    def receive_request_head(self, data: bytes) -> H1RequestHead | None:
        """Feed received bytes (server side); return the request head once a full
        one is available, else None."""

    @property
    def response_is_last(self) -> bool:
        """hyper `Encoder::is_last` of the last `serialize_response`: the connection
        closes after this response (keep-alive off, a response `Connection: close`,
        a 101, or a 2xx to CONNECT)."""

    @property
    def response_close_delimited(self) -> bool:
        """hyper `Encoder::is_close_delimited` of the last `serialize_response`: an
        unknown-length HTTP/1.0 body, ended by closing the connection."""

    @property
    def parse_error_status(self) -> int | None:
        """After a failed `receive_request_head`: the automatic response status hyper's
        `Server::on_error` picks (400 / 414 / 431), or None when it answers nothing
        and just closes (an HTTP/2 preface: `Parse::VersionH2`)."""

    def take_body(self) -> bytes:
        """Drain the bytes buffered after the head (the body bytes already read)."""

    def buffered(self) -> int:
        """Number of bytes currently buffered (unparsed head, or post-head body)."""

class H1ResponseHead:
    """A parsed HTTP/1 response head (produced by `H1Codec.receive_head`)."""

    status: int
    keep_alive: bool
    headers: HeaderMap
    body_kind: str  # "empty" | "length" | "chunked" | "close"
    content_length: int | None
    is_upgrade: bool  # 101 upgrade, or 2xx to CONNECT — the connection becomes a tunnel
    http10: bool
    def __repr__(self) -> str: ...

class H1RequestHead:
    """A parsed HTTP/1 request head (produced by `H1Codec.receive_request_head`)."""

    method: str
    target: str  # request-target verbatim (origin/absolute/authority form)
    keep_alive: bool
    headers: HeaderMap
    body_kind: str  # "empty" | "length" | "chunked" | "close"
    content_length: int | None
    expect_continue: bool  # client sent `Expect: 100-continue`
    is_upgrade: bool  # CONNECT / Upgrade
    http10: bool
    allow_trailers: bool  # the request declared `TE: trailers` (hyper `te_is_trailers`)
    def __repr__(self) -> str: ...

class H1BodyDecoder:
    """A synchronous HTTP/1 body decoder (content-length / chunked / close-
    delimited) over the vendored hyper `Decoder`."""

    def __init__(self, kind: str, length: int = ...) -> None:
        """`kind`: "empty" | "length" | "chunked" | "close"; `length` is the
        Content-Length when `kind == "length"`."""

    def feed(self, data: bytes) -> None:
        """Append received body bytes."""

    def mark_eof(self) -> None:
        """Signal that the transport closed (close-delimited bodies end here)."""

    def decode(self) -> bytes | None:
        """Pull one body chunk: `bytes` if available, else None — end vs. need-more
        is distinguished by `is_complete`."""

    @property
    def is_complete(self) -> bool: ...
    def take_trailers(self) -> HeaderMap | None:
        """The chunked trailers once the body is complete, if any; taken (moved)."""

    @property
    def buffered(self) -> int:
        """Bytes buffered past the body, without moving them (hyper's
        `!read_buf().is_empty()`, `require_empty_read`)."""

    def take_buffered(self) -> bytes:
        """Bytes buffered past the completed body (the start of the next pipelined
        request) — carried into the next codec / used to reject stray bytes."""

# ===========================================================================
# HTTP/2 frame events  (src/py/h2/codec.rs — produced by `H2Codec.receive`)
# ===========================================================================

class H2FrameHeaders:
    stream_id: int
    end_stream: bool
    end_headers: bool
    method: str | None
    scheme: str | None
    authority: str | None
    path: str | None
    status: int | None
    headers: HeaderMap
    content_length: int | None  # the first `content-length`, via h2 `frame::parse_u64`; None if absent/unparsable
    content_length_invalid: bool  # a `content-length` was present but did not parse (h2: stream PROTOCOL_ERROR)
    is_informational: bool  # a 1xx response head (h2 `Headers::is_informational`)
    def __repr__(self) -> str: ...

class H2FrameData:
    stream_id: int
    end_stream: bool
    data: bytes
    flow_controlled_len: int  # payload + padding + the pad-length byte (what flow control charges)
    padding: int  # `flow_controlled_len - len(data)`: the overhead the app never sees (0 when unpadded)
    def __repr__(self) -> str: ...

class H2FrameSettings:
    ack: bool
    header_table_size: int | None
    enable_push: bool | None
    max_concurrent_streams: int | None
    initial_window_size: int | None
    max_frame_size: int | None
    max_header_list_size: int | None
    def __repr__(self) -> str: ...

class H2FrameWindowUpdate:
    stream_id: int  # 0 for a connection-level update
    increment: int

class H2FramePing:
    ack: bool
    data: bytes  # 8-byte opaque payload

class H2FrameGoAway:
    last_stream_id: int
    error_code: int
    debug_data: bytes

class H2FrameRstStream:
    stream_id: int
    error_code: int

class H2FramePriority:
    stream_id: int

class H2FrameStreamError:
    """A stream-level protocol error detected while decoding — surfaced as an
    event (not raised) so frames decoded earlier in the same batch survive. The
    driver RSTs `stream_id` with `error_code` and keeps the connection alive."""

    stream_id: int
    error_code: int

# The event union yielded by `H2Codec.receive`.
H2Frame = (
    H2FrameHeaders
    | H2FrameData
    | H2FrameSettings
    | H2FrameWindowUpdate
    | H2FramePing
    | H2FrameGoAway
    | H2FrameRstStream
    | H2FramePriority
    | H2FrameStreamError
)

# ===========================================================================
# HTTP/2 codec  (src/py/h2/codec.rs)
# ===========================================================================

class H2Codec:
    """A synchronous, zero-I/O HTTP/2 frame reader/serializer over the vendored
    `vendor_h2::{frame, hpack}`. `receive` decodes wire bytes into frame events;
    the `serialize_*` methods produce wire bytes."""

    role_client: bool

    def __init__(self, role: str = ...) -> None:
        """`role`: "client" or "server"."""

    def receive(self, data: bytes) -> list[H2Frame]:
        """Feed received bytes; return the frame events now fully decoded (a
        HEADERS block spanning CONTINUATION frames yields one event when complete).
        Raises on a connection-level protocol error; stream-level errors surface as
        `H2FrameStreamError` events."""

    def buffered(self) -> int: ...
    def set_send_header_table_size(self, val: int) -> None: ...
    def set_recv_header_table_size(self, val: int) -> None: ...
    def set_max_recv_frame_size(self, val: int) -> None: ...
    def set_max_header_list_size(self, val: int) -> None: ...
    def set_send_max_frame_size(self, val: int) -> None: ...
    def serialize_settings(
        self,
        *,
        header_table_size: int | None = ...,
        enable_push: bool | None = ...,
        max_concurrent_streams: int | None = ...,
        initial_window_size: int | None = ...,
        max_frame_size: int | None = ...,
        max_header_list_size: int | None = ...,
    ) -> bytes: ...
    def serialize_settings_ack(self) -> bytes: ...
    def serialize_request_headers(
        self,
        stream_id: int,
        method: str,
        target: str,
        headers: HeaderMap | None = ...,
        end_stream: bool = ...,
        *,
        scheme: str | None = ...,
        authority: str | None = ...,
    ) -> bytes:
        """`target` as `http::Uri` sees it: absolute-form carries its own scheme +
        authority; a bare path takes `scheme`/`authority` (raises `ValueError` when
        those are missing)."""

    def serialize_response_headers(
        self,
        stream_id: int,
        status: int,
        headers: HeaderMap | None = ...,
        end_stream: bool = ...,
        auto_date: bool = ...,
    ) -> bytes:
        """`auto_date`: insert `date` when absent (hyper proto/h2/server.rs L484)."""

    def feed_preface(self, data: bytes) -> bool | None:
        """Server side: feed bytes of the client connection preface. None = matches so
        far but incomplete; True = matched (bytes past it are in the frame buffer —
        `receive(b"")` decodes them); False = mismatch (PROTOCOL_ERROR)."""

    @staticmethod
    def match_preface(data: bytes) -> bool | None:
        """Classify a connection's first bytes against the client preface (hyper-util
        `read_version`): None = incomplete prefix, True = the full preface, False = diverged."""

    @staticmethod
    def check_send_headers(headers: HeaderMap) -> None:
        """h2 send.rs `check_headers`: raises `H2UserError(kind="malformed_headers")` for a
        connection-specific field or a `te` other than exactly `trailers`."""

    @staticmethod
    def method_is_head(method: str) -> bool:
        """`method == Method::HEAD` on the parsed `http::Method` (case-sensitive)."""

    @staticmethod
    def is_client_initiated(stream_id: int) -> bool:
        """h2 `StreamId::is_client_initiated` (odd ids)."""

    def serialize_trailers(self, stream_id: int, trailers: HeaderMap) -> bytes:
        """A trailing HEADERS frame (no pseudo-headers, END_STREAM) — request/response
        trailers after the DATA frames."""
    def serialize_data(self, stream_id: int, data: bytes, end_stream: bool = ...) -> bytes: ...
    def serialize_window_update(self, stream_id: int, increment: int) -> bytes: ...
    def serialize_ping(self, payload: bytes) -> bytes:
        """A PING frame with an 8-byte opaque payload."""

    def serialize_ping_ack(self, payload: bytes) -> bytes:
        """A PING ACK echoing the peer's 8-byte payload."""

    def serialize_go_away(self, last_stream_id: int, error_code: int, debug_data: bytes | None = ...) -> bytes: ...
    def serialize_rst_stream(self, stream_id: int, error_code: int) -> bytes: ...

# ===========================================================================
# HTTP/2 stream state + flow control  (src/py/h2/streams.rs — vendored h2 core)
# ===========================================================================

class H2StreamState:
    """The h2 per-stream state machine (`vendor_h2::proto::streams::State`),
    driven by the Python streams manager with primitives."""

    def __init__(self) -> None: ...
    # transitions
    def send_open(self, eos: bool) -> None: ...
    def recv_open(self, eos: bool, informational: bool) -> bool: ...
    def reserve_remote(self) -> None: ...
    def reserve_local(self) -> None: ...
    def recv_close(self) -> None: ...
    def recv_reset(self, stream_id: int, reason: int, queued: bool) -> None: ...
    def recv_eof(self) -> None: ...
    def send_close(self) -> bool:
        """Close the send half if it is still streaming, atomically with the check;
        False if the state had already moved (peer reset / connection failure)."""
    def set_reset(self, stream_id: int, reason: int, initiator: str) -> None:
        """`initiator`: "user" | "library" | "remote"."""

    def set_scheduled_reset(self, reason: int) -> None: ...
    # queries
    def get_scheduled_reset(self) -> int | None: ...
    def ensure_recv_open(self) -> bool: ...
    def is_scheduled_reset(self) -> bool: ...
    def is_local_error(self) -> bool: ...
    def is_remote_reset(self) -> bool: ...
    def is_reset(self) -> bool: ...
    def is_send_streaming(self) -> bool: ...
    def is_recv_headers(self) -> bool: ...
    def is_recv_streaming(self) -> bool: ...
    def is_recv_end_stream(self) -> bool: ...
    def is_closed(self) -> bool: ...
    def is_send_closed(self) -> bool: ...
    def is_idle(self) -> bool: ...
    def __repr__(self) -> str: ...

class H2FlowControl:
    """An h2 flow-control window (`vendor_h2::proto::streams::FlowControl`)."""

    def __init__(self) -> None: ...
    def window_size(self) -> int: ...
    def available(self) -> int: ...
    def has_unavailable(self) -> bool: ...
    def unclaimed_capacity(self) -> int | None: ...
    def claim_capacity(self, capacity: int) -> None: ...
    def assign_capacity(self, capacity: int) -> None: ...
    def inc_window(self, sz: int) -> None: ...
    def dec_send_window(self, sz: int) -> None: ...
    def dec_recv_window(self, sz: int) -> None: ...
    def send_data(self, sz: int) -> None: ...
    def __repr__(self) -> str: ...

# ===========================================================================
# HTTP/2 error codes  (src/py/h2/mod.rs — RFC 7540 §7)
# ===========================================================================

# ===========================================================================
# Proxy matcher  (src/py/proxy.rs — vendored hyper-util client::proxy::matcher)
# ===========================================================================

class ProxyMatcher:
    """Selects the proxy for a destination URL (vendored hyper-util matcher).
    Surfaced as `httpunk.util.proxy.Matcher`."""

    @staticmethod
    def from_env() -> ProxyMatcher: ...
    @staticmethod
    def from_parts(
        *,
        all: str | None = ...,
        http: str | None = ...,
        https: str | None = ...,
        no: str | None = ...,
    ) -> ProxyMatcher: ...
    def intercept(self, url: str) -> ProxyIntercept | None: ...

class ProxyIntercept:
    """A selected proxy: its URL plus any auth. Surfaced as
    `httpunk.util.proxy.Intercept`."""

    @property
    def uri(self) -> str: ...
    def basic_auth(self) -> str | None: ...
    def raw_auth(self) -> tuple[str, str] | None: ...
    def __repr__(self) -> str: ...

class H2ContentLength:
    """h2 stream.rs `ContentLength`: the declared body length of a message,
    decremented per DATA frame and checked at END_STREAM."""

    def __init__(self, is_head: bool = ...) -> None: ...
    def is_head(self) -> bool: ...
    def set(self, value: int) -> None:
        """Record a parsed `content-length` (no-op for a HEAD response)."""

    def dec(self, len: int) -> bool:
        """Consume body bytes; False = more data than declared, or any data on a HEAD response."""

    def is_satisfied(self) -> bool:
        """False = a declared length still unsatisfied at END_STREAM."""

class H2DataFrameBudget:
    """h2 0.4.19 counts.rs DATA-framing budget (`record_data_frame` / `release_data_frame`)
    over `DataFrameBudget::resolve`."""

    def __init__(self, configured: int | None = ..., connection_window: int | None = ...) -> None: ...
    def record(self, payload_len: int) -> int:
        """Charge a received non-final DATA frame; returns the charge taken (0 for an
        empty or large frame). Raises `H2ProtocolError(ENHANCE_YOUR_CALM)` on exhaustion."""

    def release(self, charge: int) -> None:
        """Give a charge back (saturating at the resolved budget)."""

    @property
    def available(self) -> int: ...
    @property
    def empty_frames(self) -> int: ...

# ===========================================================================
# HTTP/1 connection states  (src/h1/conn.rs) — hyper's single-owner `Conn` state
# for a multi-task runtime, ONE mutex per connection. `ServerConnection` /
# `Connection` subclass `H1ServerState` / `H1ClientState` and add only async
# machinery; server requests are keyed by a sequence number (`seq`), a stale one
# gets a deterministic answer.
# ===========================================================================

# `begin_read()` / `drain_done()` codes: between requests, then — positioned at the
# next head — how to read it.
H1_NEXT_NONE: int  # the connection can serve no more
H1_NEXT_RAISE: int  # the current request was not answered (a caller error)
H1_NEXT_CLOSE: int  # the unread body cannot be drained: closed (the transport is returned)
H1_NEXT_DRAIN: int  # run the one-poll drain, then `drain_done`
H1_READ_PARSE: int  # bytes of the next request are already buffered: `accept_head(b"")` first
H1_READ_SHUTDOWN: int  # a graceful shutdown was requested: no idle read
H1_READ_WATCHER: int  # the parked watcher's read is the idle read (its done event is returned)
H1_READ_TRANSPORT: int  # read the transport (returned); the idle park is flagged
H1_READ_EOF: int  # the transport is gone

# `respond_head()` / `detach()` codes.
H1_REQ_OK: int
H1_REQ_ALREADY: int  # already responded / detached
H1_REQ_STALE: int  # not the current request
H1_REQ_PARKED: int  # detach: a mid-message read is parked on the transport
H1_REQ_PEER_CLOSED: int  # respond_head: the client closed mid-request (`IncompleteMessage`)
H1_REQ_CONTINUE: int  # respond_head: claimed; a `100 Continue` is being written — wait, call again

class H1ServerState:
    """The connection + current-request state of one HTTP/1 server connection,
    `frozen` + subclassable. Owns the transport, the keep-alive / close / shutdown
    flags, the mid-message watcher slot and the current request's flags; holds the
    connection's `H1Codec` and the current request's `H1BodyDecoder`, locking them
    nested only where a byte move must be atomic with a state change."""

    def __init__(
        self, codec: H1Codec, transport: object, *, keep_alive: bool = ..., half_close: bool = ...
    ) -> None: ...
    @property
    def codec(self) -> H1Codec: ...
    def transport_ref(self) -> object | None:
        """The transport for a read/write; None once closed or handed off."""

    @property
    def closed(self) -> bool: ...
    @property
    def reusable(self) -> bool: ...
    @property
    def upgraded(self) -> bool: ...
    @property
    def half_close(self) -> bool: ...
    @property
    def keep_alive_enabled(self) -> bool: ...
    @property
    def shutdown_requested(self) -> bool: ...
    @property
    def has_watcher(self) -> bool: ...
    @property
    def watcher_parked(self) -> bool: ...
    @property
    def current_seq(self) -> int: ...
    def current_decoder(self) -> H1BodyDecoder | None: ...

    # ----- the accept loop -----
    def begin_read(self) -> tuple[int, object | None]:
        """`(H1_NEXT_* / H1_READ_* code, transport to close | the watcher's done event |
        the transport to read)`: the between-requests verdict, then — positioned at the
        next head (codec reset, leftover fed) — how to read it, with the idle park
        flagged in the same step."""

    def drain_done(self, complete: bool) -> tuple[int, object | None]:
        """The one-poll drain's outcome: `H1_NEXT_CLOSE` + transport, or the read verdict."""

    def unpark_idle_read(self) -> bool:
        """The idle read returned; True = a shutdown closed the connection while it was parked."""

    def accept_head(self, data: bytes) -> tuple[H1RequestHead, int, H1BodyDecoder] | None:
        """Feed the head parser; once a head is complete it is the current request:
        `(head, seq, its body decoder — fed the bytes read alongside the head)`; None =
        read more. Raises `H1ParseError` (the codec remembers the automatic status)."""

    def fail_read(self) -> object | None:
        """A head parse failure / deadline / broken transport: closed; the transport to close."""

    def stop_serving(self) -> None: ...
    def mark_closed(self) -> tuple[object | None, object | None, bool]:
        """`close()`: `(transport to close, the watcher's done event to await, ended)`;
        `ended` = the connection had already ended cleanly (hyper's orderly shutdown)."""

    def mark_unusable(self) -> None: ...
    def request_shutdown(self) -> object | None:
        """`graceful_shutdown()`: closed if a read is parked idly — the transport to shut down."""

    # ----- the current request -----
    def try_send_continue(self, seq: int) -> bool: ...
    def begin_body_read(self, seq: int) -> bool: ...
    def end_body_read(self, seq: int, complete: bool) -> None: ...
    def peer_closed_now(self, seq: int) -> bool | None:
        """The flag when the window is over (or the request stale); None = still open."""

    def peer_closed_flag(self, seq: int) -> bool: ...
    def arm_watcher(self, seq: int, done: object, *, want: bool = ..., head_negotiated: bool = ...) -> bool:
        """The whole arm decision; True = spawn the watcher, then `store_watcher_handle`."""

    def store_watcher_handle(self, handle: object) -> object | None:
        """None = stored; the handle back = refused (closed meanwhile): join it yourself."""

    def watcher_completed(self, data: bytes | None = ..., error: BaseException | None = ...) -> bool:
        """True = the request is now peer-closed (resolve its `peer_closed()`)."""

    def watcher_aborted(self) -> None: ...
    def watcher_done(self) -> object | None: ...
    def take_watcher_handle(self) -> object | None: ...
    def take_watcher_result(self) -> tuple[bytes | None, BaseException | None]: ...
    def close_window(self, seq: int) -> bool: ...
    def respond_head(
        self,
        seq: int,
        status: int,
        headers: HeaderMap | None,
        done: object,
        *,
        content_length: int | None = ...,
        chunked: bool = ...,
        want: bool = ...,
    ) -> tuple[int, bytes | None, bool]:
        """The head step as one transition: claim, verdicts, encode, head-time
        decisions, arm decision. `(H1_REQ_* code, encoded head, armed)`: `H1_REQ_OK` +
        head (+ spawn the watcher for `done` if `armed`); `H1_REQ_CONTINUE` = claimed,
        await the request's continue event and call again; `H1_REQ_PEER_CLOSED`;
        `H1_REQ_ALREADY` / `H1_REQ_STALE`. Raises the encoder's `H1UserError`."""

    def finish_response(self, seq: int) -> tuple[bool, bool, object | None, bytes | None]:
        """`(switch, close, transport, tunnel leftover)`."""

    def fail_response(self, seq: int) -> object | None: ...
    def detach(self, seq: int) -> tuple[int, object | None, bytes | None]:
        """`(H1_REQ_* code, transport, leftover)`."""

# `watcher_completed()` verdicts (H1ClientState).
H1_WATCH_HANDOFF: int  # an exchange is active: the bytes/error are its first read
H1_WATCH_IDLE_EOF: int  # hyper's clean idle close: closed, the transport returned
H1_WATCH_IDLE_BYTES: int  # bytes on an idle connection: poison
H1_WATCH_IDLE_ERROR: int  # a transport error while idle: fail
H1_WATCH_IGNORED: int  # a racing close already committed the flags

class H1ClientState:
    """The connection state of one HTTP/1 client connection, `frozen` +
    subclassable: the transport, the single in-flight slot (hyper `Conn::is_busy`),
    the error slot (first writer wins), the idle watcher's hand-off, the background
    writer's scope, the peer's version."""

    def __init__(self, transport: object) -> None: ...
    def transport_ref(self) -> object | None: ...
    @property
    def closed(self) -> bool: ...
    @property
    def busy(self) -> bool: ...
    @property
    def upgraded(self) -> bool: ...
    @property
    def peer_http10(self) -> bool: ...
    def set_peer_http10(self, value: bool) -> None: ...
    @property
    def error(self) -> BaseException | None: ...
    def is_dead(self) -> bool: ...
    @property
    def has_watcher(self) -> bool: ...
    @property
    def writer_finished(self) -> bool: ...

    # ----- the single in-flight slot -----
    def try_begin_exchange(self) -> bool: ...
    def end_exchange(self) -> bool:
        """True if the slot flipped to free (wake the idle waiters)."""

    def exchange_started(self) -> None: ...

    # ----- failure / close -----
    def fail(self, exc: BaseException | None = ...) -> object | None:
        """Store `exc` (a traceback-free copy) if none is stored, close; the transport to close."""

    def close_now(self) -> object | None: ...
    def mark_closed(self) -> tuple[object | None, object | None, object | None, bool]:
        """`(transport, watcher done event, writer scope, idle)`; `idle` = no exchange held the slot."""

    def upgrade(self) -> object | None:
        """Hand the transport off (101 / CONNECT), in one step."""

    # ----- the background body writer -----
    def store_writer(self, scope: object) -> object | None:
        """None = stored; the scope back = refused (closed meanwhile): tear it down yourself."""

    def take_writer(self) -> object | None: ...
    def writer_done(self) -> None: ...

    # ----- the idle watcher -----
    def arm_watcher(self, done: object) -> bool: ...
    def store_watcher_handle(self, handle: object) -> object | None: ...
    def watcher_aborted(self) -> None: ...
    def watcher_completed(
        self, data: bytes | None = ..., error: BaseException | None = ...
    ) -> tuple[int, object | None]:
        """`(H1_WATCH_* verdict, transport to close)`."""

    def watcher_done(self) -> object | None: ...
    def take_watcher_handle(self) -> object | None: ...
    def take_watcher_result(self) -> tuple[bytes | None, BaseException | None]: ...

class OnceLatch:
    """A one-shot latch: `try_acquire()` succeeds exactly once, from whichever task
    (or GC finalizer) gets there first; never blocks."""

    def __init__(self) -> None: ...
    def try_acquire(self) -> bool: ...
    @property
    def is_set(self) -> bool: ...

# ===========================================================================
# HTTP/2 connection state  (src/h2/conn.rs) — h2 `Streams::inner` + the state
# half of `proto::Connection`, ONE mutex per connection. The Python driver
# (`httpunk/h2/connection.py`) subclasses `H2Streams` and adds only async
# machinery; every method here is one locked check-and-act returning a verdict.
# ===========================================================================

# Verdict flags: what the caller must do after the call (`H2ConnectionBase._after`).
H2_FLAG_WAKE: int  # bytes were queued: wake the write pump
H2_FLAG_SLOT_FREED: int  # client: a MAX_CONCURRENT slot freed / the limit changed
H2_FLAG_CONN_DONE: int  # the connection failed or finished: wake the role waiters
H2_FLAG_STOP_ACCEPTING: int  # server: the graceful drain completed

# `H2RecvHeadersVerdict.kind`.
H2_HEADERS_IGNORED: int
H2_HEADERS_OPENED: int  # server: a new request stream (the spare handle was consumed)
H2_HEADERS_HEAD: int  # client: the response head
H2_HEADERS_TRAILERS: int

class H2Stopped:
    """Why a stream stopped (h2 `ensure_reason` + the stream's stored error). `reason`:
    the RST_STREAM / GOAWAY reason, if any; `conn`: a connection-level error applies
    (the connection's error, or a `GoAwayError` when `reason` is set). Neither: a local
    cancel — the driver falls back to the connection error, then CANCEL. Also the body
    queue's terminal item when the reader must raise."""

    @property
    def reason(self) -> int | None: ...
    @property
    def conn(self) -> bool: ...

class H2RecvHeadersVerdict:
    kind: int
    handle: object | None
    stream_id: int
    eof: bool
    flags: int

class H2RecvDataVerdict:
    handle: object | None
    payload: bytes | None  # None: nothing to deliver (swallowed, or an empty budgeted frame)
    budgeted: bool
    eof: bool
    flags: int

class H2SendVerdict:
    sent: int  # bytes of `data[offset:]` framed and queued (0 with done=False: wait for window)
    done: bool  # the last byte (and END_STREAM, if requested) is queued; with END_STREAM the send half closed
    stopped: H2Stopped | None
    flags: int
    handle: object | None  # server, the response complete: the request reader to notify (its body was reset)
    reader_stop: H2Stopped | None  # the stop those readers must see

class H2ResetVerdict:
    handle: object | None  # the stream's handle to notify (None: no live stream)
    stop: H2Stopped | None  # the error the body readers must see (None: a clean EOF)
    flags: int

class H2Streams:
    """The connection + stream state of one HTTP/2 connection, `frozen` + subclassable.
    The stream map, both flow-control windows, the DATA-framing budget, the SETTINGS
    state, the reset store, the GOAWAY bookkeeping, the error slot, the HPACK codec
    and the pending-frame buffer live under one mutex. `handle`s are the Python
    `Stream` objects (id + events + body queue) the state stores and hands back."""

    def __init__(
        self,
        role: str,
        *,
        initial_window_size: int,
        connection_window: int,
        max_frame_size: int,
        max_header_list_size: int,
        max_send_buf_size: int,
        max_concurrent_streams: int | None = ...,
        max_pending_accept_reset_streams: int = ...,
        max_local_error_reset_streams: int | None = ...,
        data_frame_budget: int | None = ...,
        auto_date_header: bool = ...,
        enable_push: bool | None = ...,
    ) -> None:
        """`role`: "client" | "server". Validates the RFC ranges at construction."""

    @property
    def is_server(self) -> bool: ...

    # ----- lifecycle -----
    def begin(self) -> None:
        """Queue the connection preface (client) + our initial SETTINGS + the initial
        WINDOW_UPDATE(0); the caller flushes, then starts the pumps."""

    def store_task_handles(self, read: object, pump: object) -> None: ...
    def take_read_handle(self) -> object | None: ...
    def take_pump_handle(self) -> object | None: ...

    # ----- inbound (each frame: one locked step) -----
    def receive(self, data: bytes) -> list[H2Frame]:
        """Decode inbound bytes into frames (server: consumes the client preface first)."""

    def prime(self, data: bytes) -> bool | None:
        """Seed the decoder with bytes another reader already took off the transport
        (`util.auto`'s sniff: the client preface) — hyper-util's `Rewind` without a
        wrapper transport. None = preface incomplete so far; False = mismatch."""

    def recv_headers(self, frame: H2FrameHeaders, spare: object | None = ...) -> H2RecvHeadersVerdict: ...
    def recv_data(self, frame: H2FrameData) -> H2RecvDataVerdict: ...
    def recv_window_update(self, frame: H2FrameWindowUpdate) -> list[object]:
        """Returns the handles whose senders must be woken."""

    def recv_reset(self, frame: H2FrameRstStream) -> tuple[object, H2Stopped, bool, int] | None:
        """`(handle, stop, notify_body, flags)` for a live stream, else None."""

    def recv_settings(self, frame: H2FrameSettings) -> tuple[bool, list[object], int]:
        """`(initial, wake_windows, flags)`: `initial` = the peer's first SETTINGS landed."""

    def recv_ping(self, frame: H2FramePing) -> int: ...
    def recv_go_away(self, frame: H2FrameGoAway) -> tuple[list[tuple[object, H2Stopped]], int]:
        """`(aborted, flags)`: the streams above last_stream_id, with their stop."""

    def maybe_goaway_reply(self) -> int: ...
    def send_goaway(self, reason: int) -> int: ...
    def fail(self, exc: BaseException | None = ..., message: str = ...) -> list[tuple[object, H2Stopped]]:
        """Record the error (first writer wins; `exc` is a traceback-free copy) and fan
        it out; returns the `(handle, stop)` pairs to notify."""

    def conn_error(self) -> BaseException | None: ...
    def goaway_info(self) -> tuple[int, int, bytes] | None:
        """The peer's GOAWAY as `(last_stream_id, error_code, debug_data)`."""

    def is_closed(self) -> bool: ...
    def is_failed(self) -> bool: ...

    # ----- opening (client) -----
    def try_claim_slot(self) -> bool: ...
    def can_open(self) -> bool: ...
    def release_slot_count(self) -> None: ...
    def apply_stream_limit(self, limit: int | None) -> None: ...
    def open_stream(
        self,
        method: str,
        target: str,
        headers: HeaderMap | None,
        end_stream: bool,
        is_head: bool,
        handle: object,
        *,
        scheme: str | None = ...,
        authority: str | None = ...,
    ) -> int | None:
        """Allocate the id, `send_open`, insert, HPACK-encode + queue the HEADERS. None:
        the connection failed / GOAWAY'd (the slot is released)."""

    # ----- sending -----
    def send_data(self, sid: int, data: bytes, offset: int, end_stream: bool) -> H2SendVerdict: ...
    def send_trailers(self, sid: int, trailers: HeaderMap) -> H2SendVerdict: ...
    def send_response_head(
        self, sid: int, status: int, headers: HeaderMap | None, end_stream: bool
    ) -> H2SendVerdict: ...
    def reset_stream(self, sid: int, reason: int, initiator: str = ...) -> H2ResetVerdict: ...
    def reset_on_error(self, sid: int, reason: int) -> H2ResetVerdict:
        """Library reset after a peer violation; counts toward the ENHANCE_YOUR_CALM cap."""

    def aclose_body(self, sid: int) -> H2ResetVerdict: ...

    # ----- recv-side flow control -----
    def release_capacity(self, sid: int, n: int) -> int: ...
    def release_data_frame(self, sid: int, payload_len: int) -> None: ...
    def accepted(self, sid: int) -> None: ...

    # ----- the write pump -----
    def stop_pump(self) -> None: ...
    def take_pending(self) -> tuple[bytes, bool]:
        """`(bytes, stopping)`: everything committed to the pending buffer."""

    def credit_written(self) -> list[object]:
        """Credit the flushed batch back; returns the handles whose senders to wake."""

    def has_pending(self) -> bool: ...

    # ----- graceful shutdown (server) -----
    def begin_graceful_shutdown(self) -> int: ...

    # ----- observability (tests / diagnostics) -----
    @property
    def last_processed_id(self) -> int: ...
    @property
    def max_stream_id(self) -> int: ...
    @property
    def graceful(self) -> bool: ...
    @property
    def shutdown_final(self) -> bool: ...
    @property
    def num_streams(self) -> int: ...
    def has_stream(self, sid: int) -> bool: ...
    def is_recv_end_stream(self, sid: int) -> bool: ...
    def stream_state(self, sid: int) -> str | None: ...
    def stream_recv_unreleased(self, sid: int) -> int | None: ...
    def stream_recv_reclaimed(self, sid: int) -> bool | None: ...
    def stream_data_budget_charged(self, sid: int) -> int | None: ...
    def stream_send_buffered(self, sid: int) -> int | None: ...
    def stream_send_window(self, sid: int) -> int | None: ...
    @property
    def conn_send_window(self) -> int: ...
    @property
    def conn_recv_available(self) -> int: ...
    @property
    def data_frame_budget_available(self) -> int: ...
    @property
    def data_frame_budget_empty_frames(self) -> int: ...
    @property
    def peer_initial_window_size(self) -> int: ...
    @property
    def peer_max_frame_size(self) -> int: ...
    @property
    def peer_max_concurrent_streams(self) -> int | None: ...
    @property
    def stream_limit(self) -> int | None: ...
    @property
    def num_open_streams(self) -> int: ...
    @property
    def max_pending_accept_reset_streams(self) -> int: ...
    @property
    def max_local_error_reset_streams(self) -> int | None: ...
    @property
    def local_error_resets(self) -> int: ...
    @property
    def goaway_replied(self) -> bool: ...
    def in_reset_store(self, sid: int) -> bool: ...
    def set_next_stream_id(self, sid: int) -> None: ...
    def set_max_stream_id(self, sid: int) -> None: ...
    def set_last_processed_id(self, sid: int) -> None: ...

# The vendored h2's protocol constants (frame/settings.rs, frame/stream_id.rs,
# proto/mod.rs) and hyper's client connection preface.
H2_DEFAULT_HEADER_TABLE_SIZE: int
H2_DEFAULT_INITIAL_WINDOW_SIZE: int
H2_DEFAULT_MAX_FRAME_SIZE: int
H2_MAX_MAX_FRAME_SIZE: int
H2_MAX_STREAM_ID: int
H2_DEFAULT_DATA_FRAME_OVERHEAD_THRESHOLD: int
H2_DEFAULT_DATA_FRAME_BUDGET: int
H2_MAX_RECV_EMPTY_DATA_FRAMES: int
H2_PREFACE: bytes

class H2Reason:
    """h2's `frame::Reason` error codes (RFC 9113 §7) as a Rust-defined enum: one
    member per code, `int(member)` / `__index__` is the code (a member passes into
    any `int` parameter), `==` works against members and plain ints, the hash equals
    the code's, and `H2Reason(code)` looks a code up (`ValueError` when unknown — an
    unknown peer code stays a plain int on `error_code` attributes)."""

    NO_ERROR: H2Reason
    PROTOCOL_ERROR: H2Reason
    INTERNAL_ERROR: H2Reason
    FLOW_CONTROL_ERROR: H2Reason
    SETTINGS_TIMEOUT: H2Reason
    STREAM_CLOSED: H2Reason
    FRAME_SIZE_ERROR: H2Reason
    REFUSED_STREAM: H2Reason
    CANCEL: H2Reason
    COMPRESSION_ERROR: H2Reason
    CONNECT_ERROR: H2Reason
    ENHANCE_YOUR_CALM: H2Reason
    INADEQUATE_SECURITY: H2Reason
    HTTP_1_1_REQUIRED: H2Reason

    def __init__(self, code: int) -> None: ...
    def __int__(self) -> int: ...
    def __index__(self) -> int: ...
    def __hash__(self) -> int: ...
    @property
    def value(self) -> int: ...
    @property
    def name(self) -> str: ...
