"""Type stubs for the `httpunk._httpunk` Rust extension module.

Hand-maintained to match the PyO3 surface in `src/py/**`. Everything here is
implemented in Rust: the `http`-crate `HeaderMap`, the sans-IO HTTP/1 and HTTP/2
codecs + their frame/head events, the vendored h2 stream-state and flow-control
wrappers, the error taxonomy, and the `H2Reason` code enum.

All classes are `frozen` (immutable identity, internally `Mutex`-guarded) and safe
to share across the runtime's worker threads. Getter attributes are read-only.
"""

import enum
from collections.abc import Iterable, Iterator, Mapping
from typing import TypeVar, overload

__version__: str

_T = TypeVar("_T")

# Header names/values accept `str` or `bytes` on input; names come back as
# lowercase `str`, values as `bytes` (validated by the `http` crate).
HeaderNameLike = str | bytes
HeaderValueLike = str | bytes

# ===========================================================================
# Errors  (protocol-neutral root + ConnectionClosedError in src/py/errors.rs;
# the H2* protocol errors in src/py/h2/streams.rs)
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
    """The transport closed (EOF/reset/IO error) with work still in flight — a
    transport failure, not a protocol violation (so no GOAWAY). Protocol-neutral:
    raised on both HTTP/1 and HTTP/2, hence it sits under HTTPunkError, not H2Error."""

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

class H1Codec:
    """A synchronous, zero-I/O HTTP/1 codec over the vendored hyper h1 core.
    Drives head parse/encode + body-frame encode for one request/response."""

    def __init__(self) -> None: ...
    def serialize_request(
        self,
        method: str,
        url: str,
        headers: HeaderMap | None = ...,
        *,
        http10: bool = ...,
        content_length: int | None = ...,
        chunked: bool = ...,
        trailer_fields: list[str] = ...,
    ) -> bytes:
        """Serialize a request head (request line + headers); retains the body
        encoder for `serialize_data`/`serialize_end`/`serialize_trailers`.
        `trailer_fields` declares chunked trailer field names (the `Trailer` header)
        that `serialize_trailers` may then emit."""

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

    def serialize_trailers(self, trailers: HeaderMap) -> bytes:
        """Finish a chunked body with a trailer block (the declared `trailer_fields`)
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
    def __repr__(self) -> str: ...

class H2FrameData:
    stream_id: int
    end_stream: bool
    data: bytes
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
        url: str,
        headers: HeaderMap | None = ...,
        end_stream: bool = ...,
    ) -> bytes: ...
    def serialize_response_headers(
        self,
        stream_id: int,
        status: int,
        headers: HeaderMap | None = ...,
        end_stream: bool = ...,
    ) -> bytes: ...
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
    def send_close(self) -> None: ...
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

class H2Reason(enum.IntEnum):
    NO_ERROR = 0
    PROTOCOL_ERROR = 1
    INTERNAL_ERROR = 2
    FLOW_CONTROL_ERROR = 3
    SETTINGS_TIMEOUT = 4
    STREAM_CLOSED = 5
    FRAME_SIZE_ERROR = 6
    REFUSED_STREAM = 7
    CANCEL = 8
    COMPRESSION_ERROR = 9
    CONNECT_ERROR = 10
    ENHANCE_YOUR_CALM = 11
    INADEQUATE_SECURITY = 12
    HTTP_1_1_REQUIRED = 13
