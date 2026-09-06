"""Auto h1-or-h2 server — `httpunk.util`'s analogue of hyper-util's
`server::conn::auto`: serve an already-accepted transport as h1 **or** h2 by
sniffing the client's opening bytes.

An h2 client opens with the fixed 24-byte connection preface
(`PRI * HTTP/2.0\\r\\n\\r\\nSM\\r\\n\\r\\n`, RFC 7540 §3.5); an h1 request opens with a
method token and can never begin with that prefix. So peek up to `len(PREFACE)`
bytes and compare against `PREFACE[:n]` (hyper-util's `H2_PREFACE` check).

Peeking must not lose bytes the codec needs. hyper-util wraps the IO in a `Rewind`
adapter that replays them before reading live and stays in place for the connection's
lifetime (free in Rust, an extra frame per read in Python). httpunk instead seeds the
peeked bytes into the picked server's codec (`_prime`: the h2 decoder consumes the
preface, the h1 codec's persistent read buffer holds the start of the request line)
and hands the driver the RAW transport — the protocol is fixed for the connection's
lifetime after the sniff, so nothing needs to sit in front of the transport afterwards.
Same wire behaviour, and the backends' non-blocking peek seams (`read_nowait`,
`socket`/`_ssl`) always see the real transport: no replay buffer can be overtaken.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .. import _backend
from .._httpunk import H2Codec
from ..h1.server import H1Server
from ..h2.connection import PREFACE
from ..h2.server import H2Server


if TYPE_CHECKING:
    from .._backend import BackendLike


_CANCELLED = object()  # sentinel: the sniff's read lost the race to the cancel signal


class SniffCancelledError(Exception):
    """The protocol sniff was interrupted before it could complete — e.g. a graceful
    shutdown of a connected-but-still-silent client (≈ hyper-util `ReadVersion::cancel`).
    The caller should close the connection without building a server."""


class Builder:
    """Configure and serve an auto h1-or-h2 connection — hyper-util's
    `server::conn::auto::Builder`, same shape: `http1()` / `http2()` hand out the
    per-protocol sub-builders (`Http1Builder` / `Http2Builder`), `http1_only()` /
    `http2_only()` force the protocol (no sniff), and `serve_connection(transport)`
    sniffs (unless forced) and returns the matching **un-entered** `H1Server` /
    `H2Server` built with the configured options. `backend` plays hyper-util's
    `executor` role.

    Options are stored as the servers' constructor keywords and applied to whichever
    protocol the sniff selects, exactly as hyper-util keeps an `http1::Builder` and an
    `http2::Builder` and hands the accepted IO to one of them. `serve()` is the
    default-options shortcut over this class.
    """

    def __init__(self, *, backend: BackendLike | None = None) -> None:
        self._backend = backend
        self._h1: dict[str, Any] = {}
        self._h2: dict[str, Any] = {}
        self._version: str | None = None  # hyper-util `version: Option<Version>`

    def http1(self) -> Http1Builder:
        """Http1 configuration."""
        return Http1Builder(self)

    def http2(self) -> Http2Builder:
        """Http2 configuration."""
        return Http2Builder(self)

    def http1_only(self) -> Builder:
        """Only accept HTTP/1 (no sniff). hyper-util `assert!`s the version was not
        already forced; a second force raises instead of silently overriding."""
        if self._version is not None:
            raise RuntimeError(f"protocol already forced to {self._version!r}")
        self._version = "h1"
        return self

    def http2_only(self) -> Builder:
        """Only accept HTTP/2 (no sniff). See `http1_only`."""
        if self._version is not None:
            raise RuntimeError(f"protocol already forced to {self._version!r}")
        self._version = "h2"
        return self

    def _build_h1(self, transport) -> H1Server:
        return H1Server(transport, backend=self._backend, **self._h1)

    def _build_h2(self, transport) -> H2Server:
        return H2Server(transport, backend=self._backend, **self._h2)

    async def serve_connection(self, transport: Any, *, cancel: Any = None) -> H2Server | H1Server:
        """Sniff `transport` (unless a protocol was forced) and return the matching
        **un-entered** server over the raw transport, its codec seeded with the sniffed
        bytes (hyper-util `Builder::serve_connection` + `Rewind`, see the module doc).
        Returned un-entered like `connect` — the caller drives it with
        `async with server: async for req in server: ...`.

        `cancel` (an event) makes the peek interruptible (≈ hyper-util
        `ReadVersion::cancel`): if it fires while we're parked reading a silent client's
        preface, the sniff aborts with `SniffCancelledError` so a graceful shutdown
        doesn't linger on that connection. Requires `backend` (for the read-vs-cancel race).
        """
        if self._version == "h2":
            return self._build_h2(transport)
        if self._version == "h1":
            return self._build_h1(transport)

        # Peek up to the full preface, stopping early the moment the bytes diverge
        # from it (→ definitely h1) or the peer stops sending (EOF).
        buf, matched = b"", None
        while len(buf) < len(PREFACE):
            chunk = await _sniff_read(transport, len(PREFACE) - len(buf), self._backend, cancel)
            if chunk is _CANCELLED:
                raise SniffCancelledError
            if not chunk:
                break  # EOF before a full preface -> treat as h1 (a truncated request)
            buf += chunk
            matched = H2Codec.match_preface(buf)  # hyper-util `read_version`, in the Rust core
            if matched is not None:
                break  # the full preface (-> h2), or diverged from it (-> h1)

        server = self._build_h2(transport) if matched else self._build_h1(transport)
        if buf:
            server._prime(buf)  # the sniffed bytes go into the codec; the driver reads the raw transport
        return server


class Http1Builder:
    """Http1 part of the builder (hyper-util `auto::Http1Builder`): the h1 options,
    `http2()` to cross over, and `serve_connection` to finish. Each setter returns
    `self` for chaining. Option names follow hyper-util's `http1::Builder`."""

    def __init__(self, inner: Builder) -> None:
        self._inner = inner

    def http2(self) -> Http2Builder:
        """Http2 configuration."""
        return Http2Builder(self._inner)

    def _set(self, key: str, value: Any) -> Http1Builder:
        self._inner._h1[key] = value
        return self

    def header_read_timeout(self, seconds: float | None) -> Http1Builder:
        """Max time to read a complete request head before closing (slowloris defence,
        hyper http1.rs L249). `None` disables it (hyper: `Into<Option<Duration>>`).
        Default 30s."""
        return self._set("header_read_timeout", seconds)

    def keep_alive(self, val: bool) -> Http1Builder:
        """Whether HTTP/1 connections may be kept alive after a response (default True).
        False: one request, answered `Connection: close`, then close."""
        return self._set("keep_alive", val)

    def max_headers(self, val: int | None) -> Http1Builder:
        """Max request header count before the head is rejected 431 (hyper default 100)."""
        return self._set("max_headers", val)

    def max_buf_size(self, max: int) -> Http1Builder:
        """Cap on a still-incomplete request head (431 + close beyond it); >= 8192."""
        return self._set("max_buf_size", max)

    def auto_date_header(self, enabled: bool) -> Http1Builder:
        """Write a `Date` header on responses lacking one (default True)."""
        return self._set("auto_date_header", enabled)

    def title_case_headers(self, enabled: bool) -> Http1Builder:
        """Write response header names in Title-Case (default False)."""
        return self._set("title_case_headers", enabled)

    def ignore_invalid_headers(self, enabled: bool) -> Http1Builder:
        """Skip malformed request header lines instead of rejecting the request 400
        (default False)."""
        return self._set("ignore_invalid_headers", enabled)

    def half_close(self, val: bool) -> Http1Builder:
        """Support half-closures: a client that shuts its write side while waiting for
        the response is not treated as gone (hyper `http1::Builder::half_close`, default
        False — where the mid-request EOF fails the in-flight response)."""
        return self._set("half_close", val)

    async def serve_connection(self, transport: Any, *, cancel: Any = None) -> H2Server | H1Server:
        return await self._inner.serve_connection(transport, cancel=cancel)


class Http2Builder:
    """Http2 part of the builder (hyper-util `auto::Http2Builder`): the h2 options,
    `http1()` to cross over, and `serve_connection` to finish. Each setter returns
    `self` for chaining. Option names follow hyper-util's `http2::Builder`; `None`
    means "use the default" (hyper: `Into<Option<u32>>`)."""

    def __init__(self, inner: Builder) -> None:
        self._inner = inner

    def http1(self) -> Http1Builder:
        """Http1 configuration."""
        return Http1Builder(self._inner)

    def _set(self, key: str, value: Any) -> Http2Builder:
        if value is None:
            self._inner._h2.pop(key, None)  # back to the server's default
        else:
            self._inner._h2[key] = value
        return self

    def max_concurrent_streams(self, max: int | None) -> Http2Builder:
        """SETTINGS_MAX_CONCURRENT_STREAMS advertised to the client (hyper-util
        `Http2Builder::max_concurrent_streams`)."""
        return self._set("max_concurrent_streams", max)

    def initial_stream_window_size(self, sz: int | None) -> Http2Builder:
        """Our advertised per-stream receive window (hyper-util
        `Http2Builder::initial_stream_window_size`; `H2Server(initial_window_size=)`)."""
        return self._set("initial_window_size", sz)

    def data_frame_budget(self, budget: int | None) -> Http2Builder:
        """DATA-framing budget; `None` = h2's Auto (h2 0.4.19
        `server::Builder::data_frame_budget`, not surfaced by hyper-util)."""
        return self._set("data_frame_budget", budget)

    def initial_connection_window_size(self, sz: int | None) -> Http2Builder:
        """Our advertised connection-level receive window (hyper default 1 MB)."""
        return self._set("initial_connection_window_size", sz)

    def max_frame_size(self, sz: int | None) -> Http2Builder:
        """SETTINGS_MAX_FRAME_SIZE we advertise (hyper default 16 KB; RFC range enforced)."""
        return self._set("max_frame_size", sz)

    def max_header_list_size(self, max: int | None) -> Http2Builder:
        """SETTINGS_MAX_HEADER_LIST_SIZE we advertise (hyper default 16 KB)."""
        return self._set("max_header_list_size", max)

    def max_pending_accept_reset_streams(self, max: int | None) -> Http2Builder:
        """Reset-before-accept streams tolerated before GOAWAY(ENHANCE_YOUR_CALM) — the
        Rapid-Reset cap (`None` = h2's default, 20)."""
        return self._set("max_pending_accept_reset_streams", max)

    def max_local_error_reset_streams(self, max: int | None) -> Http2Builder:
        """Library-initiated error resets tolerated before GOAWAY(ENHANCE_YOUR_CALM)
        (hyper default 1024). Unlike the other setters, `None` here means NO LIMIT —
        hyper's documented (and not advised) semantics — so it is passed through."""
        self._inner._h2["max_local_error_reset_streams"] = max
        return self

    def auto_date_header(self, enabled: bool) -> Http2Builder:
        """Insert a `Date` header on responses lacking one (default True)."""
        return self._set("auto_date_header", enabled)

    def max_send_buf_size(self, max: int | None) -> Http2Builder:
        """Per-stream cap on response DATA queued for the connection's writer (hyper
        `max_send_buf_size`, default 400 KB); the sender awaits room like flow-control window."""
        return self._set("max_send_buf_size", max)

    async def serve_connection(self, transport: Any, *, cancel: Any = None) -> H2Server | H1Server:
        return await self._inner.serve_connection(transport, cancel=cancel)


async def serve(
    transport: Any,
    *,
    backend: BackendLike | None = None,
    only: str | None = None,
    cancel: Any = None,
) -> H2Server | H1Server:
    """Sniff `transport` and return the matching **un-entered** server (`H2Server`
    or `H1Server`) with default options — the shortcut over `Builder`; use the
    `Builder` to configure per-protocol options (hyper-util `auto::Builder`).

    `only="h1"` / `only="h2"` forces the protocol without sniffing (`Builder.http1_only`
    / `http2_only`). `cancel` is `Builder.serve_connection`'s `cancel`.
    """
    builder = Builder(backend=backend)
    if only == "h2":
        builder.http2_only()
    elif only == "h1":
        builder.http1_only()
    elif only is not None:
        raise ValueError(f"only must be None, 'h1', or 'h2' (got {only!r})")
    return await builder.serve_connection(transport, cancel=cancel)


async def _sniff_read(transport, n, backend, cancel):
    """One peek read, interruptible by an optional `cancel` signal (returns the
    `_CANCELLED` sentinel if it fired). A parked `receive_some` is never cancelled
    (on tonio that leaves the socket registration armed for a dead task): the
    signal ends the read by CLOSING the transport — the sanctioned way to end a
    parked read — and the caller, who closes a cancelled connection anyway, gets
    the sentinel."""
    if cancel is None:
        return await transport.receive_some(n)
    backend = _backend.resolve(backend)

    async def _closer():
        await cancel.wait()
        backend.close_transport(transport)

    async with backend.scope() as s:
        s.spawn(_closer())
        try:
            chunk = await transport.receive_some(n)
        except Exception:
            chunk = b""  # the close ended the read (EOF on asyncio, an error on tonio)
        finally:
            s.cancel()  # the closer parks on an event, never on a read: safe to cancel
    if cancel.is_set():
        return _CANCELLED
    return chunk
