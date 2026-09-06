"""HTTP/1 server — the accepting-side analogue of the h1 client
(`connection.py` + `client.py`), and the h1 counterpart of `h2/server.py`.

The sans-IO byte work is all Rust over the vendored hyper core: `H1Codec.
receive_request_head` (head parse via `Server::parse`) + `serialize_response`
(head encode via `Server::encode`, incl. the `Date` header) + the body `Encoder`,
and `H1BodyDecoder` for the request body. Only the orchestration is Python.

HTTP/1 is strictly one request/response at a time (no multiplexing), so — like
the h1 client — there is no background pump: `next_request` reads a request head
inline, hands back a `ServerRequest`, and the app reads the request body and
sends a response on the same connection. On a keep-alive connection the loop
reads the next request; otherwise the connection closes.

Low-level by design (like `hyper::server::conn::http1`): one connection over a
caller-supplied, already-accepted transport; accepting the socket, TLS/ALPN, and
the accept loop are the caller's job. Usage:

    async with H1Server(transport) as server:
        async for request in server:
            body = await request.read()
            await request.respond(200, headers={"content-type": "text/plain"}, body=b"hi")

Cross-reference: hyper 1.10.1 `proto/h1/{role,conn,dispatch}.rs` (server path) and
`client/conn`/`server/conn`.
"""

from __future__ import annotations

import threading
from collections.abc import AsyncIterator, Awaitable
from typing import TYPE_CHECKING, Any

from .._common import PUMP_ABANDONED, PUMP_DONE, BaseServer, aclose_body, event_result, read_all
from .._httpunk import H1BodyDecoder, H1Codec
from ..exceptions import ConnectionClosedError, H1IncompleteMessageError, H1ParseError, HTTPunkError
from ..http import HeaderMap
from ..types import Version
from .connection import H1ConnectionBase
from .share import H1Upgraded


if TYPE_CHECKING:
    from .._backend import BackendLike
    from ..types import Body, HeadersInput


_READ_SIZE = 65536
_SHUTDOWN = object()  # sentinel: a graceful shutdown released an idle head-read
# The interim response hyper's server auto-sends for `Expect: 100-continue`
# (byte-identical to hyper conn.rs L413).
_CONTINUE = b"HTTP/1.1 100 Continue\r\n\r\n"
# Max bytes of an incomplete request head we'll buffer before rejecting it as
# `Parse::TooLarge` (auto 431 + close) — hyper's `DEFAULT_MAX_BUFFER_SIZE`
# (io.rs: 8192 + 4096*100). Bounds per-connection memory against a slow/oversized head.
# hyper `http1::Builder::max_buf_size`: default io.rs DEFAULT_MAX_BUFFER_SIZE; a value below
# MINIMUM_MAX_BUFFER_SIZE (= INIT_BUFFER_SIZE, 8192) is rejected (hyper `assert!`s).
_DEFAULT_MAX_BUF_SIZE = 8192 + 4096 * 100
_MIN_MAX_BUF_SIZE = 8192
_DEFAULT_HEADER_READ_TIMEOUT = 30.0  # hyper's `header_read_timeout` default (http1.rs L249)
# The HTTP/2 connection preface (client prior-knowledge). An h1 server that sees it
# closes silently with a version error rather than writing a 400 (hyper `on_parse_error`
# -> `has_h2_prefix` -> `new_version_h2`, conn.rs L29/L809-812).
_H2_PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
# The error an in-flight response gets once the client closed its side mid-request —
# hyper's `IncompleteMessage` from `mid_message_detect_eof` (conn.rs L491-508).
_PEER_CLOSED_MSG = "connection closed before message completed: the client closed its side mid-request"


def _connection_has(headers, token):
    """True if the `Connection` header (any value, comma-split, case-insensitive)
    carries `token` — e.g. a response `Connection: close`."""
    if headers is None:
        return False
    for value in headers.get_all("connection"):
        for part in bytes(value).decode("latin-1").split(","):
            if part.strip().lower() == token:
                return True
    return False


def _error_status(kind):
    """Map a request-head parse error (`H1ParseError.args[0]`, the hyper `Parse`
    variant) to hyper's automatic status (`Server::on_error`, role.rs L481-499):
    `uri_too_long` -> 414, `too_large` -> 431, everything else (method / header /
    uri / version) -> 400."""
    if kind == "uri_too_long":
        return 414
    if kind == "too_large":
        return 431
    return 400


class ServerRequest:
    """An incoming request + the handle to respond to it (hyper: the request +
    its `Sender`/response channel). One in flight at a time."""

    method: str
    target: str  # the request-target (origin/absolute/authority form)
    path: str  # alias of target
    headers: HeaderMap
    trailers: HeaderMap | None  # chunked trailers, populated once the body is read
    keep_alive: bool
    is_upgrade: bool
    version: Version  # HTTP_10 or HTTP_11 (hyper `Request::version()`)
    content_length: int | None  # declared request Content-Length (None if chunked)
    upgraded: H1Upgraded | None  # the raw tunnel once a CONNECT/Upgrade is answered

    def __init__(
        self,
        conn,
        *,
        method,
        target,
        headers,
        decoder,
        keep_alive,
        expect_continue,
        is_upgrade,
        http10,
        content_length,
    ):
        self.method = method  # str
        self.target = target  # str — the request-target (origin/absolute/authority form)
        self.path = target  # alias
        self.headers = headers  # httpunk.http.HeaderMap
        self.version = Version.HTTP_10 if http10 else Version.HTTP_11  # hyper `Request::version()`
        self.trailers = None  # chunked trailers, populated once the body is read
        self.keep_alive = keep_alive
        self.is_upgrade = is_upgrade
        self.content_length = content_length  # declared request Content-Length (None if chunked)
        # The raw tunnel once the app answers a CONNECT/Upgrade with a 101 or a 2xx
        # to CONNECT (hyper `on_upgrade`): the caller owns it and drives it directly.
        self.upgraded = None
        self._conn = conn
        self._decoder = decoder
        self._http10 = http10  # the driver's 1.0-specific negotiation keys off the codec flag
        self._expect_continue = expect_continue
        self._continue_sent = False
        self._body_done = decoder.is_complete
        self._responded = False  # the response head was sent (or the request detached)
        self._response_done = False  # the response is complete: body ended, connection state settled
        # Head-time decisions applied at completion (`ServerConnection._finish_response`).
        self._keep_alive_decision = False
        self._is_switch = False
        self._head_negotiated = False  # `_is_switch` is decided: the head is being sent (under `_arm_lock`)
        # The client closed its side (or the transport failed) while this request was
        # mid-message — set by the connection's mid-message watcher (hyper
        # `mid_message_detect_eof` -> `close_read` + `IncompleteMessage`).
        self._peer_closed = False
        # Set by the watcher on a mid-message EOF, and by `_close_window` when the
        # exchange completes: the two ends of the mid-message window `peer_closed()`
        # observes. Eager (not created on first use) so the window's close and a
        # concurrent `peer_closed()` need no flag/event handshake (free-threaded rule:
        # one transition, one primitive).
        self._peer_closed_evt = conn.backend.event()
        # `peer_closed()` / a streamed or push response asked for the read: park it as
        # soon as hyper would. Written and read only under `ServerConnection._arm_lock`,
        # together with `_head_negotiated`.
        self._watch_wanted = False

    async def aiter_bytes(self) -> AsyncIterator[bytes]:
        """Yield request body chunks, pulling transport bytes on demand (decoded
        by `H1BodyDecoder`). Sends `100 Continue` first if the client asked for it."""
        if self._decoder.is_complete:
            return
        if self._expect_continue and not self._continue_sent and not self._responded and not self._http10:
            # The client is waiting for a 1xx before sending the body (RFC 9110
            # §10.1.1). hyper auto-sends 100 Continue when the body is first polled,
            # but ONLY while `Writing::Init` (before any response) and ONLY for versions
            # > HTTP/1.0 (conn.rs L409-415, L311). Sending it after `respond()` began
            # (F15) would be parsed as the next response; sending it to a 1.0 client
            # (F16) violates the spec hyper deliberately follows.
            self._continue_sent = True
            await self._conn.write(_CONTINUE)
        try:
            while True:
                chunk = self._decoder.decode()
                if chunk is not None:
                    yield chunk
                    continue
                if self._decoder.is_complete:
                    break
                data = await self._conn.read_body_more()
                if data:
                    self._decoder.feed(data)
                else:
                    self._decoder.mark_eof()  # client closed mid-body
        except BaseException:
            self._conn.mark_unusable()
            raise
        self.trailers = self._decoder.take_trailers()
        self._body_done = True

    def read(self) -> Awaitable[bytes]:
        return read_all(self.aiter_bytes())

    async def peer_closed(self) -> bool:
        """Wait for the end of this request's mid-message window and report how it ended:
        `True` = the client closed its side (a FIN mid-request, or a transport error)
        while the response was still being produced — the signal a long-running or
        streaming handler races against its own completion to stop early;
        `False` = the exchange completed (or failed) first. Returns
        `False` at once when awaited after the response is done. Never hangs: the window
        hyper polls `mid_message_detect_eof` over is finite, so is this await.

        The observation itself is hyper's: a mid-message EOF is detected only once the
        request body is complete (`poll_read` serves the body first), not with
        `half_close`, and — runtime-forced, see `_arm_watcher` — not before the response
        head for a request announcing an upgrade (`Upgrade` / CONNECT), which may still
        be detached or switched. hyper has no API for this: its `mid_message_detect_eof`
        errors the connection with `IncompleteMessage` and the service future is dropped.
        httpunk cannot drop the app's coroutine, so the same observation is exposed
        instead; the in-flight `respond()` / `send_data` fail with
        `H1IncompleteMessageError` as hyper's would."""
        if self._response_done:
            return self._peer_closed
        self._conn._arm_watcher(self)  # the signal is being consumed: park a read as soon as possible
        await self._peer_closed_evt.wait()
        return self._peer_closed

    def _close_window(self):
        """The response is complete or failed: the mid-message window is over. Wakes a
        parked `peer_closed()` (which then reports `_peer_closed` as it stands)."""
        self._response_done = True
        self._peer_closed_evt.set()

    async def respond(
        self, status: int, *, headers: HeadersInput = None, body: Body = None, trailers: HeadersInput = None
    ) -> None:
        """Send the whole response: head + body (+ trailers). `body` is None, `bytes`,
        or a (sync/async) iterable of `bytes`. hyper: `Server::encode` + the dispatcher
        polling the response `Body`. The pull convenience over `send_response`: same
        head-time negotiation and completion, plus head+small-body write coalescing.

        `trailers` are sent after the body as chunked trailers, symmetric with the
        client's `Request.trailers` (F45): the body is framed chunked regardless of its
        shape and the fields are declared in a `Trailer` header unless one is already
        set — hyper's encoder emits only declared fields (`Encoder::encode_trailers`).
        On HTTP/1.0 (no chunked framing) they are dropped, as hyper does.

        Raises `ConnectionClosedError` if the client closed its side before or while the
        response is written (hyper: `IncompleteMessage`). With an async body that is
        raised at once even while the body is parked on its next chunk: the producer is
        cancelled at that await (hyper drops the body future) and has unwound — its
        cleanup ran — before this raises. Cancelling the task awaiting `respond()`
        cancels the producer the same way."""
        if self._responded:
            raise RuntimeError("response already sent for this request")
        self._responded = True
        await self._conn.send_response(self, status, headers, body, trailers)

    async def send_response(self, status: int, *, headers: HeadersInput = None, end_stream: bool = False) -> SendStream:
        """Push-style response: send the head now and return a `SendStream` to write
        the body with (`send_data` / `send_trailers` / `send_reset`). `end_stream=True`
        = a bodyless response (hyper: a `Body` that `is_end_stream()` at encode time —
        `content-length: 0` unless HEAD/204/304); `False` = a body of unknown length
        follows (chunked on 1.1, close-delimited on 1.0), unless the headers carry a
        `content-length`, which the encoder honours (hyper `set_length`'s
        `existing_con_len`). hyper's h1 has no push API; this is the h1 twin of h2's
        `SendResponse::send_response -> SendStream`, over the same codec calls
        `respond()` makes, so the wire behaviour is hyper's either way."""
        if self._responded:
            raise RuntimeError("response already sent for this request")
        self._responded = True
        return await self._conn.send_response_head(self, status, headers, end_stream=end_stream)

    def detach(self) -> bytes:
        """Take over the raw connection for a protocol upgrade (WebSocket, or a custom protocol):
        stop the server's accept loop and relinquish the transport WITHOUT closing it, returning
        any bytes already read past the request head (to replay). The caller — which supplied the
        transport to `H1Server` — owns it afterwards and must drive the upgrade itself (httpunk
        sends no response). Like httpunk's 101/CONNECT tunnel hand-off, but caller-driven.

        After this, the accept loop ends (`next_request` returns None) and the server's close on
        `__aexit__` is a no-op, so the transport stays open. cf. Go's `http.Hijacker`."""
        if self._responded:
            raise RuntimeError("cannot detach: a response was already sent for this request")
        conn = self._conn
        if conn._watcher_handle is not None and not conn._watcher_done.is_set():
            # A mid-message read is parked on the transport (see `_arm_watcher`; deferred
            # past the head for requests that announce an upgrade, so it is never parked
            # while a detach is still possible). It cannot be handed to the caller — one
            # reader per transport — nor cancelled without risking bytes; hyper's poll
            # model has no such task to hand off (`Connection::into_parts`). Refuse.
            raise RuntimeError(
                "cannot detach: a mid-message read is parked on the transport (the request carried no "
                "Upgrade / was not a CONNECT); detach only upgrade requests"
            )
        self._responded = True
        self._close_window()
        # Bytes buffered past the head, plus any the watcher already read (a pipelined
        # message the caller's protocol must see) — hyper's `read_buf` in `into_parts`.
        leftover = conn._stash + self._decoder.take_buffered() + (conn._watcher_data or b"")
        conn._stash, conn._watcher_data = b"", None
        conn._detach()
        return leftover

    def __repr__(self) -> str:
        return f"ServerRequest(method={self.method!r}, target={self.target!r})"


class SendStream:
    """The body half of a push-style h1 response (returned by
    `ServerRequest.send_response`): the h1 twin of h2's `SendStream`, over the codec's
    body-framing calls (`serialize_data` / `serialize_end` / `serialize_trailers`).
    Every write is awaited for backpressure (`send_all`). One response at a time per
    connection, so a `SendStream` is single-owner and not thread-safe."""

    def __init__(self, conn, req, codec):
        self._conn = conn
        self._req = req
        self._codec = codec
        self._done = False

    async def send_data(self, chunk: bytes, end_stream: bool = False) -> None:
        """Write one body chunk; `end_stream=True` finishes the body (the chunked
        terminator for a chunked body). On a bodyless framing (HEAD / 204 / 304 — the
        encoder `body_is_eof`) chunks are DISCARDED, not an error: hyper never polls the
        body then, so an app that produces one anyway is harmless.
        Fails with `ConnectionClosedError` once the client closed its side mid-request
        (hyper: the connection errored with `IncompleteMessage`, nothing more is written)."""
        self._check_open()
        self._conn._check_peer_open(self._req)
        codec = self._codec
        try:
            if codec.body_is_eof():
                buf = codec.serialize_end() if end_stream else b""
            else:
                buf = codec.serialize_data(bytes(chunk))
                if end_stream:
                    buf += codec.serialize_end()  # one write: last chunk + terminator (hyper flushes them together)
        except BaseException as exc:
            # `serialize_end` on a body short of its Content-Length: hyper `end_body` ->
            # `User::BodyWriteAborted` + `Writing::Closed`. Same poison as a failed write.
            self._done = True
            self._req._close_window()
            self._conn._fail_response(exc)
        if end_stream:
            self._done = True
        if buf:
            await self._write(buf)
        if end_stream:
            self._conn._finish_response(self._req)

    async def send_trailers(self, trailers: HeadersInput) -> None:
        """Finish a chunked body with a trailer block (hyper `Server::encode` allow-lists
        the fields named in the response's `Trailer` header; others are dropped, and a
        non-chunked body gets a bare terminator — `Encoder::encode_trailers`)."""
        self._check_open()
        self._conn._check_peer_open(self._req)
        hdrs = trailers if isinstance(trailers, HeaderMap) else HeaderMap(trailers)
        self._done = True
        try:
            buf = self._codec.serialize_trailers(hdrs)
        except BaseException as exc:
            self._req._close_window()
            self._conn._fail_response(exc)
        await self._write(buf)
        self._conn._finish_response(self._req)

    async def send_reset(self, reason: int | None = None) -> None:
        """Abort the response. HTTP/1 has no per-stream reset frame, so this is what
        hyper does when a response `Body` errors mid-stream: the connection is closed
        (`Writing::Closed`), and the client sees a truncated body. `reason` is accepted
        for symmetry with h2's `SendStream.send_reset` and ignored."""
        if self._done:
            return
        self._done = True
        self._req._close_window()
        self._conn._reusable = False
        self._conn._closed = True
        self._conn._close_transport()

    def _check_open(self):
        if self._done:
            raise RuntimeError("response body already complete")

    async def _write(self, data):
        try:
            await self._conn.write(data)
        except BaseException as exc:
            self._done = True
            self._req._close_window()
            self._conn._fail_response(exc)


class ServerConnection(H1ConnectionBase):
    """The server-side h1 driver: reads a request, hands back a `ServerRequest`,
    then writes the response; reuses the connection on keep-alive. The accepting
    analogue of the client `Connection`, over the shared `H1ConnectionBase`."""

    def __init__(
        self,
        transport,
        *,
        backend=None,
        header_read_timeout=_DEFAULT_HEADER_READ_TIMEOUT,
        keep_alive=True,
        max_headers=None,
        max_buf_size=_DEFAULT_MAX_BUF_SIZE,
        auto_date_header=True,
        title_case_headers=False,
        ignore_invalid_headers=False,
        half_close=False,
    ):
        super().__init__(transport, backend=backend)
        self._reusable = True  # keep-alive: may we read another request after this one?
        # Mid-message EOF detection (hyper conn.rs `mid_message_detect_eof`, L491-508): while
        # a request's body is complete and its response is not, hyper keeps a read pending
        # on the connection; EOF there errors the connection (`IncompleteMessage`), bytes
        # are the next pipelined request (kept in `read_buf`). httpunk re-expresses that
        # pending read as ONE parked `receive_some` in a bare watcher task (the server twin
        # of the client's idle watcher): never cancelled — it ends by data, EOF, or transport
        # close — and its result is dispatched by state when it completes (`_watch_mid_message`).
        # If the response finishes first, the still-parked read IS the next head read
        # (`_recv_head_bytes`): a hand-off, never a second reader (one reader per transport).
        # `half_close` (hyper `http1::Builder::half_close`, default False) disables it: a
        # client FIN mid-request is then ignored until the idle read after the response.
        self._half_close = half_close
        # Guards the arm decision (`_arm_watcher`): its callers — `peer_closed()`, a
        # streamed / push response, the head negotiation — can run in different tasks on
        # different threads, and "is a read parked? if not, park one" plus the
        # want/negotiated handshake must be one step (free-threaded rule).
        self._arm_lock = threading.Lock()
        self._watcher_handle = None  # bare task handle: awaited exactly once (`_take_watcher` / `close`)
        self._watcher_done = None  # event: the watcher's single read completed
        self._watcher_data = None  # its bytes (b"" = EOF); None on error / not yet
        self._watcher_error = None  # its transport error, if any
        self._stash = b""  # pipelined bytes taken from the decoder at arm time (hyper `read_buf`)
        # hyper `http1::Builder::keep_alive(false)` -> `disable_keep_alive()` on a Busy (not yet
        # idle) connection = `KA::Disabled` (http1.rs L463, conn.rs L876): the first response
        # is encoded `Connection: close` and the connection closes after it — exactly the
        # graceful-shutdown path, applied from the start. Reading the first request is unaffected.
        self._keep_alive_enabled = keep_alive
        if max_buf_size < _MIN_MAX_BUF_SIZE:
            raise ValueError("the max_buf_size cannot be smaller than the minimum that h1 specifies.")
        self._max_buf_size = max_buf_size  # cap on a still-incomplete head (hyper io.rs `max_buf_size`)
        # Per-request codec options (hyper `http1::Builder`): `max_headers` (None = hyper's
        # 100), `ignore_invalid_headers`, `title_case_headers`, `auto_date_header`.
        self._codec_options = {
            "max_headers": max_headers,
            "ignore_invalid_headers": ignore_invalid_headers,
            "title_case_headers": title_case_headers,
            "date_header": auto_date_header,
        }
        self._codec = None  # current request/response codec
        self._current = None  # current ServerRequest (for body draining)
        self._head_raw = b""  # raw bytes of the in-progress head read (h2-preface check, F49)
        self._shutdown_evt = self.backend.event()  # set by graceful_shutdown()
        # Max time to read a complete request head before closing (slowloris defence),
        # hyper's `header_read_timeout` (default 30s, http1.rs L249). `None` disables it.
        self._header_read_timeout = header_read_timeout
        # The head-read deadline always uses `backend.timeout` (every backend implements it).
        # This capability is extra: a backend that can wake a parked read from another task
        # (asyncio) lets us skip the per-read shutdown `select`; absent (tonio) → use `select`.
        # See `_read_or_shutdown` + `graceful_shutdown`.
        self._native_read_interrupt = getattr(self.backend, "native_read_interrupt", False)
        self._idle_read_parked = False  # True only while parked in the idle between-requests read

    async def start(self):
        pass  # HTTP/1 has no connection preface / handshake

    async def close(self):
        await super().close()  # closes the transport, which is what ends a parked watcher read
        handle, self._watcher_handle = self._watcher_handle, None
        if handle is not None:
            await handle  # join (never cancel) — see `_watch_mid_message`

    # ===== mid-message watcher (hyper conn.rs `mid_message_detect_eof`) =====

    def _arm_watcher(self, req, *, want=True, head_negotiated=False):
        """Park ONE read for the mid-message window of `req` — SYNC (a bare spawn). hyper
        reads from the moment a request is mid-message (bodyless at accept, or once the
        body is consumed); httpunk arms LAZILY, when the observation can be consumed: the
        first `peer_closed()` await, a `respond()` that streams an async body, or a push
        `send_response()`. Observably equivalent — hyper uses the EOF to drop the handler,
        which httpunk cannot do; a `respond()` write to a half-closed socket succeeds
        regardless; pipelined bytes are read before the next head either way; and an EOF
        that already arrived is delivered at once by the late read — while a plain
        `respond(bytes)` exchange pays no task at all (measured ~5% of h1 GET throughput).
        Skipped when hyper would not read either (`half_close`; bytes already buffered ->
        hyper's `read_buf` is non-empty and it returns Pending), when the body is not yet
        consumed (hyper's `poll_read` serves the body first), and when a task cannot
        stand in for hyper's poll: a request announcing an upgrade (`Upgrade` / CONNECT —
        hyper's `wants_upgrade`, which any `Upgrade: h2c` request sets too) may still be
        detached or switched, and a parked reader cannot be handed to the caller
        (runtime-forced divergence, documented in `detach`). That exclusion lasts only
        until the response head is negotiated with a non-switching status: from then on
        `detach()` is refused and no tunnel hand-off can follow, so the head-send paths
        call in with `head_negotiated=True` (`req._is_switch` already set) and a
        `peer_closed()` awaited earlier is honoured then — `want` records the ask — and
        the divergence shrinks to the pre-head window of those requests.

        The whole decision runs under `_arm_lock`: the callers are concurrent tasks, and
        both the "one read parked" check and the want/negotiated handshake would race
        without it (two readers on one transport; a lost arm)."""
        with self._arm_lock:
            if head_negotiated:
                req._head_negotiated = True
            if want:
                req._watch_wanted = True
            if not req._watch_wanted:
                return
            if self._half_close or self._closed or self.transport is None or self._watcher_handle is not None:
                return
            if not req._body_done or req._response_done:
                return
            if (req.is_upgrade or req.method == "CONNECT") and not (req._head_negotiated and not req._is_switch):
                return
            if self._stash:
                return  # a probe already read the next request: hyper's `read_buf` is non-empty -> no read
            buffered = req._decoder.take_buffered()
            if buffered:
                self._stash += buffered  # the next request is already here: no read (hyper `read_buf`)
                return
            self._watcher_data = self._watcher_error = None
            self._watcher_done = self.backend.event()
            self._watcher_handle = self.backend.spawn_without_results(self._watch_mid_message(req))

    async def _watch_mid_message(self, req):
        """ONE read, dispatched by state when it completes (see `_arm_watcher`). Never
        cancelled: a parked `receive_some` ends only via data, EOF, or transport close.

        - EOF / transport error while `req`'s response is still in flight: the client is
          gone — hyper `close_read()` + `IncompleteMessage` (conn.rs L501-504). Mark the
          request peer-closed (its in-flight `respond()` / `send_data` fail, `peer_closed()`
          resolves) and the connection non-reusable. The result is also left for a
          later `next_request` (it reads EOF and ends cleanly), so a racing
          `_finish_response` that re-marks the connection reusable is harmless: the DATA
          decides, not the flag.
        - bytes: a pipelined next request. Left in `_watcher_data` for the next head read
          (hyper's `read_buf`); NOT re-armed — hyper reads once and returns Pending while
          the buffer is non-empty (L495).
        - completed after the response: it was the idle read all along; `next_request`
          takes it via `_recv_head_bytes`."""
        transport = self.transport
        if transport is None:  # a racing close nulled it before we ran
            self._watcher_done.set()
            return
        try:
            data = await transport.receive_some(_READ_SIZE)
        except Exception as exc:
            self._watcher_error = exc.with_traceback(None)  # strip frames: don't pin the connection in a cycle
            data = None
        else:
            self._watcher_data = data
        if not data and not req._response_done:
            self._reusable = False
            req._peer_closed = True
            req._peer_closed_evt.set()
        self._watcher_done.set()

    async def _take_watcher(self):
        """The watcher's read result — awaited through its completion EVENT (cancellable:
        the head-read deadline and the shutdown race may abandon this wait), then its
        handle joined exactly once. Raises the transport error the read hit, if any."""
        await self._watcher_done.wait()
        handle, self._watcher_handle = self._watcher_handle, None
        if handle is not None:
            await handle
        error, self._watcher_error = self._watcher_error, None
        if error is not None:
            raise error
        data, self._watcher_data = self._watcher_data, None
        return data

    def _recv_head_bytes(self, n):
        """The next head-read awaitable: the parked watcher's read when one is armed (the
        hand-off — never a second reader on the transport), else a fresh read."""
        if self._watcher_handle is not None:
            return self._take_watcher()
        return self.transport.receive_some(n)

    def _check_peer_open(self, req):
        # hyper: once `mid_message_detect_eof` saw EOF the connection is errored and nothing
        # more is written for this exchange. Poison + close, surface `IncompleteMessage`.
        if req._peer_closed:
            req._close_window()
            self._fail_response(H1IncompleteMessageError(_PEER_CLOSED_MSG))

    async def _send_async_body(self, req, body, trailers):
        """Stream an ASYNC response body and fail fast on a client FIN. hyper polls the
        connection (so `mid_message_detect_eof`) while the body's next chunk is pending,
        so a parked producer (SSE, long poll) does not hold a dead exchange. Same shape as
        the h2 manager's `_send_async_body`: ONE pump task per response; this caller waits
        once for "pump done" or "peer closed". On the FIN the pump is CANCELLED (hyper
        drops the body future) at its suspension — the app's `await`, or, h1 having no
        write pump, possibly inside a chunk write: harmless, this connection is closed
        right below and a truncated body is what the client gets regardless (hyper:
        `IncompleteMessage`, io dropped). Also cancelled if this task is cancelled or
        unwinds: neither backend cancels scope children on a body exception, and joining
        a parked pump would hang. (A per-chunk `select` measured 4-5x slower.)"""
        self._arm_watcher(req)  # the FIN is consumable now: make sure a read is parked
        done, box = self.backend.event(), []
        abandoned = False
        async with self.backend.scope() as scope:
            scope.spawn(self._pump_async_body(body, trailers, done, box))
            try:
                winner = await self.backend.select(
                    event_result(done, PUMP_DONE), event_result(req._peer_closed_evt, PUMP_ABANDONED)
                )
                # The event also marks the window's close (`_close_window`), which cannot
                # precede this response's own completion — `req._peer_closed` is the verdict.
                abandoned = winner is PUMP_ABANDONED and req._peer_closed and not done.is_set()
            finally:
                if not done.is_set():
                    scope.cancel()  # leave with the pump gone, whatever ended the wait
        # The scope exit does not wait for a CANCELLED child to unwind (see the h2 manager's
        # `_send_async_body`): wait for the pump's own `done`, set in its `finally` after
        # the producer unwound and the generator was closed.
        await done.wait()
        if abandoned:
            req._close_window()
            self._fail_response(H1IncompleteMessageError(_PEER_CLOSED_MSG))
        if box:
            req._close_window()
            self._fail_response(box[0])

    async def _pump_async_body(self, body, trailers, done, box):
        # A bare task: never lets an exception escape (it is reported through `box`).
        try:
            await self._send_body(self._codec, body, trailers)
        except Exception as exc:
            box.append(exc)
        finally:
            await aclose_body(body)
            done.set()

    async def graceful_shutdown(self):
        # h1 `Connection::graceful_shutdown` (hyper Dispatcher `disable_keep_alive`):
        # a non-blocking signal — stop reusing the connection so the accept loop
        # ends after the current request (`next_request` returns None once
        # `_reusable` is False), and wake a read parked idly between requests
        # (`_shutdown_evt`, checked in the head-read). The caller drives the serve
        # loop to completion and closes; nothing is awaited/closed here.
        self._reusable = False
        self._shutdown_evt.set()
        if self._native_read_interrupt and self._idle_read_parked:
            # asyncio: wake the read parked idly between requests so it returns promptly and
            # the connection closes, instead of relying on a per-read select race (tonio's path).
            self.transport.interrupt_read()

    def mark_unusable(self):
        self._reusable = False

    def _detach(self):
        # Same as the base, plus mark non-reusable (a tunnel serves no more requests).
        super()._detach()
        self._reusable = False

    async def next_request(self):
        """Read the next request head and return a `ServerRequest`, or None once
        the connection can serve no more (client closed, tunnel handed off, a
        non-keep-alive response was sent, or a parse error). Drains any unread body
        of the previous request first so the wire is positioned at the next head.

        hyper: the server `Dispatcher::poll_loop` (dispatch.rs L166) →
        `poll_read_head` (L292) → `Server::parse`; the drain mirrors
        `poll_drain_or_close_read` (conn.rs L849-865)."""
        if self._closed or not self._reusable:
            return None
        leftover = b""
        if self._current is not None:
            if not self._current._response_done:
                # hyper serializes structurally (the dispatcher won't read the next
                # head until the response is fully written, dispatch.rs L628-633 /
                # `try_keep_alive`). Surface the out-of-order use — no response, or a
                # push-style body still open — rather than mis-pairing responses.
                raise RuntimeError("respond to the current request (and finish its body) before reading the next")
            if not self._current._body_done and not self._drain_unread_body(self._current):
                return None  # body not cheaply drainable — connection closed (see below)
            # Carry any pipelined bytes (the start of the next request, buffered
            # past this request's body — or taken from it when the watcher was armed)
            # into the next codec — hyper keeps them in its persistent read buffer; a
            # fresh codec would drop them (deadlock). Bytes the watcher itself read are
            # delivered by its hand-off read (`_recv_head_bytes`).
            leftover = self._stash + self._current._decoder.take_buffered()
            self._stash = b""
        self._current = None

        codec = H1Codec(**self._codec_options)
        try:
            head = await self._read_request_head(codec, leftover)
        except H1ParseError as exc:
            self._closed = True
            self._reusable = False
            if bytes(self._head_raw[: len(_H2_PREFACE)]) == _H2_PREFACE:
                # An HTTP/2 client hit this h1-only server with the prior-knowledge
                # preface. hyper closes silently with a version error rather than
                # writing a 400 (on_parse_error -> has_h2_prefix -> new_version_h2,
                # conn.rs L809-812) (F49).
                self._close_transport()
                return None
            # Any other malformed request head: auto-respond like hyper's
            # `Server::on_error` (role.rs L481-499), then close. The app never sees
            # the `H1ParseError` — hyper's service is never invoked for it either.
            await self._send_error(codec, _error_status(exc.args[0]))
            return None
        except self.backend.broken_transport_errors:
            # The transport died at the request boundary — an RST, or a TLS
            # close without close_notify (which httpunk's own abortive
            # `close_transport` produces, F33a — every httpunk client hanging
            # up looks like this). The wire outcome is identical to the clean
            # EOF below — the connection just ends with no request to serve —
            # so it surfaces as a clean end-of-iteration, extending F47's
            # contract (hyper instead surfaces an Io error from the connection
            # future; observability-only, exactly as F47's mid-head-EOF case).
            self._closed = True
            self._reusable = False
            self._close_transport()
            return None
        if head is None:  # clean EOF between requests — client closed
            self._closed = True
            return None
        self._codec = codec
        decoder = H1BodyDecoder(head.body_kind, head.content_length or 0)
        decoder.feed(codec.take_body())  # body bytes read alongside the head
        req = ServerRequest(
            self,
            method=head.method,
            target=head.target,
            headers=head.headers,
            decoder=decoder,
            keep_alive=head.keep_alive,
            expect_continue=head.expect_continue,
            is_upgrade=head.is_upgrade,
            http10=head.http10,
            content_length=head.content_length,
        )
        self._current = req
        return req

    def _drain_unread_body(self, req):
        """Discard an unread request body so the next request parses cleanly, but
        only if it's cheap — a 1:1 mirror of hyper `poll_drain_or_close_read`
        (conn.rs L849-865), which does EXACTLY ONE `poll_read_body` (`let _ =
        self.poll_read_body(cx)`, then close unless the body reached KeepAlive). Pull a
        single frame: decode one from the already-buffered bytes, or — only if the
        buffer held nothing decodable yet (need-more, not end) — do ONE non-blocking
        socket read and decode that. Reuse iff that single poll completed the body;
        otherwise `close_read()` (never loop the socket to drain an arbitrary body,
        never send the skipped `100 Continue`).

        One poll, not a loop, is deliberate and matches hyper exactly: a chunked body
        with any data frame can't cheaply drain (one `decode()` yields one DATA chunk,
        the terminating chunk unseen → not complete → close), and a half-buffered length
        body closes (hyper's length decoder reads from IO only when its buffer is empty,
        so a partial buffer yields a short frame and gives up — decode.rs Length). The
        single non-blocking read is `backend.receive_nowait`, the equivalent of hyper's
        `poll_read_body` seeing `Pending`. Returns True if drained (reusable), else False."""
        dec = req._decoder
        try:
            if dec.decode() is None and not dec.is_complete:  # buffer had no full frame → need more
                data = self.backend.receive_nowait(self.transport, _READ_SIZE)
                if data:  # b"" == nothing ready right now, or EOF -> give up (close)
                    dec.feed(data)
                    dec.decode()
        except Exception:  # noqa: S110 - a decode failure just means "not drainable → close"
            pass
        if dec.is_complete:
            req._body_done = True
            return True
        self._closed = True
        self._reusable = False
        return False

    async def _read_request_head(self, codec, initial=b""):
        # Bound the head read by `header_read_timeout` (slowloris defence, hyper http1.rs L249):
        # if the deadline wins, close with no response (hyper closes on a header-read timeout).
        # `backend.timeout(coro, seconds) -> (result, completed)` cancels the read cleanly on
        # expiry — cheap on every backend (asyncio: one task + one timer; tonio: its native
        # timeout). `None` disables the deadline.
        if self._header_read_timeout is None:
            return await self._read_head_frames(codec, initial)
        result, completed = await self.backend.timeout(
            self._read_head_frames(codec, initial), self._header_read_timeout
        )
        if not completed:
            self._closed = True
            self._reusable = False
            self._close_transport()
            return None
        return result

    async def _read_head_frames(self, codec, initial=b""):
        # Feed any carried-over pipelined bytes before touching the transport, so a
        # request already sitting in the buffer parses without a (blocking) read.
        buffered = 0
        # Retain the raw head bytes read so far (bounded by the head-size cap below)
        # so a parse error can be checked against the HTTP/2 preface (F49).
        raw = bytearray(initial)
        self._head_raw = raw
        if initial:
            buffered += len(initial)
            head = codec.receive_request_head(initial)
            if head is not None:
                return head
        idle = not initial  # no bytes of this request seen yet -> a graceful shutdown may end it
        while True:
            if idle:
                # Between requests: a graceful shutdown may end the wait (see
                # `_read_or_shutdown` — sync; returns the sentinel or the read awaitable).
                read = self._read_or_shutdown(_READ_SIZE)
                if read is _SHUTDOWN:
                    return None
                # Flag the park as idle so graceful_shutdown() may wake THIS read only —
                # never a mid-head read (a request in flight must complete).
                self._idle_read_parked = True
                try:
                    data = await read
                finally:
                    self._idle_read_parked = False
                if data is _SHUTDOWN:  # tonio select: the shutdown signal won the race
                    return None
                # An interrupt_read shutdown wake surfaces as b"" and falls into the EOF
                # return below — same outcome either way (loop ends; close, no response).
            else:
                data = await self._recv_head_bytes(_READ_SIZE)
            if not data:
                # EOF. A clean EOF between requests (nothing buffered) is a normal
                # client close. hyper additionally distinguishes a MID-head EOF
                # (`buffered > 0`) as `Parse::Eof`/`IncompleteMessage` (an error), but
                # the wire outcome is identical either way — the connection just closes
                # with no response — so we surface both as a clean end-of-iteration
                # rather than raise (F47, observability-only; raising here would change
                # `next_request`'s clean-close contract for every host loop).
                return None
            idle = False  # a request's bytes have started arriving — don't interrupt now
            buffered += len(data)
            raw += data
            head = codec.receive_request_head(data)
            if head is not None:
                return head
            # Cap the still-incomplete head at hyper's max_buf_size (io.rs L202-205):
            # once the buffered bytes reach the limit it's `Parse::TooLarge` -> auto 431
            # + close. Without this a slow/never-terminating head stream is unbounded
            # memory per connection (F14). `_error_status` maps `too_large` -> 431.
            if buffered >= self._max_buf_size:
                raise H1ParseError("too_large", "message head is too large")

    def _read_or_shutdown(self, n):
        """The next idle-read awaitable, or `_SHUTDOWN` if a graceful shutdown was already
        requested. A plain (sync) function so the fast path adds no wrapper coroutine: the
        caller awaits the returned awaitable with `_idle_read_parked` set around it (so
        `graceful_shutdown()` wakes only an idly-parked read, never a mid-head one) and
        treats an awaited `_SHUTDOWN` — or a woken `b""` — as shutdown, not data.

        asyncio (`native_read_interrupt`): a plain read; if it parks, graceful_shutdown()
        wakes it via `interrupt_read` and the read returns b"". Backends without a native
        read interrupt (tonio can't wake a parked recv from another task): race the read
        against the shutdown signal via `select`, which cancels the losing branch — hyper's
        poll instead re-checks `should_read` and completes without reading."""
        if self._shutdown_evt.is_set():
            return _SHUTDOWN

        if self._native_read_interrupt:
            # A parked watcher's underlying read is the same transport read `interrupt_read`
            # wakes (it returns b"", which the hand-off delivers as the idle EOF).
            return self._recv_head_bytes(n)

        async def _await_shutdown():
            await self._shutdown_evt.wait()
            return _SHUTDOWN

        # With a watcher armed the racer is its completion-event wait, so the losing
        # cancel never lands on the parked read itself (which ends by the close that follows).
        return self.backend.select(self._recv_head_bytes(n), _await_shutdown())

    async def _send_error(self, codec, status):
        """Best-effort automatic error response (bodyless, `Connection: close`),
        then close — hyper `Server::on_error` + `write_head`."""
        try:
            # hyper `close_read()`s before `on_error`, so `enforce_version` inserts
            # `connection: close` on the auto response. The encoder adds no header
            # itself, so inject it here (F29) rather than emitting a bare error head.
            hdrs = self._negotiate_connection_header(HeaderMap(), keep_alive=False, http10=False, resp_close=False)
            head = codec.serialize_response(status, hdrs, keep_alive=False)
            await self.transport.send_all(bytes(head) + bytes(codec.serialize_end()))  # one write (coalesced)
        except BaseException:  # noqa: S110 - best-effort: if we can't write the 400, just close
            pass
        self._close_transport()

    async def send_response(self, req, status, headers, body, trailers=None):
        """The pull path (`ServerRequest.respond`): head + whole body (+ trailers), with
        the head+small-body write coalescing of `_send_head_and_body`. Shares head-time
        negotiation (`_prepare_head`) and completion (`_finish_response`) with the push
        path, so the two cannot drift; only the write batching differs."""
        self._check_peer_open(req)
        content_length, chunked = self._body_framing(body)
        streaming = body is not None and hasattr(body, "__aiter__")
        if trailers is not None:
            # Trailers ride only on a chunked body (RFC 9112 §7.1.2), and hyper's server
            # encoder allow-lists them against the response's `Trailer` header (role.rs
            # L856-891 -> `Kind::Chunked(Some(fields))`): force chunked framing and declare
            # the fields if the app didn't — the client's F45 treatment of `Request.trailers`.
            trailers = trailers if isinstance(trailers, HeaderMap) else HeaderMap(trailers)
            headers = headers if isinstance(headers, HeaderMap) else HeaderMap(headers)
            content_length, chunked = None, True
            if "trailer" not in headers:
                headers["trailer"] = ", ".join(trailers.keys())
        head, is_switch, keep_alive = self._prepare_head(req, status, headers, content_length, chunked)
        req._keep_alive_decision = keep_alive
        req._is_switch = is_switch
        if req.is_upgrade or req.method == "CONNECT":
            # The head is negotiated: a `peer_closed()` deferred past it arms now (`want=False`:
            # a bytes body asks for nothing itself). Plain requests never defer — no lock on
            # their hot path; a streamed body arms in `_send_async_body` regardless.
            self._arm_watcher(req, want=False, head_negotiated=True)
        if streaming:
            try:
                await self.write(head)  # the head must never wait on the app's generator
            except BaseException as exc:
                req._close_window()
                self._fail_response(exc)
            await self._send_async_body(req, body, trailers)
        else:
            try:
                await self._send_head_and_body(self._codec, head, body, trailers)
            except BaseException as exc:
                req._close_window()
                self._fail_response(exc)
        self._finish_response(req)

    async def send_response_head(self, req, status, headers, *, end_stream):
        """The push path (`ServerRequest.send_response`): write the head now, return
        the `SendStream`. `end_stream` maps onto hyper's two encode inputs the same way a
        pull body does: True = no body (`_body_framing(None)`), False = a body of unknown
        length (streamed -> chunked, or length if the headers carry `content-length`)."""
        self._check_peer_open(req)
        content_length, chunked = self._body_framing(None) if end_stream else (None, True)
        head, is_switch, keep_alive = self._prepare_head(req, status, headers, content_length, chunked)
        req._keep_alive_decision = keep_alive
        req._is_switch = is_switch
        # A push producer may park between chunks: make the FIN observable (`want`). After
        # the head negotiation, so an upgrade request answered without a switch is armed
        # too — for it, or for a `peer_closed()` deferred past the head.
        self._arm_watcher(req, want=not end_stream, head_negotiated=True)
        stream = SendStream(self, req, self._codec)
        if end_stream:
            stream._done = True
            # Same as `_send_head_and_body(body=None)`: the framing's (empty) terminator.
            head += self._codec.serialize_end()
        try:
            await self.write(head)
        except BaseException as exc:
            req._close_window()
            self._fail_response(exc)
        if end_stream:
            self._finish_response(req)
        return stream

    def _fail_response(self, exc):
        """A failed response poisons the connection (hyper `Writing::Closed` + the error
        stored on the connection -> close) and `exc` surfaces as hyper's kind for it:

        - an httpunk error is hyper's own verdict, raised as it is: the client closed
          mid-request (`H1IncompleteMessageError`), a `User` error from the encoder
          (`H1UserError`: a 1xx status, `content-length` + `transfer-encoding`, a body
          short of its Content-Length), or a write into an already-torn-down transport;
        - a transport failure is hyper `Io` -> `ConnectionClosedError` (cancellation
          reaching a write is folded in the same way — the write did not complete);
        - the app's own body-iterable exception (hyper `User::Body`) propagates as
          itself — hyper wraps it because Rust must; Python carries the instance."""
        self._reusable = False
        self._closed = True
        self._close_transport()
        if isinstance(exc, HTTPunkError):
            raise exc
        transport_errors = (OSError, *self.backend.broken_transport_errors)
        if isinstance(exc, transport_errors) or not isinstance(exc, Exception):
            raise ConnectionClosedError("failed to send response") from exc
        raise exc

    def _finish_response(self, req):
        """Completion, shared by both paths: the response is fully on the wire — apply
        the head-time reuse decision (hyper `try_keep_alive` after `Writing::KeepAlive`),
        or hand off the tunnel for a protocol switch."""
        req._close_window()
        if req._is_switch:
            # Hand the raw connection (plus any bytes already buffered past the
            # request head — the start of the tunnel) to the caller and detach.
            req.upgraded = H1Upgraded(self.transport, req._decoder.take_buffered())
            self._detach()
            return
        self._reusable = req._keep_alive_decision
        if not self._reusable:
            self._closed = True
            self._close_transport()

    def _prepare_head(self, req, status, headers, content_length, chunked):
        # hyper: role.rs `Server::encode` (L364) writes the status line + headers
        # (incl. Date) + returns the body Encoder; the driver reimplements the
        # keep-alive/version negotiation hyper does in conn.rs
        # (`enforce_version`/`fix_keep_alive`, L656-702) before that call. Returns
        # `(head_bytes, is_switch, keep_alive)`; the caller writes the head.
        hdrs = headers if headers is None or isinstance(headers, HeaderMap) else HeaderMap(headers)
        http10 = req._http10
        # A protocol switch turns the connection into a raw tunnel (Server::encode
        # L378-384 forces is_last): no reuse, and we hand the transport to the caller.
        # It's a switch only when the request actually asked for the upgrade — a 101
        # whose request had no Upgrade (hyper `wants_upgrade`, role.rs), or a 2xx to a
        # CONNECT. A 101 the request didn't ask for is NOT a tunnel; it still ends the
        # connection (handled below), it just isn't handed off (F28).
        is_switch = (status == 101 and req.is_upgrade) or (req.method == "CONNECT" and 200 <= status < 300)
        # An HTTP/1.0 iterable body is close-delimited ONLY if the app gave no
        # Content-Length: with an explicit CL the encoder frames it as length(n) (hyper
        # set_length's `existing_con_len` branch, role.rs), so the client knows where the
        # body ends and the connection stays reusable. Deciding this from the body shape
        # alone (iterable ⇒ chunked ⇒ close) wrongly forced a close + dropped the
        # keep-alive header for a CL-framed streamed 1.0 response (F27).
        close_delimited = http10 and chunked and (hdrs is None or hdrs.get("content-length") is None)
        # Reuse iff the request is keep-alive, the response doesn't ask to close,
        # the body isn't close-delimited, and we're not switching protocols. (An
        # unread request body is drained-or-closed later, in next_request — matching
        # hyper, whose drain runs in a poll_read after the response is written, so the
        # already-sent response keeps its keep-alive header and a failed drain just FINs.)
        resp_close = _connection_has(hdrs, "close")
        keep_alive = req.keep_alive and not resp_close and not close_delimited and not is_switch
        if status == 101 and not is_switch:
            keep_alive = False  # a 101 the request didn't ask for still ends the connection (hyper is_last)
        # Graceful shutdown requested mid-request: this in-flight response must
        # advertise `Connection: close` and the connection must not be reused —
        # mirroring hyper's `disable_keep_alive` (KA::Disabled), whose in-flight
        # response is encoded is_last with `connection: close` inserted by
        # enforce_version (conn.rs L682-698). Without this, the `_reusable = keep_alive`
        # below would overwrite the `False` graceful_shutdown() set and keep the
        # connection alive.
        if self._shutdown_evt.is_set() or not self._keep_alive_enabled:
            keep_alive = False
        if not is_switch:
            # A switch keeps the app's `Connection: upgrade` verbatim; otherwise
            # make the wire header agree with the reuse/version decision.
            hdrs = self._negotiate_connection_header(hdrs, keep_alive, http10, resp_close)
        try:
            head = self._codec.serialize_response(
                status,
                hdrs,
                keep_alive=keep_alive,
                http10=http10,
                content_length=content_length,
                chunked=chunked,
            )
        except BaseException as exc:
            req._close_window()
            self._fail_response(exc)
        return head, is_switch, keep_alive

    @staticmethod
    def _negotiate_connection_header(hdrs, keep_alive, http10, resp_close):
        # Reimplements hyper `fix_keep_alive`/`enforce_version` (conn.rs L656-702):
        # make the wire `Connection` header agree with the reuse + version decision.
        # `Server::encode` writes whatever header is present but adds none itself.
        # hyper uses `HeaderMap::insert` (REPLACE), so set — not add — the header:
        # appending would leave a duplicate/contradictory `Connection` token next to a
        # user-set value (F48).
        if not keep_alive and not resp_close and not http10:
            # HTTP/1.1 defaults to keep-alive → must announce the close.
            hdrs = hdrs or HeaderMap()
            hdrs["connection"] = "close"
        elif keep_alive and http10:
            # HTTP/1.0 defaults to close → must announce the keep-alive.
            hdrs = hdrs or HeaderMap()
            hdrs["connection"] = "keep-alive"
        return hdrs


class H1Server(BaseServer[ServerRequest]):
    """An HTTP/1 server connection over a caller-supplied, already-accepted
    `transport` (BYO transport, like hyper's `server::conn::http1`). The
    async-context-manager + accept-iterator come from `BaseServer` (identical to
    `H2Server`).

        async with H1Server(transport) as server:
            async for request in server:
                await request.respond(200, body=b"hi")
    """

    def __init__(
        self,
        transport: Any,
        *,
        backend: BackendLike | None = None,
        header_read_timeout: float | None = _DEFAULT_HEADER_READ_TIMEOUT,
        keep_alive: bool = True,
        max_headers: int | None = None,
        max_buf_size: int = _DEFAULT_MAX_BUF_SIZE,
        auto_date_header: bool = True,
        title_case_headers: bool = False,
        ignore_invalid_headers: bool = False,
        half_close: bool = False,
    ) -> None:
        """Options mirror hyper `server::conn::http1::Builder` (defaults are hyper's):
        `header_read_timeout` (30s; None disables), `keep_alive` (False: answer one
        request with `Connection: close` and close), `max_headers` (None = 100),
        `max_buf_size` (cap on an incomplete head, >= 8192), `auto_date_header`,
        `title_case_headers`, `ignore_invalid_headers` (skip malformed request header
        lines instead of rejecting with 400), `half_close` (True: a client that shuts
        its write side mid-request is NOT treated as gone — the response still completes;
        default False, where that EOF fails the in-flight response and `peer_closed()`)."""
        self._conn = ServerConnection(
            transport,
            backend=backend,
            header_read_timeout=header_read_timeout,
            keep_alive=keep_alive,
            max_headers=max_headers,
            max_buf_size=max_buf_size,
            auto_date_header=auto_date_header,
            title_case_headers=title_case_headers,
            ignore_invalid_headers=ignore_invalid_headers,
            half_close=half_close,
        )
