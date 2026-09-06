"""HTTP/1 server — the accepting-side analogue of the h1 client
(`connection.py` + `client.py`), and the h1 counterpart of `h2/server.py`.

The sans-IO byte work is all Rust over the vendored hyper core: `H1Codec.
receive_request_head` (head parse via `Server::parse`) + `serialize_response`
(head encode via `Server::encode`, incl. the `Date` header) + the body `Encoder`,
and `H1BodyDecoder` for the request body. The connection + request STATE is Rust
too: `ServerConnection` subclasses `H1ServerState` (src/h1/conn.rs), which holds —
under one mutex — the transport, the keep-alive / close / shutdown flags, the
mid-message watcher slot and the current request's flags (responded, response
done, body done, peer closed, the head-time decisions). Every decision is one call
into it returning a verdict; this file holds only the async machinery: the head
read and its deadline, the body pumps, the watcher task, the events
(HTTPUNK_RUST_STATE_DESIGN.md §3.2).

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

Cross-reference: hyper 1.11.1 `proto/h1/{role,conn,dispatch}.rs` (server path) and
`client/conn`/`server/conn`.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable
from typing import TYPE_CHECKING, Any

from .. import _backend
from .._common import PUMP_ABANDONED, PUMP_DONE, BaseServer, aclose_body, event_result, read_all
from .._httpunk import (
    H1_NEXT_CLOSE,
    H1_NEXT_DRAIN,
    H1_NEXT_NONE,
    H1_NEXT_RAISE,
    H1_READ_EOF,
    H1_READ_PARSE,
    H1_READ_SHUTDOWN,
    H1_READ_WATCHER,
    H1_REQ_CONTINUE,
    H1_REQ_OK,
    H1_REQ_PARKED,
    H1_REQ_PEER_CLOSED,
    H1Codec,
    H1ServerState,
)
from ..exceptions import ConnectionClosedError, H1IncompleteMessageError, H1ParseError, HTTPunkError
from ..http import HeaderMap
from ..types import Version
from .connection import H1Framing
from .share import H1Upgraded


if TYPE_CHECKING:
    from .._backend import BackendLike
    from ..types import Body, HeadersInput


_READ_SIZE = 65536
_SHUTDOWN = object()  # sentinel: a graceful shutdown released an idle head-read
# The interim response hyper's server auto-sends for `Expect: 100-continue` (conn.rs L409).
_CONTINUE = H1Codec.continue_response()
_DEFAULT_HEADER_READ_TIMEOUT = 30.0  # hyper's `header_read_timeout` default (http1.rs L249)
# The error an in-flight response gets once the client closed its side mid-request —
# hyper's `IncompleteMessage` from `mid_message_detect_eof` (conn.rs L491-508).
_PEER_CLOSED_MSG = "connection closed before message completed: the client closed its side mid-request"


def _codec_options(*, max_headers, ignore_invalid_headers, title_case_headers, auto_date_header, max_buf_size):
    # ONE codec per connection (hyper's `Conn`): its read buffer persists across
    # messages, like hyper's `read_buf`. Options are hyper `http1::Builder`'s:
    # `max_headers` (None = hyper's 100), `ignore_invalid_headers`,
    # `title_case_headers`, `auto_date_header`, `max_buf_size` (validated there, >= 8192).
    options = {
        "max_headers": max_headers,
        "ignore_invalid_headers": ignore_invalid_headers,
        "title_case_headers": title_case_headers,
        "date_header": auto_date_header,
    }
    if max_buf_size is not None:
        options["max_buf_size"] = max_buf_size
    return options


class ServerRequest:
    """An incoming request + the handle to respond to it (hyper: the request +
    its `Sender`/response channel). One in flight at a time. Its state lives in
    the connection's `H1ServerState`, keyed by `_seq` (a request outliving its
    exchange gets a deterministic answer, never the next request's state)."""

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

    def __init__(self, conn, seq, head, decoder):
        self.method = head.method  # str
        self.target = head.target  # str — the request-target (origin/absolute/authority form)
        self.path = head.target  # alias
        self.headers = head.headers  # httpunk.http.HeaderMap
        self.version = Version.HTTP_10 if head.http10 else Version.HTTP_11  # hyper `Request::version()`
        self.trailers = None  # chunked trailers, populated once the body is read
        self.keep_alive = head.keep_alive
        self.is_upgrade = head.is_upgrade
        self.content_length = head.content_length  # declared request Content-Length (None if chunked)
        # The raw tunnel once the app answers a CONNECT/Upgrade with a 101 or a 2xx
        # to CONNECT (hyper `on_upgrade`): the caller owns it and drives it directly.
        self.upgraded = None
        self._conn = conn
        self._seq = seq
        self._decoder = decoder
        # The two ends of the mid-message window `peer_closed()` observes: set by the
        # watcher on a mid-message EOF, and when the exchange completes (the state's
        # `peer_closed` flag is published first — one transition, one primitive).
        self._peer_closed_evt = conn.backend.event()
        # A `100 Continue` claimed by the body reader has been written (or failed):
        # a responder told to await it parks here, so the head never overtakes it.
        # Only an `Expect: 100-continue` request can ever claim one (the state
        # records the ask in `begin_request`), so only that request pays for the event.
        self._continue_evt = conn.backend.event() if head.expect_continue else None

    @property
    def _peer_closed(self):
        """The client closed its side mid-request (as the watcher recorded it)."""
        return self._conn.peer_closed_flag(self._seq)

    async def aiter_bytes(self) -> AsyncIterator[bytes]:
        """Yield request body chunks, pulling transport bytes on demand (decoded
        by `H1BodyDecoder`). Sends `100 Continue` first if the client asked for it."""
        if self._decoder.is_complete:
            return
        conn = self._conn
        if conn.try_send_continue(self._seq):
            # The client is waiting for a 1xx before sending the body (RFC 9110
            # §10.1.1). hyper auto-sends 100 Continue when the body is first polled,
            # but ONLY while `Writing::Init` (before any response) and ONLY for versions
            # > HTTP/1.0 (conn.rs L409-415, L311) — decided in the state, under the same
            # lock as `try_respond`; a responder that lost the race awaits our write.
            try:
                await conn.write(_CONTINUE)
            finally:
                self._continue_evt.set()
        try:
            while True:
                chunk = self._decoder.decode()
                if chunk is not None:
                    yield chunk
                    continue
                if self._decoder.is_complete:
                    break
                # ONE reader on the transport: the state refuses once the next
                # request's drain owns the read (or the request is over).
                if not conn.begin_body_read(self._seq):
                    raise ConnectionClosedError("request body no longer readable")
                try:
                    data = await conn.read_body_more()
                finally:
                    conn.end_body_read(self._seq, False)
                if data:
                    self._decoder.feed(data)
                else:
                    self._decoder.mark_eof()  # client closed mid-body
        except BaseException:
            conn.mark_unusable()
            raise
        self.trailers = self._decoder.take_trailers()
        conn.end_body_read(self._seq, True)
        # The body is consumed: a `peer_closed()` asked for before it was read arms now
        # (hyper serves the body first, then keeps its mid-message read pending).
        await conn._arm_watcher(self, want=False)

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
        now = self._conn.peer_closed_now(self._seq)
        if now is not None:
            return now
        await self._conn._arm_watcher(self)  # the signal is being consumed: park a read as soon as possible
        await self._peer_closed_evt.wait()
        return self._conn.peer_closed_flag(self._seq)

    def _close_window(self):
        """The response is complete or failed: the mid-message window is over (the
        state flipped first). Wakes a parked `peer_closed()`."""
        self._conn.close_window(self._seq)
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
        await self._conn._send_response(self, status, headers, body, trailers)

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
        return await self._conn._send_response_head(self, status, headers, end_stream=end_stream)

    def detach(self) -> bytes:
        """Take over the raw connection for a protocol upgrade (WebSocket, or a custom protocol):
        stop the server's accept loop and relinquish the transport WITHOUT closing it, returning
        any bytes already read past the request head (to replay). The caller — which supplied the
        transport to `H1Server` — owns it afterwards and must drive the upgrade itself (httpunk
        sends no response). Like httpunk's 101/CONNECT tunnel hand-off, but caller-driven.

        After this, the accept loop ends (`next_request` returns None) and the server's close on
        `__aexit__` is a no-op, so the transport stays open. cf. Go's `http.Hijacker`. Refused
        while a mid-message read is parked on the transport (see `_arm_watcher`; deferred past
        the head for requests that announce an upgrade, so it is never parked while a detach is
        still possible): one reader per transport, and hyper's poll model has no such task to
        hand off (`Connection::into_parts`)."""
        code, _transport, leftover = self._conn.detach(self._seq)
        if code == H1_REQ_PARKED:
            raise RuntimeError(
                "cannot detach: a mid-message read is parked on the transport (the request carried no "
                "Upgrade / was not a CONNECT); detach only upgrade requests"
            )
        if code != H1_REQ_OK:
            raise RuntimeError("cannot detach: a response was already sent for this request")
        self._peer_closed_evt.set()
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
            self._conn._fail_response(self._req, exc)
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
            buf = self._codec.serialize_trailers(hdrs)  # bare terminator unless the request said `TE: trailers`
        except BaseException as exc:
            self._conn._fail_response(self._req, exc)
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
        self._conn._abort_response(self._req)

    def _check_open(self):
        if self._done:
            raise RuntimeError("response body already complete")

    async def _write(self, data):
        try:
            await self._conn.write(data)
        except BaseException as exc:
            self._done = True
            self._conn._fail_response(self._req, exc)


class ServerConnection(H1Framing, H1ServerState):
    """The server-side h1 driver: reads a request, hands back a `ServerRequest`,
    then writes the response; reuses the connection on keep-alive. The accepting
    analogue of the client `Connection`. The state is the Rust `H1ServerState` base;
    the async machinery (head read + deadline, body pumps, the watcher task) is here."""

    def __new__(
        cls,
        transport,
        *,
        backend=None,
        header_read_timeout=_DEFAULT_HEADER_READ_TIMEOUT,
        keep_alive=True,
        max_headers=None,
        max_buf_size=None,
        auto_date_header=True,
        title_case_headers=False,
        ignore_invalid_headers=False,
        half_close=False,
    ):
        codec = H1Codec(
            **_codec_options(
                max_headers=max_headers,
                ignore_invalid_headers=ignore_invalid_headers,
                title_case_headers=title_case_headers,
                auto_date_header=auto_date_header,
                max_buf_size=max_buf_size,
            )
        )
        # hyper `http1::Builder::keep_alive(false)` -> `disable_keep_alive()` on a Busy (not yet
        # idle) connection = `KA::Disabled` (http1.rs L463, conn.rs L876): the first response
        # is encoded `Connection: close` and the connection closes after it. `half_close`
        # (hyper `http1::Builder::half_close`, default False) disables the mid-message watcher.
        return H1ServerState.__new__(cls, codec, transport, keep_alive=keep_alive, half_close=half_close)

    def __init__(
        self,
        transport,
        *,
        backend=None,
        header_read_timeout=_DEFAULT_HEADER_READ_TIMEOUT,
        keep_alive=True,
        max_headers=None,
        max_buf_size=None,
        auto_date_header=True,
        title_case_headers=False,
        ignore_invalid_headers=False,
        half_close=False,
    ):
        self.backend = _backend.resolve(backend)
        self._codec_options = _codec_options(
            max_headers=max_headers,
            ignore_invalid_headers=ignore_invalid_headers,
            title_case_headers=title_case_headers,
            auto_date_header=auto_date_header,
            max_buf_size=max_buf_size,
        )
        self._codec = self.codec  # the one codec the state holds (hyper's `Conn`)
        self._shutdown_evt = self.backend.event()  # set by graceful_shutdown() (the select-based race)
        # Max time to read a complete request head before closing (slowloris defence),
        # hyper's `header_read_timeout` (default 30s, http1.rs L249). `None` disables it.
        self._header_read_timeout = header_read_timeout
        # The head-read deadline always uses `backend.timeout` (every backend implements it).
        # This capability is extra: a backend that can wake a parked read from another task
        # (asyncio) lets us skip the per-read shutdown `select`; absent (tonio) → use `select`.
        self._native_read_interrupt = getattr(self.backend, "native_read_interrupt", False)

    async def start(self):
        pass  # HTTP/1 has no connection preface / handshake

    async def close(self):
        # Close the transport FIRST (what ends a parked watcher read), then JOIN the
        # watcher (never cancel it — see `_watch_mid_message`). The state hands out
        # the transport and the watcher's done event in one step.
        transport, done = self.mark_closed()
        if transport is not None:
            self.backend.close_transport(transport)
        if done is not None:
            await done.wait()
            handle = self.take_watcher_handle()
            if handle is not None:
                await handle

    # ----- transport access (the state owns the transport) -----

    def write(self, data):
        transport = self.transport_ref()
        if transport is None:
            raise ConnectionClosedError("connection closed")
        return transport.send_all(data)

    def read_body_more(self):
        """Read more transport bytes for an in-flight body. Empty bytes = EOF."""
        transport = self.transport_ref()
        if transport is None:
            raise ConnectionClosedError("connection closed")
        return transport.receive_some(_READ_SIZE)

    def _close(self, transport):
        if transport is not None:
            self.backend.close_transport(transport)

    # ===== mid-message watcher (hyper conn.rs `mid_message_detect_eof`) =====

    async def _arm_watcher(self, req, *, want=True, head_negotiated=False):
        """Park ONE read for the mid-message window of `req`. hyper reads from the moment
        a request is mid-message (bodyless at accept, or once the body is consumed);
        httpunk arms LAZILY, when the observation can be consumed: the first
        `peer_closed()` await, a `respond()` that streams an async body, or a push
        `send_response()`. Observably equivalent — hyper uses the EOF to drop the handler,
        which httpunk cannot do; a `respond()` write to a half-closed socket succeeds
        regardless; pipelined bytes are read before the next head either way; and an EOF
        that already arrived is delivered at once by the late read — while a plain
        `respond(bytes)` exchange pays no task at all. Skipped when hyper would not read
        either (`half_close`; bytes already buffered -> hyper's `read_buf` is non-empty
        and it returns Pending), when the body is not yet consumed (hyper's `poll_read`
        serves the body first), and when a task cannot stand in for hyper's poll: a
        request announcing an upgrade (`Upgrade` / CONNECT — hyper's `wants_upgrade`)
        may still be detached or switched, and a parked reader cannot be handed to the
        caller (runtime-forced divergence, documented in `detach`). That exclusion lasts
        only until the response head is negotiated with a non-switching status
        (`head_negotiated`); `want` records the ask.

        The whole decision is ONE call into the state (`arm_watcher`); the spawn happens
        here, after it, and the handle is stored — or, if the connection closed in
        between, handed back and joined right here (the watcher exits at once: the
        transport is gone). Never a second reader beside the next head read."""
        done = self.backend.event()
        if not self.arm_watcher(req._seq, done, want=want, head_negotiated=head_negotiated):
            return
        handle = self.backend.spawn_without_results(self._watch_mid_message(req, done))
        refused = self.store_watcher_handle(handle)
        if refused is not None:
            await refused

    async def _watch_mid_message(self, req, done):
        """ONE read, dispatched by state when it completes (`watcher_completed`). Never
        cancelled: a parked `receive_some` ends only via data, EOF, or transport close.

        - EOF / transport error while `req`'s response is still in flight: the client is
          gone — hyper `close_read()` + `IncompleteMessage` (conn.rs L501-504). The state
          marks the request peer-closed (its in-flight `respond()` / `send_data` fail,
          `peer_closed()` resolves) and the connection non-reusable.
        - bytes: a pipelined next request. Left in the slot for the next head read
          (hyper's `read_buf`); NOT re-armed — hyper reads once and returns Pending while
          the buffer is non-empty (L495).
        - completed after the response: it was the idle read all along; `next_request`
          takes it via `_recv_head_bytes`."""
        transport = self.transport_ref()
        if transport is None:  # a racing close took it before we ran
            self.watcher_aborted()
            done.set()
            return
        data = error = None
        try:
            data = await transport.receive_some(_READ_SIZE)
        except Exception as exc:
            error = exc.with_traceback(None)  # strip frames: don't pin the connection in a cycle
        if self.watcher_completed(data, error):
            req._peer_closed_evt.set()
        done.set()

    async def _take_watcher(self, done):
        """The watcher's read result — awaited through its completion EVENT (cancellable:
        the head-read deadline and the shutdown race may abandon this wait, with the
        handle still in its slot), then its handle joined exactly once (already done:
        no suspension, so no cancellation can strand it). Raises the transport error
        the read hit, if any."""
        await done.wait()
        handle = self.take_watcher_handle()
        if handle is not None:
            await handle
        data, error = self.take_watcher_result()
        if error is not None:
            raise error
        return b"" if data is None else data

    async def _eof(self):
        return b""

    def _recv_head_bytes(self, n):
        """The next head-read awaitable: the parked watcher's read when one is armed (the
        hand-off — never a second reader on the transport), else a fresh read."""
        done = self.watcher_done()
        if done is not None:
            return self._take_watcher(done)
        transport = self.transport_ref()
        if transport is None:
            return self._eof()  # a concurrent close: the connection is over
        return transport.receive_some(n)

    def _check_peer_open(self, req):
        # hyper: once `mid_message_detect_eof` saw EOF the connection is errored and nothing
        # more is written for this exchange. Poison + close, surface `IncompleteMessage`.
        if self.peer_closed_flag(req._seq):
            self._fail_response(req, H1IncompleteMessageError(_PEER_CLOSED_MSG))

    async def _send_async_body(self, req, body, trailers):
        """Stream an ASYNC response body and fail fast on a client FIN. hyper polls the
        connection (so `mid_message_detect_eof`) while the body's next chunk is pending,
        so a parked producer (SSE, long poll) does not hold a dead exchange. Same shape as
        the h2 driver's `_send_async_body`: ONE pump task per response; this caller waits
        once for "pump done" or "peer closed". On the FIN the pump is CANCELLED (hyper
        drops the body future) at its suspension — the app's `await`, or, h1 having no
        write pump, possibly inside a chunk write: harmless, this connection is closed
        right below and a truncated body is what the client gets regardless (hyper:
        `IncompleteMessage`, io dropped). Also cancelled if this task is cancelled or
        unwinds: neither backend cancels scope children on a body exception, and joining
        a parked pump would hang. (A per-chunk `select` measured 4-5x slower.)"""
        await self._arm_watcher(req)  # the FIN is consumable now: make sure a read is parked
        done, box = self.backend.event(), []
        abandoned = False
        async with self.backend.scope() as scope:
            scope.spawn(self._pump_async_body(body, trailers, done, box))
            try:
                winner = await self.backend.select(
                    event_result(done, PUMP_DONE), event_result(req._peer_closed_evt, PUMP_ABANDONED)
                )
                # The event also marks the window's close (`_close_window`), which cannot
                # precede this response's own completion — the state's flag is the verdict.
                abandoned = winner is PUMP_ABANDONED and self.peer_closed_flag(req._seq) and not done.is_set()
            finally:
                if not done.is_set():
                    scope.cancel()  # leave with the pump gone, whatever ended the wait
        # The scope exit does not wait for a CANCELLED child to unwind (see the h2 driver's
        # `_send_async_body`): wait for the pump's own `done`, set in its `finally` after
        # the producer unwound and the generator was closed.
        await done.wait()
        if abandoned:
            self._fail_response(req, H1IncompleteMessageError(_PEER_CLOSED_MSG))
        if box:
            self._fail_response(req, box[0])

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
        # non-reusable), and wake a read parked idly between requests: asyncio can
        # wake it from another task (`interrupt_read`, decided under the state's
        # lock with the park flag); tonio races the read against `_shutdown_evt`.
        # The caller drives the serve loop to completion and closes; nothing is
        # awaited/closed here.
        transport = self.request_shutdown(self._native_read_interrupt)
        self._shutdown_evt.set()
        if transport is not None:
            transport.interrupt_read()

    async def next_request(self):
        """Read the next request head and return a `ServerRequest`, or None once
        the connection can serve no more (client closed, tunnel handed off, a
        non-keep-alive response was sent, or a parse error). Drains any unread body
        of the previous request first so the wire is positioned at the next head.

        hyper: the server `Dispatcher::poll_loop` (dispatch.rs L166) →
        `poll_read_head` (L292) → `Server::parse`; the drain mirrors
        `poll_drain_or_close_read` (conn.rs L849-865)."""
        code, obj = self.begin_read()
        if code == H1_NEXT_RAISE:
            # hyper serializes structurally (the dispatcher won't read the next head
            # until the response is fully written, dispatch.rs L628-633 /
            # `try_keep_alive`). Surface the out-of-order use — no response, or a
            # push-style body still open — rather than mis-pairing responses.
            raise RuntimeError("respond to the current request (and finish its body) before reading the next")
        if code == H1_NEXT_DRAIN:
            code, obj = self.drain_done(self._drain_unread_body())
        if code == H1_NEXT_CLOSE:
            self._close(obj)  # body not cheaply drainable — connection closed
            return None
        if code == H1_NEXT_NONE:
            return None
        codec = self._codec
        try:
            accepted = await self._read_request_head(code, obj)
        except H1ParseError:
            # hyper conn.rs `on_parse_error`: an HTTP/2 preface is `Parse::VersionH2` and
            # the connection just closes; any other malformed head gets `Server::on_error`'s
            # automatic response (400 / 414 / 431), then closes. The app never sees the
            # `H1ParseError` — hyper's service is never invoked for it either.
            transport = self.fail_read()
            status = codec.parse_error_status
            if status is None or transport is None:
                self._close(transport)
                return None
            await self._send_error(transport, codec, status)
            return None
        except self.backend.broken_transport_errors:
            # The transport died at the request boundary — an RST, or a TLS
            # close without close_notify (which httpunk's own abortive
            # `close_transport` produces, F33a). The wire outcome is identical to
            # the clean EOF below, so it surfaces as a clean end-of-iteration (F47).
            self._close(self.fail_read())
            return None
        if accepted is None:  # clean EOF between requests (or a shutdown released the idle read)
            self.stop_serving()
            return None
        head, seq, decoder = accepted  # the head is the current request (`accept_head`)
        return ServerRequest(self, seq, head, decoder)

    def _drain_unread_body(self):
        """Discard an unread request body so the next request parses cleanly, but
        only if it's cheap — a 1:1 mirror of hyper `poll_drain_or_close_read`
        (conn.rs L849-865), which does EXACTLY ONE `poll_read_body` (`let _ =
        self.poll_read_body(cx)`, then close unless the body reached KeepAlive). Pull a
        single frame: decode one from the already-buffered bytes, or — only if the
        buffer held nothing decodable yet (need-more, not end) — do ONE non-blocking
        socket read and decode that. Reuse iff that single poll completed the body;
        otherwise `close_read()` (never loop the socket to drain an arbitrary body,
        never send the skipped `100 Continue`). The state gave this drain the transport
        read (`NEXT_DRAIN`): no body reader can start beside it. Returns whether the
        body completed."""
        dec = self.current_decoder()
        transport = self.transport_ref()
        if dec is None or transport is None:
            return False
        try:
            if dec.decode() is None and not dec.is_complete:  # buffer had no full frame → need more
                if self.watcher_parked:
                    return False  # a read is parked on the transport: never a second reader beside it
                data = self.backend.receive_nowait(transport, _READ_SIZE)
                if data:  # b"" == nothing ready right now, or EOF -> give up (close)
                    dec.feed(data)
                    dec.decode()
        except Exception:  # noqa: S110 - a decode failure just means "not drainable → close"
            pass
        return dec.is_complete

    async def _read_request_head(self, code, obj):
        # Bound the head read by `header_read_timeout` (slowloris defence, hyper http1.rs L249):
        # if the deadline wins, close with no response (hyper closes on a header-read timeout).
        # `backend.timeout(coro, seconds) -> (result, completed)` cancels the read cleanly on
        # expiry — cheap on every backend (asyncio: one task + one timer; tonio: its native
        # timeout). `None` disables the deadline.
        if self._header_read_timeout is None:
            return await self._read_head_frames(code, obj)
        result, completed = await self.backend.timeout(self._read_head_frames(code, obj), self._header_read_timeout)
        if not completed:
            self._close(self.fail_read())
            return None
        return result

    async def _read_head_frames(self, code, obj):
        """Read + parse the next head from `begin_read`'s verdict: `READ_PARSE` = bytes of
        it are already buffered (pipelined), parse before touching the transport;
        `READ_WATCHER` / `READ_TRANSPORT` = the idle between-requests read (`obj` = the
        parked watcher's done event, or the transport); `READ_SHUTDOWN` / `READ_EOF` =
        nothing to read. Each read's bytes go to `accept_head`, which parses and — once
        the head is complete — makes it the current request in the same step. Returns
        `(head, seq, decoder)`, or None on EOF / a shutdown wake."""
        if code == H1_READ_PARSE:
            accepted = self.accept_head(b"")
            if accepted is not None:
                return accepted
            code = None  # a partial head: keep reading — a request in flight must complete
        while True:
            if code is None:
                data = await self._recv_head_bytes(_READ_SIZE)
            elif code in (H1_READ_SHUTDOWN, H1_READ_EOF):
                return None
            else:
                # Between requests: a graceful shutdown may end the wait. The park was
                # flagged in the state by the same step that chose this read (`begin_read`),
                # so `graceful_shutdown()` may wake THIS read only — never a mid-head read.
                try:
                    data = await self._idle_read(code, obj)
                finally:
                    self.unpark_idle_read()
                if data is _SHUTDOWN:  # tonio select: the shutdown signal won the race
                    return None
                # An interrupt_read shutdown wake surfaces as b"" and falls into the EOF
                # return below — same outcome either way (loop ends; close, no response).
            if not data:
                # EOF. A clean EOF between requests (nothing buffered) is a normal
                # client close. hyper additionally distinguishes a MID-head EOF
                # (`buffered > 0`) as `Parse::Eof`/`IncompleteMessage` (an error), but
                # the wire outcome is identical either way — the connection just closes
                # with no response — so we surface both as a clean end-of-iteration
                # rather than raise (F47, observability-only).
                return None
            code = None  # a request's bytes have started arriving — don't interrupt now
            # The codec caps a still-incomplete head at `max_buf_size` (hyper io.rs
            # L202-207): past it, `Parse::TooLarge` -> auto 431 + close.
            accepted = self.accept_head(data)
            if accepted is not None:
                return accepted

    def _idle_read(self, code, obj):
        """The idle-read awaitable for a `READ_WATCHER` (`obj` = the parked watcher's done
        event: the hand-off — never a second reader on the transport) or `READ_TRANSPORT`
        (`obj` = the transport) verdict. A plain (sync) function so the fast path adds no
        wrapper coroutine.

        asyncio (`native_read_interrupt`): the read itself; if it parks, graceful_shutdown()
        wakes it via `interrupt_read` and the read returns b"" (a parked watcher's
        underlying read is that same transport read). Backends without a native read
        interrupt (tonio can't wake a parked recv from another task): race the read against
        the shutdown signal via `select`, which cancels the losing branch — hyper's poll
        instead re-checks `should_read` and completes without reading."""
        read = self._take_watcher(obj) if code == H1_READ_WATCHER else obj.receive_some(_READ_SIZE)
        if self._native_read_interrupt:
            return read

        async def _await_shutdown():
            await self._shutdown_evt.wait()
            return _SHUTDOWN

        # With a watcher armed the racer is its completion-event wait, so the losing
        # cancel never lands on the parked read itself (which ends by the close that follows).
        return self.backend.select(read, _await_shutdown())

    async def _send_error(self, transport, codec, status):
        """Best-effort automatic error response (bodyless, `Connection: close`),
        then close — hyper `Server::on_error` + `write_head`."""
        try:
            # `keep_alive=False`: hyper `close_read()`s before `on_error`, so `enforce_version`
            # inserts `connection: close` on the automatic response (F29).
            head = codec.serialize_response(status, None, keep_alive=False)
            await transport.send_all(codec.serialize_head_and_body(head))  # one write (coalesced)
        except BaseException:  # noqa: S110 - best-effort: if we can't write the 400, just close
            pass
        self._close(transport)

    # ===== sending responses (hyper conn.rs encode_head / role.rs Server::encode) =====

    async def _send_response(self, req, status, headers, body, trailers=None):
        """The pull path (`ServerRequest.respond`): head + whole body (+ trailers), with
        the head+small-body write coalescing of `_send_head_and_body`. Shares head-time
        negotiation (`_prepare_head`) and completion (`_finish_response`) with the push
        path, so the two cannot drift; only the write batching differs. The peer-closed
        check is the claim's (`_claim_response`), which precedes this."""
        content_length, chunked = self._body_framing(body)
        streaming = body is not None and hasattr(body, "__aiter__")
        if trailers is not None and not isinstance(trailers, HeaderMap):
            # As hyper: the framing is the body's (a known-length body cannot carry
            # them) and only fields the response's own `Trailer` header declared are
            # emitted — `Encoder::encode_trailers` drops the rest.
            trailers = HeaderMap(trailers)
        # The head step: claim, verdicts, encode, the hand-off + reuse decisions recorded,
        # and a `peer_closed()` deferred past the head (an upgrade request) arms now
        # (`want=False`: a bytes body asks for nothing itself; a streamed body arms in
        # `_send_async_body` regardless).
        head = await self._respond_head(req, status, headers, content_length, chunked, want=False)
        if streaming:
            try:
                await self.write(head)  # the head must never wait on the app's generator
            except BaseException as exc:
                self._fail_response(req, exc)
            await self._send_async_body(req, body, trailers)
        else:
            try:
                await self._send_head_and_body(self._codec, head, body, trailers)
            except BaseException as exc:
                self._fail_response(req, exc)
        self._finish_response(req)

    async def _send_response_head(self, req, status, headers, *, end_stream):
        """The push path (`ServerRequest.send_response`): write the head now, return
        the `SendStream`. `end_stream` maps onto hyper's two encode inputs the same way a
        pull body does: True = no body (`_body_framing(None)`), False = a body of unknown
        length (streamed -> chunked, or length if the headers carry `content-length`).
        The peer-closed check is the claim's (`_claim_response`), which precedes this."""
        content_length, chunked = self._body_framing(None) if end_stream else (None, True)
        # A push producer may park between chunks: make the FIN observable (`want`).
        head = await self._respond_head(req, status, headers, content_length, chunked, want=not end_stream)
        stream = SendStream(self, req, self._codec)
        if end_stream:
            stream._done = True
            # Same as `_send_head_and_body(body=None)`: the framing's (empty) terminator.
            head += self._codec.serialize_end()
        try:
            await self.write(head)
        except BaseException as exc:
            self._fail_response(req, exc)
        if end_stream:
            self._finish_response(req)
        return stream

    async def _respond_head(self, req, status, headers, content_length, chunked, *, want):
        """The head step, shared by the pull and push paths — hyper conn.rs `encode_head`
        (server) as ONE transition (`respond_head`): claim the response, read the verdicts
        the claim must see (a client gone mid-request; a `100 Continue` in flight),
        `enforce_version` + `Server::encode` in the codec with hyper's `wants_keep_alive()`,
        the hand-off + reuse decisions recorded from the encoder's verdicts, and the arm
        decision (`want`: a streamed / push body asks for the read). Then the spawn the
        decision asked for (see `_arm_watcher`). Returns the encoded head.

        A `100 Continue` claimed by the body reader goes out first: the claim stands and
        the head step re-runs once its write is done (the request's continue event). A
        client that closed mid-request fails the exchange before anything is written
        (`mid_message_detect_eof` -> `IncompleteMessage`); an encoder `User` error (a 1xx
        status, content-length + transfer-encoding) fails it after the claim, as hyper's."""
        hdrs = headers if headers is None or isinstance(headers, HeaderMap) else HeaderMap(headers)
        done = self.backend.event()
        while True:
            try:
                code, head, armed = self.respond_head(
                    req._seq, status, hdrs, done, content_length=content_length, chunked=chunked, want=want
                )
            except BaseException as exc:
                self._fail_response(req, exc)
            if code == H1_REQ_OK:
                break
            if code == H1_REQ_CONTINUE:
                await req._continue_evt.wait()
                continue
            if code == H1_REQ_PEER_CLOSED:
                self._fail_response(req, H1IncompleteMessageError(_PEER_CLOSED_MSG))
            raise RuntimeError("response already sent for this request")
        if armed:
            handle = self.backend.spawn_without_results(self._watch_mid_message(req, done))
            refused = self.store_watcher_handle(handle)
            if refused is not None:
                await refused
        return head

    def _fail_response(self, req, exc):
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
        self._close(self.fail_response(req._seq))  # the window closed in the same step
        req._peer_closed_evt.set()
        if isinstance(exc, HTTPunkError):
            raise exc
        transport_errors = (OSError, *self.backend.broken_transport_errors)
        if isinstance(exc, transport_errors) or not isinstance(exc, Exception):
            raise ConnectionClosedError("failed to send response") from exc
        raise exc

    def _abort_response(self, req):
        """`SendStream.send_reset`: hyper closes the connection when a response `Body`
        errors mid-stream (`Writing::Closed`); the client sees a truncated body."""
        self._close(self.fail_response(req._seq))
        req._peer_closed_evt.set()

    def _finish_response(self, req):
        """Completion, shared by both paths: the response is fully on the wire — apply
        the head-time reuse decision (hyper `try_keep_alive` after `Writing::KeepAlive`),
        or hand off the tunnel for a protocol switch. One step in the state; the
        `peer_closed()` waiters wake after it."""
        switch, close, transport, leftover = self.finish_response(req._seq)
        req._peer_closed_evt.set()
        if switch:
            # The raw connection (plus any bytes already buffered past the request head —
            # the start of the tunnel) is the caller's; the state detached.
            req.upgraded = H1Upgraded(transport, leftover)
        elif close:
            self._close(transport)


class H1Server(BaseServer[ServerRequest]):
    """An HTTP/1 server connection over a caller-supplied, already-accepted
    `transport` (BYO transport, like hyper's `server::conn::http1`). The
    async-context-manager + accept-iterator come from `BaseServer` (identical to
    `H2Server`).

        async with H1Server(transport) as server:
            async for request in server:
                await request.respond(200, body=b"hi")
    """

    def _prime(self, data: bytes) -> None:
        """`util.auto`'s seam (hyper-util's `Rewind`, without the wrapper transport): the
        bytes the protocol sniff already read — the start of the request line — go into
        the codec's read buffer (hyper's persistent `read_buf`), so the driver reads the
        raw transport from the first head on. Before the first read: single-owner."""
        self._conn.codec.feed(data)

    def __init__(
        self,
        transport: Any,
        *,
        backend: BackendLike | None = None,
        header_read_timeout: float | None = _DEFAULT_HEADER_READ_TIMEOUT,
        keep_alive: bool = True,
        max_headers: int | None = None,
        max_buf_size: int | None = None,
        auto_date_header: bool = True,
        title_case_headers: bool = False,
        ignore_invalid_headers: bool = False,
        half_close: bool = False,
    ) -> None:
        """Options mirror hyper `server::conn::http1::Builder` (defaults are hyper's):
        `header_read_timeout` (30s; None disables), `keep_alive` (False: answer one
        request with `Connection: close` and close), `max_headers` (None = 100),
        `max_buf_size` (cap on an incomplete head, >= 8192; None = hyper's default), `auto_date_header`,
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
