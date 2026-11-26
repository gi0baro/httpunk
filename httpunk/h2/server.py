"""HTTP/2 server — the accepting-side analogue of the client (`client.py` +
`connection.py`), mirroring h2's `server.rs`.

Only the async orchestration is Python: the Rust `H2Streams(role="server")`
holds every piece of connection and stream state (the request streams, flow
control, SETTINGS, resets, the accept-queue bookkeeping of the Rapid-Reset
defence, the graceful-shutdown phases). `ServerConnection` adds the accept
queue and the response-send glue over the shared `H2ConnectionBase`.

Low-level by design (like `hyper::server::conn`): one connection over a
caller-supplied, already-accepted transport; the caller accepts the socket, does
TLS/ALPN, and runs its own accept loop. Usage:

    async with H2Server(transport) as server:
        async for request in server:            # each is a ServerRequest
            body = await request.read()
            await request.respond(200, headers={"content-type": "text/plain"}, body=b"hi")

Requests arrive as they are opened; a caller may `spawn` a handler per request to
serve them concurrently (h2 multiplexes). `request.respond` sends the response,
whose body is flow-control-gated on the client's windows.

Cross-reference: `h2 ...` comments cite hyperium/h2 0.4.21.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable
from typing import TYPE_CHECKING, Any

from .._common import BaseServer, read_all
from .._httpunk import H2Streams
from ..exceptions import ConnectionClosedError, H2KeepAliveTimeoutError, H2Reason, _reason
from ..http import HeaderMap
from ..types import Version
from .connection import H2ConnectionBase


if TYPE_CHECKING:
    from .._backend import BackendLike
    from ..types import Body, HeadersInput


_DEFAULT_MAX_CONCURRENT = 200  # hyper server SETTINGS_MAX_CONCURRENT_STREAMS (proto/h2/server.rs)
_REMOTE_RESET_MAX = 20  # h2 proto/mod.rs DEFAULT_REMOTE_RESET_STREAM_MAX (Rapid-Reset cap)
_LOCAL_ERROR_RESET_MAX = 1024  # hyper proto/h2/server.rs DEFAULT_MAX_LOCAL_ERROR_RESET_STREAMS
# hyper's HTTP/2 server profile (hyper `proto/h2/server.rs`): a 1 MB per-stream recv
# window and 1 MB connection recv window (vs the 65535 default), 16 KB max frame size,
# 16 KB max header-list size. The server does NOT advertise
# ENABLE_PUSH (it is the client's setting to gate server push).
_STREAM_WINDOW = 1024 * 1024
_CONN_WINDOW = 1024 * 1024
_MAX_FRAME_SIZE = 16 * 1024
_MAX_HEADER_LIST_SIZE = 16 * 1024
_MAX_SEND_BUF_SIZE = 400 * 1024  # hyper proto/h2/server.rs DEFAULT_MAX_SEND_BUF_SIZE (per-stream queued DATA cap)
_KEEP_ALIVE_TIMEOUT = 20.0  # hyper proto/h2/server.rs:74


class ServerRequest:
    """An incoming request + the handle to respond to it (h2: the `(Request,
    SendResponse)` pair yielded by the server `Connection`)."""

    method: str
    scheme: str | None
    authority: str | None  # the :authority pseudo-header
    path: str | None  # the :path pseudo-header
    target: str | None  # alias of path, symmetric with the client's Request.target
    headers: HeaderMap
    version: Version  # always HTTP_2 (h2 stamps it on every request, server.rs L1680)

    __slots__ = ("method", "scheme", "authority", "path", "target", "headers", "version", "_stream", "_conn")

    def __init__(self, stream, conn, frame):
        self.method = frame.method  # str, e.g. "GET"
        self.scheme = frame.scheme  # str | None
        self.authority = frame.authority  # str | None (the :authority pseudo-header)
        self.path = frame.path  # str | None (the :path pseudo-header)
        self.target = frame.path  # alias, symmetric with the client's Request.target
        self.headers = frame.headers  # httpunk.http.HeaderMap
        self.version = Version.HTTP_2
        self._stream = stream
        self._conn = conn

    @property
    def trailers(self) -> HeaderMap | None:
        """Trailing request headers (a `HeaderMap`) if the client sent a trailers
        frame after the body, else None."""
        return self._stream.trailers

    def aiter_bytes(self) -> AsyncIterator[bytes]:
        """Yield request body chunks as they arrive; each consumed chunk releases
        recv-window capacity (-> WINDOW_UPDATE), mirroring the client's read side."""
        return self._conn._aiter_body(self._stream)

    def read(self) -> Awaitable[bytes]:
        return read_all(self.aiter_bytes())

    def respond(
        self, status: int, *, headers: HeadersInput = None, body: Body = None, trailers: HeadersInput = None
    ) -> Awaitable[None]:
        """Send the whole response: HEADERS (+ body, flow-control-gated) (+ a trailing
        HEADERS frame). `body` is None, `bytes`, or a (sync/async) iterable of `bytes`.
        The pull convenience over `send_response` (h2: `SendResponse::send_response` +
        hyper's `PipeToSendStream`). Connection-specific fields are stripped from
        `headers` (hyper `strip_connection_headers`), not from `trailers`: those are
        validated (RFC 9113 §8.2.2) before anything is sent, so a rejected call leaves
        the stream untouched. As hyper's, a body of known size not at its end (`bytes`, or
        none with `trailers`) declares its length as `content-length` unless `headers`
        carry one; an empty `bytes` body without `trailers` declares none and, like no
        body, ends the stream on HEADERS.

        Raises `StreamResetError` with the client's reason if it reset the stream (also
        after its own END_STREAM, the cancelled-GET case). With an async body that is
        raised at once even while the body is parked on its next chunk: the producer is
        cancelled at that await (hyper drops the body future) and has unwound — its
        cleanup ran — before this raises."""
        return self._conn._send_response(self._stream, status, headers, body, trailers)

    async def send_response(
        self, status: int, *, headers: HeadersInput = None, end_stream: bool = False, detect_eof: bool = True
    ) -> SendStream:
        """Push-style response: send HEADERS now and return a `SendStream` to write the
        body with (`send_data` / `send_trailers` / `send_reset`). `end_stream=True` ends
        the stream on the HEADERS frame (a bodyless response). h2:
        `SendResponse::send_response(response, end_of_stream) -> SendStream`.
        Connection-specific fields are stripped from `headers` (hyper
        `strip_connection_headers`).

        `detect_eof` is accepted for API symmetry with the HTTP/1 twin and has no effect
        here: a client abandoning the request is a RST_STREAM the connection's pump
        delivers regardless (`reset_received`), there is no per-response read to arm."""
        await self._conn._send_response_head(self._stream, status, headers, end_stream=end_stream)
        stream = SendStream(self._conn, self._stream)
        if end_stream:
            stream._done = True
        return stream

    async def reset(self, error_code: int | None = None) -> None:
        """Abort this stream with RST_STREAM instead of a normal response — e.g. when a
        handler fails. Defaults to INTERNAL_ERROR; a no-op once the response has already
        been sent. Only this stream is affected (h2 `SendResponse` drop / `send_reset`)."""
        reason = int(H2Reason.INTERNAL_ERROR) if error_code is None else int(error_code)
        self._conn._reset(self._stream, reason)

    async def reset_received(self) -> H2Reason | int:
        """Wait until the client abandons this request and return the reason of its
        RST_STREAM (or of a GOAWAY that dropped the stream) — the coroutine form of h2
        `SendResponse::poll_reset` / `SendStream::poll_reset` (state.rs `ensure_reason`).
        Observable even after the request body's END_STREAM was received (a GET, or a
        fully uploaded body): only this tells the handler the client is gone. Never
        resolves for a request that completes normally (hyper's `poll_reset` stays
        Pending), so a host races it against its own completion. If the connection died
        with no reason (transport error / EOF) the connection error is raised."""
        st = self._stream
        await st.reset_evt.wait()
        stop = st.stop
        if stop is not None and stop.reason is not None:
            return _reason(stop.reason)
        raise self._conn._stopped_error(st, stop)

    def __repr__(self) -> str:
        return f"ServerRequest(method={self.method!r}, path={self.path!r})"


class SendStream:
    """The body half of a push-style response (returned by `ServerRequest.send_response`)
    — h2 share.rs `SendStream`: `send_data(data, end_of_stream)`, `send_trailers`,
    `send_reset`. Every DATA write is flow-control-gated (awaited for backpressure), and
    END_STREAM rides the final frame the caller marks. Single-owner by contract (one
    producer per response), like h2's `SendStream`."""

    __slots__ = ("_conn", "_stream", "_done")

    def __init__(self, conn, stream):
        self._conn = conn
        self._stream = stream
        self._done = False

    async def send_data(self, chunk: bytes, end_stream: bool = False) -> None:
        """Send one DATA frame's worth of body (split to the peer's max frame size and
        gated on the send window); `end_stream=True` marks the last frame END_STREAM and
        closes the send half (h2 `SendStream::send_data`). An empty chunk is sent as an
        empty DATA frame, as h2 does."""
        self._check_open()
        if end_stream:
            self._done = True
        await self._conn._send_data(self._stream, bytes(chunk), end_stream)

    async def send_trailers(self, trailers: HeadersInput) -> None:
        """End the stream with a trailing HEADERS frame (h2 `SendStream::send_trailers`);
        connection-specific fields are rejected (RFC 9113 §8.2.2, h2 `check_headers`):
        hyper strips them from heads only."""
        self._check_open()
        hdrs = trailers if isinstance(trailers, HeaderMap) else HeaderMap(trailers)
        self._conn.check_send_headers(hdrs)
        self._done = True
        self._conn._send_trailers(self._stream, hdrs)

    async def send_reset(self, reason: int | None = None) -> None:
        """Abort the stream with RST_STREAM (h2 `SendStream::send_reset`); defaults to
        CANCEL — "the stream is no longer needed" (RFC 9113 §7). A no-op once the
        response is complete or the stream is already closed."""
        if self._done:
            return
        self._done = True
        code = int(H2Reason.CANCEL) if reason is None else int(reason)
        self._conn._reset(self._stream, code)

    def _check_open(self):
        if self._done:
            raise RuntimeError("response body already complete")


class ServerConnection(H2ConnectionBase):
    """The server protocol driver: accepts requests and sends responses. All the
    read-pump / dispatch / SETTINGS / GOAWAY machinery is the shared
    `H2ConnectionBase`; the state (incl. the last-processed stream reported in
    GOAWAY, the Rapid-Reset accept-queue cap, the graceful phases) is Rust."""

    def __new__(
        cls,
        transport,
        *,
        backend=None,
        max_concurrent_streams,
        initial_window_size=None,
        data_frame_budget=None,
        initial_connection_window_size=None,
        max_frame_size=None,
        max_header_list_size=None,
        max_pending_accept_reset_streams=None,
        max_local_error_reset_streams=_LOCAL_ERROR_RESET_MAX,
        auto_date_header=True,
        max_send_buf_size=None,
        header_table_size=None,
        keep_alive_interval=None,
        keep_alive_timeout=_KEEP_ALIVE_TIMEOUT,
    ):
        # Our advertised windows / frame / header-list profile, defaulting to hyper's
        # (proto/h2/server.rs L36-40). `None` = hyper's default (its builders take
        # `Into<Option<_>>`). The Rust constructor validates the RFC ranges at
        # construction, not at the first SETTINGS write.
        return H2Streams.__new__(
            cls,
            "server",
            initial_window_size=initial_window_size if initial_window_size is not None else _STREAM_WINDOW,
            connection_window=(
                initial_connection_window_size if initial_connection_window_size is not None else _CONN_WINDOW
            ),
            max_frame_size=max_frame_size if max_frame_size is not None else _MAX_FRAME_SIZE,
            max_header_list_size=max_header_list_size if max_header_list_size is not None else _MAX_HEADER_LIST_SIZE,
            # hyper `http2::Builder::max_send_buf_size` (proto/h2/server.rs DEFAULT_MAX_SEND_BUF_SIZE, 400 KB).
            max_send_buf_size=max_send_buf_size if max_send_buf_size is not None else _MAX_SEND_BUF_SIZE,
            max_concurrent_streams=max_concurrent_streams,
            # hyper server/conn/http2.rs:275; proto/h2/server.rs:72: None = not advertised.
            header_table_size=header_table_size,
            # hyper `max_pending_accept_reset_streams`: None = h2's default (20).
            max_pending_accept_reset_streams=(
                max_pending_accept_reset_streams if max_pending_accept_reset_streams is not None else _REMOTE_RESET_MAX
            ),
            # hyper `max_local_error_reset_streams`: default Some(1024); None = NO limit (not advised).
            max_local_error_reset_streams=max_local_error_reset_streams,
            data_frame_budget=data_frame_budget,
            # hyper `http2::Builder::auto_date_header` (proto/h2/server.rs `date_header`, default true).
            auto_date_header=auto_date_header,
            keep_alive_interval=keep_alive_interval,
            keep_alive_timeout=keep_alive_timeout,
            # hyper proto/h2/server.rs:167-169: the server always pings while idle.
            keep_alive_while_idle=True,
        )

    def __init__(
        self,
        transport,
        *,
        backend=None,
        max_concurrent_streams,
        initial_window_size=None,
        initial_connection_window_size=None,
        max_frame_size=None,
        max_header_list_size=None,
        auto_date_header=True,
        keep_alive_interval=None,
        **_options,
    ):
        super().__init__(transport, backend=backend, keep_alive=keep_alive_interval is not None)
        # The resolved profile, for introspection (immutable; the state holds the copy it uses).
        self._max_concurrent_streams = max_concurrent_streams
        self._initial_window_size = initial_window_size if initial_window_size is not None else _STREAM_WINDOW
        self._initial_connection_window_size = (
            initial_connection_window_size if initial_connection_window_size is not None else _CONN_WINDOW
        )
        self._max_frame_size = max_frame_size if max_frame_size is not None else _MAX_FRAME_SIZE
        self._max_header_list_size = max_header_list_size if max_header_list_size is not None else _MAX_HEADER_LIST_SIZE
        self._auto_date_header = auto_date_header
        # Delivery queue of incoming ServerRequests to the accept loop; `None` ends it.
        self._incoming_send, self._incoming_recv = self.backend.queue()

    async def start(self):
        # h2: server.rs `handshake` (L370) — the server's connection preface is just
        # its SETTINGS (RFC 7540 §3.5); no readiness wait (it serves requests as they
        # arrive). The client's 24-byte preface is consumed by the state's `receive`.
        await self._begin()

    # ----- role glue -----

    def _on_request(self, st, frame, eof):
        self._incoming_send.send(ServerRequest(st, self, frame))
        if eof:
            st.body_send.send(None)  # recv_open already closed the recv half: no request body

    def _on_conn_done(self):
        self._incoming_send.send(None)  # end the accept loop

    def _on_stop_accepting(self):
        # A server-initiated graceful drain completed: signal "no more requests" so
        # `next_request` returns None and the caller's `async for` exits.
        self._incoming_send.send(None)

    def _on_keep_alive_timeout(self):
        # hyper proto/h2/server.rs:354-357 `abrupt_shutdown(NO_ERROR)` (h2
        # proto/connection.rs:495-502): GOAWAY, and every stream failed.
        # DIVERGENCE: h2 fails every stream with the GOAWAY's NO_ERROR (h2
        # proto/connection.rs:500-501), so hyper marks them reset with NO_ERROR; httpunk
        # stores `H2KeepAliveTimeoutError`, which a handler's body read, `respond` and
        # `reset_received` raise, naming the cause (maintainer's decision).
        # DIVERGENCE: hyper's serve future returns the keep-alive error
        # (proto/h2/server.rs:326-328); the accept loop just ends, as for every other
        # failure: `H2Server` does not report why a connection ended (maintainer's
        # decision).
        # DIVERGENCE: h2 reads nothing more once this GOAWAY is written, and hyper drops
        # the IO (h2 proto/connection.rs:371-383); here the owner closes it, and until it
        # does the read pump still handles what the client sends (maintainer's decision).
        self._after(self.send_goaway(int(H2Reason.NO_ERROR)))
        self._fail(H2KeepAliveTimeoutError("keep-alive timed out"))  # hyper proto/h2/ping.rs:510

    async def next_request(self):
        req = await self._incoming_recv.receive()
        if req is not None:
            # Accepted: it no longer occupies the accept queue (h2 `dec_num_remote_reset_streams`).
            self.accepted(req._stream.id)
        return req

    async def graceful_shutdown(self):
        # h2 `Connection::go_away_gracefully` (proto/connection.rs L688): a TWO-PHASE,
        # non-blocking shutdown. PHASE 1 — GOAWAY(2^31-1, NO_ERROR) + a shutdown PING,
        # keep accepting AND serving (a request the client already put on the wire
        # before it saw our GOAWAY is served, not refused). PHASE 2 fires on the PING's
        # ack (`recv_ping`). Idempotent; does NOT wait or close.
        self._after(self.begin_graceful_shutdown())

    # ----- sending responses (h2 server.rs SendResponse::send_response) -----

    async def _send_response(self, st, status, headers, body, trailers=None):
        """The pull path (`ServerRequest.respond`): HEADERS + the whole body (+ trailers),
        built on the push primitives so the two paths cannot drift."""
        if trailers is not None:
            trailers = self._headermap(trailers)
            # Validate BEFORE the response HEADERS go out (h2 send.rs:80-97).
            self.check_send_headers(trailers)
        # hyper proto/h2/server.rs:517-530: END_STREAM rides HEADERS when
        # `body.is_end_stream()`: no body (`Empty`) or an empty `Full`, trailers none.
        end_stream = trailers is None and (body is None or (isinstance(body, (bytes, bytearray)) and not body))
        # hyper proto/h2/server.rs:517-521: the exact size of a body not at its end
        # (`Empty` / `Full` with trailers pending, a non-empty `Full`) is declared.
        if isinstance(body, (bytes, bytearray)) and (body or trailers is not None):
            content_length = len(body)
        elif body is None and trailers is not None:
            content_length = 0
        else:
            content_length = None
        await self._send_response_head(st, status, headers, end_stream=end_stream, content_length=content_length)
        if not end_stream:
            try:
                await self._send_body(st, body, trailers)
            except Exception as exc:
                self._body_failed(st, exc)
                raise

    async def _send_response_head(self, st, status, headers, *, end_stream, content_length=None):
        # One locked step (h2 `SendResponse::send_response`, state.rs `send_open`):
        # hyper's connection-header strip and `content-length` (`content_length`: the
        # exact size of a body not at its end; the push path knows none), the transition,
        # the HPACK encode and the queue append — and for a bodyless response the
        # completion: HEADERS closed the send half, and the request's recv half goes
        # with it (h2 drops the request's RecvStream once the response is sent: an unread
        # request body is RST_STREAM(NO_ERROR)ed, the nginx-compat rule, and its
        # in-flight window returned, `release_closed_capacity`; `handle` = the reader to
        # notify). A stream the peer reset while the handler was computing surfaces as
        # the stop (hyper's first `poll_reset` window, proto/h2/server.rs L458).
        v = self.send_response_head(st.id, status, self._headermap(headers), end_stream, content_length)
        stop = v.stopped
        if stop is not None:
            if stop.reason is None and not stop.conn and st.stop is None:
                raise ConnectionClosedError("stream already closed")  # completed / cancelled locally
            raise self._stopped_error(st, stop)
        if v.flags:
            self._after(v.flags)
        if v.handle is not None:
            self._notify_reset(v.handle, v.reader_stop)


# DIVERGENCE: initial_window_size keeps its public API name; hyper's corresponding
# option is initial_stream_window_size (server/conn/http2.rs:162).
class H2Server(BaseServer[ServerRequest]):
    """An HTTP/2 server connection over a caller-supplied, already-accepted
    `transport` (BYO transport, like hyper's `server::conn::http2`; accepting the
    socket / TLS / ALPN and the accept loop are the caller's job). The
    async-context-manager + accept-iterator come from `BaseServer` (identical to
    `H1Server`).

    Use as an async context manager and iterate incoming requests:

        async with H2Server(transport) as server:
            async for request in server:
                await request.respond(200, body=b"hi")
    """

    def _prime(self, data: bytes) -> None:
        """`util.auto`'s seam (hyper-util's `Rewind`, without the wrapper transport): the
        bytes the protocol sniff already read — the client preface — go into the
        decoder, so the driver reads the raw transport from the first frame on."""
        if self._conn.prime(data) is False:
            raise ValueError("primed bytes are not an HTTP/2 client connection preface")

    def __init__(
        self,
        transport: Any,
        *,
        backend: BackendLike | None = None,
        max_concurrent_streams: int = _DEFAULT_MAX_CONCURRENT,
        initial_window_size: int | None = None,
        data_frame_budget: int | None = None,
        initial_connection_window_size: int | None = None,
        max_frame_size: int | None = None,
        max_header_list_size: int | None = None,
        max_pending_accept_reset_streams: int | None = None,
        max_local_error_reset_streams: int | None = _LOCAL_ERROR_RESET_MAX,
        auto_date_header: bool = True,
        max_send_buf_size: int | None = None,
        header_table_size: int | None = None,
        keep_alive_interval: float | None = None,
        keep_alive_timeout: float = _KEEP_ALIVE_TIMEOUT,
    ) -> None:
        """Options mirror hyper `server::conn::http2::Builder` (defaults are hyper's; `None`
        = default unless noted): `max_concurrent_streams` (200), `initial_window_size`
        (hyper `initial_stream_window_size`, 1 MB), `initial_connection_window_size` (1 MB),
        `max_frame_size` (16 KB, RFC range enforced), `max_header_list_size` (16 KB),
        `max_pending_accept_reset_streams` (h2's 20), `max_local_error_reset_streams`
        (1024; None = NO limit), `auto_date_header`, `max_send_buf_size` (400 KB: per-stream
        cap on response DATA queued for the connection's writer — the sender awaits room,
        as it awaits flow-control window); `header_table_size` (None = not advertised;
        hyper server/conn/http2.rs:275); `keep_alive_interval` (seconds, None = off) sends
        a PING after that long without reading a request, a body chunk or a pong, idle
        or not; when the PING is not answered within `keep_alive_timeout` (20 s), a
        GOAWAY is sent, every request in progress raises `H2KeepAliveTimeoutError` and
        the accept loop ends. Both are finite, nonnegative seconds, and at least 1 s: a
        shorter value is raised to it. Plus h2's own `data_frame_budget` (None = Auto)."""
        self._conn = ServerConnection(
            transport,
            backend=backend,
            max_concurrent_streams=max_concurrent_streams,
            initial_window_size=initial_window_size,
            data_frame_budget=data_frame_budget,
            initial_connection_window_size=initial_connection_window_size,
            max_frame_size=max_frame_size,
            max_header_list_size=max_header_list_size,
            max_pending_accept_reset_streams=max_pending_accept_reset_streams,
            max_local_error_reset_streams=max_local_error_reset_streams,
            auto_date_header=auto_date_header,
            max_send_buf_size=max_send_buf_size,
            header_table_size=header_table_size,
            keep_alive_interval=keep_alive_interval,
            keep_alive_timeout=keep_alive_timeout,
        )
