"""Low-level HTTP/2 client — h2: client.rs (`SendRequest` + `handshake`) over the
shared connection driver.

`Connection` is the client driver (`H2ConnectionBase` over the Rust
`H2Streams(role="client")`: the initiating side allocates ids and gates on the
peer's MAX_CONCURRENT_STREAMS, all inside the state); `H2Connection` is the
public per-connection handle, the Python analogue of hyper's
`client::conn::http2`: `http2::handshake` + the spawned `Connection` driver +
`SendRequest` collapsed into one async-context-managed object. Low-level by
design — no pool, connector, or high-level client.

Cross-reference: `h2 ...` comments cite hyperium/h2 0.4.20.
"""

from __future__ import annotations

import contextlib
from collections.abc import Awaitable
from typing import TYPE_CHECKING, Any

from .._common import BaseClientConnection
from .._httpunk import H2Codec, H2Streams
from ..exceptions import ConnectionClosedError, fresh_exc
from ..types import Response, Version
from .connection import H2ConnectionBase
from .share import H2ResponseBody
from .stream import Stream


if TYPE_CHECKING:
    from .._backend import BackendLike
    from ..types import Request


# hyper's HTTP/2 client profile (hyper `proto/h2/client.rs`): a 2 MB per-stream recv
# window and a 5 MB connection recv window (vs the 65535 protocol default), 16 KB max
# frame size, 16 KB max header-list size — the hyper *stack's* tuned profile, not
# bare-h2's empty-SETTINGS defaults. The connection window is
# raised by an initial WINDOW_UPDATE(0) (SETTINGS can't carry it).
_STREAM_WINDOW = 2 * 1024 * 1024
_CONN_WINDOW = 5 * 1024 * 1024
_MAX_FRAME_SIZE = 16 * 1024
_MAX_HEADER_LIST_SIZE = 16 * 1024
_MAX_SEND_BUF_SIZE = 1024 * 1024  # hyper proto/h2/client.rs DEFAULT_MAX_SEND_BUF_SIZE (per-stream queued DATA cap)
_LOCAL_ERROR_RESET_MAX = 1024  # hyper proto/h2/client.rs:98
_REMOTE_RESET_MAX = 20  # h2 proto/mod.rs:34


class Connection(H2ConnectionBase):
    """The client protocol driver. Created and driven by the public `H2Connection`."""

    def __new__(
        cls,
        transport,
        *,
        authority=None,
        scheme="http",
        backend=None,
        initial_window_size=None,
        data_frame_budget=None,
        max_send_buf_size=None,
        header_table_size=None,
        initial_connection_window_size=None,
        max_concurrent_streams=None,
        max_frame_size=_MAX_FRAME_SIZE,
        max_header_list_size=_MAX_HEADER_LIST_SIZE,
        max_local_error_reset_streams=_LOCAL_ERROR_RESET_MAX,
        max_pending_accept_reset_streams=None,
        max_concurrent_reset_streams=None,
        reset_stream_duration=None,
    ):
        # The Rust state takes the role + the hyper profile; the async members are
        # added in `__init__`. We advertise SETTINGS_ENABLE_PUSH=0 + our window/frame/
        # header-list profile; h2 counts.rs:106-115 initializes the DATA budget, then
        # `set_target_connection_window_size` resolves Auto (None) from the target window.
        return H2Streams.__new__(
            cls,
            "client",
            initial_window_size=initial_window_size if initial_window_size is not None else _STREAM_WINDOW,
            # hyper proto/h2/client.rs:83-135: always set the windows/header-list limit;
            # advertise the optional SETTINGS only when configured.
            connection_window=(
                initial_connection_window_size if initial_connection_window_size is not None else _CONN_WINDOW
            ),
            max_frame_size=max_frame_size,
            max_header_list_size=max_header_list_size,
            header_table_size=header_table_size,
            max_concurrent_streams=max_concurrent_streams,
            max_local_error_reset_streams=max_local_error_reset_streams,
            max_pending_accept_reset_streams=(
                max_pending_accept_reset_streams if max_pending_accept_reset_streams is not None else _REMOTE_RESET_MAX
            ),
            max_concurrent_reset_streams=max_concurrent_reset_streams,
            reset_stream_duration=reset_stream_duration,
            # hyper `client::conn::http2::Builder::max_send_buf_size` (1 MB).
            max_send_buf_size=max_send_buf_size if max_send_buf_size is not None else _MAX_SEND_BUF_SIZE,
            data_frame_budget=data_frame_budget,
            enable_push=False,
        )

    def __init__(self, transport, *, authority=None, scheme="http", backend=None, **_options):
        super().__init__(transport, backend=backend)
        # `authority`/`scheme` build the :authority/:scheme pseudo-headers for a
        # bare-path request (h2 takes :scheme from the request URI; `util.connect`
        # passes "https" for a TLS-dialed connection).
        self.authority = authority
        self.scheme = scheme
        # Signalled once the peer's initial SETTINGS have been applied (or the
        # connection failed), so requests respect the peer's limits from the first one.
        self._ready_evt = self.backend.event()
        # Wakes the MAX_CONCURRENT_STREAMS waiters (a slot freed, the limit changed, a
        # GOAWAY / failure to re-check).
        self._slot_evt = self.backend.event()

    # ----- role glue -----

    def _signal_ready(self):
        self._ready_evt.set()

    def _on_slot_freed(self):
        self._slot_evt.set()

    def _on_conn_done(self):
        self._ready_evt.set()  # unblock connect() if the handshake never completed
        self._slot_evt.set()  # wake open/ready waiters (they re-check the stored condition)

    async def connect(self):
        # h2: client.rs `handshake` (L1250) — over the caller-supplied transport,
        # flush the client preface + our initial SETTINGS, spawn the driver, then
        # wait for the peer's initial SETTINGS before we're ready for requests.
        await self._begin()
        # DIVERGENCE: client.rs L1381 returns without waiting for peer SETTINGS.
        # Keep this synchronization so connect/close cannot race the coroutine pump's handshake.
        await self._ready_evt.wait()
        err = self.conn_error()
        if err is not None:
            raise fresh_exc(err) from err

    # ----- opening (h2: client.rs send_request -> streams.rs send_request) -----

    async def _acquire_slot(self):
        """Block until a MAX_CONCURRENT_STREAMS slot is free, then claim it (h2
        counts.rs `inc_num_send_streams` gated by `can_inc_num_send_streams`). A GOAWAY
        / failure that arrives while parked fails this request PROMPTLY. The
        waiter idiom: try, clear, try again, wait."""
        while not self.try_claim_slot():
            self._raise_if_dead()
            self._slot_evt.clear()
            self._raise_if_dead()
            if self.try_claim_slot():
                return
            await self._slot_evt.wait()

    async def _open_stream(self, method, target, headers, *, end_stream, is_head):
        """Gate on MAX_CONCURRENT_STREAMS, then open the stream — id allocation,
        `send_open`, insert, HPACK encode + queue as ONE locked step (ids must be
        strictly increasing on the wire). h2 streams.rs `send_request` (L265)."""
        # Reject connection-specific request headers before ANY stream/slot state is
        # touched (h2 send.rs `send_headers` -> `check_headers`): a caller error.
        self.check_send_headers(headers)
        self._raise_if_dead()
        await self._acquire_slot()
        st = Stream(self.backend)
        try:
            sid = self.open_stream(
                method, target, headers, end_stream, is_head, st, scheme=self.scheme, authority=self.authority
            )
        except Exception:
            self._slot_evt.set()  # the claimed slot was released inside: wake a waiter
            raise
        if sid is None:  # the connection failed / GOAWAY'd meanwhile (the slot was released)
            self._slot_evt.set()
            self._raise_if_dead()
            raise ConnectionClosedError("connection closed")
        st.id = sid
        self._write_evt.set()
        return st

    async def _wait_until_ready(self):
        """Wait until a new stream can be opened: the connection is alive (not failed,
        no GOAWAY) and a slot is free (h2 `SendRequest::ready`). Non-reserving, like
        h2's `poll_ready`: `send_request` re-applies the backpressure."""
        while True:
            self._raise_if_dead()
            if self.can_open():
                return
            self._slot_evt.clear()
            self._raise_if_dead()
            if self.can_open():
                return
            await self._slot_evt.wait()

    def _send_body_background(self, stream, body, trailers=None):
        """Send the request body concurrently with (not before) the caller awaiting
        the response head: h2's `SendStream` (body) and `ResponseFuture` (head) are
        independent — an early response that resets the request body (413 / redirect
        during upload) must still deliver the received response. hyper client.rs
        `poll_pipe`: the pipe is polled once inline, and only if that leaves it pending
        is it handed to the executor as a bare task that outlives `send_request` (full
        duplex) and ends on its own — the body sent, or the stream stopped; `close()`
        waits for the last one (`release_writers`), it never cancels them."""
        offset = 0
        if body is None or isinstance(body, bytes):
            offset = self._send_body_inline(stream, body, trailers)
            if offset is None:
                return
        if self.writer_start():  # refused: the stream is stopped already, nothing to write
            self.backend.spawn_without_results(self._write_body(stream, body, trailers, offset))

    def _send_body_inline(self, stream, body, trailers):
        """The inline first poll of a buffered body (hyper's `Full`: one chunk, EOS
        set): queue what the windows take NOW. Returns the offset a background writer
        resumes from, or None when nothing is left to write — the body (and trailers)
        fully queued, or the stream already stopped, which the response reader sees."""
        try:
            offset = self._try_send_data(stream, body, 0, trailers is None) if body else None
            if offset is None and trailers is not None:
                self._send_trailers(stream, trailers)
        except Exception:
            return None
        return offset

    async def _write_body(self, stream, body, trailers, offset):
        # The stream may error/reset mid-send: that is surfaced when the response body
        # is read; a background writer has nowhere to propagate to.
        try:
            with contextlib.suppress(Exception):
                await self._send_body(stream, body, trailers, offset)
        finally:
            if self.writer_end():
                self._writers_evt.set()


# DIVERGENCE: hyper client/conn/http2.rs:365 offers initial_max_send_streams, but
# connect() waits for peer SETTINGS (see Connection.connect), so an assumed initial
# concurrency limit is never observable and is not offered.
# DIVERGENCE: initial_window_size keeps its public API name; hyper's corresponding
# option is initial_stream_window_size (client/conn/http2.rs:334).
class H2Connection(BaseClientConnection):
    """An HTTP/2 client connection over a caller-supplied, already-connected
    `transport` (BYO transport, like hyper's `client::conn::http2::handshake(io)`;
    dialing / TLS / ALPN are the caller's or `httpunk.util`'s job). Use as an
    async context manager; the driver's pumps run for the lifetime of the
    `async with` block, and the transport is closed on exit.

    `authority` (e.g. ``"example.com:443"``) builds the :authority pseudo-header
    for requests given a bare path; requests with an absolute-URL target carry
    their own authority. `__aenter__`/`__aexit__`/`request` come from
    `BaseClientConnection` (identical to `H1Connection`).
    """

    def __init__(
        self,
        transport: Any,
        *,
        authority: str | None = None,
        scheme: str = "http",
        backend: BackendLike | None = None,
        initial_window_size: int | None = None,
        data_frame_budget: int | None = None,
        max_send_buf_size: int | None = None,
        header_table_size: int | None = None,
        initial_connection_window_size: int | None = None,
        max_concurrent_streams: int | None = None,
        max_frame_size: int | None = _MAX_FRAME_SIZE,
        max_header_list_size: int = _MAX_HEADER_LIST_SIZE,
        max_local_error_reset_streams: int | None = _LOCAL_ERROR_RESET_MAX,
        max_pending_accept_reset_streams: int | None = None,
        max_concurrent_reset_streams: int | None = None,
        reset_stream_duration: float | None = None,
    ) -> None:
        """Options mirror hyper client/conn/http2.rs:334-555 (defaults from
        proto/h2/client.rs:83-104). `initial_window_size` (2 MB) and
        `initial_connection_window_size` (5 MB) accept None for the hyper default;
        both must be at most 2**31 - 1. `header_table_size` and
        `max_concurrent_streams` default to None (not advertised); the latter limits
        peer-initiated streams, not requests sent by this client.
        `max_frame_size` defaults to 16 KB (range 16,384..16,777,215); None omits
        the setting. `max_header_list_size` defaults to 16 KB.
        `max_local_error_reset_streams` defaults to 1024; None disables the limit.
        `max_pending_accept_reset_streams`, `max_concurrent_reset_streams`, and
        `reset_stream_duration` default to None for h2's defaults (20, 50, and 1 s).
        `reset_stream_duration` is a finite, nonnegative number of seconds.
        `max_send_buf_size` (1 MB) caps per-stream request DATA queued for the
        connection's writer; the body sender awaits room as it awaits flow-control
        window. `data_frame_budget` is h2's own option (None = Auto)."""
        self._conn = Connection(
            transport,
            authority=authority,
            scheme=scheme,
            backend=backend,
            initial_window_size=initial_window_size,
            data_frame_budget=data_frame_budget,
            max_send_buf_size=max_send_buf_size,
            header_table_size=header_table_size,
            initial_connection_window_size=initial_connection_window_size,
            max_concurrent_streams=max_concurrent_streams,
            max_frame_size=max_frame_size,
            max_header_list_size=max_header_list_size,
            max_local_error_reset_streams=max_local_error_reset_streams,
            max_pending_accept_reset_streams=max_pending_accept_reset_streams,
            max_concurrent_reset_streams=max_concurrent_reset_streams,
            reset_stream_duration=reset_stream_duration,
        )

    def ready(self) -> Awaitable[None]:
        """Wait until the connection can accept a new request — it's alive (not
        failed, no GOAWAY received) and a MAX_CONCURRENT_STREAMS slot is free —
        then return. Raises if the connection has failed or the peer sent GOAWAY.
        Mirrors h2's `SendRequest::ready` (client.rs L406). Best-effort /
        non-reserving: `send_request` re-applies the same backpressure."""
        return self._conn._wait_until_ready()

    @property
    def closed(self) -> bool:
        """True once the connection can serve no more requests — the driver failed or
        the peer sent GOAWAY. A synchronous liveness check so a pool can evict a dead
        shared connection (util.Singleton self-heal)."""
        return self._conn.is_closed()

    @property
    def busy(self) -> bool:
        """h2 multiplexes: there is no single in-flight slot an interrupted release
        could leave held, so an h2 connection is never `busy` in the h1 sense (the pool
        checks `closed or busy` before parking a connection, `util.pool`)."""
        return False

    async def send_request(self, request: Request) -> Response:
        """Send `request` and return its `Response` once the head arrives.

        h2: client.rs `SendRequest::send_request` (L517). Open the stream + send
        HEADERS, stream the body, then await the response head.
        """
        # A bodyless request carries END_STREAM on HEADERS (h2 `send_request` with
        # `end_of_stream`), not a trailing empty DATA frame; a statically-empty body is
        # bodyless too. A HEAD request's response never has a body regardless of
        # content-length.
        bodyless = request.body is None or (isinstance(request.body, (bytes, bytearray)) and len(request.body) == 0)
        # Validate trailers BEFORE opening the stream (h2 send.rs:80-104): they are sent by
        # the detached background body writer, whose errors are suppressed — a late
        # reject there would silently leave the stream half-open.
        if request.trailers is not None:
            self._conn.check_send_headers(request.trailers)
        # Trailers ride a trailing HEADERS frame (END_STREAM) AFTER the body.
        end_stream = bodyless and request.trailers is None
        is_head = H2Codec.method_is_head(request.method)
        stream = await self._conn._open_stream(
            request.method, request.target, request.headers, end_stream=end_stream, is_head=is_head
        )
        if not end_stream:
            self._conn._send_body_background(stream, request.body, request.trailers)

        await stream.headers_evt.wait()
        # h2's `ResponseFuture` resolves the moment the head arrives. Gate on whether the
        # head arrived (`stream.status`), NOT on the connection error: a fully-received
        # response must still be returned when the connection closes right after (a
        # subsequent stream/connection error surfaces when the body is read).
        # REGRESSION GUARD: do not re-add a post-head `if conn.error: raise` here.
        if stream.status is None:  # woken by a reset/failure, not a real response head
            stop = stream.stop
            if stop is not None:
                raise self._conn._stopped_error(stream, stop)
            err = self._conn.conn_error()
            if err is not None:
                raise fresh_exc(err) from err
            raise ConnectionClosedError("connection closed before response")
        # h2 stamps every response `Version::HTTP_2` (h2 client.rs L1726).
        return Response(stream.status, stream.headers, H2ResponseBody(stream, self._conn), version=Version.HTTP_2)
