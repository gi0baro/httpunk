"""HTTP/2 server — the accepting-side analogue of the client
(`client.py` + `connection.py` + `streams.py`), mirroring h2's `server.rs`.

Only the orchestration is new: the vendored codec (`H2Codec("server")`), the
stream state machine (`H2StreamState`), flow control (`H2FlowControl`) and the
SETTINGS sync (`settings.py`) are the same sans-IO core the client uses — the
codec is symmetric, so a server *receives* requests (HEADERS with `:method`/
`:path`) and *sends* responses (`serialize_response_headers`) with no Rust
changes. `ServerStreamManager` subclasses the role-agnostic `StreamManager`
(streams.py) — flow control, reset handling, SETTINGS application and per-frame
recv dispatch are all inherited (h2 keeps this in one place shared by both
roles); only the server role hooks + the response send live here.

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

Cross-reference: `h2 ...` comments cite hyperium/h2 v0.4.15.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable
from typing import TYPE_CHECKING, Any

from .._common import BaseServer, read_all
from .._httpunk import http_date
from ..exceptions import (
    ConnectionClosedError,
    H2ProtocolError,
    H2Reason,
    _reason,
    fresh_exc,
)
from ..http import HeaderMap
from ..types import Version
from .connection import PREFACE, H2ConnectionBase
from .settings import LocalSettings, Settings
from .stream import Stream
from .streams import _RESET_STREAM_SECS, StreamManager, _StreamError


if TYPE_CHECKING:
    from .._backend import BackendLike
    from ..types import Body, HeadersInput


_DEFAULT_MAX_CONCURRENT = 200  # hyper server SETTINGS_MAX_CONCURRENT_STREAMS (proto/h2/server.rs)
_REMOTE_RESET_MAX = 20  # h2 proto/mod.rs DEFAULT_REMOTE_RESET_STREAM_MAX (Rapid-Reset cap)
_LOCAL_ERROR_RESET_MAX = 1024  # hyper proto/h2/server.rs DEFAULT_MAX_LOCAL_ERROR_RESET_STREAMS
_MIN_MAX_FRAME_SIZE = 16_384  # RFC 9113 §6.5.2 SETTINGS_MAX_FRAME_SIZE range (h2 frame/settings.rs asserts)
_MAX_MAX_FRAME_SIZE = (1 << 24) - 1
# hyper's HTTP/2 server profile (hyper `proto/h2/server.rs`): a 1 MB per-stream recv
# window and 1 MB connection recv window (vs the 65535 default), 16 KB max frame size,
# 16 KB max header-list size. We ship the hyper stack's tuned profile, not bare-h2
# defaults (AUDIT-2026-07-09 F24). Unlike the old cut, the server does NOT advertise
# ENABLE_PUSH (no upstream does — it is the client's setting to gate server push).
_STREAM_WINDOW = 1024 * 1024
_CONN_WINDOW = 1024 * 1024
_MAX_FRAME_SIZE = 16 * 1024
_MAX_HEADER_LIST_SIZE = 16 * 1024
_MAX_SEND_BUF_SIZE = 400 * 1024  # hyper proto/h2/server.rs DEFAULT_MAX_SEND_BUF_SIZE (per-stream queued DATA cap)
_MAX_STREAM_ID = 2**31 - 1  # h2 StreamId::MAX — the phase-1 graceful GOAWAY last-stream-id
_SHUTDOWN_PING = b"SHUTDOWN"  # opaque payload of the graceful-shutdown PING (h2 Ping::SHUTDOWN)


class ServerRequest:
    """An incoming request + the handle to respond to it (h2: the `(Request,
    SendResponse)` pair yielded by the server `Connection`)."""

    method: str
    scheme: str | None
    authority: str | None  # the :authority pseudo-header
    path: str | None  # the :path pseudo-header
    target: str | None  # alias of path, symmetric with the client's Request.target
    headers: HeaderMap
    version: Version  # always HTTP_2 (h2 stamps it on every request, server.rs L1676)

    def __init__(self, stream, manager, *, method, scheme, authority, path, headers):
        self.method = method  # str, e.g. "GET"
        self.scheme = scheme  # str | None
        self.authority = authority  # str | None (the :authority pseudo-header)
        self.path = path  # str | None (the :path pseudo-header)
        self.target = path  # alias, symmetric with the client's Request.target
        self.headers = headers  # httpunk.http.HeaderMap
        self.version = Version.HTTP_2
        self._stream = stream
        self._manager = manager

    @property
    def trailers(self) -> HeaderMap | None:
        """Trailing request headers (a `HeaderMap`) if the client sent a trailers
        frame after the body, else None."""
        return self._stream.trailers

    async def aiter_bytes(self) -> AsyncIterator[bytes]:
        """Yield request body chunks as they arrive; each consumed chunk releases
        recv-window capacity (-> WINDOW_UPDATE), mirroring the client's read side."""
        while True:
            item = await self._stream.body_recv.receive()
            if item is None:  # EOF (end of stream, reset, or connection failure)
                break
            chunk, budgeted = item  # h2 0.4.19 DataEvent: (payload, is_budgeted)
            if budgeted:
                self._manager.release_data_frame(self._stream, len(chunk))  # return its buffering charge (#935)
            await self._manager.release_capacity(self._stream, len(chunk))
            yield chunk
        if self._stream.error is not None:
            raise fresh_exc(self._stream.error) from self._stream.error  # a copy per raise (exceptions.fresh_exc)

    def read(self) -> Awaitable[bytes]:
        return read_all(self.aiter_bytes())

    def respond(
        self, status: int, *, headers: HeadersInput = None, body: Body = None, trailers: HeadersInput = None
    ) -> Awaitable[None]:
        """Send the whole response: HEADERS (+ body, flow-control-gated) (+ a trailing
        HEADERS frame). `body` is None, `bytes`, or a (sync/async) iterable of `bytes`.
        The pull convenience over `send_response` (h2: `SendResponse::send_response` +
        hyper's `PipeToSendStream`, whose body may end with trailers). `trailers` are
        validated (RFC 9113 §8.2.2) before anything is sent, like the client's
        `Request.trailers`, so a rejected call leaves the stream untouched.

        Raises `StreamResetError` with the client's reason if it reset the stream (also
        after its own END_STREAM, the cancelled-GET case). With an async body that is
        raised at once even while the body is parked on its next chunk; the producer is
        then still parked inside its own await — close or wake whatever it awaits (a
        channel, an event, an upstream body) and it is finished and closed for you."""
        return self._manager.send_response(self._stream, status, headers, body, trailers)

    async def send_response(self, status: int, *, headers: HeadersInput = None, end_stream: bool = False) -> SendStream:
        """Push-style response: send HEADERS now and return a `SendStream` to write the
        body with (`send_data` / `send_trailers` / `send_reset`). `end_stream=True` ends
        the stream on the HEADERS frame (a bodyless response). h2:
        `SendResponse::send_response(response, end_of_stream) -> SendStream`."""
        await self._manager.send_response_head(self._stream, status, headers, end_stream=end_stream)
        stream = SendStream(self._manager, self._stream)
        if end_stream:
            stream._done = True
        return stream

    def reset(self, error_code: int | None = None) -> Awaitable[None]:
        """Abort this stream with RST_STREAM instead of a normal response — e.g. when a
        handler fails. Defaults to INTERNAL_ERROR; a no-op once the response has already
        been sent. Only this stream is affected (h2 `SendResponse` drop / `send_reset`)."""
        reason = int(H2Reason.INTERNAL_ERROR) if error_code is None else int(error_code)
        return self._manager.reset_stream(self._stream, reason)

    async def reset_received(self) -> H2Reason | int:
        """Wait until the client abandons this request and return the reason of its
        RST_STREAM (or of a GOAWAY that dropped the stream) — the coroutine form of h2
        `SendResponse::poll_reset` / `SendStream::poll_reset` (state.rs `ensure_reason`).
        Observable even after the request body's END_STREAM was received, which is the
        common case (a GET, or a fully uploaded body): the body reader saw a complete
        message, and only this tells the handler the client is gone. Never resolves for a
        request that completes normally (hyper's `poll_reset` stays Pending), so a host
        races it against its own completion (ASGI `http.disconnect`). If the connection
        died with no reason (transport error / EOF) the connection error is raised."""
        st = self._stream
        await st.reset_evt.wait()
        if st.reset_reason is not None:
            return _reason(st.reset_reason)
        err = st.error or self._manager._conn.error or ConnectionClosedError("connection closed")
        raise fresh_exc(err) from err

    def __repr__(self) -> str:
        return f"ServerRequest(method={self.method!r}, path={self.path!r})"


class SendStream:
    """The body half of a push-style response (returned by `ServerRequest.send_response`)
    — h2 share.rs `SendStream`: `send_data(data, end_of_stream)`, `send_trailers`,
    `send_reset`. Every DATA write is flow-control-gated (awaited for backpressure), and
    END_STREAM rides the final frame the caller marks."""

    def __init__(self, manager, stream):
        self._manager = manager
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
        await self._manager._send_data(self._stream, bytes(chunk), end_stream)
        if end_stream:
            self._manager._finish_send(self._stream)
            await self._manager._after_response(self._stream)

    async def send_trailers(self, trailers: HeadersInput) -> None:
        """End the stream with a trailing HEADERS frame (h2 `SendStream::send_trailers`);
        connection-specific fields are rejected as for any HEADERS block (RFC 9113
        §8.2.2, h2 `check_headers`)."""
        self._check_open()
        hdrs = trailers if isinstance(trailers, HeaderMap) else HeaderMap(trailers)
        self._manager.check_send_headers(hdrs)
        self._done = True
        await self._manager._send_trailers(self._stream, hdrs)
        self._manager._finish_send(self._stream)
        await self._manager._after_response(self._stream)

    async def send_reset(self, reason: int | None = None) -> None:
        """Abort the stream with RST_STREAM (h2 `SendStream::send_reset`); defaults to
        CANCEL — "the stream is no longer needed" (RFC 9113 §7). A no-op once the
        response is complete or the stream is already closed."""
        if self._done:
            return
        self._done = True
        code = int(H2Reason.CANCEL) if reason is None else int(reason)
        await self._manager.reset_stream(self._stream, code)

    def _check_open(self):
        if self._done:
            raise RuntimeError("response body already complete")


class ServerStreamManager(StreamManager):
    """The accepting side: takes client-initiated streams, delivers requests,
    receives request bodies, sends responses. h2 `server::Peer` + the recv-stream
    count in `Counts`. All flow control / reset / SETTINGS logic is the shared
    `StreamManager`; only the role hooks + the response send live here."""

    def __init__(self, conn, *, max_concurrent_streams, initial_window_size, max_pending_accept_reset_streams):
        super().__init__(conn)
        self._max_pending_accept_reset_streams = max_pending_accept_reset_streams
        # Highest client stream id we've *seen* (h2 recv `next_stream_id`): a new
        # stream must be a larger odd id. Distinct from `_last_processed_id`, the
        # GOAWAY last-stream-id.
        self._last_recv_id = 0
        # Highest client stream id we actually accepted+delivered (h2
        # `last_processed_id`, recv.rs L167): reported in GOAWAY so the client
        # knows which streams were processed (a REFUSED stream must NOT count).
        self._last_processed_id = 0
        self._max_concurrent = max_concurrent_streams  # our advertised limit
        self._our_initial_window_size = initial_window_size
        # Delivery queue of incoming ServerRequests to the accept loop.
        self._incoming_send, self._incoming_recv = conn.backend.queue()
        # Graceful shutdown (two-phase, h2). `_graceful` = shutdown started (phase 1).
        # `_max_stream_id` = the last-stream-id from our most recent GOAWAY: 2^31-1 by
        # default and through phase 1 (so streams in the ping-RTT window are served, not
        # refused), lowered to `_last_processed_id` at phase 2, after which frames on
        # higher streams are silently ignored. `_shutdown_final` marks phase 2 reached —
        # only then does draining the last stream end the accept loop.
        self._graceful = False
        self._max_stream_id = _MAX_STREAM_ID
        self._shutdown_final = False
        # Rapid-Reset defence (CVE-2023-44487): stream ids queued to the accept loop
        # but not yet pulled by the app (`_pending_accept`), and the subset of those
        # the peer has already RST'd (`_remote_reset_pending`). A reset pending-accept
        # stream stops counting as "concurrent" but still holds a queue slot, so it
        # gets a separate, smaller cap.
        self._pending_accept = set()
        self._remote_reset_pending = set()

    # ===== role hooks (h2 server `Peer` / `Dyn`) =====

    def _ensure_not_idle(self, sid):
        """A frame on a stream the client has never opened (idle): an even id (the
        client can't open one) or an odd id above the highest we've seen. h2:
        proto/peer.rs `ensure_can_open` (L76) / streams.rs `ensure_not_idle`."""
        if sid % 2 == 0 or sid > self._last_recv_id:
            raise H2ProtocolError(int(H2Reason.PROTOCOL_ERROR), f"frame on idle stream {sid}")

    def _above_goaway(self, sid):
        # A client stream above the last-stream-id of our (phase-2) GOAWAY was refused;
        # late frames on it are silently ignored (F42). `_max_stream_id` is 2^31-1 until
        # we lower it, so this is inert before we actually GOAWAY.
        return sid > self._max_stream_id

    def _recv_headers_target(self, frame):
        # h2: streams.rs `recv_headers` -> recv.rs `open` (L127) on the server
        # peer. An existing stream => trailers (the shared caller handles it); a
        # locally- or recently-reset stream => swallow late frames; a valid new
        # odd id => open the request stream + deliver it (return None: the head is
        # fully handled here); a decreased id => connection PROTOCOL_ERROR (recv.rs
        # `open` L127 -> `library_go_away` L140); a wrong-parity id is caught by
        # `ensure_can_open` (peer.rs L76).
        sid = frame.stream_id
        st = self._streams.get(sid)
        if st is not None:
            if st.state.is_local_error():
                return None  # locally reset: swallow late frames "for some time"
            if st.state.is_closed():
                # Completed normally but still stored — the inline-write window
                # (`StreamManager._drop_closed_stored`): drop it and classify
                # below exactly as if already forgotten. For a server HEADERS
                # target the deterministic post-release answer is the
                # decreased-id connection error (h2 recv.rs `open` L127) — a
                # peer sending HEADERS after its own END_STREAM violates either
                # way, and the outcome must not depend on which side of the
                # window the frame lands on.
                self._drop_closed_stored(st)
            else:
                return st  # existing stream -> the shared trailers path
        reset_at = self._reset_streams.get(sid)
        if reset_at is not None:
            if self._conn.backend.monotonic() - reset_at <= _RESET_STREAM_SECS:
                return None  # recently reset -> swallow (else a late trailers tears down the conn)
            del self._reset_streams[sid]
        # A stream opened after our final (phase-2) graceful GOAWAY is silently
        # IGNORED, not refused (h2 recv_headers L431: id > max_stream_id is dropped).
        # Through phase 1 `_max_stream_id` is still 2^31-1, so requests already in
        # flight when we started the shutdown are served normally rather than refused.
        if sid > self._max_stream_id:
            return None
        # A new request: must be a strictly-increasing client-initiated (odd) id.
        if sid % 2 == 0 or sid <= self._last_recv_id:
            raise H2ProtocolError(int(H2Reason.PROTOCOL_ERROR), f"invalid new stream id {sid}")
        self._last_recv_id = sid
        if len(self._streams) >= self._max_concurrent:
            # Over the limit we advertised: refuse just this stream with
            # REFUSED_STREAM (h2 recv.rs `open` L145 -> counts.rs
            # `can_inc_num_recv_streams` L100).
            raise _StreamError(sid, int(H2Reason.REFUSED_STREAM))
        st = Stream(
            sid,
            self._conn.backend,
            send_window=self._peer.initial_window_size,
            recv_window=self._recv_init,
        )
        # state.rs `recv_open`: receiving the request HEADERS opens the stream.
        st.state.recv_open(eos=frame.end_stream, informational=False)
        self._streams[sid] = st
        # Now processed (h2 recv.rs L167 `last_processed_id`) — only accepted
        # streams count toward the GOAWAY last-stream-id, not refused ones.
        self._last_processed_id = sid
        self._apply_content_length(st, frame)  # may raise _StreamError
        req = ServerRequest(
            st,
            self,
            method=frame.method,
            scheme=frame.scheme,
            authority=frame.authority,
            path=frame.path,
            headers=frame.headers,
        )
        self._incoming_send.send(req)
        self._pending_accept.add(sid)  # queued, not yet pulled by the app
        if frame.end_stream:
            # recv_open already closed the recv half; deliver EOF (no request body).
            st.body_send.send(None)
        return None  # the request head is fully handled

    def _note_remote_reset(self, st):
        # h2 recv.rs L886 (see hyperium/hyper#2877): a peer resetting a stream the app
        # hasn't accepted yet leaves it in the accept queue consuming memory, but it
        # no longer counts as a concurrent stream — so MAX_CONCURRENT_STREAMS can't
        # gate a HEADERS+RST flood. A separate, smaller cap does; exceeding it is a
        # connection GOAWAY(ENHANCE_YOUR_CALM). Reset-then-accept traffic decrements
        # the count in `next_request`, so only a genuine flood trips it.
        if st.id in self._pending_accept:
            if len(self._remote_reset_pending) >= self._max_pending_accept_reset_streams:
                raise H2ProtocolError(int(H2Reason.ENHANCE_YOUR_CALM), "too_many_resets")
            self._remote_reset_pending.add(st.id)

    def _on_fail(self):
        self._incoming_send.send(None)  # end the accept loop

    def _stop_accepting(self):
        # End the accept loop after a graceful drain: unlike a peer-driven close
        # (where the read-pump's EOF path calls `_on_fail`), a server-initiated
        # shutdown must itself signal "no more requests" so `next_request` returns
        # None and the caller's `async for` exits.
        self._incoming_send.send(None)

    def _release_slot(self, st):
        # The server tracks no MAX_CONCURRENT slot (it gates on len(self._streams)),
        # but once graceful shutdown has reached PHASE 2 (the real last-id GOAWAY is
        # out), the connection is "done" as soon as the last in-flight stream closes —
        # end the accept loop so the caller's serve loop returns and closes (h2 `poll`
        # returns Ready when drained after the final GOAWAY). Through phase 1 we keep
        # serving (waiting on the ping RTT), so draining does NOT end the loop yet.
        if self._shutdown_final and not self._streams:
            self._stop_accepting()

    # ===== sending responses (h2 server.rs SendResponse::send_response) =====

    async def send_response(self, st, status, headers, body, trailers=None):
        """The pull path (`ServerRequest.respond`): HEADERS + the whole body (+ trailers).
        Built on the push primitives — `send_response_head`, the inherited `send_body`
        (which ends on `_finish_send`), `_after_response` — so the two paths cannot drift."""
        if trailers is not None:
            trailers = trailers if isinstance(trailers, HeaderMap) else HeaderMap(trailers)
            # Validate BEFORE the response HEADERS go out (h2 0.4.16 #925 `check_headers`
            # on trailers): a rejected call must leave the stream able to respond.
            self.check_send_headers(trailers)
        end_stream = body is None and trailers is None
        await self.send_response_head(st, status, headers, end_stream=end_stream)
        if not end_stream:
            # END_STREAM on the final DATA (or on the trailers frame), then `_finish_send`.
            await self.send_body(st, body, trailers)
            await self._after_response(st)

    async def send_response_head(self, st, status, headers, *, end_stream):
        # h2: server.rs `SendResponse::send_response(response, end_of_stream)` (L1236);
        # state transition = state.rs `send_open` (sending response HEADERS on the
        # recv-opened stream). With `end_stream` the response is complete here.
        if st.state.is_closed():
            # hyper's first `poll_reset` window (proto/h2/server.rs L458): the peer reset
            # (or the connection died) while the handler was still computing -> the
            # request is aborted with the peer's reason (`Error::new_h2(reason)`).
            raise (
                self._send_stopped_error(st)
                if st.reset_evt.is_set()
                else ConnectionClosedError("stream already closed")
            )
        hdrs = headers if isinstance(headers, HeaderMap) else HeaderMap(headers)
        # hyper's h2 server inserts `Date` when the app didn't set one (proto/h2/server.rs
        # L484 `entry(DATE).or_insert_with(date::update_and_header_value)`), gated by
        # `auto_date_header`. Same cached value the h1 encoder writes.
        if self._conn._auto_date_header and hdrs.get("date") is None:
            hdrs["date"] = http_date()
        # Same RFC 9113 §8.2.2 rejection as the client's request/trailer paths
        # (h2 send.rs `send_headers` -> `check_headers`): checked BEFORE the
        # state transition, so a rejected call leaves the stream untouched and
        # still able to send a valid response.
        self.check_send_headers(hdrs)
        st.state.send_open(eos=end_stream)  # send response HEADERS
        # Encoded + queued as ONE step under the pump's buffer lock: the HPACK encoder's
        # dynamic table mutates on encode, so encode order MUST equal wire order — two
        # handlers encoding here and then racing for the socket desynchronized the peer's
        # table (a `date` insertion overtaken by later 3-byte blocks referencing it; the
        # peer GOAWAYs PROTOCOL_ERROR). h2 encodes inside its connection task's write path.
        codec = self._conn.codec
        self._conn.enqueue_headers(
            st, lambda: codec.serialize_response_headers(st.id, status, hdrs, end_stream=end_stream)
        )
        if end_stream:
            self._close_stream(st)  # bodyless response — HEADERS closed the send half
            await self._after_response(st)

    async def _after_response(self, st):
        # h2 drops the request's RecvStream/SendStream once the response is sent.
        # If the app never consumed the request body, that drop (a) RST_STREAM(
        # NO_ERROR)s while the client is still sending so it stops — the nginx-compat
        # rule, `maybe_cancel` (streams.rs L1601) — and (b) returns the in-flight
        # body's connection-window capacity, `release_closed_capacity` (recv.rs L493).
        # Without this an unread upload (early 401/403/413) pins the connection recv
        # window and the client is never told to stop.
        if not st.state.is_closed():  # recv half still open -> client still sending
            await self.reset_stream(st, int(H2Reason.NO_ERROR))
        else:  # fully closed but body may sit buffered-unread -> just reclaim its window
            conn_wu = self._reclaim_stream_accounting(st)
            if conn_wu:
                self._conn.enqueue_frame(self._conn.codec.serialize_window_update(0, conn_wu))

    async def next_request(self):
        req = await self._incoming_recv.receive()
        if req is not None:
            # Accepted: it no longer occupies the accept queue, so drop it from the
            # Rapid-Reset bookkeeping (h2 `dec_num_remote_reset_streams`). If it was
            # a reset pending-accept stream, this frees a slot in the cap.
            self._pending_accept.discard(req._stream.id)
            self._remote_reset_pending.discard(req._stream.id)
        return req


class ServerConnection(H2ConnectionBase):
    """The server protocol driver: consumes the client preface, accepts requests,
    and reports the last-processed stream in GOAWAY. All the read-pump / dispatch /
    SETTINGS / GOAWAY machinery is the shared `H2ConnectionBase`."""

    def __init__(
        self,
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
    ):
        self._max_concurrent_streams = max_concurrent_streams
        # Our advertised windows / frame / header-list profile, defaulting to hyper's
        # (proto/h2/server.rs L36-40: 1 MB stream + connection windows, 16 KB frames,
        # 16 KB header list). `None` = hyper's default (its builders take `Into<Option<_>>`).
        self._initial_window_size = initial_window_size if initial_window_size is not None else _STREAM_WINDOW
        self._initial_connection_window_size = (
            initial_connection_window_size if initial_connection_window_size is not None else _CONN_WINDOW
        )
        self._max_frame_size = max_frame_size if max_frame_size is not None else _MAX_FRAME_SIZE
        if not _MIN_MAX_FRAME_SIZE <= self._max_frame_size <= _MAX_MAX_FRAME_SIZE:
            # Fail at construction, not at the first SETTINGS write (h2 frame/settings.rs
            # `set_max_frame_size` asserts the RFC range).
            raise ValueError(f"max_frame_size must be in [{_MIN_MAX_FRAME_SIZE}, {_MAX_MAX_FRAME_SIZE}]")
        self._max_header_list_size = max_header_list_size if max_header_list_size is not None else _MAX_HEADER_LIST_SIZE
        # hyper `http2::Builder::auto_date_header` (proto/h2/server.rs `date_header`, default true).
        self._auto_date_header = auto_date_header
        self._preface_buf = b""
        self._preface_ok = False
        super().__init__(
            transport,
            backend=backend,
            codec_role="server",
            settings=Settings(
                LocalSettings(
                    initial_window_size=self._initial_window_size,
                    max_frame_size=self._max_frame_size,
                    max_header_list_size=self._max_header_list_size,
                )
            ),
            # hyper `http2::Builder::max_send_buf_size` (proto/h2/server.rs DEFAULT_MAX_SEND_BUF_SIZE, 400 KB).
            max_send_buf_size=max_send_buf_size if max_send_buf_size is not None else _MAX_SEND_BUF_SIZE,
        )
        self.streams = ServerStreamManager(
            self,
            max_concurrent_streams=max_concurrent_streams,
            initial_window_size=self._initial_window_size,
            # hyper `max_pending_accept_reset_streams`: None = h2's default (20).
            max_pending_accept_reset_streams=(
                max_pending_accept_reset_streams if max_pending_accept_reset_streams is not None else _REMOTE_RESET_MAX
            ),
        )
        # hyper `max_local_error_reset_streams`: default Some(1024); None = NO limit (not advised).
        self.streams._max_local_error_resets = max_local_error_reset_streams
        self.streams._conn_recv_target = self._initial_connection_window_size  # raised via WINDOW_UPDATE(0) in _begin
        # DATA-framing budget: None = Auto (half the connection window, floored) —
        # h2 0.4.19 `server::Builder::data_frame_budget` resolved at handshake
        # (server.rs L1048-1069, L1536-1540).
        self.streams._resolve_data_frame_budget(data_frame_budget)
        # Grant a larger-than-default per-stream recv window immediately (see the
        # client for the rationale): a client that has processed our SETTINGS uploads
        # up to the advertised window before it ACKs, so we must already accept it.
        if self._initial_window_size > self.streams._recv_init:
            self.streams._recv_init = self._initial_window_size

    async def start(self):
        # h2: server.rs `handshake` (L365) — the server's connection preface is just
        # its SETTINGS (RFC 7540 §3.5); no readiness wait (it serves requests as they
        # arrive). The client's 24-byte preface is stripped in `_before_frames`. The
        # server does NOT advertise ENABLE_PUSH (it gates the *client's* push, which no
        # upstream sends server-side); it does advertise its window/frame/header-list
        # profile + MAX_CONCURRENT_STREAMS.
        settings = {
            "max_concurrent_streams": self._max_concurrent_streams,
            "initial_window_size": self._initial_window_size,
            "max_frame_size": self._max_frame_size,
            "max_header_list_size": self._max_header_list_size,
        }
        await self._begin(b"", settings)

    def _before_frames(self, data):
        # Strip the fixed 24-byte client preface (RFC 7540 §3.5) before framing
        # (h2 reads it in server.rs L1427-1441). Returns None until it's complete.
        if self._preface_ok:
            return data
        self._preface_buf += data
        if len(self._preface_buf) < len(PREFACE):
            return None
        if self._preface_buf[: len(PREFACE)] != PREFACE:
            raise H2ProtocolError(int(H2Reason.PROTOCOL_ERROR), "bad client connection preface")
        rest = self._preface_buf[len(PREFACE) :]
        self._preface_buf = b""
        self._preface_ok = True
        return rest

    def _goaway_last_stream_id(self):
        # The last request we actually *processed* (h2 `last_processed_id`), so the
        # client knows which requests were handled and which (higher, incl. refused)
        # are safe to retry.
        return self.streams._last_processed_id

    def next_request(self):
        # Exposed on the driver (like the h1 server) so the `BaseServer` accept
        # iterator is protocol-uniform.
        return self.streams.next_request()

    async def graceful_shutdown(self):
        # h2 `Connection::graceful_shutdown` (proto/connection.rs L620): a TWO-PHASE,
        # non-blocking shutdown. PHASE 1 — send GOAWAY(2^31-1, NO_ERROR) ("going away,
        # last-id not yet decided") + a shutdown PING, but keep accepting AND serving
        # streams (`_max_stream_id` stays 2^31-1). This is the whole point of the two
        # phases: a request the client already put on the wire before it saw our GOAWAY
        # is served, not refused. PHASE 2 fires on the PING's ack (`_on_pong`). Idempotent;
        # does NOT wait or close (the caller drives the connection to completion, mirroring
        # hyper-util's coordinator).
        if self.streams._graceful:
            return
        self.streams._graceful = True
        await self.send_frame(self.codec.serialize_go_away(_MAX_STREAM_ID, int(H2Reason.NO_ERROR)))
        await self.send_frame(self.codec.serialize_ping(_SHUTDOWN_PING))

    async def _on_pong(self, frame):
        # PHASE 2 of graceful shutdown: the shutdown PING's ack means the client has
        # processed everything it had sent before our GOAWAY(2^31-1), so the highest
        # stream we accepted (`_last_processed_id`) is the true last-processed id. Send
        # the final GOAWAY with it; streams above it are now ignored (`_recv_headers_target`),
        # and the connection closes once in-flight streams drain (h2 connection.rs L558-560).
        if frame.data != _SHUTDOWN_PING or not self.streams._graceful or self.streams._shutdown_final:
            return
        self.streams._shutdown_final = True
        self.streams._max_stream_id = self.streams._last_processed_id
        await self.send_frame(self.codec.serialize_go_away(self.streams._last_processed_id, int(H2Reason.NO_ERROR)))
        if not self.streams._streams:
            self.streams._stop_accepting()  # already drained -> end the accept loop now


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
    ) -> None:
        """Options mirror hyper `server::conn::http2::Builder` (defaults are hyper's; `None`
        = default unless noted): `max_concurrent_streams` (200), `initial_window_size`
        (hyper `initial_stream_window_size`, 1 MB), `initial_connection_window_size` (1 MB),
        `max_frame_size` (16 KB, RFC range enforced), `max_header_list_size` (16 KB),
        `max_pending_accept_reset_streams` (h2's 20), `max_local_error_reset_streams`
        (1024; None = NO limit), `auto_date_header`, `max_send_buf_size` (400 KB: per-stream
        cap on response DATA queued for the connection's writer — the sender awaits room,
        as it awaits flow-control window); plus h2's own `data_frame_budget` (None = Auto)."""
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
        )
