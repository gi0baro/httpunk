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
from ..exceptions import (
    ConnectionClosedError,
    H2ProtocolError,
    H2Reason,
)
from ..http import HeaderMap
from .connection import PREFACE, H2ConnectionBase
from .settings import LocalSettings, Settings
from .stream import Stream
from .streams import _RESET_STREAM_SECS, StreamManager, _StreamError


if TYPE_CHECKING:
    from .._backend import BackendLike
    from ..types import Body, HeadersInput


_DEFAULT_MAX_CONCURRENT = 200  # hyper server SETTINGS_MAX_CONCURRENT_STREAMS (proto/h2/server.rs)
_REMOTE_RESET_MAX = 20  # h2 proto/mod.rs DEFAULT_REMOTE_RESET_STREAM_MAX (Rapid-Reset cap)
# hyper's HTTP/2 server profile (hyper `proto/h2/server.rs`): a 1 MB per-stream recv
# window and 1 MB connection recv window (vs the 65535 default), 16 KB max frame size,
# 16 KB max header-list size. We ship the hyper stack's tuned profile, not bare-h2
# defaults (AUDIT-2026-07-09 F24). Unlike the old cut, the server does NOT advertise
# ENABLE_PUSH (no upstream does — it is the client's setting to gate server push).
_STREAM_WINDOW = 1024 * 1024
_CONN_WINDOW = 1024 * 1024
_MAX_FRAME_SIZE = 16 * 1024
_MAX_HEADER_LIST_SIZE = 16 * 1024
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

    def __init__(self, stream, manager, *, method, scheme, authority, path, headers):
        self.method = method  # str, e.g. "GET"
        self.scheme = scheme  # str | None
        self.authority = authority  # str | None (the :authority pseudo-header)
        self.path = path  # str | None (the :path pseudo-header)
        self.target = path  # alias, symmetric with the client's Request.target
        self.headers = headers  # httpunk.http.HeaderMap
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
            chunk = await self._stream.body_recv.receive()
            if chunk is None:  # EOF (end of stream, reset, or connection failure)
                break
            await self._manager.release_capacity(self._stream, len(chunk))
            yield chunk
        if self._stream.error is not None:
            raise self._stream.error

    def read(self) -> Awaitable[bytes]:
        return read_all(self.aiter_bytes())

    def respond(self, status: int, *, headers: HeadersInput = None, body: Body = None) -> Awaitable[None]:
        """Send the response: HEADERS (+ body, flow-control-gated). `body` is None,
        `bytes`, or a (sync/async) iterable of `bytes`. h2: `SendResponse`."""
        return self._manager.send_response(self._stream, status, headers, body)

    def reset(self, error_code: int | None = None) -> Awaitable[None]:
        """Abort this stream with RST_STREAM instead of a normal response — e.g. when a
        handler fails. Defaults to INTERNAL_ERROR; a no-op once the response has already
        been sent. Only this stream is affected (h2 `SendResponse` drop / `send_reset`)."""
        reason = int(H2Reason.INTERNAL_ERROR) if error_code is None else int(error_code)
        return self._manager.reset_stream(self._stream, reason)

    def __repr__(self) -> str:
        return f"ServerRequest(method={self.method!r}, path={self.path!r})"


class ServerStreamManager(StreamManager):
    """The accepting side: takes client-initiated streams, delivers requests,
    receives request bodies, sends responses. h2 `server::Peer` + the recv-stream
    count in `Counts`. All flow control / reset / SETTINGS logic is the shared
    `StreamManager`; only the role hooks + the response send live here."""

    def __init__(self, conn, *, max_concurrent_streams, initial_window_size):
        super().__init__(conn)
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
            if len(self._remote_reset_pending) >= _REMOTE_RESET_MAX:
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

    async def send_response(self, st, status, headers, body):
        # h2: server.rs `SendResponse::send_response` (L1236); state transition =
        # state.rs `send_open` (sending response HEADERS on the recv-opened stream).
        if st.state.is_closed():
            raise ConnectionClosedError("stream already closed")
        hdrs = headers if isinstance(headers, HeaderMap) else HeaderMap(headers)
        end_stream = body is None
        st.state.send_open(eos=end_stream)  # send response HEADERS
        await self._conn.send_frame(
            self._conn.codec.serialize_response_headers(st.id, status, hdrs, end_stream=end_stream)
        )
        if end_stream:
            self._close_stream(st)  # bodyless response — HEADERS closed the send half
        else:
            await self.send_body(st, body)  # inherited: END_STREAM on the final DATA, then close
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
            await self._reclaim_stream_capacity(st)

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

    def __init__(self, transport, *, backend=None, max_concurrent_streams, initial_window_size=None):
        self._max_concurrent_streams = max_concurrent_streams
        # Our advertised per-stream recv window, defaulting to hyper's 1 MB.
        self._initial_window_size = initial_window_size if initial_window_size is not None else _STREAM_WINDOW
        self._preface_buf = b""
        self._preface_ok = False
        super().__init__(
            transport,
            backend=backend,
            codec_role="server",
            settings=Settings(
                LocalSettings(
                    initial_window_size=self._initial_window_size,
                    max_frame_size=_MAX_FRAME_SIZE,
                    max_header_list_size=_MAX_HEADER_LIST_SIZE,
                )
            ),
        )
        self.streams = ServerStreamManager(
            self, max_concurrent_streams=max_concurrent_streams, initial_window_size=self._initial_window_size
        )
        self.streams._conn_recv_target = _CONN_WINDOW  # raised via WINDOW_UPDATE(0) in _begin
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
            "max_frame_size": _MAX_FRAME_SIZE,
            "max_header_list_size": _MAX_HEADER_LIST_SIZE,
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
    ) -> None:
        self._conn = ServerConnection(
            transport,
            backend=backend,
            max_concurrent_streams=max_concurrent_streams,
            initial_window_size=initial_window_size,
        )
