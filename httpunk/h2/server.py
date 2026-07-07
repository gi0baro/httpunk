"""HTTP/2 server — the accepting-side analogue of the client
(`client.py` + `connection.py` + `streams.py`), mirroring h2's `server.rs`.

Only the orchestration is new: the vendored codec (`H2Codec("server")`), the
stream state machine (`H2StreamState`), flow control (`H2FlowControl`) and the
SETTINGS sync (`settings.py`) are the same sans-IO core the client uses — the
codec is symmetric, so a server *receives* requests (HEADERS with `:method`/
`:path`) and *sends* responses (`serialize_response_headers`) with no Rust
changes. The flow-control logic here is copied from the client's `StreamManager`
(it is role-agnostic) so the server inherits the same fidelity.

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

import contextlib
import threading

from .._backend.tonio import TonioBackend
from .._httpunk import (
    H2Codec,
    H2FlowControl,
    H2FrameData as Data,
    H2FrameGoAway as GoAway,
    H2FrameHeaders as Headers,
    H2FramePing as Ping,
    H2FrameRstStream as RstStream,
    H2FrameSettings as SettingsFrame,
    H2FrameStreamError as StreamErrorFrame,
    H2FrameWindowUpdate as WindowUpdate,
    H2StreamError,
)
from ..exceptions import (
    ConnectionClosedError,
    GoAwayError,
    H2Error,
    H2FlowControlError,
    H2ProtocolError,
    H2Reason,
    StreamResetError,
)
from ..http import HeaderMap
from .connection import PREFACE
from .settings import Action, LocalSettings, PeerSettings, Settings
from .stream import Stream
from .streams import _StreamError


_READ_SIZE = 65536
_DEFAULT_WINDOW = 65_535
_DEFAULT_MAX_CONCURRENT = 100  # our advertised SETTINGS_MAX_CONCURRENT_STREAMS
_LOCAL_MAX_ERROR_RESETS = 1024  # h2 DEFAULT_LOCAL_RESET_COUNT_MAX
_RESET_STREAM_MAX = 50  # h2 DEFAULT_RESET_STREAM_MAX (locally-reset ids kept for late frames)
_RESET_STREAM_SECS = 1.0  # h2 DEFAULT_RESET_STREAM_SECS


class ServerRequest:
    """An incoming request + the handle to respond to it (h2: the `(Request,
    SendResponse)` pair yielded by the server `Connection`)."""

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
    def trailers(self):
        """Trailing request headers (a `HeaderMap`) if the client sent a trailers
        frame after the body, else None."""
        return self._stream.trailers

    async def aiter_bytes(self):
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

    async def read(self):
        return b"".join([chunk async for chunk in self.aiter_bytes()])

    async def respond(self, status, *, headers=None, body=None):
        """Send the response: HEADERS (+ body, flow-control-gated). `body` is None,
        `bytes`, or a (sync/async) iterable of `bytes`. h2: `SendResponse`."""
        await self._manager.send_response(self._stream, status, headers, body)

    def __repr__(self):
        return f"ServerRequest(method={self.method!r}, path={self.path!r})"


class ServerStreamManager:
    """Server-side streams: accept client-initiated streams, deliver requests,
    receive request bodies, send responses. Mirrors the client `StreamManager`,
    inverting send (responses) and recv (requests). h2: proto/streams/* server path."""

    def __init__(self, conn, *, max_concurrent_streams, initial_window_size):
        self._conn = conn
        self._streams = {}
        # Highest client stream id we've *seen* (h2 recv `next_stream_id`): a new
        # stream must be a larger odd id. Distinct from `_last_processed_id`
        # below, which is the GOAWAY last-stream-id.
        self._last_recv_id = 0
        # Highest client stream id we actually accepted+delivered (h2
        # `last_processed_id`, recv.rs L167): reported in GOAWAY so the client
        # knows which streams were processed (a REFUSED stream must NOT count).
        self._last_processed_id = 0
        # Streams we locally reset, kept briefly so late frames the client sent
        # before seeing our RST_STREAM are swallowed (h2 reset_stream_duration).
        self._reset_streams = {}
        # Peer-caused stream resets; too many => GOAWAY(ENHANCE_YOUR_CALM), the
        # Rapid-Reset / malformed-flood defence (h2 local_max_error_reset_streams).
        self._local_error_resets = 0
        self._goaway_last_id = None  # enforce a later GOAWAY may not raise last_stream_id
        self._max_concurrent = max_concurrent_streams  # our advertised limit
        self._recv_init = _DEFAULT_WINDOW  # request recv window (our advertised initial window)
        self._our_initial_window_size = initial_window_size

        self._peer = PeerSettings()  # the client's SETTINGS (limits on what we send)

        # Connection-level flow control (same model as the client).
        self._conn_send = H2FlowControl()  # what the client lets us send
        self._conn_send.inc_window(_DEFAULT_WINDOW)
        self._conn_recv = H2FlowControl()  # what we advertise for receiving
        self._conn_recv.inc_window(_DEFAULT_WINDOW)
        self._conn_recv.assign_capacity(_DEFAULT_WINDOW)
        self._conn_window_evt = conn.backend.event()
        self._send_window_lock = threading.Lock()

        # Delivery queue of incoming ServerRequests to the accept loop.
        self._incoming_send, self._incoming_recv = conn.backend.queue()

        self._goaway = None

    # ===== incoming requests (recv HEADERS) — h2 proto/streams recv path =====

    def recv_headers(self, frame):
        # h2: proto/streams/streams.rs `recv_headers` -> recv.rs `open` (L127) +
        # `recv_headers` (L156) on the server peer. Existing stream => trailers
        # (recv.rs `recv_trailers` L410); new stream => open a request.
        sid = frame.stream_id
        st = self._streams.get(sid)
        if st is not None:
            # HEADERS on an existing stream = trailers (must carry END_STREAM).
            if not frame.end_stream:
                raise _StreamError(sid, int(H2Reason.PROTOCOL_ERROR))
            st.state.recv_close()
            if not st.content_length_satisfied():
                raise _StreamError(sid, int(H2Reason.PROTOCOL_ERROR))
            st.trailers = frame.headers
            st.body_send.send(None)  # EOF
            self._close_stream(st)
            return
        # A new request. The client must use a client-initiated (odd) id, strictly
        # increasing (h2 peer.rs `ensure_can_open` L76: non-client-initiated ->
        # connection PROTOCOL_ERROR; recv.rs `open` L127 tracks id ordering).
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
        if frame.end_stream:
            # recv_open already closed the recv half; deliver EOF (no request body).
            st.body_send.send(None)

    def _apply_content_length(self, st, frame):
        # h2 recv.rs `recv_headers` content-length validation (requests too).
        raw = frame.headers.get("content-length")
        if raw is None:
            return
        if not raw or not raw.isdigit():
            raise _StreamError(st.id, int(H2Reason.PROTOCOL_ERROR))
        cl = int(raw)
        st.set_content_length(cl)
        if frame.end_stream and cl > 0:
            raise _StreamError(st.id, int(H2Reason.PROTOCOL_ERROR))

    def _ensure_not_idle(self, sid):
        """Raise a connection PROTOCOL_ERROR for a frame on a stream the client
        has never opened (idle) — an even id (client can't open one) or an odd id
        above the highest we've seen. h2: proto/peer.rs `ensure_can_open` (L76) /
        streams.rs `ensure_not_idle`."""
        if sid % 2 == 0 or sid > self._last_recv_id:
            raise H2ProtocolError(int(H2Reason.PROTOCOL_ERROR), f"frame on idle stream {sid}")

    def _recv_lookup(self, sid):
        """Resolve the stream for an inbound DATA frame, or classify why there
        isn't one — mirrors the client's `_recv_lookup` (streams.py) with server
        id semantics. Returns the Stream, None to *ignore* (locally-reset, still
        swallowing late frames), raises H2ProtocolError for an idle stream
        (connection GOAWAY), or _StreamError(STREAM_CLOSED) for a forgotten one."""
        st = self._streams.get(sid)
        if st is not None:
            if st.state.is_local_error():
                return None  # locally reset: swallow late frames "for some time"
            return st
        reset_at = self._reset_streams.get(sid)
        if reset_at is not None:
            if self._conn.backend.monotonic() - reset_at <= _RESET_STREAM_SECS:
                return None
            del self._reset_streams[sid]
        self._ensure_not_idle(sid)  # idle -> connection error
        raise _StreamError(sid, int(H2Reason.STREAM_CLOSED))  # forgotten -> RST that stream

    def _clear_expired_reset_streams(self):
        now = self._conn.backend.monotonic()
        for sid in [s for s, at in self._reset_streams.items() if now - at > _RESET_STREAM_SECS]:
            del self._reset_streams[sid]

    def _enqueue_reset_expiration(self, st):
        # Keep a locally-reset id briefly so late frames are swallowed, not errored
        # (h2 recv.rs `enqueue_reset_expiration`). Bounded to _RESET_STREAM_MAX.
        if not st.state.is_local_error():
            return
        self._clear_expired_reset_streams()
        if len(self._reset_streams) >= _RESET_STREAM_MAX:
            return
        self._reset_streams[st.id] = self._conn.backend.monotonic()

    async def recv_data(self, frame):
        # Request body — identical accounting to the client's recv_data
        # (h2 proto/streams/recv.rs `recv_data` L641): classify the stream, state
        # check, consume connection + stream recv windows, content-length, deliver.
        sz = len(frame.data)
        try:
            st = self._recv_lookup(frame.stream_id)
        except _StreamError, H2ProtocolError:
            # Forgotten (STREAM_CLOSED, connection survives) or idle (connection
            # dies). Either way account + reclaim the connection window (the peer
            # counted these bytes on the wire; h2 `ignore_data`).
            self._conn_recv.send_data(sz)
            await self._release_conn_capacity(sz)
            raise
        if st is None:  # locally-reset stream: swallow + reclaim
            self._conn_recv.send_data(sz)
            await self._release_conn_capacity(sz)
            return
        if not st.state.is_recv_streaming():
            raise H2ProtocolError(int(H2Reason.PROTOCOL_ERROR), f"unexpected DATA on stream {st.id}")
        self._conn_recv.send_data(sz)
        try:
            st.recv_flow.send_data(sz)
        except H2FlowControlError as exc:
            await self._release_conn_capacity(sz)
            raise _StreamError(st.id, int(H2Reason.FLOW_CONTROL_ERROR)) from exc
        if not st.dec_content_length(sz):
            await self._release_conn_capacity(sz)
            raise _StreamError(st.id, int(H2Reason.PROTOCOL_ERROR))
        if frame.end_stream and not st.content_length_satisfied():
            await self._release_conn_capacity(sz)
            raise _StreamError(st.id, int(H2Reason.PROTOCOL_ERROR))
        st.recv_unreleased += sz
        st.body_send.send(frame.data)
        if frame.end_stream:
            st.state.recv_close()
            st.body_send.send(None)  # EOF

    async def release_capacity(self, st, n):
        # h2 recv.rs `release_capacity` — return recv window as the app reads.
        st.recv_unreleased = max(0, st.recv_unreleased - n)
        st.recv_flow.assign_capacity(n)
        unclaimed = st.recv_flow.unclaimed_capacity()
        if unclaimed and not st.state.is_recv_end_stream():
            st.recv_flow.inc_window(unclaimed)
            await self._conn.send_frame(self._conn.codec.serialize_window_update(st.id, unclaimed))
        self._conn_recv.assign_capacity(n)
        conn_unclaimed = self._conn_recv.unclaimed_capacity()
        if conn_unclaimed:
            self._conn_recv.inc_window(conn_unclaimed)
            await self._conn.send_frame(self._conn.codec.serialize_window_update(0, conn_unclaimed))

    async def _release_conn_capacity(self, n):
        self._conn_recv.assign_capacity(n)
        conn_unclaimed = self._conn_recv.unclaimed_capacity()
        if conn_unclaimed:
            self._conn_recv.inc_window(conn_unclaimed)
            await self._conn.send_frame(self._conn.codec.serialize_window_update(0, conn_unclaimed))

    # ===== sending responses (send HEADERS + DATA) — h2 proto/streams send path =====

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
            return
        await self.send_body(st, body)

    async def send_body(self, st, body):
        # Stream the response body, END_STREAM on the final DATA frame, then close
        # the send half — identical to the client's send_body (h2 proto/streams/
        # send.rs `send_data` L297, flow-control-gated in `_reserve_send_window`).
        unset = object()
        pending = unset
        async for chunk in self._aiter_body(body):
            if pending is not unset:
                await self._send_data(st, pending, end_stream=False)
            pending = bytes(chunk)
        if pending is unset:
            await self._send_data(st, b"", end_stream=True)
        else:
            await self._send_data(st, pending, end_stream=True)
        st.state.send_close()
        self._close_stream(st)

    @staticmethod
    async def _aiter_body(body):
        if isinstance(body, (bytes, bytearray)):
            yield bytes(body)
        elif hasattr(body, "__aiter__"):
            async for chunk in body:
                yield chunk
        elif hasattr(body, "__iter__"):
            for chunk in body:
                yield chunk
        else:
            raise TypeError("body must be None, bytes, or an (async) iterable of bytes")

    async def _send_data(self, st, data, end_stream):
        if len(data) == 0:
            if end_stream:
                await self._conn.send_frame(self._conn.codec.serialize_data(st.id, b"", end_stream=True))
            return
        offset = 0
        while offset < len(data):
            n = await self._reserve_send_window(st, len(data) - offset)
            piece = data[offset : offset + n]
            last = end_stream and (offset + n == len(data))
            await self._conn.send_frame(self._conn.codec.serialize_data(st.id, piece, end_stream=last))
            offset += n

    async def _reserve_send_window(self, st, want):
        while True:
            if st.error is not None:
                raise st.error
            if self._conn.error is not None:
                raise self._conn.error
            if st.state.is_closed():
                raise StreamResetError(st.id, int(H2Reason.CANCEL))
            with self._send_window_lock:
                window = self._send_window(st)
                if window > 0:
                    n = min(window, want, self._peer.max_frame_size)
                    self._conn_send.send_data(n)
                    st.send_flow.send_data(n)
                    return n
            st.window_evt.clear()
            self._conn_window_evt.clear()
            if self._send_window(st) > 0:
                continue
            await st.window_evt.wait()

    def _send_window(self, st):
        return min(self._conn_send.window_size(), st.send_flow.window_size())

    # ===== SETTINGS + peer frames =====

    def apply_remote_settings(self, frame):
        # The client's SETTINGS: bounds on what we send (window / frame size / table).
        old_iws = self._peer.update(frame)
        self._conn.codec.set_send_header_table_size(self._peer.header_table_size)
        self._conn.codec.set_send_max_frame_size(self._peer.max_frame_size)
        if old_iws is not None:
            self._adjust_send_windows(old_iws, self._peer.initial_window_size)

    def apply_local_settings(self, local):
        # On the client's ACK of our SETTINGS, our advertised values take effect.
        if local.header_table_size is not None:
            self._conn.codec.set_recv_header_table_size(local.header_table_size)
        if local.max_frame_size is not None:
            self._conn.codec.set_max_recv_frame_size(local.max_frame_size)
        if local.max_header_list_size is not None:
            self._conn.codec.set_max_header_list_size(local.max_header_list_size)
        if local.initial_window_size is not None:
            self._adjust_recv_windows(local.initial_window_size)

    def _adjust_recv_windows(self, target):
        old = self._recv_init
        self._recv_init = target
        if target == old:
            return
        for st in list(self._streams.values()):
            if target > old:
                st.recv_flow.inc_window(target - old)
                st.recv_flow.assign_capacity(target - old)
            else:
                st.recv_flow.dec_recv_window(old - target)

    def _adjust_send_windows(self, old, new):
        for st in list(self._streams.values()):
            if new >= old:
                st.send_flow.inc_window(new - old)
                st.window_evt.set()
            else:
                st.send_flow.dec_send_window(old - new)

    def recv_window_update(self, frame):
        if frame.stream_id == 0:
            self._conn_send.inc_window(frame.increment)
            self._conn_window_evt.set()
            for st in list(self._streams.values()):
                st.window_evt.set()
            return
        st = self._streams.get(frame.stream_id)
        if st is None:
            if frame.stream_id in self._reset_streams:
                return  # locally reset -> ignore
            self._ensure_not_idle(frame.stream_id)  # idle -> connection error
            return  # forgotten stream -> ignore
        try:
            st.send_flow.inc_window(frame.increment)
        except H2FlowControlError as exc:
            raise _StreamError(frame.stream_id, int(H2Reason.FLOW_CONTROL_ERROR)) from exc
        st.window_evt.set()

    async def recv_reset(self, frame):
        # h2 streams.rs `recv_reset`: RST on stream 0 is a connection error.
        if frame.stream_id == 0:
            raise H2ProtocolError(int(H2Reason.PROTOCOL_ERROR), "RST_STREAM on stream 0")
        st = self._streams.get(frame.stream_id)
        if st is None:
            if frame.stream_id in self._reset_streams:
                return  # locally reset -> ignore
            self._ensure_not_idle(frame.stream_id)  # idle -> connection error
            return  # forgotten stream -> ignore
        st.state.recv_reset(frame.stream_id, frame.error_code, queued=False)
        st.error = StreamResetError(frame.stream_id, frame.error_code)
        st.headers_evt.set()
        st.body_send.send(None)
        st.window_evt.set()
        if st.recv_unreleased:
            n, st.recv_unreleased = st.recv_unreleased, 0
            await self._release_conn_capacity(n)
        self._close_stream(st)

    async def reset_on_error(self, stream_id, reason):
        # Reset a stream after a client-caused stream-level violation. Too many
        # such resets => GOAWAY(ENHANCE_YOUR_CALM) (h2 counts.rs
        # `max_local_error_resets`; the Rapid-Reset / malformed-flood defence —
        # matters more on a server, which faces untrusted peers).
        st = self._streams.get(stream_id)
        if st is not None:
            self._local_error_resets += 1
            if self._local_error_resets > _LOCAL_MAX_ERROR_RESETS:
                raise H2ProtocolError(int(H2Reason.ENHANCE_YOUR_CALM), "too many stream resets")
            st.error = StreamResetError(stream_id, reason)
            if not st.state.is_closed():
                st.state.set_reset(st.id, reason, "library")
            await self._conn.send_frame(self._conn.codec.serialize_rst_stream(stream_id, reason))
            st.headers_evt.set()
            st.body_send.send(None)
            st.window_evt.set()
            if st.recv_unreleased:
                n, st.recv_unreleased = st.recv_unreleased, 0
                await self._release_conn_capacity(n)
            self._close_stream(st)
            self._enqueue_reset_expiration(st)
        else:
            # A forgotten / refused stream (no local object). Just tell the client
            # it's closed; do NOT count it toward the reset cap (h2 counts only
            # `reset_on_recv_stream_err` for live streams).
            with contextlib.suppress(Exception):
                await self._conn.send_frame(self._conn.codec.serialize_rst_stream(stream_id, reason))

    def _close_stream(self, st):
        if st.state.is_closed():
            self._streams.pop(st.id, None)

    def _abort_stream(self, st, exc):
        if st.error is None:
            st.error = exc
        st.headers_evt.set()
        st.body_send.send(None)
        st.window_evt.set()
        self._streams.pop(st.id, None)

    def handle_go_away(self, last_stream_id, exc):
        # The client is shutting the connection down; stop accepting new requests.
        # A later GOAWAY may not raise the last-stream-id (h2 send.rs `recv_go_away`
        # L447) — that's a connection PROTOCOL_ERROR.
        if self._goaway_last_id is not None and last_stream_id > self._goaway_last_id:
            raise H2ProtocolError(int(H2Reason.PROTOCOL_ERROR), "GOAWAY may not raise last_stream_id")
        self._goaway_last_id = last_stream_id
        self._goaway = exc

    def fail_all(self, exc):
        for st in list(self._streams.values()):
            self._abort_stream(st, exc)
        self._incoming_send.send(None)  # end the accept loop

    async def next_request(self):
        return await self._incoming_recv.receive()


class ServerConnection:
    """Server-side protocol driver: read the client preface + SETTINGS, run the
    read-pump, dispatch. Mirrors the client `Connection`."""

    def __init__(
        self, transport, *, backend=None, max_concurrent_streams=_DEFAULT_MAX_CONCURRENT, initial_window_size=None
    ):
        self.backend = backend or TonioBackend()
        self.codec = H2Codec("server")
        self.error = None
        self._scope = self.backend.scope()
        self._send_lock = self.backend.lock()
        self._transport = transport
        self._max_concurrent_streams = max_concurrent_streams
        self._initial_window_size = initial_window_size
        self._preface_buf = b""
        self._preface_ok = False
        self._settings = Settings(LocalSettings(initial_window_size=initial_window_size))
        self.streams = ServerStreamManager(
            self, max_concurrent_streams=max_concurrent_streams, initial_window_size=initial_window_size
        )

    async def start(self):
        # h2: server.rs `handshake` (L365) / `Connection` setup — the server's
        # connection preface is just its SETTINGS (RFC 7540 §3.5). The client sends
        # the 24-byte preface + its SETTINGS; the pump strips the preface (h2 reads
        # it in server.rs L1427-1441) before framing.
        await self._scope.__aenter__()
        settings = {"enable_push": False, "max_concurrent_streams": self._max_concurrent_streams}
        if self._initial_window_size is not None:
            settings["initial_window_size"] = self._initial_window_size
        await self.send_frame(self.codec.serialize_settings(**settings))
        self._scope.spawn(self._read_pump())

    async def close(self):
        if self._transport is not None:
            self._transport.close()
        self._scope.cancel()
        await self._scope.__aexit__(None, None, None)

    async def send_frame(self, data):
        async with self._send_lock:
            await self._transport.send_all(data)

    def _consume_preface(self, data):
        # Strip the fixed 24-byte client preface (RFC 7540 §3.5) before framing.
        self._preface_buf += data
        if len(self._preface_buf) < len(PREFACE):
            return None
        if self._preface_buf[: len(PREFACE)] != PREFACE:
            raise H2ProtocolError(int(H2Reason.PROTOCOL_ERROR), "bad client connection preface")
        rest = self._preface_buf[len(PREFACE) :]
        self._preface_buf = b""
        self._preface_ok = True
        return rest

    async def _read_pump(self):
        try:
            while True:
                data = await self._transport.receive_some(_READ_SIZE)
                if not data:
                    self._fail(ConnectionClosedError("connection closed by peer"))
                    break
                if not self._preface_ok:
                    data = self._consume_preface(data)
                    if data is None:
                        continue
                for frame in self.codec.receive(data):
                    try:
                        await self._dispatch(frame)
                    except _StreamError as se:
                        await self.streams.reset_on_error(se.stream_id, se.reason)
                    except H2StreamError as se:
                        await self.streams.reset_on_error(se.args[0], se.args[1])
        except H2Error as exc:
            await self._send_goaway(exc)
            self._fail(exc)
        except Exception as exc:
            self._fail(exc)

    async def _send_goaway(self, exc):
        # GOAWAY carries the last stream we actually *processed* (h2
        # `last_processed_id`), so the client knows which requests were handled
        # and which (higher, incl. any refused) are safe to retry.
        reason = exc.args[0] if exc.args and isinstance(exc.args[0], int) else int(H2Reason.PROTOCOL_ERROR)
        with contextlib.suppress(Exception):
            await self.send_frame(self.codec.serialize_go_away(self.streams._last_processed_id, reason))

    async def _dispatch(self, frame):
        if isinstance(frame, SettingsFrame):
            await self._on_settings(frame)
        elif isinstance(frame, Headers):
            self.streams.recv_headers(frame)
        elif isinstance(frame, Data):
            await self.streams.recv_data(frame)
        elif isinstance(frame, WindowUpdate):
            self.streams.recv_window_update(frame)
        elif isinstance(frame, Ping):
            if not frame.ack:
                await self.send_frame(self.codec.serialize_ping_ack(frame.data))
        elif isinstance(frame, GoAway):
            self.streams.handle_go_away(
                frame.last_stream_id,
                GoAwayError(frame.last_stream_id, frame.error_code, frame.debug_data),
            )
        elif isinstance(frame, RstStream):
            await self.streams.recv_reset(frame)
        elif isinstance(frame, StreamErrorFrame):
            raise _StreamError(frame.stream_id, frame.error_code)

    async def _on_settings(self, frame):
        action, payload = self._settings.recv_settings(frame)
        if action is Action.APPLY_LOCAL:
            self.streams.apply_local_settings(payload)
        elif action is Action.ACK_AND_APPLY:
            await self.send_frame(self.codec.serialize_settings_ack())
            peer_frame, _is_initial = self._settings.take_remote()
            self.streams.apply_remote_settings(peer_frame)

    def _fail(self, exc):
        self.error = exc
        self.streams.fail_all(exc)


class H2Server:
    """An HTTP/2 server connection over a caller-supplied, already-accepted
    `transport` (BYO transport, like hyper's `server::conn::http2`; accepting the
    socket / TLS / ALPN and the accept loop are the caller's job).

    Use as an async context manager and iterate incoming requests:

        async with H2Server(transport) as server:
            async for request in server:
                await request.respond(200, body=b"hi")
    """

    def __init__(
        self, transport, *, backend=None, max_concurrent_streams=_DEFAULT_MAX_CONCURRENT, initial_window_size=None
    ):
        self._conn = ServerConnection(
            transport,
            backend=backend,
            max_concurrent_streams=max_concurrent_streams,
            initial_window_size=initial_window_size,
        )

    async def __aenter__(self):
        await self._conn.start()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        await self._conn.close()
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        request = await self._conn.streams.next_request()
        if request is None:  # connection closed / failed
            raise StopAsyncIteration
        return request

    async def accept(self):
        """Return the next incoming `ServerRequest`, or None once the connection
        has closed. (`async for` over the server is the ergonomic form.)"""
        return await self._conn.streams.next_request()
